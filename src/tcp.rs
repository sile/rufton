use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

const LISTENER_TOKEN: mio::Token = mio::Token(0);
const FIRST_CONNECTION_TOKEN: mio::Token = mio::Token(1);
const MAX_CONNECTIONS: usize = 4096;

fn reregister_interest(
    poll: &mut mio::Poll,
    stream: &mut mio::net::TcpStream,
    token: mio::Token,
    interest: mio::Interest,
) -> std::io::Result<()> {
    poll.registry().reregister(stream, token, interest)
}

const INITIAL_RECV_BUF_SIZE: usize = 4096;

#[derive(Debug)]
struct LineBuffer {
    buf: Vec<u8>,
    offset: usize,
    next_line_start: usize,
}

impl LineBuffer {
    fn new() -> Self {
        Self {
            buf: vec![0; INITIAL_RECV_BUF_SIZE],
            offset: 0,
            next_line_start: 0,
        }
    }

    fn read_from(&mut self, stream: &mut mio::net::TcpStream) -> std::io::Result<()> {
        loop {
            match stream.read(&mut self.buf[self.offset..]) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
                Ok(0) if self.buf.len() == self.offset => {
                    self.buf.resize(self.offset * 2, 0);
                }
                Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                Ok(n) => self.offset += n,
            }
        }
        Ok(())
    }

    fn next_line_range(&mut self) -> Option<(usize, usize)> {
        let start = self.next_line_start;
        let end = self.buf[start..self.offset]
            .iter()
            .position(|b| *b == b'\n')
            .map(|p| start + p)?;
        self.next_line_start = end + 1;
        Some((start, end))
    }

    fn compact_if_needed(&mut self) {
        if self.next_line_start > 0 {
            self.buf.copy_within(self.next_line_start..self.offset, 0);
            self.offset -= self.next_line_start;
            self.next_line_start = 0;
        }
    }

    fn has_pending(&self) -> bool {
        self.offset > 0
    }

    fn line_slice(&self, start: usize, end: usize) -> &[u8] {
        &self.buf[start..end]
    }
}

#[derive(Debug)]
struct SendBuffer {
    buf: Vec<u8>,
    offset: usize,
}

impl SendBuffer {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            offset: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn enqueue<F>(&mut self, write: F) -> std::io::Result<()>
    where
        F: FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
    {
        write(&mut self.buf)
    }

    fn flush(&mut self, stream: &mut mio::net::TcpStream) -> std::io::Result<()> {
        while self.buf.len() > self.offset {
            match stream.write(&self.buf[self.offset..]) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
                Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                Ok(n) => self.offset += n,
            }
        }
        if self.offset > self.buf.len() / 2 {
            self.buf.copy_within(self.offset.., 0);
            self.buf.truncate(self.buf.len() - self.offset);
            self.offset = 0;
        }
        Ok(())
    }

    fn needs_writable(&self, is_connected: bool) -> bool {
        !is_connected || !self.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
enum ConnectionState {
    Connecting,
    Connected,
}

#[derive(Debug)]
struct Connection {
    id: PeerId,
    stream: mio::net::TcpStream,
    state: ConnectionState,
    recv: LineBuffer,
    send: SendBuffer,
}

impl Connection {
    fn new_connecting(id: PeerId, stream: mio::net::TcpStream) -> Self {
        Self {
            id,
            stream,
            state: ConnectionState::Connecting,
            recv: LineBuffer::new(),
            send: SendBuffer::new(),
        }
    }

    fn new_connected(id: PeerId, stream: mio::net::TcpStream) -> Self {
        Self {
            id,
            stream,
            state: ConnectionState::Connected,
            recv: LineBuffer::new(),
            send: SendBuffer::new(),
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self.state, ConnectionState::Connected)
    }

    fn desired_interest(&self) -> mio::Interest {
        if !self.is_connected() {
            mio::Interest::READABLE | mio::Interest::WRITABLE
        } else if self.send.is_empty() {
            mio::Interest::READABLE
        } else {
            mio::Interest::READABLE | mio::Interest::WRITABLE
        }
    }

    fn handle_mio_event(&mut self, event: &mio::event::Event) -> std::io::Result<()> {
        if !self.is_connected() && event.is_writable() {
            if let Some(err) = self.stream.take_error()? {
                return Err(err);
            }
            match self.stream.peer_addr() {
                Ok(_) => self.state = ConnectionState::Connected,
                Err(e) if e.kind() == std::io::ErrorKind::NotConnected => {
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }

        if event.is_readable() {
            self.recv.read_from(&mut self.stream)?;
        }
        if event.is_writable() {
            self.send.flush(&mut self.stream)?;
        }
        Ok(())
    }

    fn enqueue_and_flush<F>(&mut self, write: F) -> std::io::Result<bool>
    where
        F: FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
    {
        let was_empty = self.send.is_empty();
        self.send.enqueue(write)?;
        if self.is_connected() {
            self.send.flush(&mut self.stream)?;
        }
        let need_writable = self.send.needs_writable(self.is_connected());
        Ok(was_empty && need_writable)
    }
}

#[derive(Debug)]
struct TokenPool {
    start: usize,
    len: usize,
    next: usize,
    used: Vec<bool>,
    used_count: usize,
}

impl TokenPool {
    fn new(start: mio::Token, end_exclusive: mio::Token) -> std::io::Result<Self> {
        if end_exclusive.0 <= start.0 {
            return Err(std::io::Error::other("token range must be at least 1"));
        }
        let len = end_exclusive.0 - start.0;
        Ok(Self {
            start: start.0,
            len,
            next: 0,
            used: vec![false; len],
            used_count: 0,
        })
    }

    fn allocate(&mut self) -> std::io::Result<mio::Token> {
        if self.used_count >= self.len {
            return Err(std::io::Error::other("no available tokens"));
        }
        for _ in 0..self.len {
            let i = self.next;
            self.next = (self.next + 1) % self.len;
            if !self.used[i] {
                self.used[i] = true;
                self.used_count += 1;
                return Ok(mio::Token(self.start + i));
            }
        }
        Err(std::io::Error::other("no available tokens"))
    }

    fn release(&mut self, token: mio::Token) {
        if let Some(i) = self.index(token)
            && self.used[i] {
                self.used[i] = false;
                self.used_count = self.used_count.saturating_sub(1);
            }
    }

    fn index(&self, token: mio::Token) -> Option<usize> {
        if token.0 < self.start {
            return None;
        }
        let i = token.0 - self.start;
        if i >= self.len {
            return None;
        }
        Some(i)
    }
}

#[derive(Debug)]
struct PendingQueue {
    queue: std::collections::VecDeque<mio::Token>,
    set: std::collections::HashSet<mio::Token>,
}

impl PendingQueue {
    fn new() -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            set: std::collections::HashSet::new(),
        }
    }

    fn push(&mut self, token: mio::Token) {
        if self.set.insert(token) {
            self.queue.push_back(token);
        }
    }

    fn peek(&mut self) -> Option<mio::Token> {
        loop {
            let token = *self.queue.front()?;
            if self.set.contains(&token) {
                return Some(token);
            }
            self.queue.pop_front();
        }
    }

    fn pop(&mut self) -> Option<mio::Token> {
        let token = self.peek()?;
        self.queue.pop_front();
        self.set.remove(&token);
        Some(token)
    }

    fn remove(&mut self, token: mio::Token) {
        self.set.remove(&token);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PeerId {
    seqno: u64,
    token: mio::Token,
    addr: SocketAddr,
}

#[derive(Debug)]
pub struct LineFramedTcpSocket {
    poll: mio::Poll,
    events: mio::Events,
    listener: mio::net::TcpListener,
    listener_token: mio::Token,
    tokens: TokenPool,
    connections: std::collections::HashMap<mio::Token, Connection>,
    pending: PendingQueue,
    addr_to_token: std::collections::HashMap<SocketAddr, mio::Token>,
    next_peer_seqno: u64,
    read_timeout: Option<Duration>,
}

impl LineFramedTcpSocket {
    fn next_peer_id(&mut self, token: mio::Token, addr: SocketAddr) -> PeerId {
        let seqno = self.next_peer_seqno;
        self.next_peer_seqno += 1;
        PeerId { seqno, token, addr }
    }

    fn allocate_token(&mut self) -> std::io::Result<mio::Token> {
        self.tokens.allocate()
    }

    fn remove_connection(&mut self, token: mio::Token) -> Option<Connection> {
        let conn = self.connections.remove(&token)?;
        self.pending.remove(token);
        self.tokens.release(token);
        Some(conn)
    }

    fn accept_connections(&mut self) -> std::io::Result<()> {
        loop {
            match self.listener.accept() {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
                Ok((mut stream, addr)) => {
                    let token = self.allocate_token()?;
                    let id = self.next_peer_id(token, addr);
                    stream.set_nodelay(true)?;
                    self.poll.registry().register(
                        &mut stream,
                        id.token,
                        mio::Interest::READABLE,
                    )?;
                    self.connections
                        .insert(id.token, Connection::new_connected(id, stream));
                    self.addr_to_token.entry(addr).or_insert(id.token);
                }
            }
        }
        Ok(())
    }

    fn handle_connection_event(
        &mut self,
        event: &mio::event::Event,
    ) -> std::io::Result<Option<Connection>> {
        let token = event.token();
        let (need_reregister, add_pending, disconnected) =
            match self.connections.get_mut(&token) {
                None => return Ok(None),
                Some(conn) => {
                    let prev_interest = conn.desired_interest();
                    match conn.handle_mio_event(event) {
                        Ok(()) => {
                            let add_pending = event.is_readable() && conn.recv.has_pending();
                            let next_interest = conn.desired_interest();
                            let need_reregister = if next_interest != prev_interest {
                                Some(next_interest)
                            } else {
                                None
                            };
                            (need_reregister, add_pending, false)
                        }
                        Err(_) => (None, false, true),
                    }
                }
            };

        if add_pending {
            self.pending.push(token);
        }

        if let Some(interest) = need_reregister
            && let Some(conn) = self.connections.get_mut(&token) {
                self.poll
                    .registry()
                    .reregister(&mut conn.stream, token, interest)?;
            }

        if disconnected {
            let mut conn = self.remove_connection(token).expect("just found");
            self.poll.registry().deregister(&mut conn.stream)?;
            return Ok(Some(conn));
        }

        Ok(None)
    }

    fn next_line(&mut self) -> Option<(PeerId, &[u8])> {
        loop {
            let token = self.pending.peek()?;
            let range = {
                let conn = self.connections.get_mut(&token)?;
                match conn.recv.next_line_range() {
                    Some(range) => range,
                    None => {
                        conn.recv.compact_if_needed();
                        self.pending.pop();
                        continue;
                    }
                }
            };
            let conn = self.connections.get(&token)?;
            let line = conn.recv.line_slice(range.0, range.1);
            return Some((conn.id, line));
        }
    }

    fn pump_io(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.poll.poll(&mut self.events, timeout)?;
        let events: Vec<mio::event::Event> = self.events.iter().cloned().collect();
        let mut accept_pending = false;
        for event in events {
            if event.token() == self.listener_token {
                accept_pending = true;
            } else if let Some(conn) = self.handle_connection_event(&event)? {
                self.addr_to_token.remove(&conn.id.addr);
            }
        }
        if accept_pending {
            self.accept_connections()?;
        }
        Ok(())
    }

    fn get_or_connect(&mut self, dst: SocketAddr) -> std::io::Result<mio::Token> {
        if let Some(token) = self.addr_to_token.get(&dst).copied() {
            if self.connections.get(&token).is_some() {
                return Ok(token);
            }
            self.addr_to_token.remove(&dst);
        }

        let mut stream = mio::net::TcpStream::connect(dst)?;
        let token = self.allocate_token()?;
        stream.set_nodelay(true)?;
        self.poll.registry().register(
            &mut stream,
            token,
            mio::Interest::READABLE | mio::Interest::WRITABLE,
        )?;
        let peer_id = PeerId {
            seqno: 0,
            token,
            addr: dst,
        };
        self.connections
            .insert(token, Connection::new_connecting(peer_id, stream));
        self.addr_to_token.insert(dst, token);
        Ok(token)
    }
}

impl LineFramedTcpSocket {
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let poll = mio::Poll::new()?;
        let mut listener = mio::net::TcpListener::bind(addr)?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, mio::Interest::READABLE)?;

        let tokens = TokenPool::new(
            FIRST_CONNECTION_TOKEN,
            mio::Token(FIRST_CONNECTION_TOKEN.0 + MAX_CONNECTIONS),
        )?;

        Ok(Self {
            poll,
            events: mio::Events::with_capacity(128),
            listener,
            listener_token: LISTENER_TOKEN,
            tokens,
            connections: std::collections::HashMap::new(),
            pending: PendingQueue::new(),
            addr_to_token: std::collections::HashMap::new(),
            next_peer_seqno: 0,
            read_timeout: None,
        })
    }

    pub fn send_to(&mut self, buf: &[u8], dst: SocketAddr) -> std::io::Result<usize> {
        if buf.contains(&b'\n') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "payload must not contain newline",
            ));
        }

        let token = self.get_or_connect(dst)?;

        {
            let conn = self.connections.get_mut(&token).expect("just inserted");
            let prev_interest = conn.desired_interest();
            conn.enqueue_and_flush(|out| {
                out.extend_from_slice(buf);
                out.push(b'\n');
                Ok(())
            })?;
            let next_interest = conn.desired_interest();
            if next_interest != prev_interest {
                reregister_interest(&mut self.poll, &mut conn.stream, conn.id.token, next_interest)?;
            }
        }

        let _ = self.pump_io(Some(Duration::from_millis(0)));
        Ok(buf.len())
    }

    pub fn recv_from(&mut self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        if let Some((peer, line)) = self.next_line() {
            let n = line.len().min(buf.len());
            buf[..n].copy_from_slice(&line[..n]);
            return Ok((n, peer.addr));
        }

        let deadline = self.read_timeout.map(|d| Instant::now() + d);
        loop {
            let timeout = deadline.map(|limit| {
                let now = Instant::now();
                if limit <= now {
                    Duration::from_millis(0)
                } else {
                    limit - now
                }
            });

            self.pump_io(timeout)?;

            if let Some((peer, line)) = self.next_line() {
                let n = line.len().min(buf.len());
                buf[..n].copy_from_slice(&line[..n]);
                return Ok((n, peer.addr));
            }

            if let Some(limit) = deadline
                && Instant::now() >= limit {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "read timeout",
                    ));
                }
        }
    }

    pub fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.read_timeout = dur;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn token_pool_allocates_and_releases() {
        let start = mio::Token(1);
        let end = mio::Token(4);
        let mut pool = TokenPool::new(start, end).expect("token pool");

        let t1 = pool.allocate().expect("t1");
        let t2 = pool.allocate().expect("t2");
        let t3 = pool.allocate().expect("t3");
        assert_ne!(t1, t2);
        assert_ne!(t2, t3);
        assert_ne!(t1, t3);

        assert!(pool.allocate().is_err(), "pool should be exhausted");

        pool.release(t2);
        let t4 = pool.allocate().expect("t4");
        assert_eq!(t2, t4, "released token should be reusable");
    }

    #[test]
    fn pending_queue_dedup_and_fifo() {
        let mut pending = PendingQueue::new();
        let t1 = mio::Token(1);
        let t2 = mio::Token(2);

        pending.push(t1);
        pending.push(t2);
        pending.push(t1);

        assert_eq!(pending.peek(), Some(t1));
        assert_eq!(pending.peek(), Some(t1));
        assert_eq!(pending.pop(), Some(t1));
        assert_eq!(pending.pop(), Some(t2));
        assert_eq!(pending.pop(), None);

        pending.push(t1);
        pending.push(t2);
        pending.remove(t1);
        assert_eq!(pending.pop(), Some(t2));
        assert_eq!(pending.pop(), None);
    }

    #[test]
    fn line_buffer_extracts_lines_and_compacts() {
        let mut buf = LineBuffer::new();
        let data = b"hello\nworld\n";
        buf.buf[..data.len()].copy_from_slice(data);
        buf.offset = data.len();

        let r1 = buf.next_line_range().expect("first line");
        assert_eq!(buf.line_slice(r1.0, r1.1), b"hello");
        let r2 = buf.next_line_range().expect("second line");
        assert_eq!(buf.line_slice(r2.0, r2.1), b"world");

        assert!(buf.next_line_range().is_none());
        buf.compact_if_needed();
        assert_eq!(buf.offset, 0);
        assert!(!buf.has_pending());
    }

    #[test]
    fn send_and_receive_single_line() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let mut receiver = LineFramedTcpSocket::bind(addr).expect("bind receiver");
        let mut sender = LineFramedTcpSocket::bind(addr).expect("bind sender");

        receiver
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set timeout");

        let recv_addr = receiver.listener.local_addr().expect("local addr");
        let payload = b"ping";
        sender.send_to(payload, recv_addr).expect("send");

        let mut out = [0_u8; 16];
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match receiver.recv_from(&mut out) {
                Ok((n, peer)) => {
                    assert_eq!(&out[..n], payload);
                    assert!(peer.ip().is_loopback());
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    sender
                        .pump_io(Some(Duration::from_millis(10)))
                        .expect("pump sender");
                }
                Err(e) => panic!("recv: {e}"),
            }

            if Instant::now() >= deadline {
                panic!("recv timed out");
            }
        }
    }
}
