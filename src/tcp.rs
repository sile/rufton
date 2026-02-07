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
        if let Some(i) = self.index(token) {
            if self.used[i] {
                self.used[i] = false;
                self.used_count = self.used_count.saturating_sub(1);
            }
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
enum ConnectionStorage {
    Vec(Vec<Option<Connection>>),
    Map(std::collections::HashMap<mio::Token, Connection>),
}

#[derive(Debug)]
struct ConnectionCore {
    tokens: TokenPool,
    connections: ConnectionStorage,
    pending_lines: std::collections::BTreeSet<mio::Token>,
}

#[derive(Debug)]
enum HandleEventResult {
    NotFound,
    Handled,
    Disconnected(Connection),
}

impl ConnectionCore {
    fn new_map(start: mio::Token, end_exclusive: mio::Token) -> std::io::Result<Self> {
        Ok(Self {
            tokens: TokenPool::new(start, end_exclusive)?,
            connections: ConnectionStorage::Map(std::collections::HashMap::new()),
            pending_lines: std::collections::BTreeSet::new(),
        })
    }

    fn allocate_token(&mut self) -> std::io::Result<mio::Token> {
        self.tokens.allocate()
    }

    fn insert(&mut self, token: mio::Token, connection: Connection) {
        match &mut self.connections {
            ConnectionStorage::Vec(slots) => {
                let i = self.tokens.index(token).expect("invalid token");
                slots[i] = Some(connection);
            }
            ConnectionStorage::Map(connections) => {
                connections.insert(token, connection);
            }
        }
    }

    fn get_mut(&mut self, token: mio::Token) -> Option<&mut Connection> {
        match &mut self.connections {
            ConnectionStorage::Vec(slots) => {
                let i = self.tokens.index(token)?;
                slots.get_mut(i)?.as_mut()
            }
            ConnectionStorage::Map(connections) => connections.get_mut(&token),
        }
    }

    fn get(&self, token: mio::Token) -> Option<&Connection> {
        match &self.connections {
            ConnectionStorage::Vec(slots) => {
                let i = self.tokens.index(token)?;
                slots.get(i)?.as_ref()
            }
            ConnectionStorage::Map(connections) => connections.get(&token),
        }
    }

    fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<HandleEventResult> {
        let token = event.token();
        let Some(conn) = self.get_mut(token) else {
            return Ok(HandleEventResult::NotFound);
        };

        let was_empty = conn.send.is_empty();
        if conn.handle_mio_event(event).is_ok() {
            if was_empty != conn.send.is_empty() {
                let interest = if conn.send.is_empty() {
                    mio::Interest::READABLE
                } else {
                    mio::Interest::READABLE | mio::Interest::WRITABLE
                };
                poll.registry()
                    .reregister(&mut conn.stream, token, interest)?;
            }

            if event.is_readable() && conn.recv.has_pending() {
                self.pending_lines.insert(token);
            }
            return Ok(HandleEventResult::Handled);
        }

        let mut conn = self.remove(token).expect("just found");
        poll.registry().deregister(&mut conn.stream)?;
        Ok(HandleEventResult::Disconnected(conn))
    }

    fn next_line(&mut self) -> Option<(PeerId, &[u8])> {
        loop {
            let token = *self.pending_lines.first()?;
            let range = {
                let conn = self.get_mut(token)?;
                match conn.recv.next_line_range() {
                    Some(range) => range,
                    None => {
                        conn.recv.compact_if_needed();
                        self.pending_lines.remove(&token);
                        continue;
                    }
                }
            };
            let conn = self.get(token)?;
            let line = conn.recv.line_slice(range.0, range.1);
            return Some((conn.id, line));
        }
    }

    fn remove(&mut self, token: mio::Token) -> Option<Connection> {
        let conn = match &mut self.connections {
            ConnectionStorage::Vec(slots) => {
                let i = self.tokens.index(token)?;
                slots.get_mut(i)?.take()
            }
            ConnectionStorage::Map(connections) => connections.remove(&token),
        }?;
        self.pending_lines.remove(&token);
        self.tokens.release(token);
        Some(conn)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PeerId {
    seqno: u64,
    token: mio::Token,
    addr: SocketAddr,
}

#[derive(Debug)]
struct TcpSocketInner {
    poll: mio::Poll,
    events: mio::Events,
    listener: mio::net::TcpListener,
    listener_token: mio::Token,
    core: ConnectionCore,
    addr_to_token: std::collections::HashMap<SocketAddr, mio::Token>,
    next_peer_seqno: u64,
    read_timeout: Option<Duration>,
}

impl TcpSocketInner {
    fn next_peer_id(&mut self, token: mio::Token, addr: SocketAddr) -> PeerId {
        let seqno = self.next_peer_seqno;
        self.next_peer_seqno += 1;
        PeerId { seqno, token, addr }
    }

    fn accept_connections(&mut self) -> std::io::Result<()> {
        loop {
            match self.listener.accept() {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
                Ok((mut stream, addr)) => {
                    let token = self.core.allocate_token()?;
                    let id = self.next_peer_id(token, addr);
                    stream.set_nodelay(true)?;
                    self.poll.registry().register(
                        &mut stream,
                        id.token,
                        mio::Interest::READABLE,
                    )?;
                    self.core.insert(id.token, Connection::new_connected(id, stream));
                    self.addr_to_token.entry(addr).or_insert(id.token);
                }
            }
        }
        Ok(())
    }

    fn pump_io(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.poll.poll(&mut self.events, timeout)?;
        for event in self.events.iter() {
            if event.token() == self.listener_token {
                self.accept_connections()?;
            } else {
                match self.core.handle_mio_event(&mut self.poll, event)? {
                    HandleEventResult::NotFound => {}
                    HandleEventResult::Handled => {}
                    HandleEventResult::Disconnected(conn) => {
                        self.addr_to_token.remove(&conn.id.addr);
                    }
                }
            }
        }
        Ok(())
    }

    fn get_or_connect(&mut self, dst: SocketAddr) -> std::io::Result<mio::Token> {
        if let Some(token) = self.addr_to_token.get(&dst).copied() {
            if self.core.get(token).is_some() {
                return Ok(token);
            }
            self.addr_to_token.remove(&dst);
        }

        let mut stream = mio::net::TcpStream::connect(dst)?;
        let token = self.core.allocate_token()?;
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
        self.core
            .insert(token, Connection::new_connecting(peer_id, stream));
        self.addr_to_token.insert(dst, token);
        Ok(token)
    }
}

#[derive(Debug)]
pub struct TcpSocket {
    inner: TcpSocketInner,
}

impl TcpSocket {
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let mut poll = mio::Poll::new()?;
        let mut listener = mio::net::TcpListener::bind(addr)?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, mio::Interest::READABLE)?;

        let core = ConnectionCore::new_map(
            FIRST_CONNECTION_TOKEN,
            mio::Token(FIRST_CONNECTION_TOKEN.0 + MAX_CONNECTIONS),
        )?;

        Ok(Self {
            inner: TcpSocketInner {
                poll,
                events: mio::Events::with_capacity(128),
                listener,
                listener_token: LISTENER_TOKEN,
                core,
                addr_to_token: std::collections::HashMap::new(),
                next_peer_seqno: 0,
                read_timeout: None,
            },
        })
    }

    pub fn send_to(&mut self, buf: &[u8], dst: SocketAddr) -> std::io::Result<usize> {
        if buf.iter().any(|b| *b == b'\n') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "payload must not contain newline",
            ));
        }

        let token = self.inner.get_or_connect(dst)?;

        {
            let conn = self.inner.core.get_mut(token).expect("just inserted");
            let should_reregister = conn.enqueue_and_flush(|out| {
                out.extend_from_slice(buf);
                out.push(b'\n');
                Ok(())
            })?;
            if should_reregister {
                reregister_interest(
                    &mut self.inner.poll,
                    &mut conn.stream,
                    conn.id.token,
                    mio::Interest::READABLE | mio::Interest::WRITABLE,
                )?;
            }
        }

        let _ = self.inner.pump_io(Some(Duration::from_millis(0)));
        Ok(buf.len())
    }

    pub fn recv_from(&mut self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        if let Some((peer, line)) = self.inner.core.next_line() {
            let n = line.len().min(buf.len());
            buf[..n].copy_from_slice(&line[..n]);
            return Ok((n, peer.addr));
        }

        let deadline = self.inner.read_timeout.map(|d| Instant::now() + d);
        loop {
            let timeout = deadline.map(|limit| {
                let now = Instant::now();
                if limit <= now {
                    Duration::from_millis(0)
                } else {
                    limit - now
                }
            });

            self.inner.pump_io(timeout)?;

            if let Some((peer, line)) = self.inner.core.next_line() {
                let n = line.len().min(buf.len());
                buf[..n].copy_from_slice(&line[..n]);
                return Ok((n, peer.addr));
            }

            if let Some(limit) = deadline {
                if Instant::now() >= limit {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "read timeout",
                    ));
                }
            }
        }
    }

    pub fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.inner.read_timeout = dur;
        Ok(())
    }
}
