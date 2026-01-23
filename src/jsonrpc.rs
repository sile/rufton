use std::io::{Read, Write};

#[derive(Debug)]
pub struct JsonRpcServer {
    min_token: mio::Token,
    listener: mio::net::TcpListener,
    clients: Vec<Option<Peer>>,
    next_peer_seqno: u64,
    next_request_candidates: std::collections::BTreeSet<mio::Token>,
}

impl JsonRpcServer {
    pub fn start(
        poll: &mut mio::Poll,
        min_token: mio::Token,
        max_token: mio::Token,
        listen_addr: std::net::SocketAddr,
    ) -> std::io::Result<Self> {
        if max_token.0.saturating_sub(min_token.0) == 0 {
            return Err(std::io::Error::other("token range must be at least 2"));
        }

        let mut listener = mio::net::TcpListener::bind(listen_addr)?;
        poll.registry()
            .register(&mut listener, min_token, mio::Interest::READABLE)?;

        let capacity = max_token.0.saturating_sub(min_token.0);
        Ok(Self {
            min_token,
            listener,
            clients: std::iter::repeat_with(|| None).take(capacity).collect(),
            next_peer_seqno: 0,
            next_request_candidates: std::collections::BTreeSet::new(),
        })
    }

    pub fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<bool> {
        if event.token() == self.min_token {
            loop {
                match self.listener.accept() {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                    Err(e) => return Err(e),
                    Ok((mut stream, addr)) => {
                        let id = self.next_peer_id(addr)?;
                        stream.set_nodelay(true)?;
                        poll.registry()
                            .register(&mut stream, id.token, mio::Interest::READABLE)?;
                        let i = self.token_to_index(id.token);
                        self.clients[i] = Some(Peer::new(id, stream));
                    }
                }
            }
        } else if let Some(client) = self.get_client_mut(event.token()) {
            let is_old_send_buf_empty = client.send_buf.is_empty();
            if client.handle_mio_event(event).is_ok() {
                if is_old_send_buf_empty != client.send_buf.is_empty() {
                    let interest = if client.send_buf.is_empty() {
                        mio::Interest::READABLE
                    } else {
                        mio::Interest::READABLE | mio::Interest::WRITABLE
                    };
                    poll.registry()
                        .reregister(&mut client.stream, event.token(), interest)?;
                }

                if event.is_readable() && client.recv_buf_offset > 0 {
                    self.next_request_candidates.insert(event.token());
                }
            } else {
                let i = self.token_to_index(event.token());
                if let Some(mut client) = self.clients[i].take() {
                    poll.registry().deregister(&mut client.stream)?;
                }
                self.next_request_candidates.remove(&event.token());
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next_peer_id(&mut self, addr: std::net::SocketAddr) -> std::io::Result<PeerId> {
        // Find the first available slot
        for (i, slot) in self.clients.iter().enumerate() {
            if slot.is_none() {
                let token = self.index_to_token(i);
                let seqno = self.next_peer_seqno;
                self.next_peer_seqno += 1;
                return Ok(PeerId { seqno, token, addr });
            }
        }
        Err(std::io::Error::other("no available tokens"))
    }

    fn token_to_index(&self, token: mio::Token) -> usize {
        token.0.saturating_sub(self.min_token.0 + 1)
    }

    fn index_to_token(&self, i: usize) -> mio::Token {
        mio::Token(self.min_token.0 + i + 1)
    }

    fn get_client(&self, token: mio::Token) -> Option<&Peer> {
        let i = self.token_to_index(token);
        self.clients.get(i).and_then(|c| c.as_ref())
    }

    fn get_client_mut(&mut self, token: mio::Token) -> Option<&mut Peer> {
        let i = self.token_to_index(token);
        self.clients.get_mut(i).and_then(|c| c.as_mut())
    }

    pub fn next_request_line(&mut self) -> Option<(PeerId, &[u8])> {
        loop {
            let token = self.next_request_candidates.first().copied()?;
            let client = self.get_client_mut(token).expect("bug");
            let start = client.next_line_start;
            let Some(end) = client.recv_buf[start..client.recv_buf_offset]
                .iter()
                .position(|b| *b == b'\n')
                .map(|p| start + p)
            else {
                if client.next_line_start > 0 {
                    client
                        .recv_buf
                        .copy_within(client.next_line_start..client.recv_buf_offset, 0);
                    client.recv_buf_offset -= client.next_line_start;
                    client.next_line_start = 0;
                }
                self.next_request_candidates.remove(&token);
                continue;
            };
            client.next_line_start = end + 1;

            let client = self.get_client(token).expect("bug");
            let line = &client.recv_buf[start..end];
            return Some((client.id, line));
        }
    }

    pub fn reply_ok<T>(
        &mut self,
        poll: &mut mio::Poll,
        peer_id: PeerId,
        request_id: &JsonRpcRequestId,
        result: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        let Some(client) = self.get_client_mut(peer_id.token) else {
            return Ok(());
        };
        if client.id.seqno != peer_id.seqno {
            return Ok(());
        }

        if client.send_buf.is_empty() {
            poll.registry().reregister(
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }

        writeln!(
            client.send_buf,
            r#"{{"jsonrpc":"2.0","id":{},"result":{}}}"#,
            nojson::Json(request_id),
            nojson::Json(result)
        )?;
        Ok(())
    }

    pub fn reply_err(
        &mut self,
        poll: &mut mio::Poll,
        peer_id: PeerId,
        request_id: Option<&JsonRpcRequestId>,
        code: i32,
        message: &str,
    ) -> std::io::Result<()> {
        let Some(client) = self.get_client_mut(peer_id.token) else {
            return Ok(());
        };
        if client.id.seqno != peer_id.seqno {
            return Ok(());
        }

        if client.send_buf.is_empty() {
            poll.registry().reregister(
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }

        writeln!(
            client.send_buf,
            r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}"}}}}"#,
            nojson::Json(request_id),
            code,
            message
        )?;
        Ok(())
    }

    pub fn reply_err_with_data<T>(
        &mut self,
        poll: &mut mio::Poll,
        peer_id: PeerId,
        request_id: Option<&JsonRpcRequestId>,
        code: i32,
        message: &str,
        data: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        let Some(client) = self.get_client_mut(peer_id.token) else {
            return Ok(());
        };
        if client.id.seqno != peer_id.seqno {
            return Ok(());
        }

        if client.send_buf.is_empty() {
            poll.registry().reregister(
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }

        writeln!(
            client.send_buf,
            r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}","data":{}}}}}"#,
            nojson::Json(request_id),
            code,
            message,
            nojson::Json(data)
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId {
    seqno: u64,
    token: mio::Token,
    pub addr: std::net::SocketAddr,
}

#[derive(Debug)]
struct Peer {
    id: PeerId,
    stream: mio::net::TcpStream,
    recv_buf: Vec<u8>,
    recv_buf_offset: usize,
    next_line_start: usize,
    send_buf: Vec<u8>,
    send_buf_offset: usize,
}

impl Peer {
    fn new(id: PeerId, stream: mio::net::TcpStream) -> Self {
        Self {
            id,
            stream,
            recv_buf: vec![0; 4096],
            recv_buf_offset: 0,
            next_line_start: 0,
            send_buf: Vec::new(),
            send_buf_offset: 0,
        }
    }

    fn handle_mio_event(&mut self, event: &mio::event::Event) -> std::io::Result<()> {
        if event.is_readable() {
            loop {
                match self.stream.read(&mut self.recv_buf[self.recv_buf_offset..]) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                    Ok(0) if self.recv_buf.len() == self.recv_buf_offset => {
                        self.recv_buf.resize(self.recv_buf_offset * 2, 0);
                    }
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => self.recv_buf_offset += n,
                }
            }
        }
        if event.is_writable() {
            while self.send_buf.len() > self.send_buf_offset {
                match self.stream.write(&self.send_buf[self.send_buf_offset..]) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => {
                        self.send_buf_offset += n;
                    }
                }
            }
            if self.send_buf_offset > self.send_buf.len() / 2 {
                self.send_buf.copy_within(self.send_buf_offset.., 0);
                self.send_buf
                    .truncate(self.send_buf.len() - self.send_buf_offset);
                self.send_buf_offset = 0;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JsonRpcRequestId {
    Integer(i64),
    String(String),
}

impl nojson::DisplayJson for JsonRpcRequestId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            JsonRpcRequestId::Integer(n) => f.value(n),
            JsonRpcRequestId::String(s) => f.string(s),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for JsonRpcRequestId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        match value.kind() {
            nojson::JsonValueKind::Integer => value.try_into().map(Self::Integer),
            nojson::JsonValueKind::String => value.try_into().map(Self::String),
            _ => Err(value.invalid("id must be an integer or string")),
        }
    }
}

/// JSON-RPC 2.0 predefined error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonRpcPredefinedError {
    /// Invalid JSON was received by the server
    ParseError = -32700,
    /// The JSON sent is not a valid Request object
    InvalidRequest = -32600,
    /// The method does not exist / is not available
    MethodNotFound = -32601,
    /// Invalid method parameter(s)
    InvalidParams = -32602,
    /// Internal JSON-RPC error
    InternalError = -32603,
}

impl JsonRpcPredefinedError {
    pub fn code(self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid Request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
        }
    }
}

#[derive(Debug)]
pub struct JsonRpcRequest<'text> {
    json: nojson::RawJson<'text>,
    method: std::borrow::Cow<'text, str>,
    params_index: Option<usize>,
    id: Option<JsonRpcRequestId>,
}

impl<'text> JsonRpcRequest<'text> {
    pub fn parse(line: &'text [u8]) -> Result<Self, JsonRpcPredefinedError> {
        let json = std::str::from_utf8(line)
            .ok()
            .and_then(|line| nojson::RawJson::parse(line).ok())
            .ok_or(JsonRpcPredefinedError::ParseError)?;
        Self::from_json(json).ok_or(JsonRpcPredefinedError::InvalidRequest)
    }

    pub fn method(&self) -> &str {
        self.method.as_ref()
    }

    pub fn id(&self) -> Option<&JsonRpcRequestId> {
        self.id.as_ref()
    }

    pub fn params(&self) -> Option<nojson::RawJsonValue<'text, '_>> {
        self.params_index
            .and_then(|i| self.json.get_value_by_index(i))
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }

    pub fn into_json(self) -> nojson::RawJson<'text> {
        self.json
    }

    fn from_json(json: nojson::RawJson<'text>) -> Option<Self> {
        let value = json.value();
        let mut has_jsonrpc = false;
        let mut method = None;
        let mut id = None;
        let mut params_index = None;

        for (key, val) in value.to_object().ok()? {
            let key = key.to_unquoted_string_str().ok()?;
            match key.as_ref() {
                "jsonrpc" => {
                    if val.to_unquoted_string_str().ok()? != "2.0" {
                        return None;
                    }
                    has_jsonrpc = true;
                }
                "method" => {
                    method = Some(val.to_unquoted_string_str().ok()?);
                }
                "id" => {
                    id = Some(JsonRpcRequestId::try_from(val).ok()?);
                }
                "params" => {
                    if !matches!(
                        val.kind(),
                        nojson::JsonValueKind::Object | nojson::JsonValueKind::Array
                    ) {
                        return None;
                    }
                    params_index = Some(val.index());
                }
                _ => {}
            }
        }

        has_jsonrpc.then_some(Self {
            json,
            method: method?,
            params_index,
            id,
        })
    }
}

#[derive(Debug)]
pub struct JsonRpcClient {
    min_token: mio::Token,
    max_token: mio::Token,
    connections: std::collections::HashMap<std::net::SocketAddr, ClientConnection>,
    token_to_addr: std::collections::HashMap<mio::Token, std::net::SocketAddr>,
    next_token_offset: usize,
    next_request_candidates: std::collections::BTreeSet<mio::Token>,
}

impl JsonRpcClient {
    pub fn new(min_token: mio::Token, max_token: mio::Token) -> std::io::Result<Self> {
        if max_token.0 < min_token.0 {
            return Err(std::io::Error::other("token range must be at least 1"));
        }

        Ok(Self {
            min_token,
            max_token,
            connections: std::collections::HashMap::new(),
            token_to_addr: std::collections::HashMap::new(),
            next_token_offset: 0,
            next_request_candidates: std::collections::BTreeSet::new(),
        })
    }

    pub fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<bool> {
        let addr = match self.token_to_addr.get(&event.token()).copied() {
            Some(addr) => addr,
            None => return Ok(false),
        };

        let conn = match self.connections.get_mut(&addr) {
            Some(conn) => conn,
            None => return Ok(false),
        };

        let is_old_send_buf_empty = conn.send_buf.is_empty();
        if conn.handle_mio_event(event).is_ok() {
            if is_old_send_buf_empty != conn.send_buf.is_empty() {
                let interest = if conn.send_buf.is_empty() {
                    mio::Interest::READABLE
                } else {
                    mio::Interest::READABLE | mio::Interest::WRITABLE
                };
                poll.registry()
                    .reregister(&mut conn.stream, event.token(), interest)?;
            }

            if event.is_readable() && conn.recv_buf_offset > 0 {
                self.next_request_candidates.insert(event.token());
            }
        } else {
            // Connection error, remove it
            if let Some(mut conn) = self.connections.remove(&addr) {
                poll.registry().deregister(&mut conn.stream)?;
            }
            self.token_to_addr.remove(&event.token());
            self.next_request_candidates.remove(&event.token());
        }
        Ok(true)
    }

    pub fn send_request<T>(
        &mut self,
        poll: &mut mio::Poll,
        dst: std::net::SocketAddr,
        id: Option<&JsonRpcRequestId>,
        method: &str,
        params: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        // Ensure connection exists
        if !self.connections.contains_key(&dst) {
            let mut stream = mio::net::TcpStream::connect(dst)?;
            let token = self.next_token()?;
            stream.set_nodelay(true)?;
            poll.registry()
                .register(&mut stream, token, mio::Interest::READABLE)?;
            self.connections.insert(dst, ClientConnection::new(stream));
            self.token_to_addr.insert(token, dst);
        }

        let conn = self.connections.get_mut(&dst).expect("just inserted");
        if conn.send_buf.is_empty() {
            let token = self
                .token_to_addr
                .iter()
                .find(|(_, addr)| **addr == dst)
                .map(|(t, _)| *t)
                .expect("bug");
            poll.registry().reregister(
                &mut conn.stream,
                token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }

        let params = nojson::Json(params);
        if let Some(id) = id {
            let id = nojson::Json(id);
            writeln!(
                conn.send_buf,
                r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params}}}"#,
            )?;
        } else {
            writeln!(
                conn.send_buf,
                r#"{{"jsonrpc":"2.0","method":"{method}","params":{params}}}"#,
            )?;
        }
        Ok(())
    }

    pub fn pending_send_bytes(&self, dst: std::net::SocketAddr) -> usize {
        self.connections
            .get(&dst)
            .map(|conn| conn.send_buf.len().saturating_sub(conn.send_buf_offset))
            .unwrap_or(0)
    }

    pub fn next_response_line(&mut self) -> Option<(PeerId, &[u8])> {
        loop {
            let token = self.next_request_candidates.first().copied()?;
            let addr = self.token_to_addr.get(&token).copied()?;
            let conn = self.connections.get_mut(&addr)?;

            let start = conn.response_start;
            let Some(end) = conn.recv_buf[start..conn.recv_buf_offset]
                .iter()
                .position(|b| *b == b'\n')
                .map(|p| start + p)
            else {
                if conn.response_start > 0 {
                    conn.recv_buf
                        .copy_within(conn.response_start..conn.recv_buf_offset, 0);
                    conn.recv_buf_offset -= conn.response_start;
                    conn.response_start = 0;
                }
                self.next_request_candidates.remove(&token);
                continue;
            };
            conn.response_start = end + 1;

            let conn = self.connections.get(&addr)?;
            let line = &conn.recv_buf[start..end];
            let peer_id = PeerId {
                seqno: 0,
                token,
                addr,
            };
            return Some((peer_id, line));
        }
    }

    pub fn disconnect(&mut self, dst: std::net::SocketAddr) {
        if let Some(conn) = self.connections.remove(&dst) {
            let _ = conn.stream.shutdown(std::net::Shutdown::Both);
        }
        // Remove token mapping
        self.token_to_addr.retain(|_, addr| *addr != dst);
        // Remove from candidates
        self.next_request_candidates.retain(|token| {
            self.token_to_addr
                .get(token)
                .map(|a| *a != dst)
                .unwrap_or(true)
        });
    }

    fn next_token(&mut self) -> std::io::Result<mio::Token> {
        let capacity = self.max_token.0.saturating_sub(self.min_token.0);
        if self.connections.len() >= capacity {
            return Err(std::io::Error::other("no available tokens"));
        }

        // Find an available token
        loop {
            let token = mio::Token(self.min_token.0 + self.next_token_offset);
            self.next_token_offset = (self.next_token_offset + 1) % capacity;

            if !self.token_to_addr.contains_key(&token) {
                return Ok(token);
            }
        }
    }
}

#[derive(Debug)]
struct ClientConnection {
    stream: mio::net::TcpStream,
    recv_buf: Vec<u8>,
    recv_buf_offset: usize,
    response_start: usize,
    send_buf: Vec<u8>,
    send_buf_offset: usize,
}

impl ClientConnection {
    fn new(stream: mio::net::TcpStream) -> Self {
        Self {
            stream,
            recv_buf: vec![0; 4096],
            recv_buf_offset: 0,
            response_start: 0,
            send_buf: Vec::new(),
            send_buf_offset: 0,
        }
    }

    fn handle_mio_event(&mut self, event: &mio::event::Event) -> std::io::Result<()> {
        if event.is_readable() {
            loop {
                match self.stream.read(&mut self.recv_buf[self.recv_buf_offset..]) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                    Ok(0) if self.recv_buf.len() == self.recv_buf_offset => {
                        self.recv_buf.resize(self.recv_buf_offset * 2, 0);
                    }
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => self.recv_buf_offset += n,
                }
            }
        }
        if event.is_writable() {
            while self.send_buf.len() > self.send_buf_offset {
                match self.stream.write(&self.send_buf[self.send_buf_offset..]) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => {
                        self.send_buf_offset += n;
                    }
                }
            }
            if self.send_buf_offset > self.send_buf.len() / 2 {
                self.send_buf.copy_within(self.send_buf_offset.., 0);
                self.send_buf
                    .truncate(self.send_buf.len() - self.send_buf_offset);
                self.send_buf_offset = 0;
            }
        }
        Ok(())
    }
}
