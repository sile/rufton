use std::io::{Read, Write};

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
            self.buf
                .copy_within(self.next_line_start..self.offset, 0);
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

    fn pending_bytes(&self) -> usize {
        self.buf.len().saturating_sub(self.offset)
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
    fn new_vec(start: mio::Token, end_exclusive: mio::Token) -> std::io::Result<Self> {
        let len = end_exclusive
            .0
            .checked_sub(start.0)
            .ok_or_else(|| std::io::Error::other("token range must be at least 1"))?;
        let mut slots = Vec::with_capacity(len);
        slots.resize_with(len, || None);
        Ok(Self {
            tokens: TokenPool::new(start, end_exclusive)?,
            connections: ConnectionStorage::Vec(slots),
            pending_lines: std::collections::BTreeSet::new(),
        })
    }

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

    fn pending_send_bytes(&self, token: mio::Token) -> Option<usize> {
        match &self.connections {
            ConnectionStorage::Vec(slots) => {
                let i = self.tokens.index(token)?;
                slots.get(i)?.as_ref().map(|conn| conn.send.pending_bytes())
            }
            ConnectionStorage::Map(connections) => {
                connections.get(&token).map(|conn| conn.send.pending_bytes())
            }
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

fn write_response_ok<T: nojson::DisplayJson>(
    buf: &mut Vec<u8>,
    request_id: &JsonRpcRequestId,
    result: T,
) -> std::io::Result<()> {
    writeln!(
        buf,
        r#"{{"jsonrpc":"2.0","id":{},"result":{}}}"#,
        nojson::Json(request_id),
        nojson::Json(result)
    )
}

fn write_response_err(
    buf: &mut Vec<u8>,
    request_id: Option<&JsonRpcRequestId>,
    code: i32,
    message: &str,
) -> std::io::Result<()> {
    writeln!(
        buf,
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}"}}}}"#,
        nojson::Json(request_id),
        code,
        message
    )
}

fn write_response_err_with_data<T: nojson::DisplayJson>(
    buf: &mut Vec<u8>,
    request_id: Option<&JsonRpcRequestId>,
    code: i32,
    message: &str,
    data: T,
) -> std::io::Result<()> {
    writeln!(
        buf,
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}","data":{}}}}}"#,
        nojson::Json(request_id),
        code,
        message,
        nojson::Json(data)
    )
}

fn write_request<T: nojson::DisplayJson>(
    buf: &mut Vec<u8>,
    id: Option<&JsonRpcRequestId>,
    method: &str,
    params: T,
) -> std::io::Result<()> {
    let method = nojson::Json(method);
    let params = nojson::Json(params);
    if let Some(id) = id {
        let id = nojson::Json(id);
        writeln!(
            buf,
            r#"{{"jsonrpc":"2.0","id":{id},"method":{method},"params":{params}}}"#,
        )
    } else {
        writeln!(
            buf,
            r#"{{"jsonrpc":"2.0","method":{method},"params":{params}}}"#,
        )
    }
}

#[derive(Debug)]
pub struct JsonRpcServer {
    listener_token: mio::Token,
    listener: mio::net::TcpListener,
    core: ConnectionCore,
    next_peer_seqno: u64,
}

impl JsonRpcServer {
    pub fn start(
        poll: &mut mio::Poll,
        min_token: mio::Token,
        max_token: mio::Token,
        listen_addr: std::net::SocketAddr,
    ) -> std::io::Result<Self> {
        if max_token.0 <= min_token.0 + 1 {
            return Err(std::io::Error::other("token range must be at least 2"));
        }

        let mut listener = mio::net::TcpListener::bind(listen_addr)?;
        poll.registry()
            .register(&mut listener, min_token, mio::Interest::READABLE)?;

        let core = ConnectionCore::new_vec(mio::Token(min_token.0 + 1), max_token)?;
        Ok(Self {
            listener_token: min_token,
            listener,
            core,
            next_peer_seqno: 0,
        })
    }

    pub fn addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<bool> {
        if event.token() == self.listener_token {
            loop {
                match self.listener.accept() {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
                    Err(e) => return Err(e),
                    Ok((mut stream, addr)) => {
                        let token = self.core.allocate_token()?;
                        let id = self.next_peer_id(token, addr);
                        stream.set_nodelay(true)?;
                        poll.registry()
                            .register(&mut stream, id.token, mio::Interest::READABLE)?;
                        self.core.insert(id.token, Connection::new_connected(id, stream));
                    }
                }
            }
        } else {
            match self.core.handle_mio_event(poll, event)? {
                HandleEventResult::NotFound => Ok(false),
                HandleEventResult::Handled | HandleEventResult::Disconnected(_) => Ok(true),
            }
        }
    }

    fn next_peer_id(&mut self, token: mio::Token, addr: std::net::SocketAddr) -> PeerId {
        let seqno = self.next_peer_seqno;
        self.next_peer_seqno += 1;
        PeerId { seqno, token, addr }
    }

    pub fn next_request_line(&mut self) -> Option<(PeerId, &[u8])> {
        self.core.next_line()
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
        let Some(client) = self.get_connection_mut(peer_id) else {
            return Ok(());
        };

        let should_reregister =
            client.enqueue_and_flush(|buf| write_response_ok(buf, request_id, result))?;
        if should_reregister {
            reregister_interest(
                poll,
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }
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
        let Some(client) = self.get_connection_mut(peer_id) else {
            return Ok(());
        };

        let should_reregister = client.enqueue_and_flush(|buf| {
            write_response_err(buf, request_id, code, message)
        })?;
        if should_reregister {
            reregister_interest(
                poll,
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }
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
        let Some(client) = self.get_connection_mut(peer_id) else {
            return Ok(());
        };

        let should_reregister = client.enqueue_and_flush(|buf| {
            write_response_err_with_data(buf, request_id, code, message, data)
        })?;
        if should_reregister {
            reregister_interest(
                poll,
                &mut client.stream,
                peer_id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }
        Ok(())
    }

    fn get_connection_mut(&mut self, peer_id: PeerId) -> Option<&mut Connection> {
        let conn = self.core.get_mut(peer_id.token)?;
        if conn.id.seqno != peer_id.seqno {
            return None;
        }
        Some(conn)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId {
    seqno: u64,
    token: mio::Token,
    pub addr: std::net::SocketAddr,
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

fn is_jsonrpc_2_0<'text, 'raw>(val: nojson::RawJsonValue<'text, 'raw>) -> bool {
    match val.to_unquoted_string_str() {
        Ok(version) => version == "2.0",
        Err(_) => false,
    }
}

fn require_jsonrpc_2_0<'text, 'raw>(
    val: nojson::RawJsonValue<'text, 'raw>,
) -> Result<(), nojson::JsonParseError> {
    if val.to_unquoted_string_str()? != "2.0" {
        return Err(val.invalid("unsupported JSON-RPC version"));
    }
    Ok(())
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
        let mut parts = RequestParts::new();
        let value = json.value();
        for (key, val) in value.to_object().ok()? {
            let key = key.to_unquoted_string_str().ok()?;
            parts.apply_member(key.as_ref(), val)?;
        }
        parts.finish(json)
    }
}

struct RequestParts<'text> {
    has_jsonrpc: bool,
    method: Option<std::borrow::Cow<'text, str>>,
    id: Option<JsonRpcRequestId>,
    params_index: Option<usize>,
}

impl<'text> RequestParts<'text> {
    fn new() -> Self {
        Self {
            has_jsonrpc: false,
            method: None,
            id: None,
            params_index: None,
        }
    }

    fn apply_member<'raw>(
        &mut self,
        key: &str,
        val: nojson::RawJsonValue<'text, 'raw>,
    ) -> Option<()> {
        match key {
            "jsonrpc" => {
                if !is_jsonrpc_2_0(val) {
                    return None;
                }
                self.has_jsonrpc = true;
            }
            "method" => {
                self.method = Some(val.to_unquoted_string_str().ok()?);
            }
            "id" => {
                self.id = Some(JsonRpcRequestId::try_from(val).ok()?);
            }
            "params" => {
                if !matches!(
                    val.kind(),
                    nojson::JsonValueKind::Object | nojson::JsonValueKind::Array
                ) {
                    return None;
                }
                self.params_index = Some(val.index());
            }
            _ => {}
        }
        Some(())
    }

    fn finish(self, json: nojson::RawJson<'text>) -> Option<JsonRpcRequest<'text>> {
        if !self.has_jsonrpc {
            return None;
        }
        Some(JsonRpcRequest {
            json,
            method: self.method?,
            params_index: self.params_index,
            id: self.id,
        })
    }
}

#[derive(Debug)]
pub struct JsonRpcResponse<'text> {
    json: nojson::RawJson<'text>,
    result: Result<usize, usize>,
    id: Option<JsonRpcRequestId>,
}

impl<'text> JsonRpcResponse<'text> {
    pub fn parse(line: &'text str) -> Result<Self, nojson::JsonParseError> {
        let json = nojson::RawJson::parse(line)?;
        let mut parts = ResponseParts::new();

        let value = json.value();
        for (key, val) in value.to_object()? {
            let key_str = key.to_unquoted_string_str()?;
            parts.apply_member(key_str.as_ref(), val)?;
        }

        parts.finish(json)
    }

    pub fn id(&self) -> Option<&JsonRpcRequestId> {
        self.id.as_ref()
    }

    pub fn result(
        &self,
    ) -> Result<nojson::RawJsonValue<'text, '_>, nojson::RawJsonValue<'text, '_>> {
        match self.result {
            Ok(i) => Ok(self.json.get_value_by_index(i).expect("bug")),
            Err(i) => Err(self.json.get_value_by_index(i).expect("bug")),
        }
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }

    pub fn into_json(self) -> nojson::RawJson<'text> {
        self.json
    }
}

struct ResponseParts {
    has_jsonrpc: bool,
    has_id: bool,
    id: Option<JsonRpcRequestId>,
    result_index: Option<usize>,
    error_index: Option<usize>,
}

impl ResponseParts {
    fn new() -> Self {
        Self {
            has_jsonrpc: false,
            has_id: false,
            id: None,
            result_index: None,
            error_index: None,
        }
    }

    fn apply_member<'text, 'raw>(
        &mut self,
        key: &str,
        val: nojson::RawJsonValue<'text, 'raw>,
    ) -> Result<(), nojson::JsonParseError> {
        match key {
            "jsonrpc" => {
                require_jsonrpc_2_0(val)?;
                self.has_jsonrpc = true;
            }
            "id" => {
                if !val.kind().is_null() {
                    self.id = Some(JsonRpcRequestId::try_from(val)?);
                }
                self.has_id = true;
            }
            "result" => {
                self.result_index = Some(val.index());
            }
            "error" => {
                if !val.to_member("code")?.required()?.kind().is_integer() {
                    return Err(val.invalid("non integer error code"));
                }
                self.error_index = Some(val.index());
            }
            _ => {}
        }
        Ok(())
    }

    fn finish<'text>(
        self,
        json: nojson::RawJson<'text>,
    ) -> Result<JsonRpcResponse<'text>, nojson::JsonParseError> {
        if !self.has_jsonrpc {
            return Err(json.value().invalid("missing \"jsonrpc\" member"));
        }
        if !self.has_id {
            return Err(json.value().invalid("missing \"id\" member"));
        }

        let result = if let Some(i) = self.result_index {
            Ok(i)
        } else if let Some(i) = self.error_index {
            Err(i)
        } else {
            return Err(json
                .value()
                .invalid("either \"result\" or \"error\" member is required"));
        };

        Ok(JsonRpcResponse {
            json,
            result,
            id: self.id,
        })
    }
}

#[derive(Debug)]
pub struct JsonRpcClient {
    core: ConnectionCore,
    addr_to_token: std::collections::HashMap<std::net::SocketAddr, mio::Token>,
}

impl JsonRpcClient {
    pub fn new(min_token: mio::Token, max_token: mio::Token) -> std::io::Result<Self> {
        Ok(Self {
            core: ConnectionCore::new_map(min_token, max_token)?,
            addr_to_token: std::collections::HashMap::new(),
        })
    }

    pub fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<bool> {
        match self.core.handle_mio_event(poll, event)? {
            HandleEventResult::NotFound => Ok(false),
            HandleEventResult::Handled => Ok(true),
            HandleEventResult::Disconnected(conn) => {
                self.addr_to_token.remove(&conn.id.addr);
                Ok(true)
            }
        }
    }

    pub fn send_request<T>(
        &mut self,
        poll: &mut mio::Poll,
        dst: std::net::SocketAddr,
        id: Option<&JsonRpcRequestId>,
        method: &str,
        params: T,
    ) -> std::io::Result<PeerId>
    where
        T: nojson::DisplayJson,
    {
        let token = if let Some(token) = self.addr_to_token.get(&dst).copied() {
            token
        } else {
            let mut stream = mio::net::TcpStream::connect(dst)?;
            let token = self.core.allocate_token()?;
            stream.set_nodelay(true)?;
            poll.registry().register(
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
            token
        };

        let conn = self.core.get_mut(token).expect("just inserted");
        let should_reregister =
            conn.enqueue_and_flush(|buf| write_request(buf, id, method, params))?;
        if should_reregister {
            reregister_interest(
                poll,
                &mut conn.stream,
                conn.id.token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )?;
        }
        Ok(conn.id)
    }

    pub fn pending_send_bytes(&self, dst: std::net::SocketAddr) -> usize {
        self.addr_to_token
            .get(&dst)
            .and_then(|token| self.core.pending_send_bytes(*token))
            .unwrap_or(0)
    }

    pub fn next_response_line(&mut self) -> Option<(PeerId, &[u8])> {
        self.core.next_line()
    }

    pub fn disconnect(&mut self, dst: std::net::SocketAddr) {
        let Some(token) = self.addr_to_token.remove(&dst) else {
            return;
        };
        if let Some(conn) = self.core.remove(token) {
            let _ = conn.stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_server_basic_flow() -> std::io::Result<()> {
        fn run() -> std::io::Result<()> {
            let mut poll = mio::Poll::new()?;
            let mut events = mio::Events::with_capacity(128);

            let listen_addr = ([127, 0, 0, 1], 0).into();
            let mut server =
                JsonRpcServer::start(&mut poll, mio::Token(0), mio::Token(4), listen_addr)?;
            let server_addr = server.addr()?;

            let mut client = JsonRpcClient::new(mio::Token(5), mio::Token(9))?;
            let req_id = JsonRpcRequestId::Integer(1);
            let empty_params = nojson::object(|_| Ok(()));
            client.send_request(&mut poll, server_addr, Some(&req_id), "hello", empty_params)?;

            let mut success = false;
            for _ in 0..10 {
                poll.poll(&mut events, Some(std::time::Duration::from_millis(100)))?;

                for event in events.iter() {
                    let _ = server.handle_mio_event(&mut poll, event)?
                        || client.handle_mio_event(&mut poll, event)?;
                }
                while let Some((peer, line)) = server.next_request_line() {
                    let req = JsonRpcRequest::parse(line).expect("invalid request");
                    assert_eq!(req.method, "hello");
                    assert_eq!(req.id, Some(req_id.clone()));

                    server.reply_ok(&mut poll, peer, &req_id, "world")?;
                }
                while let Some((_peer, line)) = client.next_response_line() {
                    let line = std::str::from_utf8(line).expect("invalid response");
                    let res = JsonRpcResponse::parse(line).expect("invalid response");
                    assert_eq!(res.id, Some(req_id.clone()));
                    assert!(
                        res.result()
                            .expect("unexpected error response")
                            .to_unquoted_string_str()
                            .is_ok_and(|s| s == "world")
                    );
                    success = true;
                }
            }

            assert!(success);
            Ok(())
        }

        match run() {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping jsonrpc test: {e}");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
