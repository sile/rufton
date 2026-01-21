use std::io::{Read, Write};

#[derive(Debug)]
pub struct JsonRpcServer {
    min_token: mio::Token,
    max_token: mio::Token,
    listener: mio::net::TcpListener,
    clients: std::collections::HashMap<mio::Token, Client>,
    next_request_candidates: std::collections::BTreeSet<mio::Token>,
}

#[expect(unused_variables)]
impl JsonRpcServer {
    pub fn start(
        poll: &mut mio::Poll,
        min_token: mio::Token,
        max_token: mio::Token,
        listen_addr: std::net::SocketAddr,
    ) -> std::io::Result<Self> {
        if max_token.0.saturating_sub(min_token.0) == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "token range must be at least 2",
            ));
        }

        let mut listener = mio::net::TcpListener::bind(listen_addr)?;
        poll.registry()
            .register(&mut listener, min_token, mio::Interest::READABLE)?;

        Ok(Self {
            min_token,
            max_token,
            listener,
            clients: std::collections::HashMap::new(),
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
                    Ok((mut stream, _addr)) => {
                        let token = self.next_token()?;
                        stream.set_nodelay(true)?;
                        poll.registry()
                            .register(&mut stream, token, mio::Interest::READABLE)?;
                        self.clients.insert(token, Client::new(stream));
                    }
                }
            }
        } else if let Some(client) = self.clients.get_mut(&event.token()) {
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
                poll.registry().deregister(&mut client.stream)?;
                self.clients.remove(&event.token());
                self.next_request_candidates.remove(&event.token());
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next_token(&mut self) -> std::io::Result<mio::Token> {
        // NOTE: For simplicity, uses linear search to find a free token
        (self.min_token.0..=self.max_token.0)
            .map(mio::Token)
            .skip(1) // Excludes the listener token
            .find(|t| !self.clients.contains_key(&t))
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no available tokens"))
    }

    pub fn next_request(&mut self) -> Option<JsonRpcRequest<'_>> {
        loop {
            let token = self.next_request_candidates.first().copied()?;
            let client = self.clients.get_mut(&token).expect("bug");
            let start = client.request_start;
            let Some(end) = client.recv_buf[start..client.recv_buf_offset]
                .iter()
                .position(|b| *b == b'\n')
                .map(|p| start + p)
            else {
                if client.request_start > 0 {
                    client
                        .recv_buf
                        .copy_within(client.request_start..client.recv_buf_offset, 0);
                    client.recv_buf_offset -= client.request_start;
                    client.request_start = 0;
                }
                self.next_request_candidates.remove(&token);
                continue;
            };
            client.request_start = end;

            let client = self.clients.get(&token).expect("bug");
            let line = &client.recv_buf[start..end];
            return Some(JsonRpcRequest::new(token, line));
        }
    }

    pub fn reply_ok<T>(
        &mut self,
        poll: &mut mio::Poll,
        caller: JsonRpcCaller,
        result: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        todo!()
    }

    pub fn reply_err(
        &mut self,
        poll: &mut mio::Poll,
        caller: JsonRpcCaller,
        code: i32,
        message: &str,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn reply_err_with_data<T>(
        &mut self,
        poll: &mut mio::Poll,
        caller: JsonRpcCaller,
        code: i32,
        message: &str,
        data: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        todo!()
    }
}

#[derive(Debug)]
struct Client {
    stream: mio::net::TcpStream,
    recv_buf: Vec<u8>,
    recv_buf_offset: usize,
    request_start: usize,
    send_buf: Vec<u8>,
    send_buf_offset: usize,
}

impl Client {
    fn new(stream: mio::net::TcpStream) -> Self {
        Self {
            stream,
            recv_buf: vec![0; 4096],
            recv_buf_offset: 0,
            request_start: 0,
            send_buf: Vec::new(),
            send_buf_offset: 0,
        }
    }

    fn reply_null_id_err(
        &mut self,
        poll: &mut mio::Poll,
        token: mio::Token,
        error_code: JsonRpcPredefinedErrorCode,
        error_data: String,
    ) -> std::io::Result<()> {
        let response = nojson::object(|f| {
            f.member("jsonrpc", "2.0")?;
            f.member(
                "error",
                nojson::object(|f| {
                    f.member("code", error_code.code())?;
                    f.member("message", error_code.message())?;
                    f.member("data", &error_data)
                }),
            )?;
            f.member("id", ())
        });

        writeln!(self.send_buf, "{response}")?;

        // Update poll registration
        let interest = if self.send_buf.len() > self.send_buf_offset {
            mio::Interest::READABLE | mio::Interest::WRITABLE
        } else {
            mio::Interest::READABLE
        };

        poll.registry()
            .reregister(&mut self.stream, token, interest)?;
        Ok(())
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

#[expect(dead_code)]
#[derive(Debug)]
pub struct JsonRpcCaller {
    client: mio::Token,
    id: JsonRpcRequestId,
}

#[expect(dead_code)]
#[derive(Debug)]
enum JsonRpcRequestId {
    Integer(i64),
    String(String),
}

/// JSON-RPC 2.0 predefined error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonRpcPredefinedErrorCode {
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

impl JsonRpcPredefinedErrorCode {
    pub fn code(&self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
        }
    }

    pub fn message(&self) -> &'static str {
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
    token: mio::Token,
    line: &'text [u8],
    json: Result<nojson::RawJson<'text>, JsonRpcPredefinedErrorCode>,
    method: Option<std::borrow::Cow<'text, str>>,
    params_index: Option<usize>,
    id_index: Option<usize>,
}

impl<'text> JsonRpcRequest<'text> {
    pub fn new(token: mio::Token, line: &'text [u8]) -> Self {
        let json = std::str::from_utf8(line)
            .ok()
            .and_then(|line| nojson::RawJson::parse(line).ok())
            .ok_or(JsonRpcPredefinedErrorCode::ParseError);
        let mut this = Self {
            token,
            line,
            json,
            method: None,
            params_index: None,
            id_index: None,
        };
        if this.validate_and_init() {
            this.json = Err(JsonRpcPredefinedErrorCode::InvalidRequest);
        }
        this
    }

    pub fn token(&self) -> mio::Token {
        self.token
    }

    pub fn to_result(&self) -> Result<(), JsonRpcPredefinedErrorCode> {
        let json = self.json.as_ref().map_err(|&e| e)?;
        todo!()
    }

    fn validate_and_init(&mut self) -> bool {
        let Ok(json) = &self.json else { return true };
        let value = json.value();
        let mut has_jsonrpc = false;

        for (key, val) in value.to_object().into_iter().flatten() {
            let Ok(key) = key.to_unquoted_string_str() else {
                return false;
            };
            match key.as_ref() {
                "jsonrpc" => {
                    if !val.to_unquoted_string_str().is_ok_and(|s| s == "2.0") {
                        return false;
                    }
                    has_jsonrpc = true;
                }
                "method" => {
                    if val.kind() != nojson::JsonValueKind::String {
                        return false;
                    }
                    let Ok(m) = val.to_unquoted_string_str() else {
                        return false;
                    };
                    self.method = Some(m);
                }
                "id" => {
                    if !matches!(
                        val.kind(),
                        nojson::JsonValueKind::Integer | nojson::JsonValueKind::String
                    ) {
                        return false;
                    }
                    self.id_index = Some(val.index());
                }
                "params" => {
                    if !matches!(
                        val.kind(),
                        nojson::JsonValueKind::Object | nojson::JsonValueKind::Array
                    ) {
                        return false;
                    }
                    self.params_index = Some(val.index());
                }
                _ => {}
            }
        }

        has_jsonrpc && self.method.is_some()
    }
}
