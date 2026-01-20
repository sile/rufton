use std::io::{Read, Write};

#[derive(Debug)]
pub struct JsonRpcServer {
    min_token: mio::Token,
    max_token: mio::Token,
    listener: mio::net::TcpListener,
    clients: std::collections::HashMap<mio::Token, Client>,
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
            let old_send_buf_len = client.send_buf.len();
            if client.handle_mio_event(event).is_ok() {
                if old_send_buf_len == 0 && client.send_buf.len() > 0 {
                    poll.registry().reregister(
                        &mut client.stream,
                        event.token(),
                        mio::Interest::READABLE | mio::Interest::WRITABLE,
                    )?;
                } else if old_send_buf_len > 0 && client.send_buf.len() == 0 {
                    poll.registry().reregister(
                        &mut client.stream,
                        event.token(),
                        mio::Interest::READABLE,
                    )?;
                }
            } else {
                poll.registry().deregister(&mut client.stream)?;
                self.clients.remove(&event.token());
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

    pub fn try_recv(
        &mut self,
        poll: &mut mio::Poll,
    ) -> std::io::Result<Option<JsonRpcRequest<'_>>> {
        todo!()
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
    send_buf: Vec<u8>,
    send_buf_offset: usize,
}

impl Client {
    fn new(stream: mio::net::TcpStream) -> Self {
        Self {
            stream,
            recv_buf: vec![0; 4096],
            recv_buf_offset: 0,
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
                        // NOTE: Should limit the maximum buffer size (e.g. use ring buffer)
                        self.recv_buf.resize(self.recv_buf_offset * 2, 0);
                    }
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => {
                        self.recv_buf_offset += n;
                        // todo
                    }
                }
            }
        }
        if event.is_writable() {
            while self.send_buf.len() > self.send_buf_offset {
                match self.stream.write(&self.send_buf[self.send_buf_offset..]) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                    Err(e) => return Err(e),
                    Ok(0) => return Err(std::io::ErrorKind::ConnectionReset.into()),
                    Ok(n) => {
                        self.send_buf_offset += n;
                    }
                }
            }
            self.send_buf.clear();
            self.send_buf_offset = 0;
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

#[derive(Debug)]
pub struct JsonRpcRequest<'text> {
    json: nojson::RawJson<'text>,
    method: std::borrow::Cow<'text, str>,
    caller: Option<JsonRpcCaller>,
}

impl<'text> JsonRpcRequest<'text> {
    pub fn method(&self) -> &str {
        self.method.as_ref()
    }

    pub fn params(&self) -> Option<nojson::RawJsonValue<'text, '_>> {
        self.json
            .value()
            .to_member("params")
            .expect("infallible")
            .get()
    }

    pub fn is_notification(&self) -> bool {
        self.caller.is_none()
    }

    pub fn take_caller(self) -> Option<JsonRpcCaller> {
        self.caller
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }
}
