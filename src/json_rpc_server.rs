#[derive(Debug)]
pub struct JsonRpcServer {}

#[expect(unused_variables)]
impl JsonRpcServer {
    pub fn start(
        poll: &mut mio::Poll,
        min_token: mio::Token,
        max_token: mio::Token,
        listen_addr: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn handle_event(
        &mut self,
        poll: &mut mio::Poll,
        event: &mio::event::Event,
    ) -> std::io::Result<()> {
        todo!()
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
