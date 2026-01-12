#[derive(Debug)]
pub struct JsonRpcServer {}

#[expect(unused_variables)]
impl JsonRpcServer {
    pub fn new(
        poll: &mut mio::Poll,
        token: mio::Token,
        listen_addr: std::net::SocketAddr,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn handle_mio_event(
        &mut self,
        poll: &mut mio::Poll,
        next_token: &mut mio::Token,
        event: &mio::event::Event,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn try_recv(&mut self) -> Option<JsonRpcRequest<'_>> {
        todo!()
    }

    pub fn reply_ok<T>(
        &mut self,
        poll: &mut mio::Poll,
        caller: RpcCaller,
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
        caller: RpcCaller,
        code: i32,
        message: &str,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn reply_err_with_data<T>(
        &mut self,
        poll: &mut mio::Poll,
        caller: RpcCaller,
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
pub struct RpcCaller {
    client: mio::Token,
    id: Result<i64, String>, // TODO: Either
}

#[derive(Debug)]
pub struct JsonRpcRequest<'text> {
    json: nojson::RawJson<'text>,
    method: std::borrow::Cow<'text, str>,
    caller: Option<RpcCaller>,
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

    pub fn caller(&self) -> Option<&RpcCaller> {
        self.caller.as_ref()
    }

    pub fn take_caller(self) -> Option<RpcCaller> {
        self.caller
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }
}
