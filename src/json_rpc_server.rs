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

    pub fn try_recv(&mut self) -> Option<(std::net::SocketAddr, nojson::RawJson<'_>)> {
        todo!()
    }

    pub fn send<T>(
        &mut self,
        poll: &mut mio::Poll,
        dst: std::net::SocketAddr,
        response: T,
    ) -> std::io::Result<()>
    where
        T: nojson::DisplayJson,
    {
        todo!()
    }
}

#[derive(Debug)]
pub struct JsonRpcRequest<'text> {
    pub json: nojson::RawJson<'text>,
    pub method: std::borrow::Cow<'text, str>,
}

impl<'text> JsonRpcRequest<'text> {
    pub fn method(&self) -> &str {
        self.method.as_ref()
    }

    pub fn id(&self) -> Option<nojson::RawJsonValue<'text, '_>> {
        todo!()
    }

    pub fn params(&self) -> Option<nojson::RawJsonValue<'text, '_>> {
        todo!()
    }
}
