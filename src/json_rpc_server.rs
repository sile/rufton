#[derive(Debug)]
pub struct JsonLineRpcServer {}

#[expect(unused_variables)]
impl JsonLineRpcServer {
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
