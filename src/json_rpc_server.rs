#[derive(Debug)]
pub struct JsonRpcServer {}

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
        event: &mio::event::Event,
    ) -> std::io::Result<()> {
        todo!()
    }

    pub fn call<T>(&mut self, dst: std::net::SocketAddr, method: &str, params: Option<T>) -> u64
    where
        T: nojson::DisplayJson,
    {
        todo!()
    }
}
