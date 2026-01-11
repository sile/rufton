#[derive(Debug)]
pub struct RaftNode<Storage, Transport> {
    pub storage: Storage,
    pub transport: Transport,
}

pub trait JsonLinesStorage {}

pub trait JsonLinesRpcTransport {
    type NodeAddr;

    fn broadcast<T>(&mut self, message: T)
    where
        T: nojson::DisplayJson;
    fn send<T>(&mut self, dst: &Self::NodeAddr, message: T)
    where
        T: nojson::DisplayJson;
    fn recv(&mut self) -> Option<(Self::NodeAddr, nojson::RawJsonOwned)>;
    fn local_addr(&self) -> &Self::NodeAddr;
}
