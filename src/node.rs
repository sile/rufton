#[derive(Debug)]
pub struct RaftNode<Storage, Transport> {
    pub storage: Storage,
    pub transport: Transport,
}

pub trait JsonLinesStorage {}

pub trait JsonLinesRpcTransport {
    fn broadcast<T>(&mut self, message: T)
    where
        T: nojson::DisplayJson;
    fn send<T>(&mut self, dst: raftbare::NodeId, message: T)
    where
        T: nojson::DisplayJson;
    fn recv(&mut self) -> Option<(raftbare::NodeId, nojson::RawJsonOwned)>;
}
