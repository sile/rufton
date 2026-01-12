pub const API_NODE_ID: raftbare::NodeId = raftbare::NodeId::new(u64::MAX);

pub trait JsonLinesRpcTransport {
    fn broadcast<T>(&mut self, message: T) -> std::io::Result<()>
    where
        T: nojson::DisplayJson;

    fn send<T>(&mut self, dst: raftbare::NodeId, message: T) -> std::io::Result<()>
    where
        T: nojson::DisplayJson;

    fn recv(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<Option<(raftbare::NodeId, nojson::RawJsonOwned)>>;
}
