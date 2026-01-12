pub trait JsonLinesRpcTransport {
    type NodeAddr;

    fn register_node(&mut self, id: raftbare::NodeId, addr: Self::NodeAddr);
    fn deregister_node(&mut self, id: raftbare::NodeId);

    fn broadcast<T>(&mut self, message: T)
    where
        T: nojson::DisplayJson;

    fn send<T>(&mut self, dst: raftbare::NodeId, message: T)
    where
        T: nojson::DisplayJson;

    fn poll_event(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<Option<(raftbare::NodeId, nojson::RawJsonOwned)>>;
}
