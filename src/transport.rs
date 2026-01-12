pub trait JsonLinesRpcTransport {
    fn broadcast<T>(&mut self, message: T) -> std::io::Result<()>
    where
        T: nojson::DisplayJson;

    fn send<T>(&mut self, dst: raftbare::NodeId, message: T) -> std::io::Result<()>
    where
        T: nojson::DisplayJson;

    fn recv(&mut self) -> std::io::Result<Option<(raftbare::NodeId, nojson::RawJsonOwned)>>;
}
