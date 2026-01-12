pub trait JsonLinesRpcTransport {
    type NodeAddr: Transferrable;

    fn register_node(&mut self, id: raftbare::NodeId, addr: Self::NodeAddr);

    fn deregister_node(&mut self, id: raftbare::NodeId);

    fn broadcast<T>(&mut self, message: T)
    where
        T: nojson::DisplayJson;

    fn send<T>(&mut self, dst: raftbare::NodeId, message: T)
    where
        T: nojson::DisplayJson;

    fn poll_event(&mut self, timeout: std::time::Duration) -> std::io::Result<TransportEvent>;
}

pub trait Transferrable:
    nojson::DisplayJson + for<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>>
{
}

impl<T> Transferrable for T where
    T: nojson::DisplayJson + for<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>>
{
}

#[derive(Debug)]
pub enum TransportEvent {
    Recv {
        src: raftbare::NodeId,
        message: nojson::RawJsonOwned,
    },
    Timeout,
    Interrupted,
}
