#[derive(Debug)]
pub struct RaftNode<S, T> {
    pub storage: S,
    pub transport: T,
}

pub trait JsonLinesStorage {}

pub type Todo = ();

pub trait JsonRpcTransport {
    type NodeId;

    fn broadcast(&mut self, message: Todo);
    fn send(&mut self, dst: &Self::NodeId, message: Todo);
    fn recv(&mut self) -> Option<(Self::NodeId, Todo)>;
}
