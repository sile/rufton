#[derive(Debug)]
pub struct RaftNode<S> {
    pub storage: S,
}

pub trait JsonLinesStorage {}

pub trait JsonRpcServer {}
