#[derive(Debug)]
pub struct RaftNode<S, B> {
    pub storage: S,
    pub broker: B,
}

pub trait Storage {}

pub trait Broker {}
