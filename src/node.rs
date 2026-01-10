#[derive(Debug)]
pub struct RaftNode<S, M> {
    pub storage: S,
    pub message_broker: M,
}

pub trait Storage {}

pub trait MessageBroker {}
