#[derive(Debug)]
pub struct RaftNode<Storage, Transport> {
    pub storage: Storage,
    pub transport: Transport,
}

pub trait JsonLinesStorage {}
