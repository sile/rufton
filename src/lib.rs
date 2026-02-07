mod error;
mod node_core;
mod node_persist;
mod node_types;
pub mod tcp;

pub mod conv; // TODO: private
pub mod jsonrpc;
pub mod node;
pub mod storage;

pub use crate::jsonrpc::{
    JsonRpcPredefinedError, JsonRpcRequest, JsonRpcRequestId, JsonRpcResponse,
};
pub use crate::node::{
    Action, Command, JsonLineValue, Node, ProposalId, QueryMessage, RecentCommands, StorageEntry,
};
pub use crate::storage::FileStorage;
pub use crate::tcp::LineFramedTcpSocket;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod node_tests;
