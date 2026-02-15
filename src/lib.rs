mod error;
mod node_core;
mod node_types;

pub mod conv; // TODO: private
pub mod jsonrpc;
pub mod node;
pub mod storage;

pub use crate::jsonrpc::{
    JsonRpcPredefinedError, JsonRpcRequest, JsonRpcRequestId, JsonRpcResponse,
};
pub use crate::node::{Action, Event, JsonValue, Node, NodeId, RecentCommands, StorageEntry};
pub use crate::storage::FileStorage;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod node_tests;
