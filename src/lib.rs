mod actors;
mod disk;
mod ids;
mod actors;
mod ids;

use std::fmt::{Debug, Display};

pub use actors::db::Database;
pub use actors::subtree::AggregateTree;
pub use actors::subtree::SubTree;
pub use actors::tree::Tree;
use actors::tree::{PrimaryKey, RecordValue};
pub use ids::*;
pub use relationships::*;

pub use record::{Aggregate, Change, Update};
pub use actors::fs::FileSystem;

/// Allow for an ID to be incrementable. Support the ability to increment the
/// ID inside the interal framework.
pub trait AutoIncrement: Ord + Default + Display + Debug + Clone {
    fn increment(&mut self) -> Self;
}

pub trait QueryTree<Key: PrimaryKey, Value: RecordValue> {
    type ID: PrimaryKey;

    fn bucket(value: &Value) -> Self::ID;
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum WalEvents {
    CreateSchema { id: u64, name: String },
    CreateTree { id: u64, parent: u64, name: String },
}
