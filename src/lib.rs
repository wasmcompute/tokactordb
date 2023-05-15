mod actors;
mod database;
mod disk;
mod record;
mod tx;

use std::fmt::{Debug, Display};

pub use actors::db::Database;
pub use actors::tree::Tree;

// use database::{Record, Tree};
pub use record::Collection;
pub use record::{Aggregate, Change, Update};

pub trait AutoIncrement: Ord + Default + Display + Debug + Clone {
    fn increment(&mut self) -> Self;
}

impl AutoIncrement for u64 {
    fn increment(&mut self) -> Self {
        *self += 1;
        *self
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum WalEvents {
    CreateSchema { id: u64, name: String },
    CreateTree { id: u64, parent: u64, name: String },
}
