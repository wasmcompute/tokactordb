mod actors;
mod database;
mod disk;
mod ids;
mod record;
mod relationships;
mod tx;

use std::fmt::{Debug, Display};

pub use actors::db::Database;
pub use actors::tree::Tree;
pub use ids::*;
pub use relationships::*;

// use database::{Record, Tree};
// pub use record::Collection;
pub use record::{Aggregate, Change, Update};

pub trait AutoIncrement: Ord + Default + Display + Debug + Clone {
    fn increment(&mut self) -> Self;
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum WalEvents {
    CreateSchema { id: u64, name: String },
    CreateTree { id: u64, parent: u64, name: String },
}
