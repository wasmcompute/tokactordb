use tokactor::Actor;

use super::file::FNode;

#[derive(Debug)]
pub enum DbFile {
    Memory(FNode),
    System(std::fs::File),
}

impl Actor for DbFile {}

impl DbFile {
    pub fn in_memory() -> Self {
        Self::Memory(FNode::new())
    }

    // pub async fn system() -> Self {

    // }
}
