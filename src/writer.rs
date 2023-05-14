use std::sync::{Arc, Mutex};

use crate::{database::Item, disk::Disk};

#[derive(serde::Serialize, serde::Deserialize)]
enum ItemType<'a> {
    Tree { id: u64, name: &'a str },
    Schema { tree_id: u64, name: String },
    Item { schema_id: u64, item: Item },
}

pub struct Wal {
    inner: Arc<Mutex<Disk>>,
}

impl Clone for Wal {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Wal {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Disk::new())),
        }
    }

    // pub fn write_tree(&self, id: u64, name: impl AsRef<str>) -> anyhow::Result<()> {
    //     let tree = ItemType::Tree {
    //         id,
    //         name: name.as_ref(),
    //     };
    //     item
    // }
}
