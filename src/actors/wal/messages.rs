use std::path::PathBuf;

use tokio::sync::oneshot;

use super::item::Item;

pub struct Insert {
    pub tx: oneshot::Sender<anyhow::Result<()>>,
    pub item: Item,
}

impl Insert {
    pub fn new(tx: oneshot::Sender<anyhow::Result<()>>, item: Item) -> Self {
        Self { tx, item }
    }
}

#[derive(Debug)]
pub struct Flush;

#[derive(Debug)]
pub struct WalRestore {
    pub path: PathBuf,
}

pub struct WalRestoredItems {
    pub items: Vec<Item>,
}

impl WalRestoredItems {
    pub fn new(items: Vec<Item>) -> Self {
        Self { items }
    }
}

#[derive(Debug)]
pub struct DumpWal(pub PathBuf);
