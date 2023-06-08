use std::path::PathBuf;

use tokactor::Message;
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

impl Message for Insert {}

pub struct Rx {}

impl Message for Rx {}

#[derive(Debug)]
pub struct Flush;
impl Message for Flush {}

#[derive(Debug)]
pub struct WalRestore {
    pub path: PathBuf,
}
impl Message for WalRestore {}

pub struct WalRestoredItems {
    pub items: Vec<Item>,
}

impl WalRestoredItems {
    pub fn new(items: Vec<Item>) -> Self {
        Self { items }
    }
}
impl Message for WalRestoredItems {}

#[derive(Debug)]
pub struct DumpWal(pub PathBuf);
impl Message for DumpWal {}
