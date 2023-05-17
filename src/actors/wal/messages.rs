use am::Message;
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

impl Rx {
    pub fn new() -> Self {
        Self {}
    }
}

impl Message for Rx {}

#[derive(Debug)]
pub struct Flush;
impl Message for Flush {}
