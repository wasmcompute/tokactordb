use am::Message;
use tokio::sync::oneshot;

use super::item::Item;

pub struct Insert {
    pub tx: oneshot::Sender<()>,
    pub item: Item,
}

impl Insert {
    pub fn new(tx: oneshot::Sender<()>, item: Item) -> Self {
        Self { tx, item }
    }
}

impl Message for Insert {}

pub struct Rx {
    rx: oneshot::Receiver<()>,
}

impl Rx {
    pub fn new(rx: oneshot::Receiver<()>) -> Self {
        Self { rx }
    }
}

impl Message for Rx {}

#[derive(Debug)]
pub struct Flush;
impl Message for Flush {}
