use tokactor::ActorRef;

use self::{
    db::{RestoreComplete, RestoreItem},
    tree::TreeActor,
};

pub mod db;
pub mod subtree;
pub mod tree;
pub mod wal;

#[derive(Debug, Clone)]
pub struct GenericTree {
    inner: ActorRef<TreeActor>,
}

impl GenericTree {
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self { inner }
    }

    pub async fn send_restore_complete(&self) {
        self.inner.ask(RestoreComplete).await.unwrap();
    }

    pub async fn send_generic_item(&self, item: RestoreItem) {
        self.inner.send_async(item).await.unwrap();
    }
}
