mod actor;

use std::sync::Arc;

use tokio::sync::mpsc;

pub use actor::{IndexTreeActor, IndexTreeAddresses};
use tokio::sync::oneshot;

use crate::{Change, Update};

use super::tree::{PrimaryKey, RecordValue};

pub struct SubTreeSubscriber<Key: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<Change<Arc<Key>, Arc<Value>>>,
}

impl<Key: PrimaryKey, Value: RecordValue> SubTreeSubscriber<Key, Value> {
    pub fn new(inner: mpsc::Sender<Change<Arc<Key>, Arc<Value>>>) -> Self {
        Self { inner }
    }

    pub async fn created(&self, key: Arc<Key>, new: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Set { old: None, new },
        };
        self.inner.send(change).await?;
        Ok(())
    }

    pub async fn updated(
        &self,
        key: Arc<Key>,
        old: Option<Arc<Value>>,
        new: Arc<Value>,
    ) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Set { old, new },
        };
        self.inner.send(change).await?;
        Ok(())
    }

    pub async fn _delete(&self, key: Arc<Key>, old: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Del { old },
        };
        self.inner.send(change).await?;
        Ok(())
    }
}

pub struct SubTree<ID: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<(ID, oneshot::Sender<Vec<Value>>)>,
}

impl<ID: PrimaryKey, Value: RecordValue> SubTree<ID, Value> {
    pub fn new(inner: mpsc::Sender<(ID, oneshot::Sender<Vec<Value>>)>) -> Self {
        Self { inner }
    }

    pub async fn get(&self, key: ID) -> anyhow::Result<Vec<Value>> {
        let (tx, rx) = oneshot::channel();
        self.inner.send((key, tx)).await?;
        let list = rx.await?;
        Ok(list)
    }
}
