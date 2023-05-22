mod actor;

use std::sync::Arc;

use am::Message;
use tokio::sync::mpsc;

pub use actor::{IndexTreeActor, IndexTreeAddresses};
use tokio::sync::oneshot;

use crate::{Change, Update};

use super::tree::{PrimaryKey, RecordValue};

type SubTreeRestorerSender = mpsc::Sender<(Arc<Vec<u8>>, Arc<Option<Vec<u8>>>)>;

#[derive(Debug, Clone)]
pub struct SubTreeRestorer {
    inner: SubTreeRestorerSender,
}
impl Message for SubTreeRestorer {}

impl SubTreeRestorer {
    pub fn new(inner: SubTreeRestorerSender) -> Self {
        Self { inner }
    }

    pub async fn restore_record(&self, key: Arc<Vec<u8>>, value: Arc<Option<Vec<u8>>>) {
        self.inner.send((key, value)).await.unwrap();
    }
}

type SubTreeSubscriberSender<Key, Value> =
    mpsc::Sender<(Change<Arc<Key>, Arc<Value>>, oneshot::Sender<()>)>;

/// A address to message a sub tree given a collections key and value.
/// Send updates to a subcollection when the orignial collection changes.
pub struct SubTreeSubscriber<Key: PrimaryKey, Value: RecordValue> {
    inner: SubTreeSubscriberSender<Key, Value>,
}

impl<Key: PrimaryKey, Value: RecordValue> Clone for SubTreeSubscriber<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Key: PrimaryKey, Value: RecordValue> SubTreeSubscriber<Key, Value> {
    pub fn new(inner: SubTreeSubscriberSender<Key, Value>) -> Self {
        Self { inner }
    }

    pub async fn created(self, key: Arc<Key>, new: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Set { old: None, new },
        };
        self.send(change).await
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
        self.send(change).await
    }

    pub async fn _delete(&self, key: Arc<Key>, old: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Del { old },
        };
        self.send(change).await
    }

    async fn send(&self, change: Change<Arc<Key>, Arc<Value>>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel::<()>();
        self.inner.send((change, tx)).await?;
        let _ = rx.await;
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
