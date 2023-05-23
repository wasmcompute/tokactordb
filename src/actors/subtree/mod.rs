mod aggregate;
mod index;

use std::sync::Arc;

use am::Message;
use tokio::sync::mpsc;

pub use aggregate::AggregateTreeActor;
pub use index::IndexTreeActor;
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

pub enum Request<ID: PrimaryKey, Value: RecordValue> {
    List((ID, oneshot::Sender<Vec<Value>>)),
    Item(
        (
            ID,
            usize,
            Box<dyn Fn(&mut Value) + Send + Sync + 'static>,
            oneshot::Sender<Option<()>>,
        ),
    ),
}

impl<ID: PrimaryKey, Value: RecordValue> std::fmt::Debug for Request<ID, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(arg0) => f.debug_tuple("List").field(arg0).finish(),
            Self::Item(arg0) => f
                .debug_tuple("Item")
                .field(&(&arg0.0, &arg0.1, &arg0.3))
                .finish(),
        }
    }
}

pub struct SubTree<ID: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<Request<ID, Value>>,
}

impl<ID: PrimaryKey, Value: RecordValue> SubTree<ID, Value> {
    pub fn new(inner: mpsc::Sender<Request<ID, Value>>) -> Self {
        Self { inner }
    }

    pub async fn list(&self, key: ID) -> anyhow::Result<Vec<Value>> {
        let (tx, rx) = oneshot::channel();
        self.inner.send(Request::List((key, tx))).await?;
        let list = rx.await?;
        Ok(list)
    }

    pub async fn mutate_by_index<F: Fn(&mut Value) + Send + Sync + 'static>(
        &self,
        id: ID,
        index: usize,
        f: F,
    ) -> anyhow::Result<Option<()>> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .send(Request::Item((id, index, Box::new(f), tx)))
            .await?;
        let list = rx.await?;
        Ok(list)
    }
}

pub struct AggregateTree<ID: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<(ID, oneshot::Sender<Option<Value>>)>,
}

impl<ID: PrimaryKey, Value: RecordValue> AggregateTree<ID, Value> {
    pub fn new(inner: mpsc::Sender<(ID, oneshot::Sender<Option<Value>>)>) -> Self {
        Self { inner }
    }

    pub async fn get(&self, key: ID) -> anyhow::Result<Option<Value>> {
        let (tx, rx) = oneshot::channel();
        self.inner.send((key, tx)).await?;
        let item = rx.await?;
        Ok(item)
    }
}

pub struct UtilTreeAddress<Tree, Key: PrimaryKey, Value: RecordValue> {
    pub ready_rx: oneshot::Receiver<()>,
    pub restorer: SubTreeRestorer,
    pub subscriber: SubTreeSubscriber<Key, Value>,
    pub tree: Tree,
}
