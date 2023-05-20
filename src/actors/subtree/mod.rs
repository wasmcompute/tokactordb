mod actor;

use std::sync::Arc;

use tokio::sync::mpsc;

pub use actor::{IndexTreeActor, IndexTreeAddresses};
use tokio::sync::oneshot;

use super::tree::{PrimaryKey, RecordValue};

pub struct SubTreeSubscriber<Key: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<(Arc<Key>, Arc<Value>)>,
}

impl<Key: PrimaryKey, Value: RecordValue> SubTreeSubscriber<Key, Value> {
    pub fn new(inner: mpsc::Sender<(Arc<Key>, Arc<Value>)>) -> Self {
        Self { inner }
    }
}

pub struct SubTree<ID: PrimaryKey, Value: RecordValue> {
    inner: mpsc::Sender<(ID, oneshot::Sender<Vec<Value>>)>,
}

impl<ID: PrimaryKey, Value: RecordValue> SubTree<ID, Value> {
    pub fn new(inner: mpsc::Sender<(ID, oneshot::Sender<Vec<Value>>)>) -> Self {
        Self { inner }
    }
}
