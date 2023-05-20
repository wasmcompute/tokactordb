use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Tree,
};

use super::{SubTree, SubTreeSubscriber};

pub struct IndexTreeAddresses<ID: PrimaryKey, Key: PrimaryKey, Value: RecordValue> {
    pub subscriber: SubTreeSubscriber<Key, Value>,
    pub tree: SubTree<ID, Value>,
}

pub struct IndexTreeActor<ID: PrimaryKey, Key: PrimaryKey, Value: RecordValue> {
    tree: Tree<ID, Vec<Key>>,
    source_tree: Tree<Key, Value>,
    identity: Box<dyn Fn(&Value) -> Option<&ID>>,
}

impl<ID, Key, Value> IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    pub fn new<F: Fn(&Value) -> Option<&ID> + 'static>(
        tree: Tree<ID, Vec<Key>>,
        source_tree: Tree<Key, Value>,
        identity: F,
    ) -> Self {
        Self {
            tree,
            source_tree,
            identity: Box::new(identity),
        }
    }

    pub fn spawn(self) -> IndexTreeAddresses<ID, Key, Value> {
        let (subscribe_tx, mut subscribe_rx) = mpsc::channel::<(Arc<Key>, Arc<Value>)>(10);
        let (actor_tx, mut actor_rx) = mpsc::channel::<(ID, oneshot::Sender<Vec<Value>>)>(10);

        tokio::spawn(async move { while let Some(msg) = subscribe_rx.recv().await {} });

        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = SubTree::new(actor_tx);
        IndexTreeAddresses { subscriber, tree }
    }
}
