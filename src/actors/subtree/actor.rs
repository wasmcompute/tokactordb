use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Change, Tree, Update,
};

use super::{SubTree, SubTreeSubscriber};

type IdentityFn<ID, Value> = dyn Fn(&Value) -> Option<&ID> + Send + Sync + 'static;

pub struct IndexTreeAddresses<ID: PrimaryKey, Key: PrimaryKey, Value: RecordValue> {
    pub subscriber: SubTreeSubscriber<Key, Value>,
    pub tree: SubTree<ID, Value>,
}

pub struct IndexTreeActor<ID: PrimaryKey, Key: PrimaryKey, Value: RecordValue> {
    tree: Tree<ID, Vec<Key>>,
    source_tree: Tree<Key, Value>,
    identity: Box<IdentityFn<ID, Value>>,
}

impl<ID, Key, Value> IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    pub fn new<F: Fn(&Value) -> Option<&ID> + Send + Sync + 'static>(
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

    async fn change(&mut self, change: Change<Arc<Key>, Arc<Value>>) {
        match change.update {
            Update::Set { old, new } => {
                match old {
                    Some(old) => {
                        // updated value
                        let old_id = (self.identity)(&old);
                        let new_id = (self.identity)(&new);
                        match (old_id, new_id) {
                            (Some(old_id), Some(new_id)) => {
                                if old_id == new_id {
                                    self.add_key_to_list(new_id, (*change.key).clone()).await;
                                } else {
                                    self.remove_key_from_list(old_id, &*change.key).await;
                                    self.add_key_to_list(new_id, (*change.key).clone()).await;
                                }
                            }
                            (Some(old_id), None) => {
                                self.remove_key_from_list(old_id, &*change.key).await
                            }
                            (None, Some(new_id)) => {
                                self.add_key_to_list(new_id, (*change.key).clone()).await;
                            }
                            (None, None) => {}
                        }
                    }
                    None => {
                        // new value
                        if let Some(id) = (self.identity)(&new) {
                            self.add_key_to_list(id, (*change.key).clone()).await;
                        }
                    }
                }
            }
            Update::Del { old } => {
                if let Some(old_id) = (self.identity)(&old) {
                    self.remove_key_from_list(old_id, &*change.key).await
                }
            }
        }
    }

    async fn add_key_to_list(&mut self, id: &ID, key: Key) {
        let mut list = self.tree.get(id.clone()).await.unwrap().unwrap_or(vec![]);
        if !list.contains(&key) {
            list.push(key);
            self.tree.update(id.clone(), list).await.unwrap();
        }
    }

    async fn remove_key_from_list(&mut self, id: &ID, key: &Key) {
        let mut list = self.tree.get(id.clone()).await.unwrap().unwrap_or(vec![]);
        if let Some(index) = list.iter().position(|id| id == key) {
            list.remove(index);
            self.tree.update(id.clone(), list).await.unwrap();
        }
    }

    async fn get(&self, id: ID) -> Vec<Value> {
        let ids = self.tree.get(id).await.unwrap();
        if let Some(ids) = ids {
            let mut storage = Vec::new();
            for id in ids {
                if let Some(value) = self.source_tree.get(id).await.unwrap() {
                    storage.push(value);
                }
            }
            storage
        } else {
            vec![]
        }
    }

    pub fn spawn(self) -> IndexTreeAddresses<ID, Key, Value> {
        let (subscribe_tx, mut subscribe_rx) = mpsc::channel::<Change<Arc<Key>, Arc<Value>>>(10);
        let (actor_tx, mut actor_rx) = mpsc::channel::<(ID, oneshot::Sender<Vec<Value>>)>(10);

        let _subscribe_tx = subscribe_tx.clone();
        let fut = async move {
            let _ = _subscribe_tx;
            let mut actor = self;
            loop {
                tokio::select! {
                    change = subscribe_rx.recv() => {
                        if let Some(change) = change{
                            actor.change(change).await;
                        }
                    },
                    message = actor_rx.recv() => {
                        if let Some((id, tx)) = message {
                            let _ = tx.send(actor.get(id).await);
                        }
                    }
                }
            }
        };

        tokio::spawn(fut);

        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = SubTree::new(actor_tx);
        IndexTreeAddresses { subscriber, tree }
    }
}
