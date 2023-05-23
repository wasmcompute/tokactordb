use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Aggregate, Change, Tree, Update,
};

use super::{AggregateTree, SubTreeRestorer, SubTreeSubscriber, UtilTreeAddress};

type IdentityFn<ID, Value> = dyn Fn(&Value) -> Option<&ID> + Send + Sync + 'static;

pub struct AggregateTreeActor<
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
> {
    tree: Tree<ID, (Record, Vec<Key>)>,
    _source_tree: Tree<Key, Value>,
    identity: Box<IdentityFn<ID, Value>>,
}

enum Operation {
    Create,
    Update,
    Delete,
}

impl<ID, Record, Key, Value> AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
{
    pub fn new<F: Fn(&Value) -> Option<&ID> + Send + Sync + 'static>(
        tree: Tree<ID, (Record, Vec<Key>)>,
        source_tree: Tree<Key, Value>,
        identity: F,
    ) -> Self {
        Self {
            tree,
            _source_tree: source_tree,
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
                                    self.update(new_id, &*change.key, &*old, &*new).await;
                                } else {
                                    self.delete(old_id, &*change.key, &*old).await;
                                    self.create(new_id, &change.key, &*new).await;
                                }
                            }
                            (Some(old_id), None) => {
                                self.delete(old_id, &*change.key, &*old).await;
                            }
                            (None, Some(new_id)) => {
                                // This is the same event as create. Because of an
                                // update, we can now add the value to this bucket
                                self.create(new_id, &change.key, &*new).await;
                            }
                            (None, None) => {
                                unreachable!("The identity function changed. If this triggers, our aggregate sub tree now has bad data");
                            }
                        }
                    }
                    None => {
                        // new value
                        if let Some(id) = (self.identity)(&new) {
                            self.create(id, &change.key, &*new).await;
                        }
                    }
                }
            }
            Update::Del { old } => {
                if let Some(old_id) = (self.identity)(&old) {
                    self.delete(old_id, &*change.key, &*old).await
                }
            }
        }
    }

    async fn create(&mut self, id: &ID, key: &Key, value: &Value) {
        let change = Change {
            key,
            update: Update::Set {
                old: None,
                new: value,
            },
        };
        self.handle_change(Operation::Create, id.clone(), change)
            .await;
    }

    async fn update(&mut self, id: &ID, key: &Key, old: &Value, new: &Value) {
        let change = Change {
            key,
            update: Update::Set {
                old: Some(old),
                new,
            },
        };
        self.handle_change(Operation::Update, id.clone(), change)
            .await;
    }

    async fn delete(&mut self, id: &ID, key: &Key, old: &Value) {
        let change = Change {
            key,
            update: Update::Del { old },
        };
        self.handle_change(Operation::Delete, id.clone(), change)
            .await;
    }

    async fn handle_change(&self, op: Operation, id: ID, change: Change<&Key, &Value>) {
        let (mut record, mut list) = self
            .tree
            .get(id.clone())
            .await
            .unwrap()
            .unwrap_or((Record::default(), Vec::new()));
        match op {
            Operation::Create => {
                if !list.contains(change.key) {
                    list.push(change.key.clone());
                    record.observe(change);
                }
            }
            Operation::Update => {
                if list.contains(change.key) {
                    record.observe(change);
                }
            }
            Operation::Delete => {
                if let Some(index) = list.iter().position(|id| id == change.key) {
                    list.remove(index);
                    record.observe(change);
                }
                // TODO(Alec): Should there be a panic here? Shouldn't the record always exist?
            }
        }
        self.tree.update(id, (record, list)).await.unwrap();
    }

    async fn get(&self, id: ID) -> Option<Record> {
        if let Some((record, _)) = self.tree.get(id).await.unwrap() {
            Some(record)
        } else {
            None
        }
    }

    pub fn spawn(self) -> UtilTreeAddress<AggregateTree<ID, Record>, Key, Value> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (restore_tx, mut restore_rx) =
            mpsc::channel::<(Arc<Vec<u8>>, Arc<Option<Vec<u8>>>)>(10);
        let (subscribe_tx, mut subscribe_rx) =
            mpsc::channel::<(Change<Arc<Key>, Arc<Value>>, oneshot::Sender<()>)>(10);
        let (actor_tx, mut actor_rx) = mpsc::channel::<(ID, oneshot::Sender<Option<Record>>)>(10);

        let _subscribe_tx = subscribe_tx.clone();
        let fut = async move {
            // Keep the subscriber for the changes open. This way even if the
            // references to the sender are destroyed, subscribe_rx will never
            // return `None`.
            let _ = _subscribe_tx;
            let mut actor = self;

            // First, continue looping until all of the items in the database have
            // been restored.
            while let Some((key, value)) = restore_rx.recv().await {
                if let Ok(key) = bincode::deserialize::<Key>(&key) {
                    if let Some(value) = &*value {
                        if let Ok(value) = serde_json::from_slice::<Value>(value) {
                            if let Some(id) = (actor.identity)(&value) {
                                println!("Adding item {:?} to list {:?} with {:?}", id, key, value);
                                actor.create(id, &key, &value).await;
                                continue;
                            }
                        }
                    }
                }
                println!("Skipping record")
            }

            println!("Completed restoring sublist. Starting up");
            let _ = ready_tx.send(());

            loop {
                tokio::select! {
                    change = subscribe_rx.recv() => {
                        if let Some((change, tx)) = change{
                            actor.change(change).await;
                            let _ = tx.send(());
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

        let restorer = SubTreeRestorer::new(restore_tx);
        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = AggregateTree::new(actor_tx);
        UtilTreeAddress {
            ready_rx,
            restorer,
            subscriber,
            tree,
        }
    }
}
