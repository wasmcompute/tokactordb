use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Change, Tree, Update,
};

use super::{Request, SubTree, SubTreeRestorer, SubTreeSubscriber, UtilTreeAddress};

type IdentityFn<ID, Value> = dyn Fn(&Value) -> Option<&ID> + Send + Sync + 'static;

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
                            (None, None) => {
                                unreachable!("The identity function changed. If this triggers, our index sub tree now has bad data");
                            }
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

    async fn get_item_by_index(
        &self,
        id: ID,
        index: usize,
        op: Box<dyn Fn(&mut Value) + Send + Sync + 'static>,
    ) -> Option<()> {
        let ids = self.tree.get(id).await.unwrap();
        if let Some(ids) = ids {
            if let Some(key) = ids.get(index) {
                if let Some(mut value) = self.source_tree.get(key.clone()).await.unwrap() {
                    (op)(&mut value);
                    self.source_tree.update(key.clone(), value).await.unwrap();
                    return Some(());
                }
            }
        }
        None
    }

    pub fn spawn(self) -> UtilTreeAddress<SubTree<ID, Value>, Key, Value> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (restore_tx, mut restore_rx) =
            mpsc::channel::<(Arc<Vec<u8>>, Arc<Option<Vec<u8>>>)>(10);
        let (subscribe_tx, mut subscribe_rx) =
            mpsc::channel::<(Change<Arc<Key>, Arc<Value>>, oneshot::Sender<()>)>(10);
        let (actor_tx, mut actor_rx) = mpsc::channel::<Request<ID, Value>>(10);

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
                                actor.add_key_to_list(id, key).await;
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
                        if let Some(request) = message {
                            match request {
                                Request::List((id, tx)) => tx.send(actor.get(id).await).unwrap(),
                                Request::Item((id, index, op, tx)) => {
                                    tx.send(actor.get_item_by_index(id, index, op).await).unwrap();
                                }
                            }

                        }
                    }
                }
            }
        };

        tokio::spawn(fut);

        let restorer = SubTreeRestorer::new(restore_tx);
        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = SubTree::new(actor_tx);
        UtilTreeAddress {
            ready_rx,
            restorer,
            subscriber,
            tree,
        }
    }
}
