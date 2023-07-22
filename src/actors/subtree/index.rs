use std::{pin::Pin, sync::Arc};

use futures::Future;
use tokactor::{util::builder::CtxBuilder, Actor, AsyncAsk, Ctx, DeadActorResult, Handler};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Change, Tree, Update,
};

use super::{
    messages::{ChangeItem, RestoreItem},
    IdentityFn, RefactorMeResponse, Request, SubTree, SubTreeRestorer, SubTreeSubscriber,
    UtilTreeAddress,
};

pub struct IndexTreeActor<ID: PrimaryKey, Key: PrimaryKey, Value: RecordValue> {
    tree: Tree<ID, Vec<Key>>,
    source_tree: Tree<Key, Value>,
    identity: Box<dyn IdentityFn<ID, Value>>,
}

impl<ID, Key, Value> Actor for IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
}

impl<ID, Key, Value> std::fmt::Debug for IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexTreeActor")
            .field("tree", &self.tree)
            .field("source_tree", &self.source_tree)
            .finish()
    }
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
                        let old_id = self.identity.identify(&old);
                        let new_id = self.identity.identify(&new);
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
                        if let Some(id) = self.identity.identify(&new) {
                            self.add_key_to_list(id, (*change.key).clone()).await;
                        }
                    }
                }
            }
            Update::Del { old } => {
                if let Some(old_id) = self.identity.identify(&old) {
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
                    // TODO(Alec): This needs to call a "special" update method that
                    // doesn't wait for "subscribors" to finish. otherwise, we
                    // block our selves from ever finishing the update.
                    //
                    // WE CAN'T CALL Tree::update, Tree::create from within a
                    // child collection because we could be listening for subscrptions
                    self.source_tree.update(key.clone(), value).await.unwrap();
                    return Some(());
                }
            }
        }
        None
    }

    pub fn spawn_with_ctx<P: Actor + Handler<DeadActorResult<Self>>>(
        self,
        ctx: &Ctx<P>,
    ) -> UtilTreeAddress<SubTree<ID, Value>, Key, Value> {
        let (restore_tx, subscribe_tx, get_tx) = CtxBuilder::new(self)
            .ask_asyncer::<RestoreItem>()
            .ask_asyncer::<ChangeItem<Key, Value>>()
            .ask_asyncer::<Request<ID, Value>>()
            .spawn(ctx);
        let restorer = SubTreeRestorer::new(restore_tx);
        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = SubTree::new(get_tx);
        UtilTreeAddress {
            restorer,
            subscriber,
            tree,
        }
    }
}

impl<ID, Key, Value> AsyncAsk<RestoreItem> for IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Output = ();

    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, restore: RestoreItem, _: &mut Ctx<Self>) -> Self::Future<'a> {
        if let Ok((key, Some(value))) = restore.deserialize::<Key, Value>() {
            return Box::pin(async move {
                if let Some(id) = self.identity.identify(&value) {
                    println!("Adding item {:?} to list {:?} with {:?}", id, key, value);
                    self.add_key_to_list(id, key).await;
                }
            });
        }
        println!("Skipping record");
        Box::pin(async {})
    }
}

impl<ID, Key, Value> AsyncAsk<ChangeItem<Key, Value>> for IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Output = ();
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(
        &'a mut self,
        change: ChangeItem<Key, Value>,
        _: &mut Ctx<Self>,
    ) -> Self::Future<'a> {
        Box::pin(async move { self.change(change.into_inner()).await })
    }
}

impl<ID, Key, Value> AsyncAsk<Request<ID, Value>> for IndexTreeActor<ID, Key, Value>
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Output = RefactorMeResponse<Value>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(
        &'a mut self,
        request: Request<ID, Value>,
        _: &mut Ctx<Self>,
    ) -> Self::Future<'a> {
        Box::pin(async move {
            match request {
                Request::List(id) => RefactorMeResponse::List(self.get(id).await),
                Request::Item((id, index, op)) => {
                    RefactorMeResponse::Item(self.get_item_by_index(id, index, op).await)
                }
            }
        })
    }
}
