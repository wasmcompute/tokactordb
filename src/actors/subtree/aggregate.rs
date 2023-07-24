use std::{future::Future, pin::Pin, sync::Arc};

use tokactor::{util::builder::CtxBuilder, Actor, AsyncAsk, Ctx, DeadActorResult, Handler};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Aggregate, Change, Tree, Update,
};

use super::{
    messages::{ChangeItem, RestoreItem},
    AggregateTree, IdentityFn, SubTreeRestorer, SubTreeSubscriber, UtilTreeAddress,
};

pub struct AggregateTreeActor<
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
> {
    tree: Tree<ID, (Record, Vec<Key>)>,
    _source_tree: Tree<Key, Value>,
    identity: Box<dyn IdentityFn<ID, Value>>,
}

impl<ID, Record, Key, Value> std::fmt::Debug for AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateTreeActor")
            .field("tree", &self.tree)
            .field("_source_tree", &self._source_tree)
            .finish()
    }
}

impl<ID, Record, Key, Value> Actor for AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
{
}

#[derive(Debug)]
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
                        let old_id = self.identity.identify(&old);
                        let new_id = self.identity.identify(&new);
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
                        if let Some(id) = self.identity.identify(&new) {
                            self.create(id, &change.key, &*new).await;
                        }
                    }
                }
            }
            Update::Del { old } => {
                if let Some(old_id) = self.identity.identify(&old) {
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

    pub fn spawn_with_ctx<P: Actor + Handler<DeadActorResult<Self>>>(
        self,
        ctx: &Ctx<P>,
    ) -> UtilTreeAddress<AggregateTree<ID, Record>, Key, Value> {
        let (restore_tx, subscribe_tx, get_tx) = CtxBuilder::new(self)
            .ask_asyncer::<RestoreItem>()
            .ask_asyncer::<ChangeItem<Key, Value>>()
            .ask_asyncer::<ID>()
            .spawn(ctx);
        let restorer = SubTreeRestorer::new(restore_tx);
        let subscriber = SubTreeSubscriber::new(subscribe_tx);
        let tree = AggregateTree::new(get_tx);
        UtilTreeAddress {
            restorer,
            subscriber,
            tree,
        }
    }
}

impl<ID, Record, Key, Value> AsyncAsk<RestoreItem> for AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
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
                    self.create(id, &key, &value).await;
                }
            });
        }
        println!("Skipping record");
        Box::pin(async {})
    }
}

impl<ID, Record, Key, Value> AsyncAsk<ChangeItem<Key, Value>>
    for AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Output = ();
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(
        &'a mut self,
        chg: ChangeItem<Key, Value>,
        _: &mut Ctx<Self>,
    ) -> Self::Future<'a> {
        Box::pin(async move {
            self.change(chg.into_inner()).await;
        })
    }
}

impl<ID, Record, Key, Value> AsyncAsk<ID> for AggregateTreeActor<ID, Record, Key, Value>
where
    ID: PrimaryKey,
    Record: Aggregate<Key, Value> + RecordValue,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Output = Option<Record>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, id: ID, _: &mut Ctx<Self>) -> Self::Future<'a> {
        Box::pin(async move { self.get(id).await })
    }
}
