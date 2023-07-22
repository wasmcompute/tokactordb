use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use tokactor::{Actor, Ask, AsyncAsk, Ctx, DeadActorResult, Handler};

use crate::{
    actors::{
        subtree::{AggregateTreeActor, IndexTreeActor, UtilTreeAddress},
        tree::{tree_actor, PrimaryKey, RecordValue, TreeActor},
        wal::{new_wal_actor, Wal, WalActor, WalRestoredItems},
        GenericTree,
    },
    Aggregate, AggregateTree, SubTree,
};

use super::{
    messages::{NewTreeRoot, Restore, RestoreItem, TreeRoot},
    RequestWal, RestoreComplete,
};

pub struct DbActor {
    wal: Option<Wal>,
    trees: HashMap<String, GenericTree>,
}

impl DbActor {
    pub fn new() -> Self {
        Self {
            wal: None,
            trees: HashMap::new(),
        }
    }

    fn wal(&self) -> Wal {
        (*self.wal.as_ref().unwrap()).clone()
    }
}

impl Actor for DbActor {
    fn on_start(&mut self, ctx: &mut Ctx<Self>)
    where
        Self: Actor,
    {
        let wal = new_wal_actor(ctx, Duration::from_millis(10));
        self.wal = Some(wal);
    }
}

impl Ask<NewTreeRoot> for DbActor {
    type Result = TreeRoot;

    fn handle(&mut self, message: NewTreeRoot, context: &mut Ctx<Self>) -> Self::Result {
        let address = tree_actor(message.name.clone(), message.versions, self.wal(), context);
        self.trees
            .insert(message.name, GenericTree::new(address.clone()));
        TreeRoot::new(address)
    }
}

impl AsyncAsk<Restore> for DbActor {
    type Output = WalRestoredItems;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: Restore, _: &mut Ctx<Self>) -> Self::Future<'a> {
        // The restore message given to us here is a directory. In the future we'd
        // want to do some advanced logic here such as like  get metadata files
        // and read all of the WAL logs that are avaliable to us.

        // For now we'll just hard code the one file ;)
        assert!(msg.directory.is_dir());
        let wal_address = self.wal();
        let wal_path = msg.directory.join("wal");

        Box::pin(async move {
            if !wal_path.exists() {
                WalRestoredItems::new(vec![])
            } else {
                wal_address.restore(wal_path).await
            }
        })
    }
}

impl AsyncAsk<RestoreComplete> for DbActor {
    type Output = ();
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, _: RestoreComplete, _: &mut Ctx<Self>) -> Self::Future<'a> {
        Box::pin(async move {
            let mut fut = Vec::with_capacity(self.trees.len());
            for tree in self.trees.values() {
                fut.push(tree.send_restore_complete());
            }
            futures::future::join_all(fut).await;
        })
    }
}

impl AsyncAsk<RestoreItem> for DbActor {
    type Output = ();
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: RestoreItem, _: &mut Ctx<Self>) -> Self::Future<'a> {
        if let Some(tree) = self.trees.get(&msg.table) {
            Box::pin(async move {
                tree.send_generic_item(msg).await;
            })
        } else {
            Box::pin(async move {})
        }
    }
}

impl Ask<RequestWal> for DbActor {
    type Result = Wal;

    fn handle(&mut self, _: RequestWal, _: &mut Ctx<Self>) -> Self::Result {
        self.wal()
    }
}

/*******************************************************************************
 * Spawn Index and Aggregate tree actors
 ******************************************************************************/

impl<ID, Key, Value> Ask<IndexTreeActor<ID, Key, Value>> for DbActor
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    type Result = UtilTreeAddress<SubTree<ID, Value>, Key, Value>;

    fn handle(&mut self, idx: IndexTreeActor<ID, Key, Value>, ctx: &mut Ctx<Self>) -> Self::Result {
        idx.spawn_with_ctx(ctx)
    }
}

impl<
        ID: PrimaryKey,
        Record: Aggregate<Key, Value> + RecordValue,
        Key: PrimaryKey,
        Value: RecordValue,
    > Ask<AggregateTreeActor<ID, Record, Key, Value>> for DbActor
{
    type Result = UtilTreeAddress<AggregateTree<ID, Record>, Key, Value>;

    fn handle(
        &mut self,
        agg: AggregateTreeActor<ID, Record, Key, Value>,
        ctx: &mut Ctx<Self>,
    ) -> Self::Result {
        agg.spawn_with_ctx(ctx)
    }
}

/*******************************************************************************
 * Handle the death of child actors
 * - TreeActor
 * - WalActor
 * - IndexTreeActor
 * - AggregateTreeActor
 ******************************************************************************/

impl Handler<DeadActorResult<TreeActor>> for DbActor {
    fn handle(&mut self, _: DeadActorResult<TreeActor>, _: &mut Ctx<Self>) {
        todo!()
    }
}

impl Handler<DeadActorResult<WalActor>> for DbActor {
    fn handle(&mut self, _: DeadActorResult<WalActor>, _: &mut Ctx<Self>) {
        todo!()
    }
}

impl<ID, Key, Value> Handler<DeadActorResult<IndexTreeActor<ID, Key, Value>>> for DbActor
where
    ID: PrimaryKey,
    Key: PrimaryKey,
    Value: RecordValue,
{
    fn handle(&mut self, _: DeadActorResult<IndexTreeActor<ID, Key, Value>>, _: &mut Ctx<Self>) {
        todo!()
    }
}

impl<
        ID: PrimaryKey,
        Record: Aggregate<Key, Value> + RecordValue,
        Key: PrimaryKey,
        Value: RecordValue,
    > Handler<DeadActorResult<AggregateTreeActor<ID, Record, Key, Value>>> for DbActor
{
    fn handle(
        &mut self,
        _: DeadActorResult<AggregateTreeActor<ID, Record, Key, Value>>,
        _: &mut Ctx<Self>,
    ) {
        todo!()
    }
}
