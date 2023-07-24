use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use tokactor::{
    util::builder::ActorAskRef, Actor, ActorRef, Ask, AsyncAsk, Ctx, DeadActorResult, Handler,
};

use crate::{
    actors::{
        fs::FileSystem,
        subtree::{AggregateTreeActor, IndexTreeActor, UtilTreeAddress},
        tree::{tree_actor, PrimaryKey, RecordValue, TreeActor},
        wal::{new_wal_actor, Item, Wal, WalActor, WalRestoredItems},
    },
    Aggregate, AggregateTree, SubTree,
};

use super::{
    messages::{NewTreeRoot, RestoreDbPath},
    version::{UpgradeVersion, UpgradedVersion, VersionedTreeUpgradeActor},
    RequestWal, RestoreComplete,
};

pub struct DbActor {
    wal: Option<Wal>,
    trees: HashMap<String, ActorRef<TreeActor>>,
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
    type Result = ActorRef<TreeActor>;

    fn handle(&mut self, message: NewTreeRoot, context: &mut Ctx<Self>) -> Self::Result {
        let address = tree_actor(message.name.clone(), message.versions, self.wal(), context);
        self.trees.insert(message.name, address.clone());
        address
    }
}

impl AsyncAsk<RestoreDbPath> for DbActor {
    type Output = WalRestoredItems;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: RestoreDbPath, _: &mut Ctx<Self>) -> Self::Future<'a> {
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
    type Output = anyhow::Result<()>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, _: RestoreComplete, _: &mut Ctx<Self>) -> Self::Future<'a> {
        Box::pin(async move {
            let mut fut = Vec::with_capacity(self.trees.len());
            for tree in self.trees.values() {
                fut.push(tree.ask(RestoreComplete));
            }
            let results = futures::future::join_all(fut).await;
            for result in results {
                // return an error if it was encountered
                // TODO(Alec): We probably want to clean this up a little bit
                result?;
            }
            Ok(())
        })
    }
}

impl AsyncAsk<Item> for DbActor {
    type Output = anyhow::Result<()>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: Item, _: &mut Ctx<Self>) -> Self::Future<'a> {
        Box::pin(async move {
            if let Some(tree) = self.trees.get(&msg.table) {
                tree.send_async(msg).await?;
            }
            Ok(())
        })
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

impl<Pk, Pv, Ck, Cv> Ask<VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>> for DbActor
where
    Pk: PrimaryKey,
    Pv: RecordValue,
    Ck: PrimaryKey + From<Pk>,
    Cv: RecordValue + From<Pv>,
{
    type Result = ActorAskRef<UpgradeVersion, UpgradedVersion>;

    fn handle(
        &mut self,
        actor: VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>,
        ctx: &mut Ctx<Self>,
    ) -> Self::Result {
        actor.spawn_with_ctx(ctx)
    }
}

impl Ask<FileSystem> for DbActor {
    type Result = ActorRef<FileSystem>;

    fn handle(&mut self, fs: FileSystem, ctx: &mut Ctx<Self>) -> Self::Result {
        ctx.spawn(fs)
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

impl Handler<DeadActorResult<FileSystem>> for DbActor {
    fn handle(&mut self, _: DeadActorResult<FileSystem>, _: &mut Ctx<Self>) {
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

impl<Pk, Pv, Ck, Cv> Handler<DeadActorResult<VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>>> for DbActor
where
    Pk: PrimaryKey,
    Pv: RecordValue,
    Ck: PrimaryKey + From<Pk>,
    Cv: RecordValue + From<Pv>,
{
    fn handle(
        &mut self,
        _: DeadActorResult<VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>>,
        _: &mut Ctx<Self>,
    ) {
        todo!()
    }
}
