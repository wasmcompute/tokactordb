use std::{collections::HashMap, time::Duration};

use tokactor::{Actor, Ask, AsyncAsk, AsyncHandle, Ctx, DeadActorResult, Handler};

use crate::actors::{
    tree::{tree_actor, TreeActor},
    wal::{new_wal_actor, Wal, WalActor, WalRestoredItems},
    GenericTree,
};

use super::{
    messages::{NewTreeRoot, Restore, RestoreItem, TreeRoot},
    RestoreComplete,
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
    type Result = WalRestoredItems;

    fn handle(&mut self, msg: Restore, ctx: &mut Ctx<Self>) -> AsyncHandle<Self::Result> {
        // The restore message given to us here is a directory. In the future we'd
        // want to do some advanced logic here such as like  get metadata files
        // and read all of the WAL logs that are avaliable to us.

        // For now we'll just hard code the one file ;)
        assert!(msg.directory.is_dir());
        let wal_address = self.wal();
        let wal_path = msg.directory.join("wal");

        ctx.anonymous_handle(async move {
            if !wal_path.exists() {
                WalRestoredItems::new(vec![])
            } else {
                wal_address.restore(wal_path).await
            }
        })
    }
}

impl AsyncAsk<RestoreComplete> for DbActor {
    type Result = ();

    fn handle(&mut self, _: RestoreComplete, ctx: &mut Ctx<Self>) -> AsyncHandle<Self::Result> {
        let trees = self.trees.values().map(Clone::clone).collect::<Vec<_>>();
        ctx.anonymous_handle(async move {
            for tree in trees {
                tree.send_restore_complete().await;
            }
        })
    }
}

impl AsyncAsk<RestoreItem> for DbActor {
    type Result = ();

    fn handle(&mut self, msg: RestoreItem, ctx: &mut Ctx<Self>) -> AsyncHandle<Self::Result> {
        if let Some(tree) = self.trees.get(&msg.table) {
            let tree_addr = tree.clone();
            ctx.anonymous_handle(async move {
                tree_addr.send_generic_item(msg).await;
            })
        } else {
            ctx.anonymous_handle(async move {})
        }
    }
}

impl Ask<()> for DbActor {
    type Result = Wal;

    fn handle(&mut self, _: (), _: &mut Ctx<Self>) -> Self::Result {
        self.wal()
    }
}

/**
 * Actor Failures:
 * - Tree actor
 * - Wal (Write Ahead Log) Actor
 */
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
