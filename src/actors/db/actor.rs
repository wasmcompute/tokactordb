use std::{collections::HashMap, time::Duration};

use am::{Actor, ActorRef, Ask, DeadActorResult, Handler};

use crate::actors::{
    tree::{tree_actor, TreeActor},
    wal::{new_wal_actor, Wal, WalActor},
};

use super::messages::{NewTreeRoot, TreeRoot};

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
    fn on_start(&mut self, ctx: &mut am::Ctx<Self>)
    where
        Self: Actor,
    {
        let wal = new_wal_actor(ctx, Duration::from_millis(10));
        self.wal = Some(wal);
    }
}

impl Ask<NewTreeRoot> for DbActor {
    type Result = TreeRoot;

    fn handle(&mut self, message: NewTreeRoot, context: &mut am::Ctx<Self>) -> Self::Result {
        let address = tree_actor(self.wal(), context);
        self.trees.insert(message.name, address.clone());
        TreeRoot::new(address)
    }
}

/**
 * Actor Failures:
 * - Tree actor
 * - Wal (Write Ahead Log) Actor
 */
impl Handler<DeadActorResult<TreeActor>> for DbActor {
    fn handle(&mut self, message: DeadActorResult<TreeActor>, context: &mut am::Ctx<Self>) {
        todo!()
    }
}

impl Handler<DeadActorResult<WalActor>> for DbActor {
    fn handle(&mut self, message: DeadActorResult<WalActor>, context: &mut am::Ctx<Self>) {
        todo!()
    }
}
