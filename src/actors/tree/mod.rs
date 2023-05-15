mod actor;
mod messages;

use std::marker::PhantomData;

pub use actor::*;
use am::{Actor, ActorRef, Ctx, DeadActorResult, Handler};
pub use messages::*;
use serde::{de::DeserializeOwned, Serialize};

use crate::AutoIncrement;

use super::wal::Wal;

pub fn tree_actor<A>(wal: Wal, ctx: &mut Ctx<A>) -> ActorRef<TreeActor>
where
    A: Actor + Handler<DeadActorResult<TreeActor>>,
{
    let tree = TreeActor::new(wal);
    ctx.spawn(tree)
}

pub struct Tree<Key, Value>
where
    Key: Serialize + DeserializeOwned + AutoIncrement,
    Value: Serialize + DeserializeOwned + std::fmt::Debug,
{
    inner: ActorRef<TreeActor>,
    _key: PhantomData<Key>,
    _value: PhantomData<Value>,
}

impl<Key, Value> Tree<Key, Value>
where
    Key: Serialize + DeserializeOwned + AutoIncrement,
    Value: Serialize + DeserializeOwned + std::fmt::Debug,
{
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self {
            inner,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn insert(&self, value: Value) -> anyhow::Result<()>
    where
        Key: std::fmt::Debug,
    {
        let value = serde_json::to_vec(&value)?;
        let record = InsertRecord::new(value);
        let response = self.inner.ask(record).await.unwrap();
    }
}
