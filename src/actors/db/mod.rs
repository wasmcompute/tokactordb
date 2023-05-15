mod actor;
mod messages;

use am::{Actor, ActorRef};
use serde::{de::DeserializeOwned, Serialize};

use actor::DbActor;
use messages::*;

use crate::AutoIncrement;

use super::tree::Tree;

pub struct Database {
    inner: ActorRef<DbActor>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            inner: DbActor::new().start(),
        }
    }

    pub async fn create<Key, Value>(&self, name: impl ToString) -> anyhow::Result<Tree<Key, Value>>
    where
        Key: Serialize + DeserializeOwned + AutoIncrement,
        Value: Serialize + DeserializeOwned + std::fmt::Debug,
    {
        let address = self
            .inner
            .ask(NewTreeRoot::new(name.to_string()))
            .await
            .unwrap()
            .inner;

        Ok(Tree::new(address))
    }
}
