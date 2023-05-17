mod actor;
mod messages;

use am::{Actor, ActorRef};

use actor::DbActor;
use messages::*;

use super::tree::{PrimaryKey, RecordValue, Tree};

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
        Key: PrimaryKey,
        Value: RecordValue,
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

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
