mod actor;
mod messages;

use std::{collections::HashMap, path::Path};

use am::{Actor, ActorRef};

use actor::DbActor;
pub use messages::*;

use super::{
    tree::{GenericTree, PrimaryKey, RecordValue, Tree},
    wal::WalRestoredItems,
};

pub struct Database {
    inner: ActorRef<DbActor>,
    trees: HashMap<String, GenericTree>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            inner: DbActor::new().start(),
            trees: HashMap::new(),
        }
    }

    pub async fn create<Key, Value>(
        &mut self,
        name: impl ToString,
    ) -> anyhow::Result<Tree<Key, Value>>
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

        let tree = Tree::new(address);

        self.trees.insert(name.to_string(), tree.as_generic());

        Ok(tree)
    }

    pub async fn restore(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            // there is no reason to restore
            std::fs::create_dir_all(path)?;
        } else if path.is_file() {
            anyhow::bail!("Can't restore because this is a file and not a directory")
        } else {
            let WalRestoredItems { items } =
                self.inner.async_ask(Restore::new(path)).await.unwrap();
            for item in items {
                self.inner.async_ask(RestoreItem(item)).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn dump(self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let wal = self.inner.ask(()).await.unwrap();
        wal.dump(path).await.unwrap();
        Ok(())
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
