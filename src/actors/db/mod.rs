mod actor;
mod messages;

use std::{collections::HashMap, path::Path};

use am::{Actor, ActorRef};

use actor::DbActor;
pub use messages::*;

use super::{
    subtree::SubTree,
    tree::{PrimaryKey, RecordValue, Tree},
    wal::WalRestoredItems,
    GenericTree,
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

    pub async fn create_index<Key, Value, ID, F>(
        &mut self,
        name: impl ToString,
        source_tree: &mut Tree<Key, Value>,
        f: F,
    ) -> anyhow::Result<SubTree<ID, Value>>
    where
        Key: PrimaryKey,
        Value: RecordValue,
        ID: PrimaryKey,
        F: Fn(&Value) -> Option<&ID> + 'static,
    {
        let tree = self.create::<ID, Vec<Key>>(name).await?;
        let index_tree = source_tree.register_subscriber(tree, f);
        Ok(index_tree)
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
                println!("Restoring    ->    {}", item);
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
