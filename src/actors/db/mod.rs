mod actor;
mod builder;
mod messages;
mod version;

use std::path::Path;

use am::{Actor, ActorRef};

use actor::DbActor;
pub use builder::TreeVersion;
pub use messages::*;
use tokio::sync::oneshot;
pub use version::VersionedTree;

use crate::Aggregate;

use self::builder::TreeBuilder;

use super::{
    subtree::{AggregateTree, AggregateTreeActor, IndexTreeActor, SubTree, UtilTreeAddress},
    tree::{PrimaryKey, RecordValue, Tree},
    wal::WalRestoredItems,
};

pub struct Database {
    inner: ActorRef<DbActor>,
    startup_registry: Vec<oneshot::Receiver<()>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            inner: DbActor::new().start(),
            startup_registry: Vec::new(),
        }
    }

    pub fn create<Key, Value>(
        &mut self,
        name: impl ToString,
    ) -> anyhow::Result<TreeBuilder<Key, Value>>
    where
        Key: PrimaryKey,
        Value: RecordValue,
    {
        TreeBuilder::new(self.inner.clone(), name.to_string())
    }

    pub async fn create_index<Key, Value, ID, F>(
        &mut self,
        name: impl ToString,
        source_tree: &Tree<Key, Value>,
        identity: F,
    ) -> anyhow::Result<SubTree<ID, Value>>
    where
        Key: PrimaryKey,
        Value: RecordValue,
        ID: PrimaryKey,
        F: Fn(&Value) -> Option<&ID> + Send + Sync + 'static,
    {
        let tree = self.create::<ID, Vec<Key>>(name)?.unwrap().await?;
        let source_tree_clone = source_tree.duplicate();
        let tree = IndexTreeActor::new(tree, source_tree_clone, identity);
        let UtilTreeAddress {
            subscriber,
            tree,
            ready_rx,
            restorer,
        } = tree.spawn();

        source_tree.register_restorer(restorer).await;
        source_tree.register_subscriber(subscriber);
        self.startup_registry.push(ready_rx);
        Ok(tree)
    }

    pub async fn create_aggregate<Record, Key, Value, ID, F>(
        &mut self,
        name: impl ToString,
        source_tree: &Tree<Key, Value>,
        _: Record,
        identity: F,
    ) -> anyhow::Result<AggregateTree<ID, Record>>
    where
        Record: Aggregate<Key, Value> + RecordValue + Default,
        Key: PrimaryKey,
        Value: RecordValue,
        ID: PrimaryKey,
        F: Fn(&Value) -> Option<&ID> + Send + Sync + 'static,
    {
        let tree = self
            .create::<ID, (Record, Vec<Key>)>(name)?
            .unwrap()
            .await?;
        let source_tree_clone = source_tree.duplicate();
        let tree = AggregateTreeActor::new(tree, source_tree_clone, identity);
        let UtilTreeAddress {
            subscriber,
            tree,
            ready_rx,
            restorer,
        } = tree.spawn();

        source_tree.register_restorer(restorer).await;
        source_tree.register_subscriber(subscriber);
        self.startup_registry.push(ready_rx);
        Ok(tree)
    }

    pub async fn restore(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
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
        self.inner.async_ask(RestoreComplete).await.unwrap();
        for ready in self.startup_registry.drain(..) {
            let _ = ready.await;
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
