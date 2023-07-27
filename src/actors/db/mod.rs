mod actor;
mod builder;
mod messages;
mod version;

use std::path::Path;

use tokactor::{Actor, ActorRef};

use actor::DbActor;
pub use builder::TreeVersion;
pub use messages::*;
pub use version::VersionedTree;

use crate::Aggregate;

use self::builder::TreeBuilder;

use super::{
    fs::{FileSystem, FileSystemFacade},
    subtree::{AggregateTree, AggregateTreeActor, IndexTreeActor, SubTree, UtilTreeAddress},
    tree::{PrimaryKey, RecordValue, Tree},
};

pub struct Database {
    inner: ActorRef<DbActor>,
    filesystem: FileSystemFacade,
}

impl Database {
    pub async fn new(fs: FileSystem) -> anyhow::Result<Self> {
        let database = DbActor::new().start();
        let filesystem = database.ask(fs).await?;
        let facade = FileSystemFacade::new(filesystem);
        Ok(Self {
            inner: database,
            filesystem: facade,
        })
    }

    pub fn create<Key, Value>(&self, name: impl ToString) -> anyhow::Result<TreeBuilder<Key, Value>>
    where
        Key: PrimaryKey,
        Value: RecordValue,
    {
        TreeBuilder::new(self.inner.clone(), name.to_string())
    }

    pub async fn create_index<Key, Value, ID, F>(
        &self,
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
            restorer,
        } = self.inner.ask(tree).await?;

        source_tree.register_restorer(restorer).await;
        source_tree.register_subscriber(subscriber);
        Ok(tree)
    }

    pub async fn create_aggregate<Record, Key, Value, ID, F>(
        &self,
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
            restorer,
        } = self.inner.ask(tree).await?;

        source_tree.register_restorer(restorer).await;
        source_tree.register_subscriber(subscriber);
        Ok(tree)
    }

    pub async fn restore(&self) -> anyhow::Result<()> {
        self.filesystem.open_base_dir().await?;
        // We need to validate the system is setup correctly
        self.filesystem.validate_or_create_dir("manifest").await?;
        self.filesystem.validate_or_create_dir("storage").await?;

        let manifest_fs = self.filesystem.rebase("manifest");

        // let system = ManifestSystem::restore(self.filesystem.clone())?;
        // let wal = Wal::restore(system.clone(), self.filesystem.clone())?;

        // let WalRestoredItems { items } = self.inner.async_ask(RestoreDbPath::new(path)).await?;
        // for item in items {
        //     println!("Restoring    ->    {}", item);
        //     let result = self.inner.async_ask(item).await?;
        //     result?;
        // }
        // self.inner.async_ask(RestoreComplete).await?
        Ok(())
    }

    pub async fn dump(self, _: impl AsRef<Path>) -> anyhow::Result<()> {
        // let wal = self.inner.ask(RequestWal()).await.unwrap();
        // wal.dump(path).await.unwrap();
        Ok(())
    }
}
