use std::marker::PhantomData;

use tokactor::{util::builder::ActorAskRef, ActorRef};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Tree,
};

use super::{
    actor::DbActor,
    version::{UpgradeVersion, UpgradedVersion, VersionedTree, VersionedTreeUpgradeActor},
    NewTreeRoot,
};

#[derive(Debug, Clone)]
pub struct TreeVersion {
    version: VersionedTree,
    upgrader: Option<ActorAskRef<UpgradeVersion, UpgradedVersion>>,
}

impl TreeVersion {
    fn new(version: VersionedTree) -> Self {
        Self {
            version,
            upgrader: None,
        }
    }

    pub async fn upgrade(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let result = self
            .upgrader
            .as_ref()
            .unwrap()
            .ask(UpgradeVersion::new(key, value))
            .await?;
        let UpgradedVersion { key, value } = result;
        Ok((key, value))
    }
}

pub struct TreeBuilder<Key: PrimaryKey, Value: RecordValue> {
    name: String,
    versions: Vec<TreeVersion>,
    database: ActorRef<DbActor>,
    _key: PhantomData<Key>,
    _value: PhantomData<Value>,
}

impl<Key: PrimaryKey, Value: RecordValue> TreeBuilder<Key, Value> {
    pub(crate) fn new(database: ActorRef<DbActor>, name: String) -> anyhow::Result<Self> {
        let version = VersionedTree::new::<Key, Value>(name.clone())?;
        Ok(Self {
            name,
            versions: vec![TreeVersion::new(version)],
            database,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    /// Migrate from the current version of the `Key` and `Value` and replace them
    /// with the values of `NewKey` and `NewValue`. This upgrade happens when we
    /// load in data that has an older version that what is currently recorded in
    /// the database.
    pub async fn migrate<NewKey, NewValue>(
        mut self,
    ) -> anyhow::Result<TreeBuilder<NewKey, NewValue>>
    where
        NewKey: PrimaryKey + From<Key>,
        NewValue: RecordValue + From<Value>,
    {
        // Create an actor that can upgrade
        let address = self
            .database
            .ask(VersionedTreeUpgradeActor::<Key, Value, NewKey, NewValue>::new())
            .await?;

        // Assign the new upgrader to current version
        self.versions.last_mut().unwrap().upgrader = Some(address);

        // Create a record that will record this new version of the table
        let next_version = VersionedTree::new::<NewKey, NewValue>(self.name.clone())?;
        self.versions.push(TreeVersion::new(next_version));

        // Return the newly updated tree builder, with the new updated types
        let this = TreeBuilder::<NewKey, NewValue> {
            name: self.name.clone(),
            versions: self.versions,
            database: self.database,
            _key: PhantomData,
            _value: PhantomData,
        };
        Ok(this)
    }

    /// Unwrap the builder to create a tree actor that stores the up to date version
    /// of the record.
    pub async fn unwrap(self) -> anyhow::Result<Tree<Key, Value>> {
        let address = self
            .database
            .ask(NewTreeRoot::new(self.name, self.versions))
            .await?;

        let tree = Tree::new(address);

        Ok(tree)
    }
}
