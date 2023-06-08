use std::marker::PhantomData;

use tokactor::ActorRef;
use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    Tree,
};

use super::{
    actor::DbActor,
    version::{UpgradeVersion, VersionedTree, VersionedTreeUpgradeActor},
    NewTreeRoot,
};

#[derive(Debug, Clone)]
pub struct TreeVersion {
    version: VersionedTree,
    upgrader: Option<mpsc::Sender<UpgradeVersion>>,
}

impl TreeVersion {
    fn new(version: VersionedTree) -> Self {
        Self {
            version,
            upgrader: None,
        }
    }

    pub async fn upgrade(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let (tx, rx) = oneshot::channel();
        let upgrade = UpgradeVersion::new(key, value, tx);
        let _ = self.upgrader.as_ref().unwrap().send(upgrade).await;
        rx.await.unwrap()
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

    pub fn migrate<NewKey, NewValue>(mut self) -> anyhow::Result<TreeBuilder<NewKey, NewValue>>
    where
        NewKey: PrimaryKey + From<Key>,
        NewValue: RecordValue + From<Value>,
    {
        let actor = VersionedTreeUpgradeActor::<Key, Value, NewKey, NewValue>::new();
        self.versions.last_mut().unwrap().upgrader = Some(actor.spawn());
        let next_version = VersionedTree::new::<NewKey, NewValue>(self.name.clone())?;
        self.versions.push(TreeVersion::new(next_version));
        let this = TreeBuilder::<NewKey, NewValue> {
            name: self.name.clone(),
            versions: self.versions,
            database: self.database,
            _key: PhantomData,
            _value: PhantomData,
        };
        Ok(this)
    }

    pub async fn unwrap(self) -> anyhow::Result<Tree<Key, Value>> {
        let address = self
            .database
            .ask(NewTreeRoot::new(self.name, self.versions))
            .await
            .unwrap()
            .inner;

        let tree = Tree::new(address);

        Ok(tree)
    }
}
