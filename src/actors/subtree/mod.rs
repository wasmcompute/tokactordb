mod aggregate;
mod index;
mod messages;

use std::sync::Arc;

use tokactor::util::builder::ActorAsyncAskRef;

pub use aggregate::AggregateTreeActor;
pub use index::IndexTreeActor;

use crate::{Change, Update};

use self::messages::{ChangeItem, RestoreItem};

use super::tree::{PrimaryKey, RecordValue};

trait IdentityFn<ID, Value>: Send + Sync {
    fn identify<'a>(&self, value: &'a Value) -> Option<&'a ID>;
}

impl<F, ID, Value> IdentityFn<ID, Value> for F
where
    F: Fn(&Value) -> Option<&ID> + Send + Sync,
{
    fn identify<'a>(&self, value: &'a Value) -> Option<&'a ID> {
        (self)(value)
    }
}

#[derive(Debug, Clone)]
pub struct SubTreeRestorer {
    inner: ActorAsyncAskRef<RestoreItem, ()>,
}

impl SubTreeRestorer {
    pub fn new(inner: ActorAsyncAskRef<RestoreItem, ()>) -> Self {
        Self { inner }
    }

    pub async fn restore_record(&self, key: Arc<Vec<u8>>, value: Arc<Option<Vec<u8>>>) {
        self.inner
            .ask_async(RestoreItem::new(key, value))
            .await
            .unwrap();
    }
}

/// A address to message a sub tree given a collections key and value.
/// Send updates to a subcollection when the orignial collection changes.
pub struct SubTreeSubscriber<Key: PrimaryKey, Value: RecordValue> {
    inner: ActorAsyncAskRef<ChangeItem<Key, Value>, ()>,
}

impl<Key: PrimaryKey, Value: RecordValue> std::fmt::Debug for SubTreeSubscriber<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubTreeSubscriber")
            .field("inner", &"ActorAsyncAskRef<ChangeItem<Key, Value>, ()>")
            .finish()
    }
}

impl<Key: PrimaryKey, Value: RecordValue> Clone for SubTreeSubscriber<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Key: PrimaryKey, Value: RecordValue> SubTreeSubscriber<Key, Value> {
    pub fn new(inner: ActorAsyncAskRef<ChangeItem<Key, Value>, ()>) -> Self {
        Self { inner }
    }

    pub async fn created(self, key: Arc<Key>, new: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Set { old: None, new },
        };
        self.send(change).await
    }

    pub async fn updated(
        &self,
        key: Arc<Key>,
        old: Option<Arc<Value>>,
        new: Arc<Value>,
    ) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Set { old, new },
        };
        self.send(change).await
    }

    pub async fn _delete(&self, key: Arc<Key>, old: Arc<Value>) -> anyhow::Result<()> {
        let change = Change {
            key,
            update: Update::Del { old },
        };
        self.send(change).await
    }

    async fn send(&self, change: Change<Arc<Key>, Arc<Value>>) -> anyhow::Result<()> {
        if let Err(err) = self.inner.ask_async(ChangeItem::new(change)).await {
            panic!("Failed to send error {}", err);
        }
        Ok(())
    }
}

pub enum RefactorMeResponse<Value> {
    List(Vec<Value>),
    Item(Option<()>),
}

pub enum Request<ID: PrimaryKey, Value: RecordValue> {
    List(ID),
    Item(
        #[allow(clippy::type_complexity)]
        (ID, usize, Box<dyn Fn(&mut Value) + Send + Sync + 'static>),
    ),
}

impl<ID: PrimaryKey, Value: RecordValue> std::fmt::Debug for Request<ID, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(arg0) => f.debug_tuple("List").field(arg0).finish(),
            Self::Item(arg0) => f.debug_tuple("Item").field(&(&arg0.0, &arg0.1)).finish(),
        }
    }
}

pub struct SubTree<ID: PrimaryKey, Value: RecordValue> {
    inner: ActorAsyncAskRef<Request<ID, Value>, RefactorMeResponse<Value>>,
}

impl<ID: PrimaryKey, Value: RecordValue> SubTree<ID, Value> {
    pub fn new(inner: ActorAsyncAskRef<Request<ID, Value>, RefactorMeResponse<Value>>) -> Self {
        Self { inner }
    }

    pub async fn list(&self, key: ID) -> anyhow::Result<Vec<Value>> {
        match self.inner.ask_async(Request::List(key)).await {
            Ok(RefactorMeResponse::List(list)) => Ok(list),
            Ok(RefactorMeResponse::Item(_)) => {
                panic!("Should never be responded. TODO: Please refactor me! Item returned expected List")
            }
            Err(err) => {
                panic!("Error, failed to list sub tree {}", err);
            }
        }
    }

    pub async fn mutate_by_index<F: Fn(&mut Value) + Send + Sync + 'static>(
        &self,
        id: ID,
        index: usize,
        f: F,
    ) -> anyhow::Result<Option<()>> {
        match self
            .inner
            .ask_async(Request::Item((id, index, Box::new(f))))
            .await
        {
            Ok(RefactorMeResponse::Item(option)) => Ok(option),
            Ok(RefactorMeResponse::List(_)) => {
                panic!("Should never be responded. TODO: Please refactor me! List returned expected Item")
            }
            Err(err) => {
                panic!("Error, failed to list sub tree {}", err);
            }
        }
    }
}

pub struct AggregateTree<ID: PrimaryKey, Value: RecordValue> {
    inner: ActorAsyncAskRef<ID, Option<Value>>,
}

impl<ID: PrimaryKey, Value: RecordValue> AggregateTree<ID, Value> {
    pub fn new(inner: ActorAsyncAskRef<ID, Option<Value>>) -> Self {
        Self { inner }
    }

    pub async fn get(&self, key: ID) -> anyhow::Result<Option<Value>> {
        match self.inner.ask_async(key).await {
            Ok(option) => Ok(option),
            Err(err) => {
                panic!("Error for AggregateTree: {}", err);
            }
        }
    }
}

pub struct UtilTreeAddress<Tree, Key: PrimaryKey, Value: RecordValue> {
    pub restorer: SubTreeRestorer,
    pub subscriber: SubTreeSubscriber<Key, Value>,
    pub tree: Tree,
}
