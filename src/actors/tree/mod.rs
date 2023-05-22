mod actor;
mod list;
mod memtable;
mod messages;

use std::sync::Arc;

pub use actor::*;
use am::{Actor, ActorRef, Ctx, DeadActorResult, Handler};
pub use messages::*;
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinSet,
};

use self::list::ListStream;

use super::{
    subtree::{IndexTreeActor, IndexTreeAddresses, SubTree, SubTreeSubscriber},
    wal::Wal,
    GenericTree,
};

pub fn tree_actor<A>(name: String, wal: Wal, ctx: &mut Ctx<A>) -> ActorRef<TreeActor>
where
    A: Actor + Handler<DeadActorResult<TreeActor>>,
{
    let tree = TreeActor::new(name, wal);
    ctx.spawn(tree)
}

pub struct Tree<Key, Value>
where
    Key: PrimaryKey,
    Value: RecordValue,
{
    inner: ActorRef<TreeActor>,
    subscribers: Arc<RwLock<Vec<SubTreeSubscriber<Key, Value>>>>,
}

impl<Key, Value> Tree<Key, Value>
where
    Key: PrimaryKey,
    Value: RecordValue,
{
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self {
            inner,
            subscribers: Arc::new(RwLock::new(vec![])),
        }
    }

    async fn get_unique_key(&self) -> anyhow::Result<Key> {
        Ok(self
            .inner
            .ask(GetUniqueKey::<Key>::default())
            .await
            .unwrap()
            .0)
    }

    pub async fn insert(&self, value: Value) -> anyhow::Result<Key>
    where
        Key: PrimaryKey,
    {
        let key = self.get_unique_key().await.unwrap();
        let id = bincode::serialize(&key)?;
        let json = serde_json::to_vec(&value)?;
        let record = UpdateRecord::new(id, json);

        let arc_key = Arc::new(key.clone());
        let arc_value = Arc::new(value);
        let subscribers = self
            .subscribers
            .try_read()
            .unwrap()
            .iter()
            .map(Clone::clone)
            .collect::<Vec<_>>();

        self.inner.async_ask(record).await.unwrap();
        // TODO(Alec): I know, I know, we should be doing something in between
        //             aware blocks but in this case it's ok, i swear!!!
        let mut set = JoinSet::new();
        for subscriber in subscribers.into_iter() {
            let key = arc_key.clone();
            let value = arc_value.clone();
            set.spawn(subscriber.created(key, value));
        }
        while let Some(res) = set.join_next().await {
            res?.unwrap();
        }

        Ok(key)
    }

    pub async fn update(&self, id: impl Into<Key>, value: Value) -> anyhow::Result<()> {
        let key = id.into();
        let old = self.get(key.clone()).await?.map(Arc::new);

        let id = bincode::serialize(&key)?;
        let json = serde_json::to_vec(&value)?;
        let record = UpdateRecord::new(id, json);
        self.inner.async_ask(record).await.unwrap();

        let new = Arc::new(value);
        let key = Arc::new(key);
        for subscriber in self.subscribers.try_read().unwrap().iter() {
            subscriber
                .updated(key.clone(), old.clone(), new.clone())
                .await
                .unwrap();
        }
        Ok(())
    }

    pub async fn get(&self, key: impl Into<Key>) -> anyhow::Result<Option<Value>> {
        let key = key.into();
        let bin = bincode::serialize(&key).unwrap();
        let msg = GetRecord::new(bin);
        let response = self.inner.async_ask(msg).await.unwrap();
        if let Some(value) = response.value {
            let value = serde_json::from_slice(&value).unwrap();
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn get_mem_table_snapshot(&self) -> anyhow::Result<Vec<Record>> {
        let result = self.inner.ask(GetMemTableSnapshot).await.unwrap();
        Ok(result.list)
    }

    pub async fn get_first(&self) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        self.get_head_or_tail(ListEnd::Head).await
    }

    pub async fn get_last(&self) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        self.get_head_or_tail(ListEnd::Tail).await
    }

    async fn get_head_or_tail(&self, end: ListEnd) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        let result = self.inner.ask(end).await.unwrap();
        if let Some(option) = result.option {
            Ok(Some(record_bin_to_value(&option)))
        } else {
            Ok(None)
        }
    }

    pub async fn list(&self) -> ListStream<Key, Value> {
        let tree = Self::new(self.inner.clone());
        ListStream::new(tree).await
    }

    pub async fn register_subscriber<ID, F>(
        &self,
        tree: Tree<ID, Vec<Key>>,
        identity: F,
    ) -> (SubTree<ID, Value>, oneshot::Receiver<()>)
    where
        ID: PrimaryKey,
        F: Fn(&Value) -> Option<&ID> + Send + Sync + 'static,
    {
        let source_tree = Self::new(self.inner.clone());
        let tree = IndexTreeActor::new(tree, source_tree, identity);
        let IndexTreeAddresses {
            subscriber,
            tree,
            ready_rx,
            restorer,
        } = tree.spawn();
        // TODO:
        // 1. Send ready_rx to restore
        // 2. Send restorer to tree actor
        // 3. Once all messages have been sent and restored, send a message to all tree actors that it's time to start
        // 4. wait for all ready_rx oneshot channels to recieve a message
        // 5. database should be fully restored
        self.inner.send_async(restorer).await.unwrap();
        self.subscribers.try_write().unwrap().push(subscriber);
        (tree, ready_rx)
    }

    pub(crate) fn as_generic(&self) -> GenericTree {
        GenericTree::new(self.inner.clone())
    }
}

fn record_bin_to_value<Key: PrimaryKey, Value: RecordValue>(
    record: &Record,
) -> (Key, Option<Value>) {
    let key = bincode::deserialize(&record.key).unwrap();
    if let Some(value) = &record.value {
        let json_value = serde_json::from_slice(value).unwrap();
        (key, Some(json_value))
    } else {
        (key, None)
    }
}
