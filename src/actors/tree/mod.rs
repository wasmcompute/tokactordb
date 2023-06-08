mod actor;
mod list;
mod memtable;
mod messages;

use std::sync::Arc;

pub use actor::*;
pub use messages::*;
use tokactor::{Actor, ActorRef, Ctx, DeadActorResult, Handler};
use tokio::{sync::RwLock, task::JoinSet};

use self::list::ListStream;

use super::{
    db::TreeVersion,
    subtree::{SubTreeRestorer, SubTreeSubscriber},
    wal::Wal,
    GenericTree,
};

pub fn tree_actor<A>(
    name: String,
    versions: Vec<TreeVersion>,
    wal: Wal,
    ctx: &mut Ctx<A>,
) -> ActorRef<TreeActor>
where
    A: Actor + Handler<DeadActorResult<TreeActor>>,
{
    let tree = TreeActor::new(name, versions, wal);
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

        let arc_key = Arc::new(key.clone());
        let arc_value = Arc::new(value);
        let subscribers = self
            .subscribers
            .try_read()
            .unwrap()
            .iter()
            .map(Clone::clone)
            .collect::<Vec<_>>();

        let record = UpdateRecord::new(id, json);
        self.inner.async_ask(record).await.unwrap();

        // UGGGHHH, this should be done within a transaction! Yes! One change
        // can lead to many more changes happening, but ALL WE CARE ABOUT IS THAT
        // OUR WRITE SUCCEEDED.
        //
        // We should be able to wait for all updates to complete though from within
        // the transaction.
        //
        // We do need to figure out a better way to do this, but maybe we just do
        // versioning first
        tokio::spawn(async move {
            // TODO(Alec): I know, I know, we should be doing something in between
            //             aware blocks but in this case it's ok, i swear!!!
            let mut set = JoinSet::new();
            for subscriber in subscribers.into_iter() {
                let key = arc_key.clone();
                let value = arc_value.clone();
                let old = old.clone();
                set.spawn(async move { subscriber.updated(key, old, value).await });
            }
            while let Some(res) = set.join_next().await {
                res.unwrap().unwrap();
            }
        });

        Ok(())
    }

    pub async fn get(&self, key: impl Into<Key>) -> anyhow::Result<Option<Value>> {
        let key = key.into();
        let bin = bincode::serialize(&key).unwrap();
        let msg = GetRecord::<Key, Value>::new(bin);
        let response = self.inner.async_ask(msg).await.unwrap();
        Ok(response.value)
    }

    pub(crate) async fn get_mem_table_snapshot(&self) -> anyhow::Result<Vec<Record>> {
        let result = self.inner.async_ask(GetMemTableSnapshot).await.unwrap();
        Ok(result.list)
    }

    pub async fn get_first(&self) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        self.get_head_or_tail(ListEnd::Head).await
    }

    pub async fn get_last(&self) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        self.get_head_or_tail(ListEnd::Tail).await
    }

    async fn get_head_or_tail(&self, end: ListEnd) -> anyhow::Result<Option<(Key, Option<Value>)>> {
        let result = self.inner.async_ask(end).await.unwrap();
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

    pub fn register_subscriber(&self, subscriber: SubTreeSubscriber<Key, Value>) {
        self.subscribers.try_write().unwrap().push(subscriber);
    }

    pub async fn register_restorer(&self, restorer: SubTreeRestorer) {
        self.inner.send_async(restorer).await.unwrap();
    }

    pub(crate) fn duplicate(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            subscribers: Arc::clone(&self.subscribers),
        }
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
