mod actor;
mod list;
mod memtable;
mod messages;

use std::marker::PhantomData;

pub use actor::*;
use am::{Actor, ActorRef, Ctx, DeadActorResult, Handler};
pub use messages::*;

use self::list::ListStream;

use super::{db::RestoreItem, wal::Wal};

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
    _key: PhantomData<Key>,
    _value: PhantomData<Value>,
}

impl<Key, Value> Tree<Key, Value>
where
    Key: PrimaryKey,
    Value: RecordValue,
{
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self {
            inner,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub async fn insert(&self, value: Value) -> anyhow::Result<Key>
    where
        Key: PrimaryKey,
    {
        println!("inserting {:?}", value);
        let value = serde_json::to_vec(&value)?;
        let record = InsertRecord::new(value);
        let response = self.inner.async_ask(record).await.unwrap();
        Ok(response.key)
    }

    pub async fn update(&self, id: impl Into<Key>, value: Value) -> anyhow::Result<()> {
        let id = bincode::serialize(&id.into())?;
        println!("{}", serde_json::to_string_pretty(&value)?);
        let value = serde_json::to_vec(&value)?;
        let record = UpdateRecord::new(id, value);
        self.inner.async_ask(record).await.unwrap();
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

    pub(crate) fn as_generic(&self) -> GenericTree {
        GenericTree {
            inner: self.inner.clone(),
        }
    }
}

fn record_bin_to_value<Key: PrimaryKey, Value: RecordValue>(
    record: &Record,
) -> (Key, Option<Value>) {
    let key = bincode::deserialize(&record.key).unwrap();
    if let Some(value) = &record.value {
        let value = serde_json::from_slice(value).unwrap();
        (key, Some(value))
    } else {
        (key, None)
    }
}

#[derive(Debug, Clone)]
pub struct GenericTree {
    inner: ActorRef<TreeActor>,
}

impl GenericTree {
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self { inner }
    }

    pub async fn send_generic_item(&self, item: RestoreItem) {
        self.inner.send_async(item).await.unwrap();
    }
}
