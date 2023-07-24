use std::{future::Future, pin::Pin, sync::Arc};

use tokactor::{Actor, Ask, AsyncAsk, Ctx, Handler};

use crate::actors::{
    db::{RestoreComplete, RestoreItem, TreeVersion},
    subtree::SubTreeRestorer,
    wal::Wal,
};

use super::{
    memtable::MemTable, GetMemTableSnapshot, GetRecord, GetUniqueKey, InsertRecord, InsertSuccess,
    ListEnd, PrimaryKey, Record, RecordValue, UniqueKey, UpdateRecord,
};

pub struct TreeActor {
    name: String,
    memtable: MemTable,
    max: Option<Vec<u8>>,
    wal: Wal,
    versions: Vec<TreeVersion>,
    version: u16,
    sub_trees: Option<Vec<SubTreeRestorer>>,
    write_enabled: bool,
}

impl Actor for TreeActor {}

impl TreeActor {
    pub fn new(name: String, versions: Vec<TreeVersion>, wal: Wal) -> Self {
        assert!(!versions.is_empty());
        assert!(u16::MAX as usize > versions.len());
        let version = versions.len() as u16 - 1;
        Self {
            name,
            memtable: MemTable::new(),
            max: None,
            wal,
            versions,
            version,
            sub_trees: None,
            write_enabled: false,
        }
    }

    pub fn get_unique_id<Key: PrimaryKey>(&mut self) -> Key {
        let key = if let Some(max) = self.max.as_ref() {
            let mut key: Key = bincode::deserialize(max).unwrap();
            key.increment()
        } else {
            // TODO(Alec): Implementing a hack because i just want to move forward
            //             with my life. Scan the entire memtable to find the largest
            //             ID key. This shouldn't be what we actual use if we ever
            //             move to production
            if self.memtable.is_empty() {
                Key::default()
            } else {
                // This is where the hack is (is this really a hack tho...)
                let (max_key, _) = self.memtable.get_last().unwrap();
                let mut key: Key = bincode::deserialize(&max_key).unwrap();
                key.increment()
            }
        };
        let serailize_key: Vec<u8> = bincode::serialize(&key).unwrap();
        self.max = Some(serailize_key);
        key
    }

    pub async fn upgrade(
        &self,
        mut key: Vec<u8>,
        mut data: Vec<u8>,
        mut version: u16,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        while version != self.version {
            let tree_version = self.versions.get(version as usize).unwrap();
            let (k, v) = tree_version.upgrade(key, data).await?;
            key = k;
            data = v;
            version += 1;
        }
        Ok((key, data))
    }
}

impl<Key: PrimaryKey> Ask<GetUniqueKey<Key>> for TreeActor {
    type Result = UniqueKey<Key>;

    fn handle(&mut self, _: GetUniqueKey<Key>, _: &mut Ctx<Self>) -> Self::Result {
        UniqueKey(self.get_unique_id())
    }
}

impl<Key> AsyncAsk<InsertRecord<Key>> for TreeActor
where
    Key: PrimaryKey,
{
    type Output = InsertSuccess<Key>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: InsertRecord<Key>, _: &mut Ctx<Self>) -> Self::Future<'a> {
        let key = if let Some(max) = self.max.as_ref() {
            let mut key: Key = bincode::deserialize(max).unwrap();
            key.increment()
        } else {
            // TODO(Alec): Implementing a hack because i just want to move forward
            //             with my life. Scan the entire memtable to find the largest
            //             ID key. This shouldn't be what we actual use if we ever
            //             move to production
            if self.memtable.is_empty() {
                Key::default()
            } else {
                // This is where the hack is (is this really a hack tho...)
                let (max_key, _) = self.memtable.get_last().unwrap();
                let mut key: Key = bincode::deserialize(&max_key).unwrap();
                key.increment()
            }
        };
        let wal = self.wal.clone();
        let table = self.name.clone();
        let serailize_key: Vec<u8> = bincode::serialize(&key).unwrap();

        self.memtable
            .insert(serailize_key.clone(), self.version, Some(msg.value.clone()));

        self.max = Some(serailize_key.clone());
        let write_enabled: bool = self.write_enabled;
        let version = self.version;
        Box::pin(async move {
            if write_enabled {
                if let Err(err) = wal.write(table, version, serailize_key, msg.value).await {
                    println!("{err}");
                    println!("Insertion failed to succeed")
                }
            }
            InsertSuccess::new(key)
        })
    }
}

impl AsyncAsk<UpdateRecord> for TreeActor {
    type Output = ();
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: UpdateRecord, _: &mut Ctx<Self>) -> Self::Future<'a> {
        let wal = self.wal.clone();
        let table = self.name.clone();
        self.memtable
            .insert(msg.key.clone(), self.version, Some(msg.value.clone()));
        let write_enabled: bool = self.write_enabled;
        let version = self.version;
        Box::pin(async move {
            if write_enabled {
                if let Err(err) = wal.write(table, version, msg.key, msg.value).await {
                    println!("{err}");
                    println!("Insertion failed to succeed")
                }
            }
        })
    }
}

impl<Key: PrimaryKey, Value: RecordValue> AsyncAsk<GetRecord<Key, Value>> for TreeActor {
    type Output = anyhow::Result<Option<Value>>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(
        &'a mut self,
        msg: GetRecord<Key, Value>,
        ctx: &mut Ctx<Self>,
    ) -> Self::Future<'a> {
        let option = self.memtable.get(&msg.key);
        if option.is_none() {
            return Box::pin(async move { Ok(None) });
        }
        let record = option.unwrap();
        if record.version == self.version {
            Box::pin(async move { Ok(Some(serde_json::from_slice(&record.data).unwrap())) })
        } else {
            // We need to send the version tree actors a message to update the messages
            // that are being retrieved.
            let addr = ctx.address();
            Box::pin(async move {
                // 1. Upgrade value to latest version
                let (key, value) = self.upgrade(msg.key, record.data, record.version).await?;
                // 2. Update record to reflect latest version
                addr.async_ask(UpdateRecord::new(key.clone(), value))
                    .await?;
                // 3. Get the newly updated record
                addr.async_ask(GetRecord::<Key, Value>::new(key)).await?
            })
        }
    }
}

impl AsyncAsk<ListEnd> for TreeActor {
    type Output = anyhow::Result<Option<Record>>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, msg: ListEnd, ctx: &mut Ctx<Self>) -> Self::Future<'a> {
        let option = match msg {
            ListEnd::Head => self.memtable.get_first(),
            ListEnd::Tail => self.memtable.get_last(),
        };
        if option.is_none() {
            return Box::pin(async { Ok(None) });
        }
        let (key, value_opt) = option.unwrap();
        if value_opt.is_none() {
            return Box::pin(async move { Ok(Some(Record::new(key, None))) });
        }
        let value = value_opt.unwrap();
        if self.version == value.version {
            Box::pin(async move { Ok(Some(Record::new(key, Some(value.data)))) })
        } else {
            println!("{} != {}", self.version, value.version);
            let addr = ctx.address();
            Box::pin(async move {
                println!("Upgrading");
                let (key, value) = self.upgrade(key, value.data, value.version).await?;
                // 2. Update record to reflect latest version
                addr.async_ask(UpdateRecord::new(key.clone(), value))
                    .await?;
                // 3. Return the result
                addr.async_ask(msg).await?
            })
        }
    }
}

impl AsyncAsk<GetMemTableSnapshot> for TreeActor {
    type Output = anyhow::Result<Vec<Record>>;
    type Future<'a> = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'a>>;

    fn handle<'a>(&'a mut self, _: GetMemTableSnapshot, ctx: &mut Ctx<Self>) -> Self::Future<'a> {
        let addr = ctx.address();
        // let max = self.version;
        // let versions = self.versions.clone();
        // let snapshot = self.memtable.as_sorted_vec();
        Box::pin(async move {
            let mut list = Vec::with_capacity(self.memtable.len());
            for (key, value) in self.memtable.as_iter() {
                let key = key.clone();
                if let Some(mem) = value {
                    if self.version == mem.version {
                        list.push(Record::new(key, Some(mem.data.clone())))
                    } else {
                        let (key, value) = self.upgrade(key, mem.data.clone(), mem.version).await?;
                        // 2. Update record to reflect latest version
                        addr.async_ask(UpdateRecord::new(key.clone(), value.clone()))
                            .await
                            .unwrap();
                        list.push(Record::new(key, Some(value)))
                    }
                } else {
                    list.push(Record::new(key, None));
                }
            }
            Ok(list)
        })
    }
}

impl Handler<RestoreItem> for TreeActor {
    fn handle(&mut self, item: RestoreItem, ctx: &mut Ctx<Self>) {
        let key = item.0.key;
        let value = item.0.value;
        let version = item.0.version;
        self.memtable.insert(key.clone(), version, value.clone());

        if let Some(list) = self.sub_trees.clone() {
            ctx.anonymous_task(async move {
                let key = Arc::new(key);
                let value = Arc::new(value);
                for item in list {
                    item.restore_record(key.clone(), value.clone()).await;
                }
            });
        }
    }
}

impl Ask<RestoreComplete> for TreeActor {
    type Result = ();

    fn handle(&mut self, _: RestoreComplete, _: &mut Ctx<Self>) -> Self::Result {
        // Basically just take all of the subtrees messages. When this happen, the subtree
        // will stop restoring messages and move into a ready state.
        let _ = self.sub_trees.take();
        self.write_enabled = true;
    }
}

impl Handler<SubTreeRestorer> for TreeActor {
    fn handle(&mut self, sub_tree: SubTreeRestorer, _: &mut Ctx<Self>) {
        let mut list = self.sub_trees.take().unwrap_or(vec![]);
        list.push(sub_tree);
        self.sub_trees = Some(list);
    }
}
