use std::sync::Arc;

use am::{Actor, Ask, AsyncAsk, AsyncHandle, Handler};

use crate::actors::{
    db::{RestoreComplete, RestoreItem, TreeVersion},
    subtree::SubTreeRestorer,
    wal::Wal,
};

use super::{
    memtable::MemTable, GetMemTableSnapshot, GetRecord, GetRecordResult, GetUniqueKey,
    InsertRecord, InsertSuccess, ListEnd, ListEndResult, PrimaryKey, Record, RecordValue, Snapshot,
    UniqueKey, UpdateRecord,
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
        versions: &[TreeVersion],
        max: u16,
        mut key: Vec<u8>,
        mut data: Vec<u8>,
        mut version: u16,
    ) -> (Vec<u8>, Vec<u8>) {
        while version != max {
            let tree_version = versions.get(version as usize).unwrap();
            let (k, v) = tree_version.upgrade(key, data).await;
            key = k;
            data = v;
            version += 1;
        }
        (key, data)
    }
}

impl Actor for TreeActor {}

impl<Key: PrimaryKey> Ask<GetUniqueKey<Key>> for TreeActor {
    type Result = UniqueKey<Key>;

    fn handle(&mut self, _: GetUniqueKey<Key>, _: &mut am::Ctx<Self>) -> Self::Result {
        UniqueKey(self.get_unique_id())
    }
}

impl<Key> AsyncAsk<InsertRecord<Key>> for TreeActor
where
    Key: PrimaryKey,
{
    type Result = InsertSuccess<Key>;

    fn handle(
        &mut self,
        msg: InsertRecord<Key>,
        ctx: &mut am::Ctx<Self>,
    ) -> AsyncHandle<Self::Result> {
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
        ctx.anonymous_handle(async move {
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
    type Result = ();

    fn handle(&mut self, msg: UpdateRecord, ctx: &mut am::Ctx<Self>) -> AsyncHandle<Self::Result> {
        let wal = self.wal.clone();
        let table = self.name.clone();
        self.memtable
            .insert(msg.key.clone(), self.version, Some(msg.value.clone()));
        let write_enabled: bool = self.write_enabled;
        let version = self.version;
        ctx.anonymous_handle(async move {
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
    type Result = GetRecordResult<Value>;

    fn handle(
        &mut self,
        msg: GetRecord<Key, Value>,
        ctx: &mut am::Ctx<Self>,
    ) -> AsyncHandle<Self::Result> {
        let option = self.memtable.get(&msg.key);
        if option.is_none() {
            return ctx.anonymous_handle(async move { GetRecordResult::new(None) });
        }
        let record = option.unwrap();
        if record.version == self.version {
            ctx.anonymous_handle(async move {
                GetRecordResult::new(Some(serde_json::from_slice(&record.data).unwrap()))
            })
        } else {
            // We need to send the version tree actors a message to update the messages
            // that are being retrieved.
            let versions = self.versions.clone();
            let max_version = self.version;
            let key = msg.key;
            let addr = ctx.address();
            ctx.anonymous_handle(async move {
                // 1. Upgrade value to latest version
                let (key, value) =
                    Self::upgrade(&versions, max_version, key, record.data, record.version).await;
                // 2. Update record to reflect latest version
                addr.async_ask(UpdateRecord::new(key.clone(), value))
                    .await
                    .unwrap();
                // 3. Get the newly updated record
                addr.async_ask(GetRecord::<Key, Value>::new(key))
                    .await
                    .unwrap()
            })
        }
    }
}

impl AsyncAsk<ListEnd> for TreeActor {
    type Result = ListEndResult;

    fn handle(&mut self, msg: ListEnd, ctx: &mut am::Ctx<Self>) -> AsyncHandle<Self::Result> {
        let option = match msg {
            ListEnd::Head => self.memtable.get_first(),
            ListEnd::Tail => self.memtable.get_last(),
        };
        if let Some((key, opt)) = option {
            if let Some(value) = opt {
                println!("{} == {}", self.version, value.version);
                if self.version == value.version {
                    ctx.anonymous_handle(async move { ListEndResult::new(key, value.data) })
                } else {
                    let versions = self.versions.clone();
                    let max_version = self.version;
                    let addr = ctx.address();
                    ctx.anonymous_handle(async move {
                        println!("Upgrading");
                        let (key, value) =
                            Self::upgrade(&versions, max_version, key, value.data, value.version)
                                .await;
                        // 2. Update record to reflect latest version
                        addr.async_ask(UpdateRecord::new(key.clone(), value))
                            .await
                            .unwrap();
                        // 3. Return the result
                        addr.async_ask(msg).await.unwrap()
                    })
                }
            } else {
                ctx.anonymous_handle(async move { ListEndResult::key(key) })
            }
        } else {
            ctx.anonymous_handle(async { ListEndResult::none() })
        }
    }
}

impl AsyncAsk<GetMemTableSnapshot> for TreeActor {
    type Result = Snapshot;

    fn handle(
        &mut self,
        _: GetMemTableSnapshot,
        ctx: &mut am::Ctx<Self>,
    ) -> AsyncHandle<Self::Result> {
        let max = self.version;
        let addr = ctx.address();
        let versions = self.versions.clone();
        let snapshot = self.memtable.as_sorted_vec();
        ctx.anonymous_handle(async move {
            let mut list = Vec::with_capacity(snapshot.len());
            for (key, value) in snapshot {
                if let Some(mem) = value {
                    println!("{} == {}", max, mem.version);
                    if max == mem.version {
                        list.push(Record::new(key, Some(mem.data)))
                    } else {
                        let (key, value) =
                            Self::upgrade(&versions, max, key, mem.data, mem.version).await;
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
            Snapshot { list }
        })
    }
}

impl Handler<RestoreItem> for TreeActor {
    fn handle(&mut self, item: RestoreItem, ctx: &mut am::Ctx<Self>) {
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

    fn handle(&mut self, _: RestoreComplete, _: &mut am::Ctx<Self>) -> Self::Result {
        // Basically just take all of the subtrees messages. When this happen, the subtree
        // will stop restoring messages and move into a ready state.
        let _ = self.sub_trees.take();
        self.write_enabled = true;
    }
}

impl Handler<SubTreeRestorer> for TreeActor {
    fn handle(&mut self, sub_tree: SubTreeRestorer, _: &mut am::Ctx<Self>) {
        let mut list = self.sub_trees.take().unwrap_or(vec![]);
        list.push(sub_tree);
        self.sub_trees = Some(list);
    }
}
