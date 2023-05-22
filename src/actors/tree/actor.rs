use std::sync::Arc;

use am::{Actor, Ask, AsyncAsk, AsyncHandle, Handler};

use crate::actors::{
    db::{RestoreComplete, RestoreItem},
    subtree::SubTreeRestorer,
    wal::Wal,
};

use super::{
    memtable::MemTable, GetMemTableSnapshot, GetRecord, GetRecordResult, GetUniqueKey,
    InsertRecord, InsertSuccess, ListEnd, ListEndResult, PrimaryKey, Record, Snapshot, UniqueKey,
    UpdateRecord,
};

pub struct TreeActor {
    name: String,
    memtable: MemTable,
    max: Option<Vec<u8>>,
    wal: Wal,
    sub_trees: Option<Vec<SubTreeRestorer>>,
    write_enabled: bool,
}

impl TreeActor {
    pub fn new(name: String, wal: Wal) -> Self {
        Self {
            name,
            memtable: MemTable::new(),
            max: None,
            wal,
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
            .insert(serailize_key.clone(), Some(msg.value.clone()));

        self.max = Some(serailize_key.clone());
        let write_enabled: bool = self.write_enabled;
        ctx.anonymous_handle(async move {
            if write_enabled {
                if let Err(err) = wal.write(table, serailize_key, msg.value).await {
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
            .insert(msg.key.clone(), Some(msg.value.clone()));
        let write_enabled: bool = self.write_enabled;
        ctx.anonymous_handle(async move {
            if write_enabled {
                if let Err(err) = wal.write(table, msg.key, msg.value).await {
                    println!("{err}");
                    println!("Insertion failed to succeed")
                }
            }
        })
    }
}

impl AsyncAsk<GetRecord> for TreeActor {
    type Result = GetRecordResult;

    fn handle(&mut self, msg: GetRecord, ctx: &mut am::Ctx<Self>) -> AsyncHandle<Self::Result> {
        let option = self.memtable.get(&msg.key);
        ctx.anonymous_handle(async move { GetRecordResult::new(option) })
    }
}

impl Ask<ListEnd> for TreeActor {
    type Result = ListEndResult;

    fn handle(&mut self, msg: ListEnd, _: &mut am::Ctx<Self>) -> Self::Result {
        let option = match msg {
            ListEnd::Head => self.memtable.get_first(),
            ListEnd::Tail => self.memtable.get_last(),
        }
        .map(|(key, value)| Record { key, value });
        ListEndResult { option }
    }
}

impl Ask<GetMemTableSnapshot> for TreeActor {
    type Result = Snapshot;

    fn handle(&mut self, _: GetMemTableSnapshot, _: &mut am::Ctx<Self>) -> Self::Result {
        Snapshot {
            list: self.memtable.as_sorted_vec(),
        }
    }
}

impl Handler<RestoreItem> for TreeActor {
    fn handle(&mut self, item: RestoreItem, ctx: &mut am::Ctx<Self>) {
        let key = item.0.key;
        let value = item.0.value;
        self.memtable.insert(key.clone(), value.clone());

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
