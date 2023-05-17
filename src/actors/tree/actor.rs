use am::{Actor, Ask, AsyncAsk, AsyncHandle};

use crate::actors::wal::Wal;

use super::{
    memtable::MemTable, GetMemTableSnapshot, GetRecord, GetRecordResult, InsertRecord,
    InsertSuccess, ListEnd, ListEndResult, PrimaryKey, Record, Snapshot, UpdateRecord,
};

pub struct TreeActor {
    memtable: MemTable,
    max: Option<Vec<u8>>,
    wal: Wal,
}

impl TreeActor {
    pub fn new(wal: Wal) -> Self {
        Self {
            memtable: MemTable::new(),
            max: None,
            wal,
        }
    }
}

impl Actor for TreeActor {}

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
            Key::default()
        };
        let wal = self.wal.clone();
        let serailize_key = bincode::serialize(&key).unwrap();

        self.memtable
            .insert(serailize_key.clone(), Some(msg.value.clone()));

        self.max = Some(serailize_key.clone());
        ctx.anonymous_handle(async move {
            if let Err(err) = wal.write(serailize_key, msg.value).await {
                println!("{err}");
                println!("Insertion failed to succeed")
            }
            InsertSuccess::new(key)
        })
    }
}

impl AsyncAsk<UpdateRecord> for TreeActor {
    type Result = ();

    fn handle(&mut self, msg: UpdateRecord, ctx: &mut am::Ctx<Self>) -> AsyncHandle<Self::Result> {
        let wal = self.wal.clone();
        self.memtable
            .insert(msg.key.clone(), Some(msg.value.clone()));
        ctx.anonymous_handle(async move {
            if let Err(err) = wal.write(msg.key, msg.value).await {
                println!("{err}");
                println!("Insertion failed to succeed")
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
