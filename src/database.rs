use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::SystemTime, io::Write,
};

use crc::{Crc, CRC_32_ISCSI};

use crate::{writer::Wal, AutoIncrement, Collection, disk::Disk};

#[derive(Clone)]
pub struct Tree {
    id: Arc<RwLock<u64>>,
    wal: Arc<Mutex<Disk>>,
    trees: Arc<Mutex<HashMap<String, Tree>>>,
}

impl Tree {
    pub(crate) fn new(
        name: impl ToString,
        wal: Arc<Mutex<Disk>>,
    ) -> anyhow::Result<(Self, Record<u64, String>)> {
        let this = Self {
            id: Arc::new(RwLock::new(0)),
            wal,
            trees: Arc::new(Mutex::new(HashMap::new())),
        };

        let mut tree_record = Record {
            max: 0,
            name: name.to_string(),
            collection: Collection::new(),
            database: this.clone(),
        };

        let id = tree_record.max.increment();
        let item = Item::new(&id, Some(&name.to_string()))?;
        let bytes = serde_json::to_vec(&item)?;
        this.wal.lock().unwrap().write_all(buf)

        *this.id.write().unwrap() = id;

        Ok((this, tree_record))
    }

    pub(crate) fn write(&self, )

    // fn write_tree(&self) ->

    // pub fn new_collection<K, V>(&self, name: impl ToString) -> Record<K, V>
    // where
    //     K: serde::Serialize + AutoIncrement,
    //     V: serde::Serialize,
    // {
    //     let mut record = Record {
    //         max: K::default(),
    //         name: name.to_string(),
    //         collection: Collection::<K, V>::new(),
    //         database: self.clone(),
    //     };
    //     record
    // }
}

pub struct Record<K: AutoIncrement, V> {
    max: K,
    name: String,
    collection: Collection<K, V>,
    database: Tree,
}

impl<K, V> Record<K, V>
where
    K: AutoIncrement + serde::Serialize + std::fmt::Debug + Clone,
    V: serde::Serialize,
{
    // pub(crate) fn insert(&mut self, key: K, value: Option<V>) {
    //     if key > self.max {
    //         self.max = key.clone();
    //     }

    //     if let Some(value) = value {
    //         self.collection.set(key, value);
    //     } else {
    //         self.collection.del(key);
    //     }
    // }

    pub fn create(&mut self, value: V) -> anyhow::Result<K>
    where
        K: AutoIncrement,
    {
        let key = self.max.increment();
        let item = Item::new(&key, Some(&value))?;
        self.database.write(item)?;
        self.collection.set(key.clone(), value);
        Ok(key)
    }

    // pub fn get(&self, key: &K) -> Option<&V> {
    //     self.collection.get(key)
    // }

    // pub fn get_mut<F>(&mut self, key: &K, f: F) -> anyhow::Result<Option<()>>
    // where
    //     F: Fn(&mut V),
    // {
    //     if let Some(value) = self.collection.get_mut(key) {
    //         f(value);
    //         let item = Item::new(&*self.name, &key, Some(&value))?;
    //         self.database.write(item)?;
    //         Ok(Some(()))
    //     } else {
    //         Ok(None)
    //     }
    // }

    // pub fn as_iter(&self) -> impl Iterator<Item = (&K, &V)> {
    //     self.collection.iter()
    // }

    // pub fn subscribe<A>(&self, name: impl ToString) -> ()
    // where
    //     K: Serialize + DeserializeOwned + AutoIncrement,
    //     V: Serialize + DeserializeOwned + std::fmt::Debug,
    //     A: Aggregate<K, V>,
    // {
    //     // write into the table about the log
    //     let name = name.to_string();

    //     // Create the in-memory tree that will hold data
    //     let tree = Tree::new(self.database.database.clone());
    //     let mut record = tree.new_collection::<K, V>(name.clone());

    //     // Read the wal to load the data currently written to disk
    //     // self.read_from_wal(&name, &mut record)?;

    //     // self.database.database..lock().unwrap().push(tree);

    //     Ok(record)
    // }
}

#[derive(serde::Serialize, serde::Deserialize)]
enum ItemType {
    Database {},
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Item {
    pub crc: u32,
    timestamp: u128,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Item {
    pub fn new<K, V>(key: &K, value: Option<&V>) -> anyhow::Result<Self>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        let value = if let Some(value) = value.map(|v| serde_json::to_vec(&v)) {
            Some(value?)
        } else {
            None
        };
        let mut item = Self {
            crc: 0,
            timestamp: now(),
            key: serde_json::to_vec(&key)?,
            value,
        };
        item.crc = item.calculate_crc();
        Ok(item)
    }

    pub fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(&self.key);
        digest.update(self.value.as_ref().unwrap_or(&vec![]));
        digest.finalize()
    }
}

pub fn now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
