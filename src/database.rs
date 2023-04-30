use std::{sync::Arc, time::SystemTime};

use crc::{Crc, CRC_32_ISCSI};

use crate::{AutoIncrement, Collection, Database};

#[derive(Clone)]
pub struct Tree {
    database: Database,
}

impl Tree {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub fn new_collection<K, V>(&self, name: impl ToString) -> Record<K, V>
    where
        K: serde::Serialize + AutoIncrement,
        V: serde::Serialize,
    {
        let collection = Collection::<K, V>::new(name.to_string());
        Record {
            max: K::default(),
            name: Arc::new(name.to_string()),
            collection,
            database: self.clone(),
        }
    }

    fn write(&self, item: Item) -> anyhow::Result<()> {
        self.database.write(item)
    }
}

pub struct Record<K: AutoIncrement, V> {
    max: K,
    name: Arc<String>,
    collection: Collection<K, V>,
    database: Tree,
}

impl<K, V> Record<K, V>
where
    K: AutoIncrement + serde::Serialize + std::fmt::Debug + Clone,
    V: serde::Serialize + std::fmt::Debug,
{
    pub(crate) fn insert(&mut self, key: K, value: Option<V>) {
        if key > self.max {
            self.max = key.clone();
        }

        if let Some(value) = value {
            self.collection.set(key, value);
        } else {
            self.collection.del(key);
        }
    }

    pub fn create(&mut self, value: V) -> anyhow::Result<()>
    where
        K: AutoIncrement,
    {
        let key = self.max.increment();
        let item = Item::new(&*self.name, &key, Some(&value))?;
        self.database.write(item)?;
        self.collection.set(key, value);
        Ok(())
    }

    // pub fn get(&self, key: K) -> &V {}

    // pub fn get_mut(&mut self, key: K) -> &mut V {}

    pub fn as_iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.collection.iter()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Item {
    pub crc: u32,
    timestamp: u128,
    pub name: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Item {
    pub fn new<K, V>(name: impl AsRef<str>, key: &K, value: Option<&V>) -> anyhow::Result<Self>
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
            name: name.as_ref().to_string(),
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
        digest.update(self.name.as_bytes());
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
