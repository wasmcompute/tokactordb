use std::{path::Path, sync::Arc, time::SystemTime};

use crc::{Crc, CRC_32_ISCSI};

use crate::{Collection, Database};

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
        K: serde::Serialize + Ord,
        V: serde::Serialize,
    {
        let collection = Collection::<K, V>::new(name.to_string());
        Record {
            name: Arc::new(name.to_string()),
            collection,
            database: self.clone(),
        }
    }

    fn write(&self, item: Item) -> anyhow::Result<()> {
        self.database.write(item)
    }
}

pub struct Record<K: Ord, V> {
    name: Arc<String>,
    collection: Collection<K, V>,
    database: Tree,
}

impl<K, V> Record<K, V>
where
    K: Ord + serde::Serialize + std::fmt::Debug + Clone,
    V: serde::Serialize + std::fmt::Debug,
{
    pub fn set(&mut self, key: K, value: V) -> anyhow::Result<Option<V>> {
        let item = Item::new(&*self.name, &key, Some(&value))?;
        self.database.write(item)?;
        self.collection.set(key, value);
        Ok(None)
    }

    pub fn as_iter(&self) -> impl Iterator<Item = &V> {
        self.collection.iter()
    }
}

// impl<'a> Iterator for StringHolderIter<'a> {
//     type Item = &'a str;
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.i >= self.string_holder.strings.len() {
//             None
//         } else {
//             self.i += 1;
//             Some(&self.string_holder.strings[self.i - 1])
//         }
//     }
// }

#[derive(serde::Serialize)]
pub struct Item {
    crc: u32,
    timestamp: u128,
    name: String,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
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

    fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(&self.name.as_bytes());
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
