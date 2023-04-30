mod database;
mod disk;
mod record;
mod tx;

use std::{
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

use database::{Item, Record, Tree};
use disk::Disk;
pub use record::Collection;

/// Globally shareable database object. Cloning this database object is cheap.
pub struct Database {
    trees: Arc<Mutex<Vec<Tree>>>,
    wal: Arc<Mutex<Disk>>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            trees: Arc::new(Mutex::new(Vec::new())),
            wal: self.wal.clone(),
        }
    }
}

trait Table {}

pub trait AutoIncrement: Ord + Default {
    fn increment(&mut self) -> Self;
}

impl AutoIncrement for u64 {
    fn increment(&mut self) -> Self {
        *self += 1;
        *self
    }
}

impl Database {
    pub fn new() -> Self {
        Self {
            trees: Arc::new(Mutex::new(Vec::new())),
            wal: Arc::new(Mutex::new(Disk::new())),
        }
    }

    /// Restore the database given a path to the write-ahead-log. If the log exists,
    /// open it and read it's contents into memory. Otherwise, create a new in
    /// memory disk.
    pub fn restore(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let disk = if path.exists() && path.metadata()?.is_file() {
            Disk::restore(path)?
        } else {
            Disk::new()
        };

        Ok(Self {
            trees: Arc::new(Mutex::new(Vec::new())),
            wal: Arc::new(Mutex::new(disk)),
        })
    }

    /// Create a new Record. Attempt to load any values that may exist on disk.
    /// After loading the record, return it's implementation. Must be able to read
    /// and write data.
    pub fn create<K, V>(&mut self, name: impl ToString) -> anyhow::Result<Record<K, V>>
    where
        K: serde::Serialize
            + serde::de::DeserializeOwned
            + AutoIncrement
            + Ord
            + std::fmt::Debug
            + Clone,
        V: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        // write into the table about the log
        let name = name.to_string();

        // Create the in-memory tree that will hold data
        let tree = Tree::new(self.clone());
        let mut record = tree.new_collection::<K, V>(name.clone());

        // Read the wal to load the data currently written to disk
        let wal = self.wal.lock().unwrap();
        let stream = serde_json::Deserializer::from_slice(wal.as_reader());
        for item in stream.into_iter::<Item>() {
            let item = item?;
            if item.calculate_crc() != item.crc {
                anyhow::bail!("Record is invalid");
            } else if item.name == name {
                let key: K = serde_json::from_slice(&item.key)?;
                let value: Option<V> = if let Some(value) = item.value.as_ref() {
                    Some(serde_json::from_slice(value)?)
                } else {
                    None
                };
                record.insert(key, value);
            }
        }

        // Push the tree onto a list we track
        self.trees.lock().unwrap().push(tree);

        // return the record
        Ok(record)
    }

    /// Dump all the data inside of the in-memory WAL to disk
    pub fn close(self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.wal.lock().unwrap().dump(path)?;
        Ok(())
    }

    /// Write a database Item to write-ahead-log
    fn write(&self, item: database::Item) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(&item)?;
        self.wal.lock().unwrap().write_all(&bytes)?;
        Ok(())
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
