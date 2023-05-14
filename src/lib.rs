mod database;
mod disk;
mod record;
mod tx;
mod writer;

use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use database::{Record, Tree};
use disk::Disk;
pub use record::Collection;
pub use record::{Aggregate, Change, Update};

pub trait AutoIncrement: Ord + Default + Display + Debug + Clone {
    fn increment(&mut self) -> Self;
}

impl AutoIncrement for u64 {
    fn increment(&mut self) -> Self {
        *self += 1;
        *self
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum WalEvents {
    CreateSchema { id: u64, name: String },
    CreateTree { id: u64, parent: u64, name: String },
}

struct Schema {
    parent_id: Option<u64>,
    name: String,
}

/// Globally shareable database object. Cloning this database object is cheap.
pub struct Database {
    root: Tree,
    wal: Arc<Mutex<Disk>>,
    forest: Arc<Mutex<Record<u64, String>>>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            wal: self.wal.clone(),
            forest: Arc::clone(&self.forest),
        }
    }
}

impl Database {
    pub fn new() -> anyhow::Result<Self> {
        let wal = Arc::new(Mutex::new(Disk::new()));
        let (root, forest) = Tree::new("db", wal.clone())?;
        Ok(Self {
            root,
            wal,
            forest: Arc::new(Mutex::new(forest)),
        })
    }

    // Restore the database given a path to the write-ahead-log. If the log exists,
    // open it and read it's contents into memory. Otherwise, create a new in
    // memory disk.
    // pub fn restore(path: impl AsRef<Path>) -> anyhow::Result<Self> {
    //     let path = path.as_ref();
    //     let disk = if path.exists() && path.metadata()?.is_file() {
    //         Disk::restore(path)?
    //     } else {
    //         Disk::new()
    //     };

    //     let wal = Arc::new(Mutex::new(disk));
    //     let root = Tree::new(wal.clone());
    //     let schema = root.new_collection(".schema");
    //     Ok(Self {
    //         root: Tree::new(wal.clone()),
    //         wal,
    //         schema: Arc::new(Mutex::new(schema)),
    //     })
    // }

    // pub fn schema(&self, name: impl ToString) -> anyhow::Result<Tree> {
    //     let key = self.schema.lock().unwrap().create(name.to_string())?;
    //     let branch = self.root.branch(name);
    // }

    // Create a new Record. Attempt to load any values that may exist on disk.
    // After loading the record, return it's implementation. Must be able to read
    // and write data.
    // pub fn create<K, V>(&mut self, name: impl ToString) -> anyhow::Result<Record<K, V>>
    // where
    //     K: Serialize + DeserializeOwned + AutoIncrement,
    //     V: Serialize + DeserializeOwned + std::fmt::Debug,
    // {
    //     // write into the table about the log
    //     let name = name.to_string();

    //     // Create the in-memory tree that will hold data
    //     let tree = Tree::new(self.clone());
    //     let mut record = tree.new_collection::<K, V>(name.clone());

    //     // Read the wal to load the data currently written to disk
    //     self.read_from_wal(&name, &mut record)?;

    //     // Push the tree onto a list we track
    //     self.trees.lock().unwrap().push(tree);

    //     // return the record
    //     Ok(record)
    // }

    // Dump all the data inside of the in-memory WAL to disk
    // pub fn close(self, path: impl AsRef<Path>) -> anyhow::Result<()> {
    //     self.wal.lock().unwrap().dump(path)?;
    //     Ok(())
    // }

    // Write a database Item to write-ahead-log
    // fn write(&self, item: database::Item) -> anyhow::Result<()> {
    //     let bytes = serde_json::to_vec(&item)?;
    //     self.wal.lock().unwrap().write_all(&bytes)?;
    //     Ok(())
    // }

    // fn read_from_wal<K, V>(
    //     &self,
    //     name: impl AsRef<str>,
    //     record: &mut Record<K, V>,
    // ) -> anyhow::Result<()>
    // where
    //     K: Serialize + DeserializeOwned + AutoIncrement,
    //     V: Serialize + DeserializeOwned + std::fmt::Debug,
    // {
    //     let wal = self.wal.lock().unwrap();
    //     let stream = serde_json::Deserializer::from_slice(wal.as_reader());
    //     for item in stream.into_iter::<Item>() {
    //         let item = item?;
    //         if item.calculate_crc() != item.crc {
    //             anyhow::bail!("Record is invalid");
    //         } else if item.name == name.as_ref() {
    //             let key: K = serde_json::from_slice(&item.key)?;
    //             let value: Option<V> = if let Some(value) = item.value.as_ref() {
    //                 Some(serde_json::from_slice(value)?)
    //             } else {
    //                 None
    //             };
    //             record.insert(key, value);
    //         }
    //     }
    //     Ok(())
    // }
}
