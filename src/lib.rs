mod database;
mod disk;
mod record;
mod tx;

use std::{
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

use database::{Record, Tree};
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

impl Database {
    pub fn new() -> Self {
        Self {
            trees: Arc::new(Mutex::new(Vec::new())),
            wal: Arc::new(Mutex::new(Disk::new())),
        }
    }

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

    pub fn create<K, V>(&mut self, name: impl ToString) -> Record<K, V>
    where
        K: serde::Serialize + Ord,
        V: serde::Serialize,
    {
        // write into the table about the log
        let tree = Tree::new(self.clone());
        let record = tree.new_collection::<K, V>(name);
        self.trees.lock().unwrap().push(tree);
        record
    }

    pub fn close(self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.wal.lock().unwrap().dump(path)?;
        Ok(())
    }

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
