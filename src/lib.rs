mod record;
mod storage;

use std::{collections::HashMap, marker::PhantomData};

pub use record::Collection;
use storage::WAL;

pub trait Table<K, V> {
    fn create(this: V)
    where
        Self: Sized;
}

pub struct Store<T: Table> {
    _collection: PhantomData<T>,
}

impl<T: Table> Store<T> {
    pub fn set(key: u64, inner: T) -> 
}

// Database -> Index -> Cache -> Disk
pub struct Database {
    collections: HashMap<String, Box<dyn Table>>,
    wal: WAL,
}

impl Database {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            wal: WAL::new(),
        }
    }

    pub fn create<K, V, T>(&mut self, name: impl ToString, table: T) -> Store<T>
    where
        K: serde::Serialize + Ord,
        V: serde::Serialize,
        T: Table + 'static,
    {
        self.collections.insert(name.to_string(), Box::new(table));
        self
    }
}
