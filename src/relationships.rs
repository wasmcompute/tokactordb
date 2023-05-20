use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::{
    actors::tree::{PrimaryKey, RecordValue},
    AutoIncrement,
};

#[derive(Debug)]
pub struct ID<Key: PrimaryKey, Value: RecordValue + 'static> {
    key: Key,
    _value: PhantomData<Value>,
}

unsafe impl<Key: PrimaryKey, Value: RecordValue + 'static> Send for ID<Key, Value> {}
unsafe impl<Key: PrimaryKey, Value: RecordValue + 'static> Sync for ID<Key, Value> {}

impl<Key: PrimaryKey, Value: RecordValue + 'static> PrimaryKey for ID<Key, Value> {}

impl<Key: PrimaryKey, Value: RecordValue + 'static> Default for ID<Key, Value> {
    fn default() -> Self {
        Self {
            key: Default::default(),
            _value: Default::default(),
        }
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> AutoIncrement for ID<Key, Value> {
    fn increment(&mut self) -> Self {
        let key = self.key.increment();
        Self::new(key)
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> Eq for ID<Key, Value> {}
impl<Key: PrimaryKey, Value: RecordValue + 'static> PartialEq for ID<Key, Value> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> Ord for ID<Key, Value> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}
impl<Key: PrimaryKey, Value: RecordValue + 'static> PartialOrd for ID<Key, Value> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> Clone for ID<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _value: self._value,
        }
    }
}

impl<'de, Key: PrimaryKey, Value: RecordValue + 'static> Deserialize<'de> for ID<Key, Value> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let key = Key::deserialize(deserializer)?;
        Ok(Self {
            key,
            _value: PhantomData,
        })
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> Serialize for ID<Key, Value> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.key.serialize(serializer)
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> From<Key> for ID<Key, Value> {
    fn from(value: Key) -> Self {
        Self::new(value)
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> std::fmt::Display for ID<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl<Key: PrimaryKey, Value: RecordValue + 'static> ID<Key, Value> {
    pub fn new(key: Key) -> Self {
        Self {
            key,
            _value: PhantomData,
        }
    }
}
