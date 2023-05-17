use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::actors::tree::{PrimaryKey, RecordValue};

#[derive(Debug)]
pub struct ID<Key: PrimaryKey, Value: RecordValue> {
    key: Key,
    _value: PhantomData<Value>,
}

impl<Key: PrimaryKey, Value: RecordValue> Clone for ID<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _value: self._value,
        }
    }
}

impl<'de, Key: PrimaryKey, Value: RecordValue> Deserialize<'de> for ID<Key, Value> {
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

impl<Key: PrimaryKey, Value: RecordValue> Serialize for ID<Key, Value> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.key.serialize(serializer)
    }
}

impl<Key: PrimaryKey, Value: RecordValue> From<Key> for ID<Key, Value> {
    fn from(value: Key) -> Self {
        Self::new(value)
    }
}

impl<Key: PrimaryKey, Value: RecordValue> ID<Key, Value> {
    pub fn new(key: Key) -> Self {
        Self {
            key,
            _value: PhantomData,
        }
    }
}
