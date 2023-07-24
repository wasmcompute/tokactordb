use std::{error::Error, sync::Arc};

use serde::Deserialize;

use crate::Change;

#[derive(Debug, Clone)]
pub struct RestoreItem {
    key: Arc<Vec<u8>>,
    value: Arc<Option<Vec<u8>>>,
}

impl RestoreItem {
    pub fn new(key: Arc<Vec<u8>>, value: Arc<Option<Vec<u8>>>) -> Self {
        Self { key, value }
    }

    pub fn deserialize<'a, Key, Value>(&'a self) -> Result<(Key, Option<Value>), Box<dyn Error>>
    where
        Key: Deserialize<'a>,
        Value: Deserialize<'a>,
    {
        let key: Key = bincode::deserialize(&self.key)?;
        if let Some(value) = &*self.value {
            let value: Value = serde_json::from_slice(value)?;
            Ok((key, Some(value)))
        } else {
            Ok((key, None))
        }
    }
}

#[derive(Debug)]
pub struct ChangeItem<Key, Value> {
    inner: Change<Arc<Key>, Arc<Value>>,
}

impl<Key, Value> Clone for ChangeItem<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Key, Value> ChangeItem<Key, Value> {
    pub fn new(inner: Change<Arc<Key>, Arc<Value>>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Change<Arc<Key>, Arc<Value>> {
        self.inner
    }
}
