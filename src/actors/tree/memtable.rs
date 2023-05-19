use std::collections::BTreeMap;

use super::Record;

pub struct MemTable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> usize {
        let key_len = key.len();
        let value_len = if value.is_none() {
            1
        } else {
            1 + value.as_ref().unwrap().len()
        };
        self.size += key_len + value_len;
        self.map.insert(key, value);
        self.size
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.map.get(key) {
            Some(value) => value.clone(),
            None => None,
        }
    }

    pub fn get_first(&self) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        self.map
            .first_key_value()
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    /// Largest value in the tree
    pub fn get_last(&self) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        self.map
            .last_key_value()
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn as_sorted_vec(&self) -> Vec<Record> {
        self.map
            .iter()
            .map(|(k, v)| Record {
                key: k.clone(),
                value: v.clone(),
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
