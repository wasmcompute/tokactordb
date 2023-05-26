use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct MemRecord {
    pub version: u16,
    pub data: Vec<u8>,
}

pub struct MemTable {
    map: BTreeMap<Vec<u8>, Option<MemRecord>>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, version: u16, value: Option<Vec<u8>>) -> usize {
        let key_len = key.len();
        let value_len = if value.is_none() {
            1
        } else {
            1 + value.as_ref().unwrap().len()
        };
        self.size += key_len + value_len;
        let record = value.map(|data| MemRecord { version, data });
        self.map.insert(key, record);
        self.size
    }

    pub fn get(&self, key: &[u8]) -> Option<MemRecord> {
        match self.map.get(key) {
            Some(value) => value.clone(),
            None => None,
        }
    }

    pub fn get_first(&self) -> Option<(Vec<u8>, Option<MemRecord>)> {
        self.map
            .first_key_value()
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    /// Largest value in the tree
    pub fn get_last(&self) -> Option<(Vec<u8>, Option<MemRecord>)> {
        self.map
            .last_key_value()
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn as_sorted_vec(&self) -> Vec<(Vec<u8>, Option<MemRecord>)> {
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
