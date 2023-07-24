use crate::Tree;

use super::{record_bin_to_value, PrimaryKey, Record, RecordValue};

pub struct ListStream<Key: PrimaryKey, Value: RecordValue> {
    _tree: Tree<Key, Value>,
    mem_table_snapshot: Vec<Record>,
    index: usize,
}

impl<Key: PrimaryKey, Value: RecordValue> ListStream<Key, Value> {
    pub async fn new(_tree: Tree<Key, Value>) -> Self {
        let snapshot = _tree.get_mem_table_snapshot().await.unwrap();
        Self {
            _tree,
            mem_table_snapshot: snapshot,
            index: 0,
        }
    }

    pub async fn next(&mut self) -> Option<(Key, Option<Value>)> {
        let result = self
            .mem_table_snapshot
            .get(self.index)
            .map(record_bin_to_value);
        self.index += 1;
        result
    }
}
