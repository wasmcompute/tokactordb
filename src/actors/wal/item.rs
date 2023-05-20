use std::time::SystemTime;

use crc::{Crc, CRC_32_ISCSI};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Item {
    pub crc: u32,
    timestamp: u128,
    pub table: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Item {
    pub fn new(table: String, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        let mut item = Self {
            crc: 0,
            timestamp: now(),
            table,
            key,
            value,
        };
        item.crc = item.calculate_crc();
        item
    }

    pub fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(self.table.as_bytes());
        digest.update(&self.key);
        digest.update(self.value.as_ref().unwrap_or(&vec![]));
        digest.finalize()
    }

    pub fn is_valid(&self) -> bool {
        self.calculate_crc() == self.crc
    }
}

impl std::fmt::Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = if let Some(value) = self.value.as_ref() {
            let json: serde_json::Value = serde_json::from_slice(value).unwrap();
            json.to_string()
        } else {
            "None".to_string()
        };
        write!(
            f,
            "CRC: {}, table: {}, value: {}",
            self.crc, self.table, value
        )
    }
}

pub fn now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
