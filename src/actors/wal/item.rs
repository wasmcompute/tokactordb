use std::time::SystemTime;

use crc::{Crc, CRC_32_ISCSI};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Item {
    pub crc: u32,
    timestamp: u128,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Item {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> anyhow::Result<Self> {
        let value = if let Some(value) = value.map(|v| serde_json::to_vec(&v)) {
            Some(value?)
        } else {
            None
        };
        let mut item = Self {
            crc: 0,
            timestamp: now(),
            key,
            value,
        };
        item.crc = item.calculate_crc();
        Ok(item)
    }

    pub fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(&self.key);
        digest.update(self.value.as_ref().unwrap_or(&vec![]));
        digest.finalize()
    }
}

pub fn now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
