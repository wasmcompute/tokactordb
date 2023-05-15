use std::marker::PhantomData;

use am::Message;
use serde::{de::DeserializeOwned, Serialize};

use crate::{actors::wal::Rx, AutoIncrement};

pub trait PrimaryKey:
    Sync + Send + Serialize + DeserializeOwned + AutoIncrement + std::fmt::Debug + 'static
{
}

#[derive(Debug)]
pub struct InsertRecord<Key: PrimaryKey> {
    _key: PhantomData<fn() -> Key>,
    value: Vec<u8>,
}

impl<Key: PrimaryKey> Message for InsertRecord<Key> {}

impl<Key: PrimaryKey> InsertRecord<Key> {
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            _key: PhantomData,
            value,
        }
    }
}

pub struct InsertSuccess<Key: PrimaryKey> {
    key: Key,
    rx: Rx,
}

impl<Key: PrimaryKey> InsertSuccess<Key> {
    pub fn new(key: Key, rx: Rx) -> Self {
        Self { key, rx }
    }
}

impl<Key: PrimaryKey> Message for InsertSuccess<Key> {}
