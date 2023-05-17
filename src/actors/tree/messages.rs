use std::marker::PhantomData;

use am::Message;
use serde::{de::DeserializeOwned, Serialize};

use crate::AutoIncrement;

pub trait PrimaryKey:
    Serialize + DeserializeOwned + AutoIncrement + std::fmt::Debug + Default + Send + Sync + 'static
{
}

pub trait RecordValue: Serialize + DeserializeOwned + std::fmt::Debug {}

impl<T> RecordValue for T where T: Serialize + DeserializeOwned + std::fmt::Debug {}

#[derive(Debug)]
pub struct InsertRecord<Key: PrimaryKey> {
    _key: PhantomData<Key>,
    pub value: Vec<u8>,
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
    pub key: Key,
}

impl<Key: PrimaryKey> InsertSuccess<Key> {
    pub fn new(key: Key) -> Self {
        Self { key }
    }
}

impl<Key: PrimaryKey> Message for InsertSuccess<Key> {}

#[derive(Debug)]
pub struct UpdateRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl UpdateRecord {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}
impl Message for UpdateRecord {}

#[derive(Debug)]
pub struct GetRecord {
    pub key: Vec<u8>,
}

impl GetRecord {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}
impl Message for GetRecord {}

pub struct GetRecordResult {
    pub value: Option<Vec<u8>>,
}

impl GetRecordResult {
    pub fn new(value: Option<Vec<u8>>) -> Self {
        Self { value }
    }
}
impl Message for GetRecordResult {}

#[derive(Debug)]
pub enum ListEnd {
    Head,
    Tail,
}
impl Message for ListEnd {}

#[derive(Debug)]
pub struct ListEndResult {
    pub option: Option<Record>,
}
impl Message for ListEndResult {}

#[derive(Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}
impl Message for Record {}

#[derive(Debug)]
pub struct GetMemTableSnapshot;
impl Message for GetMemTableSnapshot {}

pub struct Snapshot {
    pub list: Vec<Record>,
}
impl Message for Snapshot {}
