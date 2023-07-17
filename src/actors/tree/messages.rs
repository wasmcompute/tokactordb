use std::{any::Any, marker::PhantomData};

use serde::{de::DeserializeOwned, Serialize};

use crate::AutoIncrement;

use super::GenericTree;

pub trait PrimaryKey:
    Serialize
    + DeserializeOwned
    + AutoIncrement
    + Any
    + std::fmt::Debug
    + Default
    + Send
    + Sync
    + 'static
{
}

pub trait RecordValue:
    Serialize + DeserializeOwned + Any + Default + Send + Sync + std::fmt::Debug + 'static
{
}

impl<T> RecordValue for T where
    T: Serialize + DeserializeOwned + Any + Default + Send + Sync + std::fmt::Debug + 'static
{
}

#[derive(Debug)]
pub struct InsertRecord<Key: PrimaryKey> {
    _key: PhantomData<Key>,
    pub value: Vec<u8>,
}

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

#[derive(Debug)]
pub struct GetRecord<Key: PrimaryKey, Value: RecordValue> {
    pub key: Vec<u8>,
    pub _key: PhantomData<Key>,
    pub _value: PhantomData<Value>,
}

impl<Key: PrimaryKey, Value: RecordValue> GetRecord<Key, Value> {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            _key: PhantomData,
            _value: PhantomData,
        }
    }
}

pub struct GetRecordResult<Value: RecordValue> {
    pub value: Option<Value>,
}

impl<Value: RecordValue> GetRecordResult<Value> {
    pub fn new(value: Option<Value>) -> Self {
        Self { value }
    }
}

#[derive(Debug)]
pub enum ListEnd {
    Head,
    Tail,
}

#[derive(Debug)]
pub struct ListEndResult {
    pub option: Option<Record>,
}

impl ListEndResult {
    pub fn none() -> Self {
        Self { option: None }
    }
    pub fn key(key: Vec<u8>) -> Self {
        Self {
            option: Some(Record { key, value: None }),
        }
    }
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            option: Some(Record {
                key,
                value: Some(value),
            }),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Record {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }
}
impl Record {}

#[derive(Debug)]
pub struct GetMemTableSnapshot;

pub struct Snapshot {
    pub list: Vec<Record>,
}

#[derive(Debug)]
pub struct AddGenericTree {
    pub inner: GenericTree,
}

#[derive(Debug)]
pub struct GetUniqueKey<Key: PrimaryKey>(PhantomData<Key>);
impl<Key: PrimaryKey> Default for GetUniqueKey<Key> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[derive(Debug)]
pub struct UniqueKey<Key: PrimaryKey>(pub Key);
