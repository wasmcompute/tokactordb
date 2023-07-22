use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use tokactor::ActorRef;

use crate::actors::{tree::TreeActor, wal::Item};

use super::builder::TreeVersion;

#[derive(Debug)]
pub struct RequestWal();

#[derive(Debug)]
pub struct NewTreeRoot {
    pub name: String,
    pub versions: Vec<TreeVersion>,
}

impl NewTreeRoot {
    pub fn new(name: String, versions: Vec<TreeVersion>) -> Self {
        Self { name, versions }
    }
}

#[derive(Debug)]
pub struct TreeRoot {
    pub inner: ActorRef<TreeActor>,
}

impl TreeRoot {
    pub fn new(inner: ActorRef<TreeActor>) -> Self {
        Self { inner }
    }
}

#[derive(Debug)]
pub struct Restore {
    pub directory: PathBuf,
}

impl Restore {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }
}

#[derive(Debug)]
pub struct RestoreItem(pub Item);
impl Deref for RestoreItem {
    type Target = Item;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RestoreItem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct RestoreComplete;
