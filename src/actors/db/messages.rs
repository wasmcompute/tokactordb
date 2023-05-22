use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use am::{ActorRef, Message};

use crate::actors::{tree::TreeActor, wal::Item};

#[derive(Debug)]
pub struct NewTreeRoot {
    pub name: String,
}

impl NewTreeRoot {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
impl Message for NewTreeRoot {}

#[derive(Debug)]
pub struct TreeRoot {
    pub inner: ActorRef<TreeActor>,
}
impl Message for TreeRoot {}

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
impl Message for Restore {}

#[derive(Debug)]
pub struct RestoreItem(pub Item);
impl Message for RestoreItem {}
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
impl Message for RestoreComplete {}
