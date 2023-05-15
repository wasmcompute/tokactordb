use am::{ActorRef, Message};

use crate::actors::tree::TreeActor;

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
