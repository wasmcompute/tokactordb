use am::{Actor, Ask};
use tokio::sync::oneshot;

use crate::actors::wal::Wal;

use super::{InsertRecord, InsertSuccess, PrimaryKey};

pub struct TreeActor {
    wal: Wal,
}

impl TreeActor {
    pub fn new(wal: Wal) -> Self {
        Self { wal }
    }
}

impl Actor for TreeActor {}

impl<Key: PrimaryKey> Ask<InsertRecord<Key>> for TreeActor {
    type Result = InsertSuccess<Key>;

    fn handle(&mut self, message: InsertRecord<Key>, context: &mut am::Ctx<Self>) -> Self::Result {
        let (tx, rx) = oneshot::channel();
    }
}
