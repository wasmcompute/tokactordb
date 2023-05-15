mod actor;
mod item;
mod messages;

use std::time::Duration;

use am::{Actor, ActorRef, Ctx, DeadActorResult, Handler};
use tokio::sync::oneshot;

use self::item::Item;
use crate::disk::Disk;

pub use actor::WalActor;
pub use messages::{Insert, Rx};

#[derive(Clone)]
pub struct Wal {
    inner: ActorRef<WalActor>,
}

pub fn new_wal_actor<A>(ctx: &mut Ctx<A>, flush_buffer_sync: Duration) -> Wal
where
    A: Actor + Handler<DeadActorResult<WalActor>>,
{
    let disk = Disk::new();
    let wal = WalActor::new(disk, flush_buffer_sync);
    let address = ctx.spawn(wal);
    Wal { inner: address }
}

impl Wal {
    pub fn write(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<Rx> {
        let (tx, rx) = oneshot::channel();
        let item = Item::new(key, Some(value))?;
        let insert = Insert::new(tx, item);
        let reciver = Rx::new(rx);

        let _ = self.inner.send(insert);

        reciver
    }
}
