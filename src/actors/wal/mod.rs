mod actor;
mod item;
mod messages;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use am::{Actor, ActorRef, Ctx, DeadActorResult, Handler, Message};
use tokio::sync::oneshot;

use self::messages::{DumpWal, WalRestore};
use crate::disk::Disk;

pub use self::item::Item;
pub use actor::WalActor;
pub use messages::{Insert, Rx, WalRestoredItems};

#[derive(Clone)]
pub struct Wal {
    inner: ActorRef<WalActor>,
}

impl Message for Wal {}

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
    pub async fn write(&self, table: String, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let item = Item::new(table, key, Some(value))?;
        let insert = Insert::new(tx, item);

        if (self.inner.send_async(insert).await).is_err() {
            anyhow::bail!("Failed to write message to database")
        }
        println!("I have written to the log");
        if (rx.await).is_err() {
            anyhow::bail!("Database accepted write but the response failed to be recieved. Write may not have succeeded");
        } else {
            Ok(())
        }
    }

    pub async fn restore(&self, wals: PathBuf) -> WalRestoredItems {
        self.inner.ask(WalRestore { path: wals }).await.unwrap()
    }

    pub async fn dump(&self, wal: impl AsRef<Path>) -> anyhow::Result<()> {
        self.inner
            .ask(DumpWal(wal.as_ref().to_path_buf()))
            .await
            .unwrap();
        Ok(())
    }
}
