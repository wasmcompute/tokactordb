use std::{
    io::{IoSlice, Write},
    time::Duration,
};

use anyhow::Error;
use tokactor::{Actor, AnonymousRef, Ask, Ctx, Handler};
use tokio::{sync::oneshot, time::Instant};

use crate::actors::{fs::DbFile, wal::messages::WalRestoredItems};

use super::messages::{Flush, Insert, WalRestore};

struct FlushTask {
    now: Instant,
    _handle: AnonymousRef,
}

pub struct WalActor {
    flush: Option<FlushTask>,
    buffer: Vec<Insert>,
    disk: DbFile,
    flush_buffer_sync: Duration,
}

impl WalActor {}

impl WalActor {
    pub fn new(disk: DbFile, flush_buffer_sync: Duration) -> Self {
        Self {
            flush: None,
            buffer: Vec::new(),
            disk,
            flush_buffer_sync,
        }
    }

    fn flush(&mut self, task: FlushTask) -> (bool, Vec<oneshot::Sender<Result<(), Error>>>) {
        let now = Instant::now();
        println!(
            "Writing {} records after {} milliseconds",
            self.buffer.len(),
            (now - task.now).as_millis()
        );

        // serialize all objects
        let mut vectored = vec![];
        let mut notifiers = vec![];
        for write in self.buffer.drain(..) {
            println!("{}", write.item);
            vectored.push(bincode::serialize(&write.item).unwrap());
            notifiers.push(write.tx);
        }

        let vec = vectored.iter().map(|v| IoSlice::new(v)).collect::<Vec<_>>();

        let mut is_error = false;

        if let Err(err) = self.disk.write_vectored(&vec) {
            is_error = true;
            println!("{err}");
            println!("Failed to write buffer to wal disk");
        }
        if let Err(err) = self.disk.flush() {
            is_error = true;
            println!("{err}");
            println!("Failed to flush wal disk");
        }
        (is_error, notifiers)
    }
}

impl Actor for WalActor {
    fn mailbox_size() -> usize {
        // accept a lot of messages
        100
    }

    fn on_stopping(&mut self, ctx: &mut Ctx<Self>) {
        let me = ctx.address();
        ctx.anonymous_task(async move {
            if (me.send_async(Flush {}).await).is_err() {
                println!("Failed to flush when trying to shut down");
            }
        });
    }
}

impl Handler<Insert> for WalActor {
    fn handle(&mut self, message: Insert, ctx: &mut tokactor::Ctx<Self>) {
        println!("Pushed Message...");
        self.buffer.push(message);

        // If no flush task is currently in the queue,
        if self.flush.is_none() {
            let now = Instant::now();
            let address = ctx.address();
            let duration = self.flush_buffer_sync;
            let _handle = ctx.anonymous_task(async move {
                let _ = address.schedule(duration).await.send_async(Flush).await;
            });
            self.flush = Some(FlushTask { now, _handle });
        }
    }
}

impl Handler<Flush> for WalActor {
    fn handle(&mut self, _: Flush, context: &mut tokactor::Ctx<Self>) {
        assert!(self.flush.is_some());
        let flush = self.flush.take().unwrap();
        let (is_error, notifiers) = self.flush(flush);
        context.anonymous_task(async move {
            for notifier in notifiers {
                let result = if is_error {
                    Err(anyhow::Error::msg("Failed to flush buffer to wal disk"))
                } else {
                    Ok(())
                };
                let _ = notifier.send(result);
            }
        });
    }
}

impl Ask<WalRestore> for WalActor {
    type Result = WalRestoredItems;

    fn handle(&mut self, msg: WalRestore, _: &mut Ctx<Self>) -> Self::Result {
        assert!(self.buffer.is_empty());
        assert!(self.flush.is_none());

        let path = msg.path;
        assert!(path.is_file());

        // TODO(Alec): ooohhh aren't you naugthy, doing a blocking operation on
        //             an async thread. LOL who cares for now :P
        // self.disk = DbFile::restore(path).unwrap();
        // if self.disk.is_empty() {
        return WalRestoredItems::new(vec![]);
        // }
        // let mut reader = BufReader::new(self.disk.as_reader());
        // reader.fill_buf().unwrap();

        // let mut items: Vec<Item> = Vec::new();
        // while !reader.buffer().is_empty() {
        //     if let Ok(item) = bincode::deserialize_from(&mut reader) {
        //         items.push(item);
        //     } else {
        //         panic!("Failed to read a record from the WAL log")
        //     }
        // }
        // let (valids, invalids): (Vec<Item>, Vec<Item>) =
        //     items.into_iter().partition(|i| i.is_valid());

        // if !invalids.is_empty() {
        //     println!("Found invalid records in the WAL");
        //     for invalid in invalids {
        //         println!("INVALID: {:?}", invalid);
        //     }
        // }

        // WalRestoredItems::new(valids)
    }
}
