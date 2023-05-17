use std::{
    io::{IoSlice, Write},
    time::Duration,
};

use am::{Actor, AnonymousRef, Ctx, Handler};
use tokio::time::Instant;

use crate::disk::Disk;

use super::messages::{Flush, Insert};

struct FlushTask {
    now: Instant,
    handle: AnonymousRef,
}

pub struct WalActor {
    flush: Option<FlushTask>,
    buffer: Vec<Insert>,
    disk: Disk,
    flush_buffer_sync: Duration,
}

impl WalActor {
    pub fn new(disk: Disk, flush_buffer_sync: Duration) -> Self {
        Self {
            flush: None,
            buffer: Vec::new(),
            disk,
            flush_buffer_sync,
        }
    }
}

impl Actor for WalActor {
    fn mailbox_size() -> usize {
        // accept a lot of messages
        100
    }

    fn scheduler() -> am::Scheduler {
        am::Scheduler::NonBlocking
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
    fn handle(&mut self, message: Insert, ctx: &mut am::Ctx<Self>) {
        self.buffer.push(message);

        // If no flush task is currently in the queue,
        if self.flush.is_none() {
            let now = Instant::now();
            let address = ctx.address();
            let duration = self.flush_buffer_sync;
            let handle = ctx.anonymous_task(async move {
                println!("Scheduling flush {:?}", duration);
                let _ = address.schedule(duration).await.send_async(Flush).await;
                println!("Lets Flush");
            });
            self.flush = Some(FlushTask { now, handle });
        }
    }
}

impl Handler<Flush> for WalActor {
    fn handle(&mut self, _: Flush, context: &mut am::Ctx<Self>) {
        assert!(self.flush.is_some());
        let flush = self.flush.take().unwrap();
        let now = Instant::now();
        println!(
            "Writing {} records after {} milliseconds",
            self.buffer.len(),
            (now - flush.now).as_millis()
        );

        // serialize all objects
        let mut vectored = vec![];
        let mut notifiers = vec![];
        for write in self.buffer.drain(..) {
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
