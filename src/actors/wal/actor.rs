use std::{
    io::{IoSlice, Write},
    time::Duration,
};

use am::{Actor, AnonymousRef, Handler};
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
        am::Scheduler::Blocking
    }
}

impl Handler<Insert> for WalActor {
    fn handle(&mut self, message: Insert, ctx: &mut am::Ctx<Self>) {
        self.buffer.push(message);

        // If no flush task is currently in the queue,
        if self.flush.is_none() {
            let now = Instant::now();
            let address = ctx.address();
            let duration = self.flush_buffer_sync.clone();
            let handle = ctx.anonymous_task(async move {
                let _ = address.schedule(duration).await.send_async(Flush).await;
            });
            self.flush = Some(FlushTask { now, handle });
        }
    }
}

impl Handler<Flush> for WalActor {
    fn handle(&mut self, message: Flush, context: &mut am::Ctx<Self>) {
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

        self.disk.write_vectored(&vec);

        context.anonymous_task(async move {
            for notifier in notifiers {
                let _ = notifier.send(());
            }
        });
    }
}
