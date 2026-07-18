//! Async event writer — batches storage events from executor threads via a channel.
//!
//! `EventWriter` sends `EventRecord`s / `LogRecord`s over an unbounded mpsc
//! channel to a background Tokio task. The task accumulates them and flushes
//! to SurrealDB in batches (by count or timer), decoupling storage write
//! latency from step execution throughput. The channel is unbounded so events
//! arent lossed and we dont block the thread; if storage falls behind, depth grows and a warning is logged once it crosses the
//! high-water mark.
use std::sync::Arc;
use std::time::Duration;

use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{EventRecord, LogRecord, ScopedStorageHandle, StorageBackend};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::runtime::io_rt;

const BATCH_SIZE: usize = 64;
const FLUSH_INTERVAL_MS: u64 = 200;
/// Queue depth at which we warn that storage is falling behind. Hysteresis is
/// applied at half this value to avoid log spam around the boundary.
const HIGH_WATER_MARK: usize = 4 * BATCH_SIZE;

/// One message on the writer channel: a structured event or a step's log row.
pub(crate) enum WriterMsg {
    Event(EventRecord),
    Log(LogRecord),
}

impl From<EventRecord> for WriterMsg {
    fn from(e: EventRecord) -> Self {
        Self::Event(e)
    }
}

impl From<LogRecord> for WriterMsg {
    fn from(l: LogRecord) -> Self {
        Self::Log(l)
    }
}

/// Background event writer that decouples storage writes from step execution.
///
/// Messages are sent to a background tokio task via an unbounded mpsc channel.
/// The task accumulates events and log rows and flushes each in batches,
/// either when a batch reaches `BATCH_SIZE` or on a `FLUSH_INTERVAL_MS` timer.
/// The channel is unbounded to guarantee delivery — losing events is
/// unacceptable, and a brief storage stall should not stall the executor.
/// Sustained backlog above `HIGH_WATER_MARK` triggers a `warn!` so operators
/// can see when storage is the bottleneck.
pub(crate) struct EventWriter {
    sender: mpsc::UnboundedSender<WriterMsg>,
    handle: JoinHandle<()>,
    /// Owning code-location identity; stamped onto every event passed through
    /// `emit()`/`emit_log()`. Senders obtained via `sender()` bypass this and
    /// must stamp the field themselves.
    code_location_id: String,
}

impl EventWriter {
    pub(crate) fn new(storage: ScopedStorageHandle<SurrealStorage>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let code_location_id = storage.code_location_id().to_string();
        let backend = Arc::clone(storage.backend());
        let handle = io_rt().spawn(async move {
            batch_writer_loop(rx, &backend).await;
        });
        Self {
            sender: tx,
            handle,
            code_location_id,
        }
    }

    /// Stamps `code_location_id` from the writer onto the event, then enqueues
    /// it. Send is non-blocking and the channel is unbounded, so this only
    /// fails when the receiver has been dropped (writer is shutting down) — in
    /// which case dropping the event is the correct behavior.
    #[inline]
    pub(crate) fn emit(&self, mut event: EventRecord) {
        event.code_location_id = self.code_location_id.clone();
        let _ = self.sender.send(WriterMsg::Event(event));
    }

    /// [`Self::emit`] for a step's captured log output.
    #[inline]
    pub(crate) fn emit_log(&self, mut log: LogRecord) {
        log.code_location_id = self.code_location_id.clone();
        let _ = self.sender.send(WriterMsg::Log(log));
    }

    /// Clone the underlying sender for passing to subsystems (e.g. PoolGuard).
    /// Messages sent through the returned sender bypass `emit`'s
    /// `code_location_id` stamping and must set the field themselves.
    pub(crate) fn sender(&self) -> mpsc::UnboundedSender<WriterMsg> {
        self.sender.clone()
    }

    pub(crate) fn flush(self) {
        drop(self.sender);
        if let Err(e) = io_rt().block_on(self.handle) {
            warn!("event writer task panicked: {e:?}");
        }
    }
}

async fn flush_batch(storage: &SurrealStorage, batch: &mut Vec<EventRecord>) {
    if batch.is_empty() {
        return;
    }
    if let Err(e) = storage.store_events(batch).await {
        warn!("failed to flush event batch: {e:?}");
    }
    batch.clear();
}

async fn flush_logs(storage: &SurrealStorage, batch: &mut Vec<LogRecord>) {
    if batch.is_empty() {
        return;
    }
    if let Err(e) = storage.store_run_logs(batch).await {
        warn!("failed to flush log batch: {e:?}");
    }
    batch.clear();
}

async fn batch_writer_loop(mut rx: mpsc::UnboundedReceiver<WriterMsg>, storage: &SurrealStorage) {
    let mut events = Vec::with_capacity(BATCH_SIZE);
    let mut logs: Vec<LogRecord> = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick fires immediately; consume it so the loop ticks at the proper cadence.
    interval.tick().await;
    let mut above_high_water = false;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(WriterMsg::Event(event)) => {
                        events.push(event);
                        if events.len() >= BATCH_SIZE {
                            flush_batch(storage, &mut events).await;
                        }
                    }
                    Some(WriterMsg::Log(log)) => {
                        logs.push(log);
                        if logs.len() >= BATCH_SIZE {
                            flush_logs(storage, &mut logs).await;
                        }
                    }
                    None => {
                        flush_batch(storage, &mut events).await;
                        flush_logs(storage, &mut logs).await;
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                flush_batch(storage, &mut events).await;
                flush_logs(storage, &mut logs).await;
                let depth = rx.len();
                if depth >= HIGH_WATER_MARK && !above_high_water {
                    warn!(
                        depth,
                        "event writer queue depth high — storage may be falling behind"
                    );
                    above_high_water = true;
                } else if depth < HIGH_WATER_MARK / 2 && above_high_water {
                    above_high_water = false;
                }
            }
        }
    }
}
