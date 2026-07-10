//! Batched storage writer — drains an unbounded mpsc channel into an
//! in-memory batch, flushing to the storage backend on a periodic timer or
//! whenever `ingest()` reports the batch is full. The two daemon writers (tick
//! records, per-asset condition evaluation records) plug in via the
//! `BatchWriter` trait; the loop logic + cancel-time drain live here once.
//!
//! **Channel is unbounded** so that producers (`process_tick_result`,
//! `condition_eval_loop`) never silently drop events under load — the
//! daemon's correctness depends on every tick reaching storage. Memory
//! pressure is bounded on the writer side by the per-impl `max_batch`, which
//! triggers an immediate flush when in-memory batch size exceeds the limit
//! rather than waiting for the timer.
use std::collections::HashMap;
use std::time::Duration;

use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{ConditionEvalRecord, ScopedStorageHandle, StorageBackend, TickRecord};
use tokio_util::sync::CancellationToken;

use super::types::{ConditionEvalWriteMsg, TickWriteMsg};

/// A type-specific batch writer. Implementations own their own batch + prune
/// state; the loop driver only coordinates the timer / cancel / receive.
pub(crate) trait BatchWriter: Send + 'static {
    type Msg: Send + 'static;

    /// Periodic flush cadence — even with no `ingest`-triggered flush, the
    /// loop calls `flush()` every `flush_interval`.
    fn flush_interval(&self) -> Duration;

    /// Push `msg` into local batch state. Returns `true` if the batch is now
    /// full and should be flushed immediately, `false` to keep accumulating.
    fn ingest(&mut self, msg: Self::Msg) -> bool;

    /// Drain the local batch + prune side-effects to the storage backend.
    fn flush(&mut self) -> impl Future<Output = ()> + Send;
}

/// Spawn a tokio task running the generic batch loop driving `writer`.
/// Returns the producer end of an unbounded mpsc channel and the
/// background task's `JoinHandle` so [`crate::daemon::daemon_main_loop`]
/// can await every subdaemon at shutdown — without that, `daemon.stop()`
/// returns while subdaemons are still draining and their leftover work
/// contaminates the next test on the shared tokio runtime.
pub(crate) fn spawn_batch_writer<W: BatchWriter>(
    mut writer: W,
    cancel: CancellationToken,
) -> (
    tokio::sync::mpsc::UnboundedSender<W::Msg>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<W::Msg>();
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(writer.flush_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    rx.close();
                    while let Some(msg) = rx.recv().await {
                        writer.ingest(msg);
                    }
                    writer.flush().await;
                    return;
                }
                _ = interval.tick() => {}
                msg = rx.recv() => {
                    let Some(msg) = msg else { return; };
                    let full = writer.ingest(msg);
                    if !full { continue; }
                }
            }
            writer.flush().await;
        }
    });
    (tx, handle)
}

/// Buffers `TickRecord`s and per-automation prune requests, flushing every
/// 500ms or when the batch hits `max_batch` (default 256, or 32 for memory
/// storage; overridable via `RIVERS_TICK_BATCH_SIZE`).
pub(crate) struct TickWriter {
    handle: ScopedStorageHandle<SurrealStorage>,
    flush_interval: Duration,
    max_batch: usize,
    batch: Vec<TickRecord>,
    prune_names: HashMap<String, usize>,
}

impl TickWriter {
    fn new(handle: ScopedStorageHandle<SurrealStorage>, is_memory_storage: bool) -> Self {
        let default_batch: usize = if is_memory_storage { 32 } else { 256 };
        let max_batch: usize = std::env::var("RIVERS_TICK_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_batch);
        Self {
            handle,
            flush_interval: Duration::from_millis(500),
            max_batch,
            batch: Vec::with_capacity(max_batch),
            prune_names: HashMap::new(),
        }
    }
}

impl BatchWriter for TickWriter {
    type Msg = TickWriteMsg;

    fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    fn ingest(&mut self, msg: TickWriteMsg) -> bool {
        if let Some(max) = msg.max_ticks_retained {
            self.prune_names
                .insert(msg.record.automation_name.clone(), max);
        }
        self.batch.push(msg.record);
        self.batch.len() >= self.max_batch
    }

    async fn flush(&mut self) {
        if !self.batch.is_empty() {
            match self.handle.backend().store_ticks_batch(&self.batch).await {
                Ok(_) => self.batch.clear(),
                Err(e) => {
                    // Keep the batch for the next flush — sensor cursors are
                    // restored from stored ticks, so silent loss regresses them.
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        pending = self.batch.len(),
                        "failed to store tick batch; retrying next flush"
                    );
                    trim_backlog(&mut self.batch, self.max_batch, "tick");
                    return;
                }
            }
        }
        for (name, max) in self.prune_names.drain() {
            if let Err(e) = self.handle.scoped().prune_ticks(&name, max).await {
                tracing::warn!(
                    target: "rivers::daemon",
                    automation = %name,
                    error = %e,
                    "failed to prune ticks"
                );
            }
        }
    }
}

/// Bound a failed-flush backlog to 8 full batches, dropping the oldest.
fn trim_backlog<T>(batch: &mut Vec<T>, max_batch: usize, what: &str) {
    let cap = max_batch.saturating_mul(8).max(1);
    if batch.len() > cap {
        let dropped = batch.len() - cap;
        batch.drain(..dropped);
        tracing::error!(
            target: "rivers::daemon",
            dropped,
            "{what} backlog exceeded retry cap; dropping oldest records"
        );
    }
}

pub(crate) fn spawn_tick_writer(
    handle: ScopedStorageHandle<SurrealStorage>,
    cancel: CancellationToken,
    is_memory_storage: bool,
) -> (
    tokio::sync::mpsc::UnboundedSender<TickWriteMsg>,
    tokio::task::JoinHandle<()>,
) {
    spawn_batch_writer(TickWriter::new(handle, is_memory_storage), cancel)
}

/// Buffers per-asset `ConditionEvalRecord`s and per-key prune requests,
/// flushing every 2s or when the batch hits `max_batch` (default 256;
/// overridable via `RIVERS_CONDITION_EVAL_BATCH_SIZE`). Each flush also
/// prunes the global `condition_ticks` table when `max_evals_retained` is
/// `Some`.
pub(crate) struct ConditionEvalWriter {
    handle: ScopedStorageHandle<SurrealStorage>,
    flush_interval: Duration,
    max_batch: usize,
    max_evals_retained: Option<usize>,
    batch: Vec<ConditionEvalRecord>,
    prune_keys: HashMap<String, usize>,
}

impl ConditionEvalWriter {
    fn new(handle: ScopedStorageHandle<SurrealStorage>, max_evals_retained: Option<usize>) -> Self {
        let max_batch: usize = std::env::var("RIVERS_CONDITION_EVAL_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        Self {
            handle,
            flush_interval: Duration::from_secs(2),
            max_batch,
            max_evals_retained,
            batch: Vec::with_capacity(max_batch),
            prune_keys: HashMap::new(),
        }
    }
}

impl BatchWriter for ConditionEvalWriter {
    type Msg = ConditionEvalWriteMsg;

    fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    fn ingest(&mut self, msg: ConditionEvalWriteMsg) -> bool {
        if let Some(max) = msg.max_evals_retained {
            for e in &msg.evals {
                self.prune_keys.insert(e.asset_key.clone(), max);
            }
        }
        self.batch.extend(msg.evals);
        self.batch.len() >= self.max_batch
    }

    async fn flush(&mut self) {
        let mut flushed_evals = false;
        if !self.batch.is_empty() {
            match self
                .handle
                .backend()
                .store_condition_evals_batch(&self.batch)
                .await
            {
                Ok(_) => {
                    self.batch.clear();
                    flushed_evals = true;
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        pending = self.batch.len(),
                        "failed to store condition eval batch; retrying next flush"
                    );
                    trim_backlog(&mut self.batch, self.max_batch, "condition eval");
                    return;
                }
            }
        }
        for (key, max) in self.prune_keys.drain() {
            if let Err(e) = self.handle.scoped().prune_condition_evals(&key, max).await {
                tracing::warn!(
                    target: "rivers::daemon",
                    asset = %key,
                    error = %e,
                    "failed to prune condition evals"
                );
            }
        }
        // Ticks only grow when evals do — an idle daemon must not run the
        // prune query every 2s forever.
        if flushed_evals && let Some(max) = self.max_evals_retained {
            if let Err(e) = self.handle.scoped().prune_condition_ticks(max).await {
                tracing::warn!(
                    target: "rivers::daemon",
                    error = %e,
                    "failed to prune condition ticks"
                );
            }
        }
    }
}

pub(crate) fn spawn_condition_eval_writer(
    handle: ScopedStorageHandle<SurrealStorage>,
    cancel: CancellationToken,
    max_evals_retained: Option<usize>,
) -> (
    tokio::sync::mpsc::UnboundedSender<ConditionEvalWriteMsg>,
    tokio::task::JoinHandle<()>,
) {
    spawn_batch_writer(ConditionEvalWriter::new(handle, max_evals_retained), cancel)
}
