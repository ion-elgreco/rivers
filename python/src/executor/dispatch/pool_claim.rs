//! Pool-aware step execution: claim concurrency slots before execution, release after.
//!
//! `PoolGuard` is the primary abstraction — it encapsulates claim, lease renewal, and
//! release into a single RAII-style type. Blocking and async variants are provided.
//!
//! Emits concurrency observability events (StepSlotClaimed, StepSlotWaiting, StepSlotRenewed,
//! StepSlotReleased) through the event channel.
use std::sync::LazyLock;
use std::time::Duration;

use pyo3::prelude::*;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    ConcurrencyClaimStatus, DEFAULT_LEASE_DURATION_SECS, EventRecord, EventType,
    ScopedStorageHandle, StorageBackend,
};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::executor::event_writer::WriterMsg;
use crate::executor::ops::now_ts;
use crate::runtime::io_rt;

const DEFAULT_CLAIM_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CLAIM_POLL_JITTER: Duration = Duration::from_millis(500);
const DEFAULT_CLAIM_WARN_THRESHOLD: u32 = 30;
/// ≈10 minutes at ~1s intervals.
const DEFAULT_CLAIM_TIMEOUT: Duration = Duration::from_secs(600);

fn parse_duration_env(var: &str, default: Duration) -> Duration {
    std::env::var(var)
        .ok()
        .and_then(|v| humantime::parse_duration(&v).ok())
        .unwrap_or(default)
}

static CLAIM_POLL_INTERVAL: LazyLock<Duration> =
    LazyLock::new(|| parse_duration_env("RIVERS_CLAIM_POLL_INTERVAL", DEFAULT_CLAIM_POLL_INTERVAL));
static CLAIM_POLL_JITTER: LazyLock<Duration> =
    LazyLock::new(|| parse_duration_env("RIVERS_CLAIM_POLL_JITTER", DEFAULT_CLAIM_POLL_JITTER));
static CLAIM_TIMEOUT: LazyLock<Duration> =
    LazyLock::new(|| parse_duration_env("RIVERS_CLAIM_TIMEOUT", DEFAULT_CLAIM_TIMEOUT));

/// Holds claimed concurrency slots and a background lease renewal task.
/// If dropped without calling `release()`, slots are reclaimed by lease expiry
/// and the renewal task is aborted via `AbortOnDrop` — without this, the task
/// would keep running and hold a sender clone, blocking `EventWriter::flush`.
pub(crate) struct PoolGuard {
    storage: ScopedStorageHandle<SurrealStorage>,
    run_id: String,
    step_key: String,
    renewal: AbortOnDrop,
    events: mpsc::UnboundedSender<WriterMsg>,
}

/// Aborts the wrapped tokio task on drop. `AbortHandle::drop` alone does not
/// abort — without this, a `PoolGuard` dropped through any path other than
/// `release()` / `release_blocking()` would leak the lease-renewal task.
struct AbortOnDrop(AbortHandle);

impl AbortOnDrop {
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl PoolGuard {
    /// Async acquire: claim slots and start background lease renewal.
    pub async fn acquire(
        storage: &ScopedStorageHandle<SurrealStorage>,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        events: mpsc::UnboundedSender<WriterMsg>,
    ) -> anyhow::Result<Self> {
        claim_async_poll(storage, pools, run_id, step_key, &events).await?;
        let renewal = AbortOnDrop(spawn_lease_renewal(
            storage.clone(),
            run_id.to_string(),
            step_key.to_string(),
            events.clone(),
        ));
        Ok(Self {
            storage: storage.clone(),
            run_id: run_id.to_string(),
            step_key: step_key.to_string(),
            renewal,
            events,
        })
    }

    /// Blocking acquire: claim slots (GIL released) and start background lease renewal.
    pub fn acquire_blocking(
        py: Python,
        storage: &ScopedStorageHandle<SurrealStorage>,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        events: mpsc::UnboundedSender<WriterMsg>,
    ) -> anyhow::Result<Self> {
        let storage_c = storage.clone();
        let pools = pools.to_vec();
        let run_id_s = run_id.to_string();
        let step_key_s = step_key.to_string();
        let events_c = events.clone();
        py.detach(move || {
            io_rt().block_on(claim_async_poll(
                &storage_c,
                &pools,
                &run_id_s,
                &step_key_s,
                &events_c,
            ))
        })?;
        let renewal = AbortOnDrop(spawn_lease_renewal(
            storage.clone(),
            run_id.to_string(),
            step_key.to_string(),
            events.clone(),
        ));
        Ok(Self {
            storage: storage.clone(),
            run_id: run_id.to_string(),
            step_key: step_key.to_string(),
            renewal,
            events,
        })
    }

    /// Release slots and stop lease renewal (async).
    pub async fn release(self) {
        self.renewal.abort();
        if let Err(e) = self
            .storage
            .backend()
            .free_concurrency_slots(&self.run_id, &self.step_key)
            .await
        {
            tracing::warn!(step = %self.step_key, error = %e, "failed to release concurrency slots");
        }
        emit_event(
            &self.events,
            self.storage.code_location_id(),
            &self.run_id,
            &self.step_key,
            EventType::StepSlotReleased,
            vec![],
        );
    }

    /// Release slots and stop lease renewal (blocking, GIL released).
    pub fn release_blocking(self, py: Python) {
        self.renewal.abort();
        let storage = self.storage;
        let run_id = self.run_id;
        let step_key = self.step_key;
        let events = self.events;
        py.detach(move || {
            if let Err(e) =
                io_rt().block_on(storage.backend().free_concurrency_slots(&run_id, &step_key))
            {
                tracing::warn!(step = %step_key, error = %e, "failed to release concurrency slots");
            }
            emit_event(
                &events,
                storage.code_location_id(),
                &run_id,
                &step_key,
                EventType::StepSlotReleased,
                vec![],
            );
        });
    }
}

async fn claim_async_poll(
    storage: &ScopedStorageHandle<SurrealStorage>,
    pools: &[(String, u32)],
    run_id: &str,
    step_key: &str,
    events: &mpsc::UnboundedSender<WriterMsg>,
) -> anyhow::Result<()> {
    let poll_interval = *CLAIM_POLL_INTERVAL;
    let max_jitter = *CLAIM_POLL_JITTER;
    let timeout = *CLAIM_TIMEOUT;
    let deadline = tokio::time::Instant::now() + timeout;
    let code_location_id = storage.code_location_id();

    let mut attempt: u32 = 0;
    loop {
        match storage
            .scoped()
            .claim_concurrency_slots(pools, run_id, step_key, 0, DEFAULT_LEASE_DURATION_SECS)
            .await?
        {
            ConcurrencyClaimStatus::Claimed => {
                let pool_names: Vec<String> = pools.iter().map(|(k, _)| k.clone()).collect();
                emit_event(
                    events,
                    code_location_id,
                    run_id,
                    step_key,
                    EventType::StepSlotClaimed,
                    vec![("pools".to_string(), pool_names.join(","))],
                );
                return Ok(());
            }
            ConcurrencyClaimStatus::Pending { position, reason } => {
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "step '{step_key}' timed out waiting for pool slots after {timeout:?}"
                    );
                }
                // Emit waiting event on first pending attempt only
                if attempt == 0 {
                    emit_event(
                        events,
                        code_location_id,
                        run_id,
                        step_key,
                        EventType::StepSlotWaiting,
                        vec![("reason".to_string(), reason.to_string())],
                    );
                }
                if attempt >= DEFAULT_CLAIM_WARN_THRESHOLD
                    && attempt.is_multiple_of(DEFAULT_CLAIM_WARN_THRESHOLD)
                {
                    tracing::warn!(
                        step = step_key,
                        position = position,
                        attempt = attempt,
                        reason = %reason,
                        "step still waiting for pool slots"
                    );
                } else {
                    tracing::debug!(
                        step = step_key,
                        position = position,
                        reason = %reason,
                        "step pending for pool slots, will retry"
                    );
                }
                let jitter = rand_jitter(attempt, max_jitter);
                tokio::time::sleep(poll_interval + jitter).await;
                attempt += 1;
            }
        }
    }
}

/// Jitter from thread ID + attempt counter to avoid correlated retries.
fn rand_jitter(attempt: u32, max_jitter: Duration) -> Duration {
    let max_ms = max_jitter.as_millis() as u64;
    if max_ms == 0 {
        return Duration::ZERO;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    attempt.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos()
        .hash(&mut hasher);
    Duration::from_millis(hasher.finish() % max_ms)
}

fn spawn_lease_renewal(
    storage: ScopedStorageHandle<SurrealStorage>,
    run_id: String,
    step_key: String,
    events: mpsc::UnboundedSender<WriterMsg>,
) -> AbortHandle {
    let interval = Duration::from_secs((DEFAULT_LEASE_DURATION_SECS / 3).max(1) as u64);
    io_rt()
        .spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick is immediate — skip it
            loop {
                ticker.tick().await;
                match storage
                    .backend()
                    .renew_slot_lease(&run_id, &step_key, DEFAULT_LEASE_DURATION_SECS)
                    .await
                {
                    Ok(0) => {
                        tracing::warn!(
                            step = %step_key,
                            "lease renewal found 0 slots — lease may have expired"
                        );
                        break;
                    }
                    Ok(n) => {
                        tracing::trace!(step = %step_key, renewed = n, "renewed slot leases");
                        emit_event(
                            &events,
                            storage.code_location_id(),
                            &run_id,
                            &step_key,
                            EventType::StepSlotRenewed,
                            vec![],
                        );
                    }
                    Err(e) => {
                        tracing::warn!(step = %step_key, error = %e, "lease renewal failed");
                    }
                }
            }
        })
        .abort_handle()
}

fn emit_event(
    tx: &mpsc::UnboundedSender<WriterMsg>,
    code_location_id: &str,
    run_id: &str,
    step_key: &str,
    event_type: EventType,
    metadata: Vec<(String, String)>,
) {
    let _ = tx.send(
        EventRecord {
            code_location_id: code_location_id.to_string(),
            event_type,
            asset_key: Some(step_key.to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: now_ts(),
            metadata,
            input_data_versions: vec![],
        }
        .into(),
    );
}
