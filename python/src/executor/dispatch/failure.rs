//! Failure classification for the step retry loop.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyKeyboardInterrupt, PyMemoryError, PyTimeoutError};
use pyo3::prelude::*;
use rivers_core::execution::retry::{FailureReason, RetryPolicy, compute_delay, should_retry};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{EventType, StorageBackend};

use crate::runtime::io_rt;

/// Classify a raised exception and collect its MRO as fully-qualified class
/// names (`module.qualname`, derived-first) for the `retry_on` allow-list.
pub(crate) fn classify_pyerr(py: Python, err: &PyErr) -> (FailureReason, Vec<String>) {
    let ty = err.get_type(py);
    let mut mro_names = Vec::new();
    for item in ty.mro().iter() {
        if let Ok(name) = crate::utils::qualified_type_name(&item) {
            mro_names.push(name);
        }
    }

    // Worker-process deaths (loky kill, broken pool) are environmental, not
    // user errors — matched by MRO name so this crate needs no loky import.
    const INFRA_EXC_TYPES: [&str; 3] = [
        "loky.process_executor.TerminatedWorkerError",
        "concurrent.futures.process.BrokenProcessPool",
        "concurrent.futures._base.BrokenExecutor",
    ];

    let reason = if err.is_instance_of::<PyMemoryError>(py) {
        FailureReason::OutOfMemory
    } else if err.is_instance_of::<PyTimeoutError>(py) {
        FailureReason::Timeout
    } else if err.is_instance_of::<PyKeyboardInterrupt>(py)
        || mro_names
            .iter()
            .any(|n| n == "asyncio.exceptions.CancelledError")
    {
        FailureReason::Cancelled
    } else if mro_names
        .iter()
        .any(|n| INFRA_EXC_TYPES.contains(&n.as_str()))
    {
        FailureReason::Infrastructure
    } else {
        FailureReason::Error
    };
    (reason, mro_names)
}

/// Apply `policy` to a classified failed attempt: `Some(delay)` admits
/// another attempt after `delay` (logged); `None` means give up.
pub(crate) fn admit_retry(
    policy: &RetryPolicy,
    step_name: &str,
    reason: FailureReason,
    exc_types: &[String],
    attempt: u32,
) -> Option<Duration> {
    if !should_retry(policy, reason, exc_types, attempt) {
        return None;
    }
    let delay = compute_delay(policy, attempt, rng01());
    tracing::info!(
        step = step_name,
        attempt,
        reason = reason.as_str(),
        delay_ms = delay.as_millis() as u64,
        "step failed, retrying"
    );
    Some(delay)
}

/// Sleep out a backoff `delay` in 1s slices, polling run cancellation between
/// slices. Returns true if the run was cancelled before the delay elapsed.
pub(crate) async fn backoff_sleep_cancellable(
    storage: &SurrealStorage,
    run_id: &str,
    delay: Duration,
) -> bool {
    const SLICE: Duration = Duration::from_secs(1);
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(SLICE)).await;
        if remaining <= SLICE {
            return false;
        }
        if storage.is_cancelled(run_id).await.unwrap_or(false) {
            return true;
        }
    }
}

/// Blocking flavor for the sync lifecycle (GIL released for the wait).
pub(crate) fn backoff_sleep_cancellable_blocking(
    py: Python,
    storage: &Arc<SurrealStorage>,
    run_id: &str,
    delay: Duration,
) -> bool {
    let storage = Arc::clone(storage);
    let run_id = run_id.to_string();
    py.detach(move || io_rt().block_on(backoff_sleep_cancellable(&storage, &run_id, delay)))
}

/// One-shot cancellation probe for the retry loops — zero-backoff ladders have
/// no sleep to interrupt, so each iteration checks explicitly.
pub(crate) fn run_cancelled_blocking(
    py: Python,
    storage: &Arc<SurrealStorage>,
    run_id: &str,
) -> bool {
    let storage = Arc::clone(storage);
    let run_id = run_id.to_string();
    py.detach(move || {
        io_rt().block_on(async { storage.is_cancelled(&run_id).await.unwrap_or(false) })
    })
}

/// StepRetry events a prior (crashed) run already recorded for this step —
/// a resumed ladder continues the budget from there instead of restarting it.
pub(crate) async fn prior_step_retries(
    storage: &SurrealStorage,
    run_id: &str,
    step_key: &str,
) -> u32 {
    storage
        .get_events_for_step(run_id, step_key)
        .await
        .map(|evs| {
            evs.iter()
                .filter(|e| matches!(e.event_type, EventType::StepRetry))
                .count() as u32
        })
        .unwrap_or(0)
}

pub(crate) fn prior_step_retries_blocking(
    py: Python,
    storage: &Arc<SurrealStorage>,
    run_id: &str,
    step_key: &str,
) -> u32 {
    let storage = Arc::clone(storage);
    let run_id = run_id.to_string();
    let step_key = step_key.to_string();
    py.detach(move || io_rt().block_on(prior_step_retries(&storage, &run_id, &step_key)))
}

/// Uniform-ish sample in [0, 1) for backoff jitter, from thread ID + wall
/// clock — same dependency-free approach as `pool_claim::rand_jitter`.
pub(crate) fn rng01() -> f64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos()
        .hash(&mut hasher);
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}
