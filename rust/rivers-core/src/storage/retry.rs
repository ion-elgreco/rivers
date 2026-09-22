use std::time::Duration;

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct StorageRetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
    /// Wall-clock ceiling across every attempt. `None` bounds the loop by
    /// `max_retries` alone.
    pub max_elapsed: Option<Duration>,
}

impl Default for StorageRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            max_elapsed: None,
        }
    }
}

/// First wait after losing a race. Long enough for the winner to commit, short
/// enough that a claim is not paced by the wait.
const CONTENDED_INITIAL_BACKOFF: Duration = Duration::from_micros(250);
/// Ceiling for a contention wait. Past this the backend is the problem, not the
/// race, and the caller is better off being told. Profiled against 8 claimants
/// on one 4-slot pool: 20 ms and 10 ms are indistinguishable, and both beat a
/// 50 ms ceiling by about a third, because a claim that escalates that far has
/// stopped waiting for a commit and started waiting for its own convoy.
const CONTENDED_MAX_BACKOFF: Duration = Duration::from_millis(20);
/// How long a contended operation keeps trying. An attempt count is the wrong
/// budget here: the waits are deliberately short, so 20 of them gave up after
/// about 220 ms, while the dispatcher that asked for the slot is willing to
/// wait 600 s for it. Past this the pool is not merely contended.
const CONTENDED_MAX_ELAPSED: Duration = Duration::from_secs(5);
/// Attempts allowed before a contended operation gives up. Derived so that
/// `CONTENDED_MAX_ELAPSED` is what ends the loop rather than this: jitter can
/// halve a wait, so the shortest steady-state wait is half the ceiling, and the
/// ramp-up adds a few attempts on top of that rate.
const CONTENDED_MAX_RETRIES: u32 =
    (2 * CONTENDED_MAX_ELAPSED.as_millis() / CONTENDED_MAX_BACKOFF.as_millis()) as u32 + 16;

impl StorageRetryConfig {
    /// The schedule for an operation that lost a race, not one that hit a sick
    /// backend.
    ///
    /// Claiming a concurrency slot bumps `claim_version` on the shared pool
    /// row, so every concurrent claim on one pool is a write-write conflict by
    /// design — that bump is what fences the write-skew. The loser only has to
    /// wait for the winner to commit, which takes microseconds. Retrying it on
    /// the backend schedule instead makes every lost race cost 100 ms, which is
    /// what paces claim throughput rather than the work itself.
    ///
    /// The budget is wall-clock rather than a count, because short waits and a
    /// fixed attempt count together make a loser give up in a fraction of a
    /// second — which is what failed roughly 2 claims in 10,000 against a
    /// four-slot pool with eight claimants.
    pub fn contended(&self) -> Self {
        Self {
            max_retries: self.max_retries.max(CONTENDED_MAX_RETRIES),
            initial_backoff: CONTENDED_INITIAL_BACKOFF,
            max_backoff: CONTENDED_MAX_BACKOFF.min(self.max_backoff),
            backoff_multiplier: self.backoff_multiplier,
            max_elapsed: Some(CONTENDED_MAX_ELAPSED),
        }
    }
}

/// Spread a backoff so threads that failed together do not retry together.
///
/// Waiters that all sleep the same time wake in lockstep and conflict again, so
/// the retry accomplishes nothing but the wait. Half the backoff is kept as a
/// floor and the other half is spread, which bounds the shortest wait while
/// still breaking the convoy. The spread has to be cheap and uncorrelated
/// between threads, not statistically strong; the sub-millisecond part of the
/// clock gives both without a random-number dependency.
fn jittered(backoff: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let spread = f64::from(nanos % 1_000_000) / 1_000_000.0;
    backoff.mul_f64(0.5 + 0.5 * spread)
}

/// Classify a SurrealDB error as transient (worth retrying) or permanent.
///
/// `Internal` errors are matched by substring because the structured-error API
/// folds RocksDB/TiKV transient conflicts (`"onflict"`, `"Busy"`, `"Try again"`)
/// onto `Internal` with no dedicated variant. `NotFound(Session)` covers a
/// startup race in the local engine's `router_loop` where the first route
/// request can land before `SessionId::Initial` is registered.
///
/// The RocksDB `LOCK` file is also transient: dropping an embedded handle frees
/// the lock asynchronously (~tens of ms), so an immediate reopen of the same path
/// briefly sees a stale lock that a backoff retry clears.
pub fn is_transient_surrealdb_error(e: &surrealdb::Error) -> bool {
    use surrealdb::types::{ErrorDetails, NotFoundError, QueryError};
    match e.details() {
        ErrorDetails::Query(Some(QueryError::TimedOut { .. } | QueryError::NotExecuted)) => true,
        ErrorDetails::NotFound(Some(NotFoundError::Session { .. })) => true,
        ErrorDetails::Internal => {
            let s = e.message();
            s.contains("onflict")
                || s.contains("Busy")
                || s.contains("Try again")
                || s.contains("lock hold by current process")
                || s.contains("No locks available")
        }
        _ => false,
    }
}

/// Default `should_retry` predicate used by [`with_retry`]. Walks the anyhow
/// chain so context wrappers don't hide the underlying `surrealdb::Error`.
pub fn default_should_retry(e: &anyhow::Error) -> bool {
    e.chain()
        .find_map(|x| x.downcast_ref::<surrealdb::Error>())
        .map(is_transient_surrealdb_error)
        .unwrap_or(false)
}

/// True if the given error is SurrealDB's unique-index violation. v3 reports
/// this as `Internal` kind with an `"already contains"` message — there is no
/// dedicated `AlreadyExists` mapping. Callers using client-supplied IDs treat
/// this as a phantom-commit success after retry.
pub fn is_unique_index_violation(e: &anyhow::Error) -> bool {
    e.chain()
        .find_map(|x| x.downcast_ref::<surrealdb::Error>())
        .map(|s| s.is_internal() && s.message().contains("already contains"))
        .unwrap_or(false)
}

/// Retry an async storage operation on transient SurrealDB errors. See
/// [`with_retry_if`] for a custom predicate.
pub async fn with_retry<F, Fut, T>(config: &StorageRetryConfig, f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    with_retry_if(config, default_should_retry, f).await
}

/// Retry an async storage operation while `should_retry(&err)` returns true,
/// up to `config.max_retries` attempts with exponential backoff, and no longer
/// than `config.max_elapsed` in total when that is set.
pub async fn with_retry_if<F, Fut, T, P>(
    config: &StorageRetryConfig,
    mut should_retry: P,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
    P: FnMut(&anyhow::Error) -> bool,
{
    let mut backoff = config.initial_backoff;
    let mut attempts = 0u32;
    let started = std::time::Instant::now();

    loop {
        let attempt_no = attempts + 1;
        let max_attempts = config.max_retries.saturating_add(1);
        tracing::debug!(
            attempt = attempt_no,
            max_attempts,
            "starting storage attempt"
        );
        match f().await {
            Ok(v) => {
                if attempts > 0 {
                    tracing::info!(
                        attempts = attempt_no,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "storage operation succeeded after retry"
                    );
                }
                return Ok(v);
            }
            Err(e) => {
                if !should_retry(&e) {
                    return Err(e);
                }
                attempts += 1;
                let elapsed = started.elapsed();
                let out_of_time = config.max_elapsed.is_some_and(|cap| elapsed >= cap);
                if attempts > config.max_retries || out_of_time {
                    tracing::warn!(
                        attempts = attempts,
                        max = config.max_retries,
                        elapsed_ms = elapsed.as_millis() as u64,
                        out_of_time,
                        error = %e,
                        "storage operation exhausted retries"
                    );
                    return Err(e);
                }
                tracing::warn!(
                    attempt = attempts,
                    max = config.max_retries,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "storage operation failed with transient error, retrying"
                );
                tokio::time::sleep(jittered(backoff)).await;
                backoff = Duration::from_secs_f64(
                    (backoff.as_secs_f64() * config.backoff_multiplier)
                        .min(config.max_backoff.as_secs_f64()),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config() -> StorageRetryConfig {
        StorageRetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            max_elapsed: None,
        }
    }

    #[tokio::test]
    async fn succeeds_first_try() {
        let result = with_retry_if(
            &fast_config(),
            |_| true,
            || async { Ok::<_, anyhow::Error>(42) },
        )
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retries_then_succeeds_when_predicate_allows() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = with_retry_if(
            &fast_config(),
            |_| true,
            || {
                let n = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    if n < 2 {
                        anyhow::bail!("transient error");
                    }
                    Ok(99)
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn does_not_retry_when_predicate_rejects() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = with_retry_if(
            &fast_config(),
            |_| false,
            || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err::<i32, _>(anyhow::anyhow!("permanent")) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "predicate rejection must short-circuit"
        );
    }

    #[tokio::test]
    async fn exhausts_retries_when_predicate_keeps_allowing() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = with_retry_if(
            &fast_config(),
            |_| true,
            || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err::<i32, _>(anyhow::anyhow!("transient")) }
            },
        )
        .await;
        assert!(result.is_err());
        // initial attempt + max_retries (3) = 4 calls
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn wall_clock_budget_ends_the_loop_before_the_attempt_count() {
        let config = StorageRetryConfig {
            max_retries: 10_000,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
            max_elapsed: Some(Duration::from_millis(50)),
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let started = std::time::Instant::now();
        let result = with_retry_if(
            &config,
            |_| true,
            || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err::<i32, _>(anyhow::anyhow!("transient")) }
            },
        )
        .await;
        assert!(result.is_err());
        assert!(started.elapsed() >= config.max_elapsed.unwrap());
        assert!(
            attempts.load(std::sync::atomic::Ordering::Relaxed) < config.max_retries,
            "the deadline must end the loop, not the attempt count"
        );
    }

    /// A claim that loses many races in a row still has to be granted. A
    /// 20-attempt budget gave up on this in about 220 ms, which failed roughly
    /// 2 claims in 10,000 against a four-slot pool.
    #[tokio::test]
    async fn contended_schedule_survives_thirty_lost_races() {
        let config = StorageRetryConfig::default().contended();
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = with_retry_if(
            &config,
            |_| true,
            || {
                let n = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    if n < 30 {
                        anyhow::bail!("lost the race");
                    }
                    Ok(n)
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 30);
    }

    #[tokio::test]
    async fn default_predicate_skips_non_surrealdb_errors() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = with_retry(&fast_config(), || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err::<i32, _>(anyhow::anyhow!("plain string error")) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "plain anyhow::Error is not retryable under default predicate"
        );
    }

    #[test]
    fn classifies_query_timeout_as_transient() {
        use surrealdb::types::QueryError;
        let err = surrealdb::Error::query(
            "query timed out".to_string(),
            QueryError::TimedOut {
                duration: Duration::from_secs(5),
            },
        );
        assert!(is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_query_cancelled_as_permanent() {
        use surrealdb::types::QueryError;
        let err = surrealdb::Error::query("cancelled".to_string(), QueryError::Cancelled);
        assert!(!is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_query_not_executed_as_transient() {
        use surrealdb::types::QueryError;
        let err = surrealdb::Error::query(
            "statement not executed due to a failed transaction".to_string(),
            QueryError::NotExecuted,
        );
        assert!(is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_internal_rocksdb_conflict_as_transient() {
        // RocksDB transaction conflicts surface as Internal kind with messages
        // like "Conflict" or "Transaction conflict" — substring fallback covers
        // both via the case-insensitive "onflict" check.
        let err = surrealdb::Error::internal("Transaction conflict detected".to_string());
        assert!(is_transient_surrealdb_error(&err));
        let err = surrealdb::Error::internal("conflict on commit".to_string());
        assert!(is_transient_surrealdb_error(&err));
        let err = surrealdb::Error::internal("Busy: try again".to_string());
        assert!(is_transient_surrealdb_error(&err));
        let err = surrealdb::Error::internal("Try again later".to_string());
        assert!(is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_internal_unrelated_as_permanent() {
        let err = surrealdb::Error::internal("disk full".to_string());
        assert!(!is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_validation_as_permanent() {
        let err = surrealdb::Error::validation("bad input".to_string(), None);
        assert!(!is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_not_found_as_permanent() {
        let err = surrealdb::Error::not_found("missing record".to_string(), None);
        assert!(!is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_session_not_found_as_transient() {
        use surrealdb::types::NotFoundError;
        let err = surrealdb::Error::not_found(
            "Session not found: deadbeef".to_string(),
            NotFoundError::Session {
                id: Some("deadbeef".to_string()),
            },
        );
        assert!(is_transient_surrealdb_error(&err));
    }

    #[test]
    fn classifies_already_exists_as_permanent() {
        let err = surrealdb::Error::already_exists("dup record".to_string(), None);
        assert!(!is_transient_surrealdb_error(&err));
    }
}
