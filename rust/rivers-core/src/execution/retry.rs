//! Declarative retry policy: pure types and decision functions.
//!
//! Executor-agnostic and side-effect free — the retry loops live in the
//! executors and call [`should_retry`] / [`compute_delay`] here. `rng01` is an
//! injected uniform sample so the backoff math stays deterministic and
//! testable. Applying OOM escalation to concrete quantities lives in the
//! `rivers-k8s` adapter.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Why a step failed. Attached to `StepFailure` events and drives retry
/// eligibility. Classification is per-executor (a Python exception type
/// in-process; a pod termination reason on Kubernetes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    /// Ordinary exception raised by user code.
    Error,
    /// Killed for exceeding a memory bound (OOMKilled / exit 137 / MemoryError).
    OutOfMemory,
    /// Exceeded a wall-clock deadline.
    Timeout,
    /// Environmental: pod vanished, worker died, node drained — no user fault.
    Infrastructure,
    /// Cancellation requested; never retried.
    Cancelled,
}

/// The wait schedule between attempts. Built via the [`Backoff::constant`],
/// [`Backoff::linear`], [`Backoff::exponential`], [`Backoff::fixed`]
/// constructors. `jitter` is a relative fraction (0..=1) of the computed wait;
/// `max_delay` caps any single wait.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Backoff {
    pub shape: BackoffShape,
    pub jitter: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delay: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackoffShape {
    Constant { delay: Duration },
    Linear { step: Duration, initial: Duration },
    Exponential { initial: Duration, factor: f64 },
    Fixed { schedule: Vec<Duration> },
}

impl Backoff {
    pub fn constant(delay: Duration, jitter: f64, max_delay: Option<Duration>) -> Self {
        Self {
            shape: BackoffShape::Constant { delay },
            jitter,
            max_delay,
        }
    }

    pub fn linear(
        step: Duration,
        initial: Duration,
        jitter: f64,
        max_delay: Option<Duration>,
    ) -> Self {
        Self {
            shape: BackoffShape::Linear { step, initial },
            jitter,
            max_delay,
        }
    }

    pub fn exponential(
        initial: Duration,
        factor: f64,
        jitter: f64,
        max_delay: Option<Duration>,
    ) -> Self {
        Self {
            shape: BackoffShape::Exponential { initial, factor },
            jitter,
            max_delay,
        }
    }

    pub fn fixed(schedule: Vec<Duration>, jitter: f64) -> Self {
        Self {
            shape: BackoffShape::Fixed { schedule },
            jitter,
            max_delay: None,
        }
    }
}

/// Which failures a policy is willing to retry. `Match` is the explicit
/// allow-list: exception class names (matched against the raised exception's
/// MRO, name-based so the policy is serializable) and/or failure reasons.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryOn {
    All,
    Transient,
    Match {
        exceptions: Vec<String>,
        reasons: Vec<FailureReason>,
    },
}

impl Default for RetryOn {
    fn default() -> Self {
        RetryOn::All
    }
}

/// Grow a step's compute on retries triggered by resource exhaustion. Only
/// meaningful on executors that provision compute per step (Kubernetes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComputeEscalation {
    pub factor: f64,
    pub max_memory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cpu: Option<String>,
    pub on: Vec<FailureReason>,
}

/// A declarative retry policy attached to a step (asset) or job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<Backoff>,
    #[serde(default)]
    pub retry_on: RetryOn,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalate: Option<ComputeEscalation>,
}

/// How a policy is attached before resolution — inline, or a name into the
/// repository `retries` registry. `resolve_retry_refs` collapses every `Named`
/// to `Inline` at `resolve()`, so resolved nodes only ever hold a concrete
/// policy. Mirrors the IO-handler `Instance`/`ResourceRef` split.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RetryRef {
    Inline(RetryPolicy),
    Named(String),
}

/// Does `policy.retry_on` admit a failure classified as `reason` whose raised
/// exception has MRO class names `exc_types` (empty when there is no exception,
/// e.g. an OOM pod-kill)? Does *not* apply the budget or the `Cancelled` guard —
/// that is [`should_retry`].
pub fn matches_retry_on(policy: &RetryPolicy, reason: FailureReason, exc_types: &[String]) -> bool {
    match &policy.retry_on {
        RetryOn::All => true,
        RetryOn::Transient => matches!(
            reason,
            FailureReason::OutOfMemory | FailureReason::Timeout | FailureReason::Infrastructure
        ),
        RetryOn::Match {
            exceptions,
            reasons,
        } => reasons.contains(&reason) || exc_types.iter().any(|t| exceptions.contains(t)),
    }
}

/// Should the attempt numbered `attempt` (1-indexed, the one that just failed)
/// be followed by another? Budget-aware; folds in [`matches_retry_on`] and the
/// escalate-implies-retry rule. `Cancelled` is never retriable.
pub fn should_retry(
    policy: &RetryPolicy,
    reason: FailureReason,
    exc_types: &[String],
    attempt: u32,
) -> bool {
    if reason == FailureReason::Cancelled || attempt > policy.max_retries {
        return false;
    }
    matches_retry_on(policy, reason, exc_types)
        || policy
            .escalate
            .as_ref()
            .is_some_and(|e| e.on.contains(&reason))
}

/// The wall-clock delay before the next attempt, per the policy's [`Backoff`]
/// (shape → cap at `max_delay` → relative `jitter`). `Duration::ZERO` when no
/// backoff is set. `rng01` is a uniform sample in [0, 1); it is clamped.
pub fn compute_delay(policy: &RetryPolicy, attempt: u32, rng01: f64) -> Duration {
    let Some(backoff) = &policy.backoff else {
        return Duration::ZERO;
    };
    let n = attempt.max(1);
    // Work in f64 seconds throughout so a runaway exponential can't overflow
    // `Duration` before the cap is applied.
    let base_secs = match &backoff.shape {
        BackoffShape::Constant { delay } => delay.as_secs_f64(),
        BackoffShape::Linear { step, initial } => {
            initial.as_secs_f64() + step.as_secs_f64() * n as f64
        }
        BackoffShape::Exponential { initial, factor } => {
            initial.as_secs_f64() * factor.powi((n - 1) as i32)
        }
        BackoffShape::Fixed { schedule } => {
            if schedule.is_empty() {
                0.0
            } else {
                schedule[((n - 1) as usize).min(schedule.len() - 1)].as_secs_f64()
            }
        }
    };
    let capped = match backoff.max_delay {
        Some(m) => base_secs.min(m.as_secs_f64()),
        None => base_secs,
    };
    let j = backoff.jitter.clamp(0.0, 1.0);
    let r = rng01.clamp(0.0, 1.0);
    let wait = capped * ((1.0 - j) + 2.0 * j * r);
    if wait.is_finite() && wait > 0.0 {
        // Clamp to a sane ceiling (~317 years) so `from_secs_f64` can't panic.
        Duration::from_secs_f64(wait.min(1e10))
    } else {
        Duration::ZERO
    }
}

// NOTE: applying the escalation (parse → scale → clamp → render k8s quantities)
// lives in the `rivers-k8s` adapter (`rivers_k8s::compute::escalate_compute`),
// where `kube_quantity` / `k8s-openapi` already are — this core crate stays
// executor- and k8s-free. `ComputeEscalation` above is the neutral policy.

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    fn policy(backoff: Option<Backoff>) -> RetryPolicy {
        RetryPolicy {
            max_retries: 3,
            backoff,
            retry_on: RetryOn::All,
            escalate: None,
        }
    }

    // ── compute_delay ───────────────────────────────────────────────────────

    #[test]
    fn constant_backoff_is_flat() {
        let p = policy(Some(Backoff::constant(secs(10.0), 0.0, None)));
        for attempt in 1..=5 {
            assert_eq!(compute_delay(&p, attempt, 0.5), secs(10.0));
        }
    }

    #[test]
    fn linear_backoff_grows_by_step() {
        // initial=0 => step * n : 5, 10, 15
        let p = policy(Some(Backoff::linear(secs(5.0), secs(0.0), 0.0, None)));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(5.0));
        assert_eq!(compute_delay(&p, 2, 0.5), secs(10.0));
        assert_eq!(compute_delay(&p, 3, 0.5), secs(15.0));
    }

    #[test]
    fn linear_backoff_offsets_by_initial() {
        // initial=60, step=30 => 90, 120, 150
        let p = policy(Some(Backoff::linear(secs(30.0), secs(60.0), 0.0, None)));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(90.0));
        assert_eq!(compute_delay(&p, 2, 0.5), secs(120.0));
    }

    #[test]
    fn exponential_backoff_doubles() {
        // initial=1, factor=2 => 1, 2, 4, 8
        let p = policy(Some(Backoff::exponential(secs(1.0), 2.0, 0.0, None)));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(1.0));
        assert_eq!(compute_delay(&p, 2, 0.5), secs(2.0));
        assert_eq!(compute_delay(&p, 3, 0.5), secs(4.0));
        assert_eq!(compute_delay(&p, 4, 0.5), secs(8.0));
    }

    #[test]
    fn max_delay_caps_exponential() {
        let p = policy(Some(Backoff::exponential(
            secs(1.0),
            2.0,
            0.0,
            Some(secs(5.0)),
        )));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(1.0));
        assert_eq!(compute_delay(&p, 4, 0.5), secs(5.0)); // 8 capped to 5
        assert_eq!(compute_delay(&p, 10, 0.5), secs(5.0));
    }

    #[test]
    fn fixed_schedule_clamps_past_end() {
        let p = policy(Some(Backoff::fixed(
            vec![secs(10.0), secs(60.0), secs(300.0)],
            0.0,
        )));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(10.0));
        assert_eq!(compute_delay(&p, 3, 0.5), secs(300.0));
        assert_eq!(compute_delay(&p, 9, 0.5), secs(300.0)); // clamped to last
    }

    #[test]
    fn jitter_spans_the_expected_band() {
        // constant 10s, ±50% jitter: rng 0.0 -> 5s, 0.5 -> 10s, 1.0 -> 15s.
        let p = policy(Some(Backoff::constant(secs(10.0), 0.5, None)));
        assert_eq!(compute_delay(&p, 1, 0.0), secs(5.0));
        assert_eq!(compute_delay(&p, 1, 0.5), secs(10.0));
        assert_eq!(compute_delay(&p, 1, 1.0), secs(15.0));
    }

    #[test]
    fn no_backoff_means_zero_wait() {
        let p = policy(None);
        assert_eq!(compute_delay(&p, 1, 0.5), Duration::ZERO);
        assert_eq!(compute_delay(&p, 9, 1.0), Duration::ZERO);
    }

    // ── should_retry / matches_retry_on ─────────────────────────────────────

    #[test]
    fn retries_within_budget_then_stops() {
        let p = policy(None); // max_retries = 3
        assert!(should_retry(&p, FailureReason::Error, &[], 1));
        assert!(should_retry(&p, FailureReason::Error, &[], 3));
        assert!(!should_retry(&p, FailureReason::Error, &[], 4)); // budget spent
    }

    #[test]
    fn cancelled_is_never_retried() {
        let p = policy(None);
        assert!(!should_retry(&p, FailureReason::Cancelled, &[], 1));
    }

    #[test]
    fn transient_excludes_deterministic_errors() {
        let mut p = policy(None);
        p.retry_on = RetryOn::Transient;
        assert!(!should_retry(&p, FailureReason::Error, &[], 1));
        assert!(should_retry(&p, FailureReason::OutOfMemory, &[], 1));
        assert!(should_retry(&p, FailureReason::Timeout, &[], 1));
        assert!(should_retry(&p, FailureReason::Infrastructure, &[], 1));
    }

    #[test]
    fn match_allowlist_by_exception_name_and_reason() {
        let mut p = policy(None);
        p.retry_on = RetryOn::Match {
            exceptions: vec!["builtins.ConnectionError".into()],
            reasons: vec![FailureReason::OutOfMemory],
        };
        // exception name present in the MRO names
        assert!(should_retry(
            &p,
            FailureReason::Error,
            &[
                "builtins.ConnectionResetError".into(),
                "builtins.ConnectionError".into()
            ],
            1
        ));
        // unlisted exception, no matching reason
        assert!(!should_retry(
            &p,
            FailureReason::Error,
            &["builtins.ValueError".into()],
            1
        ));
        // reason axis matches even with no exception object
        assert!(should_retry(&p, FailureReason::OutOfMemory, &[], 1));
    }

    #[test]
    fn escalate_implies_retry_on_its_reasons() {
        // retry_on lists only ConnectionError, but escalate covers OOM.
        let mut p = policy(None);
        p.retry_on = RetryOn::Match {
            exceptions: vec!["builtins.ConnectionError".into()],
            reasons: vec![],
        };
        p.escalate = Some(ComputeEscalation {
            factor: 2.0,
            max_memory: "64Gi".into(),
            cpu_factor: None,
            max_cpu: None,
            on: vec![FailureReason::OutOfMemory],
        });
        assert!(should_retry(&p, FailureReason::OutOfMemory, &[], 1));
        // ...but a plain error still isn't retried
        assert!(!should_retry(&p, FailureReason::Error, &[], 1));
    }

    // Applying escalation to concrete quantities is tested in `rivers-k8s`
    // (`compute::escalate_compute`); here we only build the neutral policy.
    fn oom_escalation() -> ComputeEscalation {
        ComputeEscalation {
            factor: 2.0,
            max_memory: "64Gi".into(),
            cpu_factor: None,
            max_cpu: None,
            on: vec![FailureReason::OutOfMemory],
        }
    }

    // ── serde ───────────────────────────────────────────────────────────────

    #[test]
    fn policy_serde_round_trip() {
        let p = RetryPolicy {
            max_retries: 3,
            backoff: Some(Backoff::exponential(secs(1.0), 2.0, 0.25, Some(secs(60.0)))),
            retry_on: RetryOn::Transient,
            escalate: Some(oom_escalation()),
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn failure_reason_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&FailureReason::OutOfMemory).unwrap(),
            r#""out_of_memory""#
        );
    }
}
