use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::Utc;
use pyo3::prelude::*;
use rivers_core::storage::{ConditionEvalRecord, TickRecord};

/// Limits concurrent `spawn_blocking` threads waiting for the GIL — prevents
/// thundering-herd (~8MB stack per blocked thread). Gates ALL GIL interactions:
/// sync evals, async Phase 1/3, and subprocess submit/parse. See
/// [`crate::runtime`] for the broader spawn-primitive guide.
pub(crate) static GIL_SEMAPHORE: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));

#[derive(Clone, Debug)]
pub(crate) enum ResolvedEvalMode {
    /// Sync eval function, run in-process via tokio::task::spawn_blocking + GIL
    SyncInProcess,
    /// Async eval function, run in-process via pyo3-async-runtimes
    AsyncInProcess,
    /// Run in loky subprocess (sync or async — subprocess uses asyncio.run)
    Subprocess,
}

/// Precomputed call-argument data extracted once at startup from
/// the eval function's `__annotations__` dict.  Only the context
/// object differs per tick — resources and config are invariant.
pub(crate) struct PrecomputedArgs {
    pub(crate) config_instance: Option<Py<PyAny>>,
    pub(crate) resource_args: Vec<Py<PyAny>>,
}

#[derive(Clone)]
pub(crate) struct RunRequestData {
    #[allow(dead_code)]
    pub(crate) run_key: Option<String>,
    #[allow(dead_code)]
    pub(crate) tags: Option<HashMap<String, String>>,
    /// `Single` keys come from sensor/schedule eval (Python `RunRequest`
    /// only carries a string). `Multi` is reachable via gRPC `ExecuteJob`,
    /// which wraps the proto `PartitionKey` directly. Wrapping happens at
    /// the producer boundary so the dispatcher sees one type.
    pub(crate) partition_key: Option<crate::partitions::PyPartitionKey>,
    pub(crate) job_name: Option<String>,
}

/// Materialization-shape run request — pre-resolved asset selection with
/// caller-minted run_id. Used by the condition path: the engine mints the
/// `run_id` up front so it can register the run with
/// `cache.register_dispatched_run` for phantom tracking before dispatch,
/// then hands the request to the same `RunDispatcherKind` schedule/sensor
/// uses.
///
/// Distinct from `RunRequestData` because the input shape differs: jobs
/// resolve a name to a selection inside the dispatcher, while
/// materialization requests carry the selection directly.
#[derive(Clone)]
pub(crate) struct MaterializationRequestData {
    pub(crate) run_id: String,
    pub(crate) asset_selection: Vec<String>,
    pub(crate) partition_key: Option<rivers_core::storage::PartitionKey>,
    pub(crate) tags: Vec<(String, String)>,
    pub(crate) launched_by: rivers_core::storage::LaunchedBy,
}

/// A run re-execution from a stored `RunRecord`: `Job` → `dispatch_jobs`,
/// `Materialization` → `dispatch_materialization`. Reuses the run's partition + tags.
pub(crate) enum RunRerunRequest {
    Job(RunRequestData),
    Materialization(MaterializationRequestData),
}

/// What a backfill runs each partition as: an ad-hoc materialization of an asset
/// selection, or a named `Job` (whose own plan + executor are used). Reconstructed
/// from `BackfillRecord` (`job_name` → `Job`, else `Materialization`) at execution.
#[derive(Clone)]
pub(crate) enum RunType {
    Materialization(Vec<String>),
    Job(String),
}

#[derive(Clone)]
pub(crate) struct BackfillRequestData {
    pub(crate) target: RunType,
    pub(crate) partition_keys: Option<Vec<crate::partitions::PyPartitionKey>>,
    pub(crate) partition_range: Option<crate::partitions::PyPartitionKeyRange>,
    pub(crate) strategy: Option<crate::partitions::PyBackfillStrategy>,
    pub(crate) failure_policy: Option<String>,
    pub(crate) max_concurrency: u32,
    pub(crate) tags: Option<HashMap<String, String>>,
    /// Daemon-driven backfills are never previews (`false`). gRPC's
    /// `LaunchBackfill` exposes the flag — when `true`, `repo.backfill`
    /// resolves partitions and reports counts without writing a record;
    /// the resulting `PyBackfillResult.backfill_id` is empty.
    pub(crate) dry_run: bool,
    /// Pre-minted backfill id (condition dispatch persists it in the crash
    /// intent before dispatch); `None` lets the repository generate one.
    pub(crate) backfill_id: Option<String>,
}

pub(crate) enum TickOutcome {
    RunRequests(
        Vec<RunRequestData>,
        Vec<MaterializationRequestData>,
        Vec<BackfillRequestData>,
    ),
    Skipped(String),
}

pub(crate) enum SensorOutcome {
    RunRequests(
        Vec<RunRequestData>,
        Vec<MaterializationRequestData>,
        Vec<BackfillRequestData>,
        Option<String>,
    ),
    Skipped(String, Option<String>),
}

/// Unified eval outcome — returned by `dispatch_eval`, used in Phase 3 persist.
///
/// `run_requests` are job-targeted (each carries a `job_name`);
/// `materialization_requests` are ad-hoc runs over a sensor's
/// `asset_selection`. The split is decided when the eval result is parsed,
/// so `tick_processing` only has to dispatch each list to its dispatcher.
pub(crate) enum EvalOutcome {
    RunRequests {
        run_requests: Vec<RunRequestData>,
        materialization_requests: Vec<MaterializationRequestData>,
        backfill_requests: Vec<BackfillRequestData>,
        cursor: Option<String>,
    },
    Skipped {
        reason: String,
        cursor: Option<String>,
    },
}

impl EvalOutcome {
    pub(crate) fn cursor_or(&self, fallback: Option<String>) -> Option<String> {
        match self {
            EvalOutcome::RunRequests { cursor, .. } | EvalOutcome::Skipped { cursor, .. } => {
                cursor.clone().or(fallback)
            }
        }
    }
}

impl From<TickOutcome> for EvalOutcome {
    fn from(t: TickOutcome) -> Self {
        match t {
            TickOutcome::RunRequests(reqs, mat_reqs, backfills) => EvalOutcome::RunRequests {
                run_requests: reqs,
                materialization_requests: mat_reqs,
                backfill_requests: backfills,
                cursor: None,
            },
            TickOutcome::Skipped(reason) => EvalOutcome::Skipped {
                reason,
                cursor: None,
            },
        }
    }
}

impl From<SensorOutcome> for EvalOutcome {
    fn from(s: SensorOutcome) -> Self {
        match s {
            SensorOutcome::RunRequests(reqs, mat_reqs, backfills, cursor) => {
                EvalOutcome::RunRequests {
                    run_requests: reqs,
                    materialization_requests: mat_reqs,
                    backfill_requests: backfills,
                    cursor,
                }
            }
            SensorOutcome::Skipped(reason, cursor) => EvalOutcome::Skipped { reason, cursor },
        }
    }
}

#[derive(Clone)]
pub(crate) enum AutomationKind {
    Schedule {
        exec_time: String,
    },
    Sensor {
        cursor: Option<String>,
        last_tick_time: Option<f64>,
    },
}

pub(crate) struct EvalParams {
    pub(crate) name: String,
    pub(crate) eval_mode: ResolvedEvalMode,
    pub(crate) timeout: Duration,
    pub(crate) kind: AutomationKind,
    pub(crate) eval_fn: Option<Arc<Py<PyAny>>>,
    /// Default job applied to bare `RunRequest`s. `Some` for schedules
    /// (always required) and for sensors that declare `job_name`. `None`
    /// for sensors that use `asset_selection` instead.
    pub(crate) default_job_name: Option<String>,
    /// Sensor-only: asset selection used for `RunRequest`s with no
    /// `job_name`. The parse step turns those into
    /// `MaterializationRequestData`s.
    pub(crate) default_asset_selection: Option<Vec<String>>,
    /// `LaunchedBy` to stamp on runs created by this automation tick.
    /// Threaded into the parse step so materialization requests carry it
    /// without `tick_processing` having to backfill anything.
    pub(crate) launched_by: rivers_core::storage::LaunchedBy,
    pub(crate) tags: Option<HashMap<String, String>>,
    pub(crate) precomputed: Option<Arc<PrecomputedArgs>>,
}

pub(crate) struct TickResult {
    pub(crate) index: usize,
    pub(crate) result: Result<EvalOutcome, String>,
    pub(crate) prev_cursor: Option<String>,
    #[allow(dead_code)]
    pub(crate) dispatched_at: chrono::DateTime<Utc>,
}

pub(crate) struct TickWriteMsg {
    pub(crate) record: TickRecord,
    pub(crate) max_ticks_retained: Option<usize>,
}

pub(crate) struct ConditionEvalWriteMsg {
    pub(crate) evals: Vec<ConditionEvalRecord>,
    pub(crate) max_evals_retained: Option<usize>,
}

pub(crate) type BoxedPyFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;
