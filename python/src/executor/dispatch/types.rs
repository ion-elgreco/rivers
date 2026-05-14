//! Dispatch type definitions — StepInstance, StepResult, and batch tracking types.
use std::collections::HashMap;

use pyo3::prelude::*;

use super::super::ops;

/// A unit of work the executor will run: either a non-mapped step or one
/// instance of a mapped step's fan-out.
///
/// For non-mapped: `instance_name == step.name`, `mapping_key = None`,
/// `event_names = step.event_names()` (i.e. the step's outputs, or
/// `[step.name]` for single-output), `fan_out = None`.
///
/// For mapped: `instance_name = "<step.name>__<mapping_key>"`,
/// `mapping_key = Some(...)`, `event_names = [instance_name]`,
/// `fan_out = Some((source_name, value))`.
pub(crate) struct StepInstance {
    pub idx: usize,
    pub instance_name: String,
    pub mapping_key: Option<String>,
    pub event_names: Vec<String>,
    /// Pre-resolved collect-input overrides (load specs for parallel,
    /// in-memory values for in-process/async). The fan-out source value for
    /// mapped instances is *not* in here; see `fan_out`.
    pub input_overrides: HashMap<String, Py<PyAny>>,
    /// For mapped instances: `(fan_out_source_name, per-instance value)`.
    /// Backends that invoke `execute_step` inline (in-process, async) merge
    /// this into `input_overrides` at the call site; parallel uses it to
    /// build worker submit args (mapped instances carry no collect specs).
    pub fan_out: Option<(String, Py<PyAny>)>,
    pub is_async: bool,
    /// Resolved pool config — same value for every instance of a mapped step,
    /// snapshotted up-front so the lifecycle can claim under the GIL.
    pub pools: Vec<(String, u32)>,
}

/// (stdout, stderr, rust_logs) captured around a step's invocation. Populated
/// by all three backends — in-process/async wrap the call inline, the parallel
/// backend captures inside the loky child and ships the tuple back via
/// `PyWorkerResult.captured_logs` (success path) or the exception's
/// `_rivers_captured_logs` attribute (failure path).
pub(crate) type CapturedLogs = Option<(String, String, String)>;

/// Outcome of phase 4 ("run the work") of the step lifecycle. `process_outcome`
/// in `dispatch/results.rs` interprets each variant.
///
/// K8s does not produce a `WorkOutcome` — its step pods write events directly
/// to storage and the orchestrator only tracks per-pod success.
pub(crate) enum WorkOutcome {
    /// In-process / async: full `StepResult`, IO not yet performed.
    /// Routes through `process_step_result`.
    FullResult {
        step_result: ops::StepResult,
        captured_logs: CapturedLogs,
    },
    /// Parallel: pickle-safe summary, IO already performed in the loky child.
    /// Routes through `process_worker_result`. `step_config` carries the
    /// parent-side resolved Pydantic config so `run_success_hooks` fires with
    /// the same instance the worker's `context.config` produced.
    WorkerSummary {
        worker_result: Py<PyAny>,
        input_versions: Vec<(String, String)>,
        step_config: Option<Py<PyAny>>,
    },
    /// Phase 4 itself failed (spawn_blocking panic, loky exception, IO prep
    /// error, etc.). Routes through `handle_failure`. The original `PyErr` is
    /// preserved so the caller-visible exception type (e.g. `ValueError`,
    /// `TypeError`) round-trips, rather than being flattened into
    /// `ExecutionError(message)`.
    Error {
        error: PyErr,
        captured_logs: CapturedLogs,
        failure_config: Option<Py<PyAny>>,
    },
}
