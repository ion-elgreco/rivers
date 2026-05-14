//! Dispatch executor backend — runs a batch of step instances (single or mapped).
use pyo3::prelude::*;

use super::context::BatchContext;
use super::types::StepInstance;

/// What each executor backend must implement. The single seam the dispatcher
/// pushes work through is `run_instances`: a batch of `StepInstance`s (which
/// may be a mix of non-mapped steps and mapped fan-out instances).
///
/// Collect-dependency input resolution is the backend's responsibility:
/// in-process / async load mapped outputs in-memory at `run_instances` entry
/// (via `in_process::resolve_in_memory_collect_overrides`), parallel + k8s
/// build serializable load specs internally before submitting to subprocesses.
pub(crate) trait ExecutorBackend: Sync {
    /// Run a batch of step instances. The backend decides WHERE and WHEN each
    /// instance runs (inline, JoinSet, loky, k8s pods). When `max_concurrency`
    /// is `Some(N)`, no more than N instances run at once — honoured by the
    /// parallel backend's no-pool mapped fan-out (FIRST_COMPLETED windowing).
    /// Other backends use their own concurrency knobs and ignore this value.
    fn run_instances(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        instances: Vec<StepInstance>,
        max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    );
}
