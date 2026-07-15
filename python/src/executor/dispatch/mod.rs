//! Executor dispatch: step classification, routing to backends, result handling.

pub(crate) mod backend;
mod classify;
mod context;
mod failure;
mod orchestrate;
pub(crate) mod pool_claim;
pub(crate) mod results;
pub(crate) mod step_lifecycle;
mod types;

pub(crate) use backend::ExecutorBackend;
pub(crate) use context::{BatchContext, EventSink, Repo, RunScope, RunState};
pub(crate) use orchestrate::{build_step_by_name, execute_level_batch, resolve_collect_overrides};
pub(crate) use results::process_outcome;
pub(crate) use step_lifecycle::{
    AsyncWorker, SyncWorker, run_step_async_lifecycle, run_step_sync_lifecycle,
};
pub(crate) use types::{StepInstance, WorkOutcome};
