//! Shared run-lifecycle helper.
//!
//! `run_plan` is the single source of truth for the per-run sequence shared by
//! `Job::execute`, `Job::execute_run`, and `materialize_with_launcher`:
//!
//!   1. Install stdout/stderr capture (idempotent).
//!   2. Create or transition the `RunRecord` to `Started`.
//!   3. Drive the executor over the `ExecutionPlan`.
//!   4. Finalize: cancellation check → final status → end_time.
//!   5. Build the `PyRunResult`; raise the first failure when `raise_on_error`.

use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend};

use super::Executor;
use super::ops::now_ts;
use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::errors::ExecutionError;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::repository::{PyRunResult, priority_from_tags};
use crate::runtime::io_rt;

/// How `run_plan` should bring the run record to `Started`.
pub(crate) enum RunInit {
    /// Insert a fresh `RunRecord` with status `Started`.
    Create { launched_by: LaunchedBy },
    /// The record already exists (created by `Job::create_run` or by the queue
    /// dequeuer); transition its status to `Started` — unless it was canceled
    /// while waiting, in which case the run is skipped.
    Existing,
}

pub(crate) struct RunPlanArgs<'a> {
    pub plan: &'a ExecutionPlan,
    pub node_map: &'a HashMap<String, ResolvedNode>,
    pub executor: &'a Executor,
    pub storage: &'a ScopedStorageHandle<SurrealStorage>,
    pub resources: &'a HashMap<String, ResourceVariant>,
    pub io_handler_registry: &'a IOHandlerRegistry,

    /// `None` for ad-hoc runs (`materialize`, asset-selection sensors); `Some`
    /// when the run targets a user-defined `Job`.
    pub job_name: Option<String>,
    pub run_id: String,
    pub init: RunInit,
    pub partition_key: Option<PyPartitionKey>,
    pub tags: Vec<(String, String)>,
    pub config: Option<HashMap<String, Py<PyAny>>>,

    pub resume: bool,
    pub raise_on_error: bool,
}

pub(crate) fn run_plan(py: Python, args: RunPlanArgs) -> PyResult<PyRunResult> {
    py.import("rivers._capture")?
        .call_method("install", (), None)?;

    let node_names = args.plan.all_asset_names();

    let started = py.detach(|| -> PyResult<bool> {
        match args.init {
            RunInit::Create { launched_by } => {
                let priority = priority_from_tags(&args.tags);
                let record = RunRecord {
                    run_id: args.run_id.clone(),
                    code_location_id: args.storage.code_location_id().to_string(),
                    job_name: args.job_name,
                    status: RunStatus::Started,
                    start_time: now_ts(),
                    end_time: None,
                    tags: args.tags,
                    node_names: node_names.clone(),
                    priority,
                    partition_key: args.partition_key.as_ref().map(|pk| pk.into()),
                    block_reason: None,
                    launched_by,
                };
                io_rt()
                    .block_on(args.storage.backend().create_run(&record))
                    .map_err(|e| {
                        ExecutionError::new_err(format!("Failed to create run record: {e}"))
                    })?;
                Ok(true)
            }
            RunInit::Existing => io_rt()
                .block_on(args.storage.backend().try_start_run(&args.run_id))
                .map_err(|e| {
                    ExecutionError::new_err(format!("Failed to transition run to Started: {e}"))
                }),
        }
    })?;

    if !started {
        tracing::info!(
            target: "rivers::executor",
            run_id = %args.run_id,
            "run was canceled before start; skipping execution"
        );
        return Ok(build_run_result(
            args.run_id,
            RunStatus::Canceled,
            node_names,
            vec![],
        ));
    }

    let failures = args.executor.execute_plan(
        py,
        args.plan,
        args.node_map,
        &args.partition_key,
        args.storage,
        &args.run_id,
        args.resources,
        &args.config,
        args.io_handler_registry,
        args.resume,
    );

    let status = finalize_status(py, args.storage, &args.run_id, failures.is_empty())?;

    if args.raise_on_error && !failures.is_empty() {
        return Err(failures.into_iter().next().unwrap().1);
    }

    Ok(build_run_result(args.run_id, status, node_names, failures))
}

fn finalize_status(
    py: Python,
    storage: &ScopedStorageHandle<SurrealStorage>,
    run_id: &str,
    success: bool,
) -> PyResult<RunStatus> {
    py.detach(|| {
        let backend = storage.backend();
        let was_cancelled = io_rt()
            .block_on(backend.is_cancelled(run_id))
            .map_err(|e| ExecutionError::new_err(format!("Failed to check cancellation: {e}")))?;
        let status = if was_cancelled {
            RunStatus::Canceled
        } else if success {
            RunStatus::Success
        } else {
            RunStatus::Failure
        };
        io_rt()
            .block_on(backend.update_run_status(run_id, status.clone(), Some(now_ts())))
            .map_err(|e| ExecutionError::new_err(format!("Failed to update run status: {e}")))?;
        Ok(status)
    })
}

fn build_run_result(
    run_id: String,
    status: RunStatus,
    node_names: Vec<String>,
    failures: Vec<(String, PyErr)>,
) -> PyRunResult {
    let success = status == RunStatus::Success;
    let failed_assets = failures
        .into_iter()
        .map(|(name, err)| (name, err.to_string()))
        .collect();
    PyRunResult {
        success,
        run_id,
        materialized_assets: node_names,
        failed_assets,
    }
}
