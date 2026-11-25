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
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::{
    LaunchedBy, PartitionKey, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
};

use super::Executor;
use super::ops::now_ts;
use super::version_gate;
use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::config::run_config::{RunResources, parse_run_config};
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
    /// while waiting, in which case the run is skipped. `stored_key` and
    /// `launched_by` are as the record holds them.
    Existing {
        stored_key: Option<PartitionKey>,
        launched_by: LaunchedBy,
    },
}

impl RunInit {
    fn launched_by(&self) -> &LaunchedBy {
        match self {
            Self::Create { launched_by } | Self::Existing { launched_by, .. } => launched_by,
        }
    }
}

pub(crate) struct RunPlanArgs<'a> {
    pub plan: &'a ExecutionPlan,
    pub node_map: &'a Arc<HashMap<String, ResolvedNode>>,
    pub executor: &'a Executor,
    pub storage: &'a ScopedStorageHandle<AnyStorage>,
    pub resources: &'a HashMap<String, ResourceVariant>,
    pub retries: &'a HashMap<String, rivers_core::execution::retry::RetryPolicy>,
    /// Retry policy for the steps whose node sets none.
    pub default_retry: Option<&'a rivers_core::execution::retry::RetryPolicy>,
    pub io_handler_registry: &'a IOHandlerRegistry,

    /// `None` for ad-hoc runs (`materialize`, asset-selection sensors); `Some`
    /// when the run targets a user-defined `Job`.
    pub job_name: Option<String>,
    pub run_id: String,
    pub init: RunInit,
    pub partition_key: Option<PyPartitionKey>,
    pub tags: Vec<(String, String)>,
    /// The launch document, canonical (see [`crate::config::run_config`]).
    pub config: Option<String>,

    pub resume: bool,
    pub raise_on_error: bool,
}

/// What the launch document changed for this run, else the repository's own.
struct Applied<'a> {
    node_map: &'a Arc<HashMap<String, ResolvedNode>>,
    resources: &'a HashMap<String, ResourceVariant>,
    config_overrides: Option<HashMap<String, Py<PyAny>>>,
    executor: Executor,
}

pub(crate) fn run_plan(py: Python, args: RunPlanArgs) -> PyResult<PyRunResult> {
    py.import("rivers._capture")?
        .call_method("install", (), None)?;

    // Read before any record write, so a document the definitions refuse
    // leaves no run behind.
    let document = args
        .config
        .as_deref()
        .map(|json| parse_run_config(py, json))
        .transpose()?;
    let overlay = document
        .as_ref()
        .and_then(|d| d.overlay_node_map(py, args.node_map))
        .map(Arc::new);
    let run_resources = document
        .as_ref()
        .map(|d| RunResources::build(py, d, args.resources))
        .transpose()?
        .flatten();
    let executor = match document.as_ref().and_then(|d| d.execution.as_ref()) {
        // A step pod runs its one step here whatever the document says.
        Some(execution) if !super::in_step_pod() => execution.executor(args.executor),
        _ => args.executor.clone(),
    };
    let applied = Applied {
        node_map: overlay.as_ref().unwrap_or(args.node_map),
        resources: run_resources.as_ref().map_or(args.resources, |r| &r.map),
        config_overrides: document.as_ref().and_then(|d| d.config_overrides(py)),
        executor,
    };
    let result = drive(py, args, &applied);
    if let Some(run_resources) = &run_resources {
        run_resources.teardown(py);
    }
    result
}

fn drive(py: Python, args: RunPlanArgs, applied: &Applied<'_>) -> PyResult<PyRunResult> {
    let node_names = args.plan.all_asset_names();
    let launched_by = args.init.launched_by().clone();
    // A step pod runs a step of a run that its run pod started: the gate let
    // that run in, and a migration's drain waits for it to end.
    let gated = (!super::in_step_pod()).then(|| {
        version_gate::plan_run(
            args.plan,
            applied.node_map,
            &args.run_id,
            version_gate::backfill_of(&launched_by),
        )
    });
    if let Some(gated) = &gated {
        let recorded = matches!(args.init, RunInit::Existing { .. });
        py.detach(|| version_gate::check_run(args.storage, gated, recorded))?;
    }

    let (started, stored_key) = py.detach(|| -> PyResult<(bool, Option<PartitionKey>)> {
        match args.init {
            RunInit::Create { launched_by } => {
                let priority = priority_from_tags(&args.tags);
                let now = now_ts();
                let record = RunRecord {
                    run_id: args.run_id.clone(),
                    code_location_id: args.storage.code_location_id().to_string(),
                    job_name: args.job_name,
                    status: RunStatus::Started,
                    start_time: now,
                    end_time: None,
                    execution_start_time: Some(now),
                    tags: args.tags,
                    node_names: node_names.clone(),
                    priority,
                    partition_key: args.partition_key.as_ref().map(|pk| pk.into()),
                    block_reason: None,
                    launched_by,
                    action: args.plan.action.clone(),
                    config: args.config,
                };
                io_rt()
                    .block_on(args.storage.backend().create_run(&record))
                    .map_err(|e| {
                        ExecutionError::new_err(format!("Failed to create run record: {e}"))
                    })?;
                Ok((true, None))
            }
            RunInit::Existing { stored_key, .. } => {
                let started = io_rt()
                    .block_on(args.storage.backend().try_start_run(&args.run_id))
                    .map_err(|e| {
                        ExecutionError::new_err(format!("Failed to transition run to Started: {e}"))
                    })?;
                Ok((started, stored_key))
            }
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
    // A migration that began after the first check holds the run as well:
    // its drain may have read the started runs before this one started.
    if let Some(gated) = &gated {
        py.detach(|| version_gate::check_run(args.storage, gated, true))?;
    }

    let failures = applied.executor.execute_plan(
        py,
        args.plan,
        applied.node_map,
        &args.partition_key,
        stored_key.as_ref(),
        args.storage,
        &args.run_id,
        applied.resources,
        args.retries,
        args.default_retry,
        &applied.config_overrides,
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
    storage: &ScopedStorageHandle<AnyStorage>,
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
