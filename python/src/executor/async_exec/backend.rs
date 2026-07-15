//! Async executor: runs steps concurrently via tokio JoinSet + spawn_blocking.
//!
//! Each step runs in a `spawn_blocking` thread that acquires the GIL via
//! `Python::try_attach` and calls `execute_step`. Async steps internally
//! release the GIL via `bridge.run_coroutine()` → `py.detach()`, so multiple
//! threads overlap their I/O naturally.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionStep;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{EventRecord, ScopedStorageHandle};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::errors::ExecutionError;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::rt;

use super::super::dispatch::{
    AsyncWorker, BatchContext, ExecutorBackend, StepInstance, WorkOutcome, process_outcome,
    run_step_async_lifecycle,
};
use super::super::ops::{self};

fn python_not_attached_outcome() -> WorkOutcome {
    WorkOutcome::Error {
        error: ExecutionError::new_err("Python not attached in spawn_blocking"),
        captured_logs: None,
        failure_config: None,
    }
}

struct TaskResult {
    idx: usize,
    instance_name: String,
    event_names: Vec<String>,
    outcome: WorkOutcome,
}

struct SharedStepContext {
    node_map: HashMap<String, ResolvedNode>,
    partition_key: Option<PyPartitionKey>,
    resources: HashMap<String, ResourceVariant>,
    config_overrides: Option<HashMap<String, Py<PyAny>>>,
    io_handler_registry: IOHandlerRegistry,
    storage: ScopedStorageHandle<SurrealStorage>,
    run_id: String,
}

impl SharedStepContext {
    fn clone_from(py: Python, ctx: &BatchContext) -> Self {
        Self {
            node_map: ctx
                .repo
                .node_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                .collect(),
            partition_key: ctx.scope.partition_key.clone(),
            resources: ctx
                .repo
                .resources
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                .collect(),
            config_overrides: ctx.repo.config_overrides.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                    .collect()
            }),
            io_handler_registry: ctx.repo.io_handler_registry.clone_ref(py),
            storage: ctx.sink.storage.clone(),
            run_id: ctx.scope.run_id.to_string(),
        }
    }
}

/// Capture stdout/stderr (Python) + Rust logs around `execute_step`. The
/// resolved Pydantic config is dropped eagerly under the GIL on success
/// (it's already on `StepResult.config_instance`) and only carried out via
/// `failure_config` on the Err branch where the orchestrator needs it for
/// `run_failure_hooks`.
fn execute_with_capture(
    py: Python,
    step: &ExecutionStep,
    shared: &SharedStepContext,
    input_overrides: &HashMap<String, Py<PyAny>>,
    locals: Option<&pyo3_async_runtimes::TaskLocals>,
) -> WorkOutcome {
    let capture = py
        .import("rivers._capture")
        .and_then(|m| m.getattr("StepCapture"))
        .and_then(|cls| cls.call0())
        .ok()
        .inspect(|c| {
            let _ = c.call_method0("start");
            crate::log_capture::start();
        });

    let mut resolved_config: Option<Py<PyAny>> = None;
    let result = ops::execute_step(
        py,
        step,
        &shared.node_map,
        &shared.partition_key,
        &shared.resources,
        &shared.config_overrides,
        &shared.io_handler_registry,
        input_overrides,
        locals,
        &mut resolved_config,
    );

    let captured_logs = capture.and_then(|cap| {
        cap.call_method0("finish").ok().and_then(|out| {
            out.extract::<(String, String)>()
                .ok()
                .map(|(stdout, stderr)| {
                    let logs = crate::log_capture::take();
                    (stdout, stderr, logs)
                })
        })
    });

    match result {
        Ok(step_result) => WorkOutcome::FullResult {
            step_result,
            captured_logs,
        },
        Err(error) => WorkOutcome::Error {
            error,
            captured_logs,
            failure_config: resolved_config,
        },
    }
}

struct StepDispatch {
    shared: Arc<SharedStepContext>,
    step: ExecutionStep,
    input_overrides: HashMap<String, Py<PyAny>>,
    pools: Vec<(String, u32)>,
    semaphore: Option<Arc<Semaphore>>,
    locals: Option<pyo3_async_runtimes::TaskLocals>,
    events_tx: mpsc::UnboundedSender<EventRecord>,
    /// Step name reported on the `StepSlotWaiting` events emitted by the pool
    /// guard. Single-step name for `schedule_batch`; instance name for
    /// mapped fan-out.
    pool_step_name: String,
    /// Names emitted as `StepStart` once the pool slot is acquired. For
    /// multi-output steps this is `step.event_names()`; for mapped instances
    /// it's `vec![instance_name]`.
    start_event_names: Vec<String>,
    retry: Option<rivers_core::execution::retry::RetryPolicy>,
}

/// Phase-4 worker for the async backend: hops onto a blocking thread, attaches
/// the GIL, and runs `execute_with_capture`.
struct AsyncStepWorker {
    shared: Arc<SharedStepContext>,
    step: ExecutionStep,
    input_overrides: HashMap<String, Py<PyAny>>,
    locals: Option<pyo3_async_runtimes::TaskLocals>,
}

impl AsyncWorker for AsyncStepWorker {
    fn run_work(&self) -> WorkOutcome {
        Python::try_attach(|py| {
            execute_with_capture(
                py,
                &self.step,
                &self.shared,
                &self.input_overrides,
                self.locals.as_ref(),
            )
        })
        .unwrap_or_else(python_not_attached_outcome)
    }
}

async fn dispatch_step(d: StepDispatch) -> WorkOutcome {
    let StepDispatch {
        shared,
        step,
        input_overrides,
        pools,
        semaphore,
        locals,
        events_tx,
        pool_step_name,
        start_event_names,
        retry,
    } = d;
    let storage = shared.storage.clone();
    let run_id = shared.run_id.clone();
    run_step_async_lifecycle(
        storage,
        pools,
        run_id,
        pool_step_name,
        start_event_names,
        events_tx,
        semaphore,
        retry,
        AsyncStepWorker {
            shared,
            step,
            input_overrides,
            locals,
        },
    )
    .await
}

pub(crate) struct AsyncBackend {
    pub max_concurrent: Option<usize>,
}

impl AsyncBackend {
    fn semaphore(&self) -> Option<Arc<Semaphore>> {
        self.max_concurrent
            .map(|n| Arc::new(Semaphore::new(n.max(1))))
    }
}

impl ExecutorBackend for AsyncBackend {
    fn run_instances(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        instances: Vec<StepInstance>,
        _max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        let instances = super::super::in_process::resolve_in_memory_collect_overrides(
            py, ctx, instances, failures,
        );
        if instances.is_empty() {
            return;
        }

        let semaphore = self.semaphore();
        let shared = Arc::new(SharedStepContext::clone_from(py, ctx));
        let bridge_locals = ctx.repo.bridge.map(|b| b.task_locals.clone());
        let events_tx = ctx.event_sender();

        let dispatches: Vec<(usize, String, Vec<String>, StepDispatch)> = instances
            .into_iter()
            .map(|inst| {
                let step = ctx.scope.plan.steps[inst.idx].clone();
                let mut input_overrides = inst.input_overrides;
                if let Some((src, value)) = inst.fan_out {
                    input_overrides.insert(src, value);
                }
                let pool_step_name = inst.instance_name.clone();
                let start_event_names = inst.event_names.clone();
                let retry = ctx.retry_policy(&step.name);
                (
                    inst.idx,
                    inst.instance_name,
                    inst.event_names,
                    StepDispatch {
                        shared: Arc::clone(&shared),
                        step,
                        input_overrides,
                        pools: inst.pools,
                        semaphore: semaphore.clone(),
                        locals: bridge_locals.clone(),
                        events_tx: events_tx.clone(),
                        pool_step_name,
                        start_event_names,
                        retry,
                    },
                )
            })
            .collect();

        let results: Vec<TaskResult> = py.detach(|| {
            rt().block_on(async {
                let mut join_set: JoinSet<TaskResult> = JoinSet::new();
                for (idx, instance_name, event_names, dispatch) in dispatches {
                    join_set.spawn(async move {
                        TaskResult {
                            idx,
                            instance_name,
                            event_names,
                            outcome: dispatch_step(dispatch).await,
                        }
                    });
                }
                let mut results = Vec::new();
                while let Some(join_result) = join_set.join_next().await {
                    match join_result {
                        Ok(task_result) => results.push(task_result),
                        Err(e) => tracing::error!("Task panicked in async executor: {e}"),
                    }
                }
                results
            })
        });

        for TaskResult {
            idx,
            instance_name,
            event_names,
            outcome,
        } in results
        {
            let step = &ctx.scope.plan.steps[idx];
            process_outcome(
                py,
                ctx,
                step,
                &instance_name,
                &event_names,
                outcome,
                failures,
            );
        }
    }
}
