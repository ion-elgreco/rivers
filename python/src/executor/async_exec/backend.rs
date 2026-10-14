//! Async executor: runs steps concurrently via tokio JoinSet + spawn_blocking.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionStep;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::errors::ExecutionError;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::rt;

use super::super::dispatch::pool_claim::PoolGuard;
use super::super::dispatch::{
    AsyncWorker, BatchContext, ExecutorBackend, FinishedStep, OutputSender, StepCapture,
    StepInstance, WorkOutcome, process_finished_steps, run_step_async_lifecycle,
};
use super::super::event_writer::WriterMsg;
use super::super::ops::{self};

fn python_not_attached_outcome() -> WorkOutcome {
    WorkOutcome::Error {
        error: ExecutionError::new_err("Python not attached in spawn_blocking"),
        captured_logs: None,
        failure_config: None,
    }
}

struct SharedStepContext {
    node_map: HashMap<String, ResolvedNode>,
    partition_key: Option<PyPartitionKey>,
    resources: HashMap<String, ResourceVariant>,
    config_overrides: Option<HashMap<String, Py<PyAny>>>,
    io_handler_registry: IOHandlerRegistry,
    storage: ScopedStorageHandle<SurrealStorage>,
    run_id: String,
    /// The verb every step executes when this batch belongs to an action run.
    action: Option<String>,
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
            action: ctx.scope.plan.action.clone(),
        }
    }
}

/// Capture stdout/stderr (Python) + Rust logs around `execute_step`, and the
/// body of an async generator step. The resolved Pydantic config is dropped
/// eagerly under the GIL on success (it's already on
/// `StepResult.config_instance`) and only carried out via `failure_config` on
/// the Err branch where the orchestrator needs it for `run_failure_hooks`.
fn execute_with_capture(
    py: Python,
    step: &ExecutionStep,
    shared: &SharedStepContext,
    action_key: &Option<PyPartitionKey>,
    input_overrides: &HashMap<String, Py<PyAny>>,
    locals: Option<&pyo3_async_runtimes::TaskLocals>,
    outputs: &OutputSender,
) -> WorkOutcome {
    let capture = StepCapture::start(py);

    let mut resolved_config: Option<Py<PyAny>> = None;
    let result = if let Some(verb) = &shared.action {
        ops::execute_action_step(
            py,
            verb,
            step,
            &shared.node_map,
            action_key,
            &shared.resources,
            &shared.config_overrides,
            &shared.io_handler_registry,
            &shared.run_id,
            locals,
        )
    } else {
        ops::execute_step(
            py,
            step,
            &shared.node_map,
            &shared.partition_key,
            &shared.resources,
            &shared.config_overrides,
            &shared.io_handler_registry,
            input_overrides,
            &shared.run_id,
            locals,
            &mut resolved_config,
        )
        .map(|mut step_result| {
            run_async_generator_body(py, step, shared, locals, outputs, &mut step_result);
            step_result
        })
    };

    let captured_logs = capture.finish();

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

/// Run the body of an async generator step to its end on this thread. The
/// orchestrator thread records each output that the body yields before the
/// body goes on, as it records the outputs of a sync generator.
fn run_async_generator_body(
    py: Python,
    step: &ExecutionStep,
    shared: &SharedStepContext,
    locals: Option<&pyo3_async_runtimes::TaskLocals>,
    outputs: &OutputSender,
    step_result: &mut ops::StepResult,
) {
    let Some(ops::GeneratorType::Async { context, error }) = &mut step_result.generator else {
        return;
    };
    let Some(locals) = locals else {
        *error = Some(ExecutionError::new_err(format!(
            "Async node '{}' requires TaskLocals — this is an internal error",
            step.name
        )));
        return;
    };

    let selected_outputs: Vec<(String, Option<Py<PyAny>>)> = step
        .outputs
        .iter()
        .map(|name| {
            let hint = shared.node_map.get(name).and_then(|n| n.output_type_hint());
            (name.clone(), hint.map(|h| h.clone_ref(py)))
        })
        .collect();
    let config = step_result.config_instance.as_ref();

    *error = ops::run_async_generator(
        py,
        &step_result.result,
        &selected_outputs,
        context.as_ref(),
        locals,
        &mut |py, item| outputs.record(py, item, context.as_ref(), config),
    )
    .err();
}

struct StepDispatch {
    shared: Arc<SharedStepContext>,
    step: ExecutionStep,
    /// The key an action step acts on — the run's key minus members an
    /// ordering dependency failed.
    action_key: Option<PyPartitionKey>,
    input_overrides: HashMap<String, Py<PyAny>>,
    pools: Vec<(String, u32)>,
    asset_scope: Option<rivers_core::storage::AssetScope>,
    semaphore: Option<Arc<Semaphore>>,
    locals: Option<pyo3_async_runtimes::TaskLocals>,
    events_tx: mpsc::UnboundedSender<WriterMsg>,
    outputs: OutputSender,
    /// Step name reported on the `StepSlotWaiting` events emitted by the pool
    /// guard. Single-step name for `schedule_batch`; instance name for
    /// mapped fan-out.
    pool_step_name: String,
    /// Names emitted as `StepStart` once the pool slot is acquired. For
    /// multi-output steps this is `step.event_names()`; for mapped instances
    /// it's `vec![instance_name]`.
    start_event_names: Vec<String>,
    retry: Option<rivers_core::execution::retry::RetryPolicy>,
    resume: bool,
    cut_offs: u32,
}

/// Phase-4 worker for the async backend: hops onto a blocking thread, attaches
/// the GIL, and runs `execute_with_capture`.
struct AsyncStepWorker {
    shared: Arc<SharedStepContext>,
    step: ExecutionStep,
    action_key: Option<PyPartitionKey>,
    input_overrides: HashMap<String, Py<PyAny>>,
    locals: Option<pyo3_async_runtimes::TaskLocals>,
    outputs: OutputSender,
}

impl AsyncWorker for AsyncStepWorker {
    fn run_work(&self) -> WorkOutcome {
        Python::try_attach(|py| {
            execute_with_capture(
                py,
                &self.step,
                &self.shared,
                &self.action_key,
                &self.input_overrides,
                self.locals.as_ref(),
                &self.outputs,
            )
        })
        .unwrap_or_else(python_not_attached_outcome)
    }

    fn app_package(&self, py: Python) -> Option<String> {
        crate::executor::traceback::node_package(py, self.shared.node_map.get(&self.step.name))
    }
}

async fn dispatch_step(d: StepDispatch) -> (WorkOutcome, Option<PoolGuard>) {
    let StepDispatch {
        shared,
        step,
        action_key,
        input_overrides,
        pools,
        asset_scope,
        semaphore,
        locals,
        events_tx,
        outputs,
        pool_step_name,
        start_event_names,
        retry,
        resume,
        cut_offs,
    } = d;
    let storage = shared.storage.clone();
    let run_id = shared.run_id.clone();
    let action = shared.action.is_some();
    run_step_async_lifecycle(
        storage,
        pools,
        asset_scope,
        run_id,
        pool_step_name,
        start_event_names,
        events_tx,
        semaphore,
        retry,
        resume,
        cut_offs,
        action,
        AsyncStepWorker {
            shared,
            step,
            action_key,
            input_overrides,
            locals,
            outputs,
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
        let (outputs_tx, outputs_rx) = mpsc::unbounded_channel();

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
                let retry = ctx.retry_policy_for(&step);
                let action_key = ctx.action_step_partition_key(&step);
                let cut_offs = step
                    .event_names()
                    .first()
                    .map_or(0, |n| ctx.resumed_cut_offs(n));
                (
                    inst.idx,
                    inst.instance_name,
                    inst.event_names,
                    StepDispatch {
                        shared: Arc::clone(&shared),
                        action_key,
                        step,
                        input_overrides,
                        pools: inst.pools,
                        asset_scope: inst.asset_scope,
                        semaphore: semaphore.clone(),
                        locals: bridge_locals.clone(),
                        events_tx: events_tx.clone(),
                        outputs: OutputSender {
                            idx: inst.idx,
                            tx: outputs_tx.clone(),
                        },
                        pool_step_name,
                        start_event_names,
                        retry,
                        resume: ctx.scope.resume,
                        cut_offs,
                    },
                )
            })
            .collect();

        let mut tasks: JoinSet<FinishedStep> = JoinSet::new();
        for (idx, instance_name, event_names, dispatch) in dispatches {
            tasks.spawn_on(
                async move {
                    let (outcome, guard) = dispatch_step(dispatch).await;
                    FinishedStep {
                        idx,
                        instance_name,
                        event_names,
                        outcome,
                        guard,
                    }
                },
                rt().handle(),
            );
        }
        let bridge = ctx.repo.bridge;
        process_finished_steps(py, ctx, tasks, bridge, Some(outputs_rx), failures);
    }
}
