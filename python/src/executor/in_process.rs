//! In-process executor backend — runs every step in the orchestrator process,
//! no worker subprocesses or pods.
//!
//! Sync instances execute sequentially on the calling thread. Async instances
//! in the same batch are split off and handed to [`AsyncBackend`] on the
//! shared Tokio runtime with a bounded concurrency cap
//! (`AsyncBackend.max_concurrent = Some(4)`); they therefore run concurrently
//! with each other but not with the sync portion of the batch (sync runs
//! first, then async). Mapped instances flow through the same path — they're
//! just `StepInstance`s with `mapping_key = Some(...)` and a `fan_out` value.
//!
//! "In-process" here means *no IPC*, not *single-threaded* — the Tokio
//! runtime is still in this process, just running concurrent futures.
use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionStep;

use super::dispatch::{
    BatchContext, ExecutorBackend, StepInstance, SyncWorker, WorkOutcome, build_step_by_name,
    resolve_collect_overrides, run_step_sync_lifecycle,
};
use super::ops;

/// Concurrency cap for async instances delegated to [`AsyncBackend`] within
/// a single batch. The in-process backend is intentionally not unbounded: a
/// runaway async step shouldn't be able to spawn unbounded concurrent IO
/// against, e.g., the storage backend or external APIs.
const ASYNC_MAX_CONCURRENT: usize = 4;

pub(crate) struct InProcessBackend;

impl ExecutorBackend for InProcessBackend {
    fn run_instances(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        instances: Vec<StepInstance>,
        _max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        let instances = resolve_in_memory_collect_overrides(py, ctx, instances, failures);

        let (async_instances, sync_instances): (Vec<StepInstance>, Vec<StepInstance>) =
            instances.into_iter().partition(|i| i.is_async);

        // Deliberate: sync first, then async. Async tasks need the GIL to
        // call Python and the calling thread holds it throughout sync
        // execution, so pipelining only gains the share of sync time spent
        // inside `py.detach` (Rust IO etc.). Mixed sync/async batches are
        // also uncommon — same-level uniform-async-ness is the norm.
        for inst in &sync_instances {
            Self::run_step_to_completion(py, ctx, inst, failures);
        }

        if !async_instances.is_empty() {
            super::async_exec::AsyncBackend {
                max_concurrent: Some(ASYNC_MAX_CONCURRENT),
            }
            .run_instances(py, ctx, async_instances, None, failures);
        }
    }
}

/// Resolve collect/CollectStream input overrides in-memory for each non-mapped
/// instance. Backends that load collect deps directly (in-process, async)
/// call this at `run_instances` entry; backends that use serializable specs
/// (parallel, k8s) skip it. Instances whose collect resolution fails are
/// dropped from the returned vec after `record_failure_no_hooks`.
pub(crate) fn resolve_in_memory_collect_overrides(
    py: Python,
    ctx: &mut BatchContext,
    instances: Vec<StepInstance>,
    failures: &mut Vec<(String, PyErr)>,
) -> Vec<StepInstance> {
    let step_by_name = build_step_by_name(ctx.scope.plan);
    let mut resolved = Vec::with_capacity(instances.len());
    for mut inst in instances {
        if inst.fan_out.is_some() {
            // Mapped instances have no collect deps — fan_out carries the input.
            resolved.push(inst);
            continue;
        }
        let step = &ctx.scope.plan.steps[inst.idx];
        match resolve_collect_overrides(py, step, ctx, &step_by_name) {
            Ok(overrides) => {
                inst.input_overrides = overrides;
                resolved.push(inst);
            }
            Err((_msg, e)) => {
                let name = step.name.clone();
                ctx.record_failure_no_hooks(&name, e, failures);
            }
        }
    }
    resolved
}

impl InProcessBackend {
    pub(crate) fn run_step_to_completion(
        py: Python,
        ctx: &mut BatchContext,
        instance: &StepInstance,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        let step = &ctx.scope.plan.steps[instance.idx];
        // Mapped instances carry their fan-out value alongside any collect
        // overrides. Merge into one map for `execute_step` to consume.
        let merged_overrides = if let Some((src, value)) = &instance.fan_out {
            let mut merged = HashMap::with_capacity(instance.input_overrides.len() + 1);
            for (k, v) in &instance.input_overrides {
                merged.insert(k.clone(), v.clone_ref(py));
            }
            merged.insert(src.clone(), value.clone_ref(py));
            Some(merged)
        } else {
            None
        };
        let overrides_ref = merged_overrides
            .as_ref()
            .unwrap_or(&instance.input_overrides);
        run_step_sync_lifecycle(
            py,
            ctx,
            step,
            &instance.instance_name,
            &instance.event_names,
            instance.pools.clone(),
            InProcessStepWorker {
                step,
                input_overrides: overrides_ref,
            },
            failures,
        );
    }
}

pub(crate) struct InProcessStepWorker<'a> {
    pub step: &'a ExecutionStep,
    pub input_overrides: &'a HashMap<String, Py<PyAny>>,
}

impl<'a> SyncWorker for InProcessStepWorker<'a> {
    fn run_work(self, py: Python, ctx: &BatchContext) -> WorkOutcome {
        execute_step_with_capture(py, ctx, self.step, self.input_overrides)
    }
}

/// Core step execution: capture stdout/stderr, call `execute_step`, return a
/// `WorkOutcome` carrying the result and captured logs. The caller is
/// responsible for routing the outcome (`process_outcome` for non-mapped via
/// the lifecycle helper, destructured handling for mapped).
pub(crate) fn execute_step_with_capture(
    py: Python,
    ctx: &BatchContext,
    step: &ExecutionStep,
    input_overrides: &HashMap<String, Py<PyAny>>,
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

    // `resolved_config` captures the config as soon as `execute_step` resolves
    // it so the Error variant can carry it to `run_failure_hooks` — same value
    // the success path puts on `StepResult.config_instance`.
    let mut resolved_config: Option<Py<PyAny>> = None;
    let result = ops::execute_step(
        py,
        step,
        ctx.repo.node_map,
        ctx.scope.partition_key,
        ctx.repo.resources,
        ctx.repo.config_overrides,
        ctx.repo.io_handler_registry,
        input_overrides,
        ctx.repo.bridge.map(|b| &b.task_locals),
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
