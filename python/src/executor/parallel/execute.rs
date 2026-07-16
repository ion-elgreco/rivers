//! Parallel step execution — submits batches to loky and collects results.
//!
//! Pool-requiring steps run through loky with claim-gated concurrency: a tokio
//! JoinSet spawns one task per step that does `claim_async → spawn_blocking
//! (loky submit + collect) → release_async`. The pool limit naturally throttles
//! how many steps run simultaneously in the process pool.
use std::sync::Arc;

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PySet, PyTuple};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::errors::ExecutionError;
use crate::runtime::rt;

use super::super::dispatch::{
    AsyncWorker, BatchContext, ExecutorBackend, StepInstance, WorkOutcome, process_outcome,
    run_step_async_lifecycle,
};
use super::super::ops::{self, now_ts};
use super::validate_not_in_memory_io;
use super::worker_args::{build_worker_submit_args, resolve_worker_args};

/// Recover `(stdout, stderr, rust_logs)` from a worker exception that the
/// loky child stashed via `_rivers_captured_logs` before re-raising. Returns
/// `None` for pre-spawn / non-worker errors that never installed capture.
fn captured_logs_from_pyerr(py: Python, error: &PyErr) -> Option<(String, String, String)> {
    error
        .value(py)
        .getattr("_rivers_captured_logs")
        .ok()
        .and_then(|v| v.extract::<(String, String, String)>().ok())
}

pub(crate) struct ParallelBackend {
    pub max_workers: usize,
    pub max_async_concurrent: Option<usize>,
}

impl ExecutorBackend for ParallelBackend {
    fn run_instances(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        instances: Vec<StepInstance>,
        max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        // Order: submit non-pool sync to loky, run async siblings while loky workers
        // execute in their subprocesses, then collect loky results, then claim-gated
        // pool steps. The submit-before-async ordering is what makes loky and async
        // overlap.
        let (async_instances, sync_instances): (Vec<StepInstance>, Vec<StepInstance>) =
            instances.into_iter().partition(|i| i.is_async);

        let all_non_mapped = sync_instances.iter().all(|i| i.fan_out.is_none());

        // Single-instance non-mapped sync → skip loky and run via InProcess.
        // Mapped fan-out always goes through loky to preserve prior behaviour.
        if all_non_mapped && sync_instances.len() == 1 && async_instances.is_empty() {
            super::super::in_process::InProcessBackend.run_instances(
                py,
                ctx,
                sync_instances,
                None,
                failures,
            );
            return;
        }

        // Retrying steps also take the lifecycle path — the batch/windowed fast
        // paths resolve futures once and can't re-submit an attempt.
        let (pool_instances, no_pool_instances): (Vec<StepInstance>, Vec<StepInstance>) =
            sync_instances.into_iter().partition(|i| {
                !i.pools.is_empty() || ctx.retry_policy_for(&ctx.scope.plan.steps[i.idx]).is_some()
            });

        let mut loky_futures: Vec<SubmittedStep> = Vec::new();
        let mut windowed_done = false;

        if !no_pool_instances.is_empty() {
            let executor = match self.get_loky_executor(py) {
                Ok(e) => e,
                Err(e) => {
                    ctx.fail_all_instances(&no_pool_instances, &e.to_string(), failures);
                    // Still run async steps below even if loky fails.
                    if !async_instances.is_empty() {
                        super::super::async_exec::AsyncBackend {
                            max_concurrent: self.max_async_concurrent,
                        }
                        .run_instances(
                            py,
                            ctx,
                            async_instances,
                            None,
                            failures,
                        );
                    }
                    return;
                }
            };

            // Same single-instance fast path, repeated for the with-async-siblings case.
            if all_non_mapped && no_pool_instances.len() == 1 {
                super::super::in_process::InProcessBackend.run_instances(
                    py,
                    ctx,
                    no_pool_instances,
                    None,
                    failures,
                );
            } else if let Some(limit) = max_concurrency {
                self.run_no_pool_windowed(py, ctx, &executor, no_pool_instances, limit, failures);
                windowed_done = true;
            } else {
                for inst in &no_pool_instances {
                    let Some(prep) = Self::prepare_step_for_loky(py, inst, ctx, failures) else {
                        continue;
                    };
                    for name in &inst.event_names {
                        ctx.emit_start(name, now_ts());
                    }
                    match PyTuple::new(py, &prep.submit_args)
                        .and_then(|t| executor.call_method1("submit", t))
                    {
                        Ok(future) => {
                            loky_futures.push(SubmittedStep {
                                idx: prep.idx,
                                instance_name: prep.instance_name,
                                event_names: inst.event_names.clone(),
                                future: future.into(),
                                input_versions: prep.input_versions,
                                failure_config: prep.failure_config,
                            });
                        }
                        Err(e) => {
                            ctx.record_failure_no_hooks(&prep.instance_name, e, failures);
                        }
                    }
                }
            }
        }

        if !async_instances.is_empty() {
            super::super::async_exec::AsyncBackend {
                max_concurrent: self.max_async_concurrent,
            }
            .run_instances(py, ctx, async_instances, None, failures);
        }

        if !windowed_done {
            for submitted in loky_futures {
                let step = &ctx.scope.plan.steps[submitted.idx];
                let step_config = submitted.failure_config.as_ref().map(|c| c.clone_ref(py));
                let outcome = match submitted.future.call_method0(py, "result") {
                    Ok(worker_result) => WorkOutcome::WorkerSummary {
                        worker_result,
                        input_versions: submitted.input_versions,
                        step_config,
                    },
                    Err(error) => {
                        let captured_logs = captured_logs_from_pyerr(py, &error);
                        WorkOutcome::Error {
                            error,
                            captured_logs,
                            failure_config: submitted.failure_config,
                        }
                    }
                };
                process_outcome(
                    py,
                    ctx,
                    step,
                    &submitted.instance_name,
                    &submitted.event_names,
                    outcome,
                    failures,
                );
            }
        }

        if !pool_instances.is_empty() {
            self.schedule_pool_steps_loky(py, ctx, pool_instances, max_concurrency, failures);
        }
    }
}

struct SubmittedStep {
    idx: usize,
    instance_name: String,
    event_names: Vec<String>,
    future: Py<PyAny>,
    input_versions: Vec<(String, String)>,
    failure_config: Option<Py<PyAny>>,
}

struct PreparedStep {
    idx: usize,
    instance_name: String,
    submit_args: Vec<Py<PyAny>>,
    input_versions: Vec<(String, String)>,
    /// Resolved Pydantic config instance — propagated to
    /// `WorkOutcome::Error::failure_config` so failure hooks fire with the
    /// same config the worker's `context.config` produced.
    failure_config: Option<Py<PyAny>>,
}

impl ParallelBackend {
    /// Validate IO, resolve overrides, build worker + submit args for a single
    /// instance. Handles both non-mapped (input_overrides → collect specs;
    /// step.outputs forwarded) and mapped (fan-out source name + per-instance
    /// value; no collect specs; empty outputs).
    fn prepare_step_for_loky(
        py: Python,
        instance: &StepInstance,
        ctx: &mut BatchContext,
        failures: &mut Vec<(String, PyErr)>,
    ) -> Option<PreparedStep> {
        let step = &ctx.scope.plan.steps[instance.idx];
        let step_name = step.name.clone();
        let node = ctx
            .repo
            .node_map
            .get(&step_name)
            .expect("step must be in node_map");

        let func = node
            .callable(py)
            .expect("node must have a callable — validated at plan build time");

        if let Err(e) = validate_not_in_memory_io(py, &step_name, node) {
            ctx.record_failure_no_hooks(&step_name, e, failures);
            return None;
        }
        // For mapped instances, only validate the step itself — mapped fan-out
        // is single-output (no multi-asset), matching prior behaviour.
        if instance.fan_out.is_none() {
            for out_name in &step.outputs {
                if let Some(out_node) = ctx.repo.node_map.get(out_name)
                    && let Err(e) = validate_not_in_memory_io(py, out_name, out_node)
                {
                    ctx.record_failure_no_hooks(&step_name, e, failures);
                    return None;
                }
            }
        }

        let empty_overrides: HashMap<String, Py<PyAny>> = HashMap::new();
        let no_outputs: &[String] = &[];
        let (input_overrides, outputs_for_meta) = if instance.fan_out.is_some() {
            // Mapped instance: no collect specs, empty outputs.
            (empty_overrides, no_outputs)
        } else {
            let resolved = match Self::build_collect_input_overrides(
                py,
                ctx.scope.plan,
                step,
                ctx.repo.node_map,
                ctx.scope.partition_key,
                ctx.repo.io_handler_registry,
                ctx.state.mapped_instance_keys,
            ) {
                Ok(ov) => ov,
                Err(msg) => {
                    ctx.record_failure_no_hooks(&step_name, ExecutionError::new_err(msg), failures);
                    return None;
                }
            };
            (resolved, &step.outputs[..])
        };

        let fan_out_source_name = instance.fan_out.as_ref().map(|(n, _)| n.as_str());
        let fan_out_value = instance.fan_out.as_ref().map(|(_, v)| v);
        let instance_arg = if instance.fan_out.is_some() {
            Some(instance.instance_name.as_str())
        } else {
            None
        };

        let config_overrides_for_step = ctx
            .repo
            .config_overrides
            .as_ref()
            .and_then(|m| m.get(&step_name))
            .map(|obj| obj.bind(py).cast::<PyDict>())
            .transpose()
            .ok()
            .flatten();

        let worker_meta = match resolve_worker_args(
            py,
            &step_name,
            &func,
            ctx.repo.node_map,
            ctx.scope.partition_key,
            ctx.repo.resources,
            fan_out_source_name,
            &input_overrides,
            outputs_for_meta,
            ctx.repo.io_handler_registry,
            config_overrides_for_step,
        ) {
            Ok(a) => a,
            Err(e) => {
                ctx.record_failure_no_hooks(&step_name, e, failures);
                return None;
            }
        };

        let submit_args = match build_worker_submit_args(
            py,
            func,
            &step_name,
            instance_arg,
            node,
            ctx.scope.partition_key,
            &worker_meta,
            fan_out_value,
            ctx.repo.node_map,
            ctx.repo.io_handler_registry,
            outputs_for_meta,
        ) {
            Ok(a) => a,
            Err(e) => {
                ctx.record_failure_no_hooks(&step_name, e, failures);
                return None;
            }
        };

        let input_versions = if instance.fan_out.is_some() {
            // Mapped instances aren't tracked per-instance in input_data_versions today.
            Vec::new()
        } else {
            ops::collect_input_data_versions(ctx.state.data_versions, &step.graph_dependencies)
        };

        let failure_config = worker_meta
            .config_instance
            .as_ref()
            .map(|c| c.clone_ref(py));

        Some(PreparedStep {
            idx: instance.idx,
            instance_name: instance.instance_name.clone(),
            submit_args,
            input_versions,
            failure_config,
        })
    }

    /// No-pool sync instances with `max_concurrency` set: keep at most `limit`
    /// loky futures in flight at once via `concurrent.futures.wait`
    /// (`FIRST_COMPLETED`). Each completed future is routed through
    /// `process_outcome` immediately so the next instance can submit.
    fn run_no_pool_windowed(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        executor: &Bound<'_, PyAny>,
        instances: Vec<StepInstance>,
        limit: usize,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        // Pre-build submit args under the GIL for every instance (validates +
        // resolves worker metadata). Aborting on a prep failure mirrors the
        // non-windowed path.
        let mut prepared: Vec<(StepInstance, PreparedStep)> = Vec::new();
        for inst in instances {
            let Some(prep) = Self::prepare_step_for_loky(py, &inst, ctx, failures) else {
                continue;
            };
            prepared.push((inst, prep));
        }
        if prepared.is_empty() {
            return;
        }

        let cf = match py.import("concurrent.futures") {
            Ok(m) => m,
            Err(e) => {
                for (inst, _) in &prepared {
                    ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                }
                return;
            }
        };
        let cf_wait = match cf.getattr("wait") {
            Ok(w) => w,
            Err(e) => {
                for (inst, _) in &prepared {
                    ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                }
                return;
            }
        };
        let first_completed = match cf.getattr("FIRST_COMPLETED") {
            Ok(v) => v,
            Err(e) => {
                for (inst, _) in &prepared {
                    ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                }
                return;
            }
        };
        let wait_kwargs = PyDict::new(py);
        if let Err(e) = wait_kwargs.set_item("return_when", first_completed) {
            for (inst, _) in &prepared {
                ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
            }
            return;
        }

        let futures_set = match PySet::empty(py) {
            Ok(s) => s,
            Err(e) => {
                for (inst, _) in &prepared {
                    ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                }
                return;
            }
        };

        struct InFlight {
            inst_idx: usize,
            future: Py<PyAny>,
        }
        // Keyed by future pointer (matches the FIRST_COMPLETED return identity).
        let mut in_flight: HashMap<usize, InFlight> = HashMap::new();
        let mut next_idx: usize = 0;
        let mut any_failed = false;

        // Submit one prepared instance to loky, register its future. Returns
        // false if submission failed (failure already recorded).
        let submit_at = |inst_idx: usize,
                         ctx: &mut BatchContext,
                         in_flight: &mut HashMap<usize, InFlight>,
                         failures: &mut Vec<(String, PyErr)>|
         -> bool {
            let (inst, prep) = &prepared[inst_idx];
            for name in &inst.event_names {
                ctx.emit_start(name, now_ts());
            }
            let submit_tuple = match PyTuple::new(py, &prep.submit_args) {
                Ok(t) => t,
                Err(e) => {
                    ctx.record_failure_no_hooks(&inst.instance_name, e, failures);
                    return false;
                }
            };
            let future = match executor.call_method1("submit", submit_tuple) {
                Ok(f) => f,
                Err(e) => {
                    ctx.record_failure_no_hooks(&inst.instance_name, e, failures);
                    return false;
                }
            };
            if let Err(e) = futures_set.add(&future) {
                ctx.record_failure_no_hooks(&inst.instance_name, e, failures);
                return false;
            }
            let fid = future.as_ptr() as usize;
            in_flight.insert(
                fid,
                InFlight {
                    inst_idx,
                    future: future.unbind(),
                },
            );
            true
        };

        // Initial fill: submit up to `limit` instances.
        while next_idx < prepared.len() && in_flight.len() < limit {
            if !submit_at(next_idx, ctx, &mut in_flight, failures) {
                any_failed = true;
            }
            next_idx += 1;
        }

        // Drain.
        while !in_flight.is_empty() {
            let done = match cf_wait
                .call((&futures_set,), Some(&wait_kwargs))
                .and_then(|r| r.get_item(0))
            {
                Ok(d) => d,
                Err(e) => {
                    // Mark every still-in-flight + remaining instance as failed.
                    for entry in in_flight.values() {
                        let inst = &prepared[entry.inst_idx].0;
                        ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                    }
                    while next_idx < prepared.len() {
                        let inst = &prepared[next_idx].0;
                        ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                        next_idx += 1;
                    }
                    return;
                }
            };

            let completed: Vec<Py<PyAny>> = match done
                .try_iter()
                .and_then(|it| it.map(|r| r.map(|v| v.unbind())).collect())
            {
                Ok(v) => v,
                Err(e) => {
                    for entry in in_flight.values() {
                        let inst = &prepared[entry.inst_idx].0;
                        ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                    }
                    return;
                }
            };

            for fut_obj in completed {
                let fid = fut_obj.as_ptr() as usize;
                if let Err(e) = futures_set.discard(fut_obj.bind(py)) {
                    let inst = &prepared
                        .get(in_flight.get(&fid).map(|e| e.inst_idx).unwrap_or(0))
                        .map(|(i, _)| i.instance_name.as_str())
                        .unwrap_or("<unknown>");
                    ctx.record_failure_no_hooks(inst, e, failures);
                    continue;
                }
                let Some(entry) = in_flight.remove(&fid) else {
                    continue;
                };
                let (inst, prep) = &prepared[entry.inst_idx];
                let step = &ctx.scope.plan.steps[inst.idx];
                let outcome = match entry.future.call_method0(py, "result") {
                    Ok(worker_result) => WorkOutcome::WorkerSummary {
                        worker_result,
                        input_versions: prep.input_versions.clone(),
                        step_config: prep.failure_config.as_ref().map(|c| c.clone_ref(py)),
                    },
                    Err(error) => {
                        any_failed = true;
                        let captured_logs = captured_logs_from_pyerr(py, &error);
                        WorkOutcome::Error {
                            error,
                            captured_logs,
                            failure_config: prep.failure_config.as_ref().map(|c| c.clone_ref(py)),
                        }
                    }
                };
                process_outcome(
                    py,
                    ctx,
                    step,
                    &inst.instance_name,
                    &inst.event_names,
                    outcome,
                    failures,
                );

                if next_idx < prepared.len() && !any_failed {
                    if !submit_at(next_idx, ctx, &mut in_flight, failures) {
                        any_failed = true;
                    }
                    next_idx += 1;
                }
            }
        }
    }

    /// Pool-requiring steps: pre-build submit args (GIL), then JoinSet where each
    /// task runs through the shared async lifecycle, which handles pool claim,
    /// StepStart emission, the loky `spawn_blocking` hop, and pool release.
    /// `max_concurrency` (mapped-group windowing) gates the lifecycle with a
    /// semaphore so instances diverted here don't escape the window.
    fn schedule_pool_steps_loky(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        pool_instances: Vec<StepInstance>,
        max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        let executor = match self.get_loky_executor(py) {
            Ok(e) => e,
            Err(e) => {
                ctx.fail_all_instances(&pool_instances, &e.to_string(), failures);
                return;
            }
        };

        // Requires GIL + ctx, so pre-build before entering JoinSet.
        struct PreparedPoolStep {
            base: PreparedStep,
            pools: Vec<(String, u32)>,
            event_names: Vec<String>,
            instance_name: String,
            retry: Option<rivers_core::execution::retry::RetryPolicy>,
        }
        let mut prepared: Vec<PreparedPoolStep> = Vec::new();

        for inst in &pool_instances {
            let Some(base) = Self::prepare_step_for_loky(py, inst, ctx, failures) else {
                continue;
            };
            prepared.push(PreparedPoolStep {
                base,
                pools: inst.pools.clone(),
                event_names: inst.event_names.clone(),
                instance_name: inst.instance_name.clone(),
                retry: ctx.retry_policy_for(&ctx.scope.plan.steps[inst.idx]),
            });
        }

        if prepared.is_empty() {
            return;
        }

        let executor_arc = Arc::new(executor.unbind());
        let storage = ctx.sink.storage.clone();
        let run_id = ctx.scope.run_id.to_string();
        let events_tx = ctx.event_sender();
        let window = max_concurrency.map(|n| Arc::new(Semaphore::new(n)));

        type PoolResult = (usize, String, Vec<String>, WorkOutcome);

        let results: Vec<PoolResult> = py.detach(|| {
            rt().block_on(async {
                let mut join_set: JoinSet<PoolResult> = JoinSet::new();

                for prep in prepared {
                    let PreparedPoolStep {
                        base,
                        pools,
                        event_names,
                        instance_name,
                        retry,
                    } = prep;
                    let idx = base.idx;
                    let pool_step_name = instance_name.clone();
                    let storage = storage.clone();
                    let run_id = run_id.clone();
                    let events_tx = events_tx.clone();
                    let event_names_for_outcome = event_names.clone();
                    let window = window.clone();
                    let worker = LokyPoolWorker {
                        executor: Arc::clone(&executor_arc),
                        submit_args: base.submit_args,
                        input_versions: base.input_versions,
                        failure_config: base.failure_config,
                    };

                    join_set.spawn(async move {
                        let outcome = run_step_async_lifecycle(
                            storage,
                            pools,
                            run_id,
                            pool_step_name,
                            event_names,
                            events_tx,
                            window,
                            retry,
                            worker,
                        )
                        .await;
                        (idx, instance_name, event_names_for_outcome, outcome)
                    });
                }

                let mut results = Vec::new();
                while let Some(join_result) = join_set.join_next().await {
                    match join_result {
                        Ok(r) => results.push(r),
                        Err(e) => tracing::error!("pool step task panicked: {e}"),
                    }
                }
                results
            })
        });

        for (step_idx, instance_name, event_names, outcome) in results {
            let step = &ctx.scope.plan.steps[step_idx];
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

/// Phase-4 worker for the parallel pool path: submits the pre-built args to a
/// shared loky executor and blocks for the result, all under a `try_attach`d
/// GIL inside `spawn_blocking`.
struct LokyPoolWorker {
    executor: Arc<Py<PyAny>>,
    submit_args: Vec<Py<PyAny>>,
    input_versions: Vec<(String, String)>,
    failure_config: Option<Py<PyAny>>,
}

impl AsyncWorker for LokyPoolWorker {
    fn run_work(&self) -> WorkOutcome {
        Python::try_attach(|py| -> WorkOutcome {
            let cfg = || self.failure_config.as_ref().map(|c| c.clone_ref(py));
            let submit_tuple = match PyTuple::new(py, &self.submit_args) {
                Ok(t) => t,
                Err(error) => {
                    return WorkOutcome::Error {
                        error,
                        captured_logs: None,
                        failure_config: cfg(),
                    };
                }
            };
            let future = match self.executor.call_method1(py, "submit", submit_tuple) {
                Ok(f) => f,
                Err(error) => {
                    return WorkOutcome::Error {
                        error,
                        captured_logs: None,
                        failure_config: cfg(),
                    };
                }
            };
            match future.call_method0(py, "result") {
                Ok(worker_result) => WorkOutcome::WorkerSummary {
                    worker_result,
                    input_versions: self.input_versions.clone(),
                    step_config: cfg(),
                },
                Err(error) => {
                    let captured_logs = captured_logs_from_pyerr(py, &error);
                    WorkOutcome::Error {
                        error,
                        captured_logs,
                        failure_config: cfg(),
                    }
                }
            }
        })
        .unwrap_or_else(|| WorkOutcome::Error {
            error: crate::errors::ExecutionError::new_err("Python not attached in spawn_blocking"),
            captured_logs: None,
            failure_config: None,
        })
    }
}
