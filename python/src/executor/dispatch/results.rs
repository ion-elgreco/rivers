//! Step result post-processing: drives per-output iteration via
//! `ops::for_each_output` and applies one unified per-item recipe (IO write
//! for materializations, then Materialization / Observation event + dv
//! recording + StepSuccess + success hooks). All three result shapes
//! (single-output, dict multi-asset, generator multi-asset) flow through
//! the same closure body.
//!
//! Called by `run_step_to_completion` (trait_def.rs) and AsyncBackend's
//! result processing loop.

use std::collections::HashSet;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use rivers_core::execution::plan::ExecutionStep;

use crate::errors::ExecutionError;
use crate::metadata::{MetadataValue, coerce_to_metadata_value};
use crate::result_types::ResultKind;

use super::super::ops::{self, now_ts};
use super::context::BatchContext;
use super::types::{CapturedLogs, WorkOutcome};

/// Emit a Materialization event + run success hooks for one output. Shared
/// by the `Output` (with IO write) and `Materialization` (no IO) paths so
/// they can't drift on hook scoping or success emission.
///
/// `value` is `None` for the `Materialization(...)` return type (the user
/// persisted it themselves and there's no value to pass to hooks).
///
/// `final_dv` is the resolved data_version (caller has already applied any
/// UUID fallback) — used both for the materialization event and for any
/// caller-side bookkeeping (e.g. dynamic-keys KV writes) that must agree on
/// the same dv.
#[allow(clippy::too_many_arguments)]
fn emit_materialization_and_hooks(
    ctx: &mut BatchContext,
    node: &crate::repository::resolved_node::ResolvedNode,
    name: &str,
    metadata: &[(String, MetadataValue)],
    final_dv: String,
    input_versions: &[(String, String)],
    value: Option<&Py<PyAny>>,
    config_instance: Option<&Py<PyAny>>,
) {
    ctx.emit_materialization(
        name,
        metadata,
        Some(final_dv.clone()),
        input_versions.to_vec(),
        now_ts(),
    );
    ctx.state.record_data_version(name.to_string(), final_dv);
    ctx.emit_success(name);

    if node.has_success_hooks() {
        Python::attach(|py| {
            let none = py.None();
            ops::run_success_hooks(
                py,
                node,
                name,
                ctx.scope.run_id,
                value.unwrap_or(&none),
                node.metadata(),
                config_instance.map(|c| c.clone_ref(py)),
            );
        });
    }
}

/// Process a step result: drive iteration, write IO + emit events per item,
/// reconcile any unyielded outputs (generator multi only).
///
/// `step_name` is the IO + event scope: `step.name` for non-mapped (single
/// or multi-asset), `instance_name` for a mapped fan-out instance. Multi-
/// asset paths are never mapped, so `step_name == step.name` for them.
/// Stash per-partition failure marks (`mark_partition_failed`) so
/// `emit_materialization` emits a StepFailure for them instead of a
/// Materialization. Keyed by every name the step emits under — a multi-asset
/// emits per output name, not under `step.name`.
fn stash_failed_partitions(
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    marks: Vec<(crate::partitions::PyPartitionKey, String)>,
) {
    if marks.is_empty() {
        return;
    }
    if step.outputs.is_empty() {
        ctx.state.failed_partitions.insert(step.name.clone(), marks);
    } else {
        for out in &step.outputs {
            ctx.state
                .failed_partitions
                .insert(out.clone(), marks.clone());
        }
    }
}

pub(crate) fn process_step_result(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    step_name: &str,
    step_result: ops::StepResult,
    failures: &mut Vec<(String, PyErr)>,
) {
    stash_failed_partitions(ctx, step, step_result.failed_partitions.clone());
    let node = ctx.repo.node_map.get(&step.name).unwrap();

    // External asset observation: skip iteration, emit Observation only.
    if node.is_external() {
        py.detach(|| handle_observation_result(ctx, step_name, &step_result));
        return;
    }

    let is_single = step.outputs.is_empty();
    let is_non_mapped = step_name == step.name;

    // Step-level pre-iter (single non-mapped only):
    //   1. record dynamic_keys so a downstream fan-out can skip a potentially
    //      stale on-disk `__keys`. Empty Vec = ran with plain values.
    //   2. dual IO write for final graph asset node — only non-mapped (a
    //      mapped instance is a per-key write, not the step's "final" output).
    if is_single && is_non_mapped {
        ctx.state.record_dynamic_keys(
            step.name.clone(),
            step_result.dynamic_keys.clone().unwrap_or_default(),
        );

        if let Some(graph_name) = ctx.repo.graph_nodes.final_nodes.get(&step.name)
            && let Some(graph_node) = ctx.repo.node_map.get(graph_name)
            && let Err(e) = ops::handle_step_output(
                py,
                graph_name,
                graph_node,
                &step_result.result,
                ctx.scope.partition_key,
                step_result.return_hint.as_ref(),
                Vec::new(),
                ctx.repo.io_handler_registry,
            )
        {
            let gn = graph_name.clone();
            ctx.record_failure_no_hooks(&gn, e, failures);
        }
    }

    let mut yielded_names: HashSet<String> = HashSet::new();
    // Provenance is per-step, not per-instance: empty for mapped fan-out.
    let input_versions = if is_non_mapped {
        ops::collect_input_data_versions(ctx.state.data_versions, &step.graph_dependencies)
    } else {
        Vec::new()
    };
    let bridge = ctx.repo.bridge;
    let return_hint = step_result.return_hint.as_ref().map(|h| h.clone_ref(py));
    let config_instance = step_result
        .config_instance
        .as_ref()
        .map(|c| c.clone_ref(py));
    // dynamic_keys writes the `__keys` index alongside the step's value.
    // Only meaningful on the single-output path (multi-assets write per-output).
    let dynamic_keys = if is_single {
        step_result.dynamic_keys.clone()
    } else {
        None
    };

    // A generator's marks are set only as its body runs (lazily, while
    // for_each_output drives it), so drain the generator context before the
    // first output is emitted rather than up-front like the return path.
    let gen_ctx: Option<Py<PyAny>> = match &step_result.generator {
        Some(ops::GeneratorType::Sync { context })
        | Some(ops::GeneratorType::Async { context }) => context.as_ref().map(|c| c.clone_ref(py)),
        None => None,
    };
    let mut gen_marks_stashed = false;

    let result = ops::for_each_output(
        py,
        &step_result,
        &step.outputs,
        step_name,
        bridge,
        |py, item| {
            if !gen_marks_stashed {
                gen_marks_stashed = true;
                stash_failed_partitions(
                    ctx,
                    step,
                    ops::drain_failed_partitions(py, gen_ctx.as_ref()),
                );
            }
            match item {
                ops::OutputItem::Materialization {
                    name,
                    value,
                    metadata,
                    data_version,
                } => {
                    // Single-output (incl. mapped fan-out instances) shares the
                    // step's underlying node — `name` is the IO/event scope but
                    // node_map is keyed by step.name. Multi-asset outputs have
                    // their own per-output entries in node_map.
                    let node_lookup = if is_single { &step.name } else { &name };
                    let node = ctx.repo.node_map.get(node_lookup).ok_or_else(|| {
                        ExecutionError::new_err(format!(
                            "Output '{}' not found in node_map",
                            node_lookup
                        ))
                    })?;

                    // value.is_some() => Output(v): write via IO handler.
                    // value.is_none() => Materialization(...): user-managed.
                    let resolved_dv = if let Some(value) = &value {
                        let io_dv = ops::handle_step_output(
                            py,
                            &name,
                            node,
                            value,
                            ctx.scope.partition_key,
                            return_hint.as_ref(),
                            metadata.clone(),
                            ctx.repo.io_handler_registry,
                        )?;
                        io_dv.or(data_version)
                    } else {
                        data_version
                    };
                    let final_dv = resolved_dv.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

                    // Persist dynamic_keys to KV before emit_success so a
                    // racing downstream sees the new keys. Single-output Output
                    // path only — multi-asset can't carry DynamicOutputs.
                    if is_single
                        && value.is_some()
                        && let Some(keys) = dynamic_keys.as_deref()
                        && !keys.is_empty()
                    {
                        py.detach(|| {
                            ops::persist_dynamic_keys(
                                ctx.sink.storage,
                                &name,
                                ctx.scope.partition_key,
                                &final_dv,
                                keys,
                            );
                        });
                    }

                    emit_materialization_and_hooks(
                        ctx,
                        node,
                        &name,
                        &metadata,
                        final_dv,
                        &input_versions,
                        value.as_ref(),
                        config_instance.as_ref(),
                    );
                    yielded_names.insert(name);
                }
                ops::OutputItem::Observation {
                    name,
                    metadata,
                    data_version,
                } => {
                    ctx.emit_observation(&name, &metadata, data_version, now_ts());
                    ctx.emit_success(&name);
                    yielded_names.insert(name);
                }
            }
            Ok(())
        },
    );

    if let Err(e) = result {
        // Failure event scope: mapped instance fails just itself; non-mapped
        // single fails all step.event_names(); multi fails any unyielded.
        let mapped_event_names = [step_name.to_string()];
        let event_names: Vec<String> = if !is_non_mapped {
            mapped_event_names.to_vec()
        } else if is_single {
            step.event_names().to_vec()
        } else {
            step.event_names()
                .iter()
                .filter(|n| !yielded_names.contains(*n))
                .cloned()
                .collect()
        };
        handle_failure(
            py,
            ctx,
            step,
            step_name,
            &event_names,
            e,
            step_result.config_instance,
            failures,
        );
    } else if !is_single {
        // Reconciliation: any selected output not yielded by the generator
        // becomes a failure (no hooks). For dict, every output is iterated
        // by `for_each_output_dict` so this is a no-op.
        py.detach(|| {
            for name in &step.outputs {
                if !yielded_names.contains(name) {
                    let msg = format!("Generator multi-asset did not yield output '{}'", name);
                    ctx.record_failure_no_hooks(name, ExecutionError::new_err(msg), failures);
                }
            }
        });
    }
}

/// External asset observation: emit Observation event (no IO write), record
/// data_version for downstream provenance.
fn handle_observation_result(
    ctx: &mut BatchContext,
    step_name: &str,
    step_result: &ops::StepResult,
) {
    let end_ts = now_ts();
    let data_version = step_result.data_version.clone();

    ctx.emit_observation(
        step_name,
        &step_result.output_metadata,
        data_version.clone(),
        end_ts,
    );

    if let Some(dv) = data_version {
        ctx.state.record_data_version(step_name.to_string(), dv);
    }

    ctx.emit_success(step_name);
}

/// Route a phase-4 outcome through the appropriate post-processor.
///
/// - `step_name` — logical step name for non-mapped runs, or the per-instance
///   name for mapped fan-out. Used for log emission, failure hooks, and the
///   failures-map key on the Error path.
/// - `event_names` — names to emit `StepFailure` for on the Error path. Pass
///   `step.event_names()` for non-mapped, `&[instance_name]` for mapped.
pub(crate) fn process_outcome(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    step_name: &str,
    event_names: &[String],
    outcome: WorkOutcome,
    failures: &mut Vec<(String, PyErr)>,
) {
    match outcome {
        WorkOutcome::FullResult {
            step_result,
            captured_logs,
        } => {
            emit_captured_logs(ctx, step_name, captured_logs);
            process_step_result(py, ctx, step, step_name, step_result, failures);
        }
        WorkOutcome::WorkerSummary {
            worker_result,
            input_versions,
            step_config,
        } => {
            process_worker_result(
                py,
                ctx,
                step,
                step_name,
                worker_result,
                input_versions,
                step_config,
                failures,
            );
        }
        WorkOutcome::Error {
            error,
            captured_logs,
            failure_config,
        } => {
            emit_captured_logs(ctx, step_name, captured_logs);
            handle_failure(
                py,
                ctx,
                step,
                step_name,
                event_names,
                error,
                failure_config,
                failures,
            );
        }
    }
}

pub(crate) fn emit_captured_logs(ctx: &mut BatchContext, step_name: &str, logs: CapturedLogs) {
    if let Some((stdout, stderr, rust_logs)) = logs {
        ctx.emit_log_output(step_name, &stdout, &stderr, &rust_logs, now_ts());
    }
}

/// Emit a `StepFailure` event for every name in `event_names`, mark each
/// failed, run failure hooks once (scoped to `name_for_hooks`), and push the
/// original error onto `failures` keyed by `name_for_hooks`. The original
/// `error` is preserved so the caller-visible exception type round-trips.
///
/// Caller responsibilities:
/// - Non-mapped step (single or multi-output): pass `step.event_names()` and
///   `&step.name`.
/// - Mapped instance: pass `&[instance_name]` and `&instance_name`.
/// - Per-output failure inside a multi-asset (e.g. unyielded outputs of a
///   generator): pass the offending names and `&step.name`.
pub(crate) fn handle_failure(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    name_for_hooks: &str,
    event_names: &[String],
    error: PyErr,
    config_instance: Option<Py<PyAny>>,
    failures: &mut Vec<(String, PyErr)>,
) {
    let err_msg = error.to_string();
    let classified = super::failure::classify_pyerr(py, &error);
    let ts = now_ts();
    for name in event_names {
        ctx.emit_step_failure(name, &err_msg, Some(&classified));
        ctx.emit_partition_failures(name, &err_msg, ts);
        ctx.state.mark_failed(name.clone());
    }
    if let Some(node) = ctx.repo.node_map.get(&step.name) {
        ops::run_failure_hooks(
            py,
            node,
            name_for_hooks,
            ctx.scope.run_id,
            &err_msg,
            node.metadata(),
            config_instance,
        );
    }
    failures.push((name_for_hooks.to_string(), error));
}

/// Decode worker per-output items and emit Materialization / Observation
/// events with the per-output metadata + dv preserved across the IPC.
/// IO already happened in the worker subprocess (for `Output`); the
/// `Materialization` kind signals "user wrote it themselves, just emit the
/// event" and the `Observation` kind signals "external asset, emit
/// Observation event."
///
/// `worker_result.outputs` is a list of `(name, kind_tag, metadata, dv)`
/// tuples — one per selected output. `kind_tag` is the `ResultKind::as_u8`
/// discriminator. The worker is the source of truth for per-output values,
/// so we trust the tuple's `name` over `step_name` (they should equal
/// `step_name` for single-output non-mapped, the per-instance name for
/// mapped, or the per-output name for multi-asset).
pub(crate) fn process_worker_result(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    step_name: &str,
    worker_result: Py<PyAny>,
    input_versions: Vec<(String, String)>,
    step_config: Option<Py<PyAny>>,
    failures: &mut Vec<(String, PyErr)>,
) {
    // Emit captured stdout/stderr/rust_logs from the worker subprocess as a
    // `run_logs` row before output processing, mirroring the in-process path
    // (`execute_step_with_capture` → `emit_captured_logs`).
    if let Ok(logs_obj) = worker_result.getattr(py, "captured_logs")
        && !logs_obj.is_none(py)
        && let Ok((stdout, stderr, rust_logs)) = logs_obj.extract::<(String, String, String)>(py)
    {
        ctx.emit_log_output(step_name, &stdout, &stderr, &rust_logs, now_ts());
    }

    let outputs_obj = match worker_result.getattr(py, "outputs") {
        Ok(o) => o,
        Err(e) => {
            ctx.record_failure_no_hooks(step_name, e, failures);
            return;
        }
    };
    let outputs_bound = outputs_obj.bind(py);
    let outputs_list: &Bound<PyList> = match outputs_bound.cast() {
        Ok(l) => l,
        Err(e) => {
            ctx.record_failure_no_hooks(step_name, e.into(), failures);
            return;
        }
    };

    let is_single = step.outputs.is_empty();
    let end_ts = now_ts();

    // Single-output dynamic_keys come back at top-level on WorkerResult; the
    // orchestrator persists them to KV (loky workers don't write KV — embedded
    // RocksDB is single-process and the orchestrator owns the lock).
    let dynamic_keys: Option<Vec<String>> = if is_single {
        worker_result
            .getattr(py, "dynamic_keys")
            .ok()
            .and_then(|o| o.extract::<Option<Vec<String>>>(py).ok())
            .flatten()
    } else {
        None
    };

    // Worker-drained mark_partition_failed marks cross IPC as (key_json, error);
    // stash them like process_step_result so emit_materialization emits a
    // per-partition StepFailure instead of a Materialization.
    let worker_marks: Vec<(String, String)> = worker_result
        .getattr(py, "failed_partitions")
        .ok()
        .and_then(|o| o.extract::<Vec<(String, String)>>(py).ok())
        .unwrap_or_default();
    let marks: Vec<(crate::partitions::PyPartitionKey, String)> = worker_marks
        .into_iter()
        .filter_map(|(json, err)| {
            rivers_core::storage::PartitionKey::from_json(&json)
                .ok()
                .map(|core| ((&core).into(), err))
        })
        .collect();
    stash_failed_partitions(ctx, step, marks);

    for item_any in outputs_list.iter() {
        if let Err(e) = process_one_worker_item(
            py,
            ctx,
            step,
            is_single,
            &input_versions,
            end_ts,
            &item_any,
            step_config.as_ref(),
            dynamic_keys.as_deref(),
        ) {
            ctx.record_failure_no_hooks(step_name, e, failures);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_one_worker_item(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    is_single: bool,
    input_versions: &[(String, String)],
    end_ts: i64,
    item: &Bound<'_, PyAny>,
    step_config: Option<&Py<PyAny>>,
    dynamic_keys: Option<&[String]>,
) -> PyResult<()> {
    let tuple: &Bound<PyTuple> = item.cast()?;
    let name: String = tuple.get_item(0)?.extract()?;
    let kind_tag: u8 = tuple.get_item(1)?.extract()?;
    let kind = ResultKind::from_u8(kind_tag)?;
    let metadata_py = tuple.get_item(2)?;
    let data_version: Option<String> = tuple.get_item(3)?.extract()?;

    let metadata = decode_worker_metadata(py, &metadata_py)?;

    if matches!(kind, ResultKind::Observation) {
        ctx.emit_observation(&name, &metadata, data_version.clone(), end_ts);
        if let Some(dv) = data_version {
            ctx.state.record_data_version(name.clone(), dv);
        }
        ctx.emit_success(&name);
        return Ok(());
    }

    // Output and Materialization both emit a Materialization event; the only
    // difference (an IO write) already happened — or was deliberately skipped
    // — inside the worker.
    let dv = data_version.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Persist dynamic_keys to KV before emit_success so a racing downstream
    // sees the new keys. Single-output Output path only — multi-asset can't
    // carry DynamicOutputs; Materialization wouldn't be a fan-out source.
    if is_single
        && matches!(kind, ResultKind::Output)
        && let Some(keys) = dynamic_keys
        && !keys.is_empty()
    {
        py.detach(|| {
            ops::persist_dynamic_keys(ctx.sink.storage, &name, ctx.scope.partition_key, &dv, keys);
        });
    }

    ctx.emit_materialization(
        &name,
        &metadata,
        Some(dv.clone()),
        input_versions.to_vec(),
        end_ts,
    );
    ctx.state.record_data_version(name.clone(), dv);
    ctx.emit_success(&name);

    // Hooks: same node-lookup rule as the in-process consumer — single-output
    // (incl. mapped) uses step.name; multi-asset uses the per-output name.
    // The worker doesn't ship the asset value back, so hooks see py.None()
    // as the output. Pre-existing limitation of the parallel path.
    let node_lookup = if is_single { &step.name } else { &name };
    if let Some(node) = ctx.repo.node_map.get(node_lookup)
        && node.has_success_hooks()
    {
        ops::run_success_hooks(
            py,
            node,
            &name,
            ctx.scope.run_id,
            &py.None(),
            node.metadata(),
            step_config.map(|c| c.clone_ref(py)),
        );
    }
    Ok(())
}

/// Decode the per-output metadata dict shipped by the worker. The worker
/// emits raw Python values via `metadata_to_pickle_safe_dict` (calling
/// `MetadataValue.raw_value()` to unwrap), so we re-coerce per-value to
/// recover the original `MetadataValue` variant (Int, Float, Bool, Text...).
fn decode_worker_metadata(
    py: Python,
    obj: &Bound<'_, PyAny>,
) -> PyResult<Vec<(String, MetadataValue)>> {
    if obj.is_none() {
        return Ok(Vec::new());
    }
    let dict: &Bound<PyDict> = obj.cast()?;
    let mut result = Vec::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let key: String = k.extract()?;
        let val = coerce_to_metadata_value(py, &v)?;
        result.push((key, val));
    }
    Ok(result)
}
