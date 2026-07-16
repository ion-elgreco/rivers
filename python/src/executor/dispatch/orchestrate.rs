//! Dispatch orchestrator — schedules ready steps across executor backends.
use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_core::execution::plan::{ExecutionPlan, ExecutionStep, StepKind};

use crate::errors::ExecutionError;

use super::super::ops::{self, now_ts};
use super::backend::ExecutorBackend;
use super::context::BatchContext;
use super::types::StepInstance;

pub(crate) fn build_step_by_name(plan: &ExecutionPlan) -> HashMap<&str, &ExecutionStep> {
    plan.steps.iter().map(|s| (s.name.as_str(), s)).collect()
}

/// Execute a batch of independent steps at the same dependency level.
/// Classifies each step, handles framework-owned types (dep-fail, collect,
/// graph assets, multi-asset), builds `StepInstance`s for non-mapped + mapped
/// fan-outs, and dispatches them through `backend.run_instances`. The singles
/// batch and each mapped step's fan-out are dispatched as separate calls so
/// each preserves backend-specific framing (e.g. parallel's size==1 InProcess
/// fallback, mapped's `max_concurrency` windowing).
pub(crate) fn execute_level_batch(
    backend: &dyn ExecutorBackend,
    py: Python,
    ctx: &mut BatchContext,
    step_indices: &[usize],
) -> Vec<(String, PyErr)> {
    py.detach(|| {
        let mut failures: Vec<(String, PyErr)> = Vec::new();
        let mut singles: Vec<StepInstance> = Vec::new();
        let mut mapped_groups: Vec<(Vec<StepInstance>, Option<usize>)> = Vec::new();
        let mut mapped_records: Vec<MappedStepRecord> = Vec::new();

        for &idx in step_indices {
            use super::classify::{StepAction, classify_step};

            let action = classify_step(idx, ctx, &mut failures);

            match action {
                StepAction::Handled => continue,

                StepAction::Mapped { fan_out_source } => {
                    let step = &ctx.scope.plan.steps[idx];
                    ctx.emit_start(&step.name, now_ts());
                    let max_concurrency = match &step.kind {
                        StepKind::Mapped {
                            max_concurrency, ..
                        } => *max_concurrency,
                        _ => None,
                    };
                    let instances = match Python::attach(|py| {
                        build_mapped_instances(py, ctx, idx, &fan_out_source, &mut failures)
                    }) {
                        Some(i) => i,
                        None => continue,
                    };
                    let keys: Vec<String> = instances
                        .iter()
                        .filter_map(|i| i.mapping_key.clone())
                        .collect();
                    mapped_records.push(MappedStepRecord { idx, keys });
                    mapped_groups.push((instances, max_concurrency));
                }

                StepAction::Execute => {
                    let step = &ctx.scope.plan.steps[idx];
                    let node = ctx.repo.node_map.get(&step.name).unwrap();
                    let pools = ctx.step_pools(&step.name);
                    singles.push(StepInstance {
                        idx,
                        instance_name: step.name.clone(),
                        mapping_key: None,
                        event_names: step.event_names().to_vec(),
                        // Collect overrides are resolved by the backend itself
                        // (in-process / async load in-memory; parallel + k8s
                        // build their own serializable specs).
                        input_overrides: HashMap::new(),
                        fan_out: None,
                        is_async: node.is_async(),
                        pools,
                    });
                }
            }
        }

        Python::attach(|py| {
            if !singles.is_empty() {
                backend.run_instances(py, ctx, singles, None, &mut failures);
            }
            for (instances, max_conc) in mapped_groups {
                backend.run_instances(py, ctx, instances, max_conc, &mut failures);
            }
        });

        finalize_mapped_steps(ctx, &mapped_records);

        failures
    })
}

/// One mapped step's roll-up record: which step + the mapping_keys we built
/// instances for. We check `state.failed_names` for each instance after
/// `run_instances` to decide success vs failure for the mapped step.
struct MappedStepRecord {
    idx: usize,
    keys: Vec<String>,
}

fn finalize_mapped_steps(ctx: &mut BatchContext, records: &[MappedStepRecord]) {
    for rec in records {
        let step = &ctx.scope.plan.steps[rec.idx];
        let mut successful = Vec::with_capacity(rec.keys.len());
        let mut any_failed = false;
        for key in &rec.keys {
            let instance_name = format!("{}__{}", step.name, key);
            if ctx.state.was_failed(&instance_name) {
                any_failed = true;
            } else {
                successful.push(key.clone());
            }
        }
        if any_failed {
            ctx.emit_step_failure(&step.name, "One or more map instances failed", None);
            ctx.state.mark_failed(step.name.clone());
        } else {
            ctx.emit_success(&step.name);
            ctx.state.record_mapped_keys(step.name.clone(), successful);
        }
    }
}

/// Load a fan-out source, resolve mapping keys, and build a `StepInstance`
/// per mapped instance. Returns `None` (after recording a failure for the
/// mapped step) if source loading or iteration fails.
fn build_mapped_instances(
    py: Python,
    ctx: &mut BatchContext,
    idx: usize,
    fan_out_source: &str,
    failures: &mut Vec<(String, PyErr)>,
) -> Option<Vec<StepInstance>> {
    let step = &ctx.scope.plan.steps[idx];
    let node = ctx
        .repo
        .node_map
        .get(&step.name)
        .expect("mapped step must be in node_map");
    let source_node = ctx
        .repo
        .node_map
        .get(fan_out_source)
        .expect("fan-out source must be in node_map");

    let pools = ctx.step_pools(&step.name);
    let is_async = node.is_async();

    let source_output = match ops::load_fan_out_source(
        py,
        fan_out_source,
        source_node,
        ctx.scope.partition_key,
        ctx.repo.io_handler_registry,
    ) {
        Ok(v) => v,
        Err(e) => {
            ctx.record_failure_no_hooks(
                &step.name,
                ExecutionError::new_err(format!("Failed to load fan-out source: {e}")),
                failures,
            );
            return None;
        }
    };

    let iter = match source_output.bind(py).try_iter() {
        Ok(it) => it,
        Err(e) => {
            ctx.record_failure_no_hooks(
                &step.name,
                ExecutionError::new_err(format!("Fan-out source is not iterable: {e}")),
                failures,
            );
            return None;
        }
    };
    let mut items: Vec<Py<PyAny>> = Vec::new();
    for item in iter {
        match item {
            Ok(v) => items.push(v.unbind()),
            Err(e) => {
                ctx.record_failure_no_hooks(
                    &step.name,
                    ExecutionError::new_err(format!("Failed to iterate fan-out source: {e}")),
                    failures,
                );
                return None;
            }
        }
    }

    let instances = py.detach(|| {
        let predefined_keys = ops::resolve_predefined_keys(
            fan_out_source,
            ctx.scope.partition_key,
            ctx.state.step_dynamic_keys,
            ctx.state.data_versions,
            ctx.sink.storage,
        );

        let build_instance = |mapping_key: String, value: Py<PyAny>| -> StepInstance {
            let instance_name = format!("{}__{}", step.name, mapping_key);
            StepInstance {
                idx,
                instance_name: instance_name.clone(),
                mapping_key: Some(mapping_key),
                event_names: vec![instance_name],
                input_overrides: HashMap::new(),
                fan_out: Some((fan_out_source.to_string(), value)),
                is_async,
                pools: pools.clone(),
            }
        };

        if let Some(keys) = predefined_keys {
            items
                .into_iter()
                .enumerate()
                .map(|(i, item)| build_instance(keys[i].clone(), item))
                .collect()
        } else {
            // Each item may be a `DynamicOutput` PyClass we need to extract.
            Python::attach(|py| {
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, item)| {
                        let (mapping_key, value) = ops::extract_mapping_key(py, i, &item);
                        build_instance(mapping_key, value)
                    })
                    .collect()
            })
        }
    });
    Some(instances)
}

/// Resolve collect/collect_stream input overrides by loading mapped outputs in-memory.
/// Callers must pass a pre-built `step_by_name` map (see `build_step_by_name`) so the
/// O(N) build is hoisted out of per-step loops.
pub(crate) fn resolve_collect_overrides<'a>(
    py: Python,
    step: &'a ExecutionStep,
    ctx: &'a BatchContext,
    step_by_name: &HashMap<&str, &ExecutionStep>,
) -> Result<HashMap<String, Py<PyAny>>, (String, PyErr)> {
    use rivers_core::execution::plan::StepKind;

    let mut overrides: HashMap<String, Py<PyAny>> = HashMap::new();
    for dep_name in &step.plan_dependencies {
        let dep_step = match step_by_name.get(dep_name.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let result = match &dep_step.kind {
            StepKind::Collect { mapped_step } => ops::collect_mapped_outputs(
                py,
                mapped_step,
                ctx.repo.node_map,
                ctx.scope.partition_key,
                ctx.repo.io_handler_registry,
                ctx.state.mapped_instance_keys,
            ),
            StepKind::CollectStream {
                mapped_step,
                ordered,
            } => ops::collect_mapped_stream(
                py,
                mapped_step,
                *ordered,
                ctx.repo.node_map,
                ctx.scope.partition_key,
                ctx.repo.io_handler_registry,
                ctx.state.mapped_instance_keys,
            ),
            _ => continue,
        };
        match result {
            Ok(v) => {
                overrides.insert(dep_step.name.clone(), v);
            }
            Err(e) => {
                let msg = format!("Failed to resolve collect input '{}': {e}", dep_step.name);
                let err = crate::errors::ExecutionError::new_err(msg.clone());
                return Err((msg, err));
            }
        }
    }
    Ok(overrides)
}
