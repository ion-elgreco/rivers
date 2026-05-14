//! Step classification — determines which executor backend handles each step.
use pyo3::prelude::*;
use rivers_core::execution::plan::StepKind;

use crate::errors::ExecutionError;

use super::super::ops::{self, now_ts};
use super::context::BatchContext;

/// What the executor should do with a given step after pre-dispatch classification.
pub(crate) enum StepAction {
    /// Step was fully handled (dep-fail skip, collect, graph asset). Continue to next.
    Handled,
    /// Step is a mapped fan-out — needs special handling by the caller.
    Mapped { fan_out_source: String },
    /// Step is ready for normal execution.
    Execute,
}

/// Classify a step: handle dep-fail, collect/graph/step_start, emit events.
/// Returns what the caller should do next.
pub(crate) fn classify_step(
    step_idx: usize,
    ctx: &mut BatchContext,
    failures: &mut Vec<(String, PyErr)>,
) -> StepAction {
    let step = &ctx.scope.plan.steps[step_idx];
    let ts = now_ts();

    if ctx.scope.completed_steps.contains(&step.name) {
        tracing::debug!(step = %step.name, "Skipping already-completed step (resume)");
        return StepAction::Handled;
    }

    let dep_failed = step
        .plan_dependencies
        .iter()
        .any(|d| ctx.state.was_failed(d));
    if dep_failed {
        let msg = "Skipped: upstream dependency failed";
        for name in step.event_names() {
            ctx.record_failure_no_hooks(name, ExecutionError::new_err(msg.to_string()), failures);
        }
        // Graph asset coordinator step: fire failure hooks. Graph assets are
        // pure composition (no code runs for them), so the dep-fail path is
        // their only failure surface — without this, hooks defined via
        // `Asset.from_graph(hooks=[…])` would never fire on internal-task
        // failures.
        if let Some(node) = ctx.repo.node_map.get(&step.name)
            && node.is_graph_asset()
            && node.has_failure_hooks()
        {
            Python::attach(|py| {
                ops::run_failure_hooks(
                    py,
                    node,
                    &step.name,
                    ctx.scope.run_id,
                    msg,
                    node.metadata(),
                    None,
                );
            });
        }
        return StepAction::Handled;
    }

    if let StepKind::Mapped {
        ref fan_out_source, ..
    } = step.kind
    {
        return StepAction::Mapped {
            fan_out_source: fan_out_source.clone(),
        };
    }

    // Collect/CollectStream — virtual, just emit events
    if matches!(
        step.kind,
        StepKind::Collect { .. } | StepKind::CollectStream { .. }
    ) {
        ctx.emit_start(&step.name, ts);
        ctx.emit_success(&step.name);
        return StepAction::Handled;
    }

    let node = ctx
        .repo
        .node_map
        .get(&step.name)
        .expect("step must be in node_map — invalid plan");

    // Graph assets are composition-only
    if node.is_graph_asset() {
        if !ctx.state.was_failed(&step.name) {
            ctx.emit_materialization(
                &step.name,
                &[],
                None,
                ops::collect_input_data_versions(ctx.state.data_versions, &step.graph_dependencies),
                ts,
            );
            ctx.emit_success(&step.name);
            // Graph asset success hooks: fire here since there's no per-step
            // execution path that would otherwise run them. Output is None
            // because the graph asset's value is written via the dual-IO path
            // alongside the final task — re-loading it for the hook would add
            // an IO round-trip per fire.
            if node.has_success_hooks() {
                Python::attach(|py| {
                    ops::run_success_hooks(
                        py,
                        node,
                        &step.name,
                        ctx.scope.run_id,
                        &py.None(),
                        node.metadata(),
                        None,
                    );
                });
            }
        }
        return StepAction::Handled;
    }

    // Emit step_start for parent graph asset (coordinator-only, no pod runs for this)
    if let Some((graph_name, _)) = step.name.split_once('/')
        && ctx.state.mark_graph_started(graph_name.to_string())
    {
        ctx.emit_start(graph_name, ts);
    }

    // NOTE: StepStart for the step itself is NOT emitted here.
    // Each backend is responsible for emitting StepStart before execution.
    // This avoids duplicate events when the K8s backend dispatches to pods
    // that run their own execute_plan (which would emit StepStart again).

    StepAction::Execute
}
