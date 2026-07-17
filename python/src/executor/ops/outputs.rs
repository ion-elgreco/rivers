//! Per-output iteration over a step result.
//!
//! Splits a step's `StepResult` into per-output items in one place,
//! regardless of whether the asset returned a single value (`name == step.name`),
//! a dict (`{output_name: value, ...}`), or a generator (`yield Output(...)`).
//!
//! Consumers receive [`OutputItem`]s with all metadata and asset-declared
//! `data_version` sources already merged with the right precedence:
//!
//! ```text
//! per-output Output(...)/Observation(...) wrapper
//!     > generator-context (gen-only)
//!     > step-shared (step_result.output_metadata / data_version)
//!     > legacy DataVersion-in-metadata fallback
//! ```
//!
//! `OutputItem.metadata` is stripped of any `MetadataValue::DataVersion`
//! entries (those are folded into `data_version`). The IO-handler-supplied
//! data_version is NOT considered here — for a Materialization, the consumer
//! still wins via `Some(io_dv).or(item.data_version)` after `handle_step_output`.
//!
//! ## Construction discipline
//!
//! `OutputItem`'s variants are constructible only from inside this module
//! (private fields). Consumers can `match` and read but cannot fabricate a
//! partially-merged `OutputItem`; a `merge_metadata` call outside this
//! module is skipping the contract.

use std::collections::HashSet;

use pyo3::prelude::*;

use crate::context::asset::PyAssetExecutionContext;
use crate::errors::ExecutionError;
use crate::metadata::MetadataValue;
use crate::result_types::{self, ResultKind};

use super::super::async_exec::AsyncBridge;
use super::StepResult;
use super::extract_data_version;
use super::merge_metadata;

/// One output produced by a step. Constructed only by `for_each_output`.
///
/// `name` is the per-output asset name. For single-output non-multi steps,
/// `name == step.name`. For dict / generator multi-asset steps, `name` is
/// one entry from `step.outputs`.
pub(crate) enum OutputItem {
    /// Emit a Materialization event. `value.is_some()` means the framework
    /// must `handle_output` the value via the IO handler (the `Output(...)`
    /// return type); `value.is_none()` means the asset persisted itself
    /// (the `Materialization(...)` return type) and no IO call is needed.
    Materialization {
        name: String,
        /// `Some(v)` for `Output(v)`; `None` for `Materialization(...)`.
        value: Option<Py<PyAny>>,
        /// Per-output metadata merged with all higher-precedence sources,
        /// `MetadataValue::DataVersion` entries removed (folded into
        /// `data_version`).
        metadata: Vec<(String, MetadataValue)>,
        /// Asset-declared `data_version`. The IO handler may supply a
        /// higher-precedence value at write time; consumers should combine
        /// via `Some(io_dv).or(item.data_version)`.
        data_version: Option<String>,
    },
    /// `Observation(...)` — emit Observation event only.
    Observation {
        name: String,
        metadata: Vec<(String, MetadataValue)>,
        data_version: Option<String>,
    },
}

/// Build an `OutputItem` from the discriminator. The mapping is the same in
/// every iterator (`for_each_output_*`); centralising it keeps the three
/// sites in lock-step when a new variant is added.
fn item_for_kind(
    py: Python,
    kind: ResultKind,
    name: String,
    value: Option<Py<PyAny>>,
    metadata: Vec<(String, MetadataValue)>,
    data_version: Option<String>,
) -> OutputItem {
    match kind {
        ResultKind::Observation => OutputItem::Observation {
            name,
            metadata,
            data_version,
        },
        ResultKind::Materialization => OutputItem::Materialization {
            name,
            value: None,
            metadata,
            data_version,
        },
        ResultKind::Output => OutputItem::Materialization {
            name,
            value: Some(value.unwrap_or_else(|| py.None())),
            metadata,
            data_version,
        },
    }
}

/// Iterate over the outputs of a step result, calling `on_item` once per
/// selected output. Handles single-output, dict multi-asset, and generator
/// multi-asset (sync + async) shapes uniformly.
///
/// `selected_outputs` is the multi-asset's output name list (`step.outputs`).
/// Empty means single-output: one item is yielded with `name == step_name`.
/// Non-empty means multi-asset: one item per selected output (subsetting),
/// non-selected generator yields silently skipped.
///
/// `step_name` equals `step.name` for non-mapped runs and the per-instance
/// name for mapped fan-out (single-output only — multi-asset is never
/// mapped). Multi-asset paths ignore `step_name`.
///
/// On `Err` from `on_item` (or from a generator's `__next__` / `__anext__`),
/// iteration short-circuits and the error propagates.
///
/// `bridge` is required for async generators; pass `None` for sync paths.
pub(crate) fn for_each_output<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[String],
    step_name: &str,
    bridge: Option<&AsyncBridge>,
    mut on_item: F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    match &step_result.generator {
        Some(kind) => for_each_output_generator(
            py,
            step_result,
            selected_outputs,
            kind,
            bridge,
            &mut on_item,
        ),
        None if !selected_outputs.is_empty() => {
            for_each_output_dict(py, step_result, selected_outputs, &mut on_item)
        }
        None => for_each_output_single(py, step_result, step_name, &mut on_item),
    }
}

fn for_each_output_generator<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[String],
    kind: &super::GeneratorType,
    bridge: Option<&AsyncBridge>,
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let selected: HashSet<&str> = selected_outputs.iter().map(|s| s.as_str()).collect();
    let generator = &step_result.result;
    let (is_async_gen, ctx_obj) = match kind {
        super::GeneratorType::Sync { context } => (false, context.as_ref()),
        super::GeneratorType::Async { context } => (true, context.as_ref()),
    };

    let gen_ctx = ctx_obj.and_then(|c| {
        c.bind(py)
            .cast::<PyAssetExecutionContext>()
            .ok()
            .map(|bound| bound.clone().unbind())
    });

    loop {
        let yielded = match next_yield(py, generator, is_async_gen, bridge)? {
            Some(v) => v,
            None => break,
        };

        let extracted = result_types::try_extract_result_type(py, &yielded)?;
        let ext = extracted.ok_or_else(|| {
            ExecutionError::new_err(
                "Generator multi-asset must yield Output(...), Observation(...), or Materialization(...)",
            )
        })?;
        let output_name = ext.output_name.ok_or_else(|| {
            ExecutionError::new_err("yield in multi-asset must include output_name")
        })?;

        if !selected.contains(output_name.as_str()) {
            continue;
        }

        // Cumulative ctx-level metadata (peek, not drain) + per-yield ctx dv (drain).
        let ctx_metadata = gen_ctx
            .as_ref()
            .map(|c| c.borrow(py).peek_output_metadata())
            .unwrap_or_default();
        let ctx_dv = gen_ctx
            .as_ref()
            .and_then(|c| c.borrow(py).drain_data_version());

        // Merge precedence: per-yield > ctx (overlay-wins on each merge).
        // Note: the gen path historically does NOT incorporate
        // step_result.output_metadata as a shared layer — only ctx-set.
        let mut merged_metadata = ctx_metadata;
        merge_metadata(&mut merged_metadata, &ext.metadata);
        let (legacy_dv, stripped_metadata) = extract_data_version(&merged_metadata);

        // dv precedence: per-yield > ctx > legacy-from-metadata.
        let resolved_dv = ext.data_version.clone().or(ctx_dv).or(legacy_dv);

        let item = item_for_kind(
            py,
            ext.kind,
            output_name,
            ext.value,
            stripped_metadata,
            resolved_dv,
        );
        on_item(py, item)?;
    }
    Ok(())
}

fn next_yield(
    py: Python,
    generator: &Py<PyAny>,
    is_async_gen: bool,
    bridge: Option<&AsyncBridge>,
) -> PyResult<Option<Py<PyAny>>> {
    if is_async_gen {
        let anext_coro = generator.call_method0(py, "__anext__")?;
        let locals = bridge.map(|b| &b.task_locals).ok_or_else(|| {
            ExecutionError::new_err(
                "Async generator requires TaskLocals — this is an internal error",
            )
        })?;
        let future =
            pyo3_async_runtimes::into_future_with_locals(locals, anext_coro.into_bound(py))?;
        match py.detach(|| crate::runtime::rt().block_on(future)) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py) => Ok(None),
            Err(e) => Err(e),
        }
    } else {
        match generator.call_method0(py, "__next__") {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn for_each_output_dict<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[String],
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let result = &step_result.result;
    let shared_metadata = &step_result.output_metadata;
    let shared_dv = step_result.data_version.as_deref();

    for output_name in selected_outputs {
        let sliced = result.call_method1(py, "__getitem__", (output_name.as_str(),))?;

        let (value, per_metadata, per_dv, kind) =
            match result_types::try_extract_result_type(py, &sliced)? {
                Some(ext) => (ext.value, ext.metadata, ext.data_version, ext.kind),
                None => (Some(sliced), Vec::new(), None, ResultKind::Output),
            };

        let mut merged_metadata = shared_metadata.clone();
        merge_metadata(&mut merged_metadata, &per_metadata);
        let (legacy_dv, stripped_metadata) = extract_data_version(&merged_metadata);

        // dv precedence: per-output > shared > legacy-from-metadata.
        let resolved_dv = per_dv.or(shared_dv.map(String::from)).or(legacy_dv);

        let item = item_for_kind(
            py,
            kind,
            output_name.clone(),
            value,
            stripped_metadata,
            resolved_dv,
        );
        on_item(py, item)?;
    }
    Ok(())
}

fn for_each_output_single<F>(
    py: Python,
    step_result: &StepResult,
    step_name: &str,
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let (legacy_dv, stripped_metadata) = extract_data_version(&step_result.output_metadata);
    let resolved_dv = step_result.data_version.clone().or(legacy_dv);

    let item = item_for_kind(
        py,
        step_result.result_kind,
        step_name.to_string(),
        Some(step_result.result.clone_ref(py)),
        stripped_metadata,
        resolved_dv,
    );
    on_item(py, item)
}
