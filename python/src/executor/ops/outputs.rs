//! Per-output iteration over a step result.
//!
//! Splits a step's `StepResult` into per-output items in one place,
//! regardless of whether the asset returned a single value (`name == step.name`),
//! a dict (`{output_name: value, ...}`), or a generator (`yield Output(...)`).
//! A multi-asset output's value is checked here against its
//! `AssetDef.type_hint`, so every executor checks it the same way.
//!
//! Consumers receive [`OutputItem`]s with all metadata and asset-declared
//! `data_version` sources already merged with the right precedence:
//!
//! ```text
//! per-output Output(...)/Observation(...) wrapper
//!     > generator-context (gen-only)
//!     > step-shared (step_result.output_metadata / data_version)
//!     > legacy DataVersion-in-metadata fallback
//!     > the validator of the output's type hint (metadata only)
//! ```
//!
//! `OutputItem.metadata` is stripped of any `MetadataValue::DataVersion`
//! entries (those are folded into `data_version`). What the IO handler sets
//! during the write is NOT considered here — for a Materialization, the
//! consumer overlays it after `write_output`: the handler's metadata wins via
//! `merge_metadata`, its data_version via `.or(item.data_version)`.
//!
//! ## Construction discipline
//!
//! `OutputItem`'s variants are constructible only from inside this module
//! (private fields). Consumers can `match` and read but cannot fabricate a
//! partially-merged `OutputItem`; a `merge_metadata` call outside this
//! module, other than that handler overlay, is skipping the contract.

use std::collections::HashMap;

use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

use crate::context::asset::PyAssetExecutionContext;
use crate::errors::ExecutionError;
use crate::metadata::MetadataValue;
use crate::result_types::{self, ResultKind};
use crate::runtime::rt;

use super::StepResult;
use super::annotations::validate_return_type;
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
/// `selected_outputs` is the multi-asset's outputs (`step.outputs`), each
/// with its `AssetDef.type_hint`. Empty means single-output: one item is
/// yielded with `name == step_name`. Non-empty means multi-asset: one item
/// per selected output (subsetting), non-selected generator yields silently
/// skipped. An item carries its value as checked against the type hint, e.g.
/// a frame that pandera coerced; a value that fails the check ends the
/// iteration with `AssetOutputValidationError`.
///
/// `step_name` equals `step.name` for non-mapped runs and the per-instance
/// name for mapped fan-out (single-output only — multi-asset is never
/// mapped). Multi-asset paths ignore `step_name`.
///
/// On `Err` from `on_item` (or from a generator), iteration short-circuits
/// and the error propagates.
pub(crate) fn for_each_output<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[(String, Option<Py<PyAny>>)],
    step_name: &str,
    mut on_item: F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    match &step_result.generator {
        Some(kind) => {
            for_each_output_generator(py, step_result, selected_outputs, kind, &mut on_item)
        }
        None if !selected_outputs.is_empty() => {
            for_each_output_dict(py, step_result, selected_outputs, &mut on_item)
        }
        None => for_each_output_single(py, step_result, step_name, &mut on_item),
    }
}

/// Checks an `Output` value against its output's type hint. Returns the value
/// to store, and `metadata` over the entries of the hint's validator.
fn check_output(
    py: Python,
    name: &str,
    kind: ResultKind,
    value: Option<Py<PyAny>>,
    hint: Option<&Py<PyAny>>,
    metadata: Vec<(String, MetadataValue)>,
) -> PyResult<(Option<Py<PyAny>>, Vec<(String, MetadataValue)>)> {
    let (ResultKind::Output, Some(hint)) = (kind, hint) else {
        return Ok((value, metadata));
    };
    let value = value.unwrap_or_else(|| py.None());
    let checked = validate_return_type(py, &value, Some(hint), name)?;
    let mut merged = checked.metadata;
    merge_metadata(&mut merged, &metadata);
    Ok((Some(checked.value), merged))
}

fn for_each_output_generator<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[(String, Option<Py<PyAny>>)],
    kind: &super::GeneratorType,
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    match kind {
        super::GeneratorType::Sync { context } => {
            let generator = &step_result.result;
            let next = |py: Python| match generator.call_method0(py, "__next__") {
                Ok(v) => Ok(Some(v)),
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => Ok(None),
                Err(e) => Err(e),
            };
            for_each_yield(py, selected_outputs, context.as_ref(), next, on_item)
        }
        // Recorded as its worker ran the body.
        super::GeneratorType::Async { error, .. } => {
            error.as_ref().map_or(Ok(()), |e| Err(e.clone_ref(py)))
        }
    }
}

/// Run an async generator to its end on this thread, the step's worker,
/// calling `on_item` for each output as the body yields it. The body goes on
/// only when `on_item` returns `Ok`.
pub(crate) fn run_async_generator<F>(
    py: Python,
    generator: &Py<PyAny>,
    selected_outputs: &[(String, Option<Py<PyAny>>)],
    context: Option<&Py<PyAny>>,
    locals: &TaskLocals,
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let next = |py: Python| {
        let anext = generator.call_method0(py, "__anext__")?;
        let future = pyo3_async_runtimes::into_future_with_locals(locals, anext.into_bound(py))?;
        match py.detach(|| rt().block_on(future)) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.is_instance_of::<PyStopAsyncIteration>(py) => Ok(None),
            Err(e) => Err(e),
        }
    };
    for_each_yield(py, selected_outputs, context, next, on_item)
}

/// One item per selected output a generator yields, as `next` returns them.
fn for_each_yield<F>(
    py: Python,
    selected_outputs: &[(String, Option<Py<PyAny>>)],
    context: Option<&Py<PyAny>>,
    mut next: impl FnMut(Python) -> PyResult<Option<Py<PyAny>>>,
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let selected: HashMap<&str, Option<&Py<PyAny>>> = selected_outputs
        .iter()
        .map(|(name, hint)| (name.as_str(), hint.as_ref()))
        .collect();
    let gen_ctx = context.and_then(|c| {
        c.bind(py)
            .cast::<PyAssetExecutionContext>()
            .ok()
            .map(|bound| bound.clone().unbind())
    });

    while let Some(yielded) = next(py)? {
        let extracted = result_types::try_extract_result_type(py, &yielded)?;
        let ext = extracted.ok_or_else(|| {
            ExecutionError::new_err(
                "Generator multi-asset must yield Output(...), Observation(...), or Materialization(...)",
            )
        })?;
        let output_name = ext.output_name.ok_or_else(|| {
            ExecutionError::new_err("yield in multi-asset must include output_name")
        })?;

        let Some(&hint) = selected.get(output_name.as_str()) else {
            continue;
        };

        // Cumulative ctx-level metadata (peek, not drain) + per-yield ctx dv (drain).
        let ctx_metadata = gen_ctx
            .as_ref()
            .map(|c| c.borrow(py).peek_output_metadata(py))
            .unwrap_or_default();
        let ctx_dv = gen_ctx
            .as_ref()
            .and_then(|c| c.borrow(py).drain_data_version());

        // Merge precedence: per-yield > ctx (overlay-wins on each merge).
        // Note: the gen path historically does NOT incorporate
        // step_result.output_metadata as a shared layer — only ctx-set.
        let mut merged_metadata = ctx_metadata;
        merge_metadata(&mut merged_metadata, &ext.metadata);
        let (value, merged_metadata) =
            check_output(py, &output_name, ext.kind, ext.value, hint, merged_metadata)?;
        let (legacy_dv, stripped_metadata) = extract_data_version(&merged_metadata);

        // dv precedence: per-yield > ctx > legacy-from-metadata.
        let resolved_dv = ext.data_version.clone().or(ctx_dv).or(legacy_dv);

        let item = item_for_kind(
            py,
            ext.kind,
            output_name,
            value,
            stripped_metadata,
            resolved_dv,
        );
        on_item(py, item)?;
    }
    Ok(())
}

fn for_each_output_dict<F>(
    py: Python,
    step_result: &StepResult,
    selected_outputs: &[(String, Option<Py<PyAny>>)],
    on_item: &mut F,
) -> PyResult<()>
where
    F: FnMut(Python, OutputItem) -> PyResult<()>,
{
    let result = &step_result.result;
    let shared_metadata = &step_result.output_metadata;
    let shared_dv = step_result.data_version.as_deref();

    for (output_name, hint) in selected_outputs {
        let sliced = result.call_method1(py, "__getitem__", (output_name.as_str(),))?;

        let (value, per_metadata, per_dv, kind) =
            match result_types::try_extract_result_type(py, &sliced)? {
                Some(ext) => (ext.value, ext.metadata, ext.data_version, ext.kind),
                None => (Some(sliced), Vec::new(), None, ResultKind::Output),
            };

        let mut merged_metadata = shared_metadata.clone();
        merge_metadata(&mut merged_metadata, &per_metadata);
        let (value, merged_metadata) =
            check_output(py, output_name, kind, value, hint.as_ref(), merged_metadata)?;
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
