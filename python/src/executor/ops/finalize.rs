//! Step lifecycle event emission, hook execution, and dv resolution helpers.
//!
//! Per-output finalization (Materialization/Observation events + hooks +
//! dv recording) lives in `dispatch/results.rs` as the unified consumer
//! body that runs around `ops::for_each_output`. This module supplies the
//! lower-level building blocks: per-event emitters, hook runners,
//! `resolve_data_version`, `extract_data_version`, etc.
use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_core::storage::{AssetRecord, EventRecord, EventType, ScopedStorageHandle};
pub(crate) use rivers_core::util::now_ts;

use crate::executor::event_writer::EventWriter;
use crate::metadata::MetadataValue;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

use crate::context::hook::PyHookContext;
use rivers_core::storage::surrealdb_backend::SurrealStorage;

fn emit_step_event(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    event_type: EventType,
    metadata: Vec<(String, String)>,
    ts: i64,
    partition_key: Option<&PyPartitionKey>,
) {
    writer.emit(EventRecord {
        code_location_id: String::new(),
        event_type,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: partition_key.map(|k| k.into()),
        timestamp: ts,
        metadata,
        input_data_versions: vec![],
    });
}

pub(crate) fn emit_step_start(writer: &EventWriter, run_id: &str, step_name: &str, ts: i64) {
    emit_step_event(
        writer,
        run_id,
        step_name,
        EventType::StepStart,
        Vec::new(),
        ts,
        None,
    );
}

/// Emit `StepStart` directly via an `EventWriter`'s sender clone. Use this
/// from spawned tasks (Tokio JoinSet, loky callback) where the orchestrator's
/// `&EventWriter` isn't available — typically right after a successful pool
/// claim, so the event reflects the actual moment execution begins.
pub(crate) fn emit_step_start_via_tx(
    tx: &tokio::sync::mpsc::UnboundedSender<EventRecord>,
    code_location_id: &str,
    run_id: &str,
    step_name: &str,
    ts: i64,
) {
    let _ = tx.send(EventRecord {
        code_location_id: code_location_id.to_string(),
        event_type: EventType::StepStart,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: Vec::new(),
        input_data_versions: vec![],
    });
}

pub(crate) fn emit_step_success(writer: &EventWriter, run_id: &str, step_name: &str, ts: i64) {
    emit_step_event(
        writer,
        run_id,
        step_name,
        EventType::StepSuccess,
        Vec::new(),
        ts,
        None,
    );
}

pub(crate) fn emit_step_failure(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    error_msg: &str,
    ts: i64,
) {
    emit_step_event(
        writer,
        run_id,
        step_name,
        EventType::StepFailure,
        vec![("error".to_string(), error_msg.to_string())],
        ts,
        None,
    );
}

pub(crate) fn emit_step_retry(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    attempt: u32,
    reason: rivers_core::execution::retry::FailureReason,
    delay: std::time::Duration,
    ts: i64,
) {
    use rivers_core::execution::retry::meta;
    emit_step_event(
        writer,
        run_id,
        step_name,
        EventType::StepRetry,
        vec![
            (meta::ATTEMPT.to_string(), attempt.to_string()),
            (meta::REASON.to_string(), reason.as_str().to_string()),
            (
                meta::NEXT_DELAY_MS.to_string(),
                delay.as_millis().to_string(),
            ),
        ],
        ts,
        None,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_step_retry_via_tx(
    tx: &tokio::sync::mpsc::UnboundedSender<EventRecord>,
    code_location_id: &str,
    run_id: &str,
    step_name: &str,
    attempt: u32,
    reason: rivers_core::execution::retry::FailureReason,
    delay: std::time::Duration,
    ts: i64,
) {
    use rivers_core::execution::retry::meta;
    let _ = tx.send(EventRecord {
        code_location_id: code_location_id.to_string(),
        event_type: EventType::StepRetry,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![
            (meta::ATTEMPT.to_string(), attempt.to_string()),
            (meta::REASON.to_string(), reason.as_str().to_string()),
            (
                meta::NEXT_DELAY_MS.to_string(),
                delay.as_millis().to_string(),
            ),
        ],
        input_data_versions: vec![],
    });
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_log_output_via_tx(
    tx: &tokio::sync::mpsc::UnboundedSender<EventRecord>,
    code_location_id: &str,
    run_id: &str,
    step_name: &str,
    stdout: &str,
    stderr: &str,
    logs: &str,
    ts: i64,
) {
    if stdout.is_empty() && stderr.is_empty() && logs.is_empty() {
        return;
    }
    let mut metadata = Vec::new();
    if !stdout.is_empty() {
        metadata.push(("stdout".to_string(), stdout.to_string()));
    }
    if !stderr.is_empty() {
        metadata.push(("stderr".to_string(), stderr.to_string()));
    }
    if !logs.is_empty() {
        metadata.push(("logs".to_string(), logs.to_string()));
    }
    let _ = tx.send(EventRecord {
        code_location_id: code_location_id.to_string(),
        event_type: EventType::LogOutput,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata,
        input_data_versions: vec![],
    });
}

pub(crate) fn emit_partition_failure(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    partition_key: &PyPartitionKey,
    error: &str,
    ts: i64,
) {
    emit_step_event(
        writer,
        run_id,
        step_name,
        EventType::StepFailure,
        vec![("error".to_string(), error.to_string())],
        ts,
        Some(partition_key),
    );
}

pub(crate) fn emit_log_output(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    stdout: &str,
    stderr: &str,
    logs: &str,
    ts: i64,
) {
    if stdout.is_empty() && stderr.is_empty() && logs.is_empty() {
        return;
    }
    let mut metadata = Vec::new();
    if !stdout.is_empty() {
        metadata.push(("stdout".to_string(), stdout.to_string()));
    }
    if !stderr.is_empty() {
        metadata.push(("stderr".to_string(), stderr.to_string()));
    }
    if !logs.is_empty() {
        metadata.push(("logs".to_string(), logs.to_string()));
    }
    writer.emit(EventRecord {
        code_location_id: String::new(),
        event_type: EventType::LogOutput,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata,
        input_data_versions: vec![],
    });
}

/// `input_data_versions` carries `(dep_name, data_version)` pairs captured at
/// read time by the executor — not looked up from storage after the fact.
fn emit_event(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    partition_key: &Option<PyPartitionKey>,
    output_metadata: &[(String, MetadataValue)],
    event_type: EventType,
    input_data_versions: Vec<(String, String)>,
    ts: i64,
) {
    let pk = partition_key.as_ref().map(|k| k.into());
    let metadata: Vec<(String, String)> = output_metadata
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                serde_json::to_string(v).unwrap_or_else(|_| format!("{:?}", v)),
            )
        })
        .collect();
    writer.emit(EventRecord {
        code_location_id: String::new(),
        event_type,
        asset_key: Some(step_name.to_string()),
        run_id: run_id.to_string(),
        partition_key: pk,
        timestamp: ts,
        metadata,
        input_data_versions,
    });
}

pub(crate) fn emit_materialization(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    partition_key: &Option<PyPartitionKey>,
    output_metadata: &[(String, MetadataValue)],
    data_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
    ts: i64,
) {
    emit_event(
        writer,
        run_id,
        step_name,
        partition_key,
        output_metadata,
        EventType::Materialization { data_version },
        input_data_versions,
        ts,
    );
}

pub(crate) fn emit_observation(
    writer: &EventWriter,
    run_id: &str,
    step_name: &str,
    partition_key: &Option<PyPartitionKey>,
    output_metadata: &[(String, MetadataValue)],
    data_version: Option<String>,
    ts: i64,
) {
    emit_event(
        writer,
        run_id,
        step_name,
        partition_key,
        output_metadata,
        EventType::Observation { data_version },
        vec![],
        ts,
    );
}

pub(crate) fn register_assets_from_nodes(
    storage: &ScopedStorageHandle<SurrealStorage>,
    node_map: &HashMap<String, ResolvedNode>,
    py: Python,
) {
    let code_location_id = storage.code_location_id().to_string();
    let records: Vec<AssetRecord> = node_map
        .iter()
        .map(|(name, node)| AssetRecord {
            code_location_id: code_location_id.clone(),
            asset_key: name.clone(),
            tags: node.tags().unwrap_or_default(),
            kinds: node.kinds(),
            asset_group: node.group(),
            code_version: node.code_version(),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: node.pool(),
        })
        .collect();
    py.detach(|| {
        let _ = io_rt().block_on(storage.scoped().register_assets(&records));
    });
}

/// Extract any DataVersion from output metadata, removing it from the list.
/// Returns (data_version, remaining_metadata).
pub(crate) fn extract_data_version(
    output_metadata: &[(String, MetadataValue)],
) -> (Option<String>, Vec<(String, MetadataValue)>) {
    let mut data_version = None;
    let mut remaining = Vec::new();
    for (k, v) in output_metadata {
        if let MetadataValue::DataVersion { value } = v {
            data_version = Some(value.clone());
        } else {
            remaining.push((k.clone(), v.clone()));
        }
    }
    (data_version, remaining)
}

/// Look up the data versions for upstream dependencies from an in-memory map.
/// The map is maintained by the executor as each step completes, ensuring
/// we capture the versions that were actually available at read time (not
/// what storage might have after a concurrent write).
pub(crate) fn collect_input_data_versions(
    data_versions: &HashMap<String, String>,
    dependencies: &[String],
) -> Vec<(String, String)> {
    let mut versions = Vec::with_capacity(dependencies.len());
    for dep in dependencies {
        if let Some(dv) = data_versions.get(dep) {
            versions.push((dep.clone(), dv.clone()));
        }
    }
    versions
}

/// Hook errors are logged but do NOT fail the step.
fn run_hooks(
    py: Python,
    hooks: &[Py<crate::hooks::PyHook>],
    step_name: &str,
    status: &str,
    ctx: &Py<PyAny>,
) {
    for hook in hooks {
        let hook_ref = hook.borrow(py);
        if let Some(func) = hook_ref.func() {
            let func = func.clone_ref(py);
            let name = hook_ref.resolve_name().to_owned();
            drop(hook_ref);
            if let Err(e) = func.call1(py, (ctx.clone_ref(py),)) {
                tracing::warn!(
                    target: "rivers::executor",
                    hook = %name,
                    asset = %step_name,
                    error = %e,
                    "{status} hook failed"
                );
            }
        }
    }
}

pub(crate) fn run_success_hooks(
    py: Python,
    node: &ResolvedNode,
    step_name: &str,
    run_id: &str,
    output: &Py<PyAny>,
    metadata: Option<HashMap<String, String>>,
    config_instance: Option<Py<PyAny>>,
) {
    let hooks = node.success_hooks();
    if hooks.is_empty() {
        return;
    }
    let ctx = match Py::new(
        py,
        PyHookContext::new(
            step_name.to_string(),
            run_id.to_string(),
            "success".to_string(),
            Some(output.clone_ref(py)),
            None,
            metadata,
        )
        .with_config(config_instance),
    ) {
        Ok(c) => c.into_any(),
        Err(_) => return,
    };
    run_hooks(py, hooks, step_name, "success", &ctx);
}

pub(crate) fn run_failure_hooks(
    py: Python,
    node: &ResolvedNode,
    step_name: &str,
    run_id: &str,
    error_msg: &str,
    metadata: Option<HashMap<String, String>>,
    config_instance: Option<Py<PyAny>>,
) {
    let hooks = node.failure_hooks();
    if hooks.is_empty() {
        return;
    }
    let ctx = match Py::new(
        py,
        PyHookContext::new(
            step_name.to_string(),
            run_id.to_string(),
            "failure".to_string(),
            None,
            Some(error_msg.to_string()),
            metadata,
        )
        .with_config(config_instance),
    ) {
        Ok(c) => c.into_any(),
        Err(_) => return,
    };
    run_hooks(py, hooks, step_name, "failure", &ctx);
}
