//! PyO3 → daemon-types conversion. Eval functions return Python objects
//! (`PyRunRequest`, `PyBackfillRequest`, `PySkipReason`, `PyScheduleTickResult`,
//! `PySensorTickResult`); this module turns them into the daemon's owned data
//! types (`RunRequestData`, `BackfillRequestData`, `TickOutcome`, `SensorOutcome`)
//! once, so the rest of the daemon never reaches across the GIL again.
use pyo3::prelude::*;
use rivers_core::storage::LaunchedBy;

use super::types::{
    BackfillRequestData, MaterializationRequestData, PrecomputedArgs, RunRequestData,
    SensorOutcome, TickOutcome,
};
use crate::automation::{PyBackfillRequest, PyRunRequest, PySkipReason};

pub(crate) fn assemble_call_args(
    py: Python,
    context: Py<PyAny>,
    pre: &PrecomputedArgs,
) -> Vec<Py<PyAny>> {
    let mut args = Vec::with_capacity(1 + pre.resource_args.len());
    args.push(context);
    for r in &pre.resource_args {
        args.push(r.clone_ref(py));
    }
    args
}

/// Extract `TickOutcome` from parsed schedule result parts. Schedules always
/// require a `job_name`, so the materialization-request list is always empty.
pub(crate) fn extract_tick_outcome_from_parts(
    py: Python,
    reqs: &[Py<PyRunRequest>],
    backfill_reqs: &[Py<PyBackfillRequest>],
    skip: Option<&Py<PySkipReason>>,
    launched_by: &LaunchedBy,
) -> TickOutcome {
    if let Some(skip) = skip {
        TickOutcome::Skipped(skip.borrow(py).message.clone())
    } else {
        TickOutcome::RunRequests(
            extract_run_request_data(py, reqs),
            Vec::new(),
            extract_backfill_request_data(py, backfill_reqs, launched_by),
        )
    }
}

/// Extract `SensorOutcome` from parsed sensor result parts. Splits each
/// `PyRunRequest` into either a `RunRequestData` (when it carries a
/// `job_name`) or a `MaterializationRequestData` (when it doesn't and the
/// sensor declares `asset_selection`). Requests with neither — which the
/// resolve-time validator should keep out — are dropped silently here so a
/// programmatic edge case can't crash the daemon.
pub(crate) fn extract_sensor_outcome_from_parts(
    py: Python,
    reqs: &[Py<PyRunRequest>],
    backfill_reqs: &[Py<PyBackfillRequest>],
    skip: Option<&Py<PySkipReason>>,
    new_cursor: Option<String>,
    default_asset_selection: Option<&[String]>,
    launched_by: &LaunchedBy,
) -> SensorOutcome {
    if let Some(skip) = skip {
        SensorOutcome::Skipped(skip.borrow(py).message.clone(), new_cursor)
    } else {
        let (run_reqs, mat_reqs) =
            split_run_requests(py, reqs, default_asset_selection, launched_by);
        SensorOutcome::RunRequests(
            run_reqs,
            mat_reqs,
            extract_backfill_request_data(py, backfill_reqs, launched_by),
            new_cursor,
        )
    }
}

/// For each `PyRunRequest`, route to either the named-`Job` queue (when it
/// carries a `job_name`) or the materialize queue (when it has no
/// `job_name` and the sensor declares `default_asset_selection`).
fn split_run_requests(
    py: Python,
    reqs: &[Py<PyRunRequest>],
    default_asset_selection: Option<&[String]>,
    launched_by: &LaunchedBy,
) -> (Vec<RunRequestData>, Vec<MaterializationRequestData>) {
    let mut run_reqs: Vec<RunRequestData> = Vec::new();
    let mut mat_reqs: Vec<MaterializationRequestData> = Vec::new();
    for rr in reqs {
        let rr = rr.borrow(py);
        if rr.job_name.is_some() {
            run_reqs.push(RunRequestData {
                run_key: rr.run_key.clone(),
                tags: rr.tags.clone(),
                partition_key: rr
                    .partition_key
                    .clone()
                    .map(|k| crate::partitions::PyPartitionKey::Single { key: vec![k] }),
                job_name: rr.job_name.clone(),
            });
        } else if let Some(sel) = default_asset_selection {
            mat_reqs.push(MaterializationRequestData {
                run_id: uuid::Uuid::new_v4().to_string(),
                asset_selection: sel.to_vec(),
                partition_key: rr.partition_key.as_ref().map(|k| {
                    rivers_core::storage::PartitionKey::Single {
                        keys: vec![k.clone()],
                    }
                }),
                tags: rr
                    .tags
                    .as_ref()
                    .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default(),
                launched_by: launched_by.clone(),
            });
        }
        // else: dropped — resolve-time validation should keep this state out
    }
    (run_reqs, mat_reqs)
}

pub(crate) fn extract_run_request_data(
    py: Python,
    reqs: &[Py<PyRunRequest>],
) -> Vec<RunRequestData> {
    reqs.iter()
        .map(|rr| {
            let rr = rr.borrow(py);
            RunRequestData {
                run_key: rr.run_key.clone(),
                tags: rr.tags.clone(),
                partition_key: rr
                    .partition_key
                    .clone()
                    .map(|k| crate::partitions::PyPartitionKey::Single { key: vec![k] }),
                job_name: rr.job_name.clone(),
            }
        })
        .collect()
}

pub(crate) fn extract_backfill_request_data(
    py: Python,
    reqs: &[Py<PyBackfillRequest>],
    launched_by: &LaunchedBy,
) -> Vec<BackfillRequestData> {
    reqs.iter()
        .map(|br| {
            let br = br.borrow(py);
            BackfillRequestData {
                target: crate::daemon::RunType::Materialization(br.selection.clone()),
                partition_keys: br.partition_keys.clone(),
                partition_range: br.partition_range.clone(),
                strategy: br.strategy.clone(),
                failure_policy: br.failure_policy.clone(),
                max_concurrency: br.max_concurrency,
                tags: br.tags.clone(),
                dry_run: false,
                backfill_id: None,
                launched_by: launched_by.clone(),
            }
        })
        .collect()
}
