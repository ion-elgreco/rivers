//! Server functions for querying run records and their events from storage.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

use crate::types::{EventsPage, RunFilter, RunRecord, RunsPage, RunsSummary, StoredEvent};

/// Latest runs across all code locations. `status` accepts the wire-string
/// form of [`RunStatus`] (`"Success"`, `"Failure"`, `"Started"`,
/// `"NotStarted"`, `"Queued"`); unknown values disable the filter rather
/// than error. `limit` defaults to 50.
#[server]
pub async fn get_runs(
    limit: Option<usize>,
    status: Option<String>,
) -> Result<Vec<RunRecord>, ServerFnError> {
    use rivers_core::storage::{RunStatus, StorageBackend};
    let state = expect_context::<crate::state::AppState>();
    let status_filter = status.and_then(|s| match s.as_str() {
        "Success" => Some(RunStatus::Success),
        "Failure" => Some(RunStatus::Failure),
        "Started" => Some(RunStatus::Started),
        "NotStarted" => Some(RunStatus::NotStarted),
        "Queued" => Some(RunStatus::Queued),
        _ => None,
    });
    state
        .storage
        .get_all_runs(limit.unwrap_or(50), status_filter)
        .await
        .map(|runs| runs.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_run(run_id: String) -> Result<Option<RunRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_run(&run_id)
        .await
        .map(|opt| opt.map(Into::into))
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_runs_by_ids(run_ids: Vec<String>) -> Result<Vec<RunRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_runs_by_ids(&run_ids, None)
        .await
        .map(|runs| runs.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_run_events(run_id: String) -> Result<Vec<StoredEvent>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_events_for_run(&run_id)
        .await
        .map(|evts| evts.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Step events for a run — backs the timeline/DAG.
#[server]
pub async fn get_run_step_events(run_id: String) -> Result<Vec<StoredEvent>, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_run_step_events(&run_id)
        .await
        .map(|evts| evts.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// A run's `LogOutput` events (stdout/stderr/logs) — typically small.
#[server]
pub async fn get_run_log_events(run_id: String) -> Result<Vec<StoredEvent>, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_run_log_events(&run_id)
        .await
        .map(|evts| evts.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// A page of a run's structured (non-log) events, optionally scoped to one
/// asset (the selected step). Backs the run-detail events table.
#[server(input = Json)]
pub async fn get_run_structured_events_page(
    run_id: String,
    asset_key: Option<String>,
    offset: u64,
    limit: u64,
) -> Result<EventsPage, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    let (rows, total) = state
        .storage
        .get_run_structured_events_page(&run_id, asset_key.as_deref(), offset, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    Ok(EventsPage {
        rows: rows.into_iter().map(Into::into).collect(),
        total,
    })
}

/// A page of one asset's events of a single type within a run (e.g.
/// `Materialization`). Backs the run-detail asset drawer.
#[server(input = Json)]
pub async fn get_run_asset_events_page(
    run_id: String,
    asset_key: String,
    event_type: String,
    offset: u64,
    limit: u64,
) -> Result<EventsPage, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    let (rows, total) = state
        .storage
        .get_run_asset_events_page(&run_id, &asset_key, &event_type, offset, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    Ok(EventsPage {
        rows: rows.into_iter().map(Into::into).collect(),
        total,
    })
}

/// Paginated + filtered runs list. Returns the visible page of rows plus the
/// total row count matching the filter (for pagination controls). The input
/// codec is JSON (not the default URL-encoded form) because `RunFilter` is a
/// nested struct — URL form encoding can't round-trip nested objects and the
/// server-side decoder would reject the request with "missing field offset".
#[server(input = Json)]
pub async fn get_runs_page(
    offset: u64,
    limit: u64,
    filter: RunFilter,
) -> Result<RunsPage, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_all_runs_page(offset, limit, &filter.into())
        .await
        .map(Into::into)
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Aggregate run counts (total, by status, last 24h) for the runs-list header.
#[server]
pub async fn get_runs_summary() -> Result<RunsSummary, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    let cutoff = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(0)
        .saturating_sub(86_400_000_000_000);
    state
        .storage
        .get_all_runs_summary(cutoff)
        .await
        .map(Into::into)
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Returns the most recent run per job, for the given list of job names.
/// Jobs with no runs are omitted. Used by the jobs-list page to render the
/// "last run" column without fetching the full runs table.
#[server]
pub async fn get_last_run_per_job(
    job_names: Vec<String>,
) -> Result<Vec<(String, RunRecord)>, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_all_last_run_per_job(&job_names)
        .await
        .map(|pairs| pairs.into_iter().map(|(n, r)| (n, r.into())).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Runs that touched `asset_key` within a single code location, newest
/// first. Implementation note: `limit` is the *output* cap; we always
/// fetch up to 1000 candidates from storage and filter in memory because
/// the storage layer doesn't support per-asset filtering yet.
#[server]
pub async fn get_runs_for_asset(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    limit: Option<usize>,
) -> Result<Vec<RunRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    // Scoped scan: only this CL's runs match `asset_key` (asset_keys are
    // per-CL — string match across CLs would surface foreign assets that
    // happen to share a name).
    let runs = state
        .storage
        .for_code_location(&ctx)
        .get_runs(limit.unwrap_or(1000), None)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    Ok(runs
        .into_iter()
        .filter(|r| r.node_names.contains(&asset_key))
        .take(limit.unwrap_or(10))
        .map(Into::into)
        .collect())
}
