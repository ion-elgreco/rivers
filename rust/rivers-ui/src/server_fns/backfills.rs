//! Server functions for querying and managing backfills from storage.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

use crate::types::{BackfillFilter, BackfillInfo, BackfillsPage, BackfillsSummary};

/// Backfills owned by `(loc_ns, loc_name)`. `status` accepts the wire-string
/// form of `BackfillStatus` (`"Requested"`, `"InProgress"`,
/// `"CompletedSuccess"`, `"CompletedFailed"`, `"Canceled"`); unknown values
/// disable the filter. `limit` defaults to 50.
#[server]
pub async fn get_backfills(
    loc_ns: String,
    loc_name: String,
    limit: Option<usize>,
    status: Option<String>,
) -> Result<Vec<BackfillInfo>, ServerFnError> {
    use rivers_core::storage::{BackfillStatus, StorageBackend};
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let status_filter = status.and_then(|s| match s.as_str() {
        "Requested" => Some(BackfillStatus::Requested),
        "InProgress" => Some(BackfillStatus::InProgress),
        "CompletedSuccess" => Some(BackfillStatus::CompletedSuccess),
        "CompletedFailed" => Some(BackfillStatus::CompletedFailed),
        "Canceled" => Some(BackfillStatus::Canceled),
        _ => None,
    });
    state
        .storage
        .for_code_location(&ctx)
        .get_backfills(Some(limit.unwrap_or(50)), status_filter)
        .await
        .map(|bfs| bfs.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_backfill(backfill_id: String) -> Result<Option<BackfillInfo>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_backfill(&backfill_id)
        .await
        .map(|opt| opt.map(Into::into))
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Paginated + filtered backfills list. JSON codec because `BackfillFilter`
/// is a nested struct.
#[server(input = Json)]
pub async fn get_backfills_page(
    offset: u64,
    limit: u64,
    filter: BackfillFilter,
) -> Result<BackfillsPage, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_all_backfills_page(offset, limit, &filter.into())
        .await
        .map(Into::into)
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Aggregate backfill counts for the list-page status pills.
#[server]
pub async fn get_backfills_summary() -> Result<BackfillsSummary, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .get_all_backfills_summary()
        .await
        .map(Into::into)
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Mark a backfill `Canceled` with a current end-time stamp. Returns `true`
/// on success. Doesn't kill in-flight per-partition runs — those continue
/// to completion; the cancellation is only visible to subsequent dispatch.
#[server]
pub async fn cancel_backfill(backfill_id: String) -> Result<bool, ServerFnError> {
    use rivers_core::storage::{BackfillStatus, StorageBackend};
    let state = expect_context::<crate::state::AppState>();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    state
        .storage
        .update_backfill_status(&backfill_id, BackfillStatus::Canceled, Some(now))
        .await
        .map(|_| true)
        .map_err(|e| ServerFnError::new(e.to_string()))
}
