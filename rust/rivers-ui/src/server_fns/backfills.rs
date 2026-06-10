//! Server functions for querying and managing backfills from storage.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

use crate::types::{
    BackfillFilter, BackfillInfo, BackfillPartitionsPage, BackfillsPage, BackfillsSummary,
};

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

/// A window of a backfill's partitions with real display keys + exact status
/// (from the record's completed/failed/canceled sets), for the heatmap.
#[server]
pub async fn get_backfill_partitions(
    backfill_id: String,
    offset: u64,
    limit: u64,
) -> Result<BackfillPartitionsPage, ServerFnError> {
    use rivers_core::storage::{PartitionKey, StorageBackend};
    use std::collections::HashSet;
    let state = expect_context::<crate::state::AppState>();
    let record = state
        .storage
        .get_backfill(&backfill_id)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .ok_or_else(|| ServerFnError::new("backfill not found"))?;

    let completed: HashSet<&PartitionKey> = record.completed_partitions.iter().collect();
    let failed: HashSet<&PartitionKey> = record.failed_partitions.iter().collect();
    let canceled: HashSet<&PartitionKey> = record.canceled_partitions.iter().collect();

    let total: u64 = record
        .partition_keys
        .iter()
        .map(|pk| pk.member_count() as u64)
        .sum();
    let rows: Vec<crate::types::BackfillPartitionCell> = record
        .partition_keys
        .iter()
        .flat_map(|pk| pk.members_preview(offset as usize + limit as usize))
        .skip(offset as usize)
        .take(limit as usize)
        .map(|m| {
            let status = if completed.contains(&m) {
                "done"
            } else if failed.contains(&m) {
                "failed"
            } else if canceled.contains(&m) {
                "canceled"
            } else {
                "pending"
            };
            crate::types::BackfillPartitionCell {
                key: crate::types::partition_key_to_display(m),
                status: status.to_string(),
            }
        })
        .collect();
    Ok(BackfillPartitionsPage { rows, total })
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
