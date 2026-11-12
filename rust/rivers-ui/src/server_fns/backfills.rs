//! Server functions for querying and managing backfills from storage.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

use crate::types::{
    BackfillFilter, BackfillInfo, BackfillPartitionsPage, BackfillsPage, BackfillsSummary, Page,
    RunRecord,
};

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

/// One page of the partitions of a backfill, each with what happened to it,
/// for the heatmap.
#[server]
pub async fn get_backfill_partitions(
    backfill_id: String,
    offset: u64,
    limit: u64,
) -> Result<BackfillPartitionsPage, ServerFnError> {
    use rivers_core::execution::backfill_partitions::{
        BackfillPartitionState, backfill_partitions_page,
    };
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    let record = state
        .storage
        .get_backfill(&backfill_id)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .ok_or_else(|| ServerFnError::new("backfill not found"))?;

    let rows = backfill_partitions_page(&*state.storage, &record, offset, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .into_iter()
        .map(|(partition, state)| {
            let status = match state {
                BackfillPartitionState::Completed => "done",
                BackfillPartitionState::Failed => "failed",
                BackfillPartitionState::Canceled => "canceled",
                BackfillPartitionState::Pending => "pending",
            };
            crate::types::BackfillPartitionCell {
                key: crate::types::partition_key_to_display(partition),
                status: status.to_string(),
            }
        })
        .collect();
    Ok(BackfillPartitionsPage {
        rows,
        total: record.num_partitions,
    })
}

/// One page of the runs of a backfill, the run linked last first.
#[server]
pub async fn get_backfill_runs(
    backfill_id: String,
    offset: u64,
    limit: u64,
) -> Result<Page<RunRecord>, ServerFnError> {
    use rivers_core::execution::backfill_runs::backfill_runs_page;
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    let record = state
        .storage
        .get_backfill(&backfill_id)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .ok_or_else(|| ServerFnError::new("backfill not found"))?;

    let rows: Vec<RunRecord> = backfill_runs_page(&*state.storage, &record, offset, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(Page {
        rows,
        total: record.run_ids.len() as u64,
    })
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

/// Cancel a backfill. One whose runs all ended keeps its own final status.
/// Runs in flight go on to their end; the cancel stops the runs not started.
#[server]
pub async fn cancel_backfill(backfill_id: String) -> Result<bool, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .cancel_backfill(&backfill_id)
        .await
        .map(|_| true)
        .map_err(|e| ServerFnError::new(e.to_string()))
}
