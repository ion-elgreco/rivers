//! Server functions for querying concurrency pool and run queue data from storage.

use leptos::prelude::*;

#[cfg(feature = "ssr")]
use crate::types::SlotHolder;
use crate::types::{PoolDetail, PoolInfo, RunRecord};

#[server]
pub async fn get_all_pools(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<PoolInfo>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_all_pool_infos()
        .await
        .map(|pools| pools.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_pool_detail(
    loc_ns: String,
    loc_name: String,
    pool_key: String,
) -> Result<PoolDetail, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);

    let info: PoolInfo = scoped
        .get_pool_info(&pool_key)
        .await
        .map(Into::into)
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let holders: Vec<SlotHolder> = scoped
        .get_pool_slot_holders(&pool_key)
        .await
        .map(|h| h.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(PoolDetail { info, holders })
}

/// All currently-queued runs across every code location, sorted by
/// dispatch order (priority desc, then `start_time` asc) so the queue page
/// shows runs in the order the coordinator would dequeue them.
#[server]
pub async fn get_queued_runs() -> Result<Vec<RunRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let state = expect_context::<crate::state::AppState>();
    let mut runs: Vec<RunRecord> = state
        .storage
        .get_all_queued_runs()
        .await
        .map(|runs| runs.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    runs.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then(a.start_time.cmp(&b.start_time))
    });
    Ok(runs)
}
