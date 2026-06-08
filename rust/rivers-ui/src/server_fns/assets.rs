//! Server functions for querying asset records and their events from storage.
//!
//! Each function takes `(loc_ns, loc_name)` as the first two parameters,
//! resolves the active code location's stable identity via the registry,
//! then issues per-CL storage queries through the `ScopedStorage` returned
//! by `for_code_location`.

use leptos::prelude::*;

use crate::types::{AssetRecord, EventsPage, StoredEvent};

#[server]
pub async fn get_assets(
    loc_ns: String,
    loc_name: String,
    tag: Option<String>,
    kind: Option<String>,
    group: Option<String>,
) -> Result<Vec<AssetRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);
    let mut records = scoped
        .get_asset_records()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let edges = scoped
        .get_graph_topology()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .map(|t| t.edges)
        .unwrap_or_default();
    let staleness = rivers_core::staleness::compute_staleness(&records, &edges);
    if let Some(ref t) = tag {
        records.retain(|r| r.tags.contains(t));
    }
    if let Some(ref k) = kind {
        records.retain(|r| r.kinds.contains(k));
    }
    if let Some(ref g) = group {
        records.retain(|r| r.asset_group.as_deref() == Some(g.as_str()));
    }
    Ok(records
        .into_iter()
        .map(|r| {
            let status = staleness
                .get(&r.asset_key)
                .map(|(s, _)| s.clone())
                .unwrap_or_default()
                .into();
            AssetRecord::from_core_with_staleness(r, status)
        })
        .collect())
}

#[server]
pub async fn get_asset(
    loc_ns: String,
    loc_name: String,
    key: String,
) -> Result<Option<AssetRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);
    let Some(record) = scoped
        .get_asset_record(&key)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
    else {
        return Ok(None);
    };
    // Staleness depends on the rest of the graph, so we have to fetch the
    // full picture once. Cheap at typical CL sizes.
    let all_records = scoped
        .get_asset_records()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let edges = scoped
        .get_graph_topology()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .map(|t| t.edges)
        .unwrap_or_default();
    let mut staleness = rivers_core::staleness::compute_staleness(&all_records, &edges);
    let status: crate::types::StaleStatus = staleness
        .remove(&key)
        .map(|(s, _)| s)
        .unwrap_or_default()
        .into();
    Ok(Some(AssetRecord::from_core_with_staleness(record, status)))
}

#[server]
pub async fn get_asset_events(
    loc_ns: String,
    loc_name: String,
    key: String,
    limit: Option<usize>,
) -> Result<Vec<StoredEvent>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_events_for_asset(&key, limit.unwrap_or(50))
        .await
        .map(|evts| evts.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Paginated events for the asset-detail Events tab. `filter` is the active pill;
/// the server filters by event type so `total` is the true per-filter count.
#[server]
pub async fn get_asset_events_page(
    loc_ns: String,
    loc_name: String,
    key: String,
    filter: String,
    offset: u64,
    limit: u64,
) -> Result<EventsPage, ServerFnError> {
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let event_types: Vec<String> = match filter.as_str() {
        "mat" => vec!["Materialization".to_string()],
        "fail" => vec!["StepFailure".to_string()],
        _ => vec![
            "Materialization".to_string(),
            "Observation".to_string(),
            "StepFailure".to_string(),
        ],
    };
    let (rows, total) = state
        .storage
        .get_events_for_asset_page(ctx.id(), &key, &event_types, offset, limit)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    Ok(EventsPage {
        rows: rows.into_iter().map(Into::into).collect(),
        total,
    })
}

#[server]
pub async fn get_asset_events_by_partition(
    loc_ns: String,
    loc_name: String,
    key: String,
    partition_key: String,
    limit: Option<usize>,
) -> Result<Vec<StoredEvent>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_partition_events(&key, &partition_key, limit.unwrap_or(50))
        .await
        .map(|evts| evts.into_iter().map(Into::into).collect())
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn get_dynamic_partitions(
    loc_ns: String,
    loc_name: String,
    name: String,
) -> Result<Vec<String>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_dynamic_partitions(&name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}
