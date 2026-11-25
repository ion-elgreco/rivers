//! Server functions for querying asset records and their events from storage.
//!
//! Each function takes `(loc_ns, loc_name)` as the first two parameters,
//! resolves the active code location's stable identity via the registry,
//! then issues per-CL storage queries through the `ScopedStorage` returned
//! by `for_code_location`.

use leptos::prelude::*;

use crate::types::{AssetMigration, AssetRecord, EventsPage, StoredEvent};

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
    let topology = scoped
        .get_graph_topology()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let staleness = scoped
        .staleness_for(&records, topology.as_ref())
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
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
    let topology = scoped
        .get_graph_topology()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let mut staleness = scoped
        .staleness_for(&all_records, topology.as_ref())
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
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
        // A deletion flips the asset to Missing, so it must be visible here —
        // without it the timeline shows no reason for the state change.
        _ => vec![
            "Materialization".to_string(),
            "Observation".to_string(),
            "StepFailure".to_string(),
            "ActionCompleted".to_string(),
            "Deletion".to_string(),
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

/// The migrations of the stored data of this location's assets that have not
/// ended. Of one that drains: the started runs that it waits for, and the
/// assets that it reads which migrate first.
#[server]
pub async fn get_asset_migrations(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<AssetMigration>, ServerFnError> {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use rivers_core::assets::graph::from_topology;
    use rivers_core::assets::migrations::drain_runs;
    use rivers_core::assets::versions::CodeVersions;
    use rivers_core::storage::{AssetVersionState, ScopedStorageHandle};

    use crate::types::MigrationState;

    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let handle = ScopedStorageHandle::new(Arc::clone(&state.storage), ctx);
    let failed = |e: anyhow::Error| ServerFnError::new(e.to_string());
    let rows: Vec<_> = handle
        .scoped()
        .get_asset_versions(None)
        .await
        .map_err(failed)?
        .into_iter()
        .filter(|row| row.state != AssetVersionState::Ready)
        .collect();
    let code = if rows
        .iter()
        .any(|row| row.state == AssetVersionState::Draining)
    {
        let topology = handle
            .scoped()
            .get_graph_topology()
            .await
            .map_err(failed)?
            .unwrap_or_default();
        Some(CodeVersions::new(&from_topology(&topology), HashMap::new()))
    } else {
        None
    };
    let migrating: HashSet<&str> = rows.iter().map(|row| row.asset_key.as_str()).collect();

    let mut migrations = Vec::with_capacity(rows.len());
    for row in &rows {
        let (drain, upstream) = match &code {
            Some(code) if row.state == AssetVersionState::Draining => {
                let runs = drain_runs(&handle, code, row).await.map_err(failed)?;
                let upstream = code
                    .reads(&row.asset_key)
                    .iter()
                    .filter(|key| migrating.contains(key.as_str()))
                    .cloned()
                    .collect();
                (runs.into_iter().map(|run| run.run_id).collect(), upstream)
            }
            _ => (Vec::new(), Vec::new()),
        };
        let state = match row.state {
            AssetVersionState::Pending | AssetVersionState::Ready => MigrationState::Pending,
            AssetVersionState::Draining => MigrationState::Draining,
            AssetVersionState::Running => MigrationState::Running,
            AssetVersionState::Failed => MigrationState::Failed,
            AssetVersionState::Blocked => MigrationState::Blocked,
        };
        migrations.push(AssetMigration {
            asset_key: row.asset_key.clone(),
            applied_version: row.applied_version,
            state,
            target_version: row.target_version,
            reason: row.reason.clone(),
            run_id: row.run_id.clone(),
            backfill_id: row.backfill_id.clone(),
            drain_runs: drain,
            upstream,
        });
    }
    Ok(migrations)
}
