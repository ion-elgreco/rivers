//! Server functions for overview/dashboard statistics and deployment info.

use leptos::prelude::*;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use crate::types::{AssetDefinitionInfo, PartitionDetail, PartitionStatus, RunStats};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentInfo {
    pub version: String,
    pub storage_type: String,
    pub grpc_url: String,
    pub grpc_connected: bool,
    /// Registry backend: `"embedded"` (in-process Static) or `"grpc"` (operator).
    pub code_location_mode: String,
    pub code_locations_total: usize,
    pub code_locations_ready: usize,
    pub asset_count: usize,
    pub run_count: usize,
    pub event_count: usize,
    pub tick_count: usize,
    pub daemon_active: bool,
    pub daemon_schedules: usize,
    pub daemon_sensors: usize,
}

#[server]
pub async fn get_run_stats() -> Result<RunStats, ServerFnError> {
    use rivers_core::storage::{RunStatus, StorageBackend};
    let state = expect_context::<crate::state::AppState>();

    let runs = state
        .storage
        .get_all_runs(10000, None)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let mut stats = RunStats {
        total: runs.len(),
        ..Default::default()
    };
    for run in &runs {
        match run.status {
            RunStatus::Success => stats.success += 1,
            RunStatus::Failure => stats.failure += 1,
            RunStatus::Started => stats.started += 1,
            RunStatus::NotStarted => stats.not_started += 1,
            RunStatus::Queued => stats.queued += 1,
            RunStatus::Canceled => stats.canceled += 1,
        }
    }
    Ok(stats)
}

#[server]
pub async fn get_assets_info(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<AssetDefinitionInfo>, ServerFnError> {
    use rivers_api::rivers::GetAssetsInfoRequest;
    use rivers_core::storage::StorageBackend;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .get_assets_info(GetAssetsInfoRequest {})
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let mut assets: Vec<AssetDefinitionInfo> = resp
        .into_inner()
        .assets
        .into_iter()
        .map(|a| {
            let partition_def = a
                .partition_def
                .map(|pd| crate::types::PartitionDefinitionInfo {
                    kind: pd.kind,
                    keys: pd.keys,
                    dimensions: pd
                        .dimensions
                        .into_iter()
                        .map(|d| crate::types::PartitionDimensionInfo {
                            name: d.name,
                            keys: d.keys,
                            total_count: d.total_count,
                            keys_truncated: d.keys_truncated,
                        })
                        .collect(),
                    total_count: pd.total_count,
                    keys_truncated: pd.keys_truncated,
                    dynamic_name: pd.dynamic_name,
                });
            let hooks = a
                .hooks
                .into_iter()
                .map(|h| crate::types::HookInfo {
                    hook_type: h.hook_type,
                    function_name: h.function_name,
                })
                .collect();
            AssetDefinitionInfo {
                asset_key: a.asset_key,
                description: a.description,
                partition_def,
                hooks,
                io_handler: a.io_handler,
                has_self_dependency: a.has_self_dependency,
                is_external: a.is_external,
                automation_condition: a.automation_condition,
                tags: a.tags,
                kinds: a.kinds,
                group: a.group,
                code_version: a.code_version,
                asset_type: a.asset_type,
            }
        })
        .collect();

    // Dynamic partitions are storage-managed, so the def-level `total_count` is 0.
    // Fill in the real count from storage — but only when the location actually
    // has a Dynamic asset, so the common all-static case skips the registry
    // lookup + per-asset storage round-trips entirely.
    let has_dynamic = assets.iter().any(|a| {
        a.partition_def
            .as_ref()
            .is_some_and(|pd| pd.dynamic_namespace().is_some())
    });
    if has_dynamic && let Ok(ctx) = super::resolve_identity(&loc_ns, &loc_name).await {
        let scoped = state.storage.for_code_location(&ctx);
        for asset in assets.iter_mut() {
            let Some(pd) = asset.partition_def.as_mut() else {
                continue;
            };
            let Some(ns) = pd.dynamic_namespace().map(str::to_string) else {
                continue;
            };
            if let Ok(n) = scoped.count_dynamic_partitions(&ns).await {
                pd.total_count = n;
            }
        }
    }
    Ok(assets)
}

/// Expand a stored `PartitionKey` into the individual partition-definition
/// keys it represents. A `Single` entry may bundle multiple keys when a run
/// materializes several at once — each one counts independently.
#[cfg(feature = "ssr")]
fn partition_key_members(pk: &rivers_core::storage::PartitionKey) -> Vec<String> {
    // Each individual member, formatted to match the gRPC-windowed keys so the
    // heatmap's `materialized_keys.contains(window_key)` lookups hit. A Multi
    // format mismatch here is what grays out every block despite a right count.
    pk.members()
        .into_iter()
        .map(crate::types::partition_key_to_display)
        .collect()
}

/// Tri-state per-partition status (Materialized / Failed / Missing) for the
/// asset-detail partition heatmap. Joins materialized keys from storage,
/// failed keys from event scan (last 10k events), and the partition
/// definition's full key universe from gRPC. When the gRPC-side
/// definition is unavailable, falls back to listing only materialized keys.
#[server]
pub async fn get_partition_status(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    offset: u64,
    // For a Dynamic asset, its namespace name (keys are storage-managed); empty
    // for all other kinds, which window via gRPC.
    dynamic_name: String,
) -> Result<PartitionStatus, ServerFnError> {
    use rivers_api::rivers::GetPartitionKeysRequest;
    use rivers_core::storage::StorageBackend;
    use std::collections::HashSet;

    // The heatmap renders one cell per key, so it pages in fixed windows
    // (`offset` = page start); the summary counts below stay global. Keep in
    // sync with `PartitionsTab`'s `PAGE`.
    const HEATMAP_PAGE: u64 = 1000;

    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);

    // Summary count — an aggregate, not one row per materialized partition.
    let materialized_count = scoped
        .count_materialized_partitions(&asset_key)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))? as usize;

    // Total + key window at `offset`. Dynamic keys are storage-managed, so source
    // them from storage; every other kind windows via gRPC.
    let mut window_keys: Vec<String> = Vec::new();
    let mut total: usize = 0;
    if !dynamic_name.is_empty() {
        if let Ok(all) = scoped.get_dynamic_partitions(&dynamic_name).await {
            total = all.len();
            window_keys = all
                .into_iter()
                .skip(offset as usize)
                .take(HEATMAP_PAGE as usize)
                .collect();
        }
    } else if let Ok((_, mut client)) = state.connect_to(&loc_ns, &loc_name).await
        && let Ok(resp) = client
            .get_partition_keys(GetPartitionKeysRequest {
                asset_key: asset_key.clone(),
                offset,
                limit: HEATMAP_PAGE,
                query: String::new(),
                dimension: String::new(),
            })
            .await
    {
        let resp = resp.into_inner();
        total = resp.total as usize;
        window_keys = resp.keys;
    }

    // Classify only the visible window. Materialized membership comes from the
    // materialized set and failed from a bounded event scan. (A windowed status
    // query that avoids fetching the full materialized set for the window is a
    // follow-up.)
    let materialized_spks = scoped
        .get_materialized_partitions(&asset_key)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let materialized_keys: HashSet<String> = materialized_spks
        .iter()
        .flat_map(partition_key_members)
        .collect();

    let events = scoped
        .get_events_for_asset(&asset_key, 10000)
        .await
        .unwrap_or_default();
    let mut failed_keys: HashSet<String> = HashSet::new();
    for evt in &events {
        if matches!(evt.event_type, rivers_core::storage::EventType::StepFailure)
            && let Some(ref pk) = evt.partition_key
        {
            for k in partition_key_members(pk) {
                if !materialized_keys.contains(&k) {
                    failed_keys.insert(k);
                }
            }
        }
    }
    let failed_count = failed_keys.len();

    // gRPC unavailable / no partition def → fall back to listing materialized keys.
    let detail_keys: Vec<String> = if total == 0 {
        total = materialized_keys.len();
        materialized_keys
            .iter()
            .take(HEATMAP_PAGE as usize)
            .cloned()
            .collect()
    } else {
        window_keys
    };

    let partition_details: Vec<PartitionDetail> = detail_keys
        .iter()
        .map(|key| {
            let status = if materialized_keys.contains(key) {
                "Materialized"
            } else if failed_keys.contains(key) {
                "Failed"
            } else {
                "Missing"
            };
            PartitionDetail {
                key: key.clone(),
                status: status.to_string(),
                last_timestamp: None,
            }
        })
        .collect();

    // Clamp to the definition's total: stale storage rows (partitions no longer
    // in the current def) must not make the summary show materialized > total.
    let materialized = materialized_count.min(total);
    let failed = failed_count.min(total - materialized);
    let missing = total - materialized - failed;
    Ok(PartitionStatus {
        asset_key,
        total_partitions: total,
        materialized,
        failed,
        missing,
        partition_details,
    })
}

/// A window `[offset, offset+limit)` of an asset's keys plus the `total`, fetched
/// on demand. `query` filters by substring (`total` = match count); `dimension`
/// pages a Multi dimension instead of the asset's single-dim keys.
#[server]
pub async fn get_partition_keys_page(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    dimension: String,
    query: String,
    offset: u64,
    limit: u64,
) -> Result<(Vec<String>, u64), ServerFnError> {
    use rivers_api::rivers::GetPartitionKeysRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let resp = client
        .get_partition_keys(GetPartitionKeysRequest {
            asset_key,
            offset,
            limit,
            query,
            dimension,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .into_inner();
    Ok((resp.keys, resp.total))
}

/// Index of a key in definition order for the picker's jump; `dimension`
/// resolves within a Multi dimension. `-1` if absent.
#[server]
pub async fn get_partition_key_index(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    dimension: String,
    key: String,
) -> Result<i64, ServerFnError> {
    use rivers_api::rivers::GetPartitionKeyIndexRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let resp = client
        .get_partition_key_index(GetPartitionKeyIndexRequest {
            asset_key,
            key,
            dimension,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?
        .into_inner();
    Ok(resp.index)
}

/// Load a Dynamic partition set's full key list from storage (`partition_key
/// ASC`). Its keys are storage-managed, not in the in-memory def, so this reads
/// storage directly like `get_partition_status`; callers filter/window in memory.
#[cfg(feature = "ssr")]
async fn load_dynamic_partition_keys(
    loc_ns: &str,
    loc_name: &str,
    dynamic_name: &str,
) -> Result<Vec<String>, ServerFnError> {
    use rivers_core::storage::StorageBackend;

    let ctx = super::resolve_identity(loc_ns, loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_dynamic_partitions(dynamic_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// `[offset, offset+limit)` window of a Dynamic set's keys + total; `query`
/// filters by substring. Dynamic companion of `get_partition_keys_page`.
#[server]
pub async fn get_dynamic_partition_keys_page(
    loc_ns: String,
    loc_name: String,
    dynamic_name: String,
    query: String,
    offset: u64,
    limit: u64,
) -> Result<(Vec<String>, u64), ServerFnError> {
    let all = load_dynamic_partition_keys(&loc_ns, &loc_name, &dynamic_name).await?;
    // Case-sensitive, like the asset-side `get_partition_keys_filtered`.
    let filtered: Vec<String> = if query.is_empty() {
        all
    } else {
        all.into_iter().filter(|k| k.contains(&query)).collect()
    };
    let total = filtered.len() as u64;
    let window: Vec<String> = filtered
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect();
    Ok((window, total))
}

/// Index of `key` in a Dynamic set's stored order (`-1` if absent), for the
/// picker's jump. Dynamic companion of `get_partition_key_index`.
#[server]
pub async fn get_dynamic_partition_key_index(
    loc_ns: String,
    loc_name: String,
    dynamic_name: String,
    key: String,
) -> Result<i64, ServerFnError> {
    let all = load_dynamic_partition_keys(&loc_ns, &loc_name, &dynamic_name).await?;
    Ok(all
        .iter()
        .position(|k| k == &key)
        .map(|i| i as i64)
        .unwrap_or(-1))
}

/// Aggregate stats for the deployment page: storage-side counts (assets,
/// runs, events), gRPC-side daemon counts (schedules, sensors, ticks),
/// connectivity, and a heuristic `daemon_active` flag (a tick within the
/// last 5 min). Falls back gracefully when the gRPC endpoint isn't
/// reachable so storage-side fields still render.
#[server]
pub async fn get_deployment_info(
    loc_ns: String,
    loc_name: String,
) -> Result<DeploymentInfo, ServerFnError> {
    use rivers_api::rivers::{GetSchedulesRequest, GetSensorsRequest, PingRequest};
    use rivers_core::storage::StorageBackend;

    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);

    let asset_records = scoped.get_asset_records().await.unwrap_or_default();
    let asset_count = asset_records.len();

    let run_count = scoped
        .get_runs(100000, None)
        .await
        .map(|r| r.len())
        .unwrap_or(0);

    // Resolve the requested location via the registry. `connect_to` errors
    // when the entry is missing or non-Ready — treated here as "not configured"
    // so the deployment page still renders the storage-side fields even
    // pre-reconcile.
    let mut grpc_url = "Not configured".to_string();
    let mut grpc_connected = false;
    let mut event_count = 0usize;
    let mut tick_count = 0usize;
    let mut daemon_active = false;
    let mut daemon_schedules = 0usize;
    let mut daemon_sensors = 0usize;

    let code_location_mode = state.registry.mode().to_string();
    let registry_entries = state.registry.list().await.unwrap_or_default();
    let code_locations_total = registry_entries.len();
    let code_locations_ready = registry_entries.iter().filter(|e| e.is_ready()).count();

    if let Ok((entry, mut client)) = state.connect_to(&loc_ns, &loc_name).await {
        grpc_url = entry.grpc_endpoint.clone();
        grpc_connected = client.ping(PingRequest {}).await.is_ok();

        for ar in &asset_records {
            event_count += scoped
                .get_events_for_asset(&ar.asset_key, 100000)
                .await
                .map(|e| e.len())
                .unwrap_or(0);
        }

        let now = chrono::Utc::now().timestamp();
        if let Ok(resp) = client.get_schedules(GetSchedulesRequest {}).await {
            let schedules = resp.into_inner().schedules;
            daemon_schedules = schedules.len();
            for s in &schedules {
                let ticks = scoped.get_ticks(&s.name, 100).await.unwrap_or_default();
                tick_count += ticks.len();
                if ticks.first().is_some_and(|t| now - t.timestamp < 300) {
                    daemon_active = true;
                }
            }
        }
        if let Ok(resp) = client.get_sensors(GetSensorsRequest {}).await {
            let sensors = resp.into_inner().sensors;
            daemon_sensors = sensors.len();
            for s in &sensors {
                let ticks = scoped.get_ticks(&s.name, 100).await.unwrap_or_default();
                tick_count += ticks.len();
                if ticks.first().is_some_and(|t| now - t.timestamp < 300) {
                    daemon_active = true;
                }
            }
        }
    }

    Ok(DeploymentInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        storage_type: state.storage.backend_kind().label(),
        grpc_url,
        grpc_connected,
        code_location_mode,
        code_locations_total,
        code_locations_ready,
        asset_count,
        run_count,
        event_count,
        tick_count,
        daemon_active,
        daemon_schedules,
        daemon_sensors,
    })
}
