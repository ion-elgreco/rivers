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

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .get_assets_info(GetAssetsInfoRequest {})
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp
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
                        })
                        .collect(),
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
        .collect())
}

/// Expand a stored `PartitionKey` into the individual partition-definition
/// keys it represents. A `Single` entry may bundle multiple keys when a run
/// materializes several at once — each one counts independently.
#[cfg(feature = "ssr")]
fn partition_key_members(pk: &rivers_core::storage::PartitionKey) -> Vec<String> {
    use rivers_core::storage::PartitionKey;
    match pk {
        PartitionKey::Single { keys } => keys.clone(),
        PartitionKey::Multi { dims } => vec![
            dims.iter()
                .map(|(d, ks)| format!("{d}={}", ks.join("|")))
                .collect::<Vec<_>>()
                .join(", "),
        ],
    }
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
) -> Result<PartitionStatus, ServerFnError> {
    use rivers_api::rivers::GetAssetsInfoRequest;
    use rivers_core::storage::StorageBackend;
    use std::collections::HashSet;

    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);

    let materialized_spks = scoped
        .get_materialized_partitions(&asset_key)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let materialized_keys: HashSet<String> = materialized_spks
        .iter()
        .flat_map(partition_key_members)
        .collect();

    // Failed partitions are inferred by scanning events for StepFailure.
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

    let mut all_keys: Vec<String> = Vec::new();
    if let Ok((_, mut client)) = state.connect_to(&loc_ns, &loc_name).await
        && let Ok(resp) = client.get_assets_info(GetAssetsInfoRequest {}).await
    {
        for a in resp.into_inner().assets {
            if a.asset_key == asset_key
                && let Some(pd) = a.partition_def
            {
                all_keys = pd.keys;
            }
        }
    }

    if all_keys.is_empty() {
        // Fallback: only show materialized
        let details: Vec<PartitionDetail> = materialized_keys
            .iter()
            .map(|key| PartitionDetail {
                key: key.clone(),
                status: "Materialized".to_string(),
                last_timestamp: None,
            })
            .collect();
        let count = details.len();
        Ok(PartitionStatus {
            asset_key,
            total_partitions: count,
            materialized: count,
            failed: 0,
            missing: 0,
            partition_details: details,
        })
    } else {
        let total = all_keys.len();
        let mut details: Vec<PartitionDetail> = Vec::with_capacity(total);
        let mut mat_count = 0;
        let mut fail_count = 0;
        let mut miss_count = 0;

        for key in &all_keys {
            if materialized_keys.contains(key) {
                mat_count += 1;
                details.push(PartitionDetail {
                    key: key.clone(),
                    status: "Materialized".to_string(),
                    last_timestamp: None,
                });
            } else if failed_keys.contains(key) {
                fail_count += 1;
                details.push(PartitionDetail {
                    key: key.clone(),
                    status: "Failed".to_string(),
                    last_timestamp: None,
                });
            } else {
                miss_count += 1;
                details.push(PartitionDetail {
                    key: key.clone(),
                    status: "Missing".to_string(),
                    last_timestamp: None,
                });
            }
        }

        Ok(PartitionStatus {
            asset_key,
            total_partitions: total,
            materialized: mat_count,
            failed: fail_count,
            missing: miss_count,
            partition_details: details,
        })
    }
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
