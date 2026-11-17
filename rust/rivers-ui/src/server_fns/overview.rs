//! Server functions for overview/dashboard statistics and deployment info.

use leptos::prelude::*;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use crate::types::{AssetDefinitionInfo, ResourceInfo};

/// The schema version of the database and the oldest rivers build, by schema
/// version, that can read and write it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub version: u32,
    pub min_reader: u32,
    pub min_writer: u32,
}

/// The rivers build of one code location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationVersion {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    /// `None`: a build that does not record it, or one that never started.
    pub rivers_version: Option<String>,
    pub schema_version: Option<u32>,
    /// Whether its build can write the database. `None`: not known.
    pub can_write: Option<bool>,
}

impl CodeLocationVersion {
    /// The row text of the deployment page.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.phase != "Ready" && !self.phase.is_empty() {
            parts.push(self.phase.clone());
        }
        match (&self.rivers_version, self.schema_version) {
            (Some(rivers), Some(schema)) => parts.push(format!("{rivers} · schema {schema}")),
            _ => parts.push("unknown".into()),
        }
        if self.can_write == Some(false) {
            parts.push("below write floor".into());
        }
        parts.join(" · ")
    }
}

/// Each registry entry with the build that its directory entry records.
#[cfg(feature = "ssr")]
fn code_location_versions(
    registry: &[crate::types::CodeLocationEntry],
    directory: &[(
        rivers_core::assets::remote::LocationRef,
        rivers_core::storage::CodeLocationDirectoryEntry,
    )],
    schema: Option<SchemaInfo>,
) -> Vec<CodeLocationVersion> {
    registry
        .iter()
        .map(|entry| {
            let recorded = directory
                .iter()
                .find(|(location, _)| {
                    location.namespace() == entry.namespace && location.name() == entry.name
                })
                .map(|(_, recorded)| recorded);
            let schema_version = recorded.and_then(|r| r.schema_version);

            CodeLocationVersion {
                namespace: entry.namespace.clone(),
                name: entry.name.clone(),
                phase: entry.phase.clone(),
                rivers_version: recorded.and_then(|r| r.rivers_version.clone()),
                schema_version,
                can_write: schema_version
                    .zip(schema)
                    .map(|(build, schema)| build >= schema.min_writer),
            }
        })
        .collect()
}

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
    /// `None`: the store could not be read.
    pub schema: Option<SchemaInfo>,
    pub code_locations: Vec<CodeLocationVersion>,
}

/// The resources a launch document may override, for the launch dialogs.
#[server]
pub async fn get_resources_info(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<ResourceInfo>, ServerFnError> {
    use rivers_api::rivers::GetAssetsInfoRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    let resp = client
        .get_assets_info(GetAssetsInfoRequest {})
        .await
        .map_err(super::grpc_err)?;
    Ok(resp
        .into_inner()
        .resources
        .into_iter()
        .map(|r| ResourceInfo {
            key: r.key,
            config_schema: r.config_schema,
        })
        .collect())
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
        .map_err(super::grpc_err)?;

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
            let actions = a
                .actions
                .into_iter()
                .map(|act| crate::types::AssetActionInfo {
                    name: act.name,
                    outcome: act.outcome,
                    exclusive: act.exclusive,
                    partitioning: act.partitioning,
                    description: act.description,
                    config_schema: act.config_schema,
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
                actions,
                config_schema: a.config_schema,
                metadata: a.metadata,
            }
        })
        .collect();

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

/// A window `[offset, offset+limit)` of an asset's keys, each with its index in
/// definition order, plus the `total`, fetched on demand.
#[server]
pub async fn get_partition_keys_page(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    dimension: String,
    query: String,
    offset: u64,
    limit: u64,
) -> Result<(Vec<(u64, String)>, u64), ServerFnError> {
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
        .map_err(super::grpc_err)?
        .into_inner();
    Ok((
        resp.indices.into_iter().zip(resp.keys).collect(),
        resp.total,
    ))
}

/// Index of a key in definition order for the picker's jump; `-1` if absent.
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
        .map_err(super::grpc_err)?
        .into_inner();
    Ok(resp.index)
}

/// Load a Dynamic partition set's full key list from storage.
#[cfg(feature = "ssr")]
pub(super) async fn load_dynamic_partition_keys(
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

/// `[offset, offset+limit)` window of a Dynamic set's keys, each with its index in
/// stored order, + total; `query` filters by substring.
#[server]
pub async fn get_dynamic_partition_keys_page(
    loc_ns: String,
    loc_name: String,
    dynamic_name: String,
    query: String,
    offset: u64,
    limit: u64,
) -> Result<(Vec<(u64, String)>, u64), ServerFnError> {
    let all = load_dynamic_partition_keys(&loc_ns, &loc_name, &dynamic_name).await?;
    let filtered: Vec<(u64, String)> = all
        .into_iter()
        .enumerate()
        .map(|(i, k)| (i as u64, k))
        .filter(|(_, k)| k.contains(&query))
        .collect();
    let total = filtered.len() as u64;
    let window: Vec<(u64, String)> = filtered
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect();
    Ok((window, total))
}

/// Index of `key` in a Dynamic set's stored order (`-1` if absent), for the picker's jump.
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

/// Aggregate stats for the deployment page.
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

    let schema = state
        .storage
        .schema_stamps()
        .await
        .ok()
        .flatten()
        .map(|stamps| SchemaInfo {
            version: stamps.version,
            min_reader: stamps.min_reader,
            min_writer: stamps.min_writer,
        });
    let directory = state
        .storage
        .list_code_locations()
        .await
        .unwrap_or_default();
    let code_locations = code_location_versions(&registry_entries, &directory, schema);

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

        let now = jiff::Timestamp::now().as_second();
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
        schema,
        code_locations,
    })
}

#[cfg(all(test, feature = "ssr"))]
mod tests {
    #[test]
    fn code_location_versions_join_the_registry_with_the_directory() {
        use super::{CodeLocationVersion, SchemaInfo, code_location_versions};
        use crate::types::CodeLocationEntry;
        use rivers_core::assets::remote::LocationRef;
        use rivers_core::storage::CodeLocationDirectoryEntry;

        let registered = |name: &str, phase: &str| CodeLocationEntry {
            namespace: "team".into(),
            name: name.into(),
            grpc_endpoint: String::new(),
            image: String::new(),
            module: String::new(),
            phase: phase.into(),
            observed_generation: 0,
            identity: name.into(),
        };
        let recorded = |namespace: &str, name: &str, schema: Option<u32>| {
            (
                LocationRef::new(namespace, name).unwrap(),
                CodeLocationDirectoryEntry {
                    schema_version: schema,
                    rivers_version: schema.map(|v| format!("0.{v}.0")),
                    ..CodeLocationDirectoryEntry::new(name)
                },
            )
        };
        let registry = [
            registered("current", "Ready"),
            registered("behind", "Ready"),
            registered("old-build", "Ready"),
            registered("starting", "Deploying"),
        ];
        let directory = [
            recorded("team", "current", Some(13)),
            recorded("team", "behind", Some(12)),
            recorded("team", "old-build", None),
            recorded("other", "starting", Some(13)),
        ];
        let schema = SchemaInfo {
            version: 13,
            min_reader: 12,
            min_writer: 13,
        };

        let versions = code_location_versions(&registry, &directory, Some(schema));

        let rows: Vec<_> = versions
            .iter()
            .map(|v| (v.name.as_str(), v.schema_version, v.can_write, v.summary()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("current", Some(13), Some(true), "0.13.0 · schema 13".into()),
                (
                    "behind",
                    Some(12),
                    Some(false),
                    "0.12.0 · schema 12 · below write floor".into()
                ),
                ("old-build", None, None, "unknown".into()),
                ("starting", None, None, "Deploying · unknown".into()),
            ]
        );

        let no_schema: Vec<CodeLocationVersion> =
            code_location_versions(&registry[..1], &directory, None);
        assert_eq!(no_schema[0].can_write, None);
        assert_eq!(no_schema[0].summary(), "0.13.0 · schema 13");
    }
}
