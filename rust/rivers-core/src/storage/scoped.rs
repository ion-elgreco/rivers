use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;

use crate::assets::graph::GraphTopology;
use crate::assets::remote::{LocationRef, RemoteRef};

use super::{
    AssetRecord, AssetScope, BackfillRecord, BackfillStatus, CodeLocationContext,
    ConcurrencyClaimStatus, CoordinatorRunInfo, PartitionKey, PerCodeLocationStorage, PoolInfo,
    PoolLimit, RunRecord, RunStatus, SlotHolder, SortOrder, StaleCause, StaleStatus,
    StorageBackend, StoredConditionEval, StoredConditionTick, StoredEvent, StoredTick,
};

/// A backend reference pre-bound to a [`CodeLocationContext`].
pub struct ScopedStorage<'a, S: ?Sized> {
    pub(super) backend: &'a S,
    pub(super) code_location_id: &'a str,
}

impl<'a, S: ?Sized> ScopedStorage<'a, S> {
    pub fn code_location_id(&self) -> &str {
        self.code_location_id
    }
}

/// Owned counterpart to [`ScopedStorage`] — bundles `Arc<S>` with a [`CodeLocationContext`].
pub struct ScopedStorageHandle<S> {
    backend: Arc<S>,
    ctx: CodeLocationContext,
}

impl<S> Clone for ScopedStorageHandle<S> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            ctx: self.ctx.clone(),
        }
    }
}

impl<S> ScopedStorageHandle<S> {
    pub fn new(backend: Arc<S>, ctx: CodeLocationContext) -> Self {
        Self { backend, ctx }
    }

    pub fn backend(&self) -> &Arc<S> {
        &self.backend
    }

    pub fn ctx(&self) -> &CodeLocationContext {
        &self.ctx
    }

    pub fn code_location_id(&self) -> &str {
        self.ctx.id()
    }
}

impl<S: StorageBackend> ScopedStorageHandle<S> {
    pub fn scoped(&self) -> ScopedStorage<'_, S> {
        self.backend.for_code_location(&self.ctx)
    }
}

#[allow(private_bounds)]
impl<'a, S: PerCodeLocationStorage + ?Sized> ScopedStorage<'a, S> {
    pub async fn get_events_for_asset(
        &self,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        self.backend
            .get_events_for_asset(self.code_location_id, asset_key, limit)
            .await
    }

    pub async fn get_latest_materialization(
        &self,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        self.backend
            .get_latest_materialization(self.code_location_id, asset_key, partition)
            .await
    }

    pub async fn register_assets(&self, records: &[AssetRecord]) -> Result<()> {
        self.backend
            .register_assets(self.code_location_id, records)
            .await
    }

    pub async fn get_asset_record(&self, asset_key: &str) -> Result<Option<AssetRecord>> {
        self.backend
            .get_asset_record(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_asset_records(&self) -> Result<Vec<AssetRecord>> {
        self.backend.get_asset_records(self.code_location_id).await
    }

    pub async fn get_asset_records_by_keys(&self, keys: &[String]) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_asset_records_by_keys(self.code_location_id, keys)
            .await
    }

    pub async fn get_assets_by_tag(&self, tag: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_tag(self.code_location_id, tag)
            .await
    }

    pub async fn get_assets_by_kind(&self, kind: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_kind(self.code_location_id, kind)
            .await
    }

    pub async fn get_assets_by_group(&self, group: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_group(self.code_location_id, group)
            .await
    }

    pub async fn set_block_reason_by_status(
        &self,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.backend
            .set_block_reason_by_status(self.code_location_id, status, reason)
            .await
    }

    pub async fn coordinator_tick_query(
        &self,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        self.backend
            .coordinator_tick_query(self.code_location_id)
            .await
    }

    pub async fn add_dynamic_partitions(
        &self,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> Result<()> {
        self.backend
            .add_dynamic_partitions(self.code_location_id, partitions_def_name, partition_keys)
            .await
    }

    pub async fn delete_dynamic_partition(
        &self,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.backend
            .delete_dynamic_partition(self.code_location_id, partitions_def_name, partition_key)
            .await
    }

    pub async fn get_dynamic_partitions(&self, partitions_def_name: &str) -> Result<Vec<String>> {
        self.backend
            .get_dynamic_partitions(self.code_location_id, partitions_def_name)
            .await
    }

    pub async fn has_dynamic_partition(
        &self,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        self.backend
            .has_dynamic_partition(self.code_location_id, partitions_def_name, partition_key)
            .await
    }

    pub async fn get_ticks(&self, automation_name: &str, limit: usize) -> Result<Vec<StoredTick>> {
        self.backend
            .get_ticks(self.code_location_id, automation_name, limit)
            .await
    }

    pub async fn prune_ticks(&self, automation_name: &str, max_ticks: usize) -> Result<usize> {
        self.backend
            .prune_ticks(self.code_location_id, automation_name, max_ticks)
            .await
    }

    pub async fn get_condition_ticks(&self, limit: usize) -> Result<Vec<StoredConditionTick>> {
        self.backend
            .get_condition_ticks(self.code_location_id, limit)
            .await
    }

    pub async fn prune_condition_history(&self, max_ticks: usize) -> Result<usize> {
        self.backend
            .prune_condition_history(self.code_location_id, max_ticks)
            .await
    }

    pub async fn get_condition_evals(
        &self,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        self.backend
            .get_condition_evals(self.code_location_id, asset_key, limit)
            .await
    }

    pub async fn get_condition_evals_for_tick(
        &self,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        self.backend
            .get_condition_evals_for_tick(self.code_location_id, tick_id)
            .await
    }

    pub async fn get_partition_events(
        &self,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        self.backend
            .get_partition_events(self.code_location_id, asset_key, partition_key, limit)
            .await
    }

    pub async fn get_materialized_partitions(&self, asset_key: &str) -> Result<Vec<PartitionKey>> {
        self.backend
            .get_materialized_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn count_materialized_partitions(&self, asset_key: &str) -> Result<u64> {
        self.backend
            .count_materialized_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn count_dynamic_partitions(&self, partitions_def_name: &str) -> Result<u64> {
        self.backend
            .count_dynamic_partitions(self.code_location_id, partitions_def_name)
            .await
    }

    pub async fn get_partition_timestamps(
        &self,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        self.backend
            .get_partition_timestamps(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_partition_timestamps_since(
        &self,
        asset_key: &str,
        since_timestamp: i64,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        self.backend
            .get_partition_timestamps_since(self.code_location_id, asset_key, since_timestamp)
            .await
    }

    pub async fn get_partition_timestamps_for_keys(
        &self,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        self.backend
            .get_partition_timestamps_for_keys(self.code_location_id, asset_key, keys)
            .await
    }

    pub async fn get_in_progress_partitions(&self, asset_key: &str) -> Result<Vec<PartitionKey>> {
        self.backend
            .get_in_progress_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_failed_partitions(
        &self,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> Result<HashMap<PartitionKey, i64>> {
        self.backend
            .get_failed_partitions(self.code_location_id, asset_key, materialized)
            .await
    }

    pub async fn get_asset_deletion_timestamps(&self) -> Result<HashMap<String, i64>> {
        self.backend
            .get_asset_deletion_timestamps(self.code_location_id)
            .await
    }

    pub async fn get_backfills(
        &self,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        self.backend
            .get_backfills(self.code_location_id, limit, status)
            .await
    }

    pub async fn set_pool_limit(
        &self,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        self.backend
            .set_pool_limit(self.code_location_id, pool_key, limit, lease_duration_secs)
            .await
    }

    pub async fn get_pool_limits(&self) -> Result<Vec<PoolLimit>> {
        self.backend.get_pool_limits(self.code_location_id).await
    }

    pub async fn get_pool_info(&self, pool_key: &str) -> Result<PoolInfo> {
        self.backend
            .get_pool_info(self.code_location_id, pool_key)
            .await
    }

    pub async fn get_all_pool_infos(&self) -> Result<Vec<PoolInfo>> {
        self.backend.get_all_pool_infos(self.code_location_id).await
    }

    pub async fn claim_concurrency_slots(
        &self,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
        scope: Option<&AssetScope>,
    ) -> Result<ConcurrencyClaimStatus> {
        self.backend
            .claim_concurrency_slots(
                self.code_location_id,
                pools,
                run_id,
                step_key,
                priority,
                lease_duration_secs,
                scope,
            )
            .await
    }

    pub async fn get_pool_slot_holders(&self, pool_key: &str) -> Result<Vec<SlotHolder>> {
        self.backend
            .get_pool_slot_holders(self.code_location_id, pool_key)
            .await
    }

    pub async fn get_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_runs(self.backend, self.code_location_id, limit, status).await
    }

    pub async fn get_queued_runs(&self) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_queued_runs(self.backend, self.code_location_id).await
    }

    pub async fn get_stalled_not_started_runs(&self, cutoff_ns: i64) -> Result<Vec<String>> {
        PerCodeLocationStorage::get_stalled_not_started_runs(
            self.backend,
            self.code_location_id,
            cutoff_ns,
        )
        .await
    }

    pub async fn get_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: SortOrder,
    ) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_runs_since(
            self.backend,
            self.code_location_id,
            since_timestamp,
            status,
            order,
        )
        .await
    }

    pub async fn get_runs_ended_since(
        &self,
        since: i64,
        status: RunStatus,
        job_names: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_runs_ended_since(
            self.backend,
            self.code_location_id,
            since,
            status,
            job_names,
            limit,
        )
        .await
    }

    pub async fn get_condition_eval_state(
        &self,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        self.backend
            .get_condition_eval_state(self.code_location_id)
            .await
    }

    pub async fn set_condition_eval_state(
        &self,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        self.backend
            .set_condition_eval_state(self.code_location_id, state)
            .await
    }

    pub async fn get_condition_pending_dispatch(
        &self,
    ) -> Result<Option<crate::condition::PendingDispatch>> {
        self.backend
            .get_condition_pending_dispatch(self.code_location_id)
            .await
    }

    pub async fn set_condition_pending_dispatch(
        &self,
        pending: &crate::condition::PendingDispatch,
    ) -> Result<()> {
        self.backend
            .set_condition_pending_dispatch(self.code_location_id, pending)
            .await
    }

    pub async fn get_graph_topology(&self) -> Result<Option<crate::assets::graph::GraphTopology>> {
        self.backend.get_graph_topology(self.code_location_id).await
    }

    pub async fn set_graph_topology(
        &self,
        topology: &crate::assets::graph::GraphTopology,
        key_lists: &[crate::assets::partitions_fingerprint::KeyList<'_>],
    ) -> Result<()> {
        self.backend
            .set_graph_topology(self.code_location_id, topology, key_lists)
            .await
    }

    pub async fn get_partition_keys(&self, digest: &str) -> Result<Option<Vec<String>>> {
        self.backend
            .get_partition_keys(self.code_location_id, digest)
            .await
    }
}

/// Per-CL methods that need to reach the unscoped `StorageBackend` KV API.
impl<'a, S: StorageBackend + ?Sized> ScopedStorage<'a, S> {
    /// Compute staleness for every asset in this code location.
    pub async fn compute_staleness(
        &self,
    ) -> Result<HashMap<String, (StaleStatus, Vec<StaleCause>)>> {
        let records = self.get_asset_records().await?;
        let topology = self.get_graph_topology().await?;
        self.staleness_for(&records, topology.as_ref()).await
    }

    /// Staleness of this code location's `records`. A remote upstream counts
    /// with its owner's data version, and is left out of the result.
    pub async fn staleness_for(
        &self,
        records: &[AssetRecord],
        topology: Option<&GraphTopology>,
    ) -> Result<HashMap<String, (StaleStatus, Vec<StaleCause>)>> {
        let Some(topology) = topology else {
            return Ok(crate::staleness::compute_staleness(records, &[]));
        };
        let remote: HashSet<&str> = topology
            .nodes
            .iter()
            .filter(|node| node.remote.is_some())
            .map(|node| node.name.as_str())
            .collect();
        if remote.is_empty() {
            return Ok(crate::staleness::compute_staleness(
                records,
                &topology.edges,
            ));
        }

        // A row of a remote's name is left from when the asset was local.
        let mut all: Vec<AssetRecord> = records
            .iter()
            .filter(|record| !remote.contains(record.asset_key.as_str()))
            .cloned()
            .collect();
        all.extend(self.remote_upstream_records(topology).await?);
        let mut staleness = crate::staleness::compute_staleness(&all, &topology.edges);
        staleness.retain(|key, _| !remote.contains(key.as_str()));
        Ok(staleness)
    }

    /// Staleness of `keys` of this code location, as [`Self::staleness_for`]
    /// gives it over all records. Reads only the records of `keys` and of the
    /// assets upstream of them. A key without a record has no entry.
    pub async fn staleness_of(
        &self,
        keys: &[String],
        topology: Option<&GraphTopology>,
    ) -> Result<HashMap<String, (StaleStatus, Vec<StaleCause>)>> {
        let Some(topology) = topology else {
            let records = self.get_asset_records_by_keys(keys).await?;
            return self.staleness_for(&records, None).await;
        };

        let (owned, upstream) = crate::staleness::upstream_of(keys, topology);
        let records = self.get_asset_records_by_keys(&owned).await?;
        let mut staleness = self.staleness_for(&records, Some(&upstream)).await?;
        staleness.retain(|key, _| keys.contains(key));
        Ok(staleness)
    }

    /// The owner's record of each remote node in `topology`, under the local
    /// name (see [`crate::staleness::remote_input_record`]). A remote whose
    /// owner is not registered, or has no such asset, has none.
    pub async fn remote_upstream_records(
        &self,
        topology: &GraphTopology,
    ) -> Result<Vec<AssetRecord>> {
        let remotes: Vec<(&str, &RemoteRef)> = topology
            .nodes
            .iter()
            .filter_map(|node| Some((node.name.as_str(), node.remote.as_ref()?)))
            .collect();
        let locations: Vec<&LocationRef> = remotes
            .iter()
            .map(|(_, remote)| &remote.code_location)
            .collect();
        let identities = super::resolve_code_locations(self.backend, locations).await?;
        let resolved: Vec<(&str, &str, &String)> = remotes
            .into_iter()
            .filter_map(|(local, remote)| {
                let identity = identities.get(&remote.code_location)?.as_deref()?;
                Some((local, identity, &remote.asset_key))
            })
            .collect();

        let mut keys: HashMap<&str, Vec<String>> = HashMap::new();
        for &(_, identity, key) in &resolved {
            keys.entry(identity).or_default().push(key.clone());
        }
        let mut owners: HashMap<&str, HashMap<String, AssetRecord>> = HashMap::new();
        for (identity, keys) in keys {
            let records = self
                .backend
                .get_asset_records_by_keys(identity, &keys)
                .await?;
            owners.insert(
                identity,
                records
                    .into_iter()
                    .map(|record| (record.asset_key.clone(), record))
                    .collect(),
            );
        }

        Ok(resolved
            .into_iter()
            .filter_map(|(local, identity, key)| {
                let owner = owners[identity].get(key)?;
                Some(crate::staleness::remote_input_record(local, owner))
            })
            .collect())
    }

    fn dynamic_keys_kv_key(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
    ) -> String {
        let partition_str = partition.map(|p| p.to_json());
        crate::dynamic_keys_key(
            self.code_location_id,
            asset_key,
            partition_str.as_deref(),
            data_version,
        )
    }

    /// Persist a fan-out source's mapping keys, scoped by `data_version`.
    pub async fn set_dynamic_keys(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
        keys: &[String],
    ) -> Result<()> {
        let key = self.dynamic_keys_kv_key(asset_key, partition, data_version);
        let bytes = serde_json::to_vec(keys)?;
        self.backend.kv_set(&key, &bytes).await
    }

    /// Read the fan-out mapping keys for a specific materialization.
    pub async fn get_dynamic_keys(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
    ) -> Result<Option<Vec<String>>> {
        let key = self.dynamic_keys_kv_key(asset_key, partition, data_version);
        match self.backend.kv_get(&key).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        }
    }
}
