//! Remote deps: upstream assets owned by another code location. Their
//! records and partition status are read from the owner's scope and kept
//! under the local name, so the evaluator sees them like local deps. The
//! owner's runs are tracked per location, apart from this location's own.

use std::collections::{HashMap, HashSet};

use crate::assets::remote::LocationRef;
use crate::storage::{
    AssetRecord, CodeLocationContext, PartitionKey, RunRecord, RunStatus, ScopedStorage, SortOrder,
    StorageBackend, resolve_code_locations,
};

use super::{
    AssetConditionCache, PartitionStatusEntry, derive_failure_floors, run_status_is_terminal,
};

/// Wait before looking up an unregistered location again.
const RETRY_NANOS: i64 = 5_000_000_000;
/// Wait before checking a resolved location's identity again.
const REVALIDATE_NANOS: i64 = 30_000_000_000;

/// An upstream asset owned by another code location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDep {
    /// The remote node's name in this location's graph.
    pub local: String,
    pub location: LocationRef,
    /// The asset's key in the owning location.
    pub asset_key: String,
}

/// Remote deps grouped by owning location.
#[derive(Default)]
pub(super) struct RemoteScopes {
    pub(super) slots: HashMap<LocationRef, RemoteSlot>,
    locals: HashSet<String>,
}

impl RemoteScopes {
    fn new(deps: Vec<RemoteDep>) -> Self {
        let mut scopes = Self::default();
        for dep in deps {
            scopes.locals.insert(dep.local.clone());
            scopes
                .slots
                .entry(dep.location)
                .or_insert_with(|| RemoteSlot {
                    bindings: HashMap::new(),
                    state: SlotState::Pending,
                })
                .bindings
                .entry(dep.asset_key)
                .or_default()
                .push(dep.local);
        }
        scopes
    }

    /// Whether `name` is a remote dep's local name.
    pub(super) fn is_local(&self, name: &str) -> bool {
        self.locals.contains(name)
    }

    pub(super) fn slot_mut(&mut self, location: &LocationRef) -> &mut RemoteSlot {
        self.slots
            .get_mut(location)
            .expect("refresh only visits registered locations")
    }
}

pub(super) struct RemoteSlot {
    /// The owner's asset key → the local names bound to it.
    pub(super) bindings: HashMap<String, Vec<String>>,
    pub(super) state: SlotState,
}

impl RemoteSlot {
    /// Partitions of `key` that a live run in the owner is materializing.
    fn live_partitions(&self, key: &str) -> HashSet<PartitionKey> {
        let SlotState::Resolved { scope, .. } = &self.state else {
            return HashSet::new();
        };
        scope
            .live_runs
            .values()
            .filter(|run| !run.action && run.asset_keys.iter().any(|k| k == key))
            .filter_map(|run| run.partition_key.as_ref())
            .flat_map(|pk| pk.members())
            .collect()
    }
}

pub(super) enum SlotState {
    /// Not looked up yet.
    Pending,
    /// No directory entry.
    Unresolved { next_attempt: i64 },
    Resolved {
        scope: RemoteScope,
        next_revalidate: i64,
    },
}

impl SlotState {
    fn due(&self, now: i64) -> bool {
        match self {
            Self::Pending => true,
            Self::Unresolved { next_attempt } => now >= *next_attempt,
            Self::Resolved {
                next_revalidate, ..
            } => now >= *next_revalidate,
        }
    }

    fn identity(&self) -> Option<&str> {
        match self {
            Self::Resolved { scope, .. } => Some(scope.ctx.id()),
            Self::Pending | Self::Unresolved { .. } => None,
        }
    }

    fn retry_at(&mut self, at: i64) {
        match self {
            Self::Pending => *self = Self::Unresolved { next_attempt: at },
            Self::Unresolved { next_attempt } => *next_attempt = at,
            Self::Resolved {
                next_revalidate, ..
            } => *next_revalidate = at,
        }
    }
}

/// A resolved location: where to read, and how far its runs and
/// observations have been read.
pub(super) struct RemoteScope {
    pub(super) ctx: CodeLocationContext,
    /// Trails the newest run start_time seen by 1ns, like the local cursor.
    pub(super) run_cursor: i64,
    /// Trails the newest observation seen by 1ns.
    pub(super) observation_cursor: i64,
    /// Terminal runs already applied (`run_id → start_time`), pruned at the
    /// run cursor.
    pub(super) applied_run_ids: HashMap<String, i64>,
    /// Live runs in the owner that touch a bound key.
    pub(super) live_runs: HashMap<String, LiveRemoteRun>,
    /// Failure floors by owner key, with the failure's time.
    pub(super) failed: HashMap<String, i64>,
}

pub(super) struct LiveRemoteRun {
    /// The bound keys the run touches.
    pub(super) asset_keys: Vec<String>,
    pub(super) partition_key: Option<PartitionKey>,
    /// An action run is not a materialization attempt. It is watched only to
    /// re-read its keys when it ends.
    pub(super) action: bool,
}

impl LiveRemoteRun {
    /// `run`'s view from here; `None` if it touches no bound key.
    pub(super) fn of(run: &RunRecord, bindings: &HashMap<String, Vec<String>>) -> Option<Self> {
        let asset_keys: Vec<String> = run
            .node_names
            .iter()
            .filter(|name| bindings.contains_key(*name))
            .cloned()
            .collect();
        (!asset_keys.is_empty()).then(|| Self {
            asset_keys,
            partition_key: run.partition_key.clone(),
            action: run.is_action(),
        })
    }
}

/// What was read from one location, by the owner's asset key.
#[derive(Default)]
pub(super) struct RemoteRead {
    /// `None`: the owner has no such asset.
    records: Vec<(String, Option<AssetRecord>)>,
    /// Read for keys with a partitioned local.
    partitions: Vec<(String, RemotePartitions)>,
}

impl RemoteRead {
    pub(super) fn record(&self, key: &str) -> Option<&AssetRecord> {
        self.records
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, record)| record.as_ref())
    }
}

/// An owner's partition rows for one key.
#[derive(Default, Clone)]
struct RemotePartitions {
    /// Rows newer than the cached ones, or every row when `full`.
    timestamps: Vec<(PartitionKey, i64)>,
    full: bool,
    in_progress: HashSet<PartitionKey>,
    failed: HashMap<PartitionKey, i64>,
}

impl RemotePartitions {
    /// Merge into `entry` with `live` added to the in-progress keys.
    fn apply(self, entry: &mut PartitionStatusEntry, live: &HashSet<PartitionKey>) -> bool {
        let mut changed = false;
        if self.full {
            let timestamps: HashMap<_, _> = self.timestamps.into_iter().collect();
            changed |= timestamps != entry.timestamps;
            entry.timestamps = timestamps;
        } else {
            for (pk, ts) in self.timestamps {
                changed |= entry.timestamps.insert(pk, ts) != Some(ts);
            }
        }

        let in_progress: HashSet<PartitionKey> = self
            .in_progress
            .into_iter()
            .chain(live.iter().cloned())
            .collect();
        let failed_timestamps: HashMap<PartitionKey, i64> = self
            .failed
            .into_iter()
            .filter(|(pk, failed)| entry.timestamps.get(pk).is_none_or(|mat| failed > mat))
            .collect();
        changed |= in_progress != entry.in_progress || failed_timestamps != entry.failed_timestamps;

        entry.in_progress = in_progress;
        entry.failed = failed_timestamps.keys().cloned().collect();
        entry.failed_timestamps = failed_timestamps;
        changed
    }
}

impl AssetConditionCache {
    /// Register the remote deps; each is read from its owner's scope.
    pub fn set_remote_deps(&mut self, deps: Vec<RemoteDep>) {
        self.remote = RemoteScopes::new(deps);
    }

    /// Local names of remote deps with a live run in their owner.
    pub fn remote_in_progress_keys(&self) -> impl Iterator<Item = &String> {
        self.remote.slots.values().flat_map(|slot| {
            let live_runs = match &slot.state {
                SlotState::Resolved { scope, .. } => Some(scope.live_runs.values()),
                SlotState::Pending | SlotState::Unresolved { .. } => None,
            };
            live_runs
                .into_iter()
                .flatten()
                .filter(|run| !run.action)
                .flat_map(|run| &run.asset_keys)
                .flat_map(|key| &slot.bindings[key])
        })
    }

    /// Local names of remote deps whose owner's latest run failed.
    pub fn remote_failed_keys(&self) -> impl Iterator<Item = &String> {
        self.remote.slots.values().flat_map(|slot| {
            let failed = match &slot.state {
                SlotState::Resolved { scope, .. } => Some(scope.failed.keys()),
                SlotState::Pending | SlotState::Unresolved { .. } => None,
            };
            failed
                .into_iter()
                .flatten()
                .flat_map(|key| &slot.bindings[key])
        })
    }

    /// Owner keys bound to a partitioned local. A keyed run's outcome on
    /// them lives in the partition status, not in a floor.
    pub(super) fn partitioned_remote_keys<'s>(&self, slot: &'s RemoteSlot) -> HashSet<&'s str> {
        slot.bindings
            .iter()
            .filter(|(_, locals)| locals.iter().any(|local| self.is_partitioned(local)))
            .map(|(key, _)| key.as_str())
            .collect()
    }

    /// Resolve the due locations, then follow every resolved one. Returns
    /// `true` if anything changed. A failed read is logged and retried; it
    /// never fails the tick.
    pub(super) async fn refresh_remotes<S: StorageBackend>(
        &mut self,
        storage: &S,
        now: i64,
    ) -> bool {
        let mut changed = false;

        let due: Vec<LocationRef> = self
            .remote
            .slots
            .iter()
            .filter(|(_, slot)| slot.state.due(now))
            .map(|(location, _)| location.clone())
            .collect();
        if !due.is_empty() {
            match resolve_code_locations(storage, &due).await {
                Ok(identities) => {
                    for (location, identity) in identities {
                        changed |= match identity {
                            Some(identity) => {
                                self.resolve_remote(storage, &location, identity, now).await
                            }
                            None => self.unresolve_remote(&location, now),
                        };
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: "rivers::conditions",
                        %error,
                        "code location directory lookup failed; retrying"
                    );
                    for location in &due {
                        changed |= self.postpone_remote(location, now);
                    }
                }
            }
        }

        let resolved: Vec<LocationRef> = self
            .remote
            .slots
            .iter()
            .filter(|(_, slot)| matches!(slot.state, SlotState::Resolved { .. }))
            .map(|(location, _)| location.clone())
            .collect();
        for location in resolved {
            changed |= self.refresh_remote_scope(storage, &location).await;
        }
        changed
    }

    /// `location` is registered under `identity`. A new identity (first
    /// lookup, or a recreated location) reads everything again.
    async fn resolve_remote<S: StorageBackend>(
        &mut self,
        storage: &S,
        location: &LocationRef,
        identity: String,
        now: i64,
    ) -> bool {
        if let SlotState::Resolved {
            scope,
            next_revalidate,
        } = &mut self.remote.slot_mut(location).state
            && scope.ctx.id() == identity
        {
            *next_revalidate = now + REVALIDATE_NANOS;
            return false;
        }

        let ctx = CodeLocationContext::new(identity);
        let slot = &self.remote.slots[location];
        let (scope, read) = match self.load_remote_scope(storage, ctx, slot).await {
            Ok(loaded) => loaded,
            Err(error) => {
                tracing::warn!(
                    target: "rivers::conditions",
                    %location,
                    %error,
                    "reading remote deps failed; retrying"
                );
                return self.postpone_remote(location, now);
            }
        };

        let slot = self.remote.slot_mut(location);
        match slot.state.identity() {
            Some(old) => tracing::info!(
                target: "rivers::conditions",
                %location,
                old,
                new = scope.ctx.id(),
                "remote code location changed identity"
            ),
            None => tracing::info!(
                target: "rivers::conditions",
                %location,
                identity = scope.ctx.id(),
                "remote code location resolved"
            ),
        }
        slot.state = SlotState::Resolved {
            scope,
            next_revalidate: now + REVALIDATE_NANOS,
        };

        self.apply_remote_read(location, read)
    }

    async fn load_remote_scope<S: StorageBackend>(
        &self,
        storage: &S,
        ctx: CodeLocationContext,
        slot: &RemoteSlot,
    ) -> anyhow::Result<(RemoteScope, RemoteRead)> {
        let scoped = storage.for_code_location(&ctx);

        // Cursors first, as in `initial_load`: a run marked applied here was
        // terminal before the reads below.
        let mut run_cursor = 0;
        let mut applied_run_ids = HashMap::new();
        if let Some(newest) = scoped.get_runs(1, None).await?.first() {
            run_cursor = newest.start_time.saturating_sub(1);
            for run in scoped
                .get_runs_since(run_cursor, None, SortOrder::Asc)
                .await?
            {
                if run_status_is_terminal(&run.status) {
                    applied_run_ids.insert(run.run_id, run.start_time);
                }
            }
        }
        let observation_cursor = storage
            .get_latest_observation_ts(ctx.id())
            .await?
            .map_or(-1, |ts| ts - 1);

        // Live runs before records: a run that ends in between is still
        // tracked, and its end re-reads its keys.
        let mut live_runs = HashMap::new();
        for status in [RunStatus::Started, RunStatus::NotStarted, RunStatus::Queued] {
            for run in scoped
                .get_runs_since(0, Some(status), SortOrder::Asc)
                .await?
            {
                if let Some(live) = LiveRemoteRun::of(&run, &slot.bindings) {
                    live_runs.insert(run.run_id, live);
                }
            }
        }

        let read = self
            .read_remote_keys(&scoped, slot, slot.bindings.keys(), |_| true)
            .await?;

        let failed_runs = scoped
            .get_runs_since(0, Some(RunStatus::Failure), SortOrder::Asc)
            .await?;
        let deletion_ts = scoped.get_asset_deletion_timestamps().await?;
        let partitioned = self.partitioned_remote_keys(slot);
        let failed = derive_failure_floors(
            storage,
            &failed_runs,
            |run, key| {
                slot.bindings.contains_key(key)
                    && !(run.partition_key.is_some() && partitioned.contains(key))
            },
            |key| read.record(key),
            &deletion_ts,
        )
        .await?;

        let scope = RemoteScope {
            ctx,
            run_cursor,
            observation_cursor,
            applied_run_ids,
            live_runs,
            failed,
        };
        Ok((scope, read))
    }

    /// Read the record of each of `keys` and, for keys with a partitioned
    /// local, the partition rows: all of them where `full(key)`, otherwise
    /// those newer than the cached ones.
    pub(super) async fn read_remote_keys<'k, S: StorageBackend>(
        &self,
        scoped: &ScopedStorage<'_, S>,
        slot: &RemoteSlot,
        keys: impl IntoIterator<Item = &'k String>,
        full: impl Fn(&str) -> bool,
    ) -> anyhow::Result<RemoteRead> {
        let mut read = RemoteRead::default();
        for key in keys {
            read.records
                .push((key.clone(), scoped.get_asset_record(key).await?));

            let Some(local) = slot.bindings[key]
                .iter()
                .find(|local| self.is_partitioned(local))
            else {
                continue;
            };
            let cached = self
                .partition_status
                .get(local)
                .filter(|_| !full(key))
                .map(|entry| &entry.timestamps);
            let since = cached
                .and_then(|timestamps| timestamps.values().max())
                .map_or(-1, |newest| newest - 1);
            let no_rows = HashMap::new();
            read.partitions.push((
                key.clone(),
                RemotePartitions {
                    timestamps: scoped.get_partition_timestamps_since(key, since).await?,
                    full: cached.is_none(),
                    in_progress: scoped
                        .get_in_progress_partitions(key)
                        .await?
                        .into_iter()
                        .collect(),
                    failed: scoped
                        .get_failed_partitions(key, cached.unwrap_or(&no_rows))
                        .await?,
                },
            ));
        }
        Ok(read)
    }

    /// `location` has no directory entry: its deps count as missing.
    fn unresolve_remote(&mut self, location: &LocationRef, now: i64) -> bool {
        let slot = self.remote.slot_mut(location);
        match &slot.state {
            SlotState::Resolved { .. } => tracing::warn!(
                target: "rivers::conditions",
                %location,
                "remote code location is no longer registered; its assets count as missing"
            ),
            SlotState::Pending => tracing::info!(
                target: "rivers::conditions",
                %location,
                "remote code location is not registered; its assets count as missing"
            ),
            SlotState::Unresolved { .. } => {}
        }
        slot.state = SlotState::Unresolved {
            next_attempt: now + RETRY_NANOS,
        };

        let mut read = RemoteRead::default();
        for (key, locals) in &self.remote.slots[location].bindings {
            read.records.push((key.clone(), None));
            if locals.iter().any(|local| self.is_partitioned(local)) {
                let nothing = RemotePartitions {
                    full: true,
                    ..Default::default()
                };
                read.partitions.push((key.clone(), nothing));
            }
        }
        self.apply_remote_read(location, read)
    }

    /// Retry `location` soon and keep what is known. A dep with nothing
    /// known yet counts as missing.
    fn postpone_remote(&mut self, location: &LocationRef, now: i64) -> bool {
        self.remote
            .slot_mut(location)
            .state
            .retry_at(now + RETRY_NANOS);

        let mut changed = false;
        let locals: Vec<String> = self.remote.slots[location]
            .bindings
            .values()
            .flatten()
            .cloned()
            .collect();
        for local in locals {
            if !self.records.contains_key(&local) {
                let placeholder = self.remote_placeholder(&local);
                self.records.insert(local.clone(), placeholder);
                changed = true;
            }
            if self.is_partitioned(&local) && !self.partition_status.contains_key(&local) {
                self.partition_status
                    .insert(local, PartitionStatusEntry::default());
                changed = true;
            }
        }
        changed
    }

    /// A record that was never materialized, so `missing` holds for it. A dep
    /// with no record at all evaluates false for every condition.
    fn remote_placeholder(&self, local: &str) -> AssetRecord {
        AssetRecord {
            code_location_id: self.ctx.id().to_string(),
            asset_key: local.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    /// Store `read` under every local bound to each key.
    pub(super) fn apply_remote_read(&mut self, location: &LocationRef, read: RemoteRead) -> bool {
        let slot = &self.remote.slots[location];
        let mut records = Vec::new();
        for (key, record) in &read.records {
            for local in &slot.bindings[key] {
                records.push(match record {
                    Some(record) => AssetRecord {
                        asset_key: local.clone(),
                        ..record.clone()
                    },
                    None => self.remote_placeholder(local),
                });
            }
        }
        let mut partitions = Vec::new();
        for (key, patch) in read.partitions {
            let live = slot.live_partitions(&key);
            for local in slot.bindings[&key]
                .iter()
                .filter(|l| self.is_partitioned(l))
            {
                partitions.push((local.clone(), patch.clone(), live.clone()));
            }
        }

        let mut changed = false;
        for record in records {
            if self.records.get(&record.asset_key) != Some(&record) {
                self.records.insert(record.asset_key.clone(), record);
                changed = true;
            }
        }
        for (local, patch, live) in partitions {
            let entry = self.partition_status.entry(local).or_default();
            changed |= patch.apply(entry, &live);
        }
        changed
    }

    /// Mark the partitions of `key` that live runs materialize as in progress.
    pub(super) fn add_live_partitions(&mut self, location: &LocationRef, key: &str) -> bool {
        let slot = &self.remote.slots[location];
        let live = slot.live_partitions(key);
        let locals: Vec<String> = slot.bindings[key]
            .iter()
            .filter(|local| self.is_partitioned(local))
            .cloned()
            .collect();

        let mut changed = false;
        for local in locals {
            let entry = self.partition_status.entry(local).or_default();
            for pk in &live {
                changed |= entry.in_progress.insert(pk.clone());
            }
        }
        changed
    }
}
