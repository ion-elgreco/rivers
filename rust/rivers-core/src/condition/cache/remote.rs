//! Remote deps: upstream assets owned by another code location. Their
//! records and partition status are read from the owner's scope and kept
//! under the local name, so the evaluator sees them like local deps.

use std::collections::{HashMap, HashSet};

use crate::assets::remote::LocationRef;
use crate::storage::{
    AssetRecord, CodeLocationContext, ScopedStorage, StorageBackend, resolve_code_locations,
};

use super::{AssetConditionCache, PartitionStatusEntry};

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
    slots: HashMap<LocationRef, RemoteSlot>,
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

    fn slot_mut(&mut self, location: &LocationRef) -> &mut RemoteSlot {
        self.slots
            .get_mut(location)
            .expect("refresh only visits registered locations")
    }
}

struct RemoteSlot {
    /// The owner's asset key → the local names bound to it.
    bindings: HashMap<String, Vec<String>>,
    state: SlotState,
}

enum SlotState {
    /// Not looked up yet.
    Pending,
    /// No directory entry.
    Unresolved { next_attempt: i64 },
    Resolved {
        ctx: CodeLocationContext,
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
            Self::Resolved { ctx, .. } => Some(ctx.id()),
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

/// One location's state, keyed by local name.
#[derive(Default)]
struct RemoteSnapshot {
    records: Vec<AssetRecord>,
    partitions: Vec<(String, PartitionStatusEntry)>,
}

/// Partition status of `asset_key` in `scoped`'s location.
pub(super) async fn fetch_partition_status<S: StorageBackend>(
    scoped: &ScopedStorage<'_, S>,
    asset_key: &str,
) -> anyhow::Result<PartitionStatusEntry> {
    let timestamps: HashMap<_, _> = scoped
        .get_partition_timestamps(asset_key)
        .await?
        .into_iter()
        .collect();
    let in_progress = scoped
        .get_in_progress_partitions(asset_key)
        .await?
        .into_iter()
        .collect();
    let failed_timestamps = scoped.get_failed_partitions(asset_key, &timestamps).await?;

    Ok(PartitionStatusEntry {
        in_progress,
        failed: failed_timestamps.keys().cloned().collect(),
        failed_timestamps,
        timestamps,
    })
}

impl AssetConditionCache {
    /// Register the remote deps; each is read from its owner's scope.
    pub fn set_remote_deps(&mut self, deps: Vec<RemoteDep>) {
        self.remote = RemoteScopes::new(deps);
    }

    /// Bring every due location up to date. Returns `true` if anything
    /// changed. A failed read is logged and retried; it never fails the tick.
    pub(super) async fn refresh_remotes<S: StorageBackend>(
        &mut self,
        storage: &S,
        now: i64,
    ) -> bool {
        let due: Vec<LocationRef> = self
            .remote
            .slots
            .iter()
            .filter(|(_, slot)| slot.state.due(now))
            .map(|(location, _)| location.clone())
            .collect();
        if due.is_empty() {
            return false;
        }

        let identities = match resolve_code_locations(storage, &due).await {
            Ok(identities) => identities,
            Err(error) => {
                tracing::warn!(
                    target: "rivers::conditions",
                    %error,
                    "code location directory lookup failed; retrying"
                );
                let mut changed = false;
                for location in &due {
                    changed |= self.postpone_remote(location, now);
                }
                return changed;
            }
        };

        let mut changed = false;
        for (location, identity) in identities {
            changed |= match identity {
                Some(identity) => self.load_remote(storage, &location, identity, now).await,
                None => self.unresolve_remote(&location, now),
            };
        }
        changed
    }

    /// Read the owner's state for `location` under `identity`.
    async fn load_remote<S: StorageBackend>(
        &mut self,
        storage: &S,
        location: &LocationRef,
        identity: String,
        now: i64,
    ) -> bool {
        let ctx = CodeLocationContext::new(identity);
        let slot = &self.remote.slots[location];
        let snapshot = match self.fetch_remote_snapshot(storage, &ctx, slot).await {
            Ok(snapshot) => snapshot,
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
            Some(old) if old == ctx.id() => {}
            Some(old) => tracing::info!(
                target: "rivers::conditions",
                %location,
                old,
                new = ctx.id(),
                "remote code location changed identity"
            ),
            None => tracing::info!(
                target: "rivers::conditions",
                %location,
                identity = ctx.id(),
                "remote code location resolved"
            ),
        }
        slot.state = SlotState::Resolved {
            ctx,
            next_revalidate: now + REVALIDATE_NANOS,
        };

        self.apply_remote_snapshot(snapshot)
    }

    async fn fetch_remote_snapshot<S: StorageBackend>(
        &self,
        storage: &S,
        ctx: &CodeLocationContext,
        slot: &RemoteSlot,
    ) -> anyhow::Result<RemoteSnapshot> {
        let scoped = storage.for_code_location(ctx);
        let mut snapshot = RemoteSnapshot::default();
        for (asset_key, locals) in &slot.bindings {
            let record = scoped.get_asset_record(asset_key).await?;
            let partitions = if locals.iter().any(|local| self.is_partitioned(local)) {
                Some(fetch_partition_status(&scoped, asset_key).await?)
            } else {
                None
            };

            for local in locals {
                snapshot.records.push(match &record {
                    Some(record) => AssetRecord {
                        asset_key: local.clone(),
                        ..record.clone()
                    },
                    None => self.remote_placeholder(local),
                });
                if let Some(entry) = &partitions
                    && self.is_partitioned(local)
                {
                    snapshot.partitions.push((local.clone(), entry.clone()));
                }
            }
        }
        Ok(snapshot)
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

        let snapshot = self.remote_placeholders(location);
        self.apply_remote_snapshot(snapshot)
    }

    /// Retry `location` soon and keep what is known. A dep with nothing
    /// known yet gets a placeholder.
    fn postpone_remote(&mut self, location: &LocationRef, now: i64) -> bool {
        self.remote
            .slot_mut(location)
            .state
            .retry_at(now + RETRY_NANOS);

        let mut snapshot = self.remote_placeholders(location);
        snapshot
            .records
            .retain(|r| !self.records.contains_key(&r.asset_key));
        snapshot
            .partitions
            .retain(|(local, _)| !self.partition_status.contains_key(local));
        self.apply_remote_snapshot(snapshot)
    }

    fn remote_placeholders(&self, location: &LocationRef) -> RemoteSnapshot {
        let mut snapshot = RemoteSnapshot::default();
        for local in self.remote.slots[location].bindings.values().flatten() {
            snapshot.records.push(self.remote_placeholder(local));
            if self.is_partitioned(local) {
                snapshot
                    .partitions
                    .push((local.clone(), PartitionStatusEntry::default()));
            }
        }
        snapshot
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

    fn apply_remote_snapshot(&mut self, snapshot: RemoteSnapshot) -> bool {
        let mut changed = false;
        for record in snapshot.records {
            if self.records.get(&record.asset_key) != Some(&record) {
                self.records.insert(record.asset_key.clone(), record);
                changed = true;
            }
        }
        for (local, entry) in snapshot.partitions {
            if self.partition_status.get(&local) != Some(&entry) {
                self.partition_status.insert(local, entry);
                changed = true;
            }
        }
        changed
    }
}
