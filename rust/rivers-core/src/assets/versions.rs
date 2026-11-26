//! The version of the stored data of each asset against the version that
//! its code declares (RFC-048). Registration records the declared versions;
//! the gate holds or refuses the runs that would touch an asset whose data
//! is not at that version.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use petgraph::Direction;

use super::graph::{AssetGraph, GraphTopology, NodeKind};
use crate::storage::{
    AssetVersionRecord, AssetVersionState, MigrationKind, ScopedStorage, StorageBackend,
};
use crate::util::now_ts;

/// How many times registration reads a version row again when another
/// writer changed it first.
const REGISTER_ATTEMPTS: usize = 10;

/// The version row of an asset that has none. Data stored before the asset
/// declared a version is at version 1; an asset without stored data starts
/// at `declared`. `partitions` is the partitions definition of the stored
/// data, as JSON.
pub fn first_version_row(
    code_location_id: &str,
    asset_key: &str,
    declared: u32,
    has_data: bool,
    partitions: Option<String>,
) -> AssetVersionRecord {
    let row = AssetVersionRecord {
        code_location_id: code_location_id.to_string(),
        asset_key: asset_key.to_string(),
        applied_version: if has_data { 1 } else { declared },
        partitions,
        state: AssetVersionState::Ready,
        target_version: None,
        reason: None,
        started_at: None,
        run_id: None,
        backfill_id: None,
        owner: None,
        lease_expires_at: None,
        revision: 0,
        updated_at: 0,
    };
    registered_version(&row, declared).unwrap_or(row)
}

/// `row` after code that declares `declared` registers, or `None` when the
/// row does not change. A `ready` row below `declared` gets a pending
/// migration to it, and a pending migration moves its target up to it. A
/// migration that began, failed or is blocked keeps its target. Code that
/// declares the applied version or a lower one is old code: it changes
/// nothing.
pub fn registered_version(row: &AssetVersionRecord, declared: u32) -> Option<AssetVersionRecord> {
    if declared <= row.applied_version {
        return None;
    }
    match row.state {
        AssetVersionState::Ready => Some(AssetVersionRecord {
            state: AssetVersionState::Pending,
            target_version: Some(declared),
            reason: None,
            started_at: None,
            run_id: None,
            backfill_id: None,
            owner: None,
            lease_expires_at: None,
            ..row.clone()
        }),
        AssetVersionState::Pending if row.target_version < Some(declared) => {
            Some(AssetVersionRecord {
                target_version: Some(declared),
                ..row.clone()
            })
        }
        _ => None,
    }
}

/// Record the versions that this code declares, `(asset key, version)` for
/// each asset that it owns, before `topology`, the code's own, replaces the
/// stored topology. Only an asset that declares a version above 1 gets a
/// row. For the first row of an asset with stored data, the stored topology
/// gives the partitions definition of that data.
pub async fn register_asset_versions<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    declared: &[(&str, u32)],
    topology: &GraphTopology,
) -> Result<()> {
    let declared: Vec<(&str, u32)> = declared
        .iter()
        .copied()
        .filter(|&(_, version)| version > 1)
        .collect();
    if declared.is_empty() {
        return Ok(());
    }

    let keys: Vec<String> = declared.iter().map(|(key, _)| key.to_string()).collect();
    let mut rows: HashMap<String, AssetVersionRecord> = storage
        .get_asset_versions(Some(&keys))
        .await?
        .into_iter()
        .map(|row| (row.asset_key.clone(), row))
        .collect();
    let mut first_rows = first_version_rows(storage, &declared, &rows, topology).await?;

    for (key, version) in declared {
        let row = rows.remove(key);
        let first = first_rows.remove(key);
        register_version(storage, key, version, row, first).await?;
    }
    Ok(())
}

/// The first rows of the `declared` assets that have no row in `rows`.
async fn first_version_rows<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    declared: &[(&str, u32)],
    rows: &HashMap<String, AssetVersionRecord>,
    topology: &GraphTopology,
) -> Result<HashMap<String, AssetVersionRecord>> {
    let new: Vec<(&str, u32)> = declared
        .iter()
        .copied()
        .filter(|(key, _)| !rows.contains_key(*key))
        .collect();
    if new.is_empty() {
        return Ok(HashMap::new());
    }

    let keys: Vec<String> = new.iter().map(|(key, _)| key.to_string()).collect();
    let with_data: HashSet<String> = storage
        .get_asset_records_by_keys(&keys)
        .await?
        .into_iter()
        .filter(|record| record.last_timestamp.is_some())
        .map(|record| record.asset_key)
        .collect();
    let stored = if with_data.is_empty() {
        None
    } else {
        storage.get_graph_topology().await?
    };

    let mut first_rows = HashMap::new();
    for (key, version) in new {
        let has_data = with_data.contains(key);
        // Stored data has the definition in the stored topology; without
        // stored data or a stored node, the code's definition applies.
        let node = stored
            .as_ref()
            .filter(|_| has_data)
            .and_then(|stored| stored.owned_node(key))
            .or_else(|| topology.owned_node(key));
        let partitions = node
            .and_then(|node| node.partitions.as_ref())
            .map(serde_json::to_string)
            .transpose()?;
        let row = first_version_row(
            storage.code_location_id(),
            key,
            version,
            has_data,
            partitions,
        );
        first_rows.insert(key.to_string(), row);
    }
    Ok(first_rows)
}

/// Write the row of one asset. After a write that another writer won, read
/// the row again and decide again.
async fn register_version<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    key: &str,
    declared: u32,
    mut row: Option<AssetVersionRecord>,
    first: Option<AssetVersionRecord>,
) -> Result<()> {
    for _ in 0..REGISTER_ATTEMPTS {
        let (next, expected) = match (&row, &first) {
            (Some(row), _) => match registered_version(row, declared) {
                Some(next) => (next, Some(row.revision)),
                None => return Ok(()),
            },
            (None, Some(first)) => (first.clone(), None),
            (None, None) => bail!("asset '{key}': its version row was deleted during registration"),
        };
        if storage.put_asset_version(&next, expected).await? {
            return Ok(());
        }
        row = storage
            .get_asset_versions(Some(&[key.to_string()]))
            .await?
            .pop();
    }
    bail!(
        "asset '{key}': other writers changed its version row {REGISTER_ATTEMPTS} times during \
         registration"
    )
}

/// What the gate decides for a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionGate {
    Go,
    /// The run waits until the asset is ready.
    Wait(String),
    /// The run never runs: its code is older than the stored data.
    Refuse(String),
}

impl VersionGate {
    /// Why the gate holds or refuses the run.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Go => None,
            Self::Wait(reason) | Self::Refuse(reason) => Some(reason),
        }
    }
}

/// A run as the gate sees it. Each asset comes with the version that the
/// run's code declares for it: `None` for an asset that the code does not
/// own.
#[derive(Debug, Default)]
pub struct GatedRun<'a> {
    /// `None` for the runs of a new backfill, before they exist.
    pub run_id: Option<&'a str>,
    /// The backfill that the run is part of.
    pub backfill_id: Option<&'a str>,
    /// The assets that the run materializes or runs an action on.
    pub writes: Vec<(&'a str, Option<u32>)>,
    /// The assets that those read: their inputs and deps.
    pub reads: Vec<(&'a str, Option<u32>)>,
    /// `(asset, version, time)` for each migration that the run's code
    /// declares with a start time.
    pub start_times: Vec<(&'a str, u32, i64)>,
}

impl GatedRun<'_> {
    /// The assets whose version rows the gate reads.
    pub fn asset_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .writes
            .iter()
            .chain(&self.reads)
            .map(|(key, _)| key.to_string())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// When the migration in `row` starts, by the run's code.
    fn start_at(&self, row: &AssetVersionRecord) -> Option<i64> {
        latest_start(
            row,
            self.start_times
                .iter()
                .filter(|(key, ..)| *key == row.asset_key)
                .map(|&(_, to, at)| (to, at)),
        )
    }
}

/// When the migration in `row` starts: the latest of the start times
/// `(version, time)` of the versions that it applies. `None` when none of
/// them has one: it starts at once.
fn latest_start(row: &AssetVersionRecord, starts: impl Iterator<Item = (u32, i64)>) -> Option<i64> {
    let target = row.target_version?;
    starts
        .filter(|&(to, _)| to > row.applied_version && to <= target)
        .map(|(_, at)| at)
        .max()
}

/// Whether `run` may run against the version rows `rows`. Only an asset with
/// a row is checked:
///
/// - code that declares a lower version than the stored data never writes
///   the asset;
/// - a row that is not `ready` holds the run, unless the run is the
///   migration's own work;
/// - code that declares a higher version than a `ready` row waits for that
///   migration.
///
/// A refusal wins over a wait. The run's assets are checked in key order,
/// the ones it writes first.
pub fn asset_version_gate(run: &GatedRun<'_>, rows: &[AssetVersionRecord]) -> VersionGate {
    let rows: HashMap<&str, &AssetVersionRecord> = rows
        .iter()
        .map(|row| (row.asset_key.as_str(), row))
        .collect();
    let mut writes = run.writes.clone();
    writes.sort_unstable();
    let mut reads = run.reads.clone();
    reads.sort_unstable();

    let mut wait = None;
    let assets = writes
        .iter()
        .map(|asset| (asset, true))
        .chain(reads.iter().map(|asset| (asset, false)));
    for (&(key, declared), written) in assets {
        let Some(row) = rows.get(key) else {
            continue;
        };
        if written
            && let Some(declared) = declared
            && declared < row.applied_version
        {
            return VersionGate::Refuse(format!(
                "this code declares {key} version {declared}; the stored data is at version {}",
                row.applied_version
            ));
        }
        if wait.is_none() {
            wait = wait_reason(run, key, declared, row);
        }
    }
    wait.map_or(VersionGate::Go, VersionGate::Wait)
}

/// Why `run` waits for the asset `key`, if it does.
fn wait_reason(
    run: &GatedRun<'_>,
    key: &str,
    declared: Option<u32>,
    row: &AssetVersionRecord,
) -> Option<String> {
    let target = row.target_version.unwrap_or(row.applied_version);
    match row.state {
        AssetVersionState::Ready => declared
            .filter(|&declared| declared > row.applied_version)
            .map(|declared| format!("{key} waits for its migration to version {declared}")),
        _ if is_migration_work(run, row) => None,
        AssetVersionState::Pending
            if let Some(at) = run.start_at(row).filter(|&at| at > now_ts()) =>
        {
            Some(format!(
                "{key} waits for its migration to version {target}, which starts at {}",
                timestamp(at)
            ))
        }
        AssetVersionState::Pending | AssetVersionState::Draining | AssetVersionState::Running => {
            Some(format!("{key} is migrating to version {target}"))
        }
        AssetVersionState::Failed => Some(match &row.reason {
            Some(reason) => format!("the migration of {key} to version {target} failed: {reason}"),
            None => format!("the migration of {key} to version {target} failed"),
        }),
        AssetVersionState::Blocked => Some(
            row.reason
                .clone()
                .unwrap_or_else(|| format!("{key} is blocked")),
        ),
    }
}

/// `at`, nanoseconds since the epoch, as an RFC 3339 time in UTC.
fn timestamp(at: i64) -> String {
    jiff::Timestamp::from_nanosecond(i128::from(at))
        .map_or_else(|_| at.to_string(), |at| at.to_string())
}

/// Whether `run` is the convert run or a run of the rebuild backfill of the
/// migration in `row`.
fn is_migration_work(run: &GatedRun<'_>, row: &AssetVersionRecord) -> bool {
    run.run_id
        .is_some_and(|id| row.run_id.as_deref() == Some(id))
        || run
            .backfill_id
            .is_some_and(|id| row.backfill_id.as_deref() == Some(id))
}

/// The version that the convert run `run_id` moves the stored data of `key`
/// to: the one after the applied version, when `row` is a running migration
/// that started this run. A convert runs only for its migration, so that a
/// stale or manual run never converts the data twice.
pub fn convert_version(
    key: &str,
    row: Option<&AssetVersionRecord>,
    run_id: &str,
) -> std::result::Result<u32, String> {
    row.filter(|row| {
        row.state == AssetVersionState::Running
            && row.run_id.as_deref() == Some(run_id)
            && row
                .target_version
                .is_some_and(|target| target > row.applied_version)
    })
    .map(|row| row.applied_version + 1)
    .ok_or_else(|| {
        format!(
            "no migration of {key} started this run; the migrate action runs only for a migration"
        )
    })
}

/// [`asset_version_gate`] over the stored rows of the run's assets.
pub async fn check_asset_versions<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    run: &GatedRun<'_>,
) -> Result<VersionGate> {
    let keys = run.asset_keys();
    if keys.is_empty() {
        return Ok(VersionGate::Go);
    }
    let rows = storage.get_asset_versions(Some(&keys)).await?;
    Ok(asset_version_gate(run, &rows))
}

/// A migration that the code declares: the step of an asset's stored data
/// to version `to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredMigration {
    pub to: u32,
    pub kind: MigrationKind,
    /// Readers of the version before cannot read the new data.
    pub breaking: bool,
    /// Nanoseconds since the epoch: the migration does not start before.
    pub start_at: Option<i64>,
}

/// What the gate and the migration loop need from the code of a code
/// location: the version that the code declares for each asset that it
/// owns, the assets that each node reads, and the migrations of each asset.
#[derive(Debug, Default)]
pub struct CodeVersions {
    declared: HashMap<String, u32>,
    reads: HashMap<String, Vec<String>>,
    migrations: HashMap<String, Vec<DeclaredMigration>>,
    partitioned: HashSet<String>,
}

impl CodeVersions {
    /// `declared` holds the version of each asset that the code owns.
    pub fn new(graph: &AssetGraph, declared: HashMap<String, u32>) -> Self {
        let reads = graph
            .node_indices()
            .map(|idx| {
                let name = &graph[idx].name;
                (name.clone(), read_assets(graph, std::slice::from_ref(name)))
            })
            .collect();
        Self {
            declared,
            reads,
            ..Self::default()
        }
    }

    /// The same, with the migrations of each asset and the assets that have
    /// partitions.
    pub fn with_migrations(
        self,
        migrations: HashMap<String, Vec<DeclaredMigration>>,
        partitioned: HashSet<String>,
    ) -> Self {
        Self {
            migrations,
            partitioned,
            ..self
        }
    }

    /// The version that the code declares for `key`: `None` for an asset
    /// that it does not own.
    pub fn declared(&self, key: &str) -> Option<u32> {
        self.declared.get(key).copied()
    }

    /// The migration of `key` to version `to`.
    pub fn migration(&self, key: &str, to: u32) -> Option<DeclaredMigration> {
        self.migrations
            .get(key)?
            .iter()
            .find(|migration| migration.to == to)
            .copied()
    }

    pub fn is_partitioned(&self, key: &str) -> bool {
        self.partitioned.contains(key)
    }

    /// When the migration in `row` starts, by this code: the latest start
    /// time of the versions that it applies. `None` when it starts at once.
    pub fn start_at(&self, row: &AssetVersionRecord) -> Option<i64> {
        latest_start(
            row,
            self.start_times(&row.asset_key).map(|(_, to, at)| (to, at)),
        )
    }

    /// `(asset, version, time)` for each migration of `key` with a start
    /// time.
    fn start_times<'a>(&'a self, key: &'a str) -> impl Iterator<Item = (&'a str, u32, i64)> {
        self.migrations
            .get(key)
            .into_iter()
            .flatten()
            .filter_map(move |m| Some((key, m.to, m.start_at?)))
    }

    /// The assets that `key` reads: its inputs and deps.
    pub fn reads(&self, key: &str) -> &[String] {
        self.reads.get(key).map_or(&[], Vec::as_slice)
    }

    /// The assets that this code owns.
    pub fn assets(&self) -> impl Iterator<Item = &str> {
        self.declared.keys().map(String::as_str)
    }

    /// `keys` and each asset that they read, directly or through other
    /// assets.
    pub fn with_upstream<'a>(
        &'a self,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> HashSet<&'a str> {
        let mut seen = HashSet::new();
        let mut stack: Vec<&str> = keys.into_iter().collect();
        while let Some(key) = stack.pop() {
            if seen.insert(key) {
                stack.extend(self.reads(key).iter().map(String::as_str));
            }
        }
        seen
    }

    /// `key` and the nodes that read it.
    pub fn touching<'a>(&'a self, key: &'a str) -> HashSet<&'a str> {
        self.reads
            .iter()
            .filter(|(_, reads)| reads.iter().any(|read| read == key))
            .map(|(name, _)| name.as_str())
            .chain([key])
            .collect()
    }

    /// Whether a run that writes `writes` may run against `rows`: the run
    /// `run_id`, or a run of the backfill `backfill_id`.
    pub fn gate(
        &self,
        run_id: Option<&str>,
        backfill_id: Option<&str>,
        writes: &[String],
        rows: &[AssetVersionRecord],
    ) -> VersionGate {
        if rows.is_empty() {
            return VersionGate::Go;
        }
        let written: HashSet<&str> = writes.iter().map(String::as_str).collect();
        let reads: HashSet<&str> = writes
            .iter()
            .filter_map(|name| self.reads.get(name))
            .flatten()
            .map(String::as_str)
            .filter(|name| !written.contains(name))
            .collect();
        let declared = |name: &str| self.declared.get(name).copied();
        let start_times = written
            .iter()
            .chain(&reads)
            .flat_map(|&name| self.start_times(name))
            .collect();
        let run = GatedRun {
            run_id,
            backfill_id,
            writes: written
                .into_iter()
                .map(|name| (name, declared(name)))
                .collect(),
            reads: reads
                .into_iter()
                .map(|name| (name, declared(name)))
                .collect(),
            start_times,
        };
        asset_version_gate(&run, rows)
    }

    /// The assets of `keys` that automation leaves alone for now, each with
    /// the reason: a new run of the asset would wait or be refused at the
    /// gate.
    pub fn paused<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a str>,
        rows: &[AssetVersionRecord],
    ) -> HashMap<String, String> {
        // A ready row at the version that this code declares, or of an asset
        // that this code does not own, holds no run.
        let rows: Vec<AssetVersionRecord> = rows
            .iter()
            .filter(|row| {
                row.state != AssetVersionState::Ready
                    || self
                        .declared(&row.asset_key)
                        .is_some_and(|declared| declared != row.applied_version)
            })
            .cloned()
            .collect();
        if rows.is_empty() {
            return HashMap::new();
        }
        keys.into_iter()
            .filter_map(|key| {
                let gate = self.gate(None, None, &[key.to_string()], &rows);
                Some((key.to_string(), gate.reason()?.to_string()))
            })
            .collect()
    }

    /// [`Self::gate`] over the stored rows, for a new manual run or backfill.
    pub async fn check<S: StorageBackend>(
        &self,
        storage: &ScopedStorage<'_, S>,
        backfill_id: Option<&str>,
        writes: &[String],
    ) -> Result<VersionGate> {
        let rows = storage.get_asset_versions(None).await?;
        Ok(self.gate(None, backfill_id, writes, &rows))
    }
}

/// The assets that the nodes `writes` read: the inputs and deps of each,
/// through tasks, which store nothing. `writes` are left out, and so is a
/// name that is not in `graph`.
fn read_assets(graph: &AssetGraph, writes: &[String]) -> Vec<String> {
    let index: HashMap<&str, _> = graph
        .node_indices()
        .map(|idx| (graph[idx].name.as_str(), idx))
        .collect();
    let written: HashSet<&str> = writes.iter().map(String::as_str).collect();
    let mut seen = HashSet::new();
    let mut stack: Vec<_> = writes
        .iter()
        .filter_map(|name| index.get(name.as_str()).copied())
        .collect();
    let mut reads = Vec::new();
    while let Some(idx) = stack.pop() {
        for dep in graph.neighbors_directed(idx, Direction::Outgoing) {
            if !seen.insert(dep) {
                continue;
            }
            let node = &graph[dep];
            if node.kind == NodeKind::Task {
                stack.push(dep);
            } else if !written.contains(node.name.as_str()) {
                reads.push(node.name.clone());
            }
        }
    }
    reads.sort_unstable();
    reads
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(applied_version: u32, state: AssetVersionState) -> AssetVersionRecord {
        AssetVersionRecord {
            code_location_id: "sales".to_string(),
            asset_key: "orders".to_string(),
            applied_version,
            partitions: Some(r#"{"kind":"dynamic","name":"regions"}"#.to_string()),
            state,
            target_version: None,
            reason: None,
            started_at: None,
            run_id: None,
            backfill_id: None,
            owner: None,
            lease_expires_at: None,
            revision: 4,
            updated_at: 100,
        }
    }

    fn pending(applied_version: u32, target_version: u32) -> AssetVersionRecord {
        AssetVersionRecord {
            target_version: Some(target_version),
            ..row(applied_version, AssetVersionState::Pending)
        }
    }

    #[test]
    fn an_asset_without_stored_data_starts_at_its_declared_version() {
        let first = first_version_row("sales", "orders", 3, false, Some("{}".to_string()));

        assert_eq!(
            first,
            AssetVersionRecord {
                partitions: Some("{}".to_string()),
                revision: 0,
                updated_at: 0,
                ..row(3, AssetVersionState::Ready)
            }
        );
    }

    #[test]
    fn stored_data_starts_at_version_1_and_waits_for_a_migration() {
        let first = first_version_row("sales", "orders", 3, true, None);

        assert_eq!(
            first,
            AssetVersionRecord {
                partitions: None,
                revision: 0,
                updated_at: 0,
                ..pending(1, 3)
            }
        );
    }

    #[test]
    fn a_higher_declared_version_makes_a_ready_row_pending() {
        // What the last migration left behind goes; the data stays as it is.
        let ready = AssetVersionRecord {
            reason: Some("old".to_string()),
            started_at: Some(50),
            run_id: Some("run-1".to_string()),
            backfill_id: Some("backfill-1".to_string()),
            owner: Some("daemon-1".to_string()),
            lease_expires_at: Some(60),
            ..row(2, AssetVersionState::Ready)
        };

        assert_eq!(registered_version(&ready, 4), Some(pending(2, 4)));
    }

    #[test]
    fn a_pending_migration_moves_its_target_up_only() {
        assert_eq!(registered_version(&pending(1, 2), 3), Some(pending(1, 3)));
        assert_eq!(registered_version(&pending(1, 3), 3), None);
        assert_eq!(registered_version(&pending(1, 3), 2), None);
    }

    #[test]
    fn old_code_and_the_same_code_change_nothing() {
        for state in [
            AssetVersionState::Ready,
            AssetVersionState::Pending,
            AssetVersionState::Draining,
            AssetVersionState::Running,
            AssetVersionState::Failed,
            AssetVersionState::Blocked,
        ] {
            for declared in [1, 2, 3] {
                assert_eq!(registered_version(&row(3, state), declared), None);
            }
        }
    }

    #[test]
    fn a_migration_that_began_failed_or_is_blocked_keeps_its_target() {
        for state in [
            AssetVersionState::Draining,
            AssetVersionState::Running,
            AssetVersionState::Failed,
            AssetVersionState::Blocked,
        ] {
            let busy = AssetVersionRecord {
                target_version: Some(3),
                ..row(1, state)
            };
            assert_eq!(registered_version(&busy, 4), None, "{state:?}");
        }
    }

    fn versioned(
        asset_key: &str,
        applied_version: u32,
        state: AssetVersionState,
    ) -> AssetVersionRecord {
        AssetVersionRecord {
            asset_key: asset_key.to_string(),
            target_version: (state != AssetVersionState::Ready).then_some(applied_version + 1),
            ..row(applied_version, state)
        }
    }

    fn gated<'a>(
        writes: &[(&'a str, Option<u32>)],
        reads: &[(&'a str, Option<u32>)],
    ) -> GatedRun<'a> {
        GatedRun {
            run_id: Some("run-1"),
            backfill_id: None,
            writes: writes.to_vec(),
            reads: reads.to_vec(),
            start_times: Vec::new(),
        }
    }

    fn wait(reason: &str) -> VersionGate {
        VersionGate::Wait(reason.to_string())
    }

    /// 2100-01-01T00:00:00Z and 2001-01-01T00:00:00Z, in nanoseconds.
    const LATER: i64 = 4_102_444_800_000_000_000;
    const PASSED: i64 = 978_307_200_000_000_000;

    #[test]
    fn a_migration_that_starts_later_holds_the_run_until_its_start() {
        let reader = |start_times| GatedRun {
            start_times,
            ..gated(&[("summary", Some(1))], &[("orders", Some(2))])
        };
        let pending = [versioned("orders", 1, AssetVersionState::Pending)];

        assert_eq!(
            asset_version_gate(&reader(vec![("orders", 2, LATER)]), &pending),
            wait(
                "orders waits for its migration to version 2, which starts at 2100-01-01T00:00:00Z"
            )
        );
        // A start that passed, or one of a version that the migration does
        // not apply, does not move the start.
        for start_times in [
            vec![("orders", 2, PASSED)],
            vec![("orders", 3, LATER)],
            vec![("customers", 2, LATER)],
        ] {
            assert_eq!(
                asset_version_gate(&reader(start_times), &pending),
                wait("orders is migrating to version 2")
            );
        }
        let draining = [versioned("orders", 1, AssetVersionState::Draining)];
        assert_eq!(
            asset_version_gate(&reader(vec![("orders", 2, LATER)]), &draining),
            wait("orders is migrating to version 2")
        );
    }

    #[test]
    fn assets_without_a_row_go() {
        let rows = [versioned("customers", 2, AssetVersionState::Running)];
        let run = gated(&[("orders", Some(3))], &[("payments", Some(1))]);

        assert_eq!(asset_version_gate(&run, &rows), VersionGate::Go);
        assert_eq!(asset_version_gate(&run, &[]), VersionGate::Go);
    }

    #[test]
    fn a_migration_holds_the_runs_that_write_or_read_its_asset() {
        for state in [
            AssetVersionState::Pending,
            AssetVersionState::Draining,
            AssetVersionState::Running,
        ] {
            let rows = [versioned("orders", 2, state)];
            let writer = gated(&[("orders", Some(3))], &[]);
            let reader = gated(&[("summary", Some(1))], &[("orders", Some(3))]);

            for run in [writer, reader] {
                assert_eq!(
                    asset_version_gate(&run, &rows),
                    wait("orders is migrating to version 3"),
                    "{state:?}"
                );
            }
        }
    }

    #[test]
    fn a_failed_or_blocked_migration_holds_with_its_reason() {
        let failed = AssetVersionRecord {
            reason: Some("KeyError: 'currency'".to_string()),
            ..versioned("orders", 2, AssetVersionState::Failed)
        };
        let blocked = AssetVersionRecord {
            reason: Some("the partition keys of orders changed".to_string()),
            ..versioned("orders", 2, AssetVersionState::Blocked)
        };
        let run = gated(&[("orders", Some(3))], &[]);

        assert_eq!(
            asset_version_gate(&run, &[failed]),
            wait("the migration of orders to version 3 failed: KeyError: 'currency'")
        );
        assert_eq!(
            asset_version_gate(&run, &[blocked]),
            wait("the partition keys of orders changed")
        );
    }

    #[test]
    fn the_runs_of_the_migration_itself_go() {
        let converting = AssetVersionRecord {
            run_id: Some("run-1".to_string()),
            ..versioned("orders", 2, AssetVersionState::Running)
        };
        let rebuilding = AssetVersionRecord {
            backfill_id: Some("backfill-1".to_string()),
            ..versioned("orders", 2, AssetVersionState::Running)
        };
        let convert = gated(&[("orders", Some(3))], &[]);
        let rebuild = GatedRun {
            run_id: Some("run-2"),
            backfill_id: Some("backfill-1"),
            ..gated(&[("orders", Some(3))], &[])
        };
        let other = GatedRun {
            run_id: Some("run-3"),
            backfill_id: Some("backfill-2"),
            ..gated(&[("orders", Some(3))], &[])
        };

        assert_eq!(
            asset_version_gate(&convert, &[converting.clone()]),
            VersionGate::Go
        );
        assert_eq!(
            asset_version_gate(&rebuild, &[rebuilding.clone()]),
            VersionGate::Go
        );
        for row in [converting, rebuilding] {
            assert_eq!(
                asset_version_gate(&other, &[row]),
                wait("orders is migrating to version 3")
            );
        }
        // The runs of a new backfill have no run id yet.
        let new_backfill = GatedRun {
            run_id: None,
            backfill_id: Some("backfill-3"),
            ..gated(&[("orders", Some(3))], &[])
        };
        let pending = versioned("orders", 2, AssetVersionState::Pending);
        assert_eq!(
            asset_version_gate(&new_backfill, &[pending]),
            wait("orders is migrating to version 3")
        );
    }

    #[test]
    fn a_convert_runs_the_next_version_of_the_migration_that_started_it() {
        let running = AssetVersionRecord {
            target_version: Some(4),
            run_id: Some("run-1".to_string()),
            ..row(2, AssetVersionState::Running)
        };

        assert_eq!(convert_version("orders", Some(&running), "run-1"), Ok(3));
    }

    #[test]
    fn no_other_run_converts_the_data() {
        let refused = Err(
            "no migration of orders started this run; the migrate action runs only for a migration"
                .to_string(),
        );
        let running = AssetVersionRecord {
            run_id: Some("run-1".to_string()),
            ..versioned("orders", 2, AssetVersionState::Running)
        };
        // The run id is set when the migration starts to run.
        let draining = AssetVersionRecord {
            run_id: Some("run-1".to_string()),
            ..versioned("orders", 2, AssetVersionState::Draining)
        };
        let done = AssetVersionRecord {
            target_version: Some(2),
            ..running.clone()
        };

        assert_eq!(convert_version("orders", None, "run-1"), refused);
        assert_eq!(convert_version("orders", Some(&running), "run-2"), refused);
        for row in [
            draining,
            done,
            versioned("orders", 2, AssetVersionState::Ready),
        ] {
            assert_eq!(convert_version("orders", Some(&row), "run-1"), refused);
        }
    }

    #[test]
    fn old_code_never_writes_newer_data() {
        let ready = versioned("orders", 3, AssetVersionState::Ready);
        let refused = VersionGate::Refuse(
            "this code declares orders version 2; the stored data is at version 3".to_string(),
        );

        let writer = gated(&[("orders", Some(2))], &[]);
        assert_eq!(asset_version_gate(&writer, &[ready.clone()]), refused);
        // Readers in the same code location deploy with the asset.
        let reader = gated(&[("summary", Some(1))], &[("orders", Some(2))]);
        assert_eq!(asset_version_gate(&reader, &[ready]), VersionGate::Go);
        // Also while the data migrates further.
        let migrating = versioned("orders", 3, AssetVersionState::Running);
        assert_eq!(asset_version_gate(&writer, &[migrating]), refused);
    }

    #[test]
    fn new_code_waits_for_the_migration_to_its_version() {
        let ready = [versioned("orders", 2, AssetVersionState::Ready)];
        let waits = wait("orders waits for its migration to version 3");

        assert_eq!(
            asset_version_gate(&gated(&[("orders", Some(3))], &[]), &ready),
            waits
        );
        let reader = gated(&[("summary", Some(1))], &[("orders", Some(3))]);
        assert_eq!(asset_version_gate(&reader, &ready), waits);
        for declared in [Some(2), None] {
            let run = gated(&[("orders", declared)], &[]);
            assert_eq!(asset_version_gate(&run, &ready), VersionGate::Go);
        }
    }

    #[test]
    fn a_refusal_wins_and_waits_name_the_first_asset_in_key_order() {
        let rows = [
            versioned("payments", 1, AssetVersionState::Pending),
            versioned("orders", 1, AssetVersionState::Pending),
            versioned("customers", 4, AssetVersionState::Ready),
        ];

        let waiting = gated(&[("payments", Some(2)), ("orders", Some(2))], &[]);
        assert_eq!(
            asset_version_gate(&waiting, &rows),
            wait("orders is migrating to version 2")
        );
        let refused = gated(
            &[
                ("payments", Some(2)),
                ("orders", Some(2)),
                ("customers", Some(3)),
            ],
            &[],
        );
        assert!(matches!(
            asset_version_gate(&refused, &rows),
            VersionGate::Refuse(reason) if reason.starts_with("this code declares customers")
        ));
    }

    #[test]
    fn a_run_reads_the_rows_of_its_assets_once() {
        let run = gated(
            &[("orders", Some(2)), ("customers", None)],
            &[("orders", Some(2)), ("raw", None)],
        );

        assert_eq!(run.asset_keys(), ["customers", "orders", "raw"]);
    }

    /// `raw` → task `orders/load` → graph asset `orders`, which `summary`
    /// reads beside its dep `customers`.
    fn graph() -> AssetGraph {
        let mut graph = AssetGraph::new();
        let mut node = |name: &str, kind: NodeKind| {
            graph.add_node(crate::assets::graph::DagNode {
                name: name.to_string(),
                kind,
                inputs: Vec::new(),
                outputs: Vec::new(),
                sub_graph: None,
            })
        };
        let raw = node("raw", NodeKind::Asset);
        let customers = node("customers", NodeKind::Asset);
        let load = node("orders/load", NodeKind::Task);
        let orders = node("orders", NodeKind::GraphAsset);
        let summary = node("summary", NodeKind::Asset);
        for (node, dep) in [
            (load, raw),
            (orders, load),
            (summary, orders),
            (summary, customers),
        ] {
            graph.add_edge(node, dep, ());
        }
        graph
    }

    #[test]
    fn read_assets_are_the_inputs_and_deps_through_tasks() {
        let graph = graph();
        let reads = |writes: &[&str]| {
            let writes: Vec<String> = writes.iter().map(|w| w.to_string()).collect();
            read_assets(&graph, &writes)
        };

        assert_eq!(reads(&["orders"]), ["raw"]);
        assert_eq!(reads(&["orders/load"]), ["raw"]);
        assert_eq!(reads(&["summary"]), ["customers", "orders"]);
        assert_eq!(
            reads(&["summary", "orders", "unknown"]),
            ["customers", "raw"]
        );
        assert!(reads(&["raw"]).is_empty());
    }

    #[test]
    fn with_upstream_adds_each_asset_that_the_keys_read() {
        let code = CodeVersions::new(&graph(), HashMap::new());
        let mut upstream: Vec<&str> = code.with_upstream(["summary"]).into_iter().collect();
        upstream.sort_unstable();

        assert_eq!(upstream, ["customers", "orders", "raw", "summary"]);
        assert_eq!(code.with_upstream(["raw"]), HashSet::from(["raw"]));
    }

    #[test]
    fn code_versions_gate_a_run_by_the_assets_that_it_writes() {
        let code = CodeVersions::new(
            &graph(),
            HashMap::from([("orders".to_string(), 2), ("raw".to_string(), 2)]),
        );
        let names = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let raw = versioned("raw", 1, AssetVersionState::Pending);

        assert_eq!(
            code.gate(Some("run-1"), None, &names(&["orders"]), &[]),
            VersionGate::Go
        );
        // `orders` reads `raw` in its task; `summary` does not read `raw`.
        let held = code.gate(Some("run-1"), None, &names(&["orders"]), &[raw.clone()]);
        assert_eq!(held, wait("raw is migrating to version 2"));
        assert_eq!(held.reason(), Some("raw is migrating to version 2"));
        let summary = code.gate(Some("run-1"), None, &names(&["summary"]), &[raw.clone()]);
        assert_eq!(summary, VersionGate::Go);
        // The runs of the migration's rebuild go.
        let rebuilding = AssetVersionRecord {
            backfill_id: Some("backfill-1".to_string()),
            ..raw
        };
        let rebuild = code.gate(
            Some("run-2"),
            Some("backfill-1"),
            &names(&["raw", "orders"]),
            &[rebuilding],
        );
        assert_eq!(rebuild, VersionGate::Go);
        // The declared versions come from the code.
        let newer = versioned("orders", 3, AssetVersionState::Ready);
        assert_eq!(
            code.gate(None, None, &names(&["orders"]), &[newer]),
            VersionGate::Refuse(
                "this code declares orders version 2; the stored data is at version 3".to_string()
            )
        );
    }

    #[test]
    fn a_migration_starts_at_the_latest_start_of_the_versions_that_it_applies() {
        let step = |to, start_at| DeclaredMigration {
            to,
            kind: MigrationKind::VersionOnly,
            breaking: true,
            start_at,
        };
        let code = CodeVersions::new(&graph(), HashMap::from([("orders".to_string(), 4)]))
            .with_migrations(
                HashMap::from([(
                    "orders".to_string(),
                    vec![step(2, Some(PASSED)), step(3, Some(LATER)), step(4, None)],
                )]),
                HashSet::new(),
            );
        let pending = |applied, target| AssetVersionRecord {
            target_version: Some(target),
            ..versioned("orders", applied, AssetVersionState::Pending)
        };

        assert_eq!(code.start_at(&pending(1, 4)), Some(LATER));
        assert_eq!(code.start_at(&pending(1, 2)), Some(PASSED));
        assert_eq!(code.start_at(&pending(3, 4)), None);
        assert_eq!(
            code.gate(None, None, &["summary".to_string()], &[pending(1, 4)]),
            wait(
                "orders waits for its migration to version 4, which starts at 2100-01-01T00:00:00Z"
            )
        );
    }

    #[test]
    fn automation_leaves_a_migrating_asset_and_its_readers_alone() {
        let code = CodeVersions::new(
            &graph(),
            HashMap::from([("orders".to_string(), 2), ("customers".to_string(), 1)]),
        );
        let paused = |rows: &[AssetVersionRecord]| {
            let mut paused: Vec<(String, String)> = code
                .paused(["orders", "summary", "customers"], rows)
                .into_iter()
                .collect();
            paused.sort();
            paused
        };
        let with_reader = |reason: &str| {
            vec![
                ("orders".to_string(), reason.to_string()),
                ("summary".to_string(), reason.to_string()),
            ]
        };

        assert!(paused(&[]).is_empty());
        assert!(paused(&[versioned("orders", 2, AssetVersionState::Ready)]).is_empty());
        assert_eq!(
            paused(&[versioned("orders", 1, AssetVersionState::Running)]),
            with_reader("orders is migrating to version 2")
        );
        let failed = AssetVersionRecord {
            reason: Some("the warehouse is down".to_string()),
            ..versioned("orders", 1, AssetVersionState::Failed)
        };
        assert_eq!(
            paused(&[failed]),
            with_reader("the migration of orders to version 2 failed: the warehouse is down")
        );
        // Old code never writes newer data; its readers read it as before.
        assert_eq!(
            paused(&[versioned("orders", 3, AssetVersionState::Ready)]),
            [(
                "orders".to_string(),
                "this code declares orders version 2; the stored data is at version 3".to_string()
            )]
        );
    }
}
