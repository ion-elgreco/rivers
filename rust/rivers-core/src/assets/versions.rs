//! The version of the stored data of each asset against the version that
//! its code declares (RFC-048). Registration records the declared versions;
//! the gate holds or refuses the runs that would touch an asset whose data
//! is not at that version.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use petgraph::Direction;

use super::graph::{AssetGraph, GraphTopology, NodeKind};
use crate::storage::{AssetVersionRecord, AssetVersionState, ScopedStorage, StorageBackend};

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

/// Whether `run` is the convert run or a run of the rebuild backfill of the
/// migration in `row`.
fn is_migration_work(run: &GatedRun<'_>, row: &AssetVersionRecord) -> bool {
    run.run_id
        .is_some_and(|id| row.run_id.as_deref() == Some(id))
        || run
            .backfill_id
            .is_some_and(|id| row.backfill_id.as_deref() == Some(id))
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

/// The assets that the nodes `writes` read: the inputs and deps of each,
/// through tasks, which store nothing. `writes` are left out, and so is a
/// name that is not in `graph`.
pub fn read_assets(graph: &AssetGraph, writes: &[String]) -> Vec<String> {
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
        }
    }

    fn wait(reason: &str) -> VersionGate {
        VersionGate::Wait(reason.to_string())
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
}
