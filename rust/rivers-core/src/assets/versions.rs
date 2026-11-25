//! The version of the stored data of each asset against the version that
//! its code declares (RFC-048). Registration records the declared versions.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};

use super::graph::GraphTopology;
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
}
