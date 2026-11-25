//! The asset version gate at the start of a run (RFC-048): a run that would
//! write or read an asset whose stored data is not at the version that its
//! code declares does not run a step.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use rivers_core::assets::versions::{GatedRun, VersionGate, check_asset_versions};
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::{LaunchedBy, ScopedStorageHandle};

use crate::errors::{AssetMigrationPendingError, AssetVersionMismatchError};
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

/// The backfill that a run of `launched_by` is part of.
pub(crate) fn backfill_of(launched_by: &LaunchedBy) -> Option<&str> {
    match launched_by {
        LaunchedBy::Backfill { backfill_id } => Some(backfill_id),
        _ => None,
    }
}

/// The run of `plan` as the gate sees it: the assets that the plan writes and
/// the ones that its steps read, each with the version that `node_map`
/// declares for it.
pub(crate) fn plan_run<'a>(
    plan: &'a ExecutionPlan,
    node_map: &'a HashMap<String, ResolvedNode>,
    run_id: &'a str,
    backfill_id: Option<&'a str>,
) -> GatedRun<'a> {
    let writes: HashSet<&str> = plan.asset_names().collect();
    let reads: HashSet<&str> = plan
        .steps
        .iter()
        .flat_map(|step| &step.graph_dependencies)
        .map(String::as_str)
        .filter(|dep| !writes.contains(dep))
        .collect();
    GatedRun {
        run_id: Some(run_id),
        backfill_id,
        writes: declared_versions(writes, node_map),
        reads: declared_versions(reads, node_map),
    }
}

/// Each of `names` with the version that `node_map` declares for it: `None`
/// for a node that is not an asset of this code location.
pub(crate) fn declared_versions<'a>(
    names: impl IntoIterator<Item = &'a str>,
    node_map: &HashMap<String, ResolvedNode>,
) -> Vec<(&'a str, Option<u32>)> {
    names
        .into_iter()
        .map(|name| {
            let declared = match node_map.get(name) {
                Some(ResolvedNode::Asset(asset)) if asset.remote.is_none() => {
                    Some(asset.schema_version)
                }
                _ => None,
            };
            (name, declared)
        })
        .collect()
}

/// Raise when the gate holds or refuses `run`: `AssetMigrationPendingError`
/// or `AssetVersionMismatchError`. Before that, a run that has a record
/// (`recorded`) fails with the reason. Blocks: call it without the GIL.
pub(crate) fn check_run(
    storage: &ScopedStorageHandle<AnyStorage>,
    run: &GatedRun<'_>,
    recorded: bool,
) -> PyResult<()> {
    let gate = io_rt()
        .block_on(check_asset_versions(&storage.scoped(), run))
        .map_err(crate::storage::to_py_err)?;
    let (reason, pending) = match gate {
        VersionGate::Go => return Ok(()),
        VersionGate::Wait(reason) => (reason, true),
        VersionGate::Refuse(reason) => (reason, false),
    };
    if recorded && let Some(run_id) = run.run_id {
        io_rt().block_on(crate::daemon::fail_unlaunched_run(
            storage.backend(),
            storage.code_location_id(),
            run_id,
            &reason,
        ));
    }
    Err(if pending {
        AssetMigrationPendingError::new_err(reason)
    } else {
        AssetVersionMismatchError::new_err(reason)
    })
}
