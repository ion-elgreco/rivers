//! The asset version gate at the start of a run (RFC-048): a run that would
//! write or read an asset whose stored data is not at the version that its
//! code declares does not run a step.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use rivers_core::assets::graph::AssetGraph;
use rivers_core::assets::versions::{CodeVersions, GatedRun, VersionGate, check_asset_versions};
use rivers_core::concurrency::fail_unlaunched_run;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::any::AnyStorage;

use crate::errors::{AssetMigrationPendingError, AssetVersionMismatchError};
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

/// The version that `node` declares: `None` for a node that is not an asset
/// of this code location.
fn declared_version(node: &ResolvedNode) -> Option<u32> {
    match node {
        ResolvedNode::Asset(asset) if asset.remote.is_none() => Some(asset.schema_version),
        _ => None,
    }
}

/// The versions that the code of `node_map` declares, for the gate of a run
/// that is known by the assets that it writes.
pub(crate) fn code_versions(
    node_map: &HashMap<String, ResolvedNode>,
    graph: &AssetGraph,
) -> CodeVersions {
    let declared = node_map
        .iter()
        .filter_map(|(name, node)| Some((name.clone(), declared_version(node)?)))
        .collect();
    CodeVersions::new(graph, declared)
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
    let declared = |name: &'a str| (name, node_map.get(name).and_then(declared_version));
    GatedRun {
        run_id: Some(run_id),
        backfill_id,
        writes: writes.into_iter().map(declared).collect(),
        reads: reads.into_iter().map(declared).collect(),
    }
}

/// `Ok` when the gate lets the run go, else `AssetMigrationPendingError` or
/// `AssetVersionMismatchError` with the reason.
pub(crate) fn gate_result(gate: VersionGate) -> PyResult<()> {
    match gate {
        VersionGate::Go => Ok(()),
        VersionGate::Wait(reason) => Err(AssetMigrationPendingError::new_err(reason)),
        VersionGate::Refuse(reason) => Err(AssetVersionMismatchError::new_err(reason)),
    }
}

/// [`gate_result`] for `run`. Before it raises, a run that has a record
/// (`recorded`) fails with the reason. Blocks: call it without the GIL.
pub(crate) fn check_run(
    storage: &ScopedStorageHandle<AnyStorage>,
    run: &GatedRun<'_>,
    recorded: bool,
) -> PyResult<()> {
    let gate = io_rt()
        .block_on(check_asset_versions(&storage.scoped(), run))
        .map_err(crate::storage::to_py_err)?;
    if recorded && let (Some(run_id), Some(reason)) = (run.run_id, gate.reason()) {
        io_rt().block_on(fail_unlaunched_run(
            storage.backend(),
            storage.code_location_id(),
            run_id,
            reason,
        ));
    }
    gate_result(gate)
}
