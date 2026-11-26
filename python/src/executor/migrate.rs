//! The built-in `migrate` verb (RFC-048): a run of it runs the convert of
//! the migration that started it.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::assets::versions::{changes_keys, convert_version};
use rivers_core::concurrency::fail_unlaunched_run;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::any::AnyStorage;

use crate::assets::action::MIGRATE;
use crate::errors::ExecutionError;
use crate::partitions::PartitionsDefinition;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

/// For a run of `migrate`: the version that the convert of each target moves
/// its stored data to, by the target's version record, and whether that step
/// changes the target's partition keys. `Err` with the reason when no
/// migration of a target started this run, or when this code has no convert
/// to that version. Empty for the runs of other verbs.
pub(crate) fn convert_versions(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    storage: &ScopedStorageHandle<AnyStorage>,
    run_id: &str,
) -> PyResult<Result<Vec<(String, u32, bool)>, String>> {
    if plan.verb() != Some(MIGRATE) {
        return Ok(Ok(Vec::new()));
    }
    let targets: Vec<String> = plan.steps.iter().map(|step| step.name.clone()).collect();
    let rows = io_rt()
        .block_on(storage.scoped().get_asset_versions(Some(&targets)))
        .map_err(crate::storage::to_py_err)?;
    Ok(targets
        .into_iter()
        .map(|target| {
            let row = rows.iter().find(|row| row.asset_key == target);
            let to = convert_version(&target, row, run_id)?;
            let node = match node_map.get(&target) {
                Some(node @ ResolvedNode::Asset(asset)) if asset.has_convert(to) => node,
                _ => {
                    return Err(format!(
                        "this code has no convert of {target} to version {to}"
                    ));
                }
            };
            let code = node.partitions_def().map(PartitionsDefinition::fingerprint);
            let changes = row
                .map(|row| changes_keys(row, to, code.as_ref()))
                .transpose()
                .map_err(|e| format!("{e:#}"))?
                .unwrap_or(false);
            Ok((target, to, changes))
        })
        .collect())
}

/// For a run of `migrate`: `node_map` with the `migrate` of each target bound
/// to its convert ([`convert_versions`]). `None` for the runs of other verbs.
/// Before it raises, a run that has a record (`recorded`) fails with the
/// reason.
pub(crate) fn bind_converts(
    py: Python,
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    storage: &ScopedStorageHandle<AnyStorage>,
    run_id: &str,
    recorded: bool,
) -> PyResult<Option<Arc<HashMap<String, ResolvedNode>>>> {
    if plan.verb() != Some(MIGRATE) {
        return Ok(None);
    }
    let versions = match py.detach(|| convert_versions(plan, node_map, storage, run_id))? {
        Ok(versions) => versions,
        Err(reason) => {
            if recorded {
                py.detach(|| {
                    io_rt().block_on(fail_unlaunched_run(
                        storage.backend(),
                        storage.code_location_id(),
                        run_id,
                        &reason,
                    ))
                });
            }
            return Err(ExecutionError::new_err(reason));
        }
    };

    let mut bound = node_map.clone();
    for (target, to, changes_keys) in versions {
        if let Some(ResolvedNode::Asset(asset)) = bound.get_mut(&target) {
            let converting = asset
                .with_convert(py, to, changes_keys)
                .expect("convert_versions found the convert");
            *asset = Arc::new(converting);
        }
    }
    Ok(Some(Arc::new(bound)))
}
