//! The built-in `migrate` verb (RFC-048): a run of it runs the convert of
//! the migration that started it.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::assets::versions::convert_version;
use rivers_core::concurrency::fail_unlaunched_run;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::any::AnyStorage;

use crate::assets::action::MIGRATE;
use crate::errors::ExecutionError;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

/// For a run of `migrate`: the version that the convert of each target moves
/// its stored data to, by the target's version record. `Err` with the reason
/// when no migration of a target started this run, or when this code has no
/// convert to that version. Empty for the runs of other verbs. Blocks: call
/// it without the GIL.
pub(crate) fn convert_versions(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    storage: &ScopedStorageHandle<AnyStorage>,
    run_id: &str,
) -> PyResult<Result<Vec<(String, u32)>, String>> {
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
            match node_map.get(&target) {
                Some(ResolvedNode::Asset(asset)) if asset.has_convert(to) => Ok((target, to)),
                _ => Err(format!(
                    "this code has no convert of {target} to version {to}"
                )),
            }
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
    for (target, to) in versions {
        if let Some(ResolvedNode::Asset(asset)) = bound.get_mut(&target) {
            let converting = asset
                .with_convert(py, to)
                .expect("convert_versions found the convert");
            *asset = Arc::new(converting);
        }
    }
    Ok(Some(Arc::new(bound)))
}
