//! Data versions of remote inputs, read from the owning code location's scope.

use std::collections::HashMap;

use rivers_core::assets::remote::RemoteRef;
use rivers_core::execution::plan::{ExecutionPlan, ExecutionStep};
use rivers_core::storage::{CodeLocationContext, StorageBackend, resolve_code_locations};

use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

use super::event_writer::EventWriter;
use super::ops::{self, StorageHandle, now_ts};

/// Put the owner's data version of each remote dep in `remote` into
/// `data_versions`, under the local name. With `warn_to`, a remote whose
/// owner is not registered gets a warning in the log of each step that
/// loads it.
pub(super) fn prefill_remote_dep_versions(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    remote: &[&str],
    storage: StorageHandle,
    warn_to: Option<(&EventWriter, &str)>,
    data_versions: &mut HashMap<String, String>,
) {
    let refs: Vec<(&str, &RemoteRef)> = remote
        .iter()
        .filter_map(|local| Some((*local, node_map.get(*local)?.remote()?)))
        .collect();
    if refs.is_empty() {
        return;
    }

    let backend = storage.backend().as_ref();
    let identities = match io_rt().block_on(resolve_code_locations(
        backend,
        refs.iter().map(|(_, remote)| &remote.code_location),
    )) {
        Ok(identities) => identities,
        Err(error) => {
            tracing::warn!(
                target: "rivers::executor",
                %error,
                "looking up remote code locations failed; remote inputs have no data versions"
            );
            return;
        }
    };

    let mut unresolved = Vec::new();
    for &(local, remote) in &refs {
        let Some(identity) = identities.get(&remote.code_location).cloned().flatten() else {
            unresolved.push((local, remote));
            continue;
        };
        let ctx = CodeLocationContext::new(&identity);
        match io_rt().block_on(
            backend
                .for_code_location(&ctx)
                .get_asset_record(&remote.asset_key),
        ) {
            Ok(record) => {
                if let Some(dv) = record.and_then(|r| r.last_data_version) {
                    data_versions.insert(local.to_string(), dv);
                }
            }
            Err(error) => tracing::warn!(
                target: "rivers::executor",
                code_location = %remote.code_location,
                asset = %remote.asset_key,
                %error,
                "reading a remote asset record failed; its data version is not tracked"
            ),
        }
    }

    if let Some((writer, run_id)) = warn_to {
        warn_unresolved(plan, node_map, &unresolved, writer, run_id);
    }
}

fn warn_unresolved(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    unresolved: &[(&str, &RemoteRef)],
    writer: &EventWriter,
    run_id: &str,
) {
    for step in &plan.steps {
        let loaded: Vec<&RemoteRef> = unresolved
            .iter()
            .filter(|(local, _)| loads(node_map, step, local))
            .map(|(_, remote)| *remote)
            .collect();
        if loaded.is_empty() {
            continue;
        }

        crate::log_capture::start();
        for remote in loaded {
            tracing::warn!(
                target: "rivers::executor",
                "code location '{}' is not registered; loading '{}' through the handler \
                 without data-version tracking",
                remote.code_location,
                remote.asset_key,
            );
        }
        let logs = crate::log_capture::take();
        ops::emit_log_output(writer, run_id, &step.name, "", "", &logs, now_ts());
    }
}

/// Whether `step` loads `dep` through a handler, rather than listing it
/// for lineage only.
fn loads(node_map: &HashMap<String, ResolvedNode>, step: &ExecutionStep, dep: &str) -> bool {
    step.graph_dependencies.iter().any(|d| d == dep)
        && step
            .event_names()
            .iter()
            .filter_map(|name| node_map.get(name))
            .any(|node| !node.is_lineage_dep(dep))
}
