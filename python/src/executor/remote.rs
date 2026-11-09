//! Data versions of remote inputs, read from the owning code location's scope.

use std::collections::HashMap;

use rivers_core::assets::graph::GraphTopology;
use rivers_core::assets::partitions_fingerprint::{
    PartitionDrift, describe_partitions, partition_drift,
};
use rivers_core::assets::remote::{LocationRef, RemoteRef};
use rivers_core::execution::plan::{ExecutionPlan, ExecutionStep};
use rivers_core::storage::{CodeLocationContext, StorageBackend, resolve_code_locations};

use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::io_rt;

use super::event_writer::EventWriter;
use super::ops::{self, StorageHandle, now_ts};

/// Put the owner's data version of each remote dep in `remote` into
/// `data_versions`, under the local name. With `warn_to`, a remote whose
/// owner is not registered, or whose mirror no longer matches the owner's
/// partitions definition, gets a warning in the log of each step that loads
/// it.
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

    let mut warnings: Vec<(&str, String)> = Vec::new();
    let mut owners: HashMap<&LocationRef, Option<GraphTopology>> = HashMap::new();
    for &(local, remote) in &refs {
        let Some(identity) = identities.get(&remote.code_location).cloned().flatten() else {
            warnings.push((
                local,
                format!(
                    "code location '{}' is not registered; loading '{}' through the handler \
                     without data-version tracking",
                    remote.code_location, remote.asset_key,
                ),
            ));
            continue;
        };
        let ctx = CodeLocationContext::new(&identity);
        if warn_to.is_some() {
            let owner = owners
                .entry(&remote.code_location)
                .or_insert_with(|| owner_topology(backend, &ctx, &remote.code_location));
            if let Some(warning) = owner
                .as_ref()
                .and_then(|owner| drift_warning(node_map, local, remote, owner))
            {
                warnings.push((local, warning));
            }
        }
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
        warn_loading_steps(plan, node_map, &warnings, writer, run_id);
    }
}

/// The owner's topology; `None` when it has none or reading it failed.
fn owner_topology<S: StorageBackend>(
    backend: &S,
    ctx: &CodeLocationContext,
    location: &LocationRef,
) -> Option<GraphTopology> {
    match io_rt().block_on(backend.for_code_location(ctx).get_graph_topology()) {
        Ok(topology) => topology,
        Err(error) => {
            tracing::warn!(
                target: "rivers::executor",
                code_location = %location,
                %error,
                "reading a remote code location's topology failed; partition drift is not checked"
            );
            None
        }
    }
}

/// Why loading `local` may not read what its mirror describes.
fn drift_warning(
    node_map: &HashMap<String, ResolvedNode>,
    local: &str,
    remote: &RemoteRef,
    owner: &GraphTopology,
) -> Option<String> {
    let mirror = node_map
        .get(local)?
        .partitions_def()
        .map(|def| def.fingerprint());
    match partition_drift(owner, &remote.asset_key, mirror.as_ref()) {
        PartitionDrift::Same | PartitionDrift::Compatible => None,
        PartitionDrift::Incompatible => {
            let owned = owner
                .owned_node(&remote.asset_key)
                .and_then(|node| owner.partitions_of(node));
            Some(format!(
                "partition definition of '{local}' differs from code location '{}' (mirror {}, \
                 owner {}); loading anyway",
                remote.code_location,
                describe_partitions(mirror.as_ref()),
                describe_partitions(owned),
            ))
        }
        PartitionDrift::OwnerNodeMissing => Some(format!(
            "code location '{}' has no asset '{}'; loading '{local}' through the handler anyway",
            remote.code_location, remote.asset_key,
        )),
    }
}

/// Write each warning `(local, message)` into the log of every step that
/// loads `local`.
fn warn_loading_steps(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    warnings: &[(&str, String)],
    writer: &EventWriter,
    run_id: &str,
) {
    for step in &plan.steps {
        let messages: Vec<&String> = warnings
            .iter()
            .filter(|(local, _)| loads(node_map, step, local))
            .map(|(_, message)| message)
            .collect();
        if messages.is_empty() {
            continue;
        }

        crate::log_capture::start();
        for message in messages {
            tracing::warn!(target: "rivers::executor", "{message}");
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
