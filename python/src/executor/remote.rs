//! Data versions of remote inputs, read from the owning code location's scope.

use std::collections::HashMap;

use rivers_core::assets::graph::GraphTopology;
use rivers_core::assets::partitions_fingerprint::{
    PartitionDefinitions, PartitionDrift, describe_partitions, partition_drift,
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
    let mut owners: HashMap<&LocationRef, Option<(GraphTopology, PartitionDefinitions)>> =
        HashMap::new();
    let mut tracked: Vec<(&str, &str, &RemoteRef)> = Vec::new();
    for &(local, remote) in &refs {
        let Some(identity) = identities
            .get(&remote.code_location)
            .and_then(Option::as_deref)
        else {
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
        if warn_to.is_some() {
            let ctx = CodeLocationContext::new(identity);
            let owner = owners
                .entry(&remote.code_location)
                .or_insert_with(|| owner_topology(backend, &ctx, &remote.code_location));
            if let Some(warning) = owner.as_ref().and_then(|(owner, definitions)| {
                drift_warning(node_map, local, remote, owner, definitions)
            }) {
                warnings.push((local, warning));
            }
        }
        tracked.push((local, identity, remote));
    }

    let versions = owner_data_versions(backend, &tracked);
    for (local, identity, remote) in tracked {
        if let Some(dv) = versions
            .get(identity)
            .and_then(|owned| owned.get(&remote.asset_key))
        {
            data_versions.insert(local.to_string(), dv.clone());
        }
    }

    if let Some((writer, run_id)) = warn_to {
        warn_loading_steps(plan, node_map, &warnings, writer, run_id);
    }
}

/// The data versions of each owner identity in `tracked`, by owner asset key,
/// read with one query per owner. An owner whose read failed has none.
fn owner_data_versions<'a, S: StorageBackend>(
    backend: &S,
    tracked: &[(&str, &'a str, &'a RemoteRef)],
) -> HashMap<&'a str, HashMap<String, String>> {
    let mut keys: HashMap<&str, (&LocationRef, Vec<String>)> = HashMap::new();
    for &(_, identity, remote) in tracked {
        keys.entry(identity)
            .or_insert_with(|| (&remote.code_location, Vec::new()))
            .1
            .push(remote.asset_key.clone());
    }

    let mut versions = HashMap::new();
    for (identity, (location, keys)) in keys {
        let ctx = CodeLocationContext::new(identity);
        let scoped = backend.for_code_location(&ctx);
        match io_rt().block_on(scoped.get_asset_records_by_keys(&keys)) {
            Ok(records) => {
                let owned = records
                    .into_iter()
                    .filter_map(|record| Some((record.asset_key, record.last_data_version?)))
                    .collect();
                versions.insert(identity, owned);
            }
            Err(error) => tracing::warn!(
                target: "rivers::executor",
                code_location = %location,
                %error,
                "reading remote asset records failed; their data versions are not tracked"
            ),
        }
    }
    versions
}

/// The owner's topology and partitions definitions; `None` when it has no
/// topology or a read failed.
fn owner_topology<S: StorageBackend>(
    backend: &S,
    ctx: &CodeLocationContext,
    location: &LocationRef,
) -> Option<(GraphTopology, PartitionDefinitions)> {
    let scoped = backend.for_code_location(ctx);
    let read = io_rt().block_on(async {
        let Some(topology) = scoped.get_graph_topology().await? else {
            return anyhow::Ok(None);
        };
        Ok(Some((topology, scoped.get_partition_definitions().await?)))
    });
    match read {
        Ok(owner) => owner,
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
    definitions: &PartitionDefinitions,
) -> Option<String> {
    let mirror = node_map
        .get(local)?
        .partitions_def()
        .map(|def| def.fingerprint());
    match partition_drift(owner, definitions, &remote.asset_key, mirror.as_ref()) {
        PartitionDrift::Same | PartitionDrift::Compatible => None,
        PartitionDrift::Incompatible => {
            let owned = definitions.get(&remote.asset_key);
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
/// loads `local`, and once into the process log.
fn warn_loading_steps(
    plan: &ExecutionPlan,
    node_map: &HashMap<String, ResolvedNode>,
    warnings: &[(&str, String)],
    writer: &EventWriter,
    run_id: &str,
) {
    for (local, message) in warnings {
        if plan.steps.iter().any(|step| loads(node_map, step, local)) {
            tracing::warn!(target: "rivers::executor", "{message}");
        }
    }

    for step in &plan.steps {
        let logs: String = warnings
            .iter()
            .filter(|(local, _)| loads(node_map, step, local))
            .map(|(_, message)| crate::log_capture::warn_line("rivers::executor", message))
            .collect();
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
