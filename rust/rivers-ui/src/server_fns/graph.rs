//! Server functions for fetching graph topology and computing DAG layout.
//!
//! Topology is keyed per code location in storage; each server function
//! takes `(loc_ns, loc_name)`, resolves the CL identity via the registry,
//! and reads `kv["graph_topology:<id>"]`. The `--synthetic` developer
//! override on the standalone UI binary bypasses storage entirely and
//! serves a fixed in-memory graph for every CL.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

#[allow(unused_imports)]
use crate::components::dag::layout::{LayoutResult, compute_layout};
use crate::types::{GraphTopology, RemoteDownstream};

#[cfg(feature = "ssr")]
async fn load_topology(
    loc_ns: &str,
    loc_name: &str,
) -> Result<rivers_core::assets::graph::GraphTopology, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    if let Some(ref synthetic) = state.graph {
        return Ok((**synthetic).clone());
    }
    let entry = state
        .registry
        .lookup(loc_ns, loc_name)
        .await
        .ok_or_else(|| {
            ServerFnError::new(format!(
                "code location {loc_ns}/{loc_name} not found in registry"
            ))
        })?;
    Ok(read_topology(&state, entry.identity)
        .await
        .unwrap_or_default())
}

#[cfg(feature = "ssr")]
async fn read_topology(
    state: &crate::state::AppState,
    identity: String,
) -> Option<rivers_core::assets::graph::GraphTopology> {
    use rivers_core::storage::StorageBackend;

    let ctx = rivers_core::storage::CodeLocationContext::new(identity);
    state
        .storage
        .for_code_location(&ctx)
        .get_graph_topology()
        .await
        .ok()
        .flatten()
}

/// The UI topology. A remote node is resolved when the registry knows its
/// owning code location, and then carries its partition drift against the
/// owner's topology, if the owner wrote one.
#[cfg(feature = "ssr")]
async fn load_ui_topology(loc_ns: &str, loc_name: &str) -> Result<GraphTopology, ServerFnError> {
    use std::collections::HashMap;

    use rivers_core::assets::graph::GraphTopology as CoreTopology;

    use crate::types::RemoteDrift;

    let topology = load_topology(loc_ns, loc_name).await?;
    let state = expect_context::<crate::state::AppState>();

    // Location → `None` when unregistered, `Some(None)` without a topology.
    let mut owners: HashMap<String, Option<Option<CoreTopology>>> = HashMap::new();
    let mut remotes: HashMap<String, (bool, Option<RemoteDrift>)> = HashMap::new();
    for node in &topology.nodes {
        let Some(remote) = &node.remote else {
            continue;
        };
        let location = remote.code_location.to_string();
        if !owners.contains_key(&location) {
            let entry = state
                .registry
                .lookup(
                    remote.code_location.namespace(),
                    remote.code_location.name(),
                )
                .await;
            let owner = match entry {
                Some(entry) => Some(read_topology(&state, entry.identity).await),
                None => None,
            };
            owners.insert(location.clone(), owner);
        }
        let seen = match &owners[&location] {
            None => (false, None),
            Some(None) => (true, None),
            Some(Some(owner)) => {
                let mirror = topology.partitions_of(node);
                (true, RemoteDrift::between(owner, &remote.asset_key, mirror))
            }
        };
        remotes.insert(node.name.clone(), seen);
    }

    let mut ui: GraphTopology = topology.into();
    for node in &mut ui.nodes {
        if let (Some(remote), Some((resolved, drift))) =
            (node.remote.as_mut(), remotes.remove(&node.name))
        {
            remote.resolved = resolved;
            remote.drift = drift;
        }
    }
    Ok(ui)
}

/// Layout of what the lineage page shows: the kind and group filters, with
/// graph assets outside `expanded` collapsed. See [`GraphTopology::visible`].
#[server(input = Json)]
pub async fn get_graph_layout(
    loc_ns: String,
    loc_name: String,
    center_layers: bool,
    kinds: Vec<String>,
    groups: Vec<String>,
    expanded: Vec<String>,
) -> Result<LayoutResult, ServerFnError> {
    let topo = load_ui_topology(&loc_ns, &loc_name).await?;
    Ok(compute_layout(
        &topo.visible(&kinds, &groups, &expanded),
        center_layers,
    ))
}

#[server]
pub async fn get_graph_topology(
    loc_ns: String,
    loc_name: String,
) -> Result<GraphTopology, ServerFnError> {
    load_ui_topology(&loc_ns, &loc_name).await
}

/// Returns (ancestors, descendants) for the given node name.
#[server]
pub async fn get_node_lineage(
    loc_ns: String,
    loc_name: String,
    node_name: String,
) -> Result<(Vec<String>, Vec<String>), ServerFnError> {
    let core_topo = load_topology(&loc_ns, &loc_name).await?;
    let topo: GraphTopology = core_topo.into();
    Ok(topo.lineage(&node_name))
}

/// The assets of other code locations that depend on `asset_key`, from the
/// remotes each one lists in the code location directory.
#[server]
pub async fn get_remote_downstream(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
) -> Result<Vec<RemoteDownstream>, ServerFnError> {
    use rivers_core::assets::remote::LocationRef;
    use rivers_core::storage::{StorageBackend, downstream_of};

    let state = expect_context::<crate::state::AppState>();
    let owner =
        LocationRef::new(loc_ns, loc_name).map_err(|e| ServerFnError::new(e.to_string()))?;
    let entries = state
        .storage
        .list_code_locations()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;
    Ok(downstream_of(&entries, &owner, &asset_key)
        .into_iter()
        .map(|(consumer, asset_key)| RemoteDownstream {
            code_location: consumer.to_string(),
            asset_key,
        })
        .collect())
}
