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
use crate::types::GraphTopology;

#[cfg(feature = "ssr")]
async fn load_topology(
    loc_ns: &str,
    loc_name: &str,
) -> Result<rivers_core::assets::graph::GraphTopology, ServerFnError> {
    use rivers_core::storage::StorageBackend;

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
    let ctx = rivers_core::storage::CodeLocationContext::new(entry.identity);
    let topo = state
        .storage
        .for_code_location(&ctx)
        .get_graph_topology()
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    Ok(topo)
}

/// The UI topology, with each remote node marked resolved when the registry
/// knows its owning code location.
#[cfg(feature = "ssr")]
async fn load_ui_topology(loc_ns: &str, loc_name: &str) -> Result<GraphTopology, ServerFnError> {
    let mut topo: GraphTopology = load_topology(loc_ns, loc_name).await?.into();
    let state = expect_context::<crate::state::AppState>();
    let mut known: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
    for remote in topo.nodes.iter_mut().filter_map(|n| n.remote.as_mut()) {
        let resolved = match known.get(&remote.code_location) {
            Some(&resolved) => resolved,
            None => {
                let (ns, name) = remote.location();
                let resolved = state.registry.lookup(ns, name).await.is_some();
                known.insert(remote.code_location.clone(), resolved);
                resolved
            }
        };
        remote.resolved = resolved;
    }
    Ok(topo)
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
