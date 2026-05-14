//! Server functions for fetching graph topology and computing DAG layout.
//!
//! Topology is keyed per code location in storage; each server function
//! takes `(loc_ns, loc_name)`, resolves the CL identity via the registry,
//! and reads `kv["graph_topology:<id>"]`. The `--synthetic` developer
//! override on the standalone UI binary bypasses storage entirely and
//! serves a fixed in-memory graph for every CL.

use leptos::prelude::*;

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

#[server]
pub async fn get_graph_layout(
    loc_ns: String,
    loc_name: String,
    center_layers: Option<bool>,
) -> Result<LayoutResult, ServerFnError> {
    let core_topo = load_topology(&loc_ns, &loc_name).await?;
    let topo: GraphTopology = core_topo.into();
    Ok(compute_layout(&topo, center_layers.unwrap_or(false)))
}

#[server]
pub async fn get_graph_topology(
    loc_ns: String,
    loc_name: String,
) -> Result<GraphTopology, ServerFnError> {
    let core_topo = load_topology(&loc_ns, &loc_name).await?;
    Ok(core_topo.into())
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
