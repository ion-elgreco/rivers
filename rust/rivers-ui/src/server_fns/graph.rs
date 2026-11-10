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

/// The topology of `loc_ns/loc_name`, and the identity it was read under
/// (`None` for the synthetic graph).
#[cfg(feature = "ssr")]
async fn load_topology(
    state: &crate::state::AppState,
    loc_ns: &str,
    loc_name: &str,
) -> Result<(rivers_core::assets::graph::GraphTopology, Option<String>), ServerFnError> {
    if let Some(ref synthetic) = state.graph {
        return Ok(((**synthetic).clone(), None));
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
    let topology = read_topology(state, &entry.identity)
        .await
        .unwrap_or_default();
    Ok((topology, Some(entry.identity)))
}

#[cfg(feature = "ssr")]
async fn read_topology(
    state: &crate::state::AppState,
    identity: &str,
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

#[cfg(feature = "ssr")]
async fn read_definitions(
    state: &crate::state::AppState,
    identity: &str,
) -> rivers_core::assets::partitions_fingerprint::PartitionDefinitions {
    use rivers_core::storage::StorageBackend;

    let ctx = rivers_core::storage::CodeLocationContext::new(identity);
    state
        .storage
        .for_code_location(&ctx)
        .get_partition_definitions()
        .await
        .unwrap_or_default()
}

/// The UI topology. A remote node is resolved when the registry knows its
/// owning code location, and then carries its partition drift against the
/// owner's topology, if the owner wrote one. The partitions definitions are
/// read only when the topology has a remote node, once per code location.
#[cfg(feature = "ssr")]
async fn load_ui_topology(
    state: &crate::state::AppState,
    loc_ns: &str,
    loc_name: &str,
) -> Result<GraphTopology, ServerFnError> {
    use std::collections::HashMap;

    use rivers_core::assets::graph::GraphTopology as CoreTopology;
    use rivers_core::assets::partitions_fingerprint::PartitionDefinitions;

    use crate::types::RemoteDrift;

    let (topology, identity) = load_topology(state, loc_ns, loc_name).await?;
    if topology.nodes.iter().all(|node| node.remote.is_none()) {
        return Ok(topology.into());
    }
    let mirrors = match &identity {
        Some(identity) => read_definitions(state, identity).await,
        None => PartitionDefinitions::default(),
    };

    // Location → `None` when unregistered, `Some(None)` without a topology.
    type Owner = Option<Option<(CoreTopology, PartitionDefinitions)>>;
    let mut owners: HashMap<String, Owner> = HashMap::new();
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
                Some(entry) => match read_topology(state, &entry.identity).await {
                    Some(topology) => Some(Some((
                        topology,
                        read_definitions(state, &entry.identity).await,
                    ))),
                    None => Some(None),
                },
                None => None,
            };
            owners.insert(location.clone(), owner);
        }
        let seen = match &owners[&location] {
            None => (false, None),
            Some(None) => (true, None),
            Some(Some((owner, definitions))) => {
                let mirror = mirrors.get(&node.name);
                (
                    true,
                    RemoteDrift::between(owner, definitions, &remote.asset_key, mirror),
                )
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
    let state = expect_context::<crate::state::AppState>();
    let topo = load_ui_topology(&state, &loc_ns, &loc_name).await?;
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
    let state = expect_context::<crate::state::AppState>();
    load_ui_topology(&state, &loc_ns, &loc_name).await
}

/// Returns (ancestors, descendants) for the given node name.
#[server]
pub async fn get_node_lineage(
    loc_ns: String,
    loc_name: String,
    node_name: String,
) -> Result<(Vec<String>, Vec<String>), ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    let (core_topo, _) = load_topology(&state, &loc_ns, &loc_name).await?;
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

#[cfg(all(test, feature = "ssr"))]
mod tests {
    use std::sync::Arc;

    use rivers_core::assets::graph::{GraphTopology, NodeKind, TopologyNode};
    use rivers_core::assets::partitions_fingerprint::{
        PartitionDefinitions, PartitionsFingerprint,
    };
    use rivers_core::assets::remote::{LocationRef, RemoteRef};
    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use rivers_core::storage::{CodeLocationContext, StorageBackend};

    use crate::code_location_registry::Registry;
    use crate::state::AppState;
    use crate::types::{CodeLocationEntry, RemoteDrift, RemoteNode};

    fn statics(keys: &[&str]) -> PartitionsFingerprint {
        PartitionsFingerprint::static_keys(keys.iter().map(|k| k.to_string()))
    }

    fn entry(name: &str, identity: &str) -> CodeLocationEntry {
        CodeLocationEntry {
            namespace: "dev".into(),
            name: name.into(),
            grpc_endpoint: String::new(),
            image: String::new(),
            module: String::new(),
            phase: "Ready".into(),
            observed_generation: 1,
            identity: identity.into(),
        }
    }

    fn remote(name: &str, location: &str, asset_key: &str) -> TopologyNode {
        TopologyNode {
            remote: Some(RemoteRef {
                code_location: LocationRef::new("dev", location).unwrap(),
                asset_key: asset_key.into(),
            }),
            ..TopologyNode::new(name, NodeKind::Asset)
        }
    }

    async fn write(
        storage: &SurrealStorage,
        identity: &str,
        nodes: Vec<TopologyNode>,
        partitioned: &[(&str, PartitionsFingerprint)],
    ) {
        let mut definitions = PartitionDefinitions::default();
        for (name, fingerprint) in partitioned {
            let index = definitions.intern(fingerprint.clone());
            definitions.nodes.insert(name.to_string(), index);
        }
        let topology = GraphTopology {
            nodes,
            edges: vec![],
        };
        storage
            .for_code_location(&CodeLocationContext::new(identity))
            .set_graph_topology(&topology, &definitions)
            .await
            .unwrap();
    }

    /// The mirror comes from the consumer's definitions row and the owner's
    /// definition from the owner's.
    #[tokio::test]
    async fn remote_nodes_carry_the_drift_against_the_owners_definitions() {
        let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
        write(
            &storage,
            "wh-id",
            vec![
                TopologyNode::new("raw_orders", NodeKind::Asset),
                TopologyNode::new("refunds", NodeKind::Asset),
            ],
            &[
                ("raw_orders", statics(&["eu"])),
                ("refunds", statics(&["eu", "us"])),
            ],
        )
        .await;
        write(
            &storage,
            "an-id",
            vec![
                remote("orders", "warehouse", "raw_orders"),
                remote("refunds", "warehouse", "refunds"),
                remote("missing", "warehouse", "no_such_asset"),
                remote("shop_orders", "shop", "raw_orders"),
                remote("ghost", "nowhere", "ghost"),
                TopologyNode::new("report", NodeKind::Asset),
            ],
            &[
                ("orders", statics(&["eu", "us"])),
                ("refunds", statics(&["us", "eu"])),
            ],
        )
        .await;
        let state = AppState {
            storage,
            graph: None,
            registry: Registry::Static(Arc::new(vec![
                entry("warehouse", "wh-id"),
                entry("shop", "shop-id"),
                entry("analytics", "an-id"),
            ])),
        };

        let ui = super::load_ui_topology(&state, "dev", "analytics")
            .await
            .unwrap();
        let seen = |name: &str| {
            let node = ui.nodes.iter().find(|n| n.name == name).unwrap();
            node.remote
                .as_ref()
                .map(|r: &RemoteNode| (r.resolved, r.drift.clone()))
        };
        assert_eq!(
            seen("orders"),
            Some((
                true,
                Some(RemoteDrift::Incompatible {
                    mirror: "static (2 keys)".into(),
                    owner: "static (1 key)".into(),
                })
            ))
        );
        assert_eq!(seen("refunds"), Some((true, None)));
        assert_eq!(
            seen("missing"),
            Some((true, Some(RemoteDrift::OwnerNodeMissing)))
        );
        assert_eq!(seen("shop_orders"), Some((true, None)), "no topology");
        assert_eq!(seen("ghost"), Some((false, None)), "not registered");
        assert_eq!(seen("report"), None);
    }
}
