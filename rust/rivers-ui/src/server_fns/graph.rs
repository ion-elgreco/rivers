//! Server functions for fetching graph topology and computing DAG layout.
//!
//! Topology is keyed per code location in storage; each server function
//! takes `(loc_ns, loc_name)`, resolves the CL identity via the registry,
//! and reads `kv["graph_topology:<id>"]`. The `--synthetic` developer
//! override on the standalone UI binary bypasses storage entirely and
//! serves a fixed in-memory graph for every CL.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

#[allow(unused_imports)]
use crate::components::dag::layout::{LayoutResult, compute_layout};
use crate::types::{GraphTopology, RemoteDownstream, StaleStatus};

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

/// The code location that owns a remote node: `None` when the registry does
/// not know it. Its topology is `None` when it wrote none.
#[cfg(feature = "ssr")]
type Owner = Option<(
    rivers_core::storage::CodeLocationContext,
    Option<rivers_core::assets::graph::GraphTopology>,
)>;

/// The owner of each remote node of `topology`, by code location.
#[cfg(feature = "ssr")]
async fn load_owners<'t>(
    state: &crate::state::AppState,
    topology: &'t rivers_core::assets::graph::GraphTopology,
) -> HashMap<&'t rivers_core::assets::remote::LocationRef, Owner> {
    use rivers_core::storage::CodeLocationContext;

    let mut owners = HashMap::new();
    for (_, remote) in topology.remotes() {
        let location = &remote.code_location;
        if owners.contains_key(location) {
            continue;
        }
        let entry = state
            .registry
            .lookup(location.namespace(), location.name())
            .await;
        let owner = match entry {
            Some(entry) => {
                let topology = read_topology(state, &entry.identity).await;
                Some((CodeLocationContext::new(entry.identity), topology))
            }
            None => None,
        };
        owners.insert(location, owner);
    }
    owners
}

/// The UI topology. A remote node is resolved when the registry knows its
/// owning code location, and then carries its partition drift against the
/// owner's topology, if the owner wrote one.
#[cfg(feature = "ssr")]
async fn load_ui_topology(
    state: &crate::state::AppState,
    loc_ns: &str,
    loc_name: &str,
) -> Result<GraphTopology, ServerFnError> {
    use rivers_core::assets::partitions_fingerprint::partition_drift;
    use rivers_core::storage::CodeLocationContext;

    use crate::types::RemoteDrift;

    let (topology, identity) = load_topology(state, loc_ns, loc_name).await?;
    let consumer = CodeLocationContext::new(identity.unwrap_or_default());
    let owners = load_owners(state, &topology).await;

    let mut remotes: HashMap<String, (bool, Option<RemoteDrift>)> = HashMap::new();
    for node in &topology.nodes {
        let Some(remote) = &node.remote else {
            continue;
        };
        let seen = match &owners[&remote.code_location] {
            None => (false, None),
            Some((_, None)) => (true, None),
            Some((owner_ctx, Some(owner))) => {
                let mirror = node.partitions.as_ref();
                let check = partition_drift(owner, &remote.asset_key, mirror);
                let settled = state
                    .key_comparisons
                    .settle(state.storage.as_ref(), &consumer, owner_ctx, check)
                    .await;
                let drift = match settled {
                    Ok(drift) => {
                        let owned = owner.owned_partitions(&remote.asset_key);
                        RemoteDrift::new(drift, mirror, owned)
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "rivers::ui",
                            node = %node.name,
                            %error,
                            "reading static partition keys failed; partition drift is not shown"
                        );
                        None
                    }
                };
                (true, drift)
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

/// The owner's status of each remote node, by node name. A node has none
/// when the registry does not know its owner, or the owner has no record of
/// the asset.
#[cfg(feature = "ssr")]
async fn load_remote_statuses(
    state: &crate::state::AppState,
    loc_ns: &str,
    loc_name: &str,
) -> Result<HashMap<String, StaleStatus>, ServerFnError> {
    use rivers_core::assets::remote::{LocationRef, RemoteRef};
    use rivers_core::storage::StorageBackend;

    let (topology, _) = load_topology(state, loc_ns, loc_name).await?;
    let owners = load_owners(state, &topology).await;
    let remotes: Vec<(&str, &RemoteRef)> = topology.remotes().collect();

    let mut keys: HashMap<&LocationRef, Vec<String>> = HashMap::new();
    for (_, remote) in &remotes {
        let owner_keys = keys.entry(&remote.code_location).or_default();
        if !owner_keys.contains(&remote.asset_key) {
            owner_keys.push(remote.asset_key.clone());
        }
    }

    let mut staleness = HashMap::new();
    for (location, keys) in keys {
        let Some((owner, owner_topology)) = &owners[location] else {
            continue;
        };
        let owner_staleness = state
            .storage
            .for_code_location(owner)
            .staleness_of(&keys, owner_topology.as_ref())
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))?;
        staleness.insert(location, owner_staleness);
    }

    Ok(remotes
        .into_iter()
        .filter_map(|(name, remote)| {
            let (status, _) = staleness
                .get(&remote.code_location)?
                .get(&remote.asset_key)?;
            Some((name.to_string(), status.clone().into()))
        })
        .collect())
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

/// The owner's status of each remote node, by node name. Only the status
/// leaves the owner.
#[server]
pub async fn get_remote_statuses(
    loc_ns: String,
    loc_name: String,
) -> Result<HashMap<String, StaleStatus>, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    load_remote_statuses(&state, &loc_ns, &loc_name).await
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
    use rivers_core::assets::partitions_fingerprint::{KeyList, PartitionsFingerprint};
    use rivers_core::assets::remote::{LocationRef, RemoteRef};
    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use rivers_core::storage::{CodeLocationContext, StorageBackend};

    use crate::code_location_registry::Registry;
    use crate::state::AppState;
    use crate::types::{CodeLocationEntry, RemoteDrift, RemoteNode};

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

    fn owned(name: &str, keys: &[&str]) -> TopologyNode {
        TopologyNode {
            partitions: Some(PartitionsFingerprint::static_keys(keys.iter().copied())),
            ..TopologyNode::new(name, NodeKind::Asset)
        }
    }

    fn remote(name: &str, location: &str, asset_key: &str, keys: &[&str]) -> TopologyNode {
        TopologyNode {
            remote: Some(RemoteRef {
                code_location: LocationRef::new("dev", location).unwrap(),
                asset_key: asset_key.into(),
            }),
            partitions: (!keys.is_empty())
                .then(|| PartitionsFingerprint::static_keys(keys.iter().copied())),
            ..TopologyNode::new(name, NodeKind::Asset)
        }
    }

    /// `nodes` as the topology of `identity`, with a key row for each list
    /// of `keys`.
    async fn write(
        storage: &SurrealStorage,
        identity: &str,
        nodes: Vec<TopologyNode>,
        keys: &[&[&str]],
    ) {
        let key_lists: Vec<KeyList> = keys
            .iter()
            .map(|keys| KeyList::new(keys.iter().copied()))
            .collect();
        let topology = GraphTopology {
            nodes,
            edges: vec![],
        };
        storage
            .for_code_location(&CodeLocationContext::new(identity))
            .set_graph_topology(&topology, &key_lists)
            .await
            .unwrap();
    }

    fn warehouse() -> Vec<TopologyNode> {
        vec![
            owned("raw_orders", &["eu"]),
            owned("refunds", &["eu", "us"]),
            owned("regions", &["apac", "eu", "us"]),
            owned("sites", &["apac", "eu", "uk"]),
        ]
    }

    fn analytics() -> Vec<TopologyNode> {
        vec![
            remote("orders", "warehouse", "raw_orders", &["eu", "us"]),
            remote("refunds", "warehouse", "refunds", &["us", "eu"]),
            remote("regions", "warehouse", "regions", &["eu", "us"]),
            remote("sites", "warehouse", "sites", &["eu", "us"]),
            remote("missing", "warehouse", "no_such_asset", &[]),
            remote("shop_orders", "shop", "raw_orders", &[]),
            remote("ghost", "nowhere", "ghost", &[]),
            TopologyNode::new("report", NodeKind::Asset),
        ]
    }

    fn state(storage: &Arc<SurrealStorage>) -> AppState {
        AppState {
            storage: Arc::clone(storage),
            graph: None,
            registry: Registry::Static(Arc::new(vec![
                entry("warehouse", "wh-id"),
                entry("shop", "shop-id"),
                entry("analytics", "an-id"),
            ])),
            key_comparisons: Default::default(),
        }
    }

    async fn seen(
        state: &AppState,
    ) -> impl Fn(&str) -> Option<(bool, Option<RemoteDrift>)> + use<> {
        let ui = super::load_ui_topology(state, "dev", "analytics")
            .await
            .unwrap();
        move |name: &str| {
            let node = ui.nodes.iter().find(|n| n.name == name).unwrap();
            node.remote
                .as_ref()
                .map(|r: &RemoteNode| (r.resolved, r.drift.clone()))
        }
    }

    fn compatible(mirror: &str, owner: &str) -> Option<(bool, Option<RemoteDrift>)> {
        let (mirror, owner) = (mirror.into(), owner.into());
        Some((true, Some(RemoteDrift::Compatible { mirror, owner })))
    }

    fn incompatible(mirror: &str, owner: &str) -> Option<(bool, Option<RemoteDrift>)> {
        let (mirror, owner) = (mirror.into(), owner.into());
        Some((true, Some(RemoteDrift::Incompatible { mirror, owner })))
    }

    /// The mirror comes from the consumer's topology and the owner's
    /// definition from the owner's. Key rows decide when the owner has more
    /// static keys.
    #[tokio::test]
    async fn remote_nodes_carry_the_drift_against_the_owners_definitions() {
        let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
        write(
            &storage,
            "wh-id",
            warehouse(),
            &[
                &["eu"],
                &["eu", "us"],
                &["apac", "eu", "us"],
                &["apac", "eu", "uk"],
            ],
        )
        .await;
        write(&storage, "an-id", analytics(), &[&["eu", "us"]]).await;

        let seen = seen(&state(&storage)).await;
        assert_eq!(
            seen("orders"),
            incompatible("static (2 keys)", "static (1 key)")
        );
        assert_eq!(seen("refunds"), Some((true, None)));
        assert_eq!(
            seen("regions"),
            compatible("static (2 keys)", "static (3 keys)")
        );
        assert_eq!(
            seen("sites"),
            incompatible("static (2 keys)", "static (3 keys)"),
            "'us' is not an owner key"
        );
        assert_eq!(
            seen("missing"),
            Some((true, Some(RemoteDrift::OwnerNodeMissing)))
        );
        assert_eq!(seen("shop_orders"), Some((true, None)), "no topology");
        assert_eq!(seen("ghost"), Some((false, None)), "not registered");
        assert_eq!(seen("report"), None);
    }

    /// Registers `key` in `identity`; materializes it when `dv` is set.
    async fn asset(
        storage: &SurrealStorage,
        identity: &str,
        key: &str,
        code_version: Option<&str>,
        dv: Option<&str>,
        inputs: &[(&str, &str)],
    ) {
        use rivers_core::storage::{AssetRecord, EventRecord, EventType};

        let record = AssetRecord {
            code_location_id: identity.into(),
            asset_key: key.into(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: code_version.map(str::to_string),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        storage
            .for_code_location(&CodeLocationContext::new(identity))
            .register_assets(&[record])
            .await
            .unwrap();
        let Some(dv) = dv else {
            return;
        };
        storage
            .store_event(&EventRecord {
                code_location_id: identity.into(),
                event_type: EventType::Materialization {
                    data_version: Some(dv.into()),
                },
                asset_key: Some(key.into()),
                run_id: format!("{identity}-{key}"),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: inputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            })
            .await
            .unwrap();
    }

    /// `regions` is stale through `seed`, two steps up in its owner. `shop`
    /// wrote no topology. The warehouse has no record of `refunds` and no
    /// asset `no_such_asset`; `nowhere` is not registered.
    #[tokio::test]
    async fn remote_nodes_get_the_owners_status() {
        use crate::types::StaleStatus;

        let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
        let edge = |from: &str, to: &str| (from.to_string(), to.to_string());
        let mut nodes = warehouse();
        nodes.push(TopologyNode::new("seed", NodeKind::Asset));
        nodes.push(TopologyNode::new("clean", NodeKind::Asset));
        storage
            .for_code_location(&CodeLocationContext::new("wh-id"))
            .set_graph_topology(
                &GraphTopology {
                    nodes,
                    edges: vec![edge("clean", "seed"), edge("regions", "clean")],
                },
                &[],
            )
            .await
            .unwrap();
        write(&storage, "an-id", analytics(), &[]).await;
        asset(&storage, "wh-id", "seed", Some("v1"), Some("s1"), &[]).await;
        asset(&storage, "wh-id", "seed", Some("v2"), None, &[]).await;
        asset(
            &storage,
            "wh-id",
            "clean",
            None,
            Some("c1"),
            &[("seed", "s1")],
        )
        .await;
        asset(
            &storage,
            "wh-id",
            "regions",
            None,
            Some("r1"),
            &[("clean", "c1")],
        )
        .await;
        asset(&storage, "wh-id", "raw_orders", None, Some("o1"), &[]).await;
        asset(&storage, "wh-id", "sites", None, None, &[]).await;
        asset(&storage, "shop-id", "raw_orders", None, Some("p1"), &[]).await;

        let statuses = super::load_remote_statuses(&state(&storage), "dev", "analytics")
            .await
            .unwrap();

        let mut statuses: Vec<(String, StaleStatus)> = statuses.into_iter().collect();
        statuses.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            statuses,
            [
                ("orders".to_string(), StaleStatus::UpToDate),
                ("regions".to_string(), StaleStatus::Stale),
                ("shop_orders".to_string(), StaleStatus::UpToDate),
                ("sites".to_string(), StaleStatus::Missing),
            ]
        );
    }

    /// A pair of digests compared once is not read again: the answers stay
    /// after the key rows are gone, and a new state without them finds the
    /// rows missing.
    #[tokio::test]
    async fn the_answer_of_a_key_comparison_is_kept() {
        let storage = Arc::new(SurrealStorage::new_memory().await.unwrap());
        write(
            &storage,
            "wh-id",
            warehouse(),
            &[
                &["eu"],
                &["eu", "us"],
                &["apac", "eu", "us"],
                &["apac", "eu", "uk"],
            ],
        )
        .await;
        write(&storage, "an-id", analytics(), &[&["eu", "us"]]).await;
        let kept = state(&storage);
        let first = seen(&kept).await;
        assert_eq!(
            first("regions"),
            compatible("static (2 keys)", "static (3 keys)")
        );

        write(&storage, "wh-id", warehouse(), &[]).await;
        write(&storage, "an-id", analytics(), &[]).await;
        let without_rows = |identity: &str, keys: &[&str]| {
            let digest = KeyList::new(keys.iter().copied()).digest;
            let storage = Arc::clone(&storage);
            let identity = identity.to_string();
            async move {
                storage
                    .for_code_location(&CodeLocationContext::new(identity))
                    .get_partition_keys(&digest)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(without_rows("wh-id", &["apac", "eu", "us"]).await, None);
        assert_eq!(without_rows("an-id", &["eu", "us"]).await, None);

        let again = seen(&kept).await;
        assert_eq!(
            again("regions"),
            compatible("static (2 keys)", "static (3 keys)")
        );
        assert_eq!(
            again("sites"),
            incompatible("static (2 keys)", "static (3 keys)")
        );
        assert_eq!(again("refunds"), Some((true, None)));

        let fresh = seen(&state(&storage)).await;
        assert_eq!(
            fresh("regions"),
            incompatible("static (2 keys)", "static (3 keys)"),
            "a missing key row is incompatible"
        );
        assert_eq!(fresh("refunds"), Some((true, None)), "needs no key row");
    }
}
