use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use super::StorageBackend;
use crate::assets::graph::GraphTopology;
use crate::assets::remote::LocationRef;

/// A code location's entry in the directory. Stored as JSON under
/// [`crate::code_location_directory_key`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationDirectoryEntry {
    /// The storage identity the location's rows are scoped by.
    pub identity: String,
    /// Where the location serves gRPC, if it says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_endpoint: Option<String>,
    /// The Python module the location loads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// When the location last showed it is alive, in nanoseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<i64>,
    /// The remote assets the location consumes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remotes: Vec<RemoteConsumer>,
}

impl CodeLocationDirectoryEntry {
    pub fn new(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
            ..Self::default()
        }
    }
}

/// A remote asset, as the location that consumes it declares it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteConsumer {
    /// The name in the consuming location.
    pub local: String,
    /// The owning location.
    pub code_location: LocationRef,
    /// The asset's key in the owning location.
    pub asset_key: String,
    /// The consuming location's assets that depend on it.
    #[serde(default)]
    pub downstream: Vec<String>,
}

impl RemoteConsumer {
    /// The remote nodes of `topology`, each with the assets that depend on
    /// it. A graph task counts as its graph asset.
    pub fn from_topology(topology: &GraphTopology) -> Vec<Self> {
        let parents: HashMap<&str, &str> = topology
            .nodes
            .iter()
            .filter_map(|node| Some((node.name.as_str(), node.parent_graph.as_deref()?)))
            .collect();
        topology
            .nodes
            .iter()
            .filter_map(|node| {
                let remote = node.remote.as_ref()?;
                let downstream: BTreeSet<&str> = topology
                    .edges
                    .iter()
                    .filter(|(_, upstream)| *upstream == node.name)
                    .map(|(from, _)| parents.get(from.as_str()).copied().unwrap_or(from))
                    .collect();
                Some(Self {
                    local: node.name.clone(),
                    code_location: remote.code_location.clone(),
                    asset_key: remote.asset_key.clone(),
                    downstream: downstream.into_iter().map(str::to_string).collect(),
                })
            })
            .collect()
    }
}

/// The assets of other code locations that depend on `owner`'s
/// `asset_key`, as `(consumer, asset)` pairs sorted by both.
pub fn downstream_of(
    entries: &[(LocationRef, CodeLocationDirectoryEntry)],
    owner: &LocationRef,
    asset_key: &str,
) -> Vec<(LocationRef, String)> {
    let mut downstream: Vec<(LocationRef, String)> = entries
        .iter()
        .flat_map(|(consumer, entry)| {
            entry
                .remotes
                .iter()
                .filter(|remote| &remote.code_location == owner && remote.asset_key == asset_key)
                .flat_map(move |remote| {
                    remote
                        .downstream
                        .iter()
                        .map(move |asset| (consumer.clone(), asset.clone()))
                })
        })
        .collect();
    downstream.sort_by_cached_key(|(consumer, asset)| (consumer.to_string(), asset.clone()));
    downstream.dedup();
    downstream
}

/// Storage identity of each location; `None` for a location that never
/// registered.
pub async fn resolve_code_locations<'a, S: StorageBackend + ?Sized>(
    storage: &S,
    locations: impl IntoIterator<Item = &'a LocationRef>,
) -> anyhow::Result<HashMap<LocationRef, Option<String>>> {
    let mut out = HashMap::new();
    for location in locations {
        if !out.contains_key(location) {
            let identity = storage.get_code_location_identity(location).await?;
            out.insert(location.clone(), identity);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::assets::remote::RemoteRef;

    #[test]
    fn downstream_of_lists_consumers_of_one_owner_key() {
        let warehouse = LocationRef::new("dev", "warehouse").unwrap();
        let lake = LocationRef::new("dev", "lake").unwrap();
        let consumes =
            |location: &LocationRef, key: &str, local: &str, downstream: &[&str]| RemoteConsumer {
                local: local.into(),
                code_location: location.clone(),
                asset_key: key.into(),
                downstream: downstream.iter().map(|d| d.to_string()).collect(),
            };
        let entry = |remotes| CodeLocationDirectoryEntry {
            remotes,
            ..CodeLocationDirectoryEntry::new("id")
        };
        let entries = vec![
            (
                LocationRef::new("dev", "shop").unwrap(),
                entry(vec![
                    consumes(&warehouse, "raw_orders", "orders", &["report"]),
                    consumes(&lake, "raw_orders", "lake_orders", &["lake_report"]),
                ]),
            ),
            (
                LocationRef::new("dev", "analytics").unwrap(),
                entry(vec![
                    consumes(
                        &warehouse,
                        "raw_orders",
                        "wh_orders",
                        &["order_count", "daily"],
                    ),
                    consumes(&warehouse, "customers", "customers", &["churn"]),
                ]),
            ),
        ];

        let downstream: Vec<(String, String)> = downstream_of(&entries, &warehouse, "raw_orders")
            .into_iter()
            .map(|(consumer, asset)| (consumer.to_string(), asset))
            .collect();
        assert_eq!(
            downstream,
            [
                ("dev/analytics".into(), "daily".into()),
                ("dev/analytics".into(), "order_count".into()),
                ("dev/shop".into(), "report".into()),
            ]
        );
        assert!(downstream_of(&entries, &warehouse, "missing").is_empty());
    }

    #[test]
    fn remotes_list_their_downstream_assets() {
        let warehouse = LocationRef::new("dev", "warehouse").unwrap();
        let orders = TopologyNode {
            remote: Some(RemoteRef {
                code_location: warehouse.clone(),
                asset_key: "raw_orders".into(),
            }),
            ..TopologyNode::new("orders", NodeKind::Asset)
        };
        let task = TopologyNode {
            parent_graph: Some("report".into()),
            ..TopologyNode::new("report/load", NodeKind::Task)
        };
        let topology = GraphTopology {
            nodes: vec![
                orders,
                task,
                TopologyNode::new("report", NodeKind::GraphAsset),
                TopologyNode::new("order_count", NodeKind::Asset),
                TopologyNode::new("doubled", NodeKind::Asset),
            ],
            edges: vec![
                ("order_count".into(), "orders".into()),
                ("report/load".into(), "orders".into()),
                ("doubled".into(), "order_count".into()),
            ],
            ..Default::default()
        };

        assert_eq!(
            RemoteConsumer::from_topology(&topology),
            vec![RemoteConsumer {
                local: "orders".into(),
                code_location: warehouse,
                asset_key: "raw_orders".into(),
                downstream: vec!["order_count".into(), "report".into()],
            }]
        );
    }
}
