//! Asset dependency graph built on petgraph.

use petgraph::Directed;
use petgraph::algo::{has_path_connecting, is_cyclic_directed, tarjan_scc};
use petgraph::graph::{Graph, NodeIndex};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::repo::CodeRepository;

/// `ByName` is produced at parse time from annotations; `Resolved` is populated
/// by `resolve_asset_graph`.
#[derive(Debug, Clone)]
pub enum NodeRef {
    ByName(String),
    Resolved(NodeIndex),
}

/// Serializes as `"asset"` / `"task"` / `"graph_asset"` — wire format consumed
/// by the UI DTO.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Asset,
    Task,
    GraphAsset,
}

impl NodeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Asset => "asset",
            Self::Task => "task",
            Self::GraphAsset => "graph_asset",
        }
    }
}

impl std::str::FromStr for NodeKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "asset" => Ok(Self::Asset),
            "task" => Ok(Self::Task),
            "graph_asset" => Ok(Self::GraphAsset),
            other => Err(format!("unknown NodeKind: '{other}'")),
        }
    }
}

#[derive(Debug)]
pub struct DagNode {
    pub name: String,
    pub kind: NodeKind,
    pub inputs: Vec<NodeRef>,
    pub outputs: Vec<String>,
    /// Internal DAG — only populated when `kind == GraphAsset`.
    pub sub_graph: Option<AssetGraph>,
}

impl DagNode {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            kind: NodeKind::Asset,
            inputs: Vec::new(),
            outputs: Vec::new(),
            sub_graph: None,
        }
    }
}

pub type AssetGraph = Graph<DagNode, (), Directed>;

/// Serializable graph topology for the UI binary — names + edges only,
/// no Python callables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphTopology {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<(String, String)>, // (from, to) — dependency direction
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    pub name: String,
    pub kind: NodeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// For tasks inside a graph asset: the name of the parent graph asset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_graph: Option<String>,
}

pub fn to_topology(graph: &AssetGraph) -> GraphTopology {
    let nodes: Vec<TopologyNode> = graph
        .node_indices()
        .map(|idx| {
            let node = &graph[idx];
            TopologyNode {
                name: node.name.clone(),
                kind: node.kind,
                group: None,
                parent_graph: None,
            }
        })
        .collect();

    let mut edges: Vec<(String, String)> = graph
        .edge_indices()
        .filter_map(|eidx| {
            graph
                .edge_endpoints(eidx)
                .map(|(from, to)| (graph[from].name.clone(), graph[to].name.clone()))
        })
        .collect();
    edges.sort();

    GraphTopology { nodes, edges }
}

/// Format a cycle as "a -> b -> c -> a" using node names from the graph.
fn format_cycle(graph: &AssetGraph, scc: &[NodeIndex]) -> String {
    let mut names: Vec<&str> = scc.iter().map(|idx| graph[*idx].name.as_str()).collect();
    if let Some(first) = names.first() {
        names.push(first);
    }
    names.join(" -> ")
}

/// Detect cycles using `tarjan_scc` to identify the exact nodes involved.
pub fn detect_cycles(graph: &AssetGraph) -> Result<(), String> {
    if !is_cyclic_directed(graph) {
        return Ok(());
    }
    let sccs = tarjan_scc(graph);
    let cycles: Vec<String> = sccs
        .iter()
        .filter(|scc| scc.len() > 1)
        .map(|scc| format_cycle(graph, scc))
        .collect();

    if cycles.len() == 1 {
        Err(format!("Cycle detected in asset graph: {}", cycles[0]))
    } else {
        let listed = cycles
            .iter()
            .enumerate()
            .map(|(i, c)| format!("  {}. {}", i + 1, c))
            .collect::<Vec<_>>()
            .join("\n");
        Err(format!(
            "{} cycles detected in asset graph:\n{}",
            cycles.len(),
            listed
        ))
    }
}

pub fn find_node_by_name(graph: &AssetGraph, name: &str) -> Option<NodeIndex> {
    graph.node_indices().find(|&idx| graph[idx].name == name)
}

pub fn name_to_index(graph: &AssetGraph) -> HashMap<&str, NodeIndex> {
    graph
        .node_indices()
        .map(|idx| (graph[idx].name.as_str(), idx))
        .collect()
}

/// Edges point downstream → upstream, so a path from `downstream` to `upstream`
/// means the upstream is reachable.
pub fn is_upstream_of(graph: &AssetGraph, downstream: NodeIndex, upstream: NodeIndex) -> bool {
    has_path_connecting(graph, downstream, upstream, None)
}

/// Name-based reachability check. Returns `Err` if either name is missing.
pub fn is_reachable(graph: &AssetGraph, from_name: &str, to_name: &str) -> Result<bool, String> {
    let from_idx = find_node_by_name(graph, from_name)
        .ok_or_else(|| format!("Node '{}' not found in graph", from_name))?;
    let to_idx = find_node_by_name(graph, to_name)
        .ok_or_else(|| format!("Node '{}' not found in graph", to_name))?;
    Ok(has_path_connecting(graph, from_idx, to_idx, None))
}

/// DFS in `dir` from `name`; result excludes `name` itself.
///
/// Outgoing edges lead to dependencies (upstream); Incoming come from dependents (downstream).
fn traverse(graph: &AssetGraph, name: &str, dir: petgraph::Direction) -> HashSet<String> {
    let mut result = HashSet::new();
    let Some(start) = find_node_by_name(graph, name) else {
        return result;
    };
    let mut stack = vec![start];
    while let Some(idx) = stack.pop() {
        for neighbor in graph.neighbors_directed(idx, dir) {
            if result.insert(graph[neighbor].name.clone()) {
                stack.push(neighbor);
            }
        }
    }
    result
}

/// Transitive dependencies of `name`, excluding `name` itself.
pub fn ancestors(graph: &AssetGraph, name: &str) -> HashSet<String> {
    traverse(graph, name, petgraph::Direction::Outgoing)
}

/// Transitive dependents of `name`, excluding `name` itself.
pub fn descendants(graph: &AssetGraph, name: &str) -> HashSet<String> {
    traverse(graph, name, petgraph::Direction::Incoming)
}

/// Upstream closure: `targets` plus every transitive dependency they require.
pub fn upstream_closure(graph: &AssetGraph, targets: &HashSet<String>) -> HashSet<String> {
    let lookup = name_to_index(graph);
    let mut result = HashSet::new();
    let mut stack: Vec<NodeIndex> = targets
        .iter()
        .filter_map(|name| lookup.get(name.as_str()).copied())
        .collect();

    while let Some(idx) = stack.pop() {
        let name = graph[idx].name.clone();
        if result.insert(name) {
            for dep_idx in graph.neighbors_directed(idx, petgraph::Direction::Outgoing) {
                stack.push(dep_idx);
            }
        }
    }
    result
}

pub fn materialization_requires(
    graph: &AssetGraph,
    target: &str,
    candidate: &str,
) -> Result<bool, String> {
    is_reachable(graph, target, candidate)
}

/// Validate that every dependency of every selected node is also selected.
/// Returns `Err` with the list of `(node, missing_dep)` pairs.
pub fn validate_subgraph_completeness(
    graph: &AssetGraph,
    selection: &HashSet<String>,
) -> Result<(), Vec<(String, String)>> {
    let lookup = name_to_index(graph);
    let mut missing = Vec::new();
    for name in selection {
        if let Some(&idx) = lookup.get(name.as_str()) {
            for dep_idx in graph.neighbors_directed(idx, petgraph::Direction::Outgoing) {
                let dep_name = &graph[dep_idx].name;
                if !selection.contains(dep_name) {
                    missing.push((name.clone(), dep_name.clone()));
                }
            }
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

impl GraphTopology {
    /// (transitive dependencies, transitive dependents) of `node_name`.
    pub fn lineage(&self, node_name: &str) -> (HashSet<String>, HashSet<String>) {
        let mut deps_of: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut dependents_of: HashMap<&str, Vec<&str>> = HashMap::new();
        for (from, to) in &self.edges {
            deps_of.entry(from.as_str()).or_default().push(to.as_str());
            dependents_of
                .entry(to.as_str())
                .or_default()
                .push(from.as_str());
        }

        let walk = |adj: &HashMap<&str, Vec<&str>>| -> HashSet<String> {
            let mut result = HashSet::new();
            let mut stack: Vec<&str> = adj.get(node_name).cloned().unwrap_or_default();
            while let Some(n) = stack.pop() {
                if result.insert(n.to_string())
                    && let Some(next) = adj.get(n)
                {
                    stack.extend(next.iter());
                }
            }
            result
        };

        (walk(&deps_of), walk(&dependents_of))
    }
}

impl CodeRepository {
    pub fn resolve_asset_graph(&mut self) -> Result<(), String> {
        let mut graph: AssetGraph = Graph::new();
        let mut name_to_index: HashMap<String, NodeIndex> = HashMap::new();

        for (name, asset_inputs) in self.assets() {
            let mut node = DagNode::new(name);
            node.inputs = asset_inputs.clone();
            let idx = graph.add_node(node);
            name_to_index.insert(name.to_string(), idx);
        }

        // Resolve ByName → Resolved and add edges.
        let node_indices: Vec<NodeIndex> = graph.node_indices().collect();
        for idx in node_indices {
            let inputs = std::mem::take(&mut graph[idx].inputs);
            let mut resolved_inputs = Vec::<NodeRef>::with_capacity(inputs.len());

            for input in inputs {
                let NodeRef::ByName(dep_name) = input else {
                    // Annotations only produce ByName; a pre-resolved ref means caller bug.
                    unreachable!(
                        "resolve_asset_graph expected NodeRef::ByName, got a resolved ref"
                    );
                };
                let Some(&dep_idx) = name_to_index.get(&dep_name) else {
                    return Err(format!(
                        "Asset '{}': unresolved input '{}'",
                        &graph[idx].name, dep_name
                    ));
                };
                if dep_idx == idx {
                    return Err(format!(
                        "Asset '{}': depends on itself; use SelfDependency[T] \
                         to reference an asset's own previous output",
                        &graph[idx].name
                    ));
                }
                resolved_inputs.push(NodeRef::Resolved(dep_idx));
                graph.add_edge(idx, dep_idx, ());
            }
            graph[idx].inputs = resolved_inputs;
        }

        detect_cycles(&graph)?;

        self.graph = Some(graph);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::CodeRepository;
    use std::collections::BTreeMap;

    fn make_repo(assets: Vec<(&str, Vec<&str>)>) -> CodeRepository {
        let mut map = BTreeMap::new();
        for (name, deps) in assets {
            map.insert(
                name.to_string(),
                deps.into_iter()
                    .map(|d| NodeRef::ByName(d.to_string()))
                    .collect(),
            );
        }
        CodeRepository::new(map)
    }

    /// Build a resolved graph from a list of (name, deps) pairs.
    fn make_graph(assets: Vec<(&str, Vec<&str>)>) -> AssetGraph {
        let mut repo = make_repo(assets);
        repo.resolve_asset_graph().unwrap();
        repo.graph.unwrap()
    }

    #[test]
    fn test_linear_chain_resolves() {
        let mut repo = make_repo(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert!(repo.resolve_asset_graph().is_ok());
    }

    #[test]
    fn test_diamond_resolves() {
        let mut repo = make_repo(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        assert!(repo.resolve_asset_graph().is_ok());
    }

    #[test]
    fn test_simple_cycle_detected() {
        let mut repo = make_repo(vec![("a", vec!["b"]), ("b", vec!["a"])]);
        assert_eq!(
            repo.resolve_asset_graph().unwrap_err(),
            "Cycle detected in asset graph: b -> a -> b"
        );
    }

    #[test]
    fn test_three_node_cycle_detected() {
        let mut repo = make_repo(vec![("a", vec!["c"]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert_eq!(
            repo.resolve_asset_graph().unwrap_err(),
            "Cycle detected in asset graph: b -> c -> a -> b"
        );
    }

    #[test]
    fn test_cycle_with_non_cyclic_nodes() {
        let mut repo = make_repo(vec![
            ("a", vec!["b"]),
            ("b", vec!["a"]),
            ("c", vec![]),
            ("d", vec!["a", "c"]),
        ]);
        assert_eq!(
            repo.resolve_asset_graph().unwrap_err(),
            "Cycle detected in asset graph: b -> a -> b"
        );
    }

    #[test]
    fn test_unresolved_input_error() {
        let mut repo = make_repo(vec![("a", vec!["nonexistent"])]);
        assert_eq!(
            repo.resolve_asset_graph().unwrap_err(),
            "Asset 'a': unresolved input 'nonexistent'"
        );
    }

    #[test]
    fn test_self_dependency_rejected_with_hint() {
        let mut repo = make_repo(vec![("foo", vec!["foo"])]);
        let err = repo.resolve_asset_graph().unwrap_err();
        assert!(
            err.starts_with("Asset 'foo': depends on itself"),
            "unexpected error: {err}"
        );
        assert!(err.contains("SelfDependency"), "missing hint: {err}");
    }

    #[test]
    fn test_detect_cycles_on_acyclic_graph() {
        let mut graph: AssetGraph = Graph::new();
        let _a = graph.add_node(DagNode::new("a"));
        let b = graph.add_node(DagNode::new("b"));
        graph.add_edge(b, _a, ());
        assert!(detect_cycles(&graph).is_ok());
    }

    #[test]
    fn test_detect_cycles_returns_exact_message() {
        let mut graph: AssetGraph = Graph::new();
        let x = graph.add_node(DagNode::new("x"));
        let y = graph.add_node(DagNode::new("y"));
        graph.add_edge(x, y, ());
        graph.add_edge(y, x, ());
        assert_eq!(
            detect_cycles(&graph).unwrap_err(),
            "Cycle detected in asset graph: y -> x -> y"
        );
    }

    #[test]
    fn test_is_reachable_direct_dep() {
        // b depends on a → is_reachable("b", "a") == true
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"])]);
        assert!(is_reachable(&graph, "b", "a").unwrap());
        assert!(!is_reachable(&graph, "a", "b").unwrap());
    }

    #[test]
    fn test_is_reachable_transitive() {
        // a -> b -> c  (c depends on b depends on a)
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert!(is_reachable(&graph, "c", "a").unwrap());
        assert!(!is_reachable(&graph, "a", "c").unwrap());
    }

    #[test]
    fn test_is_reachable_unrelated() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec![])]);
        assert!(!is_reachable(&graph, "a", "b").unwrap());
    }

    #[test]
    fn test_is_reachable_unknown_name() {
        let graph = make_graph(vec![("a", vec![])]);
        assert!(is_reachable(&graph, "a", "missing").is_err());
        assert!(is_reachable(&graph, "missing", "a").is_err());
    }

    #[test]
    fn test_is_upstream_of_diamond() {
        //   a
        //  / \
        // b   c
        //  \ /
        //   d
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        assert!(is_upstream_of(
            &graph,
            find_node_by_name(&graph, "d").unwrap(),
            find_node_by_name(&graph, "a").unwrap(),
        ));
        assert!(!is_upstream_of(
            &graph,
            find_node_by_name(&graph, "a").unwrap(),
            find_node_by_name(&graph, "d").unwrap(),
        ));
    }

    #[test]
    fn test_ancestors_chain() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert_eq!(
            ancestors(&graph, "c"),
            HashSet::from(["a".into(), "b".into()])
        );
        assert_eq!(ancestors(&graph, "a"), HashSet::new());
    }

    #[test]
    fn test_descendants_chain() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert_eq!(
            descendants(&graph, "a"),
            HashSet::from(["b".into(), "c".into()])
        );
        assert_eq!(descendants(&graph, "c"), HashSet::new());
    }

    #[test]
    fn test_ancestors_descendants_diamond() {
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        assert_eq!(
            ancestors(&graph, "d"),
            HashSet::from(["a".into(), "b".into(), "c".into()])
        );
        assert_eq!(
            descendants(&graph, "a"),
            HashSet::from(["b".into(), "c".into(), "d".into()])
        );
        assert_eq!(ancestors(&graph, "b"), HashSet::from(["a".into()]));
        assert_eq!(descendants(&graph, "b"), HashSet::from(["d".into()]));
    }

    #[test]
    fn test_ancestors_unknown_node() {
        let graph = make_graph(vec![("a", vec![])]);
        assert_eq!(ancestors(&graph, "missing"), HashSet::new());
    }

    #[test]
    fn test_upstream_closure_single_root() {
        let graph = make_graph(vec![("a", vec![])]);
        let targets = HashSet::from(["a".into()]);
        assert_eq!(
            upstream_closure(&graph, &targets),
            HashSet::from(["a".into()])
        );
    }

    #[test]
    fn test_upstream_closure_chain() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let targets = HashSet::from(["c".into()]);
        assert_eq!(
            upstream_closure(&graph, &targets),
            HashSet::from(["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn test_upstream_closure_partial_selection() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let targets = HashSet::from(["b".into()]);
        assert_eq!(
            upstream_closure(&graph, &targets),
            HashSet::from(["a".into(), "b".into()])
        );
    }

    #[test]
    fn test_materialization_requires() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert!(materialization_requires(&graph, "c", "a").unwrap());
        assert!(!materialization_requires(&graph, "a", "c").unwrap());
    }

    #[test]
    fn test_subgraph_complete() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"])]);
        let sel = HashSet::from(["a".into(), "b".into()]);
        assert!(validate_subgraph_completeness(&graph, &sel).is_ok());
    }

    #[test]
    fn test_subgraph_missing_dep() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let sel = HashSet::from(["c".into()]);
        let missing = validate_subgraph_completeness(&graph, &sel).unwrap_err();
        assert_eq!(missing, vec![("c".into(), "b".into())]);
    }

    #[test]
    fn test_subgraph_roots_only() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec![])]);
        let sel = HashSet::from(["a".into()]);
        assert!(validate_subgraph_completeness(&graph, &sel).is_ok());
    }

    fn topo_node(name: &str) -> TopologyNode {
        TopologyNode {
            name: name.into(),
            kind: NodeKind::Asset,
            group: None,
            parent_graph: None,
        }
    }

    #[test]
    fn test_topology_lineage_chain() {
        let topo = GraphTopology {
            nodes: vec![topo_node("a"), topo_node("b"), topo_node("c")],
            edges: vec![("c".into(), "b".into()), ("b".into(), "a".into())],
        };
        let (anc, desc) = topo.lineage("b");
        assert_eq!(anc, HashSet::from(["a".into()]));
        assert_eq!(desc, HashSet::from(["c".into()]));
    }

    #[test]
    fn test_topology_lineage_diamond() {
        let topo = GraphTopology {
            nodes: vec![
                topo_node("a"),
                topo_node("b"),
                topo_node("c"),
                topo_node("d"),
            ],
            edges: vec![
                ("b".into(), "a".into()),
                ("c".into(), "a".into()),
                ("d".into(), "b".into()),
                ("d".into(), "c".into()),
            ],
        };
        let (anc, desc) = topo.lineage("d");
        assert_eq!(anc, HashSet::from(["a".into(), "b".into(), "c".into()]));
        assert_eq!(desc, HashSet::new());

        let (anc, desc) = topo.lineage("a");
        assert_eq!(anc, HashSet::new());
        assert_eq!(desc, HashSet::from(["b".into(), "c".into(), "d".into()]));
    }

    #[test]
    fn test_topology_lineage_unknown_node() {
        let topo = GraphTopology {
            nodes: vec![],
            edges: vec![],
        };
        let (anc, desc) = topo.lineage("missing");
        assert!(anc.is_empty());
        assert!(desc.is_empty());
    }

    #[test]
    fn test_to_topology_linear_chain() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let topo = to_topology(&graph);

        assert_eq!(topo.nodes.len(), 3);
        let mut node_names: Vec<&str> = topo.nodes.iter().map(|n| n.name.as_str()).collect();
        node_names.sort();
        assert_eq!(node_names, vec!["a", "b", "c"]);

        for n in &topo.nodes {
            assert_eq!(n.kind, NodeKind::Asset);
            assert_eq!(n.group, None);
            assert_eq!(n.parent_graph, None);
        }

        assert_eq!(topo.edges.len(), 2);
        let expected_edges = vec![
            ("b".to_string(), "a".to_string()),
            ("c".to_string(), "b".to_string()),
        ];
        assert_eq!(topo.edges, expected_edges);
    }

    #[test]
    fn test_to_topology_diamond() {
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let topo = to_topology(&graph);

        assert_eq!(topo.nodes.len(), 4);
        let mut node_names: Vec<&str> = topo.nodes.iter().map(|n| n.name.as_str()).collect();
        node_names.sort();
        assert_eq!(node_names, vec!["a", "b", "c", "d"]);

        assert_eq!(topo.edges.len(), 4);
        let mut expected_edges = vec![
            ("b".to_string(), "a".to_string()),
            ("c".to_string(), "a".to_string()),
            ("d".to_string(), "b".to_string()),
            ("d".to_string(), "c".to_string()),
        ];
        expected_edges.sort();
        assert_eq!(topo.edges, expected_edges);
    }

    #[test]
    fn test_to_topology_empty_graph() {
        let graph = make_graph(vec![]);
        let topo = to_topology(&graph);
        assert_eq!(topo.nodes.len(), 0);
        assert_eq!(topo.edges.len(), 0);
    }

    #[test]
    fn test_to_topology_single_node_no_edges() {
        let graph = make_graph(vec![("a", vec![])]);
        let topo = to_topology(&graph);
        assert_eq!(topo.nodes.len(), 1);
        assert_eq!(topo.nodes[0].name, "a");
        assert_eq!(topo.nodes[0].kind, NodeKind::Asset);
        assert_eq!(topo.edges.len(), 0);
    }

    #[test]
    fn test_to_topology_task_and_graph_asset_kinds() {
        let mut graph: AssetGraph = Graph::new();
        let asset = DagNode::new("a");
        let mut task = DagNode::new("t");
        task.kind = NodeKind::Task;
        let mut graph_asset = DagNode::new("g");
        graph_asset.kind = NodeKind::GraphAsset;
        graph.add_node(asset);
        graph.add_node(task);
        graph.add_node(graph_asset);

        let topo = to_topology(&graph);
        let kinds: HashMap<&str, NodeKind> = topo
            .nodes
            .iter()
            .map(|n| (n.name.as_str(), n.kind))
            .collect();
        assert_eq!(kinds["a"], NodeKind::Asset);
        assert_eq!(kinds["t"], NodeKind::Task);
        assert_eq!(kinds["g"], NodeKind::GraphAsset);
    }

    #[test]
    fn test_detect_cycles_multiple_independent_cycles() {
        // Two disjoint 2-cycles: (a↔b) and (c↔d).
        let mut repo = make_repo(vec![
            ("a", vec!["b"]),
            ("b", vec!["a"]),
            ("c", vec!["d"]),
            ("d", vec!["c"]),
        ]);
        let err = repo.resolve_asset_graph().unwrap_err();
        assert!(
            err.starts_with("2 cycles detected in asset graph:\n"),
            "unexpected prefix: {err}"
        );
        // Both cycles must be listed (order is determined by tarjan_scc,
        // so check participation rather than exact ordering).
        assert!(err.contains("a -> b -> a") || err.contains("b -> a -> b"));
        assert!(err.contains("c -> d -> c") || err.contains("d -> c -> d"));
    }

    #[test]
    fn test_upstream_closure_multi_target_dedups_shared_ancestors() {
        //   a
        //  / \
        // b   c
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["a"])]);
        let targets = HashSet::from(["b".into(), "c".into()]);
        assert_eq!(
            upstream_closure(&graph, &targets),
            HashSet::from(["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn test_upstream_closure_ignores_unknown_targets() {
        let graph = make_graph(vec![("a", vec![])]);
        let targets = HashSet::from(["a".into(), "missing".into()]);
        assert_eq!(
            upstream_closure(&graph, &targets),
            HashSet::from(["a".into()])
        );
    }

    #[test]
    fn test_subgraph_multiple_missing_deps_accumulate() {
        // d depends on b and c; selection only has d.
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let sel = HashSet::from(["d".into()]);
        let mut missing = validate_subgraph_completeness(&graph, &sel).unwrap_err();
        missing.sort();
        assert_eq!(
            missing,
            vec![("d".into(), "b".into()), ("d".into(), "c".into())]
        );
    }

    #[test]
    fn test_subgraph_unknown_name_in_selection_is_ignored() {
        let graph = make_graph(vec![("a", vec![])]);
        let sel = HashSet::from(["a".into(), "missing".into()]);
        assert!(validate_subgraph_completeness(&graph, &sel).is_ok());
    }

    #[test]
    fn test_ancestors_and_descendants_exclude_self() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        assert!(!ancestors(&graph, "b").contains("b"));
        assert!(!descendants(&graph, "b").contains("b"));
    }
}
