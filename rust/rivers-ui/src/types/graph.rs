use serde::{Deserialize, Serialize};

/// Asset DAG topology read from the per-CL storage blob. `edges` are
/// `(from, to)` where `from` depends on `to`. Mirrors
/// `rivers_core::assets::graph::GraphTopology`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphTopology {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<(String, String)>,
}

impl GraphTopology {
    /// What the lineage page shows. A node inside a collapsed graph asset
    /// shows as the outermost collapsed graph around it, and its edges move
    /// there; then the kind and group filters apply. An empty filter keeps
    /// everything; a group filter drops ungrouped nodes.
    pub fn visible(&self, kinds: &[String], groups: &[String], expanded: &[String]) -> Self {
        use std::collections::{HashMap, HashSet};

        let parent: HashMap<&str, &str> = self
            .nodes
            .iter()
            .filter_map(|n| n.parent_graph.as_deref().map(|p| (n.name.as_str(), p)))
            .collect();
        let expanded: HashSet<&str> = expanded.iter().map(String::as_str).collect();
        let shown_as = |name: &str| -> String {
            let (mut shown, mut current) = (name, name);
            for _ in 0..=parent.len() {
                let Some(&p) = parent.get(current) else { break };
                if !expanded.contains(p) {
                    shown = p;
                }
                current = p;
            }
            shown.to_string()
        };

        let nodes: Vec<TopologyNode> = self
            .nodes
            .iter()
            .filter(|n| shown_as(&n.name) == n.name)
            .filter(|n| kinds.is_empty() || kinds.contains(&n.kind))
            .filter(|n| groups.is_empty() || n.group.as_ref().is_some_and(|g| groups.contains(g)))
            .cloned()
            .collect();
        let names: HashSet<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
        let mut seen = HashSet::new();
        let edges = self
            .edges
            .iter()
            .map(|(from, to)| (shown_as(from), shown_as(to)))
            .filter(|(from, to)| {
                from != to && names.contains(from.as_str()) && names.contains(to.as_str())
            })
            .filter(|edge| seen.insert(edge.clone()))
            .collect();
        Self { nodes, edges }
    }

    /// Compute (ancestors, descendants) of `node_name` from the edge list.
    /// Ancestors = transitive dependencies; descendants = transitive dependents.
    pub fn lineage(&self, node_name: &str) -> (Vec<String>, Vec<String>) {
        use std::collections::{HashMap, HashSet};

        let mut deps_of: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut dependents_of: HashMap<&str, Vec<&str>> = HashMap::new();
        for (from, to) in &self.edges {
            deps_of.entry(from.as_str()).or_default().push(to.as_str());
            dependents_of
                .entry(to.as_str())
                .or_default()
                .push(from.as_str());
        }

        let mut ancestors = HashSet::new();
        let mut stack: Vec<&str> = deps_of.get(node_name).cloned().unwrap_or_default();
        while let Some(n) = stack.pop() {
            if ancestors.insert(n.to_string())
                && let Some(deps) = deps_of.get(n)
            {
                stack.extend(deps.iter());
            }
        }

        let mut descendants = HashSet::new();
        let mut stack: Vec<&str> = dependents_of.get(node_name).cloned().unwrap_or_default();
        while let Some(n) = stack.pop() {
            if descendants.insert(n.to_string())
                && let Some(deps) = dependents_of.get(n)
            {
                stack.extend(deps.iter());
            }
        }

        let mut anc: Vec<String> = ancestors.into_iter().collect();
        let mut desc: Vec<String> = descendants.into_iter().collect();
        anc.sort();
        desc.sort();
        (anc, desc)
    }

    /// Direct upstream dependencies (one hop) of `node_name`.
    ///
    /// Edges are `(from, to)` where `from` depends on `to`, so the upstream of
    /// `node_name` are the `to` of edges where it is the `from`.
    pub fn direct_upstream(&self, node_name: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|(from, _)| from == node_name)
            .map(|(_, to)| to.clone())
            .collect()
    }

    /// Direct downstream dependents (one hop) of `node_name`.
    ///
    /// The downstream of `node_name` are the `from` of edges where it is the
    /// `to` (i.e. the assets that depend on it).
    pub fn direct_downstream(&self, node_name: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|(_, to)| to == node_name)
            .map(|(from, _)| from.clone())
            .collect()
    }
}

/// One node in [`GraphTopology`]. `kind` is one of `"asset"`, `"task"`,
/// `"graph_asset"`. `parent_graph` is `Some(name)` when the node lives
/// inside a graph asset (its name appears in that asset's
/// `inner_invocations`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub parent_graph: Option<String>,
}

#[cfg(test)]
mod topology_tests {
    use super::{GraphTopology, TopologyNode};

    fn node(name: &str) -> TopologyNode {
        TopologyNode {
            name: name.into(),
            kind: "asset".into(),
            group: None,
            parent_graph: None,
        }
    }

    fn inside(name: &str, graph: &str) -> TopologyNode {
        TopologyNode {
            name: name.into(),
            kind: "task".into(),
            group: None,
            parent_graph: Some(graph.into()),
        }
    }

    fn edge(from: &str, to: &str) -> (String, String) {
        (from.into(), to.into())
    }

    fn names(topo: &GraphTopology) -> Vec<&str> {
        topo.nodes.iter().map(|n| n.name.as_str()).collect()
    }

    /// A collapsed graph asset stands in for its children: their edges move
    /// to it, edges between them vanish, and the resulting duplicates merge.
    #[test]
    fn visible_folds_children_into_a_collapsed_graph() {
        let topo = GraphTopology {
            nodes: vec![
                node("raw"),
                node("pipe"),
                inside("pipe/a", "pipe"),
                inside("pipe/b", "pipe"),
                node("report"),
            ],
            edges: vec![
                edge("pipe/a", "raw"),
                edge("pipe/b", "pipe/a"),
                edge("pipe", "pipe/b"),
                edge("report", "pipe/a"),
                edge("report", "pipe/b"),
            ],
        };

        let collapsed = topo.visible(&[], &[], &[]);
        assert_eq!(names(&collapsed), vec!["raw", "pipe", "report"]);
        assert_eq!(
            collapsed.edges,
            vec![edge("pipe", "raw"), edge("report", "pipe")]
        );

        let expanded = topo.visible(&[], &[], &["pipe".into()]);
        assert_eq!(expanded.nodes.len(), 5);
        assert_eq!(expanded.edges, topo.edges);
    }

    /// Inside a collapsed outer graph, an expanded inner graph still hides:
    /// everything shows as the outermost collapsed graph.
    #[test]
    fn visible_folds_nested_graphs_into_the_outermost_collapsed_one() {
        let topo = GraphTopology {
            nodes: vec![
                node("src"),
                node("outer"),
                inside("inner", "outer"),
                inside("leaf", "inner"),
            ],
            edges: vec![edge("leaf", "src")],
        };
        let shown = topo.visible(&[], &[], &["inner".into()]);
        assert_eq!(names(&shown), vec!["src", "outer"]);
        assert_eq!(shown.edges, vec![edge("outer", "src")]);
    }

    #[test]
    fn visible_applies_kind_and_group_filters() {
        let grouped = |name: &str, group: Option<&str>| TopologyNode {
            group: group.map(Into::into),
            ..node(name)
        };
        let topo = GraphTopology {
            nodes: vec![
                grouped("a", Some("g1")),
                grouped("b", Some("g2")),
                grouped("c", None),
                inside("t", "b"),
            ],
            edges: vec![edge("b", "a"), edge("c", "b")],
        };

        let g1_g2 = topo.visible(&[], &["g1".into(), "g2".into()], &[]);
        assert_eq!(names(&g1_g2), vec!["a", "b"]);
        assert_eq!(g1_g2.edges, vec![edge("b", "a")]);

        let tasks = topo.visible(&["task".into()], &[], &["b".into()]);
        assert_eq!(names(&tasks), vec!["t"]);
        assert!(tasks.edges.is_empty());
    }

    /// `summary` depends on `raw_data` → edge `(summary, raw_data)`. Selecting
    /// `raw_data`, `summary` must be DOWNSTREAM (it consumes raw_data), not
    /// upstream. Regression for the swapped lineage labels (issue #57).
    #[test]
    fn direct_upstream_downstream_directions() {
        let topo = GraphTopology {
            nodes: vec![node("raw_data"), node("summary")],
            edges: vec![("summary".into(), "raw_data".into())],
        };

        // raw_data is a source: no upstream, summary downstream.
        assert!(topo.direct_upstream("raw_data").is_empty());
        assert_eq!(topo.direct_downstream("raw_data"), vec!["summary"]);

        // summary consumes raw_data: raw_data upstream, no downstream.
        assert_eq!(topo.direct_upstream("summary"), vec!["raw_data"]);
        assert!(topo.direct_downstream("summary").is_empty());
    }
}
