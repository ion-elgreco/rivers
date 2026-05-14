//! Synthetic DAG graph generation for benchmarking and testing the layout engine.

use crate::types::{GraphTopology, TopologyNode};

/// Parse a target node count from the `--synthetic` CLI flag. Accepts the
/// shorthand suffixes `1k`, `10k`, `50k` plus any plain integer; falls back
/// to 100 for unrecognized inputs so a typo doesn't blow up.
pub fn parse_node_count(s: &str) -> usize {
    match s.to_lowercase().as_str() {
        "1k" => 1_000,
        "10k" => 10_000,
        "50k" => 50_000,
        other => other.parse().unwrap_or(100),
    }
}

/// Generate a realistic synthetic DAG topology.
/// Creates a layered graph with groups, cross-layer edges, and long-span edges.
pub fn generate_synthetic_graph(target_nodes: usize) -> GraphTopology {
    let group_names = [
        "data_ingestion",
        "data_processing",
        "feature_engineering",
        "analytics",
        "ml_pipeline",
        "reporting",
        "monitoring",
        "compliance",
        "export",
        "archive",
        "staging",
        "validation",
    ];

    let num_groups = (target_nodes / 50).clamp(3, group_names.len());
    let num_layers = ((target_nodes as f64).sqrt() * 1.6) as usize;
    let num_layers = num_layers.clamp(5, 60);
    let nodes_per_layer = (target_nodes / num_layers).max(3);

    let mut nodes = Vec::with_capacity(num_layers * nodes_per_layer);
    let mut edges = Vec::new();

    let kinds = ["asset", "task", "graph_asset"];

    for layer in 0..num_layers {
        for i in 0..nodes_per_layer {
            let idx = layer * nodes_per_layer + i;
            let group = if i < nodes_per_layer * 9 / 10 {
                Some(group_names[i % num_groups].to_string())
            } else {
                None
            };
            let kind = if group.is_none() {
                kinds[1]
            } else if i % 20 == 0 {
                kinds[2]
            } else {
                kinds[0]
            };

            nodes.push(TopologyNode {
                name: format!("node_{idx}"),
                kind: kind.to_string(),
                group,
                parent_graph: None,
            });

            if layer > 0 {
                let prev = (layer - 1) * nodes_per_layer + i;
                edges.push((format!("node_{idx}"), format!("node_{prev}")));
            }

            if layer > 0 && i + 1 < nodes_per_layer && i % 2 == 0 {
                let prev = (layer - 1) * nodes_per_layer + i + 1;
                edges.push((format!("node_{idx}"), format!("node_{prev}")));
            }

            if layer >= 2 && i % 5 == 0 {
                let far = (layer - 2) * nodes_per_layer + i;
                edges.push((format!("node_{idx}"), format!("node_{far}")));
            }

            if layer >= 3 && i % 15 == 0 {
                let far = (layer - 3) * nodes_per_layer + i;
                edges.push((format!("node_{idx}"), format!("node_{far}")));
            }
        }
    }

    let actual = nodes.len();
    eprintln!(
        "Synthetic graph: {actual} nodes, {} edges, {num_layers} layers, {nodes_per_layer} nodes/layer, {num_groups} groups",
        edges.len()
    );

    GraphTopology { nodes, edges }
}
