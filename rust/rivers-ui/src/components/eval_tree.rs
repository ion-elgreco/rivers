//! Condition evaluation tree visualization.

use std::rc::Rc;

use leptos::prelude::*;

use crate::types::{EvalNodeResult, NodeStatus};

/// Flattened tree node for non-recursive rendering.
#[derive(Clone)]
struct FlatNode {
    depth: usize,
    label: String,
    status: NodeStatus,
    has_children: bool,
    child_indices: Vec<usize>,
}

fn flatten_tree(
    node: &EvalNodeResult,
    depth: usize,
    out: &mut Vec<FlatNode>,
    parent_indices: &mut Vec<Option<usize>>,
) {
    let idx = out.len();
    out.push(FlatNode {
        depth,
        label: node.label.clone(),
        status: node.status.clone(),
        has_children: !node.children.is_empty(),
        child_indices: vec![],
    });
    parent_indices.push(None);

    let mut child_indices = Vec::new();
    for child in &node.children {
        let child_idx = out.len();
        child_indices.push(child_idx);
        parent_indices.push(Some(idx)); // Will be overwritten by recursive call's push
        parent_indices.pop(); // Remove premature push — let recursion handle it
        flatten_tree(child, depth + 1, out, parent_indices);
        parent_indices[child_idx] = Some(idx);
    }
    out[idx].child_indices = child_indices;
}

fn all_descendants(nodes: &[FlatNode], idx: usize) -> Vec<usize> {
    if !nodes[idx].has_children {
        return Vec::new();
    }
    let mut result = Vec::new();
    let mut stack = nodes[idx].child_indices.clone();
    while let Some(i) = stack.pop() {
        result.push(i);
        stack.extend_from_slice(&nodes[i].child_indices);
    }
    result
}

fn status_label(status: &NodeStatus) -> &'static str {
    match status {
        NodeStatus::True => "True",
        NodeStatus::False => "False",
        NodeStatus::Skipped => "Skipped",
    }
}

fn status_class(status: &NodeStatus) -> &'static str {
    match status {
        NodeStatus::True => "eval-true",
        NodeStatus::False => "eval-false",
        NodeStatus::Skipped => "eval-skipped",
    }
}

#[component]
pub fn EvalTree(tree: EvalNodeResult) -> impl IntoView {
    let mut flat = Vec::new();
    let mut parent_indices = Vec::new();
    flatten_tree(&tree, 0, &mut flat, &mut parent_indices);

    let expanded_signals: Vec<(ReadSignal<bool>, WriteSignal<bool>)> = {
        let mut defaults = vec![false; flat.len()];
        for (i, node) in flat.iter().enumerate() {
            defaults[i] = node.has_children;
        }
        // Smart expansion: for And(false), only expand the first false child;
        // for Or(true), only expand the first true child.
        for i in 0..flat.len() {
            if !flat[i].has_children {
                continue;
            }
            let interesting = match flat[i].status {
                NodeStatus::False if matches!(flat[i].label.as_str(), "All of") => {
                    Some(NodeStatus::False)
                }
                NodeStatus::True if matches!(flat[i].label.as_str(), "Any of") => {
                    Some(NodeStatus::True)
                }
                _ => None,
            };
            if let Some(target_status) = interesting {
                let mut found_first = false;
                for &ci in &flat[i].child_indices {
                    let is_match = flat[ci].status == target_status;
                    if is_match && !found_first {
                        found_first = true;
                    } else if flat[ci].has_children {
                        defaults[ci] = false;
                    }
                }
            }
        }
        defaults.into_iter().map(signal).collect()
    };

    // Wrap in Rc so closures can share without O(n²) cloning
    let signals = Rc::new(expanded_signals);

    // Precompute descendants for each node (skip leaves)
    let descendants: Vec<Vec<usize>> = (0..flat.len()).map(|i| all_descendants(&flat, i)).collect();

    // Precompute ancestor signal chains for visibility checks
    let ancestor_signals: Vec<Vec<ReadSignal<bool>>> = (0..flat.len())
        .map(|i| {
            let mut chain = Vec::new();
            let mut cur = parent_indices[i];
            while let Some(p) = cur {
                chain.push(signals[p].0);
                cur = parent_indices[p];
            }
            chain
        })
        .collect();

    view! {
        <div class="eval-tree">
            {flat.iter().enumerate().map(|(i, node)| {
                let st_class = status_class(&node.status);
                let st_label = status_label(&node.status);
                let label = node.label.clone();
                let depth = node.depth;
                let has_children = node.has_children;
                let (expanded, _) = signals[i];
                let descendants_for_toggle = descendants[i].clone();
                let signals_ref = Rc::clone(&signals);
                let ancestors = ancestor_signals[i].clone();

                view! {
                    <div
                        class={format!("eval-node-flat {}", st_class)}
                        style=move || {
                            let visible = ancestors.iter().all(|sig| sig.get());
                            if visible {
                                format!("padding-left: {}rem", depth as f64 * 1.25)
                            } else {
                                "display: none".to_string()
                            }
                        }
                    >
                        <div
                            class="eval-node-header"
                            on:click=move |_| {
                                if has_children {
                                    let current = expanded.get();
                                    signals_ref[i].1.set(!current);
                                    if current {
                                        for &d in &descendants_for_toggle {
                                            signals_ref[d].1.set(false);
                                        }
                                    }
                                }
                            }
                            style=move || if has_children { "cursor: pointer" } else { "" }
                        >
                            {has_children.then(|| view! {
                                <span class="eval-toggle">
                                    {move || if expanded.get() { "\u{25BE}" } else { "\u{25B8}" }}
                                </span>
                            })}
                            {(!has_children).then(|| view! {
                                <span class="eval-toggle eval-toggle-spacer">{"\u{00B7}"}</span>
                            })}
                            <span class={format!("eval-badge {}", st_class)}>{st_label}</span>
                            <span class="eval-label">{label}</span>
                        </div>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}
