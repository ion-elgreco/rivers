//! Browser-based component tests for `EvalTree`.
//!
//! `EvalTree` flattens a recursive `EvalNodeResult` tree and renders it
//! as a flat list with depth-driven indent and per-node expand/collapse
//! toggles. Tests cover: the flat-render shape, the smart-default
//! collapsed branches for `All of (false)` / `Any of (true)`, and that
//! clicking a header collapses the subtree (rows hidden via inline
//! `display: none`).

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_all};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::eval_tree::EvalTree;
use rivers_ui::types::{EvalNodeResult, NodeStatus};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn leaf(label: &str, status: NodeStatus) -> EvalNodeResult {
    EvalNodeResult {
        node_idx: 0,
        label: label.to_string(),
        node_type: "leaf".into(),
        status,
        children: Vec::new(),
        num_partitions: None,
    }
}

fn node(label: &str, status: NodeStatus, children: Vec<EvalNodeResult>) -> EvalNodeResult {
    EvalNodeResult {
        node_idx: 0,
        label: label.to_string(),
        node_type: "branch".into(),
        status,
        children,
        num_partitions: None,
    }
}

fn visible(host: &web_sys::HtmlElement) -> Vec<web_sys::Element> {
    query_all(host, ".eval-node-flat")
        .into_iter()
        .filter(|n| {
            !n.get_attribute("style")
                .unwrap_or_default()
                .contains("display: none")
        })
        .collect()
}

#[wasm_bindgen_test]
fn flat_render_emits_one_div_per_node() {
    let tree = node(
        "All of",
        NodeStatus::True,
        vec![leaf("a", NodeStatus::True), leaf("b", NodeStatus::True)],
    );
    let target = fresh_mount_target();
    let _handle = mount_to(
        target.clone(),
        move || view! { <EvalTree tree=tree.clone() /> },
    );

    assert_eq!(query_all(&target, ".eval-node-flat").len(), 3);
    // Root + 2 children all visible by default (root expanded).
    assert_eq!(visible(&target).len(), 3);
}

#[wasm_bindgen_test]
fn smart_collapse_keeps_first_failing_child_in_all_of_false() {
    // `All of` (false) has two failing children; only the first should
    // stay expanded under the smart-default rule. Nested grandchildren
    // of the second branch must be collapsed (display:none).
    let tree = node(
        "All of",
        NodeStatus::False,
        vec![
            node(
                "first-fail",
                NodeStatus::False,
                vec![leaf("inner-a", NodeStatus::False)],
            ),
            node(
                "second-fail",
                NodeStatus::False,
                vec![leaf("inner-b", NodeStatus::False)],
            ),
        ],
    );
    let target = fresh_mount_target();
    let _handle = mount_to(
        target.clone(),
        move || view! { <EvalTree tree=tree.clone() /> },
    );

    let labels: Vec<String> = visible(&target)
        .iter()
        .map(|el| el.text_content().unwrap_or_default())
        .collect();
    let joined = labels.join("|");
    assert!(
        joined.contains("first-fail"),
        "first failing branch visible: {joined}"
    );
    assert!(
        joined.contains("inner-a"),
        "first branch's child visible: {joined}"
    );
    assert!(
        joined.contains("second-fail"),
        "sibling branch header visible: {joined}"
    );
    assert!(
        !joined.contains("inner-b"),
        "second branch's child should be hidden by smart-default: {joined}"
    );
}

#[wasm_bindgen_test]
async fn click_on_header_collapses_subtree() {
    let tree = node(
        "All of",
        NodeStatus::True,
        vec![leaf("a", NodeStatus::True), leaf("b", NodeStatus::True)],
    );
    let target = fresh_mount_target();
    let _handle = mount_to(
        target.clone(),
        move || view! { <EvalTree tree=tree.clone() /> },
    );

    assert_eq!(visible(&target).len(), 3);

    let root_header = query_all(&target, ".eval-node-header")[0].clone();
    click(&root_header, false);
    flush_effects().await;

    // After collapsing the root, only the root itself should remain visible.
    assert_eq!(visible(&target).len(), 1);
}

#[wasm_bindgen_test]
fn status_class_lights_up_per_node_status() {
    let tree = node(
        "All of",
        NodeStatus::False,
        vec![
            leaf("ok", NodeStatus::True),
            leaf("bad", NodeStatus::False),
            leaf("skip", NodeStatus::Skipped),
        ],
    );
    let target = fresh_mount_target();
    let _handle = mount_to(
        target.clone(),
        move || view! { <EvalTree tree=tree.clone() /> },
    );

    let nodes = query_all(&target, ".eval-node-flat");
    let classes: Vec<String> = nodes.iter().map(|n| n.class_name()).collect();
    let joined = classes.join("|");
    assert!(joined.contains("eval-true"));
    assert!(joined.contains("eval-false"));
    assert!(joined.contains("eval-skipped"));
}
