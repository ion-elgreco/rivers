//! Browser-based component tests for `DagGraph`.
//!
//! `DagGraph` renders a layered Sugiyama layout to SVG. Three rendering
//! paths exist depending on viewport width:
//!   - full (LOD 0): capsule + dot + text,
//!   - simplified (LOD 1): plain rect + accent rail,
//!   - micro (LOD 2): single colored rect with ~25% opacity.
//!
//! These tests pin each path against a fixed mini-graph and check that
//! selection / materialization / external-asset state surfaces in the
//! rendered attributes.
//!
//! IMPORTANT: `on_node_click` must always be supplied — the no-callback
//! branch wraps each node in an `<a href=use_current_location()...>`
//! link, and `use_current_location` panics outside a router context.

#![cfg(target_arch = "wasm32")]

mod common;

use std::collections::HashSet;

use common::{flush_effects, fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::dag::layout::{LayoutEdge, LayoutNode, LayoutResult};
use rivers_ui::components::dag::render::DagGraph;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn node(id: &str, kind: &str, x: f64, y: f64) -> LayoutNode {
    LayoutNode {
        id: id.to_string(),
        kind: kind.to_string(),
        group: None,
        parent_graph: None,
        x,
        y,
        width: 160.0,
        height: 36.0,
    }
}

fn edge(src: &str, tgt: &str) -> LayoutEdge {
    LayoutEdge {
        source: src.to_string(),
        target: tgt.to_string(),
        waypoints: Vec::new(),
    }
}

fn small_layout() -> LayoutResult {
    LayoutResult {
        nodes: vec![
            node("upstream", "asset", 0.0, 0.0),
            node("middle", "task", 200.0, 0.0),
            node("downstream", "asset", 400.0, 0.0),
        ],
        edges: vec![edge("upstream", "middle"), edge("middle", "downstream")],
        groups: Vec::new(),
        width: 600.0,
        height: 100.0,
    }
}

#[wasm_bindgen_test]
fn full_lod_renders_one_dag_node_group_per_layout_node() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_id: String| {})
                viewport=Signal::derive(move || viewport.get())
            />
        }
    });

    assert_eq!(query_all(&target, "g.dag-node").len(), 3);
    // Full LOD draws an arrow-marker'd path per edge inside the edges <g>.
    assert_eq!(query_all(&target, "path.dag-edge").len(), 2);
}

#[wasm_bindgen_test]
async fn micro_lod_collapses_edges_into_single_path() {
    let target = fresh_mount_target();
    // viewport width > 10_000 → LOD 2 (micro).
    let viewport = RwSignal::new((-1000.0_f64, -1000.0_f64, 12_000.0_f64, 12_000.0_f64));
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_id: String| {})
                viewport=Signal::derive(move || viewport.get())
            />
        }
    });
    flush_effects().await;

    // Micro LOD draws nodes as bare <rect>s, no `g.dag-node` group.
    assert_eq!(query_all(&target, "g.dag-node").len(), 0);
    // All visible edges fold into a single combined path.
    let edges = query_all(&target, "path.dag-edge");
    assert!(
        edges.is_empty(),
        "micro LOD should not emit per-edge .dag-edge paths"
    );
}

#[wasm_bindgen_test]
fn off_viewport_nodes_are_culled() {
    // Viewport sits far to the right of the layout's nodes (which live at
    // x ∈ [0, 400]). The 25% margin around the viewport still won't reach
    // them, so `visible_nodes` should be empty.
    let target = fresh_mount_target();
    let viewport = RwSignal::new((10_000.0_f64, 0.0, 200.0_f64, 200.0_f64));
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_| {})
                viewport=Signal::derive(move || viewport.get())
            />
        }
    });

    assert_eq!(query_all(&target, "g.dag-node").len(), 0);
}

#[wasm_bindgen_test]
fn materialized_node_paints_status_dot_green() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let mat: HashSet<String> = ["middle".to_string()].into_iter().collect();
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_| {})
                viewport=Signal::derive(move || viewport.get())
                materialized_keys=mat.clone()
            />
        }
    });

    // Three nodes, three status circles; the materialized one is the
    // success-green #22c55e. The others fall back to muted #535559.
    let circles = query_all(&target, "g.dag-node circle");
    let fills: Vec<String> = circles
        .iter()
        .map(|c| c.get_attribute("fill").unwrap_or_default())
        .collect();
    assert_eq!(
        fills
            .iter()
            .filter(|f| f.eq_ignore_ascii_case("#22c55e"))
            .count(),
        1
    );
}

#[wasm_bindgen_test]
fn external_node_uses_dashed_capsule_stroke() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let ext: HashSet<String> = ["upstream".to_string()].into_iter().collect();
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_| {})
                viewport=Signal::derive(move || viewport.get())
                external_keys=ext.clone()
            />
        }
    });
    let dashed = query_all(&target, "rect[stroke-dasharray='5 3']");
    assert_eq!(
        dashed.len(),
        1,
        "exactly one node should carry the external-asset dashed stroke"
    );
}

#[wasm_bindgen_test]
fn selected_node_emits_pulsing_halo_rect() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_| {})
                viewport=Signal::derive(move || viewport.get())
                selected_node="middle".to_string()
            />
        }
    });

    // The selection halo carries an SVG <animate> child for the pulse.
    let animates = query_all(&target, "g.dag-node animate");
    assert!(
        !animates.is_empty(),
        "selecting a node should add at least one <animate> for the halo pulse"
    );
}

#[wasm_bindgen_test]
async fn click_on_node_invokes_callback_with_node_id() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let captured = RwSignal::new(Vec::<String>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(move |id: String| { captured.update(|v| v.push(id)) })
                viewport=Signal::derive(move || viewport.get())
            />
        }
    });

    let first_node = query_one(&target, "g.dag-node");
    common::click(&first_node, false);
    flush_effects().await;

    let got = captured.get_untracked();
    assert_eq!(got.len(), 1);
    // The first DOM-rendered node corresponds to the first entry in
    // `visible_nodes` — which the layout's node order pins to "upstream".
    assert_eq!(got[0], "upstream");
}

#[wasm_bindgen_test]
fn changed_keys_attach_new_chip_to_node() {
    let target = fresh_mount_target();
    let viewport = RwSignal::new((-50.0_f64, -50.0_f64, 800.0_f64, 200.0_f64));
    let chg: HashSet<String> = ["downstream".to_string()].into_iter().collect();
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DagGraph
                layout=small_layout()
                on_node_click=Callback::new(|_| {})
                viewport=Signal::derive(move || viewport.get())
                changed_keys=chg.clone()
            />
        }
    });

    // The "NEW" chip text is the only place "NEW" should appear in the SVG.
    let body_text = target.text_content().unwrap_or_default();
    assert!(
        body_text.contains("NEW"),
        "expected NEW chip text, got: {body_text}"
    );
}
