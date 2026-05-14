//! SVG rendering of the DAG graph.

use leptos::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::layout::{LayoutEdge, LayoutNode, LayoutResult};

fn kind_accent(kind: &str) -> &'static str {
    match kind {
        "asset" => "#ff8f78",       // primary (rust)
        "task" => "#50e1f9",        // secondary (cyan)
        "graph_asset" => "#ff775d", // primary-container (deep rust)
        _ => "#535559",             // muted
    }
}

fn kind_label(kind: &str) -> &'static str {
    match kind {
        "asset" => "Asset",
        "task" => "Task",
        "graph_asset" => "Graph",
        _ => "Asset",
    }
}

fn edge_path(edge: &LayoutEdge, nodes: &HashMap<String, &LayoutNode>) -> String {
    edge.path_d(nodes)
}

#[inline]
fn rect_visible(x: f64, y: f64, w: f64, h: f64, vx1: f64, vy1: f64, vx2: f64, vy2: f64) -> bool {
    x + w >= vx1 && x <= vx2 && y + h >= vy1 && y <= vy2
}

/// Level of detail based on visible viewport width (in SVG units).
/// `0` = full detail, `1` = simplified (no text labels), `2` = micro
/// (colored dots only). Thresholds are chosen so a typical browser viewport
/// shows full detail; level 1 and 2 kick in only when zoomed out beyond
/// what text glyphs can render legibly.
fn lod_level(viewport_w: f64) -> u8 {
    if viewport_w > 10_000.0 {
        2
    } else if viewport_w > 4_000.0 {
        1
    } else {
        0
    }
}

/// SVG renderer for a [`LayoutResult`]. Honors the LOD strategy (full /
/// simplified / micro) based on the visible viewport size, and overlays
/// per-node decorations (selection ring, materialized/stale dots,
/// ancestor/descendant lineage stroke, "NEW" chip for changed nodes).
#[component]
pub fn DagGraph(
    layout: LayoutResult,
    #[prop(optional)] on_node_click: Option<Callback<String>>,
    #[prop(optional)] materialized_keys: HashSet<String>,
    /// Subset of `materialized_keys` whose `stale_status == Stale` — drives the
    /// orange dot variant so the renderer doesn't fall back to the all-or-nothing
    /// "has timestamp" signal.
    #[prop(optional)]
    stale_keys: HashSet<String>,
    #[prop(optional)] external_keys: HashSet<String>,
    #[prop(optional)] ancestor_keys: HashSet<String>,
    #[prop(optional)] descendant_keys: HashSet<String>,
    #[prop(optional)] selected_node: String,
    #[prop(optional)] graph_asset_names: HashSet<String>,
    #[prop(optional)] expanded_graphs: HashSet<String>,
    /// Viewport bounds in SVG coordinates: (x, y, width, height)
    #[prop(into)]
    viewport: Signal<(f64, f64, f64, f64)>,
    /// Nodes marked as "changed in the last deploy". Rendered with a small
    /// "NEW" chip + accent ring. Empty = nothing flagged.
    #[prop(optional)]
    changed_keys: HashSet<String>,
) -> impl IntoView {
    let has_selection = !selected_node.is_empty();
    let node_ref_map: HashMap<String, &LayoutNode> =
        layout.nodes.iter().map(|n| (n.id.clone(), n)).collect();

    // Pre-compute all edge paths (strings are cheap, avoids recomputing on viewport change)
    let precomputed_edges: Vec<(LayoutEdge, String)> = layout
        .edges
        .iter()
        .map(|e| (e.clone(), edge_path(e, &node_ref_map)))
        .collect();

    let all_nodes = Arc::new(layout.nodes);
    let all_groups = Arc::new(layout.groups);
    let all_edges = Arc::new(precomputed_edges);

    let node_pos: Arc<HashMap<String, (f64, f64)>> = Arc::new(
        all_nodes
            .iter()
            .map(|n| (n.id.clone(), (n.x + n.width / 2.0, n.y + n.height / 2.0)))
            .collect(),
    );

    let view_box_str = Signal::derive(move || {
        let (x, y, w, h) = viewport.get();
        format!("{x} {y} {w} {h}")
    });

    // Visible rect with margin for smoother scrolling
    let vis_rect = Memo::new(move |_| {
        let (vx, vy, vw, vh) = viewport.get();
        let margin = vw.max(vh) * 0.25;
        (vx - margin, vy - margin, vx + vw + margin, vy + vh + margin)
    });

    let lod = Memo::new(move |_| {
        let (_, _, vw, _) = viewport.get();
        lod_level(vw)
    });

    let nodes_ref = Arc::clone(&all_nodes);
    let visible_nodes = Memo::new(move |_| {
        let (x1, y1, x2, y2) = vis_rect.get();
        nodes_ref
            .iter()
            .filter(|n| rect_visible(n.x, n.y, n.width, n.height, x1, y1, x2, y2))
            .cloned()
            .collect::<Vec<_>>()
    });

    // Group bounding boxes are intentionally not computed or drawn — see the
    // comment in the view below for why.
    let _ = &all_groups;

    let edges_ref = Arc::clone(&all_edges);
    let node_pos_ref = Arc::clone(&node_pos);
    let visible_edges = Memo::new(move |_| {
        let (x1, y1, x2, y2) = vis_rect.get();
        edges_ref
            .iter()
            .filter(|(e, _)| {
                let src_vis = node_pos_ref
                    .get(&e.source)
                    .map(|&(px, py)| px >= x1 && px <= x2 && py >= y1 && py <= y2)
                    .unwrap_or(false);
                let tgt_vis = node_pos_ref
                    .get(&e.target)
                    .map(|&(px, py)| px >= x1 && px <= x2 && py >= y1 && py <= y2)
                    .unwrap_or(false);
                src_vis || tgt_vis
            })
            .cloned()
            .collect::<Vec<_>>()
    });

    view! {
        <div class="dag-container">
            <svg viewBox=move || view_box_str.get()>
                <defs>
                    <marker
                        id="arrowhead"
                        markerWidth="10"
                        markerHeight="10"
                        refX="0"
                        refY="5"
                        orient="auto"
                        markerUnits="userSpaceOnUse"
                    >
                        <polygon points="0 1, 10 5, 0 9" fill="#535559"/>
                    </marker>
                    <filter id="node-shadow" x="-4%" y="-4%" width="108%" height="116%">
                        <feDropShadow dx="0" dy="1" stdDeviation="2" flood-opacity="0.2" flood-color="#000"/>
                    </filter>
                </defs>

                // Groups — bounding boxes intentionally not drawn. Assets in the
                // same group can belong to unrelated dependency chains, so group
                // boxes imply a containment relationship that doesn't match the
                // actual lineage structure.

                // Edges
                {
                let edge_anc = ancestor_keys.clone();
                let edge_desc = descendant_keys.clone();
                let edge_sel = selected_node.clone();
                move || {
                    let cur_lod = lod.get();
                    let anc = &edge_anc;
                    let desc = &edge_desc;
                    let sel = &edge_sel;

                    if cur_lod >= 2 {
                        // Micro LOD: batch all edges into a single path for minimal DOM
                        let combined: String = visible_edges.get().iter().map(|(_, d)| d.as_str()).collect::<Vec<_>>().join(" ");
                        if combined.is_empty() {
                            view! { <g/> }.into_any()
                        } else {
                            view! {
                                <path
                                    d={combined}
                                    stroke="#535559"
                                    stroke-width="1"
                                    fill="none"
                                />
                            }.into_any()
                        }
                    } else {
                        // Build lineage node set for edge dimming
                        let lineage_set: HashSet<&str> = if has_selection {
                            let mut s: HashSet<&str> = HashSet::new();
                            if !sel.is_empty() {
                                s.insert(sel.as_str());
                            }
                            s.extend(anc.iter().map(|s| s.as_str()));
                            s.extend(desc.iter().map(|s| s.as_str()));
                            s
                        } else {
                            HashSet::new()
                        };

                        view! {
                            <g>
                                {visible_edges.get().into_iter().map(|(e, d)| {
                                    let edge_in_lineage = !has_selection
                                        || (lineage_set.contains(e.source.as_str())
                                            && lineage_set.contains(e.target.as_str()));
                                    let edge_opacity = if edge_in_lineage { "1" } else { "0.15" };

                                    // When selection is active, color in-lineage edges to match the flow direction.
                                    // Upstream (both endpoints are ancestors or the selected node): secondary/cyan.
                                    // Downstream (both are descendants or selected): primary/rust.
                                    let (edge_stroke, flowing) = if has_selection && edge_in_lineage {
                                        let src_anc = anc.contains(&e.source) || sel == &e.source;
                                        let tgt_anc = anc.contains(&e.target) || sel == &e.target;
                                        let src_desc = desc.contains(&e.source) || sel == &e.source;
                                        let tgt_desc = desc.contains(&e.target) || sel == &e.target;
                                        if src_anc && tgt_anc {
                                            ("#50e1f9", true)
                                        } else if src_desc && tgt_desc {
                                            ("#ff8f78", true)
                                        } else {
                                            ("#535559", false)
                                        }
                                    } else {
                                        ("#535559", false)
                                    };
                                    let edge_class = if flowing { "dag-edge dag-edge-flowing" } else { "dag-edge" };
                                    view! {
                                        <path
                                            class={edge_class}
                                            d={d}
                                            stroke={edge_stroke}
                                            stroke-width="2"
                                            fill="none"
                                            opacity={edge_opacity}
                                            marker-end="url(#arrowhead)"
                                        />
                                    }
                                }).collect::<Vec<_>>()}
                            </g>
                        }.into_any()
                    }
                }}

                // Nodes

                {move || {
                    let cur_lod = lod.get();
                    let mat_keys = &materialized_keys;
                    let stl_keys = &stale_keys;
                    let ext_keys = &external_keys;
                    let anc_keys = &ancestor_keys;
                    let desc_keys = &descendant_keys;
                    let sel_node = &selected_node;
                    let ga_names = &graph_asset_names;
                    let exp_graphs = &expanded_graphs;
                    let chg_keys = &changed_keys;

                    visible_nodes.get().into_iter().map(|node| {
                        match cur_lod {
                            2 => render_node_micro(&node),
                            1 => render_node_simplified(&node),
                            _ => render_node_full(
                                &node,
                                on_node_click,
                                mat_keys,
                                stl_keys,
                                ext_keys,
                                anc_keys,
                                desc_keys,
                                sel_node,
                                has_selection,
                                ga_names,
                                exp_graphs,
                                chg_keys,
                            ),
                        }
                    }).collect::<Vec<_>>()
                }}
            </svg>
        </div>
    }
}

/// Full detail node rendering (LOD 0)
fn render_node_full(
    node: &LayoutNode,
    on_node_click: Option<Callback<String>>,
    materialized_keys: &HashSet<String>,
    stale_keys: &HashSet<String>,
    external_keys: &HashSet<String>,
    ancestor_keys: &HashSet<String>,
    descendant_keys: &HashSet<String>,
    selected_node: &str,
    has_selection: bool,
    graph_asset_names: &HashSet<String>,
    expanded_graphs: &HashSet<String>,
    changed_keys: &HashSet<String>,
) -> AnyView {
    let accent = kind_accent(&node.kind);
    let kind_text = kind_label(&node.kind);
    let is_graph_asset = graph_asset_names.contains(&node.id);
    let is_expanded = expanded_graphs.contains(&node.id);
    let node_id = node.id.clone();
    let is_external = external_keys.contains(&node.id);
    let is_materialized = materialized_keys.contains(&node.id);
    let is_stale = stale_keys.contains(&node.id);
    let is_selected = !selected_node.is_empty() && selected_node == node.id;
    let is_ancestor = ancestor_keys.contains(&node.id);
    let is_descendant = descendant_keys.contains(&node.id);
    let is_in_lineage = is_selected || is_ancestor || is_descendant;
    let is_changed = changed_keys.contains(&node.id);

    // When a node is selected, dim nodes outside the lineage
    let opacity = if has_selection && !is_in_lineage {
        "0.3"
    } else {
        "1"
    };

    // Stroke color: highlight ancestors (secondary/cyan), descendants (primary/rust), selected (white)
    let lineage_stroke = if is_selected {
        Some("#e4e6eb") // white
    } else if is_ancestor {
        Some("#50e1f9") // secondary (cyan)
    } else if is_descendant {
        Some("#ff8f78") // primary (rust)
    } else {
        None
    };

    let _ = accent; // kind accent no longer drawn as a left rail on the capsule
    let x = node.x;
    let y = node.y;
    let w = node.width;
    let h = node.height;
    let capsule_r = h / 2.0;

    // Status dot on the LEFT of the capsule (Rivers pattern)
    let status_x = x + capsule_r;
    let status_y = y + h / 2.0;
    let status_color = if is_stale {
        "#f59e0b" // warning (orange) — materialized but upstream newer
    } else if is_materialized {
        "#22c55e" // success (green) — up-to-date
    } else {
        "#535559" // muted (gray) — missing
    };

    let stroke_color = if let Some(lc) = lineage_stroke {
        lc
    } else if is_external {
        "#535559"
    } else if is_changed {
        "#ff8f78"
    } else {
        "rgba(70,72,75,0.35)"
    };
    let stroke_width = if is_selected {
        "1.5"
    } else if is_changed {
        "1.2"
    } else {
        "1"
    };

    // Text area starts to the right of the status dot
    let text_x = x + capsule_r * 2.0 - 2.0;
    let primary_y = y + h / 2.0 - 2.0;
    let secondary_y = y + h / 2.0 + 12.0;

    // Leaf name for primary text (Rivers: a.key.split("/").pop())
    let leaf_name: String = node.id.rsplit('/').next().unwrap_or(&node.id).to_string();
    let secondary_text: String = match (&node.group, kind_text) {
        (Some(g), k) if !k.is_empty() => format!("{g} · {k}"),
        (Some(g), _) => g.clone(),
        (None, k) => k.to_string(),
    };

    // Ellipsis-truncate to fit the available capsule width. The right end of
    // the capsule curves inward, so trim the safe zone a bit more for text on
    // the bottom row (where the curve eats a few pixels).
    let avail_w = (w - (capsule_r * 2.0) - 16.0).max(30.0);
    let char_w = 7.5; // approx width of mono 11.5px glyph
    let max_chars = (avail_w / char_w) as usize;
    let label_text = if leaf_name.chars().count() > max_chars && max_chars > 1 {
        let keep: String = leaf_name
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect();
        format!("{keep}\u{2026}")
    } else {
        leaf_name.clone()
    };

    // Secondary text uses the 9.5px font (~6px per char) and a slightly tighter
    // safe width to avoid clipping against the curved right edge of the pill.
    let char_w_small = 6.0;
    let secondary_avail_w = (avail_w - 6.0).max(24.0);
    let secondary_max_chars = (secondary_avail_w / char_w_small) as usize;
    let secondary_text =
        if secondary_text.chars().count() > secondary_max_chars && secondary_max_chars > 1 {
            let keep: String = secondary_text
                .chars()
                .take(secondary_max_chars.saturating_sub(1))
                .collect();
            format!("{keep}\u{2026}")
        } else {
            secondary_text
        };

    let tooltip = node.id.clone();

    let node_view = view! {
        <g
            class="dag-node"
            opacity={opacity}
            on:click=move |ev| {
                if let Some(ref cb) = on_node_click {
                    ev.prevent_default();
                    cb.run(node_id.clone());
                }
            }
        >
            // Selected-state halo — a slightly larger capsule ring that pulses
            {is_selected.then(|| view! {
                <rect
                    x={(x - 3.0).to_string()}
                    y={(y - 3.0).to_string()}
                    width={(w + 6.0).to_string()}
                    height={(h + 6.0).to_string()}
                    rx={((h + 6.0) / 2.0).to_string()}
                    fill="none"
                    stroke="#ff8f78"
                    stroke-width="1"
                    opacity="0.4"
                >
                    <animate attributeName="opacity" values="0.2;0.6;0.2" dur="2s" repeatCount="indefinite"/>
                </rect>
            })}

            // Main capsule
            <rect
                x={x.to_string()}
                y={y.to_string()}
                width={w.to_string()}
                height={h.to_string()}
                rx={capsule_r.to_string()}
                fill="#171a1d"
                stroke={stroke_color}
                stroke-width={stroke_width}
                stroke-dasharray=move || if is_external { "5 3" } else { "" }
                filter="url(#node-shadow)"
            />

            // Status dot (LEFT side, inside the capsule)
            <circle
                cx={status_x.to_string()}
                cy={status_y.to_string()}
                r="5"
                fill={status_color}
            >
            </circle>
            // Running ripple (when materialized and within current lineage selection)
            {(is_materialized && is_in_lineage).then(|| view! {
                <circle
                    cx={status_x.to_string()}
                    cy={status_y.to_string()}
                    r="6"
                    fill="none"
                    stroke={status_color}
                    stroke-width="1"
                    opacity="0.6"
                >
                    <animate attributeName="r" values="6;12;6" dur="1.6s" repeatCount="indefinite"/>
                    <animate attributeName="opacity" values="0.6;0;0.6" dur="1.6s" repeatCount="indefinite"/>
                </circle>
            })}

            // Primary text — asset name leaf
            <text
                x={text_x.to_string()}
                y={primary_y.to_string()}
                fill="#e4e6eb"
                font-size="11.5"
                font-weight="500"
                font-family="JetBrains Mono, monospace"
            >
                <title>{tooltip}</title>
                {label_text}
            </text>

            // Secondary text — group · kind
            <text
                x={text_x.to_string()}
                y={secondary_y.to_string()}
                fill="#8b8f9e"
                font-size="9.5"
                font-family="JetBrains Mono, monospace"
            >
                {secondary_text}
            </text>

            // Expand/collapse indicator for graph_asset nodes
            {is_graph_asset.then(|| {
                let indicator = if is_expanded { "\u{25BC}" } else { "\u{25B6}" };
                view! {
                    <text
                        x={(x + w - capsule_r - 4.0).to_string()}
                        y={(y + h / 2.0 + 4.0).to_string()}
                        fill="#8b8f9e"
                        font-size="9"
                        style="cursor: pointer"
                    >
                        {indicator}
                    </text>
                }
            })}

            // "NEW" chip — shown when the deploy view flags this node as changed
            {is_changed.then(|| {
                let chip_x = x + w - 30.0;
                let chip_y = y - 6.0;
                view! {
                    <>
                        <rect
                            x={chip_x.to_string()}
                            y={chip_y.to_string()}
                            width="24"
                            height="12"
                            rx="2"
                            fill="#ff8f78"
                        />
                        <text
                            x={(chip_x + 12.0).to_string()}
                            y={(chip_y + 9.0).to_string()}
                            text-anchor="middle"
                            fill="#2a0f07"
                            font-size="8"
                            font-weight="700"
                            font-family="JetBrains Mono, monospace"
                        >
                            "NEW"
                        </text>
                    </>
                }
            })}
        </g>
    };

    if on_node_click.is_some() {
        node_view.into_any()
    } else {
        let (lns, lnm) = crate::loc::use_current_location().get();
        let href = crate::loc::loc_path(&lns, &lnm, &format!("assets/{}", node.id));
        view! {
            <a href={href}>
                {node_view}
            </a>
        }
        .into_any()
    }
}

/// Simplified node rendering (LOD 1) — rect + accent bar, no text
fn render_node_simplified(node: &LayoutNode) -> AnyView {
    let accent = kind_accent(&node.kind);
    let x = node.x;
    let y = node.y;
    let w = node.width;
    let h = node.height;

    view! {
        <g class="dag-node">
            <rect
                x={x.to_string()}
                y={y.to_string()}
                width={w.to_string()}
                height={h.to_string()}
                rx="4"
                fill="#171a1d"
                stroke="rgba(70,72,75,0.15)"
                stroke-width="1"
            />
            <rect
                x={x.to_string()}
                y={y.to_string()}
                width="4"
                height={h.to_string()}
                rx="2"
                fill={accent}
            />
        </g>
    }
    .into_any()
}

/// Micro node rendering (LOD 2) — single colored rectangle
fn render_node_micro(node: &LayoutNode) -> AnyView {
    let accent = kind_accent(&node.kind);

    view! {
        <rect
            x={node.x.to_string()}
            y={node.y.to_string()}
            width={node.width.to_string()}
            height={node.height.to_string()}
            rx="4"
            fill={accent}
            opacity="0.7"
        />
    }
    .into_any()
}
