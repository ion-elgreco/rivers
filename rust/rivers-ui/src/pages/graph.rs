//! Full graph visualization page.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::dag::render::DagGraph;
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::materialize_dialog::MaterializeDialog;
use crate::components::multi_select::{MultiSelect, SelectOption};
use crate::components::ui_kit::{Crumb, DagMinimap, KindBadge, MinimapNode, Tag, Topbar};
use crate::helpers::{common_actions, partition_picker_for_assets, use_query_param_list};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::{get_asset, get_assets};
use crate::server_fns::graph::{get_graph_layout, get_graph_topology, get_node_lineage};
use crate::server_fns::overview::get_assets_info;

fn get_element_size(target: &Option<leptos::web_sys::EventTarget>) -> (f64, f64) {
    #[cfg(target_arch = "wasm32")]
    {
        use leptos::wasm_bindgen::JsCast;
        if let Some(t) = target {
            if let Ok(el) = t.clone().dyn_into::<leptos::web_sys::HtmlElement>() {
                return (el.client_width() as f64, el.client_height() as f64);
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = target;
    }
    (0.0, 0.0)
}

/// True when a click landed on empty canvas rather than on a node, an edge, or
/// the overlay chrome (zoom buttons, legend, minimap, context menu). Blank space
/// inside the graph targets the `<svg>` element itself; the zoom-control icons
/// are also `<svg>`, so the parent must be the `.dag-container`.
fn is_bare_canvas_click(ev: &leptos::ev::MouseEvent) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        use leptos::wasm_bindgen::JsCast;
        let Some(el) = ev
            .target()
            .and_then(|t| t.dyn_into::<leptos::web_sys::Element>().ok())
        else {
            return false;
        };
        if !el.tag_name().eq_ignore_ascii_case("svg") {
            return false;
        }
        return el
            .parent_element()
            .map(|p| p.class_list().contains("dag-container"))
            .unwrap_or(false);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = ev;
        false
    }
}

/// Client coordinates → coordinates relative to the viewport element, which
/// is what the absolutely-positioned context menu is placed against.
fn viewport_relative(
    viewport_ref: NodeRef<leptos::html::Div>,
    client_x: f64,
    client_y: f64,
) -> (f64, f64) {
    #[cfg(target_arch = "wasm32")]
    {
        if let Some(el) = viewport_ref.get_untracked() {
            let rect = el.get_bounding_client_rect();
            return (client_x - rect.left(), client_y - rect.top());
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = viewport_ref;
    }
    (client_x, client_y)
}

/// Selection transition for a node click. A plain click replaces the whole
/// selection; shift/ctrl/cmd toggles a single node. Click order is preserved
/// so the last entry is always the focused node.
fn apply_node_click(selected: &[String], node: &str, additive: bool) -> Vec<String> {
    if !additive {
        return vec![node.to_string()];
    }
    let mut next = selected.to_vec();
    match next.iter().position(|k| k == node) {
        Some(pos) => {
            next.remove(pos);
        }
        None => next.push(node.to_string()),
    }
    next
}

#[component]
pub fn GraphPage() -> impl IntoView {
    let (center_layers, set_center_layers) = signal(false);
    let (expanded_graphs, set_expanded_graphs) = signal(std::collections::HashSet::<String>::new());
    // SSE kicks only drive node-status refetches — the DAG layout and
    // topology come from the code-location gRPC and don't change mid-session.
    let (refresh_tick, set_refresh_tick) = signal(0u64);
    let loc = use_current_location();
    let layout = Resource::new(
        move || (center_layers.get(), loc.get()),
        |(cl, (ns, name))| async move { get_graph_layout(ns, name, Some(cl)).await },
    );
    let topology = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_graph_topology(ns, name).await },
    );
    let assets_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_assets_info(ns, name).await },
    );
    let all_assets = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_assets(ns, name, None, None, None),
    );

    let live_status = use_live_kick(
        &["lineage"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );
    // Multi-selection in click order; the last entry is the focused node.
    let (selected_nodes, set_selected_nodes) = signal(Vec::<String>::new());
    let focused_node = Signal::derive(move || selected_nodes.get().last().cloned());
    let (filter_kind, set_filter_kind) = use_query_param_list("kind");
    let (filter_group, set_filter_group) = use_query_param_list("group");

    // Memos, not derives: a derive re-walks the whole topology on every read
    // (each selection change reads these several times); a memo recomputes
    // only when the topology itself changes.
    let graph_asset_names = Memo::new(move |_| {
        topology
            .get()
            .and_then(|r| r.ok())
            .map(|t| {
                t.nodes
                    .iter()
                    .filter(|n| n.kind == "graph_asset")
                    .map(|n| n.name.clone())
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    // Task nodes can be selected for lineage tracing but never materialized.
    let task_node_names = Memo::new(move |_| {
        topology
            .get()
            .and_then(|r| r.ok())
            .map(|t| {
                t.nodes
                    .iter()
                    .filter(|n| n.kind == "task")
                    .map(|n| n.name.clone())
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    // ViewBox-based zoom/pan state.
    // vb_scale = SVG units per screen pixel (higher = more zoomed out).
    let (vb_x, set_vb_x) = signal(0.0f64);
    let (vb_y, set_vb_y) = signal(0.0f64);
    let (vb_scale, set_vb_scale) = signal(1.0f64);

    // SSR defaults — updated on client via NodeRef + event handlers.
    let (el_w, set_el_w) = signal(1200.0f64);
    let (el_h, set_el_h) = signal(800.0f64);

    let (layout_w, set_layout_w) = signal(0.0f64);
    let (layout_h, set_layout_h) = signal(0.0f64);

    let (did_auto_fit, set_did_auto_fit) = signal(false);

    let (dragging, set_dragging) = signal(false);
    let (drag_start_x, set_drag_start_x) = signal(0.0f64);
    let (drag_start_y, set_drag_start_y) = signal(0.0f64);
    let (drag_start_vb_x, set_drag_start_vb_x) = signal(0.0f64);
    let (drag_start_vb_y, set_drag_start_vb_y) = signal(0.0f64);

    let (ctx_menu, set_ctx_menu) = signal(None::<(f64, f64, String)>);

    let viewport_ref = NodeRef::<leptos::html::Div>::new();

    // Effect doesn't run on SSR, so this is client-only.
    Effect::new(move |_| {
        if let Some(el) = viewport_ref.get() {
            let w = el.client_width() as f64;
            let h = el.client_height() as f64;
            if w > 0.0 && h > 0.0 {
                set_el_w.set(w);
                set_el_h.set(h);
            }
        }
    });

    Effect::new(move |_| {
        let lw = layout_w.get();
        let lh = layout_h.get();
        let ew = el_w.get();
        let eh = el_h.get();
        if !did_auto_fit.get_untracked() && lw > 0.0 && lh > 0.0 && ew > 0.0 && eh > 0.0 {
            let scale = (lw / ew).max(lh / eh).max(1.0);
            set_vb_scale.set(scale);
            set_vb_x.set(0.0);
            set_vb_y.set(0.0);
            set_did_auto_fit.set(true);
        }
    });

    let asset_detail = Resource::new(
        move || (loc.get(), focused_node.get()),
        |((ns, name), key)| async move {
            match key {
                Some(k) => get_asset(ns, name, k).await,
                None => Ok(None),
            }
        },
    );

    let lineage = Resource::new(
        move || (focused_node.get(), loc.get()),
        |(key, (ns, name))| async move {
            match key {
                Some(k) => get_node_lineage(ns, name, k).await.ok(),
                None => None,
            }
        },
    );

    let ancestor_keys = Signal::derive(move || {
        lineage
            .get()
            .flatten()
            .map(|(anc, _)| {
                anc.into_iter()
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    let descendant_keys = Signal::derive(move || {
        lineage
            .get()
            .flatten()
            .map(|(_, desc)| {
                desc.into_iter()
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    // Materialize always routes through the dialog so the user previews the
    // asset list (and partition keys) before anything launches.
    let asset_info_by_key = Memo::new(move |_| {
        assets_info
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect::<std::collections::HashMap<String, crate::types::AssetDefinitionInfo>>()
    });
    // The dialog's row decoration, from the page's own live resource.
    let records_by_key = Memo::new(move |_| {
        all_assets
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.asset_key.clone(), r))
            .collect::<std::collections::HashMap<String, crate::types::AssetRecord>>()
    });
    let (mat_targets, set_mat_targets) = signal(Vec::<String>::new());
    let show_dialog = RwSignal::new(false);
    let dialog_verb = RwSignal::new(Option::<crate::types::AssetActionInfo>::None);
    let dialog_destructive = RwSignal::new(false);
    let materialize_picker = Signal::derive(move || {
        partition_picker_for_assets(&mat_targets.get(), &asset_info_by_key.get())
    });
    let dialog_asset_keys = Signal::derive(move || mat_targets.get());
    // One opener for both flows — `None` verb materializes. The task-node
    // filter lives here once: task nodes can be selected for lineage tracing
    // but never executed.
    type DialogVerb = Option<crate::types::AssetActionInfo>;
    let open_dialog: Callback<(Vec<String>, DialogVerb, bool)> = Callback::new(
        move |(keys, verb, destructive): (Vec<String>, DialogVerb, bool)| {
            let tasks = task_node_names.get_untracked();
            let keys: Vec<String> = keys.into_iter().filter(|k| !tasks.contains(k)).collect();
            if keys.is_empty() {
                return;
            }
            dialog_verb.set(verb);
            dialog_destructive.set(destructive);
            set_mat_targets.set(keys);
            show_dialog.set(true);
        },
    );
    let start_materialize: Callback<Vec<String>> =
        Callback::new(move |keys: Vec<String>| open_dialog.run((keys, None, false)));
    let start_action: Callback<(Vec<String>, crate::types::AssetActionInfo, bool)> =
        Callback::new(
            move |(keys, act, destructive): (Vec<String>, crate::types::AssetActionInfo, bool)| {
                open_dialog.run((keys, Some(act), destructive))
            },
        );

    let all_kinds = Signal::derive(move || {
        layout
            .get()
            .and_then(|r| r.ok())
            .map(|l| {
                let mut kinds: Vec<String> = l.nodes.iter().map(|n| n.kind.clone()).collect();
                kinds.sort();
                kinds.dedup();
                kinds
            })
            .unwrap_or_default()
    });

    let all_groups_list = Signal::derive(move || {
        layout
            .get()
            .and_then(|r| r.ok())
            .map(|l| {
                let mut groups: Vec<String> =
                    l.nodes.iter().filter_map(|n| n.group.clone()).collect();
                groups.sort();
                groups.dedup();
                groups
            })
            .unwrap_or_default()
    });

    let materialized_keys = Signal::derive(move || {
        all_assets
            .get()
            .and_then(|r| r.ok())
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.last_timestamp.is_some())
                    .map(|r| r.asset_key.clone())
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    let stale_keys = Signal::derive(move || {
        all_assets
            .get()
            .and_then(|r| r.ok())
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.stale_status == crate::types::StaleStatus::Stale)
                    .map(|r| r.asset_key.clone())
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    let external_keys = Signal::derive(move || {
        assets_info
            .get()
            .and_then(|r| r.ok())
            .map(|infos| {
                infos
                    .iter()
                    .filter(|i| i.is_external)
                    .map(|i| i.asset_key.clone())
                    .collect::<std::collections::HashSet<String>>()
            })
            .unwrap_or_default()
    });

    // Viewport bounds in SVG coordinates: (x, y, visible_width, visible_height).
    // Based on actual element size, not layout size.
    let viewport = Signal::derive(move || {
        let w = el_w.get() * vb_scale.get();
        let h = el_h.get() * vb_scale.get();
        (vb_x.get(), vb_y.get(), w, h)
    });

    let fit_to_view = move |_| {
        let lw = layout_w.get();
        let lh = layout_h.get();
        let ew = el_w.get();
        let eh = el_h.get();
        if ew > 0.0 && eh > 0.0 && lw > 0.0 && lh > 0.0 {
            let scale = (lw / ew).max(lh / eh).max(1.0);
            set_vb_scale.set(scale);
            set_vb_x.set(0.0);
            set_vb_y.set(0.0);
        }
    };

    let zoom_in = move |_| {
        let old_scale = vb_scale.get();
        let new_scale = (old_scale / 1.2).max(0.1);
        let ew = el_w.get();
        let eh = el_h.get();
        let cx = vb_x.get() + ew * old_scale / 2.0;
        let cy = vb_y.get() + eh * old_scale / 2.0;
        set_vb_x.set(cx - ew * new_scale / 2.0);
        set_vb_y.set(cy - eh * new_scale / 2.0);
        set_vb_scale.set(new_scale);
    };

    let zoom_out = move |_| {
        let old_scale = vb_scale.get();
        let new_scale = (old_scale * 1.2).min(200.0);
        let ew = el_w.get();
        let eh = el_h.get();
        let cx = vb_x.get() + ew * old_scale / 2.0;
        let cy = vb_y.get() + eh * old_scale / 2.0;
        set_vb_x.set(cx - ew * new_scale / 2.0);
        set_vb_y.set(cy - eh * new_scale / 2.0);
        set_vb_scale.set(new_scale);
    };

    view! {
        <Topbar crumbs=vec![Crumb::new("Lineage")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <Transition fallback=move || view! { <div class="loading">"Computing layout..."</div> }>

        <div class="filter-bar">
            {
                let kind_options = Signal::derive(move || {
                    all_kinds.get().into_iter().map(|k| SelectOption {
                        label: k.clone(),
                        value: k,
                        enabled: true,
                    }).collect::<Vec<_>>()
                });
                let group_options = Signal::derive(move || {
                    all_groups_list.get().into_iter().map(|g| SelectOption {
                        label: g.clone(),
                        value: g,
                        enabled: true,
                    }).collect::<Vec<_>>()
                });
                let toggle_kind_cb: Callback<String> = Callback::new({
                    let set_fk = set_filter_kind.clone();
                    move |val: String| {
                        let mut cur = filter_kind.get();
                        if cur.contains(&val) { cur.retain(|v| v != &val); } else { cur.push(val); }
                        set_fk(cur);
                    }
                });
                let toggle_group_cb: Callback<String> = Callback::new({
                    let set_fg = set_filter_group.clone();
                    move |val: String| {
                        let mut cur = filter_group.get();
                        if cur.contains(&val) { cur.retain(|v| v != &val); } else { cur.push(val); }
                        set_fg(cur);
                    }
                });
                view! {
                    <MultiSelect
                        options=kind_options
                        selected=filter_kind
                        on_toggle=toggle_kind_cb
                        placeholder="Kinds"
                    />
                    <MultiSelect
                        options=group_options
                        selected=filter_group
                        on_toggle=toggle_group_cb
                        placeholder="Groups"
                    />
                }
            }
        </div>

        <div class="split-panel">
            <div class="split-panel-main dag-viewport"
                node_ref=viewport_ref
                on:contextmenu=move |ev| {
                    ev.prevent_default();
                    set_ctx_menu.set(None);
                }
                on:wheel=move |ev| {
                    ev.prevent_default();
                    let delta = ev.delta_y();
                    let factor = if delta > 0.0 { 1.1 } else { 0.9 };
                    let old_scale = vb_scale.get();
                    let new_scale = (old_scale * factor).clamp(0.1, 200.0);

                    let ox = ev.offset_x() as f64;
                    let oy = ev.offset_y() as f64;

                    let (elw, elh) = get_element_size(&ev.current_target());
                    if elw > 0.0 && elh > 0.0 {
                        set_el_w.set(elw);
                        set_el_h.set(elh);

                        let frac_x = ox / elw;
                        let frac_y = oy / elh;
                        // SVG point under the cursor before/after zoom must be
                        // at the same screen fraction.
                        let svg_x = vb_x.get() + frac_x * elw * old_scale;
                        let svg_y = vb_y.get() + frac_y * elh * old_scale;
                        set_vb_x.set(svg_x - frac_x * elw * new_scale);
                        set_vb_y.set(svg_y - frac_y * elh * new_scale);
                    }
                    set_vb_scale.set(new_scale);
                }
                on:mousedown=move |ev| {
                    if ev.button() == 0 {
                        set_dragging.set(true);
                        set_drag_start_x.set(ev.client_x() as f64);
                        set_drag_start_y.set(ev.client_y() as f64);
                        set_drag_start_vb_x.set(vb_x.get());
                        set_drag_start_vb_y.set(vb_y.get());
                    }
                }
                on:mousemove=move |ev| {
                    if dragging.get() {
                        let dx = ev.client_x() as f64 - drag_start_x.get();
                        let dy = ev.client_y() as f64 - drag_start_y.get();
                        let scale = vb_scale.get();
                        set_vb_x.set(drag_start_vb_x.get() - dx * scale);
                        set_vb_y.set(drag_start_vb_y.get() - dy * scale);
                    }
                }
                on:mouseup=move |_| set_dragging.set(false)
                on:mouseleave=move |_| set_dragging.set(false)
                on:click=move |ev| {
                    if is_bare_canvas_click(&ev)
                        && !(ev.shift_key() || ev.ctrl_key() || ev.meta_key())
                    {
                        set_selected_nodes.set(Vec::new());
                        set_ctx_menu.set(None);
                    }
                }
            >
                    {move || {
                        layout.get().map(|result| match result {
                            Ok(mut layout_result) => {
                                let fk = filter_kind.get();
                                let fg = filter_group.get();

                                // Collapse: hide children of non-expanded graph assets
                                let exp = expanded_graphs.get();
                                {
                                    let mut remap = std::collections::HashMap::<String, String>::new();
                                    for node in &layout_result.nodes {
                                        if let Some(ref pg) = node.parent_graph
                                            && !exp.contains(pg) {
                                                remap.insert(node.id.clone(), pg.clone());
                                            }
                                    }
                                    if !remap.is_empty() {
                                        let hidden: std::collections::HashSet<&str> =
                                            remap.keys().map(|s| s.as_str()).collect();
                                        layout_result.nodes.retain(|n| !hidden.contains(n.id.as_str()));

                                        let visible: std::collections::HashSet<String> =
                                            layout_result.nodes.iter().map(|n| n.id.clone()).collect();
                                        let mut seen = std::collections::HashSet::new();
                                        layout_result.edges = layout_result.edges.into_iter().filter_map(|mut e| {
                                            if let Some(pg) = remap.get(&e.source) {
                                                e.source = pg.clone();
                                                e.waypoints.clear();
                                            }
                                            if let Some(pg) = remap.get(&e.target) {
                                                e.target = pg.clone();
                                                e.waypoints.clear();
                                            }
                                            if e.source == e.target { return None; }
                                            if !visible.contains(&e.source) || !visible.contains(&e.target) { return None; }
                                            let key = (e.source.clone(), e.target.clone());
                                            if seen.insert(key) { Some(e) } else { None }
                                        }).collect();
                                    }
                                }

                                if !fk.is_empty() {
                                    layout_result.nodes.retain(|n| fk.contains(&n.kind));
                                    let node_ids: std::collections::HashSet<String> = layout_result.nodes.iter().map(|n| n.id.clone()).collect();
                                    layout_result.edges.retain(|e| node_ids.contains(&e.source) && node_ids.contains(&e.target));
                                }
                                if !fg.is_empty() {
                                    layout_result.nodes.retain(|n| n.group.as_ref().map(|g| fg.contains(g)).unwrap_or(false));
                                    let node_ids: std::collections::HashSet<String> = layout_result.nodes.iter().map(|n| n.id.clone()).collect();
                                    layout_result.edges.retain(|e| node_ids.contains(&e.source) && node_ids.contains(&e.target));
                                }

                                set_layout_w.set(layout_result.width.max(400.0));
                                set_layout_h.set(layout_result.height.max(300.0));

                                let mat_keys = materialized_keys.get();
                                let stl_keys = stale_keys.get();
                                let ext_keys = external_keys.get();
                                let anc_keys = ancestor_keys.get();
                                let desc_keys = descendant_keys.get();
                                let sel_set: std::collections::HashSet<String> =
                                    selected_nodes.get().into_iter().collect();
                                let focus_node = focused_node.get().unwrap_or_default();

                                let ga_names = graph_asset_names.get();
                                let exp_graphs = expanded_graphs.get();

                                // Snapshot for the minimap before DagGraph consumes layout_result.
                                let mini_nodes: Vec<MinimapNode> = layout_result.nodes.iter().map(|n| {
                                    let status = if ext_keys.contains(&n.id) { "external" }
                                        else if stl_keys.contains(&n.id) { "stale" }
                                        else if mat_keys.contains(&n.id) { "success" }
                                        else { "missing" }.to_string();
                                    MinimapNode {
                                        id: n.id.clone(),
                                        x: n.x,
                                        y: n.y,
                                        width: n.width,
                                        height: n.height,
                                        status,
                                    }
                                }).collect();
                                let mini_edges: Vec<(String, String)> = layout_result.edges.iter()
                                    .map(|e| (e.source.clone(), e.target.clone()))
                                    .collect();
                                let anc_for_mini = anc_keys.clone();
                                let desc_for_mini = desc_keys.clone();
                                let sel_for_mini = focus_node.clone();
                                view! {
                                    <DagGraph
                                        layout=layout_result
                                        on_node_click=Callback::new(move |(name, additive): (String, bool)| {
                                            let ga = graph_asset_names.get_untracked();
                                            if !additive && ga.contains(&name) {
                                                set_expanded_graphs.update(|set| {
                                                    if !set.remove(&name) {
                                                        set.insert(name.clone());
                                                    }
                                                });
                                            }
                                            set_selected_nodes
                                                .update(|sel| *sel = apply_node_click(sel, &name, additive));
                                            set_ctx_menu.set(None);
                                        })
                                        on_node_context=Callback::new(move |(name, cx, cy): (String, f64, f64)| {
                                            let (x, y) = viewport_relative(viewport_ref, cx, cy);
                                            set_ctx_menu.set(Some((x, y, name)));
                                        })
                                        materialized_keys=mat_keys
                                        stale_keys=stl_keys
                                        external_keys=ext_keys
                                        ancestor_keys=anc_keys
                                        descendant_keys=desc_keys
                                        selected_nodes=sel_set
                                        focused_node=focus_node
                                        viewport=viewport
                                        graph_asset_names=ga_names
                                        expanded_graphs=exp_graphs
                                    />
                                    <DagMinimap
                                        nodes=mini_nodes
                                        edges=mini_edges
                                        viewport=viewport
                                        selected=sel_for_mini
                                        ancestors=anc_for_mini
                                        descendants=desc_for_mini
                                        on_pan=Callback::new(move |(x, y): (f64, f64)| {
                                            set_vb_x.set(x);
                                            set_vb_y.set(y);
                                        })
                                    />
                                }.into_any()
                            }
                            Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                        })
                    }}

                <div class="dag-legend">
                    <div class="dag-legend-hint">"click to select · shift-click to add · right-click for actions"</div>
                    <div class="dag-legend-row">
                        <span class="dag-legend-item"><span class="dag-legend-dot dag-legend-dot--success"></span>"materialized"</span>
                        <span class="dag-legend-item"><span class="dag-legend-dot dag-legend-dot--running"></span>"running"</span>
                        <span class="dag-legend-item"><span class="dag-legend-dot dag-legend-dot--stale"></span>"stale"</span>
                        <span class="dag-legend-item"><span class="dag-legend-dot dag-legend-dot--external"></span>"external"</span>
                    </div>
                </div>

                {move || {
                    ctx_menu.get().map(|(x, y, node_name)| {
                        let name_for_mat = node_name.clone();
                        let name_for_ctx_label = node_name.clone();
                        let name_for_detail = node_name.clone();
                        view! {
                            <div class="context-menu" style=format!("left: {x}px; top: {y}px")>
                                <button class="context-menu-item" on:click=move |_| {
                                    // Right-clicking inside a multi-selection acts on the
                                    // whole selection; outside it, on just that node.
                                    let sel = selected_nodes.get_untracked();
                                    let targets = if sel.contains(&name_for_mat) {
                                        sel
                                    } else {
                                        vec![name_for_mat.clone()]
                                    };
                                    start_materialize.run(targets);
                                    set_ctx_menu.set(None);
                                }>{move || {
                                    let sel = selected_nodes.get();
                                    if sel.len() > 1 && sel.contains(&name_for_ctx_label) {
                                        format!("Materialize {} assets…", sel.len())
                                    } else {
                                        "Materialize…".to_string()
                                    }
                                }}</button>
                                <button class="context-menu-item" on:click=move |_| {
                                    set_selected_nodes.set(vec![name_for_detail.clone()]);
                                    set_ctx_menu.set(None);
                                }>"View Details"</button>
                            </div>
                        }
                    })
                }}

                <div class="dag-zoom-controls">
                    <button
                        class="btn btn-tertiary dag-zoom-btn"
                        on:click=zoom_in
                        title="Zoom in"
                    >
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                            <path d="M6 2v8M2 6h8" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>
                        </svg>
                    </button>
                    <button
                        class="btn btn-tertiary dag-zoom-btn"
                        on:click=zoom_out
                        title="Zoom out"
                    >
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                            <path d="M2 6h8" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>
                        </svg>
                    </button>
                    <button
                        class="btn btn-tertiary dag-zoom-text-btn"
                        on:click=fit_to_view
                        title="Fit to view"
                    >"Fit"</button>
                    <div class="dag-zoom-divider"></div>
                    <button
                        class=move || if center_layers.get() {
                            "btn btn-tertiary dag-zoom-text-btn dag-zoom-text-btn--active"
                        } else {
                            "btn btn-tertiary dag-zoom-text-btn"
                        }
                        on:click=move |_| set_center_layers.update(|v| *v = !*v)
                        title="Center layers vertically"
                    >"center"</button>
                </div>
            </div>

            {move || {
                let sel = selected_nodes.get();
                if sel.is_empty() {
                    return None;
                }

                // More than one node selected → summarize the selection instead of
                // one asset's detail, so it's obvious what a materialize would cover.
                if sel.len() > 1 {
                    let tasks = task_node_names.get();
                    let mat_keys: Vec<String> =
                        sel.iter().filter(|k| !tasks.contains(*k)).cloned().collect();
                    let n_mat = mat_keys.len();
                    let n_skipped = sel.len() - n_mat;
                    let sel_for_list = sel.clone();
                    return Some(view! {
                        <div class="dag-selected-sidebar">
                            <div class="dag-sidebar-header">
                                <div class="dag-sidebar-status dag-sidebar-status--healthy">
                                    <span class="dag-sidebar-status-dot"></span>
                                    <span>{format!("{} SELECTED", sel.len())}</span>
                                </div>
                                <button
                                    class="dag-sidebar-close"
                                    on:click=move |_| set_selected_nodes.set(Vec::new())
                                    title="Clear selection"
                                >"×"</button>
                            </div>

                            <div class="dag-sidebar-section">
                                <div class="section-header-label">"SELECTION"</div>
                                <div class="dag-sidebar-links">
                                    {sel_for_list.into_iter().map(|k| {
                                        let k_for_focus = k.clone();
                                        let k_for_drop = k.clone();
                                        view! {
                                            <div class="dag-sidebar-sel-row">
                                                <button
                                                    class="dag-sidebar-link"
                                                    title="Show details"
                                                    on:click=move |_| set_selected_nodes.set(vec![k_for_focus.clone()])
                                                >{k}</button>
                                                <button
                                                    class="dag-sidebar-sel-drop"
                                                    title="Remove from selection"
                                                    on:click=move |_| set_selected_nodes.update(|s| {
                                                        *s = apply_node_click(s, &k_for_drop, true);
                                                    })
                                                >"×"</button>
                                            </div>
                                        }
                                    }).collect::<Vec<_>>()}
                                </div>
                            </div>

                            {(n_skipped > 0).then(|| view! {
                                <div class="dag-sidebar-empty">
                                    {format!("{n_skipped} task node(s) can't be materialized directly")}
                                </div>
                            })}

                            <div class="dag-sidebar-footer">
                                <button
                                    class="btn btn-primary dag-sidebar-action"
                                    disabled=n_mat == 0
                                    on:click={
                                        let keys = mat_keys.clone();
                                        move |_| { start_materialize.run(keys.clone()); }
                                    }
                                >
                                    <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                                        <path d="M3 2l7 4-7 4V2z"/>
                                    </svg>
                                    {format!("Materialize {n_mat} assets…")}
                                </button>
                                {common_actions(&mat_keys, &asset_info_by_key.get()).into_iter().map(|act| {
                                    let destructive = act.is_destructive();
                                    let label = act.name.clone();
                                    let keys = mat_keys.clone();
                                    let title = crate::helpers::action_title(&act, true);
                                    view! {
                                        <button
                                            class=if destructive { "btn btn-danger dag-sidebar-action" } else { "btn dag-sidebar-action" }
                                            title=title
                                            on:click=move |_| { start_action.run((keys.clone(), act.clone(), destructive)); }
                                        >{label}</button>
                                    }
                                }).collect::<Vec<_>>()}
                            </div>
                        </div>
                    }.into_any());
                }

                let node_name = sel[0].clone();
                {
                    let topo = topology.get().and_then(|r| r.ok());
                    let (upstream, downstream): (Vec<String>, Vec<String>) = topo
                        .map(|t| (t.direct_upstream(&node_name), t.direct_downstream(&node_name)))
                        .unwrap_or_default();

                    Some(view! {
                        <div class="dag-selected-sidebar">
                            <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
                                {move || {
                                    asset_detail.get().map(|result| match result {
                                        Ok(Some(record)) => {
                                            let (lns, lnm) = loc.get();
                                            let href = loc_path(&lns, &lnm, &format!("assets/{}", record.asset_key));
                                            let mat_key = record.asset_key.clone();
                                            let is_task = task_node_names.get().contains(&record.asset_key);
                                            let (status_word, status_cls) = match record.stale_status {
                                                crate::types::StaleStatus::UpToDate => ("UP-TO-DATE", "dag-sidebar-status--healthy"),
                                                crate::types::StaleStatus::Stale => ("STALE", "dag-sidebar-status--stale"),
                                                crate::types::StaleStatus::Missing => ("MISSING", "dag-sidebar-status--pending"),
                                            };
                                            // The relative-time piece is emitted as a reactive
                                            // `<RelTimeOpt>` so it ticks live.
                                            let static_parts: Vec<String> = [
                                                record.asset_group.clone(),
                                                record.kinds.first().cloned(),
                                            ].into_iter().flatten().filter(|s| !s.is_empty()).collect();
                                            let last_ts: Option<i64> = record.last_timestamp;
                                            let kind_for_badge = record.kinds.first().cloned().unwrap_or_default();
                                            let tags = record.tags.clone();
                                            let ups = upstream.clone();
                                            let downs = downstream.clone();
                                            let node_verbs = common_actions(
                                                std::slice::from_ref(&record.asset_key),
                                                &asset_info_by_key.get(),
                                            );
                                            let key_for_actions = record.asset_key.clone();
                                            view! {
                                                <div class="dag-sidebar-header">
                                                    <div class=format!("dag-sidebar-status {status_cls}")>
                                                        <span class="dag-sidebar-status-dot"></span>
                                                        <span>{status_word}</span>
                                                    </div>
                                                    <button
                                                        class="dag-sidebar-close"
                                                        on:click=move |_| set_selected_nodes.set(Vec::new())
                                                        title="Close"
                                                    >"×"</button>
                                                </div>

                                                <div>
                                                    <div class="dag-sidebar-title">{record.asset_key.clone()}</div>
                                                    <div class="dag-sidebar-subtitle">
                                                        {static_parts.iter().enumerate().map(|(i, s)| {
                                                            let prefix = if i > 0 { " · " } else { "" };
                                                            view! { <>{prefix}{s.clone()}</> }
                                                        }).collect::<Vec<_>>()}
                                                        {(!static_parts.is_empty()).then_some(view! { " · " })}
                                                        <crate::now::RelTimeOpt ts=last_ts fallback="never materialized"/>
                                                    </div>
                                                </div>

                                                <div class="dag-sidebar-chips">
                                                    {(!kind_for_badge.is_empty()).then(|| view! { <KindBadge kind=kind_for_badge/> })}
                                                    {tags.into_iter().map(|t| view! { <Tag label=t/> }).collect::<Vec<_>>()}
                                                </div>

                                                <div class="dag-sidebar-section">
                                                    <div class="section-header-label">{format!("UPSTREAM · {}", ups.len())}</div>
                                                    {if ups.is_empty() {
                                                        view! { <span class="dag-sidebar-empty">"—"</span> }.into_any()
                                                    } else {
                                                        view! {
                                                            <div class="dag-sidebar-links">
                                                                {ups.into_iter().map(|u| {
                                                                    let u_for_click = u.clone();
                                                                    view! {
                                                                        <button
                                                                            class="dag-sidebar-link"
                                                                            on:click=move |_| set_selected_nodes.set(vec![u_for_click.clone()])
                                                                        >
                                                                            <span class="dag-sidebar-link-arrow">"↑"</span>{u}
                                                                        </button>
                                                                    }
                                                                }).collect::<Vec<_>>()}
                                                            </div>
                                                        }.into_any()
                                                    }}
                                                </div>

                                                <div class="dag-sidebar-section">
                                                    <div class="section-header-label">{format!("DOWNSTREAM · {}", downs.len())}</div>
                                                    {if downs.is_empty() {
                                                        view! { <span class="dag-sidebar-empty">"—"</span> }.into_any()
                                                    } else {
                                                        view! {
                                                            <div class="dag-sidebar-links">
                                                                {downs.into_iter().map(|d| {
                                                                    let d_for_click = d.clone();
                                                                    view! {
                                                                        <button
                                                                            class="dag-sidebar-link"
                                                                            on:click=move |_| set_selected_nodes.set(vec![d_for_click.clone()])
                                                                        >
                                                                            <span class="dag-sidebar-link-arrow">"↓"</span>{d}
                                                                        </button>
                                                                    }
                                                                }).collect::<Vec<_>>()}
                                                            </div>
                                                        }.into_any()
                                                    }}
                                                </div>

                                                <div class="dag-sidebar-footer">
                                                    <A href={href} attr:class="btn btn-tertiary dag-sidebar-action">"Details"</A>
                                                    <button
                                                        class="btn btn-primary dag-sidebar-action"
                                                        disabled=is_task
                                                        title=if is_task { "Task nodes materialize with their parent graph asset" } else { "Preview and materialize this asset" }
                                                        on:click=move |_| { start_materialize.run(vec![mat_key.clone()]); }
                                                    >
                                                        <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                                                            <path d="M3 2l7 4-7 4V2z"/>
                                                        </svg>
                                                        "Materialize…"
                                                    </button>
                                                    {node_verbs.into_iter().map(|act| {
                                                        let destructive = act.is_destructive();
                                                        let label = act.name.clone();
                                                        let k = key_for_actions.clone();
                                                        let title = crate::helpers::action_title(&act, false);
                                                        view! {
                                                            <button
                                                                class=if destructive { "btn btn-danger dag-sidebar-action" } else { "btn dag-sidebar-action" }
                                                                title=title
                                                                on:click=move |_| { start_action.run((vec![k.clone()], act.clone(), destructive)); }
                                                            >{label}</button>
                                                        }
                                                    }).collect::<Vec<_>>()}
                                                </div>
                                            }.into_any()
                                        }
                                        Ok(None) => view! { <p class="dag-sidebar-empty">"No data available."</p> }.into_any(),
                                        Err(e) => view! { <div class="error-msg">{format!("{e}")}</div> }.into_any(),
                                    })
                                }}
                            </Transition>
                        </div>
                    }.into_any())
                }
            }}
        </div>
        </Transition>

        <MaterializeDialog
            show=show_dialog
            asset_keys=dialog_asset_keys
            picker=materialize_picker
            action=dialog_verb
            destructive=dialog_destructive
            records=records_by_key
        />
    }
}

#[cfg(test)]
mod tests {
    use super::apply_node_click;

    fn keys(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn plain_click_replaces_selection() {
        assert_eq!(
            apply_node_click(&keys(&["a", "b"]), "c", false),
            keys(&["c"])
        );
    }

    #[test]
    fn plain_click_on_selected_node_keeps_it_selected() {
        assert_eq!(
            apply_node_click(&keys(&["a", "b"]), "a", false),
            keys(&["a"])
        );
    }

    #[test]
    fn additive_click_appends_in_click_order() {
        assert_eq!(
            apply_node_click(&keys(&["a"]), "b", true),
            keys(&["a", "b"])
        );
    }

    #[test]
    fn additive_click_toggles_off_an_already_selected_node() {
        assert_eq!(
            apply_node_click(&keys(&["a", "b", "c"]), "b", true),
            keys(&["a", "c"])
        );
    }

    #[test]
    fn additive_click_on_empty_selection_selects_one() {
        assert_eq!(apply_node_click(&[], "a", true), keys(&["a"]));
    }

    #[test]
    fn last_entry_is_the_focused_node() {
        // The sidebar/lineage read `selected.last()`, so re-adding a node has to
        // move focus to it rather than leaving focus on a stale entry.
        let sel = apply_node_click(&keys(&["a", "b"]), "b", true);
        assert_eq!(sel.last().map(String::as_str), Some("a"));
        let sel = apply_node_click(&sel, "b", true);
        assert_eq!(sel.last().map(String::as_str), Some("b"));
    }
}
