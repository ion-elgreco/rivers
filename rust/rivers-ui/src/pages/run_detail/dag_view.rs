use leptos::prelude::*;

use super::timeline::StepStatus;

#[component]
pub(super) fn RunDagView(
    layout: crate::components::dag::layout::LayoutResult,
    status_by_asset: std::collections::HashMap<String, StepStatus>,
    duration_by_asset: std::collections::HashMap<String, String>,
    selected_step: ReadSignal<Option<String>>,
    on_select: WriteSignal<Option<String>>,
) -> impl IntoView {
    use crate::components::dag::layout::LayoutNode;

    // Nodes render ~72px tall (fits 2-line names + header + duration), but our layout
    // was computed with 48-tall node boxes + 16 gap. Scale y positions so we preserve
    // the same visual gap between rendered nodes and avoid overlap.
    const RENDER_H: f64 = 72.0;
    const LAYOUT_H: f64 = 48.0;
    const LAYOUT_GAP: f64 = 16.0;
    let y_scale: f64 = (RENDER_H + LAYOUT_GAP) / (LAYOUT_H + LAYOUT_GAP);

    let width = layout.width.max(400.0);
    let height = (layout.height * y_scale).max(200.0) + 32.0;
    let node_by_id: std::collections::HashMap<String, LayoutNode> = layout
        .nodes
        .iter()
        .map(|n| (n.id.clone(), n.clone()))
        .collect();

    let edge_views: Vec<_> = layout
        .edges
        .iter()
        .filter_map(|e| {
            let src = node_by_id.get(&e.source)?;
            let tgt = node_by_id.get(&e.target)?;
            let x1 = src.x + src.width;
            let y1 = src.y * y_scale + RENDER_H / 2.0;
            let x2 = tgt.x;
            let y2 = tgt.y * y_scale + RENDER_H / 2.0;
            let dx = (x2 - x1).abs();
            let cpx1 = x1 + dx * 0.55;
            let cpx2 = x2 - dx * 0.55;
            let d = format!("M{x1},{y1} C{cpx1},{y1} {cpx2},{y2} {x2},{y2}");
            let running = matches!(status_by_asset.get(&e.source), Some(StepStatus::Running))
                || matches!(status_by_asset.get(&e.target), Some(StepStatus::Running));
            Some((d, running))
        })
        .collect();

    let node_views: Vec<_> = layout
        .nodes
        .iter()
        .map(|n| {
            let status = status_by_asset.get(&n.id).cloned();
            let rail_color = match &status {
                Some(StepStatus::Success) => "var(--success)",
                Some(StepStatus::Running) => "var(--secondary)",
                Some(StepStatus::Failure) => "var(--error)",
                None => "var(--text-muted)",
            };
            let rail_glow = matches!(status, Some(StepStatus::Running));
            let pos_style = format!(
                "left:{}px; top:{}px; width:{}px",
                n.x, n.y * y_scale, n.width
            );
            let rail_style = if rail_glow {
                format!("background:{rail_color}; box-shadow:0 0 8px {rail_color}")
            } else {
                format!("background:{rail_color}")
            };
            // Prefer the asset group as the small header label (Rivers shows `task.type`);
            // fall back to `kind` if no group is set. Skip the label entirely when kind is
            // the generic "asset" — nothing informative to show.
            let header_label: Option<String> = n
                .group
                .clone()
                .or_else(|| {
                    if n.kind.is_empty() || n.kind.eq_ignore_ascii_case("asset") {
                        None
                    } else {
                        Some(n.kind.clone())
                    }
                });
            let dur = duration_by_asset.get(&n.id).cloned().unwrap_or_default();
            let full_key = n.id.clone();
            let is_running = matches!(status, Some(StepStatus::Running));
            let key_for_click = full_key.clone();
            let key_for_match = full_key.clone();
            let is_selected = Signal::derive(move || {
                selected_step.get().as_ref() == Some(&key_for_match)
            });
            let node_cls = move || if is_selected.get() {
                "run-dag-node run-dag-node--selected"
            } else {
                "run-dag-node"
            };
            view! {
                <div
                    class=node_cls
                    style=pos_style
                    on:click=move |_| {
                        // Toggle: clicking the already-selected node clears the filter.
                        let already = selected_step.get_untracked().as_ref() == Some(&key_for_click);
                        if already {
                            on_select.set(None);
                        } else {
                            on_select.set(Some(key_for_click.clone()));
                        }
                    }
                >
                    <div class="run-dag-node-rail" style=rail_style></div>
                    <div class="run-dag-node-header">
                        {header_label.map(|l| view! { <span class="run-dag-node-type">{l}</span> })}
                        {is_running.then(|| view! { <span class="run-dag-node-running">"● running"</span> })}
                    </div>
                    <div class="run-dag-node-name" title=full_key.clone()>{full_key.clone()}</div>
                    <div class="run-dag-node-footer">
                        <span class="run-dag-node-dur">{dur}</span>
                        {is_running.then(|| view! {
                            <div class="run-dag-node-progress">
                                <div class="run-dag-node-progress-fill"></div>
                            </div>
                        })}
                    </div>
                </div>
            }
        })
        .collect();

    view! {
        <div class="run-dag-canvas" style=format!("width:{}px; height:{}px", width, height)>
            <svg class="run-dag-edges" width=width height=height>
                <defs>
                    <marker id="run-dag-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                        <path d="M0,0 L10,5 L0,10" fill="var(--outline-variant)" opacity="0.6"/>
                    </marker>
                    <marker id="run-dag-arrow-flow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                        <path d="M0,0 L10,5 L0,10" fill="var(--secondary)"/>
                    </marker>
                </defs>
                {edge_views.into_iter().map(|(d, running)| {
                    let stroke = if running { "var(--secondary-dim)" } else { "var(--outline-variant)" };
                    let marker = if running { "url(#run-dag-arrow-flow)" } else { "url(#run-dag-arrow)" };
                    let flow_view = running.then(|| view! {
                        <path d=d.clone() stroke="var(--secondary)" stroke-width="1.4" fill="none" stroke-dasharray="3 8" opacity="0.9" class="run-dag-edge-flow"/>
                    });
                    view! {
                        <g>
                            <path d=d stroke=stroke stroke-width="1.2" fill="none" opacity="0.65" marker-end=marker/>
                            {flow_view}
                        </g>
                    }
                }).collect::<Vec<_>>()}
            </svg>
            {node_views}
        </div>
    }
}
