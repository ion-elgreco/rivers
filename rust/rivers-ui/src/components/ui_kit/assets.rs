use leptos::prelude::*;
use leptos::web_sys;
use leptos_router::components::A;

use super::badges::KindBadge;

/// Asset-chip stack used on runs/jobs/backfills rows. Shows up to `max` chips
/// with colored dots; overflow becomes `+N`.
///
/// Chips are rendered as `<span>` rather than `<a>` because `AssetStack` lives
/// inside an outer `<a class="grid-row">` in every caller, and HTML5 forbids
/// nested anchors. The browser's parser silently hoists nested `<a>` children
/// out of the outer anchor, producing a DOM that does not match Leptos's view
/// tree — the hydrator then lands on an empty `<span class="asset-stack">` and
/// panics with an "expected text node" error. Clicks still navigate via an
/// on:click handler that stops propagation so the outer row link doesn't fire.
#[component]
pub fn AssetStack(
    #[prop(into)] assets: Vec<String>,
    #[prop(optional, default = 2)] max: usize,
    #[prop(optional, into)] overflow_href: Option<String>,
) -> impl IntoView {
    let navigate = leptos_router::hooks::use_navigate();
    let n = assets.len();
    let shown: Vec<_> = assets.iter().take(max).cloned().collect();
    let extra = n.saturating_sub(max);
    let (lns, lnm) = crate::loc::use_current_location().get_untracked();
    let chips = {
        let navigate = navigate.clone();
        shown
            .into_iter()
            .map(move |k| {
                let href = crate::loc::loc_path(&lns, &lnm, &format!("assets/{}", k));
                let title = k.clone();
                let nav = navigate.clone();
                view! {
                    <span
                        class="asset-stack-chip"
                        title=title
                        role="link"
                        tabindex="0"
                        on:click=move |ev: leptos::ev::MouseEvent| {
                            ev.prevent_default();
                            ev.stop_propagation();
                            nav(&href, Default::default());
                        }
                    >
                        <span class="asset-stack-chip-dot"></span>
                        {k}
                    </span>
                }
            })
            .collect::<Vec<_>>()
    };
    // Overflow tooltip lists all assets beyond the shown window
    let tip = if extra > 0 {
        Some(
            assets
                .iter()
                .skip(max)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        )
    } else {
        None
    };
    let overflow = (extra > 0).then(|| {
        let label = format!("+{extra}");
        let t = tip.unwrap_or_default();
        let nav = navigate.clone();
        match overflow_href {
            Some(href) => view! {
                <span
                    class="asset-stack-more"
                    data-tip=t
                    role="link"
                    tabindex="0"
                    on:click=move |ev: leptos::ev::MouseEvent| {
                        ev.prevent_default();
                        ev.stop_propagation();
                        nav(&href, Default::default());
                    }
                >{label}</span>
            }
            .into_any(),
            None => view! { <span class="asset-stack-more" data-tip=t>{label}</span> }.into_any(),
        }
    });
    view! {
        <span class="asset-stack">
            {chips}
            {overflow}
        </span>
    }
}

/// Stacked row in an asset-summary list: mono key + optional kind badge +
/// materialization label, with a colored left rail. Shared by backfill-detail
/// and job-detail's asset-selection sections.
#[component]
pub fn AssetSummaryRow(
    #[prop(into)] asset_key: String,
    asset: Option<crate::types::AssetRecord>,
) -> impl IntoView {
    let (rail_color, kind_opt, last_ts, missing) = match asset {
        Some(a) => {
            let color = match a.stale_status {
                crate::types::StaleStatus::UpToDate => "var(--success)",
                crate::types::StaleStatus::Stale => "var(--warning)",
                crate::types::StaleStatus::Missing => "var(--text-muted)",
            };
            let kind = a.kinds.into_iter().next();
            (color, kind, a.last_timestamp, false)
        }
        None => ("var(--text-muted)", None, None, true),
    };
    let (lns, lnm) = crate::loc::use_current_location().get_untracked();
    let href = crate::loc::loc_path(&lns, &lnm, &format!("assets/{}", asset_key));
    let style = format!("border-left-color: {}", rail_color);
    view! {
        <A href={href} attr:class="asset-summary-row" attr:style=style>
            <span class="asset-summary-key">{asset_key}</span>
            {kind_opt.map(|k| view! { <KindBadge kind=k/> })}
            <span class="asset-summary-mat">
                {if missing {
                    view! { "—" }.into_any()
                } else {
                    view! { <crate::now::RelTimeOpt ts=last_ts/> }.into_any()
                }}
            </span>
        </A>
    }
}

#[derive(Clone, Debug)]
pub struct MinimapNode {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    /// Rivers-style status color: "success" | "running" | "failed" | "stale" | "missing" | "external".
    pub status: String,
}

/// DAG overview mini-graph + current viewport indicator. Absolutely positioned
/// bottom-left of the parent container. Click on the map to center the viewport
/// on that layout-coordinate position (if `on_pan` is provided).
#[component]
pub fn DagMinimap(
    #[prop(into)] nodes: Vec<MinimapNode>,
    #[prop(into)] edges: Vec<(String, String)>,
    /// Current viewport in layout coordinates: (x, y, w, h).
    #[prop(into)]
    viewport: Signal<(f64, f64, f64, f64)>,
    /// Selected node id (empty = none); highlights with primary color.
    #[prop(optional, into, default = String::new())]
    selected: String,
    #[prop(optional)] ancestors: std::collections::HashSet<String>,
    #[prop(optional)] descendants: std::collections::HashSet<String>,
    /// Callback fired when the user clicks a point in the map. Receives the
    /// top-left viewport coordinate in layout space (i.e. pass this to
    /// `set_vb_x` / `set_vb_y`).
    #[prop(optional)]
    on_pan: Option<Callback<(f64, f64)>>,
) -> impl IntoView {
    if nodes.is_empty() {
        return view! { <></> }.into_any();
    }
    const MAP_W: f64 = 220.0;
    const MAP_H: f64 = 120.0;

    // Compute bounding box from nodes
    let (min_x, min_y, max_x, max_y) = nodes.iter().fold(
        (
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ),
        |(mx, my, xx, yy), n| {
            (
                mx.min(n.x),
                my.min(n.y),
                xx.max(n.x + n.width),
                yy.max(n.y + n.height),
            )
        },
    );
    let bbox_w = (max_x - min_x).max(1.0);
    let bbox_h = (max_y - min_y).max(1.0);
    let scale = (MAP_W / bbox_w).min(MAP_H / bbox_h);

    let xf = move |x: f64, y: f64| ((x - min_x) * scale, (y - min_y) * scale);

    let has_selection = !selected.is_empty();
    let sel_id = selected.clone();

    let in_lineage =
        |id: &str| -> bool { id == sel_id || ancestors.contains(id) || descendants.contains(id) };

    let node_rects: Vec<_> = nodes
        .iter()
        .map(|n| {
            let (x, y) = xf(n.x, n.y);
            let w = (n.width * scale).max(2.0);
            let h = (n.height * scale).max(1.5);
            let in_l = in_lineage(&n.id);
            let color = if has_selection && in_l {
                "var(--accent)"
            } else {
                match n.status.as_str() {
                    "running" => "var(--secondary)",
                    "failed" => "var(--error)",
                    "stale" => "var(--warning)",
                    "missing" | "external" => "var(--text-muted)",
                    _ => "var(--success)",
                }
            };
            let opacity = if has_selection && !in_l {
                "0.3"
            } else {
                "0.85"
            };
            view! {
                <rect x=x y=y width=w height=h rx="1" fill=color opacity=opacity/>
            }
        })
        .collect();

    let edge_lines: Vec<_> = edges
        .iter()
        .filter_map(|(a, b)| {
            let na = nodes.iter().find(|n| n.id == *a)?;
            let nb = nodes.iter().find(|n| n.id == *b)?;
            let (x1, y1) = xf(na.x + na.width, na.y + na.height / 2.0);
            let (x2, y2) = xf(nb.x, nb.y + nb.height / 2.0);
            Some(view! {
                <line x1=x1 y1=y1 x2=x2 y2=y2 stroke="var(--bg-highest)" stroke-width="0.6"/>
            })
        })
        .collect();

    let vp_rect = move || {
        let (vx, vy, vw, vh) = viewport.get();
        let (x, y) = xf(vx, vy);
        let w = vw * scale;
        let h = vh * scale;
        view! {
            <rect
                class="dag-minimap-viewport"
                x=x
                y=y
                width=w.max(4.0)
                height=h.max(4.0)
                rx="2"
            />
        }
    };

    let on_click_handler = move |ev: leptos::ev::MouseEvent| {
        if let Some(cb) = on_pan {
            let target = event_target::<web_sys::HtmlElement>(&ev);
            let w = target.client_width() as f64;
            let h = target.client_height() as f64;
            if w > 0.0 && h > 0.0 && scale > 0.0 {
                let (_, _, vw, vh) = viewport.get_untracked();
                // Convert click position back into layout coordinates, centered on click
                let x_layout = min_x + (ev.offset_x() as f64 / w) * bbox_w;
                let y_layout = min_y + (ev.offset_y() as f64 / h) * bbox_h;
                cb.run((x_layout - vw / 2.0, y_layout - vh / 2.0));
            }
        }
    };
    view! {
        <div class="dag-minimap">
            <div class="dag-minimap-label">"MAP"</div>
            <svg width=MAP_W height=MAP_H on:click=on_click_handler>
                {edge_lines}
                {node_rects}
                {vp_rect}
            </svg>
        </div>
    }
    .into_any()
}
