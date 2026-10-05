use leptos::prelude::*;
use leptos_router::components::A;

use super::badges::StatusChip;

#[component]
pub fn SectionHeader(
    #[prop(into)] label: String,
    #[prop(optional, into)] count: Option<String>,
) -> impl IntoView {
    view! {
        <div class="section-header">
            <span class="section-header-label">{label}</span>
            {count.map(|c| view! { <span class="section-header-count">{c}</span> })}
        </div>
    }
}

/// Rail color variant used by [`StatTile`] and [`SummaryCard`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Rail {
    #[default]
    None,
    Muted,
    Success,
    Error,
    Running,
    Primary,
}

impl Rail {
    fn class(self, base: &str) -> String {
        let suffix = match self {
            Rail::None => return String::new(),
            Rail::Muted => "muted",
            Rail::Success => "success",
            Rail::Error => "error",
            Rail::Running => "running",
            Rail::Primary => "primary",
        };
        format!("{base}-rail {base}-rail--{suffix}")
    }
}

#[component]
pub fn StatTile(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional, into)] suffix: Option<String>,
    #[prop(optional)] rail: Rail,
    #[prop(optional, into)] href: Option<String>,
) -> impl IntoView {
    let rail_cls = rail.class("stat-tile");
    let body = view! {
        <span class=rail_cls></span>
        <span class="stat-tile-label">{label}</span>
        <span class="stat-tile-value">
            {value}
            {suffix.map(|s| view! { <span class="stat-tile-value-suffix">{s}</span> })}
        </span>
    };
    if let Some(h) = href {
        view! { <A href=h attr:class="stat-tile">{body}</A> }.into_any()
    } else {
        view! { <div class="stat-tile">{body}</div> }.into_any()
    }
}

/// `kind` is one of `helpers::CHIP_KINDS` (same vocabulary as [`StatusChip`]).
#[component]
pub fn SummaryCard(
    #[prop(into)] title: String,
    #[prop(into)] description: String,
    #[prop(into)] kind: String,
    #[prop(optional, into)] href: Option<String>,
    /// Top-right monospace line — relative-time label that ticks live.
    #[prop(optional)]
    meta_ts: Option<i64>,
) -> impl IntoView {
    let rail = match kind.as_str() {
        "success" => Rail::Success,
        "running" => Rail::Running,
        "failed" => Rail::Error,
        _ => Rail::Muted,
    };
    let rail_cls = rail.class("summary-card");

    let body = view! {
        <span class=rail_cls></span>
        <div style="min-width:0">
            <div class="summary-card-title">
                <span class="summary-card-name">{title}</span>
                <StatusChip kind=kind/>
            </div>
            <div class="summary-card-desc">{description}</div>
        </div>
        <div class="summary-card-meta">
            {meta_ts.map(|ts| view! { <span class="meta-muted"><crate::now::RelTime ts=ts/></span> })}
        </div>
    };

    if let Some(h) = href {
        view! { <A href=h attr:class="summary-card">{body}</A> }.into_any()
    } else {
        view! { <div class="summary-card">{body}</div> }.into_any()
    }
}

#[component]
pub fn DonutStatCard(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional, into)] sub: Option<String>,
    /// 0.0 .. 1.0 — fraction of the ring filled.
    #[prop(into)]
    ring_value: Signal<f64>,
    #[prop(optional, into, default = "var(--success)".to_string())] color: String,
    #[prop(optional)] live: bool,
) -> impl IntoView {
    const R: f64 = 22.0;
    let circumference = 2.0 * std::f64::consts::PI * R;
    let dash = Signal::derive(move || {
        let v = ring_value.get().clamp(0.0, 1.0);
        format!("{:.2} {:.2}", v * circumference, circumference)
    });
    let color_track = color.clone();
    let color_live = color.clone();
    view! {
        <div class="donut-stat">
            {live.then(|| view! {
                <span class="donut-stat-live" style=format!("background:{color_live}")></span>
            })}
            <svg width="56" height="56" class="donut-stat-svg">
                <circle cx="28" cy="28" r="22" stroke="var(--bg-highest)" stroke-width="3" fill="none"/>
                <circle cx="28" cy="28" r="22" stroke=color_track stroke-width="3" stroke-linecap="round" fill="none"
                        stroke-dasharray=dash
                        style="transition: stroke-dasharray 600ms ease"/>
            </svg>
            <div>
                <div class="section-header-label" style="margin-bottom:4px">{label}</div>
                <div style="display:flex; align-items:baseline">
                    <span class="donut-stat-value">{value}</span>
                    {sub.map(|s| view! { <span class="donut-stat-sub">{s}</span> })}
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn EmptyState(
    #[prop(into)] message: String,
    #[prop(optional, into)] hint: Option<String>,
    /// Inside a page section: no illustration, less padding.
    #[prop(optional)]
    compact: bool,
) -> impl IntoView {
    if compact {
        return view! {
            <div class="empty-state">
                <span class="empty-state-msg">{message}</span>
                {hint.map(|h| view! { <span class="empty-state-hint">{h}</span> })}
            </div>
        }
        .into_any();
    }
    view! {
        <div class="empty-state-rich">
            <svg class="empty-state-wave" width="96" height="40" viewBox="0 0 96 40" fill="none">
                <path
                    d="M2 28 C 12 18, 24 38, 36 24 S 60 14, 72 26 S 90 18, 94 24"
                    stroke="var(--accent)"
                    stroke-width="1.5"
                    fill="none"
                    stroke-linecap="round"
                />
                <path
                    d="M2 32 C 14 22, 26 34, 40 28 S 66 18, 76 30 S 92 22, 94 28"
                    stroke="var(--secondary)"
                    stroke-width="1.5"
                    fill="none"
                    stroke-linecap="round"
                    opacity="0.7"
                />
            </svg>
            <div class="empty-state-msg">{message}</div>
            {hint.map(|h| view! { <div class="empty-state-hint">{h}</div> })}
        </div>
    }
    .into_any()
}

/// Queue "bottleneck explainer" card at the top of the queue screen.
#[component]
pub fn BottleneckCard(
    #[prop(into)] title: String,
    #[prop(into)] sub: String,
    #[prop(optional)] warn_only: bool,
    #[prop(optional)] children: Option<Children>,
) -> impl IntoView {
    let cls = if warn_only {
        "bottleneck-card bottleneck-card--warn"
    } else {
        "bottleneck-card"
    };
    view! {
        <div class=cls>
            <div>
                <div class="bottleneck-card-label">"BOTTLENECK"</div>
                <div class="bottleneck-card-title">{title}</div>
                <div class="bottleneck-card-sub">{sub}</div>
            </div>
            <div>{children.map(|c| c())}</div>
        </div>
    }
}

/// "N assets need attention" warning banner with optional sub-breakdown.
///
/// When both `collapsed` and `on_toggle` are supplied, the banner shows a
/// Collapse/Expand button with chevron on the right — mirrors the Rivers
/// `setAttentionOpen` pattern on the AssetsScreen.
#[component]
pub fn AttentionBanner(
    #[prop(into)] count: usize,
    /// e.g. "3 failed · 12 stale · 4 behind"
    #[prop(optional, into)]
    breakdown: Option<String>,
    #[prop(optional, into, default = "asset".to_string())] noun: String,
    /// Current collapsed state. When present (with `on_toggle`), shows the toggle button.
    #[prop(optional, into)]
    collapsed: Option<Signal<bool>>,
    #[prop(optional, into)] on_toggle: Option<Callback<()>>,
) -> impl IntoView {
    if count == 0 {
        return view! { <></> }.into_any();
    }
    let plural = if count == 1 {
        noun.clone()
    } else {
        format!("{noun}s")
    };
    let toggle_view = match (collapsed, on_toggle) {
        (Some(c), Some(cb)) => Some(view! {
            <button
                class="attention-banner-toggle"
                aria-expanded=move || (!c.get()).to_string()
                on:click=move |_| cb.run(())
            >
                <span class="attention-banner-toggle-label">{move || if c.get() { "Expand" } else { "Collapse" }}</span>
                <span
                    class="attention-banner-toggle-chevron"
                    class:attention-banner-toggle-chevron--open=move || !c.get()
                >"›"</span>
            </button>
        }),
        _ => None,
    };
    view! {
        <div class="attention-banner">
            <span class="attention-banner-dot"></span>
            <span class="attention-banner-msg">
                <strong>{count}</strong> " " {plural} {if count == 1 { " needs" } else { " need" }} " attention"
            </span>
            {breakdown.map(|b| view! { <span class="attention-banner-sub">{b}</span> })}
            {toggle_view.map(|t| view! { <span class="attention-banner-spacer"></span> {t} })}
        </div>
    }
    .into_any()
}

/// Large-format donut: 120x120 ring + big display value + unit + inline metrics.
/// Used on Overview as the featured 24h runs indicator.
#[component]
pub fn FeaturedDonut(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional, into)] unit: Option<String>,
    /// 0.0..1.0 ring fill fraction.
    #[prop(into)]
    ring_value: Signal<f64>,
    #[prop(optional, into, default = "var(--success)".to_string())] color: String,
    /// Inline `(label, value)` metrics below the value.
    #[prop(optional)]
    metrics: Vec<(String, String)>,
) -> impl IntoView {
    const R: f64 = 50.0;
    let circ = 2.0 * std::f64::consts::PI * R;
    let dash = Signal::derive(move || {
        let v = ring_value.get().clamp(0.0, 1.0);
        format!("{:.2} {:.2}", v * circ, circ)
    });
    let color_for_svg = color.clone();
    let metrics_view = metrics
        .into_iter()
        .map(|(k, v)| view! { <span><strong>{v}</strong> " " {k}</span> })
        .collect::<Vec<_>>();
    view! {
        <div class="featured-donut">
            <svg width="120" height="120" class="featured-donut-ring">
                <circle cx="60" cy="60" r="50" stroke="var(--bg-highest)" stroke-width="4" fill="none"/>
                <circle cx="60" cy="60" r="50" stroke=color_for_svg stroke-width="4" stroke-linecap="round" fill="none"
                        stroke-dasharray=dash
                        style="transition: stroke-dasharray 600ms ease"/>
            </svg>
            <div style="flex:1; min-width:0">
                <div class="featured-donut-label">{label}</div>
                <div class="featured-donut-value">
                    {value}
                    {unit.map(|u| view! { <span class="featured-donut-unit">{u}</span> })}
                </div>
                <div class="featured-donut-meta">{metrics_view}</div>
            </div>
        </div>
    }
}

/// Blank tiles that complete the last row of a `.meta-tile-grid`, so the
/// grid's separator colour never shows as an empty block.
pub fn meta_tile_fill(tiles: usize, cols: usize) -> impl IntoView {
    let missing = (cols - tiles % cols) % cols;
    (0..missing)
        .map(|_| view! { <div class="meta-tile" aria-hidden="true"></div> })
        .collect_view()
}
