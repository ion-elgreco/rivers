//! Rivers design-system primitives.

use leptos::prelude::*;
use leptos::web_sys;
use leptos_router::components::A;

/// Small pill with a colored dot indicator and a lowercase label.
///
/// Callers pass one of `helpers::CHIP_KINDS` (typically via
/// `run_status_kind` / `backfill_status_kind`). The kind is used directly as
/// the `.dot-{kind}` class suffix and the visible label — no free-form
/// string normalization, so a mistyped kind shows up in the DOM instead of
/// being silently rewritten.
#[component]
pub fn StatusChip(#[prop(into)] kind: String, #[prop(optional)] small: bool) -> impl IntoView {
    let dot_cls = format!("dot dot-{kind}");
    let chip_cls = if small { "chip chip--sm" } else { "chip" };
    view! {
        <span class={chip_cls}>
            <span class={dot_cls}></span>
            {kind}
        </span>
    }
}

#[component]
pub fn Sparkline(
    #[prop(into)] points: Vec<f64>,
    #[prop(optional, into, default = "var(--secondary)".to_string())] color: String,
    #[prop(optional, default = 120)] width: u32,
    #[prop(optional, default = 28)] height: u32,
) -> impl IntoView {
    if points.len() < 2 {
        return view! { <svg width=width height=height></svg> }.into_any();
    }
    let max = points.iter().cloned().fold(f64::MIN, f64::max);
    let min = points.iter().cloned().fold(f64::MAX, f64::min);
    let range = (max - min).max(1e-9);
    let step = width as f64 / (points.len() - 1) as f64;
    let mut d = String::with_capacity(points.len() * 12);
    for (i, v) in points.iter().enumerate() {
        let x = i as f64 * step;
        let y = height as f64 - ((v - min) / range) * height as f64;
        if i == 0 {
            d.push_str(&format!("M{x:.2},{y:.2}"));
        } else {
            d.push_str(&format!(" L{x:.2},{y:.2}"));
        }
    }
    let last_idx = points.len() - 1;
    let last_x = last_idx as f64 * step;
    let last_y = height as f64 - ((points[last_idx] - min) / range) * height as f64;
    view! {
        <svg width=width height=height style="display:block">
            <path d=d stroke=color.clone() stroke-width="1.3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
            <circle cx=format!("{last_x:.2}") cy=format!("{last_y:.2}") r="2.5" fill=color/>
        </svg>
    }
    .into_any()
}

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

/// Rail color variant used by [`StatTile`], [`SummaryCard`], and [`AlertCard`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Rail {
    #[default]
    None,
    Muted,
    Success,
    Error,
    Running,
    Primary,
    Warning,
    Critical,
    Info,
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
            Rail::Warning => "warning",
            Rail::Critical => "critical",
            Rail::Info => "info",
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

#[component]
pub fn HeroStat(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional, into)] unit: Option<String>,
    /// Inline `(label, value)` metric pairs rendered in a mono-space row.
    #[prop(optional)]
    metrics: Vec<(String, String)>,
    /// Optional throughput series rendered as a right-side histogram.
    #[prop(optional)]
    throughput: Option<Vec<f64>>,
    #[prop(optional, into)] throughput_label: Option<String>,
) -> impl IntoView {
    let metrics_view = metrics
        .into_iter()
        .map(|(k, v)| {
            view! {
                <span>
                    <strong>{v}</strong>
                    " "
                    {k}
                </span>
            }
        })
        .collect::<Vec<_>>();

    let right = throughput.map(|points| {
        let max = points.iter().cloned().fold(0.0_f64, f64::max).max(1.0);
        let n = points.len();
        let bars = points
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let pct = (v / max * 100.0).clamp(0.0, 100.0);
                let is_now = i + 1 == n;
                let is_recent = !is_now && i + 4 >= n;
                let cls = if is_now {
                    "bar bar--now"
                } else if is_recent {
                    "bar bar--recent"
                } else {
                    "bar"
                };
                view! { <div class=cls style=format!("height:{pct:.1}%")></div> }
            })
            .collect::<Vec<_>>();
        view! {
            <div>
                <div class="section-header-label" style="margin-bottom:10px">
                    {throughput_label.unwrap_or_else(|| "THROUGHPUT".to_string())}
                </div>
                <div class="throughput-bars">{bars}</div>
            </div>
        }
    });

    view! {
        <div class="hero-stat">
            <div>
                <div class="section-header-label hero-stat-label">{label}</div>
                <h1 class="hero-stat-value">
                    {value}
                    {unit.map(|u| view! { <span class="hero-stat-value-unit">{u}</span> })}
                </h1>
                <div class="hero-stat-meta">{metrics_view}</div>
            </div>
            {right}
        </div>
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
                <StatusChip kind=kind small=true/>
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

/// Severity variant for [`AlertCard`]. Maps to the alert-card left rail
/// color and the `SEV` chip displayed in the header row.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AlertSev {
    Critical,
    Warning,
    Info,
}

#[component]
pub fn AlertCard(
    sev: AlertSev,
    #[prop(into)] id: String,
    /// Timestamp (unix-nanos) for the live-ticking "Xm ago" age label.
    age_ts: i64,
    #[prop(into)] title: String,
    #[prop(optional, into)] rule: Option<String>,
    #[prop(optional, into)] source: Option<String>,
) -> impl IntoView {
    let (sev_cls, rail_cls, label) = match sev {
        AlertSev::Critical => (
            "alert-card-sev--critical",
            "alert-card-rail--critical",
            "critical",
        ),
        AlertSev::Warning => (
            "alert-card-sev--warning",
            "alert-card-rail--warning",
            "warning",
        ),
        AlertSev::Info => ("alert-card-sev--info", "alert-card-rail--info", "info"),
    };
    view! {
        <div class="alert-card">
            <span class=format!("alert-card-rail {rail_cls}")></span>
            <div class="alert-card-head">
                <span class=format!("alert-card-sev {sev_cls}")>{label}</span>
                <span class="alert-card-id">{id}</span>
                <span class="alert-card-age"><crate::now::RelTime ts=age_ts/></span>
            </div>
            <div class="alert-card-title">{title}</div>
            {(rule.is_some() || source.is_some()).then(|| view! {
                <div class="alert-card-rule">
                    {rule.map(|r| view! { <>"rule: "<strong>{r}</strong></> })}
                    {source.map(|s| view! { <>" · source: "<strong>{s}</strong></> })}
                </div>
            })}
        </div>
    }
}

#[derive(Clone)]
pub struct Crumb {
    pub label: String,
    pub href: Option<String>,
    pub mono: bool,
    pub copy_value: Option<String>,
}

impl Crumb {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: None,
            mono: false,
            copy_value: None,
        }
    }
    pub fn linked(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: Some(href.into()),
            mono: false,
            copy_value: None,
        }
    }
    pub fn mono(mut self) -> Self {
        self.mono = true;
        self
    }
    /// Render this crumb as a click-to-copy chip that copies the given value.
    pub fn copyable(mut self, value: impl Into<String>) -> Self {
        self.copy_value = Some(value.into());
        self
    }
}

#[component]
pub fn Topbar(
    #[prop(into)] crumbs: Vec<Crumb>,
    #[prop(optional)] children: Option<Children>,
) -> impl IntoView {
    let last = crumbs.len().saturating_sub(1);
    let rendered: Vec<_> = crumbs
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            let sep = (i > 0).then(|| view! { <span class="topbar-crumb-sep">"/"</span> });
            let mut cls = String::from("topbar-crumb");
            if c.mono {
                cls.push_str(" topbar-crumb--mono");
            }
            if i == last {
                cls.push_str(" topbar-crumb--current");
            }
            if c.copy_value.is_some() {
                cls.push_str(" copyable");
            }
            let item = if let Some(href) = c.href.clone() {
                view! { <A href=href attr:class=cls>{c.label.clone()}</A> }.into_any()
            } else if let Some(cv) = c.copy_value.clone() {
                view! { <span class=cls data-copy=cv title="Click to copy">{c.label.clone()}</span> }.into_any()
            } else {
                view! { <span class=cls>{c.label.clone()}</span> }.into_any()
            };
            view! { <>{sep}{item}</> }
        })
        .collect();

    view! {
        <div class="topbar">
            <div class="topbar-row">
                <div class="topbar-crumbs">{rendered}</div>
                <div class="topbar-actions">
                    {children.map(|c| c())}
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn FilterPills(children: Children) -> impl IntoView {
    view! { <div class="filter-pills">{children()}</div> }
}

#[component]
pub fn FilterPill(
    #[prop(into)] label: String,
    #[prop(optional, into)] count: Option<String>,
    #[prop(into)] active: Signal<bool>,
    #[prop(into)] on_click: Callback<()>,
) -> impl IntoView {
    let cls = move || {
        if active.get() {
            "filter-pill filter-pill--active"
        } else {
            "filter-pill"
        }
    };
    view! {
        <button class=cls on:click=move |_| on_click.run(())>
            {label}
            {count.map(|c| view! { <span class="count">{c}</span> })}
        </button>
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HeatCell {
    Done,
    Running,
    Failed,
    Pending,
    Canceled,
}

/// Partition / backfill heatmap — CSS-grid of status-colored cells.
/// Default layout is 30 columns; pass `compact=true` for a 20-column variant.
/// Pass `freshness_gradient=true` to apply an opacity gradient across `Done`
/// cells so oldest cells look faded and newest look vivid (Rivers' partition
/// freshness pattern for asset detail).
#[component]
pub fn PartitionHeatmap(
    #[prop(into)] cells: Vec<HeatCell>,
    #[prop(optional)] compact: bool,
    #[prop(optional)] legend: bool,
    #[prop(optional)] freshness_gradient: bool,
    /// Optional per-cell labels (e.g. partition keys). When present, hovering a
    /// cell reveals a Rivers-style inline info bar below the grid showing the
    /// key, index, and status.
    #[prop(optional, into)]
    labels: Vec<String>,
) -> impl IntoView {
    let grid_cls = if compact {
        "heatmap heatmap--sm"
    } else {
        "heatmap"
    };
    let total = cells.len();
    let (hover, set_hover) = signal(Option::<usize>::None);
    let labels_arc = std::sync::Arc::new(labels);

    let rendered = cells
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let cls = match c {
                HeatCell::Done => "heatmap-cell heatmap-cell--done",
                HeatCell::Running => "heatmap-cell heatmap-cell--running",
                HeatCell::Failed => "heatmap-cell heatmap-cell--failed",
                HeatCell::Pending => "heatmap-cell heatmap-cell--pending",
                HeatCell::Canceled => "heatmap-cell heatmap-cell--canceled",
            };
            let status_word = match c {
                HeatCell::Done => "done",
                HeatCell::Running => "running",
                HeatCell::Failed => "failed",
                HeatCell::Pending => "pending",
                HeatCell::Canceled => "canceled",
            };
            let style = if freshness_gradient && matches!(c, HeatCell::Done) {
                let op = 0.4 + (i as f64 / (total.max(1)) as f64) * 0.55;
                Some(format!("opacity:{op:.2}"))
            } else {
                None
            };
            let title = labels_arc
                .get(i)
                .map(|k| format!("{k} · {status_word}"))
                .unwrap_or_else(|| status_word.to_string());
            view! {
                <div
                    class=cls
                    style=style
                    title=title
                    on:mouseenter=move |_| set_hover.set(Some(i))
                    on:mouseleave=move |_| set_hover.set(None)
                ></div>
            }
        })
        .collect::<Vec<_>>();

    // Snapshot per-cell data for the info-bar lookup — wrap in Arc so the view
    // closure can be cloned (Leptos needs Fn, not FnOnce).
    let info_lookup: std::sync::Arc<Vec<(String, &'static str, &'static str)>> =
        std::sync::Arc::new(
            cells
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let key = labels_arc
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("#{}", i + 1));
                    let word = match c {
                        HeatCell::Done => "done",
                        HeatCell::Running => "running",
                        HeatCell::Failed => "failed",
                        HeatCell::Pending => "pending",
                        HeatCell::Canceled => "canceled",
                    };
                    let cls = match c {
                        HeatCell::Done => "heatmap-info-dot--done",
                        HeatCell::Running => "heatmap-info-dot--running",
                        HeatCell::Failed => "heatmap-info-dot--failed",
                        HeatCell::Pending => "heatmap-info-dot--pending",
                        HeatCell::Canceled => "heatmap-info-dot--canceled",
                    };
                    (key, word, cls)
                })
                .collect(),
        );
    let has_labels = !labels_arc.is_empty();
    let total_cells = total;

    view! {
        <div>
            <div class=grid_cls>{rendered}</div>
            <Show when=move || has_labels && hover.get().is_some()>
                {
                    let info_lookup = info_lookup.clone();
                    move || {
                        let i = hover.get()?;
                        let (key, word, cls) = info_lookup.get(i)?.clone();
                        Some(view! {
                            <div class="heatmap-info">
                                <span class=format!("heatmap-info-dot {cls}")></span>
                                <span class="heatmap-info-label">"partition"</span>
                                <span class="heatmap-info-key">{key}</span>
                                <span class="heatmap-info-sep">"·"</span>
                                <span class="heatmap-info-pos">{format!("#{} of {}", i + 1, total_cells)}</span>
                                <span class="heatmap-info-sep">"·"</span>
                                <span class=format!("heatmap-info-state heatmap-info-state--{word}")>{word}</span>
                            </div>
                        })
                    }
                }
            </Show>
            {legend.then(|| view! {
                <div class="heatmap-legend">
                    <span><span class="heatmap-legend-swatch" style="background:var(--success)"></span>"done"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--secondary)"></span>"running"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--error)"></span>"failed"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--bg-highest)"></span>"pending"</span>
                </div>
            })}
        </div>
    }
}

#[component]
pub fn EmptyState(
    #[prop(into)] message: String,
    #[prop(optional, into)] hint: Option<String>,
) -> impl IntoView {
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
            <div style="color:var(--text); font-weight:500">{message}</div>
            {hint.map(|h| view! { <div style="margin-top:4px; opacity:0.7">{h}</div> })}
        </div>
    }
}

/// ConditionReplay — real per-sub-condition history for a single asset over
/// the last N minutes. Fetches recent `ConditionEvalRecord`s, walks the
/// evaluation tree, and plots each sub-node's status per time bucket. Click a
/// cell to inspect what blocked (or caused) a fire at that moment.
#[component]
pub fn ConditionReplay(
    #[prop(into)] asset_key: String,
    #[prop(optional, default = 60)] minutes: u32,
) -> impl IntoView {
    use crate::loc::use_current_location;
    use crate::server_fns::automation::get_condition_evals;
    use crate::types::NodeStatus;

    let key = asset_key.clone();
    let loc = use_current_location();
    let evals = Resource::new(
        move || (loc.get(), key.clone()),
        |((ns, name), k)| get_condition_evals(ns, name, k, Some(200)),
    );

    let (cursor, set_cursor) = signal(Option::<usize>::None);

    // Bucket indices are computed relative to `now_ns` at render time; when
    // the resource refreshes, the same index would point to a different
    // moment. Reset the cursor on refresh to avoid silently drifting state.
    Effect::new(move |prev: Option<()>| {
        evals.get();
        if prev.is_some() {
            set_cursor.set(None);
        }
    });

    view! {
        <div class="cond-replay">
            <Transition fallback=move || view! {
                <div class="cond-replay-empty">"Loading evaluation history..."</div>
            }>
                {move || {
                    let records = evals.get().and_then(|r| r.ok()).unwrap_or_default();

                    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    let window_ns = minutes as i64 * 60 * 1_000_000_000;
                    let mut recent: Vec<_> = records
                        .into_iter()
                        .filter(|e| now_ns.saturating_sub(e.timestamp) < window_ns)
                        .collect();
                    recent.sort_by_key(|e| e.timestamp);

                    if recent.is_empty() {
                        return view! {
                            <>
                                <div class="cond-replay-head">
                                    <span class="section-header-label">{format!("CONDITION REPLAY · LAST {minutes} MIN")}</span>
                                </div>
                                <div class="cond-replay-empty">{format!("No evaluations in the last {minutes} minutes.")}</div>
                            </>
                        }.into_any();
                    }

                    // Flatten the latest eval's tree into a depth-first list of sub-nodes.
                    let latest = recent.last().unwrap();
                    let mut sub_nodes: Vec<(u32, String, String, usize)> = Vec::new();
                    fn walk(
                        node: &crate::types::EvalNodeResult,
                        depth: usize,
                        out: &mut Vec<(u32, String, String, usize)>,
                    ) {
                        out.push((node.node_idx, node.label.clone(), node.node_type.clone(), depth));
                        for c in &node.children {
                            walk(c, depth + 1, out);
                        }
                    }
                    walk(&latest.tree, 0, &mut sub_nodes);

                    // Bucket evals into N minute-width slots, keeping only the latest per bucket.
                    let n_buckets = minutes as usize;
                    let bucket_ns = (window_ns / n_buckets as i64).max(1);
                    let mut bucket_status: Vec<Option<std::collections::HashMap<u32, NodeStatus>>> =
                        vec![None; n_buckets];
                    let mut bucket_trees: Vec<Option<crate::types::EvalNodeResult>> =
                        vec![None; n_buckets];
                    let mut bucket_times: Vec<Option<i64>> = vec![None; n_buckets];
                    for eval in &recent {
                        let age = now_ns.saturating_sub(eval.timestamp);
                        if age < 0 || age >= window_ns { continue; }
                        let slot = (n_buckets - 1)
                            .saturating_sub((age / bucket_ns) as usize)
                            .min(n_buckets - 1);
                        if let Some(existing_ts) = bucket_times[slot]
                            && eval.timestamp <= existing_ts { continue; }
                        let mut map = std::collections::HashMap::new();
                        fn collect_statuses(
                            node: &crate::types::EvalNodeResult,
                            map: &mut std::collections::HashMap<u32, NodeStatus>,
                        ) {
                            map.insert(node.node_idx, node.status.clone());
                            for c in &node.children {
                                collect_statuses(c, map);
                            }
                        }
                        collect_statuses(&eval.tree, &mut map);
                        bucket_status[slot] = Some(map);
                        bucket_trees[slot] = Some(eval.tree.clone());
                        bucket_times[slot] = Some(eval.timestamp);
                    }

                    // Build a per-sub-node track.
                    let bucket_status_for_tracks = bucket_status.clone();
                    let tracks: Vec<_> = sub_nodes.iter().map(|(idx, label, node_type, depth)| {
                        let idx = *idx;
                        let is_op = matches!(node_type.as_str(), "And" | "Or" | "Not");
                        let bucket_status_cells = bucket_status_for_tracks.clone();
                        let cells: Vec<_> = (0..n_buckets).map(|b| {
                            let (base_cls, title_verb) = match bucket_status_cells[b].as_ref().and_then(|m| m.get(&idx)) {
                                Some(NodeStatus::True) => ("cond-cell cond-cell--true", "true"),
                                Some(NodeStatus::False) => ("cond-cell cond-cell--false", "false"),
                                Some(NodeStatus::Skipped) => ("cond-cell cond-cell--skipped", "skipped"),
                                None => ("cond-cell cond-cell--missing", "no tick"),
                            };
                            let bucket_age = (n_buckets - 1 - b) as u32;
                            let title_str = format!("{title_verb} · {bucket_age}m ago");
                            let cls_reactive = move || {
                                if cursor.get() == Some(b) {
                                    format!("{base_cls} cond-cell--cursor")
                                } else {
                                    base_cls.to_string()
                                }
                            };
                            view! {
                                <span
                                    class=cls_reactive
                                    title=title_str
                                    on:click=move |_| {
                                        if cursor.get() == Some(b) {
                                            set_cursor.set(None);
                                        } else {
                                            set_cursor.set(Some(b));
                                        }
                                    }
                                ></span>
                            }
                        }).collect();

                        let indent = *depth * 12;
                        let label_class = if is_op { "cond-track-label cond-track-label--op" } else { "cond-track-label" };
                        let display_label = if is_op {
                            format!("{} {}", node_type.to_ascii_lowercase(), label)
                        } else {
                            label.clone()
                        };
                        view! {
                            <div class="cond-track">
                                <span
                                    class=label_class
                                    style=format!("padding-left:{indent}px")
                                    title=label.clone()
                                >{display_label}</span>
                                <div class="cond-track-cells">{cells}</div>
                            </div>
                        }
                    }).collect();

                    // Cursor detail: shows what happened at the selected minute.
                    // Tree-aware: walks from root, expecting True (fire). When a
                    // node's status differs from expected, recurse into the
                    // sub-tree that caused the mismatch, inverting expectation
                    // through NOT nodes. Leaves reached this way are the real
                    // blockers — a False leaf under a NOT is *not* a blocker.
                    fn fault_leaves(
                        node: &crate::types::EvalNodeResult,
                        expected: NodeStatus,
                        out: &mut Vec<String>,
                    ) {
                        // Skipped means short-circuited — don't count as fault
                        if matches!(node.status, NodeStatus::Skipped) { return; }
                        if node.status == expected { return; }
                        match node.node_type.as_str() {
                            "And" => {
                                // Expected True, got False → at least one child is False.
                                // Recurse into children still expecting True.
                                for c in &node.children {
                                    fault_leaves(c, NodeStatus::True, out);
                                }
                            }
                            "Or" => {
                                // Expected True, got False → all children False.
                                for c in &node.children {
                                    fault_leaves(c, NodeStatus::True, out);
                                }
                            }
                            "Not" => {
                                // Expected True, got False → child is True but we needed False.
                                // Recurse with inverted expectation.
                                if let Some(c) = node.children.first() {
                                    fault_leaves(c, NodeStatus::False, out);
                                }
                            }
                            _ => {
                                // Leaf didn't match — it's a responsible node.
                                out.push(node.label.clone());
                            }
                        }
                    }

                    let bucket_trees_for_detail = bucket_trees;
                    let bucket_times_for_detail = bucket_times;
                    let cursor_detail = move || {
                        let Some(b) = cursor.get() else { return ().into_any(); };
                        let Some(tree) = bucket_trees_for_detail.get(b).and_then(|x| x.as_ref()) else {
                            return ().into_any();
                        };
                        let Some(ts) = bucket_times_for_detail.get(b).copied().flatten() else {
                            return ().into_any();
                        };
                        let age_min = (now_ns.saturating_sub(ts) / 60_000_000_000) as u32;
                        let age_label = if age_min == 0 { "just now".to_string() } else { format!("{age_min}m ago") };

                        let root_status = tree.status.clone();
                        let (tag_cls, tag_label) = match root_status {
                            NodeStatus::True => ("cond-verdict cond-verdict--fire", "FIRED"),
                            NodeStatus::False => ("cond-verdict cond-verdict--block", "SUPPRESSED"),
                            NodeStatus::Skipped => ("cond-verdict cond-verdict--skip", "SKIPPED"),
                        };

                        // Only compute blockers when the root was suppressed.
                        let blockers = if matches!(root_status, NodeStatus::False) {
                            let mut v = Vec::new();
                            fault_leaves(tree, NodeStatus::True, &mut v);
                            v.dedup();
                            v
                        } else {
                            Vec::new()
                        };

                        view! {
                            <div class="cond-cursor-detail">
                                <span class=tag_cls>{tag_label}</span>
                                <span class="cond-cursor-time">"at " <b>{age_label}</b></span>
                                {(!blockers.is_empty()).then(|| view! {
                                    <>
                                        <span class="cond-cursor-sep">"·"</span>
                                        <span class="cond-cursor-blockers">
                                            "blockers: "
                                            <span class="cond-cursor-blocker-list">{blockers.join(", ")}</span>
                                        </span>
                                    </>
                                })}
                            </div>
                        }.into_any()
                    };

                    view! {
                        <>
                            <div class="cond-replay-head">
                                <span class="section-header-label">{format!("CONDITION REPLAY · LAST {minutes} MIN")}</span>
                                <span class="cond-replay-hint">
                                    {recent.len().to_string()} " ticks · click a cell to inspect"
                                </span>
                            </div>
                            <div class="cond-tracks">{tracks}</div>
                            <div class="cond-axis">
                                <span>{format!("−{minutes}m")}</span>
                                <span>{format!("−{}m", minutes * 3 / 4)}</span>
                                <span>{format!("−{}m", minutes / 2)}</span>
                                <span>{format!("−{}m", minutes / 4)}</span>
                                <span>"now"</span>
                            </div>
                            {cursor_detail}
                            <div class="cond-legend">
                                <span><span class="cond-swatch cond-swatch--true"></span> "true"</span>
                                <span><span class="cond-swatch cond-swatch--false"></span> "false"</span>
                                <span><span class="cond-swatch cond-swatch--skipped"></span> "skipped"</span>
                                <span><span class="cond-swatch cond-swatch--missing"></span> "no tick"</span>
                            </div>
                        </>
                    }.into_any()
                }}
            </Transition>
        </div>
    }
}

/// Global evaluation timeline — bucketed bar chart showing when the automation
/// engine evaluated conditions (bar height = eval count per bucket) and when
/// those evals resulted in a materialization request (accent-colored bars).
#[component]
pub fn EvalTimelineBars(
    /// (eval_count, had_request) buckets ordered oldest → newest.
    #[prop(into)]
    buckets: Vec<(u32, bool)>,
) -> impl IntoView {
    let total_ticks: u64 = buckets.iter().map(|(c, _)| *c as u64).sum();
    let fire_count: usize = buckets.iter().filter(|(_, r)| *r).count();
    let n = buckets.len().max(1);
    let mins_per_bucket = 60.0 / n as f64;
    let last_fire_idx = buckets.iter().rposition(|(_, r)| *r);
    let last_fire_label = last_fire_idx
        .map(|i| {
            let mins_ago = ((n - 1 - i) as f64 * mins_per_bucket).round() as u32;
            if mins_ago == 0 {
                "now".to_string()
            } else {
                format!("{mins_ago}m ago")
            }
        })
        .unwrap_or_else(|| "—".to_string());

    let max = buckets.iter().map(|(c, _)| *c).max().unwrap_or(1).max(1) as f64;
    let bars = buckets
        .into_iter()
        .map(|(c, req)| {
            let h = (c as f64 / max * 100.0).clamp(2.0, 100.0);
            let color = if req { "var(--accent)" } else { "var(--bg-highest)" };
            let plural = if c == 1 { "tick" } else { "ticks" };
            let title = if req {
                format!("{c} {plural} · fired")
            } else {
                format!("{c} {plural}")
            };
            view! {
                <div
                    title=title
                    style=format!("flex:1; min-width:1px; height:{h:.0}%; background:{color}; border-radius:1px")
                ></div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="eval-timeline-panel">
            <div class="eval-timeline-head">
                <span class="section-header-label">"GLOBAL EVALUATION TIMELINE · LAST HOUR"</span>
                <span class="eval-timeline-stats">
                    <span class="eval-timeline-stat">
                        <span class="eval-timeline-stat-num">{total_ticks.to_string()}</span>
                        " ticks"
                    </span>
                    <span class="eval-timeline-sep">"·"</span>
                    <span class="eval-timeline-stat">
                        <span class="eval-timeline-stat-num" style="color:var(--accent)">{fire_count.to_string()}</span>
                        " fires"
                    </span>
                    <span class="eval-timeline-sep">"·"</span>
                    <span class="eval-timeline-stat">
                        "last fire "
                        <span class="eval-timeline-stat-num">{last_fire_label}</span>
                    </span>
                </span>
            </div>
            <div class="eval-timeline-bars">{bars}</div>
            <div class="eval-timeline-axis">
                <span>"−60m"</span>
                <span>"−45m"</span>
                <span>"−30m"</span>
                <span>"−15m"</span>
                <span>"now"</span>
            </div>
            <div class="eval-timeline-legend">
                <span class="eval-timeline-legend-item">
                    <span class="eval-timeline-swatch" style="background:var(--bg-highest)"></span>
                    "tick — no fire"
                </span>
                <span class="eval-timeline-legend-item">
                    <span class="eval-timeline-swatch" style="background:var(--accent)"></span>
                    "tick — run requested"
                </span>
                <span class="eval-timeline-legend-hint">"each bar = " {format!("{:.0}s", mins_per_bucket * 60.0)} " window; height = ticks in that window"</span>
            </div>
        </div>
    }
}

#[derive(Clone, Debug)]
pub struct LineageNode {
    pub id: String,
    pub label: String,
    pub column: u8, // 0 = source, 1 = pipeline, 2 = asset
    pub row: u8,
}

#[component]
pub fn LineageMini(
    #[prop(into)] nodes: Vec<LineageNode>,
    #[prop(into)] edges: Vec<(String, String)>,
) -> impl IntoView {
    let col_x = [30.0f64, 280.0, 560.0];
    let row_step = 50.0f64;
    let nw = 130.0;
    let nh = 22.0;

    let pos = |n: &LineageNode| -> (f64, f64) {
        let col = n.column.min(2) as usize;
        (col_x[col], 20.0 + n.row as f64 * row_step)
    };

    let edges_rendered: Vec<_> = edges
        .iter()
        .filter_map(|(a, b)| {
            let na = nodes.iter().find(|n| &n.id == a)?;
            let nb = nodes.iter().find(|n| &n.id == b)?;
            let (ax, ay) = pos(na);
            let (bx, by) = pos(nb);
            let x1 = ax + nw;
            let y1 = ay + nh / 2.0;
            let x2 = bx;
            let y2 = by + nh / 2.0;
            let d = format!(
                "M{x1:.1},{y1:.1} C{:.1},{y1:.1} {:.1},{y2:.1} {x2:.1},{y2:.1}",
                x1 + 60.0,
                x2 - 60.0
            );
            Some(view! { <path d=d class="lineage-mini-edge"/> })
        })
        .collect();

    let nodes_rendered: Vec<_> = nodes
        .iter()
        .map(|n| {
            let (x, y) = pos(n);
            let rail_cls = match n.column {
                0 => "lineage-mini-rail--source",
                1 => "lineage-mini-rail--pipeline",
                _ => "lineage-mini-rail--asset",
            };
            view! {
                <g transform=format!("translate({x:.0}, {y:.0})")>
                    <rect x="0" y="0" width=nw height=nh rx="2" class="lineage-mini-node"/>
                    <rect x="0" y="2" width="2" height="18" class=rail_cls/>
                    <text x="10" y="14" class="lineage-mini-node-text">{n.label.clone()}</text>
                </g>
            }
        })
        .collect();

    view! {
        <div class="lineage-mini-wrap">
            <svg width="100%" height="200" viewBox="0 0 720 200" style="display:block">
                {edges_rendered}
                {nodes_rendered}
            </svg>
        </div>
    }
}

#[derive(Clone, Debug)]
pub struct StripRun {
    pub id: String,
    pub status: &'static str, // "ok" | "err" | "retry"
    pub duration_s: f64,
    pub live: bool,
}

#[component]
pub fn RecentRunsStrip(
    #[prop(into)] runs: Vec<StripRun>,
    #[prop(optional, into, default = "RECENT RUNS".to_string())] label: String,
) -> impl IntoView {
    if runs.is_empty() {
        return view! {
            <div class="runs-strip">
                <div class="runs-strip-head">
                    <span class="section-header-label">{label}</span>
                    <span class="section-header-count">"no runs"</span>
                </div>
            </div>
        }
        .into_any();
    }
    let max = runs
        .iter()
        .map(|r| r.duration_s)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let avg = runs.iter().map(|r| r.duration_s).sum::<f64>() / runs.len() as f64;
    let ok_count = runs.iter().filter(|r| r.status == "ok").count();
    let err_count = runs.iter().filter(|r| r.status == "err").count();
    let avg_offset = (1.0 - (avg / max)) * 60.0 + 8.0;

    let bars = runs
        .iter()
        .map(|r| {
            let h = (r.duration_s / max * 60.0).max(8.0);
            let cls = match r.status {
                "err" => "runs-strip-bar runs-strip-bar--err",
                "retry" => "runs-strip-bar runs-strip-bar--retry",
                _ => "runs-strip-bar runs-strip-bar--ok",
            };
            let cls = if r.live {
                format!("{cls} runs-strip-bar--live")
            } else {
                cls.to_string()
            };
            let x_marker = (r.status == "err").then(|| {
                view! {
                    <span class="runs-strip-bar-x">"✕"</span>
                }
            });
            // Rivers tooltip: "run · status · Xs · [live]"
            let status_label = match r.status {
                "err" => "failed",
                "retry" => "retry",
                _ => "ok",
            };
            let live_suffix = if r.live { " · live" } else { "" };
            let tip = format!(
                "{} · {} · {:.1}s{}",
                r.id, status_label, r.duration_s, live_suffix
            );
            view! {
                <div class=cls style=format!("height:{h:.0}px") title=tip>
                    {x_marker}
                </div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="runs-strip">
            <div class="runs-strip-head">
                <span class="section-header-label">{label}</span>
                <div class="stats">
                    <span>"avg " {format!("{:.1}s", avg)}</span>
                    <span style="color:var(--success)">"✓ " {ok_count}</span>
                    <span style="color:var(--error)">"✕ " {err_count}</span>
                </div>
            </div>
            <div class="runs-strip-bars">
                <span class="runs-strip-avg" style=format!("top:{avg_offset:.1}px")></span>
                {bars}
            </div>
        </div>
    }
    .into_any()
}

#[derive(Clone, Debug)]
pub struct Tick {
    pub kind: &'static str, // "schedule" | "sensor" | "condition"
    pub label: String,
    /// Minutes from now (0..horizon_minutes).
    pub at_minutes: f64,
}

#[component]
pub fn NextTicksStrip(
    #[prop(into)] ticks: Vec<Tick>,
    #[prop(optional, default = 60)] horizon_minutes: u32,
) -> impl IntoView {
    let gridlines = (1..=6)
        .map(|i| {
            let pct = i as f64 / 7.0 * 100.0;
            view! { <span class="next-ticks-gridline" style=format!("left:{pct:.1}%")></span> }
        })
        .collect::<Vec<_>>();

    let lane_for = |kind: &str| -> i32 {
        match kind {
            "schedule" => 0,
            "sensor" => 1,
            _ => 2,
        }
    };
    let horizon = horizon_minutes.max(1) as f64;
    let tick_views = ticks
        .iter()
        .map(|t| {
            let cls = match t.kind {
                "schedule" => "next-ticks-tick next-ticks-tick--schedule",
                "sensor" => "next-ticks-tick next-ticks-tick--sensor",
                _ => "next-ticks-tick next-ticks-tick--condition",
            };
            let lane = lane_for(t.kind);
            let top = 10.0 + lane as f64 * 18.0;
            let pct = (t.at_minutes / horizon * 100.0).clamp(0.0, 100.0);
            view! {
                <span class=cls style=format!("left:{pct:.1}%; top:{top:.0}px") title=t.label.clone()></span>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="next-ticks">
            <div class="section-header">
                <span class="section-header-label">"NEXT " {horizon_minutes} " MIN"</span>
                <span class="section-header-count">{format!("{} upcoming", ticks.len())}</span>
            </div>
            <div class="next-ticks-track">
                {gridlines}
                <span class="next-ticks-playhead"></span>
                {tick_views}
            </div>
        </div>
    }
}

/// Stacked pool-utilization bar: `used` / `free` / `queued` percentages.
/// Values are clamped to `[0, 100]` each.
#[component]
pub fn PoolUtilBar(
    #[prop(into)] used_pct: f64,
    #[prop(optional, into, default = 0.0)] queued_pct: f64,
) -> impl IntoView {
    let used = used_pct.clamp(0.0, 100.0);
    let queued = queued_pct.clamp(0.0, 100.0);
    let free = (100.0 - used).max(0.0);
    let used_cls = if used >= 90.0 {
        "pool-bar-used pool-bar-used--crit"
    } else if used >= 70.0 {
        "pool-bar-used pool-bar-used--warn"
    } else {
        "pool-bar-used pool-bar-used--ok"
    };
    view! {
        <div class="pool-bar" role="progressbar">
            <span class=used_cls style=format!("width:{used:.1}%")></span>
            <span class="pool-bar-free" style=format!("width:{free:.1}%")></span>
            {(queued > 0.0).then(|| view! {
                <span class="pool-bar-queued" style=format!("width:{queued:.1}%")></span>
            })}
        </div>
    }
}

#[derive(Clone, Debug)]
pub struct QueuedRun {
    pub id: String,
    pub position: usize,
    pub job: String,
    pub priority: &'static str, // "high" | "normal" | "low"
    /// Timestamp (unix-nanos) the run entered the queue; rendered as a
    /// live-ticking "Xs / Xm ago" label.
    pub queued_at: i64,
    pub href: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LaneSpec {
    pub id: String,
    pub label: String,
    /// CSS var name or color literal, e.g. `"var(--error)"`.
    pub color: String,
    pub runs: Vec<QueuedRun>,
}

#[component]
pub fn QueueLanes(#[prop(into)] lanes: Vec<LaneSpec>) -> impl IntoView {
    let rendered = lanes
        .into_iter()
        .map(|lane| {
            let color_rail = lane.color.clone();
            let color_swatch = lane.color.clone();
            let count = lane.runs.len();
            let runs = lane
                .runs
                .into_iter()
                .map(|r| {
                    let prio_cls = match r.priority {
                        "high" => "queue-lane-card-priority queue-lane-card-priority--high",
                        "low" => "queue-lane-card-priority queue-lane-card-priority--low",
                        _ => "queue-lane-card-priority",
                    };
                    let rail_style = format!("border-left-color:{}", color_rail);
                    let body = view! {
                        <>
                            <div class="queue-lane-card-top">
                                <span class="queue-lane-card-id">{r.id.clone()}</span>
                                <span class=prio_cls>{r.priority}</span>
                            </div>
                            <div class="queue-lane-card-job">{r.job}</div>
                            <div class="queue-lane-card-meta">
                                <span>{format!("pos #{}", r.position)}</span>
                                <span><crate::now::RelTime ts=r.queued_at/></span>
                            </div>
                        </>
                    };
                    if let Some(href) = r.href {
                        view! { <A href=href attr:class="queue-lane-card" attr:style=rail_style>{body}</A> }.into_any()
                    } else {
                        view! { <div class="queue-lane-card" style=rail_style>{body}</div> }.into_any()
                    }
                })
                .collect::<Vec<_>>();
            view! {
                <div class="queue-lane">
                    <div class="queue-lane-head" style=format!("border-bottom-color:{}", lane.color)>
                        <span class="queue-lane-head-swatch" style=format!("background:{color_swatch}")></span>
                        <span class="queue-lane-head-name">{lane.label}</span>
                        <span class="queue-lane-head-count">{count}</span>
                    </div>
                    {runs}
                </div>
            }
        })
        .collect::<Vec<_>>();
    view! { <div class="queue-lanes">{rendered}</div> }
}

#[derive(Clone, Debug)]
pub struct DeploymentNode {
    pub label: String,
    pub sub: String,
    /// 1-character glyph (e.g. emoji or unicode) drawn inside the circle.
    pub glyph: String,
    pub ok: bool,
}

#[component]
pub fn DeploymentDiagram(#[prop(into)] nodes: Vec<DeploymentNode>) -> impl IntoView {
    let n = nodes.len().max(1);
    let total_w = 820.0;
    let h = 160.0;
    let col_w = total_w / n as f64;

    let node_views = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let cx = col_w * (i as f64 + 0.5);
            let cy = 70.0;
            let ring_cls = if node.ok {
                "deployment-node-ring deployment-node-ring--ok"
            } else {
                "deployment-node-ring deployment-node-ring--warn"
            };
            let stroke = if node.ok { "var(--success)" } else { "var(--warning)" };
            view! {
                <g>
                    <circle cx=cx cy=cy r="34" fill="var(--bg-surface-low)" stroke=stroke stroke-width="1.5"/>
                    <circle cx=cx cy=cy r="38" class=ring_cls/>
                    <text x=cx y=cy + 6.0 text-anchor="middle" font-size="20" fill="var(--text)">
                        {node.glyph.clone()}
                    </text>
                    <text x=cx y=cy + 56.0 class="deployment-node-label">{node.label.clone()}</text>
                    <text x=cx y=cy + 74.0 class="deployment-node-sub">{node.sub.clone()}</text>
                </g>
            }
        })
        .collect::<Vec<_>>();

    let links = (0..n.saturating_sub(1))
        .map(|i| {
            let x1 = col_w * (i as f64 + 0.5) + 38.0;
            let x2 = col_w * (i as f64 + 1.5) - 38.0;
            let y = 70.0;
            view! {
                <>
                    <line x1=x1 y1=y x2=x2 y2=y class="deployment-link-base"/>
                    <line x1=x1 y1=y x2=x2 y2=y class="deployment-link-flow" stroke="var(--accent)"/>
                </>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="deployment-diagram">
            <svg width="100%" height=h viewBox=format!("0 0 {:.0} {:.0}", total_w, h) style="display:block">
                {links}
                {node_views}
            </svg>
        </div>
    }
}

#[component]
pub fn RiversSearch(
    #[prop(into)] value: Signal<String>,
    #[prop(into)] on_input: Callback<String>,
    #[prop(optional, into, default = "Search…".to_string())] placeholder: String,
) -> impl IntoView {
    view! {
        <div class="rv-search">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="11" cy="11" r="8"/>
                <line x1="21" y1="21" x2="16.65" y2="16.65"/>
            </svg>
            <input
                type="text"
                placeholder=placeholder
                prop:value=move || value.get()
                on:input=move |ev| on_input.run(event_target_value(&ev))
            />
        </div>
    }
}

#[component]
pub fn FilterPillGroup(
    #[prop(into)] label: String,
    #[prop(into)] items: Vec<(String, Option<usize>)>,
    #[prop(into)] active: Signal<String>,
    #[prop(into)] on_select: Callback<String>,
) -> impl IntoView {
    view! {
        <div class="filter-pill-group">
            <span class="filter-pill-group-label">{label}</span>
            {items.into_iter().map(|(id, count)| {
                let id_for_cls = id.clone();
                let id_for_cb = id.clone();
                let cb = on_select;
                let cls = move || {
                    if active.get() == id_for_cls {
                        "filter-pill filter-pill--active"
                    } else {
                        "filter-pill"
                    }
                };
                view! {
                    <button class=cls on:click=move |_| cb.run(id_for_cb.clone())>
                        {id.clone()}
                        {count.map(|n| view! { <span class="count">{n}</span> })}
                    </button>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

/// Underline-style tab bar (replaces the Material-style `.tab-bar`).
/// Items are `(id, label, optional count)`.
#[component]
pub fn UnderlineTabs(
    #[prop(into)] tabs: Vec<(String, String, Option<usize>)>,
    #[prop(into)] active: Signal<String>,
    #[prop(into)] on_select: Callback<String>,
) -> impl IntoView {
    view! {
        <div class="tabs-underline">
            {tabs.into_iter().map(|(id, label, count)| {
                let id_for_cls = id.clone();
                let id_for_cb = id.clone();
                let cb = on_select;
                let cls = move || {
                    if active.get() == id_for_cls {
                        "tab active"
                    } else {
                        "tab"
                    }
                };
                view! {
                    <button class=cls on:click=move |_| cb.run(id_for_cb.clone())>
                        {label}
                        {count.map(|n| view! { <span class="tab-count">{n}</span> })}
                    </button>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

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
    let (lns, lnm) = crate::loc::use_current_location().get();
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

/// Partition summary cell — `[D] 12 of 365` style, with hover title for full key.
///
/// Scheme is derived from the partition key format: a hyphenated ISO date like
/// `2025-04-19` → `D` (daily), an ISO datetime like `2025-04-19T14` → `H`
/// (hourly), and anything else → `·` (custom/static).
#[component]
pub fn PartitionCell(
    /// 'D' daily, 'H' hourly, '·' static/no partitioning.
    #[prop(into, default = "·".to_string())]
    scheme: String,
    #[prop(optional, into)] count_label: Option<String>,
) -> impl IntoView {
    let tip = match scheme.as_str() {
        "D" => "daily partition",
        "H" => "hourly partition",
        _ => "static / no partitioning",
    }
    .to_string();
    view! {
        <span class="partition-cell">
            <span class="partition-cell-badge" data-tip=tip>{scheme}</span>
            {count_label.map(|c| view! { <span class="partition-cell-count">{c}</span> })}
        </span>
    }
}

pub fn partition_scheme_for(key: &str) -> &'static str {
    // Daily: "YYYY-MM-DD"
    if key.len() == 10 && &key[4..5] == "-" && &key[7..8] == "-" {
        return "D";
    }
    // Hourly: contains "T" separator
    if key.contains('T') && key.len() >= 13 {
        return "H";
    }
    "·"
}

/// Duration cell: mono-font human label (e.g. `"2h 14m"`) with the precise
/// clock form (e.g. `"02:14:08"`) surfaced as a hover tooltip. Styled with a
/// `copy` cursor to hint that hover reveals the full form.
#[component]
pub fn DurationCell(
    /// Human label e.g. "2h 14m".
    #[prop(into)]
    human: String,
    /// Clock form e.g. "02:14:08". When empty, the human label doubles as the tooltip.
    #[prop(into, default = String::new())]
    clock: String,
) -> impl IntoView {
    let tip = if clock.is_empty() {
        human.clone()
    } else {
        clock
    };
    view! {
        <span
            class="grid-cell-muted"
            title=tip
            style="cursor:help; font-family:'JetBrains Mono',monospace"
        >
            {human}
        </span>
    }
}

/// "Launched by" composite cell: icon glyph + short label + optional sub-line
/// (e.g. the schedule name or job name). Driven by the first-class
/// `LaunchedBy` field on the run record.
#[component]
pub fn LaunchedByCell(
    launched_by: crate::types::LaunchedBy,
    /// Optional override sub-line (e.g. a non-default job name for manual runs).
    #[prop(optional)]
    sub: Option<String>,
) -> impl IntoView {
    let (glyph, color, label, payload) = crate::helpers::launched_by_display(&launched_by);
    let sub_line = sub.or(payload);
    view! {
        <span style="display:flex; align-items:center; gap:8px; min-width:0">
            <span style=format!("color:{color}; font-size:12px; flex-shrink:0; width:14px; text-align:center")>{glyph}</span>
            <span style="display:flex; flex-direction:column; min-width:0; gap:1px">
                <span class="grid-cell-mono" style="color:var(--text); font-size:12px">{label}</span>
                {sub_line.map(|s| view! {
                    <span class="grid-cell-muted" style="font-size:10.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">{s}</span>
                })}
            </span>
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
    let (lns, lnm) = crate::loc::use_current_location().get();
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

/// Run/backfill chip list for tick history rows.
///
/// Prefers backfill chips over raw run chips (sub-runs are an implementation
/// detail of the backfill). Renders a dash when neither list has entries.
/// Shared by schedule-detail and sensor-detail tick grids.
#[component]
pub fn TickRunChips(run_ids: Vec<String>, backfill_ids: Vec<String>) -> impl IntoView {
    let (lns, lnm) = crate::loc::use_current_location().get();
    if !backfill_ids.is_empty() {
        let lns_b = lns.clone();
        let lnm_b = lnm.clone();
        view! {
            <span style="display:flex; flex-wrap:wrap; gap:4px">
                {backfill_ids.into_iter().map(move |bid| {
                    let href = crate::loc::loc_path(&lns_b, &lnm_b, &format!("backfills/{}", bid));
                    let short = crate::helpers::short_id(&bid, 10);
                    let title = format!("Backfill {bid}");
                    view! {
                        <A
                            href=href
                            attr:class="tag tag--backfill"
                            attr:style="font-size:10.5px"
                            attr:title=title
                        >
                            <span class="chip-backfill-prefix">"BF"</span>
                            {short}
                        </A>
                    }
                }).collect::<Vec<_>>()}
            </span>
        }
        .into_any()
    } else if !run_ids.is_empty() {
        view! {
            <span style="display:flex; flex-wrap:wrap; gap:4px">
                {run_ids.into_iter().map(move |id| {
                    let href = crate::loc::loc_path(&lns, &lnm, &format!("runs/{}", id));
                    let short = crate::helpers::short_id(&id, 8);
                    view! {
                        <A href=href attr:class="tag" attr:style="font-size:10.5px">{short}</A>
                    }
                }).collect::<Vec<_>>()}
            </span>
        }
        .into_any()
    } else {
        view! {
            <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px">"—"</span>
        }
        .into_any()
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

/// 20-bar throughput histogram — color-gradient across age (older→surface-highest,
/// mid→secondary-dim, newest→secondary with glow). Heights are normalized to max.
#[component]
pub fn ThroughputBars(
    #[prop(into)] points: Vec<f64>,
    #[prop(optional, default = 64)] height_px: u32,
) -> impl IntoView {
    if points.is_empty() {
        return view! { <div style=format!("height:{height_px}px")></div> }.into_any();
    }
    let max = points.iter().cloned().fold(0.0_f64, f64::max).max(1.0);
    let n = points.len();
    let bars = points
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            let pct = (v / max * 100.0).clamp(0.0, 100.0);
            let is_now = i + 1 == n;
            let is_recent = !is_now && i + 4 >= n;
            let cls = if is_now {
                "bar bar--now"
            } else if is_recent {
                "bar bar--recent"
            } else {
                "bar"
            };
            view! { <div class=cls style=format!("height:{pct:.1}%")></div> }
        })
        .collect::<Vec<_>>();
    view! {
        <div class="throughput-bars" style=format!("height:{height_px}px")>{bars}</div>
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

#[component]
pub fn DeployRow(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional)] mono: bool,
) -> impl IntoView {
    let val_cls = if mono {
        "deploy-row-value deploy-row-value--mono"
    } else {
        "deploy-row-value"
    };
    view! {
        <div class="deploy-row">
            <span class="deploy-row-label">{label}</span>
            <span class=val_cls>{value}</span>
        </div>
    }
}

#[component]
pub fn DeployCard(
    #[prop(into)] label: String,
    #[prop(optional, into)] status_label: Option<String>,
    #[prop(optional)] status_ok: bool,
    children: Children,
) -> impl IntoView {
    let status_chip = status_label.map(|text| {
        let dot_cls = if status_ok {
            "dot dot-healthy"
        } else {
            "dot dot-warn"
        };
        view! {
            <span class="chip">
                <span class=dot_cls></span>
                {text}
            </span>
        }
    });
    view! {
        <div class="deploy-card">
            <div class="deploy-card-head">
                <span class="deploy-card-label">{label}</span>
                {status_chip}
            </div>
            <div class="deploy-card-body">{children()}</div>
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
                <strong>{count}</strong> " " {plural} " need" {(count != 1).then_some("")} " attention"
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

#[component]
pub fn Tag(
    #[prop(into)] label: String,
    #[prop(optional, into)] color: Option<String>,
) -> impl IntoView {
    let style = color.map(|c| format!("color:{c}"));
    view! { <span class="rv-tag" style=style>{label}</span> }
}

/// Kind badge — colored by language family (`python` / `sql` / `api`).
#[component]
pub fn KindBadge(#[prop(into)] kind: String) -> impl IntoView {
    let k = kind.to_ascii_lowercase();
    let cls = match k.as_str() {
        "python" | "py" => "kind-badge kind-badge--python",
        "sql" => "kind-badge kind-badge--sql",
        "api" | "http" => "kind-badge kind-badge--api",
        _ => "kind-badge kind-badge--other",
    };
    view! { <span class=cls>{kind}</span> }
}

#[component]
pub fn ProgressBar(
    /// 0.0 .. 1.0
    #[prop(into)]
    value: Signal<f64>,
    #[prop(optional, into, default = "var(--secondary)".to_string())] color: String,
    #[prop(optional, default = 4)] height_px: u32,
) -> impl IntoView {
    let bar_style = format!("height:{height_px}px");
    let color_c = color.clone();
    let fill_style = move || {
        let pct = (value.get() * 100.0).clamp(0.0, 100.0);
        format!("width:{pct:.1}%; background:{color_c}")
    };
    view! {
        <div class="rv-progress" style=bar_style>
            <div class="rv-progress-fill" style=fill_style></div>
        </div>
    }
}

#[derive(Clone, Debug)]
pub struct LayerRollup {
    pub name: String,
    pub fresh: usize,
    pub stale: usize,
    pub failed: usize,
    pub total: usize,
    pub spark: Vec<f64>,
    pub materializations_24h: usize,
}

/// Warehouse-state summary strip: one card per asset layer with stacked
/// fresh/stale/failed bar, 24h sparkline, and a materializations count.
#[component]
pub fn AssetsHeroStrip(
    #[prop(into)] layers: Vec<LayerRollup>,
    #[prop(optional, into)] active_layer: Option<Signal<Option<String>>>,
    #[prop(optional)] on_select: Option<Callback<String>>,
) -> impl IntoView {
    if layers.is_empty() {
        return view! { <></> }.into_any();
    }
    let cols = layers.len();
    let rollup_row = layers
        .into_iter()
        .map(|l| {
            let is_active = active_layer
                .as_ref()
                .and_then(|s| s.get())
                .map(|a| a == l.name)
                .unwrap_or(false);
            let cls = if is_active {
                "hero-strip-card hero-strip-card--active"
            } else {
                "hero-strip-card"
            };
            let total = l.total.max(1) as f64;
            let fresh_pct = l.fresh as f64 / total * 100.0;
            let stale_pct = l.stale as f64 / total * 100.0;
            let failed_pct = l.failed as f64 / total * 100.0;
            let mats = l.materializations_24h;

            // Build sparkline path
            let spark_path = if l.spark.is_empty() {
                String::new()
            } else {
                let w = 96.0;
                let h = 22.0;
                let step = w / (l.spark.len() - 1).max(1) as f64;
                let max = l.spark.iter().cloned().fold(0.0_f64, f64::max).max(1e-9);
                l.spark
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let x = i as f64 * step;
                        let y = 26.0 - (v / max) * h;
                        if i == 0 {
                            format!("M{x:.1},{y:.1}")
                        } else {
                            format!(" L{x:.1},{y:.1}")
                        }
                    })
                    .collect::<String>()
            };
            let spark_area = if spark_path.is_empty() {
                String::new()
            } else {
                format!("{spark_path} L96,28 L0,28 Z")
            };
            let name_click = l.name.clone();
            let cb = on_select;
            view! {
                <button
                    class=cls
                    on:click=move |_| { if let Some(c) = cb { c.run(name_click.clone()); } }
                >
                    <div class="hero-strip-card-head">
                        <span class="hero-strip-card-name">{l.name.clone()}</span>
                        <span class="hero-strip-card-count">{l.total}</span>
                    </div>
                    <div class="hero-strip-card-stacked">
                        <span style=format!("width:{fresh_pct:.1}%; background:var(--success)")></span>
                        <span style=format!("width:{stale_pct:.1}%; background:var(--warning)")></span>
                        <span style=format!("width:{failed_pct:.1}%; background:var(--error)")></span>
                    </div>
                    <svg width="100%" height="28" viewBox="0 0 96 28" preserveAspectRatio="none" style="display:block">
                        <path d=spark_area fill="rgba(255,143,120,0.2)"/>
                        <path d=spark_path stroke="var(--accent)" stroke-width="1" fill="none"/>
                    </svg>
                    <div class="hero-strip-card-foot">
                        <span>{mats} " mats · 24h"</span>
                        <span>{format!("{:.0}% fresh", fresh_pct)}</span>
                    </div>
                </button>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="hero-strip">
            <div class="hero-strip-head">
                <div>
                    <div class="section-header-label">"WAREHOUSE STATE"</div>
                    <div style="font-family:'JetBrains Mono',monospace; font-size:11px; color:var(--text-muted); margin-top:2px">
                        "last 24h · click a layer to filter"
                    </div>
                </div>
                <div class="hero-strip-legend">
                    <span><span class="hero-strip-legend-swatch" style="background:var(--success)"></span>"fresh"</span>
                    <span><span class="hero-strip-legend-swatch" style="background:var(--warning)"></span>"stale"</span>
                    <span><span class="hero-strip-legend-swatch" style="background:var(--error)"></span>"failed"</span>
                </div>
            </div>
            <div class="hero-strip-row" style=format!("grid-template-columns:repeat({cols}, 1fr)")>
                {rollup_row}
            </div>
        </div>
    }.into_any()
}

/// Blast-radius banner: how many downstream assets/jobs/dashboards depend on
/// this one. Shows a CRITICAL pill when the fan-out is large. Clicking
/// "Show list" expands a grid of descendant asset keys.
#[component]
pub fn WhoDependsBanner(
    #[prop(into)] downstream_assets: usize,
    #[prop(optional, default = 0)] jobs: usize,
    #[prop(optional, default = 0)] dashboards: usize,
    #[prop(optional)] is_critical: bool,
    #[prop(optional, into)] open_href: Option<String>,
    /// Optional descendant asset keys to render in the expand panel.
    #[prop(optional)]
    descendants: Vec<String>,
) -> impl IntoView {
    if downstream_assets == 0 && jobs == 0 && dashboards == 0 {
        return view! {
            <div class="blast-banner-leaf">
                <span>"✓"</span>
                <span>"No downstream assets, jobs, or dashboards depend on this — leaf asset."</span>
            </div>
        }
        .into_any();
    }

    let (open, set_open) = signal(false);
    let has_list = !descendants.is_empty();
    let cls = if is_critical {
        "blast-banner blast-banner--critical"
    } else {
        "blast-banner"
    };
    view! {
        <div class=cls>
            <div class="blast-banner-head">
                <div style="display:flex; align-items:center; gap:6px">
                    <span class="blast-banner-label">"blast radius"</span>
                    {is_critical.then(|| view! {
                        <span class="blast-banner-pill">"CRITICAL"</span>
                    })}
                </div>
                <div class="blast-banner-counts">
                    <span><strong>{downstream_assets}</strong> " " <span class="muted">
                        {if downstream_assets == 1 { "asset" } else { "assets" }}
                    </span></span>
                    <span><strong>{jobs}</strong> " " <span class="muted">
                        {if jobs == 1 { "job" } else { "jobs" }}
                    </span></span>
                    <span><strong>{dashboards}</strong> " " <span class="muted">
                        {if dashboards == 1 { "dashboard" } else { "dashboards" }}
                    </span></span>
                    <span class="muted">"depend on this"</span>
                </div>
                <div style="flex:1"></div>
                {has_list.then(|| view! {
                    <button
                        on:click=move |_| set_open.update(|v| *v = !*v)
                        style="background:transparent; color:var(--text-muted); font-family:'JetBrains Mono',monospace; font-size:11px; border:0; cursor:pointer"
                    >
                        {move || if open.get() { "Hide list" } else { "Show list" }}
                    </button>
                })}
                {open_href.map(|href| view! {
                    <A href=href attr:style="font-family:'JetBrains Mono',monospace; font-size:11px; color:var(--accent)">
                        "open in lineage →"
                    </A>
                })}
            </div>
            <Show when=move || open.get() && has_list>
                <div style="margin-top:12px; padding-top:12px; border-top:1px solid var(--bg-highest)">
                    <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(220px, 1fr)); gap:6px">
                        {
                            let (lns, lnm) = crate::loc::use_current_location().get();
                            descendants.iter().take(18).map(move |k| {
                            let href = crate::loc::loc_path(&lns, &lnm, &format!("assets/{}", k));
                            let label = k.clone();
                            view! {
                                <A href=href attr:style="display:flex; gap:8px; padding:6px 10px; background:var(--bg-surface); border-radius:3px; border-left:2px solid var(--secondary); font-family:'JetBrains Mono',monospace; font-size:11.5px; color:var(--text); text-decoration:none; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                    <span style="color:var(--text-muted)">"→"</span>
                                    {label}
                                </A>
                            }
                        }).collect::<Vec<_>>()
                        }
                    </div>
                    {(descendants.len() > 18).then(|| view! {
                        <div style="font-family:'JetBrains Mono',monospace; font-size:11px; color:var(--text-muted); margin-top:8px; text-align:center">
                            {format!("+{} more", descendants.len() - 18)}
                        </div>
                    })}
                </div>
            </Show>
        </div>
    }
    .into_any()
}

#[derive(Clone, Debug)]
pub struct GlyphEvent {
    /// Minutes before "now" (0 = now, horizon = oldest shown).
    pub minutes_ago: f64,
    /// "◆" materialization, "▲" failure, "◐" retry, "○" observation.
    pub glyph: &'static str,
    pub status: &'static str, // "ok" | "err" | "warn" | "info"
    /// Optional run-id; if `selected_run` matches this, event stays vivid.
    pub run: Option<String>,
    pub label: String,
}

/// Horizontal time axis with glyph-plotted events.
/// Oldest on the left, "now" on the right.
#[component]
pub fn EventGlyphTimeline(
    #[prop(into)] events: Vec<GlyphEvent>,
    #[prop(optional, default = 180.0)] horizon_minutes: f64,
    #[prop(optional, into)] selected_run: Option<String>,
) -> impl IntoView {
    let ticks = [0, 30, 60, 90, 120, 150, 180]
        .iter()
        .map(|m| {
            let pct = 100.0 - (*m as f64 / horizon_minutes * 100.0);
            view! {
                <span class="glyph-timeline-tick" style=format!("left:{pct:.1}%")>
                    {if *m == 0 { "now".to_string() } else { format!("-{m}m") }}
                </span>
            }
        })
        .collect::<Vec<_>>();

    // Distinct runs referenced in the events — assign a palette color per run
    // so the per-event run-bar matches the runs legend at the bottom.
    let run_palette = [
        "var(--accent)",
        "var(--secondary)",
        "#d4a5ff",
        "#f5b342",
        "#7dd67a",
    ];
    let mut run_ids: Vec<String> = Vec::new();
    for e in &events {
        if let Some(ref r) = e.run
            && !r.is_empty()
            && !run_ids.contains(r)
        {
            run_ids.push(r.clone());
        }
    }
    let run_color_for = |r: &str| -> &'static str {
        let idx = run_ids.iter().position(|x| x == r).unwrap_or(0);
        run_palette[idx % run_palette.len()]
    };

    let sel = selected_run.as_ref().cloned();
    let events_v = events
        .iter()
        .map(|e| {
            let pct = 100.0 - (e.minutes_ago / horizon_minutes * 100.0).clamp(0.0, 100.0);
            let icon_cls = match e.status {
                "err" => "glyph-event-icon glyph-event-icon--err",
                "warn" => "glyph-event-icon glyph-event-icon--warn",
                "info" => "glyph-event-icon glyph-event-icon--info",
                _ => "glyph-event-icon glyph-event-icon--ok",
            };
            let bar_color = e
                .run
                .as_deref()
                .map(run_color_for)
                .unwrap_or("var(--text-muted)");
            let dim = match (&sel, &e.run) {
                (Some(want), Some(have)) => want != have,
                (Some(_), None) => true,
                _ => false,
            };
            let cls = if dim {
                "glyph-event glyph-event--dim"
            } else {
                "glyph-event"
            };
            view! {
                <div class=cls style=format!("left:{pct:.1}%") title=e.label.clone()>
                    <span class=icon_cls>{e.glyph}</span>
                    <span class="glyph-event-bar" style=format!("background:{bar_color}")></span>
                </div>
            }
        })
        .collect::<Vec<_>>();

    // Glyph legend — fixed set matching Rivers' cmap (MAT/FAI/OBS colored)
    let glyph_legend: Vec<_> = [
        ("◆", "MAT", "var(--success)"),
        ("▲", "FAI", "var(--error)"),
        ("◐", "RET", "var(--warning)"),
        ("○", "OBS", "var(--secondary)"),
    ]
    .iter()
    .map(|(g, label, color)| {
        view! {
            <span class="glyph-legend-item">
                <span class="glyph-legend-icon" style=format!("color:{color}")>{*g}</span>
                <span class="glyph-legend-label">{*label}</span>
            </span>
        }
    })
    .collect();

    // Runs legend — one swatch per distinct run id present in events
    let runs_legend = run_ids.iter().map(|r| {
        let color = run_color_for(r);
        let short = crate::helpers::short_id(r, 8);
        view! {
            <span class="glyph-runs-legend-item" title=r.clone()>
                <span class="glyph-runs-legend-swatch" style=format!("background:{color}")></span>
                <span>{format!("#{short}")}</span>
            </span>
        }
    }).collect::<Vec<_>>();
    let show_runs_legend = !run_ids.is_empty();

    view! {
        <div class="glyph-timeline-panel">
            <div class="glyph-timeline-header">
                <span class="section-header-label">"EVENT TIMELINE · LAST 3H"</span>
                <div class="glyph-legend">{glyph_legend}</div>
            </div>
            <div class="glyph-timeline">
                <span class="glyph-timeline-axis"></span>
                {ticks}
                {events_v}
            </div>
            <Show when=move || show_runs_legend>
                <div class="glyph-runs-legend">
                    <span class="section-header-label">"RUNS"</span>
                    {runs_legend.clone()}
                </div>
            </Show>
        </div>
    }
}
