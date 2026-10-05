use leptos::prelude::*;
use leptos_router::components::A;

use super::assets::AssetStack;
use super::badges::StatusChip;
use super::partitions::{PartitionCell, partition_scheme_for};

#[derive(Clone, Debug)]
pub struct StripRun {
    pub id: String,
    /// Chip vocabulary from `run_status_kind`.
    pub kind: &'static str,
    /// `None` until the run has an end time.
    pub duration_s: Option<f64>,
}

impl StripRun {
    /// Bars for the newest `n` of `runs` (newest first), oldest on the left.
    pub fn from_runs(runs: &[crate::types::RunRecord], n: usize) -> Vec<StripRun> {
        runs.iter()
            .take(n)
            .rev()
            .map(|r| StripRun {
                id: crate::helpers::short_id(&r.run_id, 8),
                kind: crate::helpers::run_status_kind(&r.status),
                duration_s: r.end_time.map(|e| (e - r.start_time).max(0) as f64 / 1e9),
            })
            .collect()
    }
}

fn format_strip_secs(s: f64) -> String {
    if s < 60.0 {
        format!("{s:.1}s")
    } else {
        crate::helpers::format_seconds(s as i64)
    }
}

#[component]
pub fn RecentRunsStrip(
    #[prop(into)] runs: Vec<StripRun>,
    #[prop(optional, into, default = "RUN DURATIONS".to_string())] label: String,
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
    let finished: Vec<f64> = runs.iter().filter_map(|r| r.duration_s).collect();
    let max = finished.iter().copied().fold(0.0_f64, f64::max).max(1.0);
    let avg = (!finished.is_empty()).then(|| finished.iter().sum::<f64>() / finished.len() as f64);
    let ok_count = runs.iter().filter(|r| r.kind == "success").count();
    let err_count = runs.iter().filter(|r| r.kind == "failed").count();

    let bars = runs
        .iter()
        .map(|r| {
            let h = r.duration_s.map_or(8.0, |d| (d / max * 60.0).max(8.0));
            let cls = match r.kind {
                "success" => "runs-strip-bar runs-strip-bar--ok",
                "failed" => "runs-strip-bar runs-strip-bar--err",
                "canceled" => "runs-strip-bar runs-strip-bar--canceled",
                "running" => "runs-strip-bar runs-strip-bar--retry runs-strip-bar--live",
                _ => "runs-strip-bar runs-strip-bar--retry",
            };
            let x_marker = (r.kind == "failed").then(|| {
                view! {
                    <span class="runs-strip-bar-x">"✕"</span>
                }
            });
            let tip = match r.duration_s {
                Some(d) => format!("{} · {} · {}", r.id, r.kind, format_strip_secs(d)),
                None => format!("{} · {}", r.id, r.kind),
            };
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
                    <span>"avg " {avg.map_or_else(|| "—".to_string(), format_strip_secs)}</span>
                    <span style="color:var(--success)">"✓ " {ok_count}</span>
                    <span style="color:var(--error)">"✕ " {err_count}</span>
                </div>
            </div>
            <div class="runs-strip-bars">
                {avg.map(|a| {
                    let offset = (1.0 - (a / max)) * 60.0 + 8.0;
                    view! { <span class="runs-strip-avg" style=format!("top:{offset:.1}px")></span> }
                })}
                {bars}
            </div>
        </div>
    }
    .into_any()
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
            <span style=format!("color:{color}; font-size:var(--fs-sm); flex-shrink:0; width:14px; text-align:center")>{glyph}</span>
            <span style="display:flex; flex-direction:column; min-width:0; gap:1px">
                <span class="grid-cell-mono" style="color:var(--text); font-size:var(--fs-sm)">{label}</span>
                {sub_line.map(|s| view! {
                    <span class="grid-cell-muted" style="font-size:var(--fs-xs); overflow:hidden; text-overflow:ellipsis; white-space:nowrap">{s}</span>
                })}
            </span>
        </span>
    }
}

/// Top-bar feedback after a launch: a link to the new run.
#[component]
pub fn RunLaunched(#[prop(into)] run_id: String, #[prop(optional)] queued: bool) -> impl IntoView {
    let (ns, name) = crate::loc::use_current_location().get_untracked();
    let href = crate::loc::loc_path(&ns, &name, &format!("runs/{run_id}"));
    let text = format!(
        "Run {} {}",
        crate::helpers::short_id(&run_id, 8),
        if queued { "queued" } else { "started" },
    );
    view! { <A href=href attr:class="launch-result">{text}</A> }
}

/// Run history table for detail pages (job, backfill, asset). The runs page
/// keeps its own wider table with selection and launch columns.
#[component]
pub fn RunsGrid(
    rows: Vec<crate::types::RunRecord>,
    #[prop(optional)] show_assets: bool,
) -> impl IntoView {
    let (ns, name) = crate::loc::use_current_location().get_untracked();
    let grid = if show_assets {
        "grid-template-columns: 88px 0.7fr 1.5fr 0.9fr 0.8fr 0.6fr"
    } else {
        "grid-template-columns: 88px 0.7fr 1.4fr 0.8fr 0.6fr"
    };
    view! {
        <div class="grid-table">
            <div class="grid-table-head" style=grid>
                <span>"RUN"</span>
                <span>"STATUS"</span>
                {show_assets.then(|| view! { <span>"ASSETS"</span> })}
                <span>"PARTITION"</span>
                <span>"STARTED"</span>
                <span>"DURATION"</span>
            </div>
            {rows
                .into_iter()
                .map(|r| {
                    let href = crate::loc::loc_path(&ns, &name, &format!("runs/{}", r.run_id));
                    let sid = crate::helpers::short_id(&r.run_id, 8);
                    let rail_cls = format!(
                        "grid-row-rail grid-row-rail--{}",
                        crate::helpers::run_status_class(&r.status)
                    );
                    let st_kind = crate::helpers::run_status_kind(&r.status);
                    let start_ts = r.start_time;
                    let created_abs = crate::helpers::format_timestamp(Some(r.start_time));
                    let duration = crate::helpers::format_duration(Some(r.start_time), r.end_time);
                    let assets = r.node_names.clone();
                    let partition = r.partition_key.clone().map(|p| {
                        let scheme = p
                            .preview
                            .first()
                            .map(|k| partition_scheme_for(k))
                            .unwrap_or("·");
                        view! { <PartitionCell scheme=scheme count_label=p.label()/> }
                    });
                    view! {
                        <A href=href attr:class="grid-row" attr:style=grid attr:title=created_abs>
                            <span class=rail_cls></span>
                            <span class="grid-cell-mono">{sid}</span>
                            <StatusChip kind=st_kind/>
                            {show_assets.then(|| view! { <AssetStack assets=assets/> })}
                            {match partition {
                                Some(cell) => cell.into_any(),
                                None => view! { <span class="grid-cell-muted">"—"</span> }.into_any(),
                            }}
                            <span class="grid-cell-muted"><crate::now::RelTime ts=start_ts/></span>
                            <span class="grid-cell-muted">{duration}</span>
                        </A>
                    }
                })
                .collect::<Vec<_>>()}
        </div>
    }
}
