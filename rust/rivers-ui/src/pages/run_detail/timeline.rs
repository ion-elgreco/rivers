use leptos::prelude::*;

use crate::components::ui_kit::EmptyState;
use crate::now::use_now;
use crate::types::{EventType, StoredEvent};

use super::dag_view::RunDagView;

#[derive(Clone)]
struct GanttStep {
    asset: String,
    start: i64,
    end: Option<i64>,
    status: StepStatus,
}

#[derive(Clone, PartialEq)]
pub(super) enum StepStatus {
    Running,
    Success,
    Failure,
}

struct AssetEvents {
    start: Option<i64>,
    end: Option<i64>,
    status: Option<StepStatus>,
}

/// Groups all events by asset first, so the result is correct regardless of event ordering.
fn build_gantt_steps(events: &[StoredEvent]) -> Vec<GanttStep> {
    use std::collections::HashMap;

    let mut by_asset: HashMap<String, AssetEvents> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for evt in events {
        let asset = match &evt.asset_key {
            Some(k) => k.clone(),
            None => continue,
        };

        let entry = by_asset.entry(asset.clone()).or_insert_with(|| {
            order.push(asset.clone());
            AssetEvents {
                start: None,
                end: None,
                status: None,
            }
        });

        match evt.event_type {
            EventType::StepStart => {
                entry.start = Some(entry.start.map_or(evt.timestamp, |s| s.min(evt.timestamp)));
            }
            EventType::StepSuccess => {
                entry.end = Some(entry.end.map_or(evt.timestamp, |e| e.max(evt.timestamp)));
                entry.status = Some(StepStatus::Success);
            }
            EventType::StepFailure => {
                entry.end = Some(entry.end.map_or(evt.timestamp, |e| e.max(evt.timestamp)));
                entry.status = Some(StepStatus::Failure);
            }
            _ => {
                if entry.end.is_none() || entry.end < Some(evt.timestamp) {
                    entry.end = Some(evt.timestamp);
                }
            }
        }
    }

    let mut steps: Vec<GanttStep> = Vec::new();
    for asset in order {
        if let Some(acc) = by_asset.remove(&asset) {
            let status = acc.status.unwrap_or(StepStatus::Running);
            let start = acc.start.unwrap_or_else(|| acc.end.unwrap_or(0));
            let end = if matches!(status, StepStatus::Running) {
                None
            } else {
                acc.end.or(acc.start)
            };
            steps.push(GanttStep {
                asset,
                start,
                end,
                status,
            });
        }
    }

    steps.sort_by_key(|s| s.start);
    steps
}

/// A "ghost: last run" overlay (previous-run delta bars + legend) was scaffolded
/// here; see [`docs/deferred/run-detail-ghost-overlay.md`] for the full design
/// and how to reintroduce it once real previous-run data lands.
#[component]
pub(super) fn RunTimelinePanel(
    events: Vec<StoredEvent>,
    run_start: Option<i64>,
    node_names: Vec<String>,
    topology: Option<crate::types::GraphTopology>,
    selected_step: ReadSignal<Option<String>>,
    on_select: WriteSignal<Option<String>>,
    view_mode: ReadSignal<String>,
    set_view_mode: WriteSignal<String>,
) -> impl IntoView {
    let steps = build_gantt_steps(&events);
    let has_steps = !steps.is_empty();

    // Anchor the displayed window to the STEP execution range, not the run-level
    // start_time. Otherwise fast runs (or runs with significant queue delay before
    // the first step) push every bar to the right edge at ~100%. Using min(step.start)
    // as range_start keeps bars left-aligned regardless of total duration.
    let range_start = steps
        .iter()
        .map(|s| s.start)
        .min()
        .unwrap_or_else(|| run_start.unwrap_or(0));

    let header_label = Signal::derive(move || {
        if view_mode.get() == "dag" {
            "EXECUTION GRAPH · dag"
        } else {
            "TASK TIMELINE · gantt"
        }
    });
    let show_gantt = Signal::derive(move || view_mode.get() != "dag");

    let asset_set: std::collections::HashSet<String> = node_names.iter().cloned().collect();
    let dag_layout: Option<crate::components::dag::layout::LayoutResult> =
        topology.as_ref().map(|topo| {
            let nodes: Vec<_> = topo
                .nodes
                .iter()
                .filter(|n| asset_set.contains(&n.name))
                .cloned()
                .collect();
            let subset: std::collections::HashSet<&str> =
                nodes.iter().map(|n| n.name.as_str()).collect();
            let edges: Vec<_> = topo
                .edges
                .iter()
                .filter(|(a, b)| subset.contains(a.as_str()) && subset.contains(b.as_str()))
                .cloned()
                .collect();
            let subset_topo = crate::types::GraphTopology { nodes, edges };
            crate::components::dag::layout::compute_layout(&subset_topo, true)
        });

    let mut status_by_asset: std::collections::HashMap<String, StepStatus> =
        std::collections::HashMap::new();
    for s in &steps {
        status_by_asset.insert(s.asset.clone(), s.status.clone());
    }

    let now_signal = use_now();

    // Reactive body: re-runs once per `now` tick. For finished steps the
    // recomputed values are identical to last tick's; for in-flight steps
    // (`end == None`) the bar widths and duration labels grow each second.
    // The Show, RunGanttBody, and RunDagView all re-instantiate per tick —
    // they're stateless given their inputs, and Leptos diffs the DOM.
    let body_view = {
        let steps = steps.clone();
        let dag_layout = dag_layout.clone();
        let status_by_asset = status_by_asset.clone();
        move || {
            let now_secs = now_signal.get();
            let now_ns = now_secs.saturating_mul(1_000_000_000);
            let range_end = steps
                .iter()
                .map(|s| s.end.unwrap_or(now_ns))
                .max()
                .unwrap_or(now_ns);
            let total_ns = (range_end - range_start).max(1) as f64;
            let total_secs = total_ns / 1e9;

            let tick_count = 8;
            let ticks_secs: Vec<(f64, f64)> = (0..=tick_count)
                .map(|i| {
                    let pct = i as f64 / tick_count as f64;
                    (pct, pct * total_secs)
                })
                .collect();

            let lane_rows: Vec<LaneRow> = steps
                .iter()
                .map(|s| {
                    let cur_dur_ns = s.end.unwrap_or(now_ns) - s.start;
                    let start_pct = (s.start - range_start) as f64 / total_ns * 100.0;
                    let width_pct = (cur_dur_ns as f64 / total_ns) * 100.0;
                    LaneRow {
                        asset: s.asset.clone(),
                        status: s.status.clone(),
                        start_pct,
                        width_pct,
                    }
                })
                .collect();

            let mut duration_by_asset: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            for s in &steps {
                let dur_ns = s.end.unwrap_or(now_ns) - s.start;
                let dur_secs = (dur_ns as f64 / 1e9).max(0.0);
                duration_by_asset.insert(s.asset.clone(), fmt_dur_short(dur_secs));
            }

            let dl = dag_layout.clone();
            let statuses = status_by_asset.clone();
            view! {
                <Show
                    when=move || show_gantt.get()
                    fallback={
                        let dl = dl.clone();
                        let statuses = statuses.clone();
                        let durations = duration_by_asset.clone();
                        move || {
                            match dl.clone() {
                                Some(layout) if !layout.nodes.is_empty() => view! {
                                    <RunDagView
                                        layout=layout
                                        status_by_asset=statuses.clone()
                                        duration_by_asset=durations.clone()
                                        selected_step=selected_step
                                        on_select=on_select
                                    />
                                }.into_any(),
                                _ => view! {
                                    <div class="run-dag-placeholder">
                                        <div class="run-dag-placeholder-title">"No lineage"</div>
                                        <div class="run-dag-placeholder-hint">"Could not resolve asset dependencies for this run."</div>
                                    </div>
                                }.into_any(),
                            }
                        }
                    }
                >
                    <RunGanttBody
                        lanes=lane_rows.clone()
                        ticks=ticks_secs.clone()
                        selected_step=selected_step
                        on_select=on_select
                    />
                </Show>
            }
        }
    };

    view! {
        <div class="run-view-panel">
            <div class="run-view-panel-header">
                <span class="section-header-label">{move || header_label.get()}</span>
                <div class="run-view-panel-actions">
                    <div class="filter-pill-group">
                        {[("dag", "DAG"), ("gantt", "Gantt")].into_iter().map(|(v, label)| {
                            let vs = v.to_string();
                            let vs_for_cls = vs.clone();
                            let vs_for_aria = vs.clone();
                            view! {
                                <button
                                    class=move || if view_mode.get() == vs_for_cls { "filter-pill filter-pill--active" } else { "filter-pill" }
                                    aria-pressed=move || (view_mode.get() == vs_for_aria).to_string()
                                    on:click=move |_| set_view_mode.set(vs.clone())
                                >{label}</button>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            </div>
            {if !has_steps {
                view! { <div class="run-view-empty"><EmptyState message="No steps recorded yet" compact=true/></div> }.into_any()
            } else {
                view! { {body_view} }.into_any()
            }}
        </div>
    }
}

#[derive(Clone)]
struct LaneRow {
    asset: String,
    status: StepStatus,
    start_pct: f64,
    width_pct: f64,
}

/// Finer units for shorter spans, so the axis ticks of a fast run stay distinct.
pub(super) fn fmt_dur_short(secs: f64) -> String {
    let abs = secs.abs();
    if abs == 0.0 {
        "0ms".to_string()
    } else if abs < 0.001 {
        format!("{}µs", (abs * 1e6).round() as i64)
    } else if abs < 0.01 {
        format!("{:.1}ms", abs * 1000.0)
    } else if abs < 1.0 {
        format!("{}ms", (abs * 1000.0).round() as i64)
    } else if abs < 10.0 {
        format!("{:.1}s", abs)
    } else if abs < 60.0 {
        format!("{}s", abs.round() as i64)
    } else {
        let total = abs.round() as i64;
        format!("{}m {}s", total / 60, total % 60)
    }
}

#[component]
fn RunGanttBody(
    lanes: Vec<LaneRow>,
    ticks: Vec<(f64, f64)>,
    selected_step: ReadSignal<Option<String>>,
    on_select: WriteSignal<Option<String>>,
) -> impl IntoView {
    let tick_view: Vec<_> = ticks
        .iter()
        .map(|(pct, secs)| {
            let left = format!("left: {:.2}%", pct * 100.0);
            let label = fmt_dur_short(*secs);
            view! {
                <div class="gantt-tick" style=left>
                    <span class="gantt-tick-label">{label}</span>
                    <span class="gantt-tick-mark"></span>
                </div>
            }
        })
        .collect();

    let lane_views: Vec<_> = lanes
        .into_iter()
        .map(|l| {
            let asset_for_select = l.asset.clone();
            let asset_for_cls = l.asset.clone();
            let is_selected = Signal::derive(move || {
                selected_step.get().as_ref() == Some(&asset_for_cls)
            });
            let status_cls = match l.status {
                StepStatus::Success => "success",
                StepStatus::Running => "running",
                StepStatus::Failure => "failed",
            };
            let row_cls = move || {
                if is_selected.get() {
                    "gantt-lane gantt-lane--selected"
                } else {
                    "gantt-lane"
                }
            };
            let dot_cls = format!("gantt-lane-dot gantt-lane-dot--{status_cls}");
            let bar_cls = format!("gantt-lane-bar gantt-lane-bar--{status_cls}");
            let bar_style = format!(
                "left: {:.2}%; width: {:.2}%",
                l.start_pct, l.width_pct
            );
            let asset_label = l.asset.clone();
            let is_running = matches!(l.status, StepStatus::Running);

            view! {
                <div
                    class=row_cls
                    on:click=move |_| on_select.set(Some(asset_for_select.clone()))
                >
                    <div class="gantt-lane-label">
                        <span class=dot_cls></span>
                        <span class="gantt-lane-name" title=asset_label.clone()>{asset_label.clone()}</span>
                    </div>
                    <div class="gantt-lane-track">
                        <div class="gantt-lane-baseline"></div>
                        <div class=bar_cls.clone() style=bar_style>
                            {is_running.then(|| view! { <div class="gantt-lane-bar-stripes"></div> })}
                        </div>
                    </div>
                </div>
            }
        })
        .collect();

    view! {
        <div class="gantt-body">
            <div class="gantt-axis">
                {tick_view}
                <div class="gantt-axis-baseline"></div>
            </div>
            <div class="gantt-lanes">
                {lane_views}
            </div>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_spans_keep_axis_ticks_distinct() {
        let labels: Vec<String> = (0..=8)
            .map(|i| fmt_dur_short(i as f64 * 0.001 / 8.0))
            .collect();
        let unique: std::collections::HashSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len(), "{labels:?}");
        assert_eq!(fmt_dur_short(0.0), "0ms");
        assert_eq!(fmt_dur_short(0.000_125), "125µs");
        assert_eq!(fmt_dur_short(0.0042), "4.2ms");
        assert_eq!(fmt_dur_short(0.25), "250ms");
        assert_eq!(fmt_dur_short(3.0), "3.0s");
    }
}
