//! Run queue view page.

use leptos::prelude::*;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::ui_kit::{
    BottleneckCard, Crumb, EmptyState, LaneSpec, QueueLanes, QueuedRun, StatTile, Topbar,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::use_now;
use crate::server_fns::pools::get_queued_runs;
use crate::types::RunRecord;

fn lane_key(record: &RunRecord) -> (&'static str, &'static str, &'static str) {
    match record.block_reason.as_deref().unwrap_or("") {
        r if r.starts_with("pool:") || r.contains("gpu") || r.contains("pool") => {
            ("pool", "Pool saturation", "var(--error)")
        }
        r if r.contains("concurrency") || r.contains("limit") => {
            ("concurrency", "Concurrency limit", "var(--warning)")
        }
        r if r.contains("backfill") => ("backfill", "Backfill in progress", "var(--accent)"),
        "" => ("waiting", "Waiting to start", "var(--text-muted)"),
        _ => ("other", "Other", "var(--secondary)"),
    }
}

fn priority_label(p: i32) -> &'static str {
    if p > 0 {
        "high"
    } else if p < 0 {
        "low"
    } else {
        "normal"
    }
}

#[component]
pub fn QueuePage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let loc = use_current_location();
    let queued = Resource::new(move || refresh_tick.get(), |_| get_queued_runs());

    let live_status = use_live_kick(
        &["runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <Topbar crumbs=vec![Crumb::new("Queue")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <div class="page-header">
            <h1>"Queue"</h1>
            <p>"Runs waiting on a resource"</p>
        </div>

        <Transition fallback=move || view! { <TableSkeleton rows=6 cols=6/> }>
            {move || {
                queued.get().map(|result| match result {
                    Ok(runs) => {
                        if runs.is_empty() {
                            return view! {
                                <EmptyState
                                    message="No runs in queue"
                                    hint="cluster is caught up · nothing waiting on a resource"
                                />
                            }.into_any();
                        }
                        let count = runs.len();
                        let high = runs.iter().filter(|r| r.priority > 0).count();
                        // Bottleneck "oldest queued" refreshes on every queue
                        // refetch (SSE-driven); per-card `RelTime` ticks live.
                        let now = use_now().get();
                        let oldest = runs
                            .iter()
                            .map(|r| r.start_time)
                            .min()
                            .map(|t| crate::helpers::format_relative_time(t, now))
                            .unwrap_or_else(|| "—".into());

                        use std::collections::BTreeMap;
                        type LaneBucket = (String, String, Vec<(usize, RunRecord)>);
                        let mut buckets: BTreeMap<&'static str, LaneBucket> = BTreeMap::new();
                        for (i, r) in runs.iter().enumerate() {
                            let (key, label, color) = lane_key(r);
                            buckets
                                .entry(key)
                                .or_insert_with(|| (label.into(), color.into(), Vec::new()))
                                .2
                                .push((i + 1, r.clone()));
                        }
                        let top = buckets
                            .iter()
                            .max_by_key(|(_, (_, _, v))| v.len())
                            .map(|(_, (label, _, v))| (label.clone(), v.len()));

                        let lanes: Vec<LaneSpec> = buckets
                            .into_iter()
                            .map(|(id, (label, color, items))| {
                                let lane_runs: Vec<QueuedRun> = items
                                    .into_iter()
                                    .map(|(pos, r)| {
                                        let short_id = if r.run_id.len() > 8 {
                                            r.run_id[..8].to_string()
                                        } else {
                                            r.run_id.clone()
                                        };
                                        let job = r.job_name.clone().unwrap_or_else(|| {
                                            r.node_names.first().cloned().unwrap_or_else(|| "—".into())
                                        });
                                        let (ns, name) = loc.get();
                                        QueuedRun {
                                            id: short_id,
                                            position: pos,
                                            job,
                                            priority: priority_label(r.priority),
                                            queued_at: r.start_time,
                                            href: Some(loc_path(&ns, &name, &format!("runs/{}", r.run_id))),
                                        }
                                    })
                                    .collect();
                                LaneSpec { id: id.into(), label, color, runs: lane_runs }
                            })
                            .collect();

                        view! {
                            {top.map(|(label, n)| {
                                let title = format!("{label} · {n} runs waiting");
                                let sub = format!("largest backlog · oldest queued {oldest}");
                                let warn = n < count / 2;
                                let suggestion_label: &str = if label.contains("Pool") {
                                    "Add 2 slots"
                                } else if label.contains("Concurrency") {
                                    "Raise concurrency limit"
                                } else if label.contains("Backfill") {
                                    "Pause active backfill"
                                } else if label.contains("Waiting") {
                                    "Scale worker pool"
                                } else {
                                    "Materialize blocking assets"
                                };
                                let bars: Vec<_> = (0..24).map(|i| {
                                    let remaining = ((n as f64 - (i as f64 / 24.0) * n as f64) / n as f64 * 100.0).clamp(0.0, 100.0);
                                    let opacity = 0.25 + (remaining / 100.0) * 0.75;
                                    let color = if i < 6 { "var(--error)" }
                                        else if i < 14 { "var(--warning)" }
                                        else { "var(--success)" };
                                    view! {
                                        <div
                                            class="drain-forecast-bar"
                                            style=format!("height:{remaining:.0}%; opacity:{opacity:.2}; background:{color}")
                                        ></div>
                                    }
                                }).collect();
                                view! {
                                    <BottleneckCard title=title sub=sub warn_only=warn>
                                        <div style="display:flex; flex-direction:column; align-items:flex-end; gap:4px; min-width:240px">
                                            <div class="section-header-label" style="align-self:flex-start">"DRAIN FORECAST · 24 MIN"</div>
                                            <div class="drain-forecast" style="width:100%">{bars}</div>
                                            <div class="drain-forecast-labels" style="width:100%">
                                                <span>"now"</span>
                                                <span>"+6m"</span>
                                                <span>"+12m"</span>
                                                <span>"+18m"</span>
                                                <span>"+24m"</span>
                                            </div>
                                            <div style="display:flex; align-items:center; gap:8px; margin-top:6px">
                                                <span class="section-header-label" style="font-size:9.5px">"SUGGESTED ACTION"</span>
                                                <button class="btn btn-primary btn-small" title=suggestion_label.to_string()>
                                                    {suggestion_label.to_string()} " →"
                                                </button>
                                            </div>
                                        </div>
                                    </BottleneckCard>
                                }
                            })}

                            <div class="stats-grid" style="grid-template-columns:repeat(3, 1fr); margin-bottom:24px">
                                <StatTile label="QUEUED RUNS" value=count.to_string() suffix="waiting"/>
                                <StatTile label="HIGH PRIORITY" value=high.to_string() suffix=format!("of {count}")/>
                                <StatTile label="OLDEST QUEUED" value=oldest suffix="waiting"/>
                            </div>

                            <QueueLanes lanes=lanes/>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error loading queue: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
