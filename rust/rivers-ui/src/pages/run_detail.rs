//! Run detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{Crumb, StatusChip, Topbar};
use crate::helpers::{
    code_location_label, format_elapsed, format_relative_time, format_timestamp,
    launched_by_display, nanos_to_datetime, run_status_kind, short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::use_now;
use crate::server_fns::actions::{cancel_run, execute_job, trigger_materialize};
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::runs::{get_run, get_run_events};
use crate::types::{EventType, RunStatus, StoredEvent};

#[derive(Clone)]
struct GanttStep {
    asset: String,
    start: i64,
    end: Option<i64>,
    status: StepStatus,
}

#[derive(Clone, PartialEq)]
enum StepStatus {
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

#[component]
pub fn RunDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `run_id` is always untracked. Resources explicitly track `params`.
    let run_id = move || params.read_untracked().get("id").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);

    let run = Resource::new(
        move || {
            params.track();
            (run_id(), refresh_tick.get())
        },
        |(id, _)| get_run(id),
    );
    let events = Resource::new(
        move || {
            params.track();
            (run_id(), refresh_tick.get())
        },
        |(id, _)| get_run_events(id),
    );
    let topology = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { crate::server_fns::graph::get_graph_topology(ns, name).await },
    );
    let locations = Resource::new(|| (), |_| list_code_locations());

    let (selected_step, set_selected_step) = signal(Option::<String>::None);
    let (log_tab, set_log_tab) = signal("events".to_string());
    let (log_level, set_log_level) = signal("all".to_string());
    let (view_mode, set_view_mode) = signal("gantt".to_string());

    let reexecute = Action::new(move |input: &(Option<String>, Vec<String>)| {
        let (job_name, assets) = input.clone();
        let (ns, name) = loc.get();
        async move {
            match job_name {
                Some(j) => execute_job(ns, name, j, None).await,
                None => trigger_materialize(ns, name, Some(assets), None, None).await,
            }
        }
    });
    let reexecute_pending = reexecute.pending();

    let cancel = Action::new(move |id: &String| {
        let id = id.clone();
        let (ns, name) = loc.get();
        async move { cancel_run(ns, name, id).await }
    });
    let cancel_pending = cancel.pending();

    let live_status = use_live_kick(
        &["runs", "events"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <div class="run-detail-layout">
        <div class="run-detail-main">
        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                run.get().map(|result| match result {
                    Ok(Some(record)) => {
                        let reexec_job = record.job_name.clone();
                        let assets_for_reexec = record.node_names.clone();
                        let status_kind = run_status_kind(&record.status);
                        let sid = short_id(&record.run_id, 8);
                        let is_active_status = matches!(record.status, RunStatus::Started | RunStatus::NotStarted | RunStatus::Queued);
                        let (_, _, trigger_label, trigger_sub) = launched_by_display(&record.launched_by);
                        // Reactive elapsed: ticks once per second while end_time
                        // is None (run still in flight), freezes when end_time
                        // is set.
                        let run_start_ns = record.start_time;
                        let run_end_ns = record.end_time;
                        let elapsed_label = move || {
                            format_elapsed(Some(run_start_ns), run_end_ns, use_now().get())
                        };
                        let (ns, name) = loc.get();
                        let runs_href = loc_path(&ns, &name, "runs");
                        let cl_id = record.code_location_id.clone();
                        let cl_label = {
                            let entries = locations.get().and_then(|r| r.ok()).unwrap_or_default();
                            code_location_label(&cl_id, &entries)
                        };
                        view! {
                            <Topbar crumbs=vec![
                                Crumb::linked("Runs", runs_href),
                                Crumb::new(sid.clone()).mono(),
                            ]>
                                <LiveStatusChip
                                    status=live_status
                                    on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
                                />
                                <button
                                    class="btn btn-tertiary"
                                    title="Export run events (CSV)"
                                    disabled=true
                                >
                                    <svg width="12" height="12" viewBox="0 0 14 14" fill="none">
                                        <path d="M7 2v7M4 6l3 3 3-3M2 11h10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>
                                    </svg>
                                    "Export"
                                </button>
                                <button
                                    class="btn btn-tertiary"
                                    on:click=move |_| { reexecute.dispatch((reexec_job.clone(), assets_for_reexec.clone())); }
                                    disabled=move || reexecute_pending.get()
                                >
                                    <svg width="12" height="12" viewBox="0 0 14 14" fill="none">
                                        <path d="M12 7a5 5 0 11-1.5-3.5M12 1.5V4H9.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/>
                                    </svg>
                                    {move || if reexecute_pending.get() { "Retrying..." } else { "Retry from" }}
                                </button>
                                {is_active_status.then(|| {
                                    let cancel_id = record.run_id.clone();
                                    view! {
                                        <button
                                            class="btn btn-danger"
                                            on:click=move |_| { cancel.dispatch(cancel_id.clone()); }
                                            disabled=move || cancel_pending.get()
                                        >
                                            <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                                                <rect x="3" y="2" width="2.5" height="8"/>
                                                <rect x="6.5" y="2" width="2.5" height="8"/>
                                            </svg>
                                            {move || if cancel_pending.get() { "Canceling..." } else { "Cancel run" }}
                                        </button>
                                    }
                                })}
                            </Topbar>

                            <div class="run-header-block">
                                <div class="section-header-label">{
                                    match record.job_name.as_deref() {
                                        Some(j) => format!("RUN {} · {}", sid, j),
                                        None => format!("RUN {}", sid),
                                    }
                                }</div>
                                <div class="run-trigger-meta">
                                    <StatusChip kind=status_kind.to_string()/>
                                    <span class="run-trigger-meta-item" title=format_relative_time(record.start_time, chrono::Utc::now().timestamp())>{format_timestamp(Some(record.start_time))}</span>
                                    <span class="run-trigger-meta-sep">"·"</span>
                                    <span class="run-trigger-meta-item">"elapsed "<span class="run-trigger-meta-value">{elapsed_label}</span></span>
                                    <span class="run-trigger-meta-sep">"·"</span>
                                    <span class="run-trigger-meta-item run-trigger-meta-trigger">{trigger_label}</span>
                                    {trigger_sub.map(|s| view! {
                                        <span class="run-trigger-meta-item"><span class="run-trigger-meta-value">{s}</span></span>
                                    })}
                                    {(!cl_id.is_empty()).then(|| view! {
                                        <span class="run-trigger-meta-sep">"·"</span>
                                        <span class="run-trigger-meta-item" title=cl_id.clone()>
                                            "code location "<span class="run-trigger-meta-value">{cl_label.clone()}</span>
                                        </span>
                                    })}
                                    {record.partition_key.as_ref().map(|p| view! {
                                        <span class="run-trigger-meta-sep">"·"</span>
                                        <span class="run-trigger-meta-item">"partition "<span class="run-trigger-meta-value">{p.clone()}</span></span>
                                    })}
                                    {(!record.tags.is_empty()).then(|| {
                                        let tag_spans = record.tags.iter().map(|(k, v)| {
                                            view! {
                                                <span class="run-trigger-meta-item run-trigger-meta-tag">
                                                    {format!("{k}=")}<span class="run-trigger-meta-value">{v.clone()}</span>
                                                </span>
                                            }
                                        }).collect::<Vec<_>>();
                                        view! {
                                            <span class="run-trigger-meta-spacer"></span>
                                            {tag_spans}
                                        }
                                    })}
                                </div>
                            </div>

                            {(record.status == RunStatus::Queued && record.block_reason.is_some()).then(|| {
                                let reason = record.block_reason.clone().unwrap_or_default();
                                view! {
                                    <div class="run-block-reason">
                                        <div class="section-header-label" style="color:var(--warning); margin-bottom:4px">"BLOCKED"</div>
                                        <div class="run-block-reason-text">{reason}</div>
                                    </div>
                                }
                            })}

                        }.into_any()
                    }
                    Ok(None) => view! { <div class="error-msg">"Run not found."</div> }.into_any(),
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <div class="tab-content">
            <Transition fallback=move || view! { <div class="loading">"Loading events..."</div> }>
                {move || {
                    let run_data = run.get().and_then(|r| r.ok()).flatten();
                    events.get().map(|result| match result {
                        Ok(evts) => {
                            let run_start = run_data.as_ref().map(|r| r.start_time);
                            view! {
                                <RunTimelinePanel
                                    events={evts.clone()}
                                    run_start={run_start}
                                    node_names={run_data.as_ref().map(|r| r.node_names.clone()).unwrap_or_default()}
                                    topology={topology.get().and_then(|r| r.ok())}
                                    selected_step=selected_step
                                    on_select=set_selected_step
                                    view_mode=view_mode
                                    set_view_mode=set_view_mode
                                />
                                <RunLogPanel
                                    events={evts}
                                    selected_step=selected_step
                                    on_clear=set_selected_step
                                    log_tab=log_tab
                                    set_log_tab=set_log_tab
                                    log_level=log_level
                                    set_log_level=set_log_level
                                />
                            }.into_any()
                        }
                        Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                    })
                }}
            </Transition>
        </div>

        </div>

        <Show when=move || selected_step.get().is_some()>
            <Transition fallback=|| ()>
                {move || {
                    let asset = selected_step.get()?;
                    let evts = events.get()?.ok()?;
                    let topo = topology.get().and_then(|r| r.ok());
                    Some(view! {
                        <RunAssetDrawer
                            asset_key=asset
                            events=evts
                            topology=topo
                            on_close=set_selected_step
                        />
                    })
                }}
            </Transition>
        </Show>
        </div>
    }
}

/// The `variant` label drives the per-event dot color so observations look
/// distinct from materializations.
fn render_event_cards(events: Vec<StoredEvent>, variant: &'static str) -> Vec<impl IntoView> {
    events
        .into_iter()
        .map(|e| {
            let part = e.partition_key.clone();
            let dv = e.data_version.clone();
            let dv_short = dv.as_ref().map(|v| {
                let head = &v[..6.min(v.len())];
                let tail = if v.len() > 10 { &v[v.len() - 4..] } else { "" };
                format!("{head}…{tail}")
            });
            let metadata = e.metadata.clone();
            let dot_cls = format!("run-asset-drawer-materialization-dot run-asset-drawer-materialization-dot--{variant}");
            view! {
                <div class="run-asset-drawer-materialization">
                    <div class="run-asset-drawer-materialization-head">
                        <span class=dot_cls></span>
                        <span class="run-asset-drawer-materialization-ts">{format_log_timestamp(e.timestamp)}</span>
                        {part.map(|p| view! { <span class="run-asset-drawer-materialization-part">{format!("partition {p}")}</span> })}
                    </div>
                    {dv.map(|v| view! {
                        <div class="run-asset-drawer-mat-kv">
                            <span class="run-asset-drawer-mat-key">"data_version"</span>
                            <span class="run-asset-drawer-mat-dv copyable" data-copy=v title="click to copy">{dv_short.unwrap_or_default()}</span>
                        </div>
                    })}
                    {(!metadata.is_empty()).then(|| view! {
                        <div class="run-asset-drawer-mat-meta">
                            {metadata.into_iter().take(6).map(|(k, v)| view! {
                                <div class="run-asset-drawer-mat-kv">
                                    <span class="run-asset-drawer-mat-key">{k}</span>
                                    <span class="run-asset-drawer-mat-val">{v.as_text()}</span>
                                </div>
                            }).collect::<Vec<_>>()}
                        </div>
                    })}
                </div>
            }
        })
        .collect()
}

/// Event log lives in the main LogPanel below — selection filters it, so we
/// don't duplicate here.
#[component]
fn RunAssetDrawer(
    asset_key: String,
    events: Vec<StoredEvent>,
    topology: Option<crate::types::GraphTopology>,
    on_close: WriteSignal<Option<String>>,
) -> impl IntoView {
    let asset_events: Vec<StoredEvent> = events
        .iter()
        .filter(|e| e.asset_key.as_ref() == Some(&asset_key))
        .cloned()
        .collect();

    let start_ns: Option<i64> = asset_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::StepStart))
        .map(|e| e.timestamp)
        .min();
    let end_ns: Option<i64> = asset_events
        .iter()
        .filter(|e| {
            matches!(
                e.event_type,
                EventType::StepSuccess | EventType::StepFailure
            )
        })
        .map(|e| e.timestamp)
        .max();
    let has_success = asset_events
        .iter()
        .any(|e| matches!(e.event_type, EventType::StepSuccess));
    let has_failure = asset_events
        .iter()
        .any(|e| matches!(e.event_type, EventType::StepFailure));
    let has_start = start_ns.is_some();
    let (status_label, status_for_chip) = if has_failure {
        ("Failed", "failed".to_string())
    } else if has_success {
        ("Success", "success".to_string())
    } else if has_start {
        ("Running", "running".to_string())
    } else {
        ("Pending", "pending".to_string())
    };
    // For finished steps the duration is fixed; for running steps it ticks
    // each second by re-reading the global `now` clock.
    let duration_view = {
        let now_signal = use_now();
        move || match (start_ns, end_ns) {
            (Some(s), Some(e)) => {
                let d = (e - s) as f64 / 1e9;
                fmt_dur_short(d)
            }
            (Some(s), None) if has_start => {
                let now_ns = now_signal.get().saturating_mul(1_000_000_000);
                let d = (now_ns - s) as f64 / 1e9;
                format!("{} (running)", fmt_dur_short(d))
            }
            _ => "—".to_string(),
        }
    };
    let started_label = start_ns
        .map(|t| format_log_timestamp(t))
        .unwrap_or_else(|| "—".to_string());
    let events_count = asset_events.len();
    let n_materializations = asset_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::Materialization))
        .count();
    let n_observations = asset_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::Observation))
        .count();

    let upstream: Vec<String> = topology
        .as_ref()
        .map(|t| t.direct_upstream(&asset_key))
        .unwrap_or_default();

    let materializations: Vec<StoredEvent> = asset_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::Materialization))
        .cloned()
        .collect();
    let observations: Vec<StoredEvent> = asset_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::Observation))
        .cloned()
        .collect();

    let (loc_ns, loc_name) = use_current_location().get();
    let asset_href = loc_path(&loc_ns, &loc_name, &format!("assets/{asset_key}"));
    view! {
        <div class="run-asset-drawer">
            <div class="run-asset-drawer-header">
                <div class="run-asset-drawer-header-text">
                    <div class="section-header-label" style="color:var(--accent); margin-bottom:6px">"● ASSET"</div>
                    <A href=asset_href attr:class="run-asset-drawer-name">{asset_key.clone()}</A>
                    <div style="margin-top:6px">
                        <StatusChip kind=status_for_chip/>
                    </div>
                </div>
                <button
                    class="run-asset-drawer-close"
                    title="Close"
                    on:click=move |_| on_close.set(None)
                >"×"</button>
            </div>

            <div class="run-asset-drawer-section">
                <div class="run-asset-drawer-stats">
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"DURATION"</div>
                        <div class="run-asset-drawer-kv-value">{duration_view}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"STARTED"</div>
                        <div class="run-asset-drawer-kv-value">{started_label}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"STATUS"</div>
                        <div class="run-asset-drawer-kv-value">{status_label}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"EVENTS"</div>
                        <div class="run-asset-drawer-kv-value">{events_count.to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"UPSTREAM"</div>
                        <div class="run-asset-drawer-kv-value">{upstream.len().to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"MATERIALIZATIONS"</div>
                        <div class="run-asset-drawer-kv-value">{n_materializations.to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"OBSERVATIONS"</div>
                        <div class="run-asset-drawer-kv-value">{n_observations.to_string()}</div>
                    </div>
                </div>
            </div>

            {(!upstream.is_empty()).then(|| view! {
                <div class="run-asset-drawer-section">
                    <div class="section-header-label" style="margin-bottom:8px">"UPSTREAM"</div>
                    <div class="run-asset-drawer-deps">
                        {upstream.into_iter().map(|dep| {
                            let href = loc_path(&loc_ns, &loc_name, &format!("assets/{dep}"));
                            view! {
                                <A href=href attr:class="run-asset-drawer-dep">
                                    <span class="run-asset-drawer-dep-arrow">"↳"</span>
                                    <span>{dep}</span>
                                </A>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            })}

            {(!materializations.is_empty()).then(|| {
                let mat_views = render_event_cards(materializations, "materialization");
                view! {
                    <div class="run-asset-drawer-section">
                        <div class="section-header-label" style="margin-bottom:8px">"OUTPUT · MATERIALIZATION"</div>
                        <div class="run-asset-drawer-materializations">{mat_views}</div>
                    </div>
                }
            })}

            {(!observations.is_empty()).then(|| {
                let obs_views = render_event_cards(observations, "observation");
                view! {
                    <div class="run-asset-drawer-section">
                        <div class="section-header-label" style="margin-bottom:8px">"OBSERVATION"</div>
                        <div class="run-asset-drawer-materializations">{obs_views}</div>
                    </div>
                }
            })}

        </div>
    }
}

/// A "ghost: last run" overlay (previous-run delta bars + legend) was scaffolded
/// here; see [`docs/deferred/run-detail-ghost-overlay.md`] for the full design
/// and how to reintroduce it once real previous-run data lands.
#[component]
fn RunTimelinePanel(
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
                    <div class="view-pill-group">
                        {["dag", "gantt"].into_iter().map(|v| {
                            let vs = v.to_string();
                            let vs_for_cls = vs.clone();
                            view! {
                                <button
                                    class=move || if view_mode.get() == vs_for_cls { "view-pill view-pill--active" } else { "view-pill" }
                                    on:click=move |_| set_view_mode.set(vs.clone())
                                >{v}</button>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            </div>
            {if !has_steps {
                view! { <div class="empty-state" style="margin:20px 16px">"No execution steps recorded yet."</div> }.into_any()
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

fn fmt_dur_short(secs: f64) -> String {
    let abs = secs.abs();
    if abs < 1.0 {
        // Sub-second: show millis so a sub-second run doesn't render every tick as "0s".
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
fn format_log_timestamp(ts: i64) -> String {
    nanos_to_datetime(ts)
        .map(|d| d.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn event_type_label(evt: &StoredEvent) -> &'static str {
    match evt.event_type {
        EventType::StepStart => "STEP_START",
        EventType::StepSuccess => "STEP_SUCCESS",
        EventType::StepFailure => "STEP_FAILURE",
        EventType::Materialization => "MATERIALIZATION",
        EventType::Observation => "OBSERVATION",
        EventType::LogOutput => "LOG_OUTPUT",
        EventType::RunQueued => "RUN_QUEUED",
        EventType::RunDequeued => "RUN_DEQUEUED",
        EventType::StepSlotClaimed => "SLOT_CLAIMED",
        EventType::StepSlotWaiting => "SLOT_WAITING",
        EventType::StepSlotRenewed => "SLOT_RENEWED",
        EventType::StepSlotReleased => "SLOT_RELEASED",
    }
}

fn event_row_class(evt: &StoredEvent) -> &'static str {
    match evt.event_type {
        EventType::StepStart => "log-row--info",
        EventType::StepSuccess => "log-row--success",
        EventType::StepFailure => "log-row--error",
        EventType::Materialization => "log-row--success",
        EventType::Observation => "log-row--info",
        EventType::LogOutput => "log-row--muted",
        EventType::RunQueued | EventType::RunDequeued => "log-row--info",
        EventType::StepSlotClaimed | EventType::StepSlotReleased => "log-row--info",
        EventType::StepSlotWaiting => "log-row--warn",
        EventType::StepSlotRenewed => "log-row--muted",
    }
}

fn metadata_value(evt: &StoredEvent, key: &str) -> Option<String> {
    evt.metadata
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_text())
}

fn event_info(evt: &StoredEvent) -> String {
    match evt.event_type {
        EventType::StepStart => "Step started".to_string(),
        EventType::StepSuccess => "Step succeeded".to_string(),
        EventType::StepFailure => evt
            .metadata
            .iter()
            .find(|(k, _)| k == "error")
            .map(|(_, v)| v.as_text())
            .unwrap_or_else(|| "Step failed".to_string()),
        EventType::Materialization => evt
            .data_version
            .as_ref()
            .map(|v| format!("Materialized (v:{})", short_id(v, 8)))
            .unwrap_or_else(|| "Materialized".to_string()),
        EventType::Observation => "Observed".to_string(),
        EventType::LogOutput => {
            let parts: Vec<&str> = evt
                .metadata
                .iter()
                .filter(|(k, _)| k == "stdout" || k == "stderr" || k == "logs")
                .map(|(k, _)| k.as_str())
                .collect();
            if parts.is_empty() {
                "No output".to_string()
            } else {
                format!("Captured {}", parts.join(", "))
            }
        }
        EventType::RunQueued => "Run queued".to_string(),
        EventType::RunDequeued => "Run dequeued".to_string(),
        EventType::StepSlotClaimed => {
            let pools = metadata_value(evt, "pools");
            format!(
                "Claimed pool slots{}",
                pools.map(|p| format!(" ({})", p)).unwrap_or_default()
            )
        }
        EventType::StepSlotWaiting => {
            let reason = metadata_value(evt, "reason");
            format!(
                "Waiting for pool slots{}",
                reason.map(|r| format!(": {}", r)).unwrap_or_default()
            )
        }
        EventType::StepSlotRenewed => "Lease renewed".to_string(),
        EventType::StepSlotReleased => "Released pool slots".to_string(),
    }
}

#[component]
fn RunDagView(
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

/// Case-insensitive whole-word match. Used only for filtering — the raw line
/// is still displayed unchanged.
fn line_matches_level(line: &str, level: &str) -> bool {
    let aliases: &[&str] = match level {
        "info" => &["INFO"],
        "debug" => &["DEBUG", "TRACE"],
        "warn" => &["WARN", "WARNING"],
        "error" => &["ERROR", "CRITICAL", "FATAL", "ERR"],
        _ => return true,
    };
    let upper = line.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let is_word_boundary = |i: usize| -> bool {
        if i >= bytes.len() {
            return true;
        }
        !bytes[i].is_ascii_alphabetic()
    };
    for needle in aliases {
        let n = needle.as_bytes();
        let mut i = 0usize;
        while i + n.len() <= bytes.len() {
            if &bytes[i..i + n.len()] == n {
                let left_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
                let right_ok = is_word_boundary(i + n.len());
                if left_ok && right_ok {
                    return true;
                }
            }
            i += 1;
        }
    }
    false
}

/// Preserves the raw text (including levels, timestamps, or `[tag]` prefixes the user wrote).
/// When `level_filter` is not `"all"`, drops lines that don't mention the level.
fn render_log_rows(data: Vec<(String, String)>, level_filter: &str) -> Vec<impl IntoView + use<>> {
    let mut out: Vec<_> = Vec::new();
    for (asset, blob) in data {
        for raw in blob.split('\n') {
            if raw.trim().is_empty() {
                continue;
            }
            if level_filter != "all" && !line_matches_level(raw, level_filter) {
                continue;
            }
            let msg_html = ansi_to_html::convert(raw).unwrap_or_else(|_| raw.to_string());
            out.push(view! {
                <div class="log-row">
                    <span class="log-row-source" title=asset.clone()>{asset.clone()}</span>
                    <span class="log-row-msg" inner_html=msg_html></span>
                </div>
            });
        }
    }
    out
}

#[component]
fn RunLogPanel(
    events: Vec<StoredEvent>,
    selected_step: ReadSignal<Option<String>>,
    on_clear: WriteSignal<Option<String>>,
    log_tab: ReadSignal<String>,
    set_log_tab: WriteSignal<String>,
    log_level: ReadSignal<String>,
    set_log_level: WriteSignal<String>,
) -> impl IntoView {
    let all_structured: Vec<StoredEvent> = events
        .iter()
        .filter(|e| !matches!(e.event_type, EventType::LogOutput))
        .cloned()
        .collect();

    let extract_metadata = |key: &str| -> Vec<(String, String)> {
        events
            .iter()
            .filter(|e| matches!(e.event_type, EventType::LogOutput))
            .flat_map(|e| {
                let asset = e.asset_key.clone().unwrap_or_default();
                e.metadata
                    .iter()
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| v.as_text())
                    .filter(|v| !v.is_empty())
                    .map(move |v| (asset.clone(), v))
            })
            .collect()
    };
    let all_stdout = extract_metadata("stdout");
    let all_stderr = extract_metadata("stderr");
    let all_logs = extract_metadata("logs");

    let make_filtered = |data: Vec<(String, String)>| {
        Signal::derive(move || {
            let sel = selected_step.get();
            match sel {
                Some(ref name) => data
                    .iter()
                    .filter(|(a, _)| a == name)
                    .cloned()
                    .collect::<Vec<_>>(),
                None => data.clone(),
            }
        })
    };

    let structured_events = Signal::derive({
        let data = all_structured.clone();
        move || {
            let sel = selected_step.get();
            match sel {
                Some(ref name) => data
                    .iter()
                    .filter(|e| e.asset_key.as_ref() == Some(name))
                    .cloned()
                    .collect::<Vec<_>>(),
                None => data.clone(),
            }
        }
    });
    let stdout_lines = make_filtered(all_stdout.clone());
    let stderr_lines = make_filtered(all_stderr.clone());
    let log_lines = make_filtered(all_logs.clone());

    let stdout_count_all = all_stdout.len();
    let stderr_count_all = all_stderr.len();
    let logs_count_all = all_logs.len();

    view! {
        <div class="log-panel">
            <div class="log-panel-header">
                <span class="log-panel-label">"LOGS"</span>
                <div class="log-panel-tabs">
                    <button
                        class=move || if log_tab.get() == "events" { "log-tab log-tab--active" } else { "log-tab" }
                        on:click=move |_| set_log_tab.set("events".to_string())
                    >"events"</button>
                    <button
                        class=move || if log_tab.get() == "logs" { "log-tab log-tab--active" } else { "log-tab" }
                        on:click=move |_| set_log_tab.set("logs".to_string())
                        disabled=move || logs_count_all == 0
                    >
                        "logs"
                        {(logs_count_all > 0).then(|| view! {
                            <span class="log-tab-badge">{logs_count_all}</span>
                        })}
                    </button>
                    <button
                        class=move || if log_tab.get() == "stdout" { "log-tab log-tab--active" } else { "log-tab" }
                        on:click=move |_| set_log_tab.set("stdout".to_string())
                        disabled=move || stdout_count_all == 0
                    >
                        "stdout"
                        {(stdout_count_all > 0).then(|| view! {
                            <span class="log-tab-badge">{stdout_count_all}</span>
                        })}
                    </button>
                    <button
                        class=move || if log_tab.get() == "stderr" { "log-tab log-tab--active" } else { "log-tab" }
                        on:click=move |_| set_log_tab.set("stderr".to_string())
                        disabled=move || stderr_count_all == 0
                    >
                        "stderr"
                        {(stderr_count_all > 0).then(|| view! {
                            <span class="log-tab-badge log-tab-badge--error">{stderr_count_all}</span>
                        })}
                    </button>
                </div>
                <Show when=move || selected_step.get().is_some()>
                    <button
                        class="log-filter-chip"
                        on:click=move |_| on_clear.set(None)
                    >
                        {move || format!("Filtered: {}", selected_step.get().unwrap_or_default())}
                        <span class="log-filter-chip-x">" x"</span>
                    </button>
                </Show>
                <Show when=move || matches!(log_tab.get().as_str(), "logs" | "stdout" | "stderr")>
                    <div class="log-panel-tabs log-level-pills" style="margin-left:auto">
                        {["all", "info", "debug", "warn", "error"].into_iter().map(|lvl| {
                            let lvl_s = lvl.to_string();
                            let lvl_for_cls = lvl_s.clone();
                            let level_cls = move || {
                                let active = log_level.get() == lvl_for_cls;
                                let base = if active { "log-tab log-tab--active" } else { "log-tab" };
                                match lvl_for_cls.as_str() {
                                    "info" => format!("{base} log-tab--info"),
                                    "debug" => format!("{base} log-tab--debug"),
                                    "warn" => format!("{base} log-tab--warn"),
                                    "error" => format!("{base} log-tab--error"),
                                    _ => base.to_string(),
                                }
                            };
                            view! {
                                <button
                                    class=level_cls
                                    on:click=move |_| set_log_level.set(lvl_s.clone())
                                >{lvl}</button>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                </Show>
                <div
                    class=move || if selected_step.get().is_some() { "log-live-indicator log-live-indicator--paused" } else { "log-live-indicator log-live-indicator--live" }
                    style=move || if matches!(log_tab.get().as_str(), "logs" | "stdout" | "stderr") { "margin-left: 12px" } else { "margin-left: auto" }
                >
                    <span class="log-live-indicator-dot"></span>
                    <span>{move || if selected_step.get().is_some() { "paused" } else { "live" }}</span>
                </div>
            </div>

            <div class="log-panel-body" style=move || if log_tab.get() == "events" { "" } else { "display:none" }>
                {move || {
                    let evts = structured_events.get();
                    if evts.is_empty() {
                        view! { <div class="log-empty">"No events recorded."</div> }.into_any()
                    } else {
                        view! {
                            <table class="log-table">
                                <thead>
                                    <tr>
                                        <th class="log-col-time">"Time"</th>
                                        <th class="log-col-asset">"Asset"</th>
                                        <th class="log-col-type">"Event"</th>
                                        <th class="log-col-info">"Info"</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {evts.into_iter().map(|evt| {
                                        let row_class = event_row_class(&evt);
                                        let ts = format_log_timestamp(evt.timestamp);
                                        let asset = evt.asset_key.clone().unwrap_or_default();
                                        let etype = event_type_label(&evt);
                                        let info = event_info(&evt);
                                        view! {
                                            <tr class={row_class}>
                                                <td class="log-col-time"><code>{ts}</code></td>
                                                <td class="log-col-asset">{asset}</td>
                                                <td class="log-col-type"><span class="log-type-badge">{etype}</span></td>
                                                <td class="log-col-info">{info}</td>
                                            </tr>
                                        }
                                    }).collect::<Vec<_>>()}
                                </tbody>
                            </table>
                        }.into_any()
                    }
                }}
            </div>

            <div class="log-panel-body" style=move || if log_tab.get() == "logs" { "" } else { "display:none" }>
                {
                    let data = log_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No logs captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
            <div class="log-panel-body" style=move || if log_tab.get() == "stdout" { "" } else { "display:none" }>
                {
                    let data = stdout_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No stdout captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
            <div class="log-panel-body" style=move || if log_tab.get() == "stderr" { "" } else { "display:none" }>
                {
                    let data = stderr_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No stderr captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
        </div>
    }
}
