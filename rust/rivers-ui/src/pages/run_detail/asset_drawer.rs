use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::pagination::PaginatedView;
use crate::components::ui_kit::StatusChip;
use crate::loc::{loc_path, use_current_location};
use crate::now::use_now;
use crate::server_fns::runs::get_run_asset_events_page;
use crate::types::{EventType, StoredEvent};

use super::logs::format_log_timestamp;
use super::timeline::fmt_dur_short;

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

/// Step-completion status for an asset within a run drawer. A per-partition
/// StepFailure (`mark_partition_failed`) is partial and must not flip a
/// succeeded step to "Failed"; only a step-level StepFailure (no `partition_key`)
/// does. Returns `(label, chip_class)`.
fn asset_chip_status(asset_events: &[StoredEvent]) -> (&'static str, &'static str) {
    let has_success = asset_events
        .iter()
        .any(|e| matches!(e.event_type, EventType::StepSuccess));
    let has_failure = asset_events
        .iter()
        .any(|e| matches!(e.event_type, EventType::StepFailure) && e.partition_key.is_none());
    let has_start = asset_events
        .iter()
        .any(|e| matches!(e.event_type, EventType::StepStart));
    if has_failure {
        ("Failed", "failed")
    } else if has_success {
        ("Success", "success")
    } else if has_start {
        ("Running", "running")
    } else {
        ("Pending", "pending")
    }
}

/// Event log lives in the main LogPanel below — selection filters it, so we
/// don't duplicate here.
#[component]
pub fn RunAssetDrawer(
    asset_key: String,
    run_id: String,
    step_events: Vec<StoredEvent>,
    topology: Option<crate::types::GraphTopology>,
    mat_page: ReadSignal<u64>,
    set_mat_page: WriteSignal<u64>,
    obs_page: ReadSignal<u64>,
    set_obs_page: WriteSignal<u64>,
    act_page: ReadSignal<u64>,
    set_act_page: WriteSignal<u64>,
    del_page: ReadSignal<u64>,
    set_del_page: WriteSignal<u64>,
    on_close: WriteSignal<Option<String>>,
) -> impl IntoView {
    // Status/timing from this asset's step events; materializations paginated below.
    let asset_step_events: Vec<StoredEvent> = step_events
        .iter()
        .filter(|e| e.asset_key.as_ref() == Some(&asset_key))
        .cloned()
        .collect();

    let start_ns: Option<i64> = asset_step_events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::StepStart))
        .map(|e| e.timestamp)
        .min();
    let end_ns: Option<i64> = asset_step_events
        .iter()
        .filter(|e| {
            matches!(
                e.event_type,
                EventType::StepSuccess | EventType::StepFailure
            )
        })
        .map(|e| e.timestamp)
        .max();
    let has_start = start_ns.is_some();
    let (status_label, chip) = asset_chip_status(&asset_step_events);
    let status_for_chip = chip.to_string();
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
    let upstream: Vec<String> = topology
        .as_ref()
        .map(|t| t.direct_upstream(&asset_key))
        .unwrap_or_default();

    // Paginated materialization / observation cards; page totals drive the headers.
    let (mat_page_size, set_mat_page_size) = signal(25u64);
    let materializations_page = {
        let run_id = run_id.clone();
        let asset_key = asset_key.clone();
        Resource::new(
            move || (mat_page.get(), mat_page_size.get()),
            move |(p, ps)| {
                let run_id = run_id.clone();
                let asset_key = asset_key.clone();
                async move {
                    get_run_asset_events_page(
                        run_id,
                        asset_key,
                        "Materialization".to_string(),
                        p * ps,
                        ps,
                    )
                    .await
                }
            },
        )
    };
    let (obs_page_size, set_obs_page_size) = signal(25u64);
    let observations_page = {
        let run_id = run_id.clone();
        let asset_key = asset_key.clone();
        Resource::new(
            move || (obs_page.get(), obs_page_size.get()),
            move |(p, ps)| {
                let run_id = run_id.clone();
                let asset_key = asset_key.clone();
                async move {
                    get_run_asset_events_page(
                        run_id,
                        asset_key,
                        "Observation".to_string(),
                        p * ps,
                        ps,
                    )
                    .await
                }
            },
        )
    };
    // Action runs report through ActionCompleted events — without this third
    // card the metadata an action attaches is unreachable from the run page.
    let (act_page_size, set_act_page_size) = signal(25u64);
    let actions_page = {
        let run_id = run_id.clone();
        let asset_key = asset_key.clone();
        Resource::new(
            move || (act_page.get(), act_page_size.get()),
            move |(p, ps)| {
                let run_id = run_id.clone();
                let asset_key = asset_key.clone();
                async move {
                    get_run_asset_events_page(
                        run_id,
                        asset_key,
                        "ActionCompleted".to_string(),
                        p * ps,
                        ps,
                    )
                    .await
                }
            },
        )
    };
    // A delete run's only asset event is its Deletion.
    let (del_page_size, set_del_page_size) = signal(25u64);
    let deletions_page = {
        let run_id = run_id.clone();
        let asset_key = asset_key.clone();
        Resource::new(
            move || (del_page.get(), del_page_size.get()),
            move |(p, ps)| {
                let run_id = run_id.clone();
                let asset_key = asset_key.clone();
                async move {
                    get_run_asset_events_page(run_id, asset_key, "Deletion".to_string(), p * ps, ps)
                        .await
                }
            },
        )
    };
    let del_total = Signal::derive(move || {
        deletions_page
            .get()
            .and_then(|r| r.ok())
            .map(|p| p.total)
            .unwrap_or(0)
    });
    let mat_total = Signal::derive(move || {
        materializations_page
            .get()
            .and_then(|r| r.ok())
            .map(|p| p.total)
            .unwrap_or(0)
    });
    let obs_total = Signal::derive(move || {
        observations_page
            .get()
            .and_then(|r| r.ok())
            .map(|p| p.total)
            .unwrap_or(0)
    });
    let act_total = Signal::derive(move || {
        actions_page
            .get()
            .and_then(|r| r.ok())
            .map(|p| p.total)
            .unwrap_or(0)
    });
    let step_count = asset_step_events.len() as u64;

    let (loc_ns, loc_name) = use_current_location().get_untracked();
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
                    class="icon-btn"
                    title="Close"
                    aria-label="Close"
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
                        <div class="run-asset-drawer-kv-value">{move || (mat_total.get() + obs_total.get() + act_total.get() + del_total.get() + step_count).to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"UPSTREAM"</div>
                        <div class="run-asset-drawer-kv-value">{upstream.len().to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"MATERIALIZATIONS"</div>
                        <div class="run-asset-drawer-kv-value">{move || mat_total.get().to_string()}</div>
                    </div>
                    <div class="run-asset-drawer-kv">
                        <div class="run-asset-drawer-kv-label">"OBSERVATIONS"</div>
                        <div class="run-asset-drawer-kv-value">{move || obs_total.get().to_string()}</div>
                    </div>
                    <Show when={move || act_total.get() > 0}>
                        <div class="run-asset-drawer-kv">
                            <div class="run-asset-drawer-kv-label">"ACTIONS"</div>
                            <div class="run-asset-drawer-kv-value">{move || act_total.get().to_string()}</div>
                        </div>
                    </Show>
                    <Show when={move || del_total.get() > 0}>
                        <div class="run-asset-drawer-kv">
                            <div class="run-asset-drawer-kv-label">"DELETIONS"</div>
                            <div class="run-asset-drawer-kv-value">{move || del_total.get().to_string()}</div>
                        </div>
                    </Show>
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

            <div class="run-asset-drawer-section">
                <div class="section-header-label" style="margin-bottom:8px">"OUTPUT · MATERIALIZATION"</div>
                <PaginatedView
                    data=materializations_page
                    page=mat_page
                    set_page=set_mat_page
                    page_size=mat_page_size
                    set_page_size=set_mat_page_size
                    empty=move || view! { <div class="log-empty">"No materializations."</div> }
                    render={move |rows: Vec<crate::types::StoredEvent>| view! {
                        <div class="run-asset-drawer-materializations">{render_event_cards(rows, "materialization")}</div>
                    }.into_any()}
                />
            </div>

            <Show when={move || obs_total.get() > 0}>
                <div class="run-asset-drawer-section">
                    <div class="section-header-label" style="margin-bottom:8px">"OBSERVATION"</div>
                    <PaginatedView
                        data=observations_page
                        page=obs_page
                        set_page=set_obs_page
                        page_size=obs_page_size
                        set_page_size=set_obs_page_size
                        render={move |rows: Vec<crate::types::StoredEvent>| view! {
                            <div class="run-asset-drawer-materializations">{render_event_cards(rows, "observation")}</div>
                        }.into_any()}
                    />
                </div>
            </Show>

            <Show when={move || act_total.get() > 0}>
                <div class="run-asset-drawer-section">
                    <div class="section-header-label" style="margin-bottom:8px">"ACTION"</div>
                    <PaginatedView
                        data=actions_page
                        page=act_page
                        set_page=set_act_page
                        page_size=act_page_size
                        set_page_size=set_act_page_size
                        render={move |rows: Vec<crate::types::StoredEvent>| view! {
                            <div class="run-asset-drawer-materializations">{render_event_cards(rows, "action")}</div>
                        }.into_any()}
                    />
                </div>
            </Show>

            <Show when={move || del_total.get() > 0}>
                <div class="run-asset-drawer-section">
                    <div class="section-header-label" style="margin-bottom:8px">"DELETION"</div>
                    <PaginatedView
                        data=deletions_page
                        page=del_page
                        set_page=set_del_page
                        page_size=del_page_size
                        set_page_size=set_del_page_size
                        render={move |rows: Vec<crate::types::StoredEvent>| view! {
                            <div class="run-asset-drawer-materializations">{render_event_cards(rows, "deletion")}</div>
                        }.into_any()}
                    />
                </div>
            </Show>

        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_type: EventType, partition_key: Option<&str>) -> StoredEvent {
        StoredEvent {
            id: String::new(),
            event_type,
            asset_key: Some("a".to_string()),
            run_id: "r".to_string(),
            partition_key: partition_key.map(str::to_string),
            timestamp: 0,
            metadata: vec![],
            data_version: None,
        }
    }

    #[test]
    fn per_partition_failure_keeps_step_success() {
        // A succeeded step with a partial (per-partition) failure stays "Success".
        let events = [
            ev(EventType::StepStart, None),
            ev(EventType::StepFailure, Some("b")),
            ev(EventType::StepSuccess, None),
        ];
        assert_eq!(asset_chip_status(&events), ("Success", "success"));
    }

    #[test]
    fn step_level_failure_is_failed() {
        let events = [
            ev(EventType::StepStart, None),
            ev(EventType::StepFailure, None),
        ];
        assert_eq!(asset_chip_status(&events), ("Failed", "failed"));
    }

    #[test]
    fn running_then_pending() {
        assert_eq!(
            asset_chip_status(&[ev(EventType::StepStart, None)]),
            ("Running", "running")
        );
        assert_eq!(asset_chip_status(&[]), ("Pending", "pending"));
    }
}
