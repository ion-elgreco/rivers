//! Runs list page.
//!
//! Displays a table of all pipeline runs with status badges, timestamps,
//! and links to individual run detail views.
//!
//! Architecture: the page never loads the full run list client-side. Two
//! Resources fetch only what the UI renders — one for the small aggregate
//! header counts, one for the visible paginated/filtered page. A SurrealDB
//! LIVE query on the server drives an SSE "kick" channel; the client hook
//! debounces kicks and refetches both Resources, so steady-state updates
//! cost one tiny round-trip each instead of a full-list download. This
//! keeps the main thread idle between interactions and guarantees the
//! page-load cost is independent of total run count.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::GridRowSkeleton;
use crate::components::pagination::PaginatedView;
use crate::components::ui_kit::{
    AssetStack, Crumb, DurationCell, EmptyState, FilterPillGroup, LaunchedByCell, PartitionCell,
    RiversSearch, StatusChip, Topbar, partition_scheme_for,
};
use crate::helpers::{
    code_location_label, format_duration, format_timestamp, run_is_active, run_status_class,
    run_status_kind,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::RelTime;
use crate::server_fns::actions::{CancelRunsResult, cancel_runs};
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::runs::{get_runs_page, get_runs_summary};
use crate::types::{CodeLocationEntry, RunFilter, RunRecord, RunStatus, RunsSummary};

const GRID: &str = "grid-template-columns: 32px 80px 1.2fr 0.7fr 1.4fr 0.6fr 0.9fr 0.8fr 1fr";

fn status_from_tab(tab: &str) -> Option<RunStatus> {
    match tab {
        "Success" => Some(RunStatus::Success),
        "Failure" => Some(RunStatus::Failure),
        "In Progress" => Some(RunStatus::Started),
        "Queued" => Some(RunStatus::Queued),
        "Starting" => Some(RunStatus::NotStarted),
        _ => None,
    }
}

#[component]
pub fn RunsListPage() -> impl IntoView {
    let (active_tab, set_active_tab) = signal("All".to_string());
    let (filter_job, set_filter_job) = signal(String::new());
    let (filter_asset, set_filter_asset) = signal(String::new());
    let (filter_partition, set_filter_partition) = signal(String::new());
    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(25u64);

    // Bumped by SSE kicks and the manual refresh button to force a refetch
    // without changing filter state.
    let (refresh_tick, set_refresh_tick) = signal(0u64);

    let page_key = move || {
        (
            active_tab.get(),
            filter_job.get(),
            filter_asset.get(),
            filter_partition.get(),
            page.get(),
            page_size.get(),
            refresh_tick.get(),
        )
    };
    let runs_page = Resource::new(page_key, |(tab, job, asset, partition, p, ps, _tick)| {
        // Empty strings are coerced to `None` server-side in the `From` impl.
        let filter = RunFilter {
            status: status_from_tab(&tab),
            job_name: None,
            job_substring: Some(job),
            asset_substring: Some(asset),
            partition_substring: Some(partition),
        };
        async move { get_runs_page(p * ps, ps, filter).await }
    });

    let locations = Resource::new(|| (), |_| list_code_locations());

    // Multiselect for bulk cancel. Ids only — rows come back from the page
    // Resource. Pruned on every page fetch so the selection never outlives
    // the visible page or a run that finished mid-selection.
    let (selected, set_selected) = signal(Vec::<String>::new());
    Effect::new(move |_| {
        if let Some(Ok(p)) = runs_page.get() {
            let live: std::collections::HashSet<&str> = p
                .rows
                .iter()
                .filter(|r| run_is_active(&r.status))
                .map(|r| r.run_id.as_str())
                .collect();
            let cur = selected.get_untracked();
            let keep: Vec<String> = cur
                .iter()
                .filter(|id| live.contains(id.as_str()))
                .cloned()
                .collect();
            if keep.len() != cur.len() {
                set_selected.set(keep);
            }
        }
    });

    let cancel_action = Action::new(move |ids: &Vec<String>| {
        let ids = ids.clone();
        async move { cancel_runs(ids).await }
    });
    // On completion keep only the failed ids selected (retryable); the
    // refresh tick pulls the new statuses in.
    Effect::new(move |_| {
        if let Some(Ok(res)) = cancel_action.value().get() {
            set_selected.set(res.failed.iter().map(|(id, _)| id.clone()).collect());
            set_refresh_tick.update(|t| *t += 1);
        }
    });

    // Summary is intentionally on a separate Resource with a narrower key:
    // only `refresh_tick` triggers a refetch, not filter/page/page_size.
    // Consequence: each live-update kick makes TWO POSTs (page + summary)
    // instead of one combined endpoint. That's the correct trade — the
    // decoupling is what keeps the status-pill badges stable while the user
    // paginates or types in a filter. Don't re-merge this into one endpoint
    // without re-deriving the same decoupling downstream.
    let summary = Resource::new(
        move || refresh_tick.get(),
        |_| async move { get_runs_summary().await },
    );

    let live_status = use_live_kick(
        &["runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let reload = Callback::new(move |_: ()| set_refresh_tick.update(|t| *t += 1));

    let active_tab_sig = Signal::derive(move || active_tab.get());
    let on_tab = Callback::new(move |v: String| {
        set_active_tab.set(v);
        set_page.set(0);
    });

    view! {
        <Topbar crumbs=vec![Crumb::new("Runs")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| reload.run(()))
            />
        </Topbar>

        // Derived from `summary` only, so typing in a job/asset/partition filter
        // does NOT re-render this block. On fetch error we render "unavailable"
        // instead of falling back to zeros — zero counts would falsely claim
        // "no runs exist" and mask the real state.
        <Transition fallback=move || view! { <GridRowSkeleton rows=2 cols=5/> }>
            {move || match summary.get() {
                None => ().into_any(),
                Some(Ok(s)) => {
                    let RunsSummary { total, in_progress, queued, failure, success, last_24h } = s;
                    let status_items: Vec<(String, Option<usize>)> = vec![
                        ("All".into(), Some(total as usize)),
                        ("In Progress".into(), Some(in_progress as usize)),
                        ("Queued".into(), Some(queued as usize)),
                        ("Failure".into(), Some(failure as usize)),
                        ("Success".into(), Some(success as usize)),
                    ];
                    view! {
                        <div class="page-header-row" style="align-items: flex-end">
                            <div class="page-header">
                                <h1>"Runs"</h1>
                                <p>
                                    "All runs · "
                                    <span class="page-header-num">{total.to_string()}</span>
                                    " total"
                                    <span class="page-header-sep">"·"</span>
                                    <span class="page-header-num">{last_24h.to_string()}</span>
                                    " in last 24h"
                                    <span class="page-header-sep">"·"</span>
                                    <span class="page-header-num page-header-num--error">
                                        {failure.to_string()}
                                    </span>
                                    " failed"
                                </p>
                            </div>
                            <FilterPillGroup
                                label="STATUS"
                                items=status_items
                                active=active_tab_sig
                                on_select=on_tab
                            />
                        </div>
                    }.into_any()
                }
                Some(Err(e)) => {
                    let status_items: Vec<(String, Option<usize>)> = vec![
                        ("All".into(), None),
                        ("In Progress".into(), None),
                        ("Queued".into(), None),
                        ("Failure".into(), None),
                        ("Success".into(), None),
                    ];
                    view! {
                        <div class="page-header-row" style="align-items: flex-end">
                            <div class="page-header">
                                <h1>"Runs"</h1>
                                <p
                                    class="page-header-summary-error"
                                    title=format!("Summary fetch failed: {e}")
                                >
                                    "Summary unavailable — will retry on next update"
                                </p>
                            </div>
                            <FilterPillGroup
                                label="STATUS"
                                items=status_items
                                active=active_tab_sig
                                on_select=on_tab
                            />
                        </div>
                    }.into_any()
                }
            }}
        </Transition>

        <div class="rv-toolbar">
            <RiversSearch
                value=Signal::derive(move || filter_job.get())
                on_input=Callback::new(move |v| { set_filter_job.set(v); set_page.set(0); })
                placeholder="filter by job…"
            />
            <RiversSearch
                value=Signal::derive(move || filter_asset.get())
                on_input=Callback::new(move |v| { set_filter_asset.set(v); set_page.set(0); })
                placeholder="filter by asset…"
            />
            <RiversSearch
                value=Signal::derive(move || filter_partition.get())
                on_input=Callback::new(move |v| { set_filter_partition.set(v); set_page.set(0); })
                placeholder="partition…"
            />
        </div>

        <PaginatedView
            data=runs_page
            page=page
            set_page=set_page
            page_size=page_size
            set_page_size=set_page_size
            fallback=move || view! { <GridRowSkeleton rows=10 cols=7/> }
            empty=move || view! {
                <EmptyState
                    message="No runs match the current filters"
                    hint="clear a filter or widen the status tab"
                />
            }
            render={move |rows: Vec<RunRecord>| {
                let locs = locations.get().and_then(|r| r.ok()).unwrap_or_default();
                view! {
                    <RunsTable
                        rows=rows
                        locations=locs
                        selected=selected
                        set_selected=set_selected
                        cancel_action=cancel_action
                    />
                }.into_any()
            }}
        />
    }
}

#[component]
fn RunsTable(
    rows: Vec<RunRecord>,
    locations: Vec<CodeLocationEntry>,
    selected: ReadSignal<Vec<String>>,
    set_selected: WriteSignal<Vec<String>>,
    cancel_action: Action<Vec<String>, Result<CancelRunsResult, ServerFnError>>,
) -> impl IntoView {
    let locations = std::sync::Arc::new(locations);
    let cancel_pending = cancel_action.pending();
    let cancellable: Vec<String> = rows
        .iter()
        .filter(|r| run_is_active(&r.status))
        .map(|r| r.run_id.clone())
        .collect();
    let n_cancellable = cancellable.len();
    let cancellable = StoredValue::new(cancellable);

    view! {
        {(n_cancellable > 0).then(|| view! {
            <div class="bulk-actions">
                <button
                    class="bulk-link-btn"
                    on:click=move |_| set_selected.set(cancellable.get_value())
                >
                    {format!("Select all cancellable ({n_cancellable})")}
                </button>
                <span class="bulk-sep">"·"</span>
                <button class="bulk-link-btn" on:click=move |_| set_selected.set(Vec::new())>
                    "Clear"
                </button>
                <Show when=move || !selected.get().is_empty()>
                    <button
                        class="btn btn-danger"
                        on:click=move |_| { cancel_action.dispatch(selected.get()); }
                        disabled=move || cancel_pending.get()
                    >
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                            <rect x="3" y="2" width="2.5" height="8"/>
                            <rect x="6.5" y="2" width="2.5" height="8"/>
                        </svg>
                        {move || {
                            if cancel_pending.get() {
                                "Canceling...".to_string()
                            } else {
                                let n = selected.get().len();
                                format!("Cancel {n} run{}", if n == 1 { "" } else { "s" })
                            }
                        }}
                    </button>
                </Show>
                {move || cancel_action.value().get().map(|r| match r {
                    Ok(res) if res.failed.is_empty() => {
                        let n = res.requested;
                        view! {
                            <span class="text-muted">
                                {format!("cancel requested for {n} run{}", if n == 1 { "" } else { "s" })}
                            </span>
                        }.into_any()
                    }
                    Ok(res) => {
                        let detail = res
                            .failed
                            .iter()
                            .map(|(id, e)| format!("{id}: {e}"))
                            .collect::<Vec<_>>()
                            .join("\n");
                        view! {
                            <span class="text-error" title=detail>
                                {format!("{} requested · {} failed", res.requested, res.failed.len())}
                            </span>
                        }.into_any()
                    }
                    Err(e) => view! {
                        <span class="text-error">{format!("cancel failed: {e}")}</span>
                    }.into_any(),
                })}
                <span class="bulk-count">{move || format!("{} selected", selected.get().len())}</span>
            </div>
        })}
        <div class="grid-table">
            <div class="grid-table-head" style=GRID>
                <span></span>
                <span>"RUN"</span>
                <span>"LAUNCHED BY"</span>
                <span>"STATUS"</span>
                <span>"ASSETS"</span>
                <span>"PARTITION"</span>
                <span>"STARTED"</span>
                <span>"DURATION"</span>
                <span>"CODE LOCATION"</span>
            </div>
            <For
                each=move || rows.clone()
                key=|r: &RunRecord| r.run_id.clone()
                children=move |r: RunRecord| {
                    let label = code_location_label(&r.code_location_id, &locations);
                    view! { <RunRow record=r code_location_label=label selected=selected set_selected=set_selected/> }
                }
            />
        </div>
    }
}

#[component]
fn RunRow(
    record: RunRecord,
    code_location_label: String,
    selected: ReadSignal<Vec<String>>,
    set_selected: WriteSignal<Vec<String>>,
) -> impl IntoView {
    let run_id = record.run_id.clone();
    let can_cancel = run_is_active(&record.status);
    let id_for_check = run_id.clone();
    let id_for_toggle = run_id.clone();
    let (ns, name) = use_current_location().get();
    let href = loc_path(&ns, &name, &format!("runs/{}", run_id));
    let short_id = if run_id.len() > 8 {
        run_id[..8].to_string()
    } else {
        run_id.clone()
    };
    let st_class = run_status_class(&record.status);
    let st_kind = run_status_kind(&record.status);
    let start_ts = record.start_time;
    let created_abs = format_timestamp(Some(record.start_time));
    let duration = format_duration(Some(record.start_time), record.end_time);
    let asset_names = record.node_names.clone();
    let partition_val = record.partition_key.clone();
    let cl_title = record.code_location_id.clone();
    let job_name = record.job_name.clone();
    let launched_by = record.launched_by.clone();
    let rail_cls = format!("grid-row-rail grid-row-rail--{}", st_class);
    let part_scheme = partition_val
        .as_ref()
        .and_then(|p| p.preview.first())
        .map(|k| partition_scheme_for(k))
        .unwrap_or("·");
    let launched_cell =
        match crate::helpers::launched_by_sub_line(&launched_by, job_name.as_deref()) {
            Some(sub) => {
                view! { <LaunchedByCell launched_by=launched_by.clone() sub=sub/> }.into_any()
            }
            None => view! { <LaunchedByCell launched_by=launched_by/> }.into_any(),
        };

    view! {
        <A href=href attr:class="grid-row" attr:style=GRID attr:title=created_abs>
            <span class=rail_cls></span>
            <span on:click=move |ev: leptos::ev::MouseEvent| ev.stop_propagation()>
                <input
                    class="asset-row-check"
                    type="checkbox"
                    disabled=!can_cancel
                    title=(!can_cancel).then_some("only queued or in-progress runs can be canceled")
                    prop:checked=move || selected.get().contains(&id_for_check)
                    on:click=move |ev| ev.stop_propagation()
                    on:change=move |_| set_selected.update(|s| {
                        if s.contains(&id_for_toggle) {
                            s.retain(|x| x != &id_for_toggle);
                        } else {
                            s.push(id_for_toggle.clone());
                        }
                    })
                />
            </span>
            <span class="grid-cell-mono">{short_id}</span>
            {launched_cell}
            <StatusChip kind=st_kind small=true/>
            <AssetStack assets=asset_names/>
            {partition_val
                .map(|p| view! { <PartitionCell scheme=part_scheme count_label=p.label()/> }.into_any())
                .unwrap_or_else(|| view! { <span class="grid-cell-muted">"—"</span> }.into_any())}
            <span class="grid-cell-muted"><RelTime ts=start_ts/></span>
            <DurationCell human=duration clock="".to_string()/>
            <span class="grid-cell-muted" title=cl_title>
                {code_location_label}
            </span>
        </A>
    }
}
