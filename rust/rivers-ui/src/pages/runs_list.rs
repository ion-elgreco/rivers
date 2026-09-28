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

use crate::components::icons::{IconStop, IconTrash};
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::GridRowSkeleton;
use crate::components::pagination::PaginatedView;
use crate::components::ui_kit::{
    AssetStack, DurationCell, EmptyState, FilterPillGroup, LaunchedByCell, PartitionCell,
    RiversSearch, StatusChip, Topbar, partition_scheme_for,
};
use crate::helpers::{
    code_location_label, format_duration, format_timestamp, run_is_active, run_status_class,
    run_status_kind,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::RelTime;
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::mutations::{BulkRunActionResult, cancel_runs, delete_runs};
use crate::server_fns::runs::{get_runs_page, get_runs_summary};
use crate::types::{CodeLocationEntry, RunFilter, RunRecord, RunStatus, RunsSummary};

const GRID: &str = "grid-template-columns: 32px 80px 1.2fr 0.7fr 1.4fr 0.6fr 0.9fr 0.8fr 1fr";

/// Status pills as `(id, label, count)`. Ids are URL values; labels use the
/// status-chip words.
fn run_status_pills(counts: [Option<u64>; 5]) -> Vec<(String, String, Option<usize>)> {
    [
        ("All", "All"),
        ("In Progress", "Running"),
        ("Queued", "Queued"),
        ("Failure", "Failed"),
        ("Success", "Success"),
    ]
    .into_iter()
    .zip(counts)
    .map(|((id, label), n)| (id.to_string(), label.to_string(), n.map(|n| n as usize)))
    .collect()
}

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

type BulkAction = Action<Vec<String>, Result<BulkRunActionResult, ServerFnError>>;

/// Wire a bulk action's completion: keep only the failed ids selected
/// (retryable), refresh the page, and compose the result line shown in the
/// bulk bar. `ok_verb` leads the success copy ("deleted", "cancel requested
/// for"); `fail_verb` labels a whole-request failure.
fn wire_bulk_completion(
    action: BulkAction,
    ok_verb: &'static str,
    fail_verb: &'static str,
    set_selected: WriteSignal<Vec<String>>,
    set_refresh_tick: WriteSignal<u64>,
    set_last_result: WriteSignal<Option<(String, bool, String)>>,
) {
    Effect::new(move |_| {
        let Some(res) = action.value().get() else {
            return;
        };
        match res {
            Ok(r) => {
                set_selected.set(r.failed.iter().map(|(id, _)| id.clone()).collect());
                set_refresh_tick.update(|t| *t += 1);
                let n = r.requested;
                let noun = if n == 1 { "run" } else { "runs" };
                if r.failed.is_empty() {
                    set_last_result.set(Some((
                        format!("{ok_verb} {n} {noun}"),
                        false,
                        String::new(),
                    )));
                } else {
                    let detail = r
                        .failed
                        .iter()
                        .map(|(id, e)| format!("{id}: {}", crate::helpers::err_text(&e)))
                        .collect::<Vec<_>>()
                        .join("\n");
                    set_last_result.set(Some((
                        format!("{ok_verb} {n} {noun} · {} failed", r.failed.len()),
                        true,
                        detail,
                    )));
                }
            }
            Err(e) => set_last_result.set(Some((
                format!("{fail_verb} failed: {}", crate::helpers::err_text(&e)),
                true,
                String::new(),
            ))),
        }
    });
}

#[component]
pub fn RunsListPage() -> impl IntoView {
    let (active_tab, set_active_tab) = signal("All".to_string());
    let (filter_job, set_filter_job) = signal(String::new());
    let (filter_asset, set_filter_asset) = signal(String::new());
    let (filter_partition, set_filter_partition) = signal(String::new());
    let (filter_verb, set_filter_verb) = signal(String::new());
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
            filter_verb.get(),
            page.get(),
            page_size.get(),
            refresh_tick.get(),
        )
    };
    let runs_page = Resource::new(
        page_key,
        |(tab, job, asset, partition, verb, p, ps, _tick)| {
            // Empty strings are coerced to `None` server-side in the `From` impl.
            let filter = RunFilter {
                status: status_from_tab(&tab),
                job_name: None,
                job_substring: Some(job),
                asset_substring: Some(asset),
                partition_substring: Some(partition),
                action: crate::helpers::verb_filter_from_input(&verb),
            };
            async move { get_runs_page(p * ps, ps, filter).await }
        },
    );

    let locations = Resource::new(|| (), |_| list_code_locations());

    // Multiselect for bulk cancel/delete. Ids only — rows come back from the
    // page Resource. Pruned on every page fetch so the selection never
    // outlives the visible page (a run that finishes mid-selection stays
    // selected: it flips from cancellable to deletable).
    let (selected, set_selected) = signal(Vec::<String>::new());
    Effect::new(move |_| {
        if let Some(Ok(p)) = runs_page.get() {
            let live: std::collections::HashSet<&str> =
                p.rows.iter().map(|r| r.run_id.as_str()).collect();
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

    let cancel_action: BulkAction = Action::new(move |ids: &Vec<String>| {
        let ids = ids.clone();
        async move { cancel_runs(ids).await }
    });
    let delete_action: BulkAction = Action::new(move |ids: &Vec<String>| {
        let ids = ids.clone();
        async move { delete_runs(ids).await }
    });
    let (last_result, set_last_result) = signal(None::<(String, bool, String)>);
    wire_bulk_completion(
        cancel_action,
        "cancel requested for",
        "cancel",
        set_selected,
        set_refresh_tick,
        set_last_result,
    );
    wire_bulk_completion(
        delete_action,
        "deleted",
        "delete",
        set_selected,
        set_refresh_tick,
        set_last_result,
    );

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
        <Topbar
            title="Runs"
            subtitle=move || view! {
                // Derived from `summary` only, so typing in a filter does not
                // re-render it. On fetch error show "unavailable", not zeros —
                // zero counts would falsely claim "no runs exist".
                <Transition>
                    {move || summary.get().map(|res| match res {
                        Ok(RunsSummary { total, failure, last_24h, .. }) => view! {
                            <span class="page-header-num">{total.to_string()}</span>
                            " total"
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num">{last_24h.to_string()}</span>
                            " in last 24h"
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num page-header-num--error">{failure.to_string()}</span>
                            " failed"
                        }.into_any(),
                        Err(e) => view! {
                            <span
                                class="page-header-summary-error"
                                title=format!("Summary fetch failed: {}", crate::helpers::err_text(&e))
                            >
                                "Summary unavailable — will retry on next update"
                            </span>
                        }.into_any(),
                    })}
                </Transition>
            }
        >
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| reload.run(()))
            />
        </Topbar>

        <Transition fallback=move || view! { <GridRowSkeleton rows=1 cols=5/> }>
            {move || summary.get().map(|res| {
                let counts = match res {
                    Ok(s) => [Some(s.total), Some(s.in_progress), Some(s.queued), Some(s.failure), Some(s.success)],
                    Err(_) => [None; 5],
                };
                view! {
                    <div class="rv-toolbar">
                        <FilterPillGroup
                            label="STATUS"
                            items=run_status_pills(counts)
                            active=active_tab_sig
                            on_select=on_tab
                        />
                    </div>
                }
            })}
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
            <RiversSearch
                value=Signal::derive(move || filter_verb.get())
                on_input=Callback::new(move |v| { set_filter_verb.set(v); set_page.set(0); })
                placeholder="verb (materialize, delete…)"
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
                    hint="Clear a filter or pick another status"
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
                        delete_action=delete_action
                        last_result=last_result
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
    cancel_action: BulkAction,
    delete_action: BulkAction,
    last_result: ReadSignal<Option<(String, bool, String)>>,
) -> impl IntoView {
    let locations = std::sync::Arc::new(locations);
    let cancel_pending = cancel_action.pending();
    let delete_pending = delete_action.pending();
    let any_pending = move || cancel_pending.get() || delete_pending.get();

    // Split the visible rows once: active runs take the cancel path,
    // terminal runs the delete path. A mixed selection shows both buttons,
    // each acting only on its own subset.
    let n_rows = rows.len();
    let all_ids = StoredValue::new(rows.iter().map(|r| r.run_id.clone()).collect::<Vec<_>>());
    let active = StoredValue::new(
        rows.iter()
            .filter(|r| run_is_active(&r.status))
            .map(|r| r.run_id.clone())
            .collect::<std::collections::HashSet<_>>(),
    );
    let selected_active = move || {
        active.with_value(|a| {
            selected
                .get()
                .into_iter()
                .filter(|id| a.contains(id))
                .collect::<Vec<_>>()
        })
    };
    let selected_finished = move || {
        active.with_value(|a| {
            selected
                .get()
                .into_iter()
                .filter(|id| !a.contains(id))
                .collect::<Vec<_>>()
        })
    };

    // Two-click confirm for cancel and delete; any selection change disarms.
    // Local to the table on purpose: a live-kick re-render also resets to
    // unarmed, which errs on the safe side.
    let cancel_armed = RwSignal::new(false);
    let delete_armed = RwSignal::new(false);
    Effect::new(move |_| {
        selected.track();
        cancel_armed.set(false);
        delete_armed.set(false);
    });

    view! {
        {(n_rows > 0).then(|| view! {
            <div class="bulk-actions">
                <button
                    class="link-btn"
                    on:click=move |_| set_selected.set(all_ids.get_value())
                >
                    {format!("Select all ({n_rows})")}
                </button>
                <span class="bulk-sep">"·"</span>
                <button class="link-btn" on:click=move |_| set_selected.set(Vec::new())>
                    "Clear"
                </button>
                <Show when=move || !selected_active().is_empty()>
                    <button
                        class="btn btn-small btn-danger"
                        on:click=move |_| {
                            if cancel_armed.get() {
                                cancel_armed.set(false);
                                cancel_action.dispatch(selected_active());
                            } else {
                                cancel_armed.set(true);
                            }
                        }
                        disabled=any_pending
                    >
                        <IconStop/>
                        {move || {
                            let runs = crate::helpers::plural(selected_active().len() as u64, "run", "runs");
                            if cancel_pending.get() {
                                "Canceling…".to_string()
                            } else if cancel_armed.get() {
                                format!("Confirm cancel {runs}?")
                            } else {
                                format!("Cancel {runs}")
                            }
                        }}
                    </button>
                </Show>
                <Show when=move || !selected_finished().is_empty()>
                    <button
                        class="btn btn-small btn-danger"
                        on:click=move |_| {
                            if delete_armed.get() {
                                delete_armed.set(false);
                                delete_action.dispatch(selected_finished());
                            } else {
                                delete_armed.set(true);
                            }
                        }
                        disabled=any_pending
                    >
                        <IconTrash/>
                        {move || {
                            let runs = crate::helpers::plural(selected_finished().len() as u64, "run", "runs");
                            if delete_pending.get() {
                                "Deleting…".to_string()
                            } else if delete_armed.get() {
                                format!("Confirm delete {runs}?")
                            } else {
                                format!("Delete {runs}")
                            }
                        }}
                    </button>
                </Show>
                {move || last_result.get().map(|(msg, is_err, detail)| view! {
                    <span
                        class=if is_err { "text-error" } else { "text-muted" }
                        title=detail
                    >
                        {msg}
                    </span>
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
    let id_for_check = run_id.clone();
    let id_for_toggle = run_id.clone();
    let (ns, name) = use_current_location().get_untracked();
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
    let sub_line = crate::helpers::launched_by_sub_line(&launched_by, job_name.as_deref());
    // Action runs surface their verb where the job name would sit.
    let sub_line = match (record.action.clone(), sub_line) {
        (Some(verb), Some(sub)) => Some(format!("{verb} · {sub}")),
        (Some(verb), None) => Some(verb),
        (None, sub) => sub,
    };
    let launched_cell = match sub_line {
        Some(sub) => view! { <LaunchedByCell launched_by=launched_by.clone() sub=sub/> }.into_any(),
        None => view! { <LaunchedByCell launched_by=launched_by/> }.into_any(),
    };

    view! {
        <A href=href attr:class="grid-row" attr:style=GRID attr:title=created_abs>
            <span class=rail_cls></span>
            <span on:click=move |ev: leptos::ev::MouseEvent| ev.stop_propagation()>
                <input
                    class="asset-row-check"
                    type="checkbox"
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
            <StatusChip kind=st_kind/>
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
