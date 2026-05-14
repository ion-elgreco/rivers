//! Backfills list page.
//!
//! Architecture mirrors the runs list: server-side pagination + filter via
//! two keyed Resources (page data + aggregate counts), live-updated via an
//! SSE kick on the `backfills` channel. Page-load cost is independent of
//! total backfill count.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::GridRowSkeleton;
use crate::components::pagination::Pagination;
use crate::components::ui_kit::{
    AssetStack, Crumb, EmptyState, FilterPillGroup, PartitionCell, ProgressBar, StatusChip, Topbar,
};
use crate::helpers::{
    backfill_status_kind, code_location_label, format_call_multiline, format_duration,
    format_timestamp,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::RelTime;
use crate::server_fns::backfills::{get_backfills_page, get_backfills_summary};
use crate::server_fns::locations::list_code_locations;
use crate::types::{BackfillFilter, BackfillInfo, BackfillsSummary, CodeLocationEntry};

const GRID: &str = "grid-template-columns: 88px 0.9fr 0.9fr 1.6fr 1.4fr 0.5fr 0.9fr 0.9fr 1fr";

fn rail_class(status: &str) -> &'static str {
    match status {
        "InProgress" => "running",
        "CompletedSuccess" => "success",
        "CompletedFailed" => "failed",
        "Canceled" => "warning",
        _ => "queued",
    }
}

fn status_from_tab(tab: &str) -> Option<String> {
    match tab {
        "In Progress" => Some("InProgress".into()),
        "Completed" => Some("CompletedSuccess".into()),
        "Failed" => Some("CompletedFailed".into()),
        "Canceled" => Some("Canceled".into()),
        _ => None,
    }
}

#[component]
pub fn BackfillsListPage() -> impl IntoView {
    let (active_tab, set_active_tab) = signal("All".to_string());
    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(25u64);
    let (refresh_tick, set_refresh_tick) = signal(0u64);

    let page_key = move || {
        (
            active_tab.get(),
            page.get(),
            page_size.get(),
            refresh_tick.get(),
        )
    };
    let backfills_page = Resource::new(page_key, |(tab, p, ps, _tick)| {
        let filter = BackfillFilter {
            status: status_from_tab(&tab),
        };
        async move { get_backfills_page(p * ps, ps, filter).await }
    });

    let locations = Resource::new(|| (), |_| list_code_locations());

    // Summary keyed only on refresh_tick so the pill badges stay stable while
    // the user flips tabs or paginates.
    let summary = Resource::new(
        move || refresh_tick.get(),
        |_| async move { get_backfills_summary().await },
    );

    let live_status = use_live_kick(
        &["backfills"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let reload = Callback::new(move |_: ()| set_refresh_tick.update(|t| *t += 1));

    let active_tab_sig = Signal::derive(move || active_tab.get());
    let on_tab = Callback::new(move |v: String| {
        set_active_tab.set(v);
        set_page.set(0);
    });

    Effect::new(move |_| {
        if let Some(Ok(p)) = backfills_page.get()
            && p.rows.is_empty()
            && p.total > 0
            && page.get_untracked() > 0
        {
            set_page.set(0);
        }
    });

    view! {
        <Topbar crumbs=vec![Crumb::new("Backfills")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| reload.run(()))
            />
        </Topbar>

        // On fetch error we render pills with `None` badges instead of falling
        // back to zero counts, which would falsely claim "no backfills exist".
        <Transition fallback=move || view! { <GridRowSkeleton rows=1 cols=5/> }>
            {move || {
                let (status_items, error_title): (Vec<(String, Option<usize>)>, Option<String>) =
                    match summary.get() {
                        Some(Ok(BackfillsSummary {
                            total, in_progress, completed_success, completed_failed, canceled,
                        })) => (
                            vec![
                                ("All".into(), Some(total as usize)),
                                ("In Progress".into(), Some(in_progress as usize)),
                                ("Completed".into(), Some(completed_success as usize)),
                                ("Failed".into(), Some(completed_failed as usize)),
                                ("Canceled".into(), Some(canceled as usize)),
                            ],
                            None,
                        ),
                        Some(Err(e)) => (
                            vec![
                                ("All".into(), None),
                                ("In Progress".into(), None),
                                ("Completed".into(), None),
                                ("Failed".into(), None),
                                ("Canceled".into(), None),
                            ],
                            Some(format!("Summary fetch failed: {e}")),
                        ),
                        None => return ().into_any(),
                    };
                view! {
                    <div class="rv-toolbar" title=error_title.unwrap_or_default()>
                        <FilterPillGroup
                            label="STATUS"
                            items=status_items
                            active=active_tab_sig
                            on_select=on_tab
                        />
                    </div>
                }.into_any()
            }}
        </Transition>

        <Transition fallback=move || view! { <GridRowSkeleton rows=10 cols=9/> }>
            {move || match backfills_page.get() {
                None => view! { <GridRowSkeleton rows=10 cols=9/> }.into_any(),
                Some(Err(e)) => view! {
                    <div class="error-msg">{format!("Error loading backfills: {e}")}</div>
                }.into_any(),
                Some(Ok(page_data)) if page_data.total == 0 => view! {
                    <EmptyState
                        message="No backfills match this filter"
                        hint="switch tabs or trigger one from an asset page"
                    />
                }.into_any(),
                Some(Ok(page_data)) => {
                    let locs = locations
                        .get()
                        .and_then(|r| r.ok())
                        .unwrap_or_default();
                    view! {
                        <BackfillsTable rows=page_data.rows locations=locs/>
                        <Pagination
                            total=page_data.total
                            page=page
                            set_page=set_page
                            page_size=page_size
                            set_page_size=set_page_size
                        />
                    }.into_any()
                }
            }}
        </Transition>
    }
}

#[component]
fn BackfillsTable(rows: Vec<BackfillInfo>, locations: Vec<CodeLocationEntry>) -> impl IntoView {
    let locations = std::sync::Arc::new(locations);
    view! {
        <div class="grid-table">
            <div class="grid-table-head" style=GRID>
                <span>"ID"</span>
                <span>"STATUS"</span>
                <span>"STRATEGY"</span>
                <span>"ASSETS"</span>
                <span>"PARTITIONS"</span>
                <span>"RUNS"</span>
                <span>"CREATED"</span>
                <span>"DURATION"</span>
                <span>"CODE LOCATION"</span>
            </div>
            <For
                each=move || rows.clone()
                key=|r: &BackfillInfo| r.backfill_id.clone()
                children=move |record: BackfillInfo| {
                    let label = code_location_label(&record.code_location_id, &locations);
                    view! { <BackfillRow record=record code_location_label=label/> }
                }
            />
        </div>
    }
}

#[component]
fn BackfillRow(record: BackfillInfo, code_location_label: String) -> impl IntoView {
    let (ns, name) = use_current_location().get();
    let href = loc_path(&ns, &name, &format!("backfills/{}", record.backfill_id));
    let short_id = if record.backfill_id.len() > 8 {
        record.backfill_id[..8].to_string()
    } else {
        record.backfill_id.clone()
    };
    let cl_title = record.code_location_id.clone();
    let rail = format!(
        "grid-row-rail grid-row-rail--{}",
        rail_class(&record.status)
    );
    let st_kind = backfill_status_kind(&record.status);
    let create_ts = record.create_time;
    let created_abs = format_timestamp(Some(record.create_time));
    let duration = format_duration(Some(record.create_time), record.end_time);
    let completed = record.completed_partitions;
    let total = record.total_partitions;
    let progress_ratio = if total > 0 {
        completed as f64 / total as f64
    } else {
        0.0
    };
    let progress_sig = Signal::derive(move || progress_ratio);
    let partition_label = format!("{completed} of {total}");
    let run_count = record.run_ids.len();
    let color = match record.status.as_str() {
        "CompletedFailed" => "var(--error)",
        "Canceled" => "var(--warning)",
        "InProgress" => "var(--secondary)",
        _ => "var(--success)",
    }
    .to_string();

    view! {
        <A href=href attr:class="grid-row" attr:style=GRID attr:title=created_abs>
            <span class=rail></span>
            <span class="grid-cell-mono">{short_id}</span>
            <StatusChip kind=st_kind small=true/>
            <span class="grid-cell-muted grid-cell-code">
                {format_call_multiline(&record.strategy)}
            </span>
            <AssetStack assets=record.asset_selection/>
            <div style="display:flex; flex-direction:column; gap:4px; min-width:0">
                <PartitionCell scheme="·" count_label=partition_label/>
                <ProgressBar value=progress_sig color=color/>
            </div>
            <span class="grid-cell-muted">{run_count}</span>
            <span class="grid-cell-muted"><RelTime ts=create_ts/></span>
            <span class="grid-cell-muted">{duration}</span>
            <span class="grid-cell-muted" title=cl_title>{code_location_label}</span>
        </A>
    }
}
