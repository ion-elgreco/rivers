//! Job detail page.
//!
//! The run history uses `get_runs_page` with an exact `job_name` filter, so
//! this page never downloads runs that belong to other jobs. Kicks on the
//! `runs` channel bump a `refresh_tick` that refetches the current slice.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::execute_job_dialog::ExecuteJobDialog;
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::pagination::Pagination;
use crate::components::ui_kit::{
    AssetStack, AssetSummaryRow, Crumb, KindBadge, PartitionCell, RecentRunsStrip, StatusChip,
    StripRun, Topbar,
};
use crate::helpers::{
    JobPartitionPicker, format_duration, format_timestamp, partition_picker_for_assets,
    run_status_class, run_status_kind, short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::execute_job;
use crate::server_fns::assets::get_assets;
use crate::server_fns::automation::get_jobs;
use crate::server_fns::overview::get_assets_info;
use crate::server_fns::runs::get_runs_page;
use crate::types::{AssetDefinitionInfo, RunFilter};

#[component]
pub fn JobDetailPage() -> impl IntoView {
    let params = use_params_map();
    let name = move || params.read_untracked().get("name").unwrap_or_default();
    let loc = use_current_location();

    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(25u64);
    let (refresh_tick, set_refresh_tick) = signal(0u64);

    let jobs = Resource::new(
        move || {
            params.track();
            (name(), loc.get())
        },
        |(_n, (ns, lname))| async move { get_jobs(ns, lname).await },
    );

    let runs_page_res = Resource::new(
        move || {
            params.track();
            (name(), page.get(), page_size.get(), refresh_tick.get())
        },
        |(job_name, p, ps, _tick)| async move {
            let filter = RunFilter {
                job_name: Some(job_name),
                ..Default::default()
            };
            get_runs_page(p * ps, ps, filter).await
        },
    );

    let all_assets = Resource::new(
        move || loc.get(),
        |(ns, name)| get_assets(ns, name, None, None, None),
    );

    let assets_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_assets_info(ns, name).await },
    );

    let live_status = use_live_kick(
        &["runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let (exec_pending, set_exec_pending) = signal(false);
    let (exec_error, set_exec_error) = signal::<Option<String>>(None);
    let show_dialog = RwSignal::new(false);
    let navigate = leptos_router::hooks::use_navigate();

    // `JobPartitionPicker::None` means there's nothing for the dialog to
    // show — skip it and submit directly.
    let job_picker = Signal::derive(move || -> JobPartitionPicker {
        let current = name();
        let Some(Ok(jobs_list)) = jobs.get() else {
            return JobPartitionPicker::None;
        };
        let Some(job) = jobs_list.into_iter().find(|j| j.name == current) else {
            return JobPartitionPicker::None;
        };
        let Some(Ok(infos)) = assets_info.get() else {
            return JobPartitionPicker::None;
        };
        let by_key: std::collections::HashMap<String, AssetDefinitionInfo> = infos
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect();
        partition_picker_for_assets(&job.asset_selection, &by_key)
    });

    let dialog_job_name: Signal<String> = Signal::derive(name);

    let on_execute = move |_| {
        if !matches!(job_picker.get(), JobPartitionPicker::None) {
            set_exec_error.set(None);
            show_dialog.set(true);
            return;
        }
        let job_name = name();
        let navigate = navigate.clone();
        let (ns, lname) = loc.get();
        set_exec_pending.set(true);
        set_exec_error.set(None);
        leptos::task::spawn_local(async move {
            let path_ns = ns.clone();
            let path_name = lname.clone();
            match execute_job(ns, lname, job_name, None).await {
                Ok(result) if !result.run_id.is_empty() => {
                    let path = loc_path(&path_ns, &path_name, &format!("runs/{}", result.run_id));
                    navigate(&path, Default::default());
                }
                Ok(_) => {
                    set_exec_error.set(Some("Execution returned no run id.".to_string()));
                    set_exec_pending.set(false);
                }
                Err(e) => {
                    set_exec_error.set(Some(format!("{e}")));
                    set_exec_pending.set(false);
                }
            }
        });
    };

    Effect::new(move |_| {
        if let Some(Ok(p)) = runs_page_res.get()
            && p.rows.is_empty()
            && p.total > 0
            && page.get_untracked() > 0
        {
            set_page.set(0);
        }
    });

    let (ns_t, name_t) = loc.get();
    let jobs_href = loc_path(&ns_t, &name_t, "jobs");
    view! {
        <Topbar crumbs=vec![
            Crumb::linked("Jobs", jobs_href),
            Crumb::new(name()).mono(),
        ]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
            <button
                class="btn btn-primary"
                on:click=on_execute
                disabled=move || exec_pending.get()
            >
                <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                    <path d="M3 2l7 4-7 4V2z"/>
                </svg>
                {move || if exec_pending.get() { "Executing..." } else { "Execute" }}
            </button>
            {move || exec_error.get().map(|msg| view! {
                <span class="text-error" style="margin-left: 0.5rem">{msg}</span>
            })}
        </Topbar>

        <ExecuteJobDialog
            show=show_dialog
            job_name=dialog_job_name
            picker=job_picker
        />

        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                let current_name = name();
                let run_records = runs_page_res
                    .get()
                    .and_then(|r| r.ok())
                    .map(|p| p.rows)
                    .unwrap_or_default();
                let last_run = run_records.first().cloned();
                let last_run_ts: Option<i64> = last_run.as_ref().map(|r| r.start_time);
                let last_run_status = last_run.as_ref()
                    .map(|r| run_status_kind(&r.status).to_string());

                jobs.get().map(|result| match result {
                    Ok(all_jobs) => {
                        if let Some(job) = all_jobs.into_iter().find(|j| j.name == current_name) {
                            let assets: Vec<String> = job.asset_selection.clone();
                            let asset_count = assets.len();
                            let asset_count_label = if asset_count == 0 {
                                "all".to_string()
                            } else {
                                asset_count.to_string()
                            };
                            let executor_type = job.executor_type.clone();

                            let strip: Vec<StripRun> = run_records.iter().rev().take(20).rev().map(|r| {
                                let status = match r.status {
                                    crate::types::RunStatus::Success => "ok",
                                    crate::types::RunStatus::Failure => "err",
                                    crate::types::RunStatus::Started | crate::types::RunStatus::NotStarted | crate::types::RunStatus::Queued => "retry",
                                    _ => "err",
                                };
                                let live = matches!(r.status, crate::types::RunStatus::Started);
                                let dur = r.end_time.map(|e| (e - r.start_time).max(1) as f64 / 1e9).unwrap_or(5.0);
                                let id = short_id(&r.run_id, 8);
                                StripRun { id, status, duration_s: dur, live }
                            }).collect();

                            let asset_records = all_assets.get().and_then(|r| r.ok()).unwrap_or_default();

                            view! {
                                <div class="meta-tile-grid meta-tile-grid--4">
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"STATUS"</div>
                                        <div class="meta-tile-value">
                                            {match last_run_status {
                                                Some(s) => view! { <StatusChip kind=s small=true/> }.into_any(),
                                                None => view! { <span>"—"</span> }.into_any(),
                                            }}
                                        </div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"EXECUTOR"</div>
                                        <div class="meta-tile-value"><KindBadge kind=executor_type/></div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"ASSETS"</div>
                                        <div class="meta-tile-value">{asset_count_label}</div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"LAST RUN"</div>
                                        <div class="meta-tile-value">
                                            <crate::now::RelTimeOpt ts=last_run_ts/>
                                        </div>
                                    </div>
                                </div>

                                <div style="margin-top:20px">
                                    <RecentRunsStrip runs=strip label="RECENT RUNS · LAST 20".to_string()/>
                                </div>

                                <div class="section-header-row" style="margin-top:24px">
                                    <span class="section-header-label">
                                        {format!("ASSET SELECTION · {}", asset_count)}
                                    </span>
                                </div>
                                {if assets.is_empty() {
                                    view! { <div class="empty-state">"This job has no asset selection."</div> }.into_any()
                                } else {
                                    view! {
                                        <div class="asset-summary-list">
                                            {assets.into_iter().map(|key| {
                                                let asset = asset_records.iter().find(|a| a.asset_key == key).cloned();
                                                view! { <AssetSummaryRow asset_key=key asset=asset/> }
                                            }).collect::<Vec<_>>()}
                                        </div>
                                    }.into_any()
                                }}
                            }.into_any()
                        } else {
                            view! { <div class="error-msg">"Job not found."</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading" style="margin-top:24px">"Loading runs..."</div> }>
            {move || {
                runs_page_res.get().map(|result| match result {
                    Ok(page_data) => {
                        let run_count = page_data.total;
                        let rows = page_data.rows;
                        view! {
                            <div class="section-header-row" style="margin-top:28px">
                                <span class="section-header-label">{format!("RUN HISTORY · {run_count}")}</span>
                            </div>
                            {if rows.is_empty() {
                                view! { <div class="empty-state">"No runs for this job."</div> }.into_any()
                            } else {
                                const GRID: &str = "grid-template-columns: 88px 0.7fr 1.5fr 0.6fr 0.9fr 0.7fr";
                                view! {
                                    <div class="grid-table">
                                        <div class="grid-table-head" style=GRID>
                                            <span>"RUN"</span>
                                            <span>"STATUS"</span>
                                            <span>"ASSETS"</span>
                                            <span>"PARTITION"</span>
                                            <span>"STARTED"</span>
                                            <span>"DURATION"</span>
                                        </div>
                                        {rows.into_iter().map(|r| {
                                            let run_id = r.run_id.clone();
                                            let (ns, lname) = loc.get();
                                            let href = loc_path(&ns, &lname, &format!("runs/{}", run_id));
                                            let sid = short_id(&run_id, 8);
                                            let st_class = run_status_class(&r.status);
                                            let st_kind = run_status_kind(&r.status);
                                            let start_ts = r.start_time;
                                            let created_abs = format_timestamp(Some(r.start_time));
                                            let duration = format_duration(Some(r.start_time), r.end_time);
                                            let partition_val: Option<String> = r.tags.iter()
                                                .find(|(k, _)| k == "partition" || k == "partition_key")
                                                .map(|(_, v)| v.clone());
                                            let asset_names = r.node_names.clone();
                                            let rail_cls = format!("grid-row-rail grid-row-rail--{}", st_class);
                                            let part_scheme = partition_val.as_deref()
                                                .map(crate::components::ui_kit::partition_scheme_for)
                                                .unwrap_or("·");
                                            view! {
                                                <A href=href attr:class="grid-row" attr:style=GRID attr:title=created_abs>
                                                    <span class=rail_cls></span>
                                                    <span class="grid-cell-mono">{sid}</span>
                                                    <StatusChip kind=st_kind small=true/>
                                                    <AssetStack assets=asset_names/>
                                                    {partition_val
                                                        .map(|p| view! { <PartitionCell scheme=part_scheme count_label=p/> }.into_any())
                                                        .unwrap_or_else(|| view! { <span class="grid-cell-muted">"—"</span> }.into_any())}
                                                    <span class="grid-cell-muted"><crate::now::RelTime ts=start_ts/></span>
                                                    <span class="grid-cell-muted">{duration}</span>
                                                </A>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                    <Pagination
                                        total=run_count
                                        page=page
                                        set_page=set_page
                                        page_size=page_size
                                        set_page_size=set_page_size
                                    />
                                }.into_any()
                            }}
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
