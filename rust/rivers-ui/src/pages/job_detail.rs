//! Job detail page.
//!
//! The run history uses `get_runs_page` with an exact `job_name` filter, so
//! this page never downloads runs that belong to other jobs. Kicks on the
//! `runs` channel bump a `refresh_tick` that refetches the current slice.

use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::components::execute_job_dialog::ExecuteJobDialog;
use crate::components::icons::IconPlay;
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::pagination::Pagination;
use crate::components::ui_kit::{
    AssetSummaryRow, Crumb, EmptyState, KindBadge, RecentRunsStrip, RunsGrid, SectionHeader,
    StatusChip, StripRun, Topbar,
};
use crate::helpers::{
    JobPartitionPicker, job_partition_picker, job_verb, replay_click, run_status_kind,
    use_confirm_armed,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::get_assets;
use crate::server_fns::automation::get_jobs;
use crate::server_fns::mutations::execute_job;
use crate::server_fns::overview::{get_assets_info, get_resources_info};
use crate::server_fns::runs::get_runs_page;
use crate::types::{AssetActionInfo, AssetDefinitionInfo, RunFilter};

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

    // Tiles and the strip always describe the newest runs, whatever history page is open.
    let latest_runs = Resource::new(
        move || {
            params.track();
            (name(), refresh_tick.get())
        },
        |(job_name, _tick)| async move {
            let filter = RunFilter {
                job_name: Some(job_name),
                ..Default::default()
            };
            get_runs_page(0, 20, filter).await
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

    // The job, resolved against its assets' declarations: the verb it runs
    // (if any) and the partition picker that verb allows. `None` until the
    // job's definition has loaded: with no verb to show, Execute waits.
    let jobs_value = crate::helpers::resource_value(jobs);
    let assets_info_value = crate::helpers::resource_value(assets_info);
    let resources_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_resources_info(ns, name).await },
    );
    let resources_info_value = crate::helpers::resource_value(resources_info);
    let launch_resources = Signal::derive(move || {
        resources_info_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
    });
    // The dialog's config editor reads the job's assets and their schemas.
    let job_assets = Signal::derive(move || {
        let current = name();
        jobs_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .find(|j| j.name == current)
            .map(|j| j.asset_selection)
            .unwrap_or_default()
    });
    let asset_info_by_key = Memo::new(move |_| {
        assets_info_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect::<std::collections::HashMap<String, AssetDefinitionInfo>>()
    });
    let job_launch = Memo::new(
        move |_| -> Option<(Option<AssetActionInfo>, JobPartitionPicker)> {
            let current = name();
            let Some(Ok(jobs_list)) = jobs_value.get() else {
                return None;
            };
            let job = jobs_list.into_iter().find(|j| j.name == current)?;
            let infos = assets_info_value
                .get()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let by_key: std::collections::HashMap<String, AssetDefinitionInfo> = infos
                .into_iter()
                .map(|i| (i.asset_key.clone(), i))
                .collect();
            let verb = job_verb(job.action.as_deref(), &job.asset_selection, &by_key);
            let picker = job_partition_picker(verb.as_ref(), &job.asset_selection, &by_key);
            Some((verb, picker))
        },
    );
    // `JobPartitionPicker::None` means there's nothing for the dialog to
    // show — skip it and submit directly.
    let job_picker = Signal::derive(move || {
        job_launch
            .get()
            .map_or(JobPartitionPicker::None, |(_, picker)| picker)
    });
    let job_verb_signal = Signal::derive(move || job_launch.get().and_then(|(verb, _)| verb));
    // The dialog picks partitions and edits config; a job needing neither
    // runs on the click.
    let job_opens_dialog = Signal::derive(move || {
        !matches!(job_picker.get(), JobPartitionPicker::None)
            || crate::components::config_editor::launch_takes_config(
                &job_assets.get(),
                &asset_info_by_key.get(),
                job_verb_signal.get().as_ref().map(|v| v.name.as_str()),
            )
    });
    let job_loaded = Signal::derive(move || job_launch.get().is_some());
    let exec_armed = use_confirm_armed(move || params.track());

    let dialog_job_name: Signal<String> = Signal::derive(name);

    let on_execute = move |_| {
        let Some((verb, _)) = job_launch.get() else {
            return;
        };
        if job_opens_dialog.get() {
            set_exec_error.set(None);
            show_dialog.set(true);
            return;
        }
        // A destructive verb takes a second click, as on the run page.
        let destructive = verb.as_ref().is_some_and(|v| v.is_destructive());
        let (dispatch, now_armed) = replay_click(destructive, exec_armed.get());
        exec_armed.set(now_armed);
        if !dispatch {
            return;
        }
        let job_name = name();
        let shown = verb.map(|v| v.name);
        let navigate = navigate.clone();
        let (ns, lname) = loc.get_untracked();
        set_exec_pending.set(true);
        set_exec_error.set(None);
        leptos::task::spawn_local(async move {
            let path_ns = ns.clone();
            let path_name = lname.clone();
            match execute_job(ns, lname, job_name, shown, None, false, None).await {
                Ok(result) if !result.run_id.is_empty() => {
                    let path = loc_path(&path_ns, &path_name, &format!("runs/{}", result.run_id));
                    navigate(&path, Default::default());
                }
                Ok(_) => {
                    set_exec_error.set(Some("Execution returned no run id.".to_string()));
                    set_exec_pending.set(false);
                }
                Err(e) => {
                    set_exec_error.set(Some(crate::helpers::err_text(&e)));
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

    let (ns_t, name_t) = loc.get_untracked();
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
            {move || exec_error.get().map(|msg| view! { <span class="text-error">{msg}</span> })}
            <button
                class=move || if job_verb_signal.get().is_some_and(|v| v.is_destructive()) {
                    "btn btn-danger"
                } else {
                    "btn btn-primary"
                }
                on:click=on_execute
                disabled=move || exec_pending.get() || !job_loaded.get()
            >
                <IconPlay/>
                {move || if exec_pending.get() {
                    "Executing…".to_string()
                } else if exec_armed.get() {
                    format!(
                        "Confirm {}?",
                        job_verb_signal.get().map(|v| v.name).unwrap_or_default()
                    )
                } else if job_opens_dialog.get() {
                    "Execute…".to_string()
                } else {
                    "Execute".to_string()
                }}
            </button>
            // The one-click launch runs the job as defined; the dialog edits
            // the launch document (metadata, resources, executor) for any job.
            <Show when=move || job_loaded.get() && !job_opens_dialog.get()>
                <button
                    class="btn"
                    title="Execute with a launch document"
                    on:click=move |_| {
                        set_exec_error.set(None);
                        show_dialog.set(true);
                    }
                    disabled=move || exec_pending.get()
                >
                    "Execute…"
                </button>
            </Show>
        </Topbar>

        <ExecuteJobDialog
            show=show_dialog
            job_name=dialog_job_name
            picker=job_picker
            verb=job_verb_signal
            assets=job_assets
            definitions=asset_info_by_key
            resources=launch_resources
        />

        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                let current_name = name();
                let latest = latest_runs
                    .get()
                    .and_then(|r| r.ok())
                    .map(|p| p.rows)
                    .unwrap_or_default();
                let last_run = latest.first().cloned();
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

                            let strip = StripRun::from_runs(&latest, 20);

                            let asset_records = all_assets.get().and_then(|r| r.ok()).unwrap_or_default();

                            view! {
                                <div class="meta-tile-grid meta-tile-grid--4">
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"STATUS"</div>
                                        <div class="meta-tile-value">
                                            {match last_run_status {
                                                Some(s) => view! { <StatusChip kind=s/> }.into_any(),
                                                None => view! { <span>"—"</span> }.into_any(),
                                            }}
                                        </div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">{if job.action.is_some() { "ACTION" } else { "EXECUTOR" }}</div>
                                        <div class="meta-tile-value">
                                            // Action steps run in the run's own process whatever
                                            // the executor, so the verb is the useful fact here.
                                            {match job.action.clone() {
                                                Some(verb) => view! { <span class="grid-cell-mono">{verb}</span> }.into_any(),
                                                None => view! { <KindBadge kind=crate::helpers::executor_label(&executor_type)/> }.into_any(),
                                            }}
                                        </div>
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
                                    <RecentRunsStrip runs=strip label="RUN DURATIONS · LAST 20".to_string()/>
                                </div>

                                <SectionHeader label="ASSET SELECTION" count=asset_count.to_string()/>
                                {if assets.is_empty() {
                                    view! { <EmptyState message="Selects all assets" compact=true/> }.into_any()
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
                            view! { <div class="error-msg">"Job not found"</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load job: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading" style="margin-top:24px">"Loading runs…"</div> }>
            {move || {
                runs_page_res.get().map(|result| match result {
                    Ok(page_data) => {
                        let run_count = page_data.total;
                        let rows = page_data.rows;
                        view! {
                            <SectionHeader label="RUN HISTORY" count=run_count.to_string()/>
                            {if rows.is_empty() {
                                view! { <EmptyState message="No runs yet" compact=true/> }.into_any()
                            } else {
                                view! {
                                    <RunsGrid rows=rows show_assets=true/>
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
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load runs: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
