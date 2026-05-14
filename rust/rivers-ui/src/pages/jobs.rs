//! Jobs list page.
//!
//! The job *definitions* come from the gRPC code-location service (static for
//! the session); the *last-run-per-job* data comes from storage and live-
//! updates on the `runs` channel. Splitting the two keeps the definitions
//! Resource keyed on `()` (one fetch per session) while kicks only rerun the
//! much cheaper last-run query.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::execute_job_dialog::ExecuteJobDialog;
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{AssetStack, Crumb, EmptyState, KindBadge, StatusChip, Topbar};
use crate::helpers::{
    JobPartitionPicker, partition_picker_for_assets, run_status_class, run_status_kind, short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::execute_job;
use crate::server_fns::automation::get_jobs;
use crate::server_fns::overview::get_assets_info;
use crate::server_fns::runs::get_last_run_per_job;
use crate::types::{AssetDefinitionInfo, RunRecord};

#[component]
pub fn JobsListPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let loc = use_current_location();

    let jobs = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_jobs(ns, name).await },
    );

    // Scoped by the already-resolved job names from `jobs`, so a live kick only
    // refetches last-runs (not definitions).
    let last_runs = Resource::new(
        move || {
            let names: Vec<String> = jobs
                .get()
                .and_then(|r| r.ok())
                .map(|js| js.into_iter().map(|j| j.name).collect())
                .unwrap_or_default();
            (refresh_tick.get(), names)
        },
        |(_tick, names)| async move {
            if names.is_empty() {
                return Ok(Vec::new());
            }
            get_last_run_per_job(names).await
        },
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

    let navigate = leptos_router::hooks::use_navigate();

    let show_dialog = RwSignal::new(false);
    let dialog_job = RwSignal::new(String::new());
    let dialog_picker = RwSignal::new(JobPartitionPicker::None);
    let dialog_job_signal: Signal<String> = dialog_job.into();
    let dialog_picker_signal: Signal<JobPartitionPicker> = dialog_picker.into();

    view! {
        <Topbar crumbs=vec![Crumb::new("Jobs")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <ExecuteJobDialog
            show=show_dialog
            job_name=dialog_job_signal
            picker=dialog_picker_signal
        />

        <Transition fallback=move || view! { <div class="loading">"Loading jobs..."</div> }>
            {move || {
                // Wait for BOTH resources before rendering — rendering with
                // last_runs==None would flip DOM structure between SSR and
                // hydration and break hydration.
                let (Some(jobs_result), Some(last_runs_result)) = (jobs.get(), last_runs.get()) else {
                    return None;
                };
                // assets_info may still be loading; default to empty so a slow
                // gRPC call doesn't block the whole table from rendering.
                let infos: Vec<AssetDefinitionInfo> =
                    assets_info.get().and_then(|r| r.ok()).unwrap_or_default();
                let asset_info_by_key: HashMap<String, AssetDefinitionInfo> =
                    infos.into_iter().map(|i| (i.asset_key.clone(), i)).collect();
                let last_runs_map: HashMap<String, RunRecord> =
                    last_runs_result.ok().unwrap_or_default().into_iter().collect();
                Some(match jobs_result {
                    Ok(records) => {
                        if records.is_empty() {
                            return Some(view! {
                                <EmptyState
                                    message="No jobs defined"
                                    hint="declare a job with rivers.asset_job() in your code location"
                                />
                            }.into_any());
                        }
                        const GRID: &str = "grid-template-columns: 1.4fr 0.7fr 1.6fr 0.8fr 1.1fr 120px";
                        view! {
                            <div class="grid-table">
                                <div class="grid-table-head" style=GRID>
                                    <span>"NAME"</span>
                                    <span>"EXECUTOR"</span>
                                    <span>"ASSETS"</span>
                                    <span>"STATUS"</span>
                                    <span>"LAST RUN"</span>
                                    <span></span>
                                </div>
                                {
                                let (ns, name) = loc.get();
                                records.into_iter().map(|job| {
                                    let job_name = job.name.clone();
                                    let exec_name = job_name.clone();
                                    let href = loc_path(&ns, &name, &format!("jobs/{}", job_name));
                                    let asset_selection = job.asset_selection.clone();
                                    let executor_type = job.executor_type.clone();

                                    let last_run = last_runs_map.get(&job_name).cloned();
                                    let rail_cls = last_run
                                        .as_ref()
                                        .map(|r| format!("grid-row-rail grid-row-rail--{}", run_status_class(&r.status)))
                                        .unwrap_or_else(|| "grid-row-rail grid-row-rail--muted".to_string());

                                    let row_picker =
                                        partition_picker_for_assets(&asset_selection, &asset_info_by_key);

                                    let (exec_pending, set_exec_pending) = signal(false);
                                    let navigate = navigate.clone();
                                    let nav_run = navigate.clone();

                                    let on_execute = {
                                        let ns = ns.clone();
                                        let name = name.clone();
                                        let row_picker = row_picker.clone();
                                        move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            ev.stop_propagation();
                                            if !matches!(row_picker, JobPartitionPicker::None) {
                                                dialog_job.set(exec_name.clone());
                                                dialog_picker.set(row_picker.clone());
                                                show_dialog.set(true);
                                                return;
                                            }
                                            let n = exec_name.clone();
                                            let navigate = navigate.clone();
                                            let ns = ns.clone();
                                            let name = name.clone();
                                            set_exec_pending.set(true);
                                            leptos::task::spawn_local(async move {
                                                let path_ns = ns.clone();
                                                let path_name = name.clone();
                                                match execute_job(ns, name, n, None).await {
                                                    Ok(result) if !result.run_id.is_empty() => {
                                                        let path = loc_path(&path_ns, &path_name, &format!("runs/{}", result.run_id));
                                                        navigate(&path, Default::default());
                                                    }
                                                    _ => {
                                                        set_exec_pending.set(false);
                                                    }
                                                }
                                            });
                                        }
                                    };

                                    let status_cell = match &last_run {
                                        Some(r) => view! { <StatusChip kind=run_status_kind(&r.status).to_string() small=true/> }.into_any(),
                                        None => view! { <span class="grid-cell-muted">"—"</span> }.into_any(),
                                    };
                                    let last_run_cell = match last_run.as_ref() {
                                        Some(r) => {
                                            let rid = short_id(&r.run_id, 8);
                                            let rhref = loc_path(&ns, &name, &format!("runs/{}", r.run_id));
                                            let start_ts = r.start_time;
                                            view! {
                                                <span style="display:flex; flex-direction:column; gap:2px; min-width:0">
                                                    <span
                                                        class="grid-cell-mono"
                                                        role="link"
                                                        tabindex="0"
                                                        style="color:var(--text); cursor:pointer"
                                                        on:click=move |ev: leptos::ev::MouseEvent| {
                                                            ev.prevent_default();
                                                            ev.stop_propagation();
                                                            nav_run(&rhref, Default::default());
                                                        }
                                                    >{rid}</span>
                                                    <span class="grid-cell-muted" style="font-size:10.5px">
                                                        <crate::now::RelTime ts=start_ts/>
                                                    </span>
                                                </span>
                                            }.into_any()
                                        }
                                        None => view! { <span class="grid-cell-muted">"never"</span> }.into_any(),
                                    };
                                    view! {
                                        <A href=href attr:class="grid-row" attr:style=GRID>
                                            <span class=rail_cls></span>
                                            <span class="grid-cell-mono">{job_name}</span>
                                            <KindBadge kind=executor_type/>
                                            {if asset_selection.is_empty() {
                                                view! { <span class="grid-cell-muted">"all"</span> }.into_any()
                                            } else {
                                                view! { <AssetStack assets=asset_selection/> }.into_any()
                                            }}
                                            {status_cell}
                                            {last_run_cell}
                                            <button
                                                class="btn btn-tertiary"
                                                on:click=on_execute
                                                disabled=move || exec_pending.get()
                                                style="justify-content:center"
                                                title="Execute job"
                                            >
                                                <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                                                    <path d="M3 2l7 4-7 4V2z"/>
                                                </svg>
                                                {move || if exec_pending.get() { "..." } else { "Execute" }}
                                            </button>
                                        </A>
                                    }
                                }).collect::<Vec<_>>()
                                }
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
