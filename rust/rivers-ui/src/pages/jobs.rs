//! Jobs list page.
//!
//! The job *definitions* come from the gRPC code-location service, fetched
//! once per page (Execute sends the verb it shows, and the server refuses a
//! job whose verb changed since); the *last-run-per-job* data comes from
//! storage and live-updates on the `runs` channel. Splitting the two keeps
//! the definitions Resource keyed on the location while kicks only rerun the
//! much cheaper last-run query.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::execute_job_dialog::ExecuteJobDialog;
use crate::components::icons::{IconPlay, IconTrash};
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{
    AssetStack, EmptyState, KindBadge, SplitButton, StatusChip, Topbar,
};
use crate::helpers::{
    JobPartitionPicker, job_partition_picker, job_verb, replay_click, run_status_class,
    run_status_kind, short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::automation::get_jobs;
use crate::server_fns::mutations::execute_job;
use crate::server_fns::overview::get_assets_info;
use crate::server_fns::runs::get_last_run_per_job;
use crate::types::{AssetActionInfo, AssetDefinitionInfo, RunRecord};

#[component]
pub fn JobsListPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let live = use_live_kick(
        &["runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );
    let loc = use_current_location();

    let jobs = Resource::new(
        move || (loc.get(), live.definitions.get()),
        |((ns, name), _)| async move { get_jobs(ns, name).await },
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
        move || (loc.get(), live.definitions.get()),
        |((ns, name), _)| async move { get_assets_info(ns, name).await },
    );

    let navigate = leptos_router::hooks::use_navigate();

    let show_dialog = RwSignal::new(false);
    let launch_resources =
        crate::components::config_editor::use_launch_resources(loc, show_dialog.into());
    let dialog_job = RwSignal::new(String::new());
    let dialog_picker = RwSignal::new(JobPartitionPicker::None);
    let dialog_verb = RwSignal::new(None::<AssetActionInfo>);
    let dialog_job_signal: Signal<String> = dialog_job.into();
    let dialog_picker_signal: Signal<JobPartitionPicker> = dialog_picker.into();
    let dialog_verb_signal: Signal<Option<AssetActionInfo>> = dialog_verb.into();
    let exec_error = RwSignal::new(Option::<String>::None);
    // The dialog's config editor reads the job's assets and their schemas.
    let jobs_value = crate::helpers::resource_value(jobs);
    let assets_info_value = crate::helpers::resource_value(assets_info);
    let dialog_assets = Signal::derive(move || {
        let job = dialog_job.get();
        jobs_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .find(|j| j.name == job)
            .map(|j| j.asset_selection)
            .unwrap_or_default()
    });
    let asset_info_by_key = crate::helpers::definitions_by_key(assets_info_value);

    view! {
        <Topbar
            title="Jobs"
            subtitle=move || view! {
                <Transition>
                    {move || jobs.get().and_then(|r| r.ok()).map(|list| view! {
                        <span class="page-header-num">{list.len().to_string()}</span>
                        {if list.len() == 1 { " job" } else { " jobs" }}
                    })}
                </Transition>
            }
        >
            <LiveStatusChip
                status=live.status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <ExecuteJobDialog
            show=show_dialog
            job_name=dialog_job_signal
            picker=dialog_picker_signal
            verb=dialog_verb_signal
            assets=dialog_assets
            definitions=asset_info_by_key
            resources=launch_resources
        />

        {move || exec_error.get().map(|msg| view! { <div class="error-msg" style="margin-bottom: 1rem">{msg}</div> })}

        <Transition fallback=move || view! { <div class="loading">"Loading jobs…"</div> }>
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
                                    hint="Add an rs.Job(name, assets=[…]) to your code location"
                                />
                            }.into_any());
                        }
                        const GRID: &str = "grid-template-columns: 1.4fr 0.7fr 1.6fr 0.8fr 1.1fr 136px";
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

                                    let verb = job_verb(job.action.as_deref(), &asset_selection, &asset_info_by_key);
                                    let row_picker =
                                        job_partition_picker(verb.as_ref(), &asset_selection, &asset_info_by_key);
                                    let destructive = verb.as_ref().is_some_and(|v| v.is_destructive());
                                    let verb_name = verb.as_ref().map(|v| v.name.clone());
                                    // The dialog picks partitions and edits config; a job
                                    // needing neither runs on the click.
                                    let opens_dialog = !matches!(row_picker, JobPartitionPicker::None)
                                        || crate::components::config_editor::launch_takes_config(
                                            &asset_selection,
                                            &asset_info_by_key,
                                            verb_name.as_deref(),
                                        );
                                    let armed_label = verb_name.clone().unwrap_or_default();

                                    let (exec_pending, set_exec_pending) = signal(false);
                                    let armed = RwSignal::new(false);
                                    let navigate = navigate.clone();
                                    let nav_run = navigate.clone();
                                    // The dialog edits the launch document for a
                                    // job that would otherwise run on the click.
                                    let open_dialog = {
                                        let exec_name = exec_name.clone();
                                        let row_picker = row_picker.clone();
                                        let verb = verb.clone();
                                        move || {
                                            dialog_job.set(exec_name.clone());
                                            dialog_picker.set(row_picker.clone());
                                            dialog_verb.set(verb.clone());
                                            show_dialog.set(true);
                                        }
                                    };

                                    let on_execute = {
                                        let ns = ns.clone();
                                        let name = name.clone();
                                        let open_dialog = open_dialog.clone();
                                        let shown = verb_name.clone();
                                        move |ev: leptos::ev::MouseEvent| {
                                            ev.prevent_default();
                                            ev.stop_propagation();
                                            if opens_dialog {
                                                open_dialog();
                                                return;
                                            }
                                            // A destructive verb takes a second click, as on the run page.
                                            let (dispatch, now_armed) = replay_click(destructive, armed.get());
                                            armed.set(now_armed);
                                            if !dispatch {
                                                return;
                                            }
                                            let n = exec_name.clone();
                                            let shown = shown.clone();
                                            let navigate = navigate.clone();
                                            let ns = ns.clone();
                                            let name = name.clone();
                                            set_exec_pending.set(true);
                                            exec_error.set(None);
                                            leptos::task::spawn_local(async move {
                                                let path_ns = ns.clone();
                                                let path_name = name.clone();
                                                match execute_job(ns, name, n.clone(), shown, None, false, None).await {
                                                    Ok(result) if !result.run_id.is_empty() => {
                                                        let path = loc_path(&path_ns, &path_name, &format!("runs/{}", result.run_id));
                                                        navigate(&path, Default::default());
                                                    }
                                                    Ok(_) => {
                                                        exec_error.set(Some(format!("Execute {n}: no run id returned.")));
                                                        set_exec_pending.set(false);
                                                    }
                                                    Err(e) => {
                                                        exec_error.set(Some(format!("Execute {n} failed: {}", crate::helpers::err_text(&e))));
                                                        set_exec_pending.set(false);
                                                    }
                                                }
                                            });
                                        }
                                    };

                                    let status_cell = match &last_run {
                                        Some(r) => view! { <StatusChip kind=run_status_kind(&r.status).to_string()/> }.into_any(),
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
                                                    <span class="grid-cell-muted" style="font-size:var(--fs-xs)">
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
                                            <span class="grid-cell-mono">
                                                {job_name}
                                                {verb_name.map(|v| view! {
                                                    <span class="grid-cell-muted" title="asset action">{format!(" · {v}")}</span>
                                                })}
                                            </span>
                                            <KindBadge kind=crate::helpers::executor_label(&executor_type)/>
                                            {if asset_selection.is_empty() {
                                                view! { <span class="grid-cell-muted">"all"</span> }.into_any()
                                            } else {
                                                view! { <AssetStack assets=asset_selection/> }.into_any()
                                            }}
                                            {status_cell}
                                            {last_run_cell}
                                            {
                                                let variant = if destructive { "btn-danger" } else { "btn-accent" };
                                                let execute = view! {
                                                    <button
                                                        class=format!("btn {variant}")
                                                        on:click=on_execute
                                                        disabled=move || exec_pending.get()
                                                        title="Execute job"
                                                    >
                                                        {if destructive { view! { <IconTrash/> }.into_any() } else { view! { <IconPlay/> }.into_any() }}
                                                        {move || if exec_pending.get() {
                                                            "Executing…".to_string()
                                                        } else if armed.get() {
                                                            format!("Confirm {armed_label}?")
                                                        } else if opens_dialog {
                                                            "Execute…".to_string()
                                                        } else {
                                                            "Execute".to_string()
                                                        }}
                                                    </button>
                                                };
                                                let cell = if opens_dialog {
                                                    execute.into_any()
                                                } else {
                                                    view! {
                                                        <SplitButton
                                                            variant=variant
                                                            disabled=Signal::derive(move || exec_pending.get())
                                                            menu_label="Execute with config…"
                                                            on_menu=Callback::new(move |()| {
                                                                armed.set(false);
                                                                open_dialog();
                                                            })
                                                        >
                                                            {execute}
                                                        </SplitButton>
                                                    }
                                                    .into_any()
                                                };
                                                view! { <span class="grid-cell-action">{cell}</span> }
                                            }
                                        </A>
                                    }
                                }).collect::<Vec<_>>()
                                }
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load jobs: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
