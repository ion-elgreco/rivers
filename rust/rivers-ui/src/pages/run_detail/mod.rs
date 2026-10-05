//! Run detail page.

use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::components::icons::{IconChevronRight, IconCopy, IconRetry, IconStop, IconTrash};
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::rerun_dialog::RerunConfigDialog;
use crate::components::traceback::RunFailures;
use crate::components::ui_kit::{Crumb, SplitButton, StatusChip, Topbar};
use crate::helpers::{
    code_location_label, format_elapsed, format_relative_time, format_timestamp,
    launched_by_display, run_status_kind, short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::now::use_now;
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::mutations::{cancel_run, delete_run, rerun_run};
use crate::server_fns::runs::{get_run, get_run_logs, get_run_step_events};
use crate::types::RunStatus;

mod asset_drawer;
mod dag_view;
mod logs;
mod timeline;

pub use asset_drawer::*;

use logs::RunLogPanel;
use timeline::RunTimelinePanel;

#[component]
pub fn RunDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `run_id` is always untracked. Resources explicitly track `params`.
    let run_id = move || params.read_untracked().get("id").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);
    // Tracked run_id for reactive children (the resources above use the untracked `run_id`).
    let run_id_memo = Memo::new(move |_| {
        params.track();
        run_id()
    });

    let run = Resource::new(
        move || {
            params.track();
            (run_id(), refresh_tick.get())
        },
        |(id, _)| get_run(id),
    );
    // Timeline/DAG need only step events; materializations are paginated elsewhere.
    let step_events = Resource::new(
        move || {
            params.track();
            (run_id(), refresh_tick.get())
        },
        |(id, _)| get_run_step_events(id),
    );
    // Client-only on purpose. RunLogPanel is mounted outside the resource
    // Transition, so if the server resolved this the SSR markup would carry log
    // rows and tab badges that the freshly-hydrated (still empty) client tree
    // does not have — an unrecoverable hydration mismatch.
    let run_logs = LocalResource::new(move || {
        params.track();
        let id = run_id();
        refresh_tick.get();
        async move { get_run_logs(id).await }
    });
    let run_logs_list =
        Signal::derive(move || run_logs.get().and_then(|r| r.ok()).unwrap_or_default());
    // Read outside the step events' Transition, so through the effect-filled copy.
    let step_events_value = crate::helpers::resource_value(step_events);
    let topology = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { crate::server_fns::graph::get_graph_topology(ns, name).await },
    );
    let locations = Resource::new(|| (), |_| list_code_locations());

    let (selected_step, set_selected_step) = signal(Option::<String>::None);
    // Asset-drawer pagination lives here, not in the drawer, so a live re-mount
    // of the drawer (the timeline Transition re-runs on each refresh tick) can't
    // reset it. Reset only when the selected asset or the run changes.
    let (mat_page, set_mat_page) = signal(0u64);
    let (obs_page, set_obs_page) = signal(0u64);
    let (act_page, set_act_page) = signal(0u64);
    let (del_page, set_del_page) = signal(0u64);
    Effect::new(move |_| {
        selected_step.track();
        run_id_memo.track();
        set_mat_page.set(0);
        set_obs_page.set(0);
        set_act_page.set(0);
        set_del_page.set(0);
    });
    let (log_tab, set_log_tab) = signal("events".to_string());
    let (log_level, set_log_level) = signal("all".to_string());
    let (view_mode, set_view_mode) = signal("gantt".to_string());

    // Re-execute reuses the run's exact config server-side (partition, tags, job
    // vs. materialization), so a partitioned run replays on its partition. Both
    // actions route by the run's owning location, not the page's.
    let reexecute = Action::new(move |run_id: &String| {
        let run_id = run_id.clone();
        async move { rerun_run(run_id, None).await }
    });
    let reexecute_pending = reexecute.pending();
    let reexecute_armed = RwSignal::new(false);
    let rerun_menu = RwSignal::new(false);
    let rerun_dialog = RwSignal::new(false);
    let config_open = RwSignal::new(false);
    let run_value = crate::helpers::resource_value(run);
    let run_record = Signal::derive(move || run_value.get().and_then(|r| r.ok()).flatten());
    // The dialog checks the document against the location that owns the run.
    let run_owner = Signal::derive(move || {
        let cl = run_record.with(|r| r.as_ref().map(|r| r.code_location_id.clone()));
        let entries = locations.get().and_then(|r| r.ok()).unwrap_or_default();
        cl.and_then(|cl| entries.into_iter().find(|e| e.identity == cl))
            .map(|e| (e.namespace, e.name))
            .unwrap_or_else(|| loc.get())
    });

    let cancel = Action::new(move |id: &String| {
        let id = id.clone();
        async move { cancel_run(id).await }
    });
    let cancel_pending = cancel.pending();

    let delete = Action::new(move |id: &String| {
        let id = id.clone();
        async move { delete_run(id).await }
    });
    let delete_pending = delete.pending();
    // Two-click confirm; disarm when navigating to a different run.
    let delete_armed = RwSignal::new(false);
    let cancel_armed = RwSignal::new(false);
    Effect::new(move |_| {
        run_id_memo.track();
        delete_armed.set(false);
        cancel_armed.set(false);
        reexecute_armed.set(false);
        rerun_menu.set(false);
        config_open.set(false);
    });
    // A deleted run has no page to stay on — back to the list. Ok(false)
    // means the run was already gone, which lands in the same place.
    let navigate = leptos_router::hooks::use_navigate();
    let navigate_rerun = navigate.clone();
    Effect::new(move |_| {
        if let Some(Ok(_)) = delete.value().get() {
            let (ns, name) = loc.get_untracked();
            navigate(&loc_path(&ns, &name, "runs"), Default::default());
        }
    });
    Effect::new(move |_| {
        if let Some(Ok(r)) = reexecute.value().get()
            && !r.run_id.is_empty()
        {
            let (ns, name) = loc.get_untracked();
            navigate_rerun(
                &loc_path(&ns, &name, &format!("runs/{}", r.run_id)),
                Default::default(),
            );
        }
    });
    let action_error = RwSignal::new(Option::<String>::None);
    Effect::new(move |_| {
        if let Some(Err(e)) = reexecute.value().get() {
            action_error.set(Some(format!(
                "Re-execute failed: {}",
                crate::helpers::err_text(&e)
            )));
        }
    });
    Effect::new(move |_| {
        if let Some(Err(e)) = cancel.value().get() {
            action_error.set(Some(format!(
                "Cancel failed: {}",
                crate::helpers::err_text(&e)
            )));
        }
    });
    Effect::new(move |_| {
        if let Some(Err(e)) = delete.value().get() {
            action_error.set(Some(format!(
                "Delete failed: {}",
                crate::helpers::err_text(&e)
            )));
        }
    });

    let live_status = use_live_kick(
        &["runs", "events"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <div class="run-detail-layout">
        <div class="run-detail-main">
        {move || {
            let id = run_id_memo.get();
            let (ns, name) = loc.get();
            let runs_href = loc_path(&ns, &name, "runs");
            view! {
                <Topbar crumbs=vec![
                    Crumb::linked("Runs", runs_href),
                    Crumb::new(short_id(&id, 8)).mono().copyable(id.clone()),
                ]>
                    <LiveStatusChip
                        status=live_status
                        on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
                    />
                    {move || action_error.get().map(|msg| view! { <span class="text-error">{msg}</span> })}
                    <Transition>
                        {move || run.get().and_then(|r| r.ok()).flatten().map(|record| {
                            let rerun_run_id = record.run_id.clone();
                            let rerun_verb = record.action.clone();
                            let rerun_verb_text = rerun_verb.clone();
                            let is_active_status = crate::helpers::run_is_active(&record.status);
                            view! {
                                {is_active_status.then(|| {
                                    let cancel_id = record.run_id.clone();
                                    view! {
                                        <button
                                            class="btn btn-danger"
                                            on:click=move |_| {
                                                if cancel_armed.get() {
                                                    cancel_armed.set(false);
                                                    action_error.set(None);
                                                    cancel.dispatch(cancel_id.clone());
                                                } else {
                                                    cancel_armed.set(true);
                                                }
                                            }
                                            disabled=move || cancel_pending.get()
                                        >
                                            <IconStop/>
                                            {move || if cancel_pending.get() {
                                                "Canceling…"
                                            } else if cancel_armed.get() {
                                                "Confirm cancel?"
                                            } else {
                                                "Cancel run"
                                            }}
                                        </button>
                                    }
                                })}
                                {(!is_active_status).then(|| {
                                    let delete_id = record.run_id.clone();
                                    view! {
                                        <button
                                            class="btn btn-danger"
                                            on:click=move |_| {
                                                if delete_armed.get() {
                                                    delete_armed.set(false);
                                                    action_error.set(None);
                                                    delete.dispatch(delete_id.clone());
                                                } else {
                                                    delete_armed.set(true);
                                                }
                                            }
                                            disabled=move || delete_pending.get()
                                        >
                                            <IconTrash/>
                                            {move || if delete_pending.get() {
                                                "Deleting…"
                                            } else if delete_armed.get() {
                                                "Confirm delete?"
                                            } else {
                                                "Delete run"
                                            }}
                                        </button>
                                    }
                                })}
                                {(!is_active_status).then(move || view! {
                                    <SplitButton
                                        variant="btn-primary"
                                        disabled=Signal::derive(move || reexecute_pending.get())
                                        menu_label="Re-execute with config…"
                                        on_menu=Callback::new(move |()| {
                                            reexecute_armed.set(false);
                                            rerun_dialog.set(true);
                                        })
                                        open=rerun_menu
                                    >
                                        <button
                                            class="btn btn-primary"
                                            on:click=move |_| {
                                                let (dispatch, armed) = crate::helpers::replay_click(
                                                    rerun_verb.is_some(),
                                                    reexecute_armed.get(),
                                                );
                                                reexecute_armed.set(armed);
                                                if dispatch {
                                                    action_error.set(None);
                                                    reexecute.dispatch(rerun_run_id.clone());
                                                }
                                            }
                                            disabled=move || reexecute_pending.get()
                                        >
                                            <IconRetry/>
                                            {move || crate::helpers::replay_button_text(
                                                rerun_verb_text.as_deref(),
                                                reexecute_armed.get(),
                                                reexecute_pending.get(),
                                            )}
                                        </button>
                                    </SplitButton>
                                })}
                            }
                        })}
                    </Transition>
                </Topbar>
            }
        }}
        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                run.get().map(|result| match result {
                    Ok(Some(record)) => {
                        let sid = short_id(&record.run_id, 8);
                        let status_kind = run_status_kind(&record.status);
                        let (_, _, trigger_label, trigger_sub) = launched_by_display(&record.launched_by);
                        // Reactive elapsed: ticks once per second while end_time
                        // is None (run still in flight), freezes when end_time
                        // is set.
                        let run_start_ns = record.start_time;
                        let run_end_ns = record.end_time;
                        let elapsed_label = move || {
                            format_elapsed(Some(run_start_ns), run_end_ns, use_now().get())
                        };
                        let cl_id = record.code_location_id.clone();
                        let cl_label = {
                            let entries = locations.get().and_then(|r| r.ok()).unwrap_or_default();
                            code_location_label(&cl_id, &entries)
                        };
                        view! {

                            <div class="run-header-block">
                                <div class="section-header-label">{
                                    match record.job_name.as_deref() {
                                        Some(j) => format!("RUN {} · {}", sid, j),
                                        None => format!("RUN {}", sid),
                                    }
                                }</div>
                                <div class="run-trigger-meta">
                                    <StatusChip kind=status_kind.to_string()/>
                                    {record.action.clone().map(|verb| view! {
                                        <span class="run-trigger-meta-item" title="asset action">
                                            "action "<span class="run-trigger-meta-value">{verb}</span>
                                        </span>
                                    })}
                                    <span class="run-trigger-meta-item" title=format_relative_time(record.start_time, jiff::Timestamp::now().as_second())>{format_timestamp(Some(record.start_time))}</span>
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
                                        <span class="run-trigger-meta-item">"partition "<span class="run-trigger-meta-value">{p.label()}</span></span>
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

                            {record.config.as_ref().map(|config| {
                                let pretty = serde_json::from_str::<serde_json::Value>(config)
                                    .and_then(|v| serde_json::to_string_pretty(&v))
                                    .unwrap_or_else(|_| config.clone());
                                let copy_text = pretty.clone();
                                view! {
                                    <div class="run-config">
                                        <div class="run-config-head">
                                            <button
                                                class="run-config-toggle"
                                                on:click=move |_| config_open.update(|o| *o = !*o)
                                                aria-expanded=move || config_open.get().to_string()
                                            >
                                                <span class="chev-btn" class:chev-btn--open=move || config_open.get()>
                                                    <IconChevronRight/>
                                                </span>
                                                <span class="section-header-label">"CONFIG"</span>
                                            </button>
                                            <button
                                                class="btn btn-small copyable"
                                                data-copy=copy_text
                                                title="Copy the launch config as JSON"
                                            >
                                                <IconCopy/>"Copy"
                                            </button>
                                        </div>
                                        <Show when=move || config_open.get()>
                                            <pre class="run-config-text">{pretty.clone()}</pre>
                                        </Show>
                                    </div>
                                }
                            })}

                        }.into_any()
                    }
                    Ok(None) => view! { <div class="error-msg">"Run not found"</div> }.into_any(),
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load run: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition>
            <Show when=move || !matches!(run.get(), Some(Ok(None)))>
                <div class="tab-content">
                    <RunFailures
                        step_events=Signal::derive(move || {
                            step_events_value.get().and_then(|r| r.ok()).unwrap_or_default()
                        })
                        run_logs=run_logs_list
                    />
                    <Transition fallback=move || view! { <div class="loading">"Loading timeline…"</div> }>
                        {move || {
                            let run_data = run.get().and_then(|r| r.ok()).flatten();
                            let steps = step_events.get()?.ok()?;
                            let run_start = run_data.as_ref().map(|r| r.start_time);
                            Some(view! {
                                <RunTimelinePanel
                                    events={steps}
                                    run_start={run_start}
                                    node_names={run_data.as_ref().map(|r| r.node_names.clone()).unwrap_or_default()}
                                    topology={topology.get().and_then(|r| r.ok())}
                                    selected_step=selected_step
                                    on_select=set_selected_step
                                    view_mode=view_mode
                                    set_view_mode=set_view_mode
                                />
                            })
                        }}
                    </Transition>
                    // Mounted once (not in a closure over the run) so a live refresh can't
                    // re-mount it and reset the scroll buffer. `run_logs` is reactive, so
                    // stdout/stderr still update live.
                    <RunLogPanel
                        run_id=run_id_memo
                        refresh_tick=refresh_tick
                        run_logs=run_logs_list
                        logs_error=Signal::derive(move || {
                            run_logs.get().and_then(|r| r.err()).map(|e| crate::helpers::err_text(&e))
                        })
                        selected_step=selected_step
                        on_clear=set_selected_step
                        log_tab=log_tab
                        set_log_tab=set_log_tab
                        log_level=log_level
                        set_log_level=set_log_level
                    />
                </div>
            </Show>
        </Transition>

        </div>

        <Show when=move || selected_step.get().is_some()>
            <Transition fallback=|| ()>
                {move || {
                    let asset = selected_step.get()?;
                    let steps = step_events.get()?.ok()?;
                    let topo = topology.get().and_then(|r| r.ok());
                    Some(view! {
                        <RunAssetDrawer
                            asset_key=asset
                            run_id=run_id()
                            step_events=steps
                            topology=topo
                            mat_page=mat_page
                            set_mat_page=set_mat_page
                            obs_page=obs_page
                            set_obs_page=set_obs_page
                            act_page=act_page
                            set_act_page=set_act_page
                            del_page=del_page
                            set_del_page=set_del_page
                            on_close=set_selected_step
                        />
                    })
                }}
            </Transition>
        </Show>
        <RerunConfigDialog show=rerun_dialog run=run_record location=run_owner/>
        </div>
    }
}
