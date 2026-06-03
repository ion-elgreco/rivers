//! Execute-job confirmation dialog.
//!
//! Modal that lets the user pick partition keys (when the job touches
//! partitioned assets) before kicking off `execute_job`. The picker UI
//! itself lives in [`PartitionPicker`]; this dialog only owns the
//! action dispatch + post-success navigation.

use leptos::prelude::*;

use crate::components::partition_picker::PartitionPicker;
use crate::helpers::JobPartitionPicker;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::{execute_job, launch_backfill};
use crate::types::SubmitPartitionKey;

/// Above this many selected partitions, submit one job-aware backfill instead of
/// a run each (mirrors `MaterializeDialog`).
const BACKFILL_THRESHOLD: usize = 2;

/// What a submit produced, so the success Effect navigates to the right page.
#[derive(Clone)]
enum ExecOutcome {
    Run(String),
    Backfill(String),
}

/// `picker = JobPartitionPicker::None` → no partition section, dialog
/// submits with `None`. `job_name` is read at confirm time so a single
/// dialog instance can serve every row on the jobs list.
#[component]
pub fn ExecuteJobDialog(
    #[prop(into)] show: RwSignal<bool>,
    #[prop(into)] job_name: Signal<String>,
    #[prop(into)] picker: Signal<JobPartitionPicker>,
) -> impl IntoView {
    // Cartesian-expanded selection — owned by us, written by
    // PartitionPicker on every toggle. Read at submit and to drive the
    // run-count label.
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let error = RwSignal::new(None::<String>);
    let nav_to = RwSignal::new(None::<String>);

    let loc = use_current_location();

    let action = Action::new(move |keys: &Vec<SubmitPartitionKey>| {
        let keys = keys.clone();
        let job = job_name.get_untracked();
        let (ns, name) = loc.get_untracked();
        async move {
            if keys.len() > BACKFILL_THRESHOLD {
                // >2 partitions → one job-aware backfill. `None` selection: the
                // server resolves the job's assets.
                let r = launch_backfill(ns, name, None, keys, None, Some(job))
                    .await
                    .map_err(|e| format!("{e}"))?;
                return Ok::<ExecOutcome, String>(ExecOutcome::Backfill(r.backfill_id));
            }
            let key_opts = if keys.is_empty() {
                vec![None]
            } else {
                keys.into_iter().map(Some).collect::<Vec<_>>()
            };
            let mut last_run_id = String::new();
            for pk in key_opts {
                last_run_id = execute_job(ns.clone(), name.clone(), job.clone(), pk)
                    .await
                    .map_err(|e| format!("{e}"))?
                    .run_id;
            }
            Ok(ExecOutcome::Run(last_run_id))
        }
    });

    // Clear leftover error on reopen — selection state is reset by
    // PartitionPicker via its own `reset` prop.
    Effect::new(move |_| {
        if show.get() {
            error.set(None);
        }
    });

    // Memoize the run count for the button label and post-submit nav
    // target. None-picker: one unpartitioned run. Otherwise the count
    // is whatever the picker has expanded into `selected`.
    let run_count = Memo::new(move |_| {
        if matches!(picker.get(), JobPartitionPicker::None) {
            1
        } else {
            selected.get().len()
        }
    });

    Effect::new(move |_| {
        let Some(result) = action.value().get() else {
            return;
        };
        match result {
            Ok(ExecOutcome::Run(run_id)) if !run_id.is_empty() => {
                show.set(false);
                let (ns, name) = loc.get();
                let path = if run_count.get_untracked() <= 1 {
                    loc_path(&ns, &name, &format!("runs/{run_id}"))
                } else {
                    loc_path(&ns, &name, &format!("jobs/{}", job_name.get_untracked()))
                };
                nav_to.set(Some(path));
            }
            Ok(ExecOutcome::Backfill(backfill_id)) if !backfill_id.is_empty() => {
                show.set(false);
                let (ns, name) = loc.get();
                nav_to.set(Some(loc_path(&ns, &name, &format!("backfills/{backfill_id}"))));
            }
            Ok(_) => error.set(Some("Execution returned no id.".to_string())),
            Err(e) => error.set(Some(e)),
        }
    });

    let pending = action.pending();

    view! {
        <Show when=move || show.get()>
            <div class="modal-overlay" on:click=move |_| show.set(false)>
                <div class="modal-content" on:click=move |ev| ev.stop_propagation()>
                    <div class="modal-header">
                        <h2>"Execute job"</h2>
                        <button class="btn btn-small" on:click=move |_| show.set(false)>"x"</button>
                    </div>
                    <div class="modal-body">
                        <div class="form-group">
                            <label>"Job"</label>
                            <div class="grid-cell-mono">{move || job_name.get()}</div>
                        </div>
                        <PartitionPicker picker=picker selected=selected reset=show/>
                        {move || error.get().map(|msg| view! {
                            <div class="error-msg">{msg}</div>
                        })}
                    </div>
                    <div class="modal-footer">
                        <button class="btn" on:click=move |_| show.set(false)>"Cancel"</button>
                        <button
                            class="btn btn-primary"
                            on:click=move |_| {
                                let p = picker.get_untracked();
                                let keys = selected.get_untracked();
                                let needs_partition = !matches!(p, JobPartitionPicker::None);
                                if needs_partition && keys.is_empty() {
                                    let msg = if matches!(p, JobPartitionPicker::Multi { .. }) {
                                        "Select at least one value for every dimension."
                                    } else {
                                        "Select at least one partition."
                                    };
                                    error.set(Some(msg.to_string()));
                                    return;
                                }
                                action.dispatch(keys);
                            }
                            disabled=move || pending.get()
                        >
                            {move || {
                                if pending.get() {
                                    "Submitting...".to_string()
                                } else {
                                    let n = run_count.get();
                                    if n > BACKFILL_THRESHOLD {
                                        format!("Backfill {n} partitions")
                                    } else if n > 1 {
                                        format!("Execute {n} runs")
                                    } else {
                                        "Execute".to_string()
                                    }
                                }
                            }}
                        </button>
                    </div>
                </div>
            </div>
        </Show>

        {move || nav_to.get().map(|path| view! {
            <leptos_router::components::Redirect path={path}/>
        })}
    }
}
