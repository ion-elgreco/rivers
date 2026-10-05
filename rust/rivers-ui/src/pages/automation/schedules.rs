use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::ui_kit::{AutomationState, EmptyState, EvaluateOutcomeShort};
use crate::loc::loc_path;
use crate::server_fns::automation::evaluate_schedule;
use crate::types::ScheduleRecord;

use super::{SortIndicator, SortToggle};

pub(super) fn sort_schedules(records: &mut [ScheduleRecord], field: &str, asc: bool) {
    records.sort_by(|a, b| {
        let ord = match field {
            "cron" => a.cron_schedule.cmp(&b.cron_schedule),
            "job" => a.job_name.cmp(&b.job_name),
            "status" => a.status.cmp(&b.status),
            _ => a.name.cmp(&b.name),
        };
        if asc { ord } else { ord.reverse() }
    });
}

pub(super) fn render_schedules_table(
    records: Vec<ScheduleRecord>,
    next_ticks: HashMap<String, String>,
    job_actions: HashMap<String, String>,
    loc_ns: String,
    loc_name: String,
    toggle_sort: SortToggle,
    sort_indicator: SortIndicator,
) -> AnyView {
    if records.is_empty() {
        return view! {
            <EmptyState
                message="No schedules defined"
                hint="Add an @rs.Schedule(cron_schedule=…, job_name=…) to your code location"
            />
        }
        .into_any();
    }

    let si_name = sort_indicator("name");
    let si_cron = sort_indicator("cron");
    let si_job = sort_indicator("job");
    let si_status = sort_indicator("status");

    let ts = toggle_sort;
    let ts1 = ts.clone();
    let ts2 = ts.clone();
    let ts3 = ts.clone();
    let ts4 = ts;

    const GRID: &str = "grid-template-columns: 1.6fr 1fr 1.2fr 0.8fr 0.9fr 1fr 100px";

    view! {
        <div class="grid-table">
            <div class="grid-table-head" style=GRID>
                <span class="sortable" on:click=move |_| ts1("name")>{format!("NAME{si_name}")}</span>
                <span class="sortable" on:click=move |_| ts2("cron")>{format!("CRON{si_cron}")}</span>
                <span class="sortable" on:click=move |_| ts3("job")>{format!("JOB{si_job}")}</span>
                <span class="sortable" on:click=move |_| ts4("status")>{format!("STATUS{si_status}")}</span>
                <span>"NEXT TICK"</span>
                <span>"TAGS"</span>
                <span></span>
            </div>
            {records.into_iter().map(|s| {
                let name = s.name.clone();
                let eval_name = name.clone();
                let href = loc_path(&loc_ns, &loc_name, &format!("automation/schedules/{}", name));
                let job_name = s.job_name.clone();
                let tick_text = next_ticks.get(&s.name).cloned().unwrap_or_else(|| "—".to_string());
                let status_raw = s.status.clone();

                let eval_ns = loc_ns.clone();
                let eval_loc = loc_name.clone();
                let eval_action = Action::new(move |_: &()| {
                    let n = eval_name.clone();
                    let ns = eval_ns.clone();
                    let lname = eval_loc.clone();
                    async move { evaluate_schedule(ns, lname, n).await }
                });
                let eval_pending = eval_action.pending();

                let cron_raw = s.cron_schedule.clone();
                let cron_copy = cron_raw.clone();
                let cron_display = s.cron_description.clone().unwrap_or_else(|| s.cron_schedule.clone());

                view! {
                    <div class="grid-row grid-row--plain" style=GRID>
                        <A href=href attr:class="grid-cell-mono schedule-name-link">{name}</A>
                        <span style="display:flex; align-items:center; gap:6px; min-width:0">
                            <code
                                class="rivers-cron-code"
                                title={cron_raw.clone()}
                            >{cron_display}</code>
                            <button
                                class="icon-btn copyable"
                                title="Copy cron expression"
                                aria-label="Copy cron expression"
                                data-copy={cron_copy}
                            >
                                <crate::components::icons::IconCopy/>
                            </button>
                        </span>
                        <span class="grid-cell-mono" style="color:var(--secondary); font-size:var(--fs-sm)">
                            {job_actions.get(&job_name).map(|v| format!("{job_name} · {v}")).unwrap_or(job_name)}
                        </span>
                        <AutomationState status=status_raw/>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm)">{tick_text}</span>
                        <span style="display:flex; gap:4px; flex-wrap:wrap">
                            {s.tags.iter().map(|(k, v)| {
                                view! { <span class="tag">{format!("{k}={v}")}</span> }
                            }).collect::<Vec<_>>()}
                        </span>
                        <span style="display:flex; align-items:center; gap:6px; justify-content:flex-end">
                            <button
                                class="btn"
                                on:click=move |_| { eval_action.dispatch(()); }
                                disabled=move || eval_pending.get()
                            >
                                {move || if eval_pending.get() { "Evaluating…" } else { "Evaluate" }}
                            </button>
                            {move || eval_action.value().get().map(|result| view! { <EvaluateOutcomeShort result/> })}
                        </span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }.into_any()
}
