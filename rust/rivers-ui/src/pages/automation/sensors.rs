use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::ui_kit::{AutomationState, EmptyState, EvaluateOutcomeShort};
use crate::loc::loc_path;
use crate::server_fns::automation::evaluate_sensor;
use crate::types::SensorRecord;

use super::{SortIndicator, SortToggle};

pub(super) fn sort_sensors(records: &mut [SensorRecord], field: &str, asc: bool) {
    records.sort_by(|a, b| {
        let ord = match field {
            "job" => a.job_name.cmp(&b.job_name),
            "status" => a.status.cmp(&b.status),
            "interval" => a.minimum_interval.cmp(&b.minimum_interval),
            _ => a.name.cmp(&b.name),
        };
        if asc { ord } else { ord.reverse() }
    });
}

pub(super) fn render_sensors_table(
    records: Vec<SensorRecord>,
    job_actions: HashMap<String, String>,
    loc_ns: String,
    loc_name: String,
    toggle_sort: SortToggle,
    sort_indicator: SortIndicator,
) -> AnyView {
    if records.is_empty() {
        return view! {
            <EmptyState
                message="No sensors defined"
                hint="Add an @rs.Sensor(job_name=…) to your code location"
            />
        }
        .into_any();
    }

    let si_name = sort_indicator("name");
    let si_job = sort_indicator("job");
    let si_status = sort_indicator("status");
    let si_interval = sort_indicator("interval");

    let ts = toggle_sort;
    let ts1 = ts.clone();
    let ts2 = ts.clone();
    let ts3 = ts.clone();
    let ts4 = ts;

    const GRID: &str = "grid-template-columns: 1.6fr 1.2fr 0.8fr 0.8fr 1.4fr 100px";

    view! {
        <div class="grid-table">
            <div class="grid-table-head" style=GRID>
                <span class="sortable" on:click=move |_| ts1("name")>{format!("NAME{si_name}")}</span>
                <span class="sortable" on:click=move |_| ts2("job")>{format!("JOB{si_job}")}</span>
                <span class="sortable" on:click=move |_| ts3("status")>{format!("STATUS{si_status}")}</span>
                <span class="sortable" on:click=move |_| ts4("interval")>{format!("INTERVAL{si_interval}")}</span>
                <span>"ASSET SELECTION"</span>
                <span></span>
            </div>
            {records.into_iter().map(|s| {
                let name = s.name.clone();
                let eval_name = name.clone();
                let href = loc_path(&loc_ns, &loc_name, &format!("automation/sensors/{}", name));
                let interval = s.minimum_interval
                    .clone()
                    .unwrap_or_else(|| "—".to_string());
                let job_name = s.job_name.clone();
                let asset_selection_str = if s.asset_selection.is_empty() {
                    "all".to_string()
                } else {
                    s.asset_selection.join(" · ")
                };
                let status_raw = s.status.clone();

                let eval_ns = loc_ns.clone();
                let eval_loc = loc_name.clone();
                let eval_action = Action::new(move |_: &()| {
                    let n = eval_name.clone();
                    let ns = eval_ns.clone();
                    let lname = eval_loc.clone();
                    async move { evaluate_sensor(ns, lname, n).await }
                });
                let eval_pending = eval_action.pending();

                view! {
                    <div class="grid-row grid-row--plain" style=GRID>
                        <A href=href attr:class="schedule-name-link">{name}</A>
                        {match job_name.clone() {
                            Some(jn) => {
                                let job_href = loc_path(&loc_ns, &loc_name, &format!("jobs/{}", jn));
                                let label = job_actions.get(&jn).map(|v| format!("{jn} · {v}")).unwrap_or(jn);
                                view! {
                                    <A href=job_href attr:class="grid-cell-mono sensor-job-link">{label}</A>
                                }.into_any()
                            }
                            None => view! { <span class="grid-cell-muted">"—"</span> }.into_any()
                        }}
                        <AutomationState status=status_raw/>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm)">{interval}</span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0">{asset_selection_str}</span>
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
