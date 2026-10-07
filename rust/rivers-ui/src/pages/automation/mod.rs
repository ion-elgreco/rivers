//! Automation page listing schedules, sensors, and automation conditions.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::ui_kit::{Topbar, UnderlineTabs};
use crate::helpers::{job_actions_by_name, use_query_param};
use crate::loc::use_current_location;
use crate::server_fns::automation::{
    get_condition_tick_detail, get_condition_ticks, get_jobs, get_latest_condition_evals,
    get_next_ticks, get_schedules, get_sensors,
};
use crate::server_fns::overview::get_assets_info;
use crate::types::{
    AssetDefinitionInfo, ConditionEvalRecord, ConditionTickDetail, ConditionTickRecord,
    ScheduleRecord, SensorRecord,
};

mod conditions;
mod schedules;
mod sensors;

use conditions::{render_conditions_tab, sort_conditions};
use schedules::{render_schedules_table, sort_schedules};
use sensors::{render_sensors_table, sort_sensors};

fn format_with_commas(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

#[component]
pub fn AutomationPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let live = use_live_kick(
        &["automation", "assets"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );
    let (active_tab, set_active_tab) = use_query_param("tab", "schedules");
    let (sort_by, set_sort_by) = use_query_param("sort", "name");
    let (sort_asc_str, set_sort_asc_str) = use_query_param("asc", "true");
    let sort_asc = Signal::derive(move || sort_asc_str.get() != "false");

    let loc = use_current_location();
    let schedules = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_schedules(ns, name).await },
    );
    let sensors = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_sensors(ns, name).await },
    );
    let assets_info = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_assets_info(ns, name).await },
    );
    // Job definitions change only when the code location reloads; they say
    // which job a schedule or sensor runs as an action.
    let jobs = Resource::new(
        move || (loc.get(), live.definitions.get()),
        |((ns, name), _)| async move { get_jobs(ns, name).await },
    );
    let job_actions = move || -> HashMap<String, String> {
        jobs.get()
            .and_then(|r| r.ok())
            .map(|js| job_actions_by_name(&js))
            .unwrap_or_default()
    };

    let sched_records =
        move || -> Vec<ScheduleRecord> { schedules.get().and_then(|r| r.ok()).unwrap_or_default() };
    let sensor_records =
        move || -> Vec<SensorRecord> { sensors.get().and_then(|r| r.ok()).unwrap_or_default() };
    let condition_assets = move || -> Vec<AssetDefinitionInfo> {
        assets_info
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|a| a.automation_condition.is_some())
            .collect()
    };

    let next_ticks = Resource::new(
        move || {
            let exprs: Vec<(String, String, String)> = sched_records()
                .iter()
                .map(|s| {
                    (
                        s.name.clone(),
                        s.cron_schedule.clone(),
                        s.timezone.clone().unwrap_or_default(),
                    )
                })
                .collect();
            (refresh_tick.get(), exprs)
        },
        // Skip the server call on empty input — server_fn's urlencoded
        // format omits empty Vec fields entirely, which the server then
        // rejects as a missing argument.
        |(_tick, exprs)| async move {
            if exprs.is_empty() {
                Ok(Vec::new())
            } else {
                get_next_ticks(exprs).await
            }
        },
    );
    let next_ticks_map = move || -> HashMap<String, String> {
        next_ticks
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, tick)| tick.map(|t| (name, t)))
            .collect()
    };

    let latest_evals = Resource::new(
        move || {
            let keys: Vec<String> = condition_assets()
                .iter()
                .map(|a| a.asset_key.clone())
                .collect();
            (loc.get(), refresh_tick.get(), keys)
        },
        |((ns, name), _tick, keys)| async move {
            if keys.is_empty() {
                Ok(Vec::new())
            } else {
                get_latest_condition_evals(ns, name, keys).await
            }
        },
    );
    let latest_evals_map = move || -> HashMap<String, ConditionEvalRecord> {
        latest_evals
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(k, v)| v.map(|e| (k, e)))
            .collect()
    };

    let condition_ticks_res = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_condition_ticks(ns, name, Some(50)),
    );
    let condition_ticks = move || -> Vec<ConditionTickRecord> {
        condition_ticks_res
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
    };

    // Row expansion state for the conditions table — hoisted to the page
    // level so it survives resource refreshes (the render function rebuilds
    // on every refresh_tick change and would otherwise drop a local signal).
    let expanded_condition = RwSignal::new(Option::<String>::None);

    // Uses Action (not Resource) to avoid interfering with the Transition.
    let (selected_tick_id, set_selected_tick_id) = signal(None::<String>);
    let tick_detail = RwSignal::new(ConditionTickDetail::default());
    let tick_detail_loading = RwSignal::new(false);
    let fetch_tick_detail = Action::new(move |tick_id: &String| {
        let id = tick_id.clone();
        let (ns, name) = loc.get_untracked();
        async move {
            tick_detail_loading.set(true);
            let result = get_condition_tick_detail(ns, name, id)
                .await
                .ok()
                .unwrap_or_default();
            tick_detail.set(result);
            tick_detail_loading.set(false);
        }
    });

    let toggle_sort = std::sync::Arc::new({
        let set_sb = set_sort_by.clone();
        let set_sa = set_sort_asc_str.clone();
        move |field: &str| {
            let f = field.to_string();
            if sort_by.get() == f {
                set_sa(if sort_asc.get() {
                    "false".to_string()
                } else {
                    "true".to_string()
                });
            } else {
                set_sb(f);
                set_sa("true".to_string());
            }
        }
    });

    let sort_indicator = std::sync::Arc::new(move |field: &str| -> String {
        if sort_by.get() == field {
            if sort_asc.get() {
                " \u{25B2}".to_string()
            } else {
                " \u{25BC}".to_string()
            }
        } else {
            String::new()
        }
    });

    view! {
        <Topbar
            title="Automation"
            subtitle=move || view! {
                <Transition>
                    {move || {
                        let n_sched = sched_records().len();
                        let n_sensors = sensor_records().len();
                        let n_cond = condition_assets().len();
                        view! {
                            <span class="page-header-num">{n_sched.to_string()}</span>
                            {if n_sched == 1 { " schedule" } else { " schedules" }}
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num">{n_sensors.to_string()}</span>
                            {if n_sensors == 1 { " sensor" } else { " sensors" }}
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num">{n_cond.to_string()}</span>
                            {if n_cond == 1 { " declarative condition" } else { " declarative conditions" }}
                        }
                    }}
                </Transition>
            }
        >
            <LiveStatusChip
                status=live.status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>


        {
            let tabs: Vec<(String, String, Option<usize>)> = vec![
                ("schedules".into(), "Schedules".into(), None),
                ("sensors".into(), "Sensors".into(), None),
                ("conditions".into(), "Declarative Automation".into(), None),
            ];
            let active_sig = Signal::derive(move || active_tab.get());
            let set_tab = set_active_tab.clone();
            let on_tab = Callback::new(move |v: String| set_tab(v));
            view! { <UnderlineTabs tabs=tabs active=active_sig on_select=on_tab/> }
        }

        <Transition fallback=move || view! { <TableSkeleton rows=5 cols=7/> }>
            {move || {
                let tab = active_tab.get();
                let field = sort_by.get();
                let asc = sort_asc.get();
                let (loc_ns, loc_name) = loc.get();

                // A failed fetch must not read as "nothing defined".
                let load_error = match tab.as_str() {
                    "sensors" => sensors.get().and_then(|r| r.err()),
                    "conditions" => assets_info.get().and_then(|r| r.err()),
                    _ => schedules.get().and_then(|r| r.err()),
                };
                if let Some(e) = load_error {
                    return view! {
                        <div class="error-msg">{format!("Couldn't load automation: {}", crate::helpers::err_text(&e))}</div>
                    }.into_any();
                }

                match tab.as_str() {
                    "sensors" => {
                        let mut records = sensor_records();
                        sort_sensors(&mut records, &field, asc);
                        render_sensors_table(records, job_actions(), loc_ns, loc_name, toggle_sort.clone(), sort_indicator.clone())
                    }
                    "conditions" => {
                        let mut assets = condition_assets();
                        let evals = latest_evals_map();
                        let ticks = condition_ticks();
                        sort_conditions(&mut assets, &evals, &field, asc);
                        render_conditions_tab(
                            assets, evals, ticks, loc_ns, loc_name, expanded_condition,
                            selected_tick_id, set_selected_tick_id,
                            fetch_tick_detail, tick_detail, tick_detail_loading,
                            toggle_sort.clone(), sort_indicator.clone(),
                        )
                    }
                    _ => {
                        let mut records = sched_records();
                        let ticks = next_ticks_map();
                        sort_schedules(&mut records, &field, asc);
                        render_schedules_table(records, ticks, job_actions(), loc_ns, loc_name, toggle_sort.clone(), sort_indicator.clone())
                    }
                }
            }}
        </Transition>
    }
}

type SortToggle = std::sync::Arc<dyn Fn(&str) + Send + Sync>;
type SortIndicator = std::sync::Arc<dyn Fn(&str) -> String + Send + Sync>;
