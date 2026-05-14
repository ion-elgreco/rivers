//! Automation page listing schedules, sensors, and automation conditions.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::ui_kit::{Crumb, EmptyState, Topbar, UnderlineTabs};
use crate::helpers::{short_id, use_query_param};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::automation::{
    evaluate_schedule, evaluate_sensor, get_condition_tick_detail, get_condition_ticks,
    get_latest_condition_evals, get_next_ticks, get_schedules, get_sensors,
};
use crate::server_fns::overview::get_assets_info;
use crate::types::{
    AssetDefinitionInfo, ConditionEvalRecord, ConditionTickDetail, ConditionTickRecord,
    ScheduleRecord, SensorRecord,
};

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
            let exprs: Vec<(String, String)> = sched_records()
                .iter()
                .map(|s| (s.name.clone(), s.cron_schedule.clone()))
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
        let (ns, name) = loc.get();
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

    let live_status = use_live_kick(
        &["automation", "assets"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <Topbar crumbs=vec![Crumb::new("Automation")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <div class="page-header">
            <h1>"Automation"</h1>
            <Transition>
                {move || {
                    let n_sched = sched_records().len();
                    let n_sensors = sensor_records().len();
                    let n_cond = condition_assets().len();
                    view! {
                        <p>
                            <span class="page-header-num">{n_sched.to_string()}</span>
                            " schedules"
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num">{n_sensors.to_string()}</span>
                            " sensors"
                            <span class="page-header-sep">"·"</span>
                            <span class="page-header-num">{n_cond.to_string()}</span>
                            " declarative conditions"
                        </p>
                    }
                }}
            </Transition>
        </div>


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

                match tab.as_str() {
                    "sensors" => {
                        let mut records = sensor_records();
                        sort_sensors(&mut records, &field, asc);
                        render_sensors_table(records, loc_ns, loc_name, toggle_sort.clone(), sort_indicator.clone())
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
                        render_schedules_table(records, ticks, loc_ns, loc_name, toggle_sort.clone(), sort_indicator.clone())
                    }
                }
            }}
        </Transition>
    }
}

fn sort_schedules(records: &mut [ScheduleRecord], field: &str, asc: bool) {
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

fn sort_sensors(records: &mut [SensorRecord], field: &str, asc: bool) {
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

fn sort_conditions(
    assets: &mut [AssetDefinitionInfo],
    evals: &HashMap<String, ConditionEvalRecord>,
    field: &str,
    asc: bool,
) {
    assets.sort_by(|a, b| {
        let ord = match field {
            "condition" => a.automation_condition.cmp(&b.automation_condition),
            "status" => {
                let a_fired = evals.get(&a.asset_key).map(|e| e.fired).unwrap_or(false);
                let b_fired = evals.get(&b.asset_key).map(|e| e.fired).unwrap_or(false);
                a_fired.cmp(&b_fired)
            }
            _ => a.asset_key.cmp(&b.asset_key),
        };
        if asc { ord } else { ord.reverse() }
    });
}

type SortToggle = std::sync::Arc<dyn Fn(&str) + Send + Sync>;
type SortIndicator = std::sync::Arc<dyn Fn(&str) -> String + Send + Sync>;

fn render_schedules_table(
    records: Vec<ScheduleRecord>,
    next_ticks: HashMap<String, String>,
    loc_ns: String,
    loc_name: String,
    toggle_sort: SortToggle,
    sort_indicator: SortIndicator,
) -> AnyView {
    if records.is_empty() {
        return view! {
            <EmptyState
                message="No schedules defined"
                hint="add @schedule(cron='0 */6 * * *') decorators in your code location"
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
                let tick_text = next_ticks.get(&s.name).cloned().unwrap_or_else(|| "-".to_string());
                let status_raw = s.status.clone();
                let status_running = status_raw.eq_ignore_ascii_case("running");

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
                                data-copy={cron_copy}
                            >
                                <svg width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.2">
                                    <rect x="4" y="4" width="8" height="8" rx="1"/>
                                    <path d="M10 4V3a1 1 0 00-1-1H3a1 1 0 00-1 1v6a1 1 0 001 1h1"/>
                                </svg>
                            </button>
                        </span>
                        <span class="grid-cell-mono" style="color:var(--secondary); font-size:11.5px">{job_name}</span>
                        <span class="status-dot-row">
                            <span class=format!("status-dot status-dot--{}", if status_running { "ok" } else { "muted" })></span>
                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{status_raw}</span>
                        </span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{tick_text}</span>
                        <span style="display:flex; gap:4px; flex-wrap:wrap">
                            {s.tags.iter().map(|(k, v)| {
                                view! { <span class="tag">{format!("{k}={v}")}</span> }
                            }).collect::<Vec<_>>()}
                        </span>
                        <span style="display:flex; align-items:center; gap:6px; justify-content:flex-end">
                            <button
                                class="btn btn-tertiary"
                                on:click=move |_| { eval_action.dispatch(()); }
                                disabled=move || eval_pending.get()
                                style="justify-content:center"
                            >
                                {move || if eval_pending.get() { "..." } else { "Evaluate" }}
                            </button>
                            {move || eval_action.value().get().map(|result| match result {
                                Ok(run_ids) => view! {
                                    <span class="text-success" style="font-size:11px">
                                        {format!("{} run(s)", run_ids.len())}
                                    </span>
                                }.into_any(),
                                Err(e) => view! {
                                    <span class="text-error" style="font-size:11px">
                                        {format!("{e}")}
                                    </span>
                                }.into_any(),
                            })}
                        </span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }.into_any()
}

fn render_sensors_table(
    records: Vec<SensorRecord>,
    loc_ns: String,
    loc_name: String,
    toggle_sort: SortToggle,
    sort_indicator: SortIndicator,
) -> AnyView {
    if records.is_empty() {
        return view! {
            <EmptyState
                message="No sensors defined"
                hint="add @sensor() event-driven triggers in your code location"
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
                let status_running = status_raw.eq_ignore_ascii_case("running");

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
                                view! {
                                    <A href=job_href attr:class="grid-cell-mono sensor-job-link">{jn}</A>
                                }.into_any()
                            }
                            None => view! { <span class="grid-cell-muted">"—"</span> }.into_any()
                        }}
                        <span class="status-dot-row">
                            <span class=format!("status-dot status-dot--{}", if status_running { "ok" } else { "muted" })></span>
                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{status_raw}</span>
                        </span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{interval}</span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0">{asset_selection_str}</span>
                        <span style="display:flex; align-items:center; gap:6px; justify-content:flex-end">
                            <button
                                class="btn btn-tertiary"
                                on:click=move |_| { eval_action.dispatch(()); }
                                disabled=move || eval_pending.get()
                                style="justify-content:center"
                            >
                                {move || if eval_pending.get() { "..." } else { "Evaluate" }}
                            </button>
                            {move || eval_action.value().get().map(|result| match result {
                                Ok(run_ids) => view! {
                                    <span class="text-success" style="font-size:11px">
                                        {format!("{} run(s)", run_ids.len())}
                                    </span>
                                }.into_any(),
                                Err(e) => view! {
                                    <span class="text-error" style="font-size:11px">
                                        {format!("{e}")}
                                    </span>
                                }.into_any(),
                            })}
                        </span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }.into_any()
}

fn render_conditions_tab(
    assets: Vec<AssetDefinitionInfo>,
    evals: HashMap<String, ConditionEvalRecord>,
    ticks: Vec<ConditionTickRecord>,
    loc_ns: String,
    loc_name: String,
    expanded_row: RwSignal<Option<String>>,
    selected_tick_id: ReadSignal<Option<String>>,
    set_selected_tick_id: WriteSignal<Option<String>>,
    fetch_tick_detail: Action<String, ()>,
    tick_detail: RwSignal<ConditionTickDetail>,
    tick_detail_loading: RwSignal<bool>,
    toggle_sort: SortToggle,
    sort_indicator: SortIndicator,
) -> AnyView {
    if assets.is_empty() {
        return view! {
            <EmptyState
                message="No assets with automation conditions"
                hint="attach AutomationCondition.eager() to an asset for declarative materialization"
            />
        }
        .into_any();
    }

    let si_name = sort_indicator("name");
    let si_cond = sort_indicator("condition");
    let si_status = sort_indicator("status");

    let ts = toggle_sort;
    let ts1 = ts.clone();
    let ts2 = ts.clone();
    let ts3 = ts;

    // Each tick evaluates every condition, so the tick count is a reasonable
    // proxy for evaluations per condition.
    let tick_count = ticks.len();

    const GRID: &str = "grid-template-columns: 24px 1.8fr 2fr 0.9fr 0.9fr 0.8fr";

    let timeline_view = (!ticks.is_empty()).then(|| {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let window_ns: i64 = 60 * 60 * 1_000_000_000;
        let bucket_ns = window_ns / 60;
        let mut buckets: Vec<(u32, bool)> = vec![(0, false); 60];
        for t in ticks.iter() {
            let age = now.saturating_sub(t.timestamp);
            if age >= 0 && age < window_ns {
                let idx = (59 - (age / bucket_ns).min(59)) as usize;
                buckets[idx].0 += 1;
                if t.total_fired > 0 {
                    buckets[idx].1 = true;
                }
            }
        }
        view! { <crate::components::ui_kit::EvalTimelineBars buckets=buckets/> }
    });

    view! {
        {timeline_view}
        <div class="grid-table" style="margin-top:20px">
            <div class="grid-table-head" style=GRID>
                <span></span>
                <span class="sortable" on:click=move |_| ts1("name")>{format!("ASSET{si_name}")}</span>
                <span class="sortable" on:click=move |_| ts2("condition")>{format!("CONDITION{si_cond}")}</span>
                <span>"LAST EVAL"</span>
                <span class="sortable" on:click=move |_| ts3("status")>{format!("RESULT{si_status}")}</span>
                <span style="text-align:right">"TICKS"</span>
            </div>
            {assets.into_iter().map(|a| {
                let asset_key = a.asset_key.clone();
                let key_click = asset_key.clone();
                let key_check = asset_key.clone();
                let key_for_replay = asset_key.clone();
                let href = loc_path(&loc_ns, &loc_name, &format!("assets/{}?tab=automation", asset_key));
                let condition = a.automation_condition.unwrap_or_default();
                let eval_info = evals.get(&a.asset_key).cloned();
                let is_expanded = Signal::derive(move || expanded_row.get().as_deref() == Some(key_check.as_str()));
                let row_cls = move || {
                    if is_expanded.get() {
                        "grid-row grid-row--static grid-row--expanded"
                    } else {
                        "grid-row grid-row--static"
                    }
                };
                let toggle_click = move |_| expanded_row.update(|e| {
                    if e.as_deref() == Some(key_click.as_str()) { *e = None; }
                    else { *e = Some(key_click.clone()); }
                });

                let (last_eval_ts, result_label, result_color) = match eval_info.as_ref() {
                    Some(e) => {
                        let (label, color) = if e.fired && !e.run_ids.is_empty() {
                            ("materialized", "var(--success)")
                        } else if e.fired {
                            ("requested", "var(--accent)")
                        } else {
                            ("suppressed", "var(--text-muted)")
                        };
                        (Some(e.timestamp), label, color)
                    }
                    None => (None, "—", "var(--text-muted)"),
                };

                let evals_formatted = format_with_commas(tick_count);

                view! {
                    <div class=row_cls style=GRID on:click=toggle_click>
                        <span
                            class="chev-btn"
                            class:chev-btn--open=move || is_expanded.get()
                        >
                            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M3 2l3 3-3 3"/>
                            </svg>
                        </span>
                        <A
                            href=href
                            attr:class="schedule-name-link"
                            on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                        >{asset_key}</A>
                        <code
                            class="grid-cell-mono"
                            style="color:var(--text-muted); font-size:11.5px; background:transparent; padding:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0"
                            title={condition.clone()}
                        >{condition.clone()}</code>
                        <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px">
                            <crate::now::RelTimeOpt ts=last_eval_ts/>
                        </span>
                        <span class="grid-cell-mono" style=format!("color:{result_color}; font-size:11.5px")>{result_label}</span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; text-align:right">{evals_formatted}</span>
                    </div>
                    <Show when=move || is_expanded.get()>
                        <div class="grid-row-expansion grid-row-expansion--accent">
                            <crate::components::ui_kit::ConditionReplay asset_key=key_for_replay.clone() minutes=60/>
                        </div>
                    </Show>
                }
            }).collect::<Vec<_>>()}
        </div>

        {if ticks.is_empty() {
            view! {
                <div class="section-header-row" style="margin-top:28px">
                    <span class="section-header-label">"EVALUATION TICKS"</span>
                </div>
                <div class="empty-state">"No evaluation ticks recorded yet."</div>
            }.into_any()
        } else {
            const GRID: &str = "grid-template-columns: 24px 1.1fr 0.5fr 0.55fr 0.55fr 1.2fr 1.2fr";
            let tick_count = ticks.len();
            view! {
                <div class="section-header-row" style="margin-top:28px">
                    <span class="section-header-label">{format!("EVALUATION TICKS · LAST {tick_count}")}</span>
                </div>
                <div class="grid-table">
                    <div class="grid-table-head" style=GRID>
                        <span></span>
                        <span>"TIME"</span>
                        <span>"DURATION"</span>
                        <span style="text-align:right">"EVALUATED"</span>
                        <span style="text-align:right">"REQUESTED"</span>
                        <span>"RUNS"</span>
                        <span>"BACKFILLS"</span>
                    </div>
                    {let loc_ns_outer = loc_ns.clone();
                    let loc_name_outer = loc_name.clone();
                    ticks.into_iter().map(move |t| {
                        let loc_ns_iter = loc_ns_outer.clone();
                        let loc_name_iter = loc_name_outer.clone();
                        let ts_abs = crate::helpers::format_timestamp_nanos(t.timestamp);
                        let ts_now = t.timestamp;
                        let dur_ms = t.eval_duration_us as f64 / 1000.0;
                        let dur_label = if dur_ms < 1.0 {
                            format!("{} µs", t.eval_duration_us)
                        } else {
                            format!("{:.1} ms", dur_ms)
                        };
                        let tick_id = t.id.clone();
                        let click_id = tick_id.clone();
                        let check_id = tick_id.clone();
                        let fired = t.total_fired;
                        let evaluated = t.total_evaluated;
                        let run_ids = t.run_ids.clone();
                        let backfill_ids = t.backfill_ids.clone();

                        let is_expanded = Signal::derive(move || {
                            selected_tick_id.get().as_deref() == Some(check_id.as_str())
                        });
                        let row_cls = move || {
                            if is_expanded.get() {
                                "grid-row grid-row--static grid-row--expanded"
                            } else {
                                "grid-row grid-row--static"
                            }
                        };
                        let toggle = move |_| {
                            if selected_tick_id.get().as_deref() == Some(click_id.as_str()) {
                                set_selected_tick_id.set(None);
                                tick_detail.set(ConditionTickDetail::default());
                            } else {
                                set_selected_tick_id.set(Some(click_id.clone()));
                                fetch_tick_detail.dispatch(click_id.clone());
                            }
                        };

                        let fired_cell = if fired > 0 {
                            view! {
                                <span class="status-dot-row" style="justify-content:flex-end">
                                    <span class="status-dot status-dot--accent"></span>
                                    <span class="grid-cell-mono" style="color:var(--accent); font-size:11.5px">{fired.to_string()}</span>
                                </span>
                            }.into_any()
                        } else {
                            view! {
                                <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px; text-align:right">"—"</span>
                            }.into_any()
                        };

                        let runs_cell = if run_ids.is_empty() {
                            if fired > 0 && backfill_ids.is_empty() {
                                view! {
                                    <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px" title="No direct runs — expand the row for per-asset detail.">"—"</span>
                                }.into_any()
                            } else {
                                view! {
                                    <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px">"—"</span>
                                }.into_any()
                            }
                        } else {
                            let chips = {
                                let loc_ns_chip = loc_ns_iter.clone();
                                let loc_name_chip = loc_name_iter.clone();
                                run_ids.into_iter().map(move |id| {
                                let href = loc_path(&loc_ns_chip, &loc_name_chip, &format!("runs/{}", id));
                                let short = short_id(&id, 8);
                                view! {
                                    <A
                                        href=href
                                        attr:class="tag"
                                        on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                                    >{short}</A>
                                }
                            }).collect::<Vec<_>>()};
                            view! {
                                <span style="display:flex; gap:4px; flex-wrap:wrap; align-items:center">{chips}</span>
                            }.into_any()
                        };

                        let backfills_cell = if backfill_ids.is_empty() {
                            view! {
                                <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px">"—"</span>
                            }.into_any()
                        } else {
                            let chips = {
                                let loc_ns_chip = loc_ns_iter.clone();
                                let loc_name_chip = loc_name_iter.clone();
                                backfill_ids.into_iter().map(move |bid| {
                                let href = loc_path(&loc_ns_chip, &loc_name_chip, &format!("backfills/{}", bid));
                                let short = short_id(&bid, 10);
                                let title = format!("Backfill {bid}");
                                view! {
                                    <A
                                        href=href
                                        attr:class="tag"
                                        attr:style="background:color-mix(in oklab, var(--secondary) 12%, transparent); color:var(--secondary); border:1px solid color-mix(in oklab, var(--secondary) 30%, transparent)"
                                        attr:title=title
                                        on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                                    >
                                        <span style="font-size:9px; letter-spacing:0.04em; margin-right:4px; opacity:0.7">"BF"</span>
                                        {short}
                                    </A>
                                }
                            }).collect::<Vec<_>>()};
                            view! {
                                <span style="display:flex; gap:4px; flex-wrap:wrap; align-items:center">{chips}</span>
                            }.into_any()
                        };

                        view! {
                            <div class=row_cls style=GRID on:click=toggle title=ts_abs>
                                <span
                                    class="chev-btn"
                                    class:chev-btn--open=move || is_expanded.get()
                                >
                                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
                                        <path d="M3 2l3 3-3 3"/>
                                    </svg>
                                </span>
                                <span class="grid-cell-mono" style="color:var(--text); font-size:11.5px">
                                    <crate::now::RelTime ts=ts_now/>
                                </span>
                                <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{dur_label}</span>
                                <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; text-align:right">{evaluated.to_string()}</span>
                                {fired_cell}
                                {runs_cell}
                                {backfills_cell}
                            </div>
                            <Show when=move || is_expanded.get()>
                                <div class="grid-row-expansion">
                                    {{
                                        let loc_ns_show = loc_ns_iter.clone();
                                        let loc_name_show = loc_name_iter.clone();
                                        move || {
                                        let loc_ns_inner = loc_ns_show.clone();
                                        let loc_name_inner = loc_name_show.clone();
                                        if tick_detail_loading.get() {
                                            return view! { <span class="text-muted" style="font-size:11.5px">"Loading..."</span> }.into_any();
                                        }
                                        let detail = tick_detail.get();
                                        if detail.evals.is_empty() {
                                            return view! { <span class="text-muted" style="font-size:11.5px">"No evaluations found."</span> }.into_any();
                                        }
                                        let fired_evals: Vec<_> = detail.evals.iter().filter(|e| e.fired).cloned().collect();
                                        let total = detail.evals.len();
                                        let mut run_set: std::collections::HashSet<&String> = std::collections::HashSet::new();
                                        let mut backfill_set: std::collections::HashSet<&String> = std::collections::HashSet::new();
                                        let mut without_link = 0usize;
                                        for e in &fired_evals {
                                            if e.run_ids.is_empty() && e.backfill_ids.is_empty() {
                                                without_link += 1;
                                            }
                                            run_set.extend(e.run_ids.iter());
                                            backfill_set.extend(e.backfill_ids.iter());
                                        }
                                        let unique_runs = run_set.len();
                                        let unique_backfills = backfill_set.len();
                                        view! {
                                            <div style="display:flex; flex-direction:column; gap:10px">
                                                <div style="display:flex; align-items:baseline; gap:10px; flex-wrap:wrap">
                                                    <span class="section-header-label">"PER-ASSET RESULT"</span>
                                                    <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11px">
                                                        {format!(
                                                            "{total} conditions evaluated · {} requested · {} run{} · {} backfill{}",
                                                            fired_evals.len(),
                                                            unique_runs,
                                                            if unique_runs == 1 { "" } else { "s" },
                                                            unique_backfills,
                                                            if unique_backfills == 1 { "" } else { "s" },
                                                        )}
                                                    </span>
                                                    {(without_link > 0).then(|| view! {
                                                        <span class="grid-cell-mono" style="color:var(--warning); font-size:11px">
                                                            {format!("{without_link} requested but no run/backfill linked")}
                                                        </span>
                                                    })}
                                                </div>
                                                {if fired_evals.is_empty() {
                                                    view! {
                                                        <div class="text-muted" style="font-size:11.5px">
                                                            "No materializations requested in this tick."
                                                        </div>
                                                    }.into_any()
                                                } else {
                                                    view! {
                                                        <div style="display:flex; flex-direction:column; gap:4px">
                                                            {let loc_ns_evals = loc_ns_inner.clone();
                                                            let loc_name_evals = loc_name_inner.clone();
                                                            fired_evals.into_iter().map(move |e| {
                                                                let loc_ns_e = loc_ns_evals.clone();
                                                                let loc_name_e = loc_name_evals.clone();
                                                                let asset_href = loc_path(
                                                                    &loc_ns_e, &loc_name_e,
                                                                    &format!("assets/{}?tab=automation&tick_id={}", e.asset_key, e.tick_id),
                                                                );
                                                                let key = e.asset_key.clone();
                                                                let run_ids = e.run_ids.clone();
                                                                let backfill_ids = e.backfill_ids.clone();

                                                                // Prefer the backfill chip over raw run chips: sub-runs
                                                                // are an implementation detail of the backfill.
                                                                let links_view = if !backfill_ids.is_empty() {
                                                                    let lns = loc_ns_e.clone();
                                                                    let lnm = loc_name_e.clone();
                                                                    let chips = backfill_ids.into_iter().map(move |bid| {
                                                                        let href = loc_path(&lns, &lnm, &format!("backfills/{}", bid));
                                                                        let short = short_id(&bid, 10);
                                                                        let title = format!("Backfill {bid}");
                                                                        view! {
                                                                            <A
                                                                                href=href
                                                                                attr:class="tag"
                                                                                attr:style="font-size:10.5px; background:color-mix(in oklab, var(--secondary) 12%, transparent); color:var(--secondary); border:1px solid color-mix(in oklab, var(--secondary) 30%, transparent)"
                                                                                attr:title=title
                                                                            >
                                                                                <span style="font-size:9px; letter-spacing:0.04em; margin-right:4px; opacity:0.7">"BF"</span>
                                                                                {short}
                                                                            </A>
                                                                        }
                                                                    }).collect::<Vec<_>>();
                                                                    view! {
                                                                        <span style="display:flex; flex-wrap:wrap; gap:4px">{chips}</span>
                                                                    }.into_any()
                                                                } else if !run_ids.is_empty() {
                                                                    let lns = loc_ns_e.clone();
                                                                    let lnm = loc_name_e.clone();
                                                                    let chips = run_ids.into_iter().map(move |rid| {
                                                                        let href = loc_path(&lns, &lnm, &format!("runs/{}", rid));
                                                                        let short = short_id(&rid, 8);
                                                                        view! {
                                                                            <A href=href attr:class="tag" attr:style="font-size:10.5px" attr:title="Run">{short}</A>
                                                                        }
                                                                    }).collect::<Vec<_>>();
                                                                    view! {
                                                                        <span style="display:flex; flex-wrap:wrap; gap:4px">{chips}</span>
                                                                    }.into_any()
                                                                } else {
                                                                    view! {
                                                                        <span class="grid-cell-mono" style="color:var(--warning); font-size:10.5px" title="Requested but no run or backfill linked — likely batched elsewhere or dropped">
                                                                            "no run"
                                                                        </span>
                                                                    }.into_any()
                                                                };
                                                                view! {
                                                                    <div style="display:grid; grid-template-columns:1fr auto; gap:10px; align-items:center; padding:6px 10px; background:var(--bg-surface); border-radius:3px">
                                                                        <A href=asset_href attr:class="grid-cell-mono" attr:style="color:var(--accent); font-size:11.5px; font-weight:500">{key}</A>
                                                                        {links_view}
                                                                    </div>
                                                                }
                                                            }).collect::<Vec<_>>()}
                                                        </div>
                                                    }.into_any()
                                                }}
                                            </div>
                                        }.into_any()
                                    }
                                    }}
                                </div>
                            </Show>
                        }
                    }).collect::<Vec<_>>()}
                </div>
            }.into_any()
        }}
    }.into_any()
}
