use std::collections::HashMap;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::ui_kit::{EmptyState, SectionHeader};
use crate::helpers::short_id;
use crate::loc::loc_path;
use crate::types::{
    AssetDefinitionInfo, ConditionEvalRecord, ConditionTickDetail, ConditionTickRecord,
};

use super::{SortIndicator, SortToggle, format_with_commas};

pub(super) fn sort_conditions(
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

pub(super) fn render_conditions_tab(
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
                hint="Set automation_condition=rs.AutomationCondition.eager() on an asset"
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
        let now = jiff::Timestamp::now().as_nanosecond() as i64;
        let window_ns: i64 = 60 * 60 * 1_000_000_000;
        let bucket_ns = window_ns / 60;
        let mut buckets: Vec<(u32, u32)> = vec![(0, 0); 60];
        for t in ticks.iter() {
            let age = now.saturating_sub(t.timestamp);
            if age >= 0 && age < window_ns {
                let idx = (59 - (age / bucket_ns).min(59)) as usize;
                buckets[idx].0 += 1;
                if t.total_fired > 0 {
                    buckets[idx].1 += 1;
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
                            <crate::components::icons::IconChevronRight/>
                        </span>
                        <A
                            href=href
                            attr:class="schedule-name-link"
                            on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                        >{asset_key}</A>
                        <code
                            class="grid-cell-mono"
                            style="color:var(--text-muted); font-size:var(--fs-sm); background:transparent; padding:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0"
                            title={condition.clone()}
                        >{condition.clone()}</code>
                        <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm)">
                            <crate::now::RelTimeOpt ts=last_eval_ts/>
                        </span>
                        <span class="grid-cell-mono" style=format!("color:{result_color}; font-size:var(--fs-sm)")>{result_label}</span>
                        <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm); text-align:right">{evals_formatted}</span>
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
                <SectionHeader label="EVALUATION TICKS"/>
                <EmptyState message="No evaluation ticks yet" compact=true/>
            }.into_any()
        } else {
            const GRID: &str = "grid-template-columns: 24px 1.1fr 0.5fr 0.55fr 0.55fr 1.2fr 1.2fr";
            let tick_count = ticks.len();
            view! {
                <SectionHeader label="EVALUATION TICKS" count=format!("last {tick_count}")/>
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
                                    <span class="grid-cell-mono" style="color:var(--accent); font-size:var(--fs-sm)">{fired.to_string()}</span>
                                </span>
                            }.into_any()
                        } else {
                            view! {
                                <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm); text-align:right">"—"</span>
                            }.into_any()
                        };

                        let runs_cell = if run_ids.is_empty() {
                            if fired > 0 && backfill_ids.is_empty() {
                                view! {
                                    <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm)" title="No direct runs — expand the row for per-asset detail.">"—"</span>
                                }.into_any()
                            } else {
                                view! {
                                    <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm)">"—"</span>
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
                                <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm)">"—"</span>
                            }.into_any()
                        } else {
                            let chips = {
                                let loc_ns_chip = loc_ns_iter.clone();
                                let loc_name_chip = loc_name_iter.clone();
                                backfill_ids.into_iter().map(move |bid| {
                                let href = loc_path(&loc_ns_chip, &loc_name_chip, &format!("backfills/{}", bid));
                                let short = short_id(&bid, 8);
                                let title = format!("Backfill {bid}");
                                view! {
                                    <A
                                        href=href
                                        attr:class="tag tag--backfill"
                                        attr:title=title
                                        on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                                    >
                                        <span class="chip-backfill-prefix">"BF"</span>
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
                                    <crate::components::icons::IconChevronRight/>
                                </span>
                                <span class="grid-cell-mono" style="color:var(--text); font-size:var(--fs-sm)">
                                    <crate::now::RelTime ts=ts_now/>
                                </span>
                                <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm)">{dur_label}</span>
                                <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm); text-align:right">{evaluated.to_string()}</span>
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
                                            return view! { <span class="text-muted" style="font-size:var(--fs-sm)">"Loading…"</span> }.into_any();
                                        }
                                        let detail = tick_detail.get();
                                        if detail.evals.is_empty() {
                                            return view! { <span class="text-muted" style="font-size:var(--fs-sm)">"No evaluations found."</span> }.into_any();
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
                                                    <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-xs)">
                                                        {format!(
                                                            "{} evaluated · {} requested · {} · {}",
                                                            crate::helpers::plural(total as u64, "condition", "conditions"),
                                                            fired_evals.len(),
                                                            crate::helpers::plural(unique_runs as u64, "run", "runs"),
                                                            crate::helpers::plural(unique_backfills as u64, "backfill", "backfills"),
                                                        )}
                                                    </span>
                                                    {(without_link > 0).then(|| view! {
                                                        <span class="grid-cell-mono" style="color:var(--warning); font-size:var(--fs-xs)">
                                                            {format!("{without_link} requested but no run/backfill linked")}
                                                        </span>
                                                    })}
                                                </div>
                                                {if fired_evals.is_empty() {
                                                    view! {
                                                        <div class="text-muted" style="font-size:var(--fs-sm)">
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
                                                                        let short = short_id(&bid, 8);
                                                                        let title = format!("Backfill {bid}");
                                                                        view! {
                                                                            <A
                                                                                href=href
                                                                                attr:class="tag tag--backfill"
                                                                                attr:style="font-size:var(--fs-xs)"
                                                                                attr:title=title
                                                                            >
                                                                                <span class="chip-backfill-prefix">"BF"</span>
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
                                                                            <A href=href attr:class="tag" attr:style="font-size:var(--fs-xs)" attr:title="Run">{short}</A>
                                                                        }
                                                                    }).collect::<Vec<_>>();
                                                                    view! {
                                                                        <span style="display:flex; flex-wrap:wrap; gap:4px">{chips}</span>
                                                                    }.into_any()
                                                                } else {
                                                                    view! {
                                                                        <span class="grid-cell-mono" style="color:var(--warning); font-size:var(--fs-xs)" title="Requested but no run or backfill linked — likely batched elsewhere or dropped">
                                                                            "no run"
                                                                        </span>
                                                                    }.into_any()
                                                                };
                                                                view! {
                                                                    <div style="display:grid; grid-template-columns:1fr auto; gap:10px; align-items:center; padding:6px 10px; background:var(--bg-surface); border-radius:3px">
                                                                        <A href=asset_href attr:class="grid-cell-mono" attr:style="color:var(--accent); font-size:var(--fs-sm); font-weight:500">{key}</A>
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
