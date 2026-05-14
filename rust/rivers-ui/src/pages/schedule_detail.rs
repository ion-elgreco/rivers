//! Schedule detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{Crumb, TickRunChips, Topbar};
use crate::helpers::{tick_counts_summary, tick_status_class};
use crate::loc::{loc_path, use_current_location};
use crate::now::RelTime;
use crate::server_fns::automation::{evaluate_schedule, get_next_tick, get_schedules, get_ticks};

#[component]
pub fn ScheduleDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `name` is always untracked. Reactive consumers explicitly track
    // `params` to refetch on navigation.
    let name = move || params.read_untracked().get("name").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let schedule = Resource::new(
        move || {
            params.track();
            (name(), refresh_tick.get(), loc.get())
        },
        |(_n, _t, (ns, lname))| async move { get_schedules(ns, lname).await },
    );
    let ticks = Resource::new(
        move || {
            params.track();
            (loc.get(), name(), refresh_tick.get())
        },
        |((ns, lname), name, _)| get_ticks(ns, lname, name, Some(50)),
    );

    let eval_action = Action::new(move |_: &()| {
        let n = name();
        let (ns, lname) = loc.get();
        async move { evaluate_schedule(ns, lname, n).await }
    });
    let eval_pending = eval_action.pending();

    let live_status = use_live_kick(
        &["automation", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let (ns_t, name_t) = loc.get();
    let auto_href = loc_path(&ns_t, &name_t, "automation?tab=schedules");
    view! {
        <Topbar crumbs=vec![
            Crumb::linked("Automation", auto_href),
            Crumb::new(name()).mono(),
        ]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
            <button
                class="btn btn-primary"
                on:click=move |_| { eval_action.dispatch(()); }
                disabled=move || eval_pending.get()
            >
                {move || if eval_pending.get() { "Evaluating..." } else { "Evaluate Now" }}
            </button>
        </Topbar>

        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                let current_name = name();
                schedule.get().map(|result| match result {
                    Ok(schedules) => {
                        if let Some(s) = schedules.into_iter().find(|s| s.name == current_name) {
                            let cron_raw = s.cron_schedule.clone();
                            let cron_display = s.cron_description.clone().unwrap_or_else(|| s.cron_schedule.clone());
                            let cron_copy_text = cron_raw.clone();
                            let cron_title = format!("{} (click to copy)", cron_raw);

                            let cron_for_next = s.cron_schedule.clone();
                            let next_tick = Resource::new(
                                move || cron_for_next.clone(),
                                |expr| get_next_tick(expr),
                            );

                            let (lns, lnm) = loc.get();
                            let job_href = loc_path(&lns, &lnm, &format!("jobs/{}", s.job_name));
                            let job_name = s.job_name.clone();
                            let timezone_value = s.timezone.clone().unwrap_or_else(|| "UTC".to_string());
                            let tags = s.tags.clone();

                            view! {
                                <div class="meta-tile-grid meta-tile-grid--4">
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"STATUS"</div>
                                        <div class="meta-tile-value">{s.status.clone()}</div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"JOB"</div>
                                        <div class="meta-tile-value"><A href=job_href>{job_name}</A></div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"TIMEZONE"</div>
                                        <div class="meta-tile-value">{timezone_value}</div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"NEXT TICK"</div>
                                        <div class="meta-tile-value">
                                            <Transition fallback=move || view! { <span>"…"</span> }>
                                                {move || next_tick.get().map(|r| {
                                                    let text = r.ok().flatten().unwrap_or_else(|| "—".to_string());
                                                    view! { <span>{text}</span> }
                                                })}
                                            </Transition>
                                        </div>
                                    </div>
                                </div>

                                <div class="section-header-row" style="margin-top:20px">
                                    <span class="section-header-label">"CRON"</span>
                                </div>
                                <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px">
                                    <code class="rivers-cron-code" style="font-size:13px; padding:6px 10px" title=cron_title.clone()>{cron_display}</code>
                                    <button
                                        class="icon-btn copyable"
                                        title="Copy cron expression"
                                        data-copy=cron_copy_text
                                    >
                                        <svg width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.2">
                                            <rect x="4" y="4" width="8" height="8" rx="1"/>
                                            <path d="M10 4V3a1 1 0 00-1-1H3a1 1 0 00-1 1v6a1 1 0 001 1h1"/>
                                        </svg>
                                    </button>
                                </div>

                                {s.description.map(|desc| view! {
                                    <div class="section-header-row" style="margin-top:20px">
                                        <span class="section-header-label">"DESCRIPTION"</span>
                                    </div>
                                    <div class="detail-panel detail-panel--prose">{desc}</div>
                                })}

                                {(!tags.is_empty()).then(|| view! {
                                    <div class="section-header-row" style="margin-top:20px">
                                        <span class="section-header-label">"TAGS"</span>
                                    </div>
                                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:4px">
                                        {tags.into_iter().map(|(k, v)| {
                                            view! { <span class="tag">{format!("{k}={v}")}</span> }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                })}
                            }.into_any()
                        } else {
                            view! { <div class="error-msg">"Schedule not found."</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading" style="margin-top:24px">"Loading ticks..."</div> }>
            {move || {
                ticks.get().map(|result| match result {
                    Ok(records) => {
                        if records.is_empty() {
                            return view! {
                                <div class="section-header-row" style="margin-top:28px">
                                    <span class="section-header-label">"TICK HISTORY"</span>
                                </div>
                                <div class="empty-state">"No tick history."</div>
                            }.into_any();
                        }
                        const GRID: &str = "grid-template-columns: 120px 130px 1fr 160px";
                        let tick_count = records.len();
                        view! {
                            <div class="section-header-row" style="margin-top:28px">
                                <span class="section-header-label">{format!("TICK HISTORY · LAST {tick_count}")}</span>
                            </div>
                            <div class="grid-table">
                                <div class="grid-table-head" style=GRID>
                                    <span>"TIME"</span>
                                    <span>"STATUS"</span>
                                    <span>"DETAIL"</span>
                                    <span>"RUNS"</span>
                                </div>
                                {records.into_iter().map(|t| {
                                    let ts_now = t.timestamp;
                                    let ts_abs = crate::helpers::format_timestamp_nanos(t.timestamp);
                                    let bar = tick_status_class(&t.status);
                                    let (status_color, dot_cls) = match bar {
                                        "success" => ("var(--success)", "status-dot--ok"),
                                        "failure" => ("var(--error)", "status-dot--err"),
                                        "warning" => ("var(--warning)", "status-dot--warn"),
                                        "running" => ("var(--secondary)", "status-dot--ok"),
                                        _ => ("var(--text-muted)", "status-dot--muted"),
                                    };
                                    let has_error = t.error.is_some();
                                    let detail_text = t.skip_reason.clone()
                                        .or_else(|| t.error.clone())
                                        .or_else(|| tick_counts_summary(&t.run_ids, &t.backfill_ids))
                                        .unwrap_or_else(|| "—".to_string());
                                    let detail_color = if has_error { "var(--error)" } else { "var(--text-muted)" };
                                    let runs_view = view! {
                                        <TickRunChips run_ids=t.run_ids.clone() backfill_ids=t.backfill_ids.clone()/>
                                    };

                                    view! {
                                        <div class="grid-row grid-row--plain" style=GRID title=ts_abs>
                                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px"><RelTime ts=ts_now/></span>
                                            <span class="status-dot-row">
                                                <span class=format!("status-dot {}", dot_cls)></span>
                                                <span class="grid-cell-mono" style=format!("color:{status_color}; font-size:11.5px")>{t.status.clone()}</span>
                                            </span>
                                            <span class="grid-cell-mono" style=format!("color:{detail_color}; font-size:11.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0")>{detail_text}</span>
                                            {runs_view}
                                        </div>
                                    }
                                }).collect::<Vec<_>>()}
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
