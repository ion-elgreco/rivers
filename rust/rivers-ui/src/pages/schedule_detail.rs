//! Schedule detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{
    AutomationState, Crumb, EvaluateOutcome, SectionHeader, TickHistory, Topbar,
};
use crate::helpers::job_actions_by_name;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::automation::{
    evaluate_schedule, get_jobs, get_next_tick, get_schedules, get_ticks,
};

#[component]
pub fn ScheduleDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `name` is always untracked. Reactive consumers explicitly track
    // `params` to refetch on navigation.
    let name = move || params.read_untracked().get("name").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let live = use_live_kick(
        &["automation", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );
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

    let jobs = Resource::new(
        move || (loc.get(), live.definitions.get()),
        |((ns, lname), _)| async move { get_jobs(ns, lname).await },
    );

    let eval_action = Action::new(move |_: &()| {
        let n = name();
        let (ns, lname) = loc.get_untracked();
        async move { evaluate_schedule(ns, lname, n).await }
    });
    let eval_pending = eval_action.pending();

    let (ns_t, name_t) = loc.get_untracked();
    let auto_href = loc_path(&ns_t, &name_t, "automation?tab=schedules");
    view! {
        <Topbar crumbs=vec![
            Crumb::linked("Automation", auto_href),
            Crumb::new(name()).mono(),
        ]>
            <LiveStatusChip
                status=live.status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
            <button
                class="btn btn-primary"
                on:click=move |_| { eval_action.dispatch(()); }
                disabled=move || eval_pending.get()
            >
                {move || if eval_pending.get() { "Evaluating…" } else { "Evaluate now" }}
            </button>
        </Topbar>

        {move || eval_action.value().get().map(|result| view! { <EvaluateOutcome result/> })}

        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                let current_name = name();
                schedule.get().map(|result| match result {
                    Ok(schedules) => {
                        if let Some(s) = schedules.into_iter().find(|s| s.name == current_name) {
                            let cron_raw = s.cron_schedule.clone();
                            let cron_display = s.cron_description.clone().unwrap_or_else(|| s.cron_schedule.clone());
                            let cron_copy_text = cron_raw.clone();

                            let cron_for_next = (s.cron_schedule.clone(), s.timezone.clone());
                            let next_tick = Resource::new(
                                move || cron_for_next.clone(),
                                |(expr, tz)| get_next_tick(expr, tz),
                            );

                            let (lns, lnm) = loc.get();
                            let job_href = loc_path(&lns, &lnm, &format!("jobs/{}", s.job_name));
                            let job_name = s.job_name.clone();
                            let verb_job = job_name.clone();
                            let timezone_value = s.timezone.clone().unwrap_or_else(|| "UTC".to_string());
                            let tags = s.tags.clone();

                            view! {
                                <div class="meta-tile-grid meta-tile-grid--4">
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"STATUS"</div>
                                        <div class="meta-tile-value"><AutomationState status=s.status.clone()/></div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"JOB"</div>
                                        <div class="meta-tile-value">
                                            <A href=job_href>{job_name}</A>
                                            {move || jobs.get()
                                                .and_then(|r| r.ok())
                                                .and_then(|js| job_actions_by_name(&js).remove(&verb_job))
                                                .map(|v| view! {
                                                    <span class="grid-cell-muted" title="asset action">{format!(" · {v}")}</span>
                                                })}
                                        </div>
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

                                <SectionHeader label="CRON"/>
                                <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px">
                                    <code class="rivers-cron-code" style="font-size:var(--fs-md); padding:6px 10px" title=cron_raw.clone()>{cron_display}</code>
                                    <button
                                        class="icon-btn copyable"
                                        title="Copy cron expression"
                                        aria-label="Copy cron expression"
                                        data-copy=cron_copy_text
                                    >
                                        <crate::components::icons::IconCopy/>
                                    </button>
                                </div>

                                {s.description.map(|desc| view! {
                                    <SectionHeader label="DESCRIPTION"/>
                                    <div class="detail-panel detail-panel--prose">{desc}</div>
                                })}

                                {(!tags.is_empty()).then(|| view! {
                                    <SectionHeader label="TAGS"/>
                                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:4px">
                                        {tags.into_iter().map(|(k, v)| {
                                            view! { <span class="tag">{format!("{k}={v}")}</span> }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                })}
                            }.into_any()
                        } else {
                            view! { <div class="error-msg">"Schedule not found"</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load schedule: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading">"Loading ticks…"</div> }>
            {move || ticks.get().map(|result| match result {
                Ok(records) => view! { <TickHistory ticks=records/> }.into_any(),
                Err(e) => view! {
                    <div class="error-msg">{format!("Couldn't load ticks: {}", crate::helpers::err_text(&e))}</div>
                }.into_any(),
            })}
        </Transition>
    }
}
