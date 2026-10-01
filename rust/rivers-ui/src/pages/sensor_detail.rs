//! Sensor detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{
    AutomationState, Crumb, EvaluateOutcome, SectionHeader, TickHistory, Topbar,
};
use crate::helpers::job_actions_by_name;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::automation::{evaluate_sensor, get_jobs, get_sensors, get_ticks};

#[component]
pub fn SensorDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `name` is always untracked. Reactive consumers explicitly track
    // `params` to refetch on navigation.
    let name = move || params.read_untracked().get("name").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let sensor = Resource::new(
        move || {
            params.track();
            (name(), refresh_tick.get(), loc.get())
        },
        |(_n, _t, (ns, lname))| async move { get_sensors(ns, lname).await },
    );
    let jobs = Resource::new(
        move || loc.get(),
        |(ns, lname)| async move { get_jobs(ns, lname).await },
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
        let (ns, lname) = loc.get_untracked();
        async move { evaluate_sensor(ns, lname, n).await }
    });
    let eval_pending = eval_action.pending();

    let live_status = use_live_kick(
        &["automation", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let (ns_t, name_t) = loc.get_untracked();
    let auto_href = loc_path(&ns_t, &name_t, "automation?tab=sensors");
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
                {move || if eval_pending.get() { "Evaluating…" } else { "Evaluate now" }}
            </button>
        </Topbar>

        {move || eval_action.value().get().map(|result| view! { <EvaluateOutcome result/> })}

        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                let current_name = name();
                sensor.get().map(|result| match result {
                    Ok(sensors) => {
                        if let Some(s) = sensors.into_iter().find(|s| s.name == current_name) {
                            let interval = s.minimum_interval
                                .clone()
                                .unwrap_or_else(|| "—".to_string());
                            let job_name = s.job_name.clone();
                            let asset_selection = s.asset_selection.clone();
                            let tags = s.tags.clone();
                            let job_value = job_name.clone().unwrap_or_else(|| "—".to_string());
                            let (lns, lnm) = loc.get();
                            let job_href = job_name.clone().map(|j| loc_path(&lns, &lnm, &format!("jobs/{}", j)));
                            let asset_count_val = if asset_selection.is_empty() {
                                "all".to_string()
                            } else {
                                asset_selection.len().to_string()
                            };
                            view! {
                                <div class="meta-tile-grid meta-tile-grid--4">
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"JOB"</div>
                                        <div class="meta-tile-value">
                                            {match job_href {
                                                Some(h) => view! { <A href=h>{job_value}</A> }.into_any(),
                                                None => view! { <span>{job_value}</span> }.into_any(),
                                            }}
                                            {move || job_name.clone()
                                                .and_then(|jn| {
                                                    jobs.get().and_then(|r| r.ok())
                                                        .and_then(|js| job_actions_by_name(&js).remove(&jn))
                                                })
                                                .map(|v| view! {
                                                    <span class="grid-cell-muted" title="asset action">{format!(" · {v}")}</span>
                                                })}
                                        </div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"STATUS"</div>
                                        <div class="meta-tile-value"><AutomationState status=s.status.clone()/></div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"MIN INTERVAL"</div>
                                        <div class="meta-tile-value">{interval}</div>
                                    </div>
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"ASSETS"</div>
                                        <div class="meta-tile-value">{asset_count_val}</div>
                                    </div>
                                </div>

                                {(!asset_selection.is_empty()).then(|| view! {
                                    <SectionHeader label="ASSET SELECTION" count=asset_selection.len().to_string()/>
                                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:4px">
                                        {{
                                            let (lns, lnm) = loc.get();
                                            asset_selection.into_iter().map(move |a| {
                                            let asset_href = loc_path(&lns, &lnm, &format!("assets/{}", a));
                                            view! { <A href={asset_href} attr:class="tag">{a}</A> }
                                        }).collect::<Vec<_>>()}}
                                    </div>
                                })}

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
                            view! { <div class="error-msg">"Sensor not found"</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load sensor: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
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
