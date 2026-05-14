//! Overview dashboard page.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::StatsSkeleton;
use crate::components::ui_kit::{
    Crumb, DonutStatCard, FeaturedDonut, Rail, SectionHeader, StatTile, SummaryCard, Topbar,
};
use crate::helpers::{run_status_kind, short_id};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::get_assets;
use crate::server_fns::automation::{get_schedules, get_sensors};
use crate::server_fns::overview::{get_assets_info, get_run_stats};
use crate::server_fns::runs::get_runs;

#[component]
pub fn OverviewPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let loc = use_current_location();

    let stats = Resource::new(move || refresh_tick.get(), |_| get_run_stats());
    let recent_runs = Resource::new(move || refresh_tick.get(), |_| get_runs(Some(10), None));
    let assets = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_assets(ns, name, None, None, None),
    );
    let schedules = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_schedules(ns, name).await },
    );
    let sensors = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_sensors(ns, name).await },
    );
    let assets_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_assets_info(ns, name).await },
    );

    let live_status = use_live_kick(
        &["runs", "assets", "backfills", "automation"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <Topbar crumbs=vec![Crumb::new("Overview")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <Transition fallback=move || view! { <div style="height:160px"></div> }>
            {move || {
                stats.get().map(|result| match result {
                    Ok(s) => {
                        let total = s.total;
                        let success = s.success;
                        let failure = s.failure;
                        let started = s.started;
                        let rate = if total > 0 { success as f64 / total as f64 } else { 0.0 };
                        let success_rate_pct = format!("{:.1}%", rate * 100.0);
                        let ring_success = Signal::derive(move || rate);

                        let sub_success = if total > 0 { format!("/ {total}") } else { "/ 0".to_string() };
                        let sub_failed = sub_success.clone();
                        let sub_running = sub_success.clone();
                        let ring_fail = Signal::derive(move || if total > 0 { failure as f64 / total as f64 } else { 0.0 });
                        let ring_run = Signal::derive(move || if total > 0 { started as f64 / total as f64 } else { 0.0 });
                        let live_run = started > 0;
                        view! {
                            <div style="margin-bottom:36px; padding:24px 0 0">
                                <FeaturedDonut
                                    label="● LIVE · TOTAL RUNS · 24H"
                                    value=format!("{total}")
                                    unit="runs".to_string()
                                    ring_value=ring_success
                                    color="var(--success)".to_string()
                                    metrics=vec![
                                        ("success".into(), success_rate_pct),
                                        ("running".into(), started.to_string()),
                                        ("failed".into(), failure.to_string()),
                                    ]
                                />
                            </div>

                            <SectionHeader label="RUN STATUS" count=format!("{total} total")/>
                            <div class="stats-grid">
                                <DonutStatCard
                                    label="Success"
                                    value=success.to_string()
                                    sub=sub_success
                                    ring_value=ring_success
                                    color="var(--success)".to_string()
                                />
                                <DonutStatCard
                                    label="Failed"
                                    value=failure.to_string()
                                    sub=sub_failed
                                    ring_value=ring_fail
                                    color="var(--error)".to_string()
                                />
                                <DonutStatCard
                                    label="In progress"
                                    value=started.to_string()
                                    sub=sub_running
                                    ring_value=ring_run
                                    color="var(--secondary)".to_string()
                                    live=live_run
                                />
                                <StatTile label="Total" value=total.to_string() rail=Rail::Primary/>
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <StatsSkeleton count=4/> }>
            {move || {
                assets.get().map(|result| match result {
                    Ok(records) => {
                        let running_assets: std::collections::HashSet<String> = recent_runs
                            .get()
                            .and_then(|r| r.ok())
                            .unwrap_or_default()
                            .into_iter()
                            .filter(|r| matches!(r.status, crate::types::RunStatus::Started))
                            .flat_map(|r| r.node_names.into_iter())
                            .collect();

                        let mut groups: std::collections::BTreeMap<String, (usize, usize, usize, usize, bool)> = std::collections::BTreeMap::new();
                        for r in &records {
                            let group = r.asset_group.clone().unwrap_or_else(|| "default".to_string());
                            let entry = groups.entry(group).or_insert((0, 0, 0, 0, false));
                            entry.0 += 1;
                            match r.stale_status {
                                crate::types::StaleStatus::UpToDate => entry.1 += 1,
                                crate::types::StaleStatus::Stale => entry.2 += 1,
                                crate::types::StaleStatus::Missing => entry.3 += 1,
                            }
                            if running_assets.contains(&r.asset_key) {
                                entry.4 = true;
                            }
                        }
                        let n_groups = groups.len();
                        let n_assets = records.len();
                        let count_label = format!("{n_groups} groups · {n_assets} assets");
                        let (ns, name) = loc.get();
                        view! {
                            <SectionHeader label="ASSET HEALTH" count=count_label/>
                            <div class="health-grid">
                                {groups.into_iter().map(move |(group, (total, up_to_date, stale, missing, running))| {
                                    let pct = if total > 0 { (up_to_date as f64 / total as f64 * 100.0) as u32 } else { 0 };
                                    let bar_class = if missing > 0 && stale == 0 { "health-bar-fill health-low" }
                                        else if stale > 0 { "health-bar-fill health-partial" }
                                        else { "health-bar-fill health-full" };
                                    let href = loc_path(&ns, &name, &format!("assets?group={}", group));
                                    let card_cls = if running {
                                        "health-card health-card-link shimmer-row"
                                    } else {
                                        "health-card health-card-link"
                                    };
                                    view! {
                                        <A href={href}>
                                            <div class=card_cls>
                                                <div class="health-card-header">
                                                    <span class="health-group-name">{group}</span>
                                                    <span class="health-count">{format!("{up_to_date}/{total} up-to-date")}</span>
                                                </div>
                                                <div class="health-bar">
                                                    <div class={bar_class} style=format!("width: {}%", pct)></div>
                                                </div>
                                                {(stale > 0 || missing > 0).then(|| view! {
                                                    <div class="health-tags">
                                                        {(stale > 0).then(|| view! { <span class="health-tag health-tag--warning">{format!("{stale} stale")}</span> })}
                                                        {(missing > 0).then(|| view! { <span class="health-tag">{format!("{missing} missing")}</span> })}
                                                    </div>
                                                })}
                                            </div>
                                        </A>
                                    }
                                }).collect::<Vec<_>>()}
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <SectionHeader label="AUTOMATION" count="schedules · sensors · conditions"/>
        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                let sched = schedules.get().and_then(|r| r.ok()).unwrap_or_default();
                let sens = sensors.get().and_then(|r| r.ok()).unwrap_or_default();
                let infos = assets_info.get().and_then(|r| r.ok()).unwrap_or_default();
                let running_schedules = sched.iter().filter(|x| x.status.eq_ignore_ascii_case("running")).count();
                let running_sensors = sens.iter().filter(|x| x.status.eq_ignore_ascii_case("running")).count();
                let condition_count = infos.iter().filter(|a| a.automation_condition.is_some()).count();
                let (ns, name) = loc.get();
                view! {
                    <div class="stats-grid">
                        <StatTile
                            label="Schedules running"
                            value=running_schedules.to_string()
                            suffix=format!("/ {}", sched.len())
                            rail=Rail::Running
                            href=loc_path(&ns, &name, "automation?tab=schedules")
                        />
                        <StatTile
                            label="Sensors running"
                            value=running_sensors.to_string()
                            suffix=format!("/ {}", sens.len())
                            rail=Rail::Running
                            href=loc_path(&ns, &name, "automation?tab=sensors")
                        />
                        <StatTile
                            label="Automation conditions"
                            value=condition_count.to_string()
                            rail=Rail::Primary
                            href=loc_path(&ns, &name, "automation?tab=conditions")
                        />
                    </div>
                }
            }}
        </Transition>

        <div style="margin-top:28px">
            <SectionHeader label="RECENT RUNS" count="last 10"/>
            <Transition fallback=move || view! { <div class="loading">"…"</div> }>
                {move || {
                    let runs_all = recent_runs.get().and_then(|r| r.ok()).unwrap_or_default();
                    if runs_all.is_empty() {
                        view! { <div class="empty-state" style="padding:16px">"No recent runs."</div> }.into_any()
                    } else {
                        let (ns, name) = loc.get();
                        view! {
                            <div style="display:flex; flex-direction:column; gap:10px">
                                {runs_all.into_iter().map(move |r| {
                                    let sid = short_id(&r.run_id, 8);
                                    let assets = r.node_names.iter().take(2).cloned().collect::<Vec<_>>().join(", ");
                                    let desc = if assets.is_empty() { "—".to_string() } else { assets };
                                    let kind = run_status_kind(&r.status).to_string();
                                    let glyph = if r.job_name.is_some() { "▶" } else { "◉" };
                                    let title = format!("{glyph} {sid}");
                                    view! {
                                        <SummaryCard
                                            title=title
                                            description=desc
                                            kind=kind
                                            href=loc_path(&ns, &name, &format!("runs/{}", r.run_id))
                                            meta_ts=r.start_time
                                        />
                                    }
                                }).collect::<Vec<_>>()}
                            </div>
                        }.into_any()
                    }
                }}
            </Transition>
        </div>

    }
}
