//! Asset detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::ui_kit::{
    Crumb, EventGlyphTimeline, GlyphEvent, RecentRunsStrip, StripRun, Topbar,
};
use crate::helpers::{
    format_duration, format_relative_time, format_timestamp, run_status_class, run_status_kind,
    short_id, use_query_param,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::trigger_materialize;
use crate::server_fns::assets::{get_asset, get_asset_events, get_assets};
use crate::server_fns::automation::{get_condition_evals, observe_asset};
use crate::server_fns::graph::get_graph_topology;
use crate::server_fns::overview::{get_assets_info, get_partition_status};
use crate::server_fns::runs::get_runs_for_asset;

fn event_type_class(t: &str) -> &'static str {
    match t {
        "Materialization" | "StepSuccess" => "ok",
        "StepFailure" => "err",
        "Observation" => "info",
        _ => "muted",
    }
}

#[component]
pub fn AssetDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `key` is always untracked — readable from any context without the
    // "outside reactive context" warning. Reactive consumers (Resources,
    // Effects, view closures that need to refetch on navigation) explicitly
    // call `params.track()` to subscribe to route changes.
    let key = move || params.read_untracked().get("key").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);

    let asset = Resource::new(
        move || {
            params.track();
            (loc.get(), key(), refresh_tick.get())
        },
        |((ns, name), key, _)| get_asset(ns, name, key),
    );
    let events = Resource::new(
        move || {
            params.track();
            (loc.get(), key(), refresh_tick.get())
        },
        |((ns, name), key, _)| get_asset_events(ns, name, key, Some(100)),
    );
    let asset_runs = Resource::new(
        move || {
            params.track();
            (loc.get(), key(), refresh_tick.get())
        },
        |((ns, name), key, _)| get_runs_for_asset(ns, name, key, Some(10)),
    );
    let assets_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_assets_info(ns, name).await },
    );
    let graph = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_graph_topology(ns, name).await },
    );
    let all_assets = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_assets(ns, name, None, None, None),
    );

    let (active_tab, set_active_tab) = use_query_param("tab", "overview");
    let (event_filter, set_event_filter) = use_query_param("event_filter", "All");
    let (meta_expanded, set_meta_expanded) = signal(false);

    // Derive asset properties into signals (avoids reading Resource outside Transition)
    let is_external = RwSignal::new(false);
    let has_partitions = RwSignal::new(false);
    Effect::new(move |_| {
        params.track();
        let current_key = key();
        let info = assets_info
            .get()
            .and_then(|r| r.ok())
            .and_then(|infos| infos.into_iter().find(|a| a.asset_key == current_key));
        is_external.set(info.as_ref().map(|i| i.is_external).unwrap_or(false));
        has_partitions.set(
            info.as_ref()
                .map(|i| i.partition_def.is_some())
                .unwrap_or(false),
        );
    });

    let live_status = use_live_kick(
        &["assets", "events"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let observe_key = key();
    let observe_action = Action::new(move |_: &()| {
        let k = observe_key.clone();
        let (ns, lname) = loc.get();
        async move { observe_asset(ns, lname, k).await }
    });
    let observe_pending = observe_action.pending();

    let mat_key = key();
    let materialize_action = Action::new(move |_: &()| {
        let k = mat_key.clone();
        let (ns, lname) = loc.get();
        async move { trigger_materialize(ns, lname, Some(vec![k]), None, None).await }
    });
    let materialize_pending = materialize_action.pending();

    let (ns_t, name_t) = loc.get();
    let assets_href = loc_path(&ns_t, &name_t, "assets");
    view! {
        <Topbar crumbs=vec![
            Crumb::linked("Assets", assets_href),
            Crumb::new(key()).mono(),
        ]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
            {move || {
                // External assets are read-only at this layer — we can only record
                // an observation. Non-external assets are materialized instead.
                if is_external.get() {
                    view! {
                        <button
                            class="btn btn-primary"
                            on:click=move |_| { observe_action.dispatch(()); }
                            disabled=move || observe_pending.get()
                        >
                            {move || if observe_pending.get() { "Observing..." } else { "Observe" }}
                        </button>
                    }.into_any()
                } else {
                    view! {
                        <button
                            class="btn btn-primary"
                            on:click=move |_| { materialize_action.dispatch(()); }
                            disabled=move || materialize_pending.get()
                        >
                            {move || if materialize_pending.get() { "Materializing..." } else { "Materialize" }}
                        </button>
                    }.into_any()
                }
            }}
            {move || observe_action.value().get().map(|result| match result {
                Ok(_) => view! { <span class="text-success" style="margin-left: 0.5rem">"Observed"</span> }.into_any(),
                Err(e) => view! { <span class="text-error" style="margin-left: 0.5rem">{format!("{e}")}</span> }.into_any(),
            })}
            {move || materialize_action.value().get().map(|result| match result {
                Ok(ref r) if r.status == "queued" => view! { <span class="text-warning" style="margin-left: 0.5rem">"Queued"</span> }.into_any(),
                Ok(_) => view! { <span class="text-success" style="margin-left: 0.5rem">"Materialized"</span> }.into_any(),
                Err(e) => view! { <span class="text-error" style="margin-left: 0.5rem">{format!("{e}")}</span> }.into_any(),
            })}
        </Topbar>


        // Wrapped in a Transition so the SSR-rendered count survives hydration
        // instead of flashing to 0 while the client resource re-resolves.
        <div class="tab-bar">
            <Transition>
                {move || {
                    // `None` while the resource is pending — badge hides rather
                    // than showing a misleading 0. Only count TERMINAL events
                    // (same filter as the events table body) so the badge matches
                    // what users see when they click in.
                    let event_count: Option<usize> = events.get().and_then(|r| r.ok()).map(|v| {
                        use crate::types::EventType::*;
                        v.iter()
                            .filter(|e| matches!(e.event_type, Materialization | Observation | StepFailure))
                            .count()
                    });
                    let mut tabs: Vec<(&str, &str, Option<usize>)> = vec![
                        ("overview", "Overview", None),
                        ("events", "Events", event_count),
                    ];
                    if has_partitions.get() {
                        tabs.push(("partitions", "Partitions", None));
                    }
                    tabs.push(("automation", "Automation", None));
                    tabs.push(("lineage", "Lineage", None));

                    tabs.into_iter().map(|(tab, label, count)| {
                        let set_tab = set_active_tab.clone();
                        let tab_str = tab.to_string();
                        let tab_str2 = tab_str.clone();
                        view! {
                            <button
                                class=move || if active_tab.get() == tab_str { "tab active" } else { "tab" }
                                on:click=move |_| set_tab(tab_str2.clone())
                            >
                                {label}
                                {count.map(|n| view! { <span class="tab-count">{n}</span> })}
                            </button>
                        }
                    }).collect::<Vec<_>>()
                }}
            </Transition>
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "overview" { "" } else { "display:none" }>
            <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
                {move || {
                    let current_key = key();
                    let rec = asset.get().and_then(|r| r.ok()).flatten();
                    let info = assets_info
                        .get()
                        .and_then(|r| r.ok())
                        .and_then(|infos| infos.into_iter().find(|a| a.asset_key == current_key));
                    let Some(record) = rec else {
                        return view! { <div class="empty-state">"Asset not found."</div> }.into_any();
                    };

                    let kind_val = if record.kinds.is_empty() { "—".to_string() } else { record.kinds.join(", ") };
                    let group_val = record.asset_group.clone().unwrap_or_else(|| "—".to_string());
                    let last_ts_val = if record.last_timestamp.is_some() {
                        format_timestamp(record.last_timestamp)
                    } else {
                        "never".to_string()
                    };
                    // Tooltip text (hover only — no reactive tick needed).
                    let last_ts_rel = record.last_timestamp
                        .map(|t| format_relative_time(t, chrono::Utc::now().timestamp()))
                        .unwrap_or_default();
                    let last_label = if is_external.get() { "LAST OBSERVED" } else { "LAST MATERIALIZED" };
                    let partitioned_val = info
                        .as_ref()
                        .and_then(|i| i.partition_def.as_ref())
                        .map(|pd| format!("{} · {} keys", pd.kind, pd.total_count))
                        .unwrap_or_else(|| "no".to_string());

                    let code_version = record.code_version.clone().unwrap_or_else(|| "—".to_string());
                    let data_version = record.last_data_version.clone().unwrap_or_else(|| "—".to_string());
                    let tags_val = if record.tags.is_empty() { "—".to_string() } else { record.tags.join(", ") };
                    let type_val = info.as_ref().map(|i| i.asset_type.clone()).unwrap_or_else(|| "Asset".to_string());
                    let io_val = info.as_ref()
                        .and_then(|i| i.io_handler.clone())
                        .unwrap_or_else(|| "default".to_string());
                    let self_dep_val = info.as_ref().map(|i| if i.has_self_dependency { "yes" } else { "no" }).unwrap_or("no").to_string();
                    let hooks_val = info.as_ref().map(|i| {
                        if i.hooks.is_empty() { "none".to_string() }
                        else { i.hooks.iter().map(|h| format!("{}({})", h.hook_type, h.function_name)).collect::<Vec<_>>().join(", ") }
                    }).unwrap_or_else(|| "none".to_string());

                    let secondary_tiles = [
                        ("TYPE", type_val),
                        ("CODE VERSION", code_version),
                        ("DATA VERSION", data_version),
                        ("TAGS", tags_val),
                        ("IO HANDLER", io_val),
                        ("HOOKS", hooks_val),
                        ("SELF DEPENDENCY", self_dep_val),
                    ];
                    let secondary_count = secondary_tiles.len();

                    let automation_cond = info.as_ref().and_then(|i| i.automation_condition.clone());

                    view! {
                        <div class="meta-tile-grid meta-tile-grid--4">
                            <div class="meta-tile">
                                <div class="meta-tile-label">"KIND"</div>
                                <div class="meta-tile-value">{kind_val}</div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">"GROUP"</div>
                                <div class="meta-tile-value">{group_val}</div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">{last_label}</div>
                                <div class="meta-tile-value" title=last_ts_rel>{last_ts_val}</div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">"PARTITIONED"</div>
                                <div class="meta-tile-value">{partitioned_val}</div>
                            </div>
                        </div>

                        <button
                            class="meta-toggle-btn"
                            on:click=move |_| set_meta_expanded.update(|v| *v = !*v)
                        >
                            <span
                                class="meta-toggle-chevron"
                                class:meta-toggle-chevron--open=move || meta_expanded.get()
                            >"›"</span>
                            {move || if meta_expanded.get() { "Hide metadata".to_string() } else { format!("Show metadata ({secondary_count})") }}
                        </button>

                        <Show when=move || meta_expanded.get()>
                            <div class="meta-tile-grid meta-tile-grid--3">
                                {secondary_tiles.iter().map(|(label, val)| view! {
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">{*label}</div>
                                        <div class="meta-tile-value meta-tile-value--secondary">{val.clone()}</div>
                                    </div>
                                }).collect::<Vec<_>>()}
                            </div>
                        </Show>

                        {automation_cond.map(|cond| view! {
                            <div class="section-header-label" style="margin: 14px 0 8px">"AUTOMATION CONDITION"</div>
                            <pre class="automation-condition"><code>{cond}</code></pre>
                        })}
                    }.into_any()
                }}
            </Transition>

            {move || if is_external.get() {
                view! {
                    <h2>"Recent Observations"</h2>
                    <Transition fallback=move || view! { <div class="loading">"Loading observations..."</div> }>
                        {move || {
                            events.get().map(|result| match result {
                                Ok(all_events) => {
                                    let observations: Vec<_> = all_events.into_iter()
                                        .filter(|e| matches!(e.event_type, crate::types::EventType::Observation))
                                        .take(10)
                                        .collect();
                                    if observations.is_empty() {
                                        return view! { <div class="empty-state">"No observations for this asset."</div> }.into_any();
                                    }
                                    view! {
                                        <div class="item-list">
                                            {observations.into_iter().map(|evt| {
                                                let evt_ts = evt.timestamp;
                                                let ts_abs = format_timestamp(Some(evt.timestamp));
                                                let dv = evt.data_version.clone();
                                                view! {
                                                    <div class="item-row item-row--muted">
                                                        <div class="item-row-bar"></div>
                                                        <div class="item-row-body">
                                                            <div class="item-row-top">
                                                                <span class="item-row-meta" title={ts_abs}><crate::now::RelTime ts=evt_ts/></span>
                                                                {dv.map(|v| {
                                                                    let short = if v.len() > 12 { format!("{}...", &v[..12]) } else { v };
                                                                    view! { <span class="tag tag-muted">{format!("v:{short}")}</span> }
                                                                })}
                                                                <span class="item-row-spacer"></span>
                                                                <span class="item-row-status item-row-status--muted">"Observed"</span>
                                                            </div>
                                                        </div>
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
                }.into_any()
            } else {
                view! {
                    <h2>"Recent Runs"</h2>
                    <Transition fallback=move || view! { <div class="loading">"Loading runs..."</div> }>
                        {move || {
                            asset_runs.get().map(|result| match result {
                                Ok(runs) => {
                                    if runs.is_empty() {
                                        return view! { <div class="empty-state">"No runs for this asset."</div> }.into_any();
                                    }
                                    let strip: Vec<StripRun> = runs.iter().rev().map(|r| {
                                        let status = match r.status {
                                            crate::types::RunStatus::Success => "ok",
                                            crate::types::RunStatus::Failure => "err",
                                            crate::types::RunStatus::Started | crate::types::RunStatus::NotStarted | crate::types::RunStatus::Queued => "retry",
                                            _ => "err",
                                        };
                                        let live = matches!(r.status, crate::types::RunStatus::Started);
                                        let dur = r.end_time.map(|e| (e - r.start_time).max(1) as f64 / 1000.0).unwrap_or(5.0);
                                        let id = short_id(&r.run_id, 8);
                                        StripRun { id, status, duration_s: dur, live }
                                    }).collect();
                                    view! {
                                        <RecentRunsStrip runs=strip/>
                                        <div class="run-list" style="margin-top:14px">
                                            {runs.into_iter().map(|r| {
                                                let run_id = r.run_id.clone();
                                                let (lns, lnm) = loc.get();
                                                let href = loc_path(&lns, &lnm, &format!("runs/{}", run_id));
                                                let sid = short_id(&run_id, 8);
                                                let st_class = run_status_class(&r.status);
                                                let st_kind = run_status_kind(&r.status);
                                                let start_ts = r.start_time;
                                                let created_abs = format_timestamp(Some(r.start_time));
                                                let duration = format_duration(Some(r.start_time), r.end_time);
                                                let partition_val: Option<String> = r.tags.iter()
                                                    .find(|(k, _)| k == "partition" || k == "partition_key")
                                                    .map(|(_, v)| v.clone());
                                                let job_name = r.job_name.clone();
                                                let job_href = job_name
                                                    .as_ref()
                                                    .map(|j| loc_path(&lns, &lnm, &format!("jobs/{}", j)));
                                                view! {
                                                    <div class={format!("run-row run-row--{}", st_class)}>
                                                        <div class="run-row-bar"></div>
                                                        <div class="run-row-body">
                                                            <div class="run-row-top">
                                                                <A href={href}><span class="run-row-id">{sid}</span></A>
                                                                {job_name.zip(job_href).map(|(j, h)| view! { <A href={h}><span class="run-row-job">{j}</span></A> })}
                                                                {partition_val.map(|p| view! {
                                                                    <span class="run-row-partition">{p}</span>
                                                                })}
                                                                <span class="run-row-spacer"></span>
                                                                <span class={format!("run-row-status run-row-status--{}", st_class)}>{st_kind}</span>
                                                            </div>
                                                            <div class="run-row-bottom">
                                                                <span class="run-row-meta" title={created_abs}><crate::now::RelTime ts=start_ts/></span>
                                                                <span class="run-row-meta">{duration}</span>
                                                            </div>
                                                        </div>
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
                }.into_any()
            }}
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "events" { "" } else { "display:none" }>
            // Show only TERMINAL events (materializations / failures / observations) —
            // intermediate events like StepStart, LogOutput, or slot-claim/renew are
            // noise at this zoom.
            <Transition>
                {move || {
                    events.get().and_then(|r| r.ok()).map(|evts| {
                        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                        let glyph_events: Vec<GlyphEvent> = evts
                            .iter()
                            .filter_map(|e| {
                                let (glyph, status) = match e.event_type {
                                    crate::types::EventType::Materialization => ("◆", "ok"),
                                    crate::types::EventType::StepFailure => ("▲", "err"),
                                    crate::types::EventType::Observation => ("○", "info"),
                                    _ => return None,
                                };
                                let minutes_ago = ((now_ns - e.timestamp) as f64) / 60_000_000_000.0;
                                Some(GlyphEvent {
                                    minutes_ago: minutes_ago.clamp(0.0, 180.0),
                                    glyph,
                                    status,
                                    run: Some(e.run_id.clone()),
                                    label: format!("{:?} · {}", e.event_type, e.run_id),
                                })
                            })
                            .take(80)
                            .collect();
                        view! { <EventGlyphTimeline events=glyph_events/> }
                    })
                }}
            </Transition>
            <div class="asset-event-filter-bar">
                {
                    let set_ef_all = set_event_filter.clone();
                    let set_ef_mat = set_event_filter.clone();
                    let set_ef_fail = set_event_filter.clone();
                    let cls = move |k: &str| {
                        if event_filter.get() == k { "asset-event-filter-pill asset-event-filter-pill--active".to_string() }
                        else { "asset-event-filter-pill".to_string() }
                    };
                    let cls_all = move || cls("All");
                    let cls_mat = move || cls("mat");
                    let cls_fail = move || cls("fail");
                    view! {
                        <button class=cls_all on:click=move |_| set_ef_all("All".to_string())>"All events"</button>
                        <button class=cls_mat on:click=move |_| set_ef_mat("mat".to_string())>"Materializations"</button>
                        <button class=cls_fail on:click=move |_| set_ef_fail("fail".to_string())>"Failures & retries"</button>
                    }
                }
            </div>
            <Transition fallback=move || view! { <div class="loading">"Loading events..."</div> }>
                {move || {
                    events.get().map(|result| match result {
                        Ok(evts) => {
                            let filter = event_filter.get();
                            let filtered: Vec<_> = evts.into_iter().filter(|e| {
                                use crate::types::EventType::*;
                                let is_shown = matches!(e.event_type, Materialization | Observation | StepFailure);
                                if !is_shown { return false; }
                                match filter.as_str() {
                                    "mat" => matches!(e.event_type, Materialization),
                                    "fail" => matches!(e.event_type, StepFailure),
                                    _ => true,
                                }
                            }).collect();

                            if filtered.is_empty() {
                                view! { <div class="empty-state">"No events matching filter."</div> }.into_any()
                            } else {
                                view! {
                                    <div class="events-list">
                                        {filtered.into_iter().map(|evt| {
                                            let evt_ts = evt.timestamp;
                                            let time_abs = crate::helpers::nanos_to_datetime(evt.timestamp)
                                                .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                                                .unwrap_or_default();
                                            let type_label = format!("{:?}", evt.event_type);
                                            let type_cls = event_type_class(&type_label);
                                            let glyph = match type_label.as_str() {
                                                "Materialization" | "StepSuccess" => "◆",
                                                "StepFailure" => "▲",
                                                "Observation" => "○",
                                                "StepStart" => "◐",
                                                _ => "•",
                                            };
                                            let mut msg_parts: Vec<String> = Vec::new();
                                            if let Some(ref v) = evt.data_version {
                                                msg_parts.push(format!("v:{}", short_id(v, 8)));
                                            }
                                            if let Some(ref p) = evt.partition_key {
                                                msg_parts.push(format!("partition {p}"));
                                            }
                                            for (k, v) in evt.metadata.iter().take(3) {
                                                let val = v.as_text();
                                                let val_short = if val.chars().count() > 48 {
                                                    format!("{}…", val.chars().take(45).collect::<String>())
                                                } else {
                                                    val
                                                };
                                                msg_parts.push(format!("{k}={val_short}"));
                                            }
                                            let message = if msg_parts.is_empty() { "—".to_string() } else { msg_parts.join(" · ") };
                                            let run_short = if evt.run_id.len() > 8 {
                                                format!("#{}", &evt.run_id[..8])
                                            } else if evt.run_id.is_empty() {
                                                "—".to_string()
                                            } else {
                                                format!("#{}", evt.run_id)
                                            };
                                            let (lns, lnm) = loc.get();
                                            let run_href = if evt.run_id.is_empty() { None } else { Some(loc_path(&lns, &lnm, &format!("runs/{}", evt.run_id))) };
                                            view! {
                                                <div class=format!("event-row event-row--{type_cls}") title={time_abs}>
                                                    <span class=format!("event-row-glyph event-row-glyph--{type_cls}")>{glyph}</span>
                                                    <span class=format!("event-row-type event-row-type--{type_cls}")>{type_label}</span>
                                                    <span class="event-row-time"><crate::now::RelTime ts=evt_ts/></span>
                                                    <span class="event-row-msg">{message}</span>
                                                    {match run_href {
                                                        Some(href) => view! { <A href=href attr:class="event-row-run">{run_short}</A> }.into_any(),
                                                        None => view! { <span class="event-row-run event-row-run--none">{run_short}</span> }.into_any(),
                                                    }}
                                                </div>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                }.into_any()
                            }
                        }
                        Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                    })
                }}
            </Transition>
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "partitions" { "" } else { "display:none" }>
            <PartitionsTab
                asset_key=key()
                dynamic_name=Signal::derive(move || {
                    assets_info
                        .get()
                        .and_then(|r| r.ok())
                        .and_then(|infos| infos.into_iter().find(|a| a.asset_key == key()))
                        .and_then(|a| a.partition_def)
                        .and_then(|pd| pd.dynamic_namespace().map(str::to_string))
                        .unwrap_or_default()
                })
            />
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "automation" { "" } else { "display:none" }>
            <AutomationTicksTab asset_key=key() refresh_tick=refresh_tick/>
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "lineage" { "" } else { "display:none" }>
            <Transition fallback=move || view! { <div class="loading">"Loading lineage..."</div> }>
                {move || {
                    let current_key = key();
                    let topo = graph.get().and_then(|r| r.ok());
                    let records_by_key: std::collections::HashMap<String, crate::types::AssetRecord> = all_assets
                        .get()
                        .and_then(|r| r.ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| (r.asset_key.clone(), r))
                        .collect();

                    let (upstream, downstream): (Vec<String>, Vec<String>) = topo
                        .map(|t| (t.direct_upstream(&current_key), t.direct_downstream(&current_key)))
                        .unwrap_or_default();

                    let (lns, lnm) = loc.get();
                    let render_col = |label: &'static str, keys: Vec<String>, empty_msg: &'static str, records: &std::collections::HashMap<String, crate::types::AssetRecord>| {
                        let count = keys.len();
                        if keys.is_empty() {
                            view! {
                                <div>
                                    <div class="section-header-label" style="margin-bottom:10px">{format!("{label} · {count}")}</div>
                                    <div class="lineage-col-empty">{empty_msg}</div>
                                </div>
                            }.into_any()
                        } else {
                            let rows: Vec<_> = keys.into_iter().map(|dep| {
                                let href = loc_path(&lns, &lnm, &format!("assets/{}", dep));
                                let rec = records.get(&dep);
                                let kind_text = rec
                                    .and_then(|r| r.kinds.first().cloned())
                                    .unwrap_or_else(|| "asset".to_string());
                                let mat_ts: Option<i64> = rec.and_then(|r| r.last_timestamp);
                                let rail_color = rec
                                    .map(|r| match r.stale_status {
                                        crate::types::StaleStatus::UpToDate => "var(--success)",
                                        crate::types::StaleStatus::Stale => "var(--warning)",
                                        crate::types::StaleStatus::Missing => "var(--text-muted)",
                                    })
                                    .unwrap_or("var(--text-muted)");
                                let style = format!("border-left-color: {rail_color}");
                                view! {
                                    <A href=href attr:class="lineage-row" attr:style=style>
                                        <span class="lineage-row-key">{dep}</span>
                                        <span class="lineage-row-kind">{kind_text}</span>
                                        <span class="lineage-row-mat">
                                            <crate::now::RelTimeOpt ts=mat_ts/>
                                        </span>
                                    </A>
                                }
                            }).collect();
                            view! {
                                <div>
                                    <div class="section-header-label" style="margin-bottom:10px">{format!("{label} · {count}")}</div>
                                    <div class="lineage-rows">{rows}</div>
                                </div>
                            }.into_any()
                        }
                    };

                    view! {
                        <div class="lineage-grid">
                            {render_col("UPSTREAM", upstream, "— no upstream dependencies (source asset)", &records_by_key)}
                            {render_col("DOWNSTREAM", downstream, "— no downstream consumers", &records_by_key)}
                        </div>
                    }.into_any()
                }}
            </Transition>
        </div>
    }
}

#[component]
fn PartitionsTab(
    asset_key: String,
    /// For a Dynamic asset, its namespace name (so `get_partition_status` sources
    /// the storage-managed keys); empty for other kinds.
    #[prop(into)]
    dynamic_name: Signal<String>,
) -> impl IntoView {
    let key = asset_key.clone();
    let mat_key = asset_key.clone();
    let loc = use_current_location();
    // Heatmap page start (one cell per key). Keep `PAGE` in sync with the
    // server's `HEATMAP_PAGE`.
    const PAGE: u64 = 1000;
    let offset = RwSignal::new(0u64);
    let partition_status = Resource::new(
        move || (key.clone(), loc.get(), offset.get(), dynamic_name.get()),
        |(key, (ns, name), offset, dyn_name)| async move {
            get_partition_status(ns, name, key, offset, dyn_name).await
        },
    );

    let materialize_missing = Action::new(move |_: &()| {
        let k = mat_key.clone();
        let (ns, name) = loc.get();
        async move { trigger_materialize(ns, name, Some(vec![k]), None, None).await }
    });
    let mat_pending = materialize_missing.pending();

    view! {
        <Transition fallback=move || view! { <div class="loading">"Loading partitions..."</div> }>
            {move || {
                partition_status.get().map(|result| match result {
                    Ok(status) => {
                        if status.partition_details.is_empty() {
                            return view! { <div class="empty-state">"This asset has no partitions, or no partitions have been materialized yet."</div> }.into_any();
                        }
                        let has_missing = status.missing > 0;
                        use crate::components::ui_kit::{HeatCell, PartitionHeatmap};
                        let cells: Vec<HeatCell> = status.partition_details.iter().map(|p| {
                            match p.status.as_str() {
                                "Materialized" => HeatCell::Done,
                                "Failed" => HeatCell::Failed,
                                _ => HeatCell::Pending,
                            }
                        }).collect();
                        let heatmap_labels: Vec<String> = status.partition_details.iter()
                            .map(|p| p.key.clone())
                            .collect();
                        // The backend caps `partition_details` to a window, so the
                        // heatmap stays bounded even for million-partition assets.
                        let shown = status.partition_details.len();
                        let total_n = status.total_partitions;
                        view! {
                            <div class="partition-header">
                                <div class="partition-summary">
                                    <span class="stat-inline"><strong>{status.materialized}</strong>" materialized"</span>
                                    <span class="stat-inline"><strong>{status.failed}</strong>" failed"</span>
                                    <span class="stat-inline"><strong>{status.missing}</strong>" missing"</span>
                                    <span class="stat-inline">"of "<strong>{status.total_partitions}</strong>" total"</span>
                                    {(total_n > PAGE as usize).then(|| {
                                        let off = offset.get() as usize;
                                        view! {
                                            <span class="stat-inline muted">
                                                {format!("showing {}–{} of {}", off + 1, off + shown, total_n)}
                                            </span>
                                            <button
                                                class="btn btn-tertiary btn-small"
                                                disabled={move || offset.get() == 0}
                                                on:click=move |_| offset.update(|o| *o = o.saturating_sub(PAGE))
                                            >
                                                "Prev"
                                            </button>
                                            <button
                                                class="btn btn-tertiary btn-small"
                                                // Braced: the `>=` would otherwise read as a tag close in `view!`.
                                                disabled={move || offset.get() as usize + shown >= total_n}
                                                on:click=move |_| offset.update(|o| *o += PAGE)
                                            >
                                                "Next"
                                            </button>
                                        }
                                    })}
                                </div>
                                {has_missing.then(|| view! {
                                    <button
                                        class="btn btn-primary btn-small"
                                        on:click=move |_| { materialize_missing.dispatch(()); }
                                        disabled=move || mat_pending.get()
                                    >
                                        {move || if mat_pending.get() { "Materializing..." } else { "Materialize Missing" }}
                                    </button>
                                })}
                            </div>
                            <PartitionHeatmap cells=cells labels=heatmap_labels legend=true freshness_gradient=true/>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}

#[component]
fn AutomationTicksTab(asset_key: String, #[prop(into)] refresh_tick: Signal<u32>) -> impl IntoView {
    let key = asset_key.clone();
    let info_key = asset_key.clone();
    let loc = use_current_location();
    let evals = Resource::new(
        move || (loc.get(), key.clone(), refresh_tick.get()),
        |((ns, name), key, _)| get_condition_evals(ns, name, key, Some(50)),
    );
    let assets_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_assets_info(ns, name).await },
    );
    let (preselect_tick_id, _) = use_query_param("tick_id", "");
    let (selected_idx, set_selected_idx) = signal(0usize);
    let preselect_applied = RwSignal::new(false);

    view! {
        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                let current_key = info_key.clone();
                assets_info.get().map(|result| match result {
                    Ok(infos) => {
                        if let Some(info) = infos.into_iter().find(|a| a.asset_key == current_key) {
                            if let Some(cond) = info.automation_condition {
                                view! {
                                    <div class="section-header-row">
                                        <span class="section-header-label">"AUTOMATION CONDITION"</span>
                                    </div>
                                    <pre class="automation-condition"><code>{cond}</code></pre>
                                }.into_any()
                            } else {
                                view! { <div class="empty-state">"This asset has no automation condition."</div> }.into_any()
                            }
                        } else {
                            view! { <div class="empty-state">"Asset definition not found."</div> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading">"Loading evaluations..."</div> }>
            {move || {
                evals.get().map(|result| match result {
                    Ok(records) => {
                        if records.is_empty() {
                            return view! { <div class="empty-state">"No evaluations yet. The daemon stores evaluations every tick."</div> }.into_any();
                        }
                        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                        let window_ns: i64 = 60 * 60 * 1_000_000_000;
                        let bucket_ns = window_ns / 60;
                        let mut buckets: Vec<(u32, bool)> = vec![(0, false); 60];
                        for r in records.iter() {
                            let age = now.saturating_sub(r.timestamp);
                            if age >= 0 && age < window_ns {
                                let idx = (59 - (age / bucket_ns).min(59)) as usize;
                                buckets[idx].0 += 1;
                                if r.fired {
                                    buckets[idx].1 = true;
                                }
                            }
                        }
                        let hist = view! { <crate::components::ui_kit::EvalTimelineBars buckets=buckets/> };
                        if !preselect_applied.get() {
                            let tid = preselect_tick_id.get();
                            if !tid.is_empty()
                                && let Some(idx) = records.iter().position(|e| e.tick_id == tid) {
                                    set_selected_idx.set(idx);
                                }
                            preselect_applied.set(true);
                        }
                        let records_for_tree = records.clone();
                        let initial_selected = selected_idx.get_untracked();

                        const GRID: &str = "grid-template-columns: 120px 110px 1fr 80px";

                        view! {
                            {hist}

                            <div class="section-header-row" style="margin-top:24px">
                                <span class="section-header-label">{format!("RECENT TICKS · LAST {}", records.len())}</span>
                            </div>
                            <div class="grid-table">
                                {records.iter().enumerate().map(|(idx, e)| {
                                    let ts_now = e.timestamp;
                                    let ts_abs = crate::helpers::format_timestamp_nanos(e.timestamp);
                                    let fired = e.fired;
                                    let dur_ms = e.eval_duration_us as f64 / 1000.0;
                                    let dur_label = if dur_ms < 1.0 {
                                        format!("{} µs", e.eval_duration_us)
                                    } else {
                                        format!("{:.0} ms", dur_ms)
                                    };
                                    let (result_label, result_color) = if fired {
                                        ("requested", "var(--accent)")
                                    } else {
                                        ("not triggered", "var(--text-comment)")
                                    };
                                    let detail_text = match e.selected_partitions.as_ref() {
                                        Some(keys) if !keys.is_empty() => {
                                            let n = keys.len();
                                            format!("→ requested {n} partition{}", if n == 1 { "" } else { "s" })
                                        }
                                        _ if fired => "→ requested".to_string(),
                                        _ => "—".to_string(),
                                    };
                                    let is_initial_selected = idx == initial_selected;
                                    let row_cls = move || {
                                        if selected_idx.get() == idx {
                                            "grid-row grid-row--static grid-row--expanded"
                                        } else {
                                            "grid-row grid-row--static"
                                        }
                                    };
                                    view! {
                                        <div
                                            class=row_cls
                                            style=GRID
                                            title=ts_abs
                                            on:click=move |_| set_selected_idx.set(idx)
                                            node_ref={
                                                let node_ref = leptos::prelude::NodeRef::<leptos::html::Div>::new();
                                                if is_initial_selected {
                                                    #[cfg(feature = "hydrate")]
                                                    {
                                                        let nr = node_ref.clone();
                                                        leptos::prelude::Effect::new(move |_| {
                                                            if let Some(el) = nr.get() {
                                                                let el: &leptos::web_sys::Element = el.as_ref();
                                                                el.scroll_into_view();
                                                            }
                                                        });
                                                    }
                                                }
                                                node_ref
                                            }
                                        >
                                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px"><crate::now::RelTime ts=ts_now/></span>
                                            <span class="grid-cell-mono" style=format!("color:{result_color}; font-size:11.5px")>{result_label}</span>
                                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0">{detail_text}</span>
                                            <span class="grid-cell-mono" style="color:var(--text-comment); font-size:11.5px; text-align:right">{dur_label}</span>
                                        </div>
                                    }
                                }).collect::<Vec<_>>()}
                            </div>

                            <div class="section-header-row" style="margin-top:24px">
                                <span class="section-header-label">"EVALUATION DETAIL"</span>
                            </div>
                            {move || {
                                let idx = selected_idx.get();
                                if let Some(eval) = records_for_tree.get(idx) {
                                    let tree = eval.tree.clone();
                                    let fired = eval.fired;
                                    let run_ids = eval.run_ids.clone();
                                    let backfill_ids = eval.backfill_ids.clone();
                                    let (lns, lnm) = loc.get();
                                    view! {
                                        // Prefer the backfill chip over raw run chips: sub-runs are
                                        // an implementation detail of the backfill.
                                        {fired.then(move || {
                                            if !backfill_ids.is_empty() {
                                                let lns = lns.clone();
                                                let lnm = lnm.clone();
                                                Some(view! {
                                                    <div style="margin-bottom: 0.75rem">
                                                        <span class="detail-label">"Backfill: "</span>
                                                        {backfill_ids.into_iter().map(move |bid| {
                                                            let href = loc_path(&lns, &lnm, &format!("backfills/{}", bid));
                                                            let short = short_id(&bid, 10);
                                                            view! {
                                                                <A href={href}>
                                                                    <span class="item-row-chip item-row-chip--backfill">
                                                                        <span class="chip-backfill-prefix">"BF"</span>
                                                                        {short}
                                                                    </span>
                                                                </A>
                                                            }
                                                        }).collect::<Vec<_>>()}
                                                    </div>
                                                }.into_any())
                                            } else if !run_ids.is_empty() {
                                                let lns = lns.clone();
                                                let lnm = lnm.clone();
                                                Some(view! {
                                                    <div style="margin-bottom: 0.75rem">
                                                        <span class="detail-label">"Run: "</span>
                                                        {run_ids.into_iter().map(move |id| {
                                                            let href = loc_path(&lns, &lnm, &format!("runs/{}", id));
                                                            let short = short_id(&id, 8);
                                                            view! { <A href={href}><span class="item-row-chip">{short}</span></A> }
                                                        }).collect::<Vec<_>>()}
                                                    </div>
                                                }.into_any())
                                            } else {
                                                None
                                            }
                                        })}
                                        <crate::components::eval_tree::EvalTree tree=tree/>
                                    }.into_any()
                                } else {
                                    view! { <div class="empty-state">"Select an evaluation to view its decision tree."</div> }.into_any()
                                }
                            }}
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
