//! Asset detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::icons::IconPlay;
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::materialize_dialog::MaterializeDialog;
use crate::components::pagination::PaginatedView;
use crate::components::traceback::TracebackView;
use crate::components::ui_kit::{
    Crumb, EmptyState, EventGlyphTimeline, GlyphEvent, RecentRunsStrip, RunLaunched, RunsGrid,
    SectionHeader, StatusChip, StripRun, TickRunChips, Topbar, UnderlineTabs, meta_tile_fill,
};
use crate::helpers::{
    JobPartitionPicker, format_timestamp, partition_picker_for_assets, short_id, use_query_param,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::{get_asset, get_asset_events, get_asset_events_page, get_assets};
use crate::server_fns::automation::{get_condition_evals, observe_asset};
use crate::server_fns::graph::get_graph_topology;
use crate::server_fns::mutations::{materialize_missing_partitions, trigger_materialize};
use crate::server_fns::overview::{get_assets_info, get_partition_status, get_resources_info};
use crate::server_fns::runs::{get_runs_for_asset, get_step_traceback};

/// Status-class vocabulary for this page's event list and glyph timeline.
/// ActionCompleted is "ok" to match the run page's success styling.
fn event_type_class(t: &crate::types::EventType) -> &'static str {
    use crate::types::EventType as E;
    match t {
        E::Materialization | E::StepSuccess | E::ActionCompleted => "ok",
        E::StepFailure => "err",
        E::Observation => "info",
        E::Deletion => "warn",
        _ => "muted",
    }
}

fn event_glyph(t: &crate::types::EventType) -> &'static str {
    use crate::types::EventType as E;
    match t {
        E::Materialization | E::StepSuccess => "◆",
        E::StepFailure => "▲",
        E::Observation => "○",
        E::ActionCompleted => "◇",
        E::Deletion => "✕",
        E::StepStart => "◐",
        _ => "•",
    }
}

/// A failure row's traceback, fetched the first time it is opened: this page
/// does not load the run's logs.
#[derive(Clone, Copy)]
struct EventTraceback {
    open: RwSignal<bool>,
    load: Action<(), Result<Option<crate::types::Traceback>, ServerFnError>>,
}

impl EventTraceback {
    /// For a `StepFailure` or `StepRetry` event, which ends an attempt.
    fn for_event(evt: &crate::types::StoredEvent) -> Option<Self> {
        use crate::types::EventType as E;
        let ended_attempt = matches!(evt.event_type, E::StepFailure | E::StepRetry)
            && evt.partition_key.is_none()
            && !evt.run_id.is_empty();
        let (run_id, step, at) = (
            evt.run_id.clone(),
            evt.asset_key.clone().unwrap_or_default(),
            evt.timestamp,
        );
        ended_attempt.then(|| Self {
            open: RwSignal::new(false),
            load: Action::new(move |_: &()| {
                let (run_id, step) = (run_id.clone(), step.clone());
                async move { get_step_traceback(run_id, step, at).await }
            }),
        })
    }

    fn toggle(self) -> impl IntoView {
        let Self { open, load } = self;
        view! {
            <button
                class="event-row-traceback-toggle"
                on:click=move |_| {
                    if load.value().get_untracked().is_none() && !load.pending().get_untracked() {
                        load.dispatch(());
                    }
                    open.update(|o| *o = !*o);
                }
            >
                {move || if open.get() { "Hide traceback" } else { "Traceback" }}
            </button>
        }
    }

    fn panel(self) -> impl IntoView {
        let Self { open, load } = self;
        move || {
            open.get().then(|| {
                let body = match load.value().get() {
                    None => view! { <span class="event-row-traceback-note">"Loading…"</span> }.into_any(),
                    Some(Ok(Some(traceback))) => view! { <TracebackView traceback/> }.into_any(),
                    Some(Ok(None)) => view! {
                        <span class="event-row-traceback-note">"No traceback was stored for this failure."</span>
                    }.into_any(),
                    Some(Err(e)) => view! {
                        <span class="error-msg">{format!("Couldn't load the traceback: {}", crate::helpers::err_text(&e))}</span>
                    }.into_any(),
                };
                view! { <div class="event-row-traceback">{body}</div> }
            })
        }
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
    // The materialize dialog's row decoration, from this page's live resource.
    let (records_by_key, records_failed) = crate::helpers::records_by_key(all_assets);

    let (active_tab, set_active_tab) = use_query_param("tab", "overview");
    let (event_filter, set_event_filter) = use_query_param("event_filter", "All");

    // Paginated events for the Events tab. Keyed on the type-filter pill so
    // `total` is the true per-filter count; page resets to 0 on change.
    let (ev_page, set_ev_page) = signal(0u64);
    let (ev_page_size, set_ev_page_size) = signal(25u64);
    let events_page = Resource::new(
        move || {
            params.track();
            (
                loc.get(),
                key(),
                event_filter.get(),
                ev_page.get(),
                ev_page_size.get(),
                refresh_tick.get(),
            )
        },
        |((ns, name), key, filter, p, ps, _)| async move {
            get_asset_events_page(ns, name, key, filter, p * ps, ps).await
        },
    );
    Effect::new(move |_| {
        event_filter.track();
        set_ev_page.set(0);
    });
    let (meta_expanded, set_meta_expanded) = signal(false);

    // Derive asset properties into signals (avoids reading Resource outside Transition)
    let is_external = RwSignal::new(false);
    let has_partitions = RwSignal::new(false);
    let asset_actions = RwSignal::new(Vec::<crate::types::AssetActionInfo>::new());
    // Externals without an observe fn expose no `observe` action — offering
    // the button anyway produced a guaranteed failure.
    let is_observable = RwSignal::new(false);
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
        asset_actions.set(
            info.as_ref()
                .map(|i| crate::helpers::offered_actions(i))
                .unwrap_or_default(),
        );
        is_observable.set(
            info.as_ref()
                .map(|i| i.actions.iter().any(|a| a.name == "observe"))
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
        let (ns, lname) = loc.get_untracked();
        async move { observe_asset(ns, lname, k).await }
    });
    let observe_pending = observe_action.pending();

    // Picker for this asset: drives dialog-vs-one-click below. Tracks params so
    // it follows navigation between assets.
    let assets_info_value = crate::helpers::resource_value(assets_info);
    let resources_info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_resources_info(ns, name).await },
    );
    let resources_info_value = crate::helpers::resource_value(resources_info);
    let launch_resources = Signal::derive(move || {
        resources_info_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
    });
    let materialize_picker = Signal::derive(move || {
        params.track();
        let current = key();
        let by_key: std::collections::HashMap<String, crate::types::AssetDefinitionInfo> =
            assets_info_value
                .get()
                .and_then(|r| r.ok())
                .unwrap_or_default()
                .into_iter()
                .map(|i| (i.asset_key.clone(), i))
                .collect();
        partition_picker_for_assets(&[current], &by_key)
    });
    let dialog_asset_keys = Signal::derive(move || {
        params.track();
        vec![key()]
    });
    let asset_info_by_key = Memo::new(move |_| {
        assets_info_value
            .get()
            .and_then(|r| r.ok())
            .unwrap_or_default()
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect::<std::collections::HashMap<String, crate::types::AssetDefinitionInfo>>()
    });
    let show_dialog = RwSignal::new(false);
    // One click materializes an unpartitioned asset — unless it takes config,
    // which only the dialog can edit.
    let materialize_opens_dialog = Signal::derive(move || {
        !matches!(materialize_picker.get(), JobPartitionPicker::None)
            || crate::components::config_editor::launch_takes_config(
                &[key()],
                &asset_info_by_key.get(),
                None,
            )
    });

    let materialize_action = Action::new(move |_: &()| {
        let k = key();
        let (ns, lname) = loc.get_untracked();
        async move { trigger_materialize(ns, lname, Some(vec![k]), None, None, None).await }
    });
    let materialize_pending = materialize_action.pending();

    // One dispatcher for every asset action button; the verb rides in the
    // action input. Partitioned assets go through the dialog instead, which
    // needs a key per run — `dialog_verb` tells it which verb to submit.
    let run_asset_action = Action::new(move |verb: &String| {
        let verb = verb.clone();
        let k = key();
        let (ns, lname) = loc.get_untracked();
        async move {
            crate::server_fns::mutations::trigger_action(
                ns,
                lname,
                verb,
                vec![k],
                None,
                None,
                false,
                None,
            )
            .await
        }
    });
    let action_pending = run_asset_action.pending();
    let dialog_verb = RwSignal::new(Option::<crate::types::AssetActionInfo>::None);
    let dialog_destructive = RwSignal::new(false);

    let (ns_t, name_t) = loc.get_untracked();
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
            {move || observe_action.value().get().map(|result| match result {
                // RunAction returns once the run is dispatched, not once the
                // verb has run — don't claim the observation already happened.
                Ok(_) => view! { <span class="text-success">"Observation requested"</span> }.into_any(),
                Err(e) => view! { <span class="text-error">{crate::helpers::err_text(&e)}</span> }.into_any(),
            })}
            {move || run_asset_action.value().get().map(|result| match result {
                Ok(run_id) => view! { <RunLaunched run_id/> }.into_any(),
                Err(e) => view! { <span class="text-error">{crate::helpers::err_text(&e)}</span> }.into_any(),
            })}
            {move || materialize_action.value().get().map(|result| match result {
                Ok(r) => {
                    let queued = r.status == "queued";
                    view! { <RunLaunched run_id=r.run_id queued=queued/> }.into_any()
                }
                Err(e) => view! { <span class="text-error">{crate::helpers::err_text(&e)}</span> }.into_any(),
            })}
            {move || {
                let partitioned = !matches!(materialize_picker.get(), JobPartitionPicker::None);
                crate::helpers::sorted_verbs(asset_actions.get()).into_iter().map(|act| {
                    let verb = act.name.clone();
                    // An Unmaterialize verb throws the asset's materialization
                    // state away — it never fires on a bare click, and the
                    // dialog it opens says what it does. A keyed verb on a
                    // partitioned asset needs the dialog's partition picker.
                    let destructive = act.is_destructive();
                    let one_click = !destructive
                        && (!partitioned || act.is_keyless())
                        && act.config_schema.is_none();
                    let label = if one_click {
                        crate::helpers::verb_label(&verb)
                    } else {
                        format!("{}…", crate::helpers::verb_label(&verb))
                    };
                    let title = crate::helpers::action_title(&act, false);
                    view! {
                        <button
                            class=if destructive { "btn btn-danger" } else { "btn" }
                            title=title
                            on:click=move |_| {
                                if one_click {
                                    run_asset_action.dispatch(verb.clone());
                                } else {
                                    dialog_verb.set(Some(act.clone()));
                                    dialog_destructive.set(destructive);
                                    show_dialog.set(true);
                                }
                            }
                            disabled=move || action_pending.get()
                        >
                            {label}
                        </button>
                    }
                }).collect_view()
            }}
            {move || {
                // External assets are read-only at this layer — we can only record
                // an observation, and only when they define an observe fn.
                // Non-external assets are materialized instead.
                if is_external.get() {
                    if !is_observable.get() {
                        return ().into_any();
                    }
                    view! {
                        <button
                            class="btn btn-primary"
                            on:click=move |_| { observe_action.dispatch(()); }
                            disabled=move || observe_pending.get()
                        >
                            <IconPlay/>
                            {move || if observe_pending.get() { "Observing…" } else { "Observe" }}
                        </button>
                    }.into_any()
                } else {
                    view! {
                        <button
                            class="btn btn-primary"
                            on:click=move |_| {
                                dialog_verb.set(None);
                                dialog_destructive.set(false);
                                if materialize_opens_dialog.get() {
                                    show_dialog.set(true);
                                } else {
                                    materialize_action.dispatch(());
                                }
                            }
                            disabled=move || materialize_pending.get()
                        >
                            <IconPlay/>
                            {move || if materialize_pending.get() {
                                "Materializing…"
                            } else if materialize_opens_dialog.get() {
                                "Materialize…"
                            } else {
                                "Materialize"
                            }}
                        </button>
                        // The one-click launch runs as defined; the dialog
                        // edits the launch document (metadata, resources,
                        // executor) for any asset.
                        <Show when=move || !materialize_opens_dialog.get()>
                            <button
                                class="btn"
                                title="Materialize with a launch document"
                                on:click=move |_| {
                                    dialog_verb.set(None);
                                    dialog_destructive.set(false);
                                    show_dialog.set(true);
                                }
                                disabled=move || materialize_pending.get()
                            >
                                "Materialize…"
                            </button>
                        </Show>
                    }.into_any()
                }
            }}
        </Topbar>


        // Wrapped in a Transition so the SSR-rendered count survives hydration
        // instead of flashing to 0 while the client resource re-resolves.
        <Transition>
            {move || {
                // `None` while the resource is pending — badge hides rather
                // than showing a misleading 0. Full count for the active filter
                // (from the paged total), not a windowed count.
                let event_count: Option<usize> =
                    events_page.get().and_then(|r| r.ok()).map(|p| p.total as usize);
                let mut tabs: Vec<(String, String, Option<usize>)> = vec![
                    ("overview".into(), "Overview".into(), None),
                    ("events".into(), "Events".into(), event_count),
                ];
                if has_partitions.get() {
                    tabs.push(("partitions".into(), "Partitions".into(), None));
                }
                tabs.push(("automation".into(), "Automation".into(), None));
                tabs.push(("lineage".into(), "Lineage".into(), None));
                let set_tab = set_active_tab.clone();
                let on_tab = Callback::new(move |v: String| set_tab(v));
                view! { <UnderlineTabs tabs=tabs active=active_tab on_select=on_tab/> }
            }}
        </Transition>

        <div class="tab-content" style=move || if active_tab.get() == "overview" { "" } else { "display:none" }>
            <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
                {move || {
                    let current_key = key();
                    let rec = match asset.get() {
                        Some(Err(e)) => return view! {
                            <div class="error-msg">{format!("Couldn't load asset: {}", crate::helpers::err_text(&e))}</div>
                        }.into_any(),
                        other => other.and_then(|r| r.ok()).flatten(),
                    };
                    let info = assets_info
                        .get()
                        .and_then(|r| r.ok())
                        .and_then(|infos| infos.into_iter().find(|a| a.asset_key == current_key));
                    let Some(record) = rec else {
                        return view! { <EmptyState message="Asset not found" compact=true/> }.into_any();
                    };

                    let kind_val = if record.kinds.is_empty() { "—".to_string() } else { record.kinds.join(", ") };
                    let group_val = record.asset_group.clone().unwrap_or_else(|| "—".to_string());
                    let last_ts = record.last_timestamp;
                    let last_ts_abs = format_timestamp(record.last_timestamp);
                    let status_kind = crate::helpers::stale_status_kind(&record.stale_status);
                    let last_label = if is_external.get() { "LAST OBSERVED" } else { "LAST MATERIALIZED" };
                    let partitioned_val = info
                        .as_ref()
                        .and_then(|i| i.partition_def.as_ref())
                        .map(|pd| format!(
                            "{} · {}",
                            crate::helpers::partition_kind_label(&pd.kind),
                            crate::helpers::plural(pd.total_count, "key", "keys"),
                        ))
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
                        <div class="meta-tile-grid meta-tile-grid--5">
                            <div class="meta-tile">
                                <div class="meta-tile-label">"STATUS"</div>
                                <div class="meta-tile-value"><StatusChip kind=status_kind/></div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">{last_label}</div>
                                <div class="meta-tile-value" title=last_ts_abs>
                                    <crate::now::RelTimeOpt ts=last_ts/>
                                </div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">"KIND"</div>
                                <div class="meta-tile-value">{kind_val}</div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">"GROUP"</div>
                                <div class="meta-tile-value">{group_val}</div>
                            </div>
                            <div class="meta-tile">
                                <div class="meta-tile-label">"PARTITIONED"</div>
                                <div class="meta-tile-value">{partitioned_val}</div>
                            </div>
                        </div>

                        <button
                            class="meta-toggle-btn"
                            aria-expanded=move || meta_expanded.get().to_string()
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
                                {meta_tile_fill(secondary_count, 3)}
                            </div>
                        </Show>

                        {automation_cond.map(|cond| view! {
                            <SectionHeader label="AUTOMATION CONDITION"/>
                            <pre class="automation-condition"><code>{cond}</code></pre>
                        })}
                    }.into_any()
                }}
            </Transition>

            {move || if is_external.get() {
                view! {
                    <SectionHeader label="RECENT OBSERVATIONS"/>
                    <Transition fallback=move || view! { <div class="loading">"Loading observations…"</div> }>
                        {move || {
                            events.get().map(|result| match result {
                                Ok(all_events) => {
                                    let observations: Vec<_> = all_events.into_iter()
                                        .filter(|e| matches!(e.event_type, crate::types::EventType::Observation))
                                        .take(10)
                                        .collect();
                                    if observations.is_empty() {
                                        return view! { <EmptyState message="No observations yet" compact=true/> }.into_any();
                                    }
                                    const GRID: &str = "grid-template-columns: 0.8fr 1.4fr 0.8fr";
                                    let (lns, lnm) = loc.get();
                                    view! {
                                        <div class="grid-table">
                                            <div class="grid-table-head" style=GRID>
                                                <span>"OBSERVED"</span>
                                                <span>"DATA VERSION"</span>
                                                <span>"RUN"</span>
                                            </div>
                                            {observations.into_iter().map(|evt| {
                                                let evt_ts = evt.timestamp;
                                                let ts_abs = format_timestamp(Some(evt.timestamp));
                                                let dv = evt.data_version.clone().unwrap_or_else(|| "—".to_string());
                                                let run = (!evt.run_id.is_empty()).then(|| {
                                                    let href = loc_path(&lns, &lnm, &format!("runs/{}", evt.run_id));
                                                    view! { <A href=href attr:class="grid-cell-mono">{short_id(&evt.run_id, 8)}</A> }
                                                });
                                                view! {
                                                    <div class="grid-row grid-row--plain" style=GRID>
                                                        <span class="grid-row-rail grid-row-rail--muted"></span>
                                                        <span class="grid-cell-muted" title=ts_abs><crate::now::RelTime ts=evt_ts/></span>
                                                        <span class="grid-cell-mono grid-cell-truncate" title=dv.clone()>{dv.clone()}</span>
                                                        {run.map(|r| r.into_any()).unwrap_or_else(|| view! { <span class="grid-cell-muted">"—"</span> }.into_any())}
                                                    </div>
                                                }
                                            }).collect::<Vec<_>>()}
                                        </div>
                                    }.into_any()
                                }
                                Err(e) => view! { <div class="error-msg">{format!("Couldn't load observations: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                            })
                        }}
                    </Transition>
                }.into_any()
            } else {
                view! {
                    <SectionHeader label="RECENT RUNS"/>
                    <Transition fallback=move || view! { <div class="loading">"Loading runs…"</div> }>
                        {move || {
                            asset_runs.get().map(|result| match result {
                                Ok(runs) => {
                                    if runs.is_empty() {
                                        return view! { <EmptyState message="No runs yet" compact=true/> }.into_any();
                                    }
                                    let strip = StripRun::from_runs(&runs, runs.len());
                                    view! {
                                        <RecentRunsStrip runs=strip/>
                                        <div class="section-gap"></div>
                                        <RunsGrid rows=runs/>
                                    }.into_any()
                                }
                                Err(e) => view! { <div class="error-msg">{format!("Couldn't load runs: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                            })
                        }}
                    </Transition>
                }.into_any()
            }}
        </div>

        <div class="tab-content" style=move || if active_tab.get() == "events" { "" } else { "display:none" }>
            // Show only TERMINAL events (materializations / failures / observations) —
            // intermediate events like StepStart or slot-claim/renew are
            // noise at this zoom.
            <Transition>
                {move || {
                    events.get().and_then(|r| r.ok()).map(|evts| {
                        let now_ns = jiff::Timestamp::now().as_nanosecond() as i64;
                        let glyph_events: Vec<GlyphEvent> = evts
                            .iter()
                            .filter_map(|e| {
                                use crate::types::EventType as E;
                                if !matches!(
                                    e.event_type,
                                    E::Materialization
                                        | E::StepFailure
                                        | E::Observation
                                        | E::ActionCompleted
                                        | E::Deletion
                                ) {
                                    return None;
                                }
                                let (glyph, status) =
                                    (event_glyph(&e.event_type), event_type_class(&e.event_type));
                                let minutes_ago = ((now_ns - e.timestamp) as f64) / 60_000_000_000.0;
                                Some(GlyphEvent {
                                    minutes_ago: minutes_ago.clamp(0.0, 180.0),
                                    glyph,
                                    status,
                                    run: Some(e.run_id.clone()),
                                    label: format!("{} · {}", e.event_type.label(), e.run_id),
                                })
                            })
                            .take(80)
                            .collect();
                        view! { <EventGlyphTimeline events=glyph_events/> }
                    })
                }}
            </Transition>
            <div class="asset-event-filters">
                <div class="filter-pill-group">
                    {[("All", "All events"), ("mat", "Materializations"), ("fail", "Failures and retries")]
                        .into_iter()
                        .map(|(id, label)| {
                            let set_filter = set_event_filter.clone();
                            view! {
                                <button
                                    class=move || if event_filter.get() == id { "filter-pill filter-pill--active" } else { "filter-pill" }
                                    aria-pressed=move || (event_filter.get() == id).to_string()
                                    on:click=move |_| set_filter(id.to_string())
                                >{label}</button>
                            }
                        })
                        .collect::<Vec<_>>()}
                </div>
            </div>
            <PaginatedView
                data=events_page
                page=ev_page
                set_page=set_ev_page
                page_size=ev_page_size
                set_page_size=set_ev_page_size
                fallback=move || view! { <div class="loading">"Loading events…"</div> }
                empty=move || view! { <EmptyState message="No events for this filter" compact=true/> }
                render={move |rows: Vec<crate::types::StoredEvent>| {
                                view! {
                                    <div class="events-list">
                                        {rows.into_iter().map(|evt| {
                                            let evt_ts = evt.timestamp;
                                            let time_abs = crate::helpers::nanos_to_datetime(evt.timestamp)
                                                .map(|d| d.strftime("%Y-%m-%d %H:%M:%S").to_string())
                                                .unwrap_or_default();
                                            let type_label = evt.event_type.label();
                                            let type_cls = event_type_class(&evt.event_type);
                                            let glyph = event_glyph(&evt.event_type);
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
                                            let traceback = EventTraceback::for_event(&evt);
                                            view! {
                                                <div class=format!("event-row event-row--{type_cls}") title={time_abs}>
                                                    <span class=format!("event-row-glyph event-row-glyph--{type_cls}")>{glyph}</span>
                                                    <span class=format!("event-row-type event-row-type--{type_cls}")>{type_label}</span>
                                                    <span class="event-row-time"><crate::now::RelTime ts=evt_ts/></span>
                                                    <span class="event-row-msg">{message}</span>
                                                    <span class="event-row-actions">
                                                        {traceback.map(EventTraceback::toggle)}
                                                        {match run_href {
                                                            Some(href) => view! { <A href=href attr:class="event-row-run">{run_short}</A> }.into_any(),
                                                            None => view! { <span class="event-row-run event-row-run--none">{run_short}</span> }.into_any(),
                                                        }}
                                                    </span>
                                                    {traceback.map(EventTraceback::panel)}
                                                </div>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                }.into_any()
                }}
            />
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
            <Transition fallback=move || view! { <div class="loading">"Loading lineage…"</div> }>
                {move || {
                    let current_key = key();
                    let topo = graph.get().and_then(|r| r.ok());
                    let records_by_key = records_by_key.get();

                    let (upstream, downstream): (Vec<String>, Vec<String>) = topo
                        .map(|t| (t.direct_upstream(&current_key), t.direct_downstream(&current_key)))
                        .unwrap_or_default();

                    let (lns, lnm) = loc.get();
                    let render_col = |label: &'static str, keys: Vec<String>, empty_msg: &'static str, records: &std::collections::HashMap<String, crate::types::AssetRecord>| {
                        let count = keys.len();
                        if keys.is_empty() {
                            view! {
                                <div>
                                    <SectionHeader label=label count=count.to_string()/>
                                    <EmptyState message=empty_msg compact=true/>
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
                                    <SectionHeader label=label count=count.to_string()/>
                                    <div class="lineage-rows">{rows}</div>
                                </div>
                            }.into_any()
                        }
                    };

                    view! {
                        <div class="lineage-grid">
                            {render_col("UPSTREAM", upstream, "No upstream assets (this is a source asset)", &records_by_key)}
                            {render_col("DOWNSTREAM", downstream, "No downstream assets", &records_by_key)}
                        </div>
                    }.into_any()
                }}
            </Transition>
        </div>

        <MaterializeDialog
            show=show_dialog
            asset_keys=dialog_asset_keys
            picker=materialize_picker
            action=dialog_verb
            destructive=dialog_destructive
            records=records_by_key
            records_failed=records_failed
            definitions=asset_info_by_key
            resources=launch_resources
        />
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

    // Launches a backfill over the asset's unmaterialized partitions; the server
    // resolves which are missing.
    let materialize_missing = Action::new(move |_: &()| {
        let k = mat_key.clone();
        let (ns, name) = loc.get_untracked();
        async move { materialize_missing_partitions(ns, name, k).await }
    });
    let mat_pending = materialize_missing.pending();

    // On success, jump to the new backfill.
    let (mm_nav, set_mm_nav) = signal(Option::<String>::None);
    Effect::new(move |_| {
        if let Some(Ok(res)) = materialize_missing.value().get()
            && !res.backfill_id.is_empty()
        {
            let (ns, name) = loc.get_untracked();
            set_mm_nav.set(Some(loc_path(
                &ns,
                &name,
                &format!("backfills/{}", res.backfill_id),
            )));
        }
    });

    view! {
        <Transition fallback=move || view! { <div class="loading">"Loading partitions…"</div> }>
            {move || {
                partition_status.get().map(|result| match result {
                    Ok(status) => {
                        if status.partition_details.is_empty() {
                            return view! { <EmptyState message="No partitions yet" compact=true/> }.into_any();
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
                                            <span class="stat-inline">
                                                {format!("showing {}–{} of {}", off + 1, off + shown, total_n)}
                                            </span>
                                            <button
                                                class="btn btn-small"
                                                disabled={move || offset.get() == 0}
                                                on:click=move |_| offset.update(|o| *o = o.saturating_sub(PAGE))
                                            >
                                                "Prev"
                                            </button>
                                            <button
                                                class="btn btn-small"
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
                                        <IconPlay/>
                                        {move || if mat_pending.get() { "Materializing…" } else { "Materialize missing" }}
                                    </button>
                                })}
                                {move || match materialize_missing.value().get() {
                                    Some(Err(e)) => Some(view! {
                                        <span class="text-error">{crate::helpers::err_text(&e)}</span>
                                    }),
                                    _ => None,
                                }}
                            </div>
                            <PartitionHeatmap cells=cells labels=heatmap_labels legend=true freshness_gradient=true/>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load partitions: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        {move || mm_nav.get().map(|path| view! {
            <leptos_router::components::Redirect path={path}/>
        })}
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
        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                let current_key = info_key.clone();
                assets_info.get().map(|result| match result {
                    Ok(infos) => {
                        if let Some(info) = infos.into_iter().find(|a| a.asset_key == current_key) {
                            if let Some(cond) = info.automation_condition {
                                view! {
                                    <SectionHeader label="AUTOMATION CONDITION"/>
                                    <pre class="automation-condition"><code>{cond}</code></pre>
                                }.into_any()
                            } else {
                                view! { <EmptyState message="No automation condition" compact=true/> }.into_any()
                            }
                        } else {
                            view! { <EmptyState message="Asset definition not found" compact=true/> }.into_any()
                        }
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load asset definition: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        <Transition fallback=move || view! { <div class="loading">"Loading evaluations…"</div> }>
            {move || {
                evals.get().map(|result| match result {
                    Ok(records) => {
                        if records.is_empty() {
                            return view! { <EmptyState message="No evaluations yet" hint="The daemon stores an evaluation every tick" compact=true/> }.into_any();
                        }
                        let now = jiff::Timestamp::now().as_nanosecond() as i64;
                        let window_ns: i64 = 60 * 60 * 1_000_000_000;
                        let bucket_ns = window_ns / 60;
                        let mut buckets: Vec<(u32, u32)> = vec![(0, 0); 60];
                        for r in records.iter() {
                            let age = now.saturating_sub(r.timestamp);
                            if age >= 0 && age < window_ns {
                                let idx = (59 - (age / bucket_ns).min(59)) as usize;
                                buckets[idx].0 += 1;
                                if r.fired {
                                    buckets[idx].1 += 1;
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

                            <SectionHeader label="RECENT TICKS" count=format!("last {}", records.len())/>
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
                                                        let nr = node_ref;
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
                                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm)"><crate::now::RelTime ts=ts_now/></span>
                                            <span class="grid-cell-mono" style=format!("color:{result_color}; font-size:var(--fs-sm)")>{result_label}</span>
                                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:var(--fs-sm); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0">{detail_text}</span>
                                            <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm); text-align:right">{dur_label}</span>
                                        </div>
                                    }
                                }).collect::<Vec<_>>()}
                            </div>

                            <SectionHeader label="EVALUATION DETAIL"/>
                            {move || {
                                let idx = selected_idx.get();
                                if let Some(eval) = records_for_tree.get(idx) {
                                    let tree = eval.tree.clone();
                                    let fired = eval.fired;
                                    let run_ids = eval.run_ids.clone();
                                    let backfill_ids = eval.backfill_ids.clone();
                                    view! {
                                        // TickRunChips prefers the backfill chip over raw run chips:
                                        // sub-runs are an implementation detail of the backfill.
                                        {fired.then(move || view! {
                                            <div class="eval-links">
                                                <TickRunChips run_ids=run_ids backfill_ids=backfill_ids/>
                                            </div>
                                        })}
                                        <crate::components::eval_tree::EvalTree tree=tree/>
                                    }.into_any()
                                } else {
                                    view! { <EmptyState message="Select an evaluation to see its decision tree" compact=true/> }.into_any()
                                }
                            }}
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load evaluations: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
