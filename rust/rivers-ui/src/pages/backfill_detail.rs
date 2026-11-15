//! Backfill detail page.

use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::components::icons::{IconRetry, IconStop};
use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::GridRowSkeleton;
use crate::components::pagination::PaginatedView;
use crate::components::ui_kit::{
    AssetSummaryRow, Crumb, EmptyState, OutlineOf, OutlineSource, PartitionOutline, ProgressBar,
    RunsGrid, SectionHeader, StatusChip, Topbar, meta_tile_fill,
};
use crate::helpers::{code_location_label, format_duration, format_timestamp, short_id};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::get_assets;
use crate::server_fns::backfills::{cancel_backfill, get_backfill, get_backfill_runs};
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::partitions::get_backfill_partition_overview;
use crate::types::PartitionOverview;

fn is_cancelable(status: &str) -> bool {
    matches!(status, "InProgress" | "Requested")
}

#[component]
pub fn BackfillDetailPage() -> impl IntoView {
    let params = use_params_map();
    // `backfill_id` is always untracked. The Resource explicitly tracks `params`.
    let backfill_id = move || params.read_untracked().get("id").unwrap_or_default();
    let loc = use_current_location();

    let (refresh_tick, set_refresh_tick) = signal(0u32);

    let backfill = Resource::new(
        move || {
            params.track();
            (backfill_id(), refresh_tick.get())
        },
        |(id, _)| get_backfill(id),
    );

    let all_assets = Resource::new(
        move || loc.get(),
        |(ns, name)| get_assets(ns, name, None, None, None),
    );
    let locations = Resource::new(|| (), |_| list_code_locations());

    // One page of the runs: a backfill can have one run per partition.
    let (runs_page, set_runs_page) = signal(0u64);
    let (runs_page_size, set_runs_page_size) = signal(50u64);
    let backfill_runs = Resource::new(
        move || {
            params.track();
            (
                backfill_id(),
                runs_page.get(),
                runs_page_size.get(),
                refresh_tick.get(),
            )
        },
        |(id, p, ps, _)| async move { get_backfill_runs(id, p * ps, ps).await },
    );

    // Made here, not in the view that waits for the record: the server and
    // the browser must make the resources in the same order. Once the reader
    // opens, filters or searches the outline, newer numbers wait: a new
    // outline would close the rows.
    let explored = RwSignal::new(false);
    let held_tick = StoredValue::new(0u32);
    let partitions = Resource::new(
        move || {
            params.track();
            let tick = if explored.get() {
                held_tick.get_value()
            } else {
                let tick = refresh_tick.get();
                held_tick.set_value(tick);
                tick
            };
            (backfill_id(), tick)
        },
        |(id, _)| get_backfill_partition_overview(id),
    );
    Effect::watch(
        move || params.track(),
        move |_, _, _| explored.set(false),
        false,
    );

    let cancel = Action::new(move |id: &String| {
        let id = id.clone();
        async move { cancel_backfill(id).await }
    });
    let cancel_pending = cancel.pending();
    let cancel_armed = crate::helpers::use_confirm_armed(move || params.track());
    let action_error = RwSignal::new(Option::<String>::None);
    Effect::new(move |_| {
        if let Some(Err(e)) = cancel.value().get() {
            action_error.set(Some(format!(
                "Cancel failed: {}",
                crate::helpers::err_text(&e)
            )));
        }
    });

    let rerun_pending = RwSignal::new(false);
    let rerun_armed = crate::helpers::use_confirm_armed(move || params.track());
    let navigate = leptos_router::hooks::use_navigate();

    let live = use_live_kick(
        &["backfills", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        {move || {
            params.track();
            let id = backfill_id();
            let (ns, name) = loc.get();
            let navigate = navigate.clone();
            view! {
                <Topbar crumbs=vec![
                    Crumb::linked("Backfills", loc_path(&ns, &name, "backfills")),
                    Crumb::new(short_id(&id, 8)).mono().copyable(id.clone()),
                ]>
                    <LiveStatusChip
                        status=live.status
                        on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
                    />
                    {move || action_error.get().map(|msg| view! { <span class="text-error">{msg}</span> })}
                    <Transition>
                        {move || backfill.get().and_then(|r| r.ok()).flatten().map(|record| {
                            let cancelable = is_cancelable(&record.status);
                            let cancel_id = record.backfill_id.clone();
                            let rerun_id = record.backfill_id.clone();
                            let rerun_verb = record.action.clone();
                            let navigate = navigate.clone();
                            view! {
                                    {cancelable.then(|| {
                                        let cancel_id = cancel_id.clone();
                                        view! {
                                            <button
                                                class="btn btn-danger"
                                                on:click=move |_| {
                                                    if cancel_armed.get() {
                                                        cancel_armed.set(false);
                                                        action_error.set(None);
                                                        cancel.dispatch(cancel_id.clone());
                                                    } else {
                                                        cancel_armed.set(true);
                                                    }
                                                }
                                                disabled=move || cancel_pending.get()
                                            >
                                                <IconStop/>
                                                {move || if cancel_pending.get() {
                                                    "Canceling…"
                                                } else if cancel_armed.get() {
                                                    "Confirm cancel?"
                                                } else {
                                                    "Cancel backfill"
                                                }}
                                            </button>
                                        }
                                    })}
                                    {(!cancelable).then(move || {
                                        let verb_text = rerun_verb.clone();
                                        view! {
                                            <button
                                                class="btn btn-primary"
                                                disabled=move || rerun_pending.get()
                                                on:click=move |_| {
                                                    let (dispatch, now_armed) = crate::helpers::replay_click(
                                                        rerun_verb.is_some(),
                                                        rerun_armed.get(),
                                                    );
                                                    rerun_armed.set(now_armed);
                                                    if !dispatch {
                                                        return;
                                                    }
                                                    let id = rerun_id.clone();
                                                    let navigate = navigate.clone();
                                                    let (ns, lname) = loc.get_untracked();
                                                    rerun_pending.set(true);
                                                    action_error.set(None);
                                                    leptos::task::spawn_local(async move {
                                                        let path_ns = ns.clone();
                                                        let path_name = lname.clone();
                                                        match crate::server_fns::mutations::rerun_backfill(ns, lname, id).await {
                                                            Ok(result) if !result.backfill_id.is_empty() => {
                                                                let path = loc_path(&path_ns, &path_name, &format!("backfills/{}", result.backfill_id));
                                                                navigate(&path, Default::default());
                                                            }
                                                            Ok(_) => {
                                                                action_error.set(Some("Re-execute returned no backfill id.".to_string()));
                                                                rerun_pending.set(false);
                                                            }
                                                            Err(e) => {
                                                                action_error.set(Some(format!("Re-execute failed: {}", crate::helpers::err_text(&e))));
                                                                rerun_pending.set(false);
                                                            }
                                                        }
                                                    });
                                                }
                                            >
                                                <IconRetry/>
                                                {move || crate::helpers::replay_button_text(
                                                    verb_text.as_deref(),
                                                    rerun_armed.get(),
                                                    rerun_pending.get(),
                                                )}
                                            </button>
                                        }
                                    })}
                            }
                        })}
                    </Transition>
                </Topbar>
            }
        }}
        <Transition fallback=move || view! { <div class="loading">"Loading…"</div> }>
            {move || {
                backfill.get().map(|result| match result {
                    Ok(Some(record)) => {
                        let duration = format_duration(Some(record.create_time), record.end_time);
                        let status_kind = crate::helpers::backfill_status_kind(&record.status);

                        let completed = record.completed_partitions;
                        let total = record.total_partitions;

                        let cl_id = record.code_location_id.clone();
                        let cl_label = {
                            let entries = locations.get().and_then(|r| r.ok()).unwrap_or_default();
                            code_location_label(&cl_id, &entries)
                        };
                        view! {
                            <div class="meta-tile-grid meta-tile-grid--4">
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Status"</div>
                                    <div class="meta-tile-value"><StatusChip kind=status_kind/></div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Strategy"</div>
                                    <div class="meta-tile-value" title=record.strategy_code.clone()>{record.strategy.clone()}</div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Launched by"</div>
                                    <div class="meta-tile-value">
                                        {
                                            let (_, _, label, sub) = crate::helpers::launched_by_display(&record.launched_by);
                                            match sub {
                                                Some(sub) => format!("{label} · {sub}"),
                                                None => label.to_string(),
                                            }
                                        }
                                    </div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Code location"</div>
                                    <div class="meta-tile-value" title=cl_id>{cl_label}</div>
                                </div>
                                // Without this the page is verb-blind, and
                                // "Re-execute" threads the record's verb.
                                {record.action.clone().map(|verb| view! {
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"Action"</div>
                                        <div class="meta-tile-value">{verb}</div>
                                    </div>
                                })}
                                {record.job_name.clone().map(|job| view! {
                                    <div class="meta-tile">
                                        <div class="meta-tile-label">"Job"</div>
                                        <div class="meta-tile-value">{job}</div>
                                    </div>
                                })}
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Max concurrency"</div>
                                    <div class="meta-tile-value">{crate::helpers::plural(record.max_concurrency as u64, "partition", "partitions")}</div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Created"</div>
                                    <div class="meta-tile-value" title=format_timestamp(Some(record.create_time))>
                                        <crate::now::RelTime ts=record.create_time/>
                                    </div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Ended"</div>
                                    <div class="meta-tile-value" title=format_timestamp(record.end_time)>
                                        <crate::now::RelTimeOpt ts=record.end_time fallback="—"/>
                                    </div>
                                </div>
                                <div class="meta-tile">
                                    <div class="meta-tile-label">"Duration"</div>
                                    <div class="meta-tile-value">{duration}</div>
                                </div>
                                {meta_tile_fill(8 + record.action.is_some() as usize + record.job_name.is_some() as usize, 4)}
                            </div>

                            <SectionHeader
                                label="PROGRESS"
                                count=format!("{completed} of {}", crate::helpers::plural(total, "partition", "partitions"))
                            />
                            <ProgressBar
                                value=Signal::derive(move || if total > 0 { completed as f64 / total as f64 } else { 0.0 })
                                color=crate::helpers::backfill_status_color(&record.status).to_string()
                                height_px=8
                            />

                        }.into_any()
                    }
                    Ok(None) => view! { <div class="error-msg">"Backfill not found"</div> }.into_any(),
                    Err(e) => view! { <div class="error-msg">{format!("Couldn't load backfill: {}", crate::helpers::err_text(&e))}</div> }.into_any(),
                })
            }}
        </Transition>

        // Outside the view of the record, which is drawn again on each live
        // update: the open rows of the outline stay open.
        <Transition>
            <Show when=move || matches!(backfill.get(), Some(Ok(Some(_))))>
                <SectionHeader label="PARTITIONS"/>
                {move || {
                    params.track();
                    view! {
                        <BackfillPartitions
                            backfill_id=backfill_id()
                            overview=partitions
                            explored=explored
                        />
                    }
                }}
            </Show>
        </Transition>

        <Transition>
            {move || {
                backfill.get().map(|result| match result {
                    Ok(Some(record)) => {
                        view! {
                            <SectionHeader label="ASSETS" count=record.asset_selection.len().to_string()/>
                            <div class="asset-summary-list">
                                {
                                    let asset_records = all_assets.get()
                                        .and_then(|r| r.ok())
                                        .unwrap_or_default();
                                    record.asset_selection.clone().into_iter().map(|key| {
                                        let asset = asset_records.iter().find(|a| a.asset_key == key).cloned();
                                        view! { <AssetSummaryRow asset_key=key asset=asset/> }
                                    }).collect::<Vec<_>>()
                                }
                            </div>

                            {(!record.tags.is_empty()).then(|| view! {
                                <SectionHeader label="TAGS"/>
                                <div class="tag-row">
                                    {record.tags.iter().map(|(k, v)| {
                                        view! { <span class="tag">{format!("{k}={v}")}</span> }
                                    }).collect::<Vec<_>>()}
                                </div>
                            })}

                            {record.error.clone().map(|e| view! {
                                <SectionHeader label="ERROR"/>
                                <pre class="error-msg error-msg--pre">{e}</pre>
                            })}

                            <SectionHeader label="RUNS" count=record.run_ids.len().to_string()/>
                            <PaginatedView
                                data=backfill_runs
                                page=runs_page
                                set_page=set_runs_page
                                page_size=runs_page_size
                                set_page_size=set_runs_page_size
                                fallback=move || view! { <GridRowSkeleton rows=3 cols=5/> }
                                empty=move || view! { <EmptyState message="No runs yet" compact=true/> }
                                render={move |runs: Vec<crate::types::RunRecord>| {
                                    view! { <RunsGrid rows=runs/> }.into_any()
                                }}
                            />
                        }.into_any()
                    }
                    Ok(None) | Err(_) => ().into_any(),
                })
            }}
        </Transition>
    }
}

/// The partitions of a backfill: tiles, a strip and an outline of the keys,
/// as on the Partitions tab of an asset.
#[component]
fn BackfillPartitions(
    backfill_id: String,
    overview: Resource<Result<PartitionOverview, ServerFnError>>,
    explored: RwSignal<bool>,
) -> impl IntoView {
    let loc = use_current_location();

    view! {
        <Transition fallback=move || view! { <div class="loading">"Loading partitions…"</div> }>
            {move || {
                overview.get().map(|result| match result {
                    Ok(overview) if overview.counts.total == 0 => {
                        view! { <EmptyState message="No partitions" compact=true/> }.into_any()
                    }
                    Ok(overview) => {
                        let (loc_ns, loc_name) = loc.get_untracked();
                        let source = OutlineSource {
                            loc_ns,
                            loc_name,
                            of: OutlineOf::Backfill(backfill_id.clone()),
                        };
                        view! {
                            <PartitionOutline source=source overview=overview explored=explored/>
                        }
                        .into_any()
                    }
                    Err(e) => view! {
                        <div class="error-msg">
                            {format!("Couldn't load partitions: {}", crate::helpers::err_text(&e))}
                        </div>
                    }
                    .into_any(),
                })
            }}
        </Transition>
    }
}
