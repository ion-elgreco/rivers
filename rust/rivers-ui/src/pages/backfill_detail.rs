//! Backfill detail page.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::pagination::PaginatedView;
use crate::components::ui_kit::{
    AssetSummaryRow, Crumb, HeatCell, PartitionHeatmap, StatusChip, Topbar,
};
use crate::helpers::{
    code_location_label, format_call_multiline, format_duration, format_timestamp, run_status_kind,
    short_id,
};
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::get_assets;
use crate::server_fns::backfills::{cancel_backfill, get_backfill, get_backfill_partitions};
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::runs::get_runs_by_ids;

fn backfill_status_label(status: &str) -> &'static str {
    match status {
        "Requested" => "Requested",
        "InProgress" => "In Progress",
        "CompletedSuccess" => "Completed",
        "CompletedFailed" => "Failed",
        "Canceled" => "Canceled",
        _ => "Unknown",
    }
}

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

    // Windowed real keys + per-partition status for the heatmap, paged so a
    // large backfill ships a bounded slice.
    let (part_page, set_part_page) = signal(0u64);
    let (part_page_size, set_part_page_size) = signal(1000u64);
    let partitions = Resource::new(
        move || {
            params.track();
            (
                backfill_id(),
                part_page.get(),
                part_page_size.get(),
                refresh_tick.get(),
            )
        },
        |(id, p, ps, _)| async move { get_backfill_partitions(id, p * ps, ps).await },
    );

    let all_assets = Resource::new(
        move || loc.get(),
        |(ns, name)| get_assets(ns, name, None, None, None),
    );
    let locations = Resource::new(|| (), |_| list_code_locations());

    let backfill_runs = Resource::new(
        move || {
            let bf = backfill.get().and_then(|r| r.ok()).flatten();
            let ids = bf.map(|b| b.run_ids.clone()).unwrap_or_default();
            (ids, refresh_tick.get())
        },
        |(ids, _)| async move {
            if ids.is_empty() {
                Ok(vec![])
            } else {
                get_runs_by_ids(ids).await
            }
        },
    );

    let cancel = Action::new(move |id: &String| {
        let id = id.clone();
        async move { cancel_backfill(id).await }
    });
    let cancel_pending = cancel.pending();

    let live_status = use_live_kick(
        &["backfills", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <Transition fallback=move || view! { <div class="loading">"Loading..."</div> }>
            {move || {
                backfill.get().map(|result| match result {
                    Ok(Some(record)) => {
                        let duration = format_duration(Some(record.create_time), record.end_time);
                        let status_label = backfill_status_label(&record.status);
                        let cancelable = is_cancelable(&record.status);
                        let cancel_id = record.backfill_id.clone();

                        let completed = record.completed_partitions;
                        let total = record.total_partitions;
                        let progress_pct = if total > 0 {
                            ((completed as f64 / total as f64) * 100.0) as u32
                        } else {
                            0
                        };

                        let short_bid = short_id(&record.backfill_id, 8);
                        let full_id = record.backfill_id.clone();
                        let rerun_id = record.backfill_id.clone();
                        let (ns_t, name_t) = loc.get();
                        let bf_href = loc_path(&ns_t, &name_t, "backfills");
                        let cl_id = record.code_location_id.clone();
                        let cl_label = {
                            let entries = locations.get().and_then(|r| r.ok()).unwrap_or_default();
                            code_location_label(&cl_id, &entries)
                        };
                        view! {
                            <Topbar crumbs=vec![
                                Crumb::linked("Backfills", bf_href),
                                Crumb::new(short_bid).mono().copyable(full_id),
                            ]>
                                <LiveStatusChip
                                    status=live_status
                                    on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
                                />
                                {cancelable.then(|| {
                                    let cancel_id = cancel_id.clone();
                                    view! {
                                        <button
                                            class="btn btn-danger"
                                            on:click=move |_| { cancel.dispatch(cancel_id.clone()); }
                                            disabled=move || cancel_pending.get()
                                        >
                                            {move || if cancel_pending.get() { "Canceling..." } else { "Cancel" }}
                                        </button>
                                    }
                                })}
                                {(!cancelable).then(move || {
                                    let (pending, set_pending) = signal(false);
                                    let navigate = leptos_router::hooks::use_navigate();
                                    view! {
                                        <button
                                            class="btn btn-primary"
                                            disabled=move || pending.get()
                                            on:click=move |_| {
                                                let id = rerun_id.clone();
                                                let navigate = navigate.clone();
                                                let (ns, lname) = loc.get();
                                                set_pending.set(true);
                                                leptos::task::spawn_local(async move {
                                                    let path_ns = ns.clone();
                                                    let path_name = lname.clone();
                                                    match crate::server_fns::actions::rerun_backfill(ns, lname, id).await {
                                                        Ok(result) if !result.backfill_id.is_empty() => {
                                                            let path = loc_path(&path_ns, &path_name, &format!("backfills/{}", result.backfill_id));
                                                            navigate(&path, Default::default());
                                                        }
                                                        _ => {
                                                            set_pending.set(false);
                                                        }
                                                    }
                                                });
                                            }
                                        >
                                            {move || if pending.get() { "Re-executing…" } else { "Re-execute" }}
                                        </button>
                                    }
                                })}
                            </Topbar>

                            <div class="backfill-meta-grid">
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Status"</div>
                                    <div class="backfill-meta-value">{status_label.to_string()}</div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Code location"</div>
                                    <div class="backfill-meta-value" title=cl_id>{cl_label}</div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Strategy"</div>
                                    <div class="backfill-meta-value grid-cell-code">{format_call_multiline(&record.strategy)}</div>
                                </div>
                                {record.job_name.clone().map(|job| view! {
                                    <div class="backfill-meta-tile">
                                        <div class="backfill-meta-label">"Job"</div>
                                        <div class="backfill-meta-value grid-cell-mono">{job}</div>
                                    </div>
                                })}
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Max concurrency"</div>
                                    <div class="backfill-meta-value">{format!("{} partitions", record.max_concurrency)}</div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Runs"</div>
                                    <div class="backfill-meta-value">{record.run_ids.len().to_string()}</div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Created"</div>
                                    <div class="backfill-meta-value"><crate::now::RelTime ts=record.create_time/></div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Ended"</div>
                                    <div class="backfill-meta-value">
                                        <crate::now::RelTimeOpt ts=record.end_time fallback="—"/>
                                    </div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Duration"</div>
                                    <div class="backfill-meta-value">{duration}</div>
                                </div>
                                <div class="backfill-meta-tile">
                                    <div class="backfill-meta-label">"Partitions"</div>
                                    <div class="backfill-meta-value">{format!("{completed} / {total}")}</div>
                                </div>
                            </div>

                            <div class="detail-grid">
                                <div class="detail-item" style="grid-column: 1 / -1">
                                    <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8px">
                                        <span class="detail-label">"PROGRESS"</span>
                                        <span class="font-mono" style="color:var(--text-muted); font-size:11.5px">{format!("{} of {} partitions", completed, total)}</span>
                                    </div>
                                    <div style="background: var(--bg-highest); border-radius: 4px; height: 10px; overflow: hidden;">
                                        <div style={format!("width: {}%; height: 100%; background: var(--success); border-radius: 4px; transition: width 0.3s;", progress_pct)}></div>
                                    </div>
                                </div>
                            </div>

                            {
                                let done = completed as usize;
                                let failed = record.failed_partitions as usize;
                                let canceled = record.canceled_partitions as usize;
                                let total_usize = total as usize;
                                let pending_count = total_usize.saturating_sub(done + failed + canceled);
                                let (scheme_label, cell_word) = if total_usize <= 24 {
                                    ("HOURLY", "hour")
                                } else if total_usize <= 96 {
                                    ("STATIC", "region")
                                } else {
                                    ("DAILY", "day")
                                };
                                let description = format!("{} cells · each cell = one {} partition · oldest top-left", total_usize, cell_word);
                                view! {
                                    <div class="partition-panel">
                                        <div class="partition-panel-header">
                                            <div>
                                                <div class="section-header-label">{format!("PARTITIONS · {}", scheme_label)}</div>
                                                <div class="partition-panel-desc">{description}</div>
                                            </div>
                                            <div class="partition-panel-legend">
                                                <span><span class="partition-legend-swatch partition-legend-swatch--done"></span>{format!("done · {}", done)}</span>
                                                {(failed > 0).then(|| view! {
                                                    <span><span class="partition-legend-swatch partition-legend-swatch--failed"></span>{format!("failed · {}", failed)}</span>
                                                })}
                                                {(canceled > 0).then(|| view! {
                                                    <span><span class="partition-legend-swatch partition-legend-swatch--pending"></span>{format!("canceled · {}", canceled)}</span>
                                                })}
                                                <span><span class="partition-legend-swatch partition-legend-swatch--pending"></span>{format!("pending · {}", pending_count)}</span>
                                            </div>
                                        </div>
                                        <PaginatedView
                                            data=partitions
                                            page=part_page
                                            set_page=set_part_page
                                            page_size=part_page_size
                                            set_page_size=set_part_page_size
                                            fallback=move || view! { <div class="loading">"Loading partitions…"</div> }
                                            render={move |rows: Vec<crate::types::BackfillPartitionCell>| {
                                                let cells: Vec<HeatCell> = rows.iter().map(|r| match r.status.as_str() {
                                                    "done" => HeatCell::Done,
                                                    "failed" => HeatCell::Failed,
                                                    "running" => HeatCell::Running,
                                                    "canceled" => HeatCell::Canceled,
                                                    _ => HeatCell::Pending,
                                                }).collect();
                                                let labels: Vec<String> = rows.into_iter().map(|r| r.key).collect();
                                                view! { <PartitionHeatmap cells=cells labels=labels/> }.into_any()
                                            }}
                                        />
                                    </div>
                                }
                            }

                            <div class="detail-grid">
                                <div class="detail-item" style="grid-column: 1 / -1">
                                    <div class="section-header-label" style="margin-bottom:10px">"ASSETS"</div>
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
                                </div>

                                {(!record.tags.is_empty()).then(|| view! {
                                    <div class="detail-item" style="grid-column: 1 / -1">
                                        <span class="detail-label">"Tags"</span>
                                        <div>
                                            {record.tags.iter().map(|(k, v)| {
                                                view! { <span class="tag">{format!("{k}={v}")}</span> }
                                            }).collect::<Vec<_>>()}
                                        </div>
                                    </div>
                                })}

                                {record.error.clone().map(|e| view! {
                                    <div class="detail-item" style="grid-column: 1 / -1">
                                        <div style="background:rgba(255,110,132,0.08); border-left:2px solid var(--error); border-radius:4px; padding:12px 16px; margin-top:6px">
                                            <div class="section-header-label" style="color:var(--error); margin-bottom:6px">"ERROR"</div>
                                            <pre style="font-family:'JetBrains Mono',monospace; font-size:12px; color:var(--error); margin:0; white-space:pre-wrap; word-break:break-word">{e}</pre>
                                        </div>
                                    </div>
                                })}
                            </div>

                            <Transition fallback=move || view! { <TableSkeleton rows=3 cols=5/> }>
                                {move || {
                                    backfill_runs.get().map(|result| match result {
                                        Ok(runs) if runs.is_empty() => {
                                            view! {
                                                <div>
                                                    <div class="section-header-label" style="margin-bottom:10px">"RUNS (0)"</div>
                                                    <div class="empty-state">"No runs yet."</div>
                                                </div>
                                            }.into_any()
                                        }
                                        Ok(mut runs) => {
                                            runs.sort_by(|a, b| b.start_time.cmp(&a.start_time));
                                            let n_runs = runs.len();
                                            view! {
                                                <div>
                                                    <div class="section-header-label" style="margin-bottom:10px">{format!("RUNS ({})", n_runs)}</div>
                                                    <div class="backfill-runs-list">
                                                        {runs.into_iter().map(|run| {
                                                            let run_id = run.run_id.clone();
                                                            let (ns, name) = loc.get();
                                                            let href = loc_path(&ns, &name, &format!("runs/{}", run_id));
                                                            let sid = short_id(&run_id, 8);
                                                            let st_kind = run_status_kind(&run.status).to_string();
                                                            let start_ts = run.start_time;
                                                            let created_abs = format_timestamp(Some(run.start_time));
                                                            let duration = format_duration(Some(run.start_time), run.end_time);
                                                            view! {
                                                                <A href={href} attr:class="backfill-runs-row">
                                                                    <span class="backfill-runs-id">{sid}</span>
                                                                    <StatusChip kind=st_kind small=true/>
                                                                    <span class="backfill-runs-trigger">"trigger: backfill"</span>
                                                                    <span class="backfill-runs-started" title={created_abs}><crate::now::RelTime ts=start_ts/></span>
                                                                    <span class="backfill-runs-duration">{duration}</span>
                                                                </A>
                                                            }
                                                        }).collect::<Vec<_>>()}
                                                    </div>
                                                </div>
                                            }.into_any()
                                        }
                                        Err(e) => view! { <div class="error-msg">{format!("Failed to load runs: {e}")}</div> }.into_any(),
                                    })
                                }}
                            </Transition>
                        }.into_any()
                    }
                    Ok(None) => view! { <div class="error-msg">"Backfill not found."</div> }.into_any(),
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
