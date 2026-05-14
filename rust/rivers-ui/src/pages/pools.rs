//! Concurrency pool dashboard page.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::ui_kit::{Crumb, PoolUtilBar, StatTile, Topbar};
use crate::helpers::{format_seconds, short_id};
use crate::loc::{loc_path, use_current_location};
use crate::now::RelTime;
use crate::server_fns::pools::{get_all_pools, get_pool_detail, get_queued_runs};
use crate::types::{PoolDetail, RunRecord};

fn lease_time_class(lease_expires_at: i64, now: i64) -> &'static str {
    if lease_expires_at <= now {
        "lease-expired"
    } else if (lease_expires_at - now) < 30_000_000_000 {
        "lease-expiring"
    } else {
        ""
    }
}

fn format_elapsed(claimed_at: i64, now: i64) -> String {
    let secs = ((now.saturating_sub(claimed_at)).max(0) / 1_000_000_000) as u64;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn format_lease_remaining(lease_expires_at: i64, now: i64) -> String {
    let delta = lease_expires_at - now;
    if delta > 0 {
        format!("in {}", format_seconds(delta / 1_000_000_000))
    } else {
        format!("expired {} ago", format_seconds((-delta) / 1_000_000_000))
    }
}

#[component]
pub fn PoolsPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let (expanded_pool, set_expanded_pool) = signal(Option::<String>::None);
    let loc = use_current_location();

    let pools = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_all_pools(ns, name),
    );
    let queued = Resource::new(move || refresh_tick.get(), |_| get_queued_runs());

    let detail = Resource::new(
        move || (loc.get(), expanded_pool.get(), refresh_tick.get()),
        |((ns, name), pool_key, _)| async move {
            match pool_key {
                Some(key) => get_pool_detail(ns, name, key).await.ok(),
                None => None,
            }
        },
    );

    let live_status = use_live_kick(
        &["pools", "runs"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    view! {
        <Topbar crumbs=vec![Crumb::new("Pools")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <div class="page-header">
            <h1>"Pools"</h1>
            <p>"Concurrency slots across the cluster"</p>
        </div>

        <Transition>
            {move || {
                let list = pools.get().and_then(|r| r.ok()).unwrap_or_default();
                let n = list.len();
                let claimed: u32 = list.iter().map(|p| p.claimed_count).sum();
                let pending: u32 = list.iter().map(|p| p.pending_count).sum();
                let slot_sum: i32 = list.iter().filter(|p| p.slot_limit > 0).map(|p| p.slot_limit).sum();
                let has_unlimited = list.iter().any(|p| p.slot_limit < 0);
                let slots_suffix = if has_unlimited || slot_sum == 0 {
                    "claimed".to_string()
                } else {
                    format!("of {slot_sum}")
                };
                view! {
                    <div class="stats-grid" style="grid-template-columns:repeat(3, 1fr); margin-bottom:24px">
                        <StatTile label="POOLS" value=n.to_string() suffix="active"/>
                        <StatTile label="SLOTS CLAIMED" value=claimed.to_string() suffix=slots_suffix/>
                        <StatTile label="STEPS PENDING" value=pending.to_string() suffix="across all pools"/>
                    </div>
                }
            }}
        </Transition>

        <Transition fallback=move || view! { <TableSkeleton rows=4 cols=5/> }>
            {move || {
                pools.get().map(|result| match result {
                    Ok(pool_list) => {
                        if pool_list.is_empty() {
                            return view! { <div class="empty-state">"No concurrency pools configured."</div> }.into_any();
                        }

                        view! {
                            <div class="pool-list">
                                {pool_list.into_iter().map(|pool| {
                                    let pct = if pool.slot_limit > 0 {
                                        (pool.claimed_count as f64 / pool.slot_limit as f64) * 100.0
                                    } else {
                                        0.0
                                    };
                                    let lease_label = format_seconds(pool.lease_duration_secs as i64);
                                    let pending = pool.pending_count;
                                    let queued_pct = if pool.slot_limit > 0 {
                                        (pending as f64 / pool.slot_limit as f64) * 100.0
                                    } else {
                                        0.0
                                    };

                                    let queued_list = queued.get().and_then(|r| r.ok()).unwrap_or_default();
                                    let waiting: Vec<RunRecord> = queued_list
                                        .into_iter()
                                        .filter(|r| r.block_reason.as_deref().map(|s| s.contains(&pool.pool_key)).unwrap_or(false))
                                        .collect();

                                    view! {
                                        <PoolRow
                                            pool_key=pool.pool_key.clone()
                                            claimed=pool.claimed_count
                                            limit=pool.slot_limit
                                            pct=pct
                                            queued_pct=queued_pct
                                            lease_label=lease_label
                                            pending=pending
                                            waiting=waiting
                                            expanded_pool=expanded_pool
                                            set_expanded_pool=set_expanded_pool
                                            detail=detail
                                        />
                                    }
                                }).collect::<Vec<_>>()}
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error loading pools: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}

#[component]
fn PoolRow(
    pool_key: String,
    claimed: u32,
    limit: i32,
    pct: f64,
    queued_pct: f64,
    lease_label: String,
    pending: u32,
    waiting: Vec<RunRecord>,
    expanded_pool: ReadSignal<Option<String>>,
    set_expanded_pool: WriteSignal<Option<String>>,
    detail: Resource<Option<PoolDetail>>,
) -> impl IntoView {
    let pool_key_click = pool_key.clone();
    let pool_key_check = pool_key.clone();
    let pool_key_detail = StoredValue::new(pool_key.clone());

    let used = claimed;
    let free = if limit > 0 {
        (limit as u32).saturating_sub(used)
    } else {
        0
    };
    let util_color = if pct >= 90.0 {
        "var(--error)"
    } else if pct >= 70.0 {
        "var(--warning)"
    } else {
        "var(--success)"
    };

    let util_label = if limit < 0 {
        format!("{used}/∞")
    } else {
        format!("{used}/{limit}")
    };

    let is_expanded =
        Signal::derive(move || expanded_pool.get().as_deref() == Some(pool_key_check.as_str()));
    let waiting_count = waiting.len();
    let waiting_for_block = waiting.clone();

    view! {
        <div class="pool-row">
            <div class="pool-row-header">
                <div class="pool-row-left">
                    <div class="pool-row-name">{pool_key.clone()}</div>
                    <div class="pool-row-util" style=format!("color:{util_color}")>
                        {format!("{:.0}% · {util_label}", pct)}
                    </div>
                </div>

                <div class="pool-row-center">
                    <div class="pool-row-stats">
                        <span style="white-space:nowrap">
                            <span style=format!("color:{util_color}")>{used.to_string()}</span>
                            " used · "
                            <span style="color:var(--text-muted)">{free.to_string()}</span>
                            " free"
                            {(pending > 0).then(|| view! {
                                <>
                                    " · "
                                    <span style="color:var(--warning)">{pending.to_string()}</span>
                                    " queued"
                                </>
                            })}
                        </span>
                        <span style="color:var(--text-comment)">
                            {format!("lease {lease_label}")}
                        </span>
                    </div>
                    <div style="position:relative">
                        <PoolUtilBar used_pct=pct queued_pct=queued_pct/>
                        {(limit > 0 && limit <= 20).then(|| {
                            let slots = limit as usize;
                            let ticks: Vec<_> = (1..slots).map(|i| {
                                let x = (i as f64 / slots as f64) * 100.0;
                                view! {
                                    <span style=format!("position:absolute; top:0; left:{x:.1}%; width:1px; height:10px; background:rgba(0,0,0,0.25); pointer-events:none")></span>
                                }
                            }).collect();
                            view! { <span>{ticks}</span> }
                        })}
                    </div>
                    <div class="pool-row-legend">
                        <span><span class="pool-row-swatch" style=format!("background:{util_color}")></span>"used"</span>
                        <span><span class="pool-row-swatch pool-row-swatch--free"></span>"free"</span>
                        {(queued_pct > 0.0).then(|| view! {
                            <span><span class="pool-row-swatch pool-row-swatch--queued"></span>"queued"</span>
                        })}
                    </div>
                </div>

                <div class="pool-row-right">
                    {(pending > 0).then(|| view! {
                        <span class="pool-row-pill">
                            {format!("{pending} pending")}
                        </span>
                    })}
                    <button
                        class="btn btn-tertiary"
                        on:click=move |_| {
                            if expanded_pool.get_untracked().as_deref() == Some(pool_key_click.as_str()) {
                                set_expanded_pool.set(None);
                            } else {
                                set_expanded_pool.set(Some(pool_key_click.clone()));
                            }
                        }
                        style="justify-content:center"
                    >
                        {move || if is_expanded.get() { "Hide holders" } else { "Show holders" }}
                        <span
                            class="chev-btn"
                            class:chev-btn--open=move || is_expanded.get()
                            style="margin-left:6px"
                        >
                            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M3 2l3 3-3 3"/>
                            </svg>
                        </span>
                    </button>
                </div>
            </div>

            <Show when=move || is_expanded.get()>
                <div class="pool-row-expansion">
                    {(waiting_count > 0).then(|| {
                        let (lns, lnm) = use_current_location().get();
                        let rows: Vec<_> = waiting_for_block.iter().enumerate().map(|(i, r)| {
                            let run_href = loc_path(&lns, &lnm, &format!("runs/{}", r.run_id));
                            let short = short_id(&r.run_id, 8);
                            let start_ts = r.start_time;
                            let job = r.job_name.clone().unwrap_or_else(|| "—".into());
                            view! {
                                <div style="display:grid; grid-template-columns:32px 100px 1fr 100px; gap:12px; align-items:center; padding:6px 0">
                                    <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11px">{format!("#{}", i + 1)}</span>
                                    <A href=run_href attr:class="grid-cell-mono" attr:style="color:var(--secondary); font-size:11.5px">{short}</A>
                                    <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{job}</span>
                                    <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; text-align:right"><RelTime ts=start_ts/></span>
                                </div>
                            }
                        }).collect();
                        view! {
                            <div>
                                <div class="section-header-label" style="margin-bottom:6px">
                                    {format!("WAITING ON THIS POOL · {waiting_count}")}
                                </div>
                                <div style="display:flex; flex-direction:column; gap:0">{rows}</div>
                            </div>
                        }
                    })}

                    {move || {
                        match detail.get() {
                            Some(Some(d)) if pool_key_detail.with_value(|k| d.info.pool_key == *k) => {
                                view! { <HoldersTable detail=d/> }.into_any()
                            }
                            _ => view! { <div class="loading" style="margin-top:12px">"Loading holders..."</div> }.into_any(),
                        }
                    }}
                </div>
            </Show>
        </div>
    }
}

#[component]
fn HoldersTable(detail: PoolDetail) -> impl IntoView {
    if detail.holders.is_empty() {
        return view! {
            <div style="margin-top:12px">
                <div class="section-header-label" style="margin-bottom:6px">"SLOT HOLDERS"</div>
                <div class="empty-state">"No active slot holders."</div>
            </div>
        }
        .into_any();
    }

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    const GRID: &str = "grid-template-columns: 100px 2fr 60px 140px 140px";

    view! {
        <div style="margin-top:12px">
            <div class="section-header-label" style="margin-bottom:6px">
                {format!("SLOT HOLDERS · {}", detail.holders.len())}
            </div>
            <div class="grid-table">
                <div class="grid-table-head" style=GRID>
                    <span>"RUN"</span>
                    <span>"STEP"</span>
                    <span>"SLOTS"</span>
                    <span>"CLAIMED"</span>
                    <span>"LEASE EXPIRES"</span>
                </div>
                {let (lns, lnm) = use_current_location().get();
                detail.holders.into_iter().map(move |h| {
                    let run_href = loc_path(&lns, &lnm, &format!("runs/{}", h.run_id));
                    let short = short_id(&h.run_id, 8);
                    let claimed = format_elapsed(h.claimed_at, now);
                    let expires = format_lease_remaining(h.lease_expires_at, now);
                    let lease_class = lease_time_class(h.lease_expires_at, now);
                    let lease_style = match lease_class {
                        "lease-expired" => "color:var(--error); font-size:11.5px",
                        "lease-expiring" => "color:var(--warning); font-size:11.5px",
                        _ => "color:var(--text-muted); font-size:11.5px",
                    };
                    view! {
                        <div class="grid-row grid-row--plain" style=GRID>
                            <A href=run_href attr:class="grid-cell-mono" attr:style="color:var(--text); font-size:11.5px">{short}</A>
                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; min-width:0">{h.step_key}</span>
                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{h.slots_consumed.to_string()}</span>
                            <span class="grid-cell-mono" style="color:var(--text-muted); font-size:11.5px">{claimed}</span>
                            <span class="grid-cell-mono" style=lease_style>{expires}</span>
                        </div>
                    }
                }).collect::<Vec<_>>()
                }
            </div>
        </div>
    }.into_any()
}
