use leptos::prelude::*;
use leptos_router::components::A;

use super::badges::StatusChip;
use super::cards::{EmptyState, SectionHeader};

/// ConditionReplay — real per-sub-condition history for a single asset over
/// the last N minutes. Fetches recent `ConditionEvalRecord`s, walks the
/// evaluation tree, and plots each sub-node's status per time bucket. Click a
/// cell to inspect what blocked (or caused) a fire at that moment.
#[component]
pub fn ConditionReplay(
    #[prop(into)] asset_key: String,
    #[prop(optional, default = 60)] minutes: u32,
) -> impl IntoView {
    use crate::loc::use_current_location;
    use crate::server_fns::automation::get_condition_evals;
    use crate::types::NodeStatus;

    let key = asset_key.clone();
    let loc = use_current_location();
    let evals = Resource::new(
        move || (loc.get(), key.clone()),
        |((ns, name), k)| get_condition_evals(ns, name, k, Some(200)),
    );

    let (cursor, set_cursor) = signal(Option::<usize>::None);

    // Bucket indices are computed relative to `now_ns` at render time; when
    // the resource refreshes, the same index would point to a different
    // moment. Reset the cursor on refresh to avoid silently drifting state.
    Effect::new(move |prev: Option<()>| {
        evals.get();
        if prev.is_some() {
            set_cursor.set(None);
        }
    });

    view! {
        <div class="cond-replay">
            <Transition fallback=move || view! {
                <div class="cond-replay-empty">"Loading evaluation history…"</div>
            }>
                {move || {
                    let records = evals.get().and_then(|r| r.ok()).unwrap_or_default();

                    let now_ns = jiff::Timestamp::now().as_nanosecond() as i64;
                    let window_ns = minutes as i64 * 60 * 1_000_000_000;
                    let mut recent: Vec<_> = records
                        .into_iter()
                        .filter(|e| now_ns.saturating_sub(e.timestamp) < window_ns)
                        .collect();
                    recent.sort_by_key(|e| e.timestamp);

                    if recent.is_empty() {
                        return view! {
                            <>
                                <div class="cond-replay-head">
                                    <span class="section-header-label">{format!("CONDITION REPLAY · LAST {minutes} MIN")}</span>
                                </div>
                                <div class="cond-replay-empty">{format!("No evaluations in the last {minutes} minutes.")}</div>
                            </>
                        }.into_any();
                    }

                    // Flatten the latest eval's tree into a depth-first list of sub-nodes.
                    let latest = recent.last().unwrap();
                    let mut sub_nodes: Vec<(u32, String, String, usize)> = Vec::new();
                    fn walk(
                        node: &crate::types::EvalNodeResult,
                        depth: usize,
                        out: &mut Vec<(u32, String, String, usize)>,
                    ) {
                        out.push((node.node_idx, node.label.clone(), node.node_type.clone(), depth));
                        for c in &node.children {
                            walk(c, depth + 1, out);
                        }
                    }
                    walk(&latest.tree, 0, &mut sub_nodes);

                    // Bucket evals into N minute-width slots, keeping only the latest per bucket.
                    let n_buckets = minutes as usize;
                    let bucket_ns = (window_ns / n_buckets as i64).max(1);
                    let mut bucket_status: Vec<Option<std::collections::HashMap<u32, NodeStatus>>> =
                        vec![None; n_buckets];
                    let mut bucket_trees: Vec<Option<crate::types::EvalNodeResult>> =
                        vec![None; n_buckets];
                    let mut bucket_times: Vec<Option<i64>> = vec![None; n_buckets];
                    for eval in &recent {
                        let age = now_ns.saturating_sub(eval.timestamp);
                        if age < 0 || age >= window_ns { continue; }
                        let slot = (n_buckets - 1)
                            .saturating_sub((age / bucket_ns) as usize)
                            .min(n_buckets - 1);
                        if let Some(existing_ts) = bucket_times[slot]
                            && eval.timestamp <= existing_ts { continue; }
                        let mut map = std::collections::HashMap::new();
                        fn collect_statuses(
                            node: &crate::types::EvalNodeResult,
                            map: &mut std::collections::HashMap<u32, NodeStatus>,
                        ) {
                            map.insert(node.node_idx, node.status.clone());
                            for c in &node.children {
                                collect_statuses(c, map);
                            }
                        }
                        collect_statuses(&eval.tree, &mut map);
                        bucket_status[slot] = Some(map);
                        bucket_trees[slot] = Some(eval.tree.clone());
                        bucket_times[slot] = Some(eval.timestamp);
                    }

                    // Build a per-sub-node track.
                    let bucket_status_for_tracks = bucket_status.clone();
                    let tracks: Vec<_> = sub_nodes.iter().map(|(idx, label, node_type, depth)| {
                        let idx = *idx;
                        let is_op = matches!(node_type.as_str(), "And" | "Or" | "Not");
                        let bucket_status_cells = bucket_status_for_tracks.clone();
                        let cells: Vec<_> = (0..n_buckets).map(|b| {
                            let (base_cls, title_verb) = match bucket_status_cells[b].as_ref().and_then(|m| m.get(&idx)) {
                                Some(NodeStatus::True) => ("cond-cell cond-cell--true", "true"),
                                Some(NodeStatus::False) => ("cond-cell cond-cell--false", "false"),
                                Some(NodeStatus::Skipped) => ("cond-cell cond-cell--skipped", "skipped"),
                                None => ("cond-cell cond-cell--missing", "no tick"),
                            };
                            let bucket_age = (n_buckets - 1 - b) as u32;
                            let title_str = format!("{title_verb} · {bucket_age}m ago");
                            let cls_reactive = move || {
                                if cursor.get() == Some(b) {
                                    format!("{base_cls} cond-cell--cursor")
                                } else {
                                    base_cls.to_string()
                                }
                            };
                            view! {
                                <span
                                    class=cls_reactive
                                    title=title_str
                                    on:click=move |_| {
                                        if cursor.get() == Some(b) {
                                            set_cursor.set(None);
                                        } else {
                                            set_cursor.set(Some(b));
                                        }
                                    }
                                ></span>
                            }
                        }).collect();

                        let indent = *depth * 12;
                        let label_class = if is_op { "cond-track-label cond-track-label--op" } else { "cond-track-label" };
                        let display_label = if is_op {
                            format!("{} {}", node_type.to_ascii_lowercase(), label)
                        } else {
                            label.clone()
                        };
                        view! {
                            <div class="cond-track">
                                <span
                                    class=label_class
                                    style=format!("padding-left:{indent}px")
                                    title=label.clone()
                                >{display_label}</span>
                                <div class="cond-track-cells">{cells}</div>
                            </div>
                        }
                    }).collect();

                    // Cursor detail: shows what happened at the selected minute.
                    // Tree-aware: walks from root, expecting True (fire). When a
                    // node's status differs from expected, recurse into the
                    // sub-tree that caused the mismatch, inverting expectation
                    // through NOT nodes. Leaves reached this way are the real
                    // blockers — a False leaf under a NOT is *not* a blocker.
                    fn fault_leaves(
                        node: &crate::types::EvalNodeResult,
                        expected: NodeStatus,
                        out: &mut Vec<String>,
                    ) {
                        // Skipped means short-circuited — don't count as fault
                        if matches!(node.status, NodeStatus::Skipped) { return; }
                        if node.status == expected { return; }
                        match node.node_type.as_str() {
                            "And" => {
                                // Expected True, got False → at least one child is False.
                                // Recurse into children still expecting True.
                                for c in &node.children {
                                    fault_leaves(c, NodeStatus::True, out);
                                }
                            }
                            "Or" => {
                                // Expected True, got False → all children False.
                                for c in &node.children {
                                    fault_leaves(c, NodeStatus::True, out);
                                }
                            }
                            "Not" => {
                                // Expected True, got False → child is True but we needed False.
                                // Recurse with inverted expectation.
                                if let Some(c) = node.children.first() {
                                    fault_leaves(c, NodeStatus::False, out);
                                }
                            }
                            _ => {
                                // Leaf didn't match — it's a responsible node.
                                out.push(node.label.clone());
                            }
                        }
                    }

                    let bucket_trees_for_detail = bucket_trees;
                    let bucket_times_for_detail = bucket_times;
                    let cursor_detail = move || {
                        let Some(b) = cursor.get() else { return ().into_any(); };
                        let Some(tree) = bucket_trees_for_detail.get(b).and_then(|x| x.as_ref()) else {
                            return ().into_any();
                        };
                        let Some(ts) = bucket_times_for_detail.get(b).copied().flatten() else {
                            return ().into_any();
                        };
                        let age_min = (now_ns.saturating_sub(ts) / 60_000_000_000) as u32;
                        let age_label = if age_min == 0 { "just now".to_string() } else { format!("{age_min}m ago") };

                        let root_status = tree.status.clone();
                        let (tag_cls, tag_label) = match root_status {
                            NodeStatus::True => ("cond-verdict cond-verdict--fire", "FIRED"),
                            NodeStatus::False => ("cond-verdict cond-verdict--block", "SUPPRESSED"),
                            NodeStatus::Skipped => ("cond-verdict cond-verdict--skip", "SKIPPED"),
                        };

                        // Only compute blockers when the root was suppressed.
                        let blockers = if matches!(root_status, NodeStatus::False) {
                            let mut v = Vec::new();
                            fault_leaves(tree, NodeStatus::True, &mut v);
                            v.dedup();
                            v
                        } else {
                            Vec::new()
                        };

                        view! {
                            <div class="cond-cursor-detail">
                                <span class=tag_cls>{tag_label}</span>
                                <span class="cond-cursor-time">"at " <b>{age_label}</b></span>
                                {(!blockers.is_empty()).then(|| view! {
                                    <>
                                        <span class="cond-cursor-sep">"·"</span>
                                        <span class="cond-cursor-blockers">
                                            "blockers: "
                                            <span class="cond-cursor-blocker-list">{blockers.join(", ")}</span>
                                        </span>
                                    </>
                                })}
                            </div>
                        }.into_any()
                    };

                    view! {
                        <>
                            <div class="cond-replay-head">
                                <span class="section-header-label">{format!("CONDITION REPLAY · LAST {minutes} MIN")}</span>
                                <span class="cond-replay-hint">
                                    {crate::helpers::plural(recent.len() as u64, "tick", "ticks")} " · click a cell to inspect"
                                </span>
                            </div>
                            <div class="cond-tracks">{tracks}</div>
                            <div class="cond-axis">
                                <span>{format!("−{minutes}m")}</span>
                                <span>{format!("−{}m", minutes * 3 / 4)}</span>
                                <span>{format!("−{}m", minutes / 2)}</span>
                                <span>{format!("−{}m", minutes / 4)}</span>
                                <span>"now"</span>
                            </div>
                            {cursor_detail}
                            <div class="cond-legend">
                                <span><span class="cond-swatch cond-swatch--true"></span> "true"</span>
                                <span><span class="cond-swatch cond-swatch--false"></span> "false"</span>
                                <span><span class="cond-swatch cond-swatch--skipped"></span> "skipped"</span>
                                <span><span class="cond-swatch cond-swatch--missing"></span> "no tick"</span>
                            </div>
                        </>
                    }.into_any()
                }}
            </Transition>
        </div>
    }
}

/// Evaluation timeline — bucketed bar chart showing when the automation
/// engine evaluated conditions (bar height = eval count per bucket) and when
/// those evals resulted in a materialization request (accent-colored bars).
#[component]
pub fn EvalTimelineBars(
    /// (eval_count, fire_count) buckets ordered oldest → newest.
    #[prop(into)]
    buckets: Vec<(u32, u32)>,
) -> impl IntoView {
    let total_ticks: u64 = buckets.iter().map(|(c, _)| *c as u64).sum();
    let fire_count: u64 = buckets.iter().map(|(_, f)| *f as u64).sum();
    let n = buckets.len().max(1);
    let mins_per_bucket = 60.0 / n as f64;
    let last_fire_idx = buckets.iter().rposition(|(_, f)| *f > 0);
    let last_fire_label = last_fire_idx
        .map(|i| {
            let mins_ago = ((n - 1 - i) as f64 * mins_per_bucket).round() as u32;
            if mins_ago == 0 {
                "now".to_string()
            } else {
                format!("{mins_ago}m ago")
            }
        })
        .unwrap_or_else(|| "—".to_string());

    let max = buckets.iter().map(|(c, _)| *c).max().unwrap_or(1).max(1) as f64;
    let bars = buckets
        .into_iter()
        .map(|(c, fires)| {
            let h = (c as f64 / max * 100.0).clamp(2.0, 100.0);
            let color = if fires > 0 { "var(--accent)" } else { "var(--bg-highest)" };
            let ticks = crate::helpers::plural(c as u64, "tick", "ticks");
            let title = if fires > 0 {
                format!("{ticks} · {fires} fired")
            } else {
                ticks
            };
            view! {
                <div
                    title=title
                    style=format!("flex:1; min-width:1px; height:{h:.0}%; background:{color}; border-radius:var(--round-xs)")
                ></div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="eval-timeline-panel">
            <div class="eval-timeline-head">
                <span class="section-header-label">"EVALUATION TIMELINE · LAST HOUR"</span>
                <span class="eval-timeline-stats">
                    <span class="eval-timeline-stat">
                        <span class="eval-timeline-stat-num">{total_ticks.to_string()}</span>
                        {if total_ticks == 1 { " tick" } else { " ticks" }}
                    </span>
                    <span class="eval-timeline-sep">"·"</span>
                    <span class="eval-timeline-stat">
                        <span class="eval-timeline-stat-num" style="color:var(--accent)">{fire_count.to_string()}</span>
                        {if fire_count == 1 { " fire" } else { " fires" }}
                    </span>
                    <span class="eval-timeline-sep">"·"</span>
                    <span class="eval-timeline-stat">
                        "last fire "
                        <span class="eval-timeline-stat-num">{last_fire_label}</span>
                    </span>
                </span>
            </div>
            <div class="eval-timeline-bars">{bars}</div>
            <div class="eval-timeline-axis">
                <span>"−60m"</span>
                <span>"−45m"</span>
                <span>"−30m"</span>
                <span>"−15m"</span>
                <span>"now"</span>
            </div>
            <div class="eval-timeline-legend">
                <span class="eval-timeline-legend-item">
                    <span class="eval-timeline-swatch" style="background:var(--bg-highest)"></span>
                    "tick — no fire"
                </span>
                <span class="eval-timeline-legend-item">
                    <span class="eval-timeline-swatch" style="background:var(--accent)"></span>
                    "tick — run requested"
                </span>
                <span class="eval-timeline-legend-hint">"each bar = " {format!("{:.0}s", mins_per_bucket * 60.0)} " window; height = ticks in that window"</span>
            </div>
        </div>
    }
}

/// Run/backfill chip list for tick history rows.
///
/// Prefers backfill chips over raw run chips (sub-runs are an implementation
/// detail of the backfill). Renders a dash when neither list has entries.
/// Shared by schedule-detail and sensor-detail tick grids.
#[component]
pub fn TickRunChips(run_ids: Vec<String>, backfill_ids: Vec<String>) -> impl IntoView {
    let (lns, lnm) = crate::loc::use_current_location().get_untracked();
    if !backfill_ids.is_empty() {
        let lns_b = lns.clone();
        let lnm_b = lnm.clone();
        view! {
            <span style="display:flex; flex-wrap:wrap; gap:4px">
                {backfill_ids.into_iter().map(move |bid| {
                    let href = crate::loc::loc_path(&lns_b, &lnm_b, &format!("backfills/{}", bid));
                    let short = crate::helpers::short_id(&bid, 8);
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
                }).collect::<Vec<_>>()}
            </span>
        }
        .into_any()
    } else if !run_ids.is_empty() {
        view! {
            <span style="display:flex; flex-wrap:wrap; gap:4px">
                {run_ids.into_iter().map(move |id| {
                    let href = crate::loc::loc_path(&lns, &lnm, &format!("runs/{}", id));
                    let short = crate::helpers::short_id(&id, 8);
                    view! {
                        <A href=href attr:class="tag" attr:style="font-size:var(--fs-xs)">{short}</A>
                    }
                }).collect::<Vec<_>>()}
            </span>
        }
        .into_any()
    } else {
        view! {
            <span class="grid-cell-mono" style="color:var(--text-comment); font-size:var(--fs-sm)">"—"</span>
        }
        .into_any()
    }
}

type EvaluateOutcomeResult = Result<crate::server_fns::automation::EvaluateResult, ServerFnError>;

/// Result of an on-demand schedule or sensor tick, shown under the top bar.
#[component]
pub fn EvaluateOutcome(result: EvaluateOutcomeResult) -> impl IntoView {
    let loc = crate::loc::use_current_location();
    match result {
        Err(e) => view! {
            <div class="error-msg eval-outcome">
                {format!("Evaluation failed: {}", crate::helpers::err_text(&e))}
            </div>
        }
        .into_any(),
        Ok(r) if r.run_ids.is_empty() => {
            let text = match r.skip_reason {
                Some(reason) => format!("Skipped: {reason}"),
                None => "Evaluated. No runs requested.".to_string(),
            };
            view! { <div class="info-msg eval-outcome">{text}</div> }.into_any()
        }
        Ok(r) => {
            let (ns, name) = loc.get_untracked();
            let count = crate::helpers::plural(r.run_ids.len() as u64, "run", "runs");
            let links = r
                .run_ids
                .into_iter()
                .map(|id| {
                    let href = crate::loc::loc_path(&ns, &name, &format!("runs/{id}"));
                    let label = crate::helpers::short_id(&id, 8);
                    view! { <A href=href attr:class="tag">{label}</A> }
                })
                .collect::<Vec<_>>();
            view! {
                <div class="success-msg eval-outcome">
                    {format!("Evaluation requested {count}: ")}
                    {links}
                </div>
            }
            .into_any()
        }
    }
}

/// One-line Evaluate outcome for a table row.
#[component]
pub fn EvaluateOutcomeShort(result: EvaluateOutcomeResult) -> impl IntoView {
    let (text, cls, tip) = match result {
        Err(e) => {
            let msg = crate::helpers::err_text(&e);
            ("failed".to_string(), "text-error", msg)
        }
        Ok(r) if r.run_ids.is_empty() => match r.skip_reason {
            Some(reason) => ("skipped".to_string(), "text-muted", reason),
            None => ("no runs".to_string(), "text-muted", String::new()),
        },
        Ok(r) => (
            crate::helpers::plural(r.run_ids.len() as u64, "run", "runs"),
            "text-success",
            String::new(),
        ),
    };
    view! { <span class=cls style="font-size:var(--fs-xs)" title=tip>{text}</span> }
}

/// Schedule or sensor state ("RUNNING" / "STOPPED" on the wire): a dot and a
/// lowercase word. Not a status chip — a running chip means a live run.
#[component]
pub fn AutomationState(#[prop(into)] status: String) -> impl IntoView {
    let running = status.eq_ignore_ascii_case("running");
    let (dot, word) = if running {
        ("status-dot--ok", "running")
    } else {
        ("status-dot--muted", "stopped")
    };
    view! {
        <span class="status-dot-row">
            <span class=format!("status-dot {dot}")></span>
            <span class="grid-cell-muted">{word}</span>
        </span>
    }
}

/// Tick history table shared by the schedule and sensor pages.
#[component]
pub fn TickHistory(ticks: Vec<crate::types::TickRecord>) -> impl IntoView {
    if ticks.is_empty() {
        return view! {
            <SectionHeader label="TICK HISTORY"/>
            <EmptyState message="No ticks yet" compact=true/>
        }
        .into_any();
    }
    const GRID: &str = "grid-template-columns: 120px 130px 1fr 160px";
    let count = format!("last {}", ticks.len());
    view! {
        <SectionHeader label="TICK HISTORY" count=count/>
        <div class="grid-table">
            <div class="grid-table-head" style=GRID>
                <span>"TIME"</span>
                <span>"STATUS"</span>
                <span>"DETAIL"</span>
                <span>"RUNS"</span>
            </div>
            {ticks
                .into_iter()
                .map(|t| {
                    let ts = t.timestamp;
                    let ts_abs = crate::helpers::format_timestamp_nanos(t.timestamp);
                    let kind = crate::helpers::tick_status_kind(&t.status);
                    let detail_cls = if t.error.is_some() {
                        "grid-cell-muted grid-cell-truncate text-error"
                    } else {
                        "grid-cell-muted grid-cell-truncate"
                    };
                    let detail = t
                        .skip_reason
                        .clone()
                        .or_else(|| t.error.clone())
                        .or_else(|| crate::helpers::tick_counts_summary(&t.run_ids, &t.backfill_ids))
                        .unwrap_or_else(|| "—".to_string());
                    view! {
                        <div class="grid-row grid-row--plain" style=GRID title=ts_abs>
                            <span class="grid-cell-muted"><crate::now::RelTime ts=ts/></span>
                            <StatusChip kind=kind/>
                            <span class=detail_cls title=detail.clone()>{detail.clone()}</span>
                            <TickRunChips run_ids=t.run_ids.clone() backfill_ids=t.backfill_ids.clone()/>
                        </div>
                    }
                })
                .collect::<Vec<_>>()}
        </div>
    }
    .into_any()
}
