use std::collections::HashMap;

use leptos::prelude::*;

use crate::loc::use_current_location;
use crate::server_fns::overview::{
    get_dynamic_partition_key_index, get_dynamic_partition_keys_page, get_partition_key_index,
    get_partition_keys_page,
};

use super::{KeySource, partition_row_body};

// ── Virtual partition list geometry ──
//
// Shared by the component and the jump seek. Scrolling is 1:1 up to
// MAX_SPACER_PX; past it the spacer is capped and the scroll coordinate scaled —
// rendered rows stay exact, the scrollbar→row mapping coarsens.

const ROW_H: f64 = 28.0;
// 12 rows, an exact ROW_H multiple — a non-multiple would clip the last row in
// scaled mode (no over-scroll to reveal it).
const VIEWPORT_H: f64 = 336.0;
const PAGE: usize = 200;
const OVERSCAN: usize = 8;
/// Browsers cap element height (~17.8M px Firefox); keep the spacer under it.
/// Past this the spacer is capped and the scroll coordinate scaled, so the tail
/// stays reachable — jump/search land exactly regardless.
const MAX_SPACER_PX: f64 = 10_000_000.0;

/// Rows that fit in the viewport (no overscan).
fn visible_rows() -> usize {
    (VIEWPORT_H / ROW_H).ceil() as usize
}

/// Whether `total` rows overflow the un-capped spacer and need scaling.
fn is_scaled(total: usize) -> bool {
    total as f64 * ROW_H > MAX_SPACER_PX
}

/// Spacer height for `total` rows — the natural height, capped at MAX_SPACER_PX.
fn spacer_px(total: usize) -> f64 {
    (total as f64 * ROW_H).min(MAX_SPACER_PX)
}

/// First (logical) visible row for a scroll offset — 1:1 below the cap, a scaled
/// fraction of the row range above it.
fn row_for_scroll(scroll: f64, total: usize) -> usize {
    if total == 0 {
        return 0;
    }
    if is_scaled(total) {
        let max_scroll = (spacer_px(total) - VIEWPORT_H).max(1.0);
        let frac = (scroll / max_scroll).clamp(0.0, 1.0);
        let last_start = total.saturating_sub(visible_rows());
        (frac * last_start as f64).round() as usize
    } else {
        (scroll / ROW_H).floor() as usize
    }
}

/// Scroll offset that brings `row` to the top of the viewport — the inverse of
/// [`row_for_scroll`], used by "jump to key".
fn scroll_for_row(row: usize, total: usize) -> f64 {
    if is_scaled(total) {
        let max_scroll = (spacer_px(total) - VIEWPORT_H).max(1.0);
        let last_start = total.saturating_sub(visible_rows()).max(1);
        (row.min(last_start) as f64 / last_start as f64) * max_scroll
    } else {
        row as f64 * ROW_H
    }
}

/// Virtualised, on-demand-paged list for a key set too large to ship inline (an
/// asset, a Multi dimension, or a Dynamic namespace — see [`KeySource`]): renders
/// only visible rows, paging on scroll. Three ways to reach any key past the
/// element-height cap — **scroll** (rolling window, scaled past the cap),
/// **search** (substring filter, position-independent), **jump** (resolve index,
/// seek there) — plus a **Selected (N)** toggle. Selection is by key string, so
/// it survives all of them.
#[component]
pub(super) fn VirtualPartitionList(
    /// Key source — see [`KeySource`].
    source: KeySource,
    total: usize,
    selected: RwSignal<Vec<String>>,
    /// Dialog-open signal — clears the list's state + selection on reopen.
    #[prop(into)]
    reset: Signal<bool>,
) -> impl IntoView {
    use std::collections::HashSet;

    let loc = use_current_location();
    let list_ref = NodeRef::<leptos::html::Div>::new();

    // false = browse/search the key space; true = review only the selected keys.
    let show_selected = RwSignal::new(false);
    // Applied filter (committed on Enter / Search, not per keystroke, so the
    // scan runs only on demand). Empty = browse the whole set.
    let query = RwSignal::new(String::new());
    let query_input = RwSignal::new(String::new());
    let not_found = RwSignal::new(false);

    // Page cache + in-flight set (keyed by page index). `view_total` sizes the
    // scrollbar (full count, or match count when filtering), corrected as pages
    // arrive; `count_known` is false until a filter's first page lands, so the
    // header shows "searching…" rather than the full count.
    let pages = RwSignal::new(HashMap::<usize, Vec<String>>::new());
    let requested = RwSignal::new(HashSet::<usize>::new());
    let view_total = RwSignal::new(total);
    let count_known = RwSignal::new(true);
    let scroll_top = RwSignal::new(0.0_f64);

    // Drop the page cache + in-flight set — every key-space change (reset, new
    // search, jump out of a filter) must do this together, so keep it in one spot.
    let clear_cache = move || {
        pages.set(HashMap::new());
        requested.set(HashSet::new());
    };

    // Clear everything (including the selection this widget owns) on reopen.
    Effect::new(move |_| {
        if reset.get() {
            show_selected.set(false);
            query.set(String::new());
            query_input.set(String::new());
            not_found.set(false);
            clear_cache();
            view_total.set(total);
            count_known.set(true);
            scroll_top.set(0.0);
            selected.set(Vec::new());
            if let Some(el) = list_ref.get_untracked() {
                el.set_scroll_top(0);
            }
        }
    });

    // [rstart, rend) rows to render, the logical top row, and whether scaled.
    let window = move || {
        let vt = view_total.get();
        let logical = row_for_scroll(scroll_top.get(), vt);
        let rstart = logical.saturating_sub(OVERSCAN);
        let rend = (logical + visible_rows() + OVERSCAN).min(vt);
        (rstart, rend, logical, is_scaled(vt))
    };

    // Fetch the pages covering the visible range, once each, for the current
    // query. A response whose query no longer matches is dropped (stale).
    let fetch_source = source.clone();
    Effect::new(move |_| {
        if show_selected.get() {
            return;
        }
        let q = query.get();
        let (rstart, rend, _, _) = window();
        if rend == 0 {
            return;
        }
        for pg in (rstart / PAGE)..=((rend - 1) / PAGE) {
            if requested.get_untracked().contains(&pg) {
                continue;
            }
            requested.update(|r| {
                r.insert(pg);
            });
            let (ns, nm) = loc.get_untracked();
            let src = fetch_source.clone();
            let q = q.clone();
            leptos::task::spawn_local(async move {
                let offset = (pg * PAGE) as u64;
                let res = match src {
                    KeySource::Asset {
                        asset_key,
                        dimension,
                    } => {
                        get_partition_keys_page(
                            ns,
                            nm,
                            asset_key,
                            dimension,
                            q.clone(),
                            offset,
                            PAGE as u64,
                        )
                        .await
                    }
                    KeySource::Dynamic { dynamic_name } => {
                        get_dynamic_partition_keys_page(
                            ns,
                            nm,
                            dynamic_name,
                            q.clone(),
                            offset,
                            PAGE as u64,
                        )
                        .await
                    }
                };
                // Query changed under us — the cache was reset; drop this result.
                if query.get_untracked() != q {
                    return;
                }
                match res {
                    Ok((keys, n)) => {
                        let n = n as usize;
                        // Equality-guard: an unchanged set still notifies, re-running
                        // this Effect once per page.
                        if view_total.get_untracked() != n {
                            view_total.set(n);
                        }
                        if !count_known.get_untracked() {
                            count_known.set(true);
                        }
                        pages.update(|m| {
                            m.insert(pg, keys);
                        });
                    }
                    // Allow a later retry if the fetch failed.
                    Err(_) => requested.update(|r| {
                        r.remove(&pg);
                    }),
                }
            });
        }
    });

    // Apply the search box as a filter: new key space → drop the cache, reset to
    // the top. The first page fetch returns the true match count.
    let apply_search = move || {
        not_found.set(false);
        show_selected.set(false);
        clear_cache();
        let q = query_input.get_untracked();
        if q.is_empty() {
            // Browse: the full count is known immediately.
            view_total.set(total);
            count_known.set(true);
        } else {
            // Filter: collapse the spacer to a one-page placeholder + "searching…"
            // until the first page returns the real match count.
            view_total.set(PAGE.min(total));
            count_known.set(false);
        }
        query.set(q);
        scroll_top.set(0.0);
        if let Some(el) = list_ref.get_untracked() {
            el.set_scroll_top(0);
        }
    };

    // Jump to a typed key in the full (unfiltered) list: resolve its index, then
    // seek the window straight there.
    let jump_source = source.clone();
    let do_jump = move || {
        let key = query_input.get_untracked();
        if key.is_empty() {
            return;
        }
        let (ns, nm) = loc.get_untracked();
        let src = jump_source.clone();
        leptos::task::spawn_local(async move {
            let res = match src {
                KeySource::Asset {
                    asset_key,
                    dimension,
                } => get_partition_key_index(ns, nm, asset_key, dimension, key).await,
                KeySource::Dynamic { dynamic_name } => {
                    get_dynamic_partition_key_index(ns, nm, dynamic_name, key).await
                }
            };
            match res {
                Ok(idx) if idx >= 0 => {
                    not_found.set(false);
                    show_selected.set(false);
                    // Jump targets the whole set — clear any active filter.
                    if !query.get_untracked().is_empty() {
                        clear_cache();
                        query.set(String::new());
                    }
                    view_total.set(total);
                    count_known.set(true);
                    let target = scroll_for_row(idx as usize, total);
                    scroll_top.set(target);
                    // Set DOM scroll next frame: jumping out of a filter regrows
                    // the spacer first; scrolling before that clamps short.
                    let lr = list_ref;
                    request_animation_frame(move || {
                        if let Some(el) = lr.get_untracked() {
                            el.set_scroll_top(target as i32);
                        }
                    });
                }
                _ => not_found.set(true),
            }
        });
    };

    view! {
        <div class="exec-dialog-partition-vtools">
            <input
                class="exec-dialog-partition-search"
                type="text"
                placeholder="Search or jump to a key…"
                prop:value=move || query_input.get()
                on:input=move |ev| {
                    query_input.set(event_target_value(&ev));
                    not_found.set(false);
                }
                on:keydown=move |ev: leptos::ev::KeyboardEvent| {
                    if ev.key() == "Enter" {
                        ev.prevent_default();
                        apply_search();
                    }
                }
            />
            <button class="btn btn-small" on:click=move |_| apply_search()>
                "Search"
            </button>
            <button class="btn btn-small" on:click=move |_| do_jump()>
                "Jump"
            </button>
        </div>
        <div class="exec-dialog-partition-head">
            <span class="exec-dialog-partition-count">
                {move || {
                    let sel = selected.get().len();
                    if show_selected.get() {
                        format!("{sel} selected")
                    } else if !count_known.get() {
                        format!("{sel} selected · searching…")
                    } else if query.get().is_empty() {
                        format!("{sel} selected · {} total", view_total.get())
                    } else {
                        format!("{sel} selected · {}", crate::helpers::plural(view_total.get() as u64, "match", "matches"))
                    }
                }}
            </span>
            <span class="exec-dialog-partition-actions">
                <button
                    class="link-btn"
                    on:click=move |_| {
                        let next = !show_selected.get_untracked();
                        show_selected.set(next);
                        not_found.set(false);
                        if !next {
                            // Back to browse: the list remounts at scrollTop 0.
                            scroll_top.set(0.0);
                        }
                    }
                >
                    {move || {
                        if show_selected.get() {
                            "Show all".to_string()
                        } else {
                            format!("Show selected ({})", selected.get().len())
                        }
                    }}
                </button>
                <span class="bulk-sep">"·"</span>
                <button class="link-btn" on:click=move |_| selected.set(Vec::new())>
                    "Clear"
                </button>
            </span>
        </div>
        {move || {
            (not_found.get() && !show_selected.get())
                .then(|| {
                    view! {
                        <div class="exec-dialog-partition-hint exec-dialog-partition-notfound">
                            "No partition matches that key."
                        </div>
                    }
                })
        }}
        {move || {
            if show_selected.get() {
                selected_review_view(selected).into_any()
            } else {
                view! {
                    <div
                        class="exec-dialog-partition-vlist"
                        node_ref=list_ref
                        style=format!("height:{VIEWPORT_H}px")
                        on:scroll=move |ev| {
                            let el = event_target::<leptos::web_sys::Element>(&ev);
                            scroll_top.set(el.scroll_top() as f64);
                        }
                    >
                        <div
                            class="exec-dialog-partition-vspacer"
                            style=move || format!("height:{}px", spacer_px(view_total.get()))
                        >
                            {move || {
                                let (rstart, rend, logical, scaled) = window();
                                let base = scroll_top.get();
                                // Borrow the cache/selection (no per-tick clone);
                                // membership via a set built once per render.
                                selected.with(|sel| {
                                    let sel_set: HashSet<&str> =
                                        sel.iter().map(String::as_str).collect();
                                    pages.with(|pgs| {
                                        (rstart..rend)
                                            .map(|idx| {
                                                let top = if scaled {
                                                    base + (idx as f64 - logical as f64) * ROW_H
                                                } else {
                                                    idx as f64 * ROW_H
                                                };
                                                let style =
                                                    format!("top:{top}px;height:{ROW_H}px");
                                                match pgs
                                                    .get(&(idx / PAGE))
                                                    .and_then(|v| v.get(idx % PAGE))
                                                {
                                                    Some(key) => {
                                                        let is_sel =
                                                            sel_set.contains(key.as_str());
                                                        let key = key.clone();
                                                        let k = key.clone();
                                                        let cls = if is_sel {
                                                            "exec-dialog-partition-row exec-dialog-partition-vrow exec-dialog-partition-row--selected"
                                                        } else {
                                                            "exec-dialog-partition-row exec-dialog-partition-vrow"
                                                        };
                                                        view! {
                                                            <div
                                                                class=cls
                                                                style=style
                                                                on:click=move |_| {
                                                                    let k = k.clone();
                                                                    selected
                                                                        .update(|s| {
                                                                            if let Some(p) = s.iter().position(|x| x == &k) {
                                                                                s.remove(p);
                                                                            } else {
                                                                                s.push(k);
                                                                            }
                                                                        });
                                                                }
                                                            >
                                                                {partition_row_body(key, is_sel)}
                                                            </div>
                                                        }
                                                            .into_any()
                                                    }
                                                    None => {
                                                        view! {
                                                            <div
                                                                class="exec-dialog-partition-row exec-dialog-partition-vrow exec-dialog-partition-vrow--loading"
                                                                style=style
                                                            >
                                                                "…"
                                                            </div>
                                                        }
                                                            .into_any()
                                                    }
                                                }
                                            })
                                            .collect::<Vec<_>>()
                                    })
                                })
                            }}
                        </div>
                    </div>
                }
                    .into_any()
            }
        }}
    }
}

/// The selected keys for review — client-side (no fetch, no cap). Click removes.
fn selected_review_view(selected: RwSignal<Vec<String>>) -> impl IntoView {
    view! {
        <div class="exec-dialog-partition-list" style=format!("max-height:{VIEWPORT_H}px")>
            {move || {
                let sel = selected.get();
                if sel.is_empty() {
                    view! {
                        <div class="exec-dialog-partition-row exec-dialog-partition-vrow--loading">
                            "Nothing selected yet."
                        </div>
                    }
                        .into_any()
                } else {
                    sel.into_iter()
                        .map(|key| {
                            let k = key.clone();
                            view! {
                                <div
                                    class="exec-dialog-partition-row exec-dialog-partition-row--selected"
                                    on:click=move |_| {
                                        let k = k.clone();
                                        selected.update(|s| s.retain(|x| x != &k));
                                    }
                                >
                                    {partition_row_body(key, true)}
                                </div>
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_any()
                }
            }}
        </div>
    }
}
