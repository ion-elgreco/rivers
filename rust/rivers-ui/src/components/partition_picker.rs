//! Shared partition picker for confirm-and-execute dialogs.
//!
//! Internal state lives entirely in the component; the parent reads the
//! cartesian-expanded result via the `selected` signal it owns.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::helpers::{JobPartitionPicker, cartesian_partition_keys};
use crate::loc::use_current_location;
use crate::server_fns::overview::{
    get_dynamic_partition_key_index, get_dynamic_partition_keys_page, get_partition_key_index,
    get_partition_keys_page,
};
use crate::types::SubmitPartitionKey;

/// Where a [`VirtualPartitionList`] pages its keys: an asset definition (gRPC)
/// or a storage-managed Dynamic namespace.
#[derive(Clone)]
enum KeySource {
    /// Asset keys via gRPC. `dimension` empty = single-dim, else a Multi dimension.
    Asset {
        asset_key: String,
        dimension: String,
    },
    /// Storage-managed Dynamic keys, by namespace.
    Dynamic { dynamic_name: String },
}

/// `selected` is parent-owned and written by this component as the user
/// toggles keys. It always carries the cartesian-expanded list of
/// `SubmitPartitionKey`s ready for submission. `reset` is typically the
/// dialog's `show` signal — when it transitions to `true` the picker
/// clears its internal state so a previously-open selection doesn't leak
/// across opens.
#[component]
pub fn PartitionPicker(
    #[prop(into)] picker: Signal<JobPartitionPicker>,
    selected: RwSignal<Vec<SubmitPartitionKey>>,
    #[prop(into)] reset: Signal<bool>,
) -> impl IntoView {
    let single_selected = RwSignal::new(Vec::<String>::new());
    let single_anchor = RwSignal::new(None::<usize>);
    let multi_selected = RwSignal::new(HashMap::<String, Vec<String>>::new());
    let multi_anchors = RwSignal::new(HashMap::<String, Option<usize>>::new());

    Effect::new(move |_| {
        if reset.get() {
            single_selected.set(Vec::new());
            single_anchor.set(None);
            multi_selected.set(HashMap::new());
            multi_anchors.set(HashMap::new());
            selected.set(Vec::new());
        }
    });

    Effect::new(move |_| {
        let p = picker.get();
        let s_single = single_selected.get();
        let s_multi = multi_selected.get();
        selected.set(collect_submit_keys(&p, &s_single, &s_multi));
    });

    let toggle_single = move |idx: usize, shift: bool| {
        let keys = match picker.get_untracked() {
            JobPartitionPicker::SingleDim { keys } => keys,
            _ => return,
        };
        let next = apply_toggle(
            &keys,
            &single_selected.get_untracked(),
            single_anchor.get_untracked(),
            idx,
            shift,
        );
        single_selected.set(next.selected);
        single_anchor.set(next.anchor);
    };

    let toggle_multi = move |dim: String, idx: usize, shift: bool| {
        let dims = match picker.get_untracked() {
            JobPartitionPicker::Multi { dimensions, .. } => dimensions,
            _ => return,
        };
        let Some(dim_info) = dims.into_iter().find(|d| d.name == dim) else {
            return;
        };
        let cur = multi_selected
            .get_untracked()
            .get(&dim)
            .cloned()
            .unwrap_or_default();
        let anchor = multi_anchors
            .get_untracked()
            .get(&dim)
            .copied()
            .unwrap_or(None);
        let next = apply_toggle(&dim_info.keys, &cur, anchor, idx, shift);
        multi_selected.update(|m| {
            m.insert(dim.clone(), next.selected);
        });
        multi_anchors.update(|m| {
            m.insert(dim, next.anchor);
        });
    };

    view! {
        {move || match picker.get() {
            JobPartitionPicker::None => ().into_any(),
            JobPartitionPicker::SingleDim { keys } => {
                let keys_for_select_all = keys.clone();
                view! {
                    <div class="form-group">
                        <PartitionList
                            label="Partitions".to_string()
                            keys=keys
                            selected_signal=single_selected.into()
                            on_toggle=Callback::new(move |(idx, shift)| toggle_single(idx, shift))
                            clear=Callback::new(move |_| single_selected.set(Vec::new()))
                            select_all=Callback::new(move |_| {
                                single_selected.set(keys_for_select_all.clone())
                            })
                        />
                    </div>
                }
                .into_any()
            }
            JobPartitionPicker::SingleDimPaged { asset_key, total } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        {format!(
                            "{total} partitions — scroll to browse, search to filter, or jump to a key.",
                        )}
                    </div>
                    <VirtualPartitionList
                        source=KeySource::Asset { asset_key, dimension: String::new() }
                        total=total as usize
                        selected=single_selected
                        reset=reset
                    />
                </div>
            }
            .into_any(),
            JobPartitionPicker::Dynamic { dynamic_name, total } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        {format!(
                            "{total} dynamic partitions — scroll to browse, search to filter, or jump to a key.",
                        )}
                    </div>
                    <VirtualPartitionList
                        source=KeySource::Dynamic { dynamic_name }
                        total=total as usize
                        selected=single_selected
                        reset=reset
                    />
                </div>
            }
            .into_any(),
            JobPartitionPicker::Multi { dimensions, asset_key } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        "Pick at least one value per dimension. The cartesian product fires one run each."
                    </div>
                    {dimensions.into_iter().map(|dim| {
                        let dim_name = dim.name.clone();
                        // Page a dimension only for a single Multi asset whose
                        // dimension overflows the inline window; else show the list.
                        if let (Some(ak), true) = (asset_key.clone(), dim.keys_truncated) {
                            let dim_total = dim.total_count as usize;
                            let seed = multi_selected
                                .get_untracked()
                                .get(&dim_name)
                                .cloned()
                                .unwrap_or_default();
                            // Per-dim selection signal, synced into multi_selected.
                            let dim_sel = RwSignal::new(seed);
                            let sync_name = dim_name.clone();
                            Effect::new(move |_| {
                                let v = dim_sel.get();
                                multi_selected.update(|m| { m.insert(sync_name.clone(), v); });
                            });
                            return view! {
                                <div class="exec-dialog-partition-dim">
                                    <label>{dim_name.clone()}</label>
                                    <div class="exec-dialog-partition-hint">
                                        {format!("{dim_total} values — scroll, search, or jump.")}
                                    </div>
                                    <VirtualPartitionList
                                        source=KeySource::Asset { asset_key: ak, dimension: dim_name }
                                        total=dim_total
                                        selected=dim_sel
                                        reset=reset
                                    />
                                </div>
                            }.into_any();
                        }
                        let dim_for_clear = dim_name.clone();
                        let dim_for_select_all = dim_name.clone();
                        let dim_for_signal = dim_name.clone();
                        let dim_for_toggle = dim_name.clone();
                        let keys = dim.keys.clone();
                        let keys_for_select_all = keys.clone();
                        let selected_signal: Signal<Vec<String>> = Signal::derive(move || {
                            multi_selected
                                .get()
                                .get(&dim_for_signal)
                                .cloned()
                                .unwrap_or_default()
                        });
                        view! {
                            <div class="exec-dialog-partition-dim">
                                <PartitionList
                                    label=dim_name
                                    keys=keys
                                    selected_signal=selected_signal
                                    on_toggle=Callback::new(move |(idx, shift)| {
                                        toggle_multi(dim_for_toggle.clone(), idx, shift)
                                    })
                                    clear=Callback::new(move |_| {
                                        multi_selected.update(|m| { m.remove(&dim_for_clear); });
                                    })
                                    select_all=Callback::new(move |_| {
                                        let dim_key = dim_for_select_all.clone();
                                        let all = keys_for_select_all.clone();
                                        multi_selected.update(|m| { m.insert(dim_key, all); });
                                    })
                                />
                            </div>
                        }.into_any()
                    }).collect::<Vec<_>>()}
                </div>
            }.into_any(),
        }}
    }
}

/// Compute the structured partition keys to submit for a given picker
/// and per-shape state. `SingleDim` wraps each selected key in `Single`;
/// `Multi` returns the cartesian product of per-dim selections as
/// `Multi` variants; `None` returns empty.
fn collect_submit_keys(
    picker: &JobPartitionPicker,
    single_selected: &[String],
    multi_selected: &HashMap<String, Vec<String>>,
) -> Vec<SubmitPartitionKey> {
    match picker {
        JobPartitionPicker::None => Vec::new(),
        JobPartitionPicker::SingleDim { .. }
        | JobPartitionPicker::SingleDimPaged { .. }
        | JobPartitionPicker::Dynamic { .. } => single_selected
            .iter()
            .cloned()
            .map(SubmitPartitionKey::Single)
            .collect(),
        JobPartitionPicker::Multi { dimensions, .. } => {
            let mut per_dim: Vec<(String, Vec<String>)> = Vec::with_capacity(dimensions.len());
            for dim in dimensions {
                per_dim.push((
                    dim.name.clone(),
                    multi_selected.get(&dim.name).cloned().unwrap_or_default(),
                ));
            }
            cartesian_partition_keys(&per_dim)
        }
    }
}

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
fn VirtualPartitionList(
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
            <button class="btn btn-tertiary btn-small" on:click=move |_| apply_search()>
                "Search"
            </button>
            <button class="btn btn-tertiary btn-small" on:click=move |_| do_jump()>
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
                        format!("{sel} selected · {} matches", view_total.get())
                    }
                }}
            </span>
            <span class="exec-dialog-partition-actions">
                <button
                    class="btn btn-tertiary btn-small"
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
                            "Browse".to_string()
                        } else {
                            format!("Selected ({})", selected.get().len())
                        }
                    }}
                </button>
                <button class="btn btn-tertiary btn-small" on:click=move |_| selected.set(Vec::new())>
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

/// The checkbox + label inside a partition row — shared by the inline,
/// virtualised, and selected-review lists. The wrapping `<div>` (class, position,
/// click) belongs to the caller; the checkbox is decorative (`prevent_default`),
/// so the row's own click drives selection.
fn partition_row_body(key: String, checked: bool) -> impl IntoView {
    view! {
        <input
            type="checkbox"
            prop:checked=checked
            tabindex="-1"
            on:click=|ev: leptos::ev::MouseEvent| ev.prevent_default()
        />
        <span>{key}</span>
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

/// Single labelled list of partition keys with multi-select + shift-range.
/// `select_all` fires with no args — the caller closes over the keys it
/// already passed in.
#[component]
fn PartitionList(
    #[prop(into)] label: String,
    keys: Vec<String>,
    selected_signal: Signal<Vec<String>>,
    on_toggle: Callback<(usize, bool)>,
    clear: Callback<()>,
    select_all: Callback<()>,
) -> impl IntoView {
    let total = keys.len();
    view! {
        <div class="exec-dialog-partition-head">
            <label>{label}</label>
            <span class="exec-dialog-partition-count">
                {move || format!("{} / {} selected", selected_signal.get().len(), total)}
            </span>
            <span class="exec-dialog-partition-actions">
                <button
                    class="btn btn-tertiary btn-small"
                    on:click=move |_| select_all.run(())
                >
                    "Select all"
                </button>
                <button
                    class="btn btn-tertiary btn-small"
                    on:click=move |_| clear.run(())
                >
                    "Clear"
                </button>
            </span>
        </div>
        <div class="exec-dialog-partition-hint">
            "Click to toggle. Shift-click to extend range."
        </div>
        <div class="exec-dialog-partition-list">
            {move || {
                let sel = selected_signal.get();
                keys.iter().enumerate().map(|(idx, key)| {
                    let key_for_display = key.clone();
                    let is_selected = sel.iter().any(|k| k == key);
                    let row_cls = if is_selected {
                        "exec-dialog-partition-row exec-dialog-partition-row--selected"
                    } else {
                        "exec-dialog-partition-row"
                    };
                    view! {
                        <div
                            class=row_cls
                            on:click=move |ev: leptos::ev::MouseEvent| {
                                ev.prevent_default();
                                on_toggle.run((idx, ev.shift_key()));
                            }
                        >
                            {partition_row_body(key_for_display, is_selected)}
                        </div>
                    }
                }).collect::<Vec<_>>()
            }}
        </div>
    }
}

/// Pure-Rust state transition for a partition-row click. Lives outside
/// the component so the multi-select + shift-range logic is unit-testable
/// without a Leptos runtime or DOM.
struct ToggleResult {
    selected: Vec<String>,
    anchor: Option<usize>,
}

/// Apply a click at `idx` with `shift` against the current `selected` /
/// `anchor` state. A plain click toggles the key and re-anchors; a
/// shift-click with an anchor extends the selection over `[anchor, idx]`
/// without removing already-selected items and without moving the anchor;
/// a shift-click without an anchor falls back to a plain toggle. An
/// out-of-bounds `idx` is a no-op.
fn apply_toggle(
    keys: &[String],
    selected: &[String],
    anchor: Option<usize>,
    idx: usize,
    shift: bool,
) -> ToggleResult {
    let Some(key) = keys.get(idx) else {
        return ToggleResult {
            selected: selected.to_vec(),
            anchor,
        };
    };

    if shift && let Some(a) = anchor {
        let lo = a.min(idx);
        let hi = a.max(idx);
        let mut next = selected.to_vec();
        for i in lo..=hi {
            let Some(k) = keys.get(i) else { continue };
            if !next.iter().any(|x| x == k) {
                next.push(k.clone());
            }
        }
        return ToggleResult {
            selected: next,
            anchor,
        };
    }

    let mut next = selected.to_vec();
    match next.iter().position(|k| k == key) {
        Some(pos) => {
            next.remove(pos);
        }
        None => next.push(key.clone()),
    }
    ToggleResult {
        selected: next,
        anchor: Some(idx),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PartitionDimensionInfo;

    fn keys(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ── apply_toggle ──

    #[test]
    fn plain_click_adds_unselected_key_and_sets_anchor() {
        let r = apply_toggle(&keys(&["a", "b", "c"]), &[], None, 1, false);
        assert_eq!(r.selected, vec!["b"]);
        assert_eq!(r.anchor, Some(1));
    }

    #[test]
    fn plain_click_removes_already_selected_key_and_re_anchors() {
        let r = apply_toggle(
            &keys(&["a", "b", "c"]),
            &["a".into(), "b".into()],
            Some(0),
            1,
            false,
        );
        assert_eq!(r.selected, vec!["a"]);
        assert_eq!(r.anchor, Some(1));
    }

    #[test]
    fn shift_click_without_anchor_falls_back_to_toggle() {
        let r = apply_toggle(&keys(&["a", "b", "c"]), &[], None, 2, true);
        assert_eq!(r.selected, vec!["c"]);
        assert_eq!(r.anchor, Some(2));
    }

    #[test]
    fn shift_click_extends_range_forward() {
        let r = apply_toggle(
            &keys(&["a", "b", "c", "d", "e"]),
            &["a".into()],
            Some(0),
            3,
            true,
        );
        assert_eq!(r.selected, vec!["a", "b", "c", "d"]);
        assert_eq!(r.anchor, Some(0));
    }

    #[test]
    fn shift_click_extends_range_backward() {
        let r = apply_toggle(
            &keys(&["a", "b", "c", "d", "e"]),
            &["e".into()],
            Some(4),
            1,
            true,
        );
        assert_eq!(r.selected, vec!["e", "b", "c", "d"]);
        assert_eq!(r.anchor, Some(4));
    }

    #[test]
    fn shift_click_preserves_already_selected_items() {
        let r = apply_toggle(
            &keys(&["a", "b", "c", "d"]),
            &["z".into(), "b".into()],
            Some(1),
            3,
            true,
        );
        assert_eq!(r.selected, vec!["z", "b", "c", "d"]);
        assert_eq!(r.anchor, Some(1));
    }

    #[test]
    fn shift_click_on_anchor_itself_is_idempotent() {
        let r = apply_toggle(&keys(&["a", "b", "c"]), &["b".into()], Some(1), 1, true);
        assert_eq!(r.selected, vec!["b"]);
        assert_eq!(r.anchor, Some(1));
    }

    #[test]
    fn out_of_bounds_idx_is_noop() {
        let r = apply_toggle(&keys(&["a", "b"]), &["a".into()], Some(0), 99, false);
        assert_eq!(r.selected, vec!["a"]);
        assert_eq!(r.anchor, Some(0));
    }

    #[test]
    fn empty_keys_is_noop() {
        let r = apply_toggle(&[], &[], None, 0, false);
        assert!(r.selected.is_empty());
        assert!(r.anchor.is_none());
    }

    #[test]
    fn consecutive_shift_clicks_extend_from_same_anchor() {
        let after_anchor = apply_toggle(&keys(&["a", "b", "c", "d"]), &[], None, 1, false);
        let after_first_shift = apply_toggle(
            &keys(&["a", "b", "c", "d"]),
            &after_anchor.selected,
            after_anchor.anchor,
            2,
            true,
        );
        assert_eq!(after_first_shift.selected, vec!["b", "c"]);
        assert_eq!(after_first_shift.anchor, Some(1));

        let after_second_shift = apply_toggle(
            &keys(&["a", "b", "c", "d"]),
            &after_first_shift.selected,
            after_first_shift.anchor,
            3,
            true,
        );
        assert_eq!(after_second_shift.selected, vec!["b", "c", "d"]);
        assert_eq!(after_second_shift.anchor, Some(1));
    }

    #[test]
    fn plain_click_after_shift_resets_anchor() {
        let r = apply_toggle(
            &keys(&["a", "b", "c", "d"]),
            &["b".into(), "c".into()],
            Some(1),
            3,
            false,
        );
        assert_eq!(r.selected, vec!["b", "c", "d"]);
        assert_eq!(r.anchor, Some(3));
    }

    // ── collect_submit_keys ──

    fn dim(name: &str, keys_arr: &[&str]) -> PartitionDimensionInfo {
        PartitionDimensionInfo {
            name: name.to_string(),
            keys: keys_arr.iter().map(|s| s.to_string()).collect(),
            total_count: keys_arr.len() as u64,
            keys_truncated: false,
        }
    }

    fn multi_key(dims: &[(&str, &str)]) -> SubmitPartitionKey {
        SubmitPartitionKey::Multi(
            dims.iter()
                .map(|(d, v)| (d.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn submit_keys_single_dim_wraps_each_selected_in_single() {
        let picker = JobPartitionPicker::SingleDim {
            keys: vec!["a".into(), "b".into(), "c".into()],
        };
        let selected = vec!["a".to_string(), "c".to_string()];
        let multi = HashMap::new();
        assert_eq!(
            collect_submit_keys(&picker, &selected, &multi),
            vec![
                SubmitPartitionKey::Single("a".to_string()),
                SubmitPartitionKey::Single("c".to_string()),
            ]
        );
    }

    #[test]
    fn submit_keys_multi_cartesians_per_dim_selections() {
        let picker = JobPartitionPicker::Multi {
            dimensions: vec![dim("color", &["r", "g"]), dim("size", &["s", "m"])],
            asset_key: None,
        };
        let selected = Vec::new();
        let mut multi = HashMap::new();
        multi.insert("color".to_string(), vec!["r".into(), "g".into()]);
        multi.insert("size".to_string(), vec!["s".into()]);
        let out = collect_submit_keys(&picker, &selected, &multi);
        assert_eq!(
            out,
            vec![
                multi_key(&[("color", "r"), ("size", "s")]),
                multi_key(&[("color", "g"), ("size", "s")]),
            ]
        );
    }

    #[test]
    fn submit_keys_multi_empty_when_any_dim_unselected() {
        let picker = JobPartitionPicker::Multi {
            dimensions: vec![dim("color", &["r"]), dim("size", &["s", "m"])],
            asset_key: None,
        };
        let selected = Vec::new();
        let mut multi = HashMap::new();
        multi.insert("color".to_string(), vec!["r".into()]);
        assert!(collect_submit_keys(&picker, &selected, &multi).is_empty());
    }

    #[test]
    fn submit_keys_none_picker_returns_empty() {
        let picker = JobPartitionPicker::None;
        let out = collect_submit_keys(&picker, &[], &HashMap::new());
        assert!(out.is_empty());
    }
}
