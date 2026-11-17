//! Shared partition picker for confirm-and-execute dialogs.
//!
//! Internal state lives entirely in the component; the parent reads the
//! cartesian-expanded result via the `selected` signal it owns.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::helpers::{JobPartitionPicker, cartesian_partition_keys};
use crate::types::{SpanSource, SubmitPartitionKey};

mod spans;
mod virtual_list;

use spans::SpanSet;
use virtual_list::{PagedSelection, VirtualPartitionList};

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
    let paged_spans = RwSignal::new(SpanSet::default());
    let paged_labels = RwSignal::new(HashMap::<u64, String>::new());
    let paged = PagedSelection::Spans {
        spans: paged_spans,
        labels: paged_labels,
    };

    Effect::new(move |_| {
        if reset.get() {
            single_selected.set(Vec::new());
            paged_spans.set(SpanSet::default());
            paged_labels.set(HashMap::new());
            single_anchor.set(None);
            multi_selected.set(HashMap::new());
            multi_anchors.set(HashMap::new());
            selected.set(Vec::new());
        }
    });

    Effect::new(move |_| {
        let keys = picker.with(|p| {
            single_selected.with(|s_single| {
                multi_selected.with(|s_multi| match span_source(p) {
                    Some(source) => {
                        paged_labels.with(|l| collect_span_keys(&source, &paged_spans.get(), l))
                    }
                    None => collect_submit_keys(p, s_single, s_multi),
                })
            })
        });
        selected.set(keys);
    });

    let toggle_single = move |idx: usize, shift: bool| {
        picker.with_untracked(|p| {
            let JobPartitionPicker::SingleDim { keys, .. } = p else {
                return;
            };
            let mut anchor = single_anchor.get_untracked();
            single_selected.update(|sel| anchor = apply_toggle(keys, sel, anchor, idx, shift));
            single_anchor.set(anchor);
        });
    };

    let toggle_multi = move |dim: String, idx: usize, shift: bool| {
        picker.with_untracked(|p| {
            let JobPartitionPicker::Multi { dimensions, .. } = p else {
                return;
            };
            let Some(dim_info) = dimensions.iter().find(|d| d.name == dim) else {
                return;
            };
            let mut anchor = multi_anchors.with_untracked(|m| m.get(&dim).copied().flatten());
            multi_selected.update(|m| {
                let sel = m.entry(dim.clone()).or_default();
                anchor = apply_toggle(&dim_info.keys, sel, anchor, idx, shift);
            });
            multi_anchors.update(|m| {
                m.insert(dim, anchor);
            });
        });
    };

    view! {
        {move || match picker.get() {
            JobPartitionPicker::None => ().into_any(),
            JobPartitionPicker::SingleDim { keys, truncated } => {
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
                        {truncated
                            .then(|| {
                                view! {
                                    <p class="exec-dialog-partition-hint">
                                        "Key lists were truncated — only shared keys from the first window are shown."
                                    </p>
                                }
                            })}
                    </div>
                }
                .into_any()
            }
            JobPartitionPicker::SingleDimPaged { asset_key, total } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        {format!(
                            "{total} partitions — scroll to browse, search to filter, or jump to a key. Shift-click selects a range.",
                        )}
                    </div>
                    <VirtualPartitionList
                        source=KeySource::Asset { asset_key, dimension: String::new() }
                        total=total as usize
                        selection=paged
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
                            "{} — scroll to browse, search to filter, or jump to a key. Shift-click selects a range.",
                            crate::helpers::plural(total, "dynamic partition", "dynamic partitions"),
                        )}
                    </div>
                    <VirtualPartitionList
                        source=KeySource::Dynamic { dynamic_name }
                        total=total as usize
                        selection=paged
                        reset=reset
                    />
                </div>
            }
            .into_any(),
            JobPartitionPicker::Multi { dimensions, asset_key, truncated } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        "Pick at least one value per dimension. The cartesian product fires one run each."
                    </div>
                    {truncated
                        .then(|| {
                            view! {
                                <p class="exec-dialog-partition-hint">
                                    "Dimension key lists were truncated — only shared keys from the first window are shown."
                                </p>
                            }
                        })}
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
                                        {format!("{} — scroll, search, or jump.", crate::helpers::plural(dim_total as u64, "value", "values"))}
                                    </div>
                                    <VirtualPartitionList
                                        source=KeySource::Asset { asset_key: ak, dimension: dim_name }
                                        total=dim_total
                                        selection=PagedSelection::Keys(dim_sel)
                                        reset=reset
                                    />
                                </div>
                            }.into_any();
                        }
                        let dim_for_clear = dim_name.clone();
                        let dim_for_select_all = dim_name.clone();
                        let dim_for_signal = dim_name.clone();
                        let dim_for_toggle = dim_name.clone();
                        let keys = dim.keys;
                        let keys_for_select_all = keys.clone();
                        let selected_signal: Signal<Vec<String>> = Signal::derive(move || {
                            multi_selected
                                .with(|m| m.get(&dim_for_signal).cloned().unwrap_or_default())
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

/// The explicit choice to run a verb whose partition key is optional on every
/// partition. An empty pick never means that: a pick emptied by a live refresh
/// would otherwise run the verb on the whole asset. Choosing it clears `selected`;
/// the host hides the picker while it is set.
#[component]
pub fn WholeAssetChoice(
    checked: RwSignal<bool>,
    selected: RwSignal<Vec<SubmitPartitionKey>>,
) -> impl IntoView {
    view! {
        <label class="whole-asset-choice">
            <input
                type="checkbox"
                prop:checked=move || checked.get()
                on:change=move |ev| {
                    let on = event_target_checked(&ev);
                    if on {
                        selected.set(Vec::new());
                    }
                    checked.set(on);
                }
            />
            "Whole asset (every partition)"
        </label>
    }
}

/// The source a paged single-dim picker selects index spans from.
fn span_source(picker: &JobPartitionPicker) -> Option<SpanSource> {
    match picker {
        JobPartitionPicker::SingleDimPaged { asset_key, .. } => {
            Some(SpanSource::Asset(asset_key.clone()))
        }
        JobPartitionPicker::Dynamic { dynamic_name, .. } => {
            Some(SpanSource::Dynamic(dynamic_name.clone()))
        }
        _ => None,
    }
}

/// A lone index whose key is known submits as that key; anything else as a
/// span the server expands.
fn collect_span_keys(
    source: &SpanSource,
    spans: &SpanSet,
    labels: &HashMap<u64, String>,
) -> Vec<SubmitPartitionKey> {
    spans
        .spans()
        .iter()
        .map(|&(start, end)| match labels.get(&start) {
            Some(key) if start == end => SubmitPartitionKey::Single(key.clone()),
            _ => SubmitPartitionKey::Span {
                source: source.clone(),
                start,
                end,
            },
        })
        .collect()
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
            let per_dim = dimensions
                .iter()
                .map(|dim| {
                    let selected = multi_selected.get(&dim.name).map_or(&[][..], Vec::as_slice);
                    (dim.name.as_str(), selected)
                })
                .collect();
            cartesian_partition_keys(per_dim)
        }
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
                {move || format!("{} / {} selected", selected_signal.with(Vec::len), total)}
            </span>
            <span class="exec-dialog-partition-actions">
                <button class="link-btn" on:click=move |_| select_all.run(())>
                    "Select all"
                </button>
                <span class="bulk-sep">"·"</span>
                <button class="link-btn" on:click=move |_| clear.run(())>
                    "Clear"
                </button>
            </span>
        </div>
        <div class="exec-dialog-partition-hint">
            "Click to toggle. Shift-click to extend range."
        </div>
        <div class="exec-dialog-partition-list">
            {move || {
                let sel = selected_signal.read();
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

/// Apply a click at `idx` with `shift` to `selected` and return the new
/// anchor. A plain click toggles the key and re-anchors; a shift-click with
/// an anchor extends the selection over `[anchor, idx]` without removing
/// already-selected items and without moving the anchor; a shift-click
/// without an anchor falls back to a plain toggle. An out-of-bounds `idx`
/// is a no-op. Lives outside the component so the multi-select +
/// shift-range logic is unit-testable without a Leptos runtime or DOM.
fn apply_toggle(
    keys: &[String],
    selected: &mut Vec<String>,
    anchor: Option<usize>,
    idx: usize,
    shift: bool,
) -> Option<usize> {
    let Some(key) = keys.get(idx) else {
        return anchor;
    };

    if shift && let Some(a) = anchor {
        let lo = a.min(idx);
        let hi = a.max(idx);
        for i in lo..=hi {
            let Some(k) = keys.get(i) else { continue };
            if !selected.iter().any(|x| x == k) {
                selected.push(k.clone());
            }
        }
        return anchor;
    }

    match selected.iter().position(|k| k == key) {
        Some(pos) => {
            selected.remove(pos);
        }
        None => selected.push(key.clone()),
    }
    Some(idx)
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
        let mut sel = vec![];
        let anchor = apply_toggle(&keys(&["a", "b", "c"]), &mut sel, None, 1, false);
        assert_eq!(sel, vec!["b"]);
        assert_eq!(anchor, Some(1));
    }

    #[test]
    fn plain_click_removes_already_selected_key_and_re_anchors() {
        let mut sel = keys(&["a", "b"]);
        let anchor = apply_toggle(&keys(&["a", "b", "c"]), &mut sel, Some(0), 1, false);
        assert_eq!(sel, vec!["a"]);
        assert_eq!(anchor, Some(1));
    }

    #[test]
    fn shift_click_without_anchor_falls_back_to_toggle() {
        let mut sel = vec![];
        let anchor = apply_toggle(&keys(&["a", "b", "c"]), &mut sel, None, 2, true);
        assert_eq!(sel, vec!["c"]);
        assert_eq!(anchor, Some(2));
    }

    #[test]
    fn shift_click_extends_range_forward() {
        let mut sel = keys(&["a"]);
        let anchor = apply_toggle(
            &keys(&["a", "b", "c", "d", "e"]),
            &mut sel,
            Some(0),
            3,
            true,
        );
        assert_eq!(sel, vec!["a", "b", "c", "d"]);
        assert_eq!(anchor, Some(0));
    }

    #[test]
    fn shift_click_extends_range_backward() {
        let mut sel = keys(&["e"]);
        let anchor = apply_toggle(
            &keys(&["a", "b", "c", "d", "e"]),
            &mut sel,
            Some(4),
            1,
            true,
        );
        assert_eq!(sel, vec!["e", "b", "c", "d"]);
        assert_eq!(anchor, Some(4));
    }

    #[test]
    fn shift_click_preserves_already_selected_items() {
        let mut sel = keys(&["z", "b"]);
        let anchor = apply_toggle(&keys(&["a", "b", "c", "d"]), &mut sel, Some(1), 3, true);
        assert_eq!(sel, vec!["z", "b", "c", "d"]);
        assert_eq!(anchor, Some(1));
    }

    #[test]
    fn shift_click_on_anchor_itself_is_idempotent() {
        let mut sel = keys(&["b"]);
        let anchor = apply_toggle(&keys(&["a", "b", "c"]), &mut sel, Some(1), 1, true);
        assert_eq!(sel, vec!["b"]);
        assert_eq!(anchor, Some(1));
    }

    #[test]
    fn out_of_bounds_idx_is_noop() {
        let mut sel = keys(&["a"]);
        let anchor = apply_toggle(&keys(&["a", "b"]), &mut sel, Some(0), 99, false);
        assert_eq!(sel, vec!["a"]);
        assert_eq!(anchor, Some(0));
    }

    #[test]
    fn empty_keys_is_noop() {
        let mut sel = vec![];
        let anchor = apply_toggle(&[], &mut sel, None, 0, false);
        assert!(sel.is_empty());
        assert!(anchor.is_none());
    }

    #[test]
    fn consecutive_shift_clicks_extend_from_same_anchor() {
        let all = keys(&["a", "b", "c", "d"]);
        let mut sel = vec![];
        let anchor = apply_toggle(&all, &mut sel, None, 1, false);

        let anchor = apply_toggle(&all, &mut sel, anchor, 2, true);
        assert_eq!(sel, vec!["b", "c"]);
        assert_eq!(anchor, Some(1));

        let anchor = apply_toggle(&all, &mut sel, anchor, 3, true);
        assert_eq!(sel, vec!["b", "c", "d"]);
        assert_eq!(anchor, Some(1));
    }

    #[test]
    fn plain_click_after_shift_resets_anchor() {
        let mut sel = keys(&["b", "c"]);
        let anchor = apply_toggle(&keys(&["a", "b", "c", "d"]), &mut sel, Some(1), 3, false);
        assert_eq!(sel, vec!["b", "c", "d"]);
        assert_eq!(anchor, Some(3));
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
            truncated: false,
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
            truncated: false,
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
            truncated: false,
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
