//! Shared partition picker for confirm-and-execute dialogs.
//!
//! Internal state lives entirely in the component; the parent reads the
//! cartesian-expanded result via the `selected` signal it owns.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::helpers::{JobPartitionPicker, cartesian_partition_keys};
use crate::types::SubmitPartitionKey;

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
            JobPartitionPicker::Multi { dimensions } => dimensions,
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
            JobPartitionPicker::Multi { dimensions } => view! {
                <div class="form-group">
                    <label>"Partitions"</label>
                    <div class="exec-dialog-partition-hint">
                        "Pick at least one value per dimension. The cartesian product fires one run each."
                    </div>
                    {dimensions.into_iter().map(|dim| {
                        let dim_name = dim.name.clone();
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
                        }
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
        JobPartitionPicker::SingleDim { .. } => single_selected
            .iter()
            .cloned()
            .map(SubmitPartitionKey::Single)
            .collect(),
        JobPartitionPicker::Multi { dimensions } => {
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
                            <input
                                type="checkbox"
                                prop:checked=is_selected
                                tabindex="-1"
                                on:click=|ev: leptos::ev::MouseEvent| ev.prevent_default()
                            />
                            <span>{key_for_display}</span>
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
