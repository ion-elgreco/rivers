//! Materialize confirmation dialog.
//!
//! ≤2 selected partitions fire one `trigger_materialize` run each; a larger
//! selection lands as a single backfill over the assets + chosen keys.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::components::partition_picker::PartitionPicker;
use crate::helpers::JobPartitionPicker;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::mutations::{launch_backfill, trigger_action, trigger_materialize};
use crate::types::{AssetRecord, StaleStatus, SubmitPartitionKey};

/// Above this many selected partitions, submit one backfill instead of a run each.
const BACKFILL_THRESHOLD: usize = 2;

/// What a submit produced, so the success Effect navigates to the right page.
#[derive(Clone)]
enum DialogOutcome {
    Run(String),
    Backfill(String),
}

/// One-line description of exactly what a submit will launch. Mirrors the
/// branching in `materialize_action` — if that changes, this must too.
fn launch_summary(n_assets: usize, n_partitions: usize, partitioned: bool) -> String {
    if n_assets == 0 {
        return "Nothing selected".to_string();
    }
    let assets = if n_assets == 1 {
        "1 asset".to_string()
    } else {
        format!("{n_assets} assets")
    };
    if !partitioned {
        return format!("{assets} · 1 run");
    }
    if n_partitions == 0 {
        return format!("{assets} · select a partition");
    }
    let parts = if n_partitions == 1 {
        "1 partition".to_string()
    } else {
        format!("{n_partitions} partitions")
    };
    if n_partitions > BACKFILL_THRESHOLD {
        format!("{assets} · {parts} · 1 backfill")
    } else if n_partitions > 1 {
        format!("{assets} · {parts} · {n_partitions} runs")
    } else {
        format!("{assets} · {parts} · 1 run")
    }
}

/// Status dot class + word for an asset's staleness. `None` = the record
/// hasn't loaded (or the key isn't an asset), which reads as unknown.
fn status_bits(record: Option<&AssetRecord>) -> (&'static str, &'static str) {
    match record.map(|r| &r.stale_status) {
        Some(StaleStatus::UpToDate) => ("mat-dialog-dot--ok", "up to date"),
        Some(StaleStatus::Stale) => ("mat-dialog-dot--stale", "stale"),
        Some(StaleStatus::Missing) => ("mat-dialog-dot--missing", "missing"),
        None => ("mat-dialog-dot--missing", ""),
    }
}

#[component]
pub fn MaterializeDialog(
    #[prop(into)] show: RwSignal<bool>,
    #[prop(into)] asset_keys: Signal<Vec<String>>,
    /// `JobPartitionPicker::None` omits the partition section and submits a
    /// single unpartitioned run. Otherwise the shared picker renders the keys;
    /// the cartesian product of selections becomes per-partition runs (≤2) or a
    /// backfill (more).
    #[prop(optional, into)]
    picker: Option<Signal<JobPartitionPicker>>,
    /// Verb to run instead of materialize. Same partition selection, submitted
    /// as action runs (or an action backfill above the threshold).
    #[prop(optional, into)]
    action: Option<Signal<Option<String>>>,
    /// The verb clears materialization state (`Outcome.Unmaterialize`). Says so
    /// on the dialog — nothing else in the product distinguishes a destructive
    /// verb from a benign one.
    #[prop(optional, into)]
    destructive: Option<Signal<bool>>,
    /// Asset records decorating the rows (staleness / group / last
    /// materialized), from the host page's live resource — the dialog used to
    /// re-fetch every asset per open.
    #[prop(into)]
    records: Signal<HashMap<String, AssetRecord>>,
) -> impl IntoView {
    let verb: Signal<Option<String>> = action.unwrap_or_else(|| Signal::derive(|| None));
    let destructive: Signal<bool> = destructive.unwrap_or_else(|| Signal::derive(|| false));
    let (selected, set_selected) = signal(Vec::<String>::new());
    let partition_keys = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let (tag_key, set_tag_key) = signal(String::new());
    let (tag_val, set_tag_val) = signal(String::new());
    let (tags, set_tags) = signal(Vec::<(String, String)>::new());
    let (nav_to, set_nav_to) = signal(Option::<String>::None);

    Effect::new(move || {
        if show.get() {
            set_selected.set(asset_keys.get());
            // `PartitionPicker` clears this too, but it is only mounted for a
            // partitioned selection — without this an unpartitioned open would
            // submit the previous open's keys.
            partition_keys.set(Vec::new());
            // Tags feed the submitted run (including `rivers/priority`), so a
            // tag typed for one open must not ride along on the next.
            set_tags.set(Vec::new());
            set_tag_key.set(String::new());
            set_tag_val.set(String::new());
        }
    });

    let loc = use_current_location();

    let materialize_action = Action::new(move |_: &()| {
        let sel = selected.get();
        let pks = partition_keys.get();
        let t = tags.get();
        let (ns, name) = loc.get();
        let verb = verb.get();
        async move {
            let tags_opt = if t.is_empty() { None } else { Some(t) };
            if pks.len() > BACKFILL_THRESHOLD {
                let r = launch_backfill(ns, name, Some(sel), pks, tags_opt, None, verb).await?;
                return Ok::<_, ServerFnError>(DialogOutcome::Backfill(r.backfill_id));
            }
            // ≤2 keys → a run each; empty pks (unpartitioned / None picker) → one
            // keyless run. Both are the same loop over `Option<key>`.
            let keys = if pks.is_empty() {
                vec![None]
            } else {
                pks.into_iter().map(Some).collect::<Vec<_>>()
            };
            let mut run_id = String::new();
            for pk in keys {
                run_id = match &verb {
                    Some(verb) => {
                        trigger_action(
                            ns.clone(),
                            name.clone(),
                            verb.clone(),
                            sel.clone(),
                            pk,
                            tags_opt.clone(),
                        )
                        .await?
                    }
                    None => {
                        trigger_materialize(
                            ns.clone(),
                            name.clone(),
                            Some(sel.clone()),
                            pk,
                            tags_opt.clone(),
                        )
                        .await?
                        .run_id
                    }
                };
            }
            Ok(DialogOutcome::Run(run_id))
        }
    });

    let pending = materialize_action.pending();

    Effect::new(move || {
        if let Some(Ok(outcome)) = materialize_action.value().get() {
            show.set(false);
            let rel = match outcome {
                DialogOutcome::Run(id) if !id.is_empty() => Some(format!("runs/{id}")),
                DialogOutcome::Backfill(id) if !id.is_empty() => Some(format!("backfills/{id}")),
                _ => None,
            };
            if let Some(rel) = rel {
                let (ns, name) = loc.get();
                set_nav_to.set(Some(loc_path(&ns, &name, &rel)));
            }
        }
    });

    let add_tag = move |_| {
        let k = tag_key.get();
        let v = tag_val.get();
        if !k.is_empty() {
            set_tags.update(|t| t.push((k, v)));
            set_tag_key.set(String::new());
            set_tag_val.set(String::new());
        }
    };

    // Picker prop is optional; default to None when absent.
    let picker_signal: Signal<JobPartitionPicker> =
        picker.unwrap_or_else(|| Signal::derive(|| JobPartitionPicker::None));

    // Memo, not Signal::derive — read from six places per render, and the
    // sibling execute_job_dialog already memoizes the same predicate.
    let is_partitioned =
        Memo::new(move |_| !matches!(picker_signal.get(), JobPartitionPicker::None));
    let summary = Signal::derive(move || {
        launch_summary(
            selected.get().len(),
            partition_keys.get().len(),
            is_partitioned.get(),
        )
    });
    let submit_label = Signal::derive(move || {
        if pending.get() {
            "Submitting…".to_string()
        } else if is_partitioned.get() && partition_keys.get().len() > BACKFILL_THRESHOLD {
            "Launch backfill".to_string()
        } else {
            verb.get().unwrap_or_else(|| "Materialize".to_string())
        }
    });

    view! {
        <Show when=move || show.get()>
            <div class="modal-overlay" on:click=move |_| show.set(false)>
                // Only the partition picker needs the second column; an
                // unpartitioned selection would just leave it empty.
                <div
                    class=move || if is_partitioned.get() {
                        "modal-content modal-content--wide"
                    } else {
                        "modal-content"
                    }
                    on:click=move |ev| ev.stop_propagation()
                >
                    <div class="modal-header">
                        <h2>{move || verb.get().unwrap_or_else(|| "Materialize".to_string())}</h2>
                        <button class="btn btn-small" on:click=move |_| show.set(false)>"x"</button>
                    </div>

                    <Show when=move || destructive.get()>
                        <p class="text-error mat-dialog-warning">
                            "Clears materialization state for the selected assets."
                        </p>
                    </Show>

                    <div class="modal-body mat-dialog-body">
                        <div class=move || if is_partitioned.get() {
                            "mat-dialog-cols"
                        } else {
                            "mat-dialog-cols mat-dialog-cols--single"
                        }>
                            <div class="mat-dialog-col">
                                <div class="mat-dialog-col-head">
                                    <label>"Assets"</label>
                                    <span class="mat-dialog-col-actions">
                                        <button
                                            class="bulk-link-btn"
                                            on:click=move |_| set_selected.set(asset_keys.get())
                                        >"Select all"</button>
                                        <span class="bulk-sep">"·"</span>
                                        <button
                                            class="bulk-link-btn"
                                            on:click=move |_| set_selected.set(Vec::new())
                                        >"Clear"</button>
                                    </span>
                                </div>
                                <div class="mat-dialog-asset-list">
                                    {
                                        {move || {
                                            let meta = records.get();
                                            asset_keys.get().into_iter().map(|key| {
                                                let k_checked = key.clone();
                                                let k_toggle = key.clone();
                                                let checked = move || selected.get().contains(&k_checked);
                                                let record = meta.get(&key);
                                                let (dot_cls, status_word) = status_bits(record);
                                                let group = record.and_then(|r| r.asset_group.clone());
                                                let last_ts = record.and_then(|r| r.last_timestamp);
                                                let sub_parts: Vec<String> = [group, (!status_word.is_empty()).then(|| status_word.to_string())]
                                                    .into_iter()
                                                    .flatten()
                                                    .collect();
                                                view! {
                                                    <label class="mat-dialog-asset-row">
                                                        <input
                                                            class="asset-row-check"
                                                            type="checkbox"
                                                            prop:checked=checked
                                                            on:change=move |_| {
                                                                let k = k_toggle.clone();
                                                                set_selected.update(|s| {
                                                                    if s.contains(&k) {
                                                                        s.retain(|x| x != &k);
                                                                    } else {
                                                                        s.push(k);
                                                                    }
                                                                });
                                                            }
                                                        />
                                                        <span class=format!("mat-dialog-dot {dot_cls}") title=status_word></span>
                                                        <span class="mat-dialog-asset-text">
                                                            <span class="mat-dialog-asset-name">{key.clone()}</span>
                                                            <span class="mat-dialog-asset-sub">
                                                                {sub_parts.iter().map(|s| {
                                                                    view! { <>{s.clone()}" · "</> }
                                                                }).collect::<Vec<_>>()}
                                                                <crate::now::RelTimeOpt ts=last_ts fallback="never materialized"/>
                                                            </span>
                                                        </span>
                                                    </label>
                                                }
                                            }).collect::<Vec<_>>()
                                        }}
                                    }
                                </div>
                            </div>

                            <div class="mat-dialog-col">
                                <Show
                                    when=move || is_partitioned.get()
                                    fallback=move || view! {
                                        <div class="form-group">
                                            <label>"Partitions"</label>
                                            <div class="mat-dialog-note">
                                                "Not partitioned — one run covers the whole selection."
                                            </div>
                                        </div>
                                    }
                                >
                                    <PartitionPicker picker=picker_signal selected=partition_keys reset=show/>
                                </Show>

                                <div class="form-group">
                                    <label>"Tags"</label>
                                    <div class="tag-input-row">
                                        <input
                                            type="text"
                                            class="form-input form-input-small"
                                            placeholder="Key"
                                            prop:value=move || tag_key.get()
                                            on:input=move |ev| {
                                                set_tag_key.set(event_target_value(&ev));
                                            }
                                        />
                                        <input
                                            type="text"
                                            class="form-input form-input-small"
                                            placeholder="Value"
                                            prop:value=move || tag_val.get()
                                            on:input=move |ev| {
                                                set_tag_val.set(event_target_value(&ev));
                                            }
                                        />
                                        <button class="btn btn-small" on:click=add_tag>"Add"</button>
                                    </div>
                                    <div class="tag-list">
                                        {move || tags.get().into_iter().enumerate().map(|(i, (k, v))| {
                                            view! {
                                                <span class="tag">
                                                    {format!("{k}={v}")}
                                                    <button class="tag-remove" on:click=move |_| {
                                                        set_tags.update(|t| { t.remove(i); });
                                                    }>"x"</button>
                                                </span>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                </div>
                            </div>
                        </div>

                        {move || materialize_action.value().get().and_then(|r| r.err()).map(|e| {
                            view! { <div class="error-msg">{format!("{e}")}</div> }
                        })}
                    </div>

                    <div class="modal-footer mat-dialog-footer">
                        <div class="mat-dialog-summary">{move || summary.get()}</div>
                        <div class="mat-dialog-actions">
                            <button class="btn" on:click=move |_| show.set(false)>"Cancel"</button>
                            <button
                                class=move || if destructive.get() {
                                    "btn btn-danger"
                                } else {
                                    "btn btn-primary"
                                }
                                on:click=move |_| { materialize_action.dispatch(()); }
                                disabled=move || {
                                    if pending.get() || selected.get().is_empty() {
                                        return true;
                                    }
                                    is_partitioned.get() && partition_keys.get().is_empty()
                                }
                            >
                                {move || submit_label.get()}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </Show>

        {move || nav_to.get().map(|path| view! {
            <leptos_router::components::Redirect path={path}/>
        })}
    }
}

#[cfg(test)]
mod tests {
    use super::{launch_summary, status_bits};

    #[test]
    fn status_bits_covers_every_arm() {
        use crate::types::{AssetRecord, StaleStatus};
        let record = |s: StaleStatus| AssetRecord {
            asset_key: "a".into(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            pool: vec![],
            stale_status: s,
        };
        let word = |r: Option<&AssetRecord>| status_bits(r).1;
        assert_eq!(word(Some(&record(StaleStatus::UpToDate))), "up to date");
        assert_eq!(word(Some(&record(StaleStatus::Stale))), "stale");
        assert_eq!(word(Some(&record(StaleStatus::Missing))), "missing");
        // Unlabeled ≠ missing: an unloaded record shows no status word.
        assert_eq!(word(None), "");
    }

    #[test]
    fn empty_selection_reads_as_nothing() {
        assert_eq!(launch_summary(0, 0, false), "Nothing selected");
        assert_eq!(launch_summary(0, 5, true), "Nothing selected");
    }

    #[test]
    fn unpartitioned_is_always_one_run() {
        assert_eq!(launch_summary(1, 0, false), "1 asset · 1 run");
        assert_eq!(launch_summary(3, 0, false), "3 assets · 1 run");
    }

    #[test]
    fn partitioned_without_keys_asks_for_one() {
        assert_eq!(launch_summary(2, 0, true), "2 assets · select a partition");
    }

    #[test]
    fn one_run_per_partition_up_to_the_threshold() {
        assert_eq!(launch_summary(1, 1, true), "1 asset · 1 partition · 1 run");
        assert_eq!(
            launch_summary(3, 2, true),
            "3 assets · 2 partitions · 2 runs"
        );
    }

    #[test]
    fn past_the_threshold_it_is_a_backfill() {
        assert_eq!(
            launch_summary(3, 3, true),
            "3 assets · 3 partitions · 1 backfill"
        );
        assert_eq!(
            launch_summary(1, 400, true),
            "1 asset · 400 partitions · 1 backfill"
        );
    }
}
