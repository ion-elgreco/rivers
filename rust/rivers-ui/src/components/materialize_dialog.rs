//! Materialize confirmation dialog.
//!
//! ≤2 selected partitions fire one `trigger_materialize` run each; a larger
//! selection lands as a single backfill over the assets + chosen keys.

use leptos::prelude::*;

use crate::components::partition_picker::PartitionPicker;
use crate::helpers::JobPartitionPicker;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::{launch_backfill, trigger_materialize};
use crate::types::SubmitPartitionKey;

/// Above this many selected partitions, submit one backfill instead of a run each.
const BACKFILL_THRESHOLD: usize = 2;

/// What a submit produced, so the success Effect navigates to the right page.
#[derive(Clone)]
enum DialogOutcome {
    Run(String),
    Backfill(String),
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
) -> impl IntoView {
    let (selected, set_selected) = signal(Vec::<String>::new());
    let partition_keys = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let (tag_key, set_tag_key) = signal(String::new());
    let (tag_val, set_tag_val) = signal(String::new());
    let (tags, set_tags) = signal(Vec::<(String, String)>::new());
    let (nav_to, set_nav_to) = signal(Option::<String>::None);

    Effect::new(move || {
        if show.get() {
            set_selected.set(asset_keys.get());
        }
    });

    let loc = use_current_location();
    let materialize_action = Action::new(move |_: &()| {
        let sel = selected.get();
        let pks = partition_keys.get();
        let t = tags.get();
        let (ns, name) = loc.get();
        async move {
            let tags_opt = if t.is_empty() { None } else { Some(t) };
            if pks.len() > BACKFILL_THRESHOLD {
                let r = launch_backfill(ns, name, Some(sel), pks, tags_opt, None).await?;
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
                run_id = trigger_materialize(
                    ns.clone(),
                    name.clone(),
                    Some(sel.clone()),
                    pk,
                    tags_opt.clone(),
                )
                .await?
                .run_id;
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

    view! {
        <Show when=move || show.get()>
            <div class="modal-overlay" on:click=move |_| show.set(false)>
                <div class="modal-content" on:click=move |ev| ev.stop_propagation()>
                    <div class="modal-header">
                        <h2>"Materialize Assets"</h2>
                        <button class="btn btn-small" on:click=move |_| show.set(false)>"x"</button>
                    </div>

                    <div class="modal-body">
                        <div class="form-group">
                            <label>"Assets"</label>
                            <div class="checkbox-list">
                                {move || {
                                    asset_keys.get().into_iter().map(|key| {
                                        let k = key.clone();
                                        let k2 = key.clone();
                                        let checked = move || selected.get().contains(&k);
                                        view! {
                                            <label class="checkbox-item">
                                                <input
                                                    type="checkbox"
                                                    checked=checked
                                                    on:change=move |_| {
                                                        let k = k2.clone();
                                                        set_selected.update(|s| {
                                                            if s.contains(&k) {
                                                                s.retain(|x| x != &k);
                                                            } else {
                                                                s.push(k);
                                                            }
                                                        });
                                                    }
                                                />
                                                <span>{key}</span>
                                            </label>
                                        }
                                    }).collect::<Vec<_>>()
                                }}
                            </div>
                        </div>

                        <PartitionPicker picker=picker_signal selected=partition_keys reset=show/>

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

                        {move || materialize_action.value().get().and_then(|r| r.err()).map(|e| {
                            view! { <div class="error-msg">{format!("{e}")}</div> }
                        })}
                    </div>

                    <div class="modal-footer">
                        <button class="btn" on:click=move |_| show.set(false)>"Cancel"</button>
                        <button
                            class="btn btn-primary"
                            on:click=move |_| { materialize_action.dispatch(()); }
                            disabled=move || {
                                if pending.get() || selected.get().is_empty() {
                                    return true;
                                }
                                let needs_partition = !matches!(
                                    picker_signal.get(),
                                    JobPartitionPicker::None
                                );
                                needs_partition && partition_keys.get().is_empty()
                            }
                        >
                            {move || if pending.get() {
                                "Submitting...".to_string()
                            } else {
                                let n = if matches!(picker_signal.get(), JobPartitionPicker::None) {
                                    1
                                } else {
                                    partition_keys.get().len()
                                };
                                if n > BACKFILL_THRESHOLD {
                                    format!("Backfill {n} partitions")
                                } else if n > 1 {
                                    format!("Materialize {n} runs")
                                } else {
                                    "Materialize".to_string()
                                }
                            }}
                        </button>
                    </div>
                </div>
            </div>
        </Show>

        {move || nav_to.get().map(|path| view! {
            <leptos_router::components::Redirect path={path}/>
        })}
    }
}
