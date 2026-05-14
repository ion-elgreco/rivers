//! Materialize confirmation dialog.
//!
//! Each cartesian-expanded partition fires its own `trigger_materialize`
//! call so a Multi selection lands as N independent runs.

use leptos::prelude::*;

use crate::components::partition_picker::PartitionPicker;
use crate::helpers::JobPartitionPicker;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::actions::trigger_materialize;
use crate::types::SubmitPartitionKey;

#[component]
pub fn MaterializeDialog(
    #[prop(into)] show: RwSignal<bool>,
    #[prop(into)] asset_keys: Signal<Vec<String>>,
    /// `JobPartitionPicker::None` omits the partition section entirely and
    /// the dialog submits a single unpartitioned run. Otherwise the
    /// shared partition picker renders a flat list (SingleDim) or one
    /// labelled selector per dimension (Multi); the cartesian product
    /// of selections fires one materialize per combination.
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
            // Empty pks means either no partitioned assets, or the
            // user hasn't picked any (None-picker case). Fire a single
            // unpartitioned materialize; otherwise one per key.
            let mut last: Option<crate::server_fns::actions::MaterializeResult> = None;
            if pks.is_empty() {
                last = Some(
                    trigger_materialize(
                        ns.clone(),
                        name.clone(),
                        Some(sel.clone()),
                        None,
                        tags_opt.clone(),
                    )
                    .await?,
                );
            } else {
                for pk in pks {
                    last = Some(
                        trigger_materialize(
                            ns.clone(),
                            name.clone(),
                            Some(sel.clone()),
                            Some(pk),
                            tags_opt.clone(),
                        )
                        .await?,
                    );
                }
            }
            Ok::<_, ServerFnError>(last.expect("at least one materialize call ran"))
        }
    });

    let pending = materialize_action.pending();

    Effect::new(move || {
        if let Some(Ok(result)) = materialize_action.value().get() {
            show.set(false);
            if !result.run_id.is_empty() {
                let (ns, name) = loc.get();
                set_nav_to.set(Some(loc_path(
                    &ns,
                    &name,
                    &format!("runs/{}", result.run_id),
                )));
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
                                "Materializing...".to_string()
                            } else {
                                let n = if matches!(picker_signal.get(), JobPartitionPicker::None) {
                                    1
                                } else {
                                    partition_keys.get().len()
                                };
                                if n > 1 { format!("Materialize {n} runs") } else { "Materialize".to_string() }
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
