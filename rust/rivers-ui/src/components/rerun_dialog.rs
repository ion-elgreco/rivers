//! Re-execute a run with an edited launch document.
//!
//! The editor opens on the run's stored document laid over the definitions'
//! defaults; the submit replaces the stored document for the new run.

use std::collections::HashMap;

use leptos::prelude::*;

use crate::components::config_editor::{ConfigEditor, use_launch_config, use_launch_resources};
use crate::helpers::close_on_navigation;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::mutations::rerun_run;
use crate::server_fns::overview::get_assets_info;
use crate::types::{AssetDefinitionInfo, RunRecord};

/// `location` owns the run: its definitions check the document.
#[component]
pub fn RerunConfigDialog(
    show: RwSignal<bool>,
    #[prop(into)] run: Signal<Option<RunRecord>>,
    #[prop(into)] location: Signal<(String, String)>,
) -> impl IntoView {
    let page_loc = use_current_location();
    close_on_navigation(show);
    let fetched = Resource::new(
        move || show.get().then(|| location.get()),
        |target| async move {
            match target {
                Some((ns, name)) => get_assets_info(ns, name).await.unwrap_or_default(),
                None => Vec::new(),
            }
        },
    );
    let fetched = crate::helpers::resource_value(fetched);
    let definitions = Signal::derive(move || {
        fetched
            .get()
            .unwrap_or_default()
            .into_iter()
            .map(|def| (def.asset_key.clone(), def))
            .collect::<HashMap<String, AssetDefinitionInfo>>()
    });
    let resources = use_launch_resources(location, show.into());
    let config = use_launch_config(
        Signal::derive(move || {
            run.with(|r| r.as_ref().map(|r| r.node_names.clone()))
                .unwrap_or_default()
        }),
        definitions,
        resources,
        Signal::derive(move || run.with(|r| r.as_ref().and_then(|r| r.action.clone()))),
        location,
    );
    let config_check = config.check;
    let stored = Signal::derive(move || run.with(|r| r.as_ref().and_then(|r| r.config.clone())));

    let error = RwSignal::new(None::<String>);
    let rerun = Action::new(move |input: &(String, String)| {
        let (run_id, document) = input.clone();
        async move { rerun_run(run_id, Some(document)).await }
    });
    let pending = rerun.pending();
    Effect::new(move || {
        if show.get() {
            error.set(None);
        }
    });
    let navigate = leptos_router::hooks::use_navigate();
    Effect::new(move || match rerun.value().get() {
        Some(Ok(r)) if !r.run_id.is_empty() => {
            show.set(false);
            let (ns, name) = page_loc.get_untracked();
            navigate(
                &loc_path(&ns, &name, &format!("runs/{}", r.run_id)),
                Default::default(),
            );
        }
        Some(Ok(_)) => error.set(Some("Re-execute returned no run id.".to_string())),
        Some(Err(e)) => error.set(Some(crate::helpers::err_text(&e))),
        None => {}
    });

    view! {
        <Show when=move || show.get()>
            <div class="modal-overlay" on:click=move |_| show.set(false)>
                <div class="modal-content" on:click=move |ev| ev.stop_propagation()>
                    <div class="modal-header">
                        <h2>"Re-execute with config"</h2>
                        <button
                            class="icon-btn"
                            on:click=move |_| show.set(false)
                            title="Close"
                            aria-label="Close"
                        >"×"</button>
                    </div>
                    <div class="modal-body">
                        <div class="form-group">
                            <label>"Run"</label>
                            <div class="grid-cell-mono">
                                {move || run.with(|r| r.as_ref().map(|r| crate::helpers::short_id(&r.run_id, 8)))}
                            </div>
                        </div>
                        <ConfigEditor
                            schema=config.schema
                            text=config.text
                            reset=show
                            check=config_check
                            base=stored
                        />
                        {move || error.get().map(|msg| view! {
                            <div class="error-msg">{msg}</div>
                        })}
                    </div>
                    <div class="modal-footer">
                        <button class="btn" on:click=move |_| show.set(false)>"Cancel"</button>
                        <button
                            class="btn btn-primary"
                            on:click=move |_| {
                                let Some(run_id) = run.with_untracked(|r| r.as_ref().map(|r| r.run_id.clone())) else {
                                    return;
                                };
                                error.set(None);
                                let document = config_check.get_untracked().payload.unwrap_or_default();
                                rerun.dispatch((run_id, document));
                            }
                            disabled=move || pending.get() || !config_check.get().issues.is_empty()
                        >
                            {move || if pending.get() { "Re-executing…" } else { "Re-execute" }}
                        </button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
