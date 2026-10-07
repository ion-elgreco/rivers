//! Global search overlay.

use leptos::prelude::*;

use crate::components::live::use_definitions;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::assets::get_assets;
use crate::server_fns::automation::{get_jobs, get_schedules, get_sensors};
use crate::server_fns::runs::get_runs;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct SearchEntry {
    label: String,
    category: String,
    href: String,
}

/// Cmd+K search overlay. Pass `open` to open it from another control.
#[component]
pub fn GlobalSearch(#[prop(optional)] open: RwSignal<bool>) -> impl IntoView {
    let (query, set_query) = signal(String::new());
    let (selected_idx, set_selected_idx) = signal(0usize);
    Effect::new(move |_| {
        if open.get() {
            set_query.set(String::new());
            set_selected_idx.set(0);
        }
    });
    let input_ref = NodeRef::<leptos::html::Input>::new();
    Effect::new(move |_| {
        if let Some(input) = input_ref.get() {
            let _ = input.focus();
        }
    });

    // Search index loads client-side only (no SSR needed for an interactive overlay).
    // Re-fetches on location switch — every gRPC-backed search source is
    // per-location — and when the code location reloads.
    let loc = use_current_location();
    let definitions = use_definitions();
    let search_index = LocalResource::new(move || {
        let (ns, name) = loc.get();
        definitions.track();
        async move {
            let mut entries = Vec::new();

            if let Ok(assets) = get_assets(ns.clone(), name.clone(), None, None, None).await {
                for a in assets {
                    entries.push(SearchEntry {
                        label: a.asset_key.clone(),
                        category: "Asset".to_string(),
                        href: loc_path(&ns, &name, &format!("assets/{}", a.asset_key)),
                    });
                }
            }
            if let Ok(jobs) = get_jobs(ns.clone(), name.clone()).await {
                for j in jobs {
                    entries.push(SearchEntry {
                        label: j.name.clone(),
                        category: "Job".to_string(),
                        href: loc_path(&ns, &name, &format!("jobs/{}", j.name)),
                    });
                }
            }
            if let Ok(schedules) = get_schedules(ns.clone(), name.clone()).await {
                for s in schedules {
                    entries.push(SearchEntry {
                        label: s.name.clone(),
                        category: "Schedule".to_string(),
                        href: loc_path(&ns, &name, &format!("automation/schedules/{}", s.name)),
                    });
                }
            }
            if let Ok(sensors) = get_sensors(ns.clone(), name.clone()).await {
                for s in sensors {
                    entries.push(SearchEntry {
                        label: s.name.clone(),
                        category: "Sensor".to_string(),
                        href: loc_path(&ns, &name, &format!("automation/sensors/{}", s.name)),
                    });
                }
            }
            if let Ok(runs) = get_runs(Some(100), None).await {
                for r in runs {
                    entries.push(SearchEntry {
                        label: format!(
                            "{} ({})",
                            crate::helpers::short_id(&r.run_id, 8),
                            r.job_name.as_deref().unwrap_or("ad-hoc")
                        ),
                        category: "Run".to_string(),
                        href: loc_path(&ns, &name, &format!("runs/{}", r.run_id)),
                    });
                }
            }
            entries
        }
    });

    let filtered = move || {
        let q = query.get().to_lowercase();
        let entries = search_index.get().unwrap_or_default();
        if q.is_empty() {
            return entries;
        }
        entries
            .into_iter()
            .filter(|e| {
                e.label.to_lowercase().contains(&q) || e.category.to_lowercase().contains(&q)
            })
            .collect::<Vec<_>>()
    };

    let navigate = leptos_router::hooks::use_navigate();

    let shortcut = window_event_listener(leptos::ev::keydown, move |ev| {
        if (ev.meta_key() || ev.ctrl_key()) && ev.key() == "k" {
            ev.prevent_default();
            open.update(|o| *o = !*o);
        } else if ev.key() == "Escape" {
            open.set(false);
        }
    });
    on_cleanup(move || shortcut.remove());

    view! {
        <Show when=move || open.get()>
            <div class="modal-overlay search-overlay" on:click=move |_| open.set(false)>
                <div class="search-modal" on:click=move |ev| ev.stop_propagation()>
                    <div class="search-input-container">
                        <input
                            type="text"
                            class="search-input"
                            placeholder="Search assets, jobs, schedules, sensors, runs…"
                            node_ref=input_ref
                            prop:value=move || query.get()
                            on:input=move |ev| {
                                set_query.set(event_target_value(&ev));
                                set_selected_idx.set(0);
                            }
                            on:keydown={
                                let navigate = navigate.clone();
                                move |ev| {
                                    let results = filtered();
                                    match ev.key().as_str() {
                                        "ArrowDown" => {
                                            ev.prevent_default();
                                            set_selected_idx.update(|i| {
                                                if *i + 1 < results.len() { *i += 1; }
                                            });
                                        }
                                        "ArrowUp" => {
                                            ev.prevent_default();
                                            set_selected_idx.update(|i| {
                                                if *i > 0 { *i -= 1; }
                                            });
                                        }
                                        "Enter" => {
                                            let idx = selected_idx.get();
                                            if let Some(entry) = results.get(idx) {
                                                open.set(false);
                                                navigate(&entry.href, Default::default());
                                            }
                                        }
                                        "Escape" => open.set(false),
                                        _ => {}
                                    }
                                }
                            }
                        />
                    </div>
                    <div class="search-results">
                        {move || {
                            let results = filtered();
                            if results.is_empty() {
                                return view! { <div class="search-empty">"No results found."</div> }.into_any();
                            }
                            let idx = selected_idx.get();
                            view! {
                                <div class="search-result-list">
                                    {results.into_iter().enumerate().map(|(i, entry)| {
                                        let href = entry.href.clone();
                                        let class = if i == idx { "search-result-item active" } else { "search-result-item" };
                                        view! {
                                            <a href={href} class={class} on:click=move |_| open.set(false)>
                                                <span class="search-result-category">{entry.category}</span>
                                                <span class="search-result-label">{entry.label}</span>
                                            </a>
                                        }
                                    }).collect::<Vec<_>>()}
                                </div>
                            }.into_any()
                        }}
                    </div>
                    <div class="search-footer">
                        <div class="search-footer-hints">
                            <span class="search-footer-hint"><kbd class="kbd">"↑"</kbd><kbd class="kbd">"↓"</kbd>" navigate"</span>
                            <span class="search-footer-hint"><kbd class="kbd">"↵"</kbd>" select"</span>
                            <span class="search-footer-hint"><kbd class="kbd">"esc"</kbd>" close"</span>
                        </div>
                        <span class="search-footer-brand">"rivers · command palette"</span>
                    </div>
                </div>
            </div>
        </Show>
    }
}
