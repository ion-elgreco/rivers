//! Global search overlay.

use leptos::prelude::*;

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

#[component]
pub fn GlobalSearch() -> impl IntoView {
    let (open, set_open) = signal(false);
    let (query, set_query) = signal(String::new());
    let (selected_idx, set_selected_idx) = signal(0usize);

    // Search index loads client-side only (no SSR needed for an interactive overlay).
    // Re-fetches on location switch — every gRPC-backed search source is per-location.
    let loc = use_current_location();
    let search_index = LocalResource::new(move || {
        let (ns, name) = loc.get();
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
                    let short_id = if r.run_id.len() > 8 {
                        format!("{}...", &r.run_id[..8])
                    } else {
                        r.run_id.clone()
                    };
                    entries.push(SearchEntry {
                        label: format!(
                            "{} ({})",
                            short_id,
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

    view! {
        // Keyboard shortcut listener (Cmd+K / Ctrl+K)
        <div
            class="global-search-trigger"
            on:keydown=move |ev| {
                if (ev.meta_key() || ev.ctrl_key()) && ev.key() == "k" {
                    ev.prevent_default();
                    set_open.update(|o| *o = !*o);
                    set_query.set(String::new());
                    set_selected_idx.set(0);
                }
                if ev.key() == "Escape" {
                    set_open.set(false);
                }
            }
            tabindex="-1"
            style="position: fixed; top: 0; left: 0; width: 0; height: 0; opacity: 0"
        ></div>

        <Show when=move || open.get()>
            <div class="modal-overlay search-overlay" on:click=move |_| set_open.set(false)>
                <div class="search-modal" on:click=move |ev| ev.stop_propagation()>
                    <div class="search-input-container">
                        <input
                            type="text"
                            class="search-input"
                            placeholder="Search assets, jobs, schedules, sensors, runs..."
                            autofocus
                            prop:value=move || query.get()
                            on:input=move |ev| {
                                set_query.set(event_target_value(&ev));
                                set_selected_idx.set(0);
                            }
                            on:keydown=move |ev| {
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
                                            let href = entry.href.clone();
                                            set_open.set(false);
                                            let _ = leptos_router::hooks::use_navigate()(
                                                &href,
                                                Default::default(),
                                            );
                                        }
                                    }
                                    "Escape" => set_open.set(false),
                                    _ => {}
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
                                            <a href={href} class={class} on:click=move |_| set_open.set(false)>
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
