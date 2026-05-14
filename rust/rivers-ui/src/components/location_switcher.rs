//! Sidebar code-location switcher (dropdown).
//!
//! Section-preserving navigation (clicking a sibling on `/runs` keeps you
//! on `/runs`) is delegated to `loc_path` + `section_of` from `crate::loc`.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_location;

use crate::loc::{loc_path, section_of, use_current_location};
use crate::types::CodeLocationEntry;

const ICON_CHEVRON: &str = r#"<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>"#;

fn phase_class(phase: &str) -> &'static str {
    match phase {
        "Ready" => "locations-dot locations-dot--ready",
        "Failed" => "locations-dot locations-dot--failed",
        _ => "locations-dot locations-dot--pending",
    }
}

#[component]
pub fn LocationSwitcher(entries: Vec<CodeLocationEntry>, collapsed: Signal<bool>) -> impl IntoView {
    let mut grouped: std::collections::BTreeMap<String, Vec<CodeLocationEntry>> =
        Default::default();
    for e in &entries {
        grouped
            .entry(e.namespace.clone())
            .or_default()
            .push(e.clone());
    }
    for v in grouped.values_mut() {
        v.sort_by(|a, b| a.name.cmp(&b.name));
    }
    // Show the namespace prefix in the trigger only when the cluster actually
    // has locations in 2+ namespaces — single-namespace clusters don't benefit
    // from the visual noise of always-prefixed names.
    let show_ns_prefix = grouped.len() > 1;

    let location = use_location();
    let active_loc = use_current_location();

    let entries_for_trigger = entries.clone();
    let active_entry = move || -> Option<CodeLocationEntry> {
        let (ns, name) = active_loc.get();
        entries_for_trigger
            .iter()
            .find(|e| e.namespace == ns && e.name == name)
            .cloned()
    };

    let trigger_label = {
        let active_entry = active_entry.clone();
        move || -> String {
            match active_entry() {
                Some(e) if show_ns_prefix => format!("{} / {}", e.namespace, e.name),
                Some(e) => e.name,
                None => "Select location".to_string(),
            }
        }
    };

    let trigger_dot_class = {
        let active_entry = active_entry.clone();
        move || -> &'static str {
            match active_entry() {
                Some(e) => phase_class(&e.phase),
                None => phase_class("Pending"),
            }
        }
    };

    let trigger_title = {
        let active_entry = active_entry.clone();
        move || -> String {
            match active_entry() {
                Some(e) => format!("{}/{} — {}", e.namespace, e.name, e.phase),
                None => "Select code location".to_string(),
            }
        }
    };

    let open = RwSignal::new(false);

    let groups_view = grouped
        .into_iter()
        .map(|(ns, locs)| {
            let items = locs
                .into_iter()
                .map(|l| {
                    let dot_cls = phase_class(&l.phase);
                    let title = format!("{}/{} — {}", l.namespace, l.name, l.phase);
                    let target = std::sync::Arc::new((l.namespace.clone(), l.name.clone()));
                    let href = {
                        let target = target.clone();
                        let location = location.clone();
                        move || {
                            let path = location.pathname.get();
                            let suffix = section_of(&path).to_string();
                            loc_path(&target.0, &target.1, &suffix)
                        }
                    };
                    let is_active = {
                        let target = target.clone();
                        move || {
                            let (cur_ns, cur_name) = active_loc.get();
                            cur_ns == target.0 && cur_name == target.1
                        }
                    };
                    view! {
                        <A
                            href=href
                            attr:class="locations-item"
                            attr:title=title
                            attr:data-active=move || is_active().to_string()
                            on:click=move |_| open.set(false)
                        >
                            <span class=dot_cls></span>
                            <span class="locations-item-name">{l.name}</span>
                        </A>
                    }
                })
                .collect_view();
            view! {
                <div class="locations-group">
                    <div class="locations-ns">{ns}</div>
                    {items}
                </div>
            }
        })
        .collect_view();

    view! {
        <div class="loc-switcher">
            <button
                class="loc-switcher-trigger"
                class:loc-switcher-trigger--collapsed=move || collapsed.get()
                class:loc-switcher-trigger--open=move || open.get()
                on:click=move |_| open.update(|o| *o = !*o)
                aria-haspopup="listbox"
                aria-expanded=move || open.get().to_string()
                title=trigger_title
            >
                <span class=trigger_dot_class></span>
                <span class="loc-switcher-label" class:nav-label--hidden=move || collapsed.get()>
                    {trigger_label}
                </span>
                <span
                    class="loc-switcher-chevron"
                    class:loc-switcher-chevron--open=move || open.get()
                    class:nav-label--hidden=move || collapsed.get()
                    inner_html=ICON_CHEVRON
                ></span>
            </button>
            <Show when=move || open.get()>
                <div class="loc-switcher-backdrop" on:click=move |_| open.set(false)></div>
            </Show>
            <div
                class="loc-switcher-popover"
                class:loc-switcher-popover--collapsed=move || collapsed.get()
                style:display=move || if open.get() { "block" } else { "none" }
            >
                <div class="locations-groups">{groups_view}</div>
            </div>
        </div>
    }
}
