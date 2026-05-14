//! Browser-based component tests for `LocationSwitcher`.
//!
//! `LocationSwitcher` reads `use_current_location()` (parses the
//! current `/locations/<ns>/<name>/...` URL) and renders `<A>` links to
//! sibling code locations. Both pieces require a `<Router>` ancestor —
//! tests use the `nav_to` helper to set the URL before mounting and
//! then mount the switcher inside a transparent `<Router>`.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::location_switcher::LocationSwitcher;
use rivers_ui::types::CodeLocationEntry;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn entry(ns: &str, name: &str, phase: &str) -> CodeLocationEntry {
    CodeLocationEntry {
        namespace: ns.to_string(),
        name: name.to_string(),
        grpc_endpoint: format!("{name}.{ns}.svc:3001"),
        image: "demo:latest".into(),
        module: "demo.pipeline".into(),
        phase: phase.to_string(),
        observed_generation: 1,
        identity: format!("{ns}/{name}"),
    }
}

fn mount_switcher(entries: Vec<CodeLocationEntry>) -> web_sys::HtmlElement {
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <LocationSwitcher
                    entries=entries.clone()
                    collapsed=Signal::derive(|| false)
                />
            </Router>
        }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn trigger_label_uses_active_name_when_single_namespace() {
    nav_to("/locations/default/analytics/runs");
    let host = mount_switcher(vec![
        entry("default", "analytics", "Ready"),
        entry("default", "ingest", "Ready"),
    ]);

    let label = query_one(&host, ".loc-switcher-label")
        .text_content()
        .unwrap();
    assert_eq!(
        label, "analytics",
        "single-namespace cluster should show only the name"
    );
}

#[wasm_bindgen_test]
fn trigger_label_prefixes_namespace_when_multiple_namespaces() {
    nav_to("/locations/team-data/analytics/runs");
    let host = mount_switcher(vec![
        entry("team-data", "analytics", "Ready"),
        entry("team-eng", "ingest", "Ready"),
    ]);

    let label = query_one(&host, ".loc-switcher-label")
        .text_content()
        .unwrap();
    assert!(
        label.contains("team-data") && label.contains("analytics"),
        "multi-namespace cluster should prefix the namespace, got: {label}"
    );
}

#[wasm_bindgen_test]
fn unknown_active_location_falls_back_to_select_label() {
    nav_to("/locations/missing/nope");
    let host = mount_switcher(vec![entry("default", "analytics", "Ready")]);

    let label = query_one(&host, ".loc-switcher-label")
        .text_content()
        .unwrap();
    assert_eq!(label, "Select location");
}

#[wasm_bindgen_test]
fn ready_phase_drives_ready_dot_class() {
    nav_to("/locations/default/analytics");
    let host = mount_switcher(vec![entry("default", "analytics", "Ready")]);
    let dot = query_one(&host, ".loc-switcher-trigger .locations-dot");
    assert!(
        dot.class_name().contains("locations-dot--ready"),
        "got: {}",
        dot.class_name()
    );
}

#[wasm_bindgen_test]
fn failed_phase_drives_failed_dot_class() {
    nav_to("/locations/default/broken");
    let host = mount_switcher(vec![entry("default", "broken", "Failed")]);
    let dot = query_one(&host, ".loc-switcher-trigger .locations-dot");
    assert!(dot.class_name().contains("locations-dot--failed"));
}

#[wasm_bindgen_test]
fn pending_phase_drives_pending_dot_class() {
    nav_to("/locations/default/queued");
    let host = mount_switcher(vec![entry("default", "queued", "Pending")]);
    let dot = query_one(&host, ".loc-switcher-trigger .locations-dot");
    assert!(dot.class_name().contains("locations-dot--pending"));
}

#[wasm_bindgen_test]
async fn clicking_trigger_opens_popover_with_one_link_per_entry() {
    nav_to("/locations/default/analytics/jobs");
    let host = mount_switcher(vec![
        entry("default", "analytics", "Ready"),
        entry("default", "ingest", "Ready"),
        entry("default", "etl", "Pending"),
    ]);

    // Popover starts hidden via inline `display: none`.
    let popover = query_one(&host, ".loc-switcher-popover");
    assert!(
        popover
            .get_attribute("style")
            .unwrap_or_default()
            .contains("none")
    );

    click(&query_one(&host, ".loc-switcher-trigger"), false);
    flush_effects().await;

    let popover = query_one(&host, ".loc-switcher-popover");
    assert!(
        popover
            .get_attribute("style")
            .unwrap_or_default()
            .contains("block")
    );

    // One <A class="locations-item"> per entry.
    let items = query_all(&host, ".locations-item");
    assert_eq!(items.len(), 3);
}

#[wasm_bindgen_test]
async fn active_location_item_carries_data_active_true() {
    nav_to("/locations/default/analytics/runs");
    let host = mount_switcher(vec![
        entry("default", "analytics", "Ready"),
        entry("default", "ingest", "Ready"),
    ]);

    click(&query_one(&host, ".loc-switcher-trigger"), false);
    flush_effects().await;

    let items = query_all(&host, ".locations-item");
    let actives: Vec<String> = items
        .iter()
        .map(|el| el.get_attribute("data-active").unwrap_or_default())
        .collect();
    // Exactly one item should be marked active and it should be analytics.
    let true_count = actives.iter().filter(|s| s == &"true").count();
    assert_eq!(true_count, 1, "got: {:?}", actives);

    let active_item = items
        .iter()
        .find(|el| el.get_attribute("data-active").unwrap_or_default() == "true")
        .unwrap();
    assert!(active_item.text_content().unwrap().contains("analytics"));
}

#[wasm_bindgen_test]
async fn item_href_preserves_current_section_suffix() {
    // A click on a sibling location while on `/runs` should keep the
    // user on `/runs` (router-preserving navigation).
    nav_to("/locations/default/analytics/runs/abc-123");
    let host = mount_switcher(vec![
        entry("default", "analytics", "Ready"),
        entry("default", "ingest", "Ready"),
    ]);

    click(&query_one(&host, ".loc-switcher-trigger"), false);
    flush_effects().await;

    let items = query_all(&host, ".locations-item");
    let ingest = items
        .iter()
        .find(|el| el.text_content().unwrap_or_default().contains("ingest"))
        .expect("ingest item should be in the popover");
    let href = ingest.get_attribute("href").unwrap_or_default();
    assert_eq!(
        href, "/locations/default/ingest/runs/abc-123",
        "section suffix should carry over"
    );
}

#[wasm_bindgen_test]
async fn entries_are_grouped_by_namespace() {
    nav_to("/locations/team-a/foo");
    let host = mount_switcher(vec![
        entry("team-a", "foo", "Ready"),
        entry("team-b", "bar", "Ready"),
        entry("team-a", "baz", "Ready"),
    ]);

    click(&query_one(&host, ".loc-switcher-trigger"), false);
    flush_effects().await;

    // Two namespace groups, each with its own header.
    assert_eq!(query_all(&host, ".locations-group").len(), 2);
    let ns_labels: Vec<String> = query_all(&host, ".locations-ns")
        .iter()
        .map(|el| el.text_content().unwrap_or_default())
        .collect();
    // BTreeMap iteration → namespace labels are in alphabetical order.
    assert_eq!(ns_labels, vec!["team-a", "team-b"]);
}
