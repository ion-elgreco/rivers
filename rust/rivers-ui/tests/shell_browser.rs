//! Browser-based component tests for `Shell` (the app layout).
//!
//! `Shell` calls the `list_code_locations()` server fn via `Resource`,
//! which 404s in tests. That's fine — we test the structural pieces
//! that always render: brand bar, sidebar nav (one `<a>` per section,
//! hrefs derived from `use_current_location`), and the collapse
//! toggle. The locations panel hits the `Transition` fallback or the
//! "error" branch.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::layout::Shell;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount_shell(path: &str) -> web_sys::HtmlElement {
    nav_to(path);
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        view! {
            <Router>
                <Shell>
                    <div class="test-page-marker">"page"</div>
                </Shell>
            </Router>
        }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
async fn shell_renders_layout_brand_and_main_content_slot() {
    let host = mount_shell("/locations/default/demo/runs");
    flush_effects().await;

    assert_eq!(query_all(&host, ".layout").len(), 1);
    assert_eq!(query_all(&host, "nav.sidebar").len(), 1);
    assert_eq!(query_all(&host, "main.main-content").len(), 1);

    let brand = query_one(&host, ".brand-text").text_content().unwrap();
    assert_eq!(brand, "rivers");
    let version = query_one(&host, ".brand-version").text_content().unwrap();
    assert!(
        version.starts_with("v"),
        "expected version to start with v, got: {version}"
    );

    // Children render inside main-content.
    assert_eq!(
        query_one(&host, "main.main-content .test-page-marker")
            .text_content()
            .unwrap(),
        "page"
    );
}

#[wasm_bindgen_test]
async fn shell_renders_one_nav_link_per_section() {
    let host = mount_shell("/locations/default/demo/runs");
    flush_effects().await;

    let labels: Vec<String> = query_all(&host, ".sidebar-nav a")
        .into_iter()
        .map(|el| {
            // Pull the visible label out of `.nav-label`.
            el.query_selector(".nav-label")
                .ok()
                .flatten()
                .and_then(|n| n.text_content())
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(
        labels,
        vec![
            "Overview",
            "Runs",
            "Backfills",
            "Assets",
            "Lineage",
            "Jobs",
            "Automation",
            "Pools",
            "Queue",
            "Deployment",
        ]
    );
}

#[wasm_bindgen_test]
async fn nav_link_hrefs_carry_active_location_prefix() {
    let host = mount_shell("/locations/team-a/proj/runs");
    flush_effects().await;

    let hrefs: Vec<String> = query_all(&host, ".sidebar-nav a")
        .into_iter()
        .map(|el| el.get_attribute("href").unwrap_or_default())
        .collect();

    // Overview points at the bare location root; siblings carry the suffix.
    assert_eq!(hrefs[0], "/locations/team-a/proj");
    assert_eq!(hrefs[1], "/locations/team-a/proj/runs");
    assert_eq!(hrefs[3], "/locations/team-a/proj/assets");
    assert_eq!(hrefs[6], "/locations/team-a/proj/automation");
}

#[wasm_bindgen_test]
async fn nav_link_for_current_section_picks_up_active_class() {
    let host = mount_shell("/locations/default/demo/jobs/loader");
    flush_effects().await;

    let links = query_all(&host, ".sidebar-nav a");
    let active: Vec<bool> = links
        .iter()
        .map(|el| el.class_name().contains("active"))
        .collect();

    // Exactly one link active — the Jobs section.
    assert_eq!(active.iter().filter(|b| **b).count(), 1);
    let active_idx = active.iter().position(|b| *b).unwrap();
    let active_label = links[active_idx]
        .query_selector(".nav-label")
        .unwrap()
        .unwrap()
        .text_content()
        .unwrap();
    assert_eq!(active_label, "Jobs");
}

#[wasm_bindgen_test]
async fn root_path_falls_back_to_slash_href_for_nav_links() {
    let host = mount_shell("/");
    flush_effects().await;

    // With no active location, every nav link's `href` collapses to `/`
    // so a click hits the redirect rather than a malformed
    // `/locations//runs`.
    let hrefs: Vec<String> = query_all(&host, ".sidebar-nav a")
        .into_iter()
        .map(|el| el.get_attribute("href").unwrap_or_default())
        .collect();
    assert!(
        hrefs.iter().all(|h| h == "/"),
        "expected every href to be `/`, got: {hrefs:?}"
    );
}

#[wasm_bindgen_test]
async fn collapse_toggle_flips_sidebar_collapsed_class() {
    let host = mount_shell("/locations/default/demo/runs");
    flush_effects().await;

    let sidebar = query_one(&host, "nav.sidebar");
    assert!(!sidebar.class_name().contains("sidebar--collapsed"));

    click(&query_one(&host, ".sidebar-toggle"), false);
    flush_effects().await;
    let sidebar = query_one(&host, "nav.sidebar");
    assert!(sidebar.class_name().contains("sidebar--collapsed"));

    click(&query_one(&host, ".sidebar-toggle"), false);
    flush_effects().await;
    let sidebar = query_one(&host, "nav.sidebar");
    assert!(!sidebar.class_name().contains("sidebar--collapsed"));
}

#[wasm_bindgen_test]
async fn search_hint_renders_with_cmd_k_kbd() {
    let host = mount_shell("/locations/default/demo");
    flush_effects().await;

    let hint = query_one(&host, ".search-hint").text_content().unwrap();
    assert!(hint.contains("Cmd+K"));
}
