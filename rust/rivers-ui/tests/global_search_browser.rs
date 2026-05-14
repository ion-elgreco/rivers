//! Browser-based component tests for `GlobalSearch`.
//!
//! The component renders nothing visible until a Cmd+K / Ctrl+K
//! keydown lands on the (positioned-offscreen) `.global-search-trigger`
//! element. These tests synthesize that keystroke to drive the open
//! flow, then assert on the modal structure. The search index loads
//! via server fns (`get_assets`, `get_jobs`, etc.) which all 404 in
//! tests — that puts the result list permanently in the empty branch,
//! which is itself worth pinning.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{flush_effects, fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::global_search::GlobalSearch;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Element, KeyboardEvent, KeyboardEventInit};

wasm_bindgen_test_configure!(run_in_browser);

fn dispatch_key(target: &Element, key: &str, meta: bool) {
    let init = KeyboardEventInit::new();
    init.set_bubbles(true);
    init.set_cancelable(true);
    init.set_key(key);
    init.set_meta_key(meta);
    let ev: KeyboardEvent =
        KeyboardEvent::new_with_keyboard_event_init_dict("keydown", &init).unwrap();
    target.dispatch_event(&ev).unwrap();
}

fn mount() -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        view! {
            <Router>
                <GlobalSearch />
            </Router>
        }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn closed_search_renders_only_the_invisible_trigger() {
    let host = mount();

    // The trigger is always present (hidden via inline styles).
    assert_eq!(query_all(&host, ".global-search-trigger").len(), 1);
    // No modal yet.
    assert_eq!(query_all(&host, ".search-modal").len(), 0);
}

#[wasm_bindgen_test]
async fn cmd_k_keydown_opens_search_modal_with_input() {
    let host = mount();

    let trigger = query_one(&host, ".global-search-trigger");
    dispatch_key(&trigger, "k", true);
    flush_effects().await;

    // Modal is in the DOM, with the input rendered inside it.
    assert_eq!(query_all(&host, ".search-modal").len(), 1);
    let input = query_one(&host, "input.search-input");
    assert!(input.dyn_ref::<web_sys::HtmlInputElement>().is_some());
}

#[wasm_bindgen_test]
async fn empty_search_index_shows_no_results_branch() {
    // With no backend in tests, every server-fn call errors and the
    // search index resolves to empty — the rendered tree should land
    // in the `.search-empty` branch rather than the result-list one.
    let host = mount();

    let trigger = query_one(&host, ".global-search-trigger");
    dispatch_key(&trigger, "k", true);
    flush_effects().await;

    // Wait an extra tick for `LocalResource` to resolve.
    flush_effects().await;
    flush_effects().await;

    let empty = query_one(&host, ".search-empty");
    assert_eq!(empty.text_content().unwrap(), "No results found.");
    assert_eq!(query_all(&host, ".search-result-item").len(), 0);
}

#[wasm_bindgen_test]
async fn footer_renders_keyboard_hints_when_open() {
    let host = mount();

    let trigger = query_one(&host, ".global-search-trigger");
    dispatch_key(&trigger, "k", true);
    flush_effects().await;

    let hints = query_all(&host, ".search-footer-hint");
    assert_eq!(hints.len(), 3); // ↑↓ navigate, ↵ select, esc close
    let brand = query_one(&host, ".search-footer-brand")
        .text_content()
        .unwrap();
    assert!(brand.contains("rivers"));
}

#[wasm_bindgen_test]
async fn escape_keydown_closes_open_search_modal() {
    let host = mount();

    let trigger = query_one(&host, ".global-search-trigger");
    dispatch_key(&trigger, "k", true);
    flush_effects().await;
    assert_eq!(query_all(&host, ".search-modal").len(), 1);

    // Escape on the input fires `set_open(false)`.
    dispatch_key(&query_one(&host, ".search-input"), "Escape", false);
    flush_effects().await;

    assert_eq!(query_all(&host, ".search-modal").len(), 0);
}
