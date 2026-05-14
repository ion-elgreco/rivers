//! Browser-based component tests for the **populated** branch of
//! `GlobalSearch` — the result list that appears once the
//! `LocalResource` for `get_assets` / `get_jobs` / `get_schedules` /
//! `get_sensors` / `get_runs` resolves with non-empty data.
//!
//! Mocks `window.fetch` per server-fn URL. The mock keys off the URL
//! prefix (`/api/get_assets...`, etc.) and returns canned JSON
//! matching each server fn's `Vec<...>` return shape.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{flush_effects, fresh_mount_target, install_fetch_mock, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::global_search::GlobalSearch;
use wasm_bindgen::{JsCast, JsValue};
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

/// Wait up to ~2s for `pred` to become true, yielding to the macrotask
/// queue between checks. The `LocalResource` chain that powers the
/// search index resolves asynchronously across multiple ticks.
async fn yield_macro() {
    use wasm_bindgen::closure::Closure;
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let cb = Closure::once_into_js(move || {
            let _ = js_sys::Function::from(resolve).call0(&JsValue::NULL);
        });
        web_sys::window()
            .unwrap()
            .set_timeout_with_callback(cb.as_ref().unchecked_ref())
            .unwrap();
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}
async fn wait_until<F: Fn() -> bool>(pred: F) -> bool {
    for _ in 0..200 {
        if pred() {
            return true;
        }
        yield_macro().await;
    }
    false
}

#[wasm_bindgen_test]
async fn populated_search_index_renders_one_result_row_per_entry() {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();

    // Map each server fn URL → canned JSON. The default `#[server]`
    // routes are `/api/<fn>{hash}` with the same `<fn>` prefix as the
    // function name — pattern-match on substring rather than exact URL.
    let _mock = install_fetch_mock(|url| {
        if url.contains("/api/get_assets") {
            Some(
                r#"[{
                    "asset_key": "asset.alpha",
                    "tags": [],
                    "kinds": ["python"],
                    "asset_group": null,
                    "code_version": null,
                    "last_event_id": null,
                    "last_run_id": null,
                    "last_timestamp": null,
                    "last_data_version": null,
                    "pool": [],
                    "stale_status": "Missing"
                }]"#
                .to_string(),
            )
        } else if url.contains("/api/get_jobs") {
            Some(
                r#"[{
                    "name": "loader_job",
                    "asset_selection": [],
                    "executor_type": "InProcess"
                }]"#
                .to_string(),
            )
        } else if url.contains("/api/get_schedules") {
            Some(
                r#"[{
                    "name": "hourly_kickoff",
                    "cron_schedule": "0 * * * *",
                    "cron_description": null,
                    "job_name": "loader_job",
                    "status": "Running",
                    "timezone": null,
                    "description": null,
                    "tags": []
                }]"#
                .to_string(),
            )
        } else if url.contains("/api/get_sensors") {
            Some(
                r#"[{
                    "name": "watchdog",
                    "job_name": null,
                    "status": "Running",
                    "minimum_interval": null,
                    "description": null,
                    "asset_selection": [],
                    "tags": []
                }]"#
                .to_string(),
            )
        } else if url.contains("/api/get_runs") {
            Some(
                r#"[{
                    "run_id": "01234567-89ab-cdef-0123-456789abcdef",
                    "job_name": "loader_job",
                    "status": "Success",
                    "start_time": 0,
                    "end_time": null,
                    "tags": [],
                    "node_names": [],
                    "priority": 0,
                    "partition_key": null
                }]"#
                .to_string(),
            )
        } else {
            None
        }
    });

    mount_to(target.clone(), || {
        view! {
            <Router>
                <GlobalSearch />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    // Open via Cmd+K and wait for the search index to load.
    let trigger = query_one(&target, ".global-search-trigger");
    dispatch_key(&trigger, "k", true);
    flush_effects().await;

    let loaded = wait_until(|| {
        target
            .query_selector(".search-result-item")
            .ok()
            .flatten()
            .is_some()
    })
    .await;
    assert!(
        loaded,
        "expected at least one search-result-item once the index resolves"
    );

    // 5 server fns each returning 1 entry → 5 results.
    let items = query_all(&target, ".search-result-item");
    assert_eq!(items.len(), 5, "got {} items", items.len());

    let categories: Vec<String> = query_all(&target, ".search-result-category")
        .iter()
        .map(|el| el.text_content().unwrap_or_default())
        .collect();
    assert!(categories.contains(&"Asset".to_string()));
    assert!(categories.contains(&"Job".to_string()));
    assert!(categories.contains(&"Schedule".to_string()));
    assert!(categories.contains(&"Sensor".to_string()));
    assert!(categories.contains(&"Run".to_string()));
}

#[wasm_bindgen_test]
async fn search_input_filters_results_by_label_substring() {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();

    let _mock = install_fetch_mock(|url| {
        if url.contains("/api/get_assets") {
            Some(
                r#"[
                    {"asset_key":"alpha","tags":[],"kinds":[],"asset_group":null,"code_version":null,"last_event_id":null,"last_run_id":null,"last_timestamp":null,"last_data_version":null,"pool":[],"stale_status":"Missing"},
                    {"asset_key":"beta","tags":[],"kinds":[],"asset_group":null,"code_version":null,"last_event_id":null,"last_run_id":null,"last_timestamp":null,"last_data_version":null,"pool":[],"stale_status":"Missing"}
                ]"#.to_string()
            )
        } else if url.contains("/api/get_jobs") {
            Some(
                r#"[{"name":"alphajob","asset_selection":[],"executor_type":"InProcess"}]"#
                    .to_string(),
            )
        } else {
            // Other server fns return empty so the result list is just the assets+job.
            Some("[]".to_string())
        }
    });

    mount_to(target.clone(), || {
        view! { <Router><GlobalSearch /></Router> }
    })
    .forget();
    flush_effects().await;

    dispatch_key(&query_one(&target, ".global-search-trigger"), "k", true);
    flush_effects().await;

    // Wait for the unfiltered list to load (3 entries).
    let _ = wait_until(|| query_all(&target, ".search-result-item").len() == 3).await;

    // Type "beta" into the input and dispatch an `input` event so the
    // component's `set_query` listener fires.
    let input_el = query_one(&target, "input.search-input");
    let input: web_sys::HtmlInputElement = input_el.clone().dyn_into().unwrap();
    input.set_value("beta");
    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    let ev = web_sys::Event::new_with_event_init_dict("input", &init).unwrap();
    input.dispatch_event(&ev).unwrap();
    flush_effects().await;

    let items = query_all(&target, ".search-result-item");
    assert_eq!(items.len(), 1, "expected `beta` filter to leave 1 row");
    let label = query_one(&items[0], ".search-result-label")
        .text_content()
        .unwrap();
    assert_eq!(label, "beta");
}

#[wasm_bindgen_test]
async fn arrow_keys_move_active_result_marker() {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();

    let _mock = install_fetch_mock(|url| {
        if url.contains("/api/get_assets") {
            Some(
                r#"[
                    {"asset_key":"a","tags":[],"kinds":[],"asset_group":null,"code_version":null,"last_event_id":null,"last_run_id":null,"last_timestamp":null,"last_data_version":null,"pool":[],"stale_status":"Missing"},
                    {"asset_key":"b","tags":[],"kinds":[],"asset_group":null,"code_version":null,"last_event_id":null,"last_run_id":null,"last_timestamp":null,"last_data_version":null,"pool":[],"stale_status":"Missing"},
                    {"asset_key":"c","tags":[],"kinds":[],"asset_group":null,"code_version":null,"last_event_id":null,"last_run_id":null,"last_timestamp":null,"last_data_version":null,"pool":[],"stale_status":"Missing"}
                ]"#.to_string()
            )
        } else {
            Some("[]".to_string())
        }
    });

    mount_to(target.clone(), || {
        view! { <Router><GlobalSearch /></Router> }
    })
    .forget();
    flush_effects().await;
    dispatch_key(&query_one(&target, ".global-search-trigger"), "k", true);
    flush_effects().await;
    let _ = wait_until(|| query_all(&target, ".search-result-item").len() == 3).await;

    fn active_idx(host: &web_sys::HtmlElement) -> Option<usize> {
        query_all(host, ".search-result-item")
            .into_iter()
            .position(|el| el.class_name().contains("active"))
    }

    assert_eq!(active_idx(&target), Some(0), "first row active by default");

    let input = query_one(&target, "input.search-input");
    dispatch_key(&input, "ArrowDown", false);
    flush_effects().await;
    assert_eq!(active_idx(&target), Some(1));

    dispatch_key(&input, "ArrowDown", false);
    flush_effects().await;
    assert_eq!(active_idx(&target), Some(2));

    // ArrowDown at the bottom should clamp.
    dispatch_key(&input, "ArrowDown", false);
    flush_effects().await;
    assert_eq!(active_idx(&target), Some(2));

    dispatch_key(&input, "ArrowUp", false);
    flush_effects().await;
    assert_eq!(active_idx(&target), Some(1));
}
