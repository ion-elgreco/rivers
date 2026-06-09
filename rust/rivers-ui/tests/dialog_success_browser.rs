//! Browser-based component tests for the **success-path** branches of
//! `ExecuteJobDialog` and `MaterializeDialog`.
//!
//! Mocks `window.fetch` so the underlying server-fn dispatch resolves
//! to a synthetic JSON response without a backend. The dialogs then
//! emit a `<Redirect>` that swaps the router URL — we inspect the
//! browser path after the action settles to verify the post-success
//! navigation contract.

#![cfg(target_arch = "wasm32")]

mod common;

use std::cell::RefCell;
use std::rc::Rc;

use common::{
    click, flush_effects, fresh_mount_target, install_fetch_mock, nav_to, query_all, query_one,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::execute_job_dialog::ExecuteJobDialog;
use rivers_ui::components::materialize_dialog::MaterializeDialog;
use rivers_ui::helpers::JobPartitionPicker;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn current_path() -> String {
    web_sys::window()
        .unwrap()
        .location()
        .pathname()
        .unwrap_or_default()
}

/// Yield to the **macrotask** queue (`setTimeout(0)`) so chained
/// Promise resolutions and the wasm-bindgen-futures executor run. A
/// microtask flush is too tight: leptos `Action`s schedule the Promise
/// resolution on a later tick than `Promise.resolve()`-style flushes,
/// so the future stays pending under `flush_effects()` alone.
async fn yield_macro() {
    use wasm_bindgen::closure::Closure;
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
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

/// Wait up to ~2s in macrotask increments for `pred` to become true.
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
async fn execute_job_success_redirects_to_run_detail() {
    nav_to("/locations/default/demo/jobs/loader");
    let target = fresh_mount_target();

    // Capture every fetched URL — useful for diagnosing wire-format
    // changes — and respond with a canned `MaterializeResult`.
    let captured: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let captured_for_mock = captured.clone();
    let _mock = install_fetch_mock(move |url| {
        captured_for_mock.borrow_mut().push(url.to_string());
        Some(
            r#"{
                "run_id": "RUN-7",
                "status": "queued"
            }"#
            .to_string(),
        )
    });

    let show = RwSignal::new(true);
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(|| "loader".to_string())
                    picker=Signal::derive(|| JobPartitionPicker::None)
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    click(&query_one(&target, ".modal-footer .btn-primary"), false);
    flush_effects().await;

    // Wait for the action → effect → redirect chain to settle.
    let arrived = wait_until(|| current_path().ends_with("/runs/RUN-7")).await;
    assert!(
        arrived,
        "expected redirect to /runs/RUN-7, got: {} (fetched: {:?})",
        current_path(),
        captured.borrow()
    );
    assert!(!show.get_untracked(), "dialog should auto-close on success");

    // At least one server-fn fetch happened.
    assert!(
        captured.borrow().iter().any(|u| u.contains("/api/")),
        "expected an /api/* fetch, got: {:?}",
        captured.borrow()
    );
}

#[wasm_bindgen_test]
async fn execute_job_with_blank_run_id_shows_error_message() {
    nav_to("/locations/default/demo/jobs/loader2");
    let target = fresh_mount_target();

    let _mock = install_fetch_mock(|_| {
        Some(
            r#"{
                "run_id": "",
                "status": "queued"
            }"#
            .to_string(),
        )
    });

    let show = RwSignal::new(true);
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(|| "loader2".to_string())
                    picker=Signal::derive(|| JobPartitionPicker::None)
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    click(&query_one(&target, ".modal-footer .btn-primary"), false);
    let surfaced = wait_until(|| {
        target
            .query_selector(".error-msg")
            .ok()
            .flatten()
            .map(|el| {
                el.text_content()
                    .unwrap_or_default()
                    .contains("returned no id")
            })
            .unwrap_or(false)
    })
    .await;
    assert!(surfaced, "expected the empty-run-id error to surface");
    assert!(show.get_untracked(), "dialog should stay open on error");
}

#[wasm_bindgen_test]
async fn materialize_success_redirects_to_run_detail() {
    nav_to("/locations/default/demo/assets");
    let target = fresh_mount_target();

    let _mock = install_fetch_mock(|_| {
        Some(
            r#"{
                "run_id": "MAT-99",
                "status": "direct"
            }"#
            .to_string(),
        )
    });

    let show = RwSignal::new(true);
    let assets = vec!["asset.a".to_string()];
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <MaterializeDialog
                    show=show
                    asset_keys=Signal::derive(move || assets.clone())
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    click(&query_one(&target, ".modal-footer .btn-primary"), false);
    let arrived = wait_until(|| current_path().ends_with("/runs/MAT-99")).await;
    assert!(
        arrived,
        "expected redirect to /runs/MAT-99, got: {}",
        current_path()
    );
    assert!(!show.get_untracked(), "dialog should auto-close on success");
}

#[wasm_bindgen_test]
async fn execute_job_with_multi_picker_fires_one_request_per_combination() {
    use rivers_ui::types::PartitionDimensionInfo;

    // Start somewhere that does NOT match the expected post-success URL,
    // so `wait_until(... ends_with /jobs/multi)` actually witnesses the
    // navigation rather than passing trivially on the initial state.
    nav_to("/locations/default/demo/assets");
    let target = fresh_mount_target();

    let captured: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let captured_for_mock = captured.clone();
    let _mock = install_fetch_mock(move |url| {
        captured_for_mock.borrow_mut().push(url.to_string());
        Some(
            r#"{
                "run_id": "RUN-MULTI",
                "status": "queued"
            }"#
            .to_string(),
        )
    });

    let show = RwSignal::new(true);
    let picker = JobPartitionPicker::Multi {
        dimensions: vec![
            PartitionDimensionInfo {
                name: "color".into(),
                keys: vec!["r".into(), "g".into()],
                total_count: 2,
                keys_truncated: false,
            },
            PartitionDimensionInfo {
                name: "size".into(),
                keys: vec!["s".into()],
                total_count: 1,
                keys_truncated: false,
            },
        ],
        asset_key: None,
    };
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(|| "multi".to_string())
                    picker=Signal::derive({
                        let picker = picker.clone();
                        move || picker.clone()
                    })
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    // Pick both colors + the size → cartesian = 2 runs.
    let rows = query_all(&target, ".exec-dialog-partition-row");
    click(&rows[0], false);
    flush_effects().await;
    click(&rows[1], false);
    flush_effects().await;
    click(&rows[2], false);
    flush_effects().await;

    click(&query_one(&target, ".modal-footer .btn-primary"), false);
    // Wait for the redirect: 2 runs → jobs/<name> page (not /runs/<id>).
    let arrived = wait_until(|| current_path().ends_with("/jobs/multi")).await;
    assert!(
        arrived,
        "expected redirect to /jobs/multi for multi-run dispatch, got: {}",
        current_path()
    );

    let api_calls = captured
        .borrow()
        .iter()
        .filter(|u| u.contains("/api/"))
        .count();
    assert_eq!(
        api_calls,
        2,
        "expected one server-fn call per cartesian combination, got: {:?}",
        captured.borrow()
    );
}
