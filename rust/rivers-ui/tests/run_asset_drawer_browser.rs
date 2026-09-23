//! Browser-based component tests for the run page's asset drawer.
//!
//! The drawer pages through one event kind per section. `window.fetch` is
//! mocked with a single canned page, so every section's query returns it.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, install_fetch_mock, nav_to, query_all};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::pages::run_detail::RunAssetDrawer;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

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

async fn wait_until<F: Fn() -> bool>(pred: F) -> bool {
    for _ in 0..200 {
        if pred() {
            return true;
        }
        yield_macro().await;
    }
    false
}

/// A delete run's only asset event is its Deletion. The drawer showed
/// "No materializations." and left the deletion out of its event count.
#[wasm_bindgen_test]
async fn deletion_events_show_in_the_asset_drawer() {
    let _mock = install_fetch_mock(|url| {
        url.contains("get_run_asset_events_page").then(|| {
            r#"{"rows":[{"id":"e1","event_type":"Deletion","asset_key":"events",
                "run_id":"r1","partition_key":null,"timestamp":1000,"metadata":[],
                "data_version":null}],"total":1}"#
                .to_string()
        })
    });
    nav_to("/locations/default/demo/runs/r1");
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, move || {
        let (mat_page, set_mat_page) = signal(0u64);
        let (obs_page, set_obs_page) = signal(0u64);
        let (act_page, set_act_page) = signal(0u64);
        let (del_page, set_del_page) = signal(0u64);
        let (_, on_close) = signal(None::<String>);
        view! {
            <Router>
                <RunAssetDrawer
                    asset_key="events".to_string()
                    run_id="r1".to_string()
                    step_events=vec![]
                    topology=None
                    mat_page=mat_page
                    set_mat_page=set_mat_page
                    obs_page=obs_page
                    set_obs_page=set_obs_page
                    act_page=act_page
                    set_act_page=set_act_page
                    del_page=del_page
                    set_del_page=set_del_page
                    on_close=on_close
                />
            </Router>
        }
    })
    .forget();

    let labels = || -> Vec<String> {
        query_all(&host, ".section-header-label")
            .iter()
            .filter_map(|e| e.text_content())
            .collect()
    };
    assert!(
        wait_until(|| labels().iter().any(|l| l == "DELETION")).await,
        "no DELETION section: {:?}",
        labels()
    );
    let kv: Vec<String> = query_all(&host, ".run-asset-drawer-kv")
        .iter()
        .filter_map(|e| e.text_content())
        .collect();
    assert!(kv.iter().any(|t| t == "DELETIONS1"), "no deletion count: {kv:?}");
    // Four event kinds, one row each, and no step events.
    assert!(kv.iter().any(|t| t == "EVENTS4"), "events count: {kv:?}");
}
