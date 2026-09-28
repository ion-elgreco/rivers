//! Browser tests for the job page's Execute button.
//!
//! `window.fetch` answers each server fn by URL, so a test decides when (and
//! whether) the job's definition arrives. The server runs the job's own verb
//! and refuses a request naming another one, so the page must send the verb
//! it showed, and must not send anything before it has a verb to show.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{
    Reply, click, fresh_mount_target, install_routing_fetch_mock, nav_to, query_all, query_one,
    request_bodies,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::{FlatRoutes, Route, Router};
use leptos_router::path;
use rivers_ui::pages::job_detail::JobDetailPage;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// `purge` runs the destructive `delete` on the unpartitioned `summary`.
const PURGE_JOB: &str = r#"[{"name": "purge", "asset_selection": ["summary"],
    "executor_type": "InProcess", "action": "delete"}]"#;

const SUMMARY_INFO: &str = r#"[{"asset_key": "summary", "description": null,
    "partition_def": null, "hooks": [], "io_handler": null,
    "has_self_dependency": false, "is_external": false,
    "automation_condition": null, "tags": [], "kinds": [], "group": null,
    "code_version": null, "asset_type": "asset",
    "actions": [{"name": "delete", "outcome": "unmaterialize", "exclusive": true,
        "partitioning": "optional", "description": null}]}]"#;

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

/// Mount the job page at `/locations/default/demo/jobs/purge`, with
/// `get_jobs` answering `jobs`, `get_assets_info` the `summary` definition,
/// `execute_job` a run id, and every other server fn failing.
fn mount_purge_page(jobs: Reply) -> (web_sys::HtmlElement, common::FetchMock, common::Requests) {
    let (mock, requests) = install_routing_fetch_mock(move |url| {
        if url.contains("get_jobs") {
            jobs.clone()
        } else if url.contains("get_assets_info") {
            Reply::Json(SUMMARY_INFO.to_string())
        } else if url.contains("execute_job") {
            Reply::Json(r#"{"run_id": "RUN-P", "status": ""}"#.to_string())
        } else {
            Reply::Error
        }
    });
    nav_to("/locations/default/demo/jobs/purge");
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        view! {
            <Router>
                <FlatRoutes fallback=|| view! { <div class="no-route"></div> }>
                    <Route path=path!("/locations/:loc_ns/:loc_name/jobs/:name") view=JobDetailPage/>
                </FlatRoutes>
            </Router>
        }
    })
    .forget();
    (target, mock, requests)
}

fn execute_button(host: &web_sys::HtmlElement) -> web_sys::HtmlButtonElement {
    query_one(host, ".topbar-actions .btn-primary")
        .dyn_into()
        .unwrap()
}

/// The routes render asynchronously: `false` until the page is there.
fn page_rendered(host: &web_sys::HtmlElement) -> bool {
    !query_all(host, ".topbar-actions .btn-primary").is_empty()
}

async fn execute_job_bodies(requests: &common::Requests) -> Vec<String> {
    let sent: Vec<web_sys::Request> = requests
        .borrow()
        .iter()
        .filter(|r| r.url().contains("execute_job"))
        .cloned()
        .collect();
    request_bodies(&sent).await
}

/// Before `get_jobs` answers, the page has no verb to show, and a click sent
/// the job keyless with no verb: a `delete` job deleted the whole table with
/// no confirm.
#[wasm_bindgen_test]
async fn execute_waits_for_the_job_definition() {
    let (host, _mock, requests) = mount_purge_page(Reply::Pending);
    assert!(
        wait_until(|| page_rendered(&host)).await,
        "the page never rendered"
    );
    for _ in 0..5 {
        yield_macro().await;
    }

    assert!(execute_button(&host).disabled());
    click(&execute_button(&host), false);
    click(&execute_button(&host), false);
    for _ in 0..5 {
        yield_macro().await;
    }
    assert_eq!(execute_job_bodies(&requests).await, Vec::<String>::new());
}

/// A failed `get_jobs` (a code location that is not Ready) is never
/// refetched on the page, so the loading window stayed open for good.
#[wasm_bindgen_test]
async fn execute_stays_off_when_the_job_definition_fails() {
    let (host, _mock, requests) = mount_purge_page(Reply::Error);
    // One error for the job's section, one for its runs.
    assert!(
        wait_until(|| query_all(&host, ".error-msg").len() == 2).await,
        "the failed get_jobs never rendered"
    );

    assert!(execute_button(&host).disabled());
    click(&execute_button(&host), false);
    click(&execute_button(&host), false);
    for _ in 0..5 {
        yield_macro().await;
    }
    assert_eq!(execute_job_bodies(&requests).await, Vec::<String>::new());
}

/// Once loaded, the page confirms the verb it shows and sends that verb, so
/// the server can refuse a job that now runs another one.
#[wasm_bindgen_test]
async fn execute_sends_the_verb_the_page_showed() {
    let (host, _mock, requests) = mount_purge_page(Reply::Json(PURGE_JOB.to_string()));
    assert!(
        wait_until(|| page_rendered(&host) && !execute_button(&host).disabled()).await,
        "Execute never enabled"
    );

    click(&execute_button(&host), false);
    assert!(
        wait_until(|| page_rendered(&host)
            && execute_button(&host).text_content().unwrap_or_default() == "Confirm delete?")
        .await,
        "the first click did not ask to confirm the delete (sent: {:?})",
        execute_job_bodies(&requests).await
    );
    click(&execute_button(&host), false);

    assert!(
        wait_until(|| requests
            .borrow()
            .iter()
            .any(|r| r.url().contains("execute_job")))
        .await,
        "no execute_job request"
    );
    assert_eq!(
        execute_job_bodies(&requests).await,
        vec![
            "loc_ns=default&loc_name=demo&job_name=purge&action=delete&whole_asset=false"
                .to_string()
        ]
    );
}
