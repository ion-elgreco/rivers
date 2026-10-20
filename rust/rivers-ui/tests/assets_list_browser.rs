//! Browser tests for the assets list page.
//!
//! `window.fetch` answers each server fn by URL, so a test decides which
//! assets the page gets and can change them between fetches.

#![cfg(target_arch = "wasm32")]

mod common;

use std::cell::RefCell;
use std::rc::Rc;

use common::{
    Reply, fresh_mount_target, install_routing_fetch_mock, nav_to, query_all, wait_until,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::{FlatRoutes, Route, Router};
use leptos_router::path;
use rivers_ui::pages::assets_list::AssetsListPage;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn record(key: &str, status: &str) -> String {
    format!(
        r#"{{"asset_key": "{key}", "tags": [], "kinds": [], "asset_group": null,
        "code_version": null, "last_event_id": null, "last_run_id": null,
        "last_timestamp": null, "last_data_version": null, "pool": [],
        "stale_status": "{status}"}}"#
    )
}

fn definition(key: &str) -> String {
    format!(
        r#"{{"asset_key": "{key}", "description": null, "partition_def": null,
        "hooks": [], "io_handler": null, "has_self_dependency": false,
        "is_external": false, "automation_condition": null, "tags": [],
        "kinds": [], "group": null, "code_version": null, "asset_type": "asset",
        "actions": []}}"#
    )
}

/// Storage records for `keys`, and the code location defining the same assets.
fn records(keys: &[(&str, &str)]) -> Store {
    let rows: Vec<String> = keys.iter().map(|(k, s)| record(k, s)).collect();
    let defs: Vec<String> = keys.iter().map(|(k, _)| definition(k)).collect();
    Store {
        records: format!("[{}]", rows.join(",")),
        definitions: format!("[{}]", defs.join(",")),
    }
}

/// What `get_assets` (storage records) and `get_assets_info` (the code
/// location's definitions) answer.
struct Store {
    records: String,
    definitions: String,
}

/// Mount the assets page, with `get_assets` and `get_assets_info` answering
/// the current value of `assets`, and the rest failing.
fn mount_assets_page(assets: Rc<RefCell<Store>>) -> (web_sys::HtmlElement, common::FetchMock) {
    let (mock, _requests) = install_routing_fetch_mock(move |url| {
        if url.contains("get_assets_info") {
            Reply::Json(assets.borrow().definitions.clone())
        } else if url.contains("get_assets") {
            Reply::Json(assets.borrow().records.clone())
        } else {
            Reply::Error
        }
    });
    nav_to("/locations/default/demo/assets");
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        view! {
            <Router>
                <FlatRoutes fallback=|| view! { <div class="no-route"></div> }>
                    <Route path=path!("/locations/:loc_ns/:loc_name/assets") view=AssetsListPage/>
                </FlatRoutes>
            </Router>
        }
    })
    .forget();
    (target, mock)
}

fn row_count(host: &web_sys::HtmlElement) -> usize {
    query_all(host, ".grid-table a.grid-row").len()
}

/// With no asset materialized yet, every asset needs attention; the table
/// must list them without a click.
#[wasm_bindgen_test]
async fn assets_needing_attention_are_listed() {
    let assets = Rc::new(RefCell::new(records(&[
        ("orders", "Missing"),
        ("revenue", "Missing"),
        ("customers", "Stale"),
    ])));
    let (host, _mock) = mount_assets_page(assets);

    assert!(
        wait_until(|| !query_all(&host, ".attention-banner").is_empty()).await,
        "the attention banner never rendered"
    );
    assert!(
        wait_until(|| row_count(&host) == 3).await,
        "expected 3 rows, got {}",
        row_count(&host)
    );
    assert!(
        query_all(&host, ".attention-banner button").is_empty(),
        "the banner must not offer to hide the rows"
    );
}
