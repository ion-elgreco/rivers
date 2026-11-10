//! Browser test for the asset page's "in other code locations" section.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, nav_to, query_all};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::remote_downstream::RemoteDownstreamSection;
use rivers_ui::types::RemoteDownstream;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount(rows: Vec<RemoteDownstream>) -> web_sys::HtmlElement {
    nav_to("/");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! { <Router><RemoteDownstreamSection rows=rows/></Router> }
    })
    .forget();
    target
}

fn row(code_location: &str, asset_key: &str) -> RemoteDownstream {
    RemoteDownstream {
        code_location: code_location.into(),
        asset_key: asset_key.into(),
    }
}

#[wasm_bindgen_test]
fn each_row_links_to_the_asset_in_its_code_location() {
    let host = mount(vec![
        row("dev/analytics", "order_count"),
        row("team-data/shop", "report"),
    ]);

    let links = query_all(&host, "a.lineage-row");
    let hrefs: Vec<String> = links
        .iter()
        .map(|a| a.get_attribute("href").unwrap_or_default())
        .collect();
    assert_eq!(
        hrefs,
        [
            "/locations/dev/analytics/assets/order_count",
            "/locations/team-data/shop/assets/report",
        ]
    );
    assert_eq!(
        links[0].text_content().unwrap(),
        "order_countremoteanalytics"
    );
    let text = host.text_content().unwrap_or_default();
    assert!(text.contains("IN OTHER CODE LOCATIONS"), "got: {text}");
}

#[wasm_bindgen_test]
fn no_rows_render_nothing() {
    let host = mount(Vec::new());

    assert!(query_all(&host, ".lineage-remote-downstream").is_empty());
}
