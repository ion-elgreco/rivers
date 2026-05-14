//! Browser-based component tests for the `AssetStack` ui_kit primitive.
//!
//! `AssetStack` reads `use_current_location()` (the parsed
//! `/locations/<ns>/<name>/...` URL) to build per-asset hrefs and uses
//! `use_navigate` for the click-handler navigation. Both require a
//! `<Router>` ancestor.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::ui_kit::AssetStack;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount_stack(
    assets: Vec<String>,
    max: Option<usize>,
    overflow_href: Option<String>,
) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        let max = max.unwrap_or(2);
        let overflow_href = overflow_href.clone();
        view! {
            <Router>
                {move || {
                    let assets = assets.clone();
                    let overflow_href = overflow_href.clone();
                    match overflow_href {
                        Some(href) => view! {
                            <AssetStack assets=assets max=max overflow_href=href />
                        }.into_any(),
                        None => view! {
                            <AssetStack assets=assets max=max />
                        }.into_any(),
                    }
                }}
            </Router>
        }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn under_max_shows_one_chip_per_asset_no_overflow() {
    let host = mount_stack(vec!["asset.a".into(), "asset.b".into()], Some(2), None);

    let chips = query_all(&host, ".asset-stack-chip");
    assert_eq!(chips.len(), 2);
    assert!(chips[0].text_content().unwrap().contains("asset.a"));

    // No overflow span when count <= max.
    assert_eq!(query_all(&host, ".asset-stack-more").len(), 0);
}

#[wasm_bindgen_test]
fn over_max_renders_max_chips_plus_overflow_marker() {
    let host = mount_stack(
        vec![
            "asset.a".into(),
            "asset.b".into(),
            "asset.c".into(),
            "asset.d".into(),
            "asset.e".into(),
        ],
        Some(2),
        None,
    );

    assert_eq!(query_all(&host, ".asset-stack-chip").len(), 2);
    let overflow = query_one(&host, ".asset-stack-more");
    assert_eq!(overflow.text_content().unwrap(), "+3");

    // Tooltip lists every overflow asset, newline-separated, in
    // encounter order (NOT the visible chips).
    let tip = overflow.get_attribute("data-tip").unwrap_or_default();
    assert_eq!(tip, "asset.c\nasset.d\nasset.e");
}

#[wasm_bindgen_test]
fn each_chip_carries_role_link_and_tabindex() {
    let host = mount_stack(vec!["asset.a".into()], Some(2), None);
    let chip = query_one(&host, ".asset-stack-chip");
    assert_eq!(chip.get_attribute("role").unwrap(), "link");
    assert_eq!(chip.get_attribute("tabindex").unwrap(), "0");
    assert_eq!(chip.get_attribute("title").unwrap(), "asset.a");
}

#[wasm_bindgen_test]
fn empty_assets_renders_zero_chips_and_no_overflow() {
    let host = mount_stack(Vec::new(), Some(2), None);
    assert_eq!(query_all(&host, ".asset-stack-chip").len(), 0);
    assert_eq!(query_all(&host, ".asset-stack-more").len(), 0);
}

#[wasm_bindgen_test]
fn overflow_with_explicit_href_still_renders_marker() {
    let host = mount_stack(
        vec!["a".into(), "b".into(), "c".into()],
        Some(1),
        Some("/locations/default/demo/runs/X/assets".into()),
    );
    let overflow = query_one(&host, ".asset-stack-more");
    assert_eq!(overflow.text_content().unwrap(), "+2");
    // Anchor-bearing overflow keeps the same `role=link` semantic.
    assert_eq!(overflow.get_attribute("role").unwrap(), "link");
}
