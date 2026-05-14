//! Browser-based component tests for `ui_kit` primitives that use
//! `leptos_router::components::A` when given an `href`.
//!
//! Mounts each test inside a transparent `<Router>` so `<A>` resolves
//! against a real `RouterContext`. Without this, `A` panics. The
//! no-`href` branches are already covered in `ui_kit_browser.rs` and
//! `ui_kit_extended_browser.rs`.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::ui_kit::{Crumb, Rail, StatTile, SummaryCard, Topbar};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount_in_router<F, V>(view_fn: F) -> web_sys::HtmlElement
where
    F: FnOnce() -> V + Send + Sync + 'static,
    V: IntoView + 'static,
{
    nav_to("/");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! { <Router>{view_fn()}</Router> }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn stat_tile_with_href_renders_anchor_with_class_stat_tile() {
    let host = mount_in_router(|| {
        view! {
            <StatTile
                label="Pipelines"
                value="42"
                rail=Rail::Primary
                href="/dashboards/pipelines"
            />
        }
    });

    let a = query_one(&host, "a.stat-tile");
    assert_eq!(a.get_attribute("href").unwrap(), "/dashboards/pipelines");
    let text = a.text_content().unwrap();
    assert!(text.contains("Pipelines"));
    assert!(text.contains("42"));
    // No <div class="stat-tile"> when href is provided.
    assert_eq!(query_all(&host, "div.stat-tile").len(), 0);
}

#[wasm_bindgen_test]
fn summary_card_with_href_wraps_body_in_anchor() {
    let host = mount_in_router(|| {
        view! {
            <SummaryCard
                title="loader.run"
                description="Daily loader pipeline"
                kind="success"
                href="/locations/default/demo/jobs/loader.run"
            />
        }
    });

    let a = query_one(&host, "a.summary-card");
    assert_eq!(
        a.get_attribute("href").unwrap(),
        "/locations/default/demo/jobs/loader.run"
    );
    // The rail span lives inside the anchor.
    let rail = query_one(&a, ".summary-card-rail");
    assert!(rail.class_name().contains("summary-card-rail--success"));
    assert!(a.text_content().unwrap().contains("loader.run"));
    // The chip-with-no-href branch does NOT also fire — there's a single wrapper.
    assert_eq!(query_all(&host, "div.summary-card").len(), 0);
}

#[wasm_bindgen_test]
fn topbar_linked_crumb_renders_anchor_with_href() {
    let host = mount_in_router(|| {
        let crumbs = vec![
            Crumb::linked("Home", "/"),
            Crumb::linked("Assets", "/locations/default/demo/assets"),
            Crumb::new("loader.run").mono(),
        ];
        view! { <Topbar crumbs=crumbs /> }
    });

    // Two of three crumbs are <A> anchors with hrefs; the last is a span.
    let anchors = query_all(&host, "a.topbar-crumb");
    assert_eq!(anchors.len(), 2);
    assert_eq!(anchors[0].get_attribute("href").unwrap(), "/");
    assert_eq!(
        anchors[1].get_attribute("href").unwrap(),
        "/locations/default/demo/assets"
    );

    // The mono final crumb is a span carrying the current marker.
    let spans = query_all(&host, "span.topbar-crumb");
    let current = spans
        .iter()
        .find(|s| s.class_name().contains("topbar-crumb--current"))
        .unwrap();
    assert!(current.class_name().contains("topbar-crumb--mono"));
    assert_eq!(current.text_content().unwrap(), "loader.run");
}

#[wasm_bindgen_test]
fn topbar_linked_crumb_only_marks_last_as_current() {
    let host = mount_in_router(|| {
        let crumbs = vec![
            Crumb::linked("Home", "/"),
            Crumb::linked("Assets", "/assets"),
            Crumb::linked("loader.run", "/loader"),
        ];
        view! { <Topbar crumbs=crumbs /> }
    });

    let anchors = query_all(&host, "a.topbar-crumb");
    assert_eq!(anchors.len(), 3);
    assert!(!anchors[0].class_name().contains("topbar-crumb--current"));
    assert!(!anchors[1].class_name().contains("topbar-crumb--current"));
    assert!(anchors[2].class_name().contains("topbar-crumb--current"));
}
