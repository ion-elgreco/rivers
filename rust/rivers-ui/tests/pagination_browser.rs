//! Browser-based component tests for `Pagination`.
//!
//! Pure-Rust unit tests in `pagination.rs` already cover `total_pages`
//! arithmetic. These tests cover the rendered DOM surface: button
//! disabled state, the info-string format, and that clicks update the
//! caller-owned `page` / `page_size` signals correctly.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::pagination::Pagination;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Element, HtmlElement};

wasm_bindgen_test_configure!(run_in_browser);

fn buttons(host: &HtmlElement) -> Vec<Element> {
    query_all(host, ".pagination-btns .btn")
}

#[wasm_bindgen_test]
fn renders_info_for_first_page() {
    let target = fresh_mount_target();
    let (page, _set_page) = signal(0u64);
    let (page_size, _set_page_size) = signal(25u64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <Pagination
                total=80
                page=page
                set_page=_set_page
                page_size=page_size
                set_page_size=_set_page_size
            />
        }
    });

    let info = query_one(&target, ".pagination-info")
        .text_content()
        .unwrap();
    assert_eq!(info, "1 - 25 of 80");
}

#[wasm_bindgen_test]
fn first_page_disables_prev_but_not_next() {
    let target = fresh_mount_target();
    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(25u64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <Pagination
                total=80
                page=page
                set_page=set_page
                page_size=page_size
                set_page_size=set_page_size
            />
        }
    });

    let btns = buttons(&target);
    assert_eq!(btns.len(), 2);
    assert!(btns[0].has_attribute("disabled"), "Prev should be disabled");
    assert!(
        !btns[1].has_attribute("disabled"),
        "Next should be enabled with 4 pages remaining"
    );
}

#[wasm_bindgen_test]
async fn next_button_advances_page_signal() {
    let target = fresh_mount_target();
    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(25u64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <Pagination
                total=80
                page=page
                set_page=set_page
                page_size=page_size
                set_page_size=set_page_size
            />
        }
    });

    let next = buttons(&target).pop().unwrap();
    click(&next, false);
    flush_effects().await;
    assert_eq!(page.get_untracked(), 1);

    click(&next, false);
    flush_effects().await;
    assert_eq!(page.get_untracked(), 2);
}

#[wasm_bindgen_test]
async fn last_page_disables_next() {
    // total=80, page_size=25 → 4 pages (0..=3). Land on the last page,
    // expect Next to be disabled and Prev to be enabled.
    let target = fresh_mount_target();
    let (page, set_page) = signal(3u64);
    let (page_size, set_page_size) = signal(25u64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <Pagination
                total=80
                page=page
                set_page=set_page
                page_size=page_size
                set_page_size=set_page_size
            />
        }
    });
    flush_effects().await;

    let btns = buttons(&target);
    assert!(!btns[0].has_attribute("disabled"));
    assert!(btns[1].has_attribute("disabled"));
}

#[wasm_bindgen_test]
fn zero_page_size_collapses_to_full_range_info() {
    // Defensive case: if a future "All" option ever sets page_size=0,
    // we don't want a div_ceil(0) panic — info must just show 1..total.
    let target = fresh_mount_target();
    let (page, set_page) = signal(0u64);
    let (page_size, set_page_size) = signal(0u64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <Pagination
                total=42
                page=page
                set_page=set_page
                page_size=page_size
                set_page_size=set_page_size
            />
        }
    });

    let info = query_one(&target, ".pagination-info")
        .text_content()
        .unwrap();
    assert_eq!(info, "1 - 42 of 42");
}
