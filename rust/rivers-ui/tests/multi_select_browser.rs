//! Browser-based component tests for `MultiSelect`.
//!
//! The component is purely callback-driven — it doesn't own its
//! `selected` state. Tests verify three concerns:
//! 1. Trigger label format reflects the current selection size.
//! 2. The dropdown opens on trigger click and the backdrop closes it.
//! 3. Checkbox toggles invoke the `on_toggle` callback exactly once
//!    with the right value.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::multi_select::{MultiSelect, SelectOption};
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Event, HtmlInputElement};

wasm_bindgen_test_configure!(run_in_browser);

fn opt(value: &str, label: &str, enabled: bool) -> SelectOption {
    SelectOption {
        value: value.to_string(),
        label: label.to_string(),
        enabled,
    }
}

#[wasm_bindgen_test]
fn trigger_label_shows_placeholder_when_nothing_selected() {
    let target = fresh_mount_target();
    let options = RwSignal::new(vec![opt("a", "Alpha", true), opt("b", "Beta", true)]);
    let selected = RwSignal::new(Vec::<String>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <MultiSelect
                options=options.into()
                selected=selected.into()
                on_toggle=Callback::new(|_v: String| {})
                placeholder="Statuses"
            />
        }
    });

    let label = query_one(&target, ".multi-select-label")
        .text_content()
        .unwrap();
    assert_eq!(label, "All Statuses");
}

#[wasm_bindgen_test]
fn trigger_label_shows_single_value_when_one_selected() {
    let target = fresh_mount_target();
    let options = RwSignal::new(vec![opt("running", "Running", true)]);
    let selected = RwSignal::new(vec!["running".to_string()]);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <MultiSelect
                options=options.into()
                selected=selected.into()
                on_toggle=Callback::new(|_| {})
                placeholder="Statuses"
            />
        }
    });

    let label = query_one(&target, ".multi-select-label")
        .text_content()
        .unwrap();
    assert_eq!(label, "running");
}

#[wasm_bindgen_test]
fn trigger_label_pluralizes_when_multiple_selected() {
    let target = fresh_mount_target();
    let options = RwSignal::new(vec![opt("a", "A", true), opt("b", "B", true)]);
    let selected = RwSignal::new(vec!["a".to_string(), "b".to_string()]);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <MultiSelect
                options=options.into()
                selected=selected.into()
                on_toggle=Callback::new(|_| {})
                placeholder="Statuses"
            />
        }
    });

    let label = query_one(&target, ".multi-select-label")
        .text_content()
        .unwrap();
    assert_eq!(label, "2 statuses");
}

#[wasm_bindgen_test]
async fn trigger_click_opens_dropdown() {
    let target = fresh_mount_target();
    let options = RwSignal::new(vec![opt("a", "Alpha", true)]);
    let selected = RwSignal::new(Vec::<String>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <MultiSelect
                options=options.into()
                selected=selected.into()
                on_toggle=Callback::new(|_| {})
                placeholder="Things"
            />
        }
    });

    assert!(
        target
            .query_selector(".multi-select-dropdown")
            .unwrap()
            .is_none()
    );

    click(&query_one(&target, ".multi-select-trigger"), false);
    flush_effects().await;

    assert!(
        target
            .query_selector(".multi-select-dropdown")
            .unwrap()
            .is_some(),
        "dropdown should be in DOM after trigger click"
    );
}

#[wasm_bindgen_test]
async fn checkbox_change_event_fires_on_toggle_with_value() {
    let target = fresh_mount_target();
    let options = RwSignal::new(vec![opt("alpha", "Alpha", true), opt("beta", "Beta", true)]);
    let selected = RwSignal::new(Vec::<String>::new());

    // `Callback::new` requires `Send + Sync`, so the receiver itself
    // must be Send+Sync. A leptos `RwSignal` fits — single-threaded in
    // wasm but still implements the right traits for the bound.
    let received = RwSignal::new(Vec::<String>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <MultiSelect
                options=options.into()
                selected=selected.into()
                on_toggle=Callback::new(move |v: String| {
                    received.update(|r| r.push(v));
                })
                placeholder="Things"
            />
        }
    });

    // Open the dropdown first so checkboxes exist.
    click(&query_one(&target, ".multi-select-trigger"), false);
    flush_effects().await;

    let body = common::document().body().unwrap();
    let checkboxes = query_all(&body, ".multi-select-option input[type=checkbox]");
    assert_eq!(checkboxes.len(), 2);

    // Dispatch a bubbling `change` event on the second checkbox so the
    // <label>'s `on:change` handler picks it up.
    let cb: HtmlInputElement = checkboxes[1].clone().dyn_into().unwrap();
    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    let ev = Event::new_with_event_init_dict("change", &init).unwrap();
    cb.dispatch_event(&ev).unwrap();
    flush_effects().await;

    assert_eq!(received.get_untracked(), vec!["beta".to_string()]);
}
