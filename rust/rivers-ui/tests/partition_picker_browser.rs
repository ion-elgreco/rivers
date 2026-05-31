//! Browser-based component tests for `PartitionPicker`.
//!
//! Run with `just wasm-test` (or `wasm-pack test --headless --chrome
//! --no-default-features --features csr` from `rust/rivers-ui/`). Compiles
//! only for `wasm32-unknown-unknown` so a plain `cargo test` against the
//! native target is unaffected.
//!
//! These tests complement the pure-logic tests in `partition_picker.rs`
//! (`apply_toggle`, `collect_submit_keys`) by exercising the full
//! component: mount → click DOM rows → read the parent-owned `selected`
//! signal → verify both the structured output and the rendered DOM
//! reflect each click.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_all};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::partition_picker::PartitionPicker;
use rivers_ui::helpers::JobPartitionPicker;
use rivers_ui::types::{PartitionDimensionInfo, SubmitPartitionKey};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Element, HtmlElement};

wasm_bindgen_test_configure!(run_in_browser);

fn rows(host: &HtmlElement) -> Vec<Element> {
    query_all(host, ".exec-dialog-partition-row")
}

fn single_picker(keys: &[&str]) -> JobPartitionPicker {
    JobPartitionPicker::SingleDim {
        keys: keys.iter().map(|s| s.to_string()).collect(),
    }
}

fn multi_picker(dims: &[(&str, &[&str])]) -> JobPartitionPicker {
    JobPartitionPicker::Multi {
        dimensions: dims
            .iter()
            .map(|(name, keys)| PartitionDimensionInfo {
                name: name.to_string(),
                keys: keys.iter().map(|s| s.to_string()).collect(),
                total_count: keys.len() as u64,
                keys_truncated: false,
            })
            .collect(),
        asset_key: None,
    }
}

#[wasm_bindgen_test]
fn none_picker_renders_no_rows() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || JobPartitionPicker::None)
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    assert!(
        rows(&target).is_empty(),
        "None picker should render no rows"
    );
    assert!(selected.get_untracked().is_empty());
}

#[wasm_bindgen_test]
fn single_dim_renders_one_row_per_key() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || single_picker(&["a", "b", "c"]))
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    let row_els = rows(&target);
    assert_eq!(row_els.len(), 3);
    assert!(row_els[0].text_content().unwrap().contains('a'));
    assert!(row_els[2].text_content().unwrap().contains('c'));
}

#[wasm_bindgen_test]
async fn click_on_row_pushes_single_partition_key() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || single_picker(&["a", "b", "c"]))
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    click(&rows(&target)[1], false);
    flush_effects().await;

    assert_eq!(
        selected.get_untracked(),
        vec![SubmitPartitionKey::Single("b".to_string())]
    );
    let row_els = rows(&target);
    assert!(
        row_els[1]
            .class_name()
            .contains("exec-dialog-partition-row--selected"),
        "clicked row should pick up the selected modifier class"
    );
}

#[wasm_bindgen_test]
async fn shift_click_extends_range_in_dom() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || single_picker(&["a", "b", "c", "d", "e"]))
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    let row_els = rows(&target);
    click(&row_els[0], false); // anchor at "a"
    flush_effects().await;
    click(&row_els[3], true); // shift to "d"
    flush_effects().await;

    let out = selected.get_untracked();
    assert_eq!(
        out,
        vec![
            SubmitPartitionKey::Single("a".to_string()),
            SubmitPartitionKey::Single("b".to_string()),
            SubmitPartitionKey::Single("c".to_string()),
            SubmitPartitionKey::Single("d".to_string()),
        ]
    );
}

#[wasm_bindgen_test]
fn multi_picker_renders_one_section_per_dimension() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || multi_picker(&[
                    ("color", &["r", "g"][..]),
                    ("size", &["s", "m"][..]),
                ]))
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    let dim_sections = target
        .query_selector_all(".exec-dialog-partition-dim")
        .unwrap();
    assert_eq!(dim_sections.length(), 2);
    // 2 dims × 2 keys each → 4 rows total.
    assert_eq!(rows(&target).len(), 4);
}

#[wasm_bindgen_test]
async fn multi_picker_clicks_yield_cartesian_product() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || multi_picker(&[
                    ("color", &["r", "g"][..]),
                    ("size", &["s"][..]),
                ]))
                selected
                reset=Signal::derive(|| false)
            />
        }
    });

    // Layout: rows[0..2] are the color dim, rows[2] is the size dim.
    let row_els = rows(&target);
    click(&row_els[0], false); // color = r
    flush_effects().await;
    click(&row_els[1], false); // color = g
    flush_effects().await;
    click(&row_els[2], false); // size  = s
    flush_effects().await;

    // Cartesian product is sorted by dim name → ("color", "size") order.
    assert_eq!(
        selected.get_untracked(),
        vec![
            SubmitPartitionKey::Multi(vec![
                ("color".to_string(), "r".to_string()),
                ("size".to_string(), "s".to_string()),
            ]),
            SubmitPartitionKey::Multi(vec![
                ("color".to_string(), "g".to_string()),
                ("size".to_string(), "s".to_string()),
            ]),
        ]
    );
}

#[wasm_bindgen_test]
async fn reset_signal_clears_selection() {
    let target = fresh_mount_target();
    let selected = RwSignal::new(Vec::<SubmitPartitionKey>::new());
    let reset = RwSignal::new(false);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <PartitionPicker
                picker=Signal::derive(move || single_picker(&["a", "b", "c"]))
                selected
                reset=Signal::derive(move || reset.get())
            />
        }
    });

    click(&rows(&target)[0], false);
    flush_effects().await;
    assert_eq!(selected.get_untracked().len(), 1);

    reset.set(true);
    flush_effects().await;

    assert!(
        selected.get_untracked().is_empty(),
        "flipping reset to true should drop the selection"
    );
}
