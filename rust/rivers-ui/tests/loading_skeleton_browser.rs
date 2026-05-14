//! Browser-based component tests for the four `loading_skeleton`
//! components. These are pure presentational placeholders, so the only
//! interesting properties to pin are the structural ones — counts,
//! class names, and the prop-driven row/col grid shape.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::loading_skeleton::{
    CardSkeleton, GridRowSkeleton, StatsSkeleton, TableSkeleton,
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn table_skeleton_default_renders_5x4() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <TableSkeleton /> });

    assert_eq!(query_all(&target, "thead tr th").len(), 4);
    assert_eq!(query_all(&target, "tbody tr").len(), 5);
    assert_eq!(query_all(&target, "tbody td").len(), 5 * 4);
}

#[wasm_bindgen_test]
fn table_skeleton_respects_explicit_dimensions() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <TableSkeleton rows=3 cols=2 /> });

    assert_eq!(query_all(&target, "thead tr th").len(), 2);
    assert_eq!(query_all(&target, "tbody tr").len(), 3);
    assert_eq!(query_all(&target, "tbody td").len(), 6);
}

#[wasm_bindgen_test]
fn card_skeleton_renders_one_card_with_three_lines() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <CardSkeleton /> });

    assert!(
        query_one(&target, ".card.skeleton-container")
            .class_name()
            .contains("skeleton-container")
    );
    assert_eq!(query_all(&target, ".skeleton").len(), 3);
}

#[wasm_bindgen_test]
fn stats_skeleton_default_count_is_4() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <StatsSkeleton /> });
    assert_eq!(query_all(&target, ".stat-card").len(), 4);
}

#[wasm_bindgen_test]
fn stats_skeleton_respects_count_prop() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <StatsSkeleton count=7 /> });
    assert_eq!(query_all(&target, ".stat-card").len(), 7);
}

#[wasm_bindgen_test]
fn grid_row_skeleton_default_renders_8_rows() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <GridRowSkeleton /> });
    assert_eq!(query_all(&target, ".grid-row").len(), 8);
}

#[wasm_bindgen_test]
fn grid_row_skeleton_grid_template_columns_carries_col_count() {
    // The `cols` prop is materialized into the inline `grid-template-columns`
    // CSS, so a layout regression there is observable in the rendered style.
    let target = fresh_mount_target();
    let _handle = mount_to(
        target.clone(),
        || view! { <GridRowSkeleton rows=2 cols=5 /> },
    );

    let row = query_one(&target, ".grid-row");
    let style = row.get_attribute("style").unwrap_or_default();
    assert!(
        style.contains("repeat(5,"),
        "expected grid-template-columns to carry cols=5, got: {style}"
    );
}
