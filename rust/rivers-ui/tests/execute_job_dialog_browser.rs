//! Browser-based component tests for `ExecuteJobDialog`.
//!
//! The dialog wraps a [`PartitionPicker`] and dispatches an `Action`
//! that calls the `execute_job` server fn. We deliberately don't fire
//! the action — these tests exercise show/hide, the partition-picker
//! delegation, and the submit-time validation that runs before any
//! server-fn call. Tests mount inside a `<Router>` because the
//! component reads `use_current_location`.
//!
//! Server-fn outcome paths (success → redirect, error → toast) require
//! a live backend or a fetch mock and are deferred.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::execute_job_dialog::ExecuteJobDialog;
use rivers_ui::helpers::JobPartitionPicker;
use rivers_ui::types::PartitionDimensionInfo;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount_dialog(
    show: RwSignal<bool>,
    job_name: &'static str,
    picker: JobPartitionPicker,
) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(move || job_name.to_string())
                    picker=Signal::derive(move || picker.clone())
                />
            </Router>
        }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn show_false_renders_no_modal() {
    let show = RwSignal::new(false);
    let host = mount_dialog(show, "demo_job", JobPartitionPicker::None);
    assert_eq!(query_all(&host, ".modal-overlay").len(), 0);
}

#[wasm_bindgen_test]
async fn show_true_renders_modal_with_job_name() {
    let show = RwSignal::new(true);
    let host = mount_dialog(show, "demo_job", JobPartitionPicker::None);
    flush_effects().await;

    assert_eq!(query_all(&host, ".modal-overlay").len(), 1);
    let body_text = query_one(&host, ".modal-body").text_content().unwrap();
    assert!(body_text.contains("demo_job"));
}

#[wasm_bindgen_test]
async fn none_picker_omits_partition_section_and_button_says_execute() {
    let show = RwSignal::new(true);
    let host = mount_dialog(show, "noparts", JobPartitionPicker::None);
    flush_effects().await;

    // No partition-row UI when picker is None.
    assert_eq!(query_all(&host, ".exec-dialog-partition-row").len(), 0);

    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert_eq!(btn.text_content().unwrap(), "Execute");
}

#[wasm_bindgen_test]
async fn single_dim_picker_renders_one_row_per_key() {
    let show = RwSignal::new(true);
    let host = mount_dialog(
        show,
        "daily_job",
        JobPartitionPicker::SingleDim {
            keys: vec![
                "2025-01-01".into(),
                "2025-01-02".into(),
                "2025-01-03".into(),
            ],
        },
    );
    flush_effects().await;

    assert_eq!(query_all(&host, ".exec-dialog-partition-row").len(), 3);
}

#[wasm_bindgen_test]
async fn submit_with_empty_single_dim_selection_shows_error() {
    let show = RwSignal::new(true);
    let host = mount_dialog(
        show,
        "daily_job",
        JobPartitionPicker::SingleDim {
            keys: vec!["2025-01-01".into(), "2025-01-02".into()],
        },
    );
    flush_effects().await;

    // No selection yet — clicking Execute should bail out with the
    // single-dim validation message before any server-fn dispatch.
    click(&query_one(&host, ".modal-footer .btn-primary"), false);
    flush_effects().await;

    let err = query_one(&host, ".error-msg").text_content().unwrap();
    assert_eq!(err, "Select at least one partition.");
}

#[wasm_bindgen_test]
async fn submit_with_empty_multi_selection_shows_multi_dim_error() {
    let show = RwSignal::new(true);
    let host = mount_dialog(
        show,
        "multi_job",
        JobPartitionPicker::Multi {
            dimensions: vec![
                PartitionDimensionInfo {
                    name: "color".into(),
                    keys: vec!["r".into(), "g".into()],
                    total_count: 2,
                    keys_truncated: false,
                },
                PartitionDimensionInfo {
                    name: "size".into(),
                    keys: vec!["s".into(), "m".into()],
                    total_count: 2,
                    keys_truncated: false,
                },
            ],
            asset_key: None,
        },
    );
    flush_effects().await;

    click(&query_one(&host, ".modal-footer .btn-primary"), false);
    flush_effects().await;

    let err = query_one(&host, ".error-msg").text_content().unwrap();
    assert_eq!(err, "Select at least one value for every dimension.");
}

#[wasm_bindgen_test]
async fn cancel_button_hides_dialog() {
    let show = RwSignal::new(true);
    let host = mount_dialog(show, "demo_job", JobPartitionPicker::None);
    flush_effects().await;

    let cancel = query_all(&host, ".modal-footer .btn")
        .into_iter()
        .find(|el| el.text_content().unwrap_or_default() == "Cancel")
        .unwrap();
    click(&cancel, false);
    flush_effects().await;

    assert!(!show.get_untracked());
    assert_eq!(query_all(&host, ".modal-overlay").len(), 0);
}

#[wasm_bindgen_test]
async fn close_button_in_header_hides_dialog() {
    let show = RwSignal::new(true);
    let host = mount_dialog(show, "demo_job", JobPartitionPicker::None);
    flush_effects().await;

    click(&query_one(&host, ".modal-header .btn"), false);
    flush_effects().await;

    assert!(!show.get_untracked());
}

#[wasm_bindgen_test]
async fn run_count_label_reflects_cartesian_product_size() {
    let show = RwSignal::new(true);
    let host = mount_dialog(
        show,
        "multi_job",
        JobPartitionPicker::Multi {
            dimensions: vec![
                PartitionDimensionInfo {
                    name: "color".into(),
                    keys: vec!["r".into(), "g".into()],
                    total_count: 2,
                    keys_truncated: false,
                },
                PartitionDimensionInfo {
                    name: "size".into(),
                    keys: vec!["s".into()],
                    total_count: 1,
                    keys_truncated: false,
                },
            ],
            asset_key: None,
        },
    );
    flush_effects().await;

    // Pick both colors + the only size → cartesian = 2 runs.
    let rows = query_all(&host, ".exec-dialog-partition-row");
    click(&rows[0], false); // color = r
    flush_effects().await;
    click(&rows[1], false); // color = g
    flush_effects().await;
    click(&rows[2], false); // size = s
    flush_effects().await;

    let btn_label = query_one(&host, ".modal-footer .btn-primary")
        .text_content()
        .unwrap();
    assert_eq!(btn_label, "Execute 2 runs");
}

#[wasm_bindgen_test]
async fn reopen_clears_previous_error() {
    let show = RwSignal::new(true);
    let host = mount_dialog(
        show,
        "daily_job",
        JobPartitionPicker::SingleDim {
            keys: vec!["2025-01-01".into()],
        },
    );
    flush_effects().await;

    click(&query_one(&host, ".modal-footer .btn-primary"), false);
    flush_effects().await;
    assert_eq!(query_all(&host, ".error-msg").len(), 1);

    // Close and reopen — the `show` effect should reset the error.
    show.set(false);
    flush_effects().await;
    show.set(true);
    flush_effects().await;

    assert_eq!(query_all(&host, ".error-msg").len(), 0);
}
