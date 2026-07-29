//! Browser-based component tests for `MaterializeDialog`.
//!
//! Mirrors the `execute_job_dialog` test approach: mount inside a
//! `<Router>` because the component reads `use_current_location`, then
//! exercise show/hide, the asset-checkbox list, the tag input, and the
//! disabled-state logic for the submit button. Server-fn dispatch is
//! out of scope (would need a fetch mock).

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, nav_to, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::materialize_dialog::MaterializeDialog;
use rivers_ui::helpers::JobPartitionPicker;
use rivers_ui::types::PartitionDimensionInfo;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::HtmlInputElement;

wasm_bindgen_test_configure!(run_in_browser);

fn mount_no_picker(show: RwSignal<bool>, asset_keys: Vec<String>) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <MaterializeDialog
                    show=show
                    asset_keys=Signal::derive(move || asset_keys.clone())
                />
            </Router>
        }
    })
    .forget();
    target
}

fn mount_with_picker(
    show: RwSignal<bool>,
    asset_keys: Vec<String>,
    picker: JobPartitionPicker,
) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        let picker_signal = Signal::derive({
            let p = picker.clone();
            move || p.clone()
        });
        view! {
            <Router>
                <MaterializeDialog
                    show=show
                    asset_keys=Signal::derive(move || asset_keys.clone())
                    picker=picker_signal
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
    let host = mount_no_picker(show, vec!["asset.a".into()]);
    assert_eq!(query_all(&host, ".modal-overlay").len(), 0);
}

#[wasm_bindgen_test]
async fn show_true_renders_modal_header_and_one_row_per_asset() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into(), "c".into()]);
    flush_effects().await;

    let header = query_one(&host, ".modal-header h2").text_content().unwrap();
    assert_eq!(header, "Materialize");

    assert_eq!(
        query_all(&host, ".mat-dialog-asset-list .mat-dialog-asset-row").len(),
        3
    );
}

#[wasm_bindgen_test]
async fn asset_rows_render_without_waiting_on_the_metadata_fetch() {
    // There is no server in the CSR harness, so `get_assets` can only fail —
    // the list must still render every key, just without status decoration.
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into()]);
    flush_effects().await;

    let names: Vec<String> = query_all(&host, ".mat-dialog-asset-name")
        .into_iter()
        .map(|el| el.text_content().unwrap_or_default())
        .collect();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

#[wasm_bindgen_test]
async fn assets_default_to_all_selected_when_dialog_opens() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into()]);
    flush_effects().await;

    let checked: Vec<bool> = query_all(&host, ".mat-dialog-asset-row input[type=checkbox]")
        .into_iter()
        .map(|el| {
            let input: HtmlInputElement = el.dyn_into().unwrap();
            input.checked()
        })
        .collect();
    assert_eq!(checked, vec![true, true]);
}

#[wasm_bindgen_test]
async fn clear_deselects_every_asset_and_select_all_restores_them() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into()]);
    flush_effects().await;

    let link_named = |name: &str| {
        query_all(&host, ".mat-dialog-col-actions .bulk-link-btn")
            .into_iter()
            .find(|el| el.text_content().unwrap_or_default() == name)
            .unwrap()
    };

    click(&link_named("Clear"), false);
    flush_effects().await;
    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "Nothing selected"
    );

    click(&link_named("Select all"), false);
    flush_effects().await;
    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "2 assets · 1 run"
    );
}

#[wasm_bindgen_test]
async fn deselecting_all_assets_disables_submit_button() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    let cb_el = query_one(&host, ".mat-dialog-asset-row input[type=checkbox]");
    let cb: HtmlInputElement = cb_el.clone().dyn_into().unwrap();
    cb.set_checked(false);

    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    let ev = web_sys::Event::new_with_event_init_dict("change", &init).unwrap();
    cb.dispatch_event(&ev).unwrap();
    flush_effects().await;

    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert!(btn.has_attribute("disabled"));
}

#[wasm_bindgen_test]
async fn add_tag_button_appends_to_tag_list() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    // Two text inputs in the tag-input row + the Add button.
    let inputs = query_all(&host, ".tag-input-row .form-input");
    assert_eq!(inputs.len(), 2);

    let key_input: HtmlInputElement = inputs[0].clone().dyn_into().unwrap();
    let val_input: HtmlInputElement = inputs[1].clone().dyn_into().unwrap();
    key_input.set_value("env");
    val_input.set_value("prod");

    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    key_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    val_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    flush_effects().await;

    let add_btn = query_one(&host, ".tag-input-row .btn");
    click(&add_btn, false);
    flush_effects().await;

    let tags = query_all(&host, ".tag-list .tag");
    assert_eq!(tags.len(), 1);
    assert!(tags[0].text_content().unwrap().contains("env=prod"));
}

/// Tags are submitted with the run (including `rivers/priority`), so one left
/// over from a previous open would silently attach to the next action run.
#[wasm_bindgen_test]
async fn reopening_drops_the_previous_tags() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    let inputs = query_all(&host, ".tag-input-row .form-input");
    let key_input: HtmlInputElement = inputs[0].clone().dyn_into().unwrap();
    let val_input: HtmlInputElement = inputs[1].clone().dyn_into().unwrap();
    key_input.set_value("env");
    val_input.set_value("prod");

    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    key_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    val_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    flush_effects().await;

    click(&query_one(&host, ".tag-input-row .btn"), false);
    flush_effects().await;
    assert_eq!(query_all(&host, ".tag-list .tag").len(), 1);

    show.set(false);
    flush_effects().await;
    show.set(true);
    flush_effects().await;

    assert!(
        query_all(&host, ".tag-list .tag").is_empty(),
        "tags from the previous open must not carry over"
    );
    let inputs = query_all(&host, ".tag-input-row .form-input");
    let key_input: HtmlInputElement = inputs[0].clone().dyn_into().unwrap();
    let val_input: HtmlInputElement = inputs[1].clone().dyn_into().unwrap();
    assert_eq!(key_input.value(), "");
    assert_eq!(val_input.value(), "");
}

#[wasm_bindgen_test]
async fn removing_a_tag_drops_it_from_the_list() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    // Add one tag.
    let inputs = query_all(&host, ".tag-input-row .form-input");
    let key_input: HtmlInputElement = inputs[0].clone().dyn_into().unwrap();
    let val_input: HtmlInputElement = inputs[1].clone().dyn_into().unwrap();
    key_input.set_value("env");
    val_input.set_value("prod");
    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    key_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    val_input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
    flush_effects().await;
    click(&query_one(&host, ".tag-input-row .btn"), false);
    flush_effects().await;
    assert_eq!(query_all(&host, ".tag-list .tag").len(), 1);

    // Click the per-tag remove button.
    click(&query_one(&host, ".tag-list .tag-remove"), false);
    flush_effects().await;
    assert_eq!(query_all(&host, ".tag-list .tag").len(), 0);
}

#[wasm_bindgen_test]
async fn cancel_hides_dialog() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    let cancel = query_all(&host, ".modal-footer .btn")
        .into_iter()
        .find(|el| el.text_content().unwrap_or_default() == "Cancel")
        .unwrap();
    click(&cancel, false);
    flush_effects().await;

    assert!(!show.get_untracked());
}

#[wasm_bindgen_test]
async fn summary_rail_reflects_partition_count_under_multi_picker() {
    let show = RwSignal::new(true);
    let host = mount_with_picker(
        show,
        vec!["asset.a".into()],
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
            truncated: false,
        },
    );
    flush_effects().await;

    // Nothing picked yet — the rail asks for a partition and submit stays off.
    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "1 asset · select a partition"
    );
    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert!(btn.has_attribute("disabled"));

    // Select two colors + the size → 2 cartesian combos.
    let rows = query_all(&host, ".exec-dialog-partition-row");
    click(&rows[0], false);
    flush_effects().await;
    click(&rows[1], false);
    flush_effects().await;
    click(&rows[2], false);
    flush_effects().await;

    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "1 asset · 2 partitions · 2 runs"
    );
    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert_eq!(btn.text_content().unwrap(), "Materialize");
    assert!(!btn.has_attribute("disabled"));
}

#[wasm_bindgen_test]
async fn crossing_the_backfill_threshold_switches_the_rail_and_button() {
    let show = RwSignal::new(true);
    let host = mount_with_picker(
        show,
        vec!["asset.a".into()],
        JobPartitionPicker::SingleDim {
            keys: vec!["k1".into(), "k2".into(), "k3".into()],
            truncated: false,
        },
    );
    flush_effects().await;

    let rows = query_all(&host, ".exec-dialog-partition-row");
    for row in rows.iter().take(3) {
        click(row, false);
        flush_effects().await;
    }

    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "1 asset · 3 partitions · 1 backfill"
    );
    assert_eq!(
        query_one(&host, ".modal-footer .btn-primary")
            .text_content()
            .unwrap(),
        "Launch backfill"
    );
}

#[wasm_bindgen_test]
async fn unpartitioned_selection_says_so_instead_of_showing_a_picker() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    assert_eq!(query_all(&host, ".exec-dialog-partition-row").len(), 0);
    assert!(
        query_one(&host, ".mat-dialog-note")
            .text_content()
            .unwrap()
            .contains("Not partitioned")
    );
}

/// The picker owns the reset, but it is only mounted for a partitioned
/// selection — reopening on an unpartitioned asset used to submit the previous
/// open's keys.
#[wasm_bindgen_test]
async fn reopening_unpartitioned_drops_the_previous_partition_keys() {
    let show = RwSignal::new(true);
    let picker = RwSignal::new(JobPartitionPicker::SingleDim {
        keys: vec!["k1".into(), "k2".into()],
        truncated: false,
    });
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, move || {
        view! {
            <Router>
                <MaterializeDialog
                    show=show
                    asset_keys=Signal::derive(|| vec!["a".to_string()])
                    picker=Signal::derive(move || picker.get())
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    let rows = query_all(&host, ".exec-dialog-partition-row");
    click(&rows[0], false);
    flush_effects().await;
    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "1 asset · 1 partition · 1 run"
    );

    // Cancel, then reopen against an unpartitioned asset.
    show.set(false);
    flush_effects().await;
    picker.set(JobPartitionPicker::None);
    show.set(true);
    flush_effects().await;

    assert_eq!(
        query_one(&host, ".mat-dialog-summary")
            .text_content()
            .unwrap(),
        "1 asset · 1 run",
        "an unpartitioned open inherited the previous selection's keys"
    );
}

/// Nothing else in the product distinguishes an `Outcome.Unmaterialize` verb
/// from a benign one, so the dialog has to.
#[wasm_bindgen_test]
async fn destructive_verb_is_named_and_flagged() {
    let show = RwSignal::new(true);
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, move || {
        view! {
            <Router>
                <MaterializeDialog
                    show=show
                    asset_keys=Signal::derive(|| vec!["a".to_string()])
                    action=Signal::derive(|| Some("purge".to_string()))
                    destructive=Signal::derive(|| true)
                />
            </Router>
        }
    })
    .forget();
    flush_effects().await;

    assert_eq!(
        query_one(&host, ".modal-header h2").text_content().unwrap(),
        "purge"
    );
    assert!(
        query_one(&host, ".mat-dialog-warning")
            .text_content()
            .unwrap()
            .contains("Clears materialization state")
    );
    assert_eq!(query_all(&host, ".modal-footer .btn-danger").len(), 1);
}
