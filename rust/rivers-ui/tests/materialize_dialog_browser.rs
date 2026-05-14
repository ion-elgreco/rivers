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
async fn show_true_renders_modal_header_and_one_checkbox_per_asset() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into(), "c".into()]);
    flush_effects().await;

    let header = query_one(&host, ".modal-header h2").text_content().unwrap();
    assert_eq!(header, "Materialize Assets");

    assert_eq!(query_all(&host, ".checkbox-list .checkbox-item").len(), 3);
}

#[wasm_bindgen_test]
async fn assets_default_to_all_selected_when_dialog_opens() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into(), "b".into()]);
    flush_effects().await;

    let checked: Vec<bool> = query_all(&host, ".checkbox-list input[type=checkbox]")
        .into_iter()
        .map(|el| {
            let input: HtmlInputElement = el.dyn_into().unwrap();
            input.checked()
        })
        .collect();
    assert_eq!(checked, vec![true, true]);
}

#[wasm_bindgen_test]
async fn deselecting_all_assets_disables_submit_button() {
    let show = RwSignal::new(true);
    let host = mount_no_picker(show, vec!["a".into()]);
    flush_effects().await;

    let cb_el = query_one(&host, ".checkbox-list input[type=checkbox]");
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
async fn submit_button_label_reflects_partition_count_under_multi_picker() {
    let show = RwSignal::new(true);
    let host = mount_with_picker(
        show,
        vec!["asset.a".into()],
        JobPartitionPicker::Multi {
            dimensions: vec![
                PartitionDimensionInfo {
                    name: "color".into(),
                    keys: vec!["r".into(), "g".into()],
                },
                PartitionDimensionInfo {
                    name: "size".into(),
                    keys: vec!["s".into()],
                },
            ],
        },
    );
    flush_effects().await;

    // Initially no partitions selected → label = "Materialize" (n=0 → 1).
    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert_eq!(btn.text_content().unwrap(), "Materialize");
    // Disabled because multi picker requires at least one selection.
    assert!(btn.has_attribute("disabled"));

    // Select two colors + the size → 2 cartesian combos.
    let rows = query_all(&host, ".exec-dialog-partition-row");
    click(&rows[0], false);
    flush_effects().await;
    click(&rows[1], false);
    flush_effects().await;
    click(&rows[2], false);
    flush_effects().await;

    let btn = query_one(&host, ".modal-footer .btn-primary");
    assert_eq!(btn.text_content().unwrap(), "Materialize 2 runs");
    assert!(!btn.has_attribute("disabled"));
}
