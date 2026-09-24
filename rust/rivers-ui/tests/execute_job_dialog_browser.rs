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

use common::{
    Reply, click, flush_effects, fresh_mount_target, go_back, install_routing_fetch_mock, nav_to,
    query_all, query_one, request_bodies, set_checked, wait_until, yield_macro,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::{FlatRoutes, Route, Router};
use leptos_router::hooks::use_location;
use leptos_router::path;
use rivers_ui::components::execute_job_dialog::ExecuteJobDialog;
use rivers_ui::helpers::JobPartitionPicker;
use rivers_ui::types::{AssetActionInfo, PartitionDimensionInfo};
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

fn mount_verb_dialog(verb: AssetActionInfo) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    let show = RwSignal::new(true);
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(|| "purge_job".to_string())
                    picker=Signal::derive(|| JobPartitionPicker::SingleDim {
                        keys: vec!["p1".into(), "p2".into()],
                        truncated: false,
                    })
                    verb=Signal::derive(move || Some(verb.clone()))
                />
            </Router>
        }
    })
    .forget();
    target
}

fn two_dim_picker() -> JobPartitionPicker {
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
        truncated: false,
    }
}

fn mount_multi_verb_dialog(verb: AssetActionInfo) -> web_sys::HtmlElement {
    nav_to("/locations/default/demo");
    let target = fresh_mount_target();
    let show = RwSignal::new(true);
    mount_to(target.clone(), move || {
        view! {
            <Router>
                <ExecuteJobDialog
                    show=show
                    job_name=Signal::derive(|| "purge_job".to_string())
                    picker=Signal::derive(two_dim_picker)
                    verb=Signal::derive(move || Some(verb.clone()))
                />
            </Router>
        }
    })
    .forget();
    target
}

fn verb(name: &str, outcome: &str, partitioning: &str) -> AssetActionInfo {
    AssetActionInfo {
        name: name.to_string(),
        outcome: outcome.to_string(),
        exclusive: true,
        partitioning: partitioning.to_string(),
        description: None,
    }
}

/// An `Optional` verb (delete) runs on every partition without a key, so an
/// empty pick must not submit: only the explicit whole-asset choice does.
#[wasm_bindgen_test]
async fn optional_key_verb_needs_a_partition_or_the_whole_asset() {
    let host = mount_verb_dialog(verb("purge", "unchanged", "optional"));
    flush_effects().await;

    click(&query_one(&host, ".modal-footer .btn-primary"), false);
    flush_effects().await;

    let errors: Vec<String> = query_all(&host, ".error-msg")
        .iter()
        .filter_map(|e| e.text_content())
        .collect();
    assert_eq!(errors, vec!["Select at least one partition.".to_string()]);

    // Choosing the whole asset hides the key list: there is nothing to pick.
    set_checked(&query_one(&host, ".whole-asset-choice input"), true);
    flush_effects().await;
    assert_eq!(query_all(&host, ".exec-dialog-partition-row").len(), 0);
}

/// A `Required` verb has no whole-asset form to offer.
#[wasm_bindgen_test]
async fn required_key_verb_offers_no_whole_asset_choice() {
    let host = mount_verb_dialog(verb("purge", "unmaterialize", "required"));
    flush_effects().await;

    assert_eq!(query_all(&host, ".whole-asset-choice").len(), 0);
    assert_eq!(query_all(&host, ".exec-dialog-partition-row").len(), 2);
}

/// Values in only some dimensions expand to no key, but that is not an empty
/// pick: submitting it keyless would run an optional-key delete on the whole
/// asset.
#[wasm_bindgen_test]
async fn optional_key_verb_rejects_a_partial_multi_pick() {
    let host = mount_multi_verb_dialog(verb("delete", "unmaterialize", "optional"));
    flush_effects().await;

    let rows = query_all(&host, ".exec-dialog-partition-row");
    click(&rows[0], false); // color = r; size left empty
    flush_effects().await;
    click(&query_one(&host, ".modal-footer .btn-danger"), false);
    flush_effects().await;

    let errors: Vec<String> = query_all(&host, ".error-msg")
        .iter()
        .filter_map(|e| e.text_content())
        .collect();
    assert_eq!(
        errors,
        vec!["Select at least one value for every dimension.".to_string()]
    );
}

/// The job pages are another route to a destructive verb; the dialog must
/// flag it the way the materialize dialog does.
#[wasm_bindgen_test]
async fn destructive_job_verb_is_flagged() {
    let host = mount_verb_dialog(verb("purge", "unmaterialize", "required"));
    flush_effects().await;

    assert!(
        query_one(&host, ".mat-dialog-warning")
            .text_content()
            .unwrap()
            .contains("Clears materialization state")
    );
    assert_eq!(query_all(&host, ".modal-footer .btn-danger").len(), 1);
    assert!(
        query_one(&host, ".modal-body")
            .text_content()
            .unwrap()
            .contains("purge")
    );
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
            truncated: false,
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
            truncated: false,
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
            truncated: false,
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
            truncated: false,
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
            truncated: false,
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

/// The jobs list keeps its page, and so its dialog, across a Back to another
/// code location, and the dialog submits against the location current at the
/// click: Execute ran prod's same-named job while the dialog showed staging's.
#[wasm_bindgen_test]
async fn back_to_another_code_location_closes_the_dialog() {
    let (_mock, requests) = install_routing_fetch_mock(|_| Reply::Pending);
    nav_to("/locations/prod/core/jobs");
    nav_to("/locations/staging/core/jobs");
    let show = RwSignal::new(true);
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, move || {
        view! {
            <Router>
                <FlatRoutes fallback=|| view! { <div class="no-route"></div> }>
                    <Route
                        path=path!("/locations/:loc_ns/:loc_name/jobs")
                        view=move || {
                            let pathname = use_location().pathname;
                            view! {
                                <span class="route-path">{move || pathname.get()}</span>
                                <ExecuteJobDialog
                                    show=show
                                    job_name=Signal::derive(|| "purge_events".to_string())
                                    picker=Signal::derive(|| JobPartitionPicker::SingleDim {
                                        keys: vec!["p1".into(), "p2".into()],
                                        truncated: false,
                                    })
                                    verb=Signal::derive(|| {
                                        Some(verb("delete", "unmaterialize", "optional"))
                                    })
                                />
                            }
                        }
                    />
                </FlatRoutes>
            </Router>
        }
    })
    .forget();
    assert!(
        wait_until(|| !query_all(&host, ".exec-dialog-partition-row").is_empty()).await,
        "the dialog never rendered"
    );
    // The dialog's opening Effects reset the pick: let them run first.
    yield_macro().await;

    click(&query_all(&host, ".exec-dialog-partition-row")[0], false);
    let picked = || {
        query_one(&host, ".exec-dialog-partition-count")
            .text_content()
            .unwrap_or_default()
    };
    assert!(
        wait_until(|| picked() == "1 / 2 selected").await,
        "the pick never registered (at {})",
        picked()
    );

    go_back();
    let route_path = || {
        query_one(&host, ".route-path")
            .text_content()
            .unwrap_or_default()
    };
    assert!(
        wait_until(|| route_path() == "/locations/prod/core/jobs").await,
        "Back never reached prod (at {})",
        route_path()
    );
    // Whatever is still on screen, a click must not reach prod.
    if let Some(submit) = host.query_selector(".modal-footer .btn-danger").unwrap() {
        click(&submit, false);
    }
    for _ in 0..5 {
        yield_macro().await;
    }

    let sent: Vec<web_sys::Request> = requests
        .borrow()
        .iter()
        .filter(|r| r.url().contains("execute_job"))
        .cloned()
        .collect();
    assert_eq!(request_bodies(&sent).await, Vec::<String>::new());
    assert!(!show.get_untracked(), "the dialog stayed open on prod");
    assert!(query_all(&host, ".modal-overlay").is_empty());
}
