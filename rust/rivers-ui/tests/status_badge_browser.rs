//! Browser-based component tests for `StatusBadge`.
//!
//! Pins the status → CSS-class mapping at the rendered DOM level so a
//! refactor that breaks the lookup table fails loudly here instead of
//! shipping as a silently-recoloured badge.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::status_badge::StatusBadge;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn class_for(status: &str) -> String {
    let target = fresh_mount_target();
    let s = status.to_string();
    let _handle = mount_to(target.clone(), move || {
        view! { <StatusBadge status=s.clone() /> }
    });
    query_one(&target, "span.badge").class_name()
}

#[wasm_bindgen_test]
fn success_status_uses_success_class() {
    let cls = class_for("SUCCESS");
    assert!(cls.contains("badge-success"), "got: {cls}");
}

#[wasm_bindgen_test]
fn running_and_started_share_warning_class() {
    assert!(class_for("RUNNING").contains("badge-warning"));
    assert!(class_for("STARTED").contains("badge-warning"));
}

#[wasm_bindgen_test]
fn failure_status_uses_error_class() {
    assert!(class_for("FAILURE").contains("badge-error"));
}

#[wasm_bindgen_test]
fn stopped_and_canceled_share_muted_class() {
    assert!(class_for("STOPPED").contains("badge-muted"));
    assert!(class_for("NOTSTARTED").contains("badge-muted"));
    assert!(class_for("CANCELED").contains("badge-muted"));
}

#[wasm_bindgen_test]
fn unknown_status_falls_back_to_info() {
    assert!(class_for("WHATEVER").contains("badge-info"));
}

#[wasm_bindgen_test]
fn lookup_is_case_insensitive() {
    // Inputs come from various sources; a lowercased "success" should
    // still hit the success branch.
    assert!(class_for("success").contains("badge-success"));
    assert!(class_for("Failure").contains("badge-error"));
}

#[wasm_bindgen_test]
fn renders_status_text_verbatim() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <StatusBadge status="QUEUED".to_string() /> }
    });
    assert_eq!(
        query_one(&target, "span.badge").text_content().unwrap(),
        "QUEUED"
    );
}
