//! Browser-based component tests for `EventTimeline`.
//!
//! Each event maps to a `<div class="timeline-item">`. The header packs
//! a timestamp, a typed badge, plus optional asset / partition / data
//! version tags. Tests pin: row count, badge class per `EventType`, and
//! that absent optional fields don't render empty tags.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::event_timeline::EventTimeline;
use rivers_ui::types::{EventType, StoredEvent};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn event(
    id: &str,
    ty: EventType,
    asset: Option<&str>,
    partition: Option<&str>,
    data_version: Option<&str>,
) -> StoredEvent {
    StoredEvent {
        id: id.to_string(),
        event_type: ty,
        asset_key: asset.map(str::to_string),
        run_id: "run-1".to_string(),
        partition_key: partition.map(str::to_string),
        // Use a specific past timestamp so the rendered absolute time is
        // deterministic across machines.
        timestamp: 1_700_000_000_000_000_000,
        metadata: Vec::new(),
        data_version: data_version.map(str::to_string),
    }
}

#[wasm_bindgen_test]
fn empty_event_list_renders_no_items() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=Vec::new() /> }
    });
    assert_eq!(query_all(&target, ".timeline-item").len(), 0);
}

#[wasm_bindgen_test]
fn each_event_renders_one_timeline_item() {
    let events = vec![
        event("a", EventType::Materialization, Some("asset.a"), None, None),
        event("b", EventType::StepFailure, Some("asset.b"), None, None),
        event("c", EventType::Observation, None, None, None),
    ];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    assert_eq!(query_all(&target, ".timeline-item").len(), 3);
}

#[wasm_bindgen_test]
fn materialization_uses_success_badge() {
    let events = vec![event(
        "a",
        EventType::Materialization,
        Some("asset.a"),
        None,
        None,
    )];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    let badge = query_one(&target, ".timeline-item-header .badge");
    assert!(
        badge.class_name().contains("badge-success"),
        "Materialization should map to badge-success, got: {}",
        badge.class_name()
    );
}

#[wasm_bindgen_test]
fn step_failure_uses_error_badge() {
    let events = vec![event("a", EventType::StepFailure, None, None, None)];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    assert!(
        query_one(&target, ".badge")
            .class_name()
            .contains("badge-error")
    );
}

#[wasm_bindgen_test]
fn unknown_event_type_falls_back_to_muted() {
    let events = vec![event("a", EventType::RunQueued, None, None, None)];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    assert!(
        query_one(&target, ".badge")
            .class_name()
            .contains("badge-muted")
    );
}

#[wasm_bindgen_test]
fn omitted_asset_and_partition_yield_no_extra_tags() {
    let events = vec![event("a", EventType::Materialization, None, None, None)];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    assert_eq!(query_all(&target, ".timeline-item-header .tag").len(), 0);
}

#[wasm_bindgen_test]
fn data_version_renders_truncated_version_tag() {
    let events = vec![event(
        "a",
        EventType::Materialization,
        None,
        None,
        Some("0123456789abcdef"),
    )];
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), move || {
        view! { <EventTimeline events=events.clone() /> }
    });
    let tag = query_one(&target, ".tag.tag-muted");
    let text = tag.text_content().unwrap();
    // First 8 chars after the `v:` prefix, per the timeline render.
    assert_eq!(text, "v:01234567");
}
