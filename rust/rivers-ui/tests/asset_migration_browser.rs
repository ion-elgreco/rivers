//! Browser tests for the migration panel of the asset page.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{
    click, fresh_mount_target, install_recording_fetch_mock, nav_to, query_all, query_one,
    request_bodies, wait_until,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use leptos_router::components::Router;
use rivers_ui::components::asset_migration::AssetMigrationPanel;
use rivers_ui::types::{AssetMigration, MigrationState};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// 2100-01-01T00:00:00Z and 2001-01-01T00:00:00Z, in nanoseconds.
const LATER: i64 = 4_102_444_800_000_000_000;
const PASSED: i64 = 978_307_200_000_000_000;

fn mount(
    migration: AssetMigration,
    declared: Option<u32>,
    start_at: Option<i64>,
) -> web_sys::HtmlElement {
    nav_to("/locations/dev/shop/assets/orders");
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! { <Router><AssetMigrationPanel migration declared start_at/></Router> }
    })
    .forget();
    target
}

fn migration(state: MigrationState) -> AssetMigration {
    AssetMigration {
        asset_key: "orders".into(),
        applied_version: 1,
        state,
        target_version: Some(2),
        reason: None,
        run_id: None,
        backfill_id: None,
        drain_runs: Vec::new(),
        upstream: Vec::new(),
    }
}

fn buttons(host: &web_sys::HtmlElement) -> Vec<String> {
    query_all(host, "button")
        .iter()
        .map(|b| b.text_content().unwrap_or_default())
        .collect()
}

#[wasm_bindgen_test]
fn a_draining_migration_names_what_it_waits_for() {
    let host = mount(
        AssetMigration {
            drain_runs: vec!["aaaaaaaa-1111".into(), "bbbbbbbb-2222".into()],
            upstream: vec!["raw".into()],
            ..migration(MigrationState::Draining)
        },
        Some(2),
        None,
    );

    let text = host.text_content().unwrap_or_default();
    assert!(text.contains("Migrating to version 2"), "got: {text}");
    assert!(
        text.contains("The stored data is at version 1."),
        "got: {text}"
    );
    assert!(
        text.contains("Waits for raw to migrate first."),
        "got: {text}"
    );
    assert!(
        text.contains("Waits for 2 started runs that write or read the asset:"),
        "got: {text}"
    );
    let hrefs: Vec<String> = query_all(&host, ".migration-panel-runs a")
        .iter()
        .map(|a| a.get_attribute("href").unwrap_or_default())
        .collect();
    assert_eq!(
        hrefs,
        [
            "/locations/dev/shop/runs/aaaaaaaa-1111",
            "/locations/dev/shop/runs/bbbbbbbb-2222",
        ]
    );
    assert!(buttons(&host).is_empty());
}

#[wasm_bindgen_test]
fn a_running_rebuild_links_its_backfill() {
    let host = mount(
        AssetMigration {
            backfill_id: Some("cccccccc-3333".into()),
            ..migration(MigrationState::Running)
        },
        Some(2),
        None,
    );

    let link = query_one(&host, ".migration-panel a");
    assert_eq!(
        link.get_attribute("href").unwrap_or_default(),
        "/locations/dev/shop/backfills/cccccccc-3333"
    );
    assert_eq!(link.text_content().unwrap_or_default(), "cccccccc");
}

#[wasm_bindgen_test]
async fn a_failed_migration_shows_its_reason_and_retries() {
    let (_mock, requests) = install_recording_fetch_mock("true");
    let reason = "the convert to version 2 failed in run 1234: the warehouse is down";
    let host = mount(
        AssetMigration {
            reason: Some(reason.into()),
            ..migration(MigrationState::Failed)
        },
        Some(2),
        None,
    );

    assert_eq!(query_all(&host, ".migration-panel--stopped").len(), 1);
    assert!(
        host.text_content()
            .unwrap_or_default()
            .contains("The migration to version 2 failed"),
    );
    assert_eq!(
        query_one(&host, "pre.migration-panel-reason")
            .text_content()
            .unwrap_or_default(),
        reason
    );
    assert_eq!(buttons(&host), ["Retry"]);

    click(&query_one(&host, "button"), false);

    assert!(wait_until(|| requests.borrow().len() == 1).await);
    let sent: Vec<web_sys::Request> = requests.borrow().iter().cloned().collect();
    assert!(
        sent[0].url().contains("retry_migration"),
        "got: {}",
        sent[0].url()
    );
    let bodies = request_bodies(&sent).await;
    assert!(bodies[0].contains("asset_key=orders"), "got: {}", bodies[0]);
}

#[wasm_bindgen_test]
fn a_pending_migration_can_be_canceled_only_after_a_rollback() {
    let rolled_back = mount(migration(MigrationState::Pending), Some(1), None);
    assert_eq!(buttons(&rolled_back), ["Cancel migration"]);
    assert!(
        rolled_back
            .text_content()
            .unwrap_or_default()
            .contains("Waits for the daemon to start it.")
    );

    let deployed = mount(migration(MigrationState::Pending), Some(2), None);
    assert!(buttons(&deployed).is_empty());
}

#[wasm_bindgen_test]
async fn a_migration_that_starts_later_shows_its_time_and_starts_now() {
    let (_mock, requests) = install_recording_fetch_mock("true");
    let host = mount(migration(MigrationState::Pending), Some(2), Some(LATER));

    let text = host.text_content().unwrap_or_default();
    assert!(
        text.contains(
            "Starts at 2100-01-01 00:00:00 UTC. Until then, runs that write or read the asset wait."
        ),
        "got: {text}"
    );
    assert_eq!(buttons(&host), ["Start now"]);

    click(&query_one(&host, "button"), false);

    assert!(wait_until(|| requests.borrow().len() == 1).await);
    let sent: Vec<web_sys::Request> = requests.borrow().iter().cloned().collect();
    assert!(
        sent[0].url().contains("start_migration"),
        "got: {}",
        sent[0].url()
    );
    let bodies = request_bodies(&sent).await;
    assert!(bodies[0].contains("asset_key=orders"), "got: {}", bodies[0]);
}

#[wasm_bindgen_test]
fn start_now_needs_a_start_time_later_and_code_that_runs_the_migration() {
    let passed = mount(migration(MigrationState::Pending), Some(2), Some(PASSED));
    assert!(buttons(&passed).is_empty());
    assert!(
        passed
            .text_content()
            .unwrap_or_default()
            .contains("Waits for the daemon to start it.")
    );

    let older_code = mount(migration(MigrationState::Pending), Some(1), Some(LATER));
    assert_eq!(buttons(&older_code), ["Cancel migration"]);

    let draining = mount(migration(MigrationState::Draining), Some(2), Some(LATER));
    assert!(buttons(&draining).is_empty());
    assert!(
        !draining
            .text_content()
            .unwrap_or_default()
            .contains("Starts at")
    );
}
