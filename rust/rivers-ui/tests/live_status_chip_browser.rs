//! Browser-based component tests for `LiveStatusChip` and the
//! non-hydrate fallback `use_live_kick`.
//!
//! `LiveStatusChip` is a pure presentation chip driven by a
//! `Signal<LiveStatus>`, so no fetch mocking or SSE is needed. The
//! production `use_live_kick` is gated on `feature = "hydrate"` and
//! opens an `EventSource` against `/api/events`; tests run under the
//! `csr` feature so they exercise the no-op fallback that just returns
//! `Reconnecting`.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::live::{LiveStatus, LiveStatusChip, use_live_kick};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn mount_chip(initial: LiveStatus) -> (web_sys::HtmlElement, RwSignal<LiveStatus>, RwSignal<u32>) {
    let target = fresh_mount_target();
    let status = RwSignal::new(initial);
    let refreshed = RwSignal::new(0u32);
    mount_to(target.clone(), move || {
        view! {
            <LiveStatusChip
                status=Signal::derive(move || status.get())
                on_refresh=Callback::new(move |_| refreshed.update(|n| *n += 1))
            />
        }
    })
    .forget();
    (target, status, refreshed)
}

#[wasm_bindgen_test]
fn live_status_paints_live_class_and_label() {
    let (host, _, _) = mount_chip(LiveStatus::Live);
    let chip = query_one(&host, ".live-chip");
    assert!(
        chip.class_name().contains("live-chip--live"),
        "got: {}",
        chip.class_name()
    );
    let label = query_one(&host, ".live-chip-label").text_content().unwrap();
    assert_eq!(label, "Live");
}

#[wasm_bindgen_test]
fn reconnecting_status_paints_reconnecting_modifier_class() {
    let (host, _, _) = mount_chip(LiveStatus::Reconnecting);
    let chip = query_one(&host, ".live-chip");
    assert!(chip.class_name().contains("live-chip--reconnecting"));
    let label = query_one(&host, ".live-chip-label").text_content().unwrap();
    assert_eq!(label, "Reconnecting");
}

#[wasm_bindgen_test]
fn stale_status_paints_stale_modifier_and_descriptive_title() {
    let (host, _, _) = mount_chip(LiveStatus::Stale);
    let chip = query_one(&host, ".live-chip");
    assert!(chip.class_name().contains("live-chip--stale"));
    let title = query_one(&host, ".live-chip-group")
        .get_attribute("title")
        .unwrap_or_default();
    assert!(
        title.contains("5-min poll"),
        "stale title should mention safety-net poll, got: {title}"
    );
}

#[wasm_bindgen_test]
async fn flipping_status_signal_updates_class_and_label_reactively() {
    let (host, status, _) = mount_chip(LiveStatus::Reconnecting);

    status.set(LiveStatus::Live);
    flush_effects().await;
    let chip = query_one(&host, ".live-chip");
    assert!(chip.class_name().contains("live-chip--live"));
    assert_eq!(
        query_one(&host, ".live-chip-label").text_content().unwrap(),
        "Live"
    );

    status.set(LiveStatus::Stale);
    flush_effects().await;
    let chip = query_one(&host, ".live-chip");
    assert!(chip.class_name().contains("live-chip--stale"));
}

#[wasm_bindgen_test]
async fn refresh_button_invokes_on_refresh_callback() {
    let (host, _, refreshed) = mount_chip(LiveStatus::Stale);

    click(&query_one(&host, ".live-chip-refresh"), false);
    flush_effects().await;
    click(&query_one(&host, ".live-chip-refresh"), false);
    flush_effects().await;

    assert_eq!(refreshed.get_untracked(), 2);
}

#[wasm_bindgen_test]
fn use_live_kick_under_csr_returns_reconnecting() {
    // The production hook lives behind `feature = "hydrate"`; under the
    // `csr` test feature only the no-op fallback compiles. Pin its
    // signal to avoid silently regressing the SSR/CSR path.
    let owner = leptos::reactive::owner::Owner::new();
    owner.with(|| {
        let s = use_live_kick(&["runs", "events"], 500, Callback::new(|_| {}));
        assert_eq!(s.get_untracked(), LiveStatus::Reconnecting);
    });
}
