//! Browser-based component tests for the small `ui_kit` primitives that
//! don't depend on the router (`leptos_router::components::A`) or any
//! external context: `StatusChip`, `Sparkline`, `KindBadge`, `Tag`,
//! `EmptyState`, `ProgressBar`, `SectionHeader`.
//!
//! Tests pin the most refactor-sensitive concerns: the kind → CSS-class
//! mapping (StatusChip / KindBadge / Sparkline degenerate paths) and
//! the prop-driven shape of the rendered DOM.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{flush_effects, fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::ui_kit::{
    EmptyState, KindBadge, ProgressBar, SectionHeader, Sparkline, StatusChip, Tag,
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn status_chip_uses_kind_in_class_and_label() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <StatusChip kind="success" /> }
    });

    let chip = query_one(&target, "span.chip");
    assert!(!chip.class_name().contains("chip--sm"));
    assert!(chip.text_content().unwrap().contains("success"));
    assert!(
        query_one(&target, "span.dot")
            .class_name()
            .contains("dot-success")
    );
}

#[wasm_bindgen_test]
fn status_chip_small_variant_adds_modifier_class() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <StatusChip kind="warning" small=true /> }
    });
    assert!(
        query_one(&target, "span.chip")
            .class_name()
            .contains("chip--sm")
    );
}

#[wasm_bindgen_test]
fn sparkline_with_zero_or_one_point_renders_empty_svg() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <Sparkline points=vec![1.0_f64] /> }
    });

    // Empty-svg branch: no <path>, no <circle>.
    assert_eq!(query_all(&target, "svg path").len(), 0);
    assert_eq!(query_all(&target, "svg circle").len(), 0);
}

#[wasm_bindgen_test]
fn sparkline_with_multiple_points_renders_path_and_endpoint_circle() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <Sparkline points=vec![1.0_f64, 2.0, 3.0, 2.0] /> }
    });

    assert_eq!(query_all(&target, "svg path").len(), 1);
    assert_eq!(query_all(&target, "svg circle").len(), 1);
}

#[wasm_bindgen_test]
fn kind_badge_python_alias_maps_to_python_class() {
    for s in ["python", "py", "PYTHON"] {
        let target = fresh_mount_target();
        let kind_for_view = s.to_string();
        let _handle = mount_to(target.clone(), move || {
            view! { <KindBadge kind=kind_for_view.clone() /> }
        });
        assert!(
            query_one(&target, ".kind-badge")
                .class_name()
                .contains("kind-badge--python"),
            "{s} should map to kind-badge--python"
        );
    }
}

#[wasm_bindgen_test]
fn kind_badge_unknown_falls_back_to_other() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <KindBadge kind="rust" /> }
    });
    assert!(
        query_one(&target, ".kind-badge")
            .class_name()
            .contains("kind-badge--other")
    );
}

#[wasm_bindgen_test]
fn tag_renders_label_and_optional_color() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <Tag label="hot" color="#f00" /> }
    });
    let tag = query_one(&target, ".rv-tag");
    assert_eq!(tag.text_content().unwrap(), "hot");
    let style = tag.get_attribute("style").unwrap_or_default();
    assert!(
        style.contains("#f00"),
        "expected color carried into style: {style}"
    );
}

#[wasm_bindgen_test]
fn tag_without_color_omits_style_attr() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <Tag label="plain" /> }
    });
    let tag = query_one(&target, ".rv-tag");
    // The style attr may either be absent or empty when no color was set.
    let style = tag.get_attribute("style").unwrap_or_default();
    assert!(style.is_empty(), "expected no inline style, got: {style:?}");
}

#[wasm_bindgen_test]
fn empty_state_renders_message_only_by_default() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <EmptyState message="No runs yet" /> }
    });
    let text = target.text_content().unwrap_or_default();
    assert!(text.contains("No runs yet"));
}

#[wasm_bindgen_test]
fn empty_state_renders_optional_hint_when_present() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <EmptyState message="Empty" hint="Try a different filter" /> }
    });
    let text = target.text_content().unwrap_or_default();
    assert!(text.contains("Empty"));
    assert!(text.contains("Try a different filter"));
}

#[wasm_bindgen_test]
async fn progress_bar_clamps_value_to_0_100_percent() {
    let target = fresh_mount_target();
    let value = RwSignal::new(0.0_f64);
    let _handle = mount_to(target.clone(), move || {
        view! { <ProgressBar value=Signal::derive(move || value.get()) /> }
    });

    let fill = query_one(&target, ".rv-progress-fill");
    assert!(
        fill.get_attribute("style")
            .unwrap_or_default()
            .contains("width:0.0%"),
        "initial 0.0 → 0.0%"
    );

    value.set(2.5); // 250% — must clamp to 100%.
    flush_effects().await;
    assert!(
        fill.get_attribute("style")
            .unwrap_or_default()
            .contains("width:100.0%"),
        "out-of-range value should clamp"
    );
}

#[wasm_bindgen_test]
fn section_header_omits_count_when_none() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <SectionHeader label="ASSETS" /> }
    });
    assert!(target.text_content().unwrap().contains("ASSETS"));
    assert_eq!(query_all(&target, ".section-header-count").len(), 0);
}

#[wasm_bindgen_test]
fn section_header_renders_count_when_set() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <SectionHeader label="ASSETS" count="42" /> }
    });
    let count = query_one(&target, ".section-header-count");
    assert_eq!(count.text_content().unwrap(), "42");
}
