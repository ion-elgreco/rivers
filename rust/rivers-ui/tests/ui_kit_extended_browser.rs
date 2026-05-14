//! Browser-based component tests for additional `ui_kit` primitives
//! that don't depend on the router (i.e. don't take an `href` they
//! actually use, or take an explicit no-href branch).
//!
//! Covers: `FilterPill`, `FilterPills`, `StatTile` (no-href branch),
//! `DonutStatCard`, `SummaryCard` (no-href branch), `AlertCard`,
//! `UnderlineTabs`, `Topbar` (plain crumbs), `EvalTimelineBars`,
//! `PartitionCell` + `partition_scheme_for`, `DurationCell`,
//! `LaunchedByCell`.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, flush_effects, fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::ui_kit::{
    AlertCard, AlertSev, Crumb, DonutStatCard, DurationCell, EvalTimelineBars, FilterPill,
    FilterPills, LaunchedByCell, PartitionCell, Rail, StatTile, SummaryCard, Topbar, UnderlineTabs,
    partition_scheme_for,
};
use rivers_ui::types::LaunchedBy;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn filter_pill_active_signal_drives_active_class() {
    let target = fresh_mount_target();
    let active = RwSignal::new(false);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <FilterPill
                label="Running"
                active=Signal::derive(move || active.get())
                on_click=Callback::new(|_| {})
            />
        }
    });

    let btn = query_one(&target, "button.filter-pill");
    assert!(!btn.class_name().contains("filter-pill--active"));

    active.set(true);
    flush_effects().await;
    let btn = query_one(&target, "button.filter-pill");
    assert!(btn.class_name().contains("filter-pill--active"));
}

#[wasm_bindgen_test]
async fn filter_pill_click_invokes_callback() {
    let target = fresh_mount_target();
    let fired = RwSignal::new(0u32);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <FilterPill
                label="Foo"
                active=Signal::derive(|| false)
                on_click=Callback::new(move |_| { fired.update(|n| *n += 1); })
            />
        }
    });

    let btn = query_one(&target, "button.filter-pill");
    click(&btn, false);
    flush_effects().await;
    click(&btn, false);
    flush_effects().await;

    assert_eq!(fired.get_untracked(), 2);
}

#[wasm_bindgen_test]
fn filter_pill_optional_count_renders_count_span() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <FilterPill
                label="A"
                count="9"
                active=Signal::derive(|| false)
                on_click=Callback::new(|_| {})
            />
        }
    });
    let cnt = query_one(&target, "button.filter-pill .count");
    assert_eq!(cnt.text_content().unwrap(), "9");
}

#[wasm_bindgen_test]
fn filter_pills_wraps_children_in_filter_pills_container() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <FilterPills>
                <FilterPill
                    label="A"
                    active=Signal::derive(|| false)
                    on_click=Callback::new(|_| {})
                />
                <FilterPill
                    label="B"
                    active=Signal::derive(|| true)
                    on_click=Callback::new(|_| {})
                />
            </FilterPills>
        }
    });

    assert_eq!(query_all(&target, ".filter-pills .filter-pill").len(), 2);
}

#[wasm_bindgen_test]
fn stat_tile_no_href_renders_div_with_label_and_value() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <StatTile label="Throughput" value="12.4" suffix="rps" /> }
    });
    let tile = query_one(&target, "div.stat-tile");
    let text = tile.text_content().unwrap();
    assert!(text.contains("Throughput"));
    assert!(text.contains("12.4"));
    assert!(text.contains("rps"));
    // Without href, the wrapper is a div, not an <a>.
    assert!(target.query_selector("a.stat-tile").unwrap().is_none());
}

#[wasm_bindgen_test]
fn stat_tile_rail_variant_emits_modifier_class() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <StatTile label="Errors" value="3" rail=Rail::Error /> }
    });
    let rail_span = query_one(&target, ".stat-tile-rail");
    assert!(
        rail_span.class_name().contains("stat-tile-rail--error"),
        "got: {}",
        rail_span.class_name()
    );
}

#[wasm_bindgen_test]
fn stat_tile_rail_none_omits_modifier_class() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <StatTile label="Idle" value="0" /> }
    });
    // Rail::None passes empty string for the rail span class; the span
    // still renders but carries no class names.
    let spans = query_all(&target, "div.stat-tile > span");
    let first = &spans[0];
    assert!(first.class_name().is_empty(), "got: {}", first.class_name());
}

#[wasm_bindgen_test]
fn donut_stat_card_renders_label_value_and_optional_sub() {
    let target = fresh_mount_target();
    let ring = RwSignal::new(0.5_f64);
    let _handle = mount_to(target.clone(), move || {
        view! {
            <DonutStatCard
                label="HEALTH"
                value="92%"
                sub="last 1h"
                ring_value=Signal::derive(move || ring.get())
            />
        }
    });
    let text = query_one(&target, ".donut-stat").text_content().unwrap();
    assert!(text.contains("HEALTH"));
    assert!(text.contains("92%"));
    assert!(text.contains("last 1h"));
}

#[wasm_bindgen_test]
fn summary_card_no_href_uses_kind_in_rail_class_and_chip() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <SummaryCard
                title="loader.run"
                description="Daily loader pipeline"
                kind="failed"
            />
        }
    });

    // failed → Rail::Error → "summary-card-rail summary-card-rail--error"
    let rail = query_one(&target, ".summary-card-rail");
    assert!(rail.class_name().contains("summary-card-rail--error"));

    // The nested StatusChip carries the same kind in its text + dot class.
    let chip = query_one(&target, ".chip");
    assert!(chip.text_content().unwrap().contains("failed"));
    let dot = query_one(&target, ".chip .dot");
    assert!(dot.class_name().contains("dot-failed"));
}

#[wasm_bindgen_test]
fn alert_card_critical_severity_uses_critical_modifier_classes() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <AlertCard
                sev=AlertSev::Critical
                id="ALRT-1"
                age_ts=0
                title="Database unreachable"
            />
        }
    });
    let sev = query_one(&target, ".alert-card-sev");
    assert!(sev.class_name().contains("alert-card-sev--critical"));
    assert_eq!(sev.text_content().unwrap(), "critical");
    let rail = query_one(&target, ".alert-card-rail");
    assert!(rail.class_name().contains("alert-card-rail--critical"));
}

#[wasm_bindgen_test]
fn alert_card_omits_rule_block_when_rule_and_source_absent() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <AlertCard
                sev=AlertSev::Info
                id="ALRT-2"
                age_ts=0
                title="Heads up"
            />
        }
    });
    assert_eq!(query_all(&target, ".alert-card-rule").len(), 0);
}

#[wasm_bindgen_test]
fn alert_card_renders_rule_block_when_either_field_present() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <AlertCard
                sev=AlertSev::Warning
                id="ALRT-3"
                age_ts=0
                title="Watch out"
                rule="freshness < 24h"
            />
        }
    });
    let rule = query_one(&target, ".alert-card-rule");
    assert!(rule.text_content().unwrap().contains("freshness < 24h"));
}

#[wasm_bindgen_test]
async fn underline_tabs_marks_active_tab_and_fires_on_select() {
    let target = fresh_mount_target();
    let active = RwSignal::new("overview".to_string());
    let captured = RwSignal::new(Vec::<String>::new());

    let tabs = vec![
        ("overview".into(), "Overview".into(), Some(3usize)),
        ("runs".into(), "Runs".into(), None),
        ("assets".into(), "Assets".into(), None),
    ];

    let _handle = mount_to(target.clone(), move || {
        view! {
            <UnderlineTabs
                tabs=tabs.clone()
                active=Signal::derive(move || active.get())
                on_select=Callback::new(move |id: String| {
                    captured.update(|v| v.push(id.clone()));
                    active.set(id);
                })
            />
        }
    });

    let buttons = query_all(&target, ".tabs-underline button.tab");
    assert_eq!(buttons.len(), 3);

    // First tab is active by default.
    assert!(buttons[0].class_name().contains("active"));
    assert!(!buttons[1].class_name().contains("active"));

    // The "Overview" tab should carry its count badge.
    assert_eq!(query_all(&target, ".tab-count").len(), 1);

    // Click the second tab; signal should flip and active class moves.
    click(&buttons[1], false);
    flush_effects().await;
    assert_eq!(captured.get_untracked(), vec!["runs".to_string()]);

    let buttons = query_all(&target, ".tabs-underline button.tab");
    assert!(!buttons[0].class_name().contains("active"));
    assert!(buttons[1].class_name().contains("active"));
}

#[wasm_bindgen_test]
fn topbar_marks_last_crumb_as_current() {
    let target = fresh_mount_target();
    let crumbs = vec![
        Crumb::new("Home"),
        Crumb::new("Assets"),
        Crumb::new("loader.run").mono(),
    ];
    let _handle = mount_to(target.clone(), move || {
        view! { <Topbar crumbs=crumbs.clone() /> }
    });

    let rendered = query_all(&target, ".topbar-crumb");
    assert_eq!(rendered.len(), 3);
    assert!(!rendered[0].class_name().contains("topbar-crumb--current"));
    assert!(rendered[2].class_name().contains("topbar-crumb--current"));
    assert!(rendered[2].class_name().contains("topbar-crumb--mono"));

    // Two separators between three crumbs.
    assert_eq!(query_all(&target, ".topbar-crumb-sep").len(), 2);
}

#[wasm_bindgen_test]
fn topbar_copyable_crumb_carries_data_copy_attr() {
    let target = fresh_mount_target();
    let crumbs = vec![Crumb::new("loader.run").copyable("loader.run::abc123")];
    let _handle = mount_to(target.clone(), move || {
        view! { <Topbar crumbs=crumbs.clone() /> }
    });

    let crumb = query_one(&target, ".topbar-crumb");
    assert!(crumb.class_name().contains("copyable"));
    assert_eq!(
        crumb.get_attribute("data-copy").unwrap(),
        "loader.run::abc123"
    );
}

#[wasm_bindgen_test]
fn eval_timeline_bars_renders_one_bar_per_bucket() {
    let target = fresh_mount_target();
    let buckets = vec![
        (3u32, false),
        (5, true),
        (1, false),
        (0, false),
        (2, true),
        (4, false),
    ];
    let _handle = mount_to(target.clone(), move || {
        view! { <EvalTimelineBars buckets=buckets.clone() /> }
    });

    let bars = query_all(&target, ".eval-timeline-bars > div");
    assert_eq!(bars.len(), 6);
}

#[wasm_bindgen_test]
fn eval_timeline_bars_summary_counts_total_ticks_and_fires() {
    let target = fresh_mount_target();
    let buckets = vec![(2u32, false), (5, true), (0, false), (3, true)];
    let _handle = mount_to(target.clone(), move || {
        view! { <EvalTimelineBars buckets=buckets.clone() /> }
    });
    let summary = query_one(&target, ".eval-timeline-stats")
        .text_content()
        .unwrap();
    assert!(
        summary.contains("10"),
        "total = 2+5+0+3 = 10, got: {summary}"
    );
    assert!(
        summary.contains("2 fires"),
        "exactly 2 fire buckets, got: {summary}"
    );
}

#[wasm_bindgen_test]
fn partition_cell_default_scheme_dot_uses_static_tooltip() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || view! { <PartitionCell /> });
    let badge = query_one(&target, ".partition-cell-badge");
    assert_eq!(badge.text_content().unwrap(), "·");
    assert_eq!(
        badge.get_attribute("data-tip").unwrap(),
        "static / no partitioning"
    );
}

#[wasm_bindgen_test]
fn partition_cell_daily_scheme_uses_daily_tooltip_and_optional_count() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <PartitionCell scheme="D" count_label="12 of 365" /> }
    });
    let badge = query_one(&target, ".partition-cell-badge");
    assert_eq!(badge.get_attribute("data-tip").unwrap(), "daily partition");
    let cnt = query_one(&target, ".partition-cell-count");
    assert_eq!(cnt.text_content().unwrap(), "12 of 365");
}

#[wasm_bindgen_test]
fn partition_scheme_for_classifies_by_format() {
    assert_eq!(partition_scheme_for("2025-04-19"), "D");
    assert_eq!(partition_scheme_for("2025-04-19T14:00:00"), "H");
    assert_eq!(partition_scheme_for("region=us"), "·");
    assert_eq!(partition_scheme_for("custom"), "·");
}

#[wasm_bindgen_test]
fn duration_cell_uses_clock_form_for_tooltip_when_provided() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <DurationCell human="2h 14m" clock="02:14:08" /> }
    });
    let span = query_one(&target, "span.grid-cell-muted");
    assert_eq!(span.text_content().unwrap(), "2h 14m");
    assert_eq!(span.get_attribute("title").unwrap(), "02:14:08");
}

#[wasm_bindgen_test]
fn duration_cell_falls_back_to_human_for_tooltip_when_clock_empty() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <DurationCell human="45s" /> }
    });
    let span = query_one(&target, "span.grid-cell-muted");
    assert_eq!(span.get_attribute("title").unwrap(), "45s");
}

#[wasm_bindgen_test]
fn launched_by_cell_manual_renders_with_label() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <LaunchedByCell launched_by=LaunchedBy::Manual /> }
    });
    let label = query_one(&target, ".grid-cell-mono");
    assert_eq!(label.text_content().unwrap().to_lowercase(), "manual");
}

#[wasm_bindgen_test]
fn launched_by_cell_schedule_carries_schedule_name_in_subline() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! {
            <LaunchedByCell
                launched_by=LaunchedBy::Schedule { name: "hourly_kickoff".into() }
            />
        }
    });
    // The sub-line is a `.grid-cell-muted` carrying the schedule name.
    let text = target.text_content().unwrap();
    assert!(text.contains("hourly_kickoff"));
}
