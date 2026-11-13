//! Browser tests for the Partitions tab of an asset: the tiles, the strip
//! with the numbers of the bar under the pointer, and the outline whose rows
//! open, filter, show more and search.

#![cfg(target_arch = "wasm32")]

mod common;

use std::cell::Cell;
use std::sync::atomic::Ordering;

use common::{
    Reply, click, count_panics, flush_effects, fresh_mount_target, gate,
    install_routing_fetch_mock, query_all, query_one, request_bodies, wait_until,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::ui_kit::{
    OutlineSource, PartitionOutline, PartitionStripView, PartitionTiles,
};
use rivers_ui::types::{
    OutlineDimension, OutlineLevel, OutlinePlace, OutlineRow, OutlineRows, PartitionCounts,
    PartitionOverview, PartitionRow, PartitionRowState, PartitionShape, PartitionStrip, StripBar,
};
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Element, HtmlElement, HtmlInputElement, KeyboardEvent, KeyboardEventInit};

wasm_bindgen_test_configure!(run_in_browser);

fn counts(total: u64, materialized: u64, failed: u64) -> PartitionCounts {
    PartitionCounts {
        total,
        materialized,
        failed,
    }
}

fn text(el: &Element) -> String {
    el.text_content().unwrap_or_default()
}

fn texts(host: &HtmlElement, selector: &str) -> Vec<String> {
    query_all(host, selector).iter().map(text).collect()
}

fn hours() -> OutlinePlace {
    OutlinePlace {
        dimensions: vec![
            OutlineDimension {
                name: "hour".into(),
                keys: 48,
                dynamic_name: String::new(),
            },
            OutlineDimension {
                name: "zone".into(),
                keys: 3,
                dynamic_name: String::new(),
            },
        ],
        ..OutlinePlace::default()
    }
}

/// A day of `hours()` with 72 partitions.
fn day(label: &str, materialized: u64, failed: u64) -> OutlineRow {
    OutlineRow {
        label: label.into(),
        holds: "24 hours".into(),
        counts: counts(72, materialized, failed),
        place: OutlinePlace {
            first_key: format!("{label}T00:00"),
            last_key: format!("{label}T23:00"),
            ..hours()
        },
    }
}

fn partition(key: &str, state: PartitionRowState) -> PartitionRow {
    PartitionRow {
        key: key.into(),
        state,
        materialized_at: (state == PartitionRowState::Materialized)
            .then_some(1_731_460_408_000_000_000),
        run_id: (state == PartitionRowState::Materialized).then(|| "7a4a7592-0000".to_string()),
        failed_at: (state == PartitionRowState::Failed).then_some(1_731_460_500_000_000_000),
    }
}

fn overview(top: OutlineRows) -> PartitionOverview {
    PartitionOverview {
        counts: counts(144, 140, 1),
        shape: PartitionShape::Dimensions(vec![("hour".into(), 48), ("zone".into(), 3)]),
        strip: None,
        top: OutlineLevel {
            heading: "hour".into(),
            rows: top,
            more: 0,
        },
        root: hours(),
        from_definition: true,
    }
}

fn source() -> OutlineSource {
    OutlineSource {
        loc_ns: "default".into(),
        loc_name: "demo".into(),
        asset_key: "trips".into(),
    }
}

fn mount_outline(overview: PartitionOverview) -> HtmlElement {
    let target = fresh_mount_target();
    mount_to(target.clone(), move || {
        view! { <PartitionOutline source=source() overview=overview/> }
    })
    .forget();
    target
}

/// A level of partitions with `more` rows that are not read, as
/// `get_partition_rows` answers it.
fn zones_json(keys: &[(&str, PartitionRowState)], more: u64) -> String {
    let level = OutlineLevel {
        heading: "zone".into(),
        rows: OutlineRows::Partitions(
            keys.iter()
                .map(|(key, state)| partition(key, *state))
                .collect(),
        ),
        more,
    };
    serde_json::to_string(&level).unwrap()
}

fn zones_reply(keys: &[(&str, PartitionRowState)]) -> Reply {
    Reply::Json(zones_json(keys, 0))
}

fn press(el: &Element, key: &str) {
    let init = KeyboardEventInit::new();
    init.set_bubbles(true);
    init.set_cancelable(true);
    init.set_key(key);
    let down = KeyboardEvent::new_with_keyboard_event_init_dict("keydown", &init).unwrap();
    el.dispatch_event(&down).unwrap();
}

/// Type `text` in the find field of the outline.
fn find(target: &HtmlElement, text: &str) {
    let input: HtmlInputElement = query_one(target, ".rv-search input").dyn_into().unwrap();
    input.set_value(text);
    let init = web_sys::EventInit::new();
    init.set_bubbles(true);
    input
        .dispatch_event(&web_sys::Event::new_with_event_init_dict("input", &init).unwrap())
        .unwrap();
}

#[wasm_bindgen_test]
fn tiles_show_the_partitions_by_state() {
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        view! {
            <PartitionTiles
                counts=counts(1_157_520, 1_157_000, 20)
                shape=PartitionShape::Dimensions(vec![("hour".into(), 4368), ("zone".into(), 265)])
                missing_action=None
            />
        }
    })
    .forget();

    let tiles = texts(&target, ".meta-tile");
    assert_eq!(tiles.len(), 4);
    assert_eq!(tiles[0], "Partitions1,157,520hour 4,368 × zone 265");
    assert_eq!(tiles[1], "Materialized1,157,00099.9%");
    assert_eq!(tiles[2], "Failed20<0.1%");
    assert_eq!(tiles[3], "Missing500<0.1%");
}

#[wasm_bindgen_test]
fn the_tile_of_the_missing_partitions_holds_the_action() {
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        let action = ViewFn::from(|| view! { <button class="fill">"Materialize missing"</button> });
        view! {
            <PartitionTiles
                counts=counts(90, 86, 1)
                shape=PartitionShape::Windows {
                    unit: "day".into(),
                    first: "2024-08-01".into(),
                    last: "2024-10-29".into(),
                }
                missing_action=Some(action)
            />
        }
    })
    .forget();

    let tiles = texts(&target, ".meta-tile");
    assert_eq!(tiles[0], "Partitions90day · 2024-08-01 → 2024-10-29");
    assert_eq!(tiles[3], "Missing3Materialize missing");
    assert_eq!(query_all(&target, ".meta-tile .fill").len(), 1);
}

#[wasm_bindgen_test]
async fn the_strip_shows_the_numbers_of_the_bar_under_the_pointer() {
    let target = fresh_mount_target();
    mount_to(target.clone(), || {
        let strip = PartitionStrip {
            heading: "hour".into(),
            note: "one bar per day · 72 partitions each".into(),
            bars: vec![
                StripBar {
                    label: "2024-06-29".into(),
                    counts: counts(72, 72, 0),
                },
                StripBar {
                    label: "2024-06-30".into(),
                    counts: counts(72, 60, 2),
                },
            ],
            marks: vec![(0, "2024-06".into())],
        };
        view! { <PartitionStripView strip=strip/> }
    })
    .forget();

    let note = query_one(&target, ".part-strip-note");
    assert_eq!(text(&note), "one bar per day · 72 partitions each");
    assert_eq!(texts(&target, ".part-axis span"), ["2024-06"]);

    let bars = query_all(&target, ".part-col");
    assert_eq!(bars.len(), 2);
    assert_eq!(query_all(&bars[0], "span").len(), 1);
    assert_eq!(query_all(&bars[1], "span").len(), 3);

    let enter = web_sys::MouseEvent::new("mouseenter").unwrap();
    bars[1].dispatch_event(&enter).unwrap();
    flush_effects().await;
    assert_eq!(
        text(&note),
        "2024-06-30 · 60 materialized · 2 failed · 10 missing"
    );

    let leave = web_sys::MouseEvent::new("mouseleave").unwrap();
    query_one(&target, ".part-bars")
        .dispatch_event(&leave)
        .unwrap();
    flush_effects().await;
    assert_eq!(text(&note), "one bar per day · 72 partitions each");
}

#[wasm_bindgen_test]
async fn a_row_opens_to_the_rows_below_it() {
    let (_mock, requests) = install_routing_fetch_mock(|url| {
        if url.contains("get_partition_rows") {
            zones_reply(&[
                ("001", PartitionRowState::Materialized),
                ("002", PartitionRowState::Failed),
                ("003", PartitionRowState::Missing),
            ])
        } else {
            Reply::Error
        }
    });
    let target = mount_outline(overview(OutlineRows::Groups(vec![
        day("2024-06-30", 70, 1),
        day("2024-06-29", 72, 0),
    ])));

    let rows = query_all(&target, ".part-row");
    assert_eq!(rows.len(), 2);
    assert_eq!(text(&rows[0]), "▸2024-06-3024 hours727097.2%11");
    assert_eq!(text(&rows[1]), "▸2024-06-2924 hours7272100%––");
    assert!(query_all(&target, ".part-leaf").is_empty());

    click(&rows[0], false);
    assert!(wait_until(|| query_all(&target, ".part-leaf").len() == 3).await);

    assert_eq!(
        rows[0].get_attribute("aria-expanded").as_deref(),
        Some("true")
    );
    assert_eq!(
        texts(&target, ".part-leaf .chip"),
        ["materialized", "failed", "missing"]
    );
    let run = query_one(&target, ".part-leaf a");
    assert_eq!(text(&run), "7a4a7592");
    assert_eq!(
        run.get_attribute("href").as_deref(),
        Some("/locations/default/demo/runs/7a4a7592-0000")
    );

    let sent = requests.borrow().clone();
    let bodies = request_bodies(&sent).await;
    assert_eq!(bodies.len(), 1);
    assert!(
        bodies[0].contains(r#""first_key":"2024-06-30T00:00""#),
        "{}",
        bodies[0]
    );
    assert!(
        bodies[0].contains(r#""asset_key":"trips""#),
        "{}",
        bodies[0]
    );

    // Closed and opened again: the rows are not read a second time.
    click(&rows[0], false);
    click(&rows[0], false);
    flush_effects().await;
    assert_eq!(requests.borrow().len(), 1);
    assert_eq!(query_all(&target, ".part-leaf").len(), 3);
}

#[wasm_bindgen_test]
async fn the_filter_shows_the_rows_that_hold_the_state() {
    let target = mount_outline(overview(OutlineRows::Groups(vec![
        day("2024-06-30", 70, 1),
        day("2024-06-29", 72, 0),
        day("2024-06-28", 60, 0),
    ])));
    let pills = query_all(&target, ".filter-pill");
    assert_eq!(
        pills.iter().map(text).collect::<Vec<_>>(),
        ["All144", "Failed1", "Missing3"]
    );

    click(&pills[1], false);
    flush_effects().await;
    assert_eq!(texts(&target, ".part-row .part-key"), ["2024-06-30"]);

    click(&pills[2], false);
    flush_effects().await;
    assert_eq!(
        texts(&target, ".part-row .part-key"),
        ["2024-06-30", "2024-06-28"]
    );

    click(&pills[0], false);
    flush_effects().await;
    assert_eq!(query_all(&target, ".part-row").len(), 3);
}

#[wasm_bindgen_test]
async fn more_rows_are_shown_when_the_reader_asks() {
    let keys: Vec<PartitionRow> = (1..=170)
        .map(|zone| partition(&format!("{zone:03}"), PartitionRowState::Materialized))
        .collect();
    let target = mount_outline(overview(OutlineRows::Partitions(keys)));

    assert_eq!(query_all(&target, ".part-leaf").len(), 60);
    let more = query_one(&target, ".part-more");
    assert_eq!(text(&more), "Show 100 more");

    click(&more, false);
    flush_effects().await;
    assert_eq!(query_all(&target, ".part-leaf").len(), 160);
    assert_eq!(text(&query_one(&target, ".part-more")), "Show 10 more");

    click(&query_one(&target, ".part-more"), false);
    flush_effects().await;
    assert_eq!(query_all(&target, ".part-leaf").len(), 170);
    assert!(query_all(&target, ".part-more").is_empty());
}

#[wasm_bindgen_test]
async fn rows_that_were_not_read_are_read_from_the_end_of_the_level() {
    let (_mock, requests) = install_routing_fetch_mock(|url| {
        if url.contains("get_partition_rows") {
            zones_reply(&[("xl", PartitionRowState::Missing)])
        } else {
            Reply::Error
        }
    });
    let mut overview = overview(OutlineRows::Partitions(vec![
        partition("small", PartitionRowState::Materialized),
        partition("medium", PartitionRowState::Materialized),
    ]));
    overview.top.more = 1;
    let target = mount_outline(overview);

    click(&query_one(&target, ".part-more"), false);
    assert!(wait_until(|| query_all(&target, ".part-leaf").len() == 3).await);

    assert_eq!(
        texts(&target, ".part-leaf .part-key"),
        ["small", "medium", "xl"]
    );
    assert!(query_all(&target, ".part-more").is_empty());
    let sent = requests.borrow().clone();
    let bodies = request_bodies(&sent).await;
    assert!(bodies[0].contains(r#""offset":2"#), "{}", bodies[0]);
}

#[wasm_bindgen_test]
async fn the_find_field_shows_the_keys_that_hold_the_text() {
    let (_mock, requests) = install_routing_fetch_mock(|url| {
        if url.contains("get_partition_rows") {
            zones_reply(&[("2024-06-30T23:00", PartitionRowState::Materialized)])
        } else {
            Reply::Error
        }
    });
    let target = mount_outline(overview(OutlineRows::Groups(vec![day(
        "2024-06-30",
        72,
        0,
    )])));

    find(&target, "30T23");

    assert!(wait_until(|| query_all(&target, ".part-leaf").len() == 1).await);
    assert_eq!(texts(&target, ".part-leaf .part-key"), ["2024-06-30T23:00"]);
    let sent = requests.borrow().clone();
    let bodies = request_bodies(&sent).await;
    assert!(bodies[0].contains(r#""query":"30T23""#), "{}", bodies[0]);

    // The rows of the outline are back when the field is empty.
    find(&target, "");
    flush_effects().await;
    assert!(query_all(&target, ".part-leaf").is_empty());
    let top: HtmlElement = query_one(&target, ".part-outline > div")
        .dyn_into()
        .unwrap();
    assert_eq!(top.style().get_property_value("display").unwrap(), "");
}

#[wasm_bindgen_test]
async fn a_row_opens_from_the_keyboard() {
    let (_mock, requests) = install_routing_fetch_mock(|url| {
        if url.contains("get_partition_rows") {
            zones_reply(&[("001", PartitionRowState::Materialized)])
        } else {
            Reply::Error
        }
    });
    let target = mount_outline(overview(OutlineRows::Groups(vec![day(
        "2024-06-30",
        72,
        0,
    )])));
    let row = query_one(&target, ".part-row");
    let expanded = || row.get_attribute("aria-expanded").unwrap_or_default();
    assert_eq!(row.get_attribute("tabindex").as_deref(), Some("0"));

    press(&row, "Enter");
    assert!(wait_until(|| query_all(&target, ".part-leaf").len() == 1).await);
    assert_eq!(expanded(), "true");

    press(&row, " ");
    flush_effects().await;
    assert_eq!(expanded(), "false");

    press(&row, "a");
    press(&row, "Tab");
    flush_effects().await;
    assert_eq!(expanded(), "false");
    assert_eq!(requests.borrow().len(), 1);
}

#[wasm_bindgen_test]
async fn the_button_names_the_rows_that_the_next_click_shows() {
    let (_mock, requests) = install_routing_fetch_mock(|url| {
        if url.contains("get_partition_rows") {
            let page: Vec<String> = (101..=200).map(|zone| format!("{zone:03}")).collect();
            let page: Vec<(&str, PartitionRowState)> = page
                .iter()
                .map(|key| (key.as_str(), PartitionRowState::Materialized))
                .collect();
            Reply::Json(zones_json(&page, 200))
        } else {
            Reply::Error
        }
    });
    let keys: Vec<PartitionRow> = (1..=100)
        .map(|zone| partition(&format!("{zone:03}"), PartitionRowState::Materialized))
        .collect();
    let mut overview = overview(OutlineRows::Partitions(keys));
    overview.top.more = 300;
    let target = mount_outline(overview);
    let line = || text(&query_one(&target, ".part-more").parent_element().unwrap());

    // 40 rows are read and not shown: the click shows them, and reads nothing.
    assert_eq!(query_all(&target, ".part-leaf").len(), 60);
    assert_eq!(line(), "Show 40 more of 340");

    click(&query_one(&target, ".part-more"), false);
    flush_effects().await;
    assert_eq!(query_all(&target, ".part-leaf").len(), 100);
    assert_eq!(line(), "Show 100 more of 300");
    assert!(requests.borrow().is_empty());

    click(&query_one(&target, ".part-more"), false);
    assert!(wait_until(|| query_all(&target, ".part-leaf").len() == 200).await);
    assert_eq!(line(), "Show 100 more of 200");
    assert_eq!(requests.borrow().len(), 1);
}

#[wasm_bindgen_test]
async fn the_answer_for_an_older_text_is_not_shown() {
    let (held, send) = gate();
    let asked = Cell::new(0);
    let (_mock, requests) = install_routing_fetch_mock(move |url| {
        if !url.contains("get_partition_rows") {
            return Reply::Error;
        }
        asked.set(asked.get() + 1);
        match asked.get() {
            1 => Reply::After(
                held.clone(),
                zones_json(&[("2024-06-30T20:00", PartitionRowState::Materialized)], 0),
            ),
            _ => zones_reply(&[("2024-06-30T23:00", PartitionRowState::Materialized)]),
        }
    });
    let target = mount_outline(overview(OutlineRows::Groups(vec![day(
        "2024-06-30",
        72,
        0,
    )])));
    let found = || texts(&target, ".part-leaf .part-key");

    find(&target, "30T2");
    assert!(wait_until(|| requests.borrow().len() == 1).await);
    find(&target, "30T23");
    assert!(wait_until(|| found() == ["2024-06-30T23:00"]).await);

    send.call0(&wasm_bindgen::JsValue::NULL).unwrap();
    assert!(!wait_until(|| found() != ["2024-06-30T23:00"]).await);
}

/// The panics after this: `start` asks for rows, the tab is closed, and then
/// the answer comes.
async fn panics_of_an_answer_after_the_tab_is_closed(
    overview: PartitionOverview,
    start: impl FnOnce(&HtmlElement),
) -> u32 {
    let (held, send) = gate();
    let (_mock, requests) = install_routing_fetch_mock(move |url| {
        if url.contains("get_partition_rows") {
            Reply::After(
                held.clone(),
                zones_json(&[("2024-06-30T23:00", PartitionRowState::Materialized)], 0),
            )
        } else {
            Reply::Error
        }
    });
    let target = fresh_mount_target();
    let mounted = mount_to(target.clone(), move || {
        view! { <PartitionOutline source=source() overview=overview/> }
    });

    start(&target);
    assert!(wait_until(|| requests.borrow().len() == 1).await);
    drop(mounted);
    let panics = count_panics();
    send.call0(&wasm_bindgen::JsValue::NULL).unwrap();

    wait_until(|| panics.load(Ordering::Relaxed) > 0).await;
    assert!(query_all(&target, ".part-leaf").is_empty());
    panics.load(Ordering::Relaxed)
}

#[wasm_bindgen_test]
async fn the_answer_of_a_search_that_comes_after_the_tab_is_closed_is_dropped() {
    let overview = overview(OutlineRows::Groups(vec![day("2024-06-30", 72, 0)]));

    let panics =
        panics_of_an_answer_after_the_tab_is_closed(overview, |target| find(target, "30T23")).await;

    assert_eq!(panics, 0);
}

#[wasm_bindgen_test]
async fn the_rows_of_a_row_that_come_after_the_tab_is_closed_are_dropped() {
    let overview = overview(OutlineRows::Groups(vec![day("2024-06-30", 72, 0)]));

    let panics = panics_of_an_answer_after_the_tab_is_closed(overview, |target| {
        click(&query_one(target, ".part-row"), false)
    })
    .await;

    assert_eq!(panics, 0);
}

#[wasm_bindgen_test]
async fn more_rows_that_come_after_the_tab_is_closed_are_dropped() {
    let mut overview = overview(OutlineRows::Groups(vec![day("2024-06-30", 72, 0)]));
    overview.top.more = 1;

    let panics = panics_of_an_answer_after_the_tab_is_closed(overview, |target| {
        click(&query_one(target, ".part-more"), false)
    })
    .await;

    assert_eq!(panics, 0);
}

#[wasm_bindgen_test]
async fn a_tab_that_is_closed_while_the_reader_types_asks_nothing() {
    let (_mock, requests) = install_routing_fetch_mock(|_| Reply::Error);
    let target = fresh_mount_target();
    let mounted = mount_to(target.clone(), || {
        let overview = overview(OutlineRows::Groups(vec![day("2024-06-30", 72, 0)]));
        view! { <PartitionOutline source=source() overview=overview/> }
    });

    find(&target, "30T23");
    drop(mounted);

    assert!(!wait_until(|| !requests.borrow().is_empty()).await);
}

#[wasm_bindgen_test]
fn an_asset_with_no_definition_has_no_filter_and_no_find_field() {
    let mut overview = overview(OutlineRows::Partitions(vec![partition(
        "2024-06-30T23:00|001",
        PartitionRowState::Materialized,
    )]));
    overview.from_definition = false;
    let target = mount_outline(overview);

    assert_eq!(
        texts(&target, ".part-notice"),
        ["The code location does not answer: these are the materialized partitions only."]
    );
    assert!(query_all(&target, ".part-toolbar").is_empty());
    assert!(query_all(&target, ".filter-pill").is_empty());
    assert!(query_all(&target, ".rv-search").is_empty());
    assert_eq!(query_all(&target, ".part-leaf").len(), 1);
}
