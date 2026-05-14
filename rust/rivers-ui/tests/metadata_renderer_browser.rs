//! Browser-based component tests for `MetadataValue` and
//! `MetadataTable`. The renderer is a big match over `MetadataDisplay`
//! variants; tests pin the rendered tag + class for a representative
//! subset rather than every variant. Pure-Rust unit tests in the
//! component module already cover `format_bytes` / `format_duration_secs`
//! arithmetic — these tests cover the DOM surface.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{fresh_mount_target, query_all, query_one};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::metadata_renderer::{MetadataTable, MetadataValue};
use rivers_ui::types::MetadataDisplay;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn render(value: MetadataDisplay) -> web_sys::HtmlElement {
    let target = fresh_mount_target();
    // `MountHandle::forget()` keeps the mounted tree alive past the
    // function return (the default `Drop` impl tears it down). Tests are
    // short-lived so leaking the handle is fine.
    mount_to(target.clone(), move || {
        view! { <MetadataValue value=value.clone() /> }
    })
    .forget();
    target
}

#[wasm_bindgen_test]
fn text_renders_as_plain_span() {
    let host = render(MetadataDisplay::Text("hello world".into()));
    assert_eq!(host.text_content().unwrap(), "hello world");
    assert_eq!(query_all(&host, "span").len(), 1);
}

#[wasm_bindgen_test]
fn int_uses_metadata_number_class() {
    let host = render(MetadataDisplay::Int(42));
    let span = query_one(&host, "span.metadata-number");
    assert_eq!(span.text_content().unwrap(), "42");
}

#[wasm_bindgen_test]
fn float_uses_metadata_number_class_and_4_decimal_format() {
    let host = render(MetadataDisplay::Float(3.14));
    let span = query_one(&host, "span.metadata-number");
    assert_eq!(span.text_content().unwrap(), "3.1400");
}

#[wasm_bindgen_test]
fn bool_true_uses_success_badge_with_check_text() {
    let host = render(MetadataDisplay::Bool(true));
    let badge = query_one(&host, "span.badge");
    assert!(badge.class_name().contains("badge-success"));
    assert_eq!(badge.text_content().unwrap(), "check");
}

#[wasm_bindgen_test]
fn bool_false_uses_muted_badge_with_x_text() {
    let host = render(MetadataDisplay::Bool(false));
    let badge = query_one(&host, "span.badge");
    assert!(badge.class_name().contains("badge-muted"));
    assert_eq!(badge.text_content().unwrap(), "x");
}

#[wasm_bindgen_test]
fn url_renders_anchor_with_target_and_rel() {
    let host = render(MetadataDisplay::Url {
        text: "Docs".into(),
        url: "https://example.com/docs".into(),
    });
    let a = query_one(&host, "a.metadata-url");
    assert_eq!(a.text_content().unwrap(), "Docs");
    assert_eq!(a.get_attribute("href").unwrap(), "https://example.com/docs");
    assert_eq!(a.get_attribute("target").unwrap(), "_blank");
    assert_eq!(a.get_attribute("rel").unwrap(), "noopener");
}

#[wasm_bindgen_test]
fn path_renders_inside_code_with_metadata_path_class() {
    let host = render(MetadataDisplay::Path("/var/log/app.log".into()));
    let code = query_one(&host, "code.metadata-path");
    assert_eq!(code.text_content().unwrap(), "/var/log/app.log");
}

#[wasm_bindgen_test]
fn json_renders_inside_pre_code() {
    let host = render(MetadataDisplay::Json(r#"{"k":1}"#.into()));
    let pre = query_one(&host, "pre.metadata-json");
    assert!(pre.text_content().unwrap().contains(r#"{"k":1}"#));
}

#[wasm_bindgen_test]
fn sql_uses_codeblock_with_sql_lang_label() {
    let host = render(MetadataDisplay::Sql("select 1".into()));
    let block = query_one(&host, ".metadata-codeblock");
    let lang = query_one(&block, ".codeblock-lang");
    assert_eq!(lang.text_content().unwrap(), "SQL");
    assert!(block.text_content().unwrap().contains("select 1"));
}

#[wasm_bindgen_test]
fn code_block_with_language_renders_lang_label() {
    let host = render(MetadataDisplay::CodeBlock {
        code: "fn main() {}".into(),
        language: Some("rust".into()),
    });
    let lang = query_one(&host, ".codeblock-lang");
    assert_eq!(lang.text_content().unwrap(), "rust");
}

#[wasm_bindgen_test]
fn code_block_without_language_omits_lang_label() {
    let host = render(MetadataDisplay::CodeBlock {
        code: "x = 1".into(),
        language: None,
    });
    assert_eq!(query_all(&host, ".codeblock-lang").len(), 0);
}

#[wasm_bindgen_test]
fn image_renders_img_with_src() {
    let host = render(MetadataDisplay::Image("https://e.com/i.png".into()));
    let img = query_one(&host, "img.metadata-image");
    assert_eq!(img.get_attribute("src").unwrap(), "https://e.com/i.png");
}

#[wasm_bindgen_test]
fn timestamp_renders_with_raw_value_in_title_attr() {
    let host = render(MetadataDisplay::Timestamp(1_700_000_000_000_000_000));
    let span = query_one(&host, "span.metadata-timestamp");
    // Title carries the raw nanos so a hover surfaces the precise value.
    assert_eq!(span.get_attribute("title").unwrap(), "1700000000000000000");
}

#[wasm_bindgen_test]
fn duration_under_1s_renders_in_milliseconds() {
    let host = render(MetadataDisplay::Duration(0.25));
    let span = query_one(&host, "span.metadata-duration");
    assert_eq!(span.text_content().unwrap(), "250ms");
}

#[wasm_bindgen_test]
fn bytes_above_1mb_uses_mb_unit() {
    let host = render(MetadataDisplay::Bytes(2 * 1024 * 1024));
    let span = query_one(&host, "span.metadata-bytes");
    assert_eq!(span.text_content().unwrap(), "2.0 MB");
}

#[wasm_bindgen_test]
fn percentage_clamps_negative_to_0() {
    let host = render(MetadataDisplay::Percentage(-0.5));
    let fill = query_one(&host, ".progress-fill");
    let style = fill.get_attribute("style").unwrap_or_default();
    assert!(
        style.contains("width: 0%"),
        "expected clamped style, got: {style}"
    );
}

#[wasm_bindgen_test]
fn percentage_clamps_above_one_to_100() {
    let host = render(MetadataDisplay::Percentage(2.5));
    let fill = query_one(&host, ".progress-fill");
    let style = fill.get_attribute("style").unwrap_or_default();
    assert!(style.contains("width: 100%"));
}

#[wasm_bindgen_test]
fn schema_renders_table_with_one_row_per_column() {
    let host = render(MetadataDisplay::Schema(vec![
        ("id".into(), "int8".into()),
        ("name".into(), "text".into()),
        ("created_at".into(), "timestamp".into()),
    ]));
    let rows = query_all(&host, "table.metadata-schema tbody tr");
    assert_eq!(rows.len(), 3);
    assert!(rows[0].text_content().unwrap().contains("id"));
    assert!(rows[0].text_content().unwrap().contains("int8"));
    assert!(rows[2].text_content().unwrap().contains("created_at"));
}

#[wasm_bindgen_test]
fn data_version_renders_inside_code_with_dedicated_class() {
    let host = render(MetadataDisplay::DataVersion("a1b2c3".into()));
    let code = query_one(&host, "code.metadata-data-version");
    assert_eq!(code.text_content().unwrap(), "a1b2c3");
}

#[wasm_bindgen_test]
fn null_renders_metadata_null_span_with_text() {
    let host = render(MetadataDisplay::Null);
    let span = query_one(&host, "span.metadata-null");
    assert_eq!(span.text_content().unwrap(), "null");
}

#[wasm_bindgen_test]
fn date_range_renders_dash_separated_pair() {
    let host = render(MetadataDisplay::DateRange {
        start: 1_700_000_000_000_000_000,
        end: 1_700_086_400_000_000_000,
    });
    let span = query_one(&host, "span.metadata-daterange");
    let text = span.text_content().unwrap();
    assert!(text.contains(" - "), "expected `start - end`, got: {text}");
}

#[wasm_bindgen_test]
fn empty_table_renders_no_metadata_placeholder() {
    let target = fresh_mount_target();
    let _handle = mount_to(target.clone(), || {
        view! { <MetadataTable entries=Vec::new() /> }
    });
    let span = query_one(&target, "span.metadata-null");
    assert_eq!(span.text_content().unwrap(), "No metadata");
}

#[wasm_bindgen_test]
fn populated_table_renders_one_row_per_entry() {
    let target = fresh_mount_target();
    let entries = vec![
        ("rows".to_string(), MetadataDisplay::Int(100)),
        ("status".to_string(), MetadataDisplay::Text("ok".into())),
        ("hot".to_string(), MetadataDisplay::Bool(true)),
    ];
    let _handle = mount_to(target.clone(), move || {
        view! { <MetadataTable entries=entries.clone() /> }
    });

    let rows = query_all(&target, ".metadata-table .metadata-row");
    assert_eq!(rows.len(), 3);

    // Spot-check that each key is rendered alongside its value.
    let keys: Vec<String> = query_all(&target, ".metadata-key")
        .iter()
        .map(|el| el.text_content().unwrap_or_default())
        .collect();
    assert_eq!(keys, vec!["rows", "status", "hot"]);
}
