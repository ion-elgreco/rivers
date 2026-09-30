//! Browser tests for the config editor: highlighting, schema issues, the
//! keys a code editor has, and the required-fields hint. `ConfigEditor` is
//! mounted on its own with the `check_config` memo a dialog would own.

#![cfg(target_arch = "wasm32")]

mod common;

use std::collections::HashMap;

use common::{
    click, flush_effects, fresh_mount_target, install_recording_fetch_mock, query_all, query_one,
    request_bodies, wait_until,
};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::config_editor::{Check, ConfigEditor, check_config, use_config_check};
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{
    Event, EventInit, HtmlElement, HtmlTextAreaElement, KeyboardEvent, KeyboardEventInit,
    MouseEvent, MouseEventInit,
};

wasm_bindgen_test_configure!(run_in_browser);

const PIPELINE: &str = r#"{"properties":{"api_key":{"title":"Api Key","type":"string"},"batch_size":{"default":100,"title":"Batch Size","type":"integer"},"mode":{"default":"fast","enum":["fast","slow"],"title":"Mode","type":"string"}},"required":["api_key"],"title":"PipelineConfig","type":"object"}"#;

struct Editor {
    host: HtmlElement,
    selected: RwSignal<Vec<String>>,
    text: RwSignal<String>,
    check: Memo<Check>,
}

fn mount(schemas: &[(&str, &str)]) -> Editor {
    let target = fresh_mount_target();
    let keys: Vec<String> = schemas.iter().map(|(k, _)| k.to_string()).collect();
    let by_key: HashMap<String, String> = schemas
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let selected = RwSignal::new(keys);
    let schemas = RwSignal::new(by_key);
    let text = RwSignal::new(String::new());
    let check = Memo::new(move |_| check_config(&text.get(), &selected.get(), &schemas.get()));
    mount_to(target.clone(), move || {
        view! {
            <ConfigEditor
                selected=selected
                schemas=schemas
                text=text
                reset=Signal::derive(|| true)
                check=check
            />
        }
    })
    .forget();
    Editor {
        host: target,
        selected,
        text,
        check,
    }
}

fn dispatch(target: &HtmlTextAreaElement, name: &str) {
    let init = EventInit::new();
    init.set_bubbles(true);
    target
        .dispatch_event(&Event::new_with_event_init_dict(name, &init).unwrap())
        .unwrap();
}

impl Editor {
    fn textarea(&self) -> HtmlTextAreaElement {
        query_one(&self.host, ".code-editor-text")
            .dyn_into()
            .unwrap()
    }

    fn value(&self) -> String {
        self.textarea().value()
    }

    fn caret(&self) -> u32 {
        self.textarea().selection_start().unwrap().unwrap()
    }

    /// Set the value and fire `input`, as typing does.
    fn type_text(&self, value: &str) {
        let ta = self.textarea();
        ta.set_value(value);
        dispatch(&ta, "input");
    }

    fn set_caret(&self, at: u32) {
        let ta = self.textarea();
        ta.set_selection_range(at, at).unwrap();
        click(&ta, false);
    }

    /// Press `key` on the textarea; `true` when the editor consumed it.
    fn press(&self, key: &str, ctrl: bool) -> bool {
        let ta = self.textarea();
        let init = KeyboardEventInit::new();
        init.set_bubbles(true);
        init.set_cancelable(true);
        init.set_key(key);
        init.set_ctrl_key(ctrl);
        let down = KeyboardEvent::new_with_keyboard_event_init_dict("keydown", &init).unwrap();
        let not_prevented = ta.dispatch_event(&down).unwrap();
        let up = KeyboardEvent::new_with_keyboard_event_init_dict("keyup", &init).unwrap();
        ta.dispatch_event(&up).unwrap();
        !not_prevented
    }

    fn texts(&self, selector: &str) -> Vec<String> {
        query_all(&self.host, selector)
            .iter()
            .map(|el| el.text_content().unwrap_or_default())
            .collect()
    }

    fn issues(&self) -> Vec<String> {
        self.texts(".code-editor-issue")
    }

    fn marks(&self) -> Vec<String> {
        self.texts(".code-editor-mark")
    }

    fn options(&self) -> Vec<String> {
        self.texts(".code-editor-option-label")
    }

    fn active_option(&self) -> Option<String> {
        query_all(&self.host, ".code-editor-option--active")
            .first()
            .and_then(|el| {
                el.query_selector(".code-editor-option-label")
                    .ok()
                    .flatten()
            })
            .map(|el| el.text_content().unwrap_or_default())
    }

    fn hint(&self) -> Option<String> {
        query_all(&self.host, ".config-editor-hint-fields")
            .first()
            .map(|el| el.text_content().unwrap_or_default())
    }
}

#[wasm_bindgen_test]
async fn highlights_tokens_by_class() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    assert_eq!(
        e.value(),
        "{\n  \"api_data\": {\n    \"batch_size\": 100,\n    \"mode\": \"fast\"\n  }\n}"
    );
    assert_eq!(
        e.texts(".tok-key"),
        vec!["\"api_data\"", "\"batch_size\"", "\"mode\""]
    );
    assert_eq!(e.texts(".tok-num"), vec!["100"]);
    assert_eq!(e.texts(".tok-str"), vec!["\"fast\""]);
    assert!(e.issues().is_empty());
    assert!(e.marks().is_empty());
    assert!(query_all(&e.host, ".code-editor--invalid").is_empty());
}

#[wasm_bindgen_test]
async fn unknown_key_is_underlined_and_listed() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{\"api_data\": {\"batch_sizes\": 1}}");
    flush_effects().await;
    assert_eq!(e.marks(), vec!["\"batch_sizes\""]);
    assert_eq!(
        e.issues(),
        vec![
            "1:15 api_data: unknown field 'batch_sizes'; expected one of api_key, batch_size, mode"
        ]
    );
    assert_eq!(query_all(&e.host, ".code-editor--invalid").len(), 1);
    assert_eq!(e.check.get_untracked().payload, None);

    // Clicking the issue puts the caret on it.
    click(&query_one(&e.host, ".code-editor-issue .link-btn"), false);
    assert_eq!(e.caret(), 14);
}

#[wasm_bindgen_test]
async fn wrong_types_and_syntax_errors_are_issues() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{\"api_data\": {\"batch_size\": \"5\"}}");
    flush_effects().await;
    assert_eq!(
        e.issues(),
        vec!["1:29 api_data.batch_size: expected integer, got string"]
    );
    assert_eq!(e.marks(), vec!["\"5\""]);

    e.type_text("{\"api_data\": ");
    flush_effects().await;
    assert_eq!(e.issues(), vec!["1:14 Expected a value"]);
    assert_eq!(e.marks(), vec![" "]);

    e.type_text("{\"other\": {}}");
    flush_effects().await;
    assert_eq!(e.issues(), vec!["1:2 'other' is not in the selection."]);

    e.type_text("{\"api_data\": {\"batch_size\": 5}}");
    flush_effects().await;
    assert!(e.issues().is_empty());
    assert_eq!(
        e.check.get_untracked().payload.as_deref(),
        Some("{\"api_data\":{\"batch_size\":5}}")
    );
}

#[wasm_bindgen_test]
async fn tab_enter_and_pairs_edit_like_a_code_editor() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{}");
    e.set_caret(1);
    assert!(e.press("Tab", false));
    flush_effects().await;
    assert_eq!(e.value(), "{  }");
    assert_eq!(e.caret(), 3);

    e.type_text("{\n  \"a\": {}\n}");
    e.set_caret(10);
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(e.value(), "{\n  \"a\": {\n    \n  }\n}");
    assert_eq!(e.caret(), 15);

    e.type_text("  x");
    e.set_caret(3);
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(e.value(), "  x\n  ");
    assert_eq!(e.caret(), 6);

    e.type_text("");
    e.set_caret(0);
    assert!(e.press("{", false));
    flush_effects().await;
    assert_eq!((e.value(), e.caret()), ("{}".to_string(), 1));
    assert!(e.press("\"", false));
    flush_effects().await;
    assert_eq!((e.value(), e.caret()), ("{\"\"}".to_string(), 2));
    // Typing the closers steps over them.
    assert!(e.press("\"", false));
    assert_eq!((e.value(), e.caret()), ("{\"\"}".to_string(), 3));
    assert!(e.press("}", false));
    assert_eq!((e.value(), e.caret()), ("{\"\"}".to_string(), 4));
    // Backspace inside an empty pair removes both.
    e.set_caret(2);
    assert!(e.press("Backspace", false));
    flush_effects().await;
    assert_eq!((e.value(), e.caret()), ("{}".to_string(), 1));
    assert!(e.press("Backspace", false));
    flush_effects().await;
    assert_eq!((e.value(), e.caret()), (String::new(), 0));

    // Inside a string a quote closes it, so nothing pairs.
    e.type_text("\"ab");
    e.set_caret(3);
    assert!(!e.press("\"", false));
    // Escape then Tab leaves the editor; a modifier passes through.
    assert!(!e.press("Escape", false));
    assert!(!e.press("Tab", false));
    assert!(!e.press("Enter", true));
    assert!(e.press("Tab", false));
}

#[wasm_bindgen_test]
async fn insert_missing_fields_adds_required_keys() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    assert_eq!(e.hint().as_deref(), Some("api_data.api_key"));
    click(&query_one(&e.host, ".config-editor-hint .link-btn"), false);
    flush_effects().await;
    let value: serde_json::Value = serde_json::from_str(&e.value()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"api_data": {"api_key": "", "batch_size": 100, "mode": "fast"}})
    );
    assert_eq!(e.hint(), None);

    // Hinted too when the asset's entry is gone; the payload holds what is set.
    e.type_text("{}");
    flush_effects().await;
    assert_eq!(e.hint().as_deref(), Some("api_data.api_key"));
    e.type_text("{\"api_data\": {\"api_key\": \"k\"}}");
    flush_effects().await;
    assert_eq!(e.hint(), None);
    assert_eq!(
        e.check.get_untracked().payload.as_deref(),
        Some("{\"api_data\":{\"api_key\":\"k\"}}")
    );
}

/// Deselecting the only configured asset removes the editor; selecting it
/// again brings the template back, in the signal and in the textarea.
#[wasm_bindgen_test]
async fn template_returns_after_the_asset_is_selected_again() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;
    let template = e.value();
    assert!(template.starts_with("{\n  \"api_data\""), "{template}");

    e.selected.set(Vec::new());
    flush_effects().await;
    assert!(query_all(&e.host, ".config-editor").is_empty());
    assert_eq!(e.text.get_untracked(), "");

    e.selected.set(vec!["api_data".to_string()]);
    // The template effect runs after the Show has mounted the textarea, so
    // the value binding follows one microtask later.
    flush_effects().await;
    flush_effects().await;
    assert_eq!(e.text.get_untracked(), template);
    assert_eq!(e.value(), template);
}

fn mousedown(target: &web_sys::Element) {
    let init = MouseEventInit::new();
    init.set_bubbles(true);
    init.set_cancelable(true);
    let ev = MouseEvent::new_with_mouse_event_init_dict("mousedown", &init).unwrap();
    target.dispatch_event(&ev).unwrap();
}

#[wasm_bindgen_test]
async fn typing_a_key_prefix_opens_completion_and_enter_inserts() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{\"api_data\": {\"ap");
    flush_effects().await;
    assert_eq!(e.options(), vec!["api_key"]);
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(e.value(), "{\"api_data\": {\"api_key\": \"\"");
    assert_eq!(e.caret(), 26);
    assert!(query_all(&e.host, ".code-editor-popup").is_empty());
}

#[wasm_bindgen_test]
async fn values_complete_after_the_colon_and_on_ctrl_space() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{\"api_data\": {\"mode\": ");
    flush_effects().await;
    assert_eq!(e.options(), vec!["\"fast\"", "\"slow\""]);
    assert_eq!(e.texts(".code-editor-option-detail"), vec!["default", ""]);
    // Escape closes it; Ctrl+Space brings it back; arrows move; Enter takes.
    assert!(e.press("Escape", false));
    flush_effects().await;
    assert!(query_all(&e.host, ".code-editor-popup").is_empty());
    assert!(e.press(" ", true));
    flush_effects().await;
    assert_eq!(e.options(), vec!["\"fast\"", "\"slow\""]);
    assert!(e.press("ArrowDown", false));
    flush_effects().await;
    assert_eq!(e.active_option().as_deref(), Some("\"slow\""));
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(e.value(), "{\"api_data\": {\"mode\": \"slow\"");
    assert_eq!(e.caret(), 28);
}

#[wasm_bindgen_test]
async fn completion_skips_present_keys_and_adds_commas() {
    let e = mount(&[("api_data", PIPELINE)]);
    flush_effects().await;

    e.type_text("{\"api_data\": {\"batch_size\": 1}}");
    e.set_caret(14);
    assert!(e.press(" ", true));
    flush_effects().await;
    assert_eq!(e.options(), vec!["api_key", "mode"]);
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(
        e.value(),
        "{\"api_data\": {\"api_key\": \"\",\"batch_size\": 1}}"
    );

    e.type_text("{\"api_data\": {\"batch_size\": 1 ");
    assert!(e.press(" ", true));
    flush_effects().await;
    assert_eq!(e.options(), vec!["api_key", "mode"]);
    assert!(e.press("ArrowDown", false));
    assert!(e.press("Enter", false));
    flush_effects().await;
    assert_eq!(
        e.value(),
        "{\"api_data\": {\"batch_size\": 1 , \"mode\": \"fast\""
    );
}

#[wasm_bindgen_test]
async fn top_level_completion_offers_assets_and_a_click_inserts() {
    let e = mount(&[("api_data", PIPELINE), ("other", PIPELINE)]);
    flush_effects().await;

    e.type_text("{");
    flush_effects().await;
    assert!(query_all(&e.host, ".code-editor-popup").is_empty());
    assert!(e.press(" ", true));
    flush_effects().await;
    assert_eq!(e.options(), vec!["api_data", "other"]);
    mousedown(&query_all(&e.host, ".code-editor-option")[1]);
    flush_effects().await;
    assert_eq!(e.value(), "{\"other\": {}");
    assert_eq!(e.caret(), 11);
    assert!(query_all(&e.host, ".code-editor-popup").is_empty());
}

/// Once the schema is satisfied, the config classes are asked; their
/// errors land on the value they name, and a schema issue typed later
/// takes over at once.
#[wasm_bindgen_test]
async fn the_config_classes_errors_show_after_the_schema_passes() {
    let (_mock, requests) = install_recording_fetch_mock(
        r#"[{"asset":"api_data","loc":[{"Key":"batch_size"}],"message":"Input should be greater than 0","kind":"greater_than"},{"asset":"api_data","loc":[{"Key":"api_key"}],"message":"Field required","kind":"missing"}]"#,
    );
    let target = fresh_mount_target();
    let selected = RwSignal::new(vec!["api_data".to_string()]);
    let schemas = RwSignal::new(HashMap::from([(
        "api_data".to_string(),
        PIPELINE.to_string(),
    )]));
    let text = RwSignal::new(String::new());
    let check = StoredValue::new(None::<Signal<Check>>);
    mount_to(target.clone(), move || {
        let checked = use_config_check(
            text,
            selected.into(),
            schemas.into(),
            Signal::derive(|| ("dev".to_string(), "demo".to_string())),
            Signal::derive(|| None),
        );
        check.set_value(Some(checked));
        view! {
            <ConfigEditor
                selected=selected
                schemas=schemas
                text=text
                reset=Signal::derive(|| true)
                check=checked
            />
        }
    })
    .forget();
    flush_effects().await;

    let issues = || {
        query_all(&target, ".code-editor-issue")
            .iter()
            .map(|el| el.text_content().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    assert!(
        wait_until(|| !issues().is_empty()).await,
        "no answer arrived"
    );
    assert_eq!(
        issues(),
        vec!["3:19 api_data.batch_size: Input should be greater than 0"]
    );
    assert_eq!(
        query_all(&target, ".code-editor-mark")
            .iter()
            .map(|el| el.text_content().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["100"]
    );
    let checked = check.get_value().unwrap().get_untracked();
    assert_eq!(checked.payload, None);
    assert_eq!(checked.missing, vec!["api_data.api_key"]);
    let bodies = request_bodies(&requests.borrow().clone()).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    assert!(bodies[0].contains("api_data"), "{}", bodies[0]);

    // A schema issue takes over; the classes' answer was for other text.
    let ta: HtmlTextAreaElement = query_one(&target, ".code-editor-text").dyn_into().unwrap();
    ta.set_value("{\"api_data\": {\"batch_sizes\": 1}}");
    dispatch(&ta, "input");
    flush_effects().await;
    assert_eq!(issues().len(), 1);
    assert!(issues()[0].contains("unknown field 'batch_sizes'"));
}
