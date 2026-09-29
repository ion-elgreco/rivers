//! Per-asset config editor shared by the launch dialogs.
//!
//! The text is the JSON the backend stores on the run —
//! `{"asset": {"field": value}}`. It opens pre-filled with each selected
//! asset's schema defaults; a field without a default (required, or a
//! `BaseSettings` field the environment resolves) is hinted, never sent.
//! [`check_config`] gives the dialogs the issues that block a submit and
//! the payload to send.

use std::collections::HashMap;

use leptos::prelude::*;
use serde_json::{Map, Value};

use crate::components::code_editor::CodeEditor;
use crate::config_schema::{self, Schema};
use crate::json_text::{self, Issue, Node, Span};
use crate::types::AssetDefinitionInfo;

fn properties(schema_json: &str) -> Option<Map<String, Value>> {
    let schema: Value = serde_json::from_str(schema_json).ok()?;
    schema.get("properties")?.as_object().cloned()
}

/// The config schema per key for a launch: each asset's own for materialize,
/// or the verb's declaration on each asset for an action run. Keys without
/// config are absent.
pub fn launch_config_schemas(
    keys: &[String],
    definitions: &HashMap<String, AssetDefinitionInfo>,
    verb: Option<&str>,
) -> HashMap<String, String> {
    keys.iter()
        .filter_map(|key| {
            let def = definitions.get(key)?;
            let schema = match verb {
                Some(verb) => def
                    .actions
                    .iter()
                    .find(|a| a.name == verb)?
                    .config_schema
                    .clone()?,
                None => def.config_schema.clone()?,
            };
            Some((key.clone(), schema))
        })
        .collect()
}

/// Whether a launch of `keys` has anything for the editor — a one-click
/// launch must open the dialog instead when it does.
pub fn launch_takes_config(
    keys: &[String],
    definitions: &HashMap<String, AssetDefinitionInfo>,
    verb: Option<&str>,
) -> bool {
    !launch_config_schemas(keys, definitions, verb).is_empty()
}

/// The editor's opening text: every selected key with a schema, holding its
/// fields' defaults. `None` when no selected key takes config.
pub fn config_template(selected: &[String], schemas: &HashMap<String, String>) -> Option<String> {
    let mut root = Map::new();
    for key in selected {
        let Some(schema) = schemas.get(key) else {
            continue;
        };
        let defaults: Map<String, Value> = properties(schema)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, prop)| Some((name, prop.get("default")?.clone())))
            .collect();
        root.insert(key.clone(), Value::Object(defaults));
    }
    if root.is_empty() {
        return None;
    }
    serde_json::to_string_pretty(&Value::Object(root)).ok()
}

/// What a submit sends: `None` for blank text or no overrides, else the JSON
/// compacted, after the checks the backend repeats — an object of objects
/// whose keys are in the selection. A key with an empty object (how the
/// template lists an asset without defaults) is dropped.
pub fn validate_config_text(text: &str, selected: &[String]) -> Result<Option<String>, String> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let value: Value =
        serde_json::from_str(text).map_err(|e| format!("Config is not valid JSON: {e}"))?;
    let Value::Object(map) = value else {
        return Err("Config must be a JSON object keyed by asset name.".to_string());
    };
    let mut out = Map::new();
    for (key, overrides) in map {
        if !selected.contains(&key) {
            return Err(format!("'{key}' is not in the selection."));
        }
        let Value::Object(fields) = overrides else {
            return Err(format!(
                "Config for '{key}' must be a JSON object of field values."
            ));
        };
        if fields.is_empty() {
            continue;
        }
        out.insert(key, Value::Object(fields));
    }
    if out.is_empty() {
        return Ok(None);
    }
    Ok(Some(Value::Object(out).to_string()))
}

/// What the dialogs learn from the text: the issues that block a submit,
/// the required fields not set (a hint), and the payload to send.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Check {
    pub issues: Vec<Issue>,
    pub missing: Vec<String>,
    pub payload: Option<String>,
}

/// Syntax first, then the shape the backend checks (an object of objects
/// keyed by selected assets), then each asset's schema. A syntax error is
/// the only issue reported, as the tree past it is a guess.
pub fn check_config(text: &str, selected: &[String], schemas: &HashMap<String, String>) -> Check {
    if text.trim().is_empty() {
        return Check::default();
    }
    let parsed = json_text::parse(text);
    if let Some(error) = parsed.error {
        return Check {
            issues: vec![error],
            ..Check::default()
        };
    }
    let Some(root) = parsed.root else {
        return Check::default();
    };
    let Node::Object { entries, .. } = &root else {
        return Check {
            issues: vec![Issue {
                span: root.span(),
                message: "Config must be a JSON object keyed by asset name.".to_string(),
            }],
            ..Check::default()
        };
    };
    let mut issues = Vec::new();
    let mut missing = Vec::new();
    for entry in entries {
        if !selected.contains(&entry.key) {
            issues.push(Issue {
                span: entry.key_span,
                message: format!("'{}' is not in the selection.", entry.key),
            });
            continue;
        }
        if !matches!(entry.value, Node::Object { .. }) {
            issues.push(Issue {
                span: entry.value.span(),
                message: format!(
                    "Config for '{}' must be a JSON object of field values.",
                    entry.key
                ),
            });
            continue;
        }
        if let Some(schema) = schemas.get(&entry.key).and_then(|s| Schema::parse(s)) {
            issues.extend(config_schema::validate(&schema, &entry.value, &entry.key));
            missing.extend(config_schema::missing_required(
                &schema,
                &entry.value,
                &entry.key,
            ));
        }
    }
    // A selected asset without an entry has set none of its fields.
    let none = Node::Object {
        span: Span::at(0),
        entries: Vec::new(),
        closed: true,
    };
    for key in selected {
        if entries.iter().any(|e| &e.key == key) {
            continue;
        }
        if let Some(schema) = schemas.get(key).and_then(|s| Schema::parse(s)) {
            missing.extend(config_schema::missing_required(&schema, &none, key));
        }
    }
    let payload = if issues.is_empty() {
        match validate_config_text(text, selected) {
            Ok(payload) => payload,
            Err(message) => {
                issues.push(Issue {
                    span: root.span(),
                    message,
                });
                None
            }
        }
    } else {
        None
    };
    Check {
        issues,
        missing,
        payload,
    }
}

/// The dialog section: the code editor over `text`, the required-fields
/// hint and a reset. Renders nothing when no selected key has a schema.
/// `reset` turning true (the dialog opening) refills the template; a
/// selection change does too until the user has typed.
#[component]
pub fn ConfigEditor(
    #[prop(into)] selected: Signal<Vec<String>>,
    #[prop(into)] schemas: Signal<HashMap<String, String>>,
    text: RwSignal<String>,
    #[prop(into)] reset: Signal<bool>,
    #[prop(into)] check: Signal<Check>,
) -> impl IntoView {
    let dirty = RwSignal::new(false);
    let template = Memo::new(move |_| config_template(&selected.get(), &schemas.get()));
    let refill = move || {
        dirty.set(false);
        text.set(template.get_untracked().unwrap_or_default());
    };
    Effect::new(move || {
        if reset.get() {
            refill();
        }
    });
    Effect::new(move || {
        let template = template.get();
        if !dirty.get_untracked() {
            text.set(template.unwrap_or_default());
        }
    });
    let issues = Signal::derive(move || check.get().issues);
    let missing = Signal::derive(move || check.get().missing);
    let scaffold = move || {
        let filled = config_schema::scaffold_missing(
            &text.get_untracked(),
            &selected.get_untracked(),
            &schemas.get_untracked(),
        );
        if let Some(filled) = filled {
            dirty.set(true);
            text.set(filled);
        }
    };

    view! {
        <Show when=move || template.get().is_some()>
            <div class="form-group config-editor">
                <div class="mat-dialog-col-head">
                    <label>"Config"</label>
                    <span class="mat-dialog-col-actions">
                        <button class="link-btn" on:click=move |_| refill()>"Reset to defaults"</button>
                    </span>
                </div>
                <CodeEditor
                    text=text
                    issues=issues
                    label="Config"
                    on_edit=Callback::new(move |()| dirty.set(true))
                />
                <Show when=move || !missing.get().is_empty()>
                    <div class="config-editor-hint">
                        "Required, not set: "
                        <span class="config-editor-hint-fields">{move || missing.get().join(", ")}</span>
                        <button class="link-btn" on:click=move |_| scaffold()>"Insert missing fields"</button>
                    </div>
                </Show>
            </div>
        </Show>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THRESHOLD: &str = r#"{"properties":{"threshold":{"default":0.5,"title":"Threshold","type":"number"},"max_retries":{"default":3,"title":"Max Retries","type":"integer"}},"title":"ThresholdConfig","type":"object"}"#;
    const PIPELINE: &str = r#"{"properties":{"api_key":{"title":"Api Key","type":"string"},"batch_size":{"default":100,"title":"Batch Size","type":"integer"},"region":{"anyOf":[{"type":"string"},{"type":"null"}],"default":null,"title":"Region"}},"required":["api_key"],"title":"PipelineConfig","type":"object"}"#;

    fn schemas(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn keys(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn messages(check: &Check) -> Vec<(String, Span)> {
        check
            .issues
            .iter()
            .map(|i| (i.message.clone(), i.span))
            .collect()
    }

    #[test]
    fn template_holds_only_defaults_of_selected_assets_with_config() {
        let schemas = schemas(&[("cfg", THRESHOLD), ("api", PIPELINE)]);
        let text = config_template(&keys(&["plain", "api", "cfg"]), &schemas).unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        // `api_key` has no default, so it is never pre-filled.
        assert_eq!(
            value,
            serde_json::json!({
                "api": {"batch_size": 100, "region": null},
                "cfg": {"threshold": 0.5, "max_retries": 3}
            })
        );
        // No selected asset takes config: no editor.
        assert_eq!(config_template(&keys(&["plain"]), &schemas), None);
        assert_eq!(config_template(&keys(&["cfg"]), &HashMap::new()), None);
    }

    #[test]
    fn template_lists_an_asset_whose_config_has_no_defaults() {
        let schema = r#"{"properties":{"api_key":{"type":"string"}},"required":["api_key"],"type":"object"}"#;
        let text = config_template(&keys(&["api"]), &schemas(&[("api", schema)])).unwrap();
        assert_eq!(text, "{\n  \"api\": {}\n}");
    }

    #[test]
    fn verb_schemas_come_from_each_assets_declaration() {
        use crate::types::AssetActionInfo;
        let mut def = AssetDefinitionInfo {
            asset_key: "a".into(),
            description: None,
            partition_def: None,
            hooks: vec![],
            io_handler: None,
            has_self_dependency: false,
            is_external: false,
            automation_condition: None,
            tags: vec![],
            kinds: vec![],
            group: None,
            code_version: None,
            asset_type: "asset".into(),
            actions: vec![AssetActionInfo {
                name: "compact".into(),
                outcome: "unchanged".into(),
                exclusive: false,
                partitioning: "optional".into(),
                description: None,
                config_schema: Some(PIPELINE.into()),
            }],
            config_schema: Some(THRESHOLD.into()),
        };
        let mut defs = HashMap::new();
        defs.insert("a".to_string(), def.clone());
        def.asset_key = "b".into();
        def.config_schema = None;
        def.actions.clear();
        defs.insert("b".to_string(), def);

        let selected = keys(&["a", "b"]);
        assert_eq!(
            launch_config_schemas(&selected, &defs, None),
            schemas(&[("a", THRESHOLD)])
        );
        assert_eq!(
            launch_config_schemas(&selected, &defs, Some("compact")),
            schemas(&[("a", PIPELINE)])
        );
        assert!(launch_config_schemas(&selected, &defs, Some("vacuum")).is_empty());
    }

    #[test]
    fn blank_or_empty_text_sends_nothing() {
        let selected = keys(&["a"]);
        assert_eq!(validate_config_text("", &selected), Ok(None));
        assert_eq!(validate_config_text("  \n", &selected), Ok(None));
        assert_eq!(validate_config_text("{}", &selected), Ok(None));
        // The template's entry for an asset without defaults, left untouched.
        assert_eq!(validate_config_text(r#"{"a": {}}"#, &selected), Ok(None));
    }

    #[test]
    fn overrides_are_compacted_and_checked_against_the_selection() {
        let selected = keys(&["a", "b"]);
        assert_eq!(
            validate_config_text(
                "{\n  \"a\": {\"threshold\": 0.9},\n  \"b\": {}\n}",
                &selected
            ),
            Ok(Some(r#"{"a":{"threshold":0.9}}"#.to_string()))
        );
        let err = validate_config_text(r#"{"c": {"x": 1}}"#, &selected).unwrap_err();
        assert_eq!(err, "'c' is not in the selection.");
        let err = validate_config_text(r#"{"a": 1}"#, &selected).unwrap_err();
        assert_eq!(err, "Config for 'a' must be a JSON object of field values.");
        let err = validate_config_text("[1]", &selected).unwrap_err();
        assert_eq!(err, "Config must be a JSON object keyed by asset name.");
        let err = validate_config_text("{\"a\": ", &selected).unwrap_err();
        assert!(err.starts_with("Config is not valid JSON:"), "{err}");
    }

    #[test]
    fn check_reports_syntax_then_shape_then_schema_issues() {
        let selected = keys(&["api", "plain"]);
        let schemas = schemas(&[("api", PIPELINE)]);
        let check = |text: &str| check_config(text, &selected, &schemas);

        let syntax = check("{\"api\": ");
        assert_eq!(
            messages(&syntax),
            vec![("Expected a value".to_string(), Span::at(8))]
        );
        assert_eq!(syntax.payload, None);
        assert_eq!(
            messages(&check("[1]")),
            vec![(
                "Config must be a JSON object keyed by asset name.".to_string(),
                Span { start: 0, end: 3 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"other": {"x": 1}}"#)),
            vec![(
                "'other' is not in the selection.".to_string(),
                Span { start: 1, end: 8 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"api": 1}"#)),
            vec![(
                "Config for 'api' must be a JSON object of field values.".to_string(),
                Span { start: 8, end: 9 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"api": {"batch_sizes": 1, "region": 2}}"#)),
            vec![
                (
                    "api: unknown field 'batch_sizes'; expected one of api_key, batch_size, region"
                        .to_string(),
                    Span { start: 9, end: 22 }
                ),
                (
                    "api.region: expected string | null, got number".to_string(),
                    Span { start: 37, end: 38 }
                ),
            ]
        );
        // An asset without a schema takes any object.
        assert!(check(r#"{"plain": {"anything": 1}}"#).issues.is_empty());
    }

    #[test]
    fn check_hints_required_fields_and_gives_the_payload() {
        let selected = keys(&["api", "plain"]);
        let schemas = schemas(&[("api", PIPELINE)]);
        let check = |text: &str| check_config(text, &selected, &schemas);

        assert_eq!(check("").missing, Vec::<String>::new());
        assert_eq!(check("{}").missing, keys(&["api.api_key"]));
        assert_eq!(check(r#"{"api": {}}"#).missing, keys(&["api.api_key"]));
        let set = check(r#"{"api": {"api_key": "k"}, "plain": {}}"#);
        assert_eq!(set.missing, Vec::<String>::new());
        assert_eq!(set.payload.as_deref(), Some(r#"{"api":{"api_key":"k"}}"#));
        assert_eq!(check(r#"{"api": {}}"#).payload, None);
        assert_eq!(check("").payload, None);
        // Issues leave no payload, and the hint still lists what is missing.
        let broken = check(r#"{"api": {"batch_size": "x"}}"#);
        assert_eq!(broken.payload, None);
        assert_eq!(broken.missing, keys(&["api.api_key"]));
    }
}
