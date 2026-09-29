//! Per-asset config editor shared by the launch dialogs.
//!
//! The text is the JSON the backend stores on the run —
//! `{"asset": {"field": value}}`. It opens pre-filled with each selected
//! asset's schema defaults; a field without a default (required, or a
//! `BaseSettings` field the environment resolves) is only listed, never sent.

use std::collections::HashMap;

use leptos::prelude::*;
use serde_json::{Map, Value};

use crate::types::AssetDefinitionInfo;

/// One field of a config class, read from its pydantic JSON schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigField {
    pub name: String,
    /// Short type label: "integer", "string | null", "enum", a model name.
    pub type_label: String,
    /// The default as compact JSON; `None` for a field without one.
    pub default: Option<String>,
}

fn type_label(prop: &Value) -> String {
    match prop.get("type") {
        Some(Value::String(t)) => return t.clone(),
        Some(Value::Array(ts)) => {
            return ts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" | ");
        }
        _ => {}
    }
    if prop.get("enum").is_some() {
        return "enum".to_string();
    }
    if let Some(Value::Array(any)) = prop.get("anyOf") {
        return any.iter().map(type_label).collect::<Vec<_>>().join(" | ");
    }
    if let Some(reference) = prop.get("$ref").and_then(Value::as_str) {
        return reference
            .rsplit('/')
            .next()
            .unwrap_or(reference)
            .to_string();
    }
    "any".to_string()
}

fn properties(schema_json: &str) -> Option<Map<String, Value>> {
    let schema: Value = serde_json::from_str(schema_json).ok()?;
    schema.get("properties")?.as_object().cloned()
}

/// The fields a config schema declares.
pub fn schema_fields(schema_json: &str) -> Vec<ConfigField> {
    properties(schema_json)
        .unwrap_or_default()
        .iter()
        .map(|(name, prop)| ConfigField {
            name: name.clone(),
            type_label: type_label(prop),
            default: prop.get("default").map(Value::to_string),
        })
        .collect()
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

/// The dialog section: a JSON textarea over `text`, the fields legend and a
/// reset. Renders nothing when no selected key has a schema. `reset` turning
/// true (the dialog opening) refills the template; a selection change does
/// too until the user has typed.
#[component]
pub fn ConfigEditor(
    #[prop(into)] selected: Signal<Vec<String>>,
    #[prop(into)] schemas: Signal<HashMap<String, String>>,
    text: RwSignal<String>,
    #[prop(into)] reset: Signal<bool>,
    #[prop(into)] error: Signal<Option<String>>,
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
    let fields = Memo::new(move |_| -> Vec<(String, Vec<ConfigField>)> {
        let schemas = schemas.get();
        selected
            .get()
            .into_iter()
            .filter_map(|key| {
                let fields = schema_fields(schemas.get(&key)?);
                Some((key, fields))
            })
            .collect()
    });

    view! {
        <Show when=move || template.get().is_some()>
            <div class="form-group config-editor">
                <div class="mat-dialog-col-head">
                    <label>"Config"</label>
                    <span class="mat-dialog-col-actions">
                        <button class="link-btn" on:click=move |_| refill()>"Reset to defaults"</button>
                    </span>
                </div>
                <textarea
                    class="form-input config-editor-text"
                    spellcheck="false"
                    aria-label="Config"
                    prop:value=move || text.get()
                    on:input=move |ev| {
                        dirty.set(true);
                        text.set(event_target_value(&ev));
                    }
                ></textarea>
                {move || error.get().map(|e| view! {
                    <div class="text-error config-editor-error">{e}</div>
                })}
                <div class="config-fields">
                    {move || fields.get().into_iter().map(|(asset, fields)| view! {
                        <div class="config-fields-asset">{asset}</div>
                        {fields.into_iter().map(|f| view! {
                            <div class="config-field">
                                <span class="config-field-name">{f.name}</span>
                                <span class="config-field-type">{f.type_label}</span>
                                {match f.default {
                                    Some(d) => view! {
                                        <span class="config-field-default">{format!("= {d}")}</span>
                                    }.into_any(),
                                    None => view! {
                                        <span class="config-field-required">"required"</span>
                                    }.into_any(),
                                }}
                            </div>
                        }).collect::<Vec<_>>()}
                    }).collect::<Vec<_>>()}
                </div>
                <div class="config-editor-note">
                    "Fields without a default are listed, not pre-filled; a BaseSettings field reads the environment unless set here."
                </div>
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

    #[test]
    fn fields_carry_type_and_default() {
        let fields = schema_fields(PIPELINE);
        let by_name: HashMap<&str, &ConfigField> =
            fields.iter().map(|f| (f.name.as_str(), f)).collect();
        assert_eq!(by_name["api_key"].type_label, "string");
        assert_eq!(by_name["api_key"].default, None);
        assert_eq!(by_name["batch_size"].default.as_deref(), Some("100"));
        assert_eq!(by_name["region"].type_label, "string | null");
        assert_eq!(by_name["region"].default.as_deref(), Some("null"));
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
}
