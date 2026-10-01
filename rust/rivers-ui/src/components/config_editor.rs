//! The launch document editor shared by the launch dialogs.
//!
//! The text is the JSON the backend stores on the run:
//! `{"assets": {"raw_users": {"config": {...}, "metadata": {...}}}}`.
//! [`launch_schema`] composes one JSON schema from the selected assets'
//! definitions; the template, the checks, the completions and the scaffold
//! all read that schema. A field without a default (required, or a
//! `BaseSettings` field the environment resolves) is hinted, never sent.
//! [`check_config`] gives the dialogs the issues that block a submit and
//! the payload to send.

use std::collections::HashMap;
use std::time::Duration;

use leptos::prelude::*;
use serde_json::{Map, Value, json};

use crate::components::code_editor::CodeEditor;
use crate::config_schema::{self, Candidate, Schema};
use crate::json_text::{self, Issue, PathSeg, Slot, Span};
use crate::server_fns::mutations::validate_config;
use crate::server_fns::overview::get_resources_info;
use crate::types::{AssetDefinitionInfo, ConfigError, ConfigLoc, ResourceInfo};

/// The config class schema a launch of `def` uses: its own for materialize,
/// the verb's declaration for an action run.
fn config_schema_of<'a>(def: &'a AssetDefinitionInfo, verb: Option<&str>) -> Option<&'a str> {
    match verb {
        Some(verb) => def
            .actions
            .iter()
            .find(|a| a.name == verb)?
            .config_schema
            .as_deref(),
        None => def.config_schema.as_deref(),
    }
}

/// Whether a launch of `keys` has anything for the editor: a config class
/// or metadata on a selected key. A one-click launch must open the dialog
/// instead when it does.
pub fn launch_takes_config(
    keys: &[String],
    definitions: &HashMap<String, AssetDefinitionInfo>,
    verb: Option<&str>,
) -> bool {
    keys.iter()
        .filter_map(|key| definitions.get(key))
        .any(|def| config_schema_of(def, verb).is_some() || !def.metadata.is_empty())
}

/// The JSON schema of the launch document for `keys`. Under `assets`, each
/// selected key gets `config` (its class schema, when it has one) and,
/// unless it is a task, `metadata`: its keys as string fields defaulting
/// to the current values, any other key allowed. Under `resources`, each of
/// `resources` by key, its current values as the defaults. `execution` names
/// the run's executor.
pub fn launch_schema(
    keys: &[String],
    definitions: &HashMap<String, AssetDefinitionInfo>,
    resources: &[ResourceInfo],
    verb: Option<&str>,
) -> String {
    let mut assets = Map::new();
    for key in keys {
        let Some(def) = definitions.get(key) else {
            continue;
        };
        let mut sections = Map::new();
        if let Some(class) = config_schema_of(def, verb) {
            let at = [
                "properties",
                "assets",
                "properties",
                key,
                "properties",
                "config",
            ];
            if let Some(config) = config_schema::inline(class, &at) {
                sections.insert("config".to_string(), config);
            }
        }
        let is_task = def.asset_type == "task";
        if !is_task {
            let current: Map<String, Value> = def
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), json!({"type": "string", "default": v})))
                .collect();
            sections.insert(
                "metadata".to_string(),
                json!({
                    "type": "object",
                    "description": "metadata for this run: keys added to or replacing the asset's own",
                    "properties": current,
                    "additionalProperties": {"type": "string"}
                }),
            );
        }
        if sections.is_empty() {
            continue;
        }
        assets.insert(
            key.clone(),
            json!({
                "type": "object",
                "description": if is_task { "task" } else { "asset" },
                "properties": sections,
                "additionalProperties": false
            }),
        );
    }
    let mut sections = Map::new();
    sections.insert(
        "assets".to_string(),
        json!({
            "type": "object",
            "description": "per-asset overrides for this run",
            "properties": assets,
            "additionalProperties": false
        }),
    );
    if !resources.is_empty() {
        let mut by_key = Map::new();
        for resource in resources {
            let at = ["properties", "resources", "properties", &resource.key];
            if let Some(schema) = config_schema::inline(&resource.config_schema, &at) {
                by_key.insert(resource.key.clone(), schema);
            }
        }
        sections.insert(
            "resources".to_string(),
            json!({
                "type": "object",
                "description": "resources rebuilt for this run with these values",
                "properties": by_key,
                "additionalProperties": false
            }),
        );
    }
    sections.insert(
        "execution".to_string(),
        json!({
            "type": "object",
            "description": "the run's executor; an asset's rivers/executor metadata still wins for its step",
            "properties": {
                "executor": {"type": "string", "enum": ["in_process", "parallel"]},
                "max_workers": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "worker processes",
                    "x-requires": {"executor": "parallel"}
                },
                "max_async_concurrent": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "async steps in flight per worker",
                    "x-requires": {"executor": "parallel"}
                }
            },
            "additionalProperties": false
        }),
    );
    let document = json!({
        "type": "object",
        "properties": sections,
        "additionalProperties": false
    });
    document.to_string()
}

/// The editor's opening text: the defaults of every config class and the
/// current metadata of every selected asset, in the document's shape.
pub fn config_template(schema_json: &str) -> Option<String> {
    config_template_over(schema_json, None)
}

/// [`config_template`] with `document` laid over it: a rerun opens on its
/// run's values, the defaults around them.
pub fn config_template_over(schema_json: &str, document: Option<&str>) -> Option<String> {
    let schema = Schema::parse(schema_json)?;
    let mut template = config_schema::template(&schema);
    if let Some(document) = document.and_then(|d| serde_json::from_str(d).ok()) {
        overlay(&mut template, document);
    }
    serde_json::to_string_pretty(&template).ok()
}

fn overlay(base: &mut Value, top: Value) {
    match (base, top) {
        (Value::Object(base), Value::Object(top)) => {
            for (key, value) in top {
                match base.get_mut(&key) {
                    Some(slot) => overlay(slot, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, top) => *base = top,
    }
}

/// What the dialogs learn from the text: the issues that block a submit,
/// the required fields not set (a hint), and the payload to send.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Check {
    pub issues: Vec<Issue>,
    pub missing: Vec<String>,
    pub payload: Option<String>,
}

/// Syntax first, then the document against its schema. A syntax error is
/// the only issue reported, as the tree past it is a guess. A required
/// field the text lacks is listed as missing; it is an issue too unless
/// its class reads the environment, which may set it when the run starts.
/// The payload is what differs from the definitions: the text compacted, a
/// value equal to its default and the parts left empty dropped; `None` for
/// blank text, an issue, or nothing that differs. Without a schema there is
/// no document.
pub fn check_config(text: &str, schema_json: Option<&str>) -> Check {
    if text.trim().is_empty() {
        return Check::default();
    }
    let Some(schema) = schema_json.and_then(Schema::parse) else {
        return Check::default();
    };
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
    let mut issues = config_schema::validate(&schema, &root, "");
    let found = config_schema::missing_required(&schema, &root, "");
    issues.extend(found.iter().filter(|m| !m.env).map(|m| Issue {
        span: m.span,
        message: format!("{}: required", m.path),
    }));
    let missing = found.into_iter().map(|m| m.path).collect();
    let mut payload = None;
    if issues.is_empty() {
        match serde_json::from_str::<Value>(text) {
            Ok(value) => payload = config_schema::compact(&schema, value).map(|v| v.to_string()),
            Err(e) => issues.push(Issue {
                span: root.span(),
                message: format!("Config is not valid JSON: {e}"),
            }),
        }
    }
    Check {
        issues,
        missing,
        payload,
    }
}

fn path_label(path: &[PathSeg]) -> String {
    let mut label = String::new();
    for seg in path {
        match seg {
            PathSeg::Key(key) if label.is_empty() => label.push_str(key),
            PathSeg::Key(key) => {
                label.push('.');
                label.push_str(key);
            }
            PathSeg::Index(index) => label.push_str(&format!("[{index}]")),
        }
    }
    label
}

/// The editor issues for what the definitions reported: each at its value
/// in the text (its key for an unknown field, the nearest object above when
/// the path is not there). A `missing` field is not an issue; it joins the
/// required hint under its document path.
pub fn server_issues(text: &str, errors: &[ConfigError]) -> (Vec<Issue>, Vec<String>) {
    let parsed = json_text::parse(text);
    let mut issues = Vec::new();
    let mut missing = Vec::new();
    for error in errors {
        let mut path: Vec<PathSeg> = error.path.iter().cloned().map(PathSeg::Key).collect();
        path.extend(error.loc.iter().map(|part| match part {
            ConfigLoc::Key(key) => PathSeg::Key(key.clone()),
            ConfigLoc::Index(index) => PathSeg::Index(*index as usize),
        }));
        let label = path_label(&path);
        if error.kind == "missing" {
            missing.push(label);
            continue;
        }
        let root = parsed.root.as_ref();
        let spans = root.and_then(|r| json_text::span_at(r, &path));
        let span = match spans {
            Some((Some(key), _)) if error.kind == "extra_forbidden" => key,
            Some((_, value)) => value,
            None => root
                .and_then(|r| {
                    (0..path.len())
                        .rev()
                        .find_map(|n| json_text::span_at(r, &path[..n]))
                })
                .map_or(Span::at(0), |(_, value)| value),
        };
        issues.push(Issue {
            span,
            message: format!("{label}: {}", error.message),
        });
    }
    (issues, missing)
}

/// The dialogs' check: the schema's, at once, plus what the definitions
/// report for the text, asked of `location` 300 ms after the last edit
/// once the schema is satisfied. `action` names the verb whose classes
/// apply. A reply for text that has changed since is dropped.
pub fn use_config_check(
    text: RwSignal<String>,
    selected: Signal<Vec<String>>,
    schema: Signal<Option<String>>,
    location: Signal<(String, String)>,
    action: Signal<Option<String>>,
) -> Signal<Check> {
    let client = Memo::new(move |_| check_config(&text.get(), schema.get().as_deref()));
    // The definitions' answer and the text it answers.
    let server = RwSignal::new((String::new(), Vec::<Issue>::new(), Vec::<String>::new()));
    let timer: StoredValue<Option<TimeoutHandle>> = StoredValue::new(None);
    Effect::new(move || {
        let check = client.get();
        if let Some(handle) = timer.get_value() {
            handle.clear();
            timer.set_value(None);
        }
        let Some(payload) = check.payload.filter(|_| check.issues.is_empty()) else {
            return;
        };
        let asked = text.get_untracked();
        let handle = set_timeout_with_handle(
            move || {
                timer.set_value(None);
                let (ns, name) = location.get_untracked();
                let selection = selected.get_untracked();
                let action = action.get_untracked();
                leptos::task::spawn_local(async move {
                    let errors = match validate_config(ns, name, selection, action, payload).await {
                        Ok(errors) => errors,
                        Err(e) => {
                            leptos::logging::warn!("config check failed: {e}");
                            return;
                        }
                    };
                    if text.get_untracked() != asked {
                        return;
                    }
                    let (issues, missing) = server_issues(&asked, &errors);
                    server.set((asked, issues, missing));
                });
            },
            Duration::from_millis(300),
        );
        timer.set_value(handle.ok());
    });
    let merged = Memo::new(move |_| {
        let mut check = client.get();
        let (answered, issues, missing) = server.get();
        if answered != text.get() {
            return check;
        }
        check.issues.extend(issues);
        for field in missing {
            if !check.missing.contains(&field) {
                check.missing.push(field);
            }
        }
        if !check.issues.is_empty() {
            check.payload = None;
        }
        check
    });
    merged.into()
}

/// A launch dialog's document: the text, its schema for `keys` (and the
/// verb's classes), and the check that gates the submit.
pub struct LaunchConfig {
    pub text: RwSignal<String>,
    pub schema: Signal<Option<String>>,
    pub check: Signal<Check>,
}

pub fn use_launch_config(
    keys: Signal<Vec<String>>,
    definitions: Signal<HashMap<String, AssetDefinitionInfo>>,
    resources: Signal<Vec<ResourceInfo>>,
    verb: Signal<Option<String>>,
    location: Signal<(String, String)>,
) -> LaunchConfig {
    let text = RwSignal::new(String::new());
    let schema: Signal<Option<String>> = Memo::new(move |_| {
        let verb = verb.get();
        definitions.with(|defs| {
            resources.with(|res| Some(launch_schema(&keys.get(), defs, res, verb.as_deref())))
        })
    })
    .into();
    let check = use_config_check(text, keys, schema, location, verb);
    LaunchConfig {
        text,
        schema,
        check,
    }
}

/// The resources a launch document may override, fetched while `open`:
/// a page whose dialog stays closed makes no call for them.
pub fn use_launch_resources(
    location: Signal<(String, String)>,
    open: Signal<bool>,
) -> Signal<Vec<ResourceInfo>> {
    let fetched = Resource::new(
        move || open.get().then(|| location.get()),
        |target| async move {
            match target {
                Some((ns, name)) => get_resources_info(ns, name).await.unwrap_or_default(),
                None => Vec::new(),
            }
        },
    );
    let value = crate::helpers::resource_value(fetched);
    Signal::derive(move || value.get().unwrap_or_default())
}

/// The completions at `caret`: the keys the object there lacks, or the
/// values its field takes, from the document's schema.
pub fn completions(text: &str, caret: usize, schema_json: Option<&str>) -> Vec<Candidate> {
    let Some(schema) = schema_json.and_then(Schema::parse) else {
        return Vec::new();
    };
    let ctx = json_text::context_at(text, &json_text::parse(text), caret);
    match ctx.slot {
        Slot::Key { .. } => config_schema::key_candidates(&schema, &ctx.path, &ctx.siblings),
        Slot::Value { .. } => config_schema::value_candidates(&schema, &ctx.path),
        Slot::None => Vec::new(),
    }
}

/// The dialog section: the code editor over `text`, the required-fields
/// hint and a reset. `reset` turning true (the dialog opening) refills the
/// template, with `base` laid over it when given; a schema change does too
/// until the user has typed.
#[component]
pub fn ConfigEditor(
    #[prop(into)] schema: Signal<Option<String>>,
    text: RwSignal<String>,
    #[prop(into)] reset: Signal<bool>,
    #[prop(into)] check: Signal<Check>,
    /// The document to open on: a rerun's stored one.
    #[prop(optional, into)]
    base: Option<Signal<Option<String>>>,
) -> impl IntoView {
    let dirty = RwSignal::new(false);
    let template = Memo::new(move |_| schema.get().as_deref().and_then(config_template));
    let opening = Memo::new(move |_| {
        let base = base.and_then(|b| b.get());
        schema
            .get()
            .as_deref()
            .and_then(|s| config_template_over(s, base.as_deref()))
    });
    let refill = move |to: Option<String>| {
        dirty.set(false);
        text.set(to.unwrap_or_default());
    };
    Effect::new(move || {
        if reset.get() {
            refill(opening.get_untracked());
        }
    });
    Effect::new(move || {
        let opening = opening.get();
        if !dirty.get_untracked() {
            text.set(opening.unwrap_or_default());
        }
    });
    let issues = Signal::derive(move || check.get().issues);
    let missing = Signal::derive(move || check.get().missing);
    let scaffold = move || {
        let filled = schema
            .get_untracked()
            .as_deref()
            .and_then(Schema::parse)
            .and_then(|schema| config_schema::scaffold_missing(&text.get_untracked(), &schema));
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
                        <button class="link-btn" on:click=move |_| refill(template.get_untracked())>"Reset to defaults"</button>
                    </span>
                </div>
                <CodeEditor
                    text=text
                    issues=issues
                    label="Config"
                    on_edit=Callback::new(move |()| dirty.set(true))
                    complete=Callback::new(move |(text, caret): (String, usize)| {
                        completions(&text, caret, schema.get_untracked().as_deref())
                    })
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
    use crate::types::AssetActionInfo;

    const THRESHOLD: &str = r#"{"properties":{"threshold":{"default":0.5,"title":"Threshold","type":"number"},"max_retries":{"default":3,"title":"Max Retries","type":"integer"}},"title":"ThresholdConfig","type":"object"}"#;
    const PIPELINE: &str = r#"{"properties":{"api_key":{"title":"Api Key","type":"string"},"batch_size":{"default":100,"title":"Batch Size","type":"integer"},"region":{"anyOf":[{"type":"string"},{"type":"null"}],"default":null,"title":"Region"}},"required":["api_key"],"title":"PipelineConfig","type":"object","x-settings":true}"#;

    fn definition(
        key: &str,
        config_schema: Option<&str>,
        metadata: &[(&str, &str)],
    ) -> AssetDefinitionInfo {
        AssetDefinitionInfo {
            asset_key: key.into(),
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
            asset_type: "single".into(),
            actions: vec![],
            config_schema: config_schema.map(str::to_string),
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn definitions(defs: Vec<AssetDefinitionInfo>) -> HashMap<String, AssetDefinitionInfo> {
        defs.into_iter().map(|d| (d.asset_key.clone(), d)).collect()
    }

    fn keys(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    const DB: &str = r#"{"properties":{"dsn":{"default":"memory://","title":"Dsn","type":"string"},"pool_size":{"default":2,"title":"Pool Size","type":"integer"},"token":{"format":"password","title":"Token","type":"string","writeOnly":true}},"title":"DbResource","type":"object"}"#;
    const STRICT: &str = r#"{"properties":{"token":{"title":"Token","type":"string"},"limit":{"default":1,"title":"Limit","type":"integer"}},"required":["token"],"title":"StrictConfig","type":"object"}"#;

    fn resource(key: &str, config_schema: &str) -> ResourceInfo {
        ResourceInfo {
            key: key.into(),
            config_schema: config_schema.into(),
        }
    }

    /// The schema of a launch of `api` (PIPELINE), `cfg` (THRESHOLD) and
    /// `plain` (no config, one metadata key), with the resource `db`.
    fn schema() -> String {
        let defs = definitions(vec![
            definition("api", Some(PIPELINE), &[]),
            definition("cfg", Some(THRESHOLD), &[]),
            definition("plain", None, &[("owner", "data")]),
        ]);
        launch_schema(
            &keys(&["api", "cfg", "plain"]),
            &defs,
            &[resource("db", DB)],
            None,
        )
    }

    fn messages(check: &Check) -> Vec<(String, Span)> {
        check
            .issues
            .iter()
            .map(|i| (i.message.clone(), i.span))
            .collect()
    }

    #[test]
    fn template_holds_defaults_and_current_metadata_of_the_selection() {
        let text = config_template(&schema()).unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        // `api_key` has no default, so it is never pre-filled; an asset
        // without metadata gets no `metadata`; a resource is listed by key
        // alone, its values sent only when typed; `execution` has no defaults.
        assert_eq!(
            value,
            json!({
                "assets": {
                    "api": {"config": {"batch_size": 100, "region": null}},
                    "cfg": {"config": {"threshold": 0.5, "max_retries": 3}},
                    "plain": {"metadata": {"owner": "data"}}
                },
                "execution": {},
                "resources": {"db": {"dsn": "memory://", "pool_size": 2}}
            })
        );
        // No selected asset takes config: a one-click launch, but the dialog
        // still edits `execution` (and metadata to add) when opened.
        let defs = definitions(vec![
            definition("bare", None, &[]),
            definition("api", Some(PIPELINE), &[]),
        ]);
        let bare = launch_schema(&keys(&["bare"]), &defs, &[], None);
        assert_eq!(config_template(&bare).unwrap(), "{\n  \"execution\": {}\n}");
        assert!(!launch_takes_config(&keys(&["bare"]), &defs, None));
        assert!(launch_takes_config(&keys(&["bare", "api"]), &defs, None));
    }

    #[test]
    fn a_stored_document_opens_over_the_defaults() {
        let schema = schema();
        let stored = r#"{"assets":{"api":{"config":{"api_key":"k","batch_size":5}},"plain":{"metadata":{"tier":"gold"}}},"execution":{"executor":"parallel"}}"#;
        let text = config_template_over(&schema, Some(stored)).unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            value,
            json!({
                "assets": {
                    "api": {"config": {"api_key": "k", "batch_size": 5, "region": null}},
                    "cfg": {"config": {"threshold": 0.5, "max_retries": 3}},
                    "plain": {"metadata": {"owner": "data", "tier": "gold"}}
                },
                "execution": {"executor": "parallel"},
                "resources": {"db": {"dsn": "memory://", "pool_size": 2}}
            })
        );
        // Left as opened, the rerun sends what the run had.
        let check = check_config(&text, Some(&schema));
        assert!(check.issues.is_empty(), "{:?}", check.issues);
        let sent: Value = serde_json::from_str(&check.payload.unwrap()).unwrap();
        assert_eq!(sent, serde_json::from_str::<Value>(stored).unwrap());
        // No stored document, or one that does not parse: the defaults.
        assert_eq!(
            config_template_over(&schema, None),
            config_template(&schema)
        );
        assert_eq!(
            config_template_over(&schema, Some("not json")),
            config_template(&schema)
        );
    }

    #[test]
    fn template_lists_a_config_class_without_defaults() {
        let class = r#"{"properties":{"api_key":{"type":"string"}},"required":["api_key"],"type":"object"}"#;
        let defs = definitions(vec![definition("api", Some(class), &[])]);
        let schema = launch_schema(&keys(&["api"]), &defs, &[], None);
        assert_eq!(
            config_template(&schema).unwrap(),
            "{\n  \"assets\": {\n    \"api\": {\n      \"config\": {}\n    }\n  },\n  \"execution\": {}\n}"
        );
    }

    #[test]
    fn verb_schemas_come_from_each_assets_declaration() {
        let mut def = definition("a", Some(THRESHOLD), &[]);
        def.actions = vec![AssetActionInfo {
            name: "compact".into(),
            outcome: "unchanged".into(),
            exclusive: false,
            partitioning: "optional".into(),
            description: None,
            config_schema: Some(PIPELINE.into()),
        }];
        let defs = definitions(vec![def, definition("b", None, &[])]);
        let selected = keys(&["a", "b"]);
        let class_of = |verb: Option<&str>| -> Option<String> {
            let schema: Value =
                serde_json::from_str(&launch_schema(&selected, &defs, &[], verb)).unwrap();
            schema
                .pointer("/properties/assets/properties/a/properties/config/title")
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        assert_eq!(class_of(None).as_deref(), Some("ThresholdConfig"));
        assert_eq!(class_of(Some("compact")).as_deref(), Some("PipelineConfig"));
        assert_eq!(class_of(Some("vacuum")), None);
    }

    #[test]
    fn tasks_get_config_but_no_metadata() {
        let mut task = definition("t", Some(THRESHOLD), &[]);
        task.asset_type = "task".into();
        let defs = definitions(vec![task]);
        let schema: Value =
            serde_json::from_str(&launch_schema(&keys(&["t"]), &defs, &[], None)).unwrap();
        let sections = schema
            .pointer("/properties/assets/properties/t/properties")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(sections.keys().collect::<Vec<_>>(), vec!["config"]);
    }

    #[test]
    fn blank_or_empty_text_sends_nothing() {
        let schema = schema();
        let check = |text: &str| check_config(text, Some(&schema));
        assert_eq!(check("").payload, None);
        assert_eq!(check("  \n").payload, None);
        assert_eq!(check("{}").payload, None);
        // The template, left untouched, and a value equal to its default.
        assert_eq!(check(&config_template(&schema).unwrap()).payload, None);
        assert_eq!(
            check(r#"{"assets": {"api": {"config": {"batch_size": 100}}}, "resources": {"db": {"pool_size": 2}}}"#)
                .payload,
            None
        );
        // Without a schema there is no document.
        assert_eq!(check_config(r#"{"x": 1}"#, None), Check::default());
    }

    #[test]
    fn payload_is_compact_and_keeps_what_is_set() {
        let schema = schema();
        let check = check_config(
            "{\n  \"assets\": {\n    \"api\": {\"config\": {\"batch_size\": 5}, \"metadata\": {}},\n    \"plain\": {\"metadata\": {\"owner\": \"me\", \"tier\": \"gold\"}}\n  }\n}",
            Some(&schema),
        );
        assert!(check.issues.is_empty(), "{:?}", check.issues);
        assert_eq!(
            check.payload.as_deref(),
            Some(
                r#"{"assets":{"api":{"config":{"batch_size":5}},"plain":{"metadata":{"owner":"me","tier":"gold"}}}}"#
            )
        );
    }

    #[test]
    fn check_reports_syntax_then_schema_issues() {
        let schema = schema();
        let check = |text: &str| check_config(text, Some(&schema));

        let syntax = check("{\"assets\": ");
        assert_eq!(
            messages(&syntax),
            vec![("Expected a value".to_string(), Span::at(11))]
        );
        assert_eq!(syntax.payload, None);
        assert_eq!(
            messages(&check("[1]")),
            vec![(
                "expected object, got array".to_string(),
                Span { start: 0, end: 3 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"other": {"x": 1}}"#)),
            vec![(
                "unknown field 'other'; expected one of assets, execution, resources".to_string(),
                Span { start: 1, end: 8 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"assets": {"other": {}}}"#)),
            vec![(
                "assets: unknown field 'other'; expected one of api, cfg, plain".to_string(),
                Span { start: 12, end: 19 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"assets": {"api": 1}}"#)),
            vec![(
                "assets.api: expected object, got number".to_string(),
                Span { start: 19, end: 20 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"assets": {"api": {"config": {"batch_sizes": 1, "region": 2}}}}"#)),
            vec![
                (
                    "assets.api.config: unknown field 'batch_sizes'; expected one of api_key, batch_size, region"
                        .to_string(),
                    Span { start: 31, end: 44 }
                ),
                (
                    "assets.api.config.region: expected string | null, got number".to_string(),
                    Span { start: 59, end: 60 }
                ),
            ]
        );
        // Metadata values are strings, known key or not; a task has none.
        assert_eq!(
            messages(&check(
                r#"{"assets": {"plain": {"metadata": {"owner": 1, "tier": "gold"}}}}"#
            )),
            vec![(
                "assets.plain.metadata.owner: expected string, got number".to_string(),
                Span { start: 44, end: 45 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"assets": {"plain": {"config": {}}}}"#)),
            vec![(
                "assets.plain: unknown field 'config'; expected one of metadata".to_string(),
                Span { start: 22, end: 30 }
            )]
        );
    }

    #[test]
    fn check_hints_required_fields_through_absent_containers() {
        let schema = schema();
        let check = |text: &str| check_config(text, Some(&schema));
        assert_eq!(check("").missing, Vec::<String>::new());
        assert_eq!(check("{}").missing, keys(&["assets.api.config.api_key"]));
        assert_eq!(
            check(r#"{"assets": {"api": {}}}"#).missing,
            keys(&["assets.api.config.api_key"])
        );
        let set = check(r#"{"assets": {"api": {"config": {"api_key": "k"}}, "plain": {}}}"#);
        assert_eq!(set.missing, Vec::<String>::new());
        assert_eq!(
            set.payload.as_deref(),
            Some(r#"{"assets":{"api":{"config":{"api_key":"k"}}}}"#)
        );
        // Issues leave no payload, and the hint still lists what is missing.
        let broken = check(r#"{"assets": {"api": {"config": {"batch_size": "x"}}}}"#);
        assert_eq!(broken.payload, None);
        assert_eq!(broken.missing, keys(&["assets.api.config.api_key"]));
    }

    #[test]
    fn a_plain_models_required_field_is_an_issue_until_set() {
        let defs = definitions(vec![
            definition("api", Some(PIPELINE), &[]),
            definition("strict", Some(STRICT), &[]),
        ]);
        let schema = launch_schema(&keys(&["api", "strict"]), &defs, &[], None);
        let check = |text: &str| check_config(text, Some(&schema));
        // The settings class may get `api_key` from the environment: a hint
        // only. The plain class cannot: an issue at the object lacking it.
        let text = r#"{"assets": {"strict": {"config": {"limit": 2}}}}"#;
        let unset = check(text);
        let config = text.find("{\"limit\"").unwrap();
        assert_eq!(
            messages(&unset),
            vec![(
                "assets.strict.config.token: required".to_string(),
                Span {
                    start: config,
                    end: text.len() - 3
                }
            )]
        );
        // The text's objects are walked before the absent containers.
        assert_eq!(
            unset.missing,
            keys(&["assets.strict.config.token", "assets.api.config.api_key"])
        );
        assert_eq!(unset.payload, None);
        let set = check(r#"{"assets": {"strict": {"config": {"token": "t"}}}}"#);
        assert_eq!(set.issues, Vec::<Issue>::new());
        assert_eq!(set.missing, keys(&["assets.api.config.api_key"]));
        assert_eq!(
            set.payload.as_deref(),
            Some(r#"{"assets":{"strict":{"config":{"token":"t"}}}}"#)
        );
    }

    #[test]
    fn server_errors_land_on_their_value_or_key_or_join_the_hint() {
        let text = "{\n  \"assets\": {\n    \"api\": {\n      \"config\": {\n        \"batch_size\": 0,\n        \"tags\": [1, 2],\n        \"extra\": 1\n      }\n    }\n  }\n}";
        let error = |loc: Vec<ConfigLoc>, kind: &str, message: &str| ConfigError {
            path: keys(&["assets", "api", "config"]),
            loc,
            message: message.to_string(),
            kind: kind.to_string(),
        };
        let errors = vec![
            error(
                vec![ConfigLoc::Key("batch_size".into())],
                "greater_than",
                "Input should be greater than 0",
            ),
            error(
                vec![ConfigLoc::Key("tags".into()), ConfigLoc::Index(1)],
                "less_than",
                "Input should be less than 2",
            ),
            error(
                vec![ConfigLoc::Key("extra".into())],
                "extra_forbidden",
                "Extra inputs are not permitted",
            ),
            error(
                vec![ConfigLoc::Key("api_key".into())],
                "missing",
                "Field required",
            ),
            error(
                vec![ConfigLoc::Key("nope".into()), ConfigLoc::Key("x".into())],
                "value_error",
                "Value error, no",
            ),
            ConfigError {
                path: keys(&["assets", "api", "metadata"]),
                loc: vec![],
                message: "metadata overrides apply to assets; 'api' is a task".into(),
                kind: "invalid".into(),
            },
        ];
        let (issues, missing) = server_issues(text, &errors);
        let at = |needle: &str| {
            let start = text.find(needle).unwrap();
            Span {
                start,
                end: start + needle.len(),
            }
        };
        let parsed = json_text::parse(text);
        let span_of = |path: &[&str]| {
            let path: Vec<PathSeg> = path.iter().map(|k| PathSeg::Key(k.to_string())).collect();
            json_text::span_at(parsed.root.as_ref().unwrap(), &path)
                .unwrap()
                .1
        };
        assert_eq!(
            issues
                .iter()
                .map(|i| (i.message.as_str(), i.span))
                .collect::<Vec<_>>(),
            vec![
                (
                    "assets.api.config.batch_size: Input should be greater than 0",
                    at("0")
                ),
                (
                    "assets.api.config.tags[1]: Input should be less than 2",
                    at("2")
                ),
                (
                    "assets.api.config.extra: Extra inputs are not permitted",
                    at("\"extra\"")
                ),
                (
                    "assets.api.config.nope.x: Value error, no",
                    span_of(&["assets", "api", "config"])
                ),
                (
                    "assets.api.metadata: metadata overrides apply to assets; 'api' is a task",
                    span_of(&["assets", "api"])
                ),
            ]
        );
        assert_eq!(missing, vec!["assets.api.config.api_key"]);
        // Text that does not parse still gets the message, at the missing value.
        let (issues, _) = server_issues("{\"assets\": ", &errors[..1]);
        assert_eq!(issues[0].span, Span::at(11));
    }

    #[test]
    fn completions_follow_the_caret_down_the_document() {
        let schema = schema();
        let labels = |text: &str, caret: usize| -> Vec<String> {
            completions(text, caret, Some(&schema))
                .into_iter()
                .map(|c| c.label)
                .collect()
        };
        // Sections, then the selected assets, then what each asset has.
        assert_eq!(labels("{", 1), keys(&["assets", "execution", "resources"]));
        let section = &completions("{", 1, Some(&schema))[0];
        assert_eq!(section.insert, "\"assets\": {}");
        assert_eq!(section.caret, 11);
        assert_eq!(section.detail, "object — per-asset overrides for this run");
        assert_eq!(labels("{\"assets\": {", 12), keys(&["api", "cfg", "plain"]));
        assert_eq!(
            labels("{\"assets\": {\"api\": {}, ", 23),
            keys(&["cfg", "plain"])
        );
        assert_eq!(
            labels("{\"assets\": {\"api\": {", 20),
            keys(&["config", "metadata"])
        );
        assert_eq!(
            completions("{\"assets\": {\"api\": {", 20, Some(&schema))[0].detail,
            "PipelineConfig"
        );
        assert_eq!(
            labels("{\"assets\": {\"plain\": {", 22),
            keys(&["metadata"])
        );
        // Inside a config: its fields, then a field's values; inside
        // metadata: the current keys with their values.
        assert_eq!(
            labels("{\"assets\": {\"api\": {\"config\": {\"b", 33),
            keys(&["api_key", "batch_size", "region"])
        );
        assert_eq!(
            labels("{\"assets\": {\"api\": {\"config\": {\"region\": ", 41),
            keys(&["null"])
        );
        assert_eq!(
            labels("{\"assets\": {\"cfg\": {\"config\": {\"threshold\": ", 44),
            keys(&["0.5"])
        );
        assert_eq!(
            labels("{\"assets\": {\"plain\": {\"metadata\": {", 36),
            keys(&["owner"])
        );
        assert_eq!(
            labels("{\"assets\": {\"plain\": {\"metadata\": {\"owner\": ", 45),
            keys(&["\"data\""])
        );
        // No schema, or outside the document.
        assert!(completions("{", 1, None).is_empty());
        assert!(labels("{}", 2).is_empty());
    }
    #[test]
    fn resources_are_checked_by_key_and_class_and_sent_when_typed() {
        let schema = schema();
        let check = |text: &str| check_config(text, Some(&schema));
        assert_eq!(
            messages(&check(r#"{"resources": {"nope": {}}}"#)),
            vec![(
                "resources: unknown field 'nope'; expected one of db".to_string(),
                Span { start: 15, end: 21 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"resources": {"db": {"pool_size": "x"}}}"#)),
            vec![(
                "resources.db.pool_size: expected integer, got string".to_string(),
                Span { start: 35, end: 38 }
            )]
        );
        let set = check(r#"{"resources": {"db": {"pool_size": 8, "token": "t"}}}"#);
        assert!(set.issues.is_empty(), "{:?}", set.issues);
        assert_eq!(
            set.payload.as_deref(),
            Some(r#"{"resources":{"db":{"pool_size":8,"token":"t"}}}"#)
        );
        assert_eq!(check(r#"{"resources": {"db": {}}}"#).payload, None);
    }

    #[test]
    fn execution_names_the_executor_and_its_counts() {
        let schema = schema();
        let check = |text: &str| check_config(text, Some(&schema));
        let set = check(r#"{"execution": {"executor": "parallel", "max_workers": 4}}"#);
        assert!(set.issues.is_empty(), "{:?}", set.issues);
        assert_eq!(
            set.payload.as_deref(),
            Some(r#"{"execution":{"executor":"parallel","max_workers":4}}"#)
        );
        assert_eq!(
            messages(&check(r#"{"execution": {"executor": "k8s"}}"#)),
            vec![(
                "execution.executor: expected one of \"in_process\", \"parallel\"".to_string(),
                Span { start: 27, end: 32 }
            )]
        );
        assert_eq!(
            messages(&check(r#"{"execution": {"max_workers": 2}}"#)),
            vec![(
                "execution: max_workers needs \"executor\": \"parallel\"".to_string(),
                Span { start: 15, end: 28 }
            )]
        );
        let labels = |text: &str, caret: usize| -> Vec<String> {
            completions(text, caret, Some(&schema))
                .into_iter()
                .map(|c| c.label)
                .collect()
        };
        assert_eq!(
            labels("{\"execution\": {", 15),
            keys(&["executor", "max_async_concurrent", "max_workers"])
        );
        assert_eq!(
            labels("{\"execution\": {\"executor\": ", 27),
            keys(&["\"in_process\"", "\"parallel\""])
        );
    }

    #[test]
    fn resource_completion_lists_keys_then_fields_with_current_values() {
        let schema = schema();
        let labels = |text: &str, caret: usize| -> Vec<String> {
            completions(text, caret, Some(&schema))
                .into_iter()
                .map(|c| c.label)
                .collect()
        };
        assert_eq!(labels("{\"resources\": {", 15), keys(&["db"]));
        let fields = completions("{\"resources\": {\"db\": {", 22, Some(&schema));
        assert_eq!(
            fields
                .iter()
                .map(|c| (c.label.as_str(), c.detail.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("dsn", "string = \"memory://\""),
                ("pool_size", "integer = 2"),
                ("token", "string"),
            ]
        );
        // A secret's value is never offered.
        assert!(labels("{\"resources\": {\"db\": {\"token\": ", 32).is_empty());
    }
}
