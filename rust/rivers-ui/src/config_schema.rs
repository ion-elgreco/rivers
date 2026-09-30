//! What the editor knows about a JSON schema: the issues a parsed document
//! has against it, the required fields it lacks, the completions a caret
//! takes, the template it opens with and the payload it sends. The schema is
//! a launch document's, composed from config classes' `model_json_schema()`
//! text placed in it with [`inline`]. Two keywords of our own:
//! `"x-launch-template": "keys"` on a container lists its keys as empty
//! objects in the template instead of their fields' defaults, for sections
//! whose values must only be sent when typed; `"x-requires": {"k": v}` on a
//! property makes it an error unless the sibling `k` is set to `v`.

use serde_json::{Map, Value};

use crate::helpers::plural;
use crate::json_text::{Issue, Node, PathSeg, Span};

pub struct Schema {
    root: Value,
}

impl Schema {
    pub fn parse(json: &str) -> Option<Schema> {
        let root: Value = serde_json::from_str(json).ok()?;
        root.is_object().then_some(Schema { root })
    }

    pub fn root(&self) -> &Value {
        &self.root
    }

    /// `node` with its `$ref` chain followed. The keywords next to a `$ref`
    /// (pydantic puts `default` there) stay on the unresolved node.
    pub fn resolve<'a>(&'a self, mut node: &'a Value) -> &'a Value {
        for _ in 0..8 {
            let target = node
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|r| r.strip_prefix('#'))
                .and_then(|pointer| self.root.pointer(pointer));
            match target {
                Some(target) => node = target,
                None => return node,
            }
        }
        node
    }

    /// The schema of the value at `path`, keywords next to its `$ref` included.
    pub fn at<'a>(&'a self, path: &[PathSeg]) -> Option<&'a Value> {
        let mut node = &self.root;
        for seg in path {
            node = self.step(node, seg)?;
        }
        Some(node)
    }

    /// The resolved node and the resolved branches of its `anyOf`, `oneOf`
    /// and `allOf`.
    fn branches<'a>(&'a self, node: &'a Value) -> Vec<&'a Value> {
        let node = self.resolve(node);
        let mut out = vec![node];
        for key in ["anyOf", "oneOf", "allOf"] {
            if let Some(list) = node.get(key).and_then(Value::as_array) {
                out.extend(list.iter().map(|b| self.resolve(b)));
            }
        }
        out
    }

    fn step<'a>(&'a self, node: &'a Value, seg: &PathSeg) -> Option<&'a Value> {
        let branches = self.branches(node);
        match seg {
            PathSeg::Key(key) => branches
                .iter()
                .find_map(|b| b.get("properties")?.get(key))
                .or_else(|| {
                    branches
                        .iter()
                        .find_map(|b| b.get("additionalProperties").filter(|a| a.is_object()))
                }),
            PathSeg::Index(index) => branches
                .iter()
                .find_map(|b| b.get("prefixItems")?.get(*index))
                .or_else(|| {
                    branches
                        .iter()
                        .find_map(|b| b.get("items").filter(|i| i.is_object()))
                }),
        }
    }

    fn default_of<'a>(&'a self, node: &'a Value) -> Option<&'a Value> {
        node.get("default")
            .or_else(|| self.resolve(node).get("default"))
    }

    fn description_of<'a>(&'a self, node: &'a Value) -> Option<&'a str> {
        node.get("description")
            .or_else(|| self.resolve(node).get("description"))
            .and_then(Value::as_str)
    }

    /// The branch of `node` that describes an object with fields.
    fn object_branch<'a>(&'a self, node: &'a Value) -> Option<&'a Value> {
        self.branches(node)
            .into_iter()
            .find(|b| b.get("properties").is_some())
    }
}

/// Short type text: `integer`, `string | null`, `"fast" | "slow"`, a model
/// name.
pub fn type_label(schema: &Schema, node: &Value) -> String {
    let resolved = schema.resolve(node);
    if let Some(members) = resolved.get("enum").and_then(Value::as_array) {
        return members
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join(" | ");
    }
    if let Some(constant) = resolved.get("const") {
        return constant.to_string();
    }
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        return reference
            .rsplit('/')
            .next()
            .unwrap_or(reference)
            .to_string();
    }
    if is_container(node)
        && let Some(title) = resolved.get("title").and_then(Value::as_str)
    {
        return title.to_string();
    }
    match resolved.get("type") {
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
    for key in ["anyOf", "oneOf"] {
        if let Some(Value::Array(list)) = resolved.get(key) {
            return list
                .iter()
                .map(|b| type_label(schema, b))
                .collect::<Vec<_>>()
                .join(" | ");
        }
    }
    "any".to_string()
}

/// An object declared in place with named fields: a document section, or
/// a class schema inlined as one. A model is a `$ref`; a `dict` field has
/// no `properties`.
fn is_container(node: &Value) -> bool {
    node.get("$ref").is_none() && node.get("properties").is_some_and(Value::is_object)
}

/// `schema_json` placed at `path` (property names, unescaped) in a larger
/// schema: its `$defs` stay with it and its `#/$defs/...` references point
/// there. A secret (`writeOnly`) loses its `default`, which pydantic masks:
/// the mask must never be offered as a value.
pub fn inline(schema_json: &str, path: &[&str]) -> Option<Value> {
    let mut value: Value = serde_json::from_str(schema_json).ok()?;
    if !value.is_object() {
        return None;
    }
    let base: String = path
        .iter()
        .map(|seg| format!("/{}", seg.replace('~', "~0").replace('/', "~1")))
        .collect();
    rebase_refs(&mut value, &base);
    Some(value)
}

fn rebase_refs(value: &mut Value, base: &str) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get_mut("$ref") {
                let rebased = reference
                    .strip_prefix("#/$defs/")
                    .map(|rest| format!("#{base}/$defs/{rest}"));
                if let Some(rebased) = rebased {
                    *reference = rebased;
                }
            }
            if map.get("writeOnly") == Some(&Value::Bool(true)) {
                map.remove("default");
            }
            for child in map.values_mut() {
                rebase_refs(child, base);
            }
        }
        Value::Array(items) => {
            for item in items {
                rebase_refs(item, base);
            }
        }
        _ => {}
    }
}

/// The text an editor opens with: the defaults the schema holds, in the
/// objects that hold them. A container with named fields is listed even
/// without defaults, as its fields complete; one with neither is not, nor
/// is a model without a default.
pub fn template(schema: &Schema) -> Value {
    template_of(schema, &schema.root).unwrap_or_else(|| Value::Object(Map::new()))
}

fn template_of(schema: &Schema, node: &Value) -> Option<Value> {
    if node.get("writeOnly") == Some(&Value::Bool(true)) {
        return None;
    }
    if let Some(default) = schema.default_of(node) {
        return Some(default.clone());
    }
    if !is_container(node) {
        return None;
    }
    let props = node.get("properties")?.as_object()?;
    if node.get("x-launch-template").and_then(Value::as_str) == Some("keys") {
        if props.is_empty() {
            return None;
        }
        let keys = props
            .keys()
            .map(|key| (key.clone(), Value::Object(Map::new())))
            .collect();
        return Some(Value::Object(keys));
    }
    let children: Map<String, Value> = props
        .iter()
        .filter_map(|(name, prop)| Some((name.clone(), template_of(schema, prop)?)))
        .collect();
    let has_fields = !props.is_empty() && !props.values().any(is_container);
    (has_fields || !children.is_empty()).then_some(Value::Object(children))
}

/// `value` without the empty containers the template lists; `None` when
/// nothing is left. A field set to `{}` stays: that is a value.
pub fn compact(schema: &Schema, value: Value) -> Option<Value> {
    compact_at(&schema.root, value)
}

fn compact_at(node: &Value, value: Value) -> Option<Value> {
    let map = match value {
        Value::Object(map) => map,
        other => return Some(other),
    };
    let props = node.get("properties").and_then(Value::as_object);
    let out: Map<String, Value> = map
        .into_iter()
        .filter_map(|(key, child)| {
            let kept = match props.and_then(|p| p.get(&key)) {
                Some(prop) => compact_at(prop, child)?,
                None => child,
            };
            Some((key, kept))
        })
        .collect();
    if out.is_empty() && is_container(node) {
        None
    } else {
        Some(Value::Object(out))
    }
}

fn type_set(node: &Value) -> Vec<&str> {
    match node.get("type") {
        Some(Value::String(t)) => vec![t.as_str()],
        Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn type_matches(type_name: &str, node: &Node) -> bool {
    match (type_name, node) {
        ("integer", Node::Num { whole, .. }) => *whole,
        ("number", Node::Num { .. })
        | ("string", Node::Str { .. })
        | ("boolean", Node::Bool { .. })
        | ("null", Node::Null { .. })
        | ("object", Node::Object { .. })
        | ("array", Node::Array { .. }) => true,
        _ => false,
    }
}

/// Whether `node` has a JSON type the (resolved) branch names; a branch
/// without `type` fits anything.
fn type_fits(schema: &Schema, branch: &Value, node: &Node) -> bool {
    let types = type_set(branch);
    if !types.is_empty() {
        return types.iter().any(|t| type_matches(t, node));
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(list) = branch.get(key).and_then(Value::as_array) {
            return list
                .iter()
                .any(|b| type_fits(schema, schema.resolve(b), node));
        }
    }
    true
}

fn equals(node: &Node, value: &Value) -> bool {
    match node {
        Node::Str { value: s, .. } => value.as_str() == Some(s),
        Node::Num { value: n, .. } => value.as_f64() == Some(*n),
        Node::Bool { value: b, .. } => value.as_bool() == Some(*b),
        Node::Null { .. } => value.is_null(),
        _ => false,
    }
}

fn child_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

fn located(path: &str, text: String) -> String {
    if path.is_empty() {
        text
    } else {
        format!("{path}: {text}")
    }
}

/// The issues `node` has against the schema; `path` names it in messages.
pub fn validate(schema: &Schema, node: &Node, path: &str) -> Vec<Issue> {
    let mut issues = Vec::new();
    check(schema, &schema.root, node, path, &mut issues);
    issues
}

fn check(schema: &Schema, sch: &Value, node: &Node, path: &str, out: &mut Vec<Issue>) {
    if matches!(node, Node::Missing { .. }) {
        return;
    }
    // Labels come from the unresolved node so a `$ref` reads as its model name.
    let label = || type_label(schema, sch);
    let sch = schema.resolve(sch);
    let issue = |text: String| Issue {
        span: node.span(),
        message: located(path, text),
    };
    for key in ["anyOf", "oneOf"] {
        let Some(list) = sch.get(key).and_then(Value::as_array) else {
            continue;
        };
        // The branches the value's type fits are checked in full; the one
        // with the fewest issues speaks for the union.
        let results: Vec<Vec<Issue>> = list
            .iter()
            .map(|b| schema.resolve(b))
            .filter(|b| type_fits(schema, b, node))
            .map(|b| {
                let mut sub = Vec::new();
                check(schema, b, node, path, &mut sub);
                sub
            })
            .collect();
        match results.into_iter().min_by_key(Vec::len) {
            Some(fewest) => out.extend(fewest),
            None => out.push(issue(format!(
                "expected {}, got {}",
                label(),
                node.type_name()
            ))),
        }
        return;
    }
    if let Some(list) = sch.get("allOf").and_then(Value::as_array) {
        for branch in list {
            check(schema, branch, node, path, out);
        }
    }
    if let Some(members) = sch.get("enum").and_then(Value::as_array) {
        if !members.iter().any(|m| equals(node, m)) {
            let list = members
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            out.push(issue(format!("expected one of {list}")));
        }
        return;
    }
    if let Some(constant) = sch.get("const") {
        if !equals(node, constant) {
            out.push(issue(format!("expected {constant}")));
        }
        return;
    }
    let types = type_set(sch);
    if !types.is_empty() && !types.iter().any(|t| type_matches(t, node)) {
        out.push(issue(format!(
            "expected {}, got {}",
            label(),
            node.type_name()
        )));
        return;
    }
    match node {
        Node::Object { entries, .. } => {
            let props = sch.get("properties").and_then(Value::as_object);
            let additional = sch.get("additionalProperties");
            for entry in entries {
                let child = child_path(path, &entry.key);
                if let Some(prop) = props.and_then(|p| p.get(&entry.key)) {
                    let requires = prop.get("x-requires").and_then(Value::as_object);
                    for (sibling, wanted) in requires.into_iter().flatten() {
                        let set = entries
                            .iter()
                            .any(|e| &e.key == sibling && equals(&e.value, wanted));
                        if !set {
                            out.push(Issue {
                                span: entry.key_span,
                                message: located(
                                    path,
                                    format!("{} needs \"{sibling}\": {wanted}", entry.key),
                                ),
                            });
                        }
                    }
                    check(schema, prop, &entry.value, &child, out);
                } else if let Some(extra) = additional.filter(|a| a.is_object()) {
                    check(schema, extra, &entry.value, &child, out);
                } else if additional == Some(&Value::Bool(true))
                    || (props.is_none() && additional.is_none())
                {
                    // Free-form object.
                } else {
                    let mut names: Vec<&str> = props
                        .map(|p| p.keys().map(String::as_str).collect())
                        .unwrap_or_default();
                    names.sort_unstable();
                    let mut text = format!("unknown field '{}'", entry.key);
                    if !names.is_empty() {
                        text.push_str(&format!("; expected one of {}", names.join(", ")));
                    }
                    out.push(Issue {
                        span: entry.key_span,
                        message: located(path, text),
                    });
                }
            }
        }
        Node::Array { items, .. } => {
            let prefix = sch.get("prefixItems").and_then(Value::as_array);
            let each = sch.get("items").filter(|i| i.is_object());
            for (index, item) in items.iter().enumerate() {
                let child = format!("{path}[{index}]");
                if let Some(p) = prefix.and_then(|p| p.get(index)) {
                    check(schema, p, item, &child, out);
                } else if let Some(each) = each {
                    check(schema, each, item, &child, out);
                }
            }
            let count = items.len() as u64;
            if let Some(min) = sch.get("minItems").and_then(Value::as_u64)
                && count < min
            {
                out.push(issue(format!(
                    "expected at least {}",
                    plural(min, "item", "items")
                )));
            }
            if let Some(max) = sch.get("maxItems").and_then(Value::as_u64)
                && count > max
            {
                out.push(issue(format!(
                    "expected at most {}",
                    plural(max, "item", "items")
                )));
            }
        }
        Node::Num { value, .. } => {
            let bound = |key: &str| sch.get(key).filter(|b| b.is_number());
            if let Some(min) = bound("minimum")
                && *value < min.as_f64().unwrap_or(f64::NEG_INFINITY)
            {
                out.push(issue(format!("must be ≥ {min}")));
            }
            if let Some(max) = bound("maximum")
                && *value > max.as_f64().unwrap_or(f64::INFINITY)
            {
                out.push(issue(format!("must be ≤ {max}")));
            }
            if let Some(min) = bound("exclusiveMinimum")
                && *value <= min.as_f64().unwrap_or(f64::NEG_INFINITY)
            {
                out.push(issue(format!("must be > {min}")));
            }
            if let Some(max) = bound("exclusiveMaximum")
                && *value >= max.as_f64().unwrap_or(f64::INFINITY)
            {
                out.push(issue(format!("must be < {max}")));
            }
            if let Some(step) = bound("multipleOf")
                && let Some(step_value) = step.as_f64().filter(|s| *s > 0.0)
                && (value / step_value).fract().abs() > 1e-9
            {
                out.push(issue(format!("must be a multiple of {step}")));
            }
        }
        Node::Str { value, .. } => {
            let count = value.chars().count() as u64;
            if let Some(min) = sch.get("minLength").and_then(Value::as_u64)
                && count < min
            {
                out.push(issue(format!(
                    "must have at least {}",
                    plural(min, "character", "characters")
                )));
            }
            if let Some(max) = sch.get("maxLength").and_then(Value::as_u64)
                && count > max
            {
                out.push(issue(format!(
                    "must have at most {}",
                    plural(max, "character", "characters")
                )));
            }
        }
        _ => {}
    }
}

/// Dotted paths of the required fields that objects in `node` lack.
pub fn missing_required(schema: &Schema, node: &Node, path: &str) -> Vec<String> {
    let mut out = Vec::new();
    collect_missing(schema, &schema.root, node, path, &mut out);
    out
}

fn collect_missing(schema: &Schema, sch: &Value, node: &Node, path: &str, out: &mut Vec<String>) {
    let Node::Object { entries, .. } = node else {
        return;
    };
    let Some(obj) = schema.object_branch(sch) else {
        return;
    };
    let required: Vec<&str> = obj
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    for key in &required {
        if !entries.iter().any(|e| e.key == *key) {
            out.push(child_path(path, key));
        }
    }
    let props = obj.get("properties").and_then(Value::as_object);
    for entry in entries {
        if let Some(prop) = props.and_then(|p| p.get(&entry.key)) {
            collect_missing(
                schema,
                prop,
                &entry.value,
                &child_path(path, &entry.key),
                out,
            );
        }
    }
    // An absent container counts as empty: what it requires is still missing.
    let none = Node::Object {
        span: Span::at(0),
        entries: Vec::new(),
        closed: true,
    };
    for (name, prop) in props.into_iter().flatten() {
        if is_container(prop)
            && !required.iter().any(|r| r == name)
            && !entries.iter().any(|e| &e.key == name)
        {
            collect_missing(schema, prop, &none, &child_path(path, name), out);
        }
    }
}

/// One completion: `label` is shown, `insert` replaces the text at the
/// caret, `detail` documents it, and `caret` is where the caret lands
/// inside `insert`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub label: String,
    pub insert: String,
    pub detail: String,
    pub caret: usize,
}

fn caret_in(insert: &str) -> usize {
    if insert.ends_with("\"\"") || insert.ends_with("{}") || insert.ends_with("[]") {
        insert.len() - 1
    } else {
        insert.len()
    }
}

/// The fields of the object at `path` that `siblings` does not have, each
/// inserting `"name": <default or placeholder>`.
pub fn key_candidates(schema: &Schema, path: &[PathSeg], siblings: &[String]) -> Vec<Candidate> {
    let Some(obj) = schema.at(path).and_then(|n| schema.object_branch(n)) else {
        return Vec::new();
    };
    let Some(props) = obj.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    let required: Vec<&str> = obj
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut out: Vec<Candidate> = props
        .iter()
        .filter(|(name, _)| !siblings.contains(name))
        .map(|(name, prop)| {
            let insert = format!("\"{name}\": {}", placeholder_value(schema, prop));
            let mut detail = type_label(schema, prop);
            if let Some(default) = schema.default_of(prop) {
                detail.push_str(&format!(" = {default}"));
            } else if required.contains(&name.as_str()) {
                detail.push_str(" · required");
            }
            if let Some(description) = schema.description_of(prop) {
                detail.push_str(&format!(" — {description}"));
            }
            Candidate {
                label: name.clone(),
                caret: caret_in(&insert),
                insert,
                detail,
            }
        })
        .collect();
    out.sort_by(|a, b| a.label.cmp(&b.label));
    out
}

fn push_unique(out: &mut Vec<Candidate>, insert: String, detail: &str) {
    if out.iter().any(|c| c.insert == insert) {
        return;
    }
    out.push(Candidate {
        label: insert.clone(),
        caret: caret_in(&insert),
        insert,
        detail: detail.to_string(),
    });
}

/// The values the position at `path` takes: its default, enum members,
/// `true`/`false`, `null` when nullable, `{}`/`[]` for containers.
pub fn value_candidates(schema: &Schema, path: &[PathSeg]) -> Vec<Candidate> {
    let Some(node) = schema.at(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(default) = schema.default_of(node) {
        push_unique(&mut out, default.to_string(), "default");
    }
    for branch in schema.branches(node) {
        if let Some(members) = branch.get("enum").and_then(Value::as_array) {
            for member in members {
                push_unique(&mut out, member.to_string(), "");
            }
            continue;
        }
        if let Some(constant) = branch.get("const") {
            push_unique(&mut out, constant.to_string(), "");
            continue;
        }
        for type_name in type_set(branch) {
            match type_name {
                "boolean" => {
                    push_unique(&mut out, "true".to_string(), "");
                    push_unique(&mut out, "false".to_string(), "");
                }
                "null" => push_unique(&mut out, "null".to_string(), ""),
                "object" => push_unique(&mut out, "{}".to_string(), ""),
                "array" => push_unique(&mut out, "[]".to_string(), ""),
                _ => {}
            }
        }
    }
    out
}

/// The default of `node`, else an empty value of its type.
pub fn placeholder_value(schema: &Schema, node: &Value) -> Value {
    if let Some(default) = schema.default_of(node) {
        return default.clone();
    }
    let resolved = schema.resolve(node);
    if let Some(first) = resolved
        .get("enum")
        .and_then(Value::as_array)
        .and_then(|m| m.first())
    {
        return first.clone();
    }
    if let Some(constant) = resolved.get("const") {
        return constant.clone();
    }
    if let Some(type_name) = type_set(resolved).first() {
        return match *type_name {
            "string" => Value::String(String::new()),
            "integer" | "number" => Value::from(0),
            "boolean" => Value::Bool(false),
            "object" => Value::Object(Map::new()),
            "array" => Value::Array(Vec::new()),
            _ => Value::Null,
        };
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(first) = resolved
            .get(key)
            .and_then(Value::as_array)
            .and_then(|l| l.first())
        {
            return placeholder_value(schema, first);
        }
    }
    Value::Null
}

/// `text` with the required fields its objects lack added, each with its
/// default or a placeholder, pretty-printed; an absent container is added
/// when it requires something. `None` when `text` is not a JSON object.
pub fn scaffold_missing(text: &str, schema: &Schema) -> Option<String> {
    let mut root = if text.trim().is_empty() {
        Map::new()
    } else {
        match serde_json::from_str(text).ok()? {
            Value::Object(root) => root,
            _ => return None,
        }
    };
    fill_required(schema, &schema.root, &mut root);
    serde_json::to_string_pretty(&Value::Object(root)).ok()
}

fn fill_required(schema: &Schema, sch: &Value, fields: &mut Map<String, Value>) {
    let Some(obj) = schema.object_branch(sch) else {
        return;
    };
    let props = obj.get("properties").and_then(Value::as_object);
    if let Some(required) = obj.get("required").and_then(Value::as_array) {
        for key in required.iter().filter_map(Value::as_str) {
            if !fields.contains_key(key)
                && let Some(prop) = props.and_then(|p| p.get(key))
            {
                fields.insert(key.to_string(), placeholder_value(schema, prop));
            }
        }
    }
    for (key, value) in fields.iter_mut() {
        if let Some(prop) = props.and_then(|p| p.get(key))
            && let Value::Object(inner) = value
        {
            fill_required(schema, prop, inner);
        }
    }
    for (name, prop) in props.into_iter().flatten() {
        if is_container(prop) && !fields.contains_key(name) {
            let mut inner = Map::new();
            fill_required(schema, prop, &mut inner);
            if !inner.is_empty() {
                fields.insert(name.clone(), Value::Object(inner));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_text::{Span, parse};

    // Captured from pydantic 2 `model_json_schema()` (see the plan for the classes).
    const CFG: &str = r##"{"$defs":{"Inner":{"properties":{"host":{"default":"localhost","title":"Host","type":"string"},"port":{"default":5432,"maximum":65535,"minimum":1,"title":"Port","type":"integer"}},"title":"Inner","type":"object"},"Region":{"enum":["eu","us"],"title":"Region","type":"string"}},"description":"Doc of Cfg.","properties":{"threshold":{"default":0.5,"description":"Cut-off","maximum":1,"minimum":0,"title":"Threshold","type":"number"},"name":{"default":"x","maxLength":8,"minLength":1,"title":"Name","type":"string"},"region":{"anyOf":[{"type":"string"},{"type":"null"}],"default":null,"title":"Region"},"mode":{"default":"fast","enum":["fast","slow"],"title":"Mode","type":"string"},"reg":{"$ref":"#/$defs/Region","default":"eu"},"inner":{"$ref":"#/$defs/Inner","default":{"host":"localhost","port":5432}},"tags":{"items":{"type":"string"},"maxItems":3,"title":"Tags","type":"array"},"extra":{"additionalProperties":{"type":"integer"},"default":{},"title":"Extra","type":"object"},"when":{"anyOf":[{"format":"date-time","type":"string"},{"type":"null"}],"default":null,"title":"When"},"required_key":{"title":"Required Key","type":"string"},"ratio":{"anyOf":[{"type":"integer"},{"type":"number"}],"default":1,"title":"Ratio"}},"required":["required_key"],"title":"Cfg","type":"object"}"##;
    const SETTINGS: &str = r#"{"additionalProperties":false,"properties":{"api_key":{"title":"Api Key","type":"string"},"batch":{"default":10,"title":"Batch","type":"integer"}},"required":["api_key"],"title":"S","type":"object"}"#;
    const INGESTION: &str = r#"{"properties":{"source_system":{"default":"demo","title":"Source System","type":"string"},"batch_size":{"default":100,"title":"Batch Size","type":"integer"},"include_inactive":{"default":false,"title":"Include Inactive","type":"boolean"}},"title":"_IngestionSettings","type":"object"}"#;
    const PAIR: &str = r##"{"$defs":{"Inner":{"properties":{"host":{"default":"localhost","title":"Host","type":"string"},"port":{"default":5432,"maximum":65535,"minimum":1,"title":"Port","type":"integer"}},"title":"Inner","type":"object"}},"properties":{"pair":{"default":[1,"a"],"maxItems":2,"minItems":2,"prefixItems":[{"type":"integer"},{"type":"string"}],"title":"Pair","type":"array"},"maybe_inner":{"anyOf":[{"$ref":"#/$defs/Inner"},{"type":"null"}],"default":null},"anything":{"additionalProperties":true,"default":{},"title":"Anything","type":"object"}},"title":"Pair","type":"object"}"##;
    const NESTED: &str = r##"{"$defs":{"Creds":{"properties":{"user":{"title":"User","type":"string"},"pw":{"default":"","title":"Pw","type":"string"}},"required":["user"],"title":"Creds","type":"object"}},"properties":{"creds":{"$ref":"#/$defs/Creds"}},"required":["creds"],"title":"Outer","type":"object"}"##;

    fn schema(json: &str) -> Schema {
        Schema::parse(json).expect("a schema")
    }

    /// `(message, span)` of each issue of `doc` against `schema_json`.
    fn issues(schema_json: &str, doc: &str) -> Vec<(String, Span)> {
        let parsed = parse(doc);
        assert_eq!(parsed.error, None, "{doc}");
        validate(&schema(schema_json), parsed.root.as_ref().unwrap(), "")
            .into_iter()
            .map(|i| (i.message, i.span))
            .collect()
    }

    fn messages(schema_json: &str, doc: &str) -> Vec<String> {
        issues(schema_json, doc)
            .into_iter()
            .map(|(m, _)| m)
            .collect()
    }

    fn missing(schema_json: &str, doc: &str) -> Vec<String> {
        let parsed = parse(doc);
        missing_required(&schema(schema_json), parsed.root.as_ref().unwrap(), "")
    }

    fn key(name: &str) -> PathSeg {
        PathSeg::Key(name.to_string())
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_valid_document_has_no_issues() {
        let doc = r#"{"threshold": 0.5, "name": "abc", "region": null, "mode": "slow", "reg": "us",
            "inner": {"host": "h", "port": 1.0}, "tags": ["a", "b"], "extra": {"k": 1},
            "when": "2024-01-01T00:00:00Z", "required_key": "r", "ratio": 1.5}"#;
        assert_eq!(messages(CFG, doc), Vec::<String>::new());
        assert_eq!(messages(CFG, "{}"), Vec::<String>::new());
        assert_eq!(
            messages(
                PAIR,
                r#"{"pair": [2, "b"], "maybe_inner": {"host": "x"}, "anything": {"x": [1]}}"#
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn unknown_fields_are_errors_at_their_key() {
        assert_eq!(
            issues(CFG, r#"{"thresholds": 1}"#),
            vec![(
                "unknown field 'thresholds'; expected one of extra, inner, mode, name, ratio, reg, region, required_key, tags, threshold, when".to_string(),
                Span { start: 1, end: 13 }
            )]
        );
        assert_eq!(
            messages(CFG, r#"{"inner": {"hots": 1}}"#),
            vec!["inner: unknown field 'hots'; expected one of host, port"]
        );
        assert_eq!(
            messages(SETTINGS, r#"{"foo": 1}"#),
            vec!["unknown field 'foo'; expected one of api_key, batch"]
        );
        // A dict field takes any key; its values keep their type.
        assert_eq!(
            messages(CFG, r#"{"extra": {"k": 1}}"#),
            Vec::<String>::new()
        );
        assert_eq!(
            messages(CFG, r#"{"extra": {"k": "x"}}"#),
            vec!["extra.k: expected integer, got string"]
        );
        assert_eq!(
            messages(PAIR, r#"{"anything": {"x": 1}}"#),
            Vec::<String>::new()
        );
    }

    #[test]
    fn type_mismatches_name_the_expected_type() {
        assert_eq!(
            messages(CFG, r#"{"threshold": "x"}"#),
            vec!["threshold: expected number, got string"]
        );
        assert_eq!(
            messages(CFG, r#"{"name": 1}"#),
            vec!["name: expected string, got number"]
        );
        assert_eq!(
            messages(CFG, r#"{"inner": 1}"#),
            vec!["inner: expected Inner, got number"]
        );
        assert_eq!(
            messages(CFG, r#"{"tags": "a"}"#),
            vec!["tags: expected array, got string"]
        );
        assert_eq!(
            messages(CFG, r#"{"inner": {"port": 1.5}}"#),
            vec!["inner.port: expected integer, got number"]
        );
        assert_eq!(
            messages(CFG, r#"{"tags": [1]}"#),
            vec!["tags[0]: expected string, got number"]
        );
        assert_eq!(
            messages(PAIR, r#"{"pair": [1, 2]}"#),
            vec!["pair[1]: expected string, got number"]
        );
    }

    #[test]
    fn unions_take_any_branch_and_report_the_fitting_one() {
        assert_eq!(messages(CFG, r#"{"region": "x"}"#), Vec::<String>::new());
        assert_eq!(
            messages(CFG, r#"{"region": 1}"#),
            vec!["region: expected string | null, got number"]
        );
        assert_eq!(messages(CFG, r#"{"ratio": 2}"#), Vec::<String>::new());
        assert_eq!(
            messages(CFG, r#"{"ratio": "x"}"#),
            vec!["ratio: expected integer | number, got string"]
        );
        assert_eq!(
            messages(PAIR, r#"{"maybe_inner": {"hots": 1}}"#),
            vec!["maybe_inner: unknown field 'hots'; expected one of host, port"]
        );
        assert_eq!(
            messages(PAIR, r#"{"maybe_inner": 5}"#),
            vec!["maybe_inner: expected Inner | null, got number"]
        );
    }

    #[test]
    fn enum_members_are_checked_before_the_type() {
        assert_eq!(
            messages(CFG, r#"{"mode": "slowly"}"#),
            vec![r#"mode: expected one of "fast", "slow""#]
        );
        assert_eq!(
            messages(CFG, r#"{"mode": 1}"#),
            vec![r#"mode: expected one of "fast", "slow""#]
        );
        assert_eq!(
            messages(CFG, r#"{"reg": "uk"}"#),
            vec![r#"reg: expected one of "eu", "us""#]
        );
        assert_eq!(messages(CFG, r#"{"reg": "us"}"#), Vec::<String>::new());
    }

    #[test]
    fn bounds_are_checked() {
        assert_eq!(
            messages(CFG, r#"{"threshold": 2}"#),
            vec!["threshold: must be ≤ 1"]
        );
        assert_eq!(
            messages(CFG, r#"{"threshold": -0.1}"#),
            vec!["threshold: must be ≥ 0"]
        );
        assert_eq!(
            messages(CFG, r#"{"inner": {"port": 0}}"#),
            vec!["inner.port: must be ≥ 1"]
        );
        assert_eq!(
            messages(CFG, r#"{"name": ""}"#),
            vec!["name: must have at least 1 character"]
        );
        assert_eq!(
            messages(CFG, r#"{"name": "abcdefghi"}"#),
            vec!["name: must have at most 8 characters"]
        );
        assert_eq!(
            messages(CFG, r#"{"tags": ["a", "b", "c", "d"]}"#),
            vec!["tags: expected at most 3 items"]
        );
        assert_eq!(
            messages(PAIR, r#"{"pair": [1]}"#),
            vec!["pair: expected at least 2 items"]
        );
        let bounded = r#"{"properties":{"n":{"exclusiveMinimum":0,"exclusiveMaximum":1,"multipleOf":0.25,"type":"number"}},"type":"object"}"#;
        assert_eq!(messages(bounded, r#"{"n": 0}"#), vec!["n: must be > 0"]);
        assert_eq!(messages(bounded, r#"{"n": 1}"#), vec!["n: must be < 1"]);
        assert_eq!(
            messages(bounded, r#"{"n": 0.3}"#),
            vec!["n: must be a multiple of 0.25"]
        );
        assert_eq!(messages(bounded, r#"{"n": 0.75}"#), Vec::<String>::new());
    }

    #[test]
    fn required_fields_the_document_lacks_are_listed() {
        assert_eq!(missing(CFG, "{}"), strings(&["required_key"]));
        assert_eq!(
            missing(CFG, r#"{"required_key": "x"}"#),
            Vec::<String>::new()
        );
        assert_eq!(missing(SETTINGS, "{}"), strings(&["api_key"]));
        assert_eq!(missing(NESTED, "{}"), strings(&["creds"]));
        assert_eq!(
            missing(NESTED, r#"{"creds": {"pw": "x"}}"#),
            strings(&["creds.user"])
        );
        assert_eq!(
            missing(NESTED, r#"{"creds": {"user": "x"}}"#),
            Vec::<String>::new()
        );
    }

    #[test]
    fn key_candidates_skip_present_fields_and_carry_details() {
        let ingestion = schema(INGESTION);
        let out = key_candidates(&ingestion, &[], &strings(&["batch_size"]));
        assert_eq!(
            out,
            vec![
                Candidate {
                    label: "include_inactive".to_string(),
                    insert: "\"include_inactive\": false".to_string(),
                    detail: "boolean = false".to_string(),
                    caret: 25,
                },
                Candidate {
                    label: "source_system".to_string(),
                    insert: "\"source_system\": \"demo\"".to_string(),
                    detail: "string = \"demo\"".to_string(),
                    caret: 23,
                },
            ]
        );

        let cfg = schema(CFG);
        let all = key_candidates(&cfg, &[], &[]);
        let by_label = |label: &str| all.iter().find(|c| c.label == label).cloned().unwrap();
        let required = by_label("required_key");
        assert_eq!(required.insert, "\"required_key\": \"\"");
        assert_eq!(required.caret, required.insert.len() - 1);
        assert_eq!(required.detail, "string · required");
        assert_eq!(by_label("threshold").detail, "number = 0.5 — Cut-off");
        assert_eq!(by_label("mode").detail, "\"fast\" | \"slow\" = \"fast\"");
        assert_eq!(
            by_label("inner").insert,
            "\"inner\": {\"host\":\"localhost\",\"port\":5432}"
        );
        assert_eq!(
            by_label("inner").detail,
            "Inner = {\"host\":\"localhost\",\"port\":5432}"
        );
        assert_eq!(by_label("tags").insert, "\"tags\": []");
        assert_eq!(by_label("tags").caret, 9);

        let nested: Vec<String> = key_candidates(&cfg, &[key("inner")], &[])
            .into_iter()
            .map(|c| c.label)
            .collect();
        assert_eq!(nested, strings(&["host", "port"]));
        let through_union: Vec<String> = key_candidates(&schema(PAIR), &[key("maybe_inner")], &[])
            .into_iter()
            .map(|c| c.label)
            .collect();
        assert_eq!(through_union, strings(&["host", "port"]));
        assert!(key_candidates(&cfg, &[key("nope")], &[]).is_empty());
        assert!(key_candidates(&cfg, &[key("tags")], &[]).is_empty());
    }

    #[test]
    fn value_candidates_list_defaults_members_and_literals() {
        let cfg = schema(CFG);
        let inserts = |path: &[PathSeg]| -> Vec<(String, String)> {
            value_candidates(&cfg, path)
                .into_iter()
                .map(|c| (c.insert, c.detail))
                .collect()
        };
        let pair = |a: &str, b: &str| (a.to_string(), b.to_string());
        assert_eq!(
            inserts(&[key("mode")]),
            vec![pair("\"fast\"", "default"), pair("\"slow\"", "")]
        );
        assert_eq!(
            inserts(&[key("reg")]),
            vec![pair("\"eu\"", "default"), pair("\"us\"", "")]
        );
        assert_eq!(inserts(&[key("region")]), vec![pair("null", "default")]);
        assert_eq!(
            inserts(&[key("inner")]),
            vec![
                pair("{\"host\":\"localhost\",\"port\":5432}", "default"),
                pair("{}", "")
            ]
        );
        assert_eq!(inserts(&[key("tags")]), vec![pair("[]", "")]);
        assert_eq!(
            inserts(&[key("required_key")]),
            Vec::<(String, String)>::new()
        );
        assert_eq!(
            inserts(&[key("tags"), PathSeg::Index(0)]),
            Vec::<(String, String)>::new()
        );

        let flags = value_candidates(&schema(INGESTION), &[key("include_inactive")]);
        assert_eq!(
            flags.iter().map(|c| c.insert.as_str()).collect::<Vec<_>>(),
            vec!["false", "true"]
        );
        let container = value_candidates(&cfg, &[key("inner")]);
        assert_eq!(container[1].caret, 1);
    }

    #[test]
    fn placeholders_follow_the_default_then_the_type() {
        let cfg = schema(CFG);
        let at = |name: &str| placeholder_value(&cfg, cfg.at(&[key(name)]).unwrap()).to_string();
        assert_eq!(at("threshold"), "0.5");
        assert_eq!(at("required_key"), "\"\"");
        assert_eq!(at("tags"), "[]");
        assert_eq!(at("when"), "null");
        assert_eq!(at("reg"), "\"eu\"");
        let bare = schema(
            r##"{"properties":{"n":{"type":"integer"},"o":{"anyOf":[{"type":"string"},{"type":"null"}]},"m":{"$ref":"#/$defs/M"}},"$defs":{"M":{"properties":{"a":{"type":"integer"}},"type":"object"}},"type":"object"}"##,
        );
        let at = |name: &str| placeholder_value(&bare, bare.at(&[key(name)]).unwrap()).to_string();
        assert_eq!(at("n"), "0");
        assert_eq!(at("o"), "\"\"");
        assert_eq!(at("m"), "{}");
    }

    /// A launch document schema over the class fixtures, as the editor
    /// composes it.
    fn document() -> Schema {
        let mut assets = Map::new();
        for (key, class) in [
            ("cfg", CFG),
            ("api", SETTINGS),
            ("outer", NESTED),
            ("ingest", INGESTION),
        ] {
            let at = [
                "properties",
                "assets",
                "properties",
                key,
                "properties",
                "config",
            ];
            assets.insert(
                key.to_string(),
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "config": inline(class, &at).unwrap(),
                        "metadata": {"type": "object", "properties": {}, "additionalProperties": {"type": "string"}}
                    },
                    "additionalProperties": false
                }),
            );
        }
        Schema {
            root: serde_json::json!({
                "type": "object",
                "properties": {"assets": {"type": "object", "properties": assets, "additionalProperties": false}},
                "additionalProperties": false
            }),
        }
    }

    #[test]
    fn inline_keeps_a_class_schemas_models_reachable() {
        let placed = inline(CFG, &["properties", "a/b"]).unwrap();
        assert_eq!(
            placed.pointer("/properties/inner/$ref").unwrap(),
            "#/properties/a~1b/$defs/Inner"
        );
        let doc = Schema {
            root: serde_json::json!({"properties": {"a/b": placed}}),
        };
        assert_eq!(
            doc.at(&[key("a/b"), key("inner"), key("port")])
                .and_then(|n| n.get("maximum")),
            Some(&serde_json::json!(65535))
        );
        assert_eq!(
            type_label(&doc, doc.at(&[key("a/b"), key("inner")]).unwrap()),
            "Inner"
        );
        assert_eq!(type_label(&doc, doc.at(&[key("a/b")]).unwrap()), "Cfg");
        assert_eq!(inline("[1]", &["x"]), None);
        // A secret's masked default is dropped.
        let secret = r#"{"properties":{"token":{"default":"**********","format":"password","type":"string","writeOnly":true}},"type":"object"}"#;
        assert_eq!(
            inline(secret, &["s"])
                .unwrap()
                .pointer("/properties/token/default"),
            None
        );
    }

    #[test]
    fn template_holds_defaults_and_the_containers_with_fields() {
        let doc = document();
        assert_eq!(
            template(&doc),
            serde_json::json!({"assets": {
                "api": {"config": {"batch": 10}},
                "cfg": {"config": {
                    "threshold": 0.5, "name": "x", "region": null, "mode": "fast", "reg": "eu",
                    "inner": {"host": "localhost", "port": 5432}, "extra": {}, "when": null, "ratio": 1
                }},
                "ingest": {"config": {"source_system": "demo", "batch_size": 100, "include_inactive": false}},
                "outer": {"config": {}}
            }})
        );
        // A `keys` container lists its keys without their values; a secret is
        // never pre-filled.
        let keyed = Schema {
            root: serde_json::json!({"type": "object", "properties": {"resources": {
                "type": "object", "x-launch-template": "keys", "properties": {
                    "db": {"type": "object", "properties": {"dsn": {"type": "string", "default": "x"}}},
                    "cache": {"type": "object", "properties": {"ttl": {"type": "integer", "default": 5}}}
                }
            }, "secret": {"type": "string", "writeOnly": true, "default": "***"}}}),
        };
        assert_eq!(
            template(&keyed),
            serde_json::json!({"resources": {"cache": {}, "db": {}}})
        );
        // Nothing known anywhere: an empty document.
        let bare = Schema {
            root: serde_json::json!({"type": "object", "properties": {"assets": {"type": "object", "properties": {
                "plain": {"type": "object", "properties": {"metadata": {"type": "object", "properties": {}}}}
            }}}}),
        };
        assert_eq!(template(&bare), serde_json::json!({}));
    }

    #[test]
    fn compact_drops_empty_containers_but_keeps_empty_values() {
        let doc = document();
        let text = serde_json::json!({"assets": {
            "cfg": {"config": {"extra": {}, "inner": {}}, "metadata": {}},
            "api": {"config": {}},
            "outer": {}
        }});
        assert_eq!(
            compact(&doc, text),
            Some(serde_json::json!({"assets": {"cfg": {"config": {"extra": {}, "inner": {}}}}}))
        );
        assert_eq!(
            compact(&doc, serde_json::json!({"assets": {"api": {"config": {}}}})),
            None
        );
        assert_eq!(compact(&doc, serde_json::json!({})), None);
    }

    #[test]
    fn a_property_can_require_a_sibling_value() {
        let doc = Schema {
            root: serde_json::json!({"type": "object", "properties": {
                "executor": {"type": "string", "enum": ["in_process", "parallel"]},
                "max_workers": {"type": "integer", "minimum": 1, "x-requires": {"executor": "parallel"}}
            }}),
        };
        let at = |text: &str| {
            let parsed = parse(text);
            validate(&doc, parsed.root.as_ref().unwrap(), "run")
                .into_iter()
                .map(|i| (i.message, i.span))
                .collect::<Vec<_>>()
        };
        assert_eq!(at(r#"{"executor": "parallel", "max_workers": 2}"#), vec![]);
        assert_eq!(
            at(r#"{"max_workers": 2}"#),
            vec![(
                "run: max_workers needs \"executor\": \"parallel\"".to_string(),
                Span { start: 1, end: 14 }
            )]
        );
        assert_eq!(
            at(r#"{"executor": "in_process", "max_workers": 0}"#),
            vec![
                (
                    "run: max_workers needs \"executor\": \"parallel\"".to_string(),
                    Span { start: 27, end: 40 }
                ),
                (
                    "run.max_workers: must be ≥ 1".to_string(),
                    Span { start: 42, end: 43 }
                ),
            ]
        );
    }

    #[test]
    fn missing_fields_are_found_through_absent_containers() {
        let doc = document();
        let at = |text: &str| {
            let parsed = parse(text);
            missing_required(&doc, parsed.root.as_ref().unwrap(), "")
        };
        assert_eq!(
            at("{}"),
            strings(&[
                "assets.api.config.api_key",
                "assets.cfg.config.required_key",
                "assets.outer.config.creds",
            ])
        );
        assert_eq!(
            at(
                r#"{"assets": {"api": {"config": {"api_key": "k"}}, "cfg": {"config": {"required_key": "r"}}, "outer": {"config": {"creds": {}}}}}"#
            ),
            strings(&["assets.outer.config.creds.user"])
        );
    }

    #[test]
    fn scaffold_adds_required_fields_and_the_containers_they_need() {
        let doc = document();
        let text = "{\n  \"assets\": {\n    \"cfg\": {\"config\": {\"threshold\": 0.9}},\n    \"outer\": {\"config\": {\"creds\": {}}}\n  }\n}";
        let out = scaffold_missing(text, &doc).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap(),
            serde_json::json!({"assets": {
                "cfg": {"config": {"threshold": 0.9, "required_key": ""}},
                "api": {"config": {"api_key": ""}},
                "outer": {"config": {"creds": {"user": ""}}}
            }})
        );
        // Blank text scaffolds from nothing; a container with nothing
        // required stays absent.
        let out = scaffold_missing("", &doc).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap(),
            serde_json::json!({"assets": {
                "api": {"config": {"api_key": ""}},
                "cfg": {"config": {"required_key": ""}},
                "outer": {"config": {"creds": {"user": ""}}}
            }})
        );
        assert_eq!(scaffold_missing("{\"assets\": ", &doc), None);
        assert_eq!(scaffold_missing("[1]", &doc), None);
    }

    #[test]
    fn type_labels_read_like_the_annotation() {
        let cfg = schema(CFG);
        let label = |name: &str| type_label(&cfg, cfg.at(&[key(name)]).unwrap());
        assert_eq!(label("region"), "string | null");
        assert_eq!(label("required_key"), "string");
        assert_eq!(label("reg"), "\"eu\" | \"us\"");
        assert_eq!(label("inner"), "Inner");
        assert_eq!(label("mode"), "\"fast\" | \"slow\"");
        assert_eq!(label("ratio"), "integer | number");
        assert_eq!(label("tags"), "array");
        let pair = schema(PAIR);
        assert_eq!(
            type_label(&pair, pair.at(&[key("maybe_inner")]).unwrap()),
            "Inner | null"
        );
        assert_eq!(type_label(&pair, &serde_json::json!({})), "any");
    }

    #[test]
    fn paths_step_through_properties_items_and_unions() {
        let cfg = schema(CFG);
        assert_eq!(cfg.at(&[]), Some(cfg.root()));
        assert_eq!(
            cfg.at(&[key("inner"), key("port")])
                .and_then(|n| n.get("maximum")),
            Some(&serde_json::json!(65535))
        );
        assert_eq!(
            cfg.at(&[key("tags"), PathSeg::Index(3)]),
            Some(&serde_json::json!({"type": "string"}))
        );
        assert_eq!(
            cfg.at(&[key("extra"), key("anything")]),
            Some(&serde_json::json!({"type": "integer"}))
        );
        assert_eq!(cfg.at(&[key("nope")]), None);
        let pair = schema(PAIR);
        assert_eq!(
            pair.at(&[key("pair"), PathSeg::Index(1)]),
            Some(&serde_json::json!({"type": "string"}))
        );
        assert_eq!(
            pair.at(&[key("maybe_inner"), key("host")])
                .and_then(|n| n.get("default")),
            Some(&serde_json::json!("localhost"))
        );
    }
}
