use std::borrow::Cow;

use anyhow::Result;
use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

use super::key_item::{self, KeyItem};

/// Partition key stored alongside events and asset partition records.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PartitionKey {
    /// Single-dimension key (e.g. `"2024-01-01"` or `["a", "b"]`).
    Single { keys: Vec<String> },
    /// Multi-dimension key (e.g. `{"date": ["2024-01-01"], "region": ["us"]}`).
    Multi { dims: Vec<(String, Vec<String>)> },
    /// Explicit set of concrete keys (each member a single Single/Multi key, never nested).
    Set { keys: Vec<PartitionKey> },
    /// `Single` whose values can be spans of time windows. The planner of a
    /// backfill writes it, as the key of a run, and a rerun of that run
    /// copies it; [`Self::expanded`] gives the `Single`.
    SingleItems { items: Vec<KeyItem> },
    /// `Multi` whose dimensions can hold spans of time windows.
    MultiItems { dims: Vec<(String, Vec<KeyItem>)> },
}

/// The values of one dimension of a key, in order.
#[derive(Clone, Copy)]
enum Values<'a> {
    Plain(&'a [String]),
    Items(&'a [KeyItem]),
}

impl Values<'_> {
    fn len(self) -> usize {
        match self {
            Self::Plain(values) => values.len(),
            Self::Items(items) => key_item::count(items),
        }
    }

    /// The values from place `skip` on, at most `take` of them.
    fn range(self, skip: usize, take: usize) -> Vec<String> {
        match self {
            Self::Plain(values) => values.iter().skip(skip).take(take).cloned().collect(),
            Self::Items(items) => key_item::values(items, skip, take),
        }
    }

    fn display(self) -> String {
        match self {
            Self::Plain(values) => values.join(","),
            Self::Items(items) => key_item::display(items),
        }
    }
}

/// What a key holds: the values of its one dimension, the values of its
/// named dimensions sorted by name, or other keys.
enum Shape<'a> {
    One(Values<'a>),
    Named(Vec<(&'a str, Values<'a>)>),
    Set(&'a [PartitionKey]),
}

/// `dims` sorted by name.
fn by_name<T>(dims: &[(String, T)]) -> Vec<&(String, T)> {
    let mut sorted: Vec<&(String, T)> = dims.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    sorted
}

impl PartitionKey {
    fn shape(&self) -> Shape<'_> {
        match self {
            Self::Single { keys } => Shape::One(Values::Plain(keys)),
            Self::SingleItems { items } => Shape::One(Values::Items(items)),
            Self::Multi { dims } => Shape::Named(
                by_name(dims)
                    .into_iter()
                    .map(|(name, values)| (name.as_str(), Values::Plain(values)))
                    .collect(),
            ),
            Self::MultiItems { dims } => Shape::Named(
                by_name(dims)
                    .into_iter()
                    .map(|(name, items)| (name.as_str(), Values::Items(items)))
                    .collect(),
            ),
            Self::Set { keys } => Shape::Set(keys),
        }
    }

    /// Expand a possibly-batched key into its individual single-valued members.
    pub fn members(&self) -> Vec<PartitionKey> {
        self.members_preview(usize::MAX)
    }

    /// Number of members this key expands to, without building them.
    pub fn member_count(&self) -> usize {
        match self.shape() {
            Shape::One(values) => values.len(),
            Shape::Named(dims) => dims
                .iter()
                .map(|(_, values)| values.len())
                .fold(1usize, |a, n| a.saturating_mul(n)),
            Shape::Set(keys) => keys.iter().map(Self::member_count).sum(),
        }
    }

    /// The first `limit` members in `members()` order, without building the rest.
    pub fn members_preview(&self, limit: usize) -> Vec<PartitionKey> {
        if limit == 0 {
            return Vec::new();
        }
        match self.shape() {
            Shape::One(values) => values
                .range(0, limit)
                .into_iter()
                .map(|value| Self::Single { keys: vec![value] })
                .collect(),
            Shape::Named(dims) => {
                let mut combos: Vec<Vec<(String, Vec<String>)>> = vec![Vec::new()];
                for (name, values) in dims {
                    // The first `limit` members hold no value past that place.
                    let values = values.range(0, limit);
                    combos = combos
                        .into_iter()
                        .flat_map(|combo| {
                            values.iter().map(move |value| {
                                let mut combo = combo.clone();
                                combo.push((name.to_string(), vec![value.clone()]));
                                combo
                            })
                        })
                        .take(limit)
                        .collect();
                }
                combos
                    .into_iter()
                    .map(|dims| Self::Multi { dims })
                    .collect()
            }
            Shape::Set(keys) => {
                let mut out = Vec::new();
                for k in keys {
                    for m in k.members_preview(limit - out.len()) {
                        out.push(m);
                        if out.len() >= limit {
                            return out;
                        }
                    }
                }
                out
            }
        }
    }

    /// The member at `index` in `members()` order, without the members before it.
    pub fn member_at(&self, index: usize) -> Option<PartitionKey> {
        match self.shape() {
            Shape::One(values) => values
                .range(index, 1)
                .pop()
                .map(|value| Self::Single { keys: vec![value] }),
            Shape::Named(dims) => {
                if index >= self.member_count() {
                    return None;
                }

                // The last dimension changes with each member.
                let mut rest = index;
                let mut member: Vec<(String, Vec<String>)> = Vec::with_capacity(dims.len());
                for (name, values) in dims.into_iter().rev() {
                    let len = values.len();
                    member.push((name.to_string(), values.range(rest % len, 1)));
                    rest /= len;
                }
                member.reverse();
                Some(Self::Multi { dims: member })
            }
            Shape::Set(keys) => {
                let mut rest = index;
                for key in keys {
                    let count = key.member_count();
                    if rest < count {
                        return key.member_at(rest);
                    }
                    rest -= count;
                }
                None
            }
        }
    }

    /// The key with each span written out as its values: a `Single`, a
    /// `Multi`, or a `Set` of those. A key without spans is not copied.
    pub fn expanded(&self) -> Cow<'_, PartitionKey> {
        match self {
            Self::SingleItems { items } => Cow::Owned(Self::Single {
                keys: key_item::values(items, 0, usize::MAX),
            }),
            Self::MultiItems { dims } => Cow::Owned(Self::Multi {
                dims: dims
                    .iter()
                    .map(|(name, items)| (name.clone(), key_item::values(items, 0, usize::MAX)))
                    .collect(),
            }),
            Self::Set { keys } if keys.iter().any(Self::has_items) => Cow::Owned(Self::Set {
                keys: keys.iter().map(|key| key.expanded().into_owned()).collect(),
            }),
            Self::Single { .. } | Self::Multi { .. } | Self::Set { .. } => Cow::Borrowed(self),
        }
    }

    /// Whether the key, or a key that it holds, is written with items.
    fn has_items(&self) -> bool {
        match self {
            Self::SingleItems { .. } | Self::MultiItems { .. } => true,
            Self::Set { keys } => keys.iter().any(Self::has_items),
            Self::Single { .. } | Self::Multi { .. } => false,
        }
    }

    /// Canonical key-as-string encoding for display and string matching. A
    /// span shows as `first..last`.
    pub fn to_display(&self) -> String {
        match self.shape() {
            Shape::One(values) => values.display(),
            Shape::Named(dims) => dims
                .iter()
                .map(|(name, values)| format!("{name}={}", values.display()))
                .collect::<Vec<_>>()
                .join("|"),
            Shape::Set(keys) => keys
                .iter()
                .map(|k| k.to_display())
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    /// The first display-syntax separator found in `key`, if any.
    pub fn reserved_display_char(key: &str) -> Option<char> {
        ['|', ','].into_iter().find(|&c| key.contains(c))
    }

    /// Inverse of [`Self::to_display`] for point lookups.
    pub fn display_candidates(display: &str) -> Vec<PartitionKey> {
        let mut out = vec![PartitionKey::Single {
            keys: vec![display.to_string()],
        }];
        if display.contains('=') {
            let mut dims: Vec<(String, Vec<String>)> = Vec::new();
            let mut ok = true;
            for part in display.split('|') {
                match part.split_once('=') {
                    Some((name, vals)) if !name.is_empty() => {
                        dims.push((
                            name.to_string(),
                            vals.split(',').map(str::to_string).collect(),
                        ));
                    }
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok && !dims.is_empty() {
                out.push(PartitionKey::Multi { dims });
            }
        }
        out
    }

    /// Serialize to JSON for CLI args / K8s CRD fields. The JSON has no form
    /// for a span: a key with spans gives the JSON of its expanded key.
    pub fn to_json(&self) -> String {
        match self {
            Self::Single { keys } => serde_json::json!({"single": keys}).to_string(),
            Self::Multi { dims } => {
                let map: std::collections::BTreeMap<&str, &Vec<String>> =
                    dims.iter().map(|(k, v)| (k.as_str(), v)).collect();
                serde_json::json!({"multi": map}).to_string()
            }
            Self::Set { keys } => {
                let members: Vec<serde_json::Value> = keys
                    .iter()
                    .filter_map(|k| serde_json::from_str(&k.to_json()).ok())
                    .collect();
                serde_json::json!({ "set": members }).to_string()
            }
            Self::SingleItems { .. } | Self::MultiItems { .. } => self.expanded().to_json(),
        }
    }

    /// Deserialize from JSON produced by `to_json()`.
    pub fn from_json(s: &str) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| anyhow::anyhow!("invalid partition key JSON: {e}"))?;
        if let Some(keys) = v.get("single") {
            let keys: Vec<String> = serde_json::from_value(keys.clone())
                .map_err(|e| anyhow::anyhow!("invalid single partition key: {e}"))?;
            Ok(Self::Single { keys })
        } else if let Some(multi) = v.get("multi") {
            let map: std::collections::BTreeMap<String, Vec<String>> =
                serde_json::from_value(multi.clone())
                    .map_err(|e| anyhow::anyhow!("invalid multi partition key: {e}"))?;
            Ok(Self::Multi {
                dims: map.into_iter().collect(),
            })
        } else if let Some(set) = v.get("set") {
            let arr = set
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid set partition key: expected array"))?;
            let keys = arr
                .iter()
                .map(|m| Self::from_json(&m.to_string()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Self::Set { keys })
        } else {
            anyhow::bail!("partition key JSON must have 'single', 'multi', or 'set' key")
        }
    }
}

impl PartialEq for PartitionKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Single { keys: a }, Self::Single { keys: b }) => a == b,
            (Self::Multi { dims: a }, Self::Multi { dims: b }) => by_name(a) == by_name(b),
            (Self::Set { keys: a }, Self::Set { keys: b }) => a == b,
            (Self::SingleItems { items: a }, Self::SingleItems { items: b }) => a == b,
            (Self::MultiItems { dims: a }, Self::MultiItems { dims: b }) => {
                by_name(a) == by_name(b)
            }
            _ => false,
        }
    }
}

impl Eq for PartitionKey {}

impl std::hash::Hash for PartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Single { keys } => keys.hash(state),
            Self::Multi { dims } => by_name(dims).hash(state),
            Self::Set { keys } => keys.hash(state),
            Self::SingleItems { items } => items.hash(state),
            Self::MultiItems { dims } => by_name(dims).hash(state),
        }
    }
}

impl SurrealValue for PartitionKey {
    fn kind_of() -> Kind {
        <std::collections::HashMap<String, Value>>::kind_of()
    }

    fn into_value(self) -> Value {
        let mut map = std::collections::BTreeMap::new();
        match self {
            Self::Single { keys } => {
                map.insert("variant".to_string(), "Single".to_string().into_value());
                map.insert("keys".to_string(), keys.into_value());
            }
            Self::Multi { mut dims } => {
                dims.sort_by(|a, b| a.0.cmp(&b.0));
                map.insert("variant".to_string(), "Multi".to_string().into_value());
                map.insert("dims".to_string(), dims.into_value());
            }
            Self::Set { keys } => {
                map.insert("variant".to_string(), "Set".to_string().into_value());
                map.insert("keys".to_string(), keys.into_value());
            }
            Self::SingleItems { items } => {
                map.insert(
                    "variant".to_string(),
                    "SingleItems".to_string().into_value(),
                );
                map.insert("items".to_string(), items.into_value());
            }
            Self::MultiItems { mut dims } => {
                dims.sort_by(|a, b| a.0.cmp(&b.0));
                map.insert("variant".to_string(), "MultiItems".to_string().into_value());
                map.insert("dims".to_string(), dims.into_value());
            }
        }
        Value::Object(map.into())
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        if let Ok(s) = String::from_value(value.clone()) {
            return Ok(Self::Single { keys: vec![s] });
        }

        let map = <std::collections::BTreeMap<String, Value>>::from_value(value)?;
        let variant = map
            .get("variant")
            .and_then(|v| String::from_value(v.clone()).ok())
            .unwrap_or_default();
        match variant.as_str() {
            "Single" => {
                let keys = map
                    .get("keys")
                    .map(|v| Vec::<String>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Single { keys })
            }
            "Multi" => {
                let dims = map
                    .get("dims")
                    .map(|v| Vec::<(String, Vec<String>)>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Multi { dims })
            }
            "Set" => {
                let keys = map
                    .get("keys")
                    .map(|v| Vec::<PartitionKey>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Set { keys })
            }
            "SingleItems" => {
                let items = map
                    .get("items")
                    .map(|v| Vec::<KeyItem>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::SingleItems { items })
            }
            "MultiItems" => {
                let dims = map
                    .get("dims")
                    .map(|v| Vec::<(String, Vec<KeyItem>)>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::MultiItems { dims })
            }
            _ => Err(SurrealError::internal(format!(
                "unknown PartitionKey variant: {variant}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
