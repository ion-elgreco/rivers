use anyhow::Result;
use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

/// Partition key stored alongside events and asset partition records.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PartitionKey {
    /// Single-dimension key (e.g. `"2024-01-01"` or `["a", "b"]`).
    Single { keys: Vec<String> },
    /// Multi-dimension key (e.g. `{"date": ["2024-01-01"], "region": ["us"]}`).
    Multi { dims: Vec<(String, Vec<String>)> },
    /// Explicit set of concrete keys (each member a single Single/Multi key, never nested).
    Set { keys: Vec<PartitionKey> },
}

impl PartitionKey {
    /// Expand a possibly-batched key into its individual single-valued members.
    pub fn members(&self) -> Vec<PartitionKey> {
        self.members_preview(usize::MAX)
    }

    /// Number of members this key expands to, without building them.
    pub fn member_count(&self) -> usize {
        match self {
            Self::Single { keys } => keys.len(),
            Self::Multi { dims } => dims
                .iter()
                .map(|(_, vs)| vs.len())
                .fold(1usize, |a, n| a.saturating_mul(n)),
            Self::Set { keys } => keys.iter().map(Self::member_count).sum(),
        }
    }

    /// The first `limit` members in `members()` order, without building the rest.
    pub fn members_preview(&self, limit: usize) -> Vec<PartitionKey> {
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::Single { keys } => keys
                .iter()
                .take(limit)
                .map(|k| Self::Single {
                    keys: vec![k.clone()],
                })
                .collect(),
            Self::Multi { dims } => {
                let mut sorted = dims.clone();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                let mut combos: Vec<Vec<(String, Vec<String>)>> = vec![Vec::new()];
                for (name, vals) in &sorted {
                    combos = combos
                        .into_iter()
                        .flat_map(|combo| {
                            vals.iter().map(move |v| {
                                let mut c = combo.clone();
                                c.push((name.clone(), vec![v.clone()]));
                                c
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
            Self::Set { keys } => {
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

    /// Canonical key-as-string encoding for display and string matching.
    pub fn to_display(&self) -> String {
        match self {
            Self::Single { keys } => keys.join(","),
            Self::Multi { dims } => {
                let mut sorted = dims.clone();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                sorted
                    .iter()
                    .map(|(dim, vals)| format!("{}={}", dim, vals.join(",")))
                    .collect::<Vec<_>>()
                    .join("|")
            }
            Self::Set { keys } => keys
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

    /// Serialize to JSON for CLI args / K8s CRD fields.
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
            (Self::Multi { dims: a }, Self::Multi { dims: b }) => {
                if a.len() != b.len() {
                    return false;
                }
                let mut a_sorted = a.clone();
                let mut b_sorted = b.clone();
                a_sorted.sort_by(|x, y| x.0.cmp(&y.0));
                b_sorted.sort_by(|x, y| x.0.cmp(&y.0));
                a_sorted == b_sorted
            }
            (Self::Set { keys: a }, Self::Set { keys: b }) => a == b,
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
            Self::Multi { dims } => {
                let mut sorted = dims.clone();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                sorted.hash(state);
            }
            Self::Set { keys } => keys.hash(state),
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
            _ => Err(SurrealError::internal(format!(
                "unknown PartitionKey variant: {variant}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PartitionKey;

    #[test]
    fn single_displays_bare_value() {
        let pk = PartitionKey::Single {
            keys: vec!["2024-01-01".into()],
        };
        assert_eq!(pk.to_display(), "2024-01-01");
    }

    #[test]
    fn batched_single_joins_values_with_comma() {
        let pk = PartitionKey::Single {
            keys: vec!["a".into(), "b".into()],
        };
        assert_eq!(pk.to_display(), "a,b");
    }

    #[test]
    fn multi_sorts_dims_and_pipe_joins() {
        let pk = PartitionKey::Multi {
            dims: vec![
                ("region".into(), vec!["us".into(), "eu".into()]),
                ("date".into(), vec!["2024-01-01".into()]),
            ],
        };
        assert_eq!(pk.to_display(), "date=2024-01-01|region=us,eu");
    }

    #[test]
    fn set_joins_member_displays() {
        let pk = PartitionKey::Set {
            keys: vec![
                PartitionKey::Single {
                    keys: vec!["a".into()],
                },
                PartitionKey::Multi {
                    dims: vec![("d".into(), vec!["x".into()])],
                },
            ],
        };
        assert_eq!(pk.to_display(), "a, d=x");
    }
}
