//! PartitionKeyRange — defines a range of partition keys for backfills.
//!
//! Single-dimension: `PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-03-31")`
//! Multi-dimension: `PartitionKeyRange.multi({"date": ("2024-01-01", "2024-03-31"), "region": ["us", "eu"]})`
use std::collections::{HashMap, HashSet};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use rivers_core::util::parse_key_datetime;

use crate::errors::{ExecutionError, PartitionDefinitionError};

use super::PyPartitionKey;
use super::definition::{PartitionsDefinition, cartesian_product};

/// Def-aware `[from, to]` membership for one single-dimension key value:
/// positional for Static (keys aren't necessarily lexicographic),
/// chronological via `fmt` for TimeWindow (custom formats aren't either),
/// plain string comparison when no definition is available.
fn key_in_range(
    key: &str,
    from_key: &str,
    to_key: &str,
    def: Option<&PartitionsDefinition>,
) -> bool {
    match def {
        Some(PartitionsDefinition::Static { keys }) => {
            match (
                keys.get_index_of(from_key),
                keys.get_index_of(to_key),
                keys.get_index_of(key),
            ) {
                (Some(f), Some(t), Some(p)) => p >= f && p <= t,
                _ => false,
            }
        }
        Some(PartitionsDefinition::TimeWindow { fmt, .. }) => {
            match (
                parse_key_datetime(key, fmt),
                parse_key_datetime(from_key, fmt),
                parse_key_datetime(to_key, fmt),
            ) {
                (Ok(k), Ok(f), Ok(t)) => k >= f && k <= t,
                _ => false,
            }
        }
        _ => key >= from_key && key <= to_key,
    }
}

/// Validate `[from, to]` endpoints against one single-dimension definition:
/// membership (Static) / format (TimeWindow) plus def-order. Returns a plain
/// message so resolve-time and validate-time callers wrap it in their own
/// error types. Dynamic defs carry no static key knowledge — nothing to check.
pub(crate) fn validate_single_dim_range(
    def: &PartitionsDefinition,
    from_key: &str,
    to_key: &str,
    dim: Option<&str>,
) -> Result<(), String> {
    let suffix = dim
        .map(|d| format!(" for dimension '{d}'"))
        .unwrap_or_default();
    match def {
        PartitionsDefinition::Static { keys } => {
            let pos = |k: &str| {
                keys.get_index_of(k)
                    .ok_or_else(|| format!("Range endpoint '{k}' is not a partition key{suffix}"))
            };
            let from_pos = pos(from_key)?;
            let to_pos = pos(to_key)?;
            if from_pos > to_pos {
                return Err(format!(
                    "from_key '{from_key}' is after to_key '{to_key}'{suffix}"
                ));
            }
            Ok(())
        }
        PartitionsDefinition::TimeWindow { fmt, .. } => {
            let parse = |k: &str| {
                parse_key_datetime(k, fmt).map_err(|_| {
                    format!("Range endpoint '{k}' does not match the partition format '{fmt}'{suffix}")
                })
            };
            let from_dt = parse(from_key)?;
            let to_dt = parse(to_key)?;
            if from_dt > to_dt {
                return Err(format!(
                    "from_key '{from_key}' is after to_key '{to_key}'{suffix}"
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Resolve `[from, to]` against one single-dimension definition using its
/// own ordering (see [`key_in_range`]). Errors on endpoints that aren't
/// members (Static) or don't parse (TimeWindow), and on inverted ranges —
/// ordering belongs to the definition, so this is where it's checked, not
/// in the range constructors.
fn resolve_single_dim_range(
    def: &PartitionsDefinition,
    from_key: &str,
    to_key: &str,
    dim: Option<&str>,
) -> PyResult<Vec<String>> {
    validate_single_dim_range(def, from_key, to_key, dim).map_err(ExecutionError::new_err)?;
    match def {
        PartitionsDefinition::Static { keys } => {
            let from_pos = keys.get_index_of(from_key).expect("validated above");
            let to_pos = keys.get_index_of(to_key).expect("validated above");
            Ok((from_pos..=to_pos)
                .filter_map(|i| keys.get_at(i).cloned())
                .collect())
        }
        PartitionsDefinition::TimeWindow { fmt, .. } => {
            let from_dt = parse_key_datetime(from_key, fmt).expect("validated above");
            let to_dt = parse_key_datetime(to_key, fmt).expect("validated above");
            Ok(def
                .enumerate_single_dim_keys()?
                .into_iter()
                .filter(|k| {
                    parse_key_datetime(k, fmt)
                        .map(|dt| dt >= from_dt && dt <= to_dt)
                        .unwrap_or(false)
                })
                .collect())
        }
        // Dynamic (storage-backed): the def's enumerator raises the precise
        // "needs storage" error.
        _ => {
            let keys = def.enumerate_single_dim_keys()?;
            Ok(keys
                .into_iter()
                .filter(|k| k.as_str() >= from_key && k.as_str() <= to_key)
                .collect())
        }
    }
}

/// Per-dimension selection: either a (from, to) range or explicit key list.
#[derive(Clone, Debug, PartialEq)]
pub enum DimensionSelection {
    Range { from_key: String, to_key: String },
    Keys(HashSet<String>),
}

impl DimensionSelection {
    /// Check whether a single key string falls within this selection,
    /// using the dimension's definition ordering when available.
    pub fn contains_key(&self, key: &str, def: Option<&PartitionsDefinition>) -> bool {
        match self {
            Self::Range { from_key, to_key } => key_in_range(key, from_key, to_key, def),
            Self::Keys(allowed) => allowed.contains(key),
        }
    }
}

/// A range of partition keys for backfill operations.
///
/// Constructed via `PartitionKeyRange.single(...)` or `PartitionKeyRange.multi(...)`.
/// Resolved into concrete `PartitionKey`s at backfill time.
#[pyclass(
    name = "PartitionKeyRange",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, PartialEq)]
pub struct PyPartitionKeyRange {
    pub(crate) inner: PartitionKeyRangeInner,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PartitionKeyRangeInner {
    Single {
        from_key: String,
        to_key: String,
    },
    Multi {
        dimensions: HashMap<String, DimensionSelection>,
    },
}

impl PyPartitionKeyRange {
    /// Check whether a partition key falls within this range, using the
    /// definition's ordering when provided (see [`key_in_range`]) — static
    /// keys and custom time formats are not necessarily lexicographic.
    pub fn contains(&self, key: &PyPartitionKey, def: Option<&PartitionsDefinition>) -> bool {
        match (&self.inner, key) {
            (
                PartitionKeyRangeInner::Single { from_key, to_key },
                PyPartitionKey::Single { key: key_parts },
            ) => {
                let k = match key_parts.first() {
                    Some(k) => k.as_str(),
                    None => return false,
                };
                key_in_range(k, from_key, to_key, def)
            }
            (PartitionKeyRangeInner::Multi { dimensions }, PyPartitionKey::Multi { keys }) => {
                let dim_defs = match def {
                    Some(PartitionsDefinition::Multi { dimensions }) => Some(dimensions),
                    _ => None,
                };
                dimensions.iter().all(|(dim_name, sel)| {
                    let dim_def = dim_defs
                        .and_then(|dims| dims.iter().find(|(n, _)| n == dim_name).map(|(_, d)| d));
                    keys.get(dim_name)
                        .and_then(|v| v.first())
                        .is_some_and(|k| sel.contains_key(k, dim_def))
                })
            }
            _ => false,
        }
    }

    /// Validate this range's endpoints against the definition it will be
    /// matched against — membership/format and def-order. Returns a plain
    /// message; callers wrap it in their error type.
    pub fn validate_against(&self, def: &PartitionsDefinition) -> Result<(), String> {
        match &self.inner {
            PartitionKeyRangeInner::Single { from_key, to_key } => {
                validate_single_dim_range(def, from_key, to_key, None)
            }
            PartitionKeyRangeInner::Multi { dimensions } => {
                let PartitionsDefinition::Multi {
                    dimensions: dim_defs,
                } = def
                else {
                    // Shape mismatches are reported by the mapping/def checks.
                    return Ok(());
                };
                for (name, sel) in dimensions {
                    if let DimensionSelection::Range { from_key, to_key } = sel
                        && let Some((_, dd)) = dim_defs.iter().find(|(n, _)| n == name)
                    {
                        validate_single_dim_range(dd, from_key, to_key, Some(name))?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Resolve this range into concrete partition keys using the given
    /// definition's ordering (positional for Static, chronological for
    /// TimeWindow — see [`resolve_single_dim_range`]).
    pub fn resolve(&self, parts_def: &PartitionsDefinition) -> PyResult<Vec<PyPartitionKey>> {
        match &self.inner {
            PartitionKeyRangeInner::Single { from_key, to_key } => {
                let filtered: Vec<PyPartitionKey> =
                    resolve_single_dim_range(parts_def, from_key, to_key, None)?
                        .into_iter()
                        .map(|k| PyPartitionKey::Single { key: vec![k] })
                        .collect();
                if filtered.is_empty() {
                    return Err(ExecutionError::new_err(format!(
                        "No partition keys found in range [{from_key}, {to_key}]"
                    )));
                }
                Ok(filtered)
            }
            PartitionKeyRangeInner::Multi {
                dimensions: dim_selections,
            } => {
                let dim_defs = match parts_def {
                    PartitionsDefinition::Multi { dimensions } => dimensions,
                    _ => {
                        return Err(ExecutionError::new_err(
                            "PartitionKeyRange.multi() requires a Multi-partitioned asset",
                        ));
                    }
                };

                let mut resolved_dims: Vec<(String, Vec<String>)> = Vec::new();
                for (dim_name, dim_def) in dim_defs {
                    let dim_keys = if let Some(sel) = dim_selections.get(dim_name) {
                        match sel {
                            DimensionSelection::Range { from_key, to_key } => {
                                let keys = resolve_single_dim_range(
                                    dim_def,
                                    from_key,
                                    to_key,
                                    Some(dim_name),
                                )?;
                                if keys.is_empty() {
                                    return Err(ExecutionError::new_err(format!(
                                        "No keys in range [{from_key}, {to_key}] for dimension '{dim_name}'"
                                    )));
                                }
                                keys
                            }
                            DimensionSelection::Keys(keys) => {
                                for k in keys {
                                    let single = PyPartitionKey::Single {
                                        key: vec![k.clone()],
                                    };
                                    if !dim_def.validate_partition_key(&single)? {
                                        return Err(ExecutionError::new_err(format!(
                                            "'{k}' is not a partition key of dimension '{dim_name}'"
                                        )));
                                    }
                                }
                                keys.iter().cloned().collect()
                            }
                        }
                    } else {
                        // Omitted dimension: include all keys
                        dim_def.enumerate_single_dim_keys()?
                    };
                    resolved_dims.push((dim_name.clone(), dim_keys));
                }

                let combos = cartesian_product(&resolved_dims);
                if combos.is_empty() {
                    return Err(ExecutionError::new_err(
                        "Partition range resolved to zero keys",
                    ));
                }
                Ok(combos
                    .into_iter()
                    .map(|dim_map| PyPartitionKey::Multi {
                        keys: dim_map
                            .into_iter()
                            .map(|(name, val)| (name, vec![val]))
                            .collect(),
                    })
                    .collect())
            }
        }
    }
}

#[pymethods]
impl PyPartitionKeyRange {
    /// Create a single-dimension range.
    ///
    /// Endpoint ordering is checked at resolve/match time against the
    /// partition definition (positional for Static, chronological for
    /// TimeWindow) — lexicographic order of the raw strings is meaningless
    /// for non-ISO formats and unordered static keys.
    ///
    /// ```python
    /// PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-03-31")
    /// ```
    #[staticmethod]
    fn single(from_key: String, to_key: String) -> Self {
        Self {
            inner: PartitionKeyRangeInner::Single { from_key, to_key },
        }
    }

    /// Create a multi-dimension range from a dict.
    ///
    /// Each value is either:
    /// - a `(from, to)` tuple for a range
    /// - a `list[str]` for explicit keys
    ///
    /// ```python
    /// PartitionKeyRange.multi({
    ///     "date": ("2024-01-01", "2024-03-31"),
    ///     "region": ["us", "eu"],
    /// })
    /// ```
    #[staticmethod]
    fn multi(dimensions: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut result = HashMap::new();
        for (k, v) in dimensions.iter() {
            let name: String = k.extract()?;
            if let Ok((from, to)) = v.extract::<(String, String)>() {
                result.insert(
                    name,
                    DimensionSelection::Range {
                        from_key: from,
                        to_key: to,
                    },
                );
            } else if let Ok(keys) = v.extract::<Vec<String>>() {
                if keys.is_empty() {
                    return Err(PartitionDefinitionError::new_err(format!(
                        "key list for dimension '{name}' must not be empty"
                    )));
                }
                result.insert(name, DimensionSelection::Keys(keys.into_iter().collect()));
            } else {
                return Err(PyTypeError::new_err(format!(
                    "dimension '{name}': expected (str, str) tuple or list[str], got {}",
                    v.get_type().name()?
                )));
            }
        }
        if result.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "dimensions dict must not be empty",
            ));
        }
        Ok(Self {
            inner: PartitionKeyRangeInner::Multi { dimensions: result },
        })
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            PartitionKeyRangeInner::Single { from_key, to_key } => {
                format!("PartitionKeyRange.single(from_key={from_key:?}, to_key={to_key:?})")
            }
            PartitionKeyRangeInner::Multi { dimensions } => {
                let dims: Vec<String> = dimensions
                    .iter()
                    .map(|(name, sel)| match sel {
                        DimensionSelection::Range { from_key, to_key } => {
                            format!("{name:?}: ({from_key:?}, {to_key:?})")
                        }
                        DimensionSelection::Keys(keys) => {
                            format!("{name:?}: {keys:?}")
                        }
                    })
                    .collect();
                format!("PartitionKeyRange.multi({{{}}})", dims.join(", "))
            }
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    /// Structural, order-independent hash consistent with `__eq__` — the
    /// Multi variant's HashMap/HashSet iterate in seed-dependent order, so
    /// contents are sorted before hashing.
    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match &self.inner {
            PartitionKeyRangeInner::Single { from_key, to_key } => {
                0u8.hash(&mut hasher);
                from_key.hash(&mut hasher);
                to_key.hash(&mut hasher);
            }
            PartitionKeyRangeInner::Multi { dimensions } => {
                1u8.hash(&mut hasher);
                let mut dims: Vec<_> = dimensions.iter().collect();
                dims.sort_by(|a, b| a.0.cmp(b.0));
                for (name, sel) in dims {
                    name.hash(&mut hasher);
                    match sel {
                        DimensionSelection::Range { from_key, to_key } => {
                            0u8.hash(&mut hasher);
                            from_key.hash(&mut hasher);
                            to_key.hash(&mut hasher);
                        }
                        DimensionSelection::Keys(keys) => {
                            1u8.hash(&mut hasher);
                            let mut sorted: Vec<&String> = keys.iter().collect();
                            sorted.sort();
                            sorted.hash(&mut hasher);
                        }
                    }
                }
            }
        }
        hasher.finish()
    }
}
