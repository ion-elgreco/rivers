//! PartitionKeyRange — defines a range of partition keys for backfills.
//!
//! Single-dimension: `PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-03-31")`
//! Multi-dimension: `PartitionKeyRange.multi({"date": ("2024-01-01", "2024-03-31"), "region": ["us", "eu"]})`
use std::collections::{HashMap, HashSet};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::{ExecutionError, PartitionDefinitionError};

use super::PyPartitionKey;
use super::definition::{PartitionsDefinition, cartesian_product};

/// Per-dimension selection: either a (from, to) range or explicit key list.
#[derive(Clone, Debug, PartialEq)]
pub enum DimensionSelection {
    Range { from_key: String, to_key: String },
    Keys(HashSet<String>),
}

impl DimensionSelection {
    /// Check whether a single key string falls within this selection.
    pub fn contains_key(&self, key: &str) -> bool {
        match self {
            Self::Range { from_key, to_key } => key >= from_key.as_str() && key <= to_key.as_str(),
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
    /// Check whether a partition key falls within this range.
    ///
    /// When a `PartitionsDefinition` is provided and is `Static`, uses positional
    /// ordering (index in the definition's key list) instead of string comparison.
    /// This is necessary because static partition keys are not necessarily
    /// lexicographically ordered. For TimeWindow/Dynamic partitions (or when no
    /// definition is provided), string comparison is correct.
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
                // Static defs: use positional ordering (keys may not be lexicographic)
                if let Some(PartitionsDefinition::Static { keys }) = def {
                    let from_pos = keys.get_index_of(from_key.as_str());
                    let to_pos = keys.get_index_of(to_key.as_str());
                    let key_pos = keys.get_index_of(k);
                    match (from_pos, to_pos, key_pos) {
                        (Some(f), Some(t), Some(p)) => p >= f && p <= t,
                        _ => false,
                    }
                } else {
                    // TimeWindow/Dynamic: string comparison is correct
                    k >= from_key.as_str() && k <= to_key.as_str()
                }
            }
            (PartitionKeyRangeInner::Multi { dimensions }, PyPartitionKey::Multi { keys }) => {
                dimensions.iter().all(|(dim_name, sel)| {
                    keys.get(dim_name)
                        .and_then(|v| v.first())
                        .is_some_and(|k| sel.contains_key(k))
                })
            }
            _ => false,
        }
    }

    /// Resolve this range into concrete partition keys using the given definition.
    pub fn resolve(&self, parts_def: &PartitionsDefinition) -> PyResult<Vec<PyPartitionKey>> {
        match &self.inner {
            PartitionKeyRangeInner::Single { from_key, to_key } => {
                let all_keys = parts_def.get_partition_keys()?;
                let filtered: Vec<PyPartitionKey> = all_keys
                    .into_iter()
                    .filter(|pk| {
                        if let PyPartitionKey::Single { key } = pk {
                            if let Some(k) = key.first() {
                                k.as_str() >= from_key.as_str() && k.as_str() <= to_key.as_str()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    })
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
                                let keys: Vec<String> = dim_def
                                    .enumerate_single_dim_keys()?
                                    .into_iter()
                                    .filter(|k| {
                                        k.as_str() >= from_key.as_str()
                                            && k.as_str() <= to_key.as_str()
                                    })
                                    .collect();
                                if keys.is_empty() {
                                    return Err(ExecutionError::new_err(format!(
                                        "No keys in range [{from_key}, {to_key}] for dimension '{dim_name}'"
                                    )));
                                }
                                keys
                            }
                            DimensionSelection::Keys(keys) => keys.iter().cloned().collect(),
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
    /// ```python
    /// PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-03-31")
    /// ```
    #[staticmethod]
    fn single(from_key: String, to_key: String) -> PyResult<Self> {
        if from_key > to_key {
            return Err(PartitionDefinitionError::new_err(format!(
                "from_key '{from_key}' must be <= to_key '{to_key}'"
            )));
        }
        Ok(Self {
            inner: PartitionKeyRangeInner::Single { from_key, to_key },
        })
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
                if from > to {
                    return Err(PartitionDefinitionError::new_err(format!(
                        "from_key '{from}' must be <= to_key '{to}' for dimension '{name}'"
                    )));
                }
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

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", self).hash(&mut hasher);
        hasher.finish()
    }
}
