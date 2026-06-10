//! PartitionKey — Single and Multi dimension partition key types.
use std::collections::HashMap;

use crate::errors::PartitionDefinitionError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

#[pyclass(name = "PartitionKey", frozen, from_py_object, module = "rivers._core")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PyPartitionKey {
    Single {
        key: Vec<String>,
    },
    Multi {
        keys: HashMap<String, Vec<String>>,
    },
    /// Internal/transport-only
    Set {
        keys: Vec<PyPartitionKey>,
    },
}

impl std::hash::Hash for PyPartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Single { key } => key.hash(state),
            Self::Multi { keys } => {
                let mut sorted: Vec<_> = keys.iter().collect();
                sorted.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
                for (k, v) in sorted {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Self::Set { keys } => {
                for k in keys {
                    k.hash(state);
                }
            }
        }
    }
}

#[pymethods]
impl PyPartitionKey {
    #[staticmethod]
    fn single(key: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(s) = key.extract::<String>() {
            Ok(Self::Single { key: vec![s] })
        } else if let Ok(mut v) = key.extract::<Vec<String>>() {
            if v.is_empty() {
                return Err(PartitionDefinitionError::new_err(
                    "partition key list must not be empty",
                ));
            }
            v.sort();
            Ok(Self::Single { key: v })
        } else {
            Err(PyTypeError::new_err("expected str or list[str]"))
        }
    }

    #[staticmethod]
    fn multi(keys: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut result = HashMap::new();
        for (k, v) in keys.iter() {
            let name: String = k.extract()?;
            if let Ok(s) = v.extract::<String>() {
                result.insert(name, vec![s]);
            } else if let Ok(mut list) = v.extract::<Vec<String>>() {
                if list.is_empty() {
                    return Err(PartitionDefinitionError::new_err(format!(
                        "partition key list for dimension '{}' must not be empty",
                        name
                    )));
                }
                list.sort();
                result.insert(name, list);
            } else {
                return Err(PyTypeError::new_err(
                    "expected str or list[str] for each dimension value",
                ));
            }
        }
        if result.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "PartitionKey dict must have at least one dimension",
            ));
        }
        Ok(Self::Multi { keys: result })
    }

    pub fn __repr__(&self) -> String {
        match self {
            Self::Single { key } => {
                if key.len() == 1 {
                    format!("PartitionKey({:?})", key[0])
                } else {
                    format!("PartitionKey({:?})", key)
                }
            }
            Self::Multi { keys } => {
                let mut sorted: Vec<_> = keys.iter().collect();
                sorted.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
                let pairs: Vec<String> = sorted
                    .iter()
                    .map(|(k, v)| {
                        if v.len() == 1 {
                            format!("{k:?}: {:?}", v[0])
                        } else {
                            format!("{k:?}: {v:?}")
                        }
                    })
                    .collect();
                format!("PartitionKey({{{}}})", pairs.join(", "))
            }
            Self::Set { keys } => {
                let members: Vec<String> = keys.iter().map(|k| k.__repr__()).collect();
                format!("PartitionKey.Set([{}])", members.join(", "))
            }
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_partition_key")?
            .unbind();
        let data = PyDict::new(py);
        match self {
            Self::Single { key } => {
                data.set_item("variant", "Single")?;
                data.set_item("key", key.clone())?;
            }
            Self::Multi { keys } => {
                data.set_item("variant", "Multi")?;
                data.set_item("keys", keys.clone())?;
            }
            Self::Set { keys } => {
                data.set_item("variant", "Set")?;
                let members: Vec<Py<PyPartitionKey>> = keys
                    .iter()
                    .map(|k| Py::new(py, k.clone()))
                    .collect::<PyResult<_>>()?;
                data.set_item("keys", members)?;
            }
        }
        let args = PyTuple::new(py, [data.into_any()])?;
        Ok((reconstruct, args.unbind().into_any()))
    }

    /// Serialize to JSON string for passing through CLI args / env vars.
    pub fn to_json(&self) -> String {
        let core: rivers_core::storage::PartitionKey = self.into();
        core.to_json()
    }

    /// Deserialize from JSON produced by `to_json()`.
    #[staticmethod]
    fn from_json(s: &str) -> PyResult<Self> {
        let core = rivers_core::storage::PartitionKey::from_json(s)
            .map_err(|e| PartitionDefinitionError::new_err(e.to_string()))?;
        Ok(Self::from(&core))
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match self {
            Self::Single { key } => {
                0u8.hash(&mut hasher);
                for k in key {
                    k.hash(&mut hasher);
                }
            }
            Self::Multi { keys } => {
                1u8.hash(&mut hasher);
                let mut sorted: Vec<_> = keys.iter().collect();
                sorted.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
                for (k, v) in sorted {
                    k.hash(&mut hasher);
                    for val in v {
                        val.hash(&mut hasher);
                    }
                }
            }
            Self::Set { keys } => {
                2u8.hash(&mut hasher);
                for k in keys {
                    k.hash(&mut hasher);
                }
            }
        }
        hasher.finish()
    }
}

impl PyPartitionKey {
    /// Expand a possibly-batched key into its individual single-valued members.
    /// `Single{key:[a,b]}` → `[Single{[a]}, Single{[b]}]`; `Multi` → the cartesian
    /// product of its per-dimension value lists, one member per combination. A
    /// single-valued key yields a one-element Vec. Used to fan a batched backfill
    /// run into per-partition contexts and materialization events.
    pub fn members(&self) -> Vec<PyPartitionKey> {
        let core: rivers_core::storage::PartitionKey = self.into();
        core.members().iter().map(PyPartitionKey::from).collect()
    }
}

impl From<&rivers_core::storage::PartitionKey> for PyPartitionKey {
    fn from(spk: &rivers_core::storage::PartitionKey) -> Self {
        match spk {
            rivers_core::storage::PartitionKey::Single { keys } => {
                let mut key = keys.clone();
                key.sort();
                Self::Single { key }
            }
            rivers_core::storage::PartitionKey::Multi { dims } => {
                let keys = dims
                    .iter()
                    .map(|(k, v)| {
                        let mut sorted_v = v.clone();
                        sorted_v.sort();
                        (k.clone(), sorted_v)
                    })
                    .collect();
                Self::Multi { keys }
            }
            rivers_core::storage::PartitionKey::Set { keys } => Self::Set {
                keys: keys.iter().map(PyPartitionKey::from).collect(),
            },
        }
    }
}

impl From<&PyPartitionKey> for rivers_core::storage::PartitionKey {
    fn from(pk: &PyPartitionKey) -> Self {
        match pk {
            PyPartitionKey::Single { key } => Self::Single { keys: key.clone() },
            PyPartitionKey::Multi { keys } => {
                // HashMap iteration order is seed-dependent — sort so the
                // converted key is deterministic.
                let mut dims: Vec<(String, Vec<String>)> =
                    keys.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                dims.sort_by(|a, b| a.0.cmp(&b.0));
                Self::Multi { dims }
            }
            PyPartitionKey::Set { keys } => Self::Set {
                keys: keys.iter().map(|k| k.into()).collect(),
            },
        }
    }
}
