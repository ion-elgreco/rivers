use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::assets::action::PyAssetAction;
use crate::assets::dep_def::DepDef;
use crate::assets::io_handler::IOHandler;
use crate::errors::AssetDefinitionError;
use crate::partitions::PartitionsDefRef;
use crate::partitions::mapping::{PartitionMapping, PartitionMappingDict};

use super::deps::io_handler_eq;

/// A newtype for `Vec<String>` that accepts `str | list[str]` from Python.
#[derive(Clone, Debug, Default)]
pub struct Kinds(pub Vec<String>);

impl std::ops::Deref for Kinds {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromPyObject<'_, '_> for Kinds {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(Kinds(vec![s]))
        } else if let Ok(v) = ob.extract::<Vec<String>>() {
            Ok(Kinds(v))
        } else {
            Err(AssetDefinitionError::new_err(
                "kinds must be a string or list of strings",
            ))
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for Kinds {
    type Target = pyo3::types::PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        pyo3::types::PyList::new(py, &self.0)
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &Kinds {
    type Target = pyo3::types::PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        pyo3::types::PyList::new(py, &self.0)
    }
}

/// Normalize `pool` and `pool_slots` from Python arguments into `Vec<(String, u32)>`.
///
/// Accepts:
/// - `pool`: `str | list[str] | None`
/// - `pool_slots`: `int | dict[str, int] | None` (default 1)
pub fn normalize_pool(
    pool: Option<&Bound<'_, PyAny>>,
    pool_slots: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<(String, u32)>> {
    let pool_obj = match pool {
        Some(p) if !p.is_none() => p,
        _ => return Ok(vec![]),
    };

    let pool_keys: Vec<String> = if let Ok(s) = pool_obj.extract::<String>() {
        vec![s]
    } else if let Ok(v) = pool_obj.extract::<Vec<String>>() {
        if v.is_empty() {
            return Ok(vec![]);
        }
        v
    } else {
        return Err(AssetDefinitionError::new_err(
            "pool must be a string or list of strings",
        ));
    };

    for key in &pool_keys {
        if key.is_empty() {
            return Err(AssetDefinitionError::new_err(
                "pool key must be a non-empty string",
            ));
        }
        // The implicit per-asset pools: a user pool with the prefix would
        // acquire exclusive whole-asset semantics on another asset's pool.
        if key.starts_with(rivers_core::storage::ASSET_POOL_PREFIX) {
            return Err(AssetDefinitionError::new_err(format!(
                "pool '{key}': the '__asset__:' prefix is reserved for the \
                 implicit per-asset pools"
            )));
        }
    }

    let slots_map: HashMap<String, u32> = match pool_slots {
        Some(ps) if !ps.is_none() => {
            if let Ok(n) = ps.extract::<u32>() {
                pool_keys.iter().map(|k| (k.clone(), n)).collect()
            } else if let Ok(dict) = ps.cast_exact::<PyDict>() {
                let mut map = HashMap::new();
                for (k, v) in dict.iter() {
                    let key: String = k.extract()?;
                    let val: u32 = v.extract().map_err(|_| {
                        AssetDefinitionError::new_err(
                            "pool_slots dict values must be positive integers",
                        )
                    })?;
                    if !pool_keys.contains(&key) {
                        return Err(AssetDefinitionError::new_err(format!(
                            "pool_slots key '{}' not in pool list {:?}",
                            key, pool_keys
                        )));
                    }
                    map.insert(key, val);
                }
                for key in &pool_keys {
                    map.entry(key.clone()).or_insert(1);
                }
                map
            } else {
                return Err(AssetDefinitionError::new_err(
                    "pool_slots must be an int or dict[str, int]",
                ));
            }
        }
        _ => pool_keys.iter().map(|k| (k.clone(), 1)).collect(),
    };

    Ok(pool_keys
        .into_iter()
        .map(|k| {
            let slots = slots_map.get(&k).copied().unwrap_or(1);
            (k, slots)
        })
        .collect())
}

/// Describes a single output within a multi-asset.
// `__str__` is hand-written rather than the `str = "..."` pyclass attribute:
// `name` is an `Option`, and a format string can only render it as `Some("x")`.
#[pyclass(module = "rivers._core")]
pub struct AssetDef {
    /// `None` only in class-form multi assets, where registration passes a
    /// copy named after the attribute. `from_multi` rejects unnamed defs.
    #[pyo3(get, set)]
    pub name: Option<String>,
    #[pyo3(get, set)]
    pub tags: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub kinds: Kinds,
    #[pyo3(get, set)]
    pub group: Option<String>,
    #[pyo3(get, set)]
    pub code_version: Option<String>,
    pub io_handler: Option<IOHandler>,
    #[pyo3(get, set)]
    pub metadata: Option<HashMap<String, String>>,
    pub partitions_def: Option<PartitionsDefRef>,
    #[pyo3(get, set)]
    pub partition_mapping: Option<PartitionMappingDict>,
    /// Pool membership: normalized (pool_key, slots_consumed) pairs.
    #[pyo3(get)]
    pub pool: Vec<(String, u32)>,
    /// Per-output dependencies. Combined with the multi-asset's top-level
    /// `deps=` at build time: input deps merge into the function's input set
    /// (de-duplicated by name), lineage-only deps become edges to this output.
    pub deps: Vec<Py<DepDef>>,
    /// Per-output actions. Merged with the multi-asset's top-level
    /// `actions=` at build time; a per-output action overrides a top-level
    /// one with the same name.
    pub actions: Vec<Py<PyAssetAction>>,
}

impl AssetDef {
    /// Raises instead of panicking when a setter runs on another thread.
    pub(crate) fn read<'py>(slf: &Bound<'py, Self>) -> PyResult<PyRef<'py, Self>> {
        slf.try_borrow().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "cannot read this AssetDef while another thread sets one of its fields",
            )
        })
    }

    /// Holds the borrow only while it clones, so callers can run Python on
    /// the copy.
    pub(crate) fn copy(slf: &Bound<'_, Self>) -> PyResult<Self> {
        let py = slf.py();
        let def = Self::read(slf)?;
        Ok(Self {
            name: def.name.clone(),
            tags: def.tags.clone(),
            kinds: def.kinds.clone(),
            group: def.group.clone(),
            code_version: def.code_version.clone(),
            io_handler: def.io_handler.as_ref().map(|h| h.clone_ref(py)),
            metadata: def.metadata.clone(),
            partitions_def: def.partitions_def.as_ref().map(|p| p.clone_ref(py)),
            partition_mapping: def.partition_mapping.clone(),
            pool: def.pool.clone(),
            deps: def.deps.iter().map(|d| d.clone_ref(py)).collect(),
            actions: def.actions.iter().map(|a| a.clone_ref(py)).collect(),
        })
    }

    /// A copy named `name` unless the def has a name of its own.
    pub(crate) fn named_copy(slf: &Bound<'_, Self>, name: &str) -> PyResult<Self> {
        let mut def = Self::copy(slf)?;
        def.name.get_or_insert_with(|| name.to_owned());
        Ok(def)
    }
}

#[pymethods]
impl AssetDef {
    fn __str__(&self) -> String {
        format!(
            "AssetDef(name={}, tags={:?}, kinds={:?}, group={:?}, code_version={:?})",
            self.name.as_deref().unwrap_or("<unnamed>"),
            self.tags,
            self.kinds,
            self.group,
            self.code_version,
        )
    }

    fn __eq__(slf: &Bound<'_, Self>, other: &Bound<'_, Self>) -> PyResult<bool> {
        let (a, b) = (Self::copy(slf)?, Self::copy(other)?);
        Ok(a.name == b.name
            && a.tags == b.tags
            && a.kinds.0 == b.kinds.0
            && a.group == b.group
            && a.code_version == b.code_version
            && a.metadata == b.metadata
            && a.pool == b.pool
            && PartitionsDefRef::opt_eq(&a.partitions_def, &b.partitions_def)
            && io_handler_eq(slf.py(), a.io_handler.as_ref(), b.io_handler.as_ref()))
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.name.hash(&mut hasher);
        hasher.finish()
    }

    /// A class-form multi asset defined in a script ships to loky workers by
    /// value, and its outputs are `AssetDef` attributes.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let Self {
            name,
            tags,
            kinds,
            group,
            code_version,
            io_handler,
            metadata,
            partitions_def,
            partition_mapping,
            pool,
            deps,
            actions,
        } = self;
        let kwargs = PyDict::new(py);
        kwargs.set_item("name", name)?;
        kwargs.set_item("tags", tags)?;
        kwargs.set_item("kinds", &kinds.0)?;
        kwargs.set_item("group", group)?;
        kwargs.set_item("code_version", code_version)?;
        kwargs.set_item("io_handler", io_handler.as_ref().map(|h| h.to_object(py)))?;
        kwargs.set_item("metadata", metadata)?;
        kwargs.set_item(
            "partitions_def",
            partitions_def.as_ref().map(|p| p.to_object(py)),
        )?;
        kwargs.set_item("partition_mapping", partition_mapping)?;
        kwargs.set_item("pool", pool.iter().map(|(key, _)| key).collect::<Vec<_>>())?;
        kwargs.set_item(
            "pool_slots",
            pool.iter().cloned().collect::<HashMap<_, _>>(),
        )?;
        kwargs.set_item("deps", deps)?;
        kwargs.set_item("actions", actions)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    /// Create a new output definition for use with `Asset.from_multi()`.
    ///
    /// `name` may be omitted only when the def is assigned as a class attribute
    /// of a class-form multi asset — the output then takes the attribute name.
    #[new]
    #[pyo3(signature = (
        name = None,
        tags = None,
        kinds = None,
        group = None,
        code_version = None,
        io_handler = None,
        metadata = None,
        partitions_def = None,
        partition_mapping = None,
        pool = None,
        pool_slots = None,
        deps = vec![],
        actions = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new<'py>(
        _py: Python<'py>,
        name: Option<String>,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        code_version: Option<String>,
        io_handler: Option<IOHandler>,
        metadata: Option<HashMap<String, String>>,
        partitions_def: Option<PartitionsDefRef>,
        partition_mapping: Option<PartitionMappingDict>,
        pool: Option<&Bound<'py, PyAny>>,
        pool_slots: Option<&Bound<'py, PyAny>>,
        deps: Vec<Py<DepDef>>,
        actions: Option<Vec<Py<PyAssetAction>>>,
    ) -> PyResult<Self> {
        let actions = actions.unwrap_or_default();
        let pool = normalize_pool(pool, pool_slots)?;
        Ok(Self {
            name,
            tags,
            kinds: kinds.unwrap_or_default(),
            group,
            code_version,
            io_handler,
            metadata,
            partitions_def,
            partition_mapping,
            pool,
            deps,
            actions,
        })
    }

    /// Read-only access to the per-output dependency list (the `deps=` argument).
    #[getter]
    fn deps(&self, py: Python) -> Vec<Py<DepDef>> {
        self.deps.iter().map(|d| d.clone_ref(py)).collect()
    }

    /// Per-output actions (the `actions=` argument) — the introspection route
    /// for a multi-asset's verbs, which live on its outputs.
    #[getter]
    fn actions(&self, py: Python) -> Vec<Py<PyAssetAction>> {
        self.actions.iter().map(|a| a.clone_ref(py)).collect()
    }

    /// The definition object, or the `partition_defs` registry name string.
    #[getter]
    fn partitions_def(&self, py: Python) -> Option<Py<PyAny>> {
        self.partitions_def.as_ref().map(|r| r.to_object(py))
    }

    #[setter]
    fn set_partitions_def(&mut self, val: Option<PartitionsDefRef>) {
        self.partitions_def = val;
    }

    /// Create an input dependency definition for use with `deps=[...]`.
    ///
    /// An input dep is matched to a function parameter by name. It can carry
    /// a `partition_mapping` and/or an `io_handler` override for loading.
    #[staticmethod]
    #[pyo3(signature = (name, partition_mapping = None, io_handler = None, metadata = None))]
    fn input(
        name: String,
        partition_mapping: Option<PartitionMapping>,
        io_handler: Option<IOHandler>,
        metadata: Option<HashMap<String, String>>,
    ) -> DepDef {
        DepDef {
            name,
            io_handler,
            partition_mapping,
            metadata,
            is_input: true,
        }
    }

    /// Create a lineage-only dependency for use with `deps=[...]`.
    ///
    /// A dep-only entry adds a graph edge (for scheduling / automation) but does
    /// not load data — the name does NOT need to match a function parameter.
    #[staticmethod]
    #[pyo3(signature = (name, partition_mapping = None))]
    fn dep(name: String, partition_mapping: Option<PartitionMapping>) -> DepDef {
        DepDef {
            name,
            io_handler: None,
            partition_mapping,
            metadata: None,
            is_input: false,
        }
    }
}
