//! Core `@Asset` decorator and the `Asset` enum (Single, Multi, Graph) that wraps all asset variants.
use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

use super::dep_def::DepDef;
use super::external_asset::{ExternalAsset, PyExternalAsset};
use super::graph_asset::{GraphAsset, PyGraphAsset};
use super::io_handler::IOHandler;
use super::multi_asset::{MultiAsset, PyMultiAsset};
use super::single_asset::{PySingleAsset, SingleAsset};
use crate::automation::PyAutomationCondition;
use crate::composition::{
    InvokedNode, InvokedNodeType, PyInvokedNodeOutput, enter_composition, exit_composition,
    extract_input_bindings, is_in_composition, observe_invocation,
};
use crate::errors::AssetDefinitionError;

fn ensure_callable(py: Python, func: &Option<Py<PyAny>>) -> PyResult<()> {
    if !func
        .as_ref()
        .map(|f| f.getattr(py, "__call__").is_ok())
        .unwrap_or(true)
    {
        return Err(AssetDefinitionError::new_err(
            "Non callable function provided as input",
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ProcessedDeps {
    partition_mappings: Option<PartitionMappingDict>,
    input_dep_names: Vec<String>,
    dep_only_names: Vec<String>,
    input_io_handlers: HashMap<String, IOHandler>,
    input_metadata: HashMap<String, HashMap<String, String>>,
}

impl ProcessedDeps {
    /// Record the per-edge fields (`partition_mapping`, `io_handler`,
    /// `metadata`) from one dep. Caller is responsible for adding the
    /// dep's name to `input_dep_names` or `dep_only_names`.
    fn record_fields(&mut self, py: Python, d: &DepDef) {
        if let Some(pm) = &d.partition_mapping {
            self.partition_mappings
                .get_or_insert_with(|| PartitionMappingDict(HashMap::new()))
                .0
                .insert(d.name.clone(), pm.clone());
        }
        if let Some(h) = &d.io_handler {
            self.input_io_handlers
                .insert(d.name.clone(), h.clone_ref(py));
        }
        if let Some(m) = &d.metadata {
            self.input_metadata.insert(d.name.clone(), m.clone());
        }
    }
}

fn process_deps(py: Python, deps: &[&DepDef]) -> ProcessedDeps {
    let mut pd = ProcessedDeps::default();
    for d in deps {
        if d.is_input {
            pd.input_dep_names.push(d.name.clone());
        } else {
            pd.dep_only_names.push(d.name.clone());
        }
        pd.record_fields(py, d);
    }
    pd
}

/// Merge one per-output input `DepDef` into the multi-asset's top-level
/// input collections. Dedups by name; raises if the same name is declared
/// twice with conflicting `partition_mapping`, `io_handler`, or `metadata`.
fn merge_input_dep(
    py: Python,
    pd: &mut ProcessedDeps,
    def: &DepDef,
    output_name: &str,
) -> PyResult<()> {
    fn input_dep_conflict(output_name: &str, dep_name: &str, field: &str) -> PyErr {
        AssetDefinitionError::new_err(format!(
            "Multi-asset output '{output_name}': input dep '{dep_name}' declared with \
             a {field} that conflicts with an earlier declaration."
        ))
    }

    if !pd.input_dep_names.iter().any(|n| n == &def.name) {
        pd.input_dep_names.push(def.name.clone());
        pd.record_fields(py, def);
        return Ok(());
    }
    let existing_pm = pd
        .partition_mappings
        .as_ref()
        .and_then(|m| m.0.get(&def.name));
    if existing_pm != def.partition_mapping.as_ref() {
        return Err(input_dep_conflict(
            output_name,
            &def.name,
            "partition_mapping",
        ));
    }
    if !io_handler_eq(
        py,
        pd.input_io_handlers.get(&def.name),
        def.io_handler.as_ref(),
    ) {
        return Err(input_dep_conflict(output_name, &def.name, "io_handler"));
    }
    if pd.input_metadata.get(&def.name) != def.metadata.as_ref() {
        return Err(input_dep_conflict(output_name, &def.name, "metadata"));
    }
    Ok(())
}

/// Process one multi-asset output's `deps`. Input deps merge into `pd`
/// (the function-level input set, shared across outputs); lineage-only
/// deps yield this output's `dep_only_names` and a merged
/// `partition_mapping` (combining per-edge mappings from `deps=` with the
/// `AssetDef.partition_mapping` dict the user passed directly).
fn collect_output_deps(
    py: Python,
    pd: &mut ProcessedDeps,
    asset_def: &AssetDef,
) -> PyResult<(Vec<String>, Option<PartitionMappingDict>)> {
    let mut dep_only_names: Vec<String> = Vec::new();
    let mut dep_pms: HashMap<String, PartitionMapping> = HashMap::new();
    for raw_dep in &asset_def.deps {
        let d = raw_dep.get();
        if d.is_input {
            merge_input_dep(py, pd, d, &asset_def.name)?;
            continue;
        }
        if !dep_only_names.contains(&d.name) {
            dep_only_names.push(d.name.clone());
        }
        if let Some(pm) = &d.partition_mapping {
            if let Some(existing) = dep_pms.get(&d.name)
                && existing != pm
            {
                return Err(AssetDefinitionError::new_err(format!(
                    "Multi-asset output '{}': dep '{}' declared with \
                     conflicting partition_mappings on the same AssetDef.",
                    asset_def.name, d.name,
                )));
            }
            dep_pms.insert(d.name.clone(), pm.clone());
        }
    }
    let partition_mapping = merge_partition_mappings(asset_def, &dep_pms)?;
    Ok((dep_only_names, partition_mapping))
}

/// Combine the `AssetDef.partition_mapping` dict (user-provided per-output
/// overrides keyed by dep name) with partition mappings derived from
/// per-output lineage-only `deps`. Raises on conflicting values for the
/// same dep name.
fn merge_partition_mappings(
    asset_def: &AssetDef,
    dep_pms: &HashMap<String, PartitionMapping>,
) -> PyResult<Option<PartitionMappingDict>> {
    if dep_pms.is_empty() {
        return Ok(asset_def.partition_mapping.clone());
    }
    let mut merged = asset_def
        .partition_mapping
        .as_ref()
        .map(|pm| pm.0.clone())
        .unwrap_or_default();
    for (k, v) in dep_pms {
        if let Some(prev) = merged.get(k)
            && prev != v
        {
            return Err(AssetDefinitionError::new_err(format!(
                "Multi-asset output '{}': dep '{}' has a partition_mapping \
                 from `deps=` that conflicts with the entry in \
                 `partition_mapping=`.",
                asset_def.name, k,
            )));
        }
        merged.insert(k.clone(), v.clone());
    }
    Ok(Some(PartitionMappingDict(merged)))
}

fn validate_input_dep_names(
    py: Python,
    wraps: &Py<PyAny>,
    input_dep_names: &[String],
) -> PyResult<()> {
    if input_dep_names.is_empty() {
        return Ok(());
    }
    let annotations = wraps.getattr(py, "__annotations__")?;
    let ann_dict: &Bound<PyDict> = annotations.cast_bound(py)?;
    let param_names: HashSet<String> = ann_dict
        .iter()
        .filter_map(|(k, _)| {
            let name: String = k.extract().ok()?;
            if name == "return" || name == "self" {
                return None;
            }
            Some(name)
        })
        .collect();

    for input_name in input_dep_names {
        if !param_names.contains(input_name) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "AssetDef.input('{}') does not match any parameter \
                     on the function. Available parameters: {:?}",
                input_name,
                param_names.iter().collect::<Vec<_>>(),
            )));
        }
    }
    Ok(())
}

pub fn name_or_fn_name(
    py: Python,
    name: Option<String>,
    func: &Option<Py<PyAny>>,
) -> Option<String> {
    name.or_else(|| {
        func.as_ref()
            .map(|f| f.getattr(py, "__name__").unwrap().to_string())
    })
}
pub fn is_coroutine_function(py: Python, func: &Option<Py<PyAny>>) -> bool {
    func.as_ref().is_some_and(|f| {
        let inspect = match py.import("inspect") {
            Ok(m) => m,
            Err(_) => return false,
        };
        let fb = f.bind(py);
        inspect
            .call_method1("iscoroutinefunction", (fb,))
            .and_then(|r| r.is_truthy())
            .unwrap_or(false)
            || inspect
                .call_method1("isasyncgenfunction", (fb,))
                .and_then(|r| r.is_truthy())
                .unwrap_or(false)
    })
}

use crate::hooks::PyHook;
use crate::partitions::PartitionsDefinition;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::mapping::{PartitionMapping, PartitionMappingDict};

fn py_opt_eq<T: pyo3::PyTypeInfo>(py: Python, a: &Option<Py<T>>, b: &Option<Py<T>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.bind(py).as_any().eq(b.bind(py).as_any()).unwrap_or(false),
        _ => false,
    }
}

fn io_handler_eq(py: Python, a: Option<&IOHandler>, b: Option<&IOHandler>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(IOHandler::ResourceRef(a)), Some(IOHandler::ResourceRef(b))) => a == b,
        (Some(IOHandler::Instance(a)), Some(IOHandler::Instance(b))) => {
            a.bind(py).eq(b.bind(py)).unwrap_or(false)
        }
        _ => false,
    }
}

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
#[pyclass(
    str = "AssetDef(name={name}, tags={tags:?}, kinds={kinds:?}, group={group:?}, code_version={code_version:?})",
    module = "rivers._core"
)]
pub struct AssetDef {
    #[pyo3(get, set)]
    pub name: String,
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
    #[pyo3(get, set)]
    pub partitions_def: Option<Py<PartitionsDefinition>>,
    #[pyo3(get, set)]
    pub partition_mapping: Option<PartitionMappingDict>,
    /// Pool membership: normalized (pool_key, slots_consumed) pairs.
    #[pyo3(get)]
    pub pool: Vec<(String, u32)>,
    /// Per-output dependencies. Combined with the multi-asset's top-level
    /// `deps=` at build time: input deps merge into the function's input set
    /// (de-duplicated by name), lineage-only deps become edges to this output.
    pub deps: Vec<Py<DepDef>>,
}

#[pymethods]
impl AssetDef {
    fn __eq__(&self, py: Python, other: &AssetDef) -> bool {
        self.name == other.name
            && self.tags == other.tags
            && self.kinds.0 == other.kinds.0
            && self.group == other.group
            && self.code_version == other.code_version
            && self.metadata == other.metadata
            && self.pool == other.pool
            && py_opt_eq(py, &self.partitions_def, &other.partitions_def)
            && io_handler_eq(py, self.io_handler.as_ref(), other.io_handler.as_ref())
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.name.hash(&mut hasher);
        hasher.finish()
    }

    /// Create a new output definition for use with `Asset.from_multi()`.
    #[new]
    #[pyo3(signature = (
        name,
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
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new<'py>(
        _py: Python<'py>,
        name: String,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        code_version: Option<String>,
        io_handler: Option<IOHandler>,
        metadata: Option<HashMap<String, String>>,
        partitions_def: Option<Py<PartitionsDefinition>>,
        partition_mapping: Option<PartitionMappingDict>,
        pool: Option<&Bound<'py, PyAny>>,
        pool_slots: Option<&Bound<'py, PyAny>>,
        deps: Vec<Py<DepDef>>,
    ) -> PyResult<Self> {
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
        })
    }

    /// Read-only access to the per-output dependency list (the `deps=` argument).
    #[getter]
    fn deps(&self, py: Python) -> Vec<Py<DepDef>> {
        self.deps.iter().map(|d| d.clone_ref(py)).collect()
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

pub enum Asset {
    Single(SingleAsset),
    Multi(MultiAsset),
    Graph(GraphAsset),
    External(ExternalAsset),
}

/// Base class for all asset types, exposed to Python as `Asset`.
///
/// `Asset` acts as both a decorator (`@Asset`, `@Asset(name=...)`) and a
/// factory for specialized asset kinds via classmethods: `from_multi`,
/// `from_graph`, and `external`.
///
/// When called as a decorator it wraps a Python function and produces a
/// `SingleAsset`. When used as a factory classmethod it produces the
/// corresponding subclass (`MultiAsset`, `GraphAsset`, `ExternalAsset`).
#[pyclass(name = "Asset", subclass, module = "rivers._core")]
pub struct PyAsset {
    pub inner: Asset,
}

impl Asset {
    pub fn name(&self) -> &Option<String> {
        match self {
            Asset::Single(asset) => &asset.name,
            Asset::Graph(asset) => &asset.name,
            Asset::External(asset) => &asset.name,
            Asset::Multi(asset) => &asset.name,
        }
    }

    /// Multi-assets do not support top-level tags.
    pub fn tags(&self) -> Option<&Vec<String>> {
        match self {
            Asset::Single(asset) => asset.tags.as_ref(),
            Asset::Graph(asset) => asset.tags.as_ref(),
            Asset::External(asset) => asset.tags.as_ref(),
            Asset::Multi(_) => None,
        }
    }

    pub fn multi_asset_names(&self) -> Option<Vec<&Option<String>>> {
        match self {
            Asset::Multi(asset) => Some(asset.assets.iter().map(|a| &a.name).collect()),
            _ => None,
        }
    }

    /// Multi-assets store handlers per-output.
    pub fn io_handler(&self) -> Option<&IOHandler> {
        match self {
            Asset::Single(a) => a.io_handler.as_ref(),
            Asset::Graph(a) => a.io_handler.as_ref(),
            Asset::External(a) => Some(&a.io_handler),
            Asset::Multi(_) => None,
        }
    }

    pub fn node_io_handler(&self) -> Option<&IOHandler> {
        match self {
            Asset::Graph(a) => a.node_io_handler.as_ref(),
            _ => None,
        }
    }

    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        match self {
            Asset::Single(a) => a.metadata.as_ref(),
            Asset::Graph(a) => a.metadata.as_ref(),
            Asset::External(a) => a.metadata.as_ref(),
            Asset::Multi(_) => None,
        }
    }

    /// Returns an empty `Kinds` for multi-assets.
    pub fn kinds(&self) -> &Kinds {
        match self {
            Asset::Single(a) => &a.kinds,
            Asset::Graph(a) => &a.kinds,
            Asset::External(a) => &a.kinds,
            Asset::Multi(_) => {
                static EMPTY: Kinds = Kinds(Vec::new());
                &EMPTY
            }
        }
    }

    pub fn group(&self) -> Option<&String> {
        match self {
            Asset::Single(a) => a.group.as_ref(),
            Asset::Graph(a) => a.group.as_ref(),
            Asset::External(a) => a.group.as_ref(),
            Asset::Multi(_) => None,
        }
    }

    pub fn code_version(&self) -> Option<&String> {
        match self {
            Asset::Single(a) => a.code_version.as_ref(),
            Asset::Graph(a) => a.code_version.as_ref(),
            Asset::Multi(_) | Asset::External(_) => None,
        }
    }

    pub fn backfill_strategy(&self) -> Option<&PyBackfillStrategy> {
        match self {
            Asset::Single(a) => a.backfill_strategy.as_ref(),
            Asset::Multi(a) => a.backfill_strategy.as_ref(),
            Asset::Graph(a) => a.backfill_strategy.as_ref(),
            Asset::External(a) => a.backfill_strategy.as_ref(),
        }
    }

    pub fn partitions_def(&self) -> Option<&Py<PartitionsDefinition>> {
        match self {
            Asset::Single(a) => a.partitions_def.as_ref(),
            Asset::Multi(a) => a.partitions_def.as_ref(),
            Asset::Graph(a) => a.partitions_def.as_ref(),
            Asset::External(a) => a.partitions_def.as_ref(),
        }
    }

    pub fn partition_mapping(&self) -> Option<&PartitionMappingDict> {
        match self {
            Asset::Single(a) => a.partition_mapping.as_ref(),
            Asset::Graph(a) => a.partition_mappings.as_ref(),
            Asset::Multi(a) => a.partition_mappings.as_ref(),
            Asset::External(_) => None,
        }
    }

    pub fn input_io_handler(&self, param_name: &str) -> Option<&IOHandler> {
        match self {
            Asset::Single(a) => a.input_io_handlers.get(param_name),
            Asset::Multi(a) => a.input_io_handlers.get(param_name),
            Asset::Graph(a) => a.input_io_handlers.get(param_name),
            Asset::External(_) => None,
        }
    }

    pub fn input_metadata(&self, param_name: &str) -> Option<&HashMap<String, String>> {
        match self {
            Asset::Single(a) => a.input_metadata.get(param_name),
            Asset::Multi(a) => a.input_metadata.get(param_name),
            Asset::Graph(a) => a.input_metadata.get(param_name),
            Asset::External(_) => None,
        }
    }

    /// External assets do not support hooks.
    pub fn hooks(&self) -> Option<&Vec<Py<PyHook>>> {
        match self {
            Asset::Single(a) => a.hooks.as_ref(),
            Asset::Graph(a) => a.hooks.as_ref(),
            Asset::Multi(a) => a.hooks.as_ref(),
            Asset::External(_) => None,
        }
    }

    pub fn automation_condition(&self) -> Option<&PyAutomationCondition> {
        match self {
            Asset::Single(a) => a.automation_condition.as_ref(),
            Asset::Graph(a) => a.automation_condition.as_ref(),
            Asset::External(a) => a.automation_condition.as_ref(),
            Asset::Multi(a) => a.automation_condition.as_ref(),
        }
    }

    pub fn pool(&self) -> &Vec<(String, u32)> {
        match self {
            Asset::Single(a) => &a.pool,
            // Multi, Graph, External don't have pool yet (pools are per-output in multi)
            _ => {
                static EMPTY: Vec<(String, u32)> = Vec::new();
                &EMPTY
            }
        }
    }

    pub fn observe_fn(&self) -> Option<&Py<PyAny>> {
        match self {
            Asset::External(a) => a.observe_fn.as_ref(),
            _ => None,
        }
    }

    /// Resolve all ResourceRef io_handlers to Instance variants using the resources dict.
    /// `io_handler_keys` contains resource keys that are validated IOHandler instances.
    pub fn resolve_io_handler_refs(
        &mut self,
        py: Python,
        handlers: &HashMap<String, &Py<PyAny>>,
        other_resource_keys: &HashSet<&String>,
    ) -> PyResult<()> {
        match self {
            Asset::Multi(multi) => {
                for inner_asset in &mut multi.assets {
                    let name = inner_asset.name.as_deref().ok_or_else(|| {
                        AssetDefinitionError::new_err(
                            "Asset name must be set before resolving io_handler refs \
                             (AssetDef already defines name)",
                        )
                    })?;
                    if let Some(ref mut handler) = inner_asset.io_handler {
                        handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                    }
                }
                let multi_name = multi.name.as_deref().unwrap_or("multi_asset");
                for handler in multi.input_io_handlers.values_mut() {
                    handler.resolve_in_place(py, handlers, other_resource_keys, multi_name)?;
                }
            }
            Asset::Graph(graph) => {
                let name = graph.name.as_deref().ok_or_else(|| {
                    AssetDefinitionError::new_err(
                        "Asset name must be set before resolving io_handler refs",
                    )
                })?;
                if let Some(ref mut handler) = graph.io_handler {
                    handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                }
                if let Some(ref mut handler) = graph.node_io_handler {
                    handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                }
                for handler in graph.input_io_handlers.values_mut() {
                    handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                }
            }
            Asset::Single(single) => {
                let name = single.name.as_deref().ok_or_else(|| {
                    AssetDefinitionError::new_err(
                        "Asset name must be set before resolving io_handler refs",
                    )
                })?;
                if let Some(ref mut handler) = single.io_handler {
                    handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                }
                for handler in single.input_io_handlers.values_mut() {
                    handler.resolve_in_place(py, handlers, other_resource_keys, name)?;
                }
            }
            Asset::External(ext) => {
                let name = ext.name.as_deref().ok_or_else(|| {
                    AssetDefinitionError::new_err(
                        "Asset name must be set before resolving io_handler refs",
                    )
                })?;
                ext.io_handler
                    .resolve_in_place(py, handlers, other_resource_keys, name)?;
            }
        }
        Ok(())
    }

    pub fn _asset_fn(&self) -> PyResult<&Py<PyAny>> {
        match self {
            Asset::Single(asset) => asset
                .wraps
                .as_ref()
                .ok_or(AssetDefinitionError::new_err("Function not set")),
            Asset::Graph(asset) => asset
                .wraps
                .as_ref()
                .ok_or(AssetDefinitionError::new_err("Function not set")),
            Asset::External(_) => Err(AssetDefinitionError::new_err(
                "External assets have no compute function",
            )),
            Asset::Multi(asset) => asset
                .wraps
                .as_ref()
                .ok_or_else(|| AssetDefinitionError::new_err("Function not set")),
        }
    }

    /// For graph assets, this also runs the function in a composition context to capture
    /// the invocation DAG.
    fn set_wraps(&mut self, py: Python<'_>, wraps: Py<PyAny>) -> PyResult<()> {
        let function_name = wraps.as_ref().getattr(py, "__name__")?.to_string();
        let is_async = is_coroutine_function(py, &Some(wraps.clone_ref(py)));
        match self {
            Asset::Single(single_asset) => {
                validate_input_dep_names(py, &wraps, &single_asset.input_dep_names)?;
                single_asset.wraps = Some(wraps);
                single_asset.is_async = is_async;
                if single_asset.name.is_none() {
                    single_asset.name = Some(function_name)
                }
            }
            Asset::Graph(graph_asset) => {
                graph_asset.wraps = Some(wraps.clone_ref(py));
                if graph_asset.name.is_none() {
                    graph_asset.name = Some(function_name)
                }
                let name = graph_asset
                    .name
                    .as_ref()
                    .ok_or_else(|| AssetDefinitionError::new_err("Graph asset must have a name"))?;
                let (invocations, order, final_n) = call_graph_fn_in_composition(py, &wraps, name)?;
                graph_asset.invocations = invocations;
                graph_asset.invocation_order = order;
                graph_asset.final_node = final_n;
            }
            Asset::External(ext) => {
                ext.observe_fn = Some(wraps);
                ext.is_async_observe = is_async;
                if ext.name.is_none() {
                    ext.name = Some(function_name)
                }
            }
            Asset::Multi(multi_asset) => {
                validate_input_dep_names(py, &wraps, &multi_asset.input_dep_names)?;
                multi_asset.wraps = Some(wraps);
                multi_asset.is_async = is_async;
                if multi_asset.name.is_none() {
                    multi_asset.name = Some(function_name)
                }
            }
        }
        Ok(())
    }
}

/// Call a graph function during composition, passing InvokedNodeOutput placeholders
/// for each of its parameters (these represent external inputs to the graph).
fn call_graph_fn_in_composition(
    py: Python,
    func: &Py<PyAny>,
    graph_name: &str,
) -> PyResult<(Vec<InvokedNode>, Vec<String>, Option<String>)> {
    let annotations = func.getattr(py, "__annotations__")?;
    let annotations: &Bound<PyDict> = annotations.cast_bound(py)?;

    let mut placeholder_args: Vec<Py<PyAny>> = Vec::new();
    for (k, _v) in annotations.iter() {
        let param_name: String = k.extract()?;
        if param_name == "return" {
            continue;
        }
        let placeholder = PyInvokedNodeOutput::new(
            param_name,
            rivers_core::composition::DEFAULT_OUTPUT_NAME.to_string(),
        );
        placeholder_args.push(placeholder.into_pyobject(py)?.into_any().unbind());
    }

    let args_tuple = PyTuple::new(py, &placeholder_args)?;
    enter_composition(graph_name);
    let call_result = func.call1(py, args_tuple);
    let ctx = exit_composition();
    let result = call_result?;

    // If the graph function returns an InvokedNodeOutput, that's the terminal
    // task whose output becomes the graph asset's output.
    let final_node = result
        .bind(py)
        .extract::<PyInvokedNodeOutput>()
        .ok()
        .map(|out| out.node_name);

    let order = ctx.invocation_order().to_vec();
    Ok((ctx.invocations.into_values().collect(), order, final_node))
}

impl PyAsset {
    pub(crate) fn inner(&self) -> &Asset {
        &self.inner
    }

    pub(crate) fn inner_mut(&mut self) -> &mut Asset {
        &mut self.inner
    }
}

#[pymethods]
impl PyAsset {
    /// Create a new single-output asset.
    ///
    /// Can be used as a bare decorator (`@Asset`) or with arguments
    /// (`@Asset(name="my_asset", kinds="table")`). The decorated function
    /// becomes the asset's compute function.
    #[new]
    #[classmethod]
    #[pyo3(signature = (
        wraps=None,
        name = None,
        tags = None,
        kinds = None,
        group = None,
        code_version = None,
        io_handler = None,
        metadata = None,
        partitions_def = None,
        deps = vec![],
        hooks = None,
        automation_condition = None,
        backfill_strategy = None,
        pool = None,
        pool_slots = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::new_ret_no_self)]
    fn new<'py>(
        cls: &Bound<'py, PyType>,
        wraps: Option<Py<PyAny>>,
        mut name: Option<String>,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        code_version: Option<String>,
        io_handler: Option<IOHandler>,
        metadata: Option<HashMap<String, String>>,
        partitions_def: Option<Py<PartitionsDefinition>>,
        deps: Vec<Py<DepDef>>,
        hooks: Option<Vec<Py<PyHook>>>,
        automation_condition: Option<PyAutomationCondition>,
        backfill_strategy: Option<PyBackfillStrategy>,
        pool: Option<&Bound<'py, PyAny>>,
        pool_slots: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = cls.py();

        ensure_callable(cls.py(), &wraps)?;

        let handler = io_handler;

        name = name_or_fn_name(py, name, &wraps);

        let pool = normalize_pool(pool, pool_slots)?;

        let deps: Vec<&DepDef> = deps.iter().map(|d| d.get()).collect();
        let pd = process_deps(py, &deps);

        let is_async = is_coroutine_function(py, &wraps);
        let py_asset = Asset::Single(SingleAsset {
            wraps,
            is_async,
            name,
            tags,
            kinds: kinds.unwrap_or_default(),
            group,
            code_version,
            io_handler: handler,
            metadata,
            partitions_def,
            partition_mapping: pd.partition_mappings,
            input_dep_names: pd.input_dep_names,
            dep_only_names: pd.dep_only_names,
            input_io_handlers: pd.input_io_handlers,
            input_metadata: pd.input_metadata,
            hooks,
            automation_condition,
            backfill_strategy,
            pool,
        });

        let base = PyAsset { inner: py_asset };
        let py_obj = Py::new(py, (PySingleAsset {}, base))?;
        Ok(py_obj.into_any())
    }

    /// Create a multi-output asset from a list of `AssetDef` output definitions.
    ///
    /// Each `AssetDef` describes one output. Top-level arguments (tags, kinds,
    /// group, code_version, io_handler) are used as defaults when the individual
    /// `AssetDef` does not specify them.
    ///
    /// Dependencies can be declared at the top level via `deps=` (applied to
    /// every output) or per-output via `AssetDef(deps=[...])`. Input deps from
    /// either source merge into the multi-asset's function-level input set
    /// (the function fires once for all outputs); lineage-only deps declared
    /// per-output only become edges to that specific output.
    #[classmethod]
    #[pyo3(signature = (
        wraps = None,
        output_defs = vec![],
        name = None,
        tags = None,
        kinds = None,
        group = None,
        code_version = None,
        io_handler = None,
        partitions_def = None,
        deps = vec![],
        hooks = None,
        automation_condition = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn from_multi(
        cls: &Bound<'_, PyType>,
        wraps: Option<Py<PyAny>>,
        output_defs: Vec<Py<AssetDef>>,
        mut name: Option<String>,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        code_version: Option<String>,
        io_handler: Option<IOHandler>,
        partitions_def: Option<Py<PartitionsDefinition>>,
        deps: Vec<Py<DepDef>>,
        hooks: Option<Vec<Py<PyHook>>>,
        automation_condition: Option<PyAutomationCondition>,
    ) -> PyResult<Py<PyAny>> {
        let py = cls.py();
        let handler = io_handler;
        let top_level_deps: Vec<&DepDef> = deps.iter().map(|d| d.get()).collect();
        let kinds = kinds.unwrap_or_default();

        ensure_callable(py, &wraps)?;

        name = name_or_fn_name(py, name, &wraps);

        let mut pd = process_deps(py, &top_level_deps);

        let mut py_assets = Vec::with_capacity(output_defs.len());
        for asset in output_defs {
            let borrow_asset_def = asset.borrow(py);
            let io_handler = borrow_asset_def
                .io_handler
                .as_ref()
                .or(handler.as_ref())
                .map(|v| v.clone_ref(py));

            let def_kinds = if borrow_asset_def.kinds.is_empty() {
                &kinds
            } else {
                &borrow_asset_def.kinds
            };

            // The fn fires once for all outputs, so input deps from any
            // AssetDef merge into the top-level input set; lineage-only
            // deps stay scoped to this output.
            let (dep_only_names, partition_mapping) =
                collect_output_deps(py, &mut pd, &borrow_asset_def)?;

            py_assets.push(SingleAsset {
                wraps: None,
                is_async: false,
                name: Some(borrow_asset_def.name.clone()),
                tags: borrow_asset_def.tags.clone().or_else(|| tags.clone()),
                kinds: def_kinds.clone(),
                group: borrow_asset_def.group.clone().or_else(|| group.clone()),
                code_version: borrow_asset_def
                    .code_version
                    .clone()
                    .or_else(|| code_version.clone()),
                io_handler,
                metadata: None,
                backfill_strategy: None,
                partitions_def: partitions_def
                    .as_ref()
                    .or(borrow_asset_def.partitions_def.as_ref())
                    .map(|p| p.clone_ref(py)),
                partition_mapping,
                input_dep_names: Vec::new(),
                dep_only_names,
                input_io_handlers: HashMap::new(),
                input_metadata: HashMap::new(),
                hooks: None,
                automation_condition: None,
                pool: borrow_asset_def.pool.clone(),
            });
        }

        // All partitioned outputs must be the same variant, and for Static defs
        // there must be a non-empty intersection of keys.
        if partitions_def.is_none() {
            let partitioned_outputs: Vec<_> = py_assets
                .iter()
                .filter_map(|a| {
                    a.partitions_def
                        .as_ref()
                        .map(|pd| (a.name.as_deref().unwrap_or("?"), pd.get()))
                })
                .collect();

            if partitioned_outputs.len() > 1 {
                let first_name = partitioned_outputs[0].0;
                let first_pd = partitioned_outputs[0].1;
                for &(name, pd) in &partitioned_outputs[1..] {
                    if !first_pd.same_variant(pd) {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "Multi-asset output '{}' has {} partitions but '{}' has {} partitions. \
                             All outputs must use the same partition type.",
                            first_name,
                            first_pd.variant_name(),
                            name,
                            pd.variant_name(),
                        )));
                    }
                }
                if let Some(first_keys) = first_pd.static_keys() {
                    let mut intersection: std::collections::HashSet<&str> =
                        first_keys.iter().map(|k| k.as_str()).collect();
                    for &(name, pd) in &partitioned_outputs[1..] {
                        if let Some(keys) = pd.static_keys() {
                            let other: std::collections::HashSet<&str> =
                                keys.iter().map(|k| k.as_str()).collect();
                            intersection = intersection.intersection(&other).copied().collect();
                            if intersection.is_empty() {
                                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                                    "Multi-asset outputs '{}' and '{}' have no overlapping \
                                     partition keys. At least one common key is required.",
                                    first_name, name,
                                )));
                            }
                        }
                    }
                }
            }
        }

        let is_async = is_coroutine_function(py, &wraps);
        let py_asset = Asset::Multi(MultiAsset {
            name,
            wraps,
            is_async,
            code_version,
            assets: py_assets,
            partitions_def,
            input_dep_names: pd.input_dep_names,
            dep_only_names: pd.dep_only_names,
            partition_mappings: pd.partition_mappings,
            input_io_handlers: pd.input_io_handlers,
            input_metadata: pd.input_metadata,
            hooks,
            automation_condition,
            backfill_strategy: None,
        });
        let base = PyAsset { inner: py_asset };
        let py_obj = Py::new(py, (PyMultiAsset {}, base))?;
        Ok(py_obj.into_any())
    }

    /// Create a graph-backed asset that composes other assets.
    ///
    /// The decorated function should call other assets to define the composition
    /// DAG. The function is executed in a composition context to capture
    /// invocations.
    #[classmethod]
    #[pyo3(signature = (
        wraps = None,
        name = None,
        tags = None,
        kinds = None,
        group = None,
        code_version = None,
        io_handler = None,
        node_io_handler = None,
        metadata = None,
        partitions_def = None,
        deps = vec![],
        hooks = None,
        automation_condition = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn from_graph(
        cls: &Bound<'_, PyType>,
        wraps: Option<Py<PyAny>>,
        mut name: Option<String>,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        code_version: Option<String>,
        io_handler: Option<IOHandler>,
        node_io_handler: Option<IOHandler>,
        metadata: Option<HashMap<String, String>>,
        partitions_def: Option<Py<PartitionsDefinition>>,
        deps: Vec<Py<DepDef>>,
        hooks: Option<Vec<Py<PyHook>>>,
        automation_condition: Option<PyAutomationCondition>,
    ) -> PyResult<Py<PyAny>> {
        let py = cls.py();
        let handler = io_handler;

        name = name_or_fn_name(py, name, &wraps);

        let deps: Vec<&DepDef> = deps.iter().map(|d| d.get()).collect();
        let pd = process_deps(py, &deps);

        let mut graph_asset = GraphAsset {
            name,
            wraps: None,
            kinds: kinds.unwrap_or_default(),
            group,
            code_version,
            tags,
            io_handler: handler,
            node_io_handler,
            metadata,
            partitions_def,
            partition_mappings: pd.partition_mappings,
            dep_only_names: pd.dep_only_names,
            input_io_handlers: pd.input_io_handlers,
            input_metadata: pd.input_metadata,
            hooks,
            automation_condition,
            backfill_strategy: None,
            invocations: Vec::new(),
            invocation_order: Vec::new(),
            final_node: None,
        };

        if let Some(ref func) = wraps {
            graph_asset.wraps = Some(func.clone_ref(py));

            let name = graph_asset
                .name
                .as_ref()
                .ok_or_else(|| AssetDefinitionError::new_err("Graph asset must have a name"))?
                .clone();
            let (invocations, order, final_n) = call_graph_fn_in_composition(py, func, &name)?;
            graph_asset.invocations = invocations;
            graph_asset.invocation_order = order;
            graph_asset.final_node = final_n;
        }

        let py_asset = Asset::Graph(graph_asset);
        let base = PyAsset { inner: py_asset };
        let py_obj = Py::new(py, (PyGraphAsset {}, base))?;
        Ok(py_obj.into_any())
    }

    /// Create an external asset representing data produced outside of rivers.
    ///
    /// Requires an `io_handler` for loading data. An optional observe function
    /// can be provided (as the decorated function) to track data freshness.
    #[classmethod]
    #[pyo3(signature = (
        wraps = None,
        name = None,
        io_handler = None,
        tags = None,
        kinds = None,
        group = None,
        metadata = None,
        partitions_def = None,
        automation_condition = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn external(
        _cls: &Bound<'_, PyType>,
        wraps: Option<Py<PyAny>>,
        name: Option<String>,
        io_handler: Option<IOHandler>,
        tags: Option<Vec<String>>,
        kinds: Option<Kinds>,
        group: Option<String>,
        metadata: Option<HashMap<String, String>>,
        partitions_def: Option<Py<PartitionsDefinition>>,
        automation_condition: Option<PyAutomationCondition>,
    ) -> PyResult<Py<PyAny>> {
        let py = _cls.py();

        let handler = io_handler.ok_or_else(|| {
            AssetDefinitionError::new_err("External assets require an io_handler")
        })?;

        let asset_name = name_or_fn_name(py, name, &wraps);

        let is_async_observe = is_coroutine_function(py, &wraps);
        let py_asset = Asset::External(ExternalAsset {
            name: asset_name,
            tags,
            kinds: kinds.unwrap_or_default(),
            group,
            io_handler: handler,
            metadata,
            partitions_def,
            observe_fn: wraps,
            is_async_observe,
            automation_condition,
            backfill_strategy: None,
        });

        let base = PyAsset { inner: py_asset };
        let py_obj = Py::new(py, (PyExternalAsset {}, base))?;
        Ok(py_obj.into_any())
    }

    /// Handle both decorator application and composition invocation.
    ///
    /// If called inside a composition context and the asset already has a function
    /// bound, this records a graph invocation and returns a placeholder output.
    /// Otherwise, the first positional argument is treated as the function to wrap.
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        slf: &Bound<'_, Self>,
        py: Python,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut();

        if is_in_composition() && this.inner._asset_fn().is_ok() {
            let name = this
                .inner
                .name()
                .as_ref()
                .ok_or_else(|| AssetDefinitionError::new_err("Asset must have a name"))?
                .clone();
            drop(this);
            let input_bindings = extract_input_bindings(args, kwargs)?;
            let registered_name = observe_invocation(&name, InvokedNodeType::Asset, input_bindings);
            let output = PyInvokedNodeOutput::new(
                registered_name,
                rivers_core::composition::DEFAULT_OUTPUT_NAME.to_string(),
            );
            return Ok(output.into_pyobject(py)?.into_any().unbind());
        }

        let f: Py<PyAny> = args.get_item(0)?.into();
        this.inner.set_wraps(py, f)?;
        drop(this);
        Ok(slf.clone().unbind().into_any())
    }

    #[getter]
    fn _asset_fn(&self) -> PyResult<&Py<PyAny>> {
        self.inner._asset_fn()
    }

    /// The asset name, or None if not yet set (e.g. before decorator application).
    #[getter]
    fn _name(&self) -> &Option<String> {
        self.inner.name()
    }

    /// The asset name. Raises AssetDefinitionError if the name has not been set.
    #[getter]
    fn name(&self) -> PyResult<String> {
        self.inner
            .name()
            .as_ref()
            .cloned()
            .ok_or_else(|| AssetDefinitionError::new_err("Asset has no name"))
    }

    #[getter]
    fn io_handler(&self) -> Option<&Py<PyAny>> {
        self.inner.io_handler().and_then(|h| match h {
            IOHandler::Instance(obj) => Some(obj),
            IOHandler::ResourceRef(_) => None,
        })
    }

    /// Internal task IO handler for graph assets.
    #[getter]
    fn node_io_handler(&self) -> Option<&Py<PyAny>> {
        self.inner.node_io_handler().and_then(|h| match h {
            IOHandler::Instance(obj) => Some(obj),
            IOHandler::ResourceRef(_) => None,
        })
    }

    #[getter]
    fn tags(&self) -> Option<Vec<String>> {
        self.inner.tags().cloned()
    }

    /// Kind labels (e.g. "table", "view").
    #[getter]
    fn kinds(&self) -> Kinds {
        self.inner.kinds().clone()
    }

    #[getter]
    fn group(&self) -> Option<String> {
        self.inner.group().cloned()
    }

    #[getter]
    fn metadata(&self) -> Option<HashMap<String, String>> {
        self.inner.metadata().cloned()
    }

    /// True if the compute/observe function is an async def.
    #[getter]
    fn is_async(&self) -> bool {
        match &self.inner {
            Asset::Single(a) => a.is_async,
            Asset::Multi(a) => a.is_async,
            Asset::External(a) => a.is_async_observe,
            Asset::Graph(_) => false,
        }
    }

    #[getter]
    fn is_single(&self) -> bool {
        matches!(self.inner, Asset::Single(_))
    }

    #[getter]
    fn is_multi(&self) -> bool {
        matches!(self.inner, Asset::Multi(_))
    }

    #[getter]
    fn is_graph(&self) -> bool {
        matches!(self.inner, Asset::Graph(_))
    }

    #[getter]
    fn is_external(&self) -> bool {
        matches!(self.inner, Asset::External(_))
    }

    #[getter]
    fn hooks(&self, py: Python) -> Option<Vec<Py<PyHook>>> {
        self.inner
            .hooks()
            .map(|h| h.iter().map(|hook| hook.clone_ref(py)).collect())
    }

    #[getter]
    fn automation_condition(&self) -> Option<PyAutomationCondition> {
        self.inner.automation_condition().cloned()
    }

    #[getter]
    fn partition_mapping(&self) -> Option<HashMap<String, PartitionMapping>> {
        self.inner.partition_mapping().map(|m| m.0.clone())
    }

    /// List of (pool_key, slots_consumed) pairs.
    #[getter]
    fn pool(&self) -> Vec<(String, u32)> {
        self.inner.pool().clone()
    }

    #[getter]
    fn code_version(&self) -> Option<String> {
        self.inner.code_version().cloned()
    }

    #[getter]
    fn partitions_def(&self) -> Option<&Py<PartitionsDefinition>> {
        self.inner.partitions_def()
    }

    #[getter]
    fn observe_fn(&self) -> Option<&Py<PyAny>> {
        self.inner.observe_fn()
    }
}
