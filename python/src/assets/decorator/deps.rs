use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::assets::action::PyAssetAction;
use crate::assets::dep_def::DepDef;
use crate::assets::io_handler::IOHandler;
use crate::errors::AssetDefinitionError;
use crate::partitions::PartitionsDefinition;
use crate::partitions::mapping::{PartitionMapping, PartitionMappingDict};

use super::asset_def::AssetDef;

#[derive(Default)]
pub(super) struct ProcessedDeps {
    pub(super) partition_mappings: Option<PartitionMappingDict>,
    pub(super) input_dep_names: Vec<String>,
    pub(super) dep_only_names: Vec<String>,
    pub(super) input_io_handlers: HashMap<String, IOHandler>,
    pub(super) input_metadata: HashMap<String, HashMap<String, String>>,
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

pub(super) fn process_deps(py: Python, deps: &[&DepDef]) -> ProcessedDeps {
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
pub(super) fn collect_output_deps(
    py: Python,
    pd: &mut ProcessedDeps,
    asset_def: &AssetDef,
    output_name: &str,
) -> PyResult<(Vec<String>, Option<PartitionMappingDict>)> {
    let mut dep_only_names: Vec<String> = Vec::new();
    let mut dep_pms: HashMap<String, PartitionMapping> = HashMap::new();
    for raw_dep in &asset_def.deps {
        let d = raw_dep.get();
        if d.is_input {
            merge_input_dep(py, pd, d, output_name)?;
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
                    output_name, d.name,
                )));
            }
            dep_pms.insert(d.name.clone(), pm.clone());
        }
    }
    let partition_mapping = merge_partition_mappings(asset_def, &dep_pms, output_name)?;
    Ok((dep_only_names, partition_mapping))
}

/// Combine the `AssetDef.partition_mapping` dict (user-provided per-output
/// overrides keyed by dep name) with partition mappings derived from
/// per-output lineage-only `deps`. Raises on conflicting values for the
/// same dep name.
fn merge_partition_mappings(
    asset_def: &AssetDef,
    dep_pms: &HashMap<String, PartitionMapping>,
    output_name: &str,
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
                output_name, k,
            )));
        }
        merged.insert(k.clone(), v.clone());
    }
    Ok(Some(PartitionMappingDict(merged)))
}

/// Every listed action must be bound to a function and names must be unique.
pub(super) fn validate_actions(actions: &[Py<PyAssetAction>], asset_desc: &str) -> PyResult<()> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for a in actions {
        let a = a.get();
        if a.func.is_none() {
            return Err(AssetDefinitionError::new_err(format!(
                "asset '{asset_desc}': action '{}' has no function — apply the \
                 AssetAction as a decorator to its body first",
                a.name
            )));
        }
        if !seen.insert(a.name.clone()) {
            return Err(AssetDefinitionError::new_err(format!(
                "asset '{asset_desc}': duplicate action '{}'",
                a.name
            )));
        }
    }
    Ok(())
}

/// Per-output actions for one multi-asset output: top-level actions plus the
/// output's own, the latter overriding same-name entries.
pub(super) fn merge_output_actions(
    py: Python,
    top_level: &[Py<PyAssetAction>],
    per_def: &[Py<PyAssetAction>],
) -> Vec<Py<PyAssetAction>> {
    let def_names: std::collections::HashSet<String> =
        per_def.iter().map(|a| a.get().name.clone()).collect();
    top_level
        .iter()
        .filter(|a| !def_names.contains(&a.get().name))
        .chain(per_def.iter())
        .map(|a| a.clone_ref(py))
        .collect()
}

pub(super) fn validate_input_dep_names(
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

/// All partitioned multi-asset outputs must be the same variant, and Static
/// defs must share at least one key.
pub(crate) fn validate_multi_output_partition_defs(
    partitioned_outputs: &[(&str, &PartitionsDefinition)],
) -> PyResult<()> {
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
    Ok(())
}

pub(super) fn io_handler_eq(py: Python, a: Option<&IOHandler>, b: Option<&IOHandler>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(IOHandler::ResourceRef(a)), Some(IOHandler::ResourceRef(b))) => a == b,
        (
            Some(IOHandler::Instance(a) | IOHandler::Resource(a)),
            Some(IOHandler::Instance(b) | IOHandler::Resource(b)),
        ) => a.bind(py).eq(b.bind(py)).unwrap_or(false),
        _ => false,
    }
}
