use std::collections::{BTreeMap, HashMap, HashSet};

use pyo3::prelude::*;

use crate::assets::decorator::{Asset, PyAsset, validate_multi_output_partition_defs};
use crate::assets::io_handler::IOHandler;
use crate::errors::{AssetDefinitionError, PartitionValidationError};
use crate::executor::ops::{enumerate_params, is_context_annotation};
use crate::partitions::mapping::PartitionMapping;
use crate::partitions::{PartitionsDefinition, resolve_partitions_def_ref};
use crate::task::{PyBashTask, PyTask};
use rivers_core::assets::graph::NodeRef;

use super::resolved_node::{ResolvedAsset, ResolvedBashTask, ResolvedNode, ResolvedTask};

/// The parameters that take upstream data, in declaration order: those
/// `enumerate_params` lists, except the context, `self` and resources.
fn upstream_params(
    py: Python,
    func: &Py<PyAny>,
    resource_keys: &HashSet<&String>,
) -> PyResult<Vec<String>> {
    let mut params = Vec::new();
    for (name, annotation) in enumerate_params(py, func)? {
        let is_ctx = annotation
            .as_ref()
            .is_some_and(|a| is_context_annotation(py, a));
        if is_ctx || name == "context" || name == "self" || resource_keys.contains(&name) {
            continue;
        }
        params.push(name);
    }
    Ok(params)
}

fn append_deps(
    py: Python,
    deps: &mut Vec<NodeRef>,
    func: &Py<PyAny>,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    deps.extend(
        upstream_params(py, func, resource_keys)?
            .into_iter()
            .map(NodeRef::ByName),
    );
    Ok(())
}

pub(crate) struct UnresolvedGraph {
    pub graph: BTreeMap<String, Vec<NodeRef>>,
    pub node_map: HashMap<String, ResolvedNode>,
    /// Task names whose deps come from graph asset composition bindings.
    pub composition_task_names: HashSet<String>,
    /// graph_name → namespaced task names (for io_handler_override)
    pub graph_task_names: HashMap<String, Vec<String>>,
    /// Step kinds for fan-out/collect steps
    pub step_kinds: HashMap<String, rivers_core::execution::plan::StepKind>,
}

pub(crate) fn build_unresolved_graph(
    py: Python,
    assets: &[Py<PyAsset>],
    tasks: &[Py<PyAny>],
    resource_keys: &HashSet<&String>,
    partition_defs: &HashMap<String, Py<PartitionsDefinition>>,
) -> PyResult<UnresolvedGraph> {
    let mut unresolved_graph: BTreeMap<String, Vec<NodeRef>> = BTreeMap::new();
    let mut node_map: HashMap<String, ResolvedNode> = HashMap::new();

    // Two definitions claiming one name used to overwrite silently, leaving
    // the graph with whichever registered last (e.g. one named `AssetDef`
    // shared by two class-form multi assets).
    let claim = |seen: &mut HashSet<String>, name: &str| -> PyResult<()> {
        if !seen.insert(name.to_string()) {
            return Err(AssetDefinitionError::new_err(format!(
                "duplicate asset name '{name}': two definitions register under it"
            )));
        }
        Ok(())
    };
    let mut claimed: HashSet<String> = HashSet::new();

    for decorator_py in assets {
        let inner_asset = &decorator_py.get().inner;
        let mut deps = Vec::new();

        match inner_asset {
            Asset::Single(single_asset) => {
                let func = single_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Add lineage-only deps from explicit deps list.
                for dep_name in &single_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                let asset_name = single_asset.name.clone().unwrap();
                claim(&mut claimed, &asset_name)?;
                unresolved_graph.insert(asset_name.clone(), deps);
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                        partition_defs,
                    )?)),
                );
            }
            Asset::Multi(multi_asset) => {
                let func = multi_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Top-level lineage-only deps apply to every output.
                for dep_name in &multi_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                for inner in &multi_asset.assets {
                    let asset_name = inner.name.clone().unwrap();
                    let output_name = asset_name.clone();
                    // Per-output lineage-only deps add edges only to this output.
                    let mut per_output_deps = deps.clone();
                    for dep_name in &inner.dep_only_names {
                        let already_present = per_output_deps.iter().any(
                            |n| matches!(n, NodeRef::ByName(existing) if existing == dep_name),
                        );
                        if !already_present {
                            per_output_deps.push(NodeRef::ByName(dep_name.clone()));
                        }
                    }
                    claim(&mut claimed, &asset_name)?;
                    unresolved_graph.insert(asset_name.clone(), per_output_deps);
                    node_map.insert(
                        asset_name,
                        ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                            py,
                            decorator_py.clone_ref(py),
                            Some(output_name),
                            partition_defs,
                        )?)),
                    );
                }

                // Re-run the per-output compatibility check with registry
                // names resolved; decoration time could only see inline defs.
                if multi_asset.partitions_def.is_none() {
                    let mut partitioned_outputs = Vec::new();
                    for inner in &multi_asset.assets {
                        if let Some(r) = &inner.partitions_def {
                            let owner =
                                inner.name.as_deref().expect("from_multi outputs are named");
                            partitioned_outputs.push((
                                owner,
                                resolve_partitions_def_ref(r, partition_defs, owner)?,
                            ));
                        }
                    }
                    validate_multi_output_partition_defs(&partitioned_outputs)?;
                }
            }
            Asset::Graph(graph_asset) => {
                let func = graph_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Add lineage-only deps from explicit deps list.
                for dep_name in &graph_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                // Graph asset depends on all its internal tasks so it
                // executes after them in the plan.
                for invocation in &graph_asset.invocations {
                    deps.push(NodeRef::ByName(invocation.name.clone()));
                }

                let asset_name = graph_asset.name.clone().unwrap();
                claim(&mut claimed, &asset_name)?;
                unresolved_graph.insert(asset_name.clone(), deps);
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                        partition_defs,
                    )?)),
                );
            }
            Asset::External(ext) => {
                let asset_name = ext.name.clone().unwrap();
                claim(&mut claimed, &asset_name)?;
                unresolved_graph.insert(asset_name.clone(), Vec::new());
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                        partition_defs,
                    )?)),
                );
            }
        };
    }

    // Build composition-derived dependency overrides from graph assets.
    // Invocation names are namespaced as "{graph_name}/{task_name}" for tasks.
    use rivers_core::composition::{InputBinding, InvocationKind};
    use rivers_core::execution::plan::StepKind;
    let mut composition_bindings: HashMap<String, Vec<InputBinding>> = HashMap::new();
    // bare task name → list of namespaced names
    let mut task_namespaced_entries: HashMap<String, Vec<String>> = HashMap::new();
    // graph_name → list of namespaced task names (for post-resolve io_handler_override)
    let mut graph_task_names: HashMap<String, Vec<String>> = HashMap::new();
    let mut step_kinds: HashMap<String, StepKind> = HashMap::new();
    // Virtual nodes that need graph entries but no node_map entries.
    let mut collect_steps: HashMap<String, Vec<NodeRef>> = HashMap::new();
    // Per-graph inheritance maps so internal tasks pick up the parent's
    // partition / io / metadata configuration.
    let mut graph_partitions_def: HashMap<String, PartitionsDefinition> = HashMap::new();
    let mut graph_partition_mappings: HashMap<String, HashMap<String, PartitionMapping>> =
        HashMap::new();
    let mut graph_input_io_handlers: HashMap<String, HashMap<String, IOHandler>> = HashMap::new();
    let mut graph_input_metadata: HashMap<String, HashMap<String, HashMap<String, String>>> =
        HashMap::new();
    for decorator_py in assets {
        let inner_asset = &decorator_py.get().inner;
        if let Asset::Graph(graph_asset) = inner_asset {
            let graph_name = graph_asset.name.clone().unwrap_or_default();
            if let Some(ref pd) = graph_asset.partitions_def {
                graph_partitions_def.insert(
                    graph_name.clone(),
                    resolve_partitions_def_ref(pd, partition_defs, &graph_name)?.clone(),
                );
            }
            if let Some(ref pm) = graph_asset.partition_mappings {
                graph_partition_mappings.insert(graph_name.clone(), pm.0.clone());
            }
            if !graph_asset.input_io_handlers.is_empty() {
                graph_input_io_handlers.insert(
                    graph_name.clone(),
                    graph_asset
                        .input_io_handlers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                        .collect(),
                );
            }
            if !graph_asset.input_metadata.is_empty() {
                graph_input_metadata.insert(graph_name.clone(), graph_asset.input_metadata.clone());
            }
            for invocation in &graph_asset.invocations {
                let kind = StepKind::from(&invocation.invocation_kind);
                if kind != StepKind::Normal {
                    step_kinds.insert(invocation.name.clone(), kind);
                }
                // Register collect/collect_stream as virtual nodes with dep on mapped step
                match &invocation.invocation_kind {
                    InvocationKind::Collect { mapped_node }
                    | InvocationKind::CollectStream { mapped_node, .. } => {
                        collect_steps.insert(
                            invocation.name.clone(),
                            vec![NodeRef::ByName(mapped_node.clone())],
                        );
                    }
                    _ => {}
                }

                // Only tasks are namespaced; asset invocations keep bare names.
                // Skip collect steps — they're virtual and handled above.
                if matches!(
                    invocation.node_type,
                    rivers_core::composition::InvokedNodeType::Task
                ) && !matches!(
                    invocation.invocation_kind,
                    InvocationKind::Collect { .. } | InvocationKind::CollectStream { .. }
                ) {
                    let bare_name = invocation.name.rsplit('/').next().unwrap().to_string();
                    task_namespaced_entries
                        .entry(bare_name)
                        .or_default()
                        .push(invocation.name.clone());
                    graph_task_names
                        .entry(graph_name.clone())
                        .or_default()
                        .push(invocation.name.clone());
                }
                composition_bindings
                    .insert(invocation.name.clone(), invocation.input_bindings.clone());
            }
        }
    }
    for (name, deps) in collect_steps {
        unresolved_graph.insert(name, deps);
    }
    let composition_task_names: HashSet<String> = composition_bindings.keys().cloned().collect();

    /// Positional arguments fill the upstream parameters in order; a parameter
    /// left unbound resolves by name. Every argument is a dependency.
    fn build_remap_from_bindings(
        py: Python,
        bindings: Vec<InputBinding>,
        wraps: &Option<Py<PyAny>>,
        resource_keys: &HashSet<&String>,
    ) -> PyResult<(Vec<NodeRef>, HashMap<String, String>)> {
        let mut remap = HashMap::new();
        let mut all_deps: Vec<NodeRef> = Vec::new();
        let mut positional = Vec::new();
        for b in bindings {
            match b.param_name {
                Some(pname) => {
                    all_deps.push(NodeRef::ByName(b.upstream_node_name.clone()));
                    remap.insert(pname, b.upstream_node_name);
                }
                None => positional.push(b.upstream_node_name),
            }
        }

        let mut positional = positional.into_iter();
        if let Some(wraps) = wraps {
            for pname in upstream_params(py, wraps, resource_keys)? {
                if remap.contains_key(&pname) {
                    continue;
                }
                match positional.next() {
                    Some(upstream) => {
                        all_deps.push(NodeRef::ByName(upstream.clone()));
                        remap.insert(pname, upstream);
                    }
                    None => all_deps.push(NodeRef::ByName(pname)),
                }
            }
        }
        all_deps.extend(positional.map(NodeRef::ByName));
        Ok((all_deps, remap))
    }

    for task_py in tasks {
        if let Ok(py_task) = task_py.cast_bound::<PyTask>(py) {
            let task = &py_task.get().inner;
            let task_name = task
                .name
                .clone()
                .ok_or_else(|| AssetDefinitionError::new_err("Task has no name"))?;

            let task_ref: Py<PyTask> = py_task.clone().unbind();

            // Create namespaced entries for each graph that uses this task.
            let has_namespaced =
                if let Some(namespaced_names) = task_namespaced_entries.remove(&task_name) {
                    for ns_name in &namespaced_names {
                        let bindings = composition_bindings.remove(ns_name).unwrap_or_default();
                        let (comp_deps, remap) =
                            build_remap_from_bindings(py, bindings, &task.wraps, resource_keys)?;
                        // Inherit partitions_def and partition mappings from parent graph asset.
                        // Only propagate mapping entries for this task's actual deps.
                        let graph_name = ns_name.split('/').next().unwrap_or("");
                        let pd_override = graph_partitions_def.get(graph_name).cloned();
                        let dep_names: HashSet<&str> = comp_deps
                            .iter()
                            .filter_map(|d| match d {
                                NodeRef::ByName(n) => Some(n.as_str()),
                                _ => None,
                            })
                            .collect();
                        fn filter_by_deps<V: Clone>(
                            source: Option<&HashMap<String, V>>,
                            dep_names: &HashSet<&str>,
                        ) -> Option<HashMap<String, V>> {
                            source
                                .map(|full| {
                                    full.iter()
                                        .filter(|(k, _)| dep_names.contains(k.as_str()))
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect::<HashMap<_, _>>()
                                })
                                .filter(|m| !m.is_empty())
                        }

                        let pm_override =
                            filter_by_deps(graph_partition_mappings.get(graph_name), &dep_names);
                        let ioh_override = graph_input_io_handlers
                            .get(graph_name)
                            .map(|full| {
                                full.iter()
                                    .filter(|(k, _)| dep_names.contains(k.as_str()))
                                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                                    .collect::<HashMap<_, _>>()
                            })
                            .filter(|m| !m.is_empty());
                        let meta_override =
                            filter_by_deps(graph_input_metadata.get(graph_name), &dep_names);
                        unresolved_graph.insert(ns_name.clone(), comp_deps);
                        node_map.insert(
                            ns_name.clone(),
                            ResolvedNode::Task(ResolvedTask::new(
                                py,
                                task_ref.clone_ref(py),
                                Some(remap),
                                Some(graph_name.to_string()),
                                pd_override,
                                pm_override,
                                ioh_override,
                                meta_override,
                                partition_defs,
                            )?),
                        );
                    }
                    true
                } else {
                    false
                };

            // Bare entries take deps from parameter names, which may not exist
            // as graph nodes — skip when this task is used exclusively inside
            // graph compositions.
            if !has_namespaced {
                let mut deps = Vec::new();
                if let Some(ref wraps) = task.wraps {
                    append_deps(py, &mut deps, wraps, resource_keys)?;
                }
                unresolved_graph.insert(task_name.clone(), deps);
                node_map.insert(
                    task_name,
                    ResolvedNode::Task(ResolvedTask::new(
                        py,
                        task_ref,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        partition_defs,
                    )?),
                );
            }
        } else if let Ok(py_bash) = task_py.cast_bound::<PyBashTask>(py) {
            let task_name = py_bash.get().name.clone();

            let bash_ref: Py<PyBashTask> = py_bash.clone().unbind();

            // Create namespaced entries for each graph that uses this BashTask.
            let has_namespaced =
                if let Some(namespaced_names) = task_namespaced_entries.remove(&task_name) {
                    for ns_name in &namespaced_names {
                        let bindings = composition_bindings.remove(ns_name).unwrap_or_default();
                        let deps: Vec<NodeRef> = bindings
                            .iter()
                            .map(|b| NodeRef::ByName(b.upstream_node_name.clone()))
                            .collect();
                        // Inherit partitions_def and partition mappings from parent graph asset.
                        let graph_name = ns_name.split('/').next().unwrap_or("");
                        let pd_override = graph_partitions_def.get(graph_name).cloned();
                        let pm_override = graph_partition_mappings
                            .get(graph_name)
                            .map(|full_pm| {
                                let dep_names: HashSet<&str> = deps
                                    .iter()
                                    .filter_map(|d| match d {
                                        NodeRef::ByName(n) => Some(n.as_str()),
                                        _ => None,
                                    })
                                    .collect();
                                full_pm
                                    .iter()
                                    .filter(|(k, _)| dep_names.contains(k.as_str()))
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect::<HashMap<_, _>>()
                            })
                            .filter(|m| !m.is_empty());
                        unresolved_graph.insert(ns_name.clone(), deps);
                        node_map.insert(
                            ns_name.clone(),
                            ResolvedNode::BashTask(ResolvedBashTask::new(
                                py,
                                bash_ref.clone_ref(py),
                                Some(graph_name.to_string()),
                                pd_override,
                                pm_override,
                            )),
                        );
                    }
                    true
                } else {
                    false
                };

            if !has_namespaced {
                unresolved_graph.insert(task_name.clone(), Vec::new());
                node_map.insert(
                    task_name,
                    ResolvedNode::BashTask(ResolvedBashTask::new(py, bash_ref, None, None, None)),
                );
            }
        } else {
            return Err(AssetDefinitionError::new_err(
                "tasks must contain Task or BashTask instances",
            ));
        }
    }

    Ok(UnresolvedGraph {
        graph: unresolved_graph,
        node_map,
        composition_task_names,
        graph_task_names,
        step_kinds,
    })
}

/// Validate partition mappings on all graph edges.
///
/// Rules:
/// - Identity mapping: both upstream and downstream must have partitions of the same type
/// - TimeWindow mapping: both must have TimeWindow partitions
/// - Static mapping: all keys in the mapping must be valid partition keys
/// - AllPartitions: always valid (fan-out / fan-in)
/// - If downstream is partitioned and upstream is partitioned, default mapping is Identity
/// - If downstream is partitioned and upstream is NOT partitioned, no mapping needed (unpartitioned dep is shared)
/// - If downstream is NOT partitioned and upstream IS partitioned, error (need AllPartitions mapping)
pub(super) fn validate_partition_mappings(
    py: Python,
    node_map: &HashMap<String, ResolvedNode>,
    unresolved_graph: &BTreeMap<String, Vec<NodeRef>>,
) -> PyResult<()> {
    // PyO3's per-variant constructors bypass the factory validation, so the
    // resolve boundary re-checks every definition before edge validation.
    let mut names: Vec<&String> = node_map.keys().collect();
    names.sort_unstable();
    for node_name in names {
        if let Some(def) = node_map[node_name].partitions_def() {
            def.validate_definition().map_err(|e| {
                PartitionValidationError::new_err(format!(
                    "Asset '{}': invalid partitions definition: {}",
                    node_name,
                    e.value(py)
                ))
            })?;
        }
    }
    for (node_name, deps) in unresolved_graph {
        let downstream_node = match node_map.get(node_name) {
            Some(n) => n,
            None => continue,
        };
        let downstream_partitions = downstream_node.partitions_def();
        let explicit_mappings = downstream_node.partition_mapping();

        for dep in deps {
            let dep_name = match dep {
                NodeRef::ByName(name) => name.as_str(),
                _ => continue,
            };

            let upstream_node = match node_map.get(dep_name) {
                Some(n) => n,
                None => continue,
            };
            let upstream_partitions = upstream_node.partitions_def();

            let mapping = explicit_mappings.as_ref().and_then(|m| m.get(dep_name));

            PartitionMapping::validate_edge(mapping, downstream_partitions, upstream_partitions)
                .map_err(|e| {
                    PartitionValidationError::new_err(format!(
                        "Asset '{}' depends on '{}': {}",
                        node_name, dep_name, e
                    ))
                })?;
        }

        // Check that all explicit mapping keys reference actual dependencies
        if let Some(ref mappings) = explicit_mappings {
            let dep_names: HashSet<&str> = deps
                .iter()
                .filter_map(|d| match d {
                    NodeRef::ByName(name) => Some(name.as_str()),
                    _ => None,
                })
                .collect();
            for mapping_key in mappings.keys() {
                if !dep_names.contains(mapping_key.as_str()) {
                    return Err(PartitionValidationError::new_err(format!(
                        "Asset '{}': partition_mapping references '{}' which is not a dependency",
                        node_name, mapping_key
                    )));
                }
            }
        }
    }
    Ok(())
}
