//! Collect specifications for fan-out gather steps (barrier and streaming).
use std::collections::HashMap;

use pyo3::prelude::*;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use rivers_core::execution::plan::{ExecutionPlan, ExecutionStep, StepKind};

use super::super::ops;
use super::execute::ParallelBackend;

impl ParallelBackend {
    /// Build `_CollectLoadSpec` / `_CollectStreamLoadSpec` input overrides for a step's
    /// collect dependencies.
    pub(super) fn build_collect_input_overrides(
        py: Python,
        plan: &ExecutionPlan,
        step: &ExecutionStep,
        node_map: &HashMap<String, ResolvedNode>,
        partition_key: &Option<PyPartitionKey>,
        registry: &IOHandlerRegistry,
        mapped_instance_keys: &HashMap<String, Vec<String>>,
    ) -> Result<HashMap<String, Py<PyAny>>, String> {
        let mut overrides: HashMap<String, Py<PyAny>> = HashMap::new();
        for dep_step in plan
            .steps
            .iter()
            .filter(|s| step.plan_dependencies.contains(&s.name))
        {
            let mapped_step_name = match &dep_step.kind {
                StepKind::Collect { mapped_step } | StepKind::CollectStream { mapped_step, .. } => {
                    Some(mapped_step.as_str())
                }
                _ => None,
            };
            let Some(mapped_step) = mapped_step_name else {
                continue;
            };

            let io_handler_ref = node_map
                .get(mapped_step)
                .and_then(|mn| mn.callable(py).ok())
                .map(|func| {
                    super::worker_args::resolve_io_handler_ref(
                        py,
                        mapped_step,
                        &func,
                        node_map,
                        registry,
                    )
                })
                .unwrap_or_else(|| registry.default_handler(py));

            let result = match &dep_step.kind {
                StepKind::Collect { mapped_step } => Self::build_collect_load_spec(
                    py,
                    mapped_step,
                    node_map,
                    partition_key,
                    mapped_instance_keys,
                    &io_handler_ref,
                ),
                StepKind::CollectStream { mapped_step, .. } => {
                    Self::build_collect_stream_load_spec(
                        py,
                        mapped_step,
                        node_map,
                        partition_key,
                        mapped_instance_keys,
                        &io_handler_ref,
                    )
                }
                _ => unreachable!(),
            };
            match result {
                Ok(v) => {
                    overrides.insert(dep_step.name.clone(), v);
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to build collect spec for '{}': {e}",
                        dep_step.name
                    ));
                }
            }
        }
        Ok(overrides)
    }

    fn build_collect_specs_list<'py>(
        py: Python<'py>,
        mapped_step: &str,
        node_map: &HashMap<String, ResolvedNode>,
        partition_key: &Option<PyPartitionKey>,
        mapped_instance_keys: &HashMap<String, Vec<String>>,
        io_handler_ref: &Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let mapped_node = node_map
            .get(mapped_step)
            .expect("mapped step must be in node_map — invalid plan");
        let keys = mapped_instance_keys
            .get(mapped_step)
            .cloned()
            .unwrap_or_default();
        let specs = Self::build_instance_io_specs(
            py,
            mapped_step,
            mapped_node,
            partition_key,
            &keys,
            io_handler_ref,
        )?;
        Ok(specs.unbind().into_any())
    }

    fn build_collect_load_spec(
        py: Python,
        mapped_step: &str,
        node_map: &HashMap<String, ResolvedNode>,
        partition_key: &Option<PyPartitionKey>,
        mapped_instance_keys: &HashMap<String, Vec<String>>,
        io_handler_ref: &Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let specs = Self::build_collect_specs_list(
            py,
            mapped_step,
            node_map,
            partition_key,
            mapped_instance_keys,
            io_handler_ref,
        )?;
        let collect = super::worker::PyCollectLoadSpec { specs };
        Ok(Py::new(py, collect)?.into_any())
    }

    fn build_collect_stream_load_spec(
        py: Python,
        mapped_step: &str,
        node_map: &HashMap<String, ResolvedNode>,
        partition_key: &Option<PyPartitionKey>,
        mapped_instance_keys: &HashMap<String, Vec<String>>,
        io_handler_ref: &Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let specs = Self::build_collect_specs_list(
            py,
            mapped_step,
            node_map,
            partition_key,
            mapped_instance_keys,
            io_handler_ref,
        )?;
        let collect = super::worker::PyCollectStreamLoadSpec { specs };
        Ok(Py::new(py, collect)?.into_any())
    }

    fn build_instance_io_specs<'py>(
        py: Python<'py>,
        mapped_step: &str,
        mapped_node: &ResolvedNode,
        partition_key: &Option<PyPartitionKey>,
        keys: &[String],
        io_handler_ref: &Py<PyAny>,
    ) -> PyResult<pyo3::Bound<'py, pyo3::types::PyList>> {
        let metadata = mapped_node.metadata();
        let partition = ops::build_partition_context(mapped_node, partition_key)?;

        let specs = pyo3::types::PyList::empty(py);
        for key in keys {
            let instance_name = format!("{}__{}", mapped_step, key);
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("asset_name", &instance_name)?;
            kwargs.set_item("downstream_asset", "collect")?;
            kwargs.set_item("asset_metadata", metadata.clone())?;
            kwargs.set_item("partition", partition.clone())?;
            kwargs.set_item("type_hint", py.None())?;
            let spec = super::worker::PyIOLoadSpec {
                handler: io_handler_ref.clone_ref(py),
                input_context_kwargs: kwargs.unbind().into_any(),
            };
            specs.append(Py::new(py, spec)?)?;
        }
        Ok(specs)
    }
}
