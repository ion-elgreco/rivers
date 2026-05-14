//! Composition PyO3 wrappers and input binding extraction.
pub use rivers_core::composition::{
    CompositionContext, InputBinding, InvocationKind, InvokedNode, InvokedNodeOutput,
    InvokedNodeType, enter_composition, exit_composition, is_in_composition,
    observe_collect_invocation, observe_invocation, observe_map_invocation,
};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::task::py_task::PyTask;

fn default_output() -> String {
    rivers_core::composition::DEFAULT_OUTPUT_NAME.to_string()
}

fn try_extract_binding(
    value: &Bound<'_, PyAny>,
    param_name: Option<String>,
) -> PyResult<Option<InputBinding>> {
    if let Ok(output) = value.extract::<PyInvokedNodeOutput>() {
        Ok(Some(InputBinding {
            upstream_node_name: output.node_name,
            output_name: output.output_name,
            param_name,
        }))
    } else if let Ok(mapped) = value.extract::<PyMappedOutput>() {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "MappedOutput '{}' must be collected via .collect() or .collect_stream() before passing to a task",
            mapped.node_name
        )))
    } else {
        Ok(None)
    }
}

#[pyclass(name = "InvokedNodeOutput", from_py_object, module = "rivers._core")]
#[derive(Debug, Clone)]
pub struct PyInvokedNodeOutput {
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub output_name: String,
}

#[pymethods]
impl PyInvokedNodeOutput {
    fn __repr__(&self) -> String {
        format!(
            "InvokedNodeOutput(node={}, output={})",
            self.node_name, self.output_name
        )
    }

    #[pyo3(signature = (task, *, max_concurrency=None))]
    fn map(
        &self,
        task: PyRef<'_, PyTask>,
        max_concurrency: Option<usize>,
    ) -> PyResult<PyMappedOutput> {
        let task_name = task.inner.name.as_deref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(".map() target Task has no name")
        })?;

        let registered_name = observe_map_invocation(
            task_name,
            &self.node_name,
            &self.output_name,
            max_concurrency,
        );

        Ok(PyMappedOutput {
            node_name: registered_name,
            output_name: default_output(),
        })
    }
}

impl PyInvokedNodeOutput {
    pub fn new(node_name: String, output_name: String) -> Self {
        Self {
            node_name,
            output_name,
        }
    }

    pub fn with_default_output(node_name: String) -> Self {
        Self::new(node_name, default_output())
    }
}

#[pyclass(name = "MappedOutput", from_py_object, module = "rivers._core")]
#[derive(Debug, Clone)]
pub struct PyMappedOutput {
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub output_name: String,
}

#[pymethods]
impl PyMappedOutput {
    fn __repr__(&self) -> String {
        format!("MappedOutput(node={})", self.node_name)
    }

    /// Barrier collect: wait for all map instances, return list[T].
    fn collect(&self) -> PyResult<PyInvokedNodeOutput> {
        Ok(PyInvokedNodeOutput::with_default_output(
            observe_collect_invocation(&self.node_name, false, false),
        ))
    }

    /// Streaming collect: return Generator[T] that yields results as they complete.
    #[pyo3(signature = (*, ordered=false))]
    fn collect_stream(&self, ordered: bool) -> PyResult<PyInvokedNodeOutput> {
        Ok(PyInvokedNodeOutput::with_default_output(
            observe_collect_invocation(&self.node_name, true, ordered),
        ))
    }
}

/// Extract input bindings from call arguments (positional + keyword).
pub fn extract_input_bindings(
    args: &Bound<'_, PyTuple>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<InputBinding>> {
    let mut bindings = Vec::new();
    for arg in args.iter() {
        if let Some(b) = try_extract_binding(&arg, None)? {
            bindings.push(b);
        }
    }
    if let Some(kw) = kwargs {
        for (k, v) in kw.iter() {
            let param: String = k.extract()?;
            if let Some(b) = try_extract_binding(&v, Some(param))? {
                bindings.push(b);
            }
        }
    }
    Ok(bindings)
}
