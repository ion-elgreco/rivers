use std::collections::HashMap;

use pyo3::prelude::*;

use super::io_handler::IOHandler;
use crate::partitions::mapping::PartitionMapping;

/// A dependency definition for an asset.
///
/// Created via `AssetDef.input(...)` (data dependency, matched to a function parameter)
/// or `AssetDef.dep(...)` (lineage-only, no data loaded).
/// Used in the `deps` parameter of `@Asset(...)`, `Asset.from_multi(...)`, and `Asset.from_graph(...)`.
#[pyclass(name = "DepDef", frozen, module = "rivers._core")]
pub struct DepDef {
    #[pyo3(get)]
    pub name: String,
    pub io_handler: Option<IOHandler>,
    #[pyo3(get)]
    pub partition_mapping: Option<PartitionMapping>,
    #[pyo3(get)]
    pub metadata: Option<HashMap<String, String>>,
    /// True = data input (matched to fn param), False = lineage-only dep.
    #[pyo3(get)]
    pub is_input: bool,
}

#[pymethods]
impl DepDef {
    fn __repr__(&self) -> String {
        let kind = if self.is_input { "input" } else { "dep" };
        format!("DepDef.{}('{}')", kind, self.name)
    }

    fn __str__(&self) -> String {
        format!("DepDef(name={}, is_input={})", self.name, self.is_input)
    }

    /// A class-form asset defined in a script ships to loky workers by value,
    /// `deps` included.
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, DepDefParts)> {
        let Self {
            name,
            io_handler,
            partition_mapping,
            metadata,
            is_input,
        } = self;
        let ctor = py.import("rivers._core")?.getattr("_reconstruct_dep_def")?;
        Ok((
            ctor,
            (
                name.clone(),
                io_handler.as_ref().map(|h| h.to_object(py)),
                partition_mapping.clone(),
                metadata.clone(),
                *is_input,
            ),
        ))
    }
}

/// `DepDef`'s pickled state, in `_reconstruct_dep_def` order.
type DepDefParts = (
    String,
    Option<Py<PyAny>>,
    Option<PartitionMapping>,
    Option<HashMap<String, String>>,
    bool,
);

/// Rebuild a `DepDef` from `__reduce__`'s parts; it has no constructor.
#[pyfunction]
pub fn _reconstruct_dep_def(
    name: String,
    io_handler: Option<IOHandler>,
    partition_mapping: Option<PartitionMapping>,
    metadata: Option<HashMap<String, String>>,
    is_input: bool,
) -> DepDef {
    DepDef {
        name,
        io_handler,
        partition_mapping,
        metadata,
        is_input,
    }
}
