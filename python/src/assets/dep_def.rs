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
}
