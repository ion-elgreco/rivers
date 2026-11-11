use pyo3::prelude::*;
use rivers_core::storage::surrealdb_backend::{BUILD_STAMPS, ProcessStatus, SchemaStatus};

#[pyclass(name = "ProcessStatus", frozen, get_all, module = "rivers._core")]
#[derive(Clone)]
pub struct PyProcessStatus {
    pub name: String,
    pub code_location: bool,
    pub writes: bool,
    pub schema_version: Option<u32>,
    pub rivers_version: Option<String>,
    pub seen: Option<i64>,
    pub stops: bool,
}

impl From<ProcessStatus> for PyProcessStatus {
    fn from(p: ProcessStatus) -> Self {
        Self {
            name: p.name,
            code_location: p.code_location,
            writes: p.writes,
            schema_version: p.schema_version,
            rivers_version: p.rivers_version,
            seen: p.seen,
            stops: p.stops,
        }
    }
}

#[pyclass(name = "SchemaStatus", frozen, get_all, module = "rivers._core")]
#[derive(Clone)]
pub struct PySchemaStatus {
    pub version: Option<u32>,
    pub min_reader: Option<u32>,
    pub min_writer: Option<u32>,
    pub build_version: u32,
    pub build_min_reader: u32,
    pub build_min_writer: u32,
    pub processes: Vec<PyProcessStatus>,
}

impl From<SchemaStatus> for PySchemaStatus {
    fn from(s: SchemaStatus) -> Self {
        Self {
            version: s.stamps.map(|stamps| stamps.version),
            min_reader: s.stamps.map(|stamps| stamps.min_reader),
            min_writer: s.stamps.map(|stamps| stamps.min_writer),
            build_version: BUILD_STAMPS.version,
            build_min_reader: BUILD_STAMPS.min_reader,
            build_min_writer: BUILD_STAMPS.min_writer,
            processes: s.processes.into_iter().map(Into::into).collect(),
        }
    }
}
