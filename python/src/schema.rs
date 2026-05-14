//! Arrow schema serialization — IPC byte conversion for MetadataValue.Schema.
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_ipc::convert::fb_to_schema;
use arrow_schema::Schema;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::PySchema;

#[pyclass(name = "Schema", frozen, skip_from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub struct PySchemaWrapper {
    inner: Arc<Schema>,
}

impl PySchemaWrapper {
    pub fn from_arc(schema: Arc<Schema>) -> Self {
        Self { inner: schema }
    }
}

/// Serialize an Arrow schema to IPC bytes.
pub fn schema_to_ipc_bytes(schema: &Schema) -> Vec<u8> {
    let generator = arrow_ipc::writer::IpcDataGenerator::default();
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut arrow_ipc::writer::DictionaryTracker::new(false),
        &Default::default(),
    );
    encoded.ipc_message.to_vec()
}

/// Deserialize an Arrow schema from IPC bytes.
pub fn schema_from_ipc_bytes(data: &[u8]) -> PyResult<Arc<Schema>> {
    let msg = arrow_ipc::root_as_message(data)
        .map_err(|e| PyValueError::new_err(format!("Invalid IPC data: {e}")))?;
    let ipc_schema = msg
        .header_as_schema()
        .ok_or_else(|| PyValueError::new_err("IPC message is not a schema"))?;
    let schema = fb_to_schema(ipc_schema);
    Ok(Arc::new(schema))
}

#[pymethods]
impl PySchemaWrapper {
    #[new]
    fn new(schema: PySchema) -> Self {
        Self {
            inner: schema.into_inner(),
        }
    }

    /// Field names as list[str].
    #[getter]
    fn names(&self) -> Vec<String> {
        self.inner
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn __len__(&self) -> usize {
        self.inner.fields().len()
    }

    fn __repr__(&self) -> String {
        let fields: Vec<String> = self
            .inner
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect();
        format!("Schema({{{}}})", fields.join(", "))
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        schema_to_ipc_bytes(&self.inner).hash(&mut hasher);
        hasher.finish()
    }

    /// Serialize to IPC bytes.
    fn to_ipc(&self) -> Vec<u8> {
        schema_to_ipc_bytes(&self.inner)
    }

    /// Deserialize from IPC bytes.
    #[staticmethod]
    fn from_ipc(data: &[u8]) -> PyResult<Self> {
        let schema = schema_from_ipc_bytes(data)?;
        Ok(Self { inner: schema })
    }

    /// Export via Arrow PyCapsule interface (__arrow_c_schema__).
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        pyo3_arrow::ffi::to_schema_pycapsule(py, self.inner.as_ref()).map_err(|e| e.into())
    }
}
