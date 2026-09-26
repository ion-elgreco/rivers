//! IO handler protocol and validation for custom asset persistence backends.
//!
//! `IOHandler` wraps either a `Py<PyAny>` instance implementing `handle_output` / `load_input`,
//! or a string resource reference resolved at execution time. `BaseIOHandler` is the Pydantic
//! base class exposed to Python. Validation ensures the protocol methods exist at definition time.
use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;

use crate::errors::AssetDefinitionError;

/// IOHandler enum: either an inline handler instance or a string resource reference.
///
/// - `Instance(Py<PyAny>)` — an object with `handle_output` and `load_input` methods
/// - `ResourceRef(String)` — a key into the repository's resources dict, resolved at execution time
/// - `Resource(Py<PyAny>)` — the handler `CodeRepository.resolve()` found for a `ResourceRef`.
///   Only that repository's resolved nodes hold it; the definition keeps the key.
#[derive(Debug)]
pub enum IOHandler {
    Instance(Py<PyAny>),
    ResourceRef(String),
    Resource(Py<PyAny>),
}

impl<'py> FromPyObject<'py, '_> for IOHandler {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        if ob.is_instance_of::<pyo3::types::PyString>() {
            let key: String = ob.extract()?;
            Ok(IOHandler::ResourceRef(key))
        } else {
            validate_io_handler_protocol(ob.py(), &ob.as_unbound().clone_ref(ob.py()))?;
            Ok(IOHandler::Instance(ob.as_unbound().clone_ref(ob.py())))
        }
    }
}

impl IOHandler {
    /// Resolve a ResourceRef to a Resource in-place.
    /// `io_handler_keys` contains resource keys pre-validated as IOHandler at extraction time.
    /// `kind` and `name` name the owner in errors, e.g. `Asset 'orders'`.
    pub fn resolve_in_place(
        &mut self,
        py: Python,
        io_handlers: &HashMap<String, &Py<PyAny>>,
        other_resource_keys: &HashSet<&String>,
        kind: &str,
        name: &str,
    ) -> PyResult<()> {
        if let IOHandler::ResourceRef(key) = self {
            if other_resource_keys.contains(key) {
                return Err(AssetDefinitionError::new_err(format!(
                    "{} '{}': io_handler references resource '{}' which does not implement \
                     the IOHandler protocol (handle_output + load_input)",
                    kind, name, key
                )));
            }
            let resource = io_handlers.get(key.as_str()).ok_or_else(|| {
                AssetDefinitionError::new_err(format!(
                    "{} '{}': io_handler references resource '{}' which is not in resources",
                    kind, name, key
                ))
            })?;
            *self = IOHandler::Resource(resource.clone_ref(py));
        }
        Ok(())
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        match self {
            IOHandler::Instance(h) => IOHandler::Instance(h.clone_ref(py)),
            IOHandler::ResourceRef(k) => IOHandler::ResourceRef(k.clone()),
            IOHandler::Resource(h) => IOHandler::Resource(h.clone_ref(py)),
        }
    }

    /// The handler instance, or None for an unresolved resource key.
    pub fn handler(&self) -> Option<&Py<PyAny>> {
        match self {
            IOHandler::Instance(h) | IOHandler::Resource(h) => Some(h),
            IOHandler::ResourceRef(_) => None,
        }
    }

    /// The handler instance, or the resource key string.
    pub fn to_object(&self, py: Python) -> Py<PyAny> {
        match self {
            IOHandler::Instance(h) | IOHandler::Resource(h) => h.clone_ref(py),
            IOHandler::ResourceRef(k) => pyo3::types::PyString::new(py, k).unbind().into_any(),
        }
    }
}

pub fn validate_io_handler_protocol(py: Python, handler: &Py<PyAny>) -> PyResult<()> {
    let io_handler_cls = py.import("rivers")?.getattr("BaseIOHandler")?;
    let isinstance = py.import("builtins")?.getattr("isinstance")?;
    if !isinstance
        .call1((handler.bind(py), &io_handler_cls))?
        .is_truthy()?
    {
        return Err(AssetDefinitionError::new_err(
            "io_handler must be a BaseIOHandler subclass instance or a string resource reference",
        ));
    }
    Ok(())
}
