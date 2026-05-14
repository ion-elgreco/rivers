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
#[derive(Debug)]
pub enum IOHandler {
    Instance(Py<PyAny>),
    ResourceRef(String),
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
    /// Resolve a ResourceRef to an Instance in-place.
    /// `io_handler_keys` contains resource keys pre-validated as IOHandler at extraction time.
    pub fn resolve_in_place(
        &mut self,
        py: Python,
        io_handlers: &HashMap<String, &Py<PyAny>>,
        other_resource_keys: &HashSet<&String>,
        asset_name: &str,
    ) -> PyResult<()> {
        if let IOHandler::ResourceRef(key) = self {
            if other_resource_keys.contains(key) {
                return Err(AssetDefinitionError::new_err(format!(
                    "Asset '{}': io_handler references resource '{}' which does not implement \
                     the IOHandler protocol (handle_output + load_input)",
                    asset_name, key
                )));
            }
            let resource = io_handlers.get(key.as_str()).ok_or_else(|| {
                AssetDefinitionError::new_err(format!(
                    "Asset '{}': io_handler references resource '{}' which is not in resources",
                    asset_name, key
                ))
            })?;
            *self = IOHandler::Instance(resource.clone_ref(py));
        }
        Ok(())
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        match self {
            IOHandler::Instance(h) => IOHandler::Instance(h.clone_ref(py)),
            IOHandler::ResourceRef(k) => IOHandler::ResourceRef(k.clone()),
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
