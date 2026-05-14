//! Configuration and resource management — Pydantic model wrappers crossing the PyO3 boundary.
use pyo3::prelude::*;

use crate::errors::ConfigurationError;

pub enum ResourceVariant {
    PydanticModel(Py<PyAny>),
    PydanticModelInstance(Py<PyAny>),
    Resource(Py<PyAny>),
    IOHandler(Py<PyAny>),
}

impl ResourceVariant {
    pub fn clone_ref(&self, py: Python) -> Self {
        match self {
            ResourceVariant::PydanticModel(c) => ResourceVariant::PydanticModel(c.clone_ref(py)),
            ResourceVariant::PydanticModelInstance(c) => {
                ResourceVariant::PydanticModelInstance(c.clone_ref(py))
            }
            ResourceVariant::Resource(c) => ResourceVariant::Resource(c.clone_ref(py)),
            ResourceVariant::IOHandler(c) => ResourceVariant::IOHandler(c.clone_ref(py)),
        }
    }
}

impl FromPyObject<'_, '_> for ResourceVariant {
    type Error = PyErr;

    fn extract(obj: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        let py = obj.py();
        let builtins = py.import("builtins")?;
        let isinstance = builtins.getattr("isinstance")?;
        let issubclass = builtins.getattr("issubclass")?;

        let inner = obj.to_owned().unbind();

        let io_handler_cls = py.import("rivers")?.getattr("BaseIOHandler")?;
        if isinstance.call1((obj, &io_handler_cls))?.is_truthy()? {
            return Ok(ResourceVariant::IOHandler(inner));
        }

        let resource_cls = py.import("rivers")?.getattr("Resource")?;
        if isinstance.call1((obj, &resource_cls))?.is_truthy()? {
            return Ok(ResourceVariant::Resource(inner));
        }

        let base_model = py.import("pydantic")?.getattr("BaseModel")?;
        if isinstance.call1((obj, &base_model))?.is_truthy()? {
            return Ok(ResourceVariant::PydanticModelInstance(inner));
        }

        let type_cls = builtins.getattr("type")?;
        let is_class = isinstance.call1((obj, &type_cls))?.is_truthy()?;

        if is_class && issubclass.call1((obj, &base_model))?.is_truthy()? {
            return Ok(ResourceVariant::PydanticModel(obj.to_owned().unbind()));
        }

        Err(ConfigurationError::new_err(format!(
            "Expected a Pydantic BaseModel/BaseSettings subclass, Resource instance or BaseIOHandler, got {}",
            obj.get_type().qualname()?
        )))
    }
}

impl<'py> IntoPyObject<'py> for ResourceVariant {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let inner = match self {
            ResourceVariant::PydanticModel(c)
            | ResourceVariant::PydanticModelInstance(c)
            | ResourceVariant::Resource(c)
            | ResourceVariant::IOHandler(c) => c,
        };
        Ok(inner.into_bound(py))
    }
}

impl<'py> IntoPyObject<'py> for &ResourceVariant {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let inner = match self {
            ResourceVariant::PydanticModel(c)
            | ResourceVariant::PydanticModelInstance(c)
            | ResourceVariant::Resource(c)
            | ResourceVariant::IOHandler(c) => c,
        };
        Ok(inner.clone_ref(py).into_bound(py))
    }
}

impl ResourceVariant {
    /// Instantiate or copy a config with optional overrides.
    /// - PydanticModel (class): call constructor with overrides as kwargs
    /// - PydanticModelInstance / Resource (instance): use model_copy(update=overrides) or clone
    pub fn instantiate_config(
        &self,
        py: Python,
        overrides: Option<&Bound<pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        match self {
            ResourceVariant::PydanticModel(cls) => match overrides {
                Some(kwargs) => cls.call(py, (), Some(kwargs)),
                None => cls.call0(py),
            },
            ResourceVariant::PydanticModelInstance(inst)
            | ResourceVariant::Resource(inst)
            | ResourceVariant::IOHandler(inst) => match overrides {
                Some(kwargs) => {
                    let update_kwargs = pyo3::types::PyDict::new(py);
                    update_kwargs.set_item("update", kwargs)?;
                    inst.call_method(py, "model_copy", (), Some(&update_kwargs))
                }
                None => Ok(inst.clone_ref(py)),
            },
        }
    }

    pub fn is_io_handler(&self) -> bool {
        matches!(self, ResourceVariant::IOHandler(_))
    }

    pub fn inner(&self) -> &Py<PyAny> {
        match self {
            ResourceVariant::PydanticModel(c)
            | ResourceVariant::PydanticModelInstance(c)
            | ResourceVariant::Resource(c)
            | ResourceVariant::IOHandler(c) => c,
        }
    }
}
