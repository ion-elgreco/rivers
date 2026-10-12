//! Configuration and resource management — Pydantic model wrappers crossing the PyO3 boundary.
pub(crate) mod run_config;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyType};

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

    /// The class and the keyword arguments a run builds this resource from
    /// with `overrides`: an instance's own fields ([`resource_fields`]) with
    /// the overrides laid over them, a class's the overrides alone. An IO
    /// handler cannot be overridden: nodes hold it through the registry.
    pub fn run_kwargs<'py>(
        &self,
        py: Python<'py>,
        overrides: &Bound<'py, PyDict>,
    ) -> PyResult<(Bound<'py, PyType>, Bound<'py, PyDict>)> {
        match self {
            ResourceVariant::IOHandler(_) => Err(ConfigurationError::new_err(
                "an IO handler cannot be overridden per run",
            )),
            ResourceVariant::PydanticModel(cls) => {
                Ok((cls.bind(py).cast::<PyType>()?.clone(), overrides.copy()?))
            }
            ResourceVariant::PydanticModelInstance(inst) | ResourceVariant::Resource(inst) => {
                let inst = inst.bind(py);
                let fields = resource_fields(inst)?.cast_into::<PyDict>()?;
                lay_over(inst, &fields, overrides)?;
                Ok((inst.get_type(), fields))
            }
        }
    }

    /// This resource rebuilt for one run with `overrides`, validated by its
    /// class: a fresh instance of the same kind (a class becomes an instance).
    pub fn for_run(
        &self,
        py: Python<'_>,
        key: &str,
        overrides: &Bound<'_, PyDict>,
    ) -> PyResult<ResourceVariant> {
        let failed = |e: PyErr| {
            let reason = e.value(py).str().map(|s| s.to_string()).unwrap_or_default();
            ConfigurationError::new_err(format!("config: resources.{key}: {reason}"))
        };
        let (cls, kwargs) = self.run_kwargs(py, overrides).map_err(failed)?;
        let built = cls.call((), Some(&kwargs)).map_err(failed)?.unbind();
        Ok(match self {
            ResourceVariant::Resource(_) => ResourceVariant::Resource(built),
            _ => ResourceVariant::PydanticModelInstance(built),
        })
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

/// `overrides` laid over `fields`, the `model_dump` of `model`. A nested
/// model takes its overrides field by field too, so the fields a document
/// leaves out keep their values at any depth, secrets included.
fn lay_over(
    model: &Bound<'_, PyAny>,
    fields: &Bound<'_, PyDict>,
    overrides: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let base_model = model.py().import("pydantic")?.getattr("BaseModel")?;
    for (key, value) in overrides.iter() {
        if let Ok(nested_overrides) = value.cast::<PyDict>()
            && let Some(nested_fields) = fields.get_item(&key)?
            && let Ok(nested_fields) = nested_fields.cast::<PyDict>()
            && let Ok(name) = key.cast::<PyString>()
            && let Ok(nested) = model.getattr(name)
            && nested.is_instance(&base_model)?
        {
            lay_over(&nested, nested_fields, nested_overrides)?;
        } else {
            fields.set_item(key, value)?;
        }
    }
    Ok(())
}

/// The fields `resource`'s class rebuilds it from, for a run or in a worker
/// process. Python mode keeps each secret a `SecretStr` that pickles with
/// its value, where JSON mode would give pydantic's mask; `round_trip`
/// dumps a `Json` field as the text it validates from and leaves computed
/// fields out.
pub(crate) fn resource_fields<'py>(resource: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let kwargs = PyDict::new(resource.py());
    kwargs.set_item("round_trip", true)?;
    resource.call_method("model_dump", (), Some(&kwargs))
}
