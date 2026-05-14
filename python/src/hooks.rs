//! Hook decorators — @Hook.success and @Hook.failure for post-execution callbacks.
//!
//! `PyHook` enum (Success / Failure) supports both bare decorator `@Hook.success` and
//! parameterized `@Hook.success(name="x")` forms via `__call__` rebinding. Hooks receive
//! a `HookContext` with asset name, run ID, and materialization metadata.
use pyo3::prelude::*;

use crate::context::hook::PyHookContext;
use crate::errors::AssetDefinitionError;

/// A hook that fires after asset execution.
///
/// Usage:
/// ```python
/// @Hook.success
/// def my_hook(context: rs.HookContext):
///     print(f"Asset {context.asset_name} succeeded!")
///
/// @Hook.failure(name="alert")
/// def on_fail(context: rs.HookContext):
///     ...
///
/// isinstance(my_hook, Hook.Success)  # True
/// isinstance(my_hook, Hook)          # True
/// ```
#[pyclass(name = "Hook", frozen, module = "rivers._core")]
pub enum PyHook {
    Success {
        _func: Option<Py<PyAny>>,
        _name: String,
    },
    Failure {
        _func: Option<Py<PyAny>>,
        _name: String,
    },
}

fn derive_name(py: Python, func: Option<&Py<PyAny>>, name: Option<&str>) -> PyResult<String> {
    match (func, name) {
        (_, Some(n)) => Ok(n.to_string()),
        (Some(f), None) => Ok(f.getattr(py, "__name__")?.to_string()),
        (None, None) => Ok(String::new()),
    }
}

impl PyHook {
    pub(crate) fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub(crate) fn is_failure(&self) -> bool {
        matches!(self, Self::Failure { .. })
    }

    pub(crate) fn func(&self) -> Option<&Py<PyAny>> {
        match self {
            Self::Success { _func, .. } | Self::Failure { _func, .. } => _func.as_ref(),
        }
    }

    pub(crate) fn resolve_name(&self) -> &str {
        match self {
            Self::Success { _name, .. } | Self::Failure { _name, .. } => _name,
        }
    }
}

#[pymethods]
impl PyHook {
    #[staticmethod]
    #[pyo3(name = "success", signature = (func=None, *, name=None))]
    fn success_ctor(py: Python, func: Option<Py<PyAny>>, name: Option<String>) -> PyResult<Self> {
        let _name = derive_name(py, func.as_ref(), name.as_deref())?;
        Ok(Self::Success { _func: func, _name })
    }

    #[staticmethod]
    #[pyo3(name = "failure", signature = (func=None, *, name=None))]
    fn failure_ctor(py: Python, func: Option<Py<PyAny>>, name: Option<String>) -> PyResult<Self> {
        let _name = derive_name(py, func.as_ref(), name.as_deref())?;
        Ok(Self::Failure { _func: func, _name })
    }

    #[getter]
    fn name(&self) -> &str {
        self.resolve_name()
    }

    fn __repr__(&self) -> String {
        let kind = match self {
            Self::Success { .. } => "Success",
            Self::Failure { .. } => "Failure",
        };
        format!("Hook.{}(name='{}')", kind, self.resolve_name())
    }

    /// When used as a decorator with arguments (e.g. `@Hook.success(name="x")`),
    /// the Hook is created without a func. `__call__` is then invoked with the
    /// decorated function, and we return a new Hook with the func set.
    #[pyo3(signature = (*args))]
    fn __call__(&self, py: Python, args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<Self> {
        if self.func().is_some() {
            return Err(AssetDefinitionError::new_err(
                "Hook is already bound to a function and cannot be called as a decorator again",
            ));
        }
        if args.len() != 1 {
            return Err(AssetDefinitionError::new_err(
                "Hook decorator expects exactly one argument (the function to wrap)",
            ));
        }
        let f: Py<PyAny> = args.get_item(0)?.unbind();
        let existing = self.resolve_name();
        let _name = derive_name(py, Some(&f), (!existing.is_empty()).then_some(existing))?;
        Ok(match self {
            Self::Success { .. } => Self::Success {
                _func: Some(f),
                _name,
            },
            Self::Failure { .. } => Self::Failure {
                _func: Some(f),
                _name,
            },
        })
    }
}

pub fn register_hooks_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "hooks", [
        PyHookContext as "HookContext",
        PyHook as "Hook",
    ])
}
