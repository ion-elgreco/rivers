//! Asset actions — named operations on an asset beyond materialize.
//!
//! An `AssetAction` is the interchange format between the two definition
//! styles: build one once, list it in decorator `actions=[...]`, assign it as
//! a class attribute, or let `@rs.action` build it from a marked classmethod.

use pyo3::prelude::*;

use crate::errors::AssetDefinitionError;
use rivers_core::execution::plan::ActionOrdering;

/// Declared upper bound for what an action does to orchestration state.
/// The outcome describes materialization state, not physical bytes — a Delta
/// `optimize` rewrites every file and is still `Unchanged`.
#[pyclass(
    name = "Outcome",
    module = "rivers._core",
    frozen,
    eq,
    hash,
    from_py_object
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionOutcome {
    /// No materialization-state change; the condition system never sees it.
    Unchanged,
    /// Reports `ActionResult.unchanged()` or `.materialized()` at runtime.
    MayMaterialize,
    /// Clears materialization state (delete).
    Unmaterialize,
    /// Reserved for the built-in `observe` on external assets.
    Observe,
}

impl ActionOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::MayMaterialize => "may_materialize",
            Self::Unmaterialize => "unmaterialize",
            Self::Observe => "observe",
        }
    }
}

/// `(getattr, (EnumType, "Variant"))` — fieldless pyclass enums don't pickle
/// by default, and `@rs.action` marker metadata rides through cloudpickle
/// when a locally-defined class asset ships to a worker by value.
fn reduce_enum_variant<'py, T: pyo3::PyTypeInfo>(
    py: Python<'py>,
    variant: String,
) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyAny>, String))> {
    let getattr = py.import("builtins")?.getattr("getattr")?;
    Ok((getattr, (py.get_type::<T>().into_any(), variant)))
}

#[pymethods]
impl ActionOutcome {
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyAny>, String))> {
        reduce_enum_variant::<Self>(py, format!("{self:?}"))
    }
}

#[pymethods]
impl PyActionConcurrency {
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyAny>, String))> {
        reduce_enum_variant::<Self>(py, format!("{self:?}"))
    }
}

#[pymethods]
impl PyActionOrdering {
    fn __reduce__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, (Bound<'py, PyAny>, String))> {
        reduce_enum_variant::<Self>(py, format!("{self:?}"))
    }
}

/// Whether an action can overlap other work on the same asset.
#[pyclass(
    name = "ActionConcurrency",
    module = "rivers._core",
    frozen,
    eq,
    hash,
    from_py_object
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyActionConcurrency {
    /// May run alongside materialize and other actions.
    Shared,
    /// Joins the asset's implicit one-slot pool: never overlaps materialize
    /// or another exclusive action on the same asset.
    Exclusive,
}

/// Asset order when one action run targets several related assets.
#[pyclass(
    name = "ActionOrdering",
    module = "rivers._core",
    frozen,
    eq,
    hash,
    from_py_object
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyActionOrdering {
    Unordered,
    Topological,
    ReverseTopological,
}

impl From<PyActionOrdering> for ActionOrdering {
    fn from(o: PyActionOrdering) -> Self {
        match o {
            PyActionOrdering::Unordered => Self::Unordered,
            PyActionOrdering::Topological => Self::Topological,
            PyActionOrdering::ReverseTopological => Self::ReverseTopological,
        }
    }
}

/// A named operation on an asset. Reusable: the same instance may be attached
/// to any number of assets (decorator `actions=[...]` or class attribute).
#[pyclass(name = "AssetAction", module = "rivers._core")]
pub struct PyAssetAction {
    pub name: String,
    pub outcome: ActionOutcome,
    pub exclusive: bool,
    pub ordering: ActionOrdering,
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
    pub description: Option<String>,
    pub func: Option<Py<PyAny>>,
    pub is_async: bool,
}

impl PyAssetAction {
    fn with_func(&self, func: Py<PyAny>, is_async: bool) -> Self {
        Self {
            name: self.name.clone(),
            outcome: self.outcome,
            exclusive: self.exclusive,
            ordering: self.ordering,
            retry: self.retry.clone(),
            description: self.description.clone(),
            func: Some(func),
            is_async,
        }
    }
}

/// Verbs with built-in meaning that user actions may not claim.
pub(crate) const RESERVED_ACTION_NAMES: [&str; 3] = ["materialize", "observe", "compose"];

/// Snapshot of one action resolved for execution (refs cloned out under the
/// GIL so dispatch can carry it without borrowing the asset). `exclusive`
/// feeds the phase-3 pool claim and `retry` the phase-4 ladder.
pub(crate) struct ResolvedActionRef {
    pub func: Option<Py<PyAny>>,
    pub is_async: bool,
    #[allow(dead_code)]
    pub exclusive: bool,
    pub ordering: ActionOrdering,
    pub outcome: ActionOutcome,
    #[allow(dead_code)]
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
}

#[pymethods]
impl PyAssetAction {
    #[new]
    #[pyo3(signature = (name, outcome, concurrency=None, ordering=None, retry=None, description=None))]
    fn new(
        name: String,
        outcome: ActionOutcome,
        concurrency: Option<PyActionConcurrency>,
        ordering: Option<PyActionOrdering>,
        retry: Option<Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<Self> {
        if outcome == ActionOutcome::Observe {
            return Err(AssetDefinitionError::new_err(
                "Outcome.Observe is reserved for the built-in observe on external assets",
            ));
        }
        if RESERVED_ACTION_NAMES.contains(&name.as_str()) {
            return Err(AssetDefinitionError::new_err(format!(
                "'{name}' is a reserved verb and cannot be an action name"
            )));
        }
        Ok(Self {
            name,
            outcome,
            exclusive: matches!(concurrency, Some(PyActionConcurrency::Exclusive)),
            ordering: ordering.unwrap_or(PyActionOrdering::Unordered).into(),
            retry: crate::retry::extract_retry_ref(retry)?,
            description,
            func: None,
            is_async: false,
        })
    }

    /// Decorator application: bind the action body.
    fn __call__(&self, py: Python, func: Py<PyAny>) -> PyResult<Self> {
        if self.func.is_some() {
            return Err(AssetDefinitionError::new_err(format!(
                "action '{}' is already bound to a function",
                self.name
            )));
        }
        let is_async = super::decorator::is_coroutine_function(py, &Some(func.clone_ref(py)));
        Ok(self.with_func(func, is_async))
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter(outcome)]
    fn outcome_py(&self) -> ActionOutcome {
        self.outcome
    }

    #[getter]
    fn exclusive(&self) -> bool {
        self.exclusive
    }

    #[getter]
    fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    fn __repr__(&self) -> String {
        format!(
            "AssetAction(name='{}', outcome={:?}, exclusive={})",
            self.name,
            self.outcome,
            if self.exclusive { "True" } else { "False" }
        )
    }
}

/// What an action actually did, reported at runtime. The declared outcome is
/// the upper bound used for planning; the report decides whether downstream
/// goes stale — without it, every no-op merge would cascade the whole graph.
#[pyclass(name = "ActionResult", frozen, module = "rivers._core")]
pub struct PyActionResult {
    pub materialized: bool,
    pub metadata: Vec<(String, crate::metadata::MetadataValue)>,
    pub data_version: Option<String>,
}

#[pymethods]
impl PyActionResult {
    /// The action left materialization state untouched — downstream stays put.
    /// `metadata` lands on the run's `ActionCompleted` event.
    #[staticmethod]
    #[pyo3(signature = (metadata=None))]
    fn unchanged(
        py: Python,
        metadata: Option<pyo3::Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let mut entries = Vec::new();
        if let Some(md) = metadata {
            for (k, v) in md.iter() {
                let key: String = k.extract()?;
                entries.push((key, crate::metadata::coerce_to_metadata_value(py, &v)?));
            }
        }
        Ok(Self {
            materialized: false,
            metadata: entries,
            data_version: None,
        })
    }

    /// The action changed the asset's data — downstream goes stale, exactly
    /// as after a materialize.
    #[staticmethod]
    #[pyo3(signature = (metadata=None, data_version=None))]
    fn materialized(
        py: Python,
        metadata: Option<pyo3::Bound<'_, pyo3::types::PyDict>>,
        data_version: Option<String>,
    ) -> PyResult<Self> {
        let mut entries = Vec::new();
        if let Some(md) = metadata {
            for (k, v) in md.iter() {
                let key: String = k.extract()?;
                entries.push((key, crate::metadata::coerce_to_metadata_value(py, &v)?));
            }
        }
        Ok(Self {
            materialized: true,
            metadata: entries,
            data_version,
        })
    }

    #[getter(data_version)]
    fn data_version_py(&self) -> Option<&String> {
        self.data_version.as_ref()
    }

    fn __repr__(&self) -> String {
        if self.materialized {
            format!(
                "ActionResult.materialized(data_version={:?})",
                self.data_version
            )
        } else {
            "ActionResult.unchanged()".to_string()
        }
    }
}
