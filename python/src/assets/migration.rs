//! Asset migrations: the steps that move an asset's stored data from one
//! version to the next.

use std::collections::HashSet;

use jiff::{Timestamp, civil};
use pyo3::prelude::*;
use rivers_core::execution::retry::RetryRef;
use rivers_core::storage::MigrationKind;

use crate::errors::AssetDefinitionError;

/// The step that moves an asset's stored data from version `to - 1` to `to`.
/// A bound function makes it a convert, `rebuild=True` a rebuild, and neither
/// a step that only records the version. Reusable: the same instance may be
/// attached to any number of assets.
#[pyclass(name = "AssetMigration", frozen, module = "rivers._core")]
pub struct PyAssetMigration {
    /// As declared: the asset checks it against its `schema_version`.
    pub to: i64,
    pub breaking: bool,
    pub rebuild: bool,
    pub retry: Option<RetryRef>,
    pub description: Option<String>,
    /// Nanoseconds since the epoch: the migration does not start before.
    pub start_at: Option<i64>,
    pub func: Option<Py<PyAny>>,
    pub is_async: bool,
}

/// `start_at` in nanoseconds since the epoch. A datetime without a time zone
/// is an error: the daemon may run in another time zone than its author.
pub(crate) fn start_at_ns(to: i64, start_at: &Bound<'_, PyAny>) -> PyResult<i64> {
    let error = |problem: String| {
        AssetDefinitionError::new_err(format!("migration to version {to}: start_at {problem}"))
    };
    let at = match start_at.extract::<Timestamp>() {
        Ok(at) => at,
        Err(_) if start_at.extract::<civil::DateTime>().is_ok() => {
            return Err(error(
                "needs a time zone, for example \
                 datetime(2026, 11, 27, 22, 0, tzinfo=ZoneInfo(\"Europe/Amsterdam\"))"
                    .to_string(),
            ));
        }
        Err(e) => return Err(error(format!("must be a datetime with a time zone: {e}"))),
    };
    i64::try_from(at.as_nanosecond()).map_err(|_| error(format!("is out of range: {at}")))
}

impl PyAssetMigration {
    pub(crate) fn kind(&self) -> MigrationKind {
        match (&self.func, self.rebuild) {
            (Some(_), _) => MigrationKind::Convert,
            (None, true) => MigrationKind::Rebuild,
            (None, false) => MigrationKind::VersionOnly,
        }
    }
}

#[pymethods]
impl PyAssetMigration {
    #[new]
    #[pyo3(signature = (
        to, *, breaking=true, rebuild=false, retry=None, description=None, start_at=None
    ))]
    fn new(
        to: i64,
        breaking: bool,
        rebuild: bool,
        retry: Option<Bound<'_, PyAny>>,
        description: Option<String>,
        start_at: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        Ok(Self {
            to,
            breaking,
            rebuild,
            retry: crate::retry::extract_retry_ref(retry)?,
            description,
            start_at: start_at.map(|at| start_at_ns(to, &at)).transpose()?,
            func: None,
            is_async: false,
        })
    }

    /// Decorator application: bind the convert function.
    fn __call__(&self, py: Python, func: Py<PyAny>) -> PyResult<Self> {
        if self.func.is_some() {
            return Err(AssetDefinitionError::new_err(format!(
                "migration to version {} is already bound to a function",
                self.to
            )));
        }
        if self.rebuild {
            return Err(AssetDefinitionError::new_err(format!(
                "migration to version {} is a rebuild: it runs the asset's own function, \
                 so it takes no function of its own",
                self.to
            )));
        }

        let is_async = super::decorator::is_coroutine_function(py, &Some(func.clone_ref(py)));
        Ok(Self {
            to: self.to,
            breaking: self.breaking,
            rebuild: self.rebuild,
            retry: self.retry.clone(),
            description: self.description.clone(),
            start_at: self.start_at,
            func: Some(func),
            is_async,
        })
    }

    #[getter]
    fn to(&self) -> i64 {
        self.to
    }

    #[getter]
    fn breaking(&self) -> bool {
        self.breaking
    }

    #[getter]
    fn rebuild(&self) -> bool {
        self.rebuild
    }

    #[getter]
    fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    /// The time before which the migration does not start, in UTC.
    #[getter]
    fn start_at(&self) -> Option<Timestamp> {
        self.start_at
            .and_then(|at| Timestamp::from_nanosecond(i128::from(at)).ok())
    }

    /// `convert` with a function, `rebuild` with `rebuild=True`, else
    /// `version_only`.
    #[getter(kind)]
    fn kind_py(&self) -> &'static str {
        self.kind().as_str()
    }

    fn __repr__(&self) -> String {
        let start_at = self
            .start_at()
            .map(|at| format!(", start_at='{at}'"))
            .unwrap_or_default();
        format!(
            "AssetMigration(to={}, kind='{}', breaking={}{start_at})",
            self.to,
            self.kind().as_str(),
            if self.breaking { "True" } else { "False" }
        )
    }

    /// A class-form asset defined in a script ships to loky workers by value,
    /// with its class attributes: its migrations are among them.
    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, MigrationParts)> {
        let Self {
            to,
            breaking,
            rebuild,
            retry,
            description,
            start_at,
            func,
            is_async,
        } = self;
        let ctor = py
            .import("rivers._core")?
            .getattr("_reconstruct_asset_migration")?
            .unbind();
        let retry = retry
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| {
                AssetDefinitionError::new_err(format!(
                    "migration to version {to}: retry policy is not serializable: {e}"
                ))
            })?;
        Ok((
            ctor,
            (
                *to,
                *breaking,
                *rebuild,
                retry,
                description.clone(),
                *start_at,
                func.as_ref().map(|f| f.clone_ref(py)),
                *is_async,
            ),
        ))
    }
}

/// `PyAssetMigration`'s pickled state, in `_reconstruct_asset_migration` order.
type MigrationParts = (
    i64,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<Py<PyAny>>,
    bool,
);

/// Rebuild an `AssetMigration` from `__reduce__`'s parts. The constructor
/// can't carry the bound function.
#[pyfunction]
pub fn _reconstruct_asset_migration(
    to: i64,
    breaking: bool,
    rebuild: bool,
    retry: Option<String>,
    description: Option<String>,
    start_at: Option<i64>,
    func: Option<Py<PyAny>>,
    is_async: bool,
) -> PyResult<PyAssetMigration> {
    let retry = retry
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| {
            AssetDefinitionError::new_err(format!(
                "migration to version {to}: unreadable retry policy: {e}"
            ))
        })?;
    Ok(PyAssetMigration {
        to,
        breaking,
        rebuild,
        retry,
        description,
        start_at,
        func,
        is_async,
    })
}

/// One migration of an output, resolved at `resolve()`.
pub(crate) struct ResolvedMigration {
    pub to: u32,
    pub kind: MigrationKind,
    pub breaking: bool,
    /// The convert function; `None` for a rebuild or a version-only migration.
    pub func: Option<Py<PyAny>>,
    pub is_async: bool,
    /// A named policy becomes inline at resolve.
    pub retry: Option<RetryRef>,
    /// Nanoseconds since the epoch: the migration does not start before.
    pub start_at: Option<i64>,
}

impl ResolvedMigration {
    pub(crate) fn new(py: Python, m: &PyAssetMigration) -> Self {
        Self {
            to: u32::try_from(m.to).expect("check_migrations keeps `to` in the range of u32"),
            kind: m.kind(),
            breaking: m.breaking,
            func: m.func.as_ref().map(|f| f.clone_ref(py)),
            is_async: m.is_async,
            retry: m.retry.clone(),
            start_at: m.start_at,
        }
    }

    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self {
            to: self.to,
            kind: self.kind,
            breaking: self.breaking,
            func: self.func.as_ref().map(|f| f.clone_ref(py)),
            is_async: self.is_async,
            retry: self.retry.clone(),
            start_at: self.start_at,
        }
    }
}

/// Check the declared version of `asset` (an asset or a multi-asset output)
/// and its migrations.
pub(crate) fn check_migrations(
    asset: &str,
    schema_version: i64,
    migrations: &[Py<PyAssetMigration>],
) -> PyResult<()> {
    let error =
        |problem: String| AssetDefinitionError::new_err(format!("asset '{asset}': {problem}"));
    if schema_version < 1 {
        return Err(error(format!(
            "schema_version must be 1 or more, got {schema_version}"
        )));
    }
    if u32::try_from(schema_version).is_err() {
        return Err(error(format!(
            "schema_version must be at most {}, got {schema_version}",
            u32::MAX
        )));
    }

    let mut seen = HashSet::new();
    for m in migrations {
        let m = m.get();
        let to = m.to;
        if !(2..=schema_version).contains(&to) {
            return Err(error(format!(
                "migration to version {to} is not between 2 and schema_version {schema_version}"
            )));
        }
        if !seen.insert(to) {
            return Err(error(format!("two migrations go to version {to}")));
        }
        if m.func.is_none() && m.retry.is_some() {
            return Err(error(format!(
                "migration to version {to} has no function, so it takes no retry=: a rebuild \
                 runs the asset's own function with the asset's retry, and a version-only \
                 migration runs nothing"
            )));
        }
    }
    Ok(())
}
