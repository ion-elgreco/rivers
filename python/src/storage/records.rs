use pyo3::prelude::*;

use rivers_core::storage::{
    AssetRecord, LaunchedBy, RunRecord, StoredEvent, StoredLog, StoredTick,
};

use crate::partitions::PyPartitionKey;

use super::format_run_status;

#[pyclass(
    name = "StoredEvent",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyStoredEvent {
    pub id: String,
    pub event_type: String,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<PyPartitionKey>,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    pub data_version: Option<String>,
    pub code_version: Option<String>,
    pub input_data_versions: Vec<(String, String)>,
}

impl From<StoredEvent> for PyStoredEvent {
    fn from(e: StoredEvent) -> Self {
        let data_version = e.event_type.data_version().map(|s| s.to_string());
        Self {
            id: format!("{}:{:?}", e.id.table.as_str(), e.id.key),
            event_type: e.event_type.type_name().to_string(),
            asset_key: e.asset_key,
            run_id: e.run_id,
            partition_key: e.partition_key.as_ref().map(PyPartitionKey::from),
            timestamp: e.timestamp,
            metadata: e.metadata,
            data_version,
            code_version: e.code_version,
            input_data_versions: e.input_data_versions,
        }
    }
}

#[pyclass(
    name = "StoredLog",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyStoredLog {
    pub id: String,
    pub run_id: String,
    pub step_key: String,
    pub timestamp: i64,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub logs: Option<String>,
    pub traceback: Option<String>,
}

impl From<StoredLog> for PyStoredLog {
    fn from(l: StoredLog) -> Self {
        Self {
            id: format!("{}:{:?}", l.id.table.as_str(), l.id.key),
            run_id: l.run_id,
            step_key: l.step_key,
            timestamp: l.timestamp,
            stdout: l.stdout,
            stderr: l.stderr,
            logs: l.logs,
            traceback: l.traceback,
        }
    }
}

#[pymethods]
impl PyStoredLog {
    fn __repr__(&self) -> String {
        let streams: Vec<&str> = [
            self.stdout.as_ref().map(|_| "stdout"),
            self.stderr.as_ref().map(|_| "stderr"),
            self.logs.as_ref().map(|_| "logs"),
            self.traceback.as_ref().map(|_| "traceback"),
        ]
        .into_iter()
        .flatten()
        .collect();
        format!(
            "StoredLog(run_id='{}', step_key='{}', streams=[{}])",
            self.run_id,
            self.step_key,
            streams.join(", ")
        )
    }
}

#[pyclass(
    name = "StaleCause",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyStaleCause {
    pub asset_key: String,
    pub category: String,
    pub reason: String,
    pub dependency: Option<String>,
}

#[pymethods]
impl PyStaleCause {
    fn __repr__(&self) -> String {
        match &self.dependency {
            Some(dep) => format!(
                "StaleCause(asset='{}', category='{}', reason='{}', dependency='{}')",
                self.asset_key, self.category, self.reason, dep
            ),
            None => format!(
                "StaleCause(asset='{}', category='{}', reason='{}')",
                self.asset_key, self.category, self.reason
            ),
        }
    }
}

#[pyclass(
    name = "AssetRecord",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyAssetRecord {
    pub asset_key: String,
    pub tags: Vec<String>,
    pub kinds: Vec<String>,
    pub group: Option<String>,
    pub code_version: Option<String>,
    pub last_event_id: Option<String>,
    pub last_run_id: Option<String>,
    pub last_timestamp: Option<i64>,
    pub last_data_version: Option<String>,
    pub last_materialization_code_version: Option<String>,
    pub last_input_data_versions: Vec<(String, String)>,
    pub pool: Vec<(String, u32)>,
}

impl From<AssetRecord> for PyAssetRecord {
    fn from(r: AssetRecord) -> Self {
        Self {
            asset_key: r.asset_key,
            tags: r.tags,
            kinds: r.kinds,
            group: r.asset_group,
            code_version: r.code_version,
            last_event_id: r.last_event_id,
            last_run_id: r.last_run_id,
            last_timestamp: r.last_timestamp,
            last_data_version: r.last_data_version,
            last_materialization_code_version: r.last_materialization_code_version,
            last_input_data_versions: r.last_input_data_versions,
            pool: r.pool,
        }
    }
}

/// Who performed a manual action — Python mirror of
/// `rivers_core::storage::UserRef`.
#[pyclass(name = "UserRef", frozen, skip_from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub struct PyUserRef {
    pub(crate) inner: rivers_core::storage::UserRef,
}

#[pymethods]
impl PyUserRef {
    /// Stable identifier: OIDC `sub` / forward-auth user header.
    #[getter]
    fn subject(&self) -> &str {
        &self.inner.subject
    }

    /// Email snapshot taken at launch time.
    #[getter]
    fn email(&self) -> Option<&str> {
        self.inner.email.as_deref()
    }

    /// Display-name snapshot taken at launch time.
    #[getter]
    fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Human-readable label: name, else email, else subject.
    #[getter]
    fn display(&self) -> &str {
        self.inner.display()
    }

    fn __repr__(&self) -> String {
        format!("UserRef(subject={:?})", self.inner.subject)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// Origin of a run — Python mirror of `rivers_core::storage::LaunchedBy`.
///
/// Variants are discriminated by `.kind`; carried payloads are exposed as
/// `.name` (schedule / sensor), `.backfill_id` (backfill), and `.user`
/// (manual runs launched through an authenticated UI session), each `None`
/// for variants that don't carry them. Use the classmethod constructors
/// (`LaunchedBy.manual()`, `LaunchedBy.schedule("daily")`, …) to build values.
#[pyclass(
    name = "LaunchedBy",
    frozen,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug)]
pub struct PyLaunchedBy {
    pub(crate) inner: LaunchedBy,
}

#[pymethods]
impl PyLaunchedBy {
    #[classmethod]
    fn manual(_cls: &Bound<'_, pyo3::types::PyType>) -> Self {
        Self {
            inner: LaunchedBy::Manual { user: None },
        }
    }

    #[classmethod]
    fn schedule(_cls: &Bound<'_, pyo3::types::PyType>, name: String) -> Self {
        Self {
            inner: LaunchedBy::Schedule { name },
        }
    }

    #[classmethod]
    fn sensor(_cls: &Bound<'_, pyo3::types::PyType>, name: String) -> Self {
        Self {
            inner: LaunchedBy::Sensor { name },
        }
    }

    #[classmethod]
    fn backfill(_cls: &Bound<'_, pyo3::types::PyType>, backfill_id: String) -> Self {
        Self {
            inner: LaunchedBy::Backfill { backfill_id },
        }
    }

    #[classmethod]
    fn condition(_cls: &Bound<'_, pyo3::types::PyType>) -> Self {
        Self {
            inner: LaunchedBy::Condition,
        }
    }

    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner {
            LaunchedBy::Manual { .. } => "manual",
            LaunchedBy::Schedule { .. } => "schedule",
            LaunchedBy::Sensor { .. } => "sensor",
            LaunchedBy::Backfill { .. } => "backfill",
            LaunchedBy::Condition => "condition",
        }
    }

    #[getter]
    fn name(&self) -> Option<&str> {
        match &self.inner {
            LaunchedBy::Schedule { name } | LaunchedBy::Sensor { name } => Some(name.as_str()),
            _ => None,
        }
    }

    #[getter]
    fn backfill_id(&self) -> Option<&str> {
        match &self.inner {
            LaunchedBy::Backfill { backfill_id } => Some(backfill_id.as_str()),
            _ => None,
        }
    }

    #[getter]
    fn user(&self) -> Option<PyUserRef> {
        match &self.inner {
            LaunchedBy::Manual { user: Some(user) } => Some(PyUserRef {
                inner: user.clone(),
            }),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            LaunchedBy::Manual { user: None } => "LaunchedBy.manual()".to_string(),
            LaunchedBy::Manual { user: Some(u) } => {
                format!("LaunchedBy.manual()  # user={}", u.subject)
            }
            LaunchedBy::Schedule { name } => format!("LaunchedBy.schedule({name:?})"),
            LaunchedBy::Sensor { name } => format!("LaunchedBy.sensor({name:?})"),
            LaunchedBy::Backfill { backfill_id } => {
                format!("LaunchedBy.backfill({backfill_id:?})")
            }
            LaunchedBy::Condition => "LaunchedBy.condition()".to_string(),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> isize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        match &self.inner {
            LaunchedBy::Manual { .. } => 0u8.hash(&mut h),
            LaunchedBy::Schedule { name } => {
                1u8.hash(&mut h);
                name.hash(&mut h);
            }
            LaunchedBy::Sensor { name } => {
                2u8.hash(&mut h);
                name.hash(&mut h);
            }
            LaunchedBy::Backfill { backfill_id } => {
                3u8.hash(&mut h);
                backfill_id.hash(&mut h);
            }
            LaunchedBy::Condition => 4u8.hash(&mut h),
        }
        h.finish() as isize
    }
}

impl From<LaunchedBy> for PyLaunchedBy {
    fn from(inner: LaunchedBy) -> Self {
        Self { inner }
    }
}

#[pyclass(
    name = "RunRecord",
    frozen,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyRunRecord {
    #[pyo3(get)]
    pub run_id: String,
    /// `None` for ad-hoc runs (`materialize`, asset-selection sensors); `Some`
    /// when the run targets a user-defined `Job`.
    #[pyo3(get)]
    pub job_name: Option<String>,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub start_time: i64,
    #[pyo3(get)]
    pub end_time: Option<i64>,
    #[pyo3(get)]
    pub tags: Vec<(String, String)>,
    #[pyo3(get)]
    pub node_names: Vec<String>,
    #[pyo3(get)]
    pub priority: i32,
    #[pyo3(get)]
    pub partition_key: Option<PyPartitionKey>,
    #[pyo3(get)]
    pub block_reason: Option<String>,
    #[pyo3(get)]
    pub launched_by: PyLaunchedBy,
    /// The verb this run executes. `None` means materialize.
    #[pyo3(get)]
    pub action: Option<String>,
    /// See [`RunRecord::config`]; the `config` getter parses it.
    pub config_json: Option<String>,
}

#[pymethods]
impl PyRunRecord {
    /// The launch document the run was launched with (see
    /// `CodeRepository.materialize`). `None` means the definitions' defaults.
    #[getter]
    fn config(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.config_json
            .as_deref()
            .map(|json| Ok(py.import("json")?.call_method1("loads", (json,))?.unbind()))
            .transpose()
    }
}

impl From<RunRecord> for PyRunRecord {
    fn from(r: RunRecord) -> Self {
        Self {
            run_id: r.run_id,
            job_name: r.job_name,
            status: format_run_status(r.status).to_string(),
            start_time: r.start_time,
            end_time: r.end_time,
            tags: r.tags,
            node_names: r.node_names,
            priority: r.priority,
            partition_key: r.partition_key.as_ref().map(PyPartitionKey::from),
            block_reason: r.block_reason,
            launched_by: r.launched_by.into(),
            action: r.action,
            config_json: r.config,
        }
    }
}

#[pyclass(
    name = "StoredTick",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyStoredTick {
    pub id: String,
    pub automation_name: String,
    pub automation_type: String,
    pub status: String,
    pub timestamp: i64,
    pub run_ids: Vec<String>,
    pub backfill_ids: Vec<String>,
    pub skip_reason: Option<String>,
    pub error: Option<String>,
    pub cursor: Option<String>,
}

impl From<StoredTick> for PyStoredTick {
    fn from(t: StoredTick) -> Self {
        Self {
            id: format!("{}:{:?}", t.id.table.as_str(), t.id.key),
            automation_name: t.automation_name,
            automation_type: t.automation_type,
            status: t.status,
            timestamp: t.timestamp,
            run_ids: t.run_ids,
            backfill_ids: t.backfill_ids,
            skip_reason: t.skip_reason,
            error: t.error,
            cursor: t.cursor,
        }
    }
}
