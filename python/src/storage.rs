//! PyStorage — Python wrapper for the SurrealDB storage backend.
//!
//! `PyStorage` holds a [`ScopedStorageHandle`] that bundles the underlying
//! `Arc<SurrealStorage>` with a [`CodeLocationContext`] (sourced from
//! `RIVERS_CODE_LOCATION_ID`), so per-CL query methods don't need a CL
//! argument from Python. Each query is exposed twice: a sync method that
//! releases the GIL while awaiting the async storage call, and an
//! `async_*` variant for `await`-friendly use from `asyncio`.
use std::sync::Arc;

use pyo3::prelude::*;

use crate::errors::StorageError;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    AssetRecord, CodeLocationContext, LaunchedBy, RunRecord, RunStatus, ScopedStorage,
    ScopedStorageHandle, StaleCauseCategory, StaleStatus, StorageBackend, StoredEvent, StoredTick,
};

use crate::partitions::PyPartitionKey;
use crate::runtime::io_rt;

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

/// Origin of a run — Python mirror of `rivers_core::storage::LaunchedBy`.
///
/// Variants are discriminated by `.kind`; carried payloads are exposed as
/// `.name` (schedule / sensor) and `.backfill_id` (backfill), both `None` for
/// variants that don't carry one. Use the classmethod constructors
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
            inner: LaunchedBy::Manual,
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
            LaunchedBy::Manual => "manual",
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

    fn __repr__(&self) -> String {
        match &self.inner {
            LaunchedBy::Manual => "LaunchedBy.manual()".to_string(),
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
            LaunchedBy::Manual => 0u8.hash(&mut h),
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
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyRunRecord {
    pub run_id: String,
    /// `None` for ad-hoc runs (`materialize`, asset-selection sensors); `Some`
    /// when the run targets a user-defined `Job`.
    pub job_name: Option<String>,
    pub status: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub tags: Vec<(String, String)>,
    pub node_names: Vec<String>,
    pub priority: i32,
    pub partition_key: Option<PyPartitionKey>,
    pub block_reason: Option<String>,
    pub launched_by: PyLaunchedBy,
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

fn to_py_err(e: anyhow::Error) -> PyErr {
    // The "database is behind this build" case gets a distinct exception (a
    // StorageError subclass) so the `rivers dev` prompt can offer the migration
    // by type rather than by message text. anyhow searches the chain.
    if let Some(m) =
        e.downcast_ref::<rivers_core::storage::surrealdb_backend::SchemaMigrationNeeded>()
    {
        return crate::errors::SchemaMigrationNeededError::new_err(m.to_string());
    }
    StorageError::new_err(format!("{e}"))
}

/// Resolve a remote connect config from kwargs → `RIVERS_SURREAL_*` env → default,
/// returning the config and whether it carries credentials. Pure (no Python, no
/// I/O), so callers run it inside `py.detach`. Shared by `connect` / `migrate_remote`.
fn resolve_remote_config(
    endpoint: String,
    username: Option<String>,
    password: Option<String>,
    namespace: Option<String>,
    database: Option<String>,
) -> (
    rivers_core::storage::surrealdb_backend::SurrealConnectConfig,
    bool,
) {
    use rivers_core::storage::surrealdb_backend::{
        DEFAULT_DATABASE, DEFAULT_NAMESPACE, SurrealConnectConfig,
    };
    // Empty strings count as unset so `username=""` doesn't shadow a populated env var.
    fn resolve(kwarg: Option<String>, env_name: &str) -> Option<String> {
        kwarg
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var(env_name).ok().filter(|s| !s.is_empty()))
    }
    let namespace = resolve(namespace, rivers_k8s::env::ENV_SURREAL_NAMESPACE)
        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
    let database = resolve(database, rivers_k8s::env::ENV_SURREAL_DATABASE)
        .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
    let username = resolve(username, rivers_k8s::env::ENV_SURREAL_USERNAME);
    let password = resolve(password, rivers_k8s::env::ENV_SURREAL_PASSWORD);
    let mut config = SurrealConnectConfig {
        endpoint,
        namespace,
        database,
        credentials: None,
    };
    if let (Some(u), Some(p)) = (username, password) {
        config = config.with_credentials(u, p);
    }
    let authenticated = config.credentials.is_some();
    (config, authenticated)
}

fn parse_run_status(s: &str) -> PyResult<RunStatus> {
    match s {
        "Queued" => Ok(RunStatus::Queued),
        "NotStarted" => Ok(RunStatus::NotStarted),
        "Started" => Ok(RunStatus::Started),
        "Success" => Ok(RunStatus::Success),
        "Failure" => Ok(RunStatus::Failure),
        "Canceled" => Ok(RunStatus::Canceled),
        _ => Err(StorageError::new_err(format!("Unknown run status: {s}"))),
    }
}

fn format_run_status(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Queued => "Queued",
        RunStatus::NotStarted => "NotStarted",
        RunStatus::Started => "Started",
        RunStatus::Success => "Success",
        RunStatus::Failure => "Failure",
        RunStatus::Canceled => "Canceled",
    }
}

#[pyclass(
    name = "StorageType",
    frozen,
    eq,
    eq_int,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyStorageType {
    Memory,
    Embedded,
    Remote,
}

#[pyclass(
    name = "PoolLimit",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolLimit {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
}

impl From<rivers_core::storage::PoolLimit> for PyPoolLimit {
    fn from(p: rivers_core::storage::PoolLimit) -> Self {
        Self {
            pool_key: p.pool_key,
            slot_limit: p.slot_limit,
            lease_duration_secs: p.lease_duration_secs,
        }
    }
}

#[pyclass(
    name = "PoolInfo",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolInfo {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    pub claimed_count: u32,
    pub pending_count: u32,
}

impl From<rivers_core::storage::PoolInfo> for PyPoolInfo {
    fn from(p: rivers_core::storage::PoolInfo) -> Self {
        Self {
            pool_key: p.pool_key,
            slot_limit: p.slot_limit,
            lease_duration_secs: p.lease_duration_secs,
            claimed_count: p.claimed_count,
            pending_count: p.pending_count,
        }
    }
}

#[pyclass(
    name = "SlotHolder",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PySlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

impl From<rivers_core::storage::SlotHolder> for PySlotHolder {
    fn from(s: rivers_core::storage::SlotHolder) -> Self {
        Self {
            run_id: s.run_id,
            step_key: s.step_key,
            slots_consumed: s.slots_consumed,
            claimed_at: s.claimed_at,
            lease_expires_at: s.lease_expires_at,
        }
    }
}

#[pyclass(
    name = "PoolBlockDetail",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolBlockDetail {
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
}

impl From<rivers_core::storage::PoolBlockDetail> for PyPoolBlockDetail {
    fn from(p: rivers_core::storage::PoolBlockDetail) -> Self {
        Self {
            pool_key: p.pool_key,
            claimed: p.claimed,
            limit: p.limit,
        }
    }
}

#[pyclass(
    name = "BlockReason",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyBlockReason {
    /// "pool_full" or "pools_full"
    pub kind: String,
    /// For pool_full: the single blocking pool. For pools_full: the first pool.
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
    /// For pools_full: all blocking pools. Empty for pool_full.
    pub pools: Vec<PyPoolBlockDetail>,
}

#[pymethods]
impl PyBlockReason {
    fn __repr__(&self) -> String {
        if self.kind == "pool_full" {
            format!(
                "BlockReason.pool_full(pool_key='{}', claimed={}, limit={})",
                self.pool_key, self.claimed, self.limit
            )
        } else {
            let details: Vec<String> = self
                .pools
                .iter()
                .map(|p| format!("'{}'({}/{})", p.pool_key, p.claimed, p.limit))
                .collect();
            format!("BlockReason.pools_full({})", details.join(", "))
        }
    }
}

impl From<rivers_core::storage::BlockReason> for PyBlockReason {
    fn from(r: rivers_core::storage::BlockReason) -> Self {
        match r {
            rivers_core::storage::BlockReason::PoolFull {
                pool_key,
                claimed,
                limit,
            } => Self {
                kind: "pool_full".to_string(),
                pool_key,
                claimed,
                limit,
                pools: vec![],
            },
            rivers_core::storage::BlockReason::PoolsFull { pools } => {
                let first = &pools[0];
                Self {
                    kind: "pools_full".to_string(),
                    pool_key: first.pool_key.clone(),
                    claimed: first.claimed,
                    limit: first.limit,
                    pools: pools.into_iter().map(PyPoolBlockDetail::from).collect(),
                }
            }
        }
    }
}

#[pyclass(
    name = "ConcurrencyClaimStatus",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyConcurrencyClaimStatus {
    /// "claimed" or "pending"
    pub status: String,
    /// Queue position (only meaningful when status == "pending").
    pub position: u32,
    /// Block reason (only present when status == "pending").
    pub reason: Option<PyBlockReason>,
}

#[pymethods]
impl PyConcurrencyClaimStatus {
    #[getter]
    fn is_claimed(&self) -> bool {
        self.status == "claimed"
    }

    fn __repr__(&self) -> String {
        if self.status == "claimed" {
            "ConcurrencyClaimStatus.Claimed".to_string()
        } else {
            format!(
                "ConcurrencyClaimStatus.Pending(position={}, reason={})",
                self.position,
                self.reason
                    .as_ref()
                    .map(|r| r.__repr__())
                    .unwrap_or_default()
            )
        }
    }
}

impl From<rivers_core::storage::ConcurrencyClaimStatus> for PyConcurrencyClaimStatus {
    fn from(s: rivers_core::storage::ConcurrencyClaimStatus) -> Self {
        match s {
            rivers_core::storage::ConcurrencyClaimStatus::Claimed => Self {
                status: "claimed".to_string(),
                position: 0,
                reason: None,
            },
            rivers_core::storage::ConcurrencyClaimStatus::Pending { position, reason } => Self {
                status: "pending".to_string(),
                position,
                reason: Some(PyBlockReason::from(reason)),
            },
        }
    }
}

/// SurrealDB-backed storage exposed to Python.
///
/// Bundles the SurrealDB connection (`Arc<SurrealStorage>`) with a stable
/// [`CodeLocationContext`] (set at construction from `RIVERS_CODE_LOCATION_ID`,
/// or [`DEFAULT_CODE_LOCATION_ID`] for tests) into a single
/// [`ScopedStorageHandle`]. Per-CL storage queries are scoped through this
/// context so the Python API stays free of CL identity arguments.
#[pyclass(name = "Storage", frozen, module = "rivers._core")]
pub struct PyStorage {
    pub(crate) handle: ScopedStorageHandle<SurrealStorage>,
    pub(crate) storage_type: PyStorageType,
}

impl PyStorage {
    pub(crate) fn cl(&self) -> &str {
        self.handle.code_location_id()
    }

    /// Borrow the per-CL [`ScopedStorage`] wrapper for sync calls. Use
    /// [`Self::handle`] when you need an owned handle to move into a spawned
    /// task or async closure.
    pub(crate) fn scoped(&self) -> ScopedStorage<'_, SurrealStorage> {
        self.handle.scoped()
    }

    /// Borrow the underlying backend `Arc` for unscoped (UUID-keyed) calls.
    pub(crate) fn backend(&self) -> &Arc<SurrealStorage> {
        self.handle.backend()
    }

    fn detect_storage_cl() -> CodeLocationContext {
        CodeLocationContext::new(rivers_k8s::env::current_code_location_id())
    }

    fn from_storage(storage: SurrealStorage, storage_type: PyStorageType) -> Self {
        Self {
            handle: ScopedStorageHandle::new(Arc::new(storage), Self::detect_storage_cl()),
            storage_type,
        }
    }
}

#[pymethods]
impl PyStorage {
    #[getter(r#type)]
    fn storage_type(&self) -> PyStorageType {
        self.storage_type
    }

    /// Create an embedded storage backed by RocksDB at the given path.
    #[staticmethod]
    fn embedded(py: Python<'_>, path: &str) -> PyResult<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| StorageError::new_err(format!("Failed to create storage dir: {e}")))?;
        let storage = py.detach(|| {
            io_rt()
                .block_on(SurrealStorage::new_embedded(path))
                .map_err(to_py_err)
        })?;
        tracing::info!(target: "rivers::storage", backend = "embedded", path = %path, "storage ready");
        Ok(Self::from_storage(storage, PyStorageType::Embedded))
    }

    /// Create an in-memory storage (useful for tests).
    #[staticmethod]
    fn memory(py: Python<'_>) -> PyResult<Self> {
        let storage = py.detach(|| {
            io_rt()
                .block_on(SurrealStorage::new_memory())
                .map_err(to_py_err)
        })?;
        tracing::info!(target: "rivers::storage", backend = "memory", "storage ready");
        Ok(Self::from_storage(storage, PyStorageType::Memory))
    }

    /// Test-only: create an embedded storage on its own dedicated tokio
    /// runtime so the storage owns the router task and its drop releases
    /// the RocksDB file lock synchronously (via `Runtime::shutdown_timeout`).
    ///
    /// Used by pytest fixtures that open and tear down many storage
    /// instances per session — without this, the shared `io_rt()` fills
    /// with fire-and-forget shutdown tasks faster than they drain and
    /// later opens hang. Production code should keep using
    /// [`embedded`](Self::embedded), which routes through `io_rt()`.
    #[staticmethod]
    fn _test_embedded(py: Python<'_>, path: &str) -> PyResult<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| StorageError::new_err(format!("Failed to create storage dir: {e}")))?;
        let storage =
            py.detach(|| SurrealStorage::new_embedded_blocking(path).map_err(to_py_err))?;
        tracing::info!(target: "rivers::storage", backend = "embedded", path = %path, "storage ready (test runtime)");
        Ok(Self::from_storage(storage, PyStorageType::Embedded))
    }

    /// Test-only: in-memory counterpart of [`_test_embedded`](Self::_test_embedded).
    #[staticmethod]
    fn _test_memory(py: Python<'_>) -> PyResult<Self> {
        let storage = py.detach(|| SurrealStorage::new_memory_blocking().map_err(to_py_err))?;
        tracing::info!(target: "rivers::storage", backend = "memory", "storage ready (test runtime)");
        Ok(Self::from_storage(storage, PyStorageType::Memory))
    }

    /// Connect to a remote SurrealDB server (e.g. "ws://host:8000").
    ///
    /// Resolution per field: explicit kwarg → `RIVERS_SURREAL_*` env var →
    /// default. `username`+`password` together attach database-scoped
    /// credentials (matching `DEFINE USER ... ON DATABASE`); when either
    /// is missing on both kwarg AND env, the connection is unauthenticated
    /// (`--unauthenticated` SurrealDB). `namespace` / `database` default to
    /// `"rivers"` / `"main"`.
    #[staticmethod]
    #[pyo3(signature = (endpoint, *, username=None, password=None, namespace=None, database=None))]
    fn connect(
        py: Python<'_>,
        endpoint: &str,
        username: Option<String>,
        password: Option<String>,
        namespace: Option<String>,
        database: Option<String>,
    ) -> PyResult<Self> {
        // Whole body runs detached: env-var resolution + config building touch
        // nothing Python, and the GIL must stay released across `block_on` to
        // avoid the daemon-task deadlock (see `Self::embedded`).
        let endpoint_owned = endpoint.to_string();
        let (storage, authenticated) = py.detach(|| -> PyResult<_> {
            let (config, authenticated) =
                resolve_remote_config(endpoint_owned, username, password, namespace, database);
            let storage = io_rt()
                .block_on(SurrealStorage::connect(config))
                .map_err(to_py_err)?;
            Ok((storage, authenticated))
        })?;
        tracing::info!(
            target: "rivers::storage",
            backend = "remote",
            endpoint = %endpoint,
            authenticated,
            "storage ready"
        );
        Ok(Self::from_storage(storage, PyStorageType::Remote))
    }

    /// Apply pending storage schema migrations to an embedded database, bringing
    /// it to this build's schema version. Backs `rivers db migrate`;
    /// idempotent. The migrating connection is opened and dropped immediately.
    #[staticmethod]
    fn migrate_embedded(py: Python<'_>, path: &str) -> PyResult<()> {
        use rivers_core::storage::surrealdb_backend::Capability;
        std::fs::create_dir_all(path)
            .map_err(|e| StorageError::new_err(format!("Failed to create storage dir: {e}")))?;
        py.detach(|| {
            io_rt()
                .block_on(SurrealStorage::new_embedded_with_capability(
                    path,
                    Capability::Migrate,
                ))
                .map_err(to_py_err)
        })?;
        tracing::info!(target: "rivers::storage", backend = "embedded", path = %path, "storage schema migrated");
        Ok(())
    }

    /// Remote counterpart of [`migrate_embedded`](Self::migrate_embedded); same
    /// field resolution as [`connect`](Self::connect).
    #[staticmethod]
    #[pyo3(signature = (endpoint, *, username=None, password=None, namespace=None, database=None))]
    fn migrate_remote(
        py: Python<'_>,
        endpoint: &str,
        username: Option<String>,
        password: Option<String>,
        namespace: Option<String>,
        database: Option<String>,
    ) -> PyResult<()> {
        use rivers_core::storage::surrealdb_backend::Capability;
        let endpoint_owned = endpoint.to_string();
        let authenticated = py.detach(|| -> PyResult<_> {
            let (config, authenticated) =
                resolve_remote_config(endpoint_owned, username, password, namespace, database);
            io_rt()
                .block_on(SurrealStorage::connect_with_capability(
                    config,
                    Capability::Migrate,
                ))
                .map_err(to_py_err)?;
            Ok(authenticated)
        })?;
        tracing::info!(target: "rivers::storage", backend = "remote", endpoint = %endpoint, authenticated, "storage schema migrated");
        Ok(())
    }

    #[pyo3(signature = (asset_key, limit=100))]
    fn get_events_for_asset(
        &self,
        py: Python<'_>,
        asset_key: &str,
        limit: usize,
    ) -> PyResult<Vec<PyStoredEvent>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_events_for_asset(asset_key, limit))
                .map(|v| v.into_iter().map(PyStoredEvent::from).collect())
                .map_err(to_py_err)
        })
    }

    fn get_events_for_run(&self, py: Python<'_>, run_id: &str) -> PyResult<Vec<PyStoredEvent>> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().get_events_for_run(run_id))
                .map(|v| v.into_iter().map(PyStoredEvent::from).collect())
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (asset_key, partition=None))]
    fn get_latest_materialization(
        &self,
        py: Python<'_>,
        asset_key: &str,
        partition: Option<&str>,
    ) -> PyResult<Option<PyStoredEvent>> {
        py.detach(|| {
            io_rt()
                .block_on(
                    self.scoped()
                        .get_latest_materialization(asset_key, partition),
                )
                .map(|opt| opt.map(PyStoredEvent::from))
                .map_err(to_py_err)
        })
    }

    fn get_asset_record(&self, py: Python<'_>, asset_key: &str) -> PyResult<Option<PyAssetRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_asset_record(asset_key))
                .map(|opt| opt.map(PyAssetRecord::from))
                .map_err(to_py_err)
        })
    }

    fn get_asset_records(&self, py: Python<'_>) -> PyResult<Vec<PyAssetRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_asset_records())
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect())
                .map_err(to_py_err)
        })
    }

    /// Compute current staleness for every asset in this code location.
    /// Returns a dict mapping asset_key → (status_str, [PyStaleCause]).
    /// `stale_status` is no longer persisted — use this when you need the
    /// current value.
    fn compute_staleness(
        &self,
        py: Python<'_>,
    ) -> PyResult<std::collections::HashMap<String, (String, Vec<PyStaleCause>)>> {
        py.detach(|| {
            let result = io_rt()
                .block_on(self.scoped().compute_staleness())
                .map_err(to_py_err)?;
            Ok(result
                .into_iter()
                .map(|(key, (status, causes))| {
                    let status_str = match status {
                        StaleStatus::UpToDate => "UpToDate",
                        StaleStatus::Stale => "Stale",
                        StaleStatus::Missing => "Missing",
                    }
                    .to_string();
                    let py_causes = causes
                        .into_iter()
                        .map(|c| {
                            let (cat, dep) = match &c.category {
                                StaleCauseCategory::Code => ("Code".to_string(), None),
                                StaleCauseCategory::Data { dependency } => {
                                    ("Data".to_string(), Some(dependency.clone()))
                                }
                            };
                            PyStaleCause {
                                asset_key: c.asset_key,
                                category: cat,
                                reason: c.reason,
                                dependency: dep,
                            }
                        })
                        .collect();
                    (key, (status_str, py_causes))
                })
                .collect())
        })
    }

    fn get_assets_by_tag(&self, py: Python<'_>, tag: &str) -> PyResult<Vec<PyAssetRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_assets_by_tag(tag))
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect())
                .map_err(to_py_err)
        })
    }

    fn get_assets_by_kind(&self, py: Python<'_>, kind: &str) -> PyResult<Vec<PyAssetRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_assets_by_kind(kind))
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect())
                .map_err(to_py_err)
        })
    }

    fn get_assets_by_group(&self, py: Python<'_>, group: &str) -> PyResult<Vec<PyAssetRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_assets_by_group(group))
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect())
                .map_err(to_py_err)
        })
    }

    fn get_run(&self, py: Python<'_>, run_id: &str) -> PyResult<Option<PyRunRecord>> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().get_run(run_id))
                .map(|opt| opt.map(PyRunRecord::from))
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (limit=100, status=None))]
    fn get_runs(
        &self,
        py: Python<'_>,
        limit: usize,
        status: Option<&str>,
    ) -> PyResult<Vec<PyRunRecord>> {
        let status = status.map(parse_run_status).transpose()?;
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_runs(limit, status))
                .map(|v| v.into_iter().map(PyRunRecord::from).collect())
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (automation_name, limit=100))]
    fn get_ticks(
        &self,
        py: Python<'_>,
        automation_name: &str,
        limit: usize,
    ) -> PyResult<Vec<PyStoredTick>> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_ticks(automation_name, limit))
                .map(|v| v.into_iter().map(PyStoredTick::from).collect())
                .map_err(to_py_err)
        })
    }

    fn kv_get(&self, py: Python<'_>, key: &str) -> PyResult<Option<Vec<u8>>> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().kv_get(key))
                .map_err(to_py_err)
        })
    }

    fn kv_set(&self, py: Python<'_>, key: &str, value: &[u8]) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().kv_set(key, value))
                .map_err(to_py_err)
        })
    }

    fn add_dynamic_partitions(
        &self,
        py: Python<'_>,
        partitions_def_name: &str,
        partition_keys: Vec<String>,
    ) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(
                    self.scoped()
                        .add_dynamic_partitions(partitions_def_name, &partition_keys),
                )
                .map_err(to_py_err)
        })
    }

    fn delete_dynamic_partition(
        &self,
        py: Python<'_>,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(
                    self.scoped()
                        .delete_dynamic_partition(partitions_def_name, partition_key),
                )
                .map_err(to_py_err)
        })
    }

    fn get_dynamic_partitions(
        &self,
        py: Python<'_>,
        partitions_def_name: &str,
    ) -> PyResult<Vec<String>> {
        let keys = py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_dynamic_partitions(partitions_def_name))
                .map_err(to_py_err)
        })?;
        if keys.is_empty() {
            let warnings = py.import("warnings")?;
            warnings.call_method1(
                "warn",
                (format!(
                    "No dynamic partitions found for '{}'. \
                     Ensure a PartitionsDefinition.dynamic('{}') exists and \
                     partitions have been added via add_dynamic_partitions().",
                    partitions_def_name, partitions_def_name
                ),),
            )?;
        }
        Ok(keys)
    }

    fn has_dynamic_partition(
        &self,
        py: Python<'_>,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> PyResult<bool> {
        py.detach(|| {
            io_rt()
                .block_on(
                    self.scoped()
                        .has_dynamic_partition(partitions_def_name, partition_key),
                )
                .map_err(to_py_err)
        })
    }

    /// Get all partition keys that have been materialized for an asset.
    fn get_materialized_partitions(
        &self,
        py: Python<'_>,
        asset_key: &str,
    ) -> PyResult<Vec<PyPartitionKey>> {
        let storage_keys = py.detach(|| {
            io_rt()
                .block_on(self.scoped().get_materialized_partitions(asset_key))
                .map_err(to_py_err)
        })?;
        Ok(storage_keys.iter().map(PyPartitionKey::from).collect())
    }

    /// Number of materialized partitions for an asset (aggregate count, not the
    /// keys) — backs the UI's partition summary.
    fn count_materialized_partitions(&self, py: Python<'_>, asset_key: &str) -> PyResult<u64> {
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().count_materialized_partitions(asset_key))
                .map_err(to_py_err)
        })
    }

    // Concurrency pools

    #[pyo3(signature = (pool_key, limit, lease_duration="5m"))]
    fn set_pool_limit(
        &self,
        py: Python<'_>,
        pool_key: &str,
        limit: i32,
        lease_duration: &str,
    ) -> PyResult<()> {
        let secs = crate::utils::parse_duration_secs_u32("lease_duration", lease_duration)?;
        py.detach(|| {
            io_rt()
                .block_on(self.scoped().set_pool_limit(pool_key, limit, secs))
                .map_err(to_py_err)
        })
    }

    fn get_pool_limits(&self, py: Python<'_>) -> PyResult<Vec<PyPoolLimit>> {
        py.detach(|| {
            let pools = io_rt()
                .block_on(self.scoped().get_pool_limits())
                .map_err(to_py_err)?;
            Ok(pools.into_iter().map(PyPoolLimit::from).collect())
        })
    }

    fn get_all_pool_infos(&self, py: Python<'_>) -> PyResult<Vec<PyPoolInfo>> {
        py.detach(|| {
            let infos = io_rt()
                .block_on(self.scoped().get_all_pool_infos())
                .map_err(to_py_err)?;
            Ok(infos.into_iter().map(PyPoolInfo::from).collect())
        })
    }

    fn get_pool_info(&self, py: Python<'_>, pool_key: &str) -> PyResult<PyPoolInfo> {
        py.detach(|| {
            let info = io_rt()
                .block_on(self.scoped().get_pool_info(pool_key))
                .map_err(to_py_err)?;
            Ok(PyPoolInfo::from(info))
        })
    }

    /// Atomically claim concurrency slots across one or more pools.
    ///
    /// Args:
    ///     pools: List of (pool_key, slots_needed) tuples.
    ///     run_id: Run identifier.
    ///     step_key: Step identifier.
    ///     priority: Priority for pending queue ordering.
    ///     lease_duration: Lease duration as human-readable string (default "5m").
    #[pyo3(name = "_claim_concurrency_slots", signature = (pools, run_id, step_key, priority=0, lease_duration="5m"))]
    fn claim_concurrency_slots(
        &self,
        py: Python<'_>,
        pools: Vec<(String, u32)>,
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration: &str,
    ) -> PyResult<PyConcurrencyClaimStatus> {
        let secs = crate::utils::parse_duration_secs_u32("lease_duration", lease_duration)?;
        py.detach(|| {
            let status = io_rt()
                .block_on(
                    self.scoped()
                        .claim_concurrency_slots(&pools, run_id, step_key, priority, secs),
                )
                .map_err(to_py_err)?;
            Ok(PyConcurrencyClaimStatus::from(status))
        })
    }

    /// Release all concurrency slots held by a specific step.
    #[pyo3(name = "_free_concurrency_slots")]
    fn free_concurrency_slots(&self, py: Python<'_>, run_id: &str, step_key: &str) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().free_concurrency_slots(run_id, step_key))
                .map_err(to_py_err)
        })
    }

    /// Release all concurrency slots and pending entries for an entire run.
    #[pyo3(name = "_free_concurrency_slots_for_run")]
    fn free_concurrency_slots_for_run(&self, py: Python<'_>, run_id: &str) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().free_concurrency_slots_for_run(run_id))
                .map_err(to_py_err)
        })
    }

    /// Renew the lease on all concurrency slots held by a specific step.
    /// Returns the number of slot rows renewed.
    #[pyo3(name = "_renew_slot_lease", signature = (run_id, step_key, lease_duration="5m"))]
    fn renew_slot_lease(
        &self,
        py: Python<'_>,
        run_id: &str,
        step_key: &str,
        lease_duration: &str,
    ) -> PyResult<u32> {
        let secs = crate::utils::parse_duration_secs_u32("lease_duration", lease_duration)?;
        py.detach(|| {
            io_rt()
                .block_on(self.backend().renew_slot_lease(run_id, step_key, secs))
                .map_err(to_py_err)
        })
    }

    /// Delete all concurrency slot rows whose lease has expired.
    /// Returns the number of expired slot rows removed.
    #[pyo3(name = "_free_expired_leases")]
    fn free_expired_leases(&self, py: Python<'_>) -> PyResult<u32> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().free_expired_leases())
                .map_err(to_py_err)
        })
    }

    fn get_queued_runs(&self, py: Python<'_>) -> PyResult<Vec<PyRunRecord>> {
        py.detach(|| {
            let runs = io_rt()
                .block_on(self.scoped().get_queued_runs())
                .map_err(to_py_err)?;
            Ok(runs.into_iter().map(PyRunRecord::from).collect())
        })
    }

    fn get_pool_slot_holders(&self, py: Python<'_>, pool_key: &str) -> PyResult<Vec<PySlotHolder>> {
        py.detach(|| {
            let holders = io_rt()
                .block_on(self.scoped().get_pool_slot_holders(pool_key))
                .map_err(to_py_err)?;
            Ok(holders.into_iter().map(PySlotHolder::from).collect())
        })
    }

    fn cancel_queued_run(&self, py: Python<'_>, run_id: &str) -> PyResult<bool> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().cancel_queued_run(run_id))
                .map_err(to_py_err)
        })
    }

    /// Create a run record (test helper). Not part of the public API.
    #[pyo3(name = "_create_run", signature = (run_id, job_name, status, start_time, priority=0, tags=vec![], block_reason=None))]
    fn create_run(
        &self,
        py: Python<'_>,
        run_id: &str,
        job_name: &str,
        status: &str,
        start_time: i64,
        priority: i32,
        tags: Vec<(String, String)>,
        block_reason: Option<String>,
    ) -> PyResult<()> {
        use rivers_core::storage::RunRecord;
        let record = RunRecord {
            run_id: run_id.to_string(),
            code_location_id: self.cl().to_string(),
            job_name: if job_name.is_empty() {
                None
            } else {
                Some(job_name.to_string())
            },
            status: parse_run_status(status)?,
            start_time,
            end_time: None,
            tags,
            node_names: vec![],
            priority,
            partition_key: None,
            block_reason,
            launched_by: LaunchedBy::Manual,
        };
        py.detach(|| {
            io_rt()
                .block_on(self.backend().create_run(&record))
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (asset_key, limit=100))]
    fn async_get_events_for_asset<'py>(
        &self,
        py: Python<'py>,
        asset_key: &str,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let asset_key = asset_key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_events_for_asset(&asset_key, limit)
                .await
                .map(|v| v.into_iter().map(PyStoredEvent::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_get_events_for_run<'py>(
        &self,
        py: Python<'py>,
        run_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let run_id = run_id.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .backend()
                .get_events_for_run(&run_id)
                .await
                .map(|v| v.into_iter().map(PyStoredEvent::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (asset_key, partition=None))]
    fn async_get_latest_materialization<'py>(
        &self,
        py: Python<'py>,
        asset_key: &str,
        partition: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let asset_key = asset_key.to_string();
        let partition = partition.map(|s| s.to_string());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_latest_materialization(&asset_key, partition.as_deref())
                .await
                .map(|opt| opt.map(PyStoredEvent::from))
                .map_err(to_py_err)
        })
    }

    fn async_get_asset_record<'py>(
        &self,
        py: Python<'py>,
        asset_key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let asset_key = asset_key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_asset_record(&asset_key)
                .await
                .map(|opt| opt.map(PyAssetRecord::from))
                .map_err(to_py_err)
        })
    }

    fn async_get_asset_records<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_asset_records()
                .await
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_get_assets_by_tag<'py>(
        &self,
        py: Python<'py>,
        tag: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let tag = tag.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_assets_by_tag(&tag)
                .await
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_get_assets_by_kind<'py>(
        &self,
        py: Python<'py>,
        kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let kind = kind.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_assets_by_kind(&kind)
                .await
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_get_assets_by_group<'py>(
        &self,
        py: Python<'py>,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let group = group.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_assets_by_group(&group)
                .await
                .map(|v| v.into_iter().map(PyAssetRecord::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_get_run<'py>(&self, py: Python<'py>, run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let run_id = run_id.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .backend()
                .get_run(&run_id)
                .await
                .map(|opt| opt.map(PyRunRecord::from))
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (limit=100, status=None))]
    fn async_get_runs<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        status: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let status = status.map(parse_run_status).transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_runs(limit, status)
                .await
                .map(|v| v.into_iter().map(PyRunRecord::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (automation_name, limit=100))]
    fn async_get_ticks<'py>(
        &self,
        py: Python<'py>,
        automation_name: &str,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let automation_name = automation_name.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_ticks(&automation_name, limit)
                .await
                .map(|v| v.into_iter().map(PyStoredTick::from).collect::<Vec<_>>())
                .map_err(to_py_err)
        })
    }

    fn async_kv_get<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let key = key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle.backend().kv_get(&key).await.map_err(to_py_err)
        })
    }

    fn async_kv_set<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let key = key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .backend()
                .kv_set(&key, &value)
                .await
                .map_err(to_py_err)
        })
    }

    fn async_add_dynamic_partitions<'py>(
        &self,
        py: Python<'py>,
        partitions_def_name: &str,
        partition_keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let name = partitions_def_name.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .add_dynamic_partitions(&name, &partition_keys)
                .await
                .map_err(to_py_err)
        })
    }

    fn async_delete_dynamic_partition<'py>(
        &self,
        py: Python<'py>,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let name = partitions_def_name.to_string();
        let key = partition_key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .delete_dynamic_partition(&name, &key)
                .await
                .map_err(to_py_err)
        })
    }

    fn async_get_dynamic_partitions<'py>(
        &self,
        py: Python<'py>,
        partitions_def_name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let name = partitions_def_name.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .get_dynamic_partitions(&name)
                .await
                .map_err(to_py_err)
        })
    }

    fn async_has_dynamic_partition<'py>(
        &self,
        py: Python<'py>,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let name = partitions_def_name.to_string();
        let key = partition_key.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .scoped()
                .has_dynamic_partition(&name, &key)
                .await
                .map_err(to_py_err)
        })
    }

    fn is_cancelled(&self, py: Python<'_>, run_id: &str) -> PyResult<bool> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().is_cancelled(run_id))
                .map_err(to_py_err)
        })
    }

    fn request_cancellation(&self, py: Python<'_>, run_id: &str) -> PyResult<()> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().request_cancellation(run_id))
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (run_id, status, completed_steps, total_steps, message=None))]
    fn set_run_outcome(
        &self,
        py: Python<'_>,
        run_id: &str,
        status: &str,
        completed_steps: u32,
        total_steps: u32,
        message: Option<&str>,
    ) -> PyResult<()> {
        use rivers_core::storage::RunOutcome;
        let outcome = match status {
            "Success" => RunOutcome::Success {
                completed_steps,
                total_steps,
            },
            "Failure" => RunOutcome::Failure {
                message: message.unwrap_or("unknown error").to_string(),
                completed_steps,
                total_steps,
            },
            "Cancelled" => RunOutcome::Cancelled {
                completed_steps,
                total_steps,
            },
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid outcome status: '{other}'. Expected 'Success', 'Failure', or 'Cancelled'"
                )));
            }
        };
        py.detach(|| {
            io_rt()
                .block_on(self.backend().set_run_outcome(run_id, &outcome))
                .map_err(to_py_err)
        })
    }

    fn get_run_progress(&self, py: Python<'_>, run_id: &str) -> PyResult<(u32, u32)> {
        let progress = py.detach(|| {
            io_rt()
                .block_on(self.backend().get_run_progress(run_id))
                .map_err(to_py_err)
        })?;
        Ok((progress.completed_steps, progress.total_steps))
    }
}

pub fn register_storage_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "storage", [
        PyStorage as "Storage",
    ], [
        PyStorageType,
        PyStoredEvent,
        PyStoredTick,
        PyStaleCause,
        PyAssetRecord,
        PyLaunchedBy,
        PyRunRecord,
        PyPoolLimit,
        PyPoolInfo,
        PyPoolBlockDetail,
        PySlotHolder,
        PyBlockReason,
        PyConcurrencyClaimStatus,
    ])
}
