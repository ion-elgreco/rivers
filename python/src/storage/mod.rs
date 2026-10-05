//! PyStorage — Python wrapper for the SurrealDB storage backend.
//!
//! `PyStorage` holds a [`ScopedStorageHandle`] that bundles the underlying
//! `Arc<SurrealStorage>` with a [`CodeLocationContext`] (sourced from
//! `RIVERS_CODE_LOCATION_ID`), so per-CL query methods don't need a CL
//! argument from Python. Each query is exposed twice: a sync method that
//! releases the GIL while awaiting the async storage call, and an
//! `async_*` variant for `await`-friendly use from `asyncio`.
mod pools;
mod records;

pub use pools::*;
pub use records::*;

use std::sync::Arc;

use pyo3::prelude::*;

use crate::errors::StorageError;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    CodeLocationContext, LaunchedBy, RunStatus, ScopedStorage, ScopedStorageHandle,
    StaleCauseCategory, StaleStatus, StorageBackend,
};

use crate::partitions::PyPartitionKey;
use crate::runtime::io_rt;

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

pub(super) fn format_run_status(s: RunStatus) -> &'static str {
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

pub(crate) trait HoldsStorage {
    fn storage(&self) -> &Arc<SurrealStorage>;
}

impl HoldsStorage for Arc<SurrealStorage> {
    fn storage(&self) -> &Arc<SurrealStorage> {
        self
    }
}

impl HoldsStorage for ScopedStorageHandle<SurrealStorage> {
    fn storage(&self) -> &Arc<SurrealStorage> {
        self.backend()
    }
}

/// A storage handle owned by a Python object or a resolved repository.
/// Dropping the last handle closes the storage, which can wait for its
/// datastore to shut down.
pub(crate) struct DetachOnClose<H: HoldsStorage>(Option<H>);

impl<H: HoldsStorage> DetachOnClose<H> {
    pub(crate) fn new(handle: H) -> Self {
        Self(Some(handle))
    }
}

impl<H: HoldsStorage> std::ops::Deref for DetachOnClose<H> {
    type Target = H;

    fn deref(&self) -> &H {
        self.0.as_ref().expect("taken only on drop")
    }
}

impl<H: HoldsStorage> Drop for DetachOnClose<H> {
    fn drop(&mut self) {
        let Some(handle) = self.0.take() else {
            return;
        };
        let storage = Arc::clone(handle.storage());
        drop(handle);
        let Some(storage) = Arc::into_inner(storage) else {
            return;
        };
        // Inside a runtime the storage closes in the background.
        if tokio::runtime::Handle::try_current().is_err() {
            Python::try_attach(|py| py.detach(|| drop(storage)));
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
    pub(crate) handle: DetachOnClose<ScopedStorageHandle<SurrealStorage>>,
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
            handle: DetachOnClose::new(ScopedStorageHandle::new(
                Arc::new(storage),
                Self::detect_storage_cl(),
            )),
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
        tracing::info!(target: "rivers::storage", backend = "embedded", path = %path, "storage ready (own runtime)");
        Ok(Self::from_storage(storage, PyStorageType::Embedded))
    }

    /// CLI scratch store: embedded storage on its own runtime, so dropping it
    /// releases its files before the scratch directory is removed.
    #[staticmethod]
    fn _scratch(py: Python<'_>, path: &str) -> PyResult<Self> {
        Self::_test_embedded(py, path)
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

    fn get_run_logs(&self, py: Python<'_>, run_id: &str) -> PyResult<Vec<PyStoredLog>> {
        py.detach(|| {
            io_rt()
                .block_on(self.backend().get_run_logs(run_id))
                .map(|v| v.into_iter().map(PyStoredLog::from).collect())
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
                        .claim_concurrency_slots(&pools, run_id, step_key, priority, secs, None),
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
    #[pyo3(name = "_create_run", signature = (run_id, job_name, status, start_time, priority=0, tags=vec![], block_reason=None, node_names=vec![], action=None, config=None))]
    #[allow(clippy::too_many_arguments)]
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
        node_names: Vec<String>,
        action: Option<String>,
        config: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        use rivers_core::storage::RunRecord;
        let config = crate::config::run_config::run_config_to_json(py, config.as_ref())?;
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
            node_names,
            priority,
            partition_key: None,
            block_reason,
            launched_by: LaunchedBy::Manual { user: None },
            action,
            config,
        };
        py.detach(|| {
            io_rt()
                .block_on(self.backend().create_run(&record))
                .map_err(to_py_err)
        })
    }

    /// Create a backfill record (test helper). Not part of the public API.
    #[pyo3(name = "_create_backfill", signature = (backfill_id, asset_selection, partition_keys, status, create_time))]
    fn create_backfill(
        &self,
        py: Python<'_>,
        backfill_id: &str,
        asset_selection: Vec<String>,
        partition_keys: Vec<String>,
        status: &str,
        create_time: i64,
    ) -> PyResult<()> {
        use rivers_core::storage::{
            BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy, PartitionKey,
        };
        let status = match status {
            "Requested" => BackfillStatus::Requested,
            "InProgress" => BackfillStatus::InProgress,
            other => {
                return Err(StorageError::new_err(format!(
                    "Unknown backfill status: {other}"
                )));
            }
        };
        let record = BackfillRecord {
            backfill_id: backfill_id.to_string(),
            code_location_id: self.cl().to_string(),
            status,
            strategy: BackfillStrategy::SingleRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection,
            job_name: None,
            partition_keys: partition_keys
                .into_iter()
                .map(|k| PartitionKey::Single { keys: vec![k] })
                .collect(),
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 4,
            tags: vec![],
            create_time,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        py.detach(|| {
            io_rt()
                .block_on(self.backend().create_backfill(&record))
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

    fn async_get_run_logs<'py>(
        &self,
        py: Python<'py>,
        run_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let run_id = run_id.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .backend()
                .get_run_logs(&run_id)
                .await
                .map(|v| v.into_iter().map(PyStoredLog::from).collect::<Vec<_>>())
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
        PyStoredLog,
        PyStoredTick,
        PyStaleCause,
        PyAssetRecord,
        PyUserRef,
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
