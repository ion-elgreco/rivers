//! SurrealDB storage backend implementation.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::{self, Any};
use surrealdb::types::{Bytes, RecordId, SurrealValue};

#[cfg(test)]
use super::DEFAULT_CODE_LOCATION_ID;
use super::{
    AssetRecord, BackfillFilter, BackfillRecord, BackfillStatus, BackfillsPage, BackfillsSummary,
    BlockReason, ConcurrencyClaimStatus, ConditionEvalRecord, ConditionTickRecord,
    CoordinatorRunInfo, EventRecord, EventType, LogRecord, PartitionKey, PerCodeLocationStorage,
    PoolBlockDetail, PoolInfo, PoolLimit, RunFilter, RunOutcome, RunProgress, RunRecord, RunStatus,
    RunsPage, RunsSummary, SlotHolder, StorageBackend, StoredConditionEval, StoredConditionTick,
    StoredEvent, StoredLog, StoredTick, TickRecord,
};
use super::{now_nanos, run_queued_event};

mod migration;
pub use crate::storage::migration::{Capability, SchemaMigrationNeeded};

#[derive(Debug, SurrealValue)]
struct DbKv {
    key: String,
    value: Bytes,
}

#[derive(Debug, SurrealValue)]
struct DbDynamicPartition {
    code_location_id: String,
    partitions_def_name: String,
    partition_key: String,
    create_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbEventWrite {
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    code_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbRunLogWrite {
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
}

impl From<&LogRecord> for DbRunLogWrite {
    fn from(l: &LogRecord) -> Self {
        Self {
            code_location_id: l.code_location_id.clone(),
            run_id: l.run_id.clone(),
            step_key: l.step_key.clone(),
            timestamp: l.timestamp,
            stdout: l.stdout.clone(),
            stderr: l.stderr.clone(),
            logs: l.logs.clone(),
        }
    }
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredRunLog {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
}

impl DbStoredRunLog {
    fn into_stored_log(self) -> StoredLog {
        StoredLog {
            id: record_id_str(&self.id),
            code_location_id: self.code_location_id,
            run_id: self.run_id,
            step_key: self.step_key,
            timestamp: self.timestamp,
            stdout: self.stdout,
            stderr: self.stderr,
            logs: self.logs,
        }
    }
}

/// One `asset_partitions` row, written in bulk via `upsert_asset_partitions`.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbAssetPartitionWrite {
    code_location_id: String,
    asset_key: String,
    partition_key: PartitionKey,
    last_event_id: String,
    last_run_id: String,
    last_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredEvent {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    #[serde(default)]
    code_version: Option<String>,
    #[serde(default)]
    input_data_versions: Vec<(String, String)>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbTickWrite {
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredTick {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbConditionTickWrite {
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredConditionTick {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbConditionEvalWrite {
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    selection_json: Option<Bytes>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredConditionEval {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    #[serde(default)]
    selection_json: Option<Bytes>,
}

impl From<&ConditionTickRecord> for DbConditionTickWrite {
    fn from(t: &ConditionTickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            timestamp: t.timestamp,
            total_evaluated: t.total_evaluated as i64,
            total_fired: t.total_fired as i64,
            eval_duration_us: t.eval_duration_us as i64,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
        }
    }
}

impl DbStoredConditionTick {
    fn into_stored(self) -> StoredConditionTick {
        StoredConditionTick {
            id: record_id_str(&self.id),
            code_location_id: self.code_location_id,
            timestamp: self.timestamp,
            total_evaluated: self.total_evaluated as u32,
            total_fired: self.total_fired as u32,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
        }
    }
}

impl From<&ConditionEvalRecord> for DbConditionEvalWrite {
    fn from(e: &ConditionEvalRecord) -> Self {
        Self {
            code_location_id: e.code_location_id.clone(),
            asset_key: e.asset_key.clone(),
            tick_id: e.tick_id.clone(),
            timestamp: e.timestamp,
            fired: e.fired,
            eval_duration_us: e.eval_duration_us as i64,
            run_ids: e.run_ids.clone(),
            tree_json: Bytes::from(e.tree_json.clone()),
            selection_json: e.selection_json.as_ref().map(|b| Bytes::from(b.clone())),
        }
    }
}

impl DbStoredConditionEval {
    fn into_stored(self) -> StoredConditionEval {
        StoredConditionEval {
            id: record_id_str(&self.id),
            code_location_id: self.code_location_id,
            asset_key: self.asset_key,
            tick_id: self.tick_id,
            timestamp: self.timestamp,
            fired: self.fired,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            tree_json: self.tree_json.to_vec(),
            selection_json: self.selection_json.map(|b| b.to_vec()),
        }
    }
}

impl From<&TickRecord> for DbTickWrite {
    fn from(t: &TickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            automation_name: t.automation_name.clone(),
            automation_type: t.automation_type.clone(),
            status: t.status.clone(),
            timestamp: t.timestamp,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
            skip_reason: t.skip_reason.clone(),
            error: t.error.clone(),
            cursor: t.cursor.clone(),
        }
    }
}

impl DbStoredTick {
    fn into_stored_tick(self) -> StoredTick {
        StoredTick {
            id: record_id_str(&self.id),
            code_location_id: self.code_location_id,
            automation_name: self.automation_name,
            automation_type: self.automation_type,
            status: self.status,
            timestamp: self.timestamp,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
            skip_reason: self.skip_reason,
            error: self.error,
            cursor: self.cursor,
        }
    }
}

impl From<&EventRecord> for DbEventWrite {
    fn from(e: &EventRecord) -> Self {
        Self {
            code_location_id: e.code_location_id.clone(),
            event_type: e.event_type.type_name().to_string(),
            asset_key: e.asset_key.clone(),
            run_id: e.run_id.clone(),
            partition_key: e.partition_key.clone(),
            timestamp: e.timestamp,
            sort_order: e.event_type.sort_order(),
            metadata: e.metadata.clone(),
            data_version: e.event_type.data_version().map(|s| s.to_string()),
            code_version: None,
            input_data_versions: Vec::new(),
        }
    }
}

impl DbStoredEvent {
    fn into_stored_event(self) -> StoredEvent {
        let event_type = EventType::from_type_name(&self.event_type, self.data_version)
            .unwrap_or(EventType::StepFailure);
        StoredEvent {
            id: record_id_str(&self.id),
            event_type,
            asset_key: self.asset_key,
            run_id: self.run_id,
            partition_key: self.partition_key,
            timestamp: self.timestamp,
            metadata: self.metadata,
            code_version: self.code_version,
            input_data_versions: self.input_data_versions,
        }
    }
}

/// Identifies which underlying SurrealDB transport a [`SurrealStorage`] was constructed with.
#[derive(Debug, Clone)]
pub enum SurrealBackendKind {
    Embedded { path: String },
    Memory,
    Remote { endpoint: String },
}

impl SurrealBackendKind {
    pub fn label(&self) -> String {
        match self {
            SurrealBackendKind::Embedded { path } => {
                format!("SurrealDB (Embedded RocksDB at {path})")
            }
            SurrealBackendKind::Memory => "SurrealDB (In-Memory)".to_string(),
            SurrealBackendKind::Remote { endpoint } => {
                format!("SurrealDB (Remote: {endpoint})")
            }
        }
    }
}

/// Default SurrealDB namespace used by every rivers connection.
pub const DEFAULT_NAMESPACE: &str = "rivers";

/// Default SurrealDB database used by every rivers connection.
pub const DEFAULT_DATABASE: &str = "main";

/// Connection parameters for [`SurrealStorage::connect`].
#[derive(Debug, Clone)]
pub struct SurrealConnectConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub credentials: Option<SurrealCredentials>,
}

impl SurrealConnectConfig {
    /// Config with default `rivers / main` scope and no credentials.
    pub fn unauthenticated(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            credentials: None,
        }
    }

    /// Attach database-scoped credentials.
    pub fn with_credentials(mut self, username: String, password: String) -> Self {
        self.credentials = Some(SurrealCredentials::Database { username, password });
        self
    }
}

/// Credentials used during [`SurrealStorage::connect`].
#[derive(Debug, Clone)]
pub enum SurrealCredentials {
    /// Sign in as a `DEFINE USER ... ON DATABASE` user.
    Database { username: String, password: String },
}

pub struct SurrealStorage {
    db: Surreal<Any>,
    retry_config: super::retry::StorageRetryConfig,
    backend_kind: SurrealBackendKind,
    /// Tokio runtime that hosts the router task when constructed via a `*_blocking` constructor.
    runtime: Option<tokio::runtime::Runtime>,
}

impl Drop for SurrealStorage {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            let _ = std::mem::replace(&mut self.db, Surreal::init());
            if tokio::runtime::Handle::try_current().is_ok() {
                runtime.shutdown_background();
            } else {
                runtime.shutdown_timeout(std::time::Duration::from_secs(5));
            }
        }
    }
}

/// Build a dedicated multi-thread runtime for one [`SurrealStorage`]
fn build_storage_runtime() -> tokio::runtime::Runtime {
    static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name(format!("rivers-stg-{id}"))
        .worker_threads(2)
        .build()
        .expect("Failed to create per-storage tokio runtime.")
}

impl SurrealStorage {
    pub fn backend_kind(&self) -> &SurrealBackendKind {
        &self.backend_kind
    }

    /// Embedded storage backed by RocksDB at the given path. Opened
    /// `ReadWrite`; use [`Self::new_embedded_with_capability`] for a read-only
    /// or migrating opener.
    pub async fn new_embedded(path: &str) -> Result<Self> {
        Self::new_embedded_with_retry(
            path,
            super::retry::StorageRetryConfig::default(),
            Capability::ReadWrite,
        )
        .await
    }

    /// Embedded storage opened with an explicit [`Capability`] (the UI opens
    /// `Read`; `rivers db migrate` opens `Migrate`).
    pub async fn new_embedded_with_capability(path: &str, cap: Capability) -> Result<Self> {
        Self::new_embedded_with_retry(path, super::retry::StorageRetryConfig::default(), cap).await
    }

    /// Variant of [`Self::new_embedded`] with a custom retry policy.
    pub async fn new_embedded_with_retry(
        path: &str,
        retry_config: super::retry::StorageRetryConfig,
        cap: Capability,
    ) -> Result<Self> {
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "embedded",
            path = %path,
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!(path = %path, "connecting rocksdb engine");
            let db = any::connect(format!("rocksdb://{path}"))
                .await
                .context("failed to open RocksDB")?;
            tracing::debug!(
                ns = DEFAULT_NAMESPACE,
                db = DEFAULT_DATABASE,
                "selecting namespace/database"
            );
            db.use_ns(DEFAULT_NAMESPACE)
                .use_db(DEFAULT_DATABASE)
                .await?;
            migration::ensure_compatible(&db, cap).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "embedded",
            path = %path,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Embedded {
                path: path.to_string(),
            },
            runtime: None,
        })
    }

    /// In-memory storage (useful for tests).
    pub async fn new_memory() -> Result<Self> {
        Self::new_memory_with_retry(super::retry::StorageRetryConfig::default()).await
    }

    /// Variant of [`Self::new_memory`] with a custom retry policy.
    pub async fn new_memory_with_retry(
        retry_config: super::retry::StorageRetryConfig,
    ) -> Result<Self> {
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "memory",
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!("connecting in-memory engine");
            let db = any::connect("mem://")
                .await
                .context("failed to create in-memory DB")?;
            tracing::debug!(
                ns = DEFAULT_NAMESPACE,
                db = DEFAULT_DATABASE,
                "selecting namespace/database"
            );
            db.use_ns(DEFAULT_NAMESPACE)
                .use_db(DEFAULT_DATABASE)
                .await?;
            tracing::debug!("applying schema");
            migration::ensure_compatible(&db, Capability::ReadWrite).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "memory",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Memory,
            runtime: None,
        })
    }

    /// Connect to a remote SurrealDB server.
    pub async fn connect(config: SurrealConnectConfig) -> Result<Self> {
        Self::connect_with_retry(
            config,
            super::retry::StorageRetryConfig::default(),
            Capability::ReadWrite,
        )
        .await
    }

    /// Connect with an explicit [`Capability`] — the production UI opens `Read`.
    pub async fn connect_with_capability(
        config: SurrealConnectConfig,
        cap: Capability,
    ) -> Result<Self> {
        Self::connect_with_retry(config, super::retry::StorageRetryConfig::default(), cap).await
    }

    /// Variant of [`Self::connect`] with a custom retry policy.
    pub async fn connect_with_retry(
        config: SurrealConnectConfig,
        retry_config: super::retry::StorageRetryConfig,
        cap: Capability,
    ) -> Result<Self> {
        let SurrealConnectConfig {
            endpoint,
            namespace,
            database,
            credentials,
        } = config;
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "remote",
            endpoint = %endpoint,
            ns = %namespace,
            db = %database,
            authenticated = credentials.is_some(),
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!(endpoint = %endpoint, "connecting remote surrealdb");
            let db = any::connect(&endpoint)
                .await
                .context("failed to connect to remote SurrealDB")?;
            if let Some(creds) = credentials.clone() {
                match creds {
                    SurrealCredentials::Database { username, password } => {
                        tracing::debug!(
                            ns = %namespace,
                            db = %database,
                            username = %username,
                            "authenticating (database scope)"
                        );
                        db.signin(surrealdb::opt::auth::Database {
                            namespace: namespace.clone(),
                            database: database.clone(),
                            username,
                            password,
                        })
                        .await
                        .context("SurrealDB signin failed")?;
                    }
                }
            }
            tracing::debug!(
                ns = %namespace,
                db = %database,
                "selecting namespace/database"
            );
            db.use_ns(&namespace).use_db(&database).await?;
            migration::ensure_compatible(&db, cap).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "remote",
            endpoint = %endpoint,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Remote {
                endpoint: endpoint.to_string(),
            },
            runtime: None,
        })
    }

    /// [`SurrealStorage`] on a dedicated runtime owned for its lifetime — test fixtures only.
    pub fn new_embedded_blocking(path: &str) -> Result<Self> {
        let runtime = build_storage_runtime();
        let mut storage = runtime.block_on(Self::new_embedded(path))?;
        storage.runtime = Some(runtime);
        Ok(storage)
    }

    /// See [`Self::new_embedded_blocking`]. In-memory variant.
    pub fn new_memory_blocking() -> Result<Self> {
        let runtime = build_storage_runtime();
        let mut storage = runtime.block_on(Self::new_memory())?;
        storage.runtime = Some(runtime);
        Ok(storage)
    }

    /// Paginated, filtered slice of runs plus total matching row count.
    pub async fn get_all_runs_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_runs_page`].
    pub async fn get_runs_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn runs_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            // "Queued" is the whole queue system: waiting (Queued) plus
            // dequeued-but-launching (NotStarted) — same bucket the runs
            // summary counts.
            if filter.status == Some(RunStatus::Queued) {
                wheres.push("status IN ['Queued', 'NotStarted']");
            } else if filter.status.is_some() {
                wheres.push("status = $status");
            }
            if filter.job_name.is_some() {
                wheres.push("job_name = $job_exact");
            }
            if filter.job_substring.is_some() {
                wheres.push(
                    "job_name IS NOT NONE AND \
                     string::contains(string::lowercase(job_name), $job_pat)",
                );
            }
            if filter.asset_substring.is_some() {
                wheres.push(
                    "array::any(node_names, |$a| string::contains(string::lowercase($a), $asset_pat))",
                );
            }
            if filter.partition_substring.is_some() {
                wheres.push(
                    "array::any(tags, |$t| ($t[0] = 'partition' OR $t[0] = 'partition_key') \
                     AND string::contains(string::lowercase($t[1]), $partition_pat))",
                );
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM runs {where_clause} ORDER BY start_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM runs {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }
            if let Some(name) = &filter.job_name {
                q = q.bind(("job_exact", name.clone()));
            }
            if let Some(pat) = &filter.job_substring {
                q = q.bind(("job_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.asset_substring {
                q = q.bind(("asset_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.partition_substring {
                q = q.bind(("partition_pat", pat.to_lowercase()));
            }

            let mut result = q.await?;
            let rows: Vec<RunRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(RunsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// A page of an asset's events (newest first) restricted to `event_types`, plus the total count.
    pub async fn get_events_for_asset_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        event_types: &[String],
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types \
                     ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types GROUP ALL;",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("types", event_types.to_vec()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A run's step events (`StepStart`/`Success`/`Failure`) — backs the timeline/DAG.
    pub async fn get_run_step_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE run_id = $id \
                     AND event_type IN ['StepStart', 'StepSuccess', 'StepFailure'] \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    /// A page of a run's events, optionally scoped to one asset, plus the total.
    pub async fn get_run_structured_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let asset_clause = if asset_key.is_some() {
                " AND asset_key = $ak"
            } else {
                ""
            };
            let sql = format!(
                "SELECT * FROM events WHERE run_id = $id{asset_clause} \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM events WHERE run_id = $id{asset_clause} GROUP ALL;"
            );
            let mut q = self
                .db
                .query(sql)
                .bind(("id", run_id.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(ak) = asset_key {
                q = q.bind(("ak", ak.to_string()));
            }
            let mut result = q.await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A page of one asset's events of a single type within a run + total.
    pub async fn get_run_asset_events_page(
        &self,
        run_id: &str,
        asset_key: &str,
        event_type: &str,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type GROUP ALL;",
                )
                .bind(("id", run_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("type", event_type.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// Aggregate run counts for the runs-list page header.
    pub async fn get_all_runs_summary(&self, cutoff_24h_ns: i64) -> Result<RunsSummary> {
        self.runs_summary_impl(None, cutoff_24h_ns).await
    }

    /// Per-CL variant of [`Self::get_all_runs_summary`].
    pub async fn get_runs_summary(
        &self,
        code_location_id: &str,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.runs_summary_impl(Some(code_location_id), cutoff_24h_ns)
            .await
    }

    async fn runs_summary_impl(
        &self,
        code_location_id: Option<&str>,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        super::retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM runs {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Started' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status IN ['Queued', 'NotStarted'] GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Failure' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Success' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}start_time > $cutoff GROUP ALL;",
            );
            let mut q = self.db.query(sql).bind(("cutoff", cutoff_24h_ns));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(RunsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                queued: take(&mut result, 2)?,
                failure: take(&mut result, 3)?,
                success: take(&mut result, 4)?,
                last_24h: take(&mut result, 5)?,
            })
        })
        .await
    }

    /// For each requested job name, return the most recent run if any.
    pub async fn get_all_last_run_per_job(
        &self,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(None, job_names).await
    }

    /// Per-CL variant of [`Self::get_all_last_run_per_job`].
    pub async fn get_last_run_per_job(
        &self,
        code_location_id: &str,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(Some(code_location_id), job_names)
            .await
    }

    async fn last_run_per_job_impl(
        &self,
        code_location_id: Option<&str>,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        super::retry::with_retry(&self.retry_config, || async {
            use std::fmt::Write;

            if job_names.is_empty() {
                return Ok(Vec::new());
            }

            let cl_filter = if code_location_id.is_some() {
                " AND code_location_id = $cl"
            } else {
                ""
            };
            let mut sql = String::with_capacity(job_names.len() * 128);
            for i in 0..job_names.len() {
                let _ = writeln!(
                    sql,
                    "SELECT * FROM runs WHERE job_name = $job_{i}{cl_filter} \
                     ORDER BY start_time DESC LIMIT 1;"
                );
            }
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            for (i, name) in job_names.iter().enumerate() {
                q = q.bind((format!("job_{i}"), name.clone()));
            }
            let mut result = q.await?;

            let mut out = Vec::with_capacity(job_names.len());
            for (i, name) in job_names.iter().enumerate() {
                let rows: Vec<RunRecord> = result.take(i)?;
                if let Some(run) = rows.into_iter().next() {
                    out.push((name.clone(), run));
                }
            }
            Ok(out)
        })
        .await
    }

    /// Paginated + filtered backfills list. Mirrors `get_all_runs_page`.
    pub async fn get_all_backfills_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_page`].
    pub async fn get_backfills_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn backfills_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            if filter.status.is_some() {
                wheres.push("status = $status");
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM backfills {where_clause} ORDER BY create_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM backfills {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }

            let mut result = q.await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(BackfillsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// Aggregate backfill counts for the list-page status pills. Unfiltered.
    pub async fn get_all_backfills_summary(&self) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(None).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_summary`].
    pub async fn get_backfills_summary(&self, code_location_id: &str) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(Some(code_location_id)).await
    }

    async fn backfills_summary_impl(
        &self,
        code_location_id: Option<&str>,
    ) -> Result<BackfillsSummary> {
        super::retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM backfills {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'InProgress' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedSuccess' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedFailed' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'Canceled' GROUP ALL;",
            );
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(BackfillsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                completed_success: take(&mut result, 2)?,
                completed_failed: take(&mut result, 3)?,
                canceled: take(&mut result, 4)?,
            })
        })
        .await
    }

    /// Subscribe to change notifications on an arbitrary table via a SurrealDB LIVE query.
    pub async fn subscribe_table(
        &self,
        table: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, ()>> {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use surrealdb::types::{Action, Object};
        let stream = self
            .db
            .select::<Vec<Object>>(table.to_string())
            .live()
            .await?;
        let table_owned: Arc<str> = Arc::from(table);
        Ok(stream
            .filter_map(move |result| {
                let table = Arc::clone(&table_owned);
                async move {
                    let notif = match result {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::warn!(
                                target: "rivers::storage",
                                table = %table,
                                error = %e,
                                "live query yielded error"
                            );
                            return None;
                        }
                    };
                    match notif.action {
                        Action::Create | Action::Update | Action::Delete => Some(()),
                        _ => None,
                    }
                }
            })
            .boxed())
    }
}

/// Render a record id as the opaque string the storage API hands out.
///
/// The `{:?}` is load-bearing: this exact text is persisted in
/// `condition_evals.tick_id` and in the `last_event_id` columns, so changing
/// the format orphans every row written by an older build. It needs a
/// migration, not a tidy-up.
fn record_id_str(id: &RecordId) -> String {
    format!("{}:{:?}", id.table.as_str(), id.key)
}

#[derive(Debug, SurrealValue, serde::Deserialize)]
struct OptStringField {
    value: Option<String>,
}

impl SurrealStorage {
    /// Persist a queued `RunRecord` and emit its `RunQueued` event in one step.
    pub async fn enqueue_run(&self, record: &RunRecord) -> Result<()> {
        let event = DbEventWrite::from(&run_queued_event(record));
        let result = super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     CREATE runs CONTENT $run;\n\
                     CREATE events CONTENT $event;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("run", record.clone()))
                .bind(("event", event.clone()))
                .await
                .context("failed to enqueue run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_run", &record.run_id)
    }

    /// Batch counterpart to [`Self::enqueue_run`].
    pub async fn enqueue_runs(&self, records: &[RunRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let events: Vec<DbEventWrite> = records
            .iter()
            .map(|r| DbEventWrite::from(&run_queued_event(r)))
            .collect();
        let result = super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .await
                .context("failed to enqueue runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_runs", &format!("batch[{}]", records.len()))
    }

    /// [`Self::enqueue_runs`] plus linking the run ids onto the owning
    /// backfill, all in one transaction — `run_ids` can never disagree with
    /// the runs table.
    /// Returns `false` when the backfill was canceled while the batch was in
    /// flight — the runs are committed but immediately swept back out of the
    /// queue, so no child outlives the cancel regardless of which side of
    /// the race committed first.
    pub async fn enqueue_backfill_runs(
        &self,
        records: &[RunRecord],
        backfill_id: &str,
    ) -> Result<bool> {
        if records.is_empty() {
            return Ok(true);
        }
        let events: Vec<DbEventWrite> = records
            .iter()
            .map(|r| DbEventWrite::from(&run_queued_event(r)))
            .collect();
        let run_ids: Vec<String> = records.iter().map(|r| r.run_id.clone()).collect();
        let result = super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     UPDATE backfills SET run_ids = array::union(run_ids, $run_ids) \
                         WHERE backfill_id = $backfill_id;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .bind(("run_ids", run_ids.clone()))
                .bind(("backfill_id", backfill_id.to_string()))
                .await
                .context("failed to enqueue backfill runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(
            result,
            "enqueue_backfill_runs",
            &format!("batch[{}]", records.len()),
        )?;
        // Cancel-vs-submit race repair: a cancel can land between the
        // InProgress flip and this commit, and its cascade over `run_ids`
        // ran before these rows existed.
        let backfill = self
            .get_backfill(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?;
        if backfill.status == BackfillStatus::InProgress {
            return Ok(true);
        }
        for record in records {
            self.cancel_queued_run(&record.run_id).await?;
        }
        Ok(false)
    }

    /// Conditionally link a run id to a live backfill — `false` when the
    /// backfill is no longer `InProgress`, in which case the caller must not
    /// create the run. Linked before the run exists, so a cancel cascade can
    /// always reach every child that will ever exist.
    pub async fn link_backfill_run(&self, backfill_id: &str, run_id: &str) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET run_ids = array::union(run_ids, [$run_id]) \
                         WHERE backfill_id = $id AND status = 'InProgress'; \
                     SELECT count() AS total FROM backfills \
                         WHERE backfill_id = $id AND status = 'InProgress' \
                         AND $run_id IN run_ids GROUP ALL",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("run_id", run_id.to_string()))
                .await?;
            let count: Option<u32> = result.take((1, "total"))?;
            Ok(count.unwrap_or(0) > 0)
        })
        .await
    }

    /// Flip a zero-run `InProgress` backfill back to `Requested` so the
    /// pickup loop re-executes it (guarded — a backfill that gained runs or
    /// moved on is left alone). Returns whether the flip applied.
    pub async fn resume_stalled_backfill(&self, backfill_id: &str) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct IdRow {
                #[allow(dead_code)]
                backfill_id: String,
            }
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET status = 'Requested' \
                         WHERE backfill_id = $id AND status = 'InProgress' \
                         AND array::len(run_ids) = 0 \
                         RETURN backfill_id",
                )
                .bind(("id", backfill_id.to_string()))
                .await?;
            let flipped: Vec<IdRow> = result.take(0)?;
            Ok(!flipped.is_empty())
        })
        .await
    }

    /// Mark a backfill `CompletedFailed` with the submission error recorded.
    pub async fn fail_backfill(&self, backfill_id: &str, error: &str) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'CompletedFailed', \
                         end_time = $end_time, error = $error \
                         WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("end_time", now_nanos()))
                .bind(("error", error.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_code_version(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<String>> {
        let mut result = self
            .db
            .query("SELECT code_version AS value FROM assets WHERE code_location_id = $cl AND asset_key = $key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .await?;
        let rows: Vec<OptStringField> = result.take(0)?;
        Ok(rows.first().and_then(|r| r.value.clone()))
    }

    /// Bulk upsert `asset_partitions` rows, matched on the table's UNIQUE (code_location_id, asset_key, partition_key) index.
    async fn upsert_asset_partitions(&self, rows: Vec<DbAssetPartitionWrite>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.db
            .query(
                "INSERT INTO asset_partitions $rows ON DUPLICATE KEY UPDATE \
                 last_event_id = $input.last_event_id, \
                 last_run_id = $input.last_run_id, \
                 last_timestamp = $input.last_timestamp",
            )
            .bind(("rows", rows))
            .await?
            .check()?;
        Ok(())
    }

    async fn query_pool_usage(
        &self,
        code_location_id: &str,
        pool_key: &str,
        now_ns: i64,
    ) -> Result<(PoolLimit, u32)> {
        let mut result = self
            .db
            .query(
                "SELECT * FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $pool_key LIMIT 1; \
                 SELECT math::sum(slots_consumed) AS total FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now GROUP ALL",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns))
            .await?;

        let pools: Vec<PoolLimit> = result.take(0)?;
        let pool = pools
            .into_iter()
            .next()
            .with_context(|| format!("pool '{}' not configured", pool_key))?;
        let claimed: Option<u32> = result.take((1, "total"))?;
        Ok((pool, claimed.unwrap_or(0)))
    }

    /// Build a SurrealQL transaction that atomically checks capacity and claims slots.
    fn build_claim_transaction(pools: &[(String, u32)]) -> String {
        let mut q = String::from("BEGIN TRANSACTION;\n");

        for (i, (_, _slots)) in pools.iter().enumerate() {
            q += &format!(
                "LET $lim_{i} = (SELECT VALUE slot_limit \
                     FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $p{i})[0] ?? 0;\n\
                 LET $used_{i} = (SELECT VALUE math::sum(slots_consumed) \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $p{i} \
                     AND lease_expires_at > $now \
                     GROUP ALL)[0] ?? 0;\n"
            );
        }

        let conditions: Vec<String> = pools
            .iter()
            .enumerate()
            .map(|(i, (_, slots))| format!("($used_{i} + {slots}) <= $lim_{i}"))
            .collect();
        q += &format!("IF {} {{\n", conditions.join(" AND "));

        for i in 0..pools.len() {
            q += &format!(
                "  UPDATE concurrency_pools \
                     SET claim_version = claim_version + 1 \
                     WHERE code_location_id = $cl AND pool_key = $p{i};\n"
            );
        }

        for (i, (_, slots)) in pools.iter().enumerate() {
            q += &format!(
                "  CREATE concurrency_slots SET \
                     code_location_id = $cl, \
                     pool_key = $p{i}, run_id = $run_id, step_key = $step_key, \
                     slots_consumed = {slots}, claimed_at = $now, \
                     lease_expires_at = $lease_exp, last_heartbeat = $now;\n"
            );
        }

        q += "  DELETE FROM pending_steps \
                  WHERE run_id = $run_id AND step_key = $step_key;\n";
        q += "};\n";
        q += "COMMIT TRANSACTION;\n";
        q += "SELECT count() AS total FROM concurrency_slots \
                  WHERE run_id = $run_id AND step_key = $step_key GROUP ALL;\n";
        q
    }

    /// Statement index of the post-COMMIT SELECT in the claim transaction query.
    fn claim_check_statement_index(num_pools: usize) -> usize {
        2 * num_pools + 3
    }

    async fn kv_get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.kv_get(key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn kv_set_json<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.kv_set(key, &bytes).await
    }
}

/// Sentinel error type used by `claim_concurrency_slots` to encode the "snapshot saw the pool as full" race as a retryable failure.
#[derive(Debug)]
struct PoolContended;

impl std::fmt::Display for PoolContended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pool snapshot saw full capacity, retry needed")
    }
}

impl std::error::Error for PoolContended {}

/// Convert a unique-index violation on a client-supplied-ID `CREATE` into `Ok(())`, logging a warning.
fn swallow_phantom_commit(result: Result<()>, op: &'static str, id: &str) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if super::retry::is_unique_index_violation(&e) => {
            tracing::warn!(
                op = op,
                id = %id,
                error = %e,
                "CREATE returned unique-index violation after retry; treating \
                 as success — a previous retry attempt likely committed before \
                 the client saw a transient error (UUID collision across \
                 processes is statistically negligible)"
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

impl StorageBackend for SurrealStorage {
    #[tracing::instrument(skip_all, target = "rivers::storage", fields(cl = %event.code_location_id, asset_key = event.asset_key))]
    async fn store_event(&self, event: &EventRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
        let cl = event.code_location_id.as_str();
        let mut db_event = DbEventWrite::from(event);

        let mut materialization_code_version: Option<String> = None;
        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let cv = self.get_code_version(cl, asset_key).await?;
                db_event.code_version = cv.clone();
                db_event.input_data_versions = event.input_data_versions.clone();
                materialization_code_version = cv;
            }

        let result: Option<DbStoredEvent> = self
            .db
            .create("events")
            .content(db_event)
            .await
            .context("failed to store event")?;
        let stored = result.context("no event returned from create")?;
        let event_id = record_id_str(&stored.id);

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let code_version = materialization_code_version;
                let input_data_versions = event.input_data_versions.clone();

                let data_version = event.event_type.data_version().map(|s| s.to_string());
                self.db
                    .query("UPDATE assets SET last_event_id = $event_id, last_run_id = $run_id, last_timestamp = $timestamp, last_data_version = $data_version, last_materialization_code_version = $mcv, last_input_data_versions = $idv WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.clone()))
                    .bind(("event_id", event_id.clone()))
                    .bind(("run_id", event.run_id.clone()))
                    .bind(("timestamp", event.timestamp))
                    .bind(("data_version", data_version))
                    .bind(("mcv", code_version))
                    .bind(("idv", input_data_versions))
                    .await?;

                if let Some(partition_key) = &event.partition_key {
                    self.upsert_asset_partitions(vec![DbAssetPartitionWrite {
                        code_location_id: cl.to_string(),
                        asset_key: asset_key.clone(),
                        partition_key: partition_key.clone(),
                        last_event_id: event_id.clone(),
                        last_run_id: event.run_id.clone(),
                        last_timestamp: event.timestamp,
                    }])
                    .await?;
                }
            }

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_observation() {
                let data_version = event.event_type.data_version().map(|s| s.to_string());
                self.db
                    .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.clone()))
                    .bind(("event_id", event_id.clone()))
                    .bind(("timestamp", event.timestamp))
                    .bind(("data_version", data_version))
                    .await?;
            }

        Ok(event_id)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = events.len()))]
    async fn store_events(&self, events: &[EventRecord]) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let db_events: Vec<DbEventWrite> = events.iter().map(DbEventWrite::from).collect();
        let results: Vec<DbStoredEvent> = self
            .db
            .insert("events")
            .content(db_events)
            .await
            .context("failed to batch store events")?;

        let event_ids: Vec<String> = results.iter().map(|e| record_id_str(&e.id)).collect();

        // Group materializations by asset (latest wins), then one bulk upsert.
        let mut latest_mat: std::collections::HashMap<(&str, &str), usize> =
            std::collections::HashMap::new();
        let mut part_rows: std::collections::HashMap<
            (&str, &str, &PartitionKey),
            DbAssetPartitionWrite,
        > = std::collections::HashMap::new();

        for (idx, (event, event_id)) in events.iter().zip(event_ids.iter()).enumerate() {
            let Some(asset_key) = &event.asset_key else {
                continue;
            };
            if !event.event_type.is_materialization() {
                continue;
            }
            let cl = event.code_location_id.as_str();
            latest_mat.insert((cl, asset_key.as_str()), idx);
            if let Some(partition_key) = &event.partition_key {
                part_rows.insert(
                    (cl, asset_key.as_str(), partition_key),
                    DbAssetPartitionWrite {
                        code_location_id: cl.to_string(),
                        asset_key: asset_key.clone(),
                        partition_key: partition_key.clone(),
                        last_event_id: event_id.clone(),
                        last_run_id: event.run_id.clone(),
                        last_timestamp: event.timestamp,
                    },
                );
            }
        }

        // One `assets` row update per materialized asset (latest event wins).
        for (&(cl, asset_key), &idx) in &latest_mat {
            let event = &events[idx];
            let event_id = &event_ids[idx];
            let code_version = self.get_code_version(cl, asset_key).await?;
            let data_version = event.event_type.data_version().map(|s| s.to_string());
            self.db
                .query("UPDATE assets SET last_event_id = $event_id, last_run_id = $run_id, last_timestamp = $timestamp, last_data_version = $data_version, last_materialization_code_version = $mcv, last_input_data_versions = $idv WHERE code_location_id = $cl AND asset_key = $asset_key")
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("event_id", event_id.clone()))
                .bind(("run_id", event.run_id.clone()))
                .bind(("timestamp", event.timestamp))
                .bind(("data_version", data_version))
                .bind(("mcv", code_version))
                .bind(("idv", event.input_data_versions.clone()))
                .await?;
        }

        // Upsert the affected partition rows in one bulk statement.
        self.upsert_asset_partitions(part_rows.into_values().collect())
            .await?;

        for (event, event_id) in events.iter().zip(event_ids.iter()) {
            let cl = event.code_location_id.as_str();
            if let Some(asset_key) = &event.asset_key
                && event.event_type.is_observation() {
                    let data_version = event.event_type.data_version().map(|s| s.to_string());
                    self.db
                        .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                        .bind(("cl", cl.to_string()))
                        .bind(("asset_key", asset_key.clone()))
                        .bind(("event_id", event_id.clone()))
                        .bind(("timestamp", event.timestamp))
                        .bind(("data_version", data_version))
                        .await?;
                }
        }

        Ok(event_ids)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = logs.len()))]
    async fn store_run_logs(&self, logs: &[LogRecord]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let rows: Vec<DbRunLogWrite> = logs.iter().map(DbRunLogWrite::from).collect();
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query("INSERT INTO run_logs $rows RETURN NONE")
                .bind(("rows", rows.clone()))
                .await
                .context("failed to store run logs")?
                .check()
                .context("failed to store run logs")?;
            Ok(())
        })
        .await
    }

    async fn get_run_logs(&self, run_id: &str) -> Result<Vec<StoredLog>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM run_logs WHERE run_id = $id ORDER BY timestamp ASC, id ASC")
                .bind(("id", run_id.to_string()))
                .await?;
            let rows: Vec<DbStoredRunLog> = result.take(0)?;
            Ok(rows.into_iter().map(|l| l.into_stored_log()).collect())
        })
        .await
    }

    async fn get_events_for_run(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE run_id = $run_id ORDER BY timestamp ASC, sort_order ASC, id ASC")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn step_completion(
        &self,
        asset_key: &str,
        run_ids: &[String],
    ) -> Result<(bool, Vec<String>)> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut completed = false;
            let mut succeeded: Vec<String> = Vec::new();
            for run_id in run_ids {
                let events = self.get_events_for_run(run_id).await?;
                for e in &events {
                    if e.asset_key.as_deref() != Some(asset_key) {
                        continue;
                    }
                    match e.event_type {
                        EventType::StepSuccess => {
                            completed = true;
                            succeeded.push(run_id.clone());
                            break;
                        }
                        EventType::StepFailure => completed = true,
                        _ => {}
                    }
                }
            }
            Ok((completed, succeeded))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(run_id = %run.run_id))]
    async fn create_run(&self, run: &RunRecord) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            let _: Option<RunRecord> = self
                .db
                .create("runs")
                .content(run.clone())
                .await
                .context("failed to create run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_run", &run.run_id)
    }

    async fn create_runs(&self, runs: &[RunRecord]) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            if runs.is_empty() {
                return Ok(());
            }
            let mut q = String::new();
            for (i, _run) in runs.iter().enumerate() {
                q += &format!("CREATE runs CONTENT $r{i};\n");
            }
            let mut query = self.db.query(&q);
            for (i, run) in runs.iter().enumerate() {
                query = query.bind((format!("r{i}"), run.clone()));
            }
            query.await.context("failed to create runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_runs", &format!("batch[{}]", runs.len()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%run_id, ?status))]
    async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query(
                        "UPDATE runs SET status = $status, end_time = $end_time WHERE run_id = $run_id",
                    )
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE runs SET status = $status WHERE run_id = $run_id")
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_run_block_reason(&self, run_id: &str, reason: Option<&str>) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query("UPDATE runs SET block_reason = $reason WHERE run_id = $run_id")
                .bind(("run_id", run_id.to_string()))
                .bind(("reason", reason.map(|s| s.to_string())))
                .await?;
            Ok(())
        })
        .await
    }

    async fn try_start_run(&self, run_id: &str) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE runs SET status = 'Started' \
                         WHERE run_id = $run_id AND status != 'Canceled'; \
                     SELECT status FROM runs WHERE run_id = $run_id LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;
            let status: Option<String> = result.take((1, "status"))?;
            match status.as_deref() {
                Some("Started") => Ok(true),
                Some(_) => Ok(false),
                None => anyhow::bail!("run {run_id} not found"),
            }
        })
        .await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE run_id = $run_id LIMIT 1")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.into_iter().next())
        })
        .await
    }

    async fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            if run_ids.is_empty() {
                return Ok(Vec::new());
            }
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids AND status = $status ORDER BY start_time ASC, run_id ASC")
                    .bind(("ids", run_ids.to_vec()))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids ORDER BY start_time ASC, run_id ASC")
                    .bind(("ids", run_ids.to_vec()))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE status = $status ORDER BY start_time DESC LIMIT $limit")
                    .bind(("status", status_str))
                    .bind(("limit", limit))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs ORDER BY start_time DESC LIMIT $limit")
                    .bind(("limit", limit))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since AND status = $status ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_queued_runs(&self) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['Queued', 'NotStarted']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn count_in_progress_runs(&self) -> Result<usize> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.len())
        })
        .await
    }

    async fn get_in_progress_runs(&self) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_observations_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
    ) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE event_type = $etype \
                     AND (code_location_id = $cl OR code_location_id = NONE) \
                     AND timestamp > $since ORDER BY timestamp DESC",
                )
                .bind(("etype", "Observation".to_string()))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_latest_observation_ts(&self, code_location_id: &str) -> Result<Option<i64>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT timestamp FROM events WHERE event_type = $etype \
                     AND (code_location_id = $cl OR code_location_id = NONE) \
                     ORDER BY timestamp DESC LIMIT 1",
                )
                .bind(("etype", "Observation".to_string()))
                .bind(("cl", code_location_id.to_string()))
                .await?;

            #[derive(Debug, SurrealValue)]
            struct TsRow {
                timestamp: i64,
            }
            let rows: Vec<TsRow> = result.take(0)?;
            Ok(rows.into_iter().next().map(|r| r.timestamp))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM kv WHERE key = $key LIMIT 1")
                .bind(("key", key.to_string()))
                .await?;
            let kvs: Vec<DbKv> = result.take(0)?;
            Ok(kvs.into_iter().next().map(|kv| kv.value.to_vec()))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        // Single atomic upsert on the UNIQUE `kv.key` index — a crash must
        // never leave the key deleted (the old DELETE+CREATE pair could).
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "INSERT INTO kv { key: $key, value: $value } \
                     ON DUPLICATE KEY UPDATE value = $input.value",
                )
                .bind(("key", key.to_string()))
                .bind(("value", Bytes::from(value.to_vec())))
                .await?
                .check()?;
            Ok(())
        })
        .await
    }

    async fn store_tick(&self, tick: &TickRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
            let db_tick = DbTickWrite::from(tick);
            let result: Option<DbStoredTick> = self
                .db
                .create("ticks")
                .content(db_tick)
                .await
                .context("failed to store tick")?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = ticks.len()))]
    async fn store_ticks_batch(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            if ticks.is_empty() {
                return Ok(vec![]);
            }
            let db_ticks: Vec<DbTickWrite> = ticks.iter().map(DbTickWrite::from).collect();
            let results: Vec<DbStoredTick> = self
                .db
                .insert("ticks")
                .content(db_ticks)
                .await
                .context("failed to batch store ticks")?;
            Ok(results.iter().map(|t| record_id_str(&t.id)).collect())
        })
        .await
    }

    async fn store_condition_tick(&self, tick: &ConditionTickRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
            let db_tick = DbConditionTickWrite::from(tick);
            let result: Option<DbStoredConditionTick> =
                self.db.create("condition_ticks").content(db_tick).await?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = evals.len()))]
    async fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            if evals.is_empty() {
                return Ok(vec![]);
            }
            let db_evals: Vec<DbConditionEvalWrite> =
                evals.iter().map(DbConditionEvalWrite::from).collect();
            let results: Vec<DbStoredConditionEval> =
                self.db.insert("condition_evals").content(db_evals).await?;
            Ok(results.iter().map(|e| record_id_str(&e.id)).collect())
        })
        .await
    }

    // ── Backfills ──

    async fn create_backfill(&self, backfill: &BackfillRecord) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            let _: Option<BackfillRecord> = self
                .db
                .create("backfills")
                .content(backfill.clone())
                .await
                .context("failed to create backfill")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_backfill", &backfill.backfill_id)
    }

    async fn update_backfill_status(
        &self,
        backfill_id: &str,
        status: BackfillStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query("UPDATE backfills SET status = $status, end_time = $end_time WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE backfills SET status = $status WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_backfill_progress(
        &self,
        backfill_id: &str,
        run_ids: &[String],
        completed: &[PartitionKey],
        failed: &[PartitionKey],
        canceled: &[PartitionKey],
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     run_ids = array::union(run_ids, $run_ids), \
                     completed_partitions = array::union(completed_partitions, $completed), \
                     failed_partitions = array::union(failed_partitions, $failed), \
                     canceled_partitions = array::union(canceled_partitions, $canceled) \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("run_ids", run_ids.to_vec()))
                .bind(("completed", completed.to_vec()))
                .bind(("failed", failed.to_vec()))
                .bind(("canceled", canceled.to_vec()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM backfills WHERE backfill_id = $id LIMIT 1")
                .bind(("id", backfill_id.to_string()))
                .await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            Ok(rows.into_iter().next())
        })
        .await
    }

    async fn try_complete_backfill(
        &self,
        backfill_id: &str,
        extra_canceled: &[PartitionKey],
    ) -> Result<Option<BackfillStatus>> {
        let backfill = self
            .get_backfill(backfill_id)
            .await?
            .context("backfill not found")?;

        if backfill.status != BackfillStatus::InProgress {
            return Ok(None);
        }

        let runs = if backfill.run_ids.is_empty() {
            Vec::new()
        } else {
            self.get_runs_by_ids(&backfill.run_ids, None).await?
        };
        let all_terminal = runs.iter().all(|r| {
            matches!(
                r.status,
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
            )
        });
        if !all_terminal {
            return Ok(None);
        }
        // Nothing to finalize: no terminal runs and no externally-canceled keys.
        if runs.is_empty() && extra_canceled.is_empty() {
            return Ok(None);
        }

        let mut completed_pks: Vec<PartitionKey> = Vec::new();
        let mut failed_pks: Vec<PartitionKey> = Vec::new();
        let mut canceled_pks: Vec<PartitionKey> = Vec::new();
        let mut any_failed = false;
        let mut any_canceled = false;

        #[derive(SurrealValue)]
        struct FailRow {
            run_id: String,
            partition_key: PartitionKey,
        }
        let success_run_ids: Vec<String> = runs
            .iter()
            .filter(|r| matches!(r.status, RunStatus::Success))
            .map(|r| r.run_id.clone())
            .collect();
        let mut failed_by_run: std::collections::HashMap<
            String,
            std::collections::HashSet<PartitionKey>,
        > = std::collections::HashMap::new();
        if !success_run_ids.is_empty() {
            let rows: Vec<FailRow> = super::retry::with_retry(&self.retry_config, || async {
                let mut res = self
                    .db
                    .query(
                        "SELECT run_id, partition_key FROM events \
                         WHERE run_id IN $rids AND event_type = 'StepFailure' \
                         AND partition_key IS NOT NONE GROUP BY run_id, partition_key",
                    )
                    .bind(("rids", success_run_ids.clone()))
                    .await?;
                Ok(res.take(0)?)
            })
            .await?;
            for row in rows {
                failed_by_run
                    .entry(row.run_id)
                    .or_default()
                    .insert(row.partition_key);
            }
        }

        for run in &runs {
            let Some(ref pk) = run.partition_key else {
                match run.status {
                    RunStatus::Failure => any_failed = true,
                    RunStatus::Canceled => any_canceled = true,
                    _ => {}
                }
                continue;
            };
            match run.status {
                RunStatus::Success => {
                    let failed_members = failed_by_run.get(&run.run_id);
                    for member in pk.members() {
                        if failed_members.is_some_and(|f| f.contains(&member)) {
                            failed_pks.push(member);
                            any_failed = true;
                        } else {
                            completed_pks.push(member);
                        }
                    }
                }
                RunStatus::Failure => {
                    failed_pks.extend(pk.members());
                    any_failed = true;
                }
                RunStatus::Canceled => {
                    canceled_pks.extend(pk.members());
                    any_canceled = true;
                }
                _ => {}
            }
        }

        if !extra_canceled.is_empty() {
            canceled_pks.extend(extra_canceled.iter().cloned());
            any_canceled = true;
        }

        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     completed_partitions = $completed, \
                     failed_partitions = $failed, \
                     canceled_partitions = $canceled \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("completed", completed_pks.clone()))
                .bind(("failed", failed_pks.clone()))
                .bind(("canceled", canceled_pks.clone()))
                .await?;
            Ok(())
        })
        .await?;

        let new_status = if any_failed {
            BackfillStatus::CompletedFailed
        } else if any_canceled {
            BackfillStatus::Canceled
        } else {
            BackfillStatus::CompletedSuccess
        };

        let now = now_nanos();
        self.update_backfill_status(backfill_id, new_status.clone(), Some(now))
            .await?;
        Ok(Some(new_status))
    }

    async fn cancel_backfill(&self, backfill_id: &str) -> Result<BackfillStatus> {
        // Settle first: if every run already finished, the cancel prevented
        // nothing and the derived outcome wins.
        if let Some(settled) = self.try_complete_backfill(backfill_id, &[]).await? {
            return Ok(settled);
        }
        let now = now_nanos();
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'Canceled', end_time = $now \
                     WHERE backfill_id = $id AND status IN ['Requested', 'InProgress']",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("now", now))
                .await?
                .check()?;
            Ok(())
        })
        .await?;
        Ok(self
            .get_backfill(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?
            .status)
    }

    // Concurrency pools

    async fn free_concurrency_slots(&self, run_id: &str, step_key: &str) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     DELETE FROM pending_steps \
                     WHERE run_id = $run_id AND step_key = $step_key",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn free_concurrency_slots_for_run(&self, run_id: &str) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots WHERE run_id = $run_id; \
                     DELETE FROM pending_steps WHERE run_id = $run_id",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> Result<u32> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

            let mut result = self
                .db
                .query(
                    "UPDATE concurrency_slots \
                         SET lease_expires_at = $lease_exp, last_heartbeat = $now \
                         WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM concurrency_slots \
                         WHERE run_id = $run_id AND step_key = $step_key GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("now", now_ns))
                .bind(("lease_exp", lease_exp))
                .await?;
            let renewed: Option<u32> = result.take((1, "total"))?;
            Ok(renewed.unwrap_or(0))
        })
        .await
    }

    async fn free_expired_leases(&self) -> Result<u32> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "SELECT count() AS total FROM concurrency_slots \
                         WHERE lease_expires_at <= $now GROUP ALL; \
                     DELETE FROM concurrency_slots WHERE lease_expires_at <= $now",
                )
                .bind(("now", now_ns))
                .await?;
            let expired: Option<u32> = result.take((0, "total"))?;
            Ok(expired.unwrap_or(0))
        })
        .await
    }

    async fn cancel_queued_run(&self, run_id: &str) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "UPDATE runs SET status = $new_status, end_time = $now \
                         WHERE run_id = $run_id AND status IN ['Queued', 'NotStarted']; \
                     DELETE FROM pending_steps WHERE run_id = $run_id; \
                     SELECT count() AS total FROM runs \
                         WHERE run_id = $run_id AND status = $new_status GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("new_status", RunStatus::Canceled))
                .bind(("now", now_ns))
                .await?;
            let count: Option<u32> = result.take((2, "total"))?;
            Ok(count.unwrap_or(0) > 0)
        })
        .await
    }

    async fn delete_run(&self, run_id: &str) -> Result<bool> {
        // Check-then-delete is race-free here: terminal statuses are
        // permanent (re-execution mints a new run_id), so a run observed
        // terminal can't be picked up by the coordinator afterwards. The
        // runs row goes last so a partial failure stays re-deletable.
        let run = match self.get_run(run_id).await? {
            None => return Ok(false),
            Some(r) => r,
        };
        if !matches!(
            run.status,
            RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
        ) {
            anyhow::bail!(
                "run '{run_id}' is {:?} — cancel it and let it finish before deleting",
                run.status
            );
        }
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM events WHERE run_id = $run_id; \
                     DELETE FROM run_logs WHERE run_id = $run_id; \
                     DELETE FROM concurrency_slots WHERE run_id = $run_id; \
                     DELETE FROM pending_steps WHERE run_id = $run_id; \
                     DELETE FROM kv WHERE key = $cancel_key; \
                     DELETE FROM runs WHERE run_id = $run_id",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("cancel_key", format!("cancel:{run_id}")))
                .await?
                .check()?;
            Ok(())
        })
        .await?;
        Ok(true)
    }

    async fn get_run_progress(&self, run_id: &str) -> Result<RunProgress> {
        super::retry::with_retry(&self.retry_config, || async {
            // Distinct steps, not raw events — a retried step re-emits
            // StepStart/StepFailure per attempt and must count once. The dedup
            // stays in-query so only scalars cross the DB hop (the operator
            // polls this every reconcile pass).
            let mut result = self
                .db
                .query(
                    "SELECT count() AS n FROM \
                         (SELECT asset_key FROM events \
                          WHERE run_id = $run_id AND event_type = 'StepStart' \
                          AND asset_key IS NOT NONE \
                          GROUP BY asset_key) \
                         GROUP ALL; \
                     SELECT count() AS n FROM \
                         (SELECT asset_key FROM events \
                          WHERE run_id = $run_id \
                          AND (event_type = 'StepSuccess' \
                               OR (event_type = 'StepFailure' AND partition_key IS NONE)) \
                          AND asset_key IS NOT NONE \
                          GROUP BY asset_key) \
                         GROUP ALL; \
                     SELECT asset_key, timestamp FROM events \
                         WHERE run_id = $run_id \
                         AND (event_type = 'StepSuccess' OR (event_type = 'StepFailure' AND partition_key IS NONE)) \
                         ORDER BY timestamp DESC LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            let started: Option<u32> = result.take((0, "n"))?;
            let terminal: Option<u32> = result.take((1, "n"))?;

            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct LastStep {
                asset_key: Option<String>,
                timestamp: i64,
            }
            let last_steps: Vec<LastStep> = result.take(2)?;
            let last = last_steps.into_iter().next();

            Ok(RunProgress {
                completed_steps: terminal.unwrap_or(0),
                total_steps: started.unwrap_or(0),
                last_step_completed_at: last.as_ref().map(|s| s.timestamp),
                last_completed_step: last.and_then(|s| s.asset_key),
            })
        })
        .await
    }

    async fn get_run_outcome(&self, run_id: &str) -> Result<Option<RunOutcome>> {
        let key = format!("run_outcome:{run_id}");
        let data = self.kv_get(&key).await?;
        match data {
            Some(bytes) => {
                let outcome: RunOutcome = serde_json::from_slice(&bytes)?;
                Ok(Some(outcome))
            }
            None => Ok(None),
        }
    }

    async fn set_run_outcome(&self, run_id: &str, outcome: &RunOutcome) -> Result<()> {
        let key = format!("run_outcome:{run_id}");
        let bytes = serde_json::to_vec(outcome)?;
        self.kv_set(&key, &bytes).await
    }

    async fn request_cancellation(&self, run_id: &str) -> Result<()> {
        let key = format!("cancel:{run_id}");
        self.kv_set(&key, b"1").await
    }

    async fn is_cancelled(&self, run_id: &str) -> Result<bool> {
        let key = format!("cancel:{run_id}");
        Ok(self.kv_get(&key).await?.is_some())
    }

    async fn get_events_for_step(&self, run_id: &str, step_key: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                         WHERE run_id = $run_id AND asset_key = $step_key \
                         ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_step_terminal_events(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                         WHERE run_id = $run_id AND asset_key = $step_key \
                         AND (event_type = 'StepSuccess' OR event_type = 'StepFailure') \
                         ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_completed_step_keys(&self, run_id: &str) -> Result<HashSet<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepSuccess'",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows.into_iter().filter_map(|r| r.asset_key).collect())
        })
        .await
    }

    async fn get_step_data_versions(&self, run_id: &str) -> Result<HashMap<String, String>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key, data_version FROM events \
                         WHERE run_id = $run_id AND event_type = 'Materialization' \
                           AND data_version IS NOT NULL",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
                data_version: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows
                .into_iter()
                .filter_map(|r| Some((r.asset_key?, r.data_version?)))
                .collect())
        })
        .await
    }
}

impl PerCodeLocationStorage for SurrealStorage {
    async fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let mut result = self
            .db
            .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let event_type_str = "Materialization".to_string();
        let mut result = if let Some(partition_key) = partition {
            for cand in PartitionKey::display_candidates(partition_key)
                .into_iter()
                .rev()
            {
                let mut result = self.db
                    .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                    .bind(("cl", code_location_id.to_string()))
                    .bind(("asset_key", asset_key.to_string()))
                    .bind(("pk", cand))
                    .bind(("event_type", event_type_str.clone()))
                    .await?;
                let events: Vec<DbStoredEvent> = result.take(0)?;
                if let Some(e) = events.into_iter().next() {
                    return Ok(Some(e.into_stored_event()));
                }
            }
            return Ok(None);
        } else {
            self.db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("event_type", event_type_str))
                .await?
        };
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().next().map(|e| e.into_stored_event()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets.into_iter().next())
    }

    async fn get_asset_records(&self, code_location_id: &str) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> Result<Vec<AssetRecord>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let keys_vec: Vec<String> = keys.to_vec();
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key IN $keys")
            .bind(("cl", code_location_id.to_string()))
            .bind(("keys", keys_vec))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, count = records.len()))]
    async fn register_assets(&self, code_location_id: &str, records: &[AssetRecord]) -> Result<()> {
        for record in records {
            let mut existing = self
                .db
                .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", record.asset_key.clone()))
                .await?;
            let found: Vec<AssetRecord> = existing.take(0)?;

            if found.is_empty() {
                let mut to_insert = record.clone();
                to_insert.code_location_id = code_location_id.to_string();
                let _: Option<AssetRecord> = self.db.create("assets").content(to_insert).await?;
            } else {
                self.db
                    .query("UPDATE assets SET tags = $tags, kinds = $kinds, asset_group = $asset_group, code_version = $code_version, pool = $pool WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", code_location_id.to_string()))
                    .bind(("asset_key", record.asset_key.clone()))
                    .bind(("tags", record.tags.clone()))
                    .bind(("kinds", record.kinds.clone()))
                    .bind(("asset_group", record.asset_group.clone()))
                    .bind(("code_version", record.code_version.clone()))
                    .bind(("pool", record.pool.clone()))
                    .await?;
            }
        }

        Ok(())
    }

    async fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND tags CONTAINS $tag")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tag", tag.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND kinds CONTAINS $kind")
            .bind(("cl", code_location_id.to_string()))
            .bind(("kind", kind.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_group = $group")
            .bind(("cl", code_location_id.to_string()))
            .bind(("group", group.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.db
            .query(
                "UPDATE runs SET block_reason = $reason \
                 WHERE status = $status AND code_location_id = $cl",
            )
            .bind(("status", format!("{:?}", status)))
            .bind(("reason", reason.map(|s| s.to_string())))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        Ok(())
    }

    async fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM concurrency_slots \
                     WHERE lease_expires_at <= $now GROUP ALL; \
                 DELETE FROM concurrency_slots WHERE lease_expires_at <= $now; \
                 SELECT run_id, code_location_id, tags, node_names, job_name, priority, partition_key, start_time \
                     FROM runs WHERE status IN ['NotStarted', 'Started'] AND code_location_id = $cl; \
                 SELECT run_id, code_location_id, tags, node_names, job_name, priority, partition_key, start_time \
                     FROM runs WHERE status = 'Queued' AND code_location_id = $cl",
            )
            .bind(("now", now_ns))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let expired: Option<u32> = result.take((0, "total"))?;
        // Statement 1 is the DELETE (no result needed)
        let in_progress: Vec<CoordinatorRunInfo> = result.take(2)?;
        let queued: Vec<CoordinatorRunInfo> = result.take(3)?;
        Ok((expired.unwrap_or(0), in_progress, queued))
    }

    async fn get_stalled_not_started_runs(
        &self,
        code_location_id: &str,
        cutoff_ns: i64,
    ) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct Candidate {
                run_id: String,
                start_time: i64,
            }
            let mut result = self
                .db
                .query(
                    "SELECT run_id, start_time FROM runs \
                         WHERE code_location_id = $cl AND status = 'NotStarted'",
                )
                .bind(("cl", code_location_id.to_string()))
                .await?;
            let candidates: Vec<Candidate> = result.take(0)?;
            if candidates.is_empty() {
                return Ok(vec![]);
            }

            let ids: Vec<String> = candidates.iter().map(|c| c.run_id.clone()).collect();
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct Dequeue {
                run_id: String,
                ts: i64,
            }
            let mut result = self
                .db
                .query(
                    "SELECT run_id, math::max(timestamp) AS ts FROM events \
                         WHERE event_type = 'RunDequeued' AND run_id IN $ids \
                         GROUP BY run_id",
                )
                .bind(("ids", ids))
                .await?;
            let dequeues: Vec<Dequeue> = result.take(0)?;
            let dequeue_ts: std::collections::HashMap<String, i64> =
                dequeues.into_iter().map(|d| (d.run_id, d.ts)).collect();

            Ok(candidates
                .into_iter()
                .filter(|c| *dequeue_ts.get(&c.run_id).unwrap_or(&c.start_time) < cutoff_ns)
                .map(|c| c.run_id)
                .collect())
        })
        .await
    }

    async fn add_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> Result<()> {
        for key in partition_keys {
            if key.is_empty() {
                anyhow::bail!("dynamic partition keys must not be empty");
            }
            if let Some(ch) = PartitionKey::reserved_display_char(key) {
                anyhow::bail!(
                    "partition key '{key}' contains reserved character '{ch}' \
                     (used by the canonical display form)"
                );
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for key in partition_keys {
            let mut result = self
                .db
                .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .bind(("key", key.clone()))
                .await?;
            let existing: Vec<DbDynamicPartition> = result.take(0)?;
            if existing.is_empty() {
                let _: Option<DbDynamicPartition> = self
                    .db
                    .create("dynamic_partitions")
                    .content(DbDynamicPartition {
                        code_location_id: code_location_id.to_string(),
                        partitions_def_name: partitions_def_name.to_string(),
                        partition_key: key.clone(),
                        create_timestamp: now,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn delete_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.db
            .query("DELETE FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        Ok(())
    }

    async fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<Vec<String>> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name ORDER BY partition_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(!rows.is_empty())
    }

    async fn get_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        limit: usize,
    ) -> Result<Vec<StoredTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored_tick()).collect())
    }

    async fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM ticks WHERE code_location_id = $cl AND automation_name = $name AND id NOT IN $keep.id RETURN BEFORE;"
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let deleted: Vec<DbStoredTick> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_condition_ticks(
        &self,
        code_location_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredConditionTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored()).collect())
    }

    async fn prune_condition_ticks(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM condition_ticks WHERE code_location_id = $cl AND id NOT IN $keep.id RETURN BEFORE;",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let deleted: Vec<DbStoredConditionTick> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND tick_id = $tick_id ORDER BY asset_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tick_id", tick_id.to_string()))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn prune_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        max_evals: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key AND id NOT IN $keep.id RETURN BEFORE;"
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .bind(("max", max_evals))
            .await?;
        let deleted: Vec<DbStoredConditionEval> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        for cand in PartitionKey::display_candidates(partition_key)
            .into_iter()
            .rev()
        {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk ORDER BY timestamp DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("pk", cand))
                .bind(("limit", limit))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            if !events.is_empty() {
                return Ok(events.into_iter().map(|e| e.into_stored_event()).collect());
            }
        }
        Ok(Vec::new())
    }

    async fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query("SELECT partition_key FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<u64> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<u64> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let mut result = self
            .db
            .query("SELECT partition_key, last_timestamp FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key AND last_timestamp IS NOT NULL")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
        }

        let rows: Vec<PartTsRow> = result.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp))
            .collect())
    }

    async fn get_partition_timestamps_since(
        &self,
        code_location_id: &str,
        asset_key: &str,
        since_timestamp: i64,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let mut result = self
            .db
            .query(
                "SELECT partition_key, last_timestamp FROM asset_partitions \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND last_timestamp > $since",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("since", since_timestamp))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
        }

        let rows: Vec<PartTsRow> = result.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp))
            .collect())
    }

    async fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query(
                "SELECT partition_key FROM events WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepStart' AND partition_key IS NOT NONE \
                 AND run_id IN (SELECT VALUE run_id FROM runs WHERE code_location_id = $cl AND status = 'Started' \
                 AND $asset_key IN node_names) \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &std::collections::HashMap<PartitionKey, i64>,
    ) -> Result<std::collections::HashMap<PartitionKey, i64>> {
        let mut result = self
            .db
            .query(
                "SELECT partition_key, math::max(timestamp) AS ts FROM events \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepFailure' AND partition_key IS NOT NONE \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct FailRow {
            partition_key: PartitionKey,
            ts: i64,
        }

        let failed_rows: Vec<FailRow> = result.take(0)?;
        let mut latest_failure: std::collections::HashMap<PartitionKey, i64> =
            std::collections::HashMap::new();
        for row in failed_rows {
            for member in row.partition_key.members() {
                latest_failure
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(row.ts))
                    .or_insert(row.ts);
            }
        }

        let mut result = self
            .db
            .query(
                "SELECT partition_key, start_time FROM runs \
                 WHERE code_location_id = $cl AND status = 'Failure' \
                 AND $asset_key IN node_names AND partition_key IS NOT NONE",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct RunFailRow {
            partition_key: PartitionKey,
            start_time: i64,
        }

        let run_rows: Vec<RunFailRow> = result.take(0)?;
        for row in run_rows {
            for member in row.partition_key.members() {
                latest_failure
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(row.start_time))
                    .or_insert(row.start_time);
            }
        }

        Ok(latest_failure
            .into_iter()
            .filter(|(pk, ts)| materialized.get(pk).is_none_or(|&mat_ts| mat_ts < *ts))
            .collect())
    }

    async fn get_backfills(
        &self,
        code_location_id: &str,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        let mut query = "SELECT * FROM backfills WHERE code_location_id = $cl".to_string();
        if status.is_some() {
            query.push_str(" AND status = $status");
        }
        query.push_str(" ORDER BY create_time DESC");
        if limit.is_some() {
            query.push_str(" LIMIT $limit");
        }
        let mut q = self
            .db
            .query(&query)
            .bind(("cl", code_location_id.to_string()));
        if let Some(s) = status {
            q = q.bind(("status", format!("{:?}", s)));
        }
        if let Some(lim) = limit {
            q = q.bind(("limit", lim));
        }
        let mut result = q.await?;
        let rows: Vec<BackfillRecord> = result.take(0)?;
        Ok(rows)
    }

    async fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        self.db
            .query(
                "UPSERT concurrency_pools SET \
                     code_location_id = $cl, \
                     pool_key = $pool_key, \
                     slot_limit = $slot_limit, \
                     lease_duration_secs = $lease_duration_secs \
                 WHERE code_location_id = $cl AND pool_key = $pool_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("slot_limit", limit))
            .bind(("lease_duration_secs", lease_duration_secs))
            .await?;
        Ok(())
    }

    async fn get_pool_limits(&self, code_location_id: &str) -> Result<Vec<PoolLimit>> {
        let mut result = self
            .db
            .query("SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let pools: Vec<PoolLimit> = result.take(0)?;
        Ok(pools)
    }

    async fn get_pool_info(&self, code_location_id: &str, pool_key: &str) -> Result<PoolInfo> {
        let now_ns = now_nanos();
        let (pool, claimed_count) = self
            .query_pool_usage(code_location_id, pool_key, now_ns)
            .await?;

        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM pending_steps \
                     WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .await?;
        let pending_count: Option<u32> = result.take((0, "total"))?;

        Ok(PoolInfo {
            pool_key: pool.pool_key,
            slot_limit: pool.slot_limit,
            lease_duration_secs: pool.lease_duration_secs,
            claimed_count,
            pending_count: pending_count.unwrap_or(0),
        })
    }

    async fn get_all_pool_infos(&self, code_location_id: &str) -> Result<Vec<PoolInfo>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC; \
                 SELECT pool_key, math::sum(slots_consumed) AS claimed \
                     FROM concurrency_slots WHERE code_location_id = $cl AND lease_expires_at > $now \
                     GROUP BY pool_key; \
                 SELECT pool_key, count() AS pending \
                     FROM pending_steps WHERE code_location_id = $cl GROUP BY pool_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("now", now_ns))
            .await?;

        let pools: Vec<PoolLimit> = result.take(0)?;

        #[derive(SurrealValue, serde::Deserialize)]
        struct ClaimedRow {
            pool_key: String,
            claimed: u32,
        }
        let claimed_rows: Vec<ClaimedRow> = result.take(1)?;
        let claimed_map: std::collections::HashMap<String, u32> = claimed_rows
            .into_iter()
            .map(|r| (r.pool_key, r.claimed))
            .collect();

        #[derive(SurrealValue, serde::Deserialize)]
        struct PendingRow {
            pool_key: String,
            pending: u32,
        }
        let pending_rows: Vec<PendingRow> = result.take(2)?;
        let pending_map: std::collections::HashMap<String, u32> = pending_rows
            .into_iter()
            .map(|r| (r.pool_key, r.pending))
            .collect();

        Ok(pools
            .into_iter()
            .map(|p| PoolInfo {
                claimed_count: claimed_map.get(&p.pool_key).copied().unwrap_or(0),
                pending_count: pending_map.get(&p.pool_key).copied().unwrap_or(0),
                pool_key: p.pool_key,
                slot_limit: p.slot_limit,
                lease_duration_secs: p.lease_duration_secs,
            })
            .collect())
    }

    async fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        anyhow::ensure!(!pools.is_empty(), "pools must not be empty");

        let predicate =
            |e: &anyhow::Error| super::retry::default_should_retry(e) || e.is::<PoolContended>();

        let result = super::retry::with_retry_if(&self.retry_config, predicate, || async {
            self.try_claim_concurrency_slots_once(
                code_location_id,
                pools,
                run_id,
                step_key,
                priority,
                lease_duration_secs,
            )
            .await
        })
        .await;

        match result {
            Ok(status) => Ok(status),
            Err(e) if e.is::<PoolContended>() => {
                anyhow::bail!("failed to claim concurrency slots — extreme contention on pool")
            }
            Err(e) => Err(e),
        }
    }

    async fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<Vec<SlotHolder>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT run_id, step_key, slots_consumed, claimed_at, lease_expires_at \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now \
                     ORDER BY claimed_at ASC",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns))
            .await?;
        let holders: Vec<SlotHolder> = result.take(0)?;
        Ok(holders)
    }

    async fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = $status ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("status", status_str))
                .bind(("limit", limit))
                .await?
        } else {
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("limit", limit))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_queued_runs(&self, code_location_id: &str) -> Result<Vec<RunRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = 'Queued'")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: super::SortOrder,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since AND status = $status ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .bind(("status", status_str))
                .await?
        } else {
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        // Retry: the caller resets to fresh state on error, discarding all latches.
        let key = crate::condition_eval_state_key(code_location_id);
        super::retry::with_retry(&self.retry_config, || async {
            self.kv_get_json(&key).await
        })
        .await
    }

    async fn set_condition_eval_state(
        &self,
        code_location_id: &str,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        self.kv_set_json(&crate::condition_eval_state_key(code_location_id), state)
            .await
    }

    async fn get_condition_pending_dispatch(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::PendingDispatch>> {
        self.kv_get_json(&crate::condition_pending_dispatch_key(code_location_id))
            .await
    }

    async fn set_condition_pending_dispatch(
        &self,
        code_location_id: &str,
        pending: &crate::condition::PendingDispatch,
    ) -> Result<()> {
        self.kv_set_json(
            &crate::condition_pending_dispatch_key(code_location_id),
            pending,
        )
        .await
    }

    async fn get_graph_topology(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::assets::graph::GraphTopology>> {
        self.kv_get_json(&crate::graph_topology_key(code_location_id))
            .await
    }

    async fn set_graph_topology(
        &self,
        code_location_id: &str,
        topology: &crate::assets::graph::GraphTopology,
    ) -> Result<()> {
        self.kv_set_json(&crate::graph_topology_key(code_location_id), topology)
            .await
    }
}

impl SurrealStorage {
    /// One attempt of the [`PerCodeLocationStorage::claim_concurrency_slots`] flow.
    async fn try_claim_concurrency_slots_once(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        let now_ns = now_nanos();
        let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

        let mut blocked = Vec::new();
        let mut limited_pools: Vec<(String, u32)> = Vec::new();
        for (pool_key, slots_needed) in pools {
            let (pool, current_used) = self
                .query_pool_usage(code_location_id, pool_key, now_ns)
                .await?;
            if pool.slot_limit < 0 {
                continue;
            }
            limited_pools.push((pool_key.clone(), *slots_needed));
            if current_used + *slots_needed > pool.slot_limit as u32 {
                blocked.push(PoolBlockDetail {
                    pool_key: pool_key.clone(),
                    claimed: current_used,
                    limit: pool.slot_limit,
                });
            }
        }

        if limited_pools.is_empty() {
            return Ok(ConcurrencyClaimStatus::Claimed);
        }

        if !blocked.is_empty() {
            let first_pool = blocked[0].pool_key.clone();
            let reason = if blocked.len() == 1 {
                let b = &blocked[0];
                BlockReason::PoolFull {
                    pool_key: b.pool_key.clone(),
                    claimed: b.claimed,
                    limit: b.limit,
                }
            } else {
                BlockReason::PoolsFull { pools: blocked }
            };
            let reason_str = reason.to_string();

            let mut result = self
                .db
                .query(
                    "UPSERT pending_steps SET \
                         code_location_id = $cl, \
                         pool_key = $pool_key, \
                         run_id = $run_id, \
                         step_key = $step_key, \
                         priority = $priority, \
                         enqueued_at = $now, \
                         block_reason = $reason \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM pending_steps \
                         WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("pool_key", first_pool))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("priority", priority))
                .bind(("now", now_ns))
                .bind(("reason", reason_str))
                .await?;
            let position: Option<u32> = result.take((1, "total"))?;

            return Ok(ConcurrencyClaimStatus::Pending {
                position: position.unwrap_or(1),
                reason,
            });
        }

        let txn_query = Self::build_claim_transaction(&limited_pools);
        let mut q = self.db.query(&txn_query);
        for (i, (pool_key, _)) in limited_pools.iter().enumerate() {
            q = q.bind((format!("p{i}"), pool_key.clone()));
        }
        q = q
            .bind(("cl", code_location_id.to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("step_key", step_key.to_string()))
            .bind(("now", now_ns))
            .bind(("lease_exp", lease_exp));

        let mut response = q.await?.check()?;

        let check_idx = Self::claim_check_statement_index(pools.len());
        let count: Option<u32> = response.take((check_idx, "total"))?;

        if count.unwrap_or(0) > 0 {
            Ok(ConcurrencyClaimStatus::Claimed)
        } else {
            Err(anyhow::Error::new(PoolContended))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BackfillFailurePolicy, BackfillStrategy, LaunchedBy};
    use super::*;

    #[test]
    fn surreal_connect_config_unauthenticated_uses_default_scope() {
        let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000");
        assert_eq!(cfg.endpoint, "ws://surrealdb:8000");
        assert_eq!(cfg.namespace, DEFAULT_NAMESPACE);
        assert_eq!(cfg.database, DEFAULT_DATABASE);
        assert!(cfg.credentials.is_none());
    }

    #[test]
    fn surreal_connect_config_with_credentials_attaches_database_creds() {
        let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000")
            .with_credentials("rivers".into(), "topsecret".into());
        match cfg.credentials {
            Some(SurrealCredentials::Database { username, password }) => {
                assert_eq!(username, "rivers");
                assert_eq!(password, "topsecret");
            }
            None => panic!("credentials should be set"),
        }
    }

    async fn make_storage() -> SurrealStorage {
        SurrealStorage::new_memory()
            .await
            .expect("failed to create in-memory storage")
    }

    /// The run-events page must scan `idx_events_run_ts` (timestamp order), not sort.
    #[tokio::test]
    async fn run_events_page_uses_ordering_index() {
        let temp = test_temp_dir::test_temp_dir!();
        let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        let rows: Vec<DbEventWrite> = (0..2000i64)
            .map(|i| DbEventWrite {
                code_location_id: "default".into(),
                event_type: if i % 50 == 0 {
                    "StepStart"
                } else {
                    "Materialization"
                }
                .into(),
                asset_key: Some("a".into()),
                run_id: "r".into(),
                partition_key: None,
                timestamp: i,
                sort_order: 0,
                metadata: vec![],
                data_version: None,
                code_version: None,
                input_data_versions: vec![],
            })
            .collect();
        s.db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", rows))
            .await
            .unwrap()
            .check()
            .unwrap();

        let plan: Vec<serde_json::Value> =
            s.db.query(
                "SELECT * FROM events WHERE run_id = 'r' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT 50 START 0 EXPLAIN",
            )
            .await
            .unwrap()
            .take(0)
            .unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("idx_events_run_ts"),
            "page should scan idx_events_run_ts: {plan}"
        );
        assert!(
            !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
            "page should not sort — the ordering index covers it: {plan}"
        );
    }

    /// The asset-events page must scan `idx_events_loc_asset_ts`, not sort every matching event.
    #[tokio::test]
    async fn asset_events_page_uses_ordering_index() {
        let temp = test_temp_dir::test_temp_dir!();
        let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        let rows: Vec<DbEventWrite> = (0..2000i64)
            .map(|i| DbEventWrite {
                code_location_id: "default".into(),
                event_type: if i % 3 == 0 {
                    "Observation"
                } else {
                    "Materialization"
                }
                .into(),
                asset_key: Some("a".into()),
                run_id: "r".into(),
                partition_key: None,
                timestamp: i,
                sort_order: 0,
                metadata: vec![],
                data_version: None,
                code_version: None,
                input_data_versions: vec![],
            })
            .collect();
        s.db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", rows))
            .await
            .unwrap()
            .check()
            .unwrap();

        let plan: Vec<serde_json::Value> =
            s.db.query(
                "SELECT * FROM events WHERE code_location_id = 'default' AND asset_key = 'a' \
                 AND event_type IN ['Materialization', 'Observation'] \
                 ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT 50 START 0 EXPLAIN",
            )
            .await
            .unwrap()
            .take(0)
            .unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("idx_events_loc_asset_ts"),
            "asset page should scan idx_events_loc_asset_ts: {plan}"
        );
        assert!(
            !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
            "asset page should not sort: {plan}"
        );
    }

    /// The UNIQUE index compares the SERIALIZED partition_key, so a reordered-dims Multi key canonicalizes to one row.
    #[tokio::test]
    async fn test_multi_partition_key_dims_order_canonicalized() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let mk = |dims: Vec<(&str, &str)>| PartitionKey::Multi {
            dims: dims
                .into_iter()
                .map(|(d, v)| (d.to_string(), vec![v.to_string()]))
                .collect(),
        };
        let row = |pk: PartitionKey, event_id: &str, ts: i64| super::DbAssetPartitionWrite {
            code_location_id: cl.to_string(),
            asset_key: "inventory".to_string(),
            partition_key: pk,
            last_event_id: event_id.to_string(),
            last_run_id: "r".to_string(),
            last_timestamp: ts,
        };

        let date_first = mk(vec![("date", "2024-01-01"), ("region", "eu")]);
        let region_first = mk(vec![("region", "eu"), ("date", "2024-01-01")]);
        storage
            .upsert_asset_partitions(vec![row(date_first.clone(), "ev1", 1)])
            .await
            .unwrap();
        storage
            .upsert_asset_partitions(vec![row(region_first, "ev2", 2)])
            .await
            .unwrap();

        let parts = storage
            .get_materialized_partitions(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            parts,
            vec![date_first],
            "dims order must canonicalize to one row"
        );
        assert_eq!(
            storage
                .count_materialized_partitions(cl, "inventory")
                .await
                .unwrap(),
            1,
            "the count must agree with the deduped key set"
        );
    }

    /// `store_events`/`store_event` upsert `asset_partitions` on the UNIQUE index to replace rather than duplicate.
    #[tokio::test]
    async fn test_upsert_asset_partitions_replaces_on_unique_index() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let pk = PartitionKey::Single {
            keys: vec!["p1".to_string()],
        };
        let row = |event_id: &str, ts: i64| super::DbAssetPartitionWrite {
            code_location_id: cl.to_string(),
            asset_key: "inventory".to_string(),
            partition_key: pk.clone(),
            last_event_id: event_id.to_string(),
            last_run_id: "r".to_string(),
            last_timestamp: ts,
        };

        storage
            .upsert_asset_partitions(vec![row("ev1", 1)])
            .await
            .unwrap();
        storage
            .upsert_asset_partitions(vec![row("ev2", 2)])
            .await
            .unwrap();

        // Upsert on the unique index updates in place: one row, latest values.
        let parts = storage
            .get_materialized_partitions(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            parts,
            vec![pk.clone()],
            "must not duplicate the partition row"
        );
        let ts = storage
            .get_partition_timestamps(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            ts,
            vec![(pk.clone(), 2)],
            "must update the existing row in place"
        );
    }

    /// Per-partition lookups receive the display string and must still match a persisted Multi key.
    #[tokio::test]
    async fn test_partition_string_lookup_matches_multi_keys() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let pk = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let mut event = make_event("inv", "r1", 100);
        event.partition_key = Some(pk.clone());
        storage.store_event(&event).await.unwrap();

        let display = pk.to_display();
        assert_eq!(display, "date=2024-01-01|region=eu");
        let latest = storage
            .get_latest_materialization(cl, "inv", Some(&display))
            .await
            .unwrap();
        assert!(
            latest.is_some(),
            "display-form lookup must match the Multi event"
        );
        let events = storage
            .get_partition_events(cl, "inv", &display, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        // Single-dim lookups keep working.
        let mut single = make_event("inv", "r2", 200);
        single.partition_key = Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        });
        storage.store_event(&single).await.unwrap();
        assert!(
            storage
                .get_latest_materialization(cl, "inv", Some("p1"))
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Display lookups must prefer the structured (Multi) reading over a legacy Single event with a Multi-looking key.
    #[tokio::test]
    async fn test_partition_string_lookup_prefers_structured_multi() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let multi = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let display = multi.to_display();

        // Legacy event from the asset's static-keyed era — NEWER timestamp.
        let mut old_single = make_event("inv", "r1", 200);
        old_single.partition_key = Some(PartitionKey::Single {
            keys: vec![display.clone()],
        });
        storage.store_event(&old_single).await.unwrap();

        let mut multi_event = make_event("inv", "r2", 100);
        multi_event.partition_key = Some(multi.clone());
        storage.store_event(&multi_event).await.unwrap();

        let latest = storage
            .get_latest_materialization(cl, "inv", Some(&display))
            .await
            .unwrap()
            .expect("lookup must match");
        assert_eq!(
            latest.run_id, "r2",
            "the structured Multi reading wins over a newer legacy Single row"
        );
        let events = storage
            .get_partition_events(cl, "inv", &display, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "only the Multi partition's events return");
        assert_eq!(events[0].run_id, "r2");

        // The Single reading still works when no Multi rows exist.
        let mut plain = make_event("inv2", "r3", 100);
        plain.partition_key = Some(PartitionKey::Single {
            keys: vec![display.clone()],
        });
        storage.store_event(&plain).await.unwrap();
        assert!(
            storage
                .get_latest_materialization(cl, "inv2", Some(&display))
                .await
                .unwrap()
                .is_some(),
            "falls back to the Single reading"
        );
    }

    fn make_event(asset_key: &str, run_id: &str, ts: i64) -> EventRecord {
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some(asset_key.to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        }
    }

    fn make_asset_record(key: &str) -> AssetRecord {
        AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    #[tokio::test]
    async fn test_create_run_swallows_duplicate_id_after_retry() {
        use super::super::retry;

        let storage = make_storage().await;
        let run = RunRecord {
            run_id: "duplicate_run".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::NotStarted,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
        };

        // First CREATE succeeds.
        storage.create_run(&run).await.unwrap();

        // Second CREATE with the same run_id is swallowed (treated as success).
        storage
            .create_run(&run)
            .await
            .expect("duplicate create_run should be swallowed as phantom-commit success");

        // The row is still the original (no overwrite, no extra row).
        let fetched = storage.get_run("duplicate_run").await.unwrap().unwrap();
        assert_eq!(fetched, run);

        let raw_err = storage
            .db
            .create::<Option<RunRecord>>("runs")
            .content(run.clone())
            .await
            .expect_err("direct CREATE bypassing swallow must still error");
        assert!(raw_err.is_internal());
        assert!(raw_err.message().contains("already contains"));
        let anyhow_err = anyhow::Error::from(raw_err);
        assert!(retry::is_unique_index_violation(&anyhow_err));
        assert!(!retry::default_should_retry(&anyhow_err));
    }

    /// Each table fed by an `rivers-ui` LIVE channel must wake its `subscribe_table` stream on a write.
    #[tokio::test]
    async fn test_subscribe_table_wakes_for_every_live_channel_table() {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::time::timeout;

        // Helper: subscribe → brief delay to let the LIVE query register →
        // run the write → assert the stream yields within the deadline.
        async fn expect_yields(
            storage: &Arc<SurrealStorage>,
            table: &'static str,
            write: impl std::future::Future<Output = ()>,
        ) {
            let mut stream = storage
                .subscribe_table(table)
                .await
                .unwrap_or_else(|e| panic!("subscribe_table({table}) failed: {e}"));
            tokio::time::sleep(Duration::from_millis(100)).await;
            write.await;
            let first = timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for notification on `{table}`"));
            assert!(
                first.is_some(),
                "live-query stream for `{table}` ended before any notification"
            );
        }

        let storage = Arc::new(make_storage().await);

        // `runs` table.
        expect_yields(&storage, "runs", async {
            let run = RunRecord {
                run_id: "live_runs".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: 1,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual { user: None },
            };
            storage.create_run(&run).await.unwrap();
        })
        .await;

        // `assets` table.
        expect_yields(&storage, "assets", async {
            storage
                .register_assets(
                    crate::storage::DEFAULT_CODE_LOCATION_ID,
                    &[make_asset_record("live_asset")],
                )
                .await
                .unwrap();
        })
        .await;

        expect_yields(&storage, "asset_partitions", async {
            storage
                .db
                .query(
                    "CREATE asset_partitions SET \
                     asset_key = 'live_asset', \
                     partition_key = {kind: 'Single', keys: ['2024-01-01']}, \
                     last_timestamp = 1",
                )
                .await
                .unwrap();
        })
        .await;

        // `events` table.
        expect_yields(&storage, "events", async {
            let ev = make_event("live_asset", "live_runs", 1);
            storage.store_event(&ev).await.unwrap();
        })
        .await;

        // `backfills` table.
        expect_yields(&storage, "backfills", async {
            let bf = BackfillRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                backfill_id: "live_bf".into(),
                status: BackfillStatus::Requested,
                strategy: BackfillStrategy::MultiRun,
                failure_policy: BackfillFailurePolicy::Continue,
                asset_selection: vec!["live_asset".into()],
                job_name: None,
                partition_keys: vec![],
                run_ids: vec![],
                completed_partitions: vec![],
                failed_partitions: vec![],
                canceled_partitions: vec![],
                max_concurrency: 1,
                tags: vec![],
                create_time: 1,
                end_time: None,
                error: None,
                launched_by: LaunchedBy::default(),
            };
            storage.create_backfill(&bf).await.unwrap();
        })
        .await;

        // `ticks` table.
        expect_yields(&storage, "ticks", async {
            let tick = TickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "live_sched".into(),
                automation_type: "Schedule".into(),
                status: "Success".into(),
                timestamp: 1,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: None,
            };
            storage.store_tick(&tick).await.unwrap();
        })
        .await;

        // `condition_ticks` table.
        expect_yields(&storage, "condition_ticks", async {
            let ct = ConditionTickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                timestamp: 1,
                total_evaluated: 0,
                total_fired: 0,
                eval_duration_us: 0,
                run_ids: vec![],
                backfill_ids: vec![],
            };
            storage.store_condition_tick(&ct).await.unwrap();
        })
        .await;

        // `condition_evals` table.
        expect_yields(&storage, "condition_evals", async {
            let ev = ConditionEvalRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "live_asset".into(),
                tick_id: "live_ct".into(),
                timestamp: 1,
                fired: false,
                eval_duration_us: 0,
                run_ids: vec![],
                tree_json: b"{}".to_vec(),
                selection_json: None,
            };
            storage.store_condition_evals_batch(&[ev]).await.unwrap();
        })
        .await;

        // `concurrency_pools` table.
        expect_yields(&storage, "concurrency_pools", async {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "live_pool", 2, 60)
                .await
                .unwrap();
        })
        .await;

        expect_yields(&storage, "concurrency_slots", async {
            storage
                .db
                .query(
                    "CREATE concurrency_slots SET \
                     pool_key = 'live_pool', run_id = 'live_runs', step_key = 's1', \
                     slots_consumed = 1, claimed_at = 1, \
                     lease_expires_at = 9999999999, last_heartbeat = 1",
                )
                .await
                .unwrap();
        })
        .await;

        // `pending_steps` table — same argument as `concurrency_slots`.
        expect_yields(&storage, "pending_steps", async {
            storage
                .db
                .query(
                    "CREATE pending_steps SET \
                     pool_key = 'live_pool', run_id = 'live_pending', step_key = 's2', \
                     priority = 0, enqueued_at = 1, block_reason = 'PoolFull'",
                )
                .await
                .unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn test_kv_set_upserts_single_row() {
        // kv_set must be a single-statement upsert on the UNIQUE key index —
        // repeated writes keep exactly one row, and a crash can never observe
        // the key deleted (unlike the old DELETE+CREATE pair).
        let storage = make_storage().await;
        storage.kv_set("k", b"v1").await.unwrap();
        storage.kv_set("k", b"v2").await.unwrap();
        storage.kv_set("k", b"v3").await.unwrap();
        assert_eq!(storage.kv_get("k").await.unwrap().unwrap(), b"v3");
        let mut res = storage
            .db
            .query("SELECT * FROM kv WHERE key = $key")
            .bind(("key", "k".to_string()))
            .await
            .unwrap();
        let rows: Vec<DbKv> = res.take(0).unwrap();
        assert_eq!(rows.len(), 1, "upsert must keep exactly one row per key");
    }

    // ── UI integration regression tests ──────────────────────────────────

    // ── get_observations_since tests ──

    // ── Zero-coverage function tests ──

    #[tokio::test]
    async fn test_new_memory_schema_tables() {
        let storage = make_storage().await;

        // Verify all 8 tables exist by querying INFO FOR DB
        let mut result = storage.db.query("INFO FOR DB").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let info = info.unwrap();
        let tables = info["tables"].as_object().unwrap();

        let expected_tables = [
            "events",
            "assets",
            "asset_partitions",
            "runs",
            "kv",
            "dynamic_partitions",
            "ticks",
            "condition_ticks",
            "condition_evals",
        ];
        for table in expected_tables {
            assert!(tables.contains_key(table), "missing table: {table}");
        }
    }

    #[tokio::test]
    async fn test_new_memory_schema_indexes() {
        let storage = make_storage().await;

        let mut result = storage.db.query("INFO FOR TABLE events").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_events_run",
            "idx_events_type",
            "idx_events_run_type",
            "idx_events_run_ts",
            "idx_events_loc_asset",
            "idx_events_loc_asset_part",
            "idx_events_loc_asset_type",
            "idx_events_loc_asset_ts",
        ] {
            assert!(indexes.contains_key(idx), "events missing index: {idx}");
        }

        // assets: 3 indexes — composite (loc, key) UNIQUE + composite (loc, group) + (loc).
        let mut result = storage.db.query("INFO FOR TABLE assets").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_assets_loc_key",
            "idx_assets_loc_group",
            "idx_assets_loc",
        ] {
            assert!(indexes.contains_key(idx), "assets missing index: {idx}");
        }

        // runs: 6 indexes
        let mut result = storage.db.query("INFO FOR TABLE runs").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_runs_status",
            "idx_runs_job",
            "idx_runs_id",
            "idx_runs_start_time",
            "idx_runs_priority",
            "idx_runs_job_time",
        ] {
            assert!(indexes.contains_key(idx), "runs missing index: {idx}");
        }

        // kv: 1 index
        let mut result = storage.db.query("INFO FOR TABLE kv").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_kv_key"),
            "kv missing index: idx_kv_key"
        );

        // dynamic_partitions: 2 indexes
        let mut result = storage
            .db
            .query("INFO FOR TABLE dynamic_partitions")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in ["idx_dyn_part", "idx_dyn_part_unique"] {
            assert!(
                indexes.contains_key(idx),
                "dynamic_partitions missing index: {idx}"
            );
        }

        // ticks: 2 composite indexes (keyed per CL).
        let mut result = storage.db.query("INFO FOR TABLE ticks").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in ["idx_ticks_loc_name", "idx_ticks_loc_name_ts"] {
            assert!(indexes.contains_key(idx), "ticks missing index: {idx}");
        }

        // condition_ticks: 1 composite index.
        let mut result = storage
            .db
            .query("INFO FOR TABLE condition_ticks")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_cond_ticks_loc_ts"),
            "condition_ticks missing index: idx_cond_ticks_loc_ts"
        );

        // condition_evals: 3 indexes (2 composite + tick_id).
        let mut result = storage
            .db
            .query("INFO FOR TABLE condition_evals")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_cond_evals_loc_key",
            "idx_cond_evals_loc_key_ts",
            "idx_cond_evals_tick",
        ] {
            assert!(
                indexes.contains_key(idx),
                "condition_evals missing index: {idx}"
            );
        }

        // asset_partitions: 1 index
        let mut result = storage
            .db
            .query("INFO FOR TABLE asset_partitions")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_asset_part"),
            "asset_partitions missing index: idx_asset_part"
        );
    }

    #[tokio::test]
    async fn test_new_embedded_schema() {
        let dir = std::env::temp_dir().join(format!("rivers_test_{}", std::process::id()));
        // Clean up from any previous failed run
        let _ = std::fs::remove_dir_all(&dir);

        let storage = SurrealStorage::new_embedded(dir.to_str().unwrap())
            .await
            .unwrap();

        // Verify tables exist by running a simple query on each
        let mut result = storage.db.query("INFO FOR DB").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let tables = info.unwrap()["tables"].as_object().unwrap().clone();

        let expected_tables = [
            "events",
            "assets",
            "asset_partitions",
            "runs",
            "kv",
            "dynamic_partitions",
            "ticks",
            "condition_ticks",
            "condition_evals",
        ];
        for table in expected_tables {
            assert!(tables.contains_key(table), "missing table: {table}");
        }

        // Verify it's functional — write and read back
        storage.kv_set("test_key", b"hello").await.unwrap();
        let val = storage.kv_get("test_key").await.unwrap().unwrap();
        assert_eq!(val, b"hello");

        // Clean up
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_backfill_crud() {
        let storage = make_storage().await;
        let record = BackfillRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "bf-001".to_string(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["my_asset".to_string()],
            job_name: None,
            partition_keys: vec![PartitionKey::Single {
                keys: vec!["2024-01-15".to_string()],
            }],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 4,
            tags: vec![("team".to_string(), "data".to_string())],
            create_time: 1000,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::Manual {
                user: Some(crate::storage::UserRef {
                    subject: "sub-42".to_string(),
                    email: Some("john.doe@example.com".to_string()),
                    name: None,
                }),
            },
        };
        storage.create_backfill(&record).await.unwrap();

        let retrieved = storage.get_backfill("bf-001").await.unwrap();
        assert!(retrieved.is_some());
        let r = retrieved.unwrap();
        assert_eq!(r.backfill_id, "bf-001");
        assert_eq!(r.status, BackfillStatus::Requested);
        assert_eq!(r.partition_keys.len(), 1);
        assert_eq!(r.launched_by, record.launched_by, "provenance roundtrips");

        // A row created without launched_by (a v2 writer) gets the V3 DDL
        // default and must deserialize back to the struct default. The
        // genuinely-absent pre-V3 read path is covered by the migration-order
        // test `test_v3_backfill_launched_by_defaults_for_legacy_rows`.
        storage
            .db
            .query("CREATE backfills CONTENT { backfill_id: 'bf-old', code_location_id: 'default', status: 'Requested', strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', asset_selection: [], partition_keys: [], run_ids: [], completed_partitions: [], failed_partitions: [], canceled_partitions: [], max_concurrency: 1, tags: [], create_time: 1, end_time: NONE, error: NONE }")
            .await
            .unwrap()
            .check()
            .unwrap();
        let old_row = storage
            .get_backfill("bf-old")
            .await
            .unwrap()
            .expect("row without an explicit launched_by must deserialize");
        assert_eq!(
            old_row.launched_by,
            LaunchedBy::Manual { user: None },
            "the DDL default round-trips to the struct default"
        );
    }

    // ── Run queue tests ──

    // ── Concurrency pool tests ──

    #[tokio::test]
    async fn test_get_pool_info_with_slots_and_pending() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                10,
                300,
            )
            .await
            .unwrap();

        let now_ns = now_nanos();
        let future_ns = now_ns + 600_000_000_000; // 10 min from now
        let past_ns = now_ns - 60_000_000_000; // 1 min ago (expired)

        for (run_id, step_key, slots, expires) in [
            ("run1", "step_a", 2i64, future_ns),
            ("run2", "step_b", 3, future_ns),
            ("run3", "step_c", 1, past_ns), // expired
        ] {
            let resp = storage
                .db
                .query(
                    "INSERT INTO concurrency_slots { \
                         pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                         slots_consumed: $slots, claimed_at: $now, \
                         lease_expires_at: $expires, last_heartbeat: $now \
                     }",
                )
                .bind(("pool_key", "database".to_string()))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("slots", slots))
                .bind(("now", now_ns))
                .bind(("expires", expires))
                .await
                .unwrap();
            resp.check().unwrap();
        }

        // Insert pending step
        let resp = storage
            .db
            .query(
                "INSERT INTO pending_steps { \
                     pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                     priority: 0, enqueued_at: $now, block_reason: 'PoolFull' \
                 }",
            )
            .bind(("pool_key", "database".to_string()))
            .bind(("run_id", "run4".to_string()))
            .bind(("step_key", "step_d".to_string()))
            .bind(("now", now_ns))
            .await
            .unwrap();
        resp.check().unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "database")
            .await
            .unwrap();
        assert_eq!(info.slot_limit, 10);
        // 2 + 3 = 5 (expired slot with slots_consumed=1 should NOT be counted)
        assert_eq!(info.claimed_count, 5);
        assert_eq!(info.pending_count, 1);
    }

    // ── Claim/Release protocol tests ──

    // This test runs against the RocksDB backend rather than `make_storage()`
    // (kv-mem). The kv-mem implementation (surrealmx) has a known race in its
    // commit-queue conflict check that causes occasional lost updates under
    // concurrent writers — production uses RocksDB, so the test exercises the
    // path that actually has to hold up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sentinel_write_write_conflict() {
        // Prove that concurrent transactions writing the same key are detected:
        // 100 tasks all increment claim_version inside BEGIN/COMMIT.
        // With conflict detection, some will fail. Without retry, the final
        // counter value equals the number of successful commits.
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = std::sync::Arc::new(
            SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
                .await
                .expect("failed to create rocksdb storage"),
        );
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "conflict_test",
                10,
                300,
            )
            .await
            .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(100));
        let conflicts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let successes = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..100 {
            let storage = storage.clone();
            let barrier = barrier.clone();
            let conflicts = conflicts.clone();
            let successes = successes.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = storage
                    .db
                    .query(
                        "BEGIN TRANSACTION; \
                         UPDATE concurrency_pools \
                             SET claim_version = claim_version + 1 \
                             WHERE pool_key = 'conflict_test'; \
                         COMMIT TRANSACTION;",
                    )
                    .await;
                match result {
                    Ok(resp) => match resp.check() {
                        Ok(_) => {
                            successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(_) => {
                            conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let total_conflicts = conflicts.load(std::sync::atomic::Ordering::Relaxed);
        let total_successes = successes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(total_conflicts + total_successes, 100);

        // Final claim_version must equal successful commits (no lost updates).
        let mut result = storage
            .db
            .query(
                "SELECT VALUE claim_version FROM concurrency_pools \
                 WHERE pool_key = 'conflict_test' LIMIT 1",
            )
            .await
            .unwrap();
        let versions: Vec<u32> = result.take(0).unwrap();
        let final_version = versions[0];

        assert_eq!(
            final_version, total_successes,
            "claim_version ({final_version}) must equal successful commits ({total_successes}), \
             conflicts={total_conflicts}"
        );

        // Log the outcome for visibility.
        eprintln!(
            "sentinel test: successes={total_successes}, conflicts={total_conflicts}, \
             claim_version={final_version}"
        );
    }

    #[tokio::test]
    async fn test_claim_check_statement_index() {
        let storage = make_storage().await;
        let pool_names = ["a", "b", "c", "d", "e"];
        for name in &pool_names {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, name, 10, 300)
                .await
                .unwrap();
        }

        for n in 1..=5 {
            let pools: Vec<(String, u32)> =
                pool_names[..n].iter().map(|k| (k.to_string(), 1)).collect();
            let query = SurrealStorage::build_claim_transaction(&pools);
            let now_ns = now_nanos();
            let lease_exp = now_ns + 300_000_000_000i64;

            let mut q = storage.db.query(&query);
            for (i, (pk, _)) in pools.iter().enumerate() {
                q = q.bind((format!("p{i}"), pk.clone()));
            }
            q = q
                .bind(("cl", crate::storage::DEFAULT_CODE_LOCATION_ID.to_string()))
                .bind(("run_id", format!("run_{n}")))
                .bind(("step_key", format!("step_{n}")))
                .bind(("now", now_ns))
                .bind(("lease_exp", lease_exp));

            let mut response = q.await.unwrap().check().unwrap();
            let idx = SurrealStorage::claim_check_statement_index(n);
            let count: Option<u32> = response.take((idx, "total")).unwrap();
            assert_eq!(
                count,
                Some(n as u32),
                "pools={n}: expected {n} slots at statement index {idx}"
            );
        }
    }

    // ── Lease renewal and expiry ──

    // -----------------------------------------------------------------------
    // Executor integration pattern tests
    // -----------------------------------------------------------------------

    /// Coordinator GC pattern: expired leases freed by free_expired_leases during tick.
    #[tokio::test]
    async fn test_coordinator_gc_frees_crashed_slots() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();

        // Claim with very short lease (1 nanosecond — already expired)
        let now_ns = now_nanos();
        let expired_lease = now_ns - 1_000_000_000; // 1 second in the past
        storage
            .db
            .query(
                "CREATE concurrency_slots SET \
                 pool_key = 'db', run_id = 'crashed_run', step_key = 'step_x', \
                 slots_consumed = 1, claimed_at = $now, \
                 lease_expires_at = $exp, last_heartbeat = $now",
            )
            .bind(("now", now_ns))
            .bind(("exp", expired_lease))
            .await
            .unwrap();

        // Pool shows 0 claimed (expired excluded from capacity check)
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);

        // GC sweep removes the physical row
        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 1);
    }

    // -----------------------------------------------------------------------
    // Coordinator tick overhead stress test
    // -----------------------------------------------------------------------

    /// Simulates a full coordinator tick cycle at various queue/run sizes and measures wall-clock time.
    #[tokio::test]
    async fn coordinator_tick_stress() {
        use std::time::Instant;

        let storage = make_storage().await;

        // Setup: 5 pools with active slots + pending steps
        for pool in ["db", "api", "gpu", "cpu", "net"] {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, pool, 10, 300)
                .await
                .unwrap();
        }

        // Scenario matrix: (in_progress_runs, queued_runs, active_pool_slots)
        let scenarios: &[(usize, usize, usize)] = &[
            (0, 0, 0),          // idle system
            (5, 10, 20),        // moderate queue
            (10, 50, 50),       // busy system
            (10, 200, 100),     // large queue
            (10, 500, 200),     // 500 queued
            (10, 1_000, 500),   // 1k queued
            (10, 2_000, 500),   // 2k queued
            (10, 5_000, 1000),  // 5k queued
            (10, 10_000, 1000), // 10k queued
        ];

        for &(n_in_progress, n_queued, n_slots) in scenarios {
            // Clean slate per scenario
            storage
                .db
                .query("DELETE FROM runs; DELETE FROM concurrency_slots; DELETE FROM pending_steps")
                .await
                .unwrap();

            let now = now_nanos();
            let lease_exp = now + 300_000_000_000i64; // 5 min from now

            // Create in-progress runs
            for i in 0..n_in_progress {
                storage
                    .create_run(&RunRecord {
                        run_id: format!("ip-{i}"),
                        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                        job_name: Some("bench".into()),
                        status: RunStatus::Started,
                        start_time: now,
                        end_time: None,
                        tags: vec![
                            ("env".into(), "prod".into()),
                            ("team".into(), format!("team-{}", i % 5)),
                        ],
                        node_names: vec![format!("asset_{i}")],
                        priority: 0,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual { user: None },
                    })
                    .await
                    .unwrap();
            }

            // Create queued runs
            for i in 0..n_queued {
                storage
                    .create_run(&RunRecord {
                        run_id: format!("q-{i}"),
                        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                        job_name: Some("bench".into()),
                        status: RunStatus::Queued,
                        start_time: now,
                        end_time: None,
                        tags: vec![("env".into(), "staging".into())],
                        node_names: vec![format!("asset_{i}")],
                        priority: i as i32 % 10,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual { user: None },
                    })
                    .await
                    .unwrap();
            }

            // Create active pool slots
            for i in 0..n_slots {
                let pool = ["db", "api", "gpu", "cpu", "net"][i % 5];
                storage
                    .db
                    .query(
                        "CREATE concurrency_slots SET \
                     pool_key = $pool, run_id = $rid, step_key = $sk, \
                     slots_consumed = 1, claimed_at = $now, \
                     lease_expires_at = $exp, last_heartbeat = $now",
                    )
                    .bind(("pool", pool))
                    .bind(("rid", format!("ip-{}", i % n_in_progress.max(1))))
                    .bind(("sk", format!("step_{i}")))
                    .bind(("now", now))
                    .bind(("exp", lease_exp))
                    .await
                    .unwrap();
            }

            // Warm up
            let _ = storage
                .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
                .await;

            let n_ticks: usize = if n_queued >= 2000 { 10 } else { 50 };
            let start = Instant::now();
            for _ in 0..n_ticks {
                let _ = storage
                    .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
                    .await
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let per_tick = elapsed / n_ticks as u32;

            eprintln!(
                "  in_progress={n_in_progress:>3}, queued={n_queued:>5}, slots={n_slots:>4} → \
                 {per_tick:>8.3?}/tick ({n_ticks} ticks in {elapsed:.3?})"
            );
        }
    }

    // ── Observability event tests ──

    #[tokio::test]
    async fn test_event_type_from_type_name_roundtrip() {
        // Verify all new event types can roundtrip through type_name / from_type_name
        let types = vec![
            EventType::RunQueued,
            EventType::RunDequeued,
            EventType::StepSlotClaimed,
            EventType::StepSlotWaiting,
            EventType::StepSlotRenewed,
            EventType::StepSlotReleased,
        ];

        for evt in types {
            let name = evt.type_name();
            let reconstructed = EventType::from_type_name(name, None).unwrap();
            assert_eq!(evt, reconstructed, "roundtrip failed for {name}");
        }
    }

    // ── get_pool_slot_holders ──

    // ── get_all_pool_infos ──

    // ── cancel_queued_run ──

    fn minimal_run(run_id: &str, status: RunStatus) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
        }
    }

    // ── try_start_run ──

    /// Concurrent cancel vs start on the same NotStarted run must settle on
    /// exactly one winner. Embedded backend — kv-mem misses some write-write
    /// conflicts.
    #[tokio::test]
    async fn test_cancel_vs_start_race_settles_consistently() {
        let temp = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        storage
            .create_run(&minimal_run("r1", RunStatus::NotStarted))
            .await
            .unwrap();

        let (canceled, started) =
            tokio::join!(storage.cancel_queued_run("r1"), storage.try_start_run("r1"));
        let (canceled, started) = (canceled.unwrap(), started.unwrap());
        assert_ne!(canceled, started, "exactly one side must win");

        let run = storage.get_run("r1").await.unwrap().unwrap();
        let expected = if canceled {
            RunStatus::Canceled
        } else {
            RunStatus::Started
        };
        assert_eq!(run.status, expected);
    }

    // ── get_stalled_not_started_runs ──

    // ── queued view includes NotStarted ──

    // ── backfill launch recovery ──

    // ── Run progress, outcome, cancellation, step events ──
}
