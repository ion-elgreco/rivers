//! SurrealDB storage backend implementation.

use std::collections::HashMap;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::{self, Any};
use surrealdb::types::{RecordId, SurrealValue};

use super::{EventRecord, PartitionKey, StorageBackend};

mod migration;
pub use migration::{Capability, SchemaMigrationNeeded};

mod backend;
mod pages;
mod per_code_location;
mod pools;
mod queries;
mod rows;
mod run_queue;

use queries::*;
use rows::*;

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
/// How many asset rows go into one `INSERT` when registering a code location.
///
/// Batching is what makes registration fast — the previous implementation ran
/// two queries per asset, so a 100 000-asset location cost 200 000 round trips.
/// Chunking keeps any single statement from growing unbounded.
const REGISTER_ASSETS_CHUNK: usize = 2_000;

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
                // The router closes the datastore only when it exits on its
                // own; shutting the runtime down first cancels it and leaves
                // the RocksDB files open until the process exits.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while runtime.metrics().num_alive_tasks() > 0
                    && std::time::Instant::now() < deadline
                {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
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

    /// Subscribe to change notifications on `table` via a SurrealDB LIVE
    /// query. Each change yields `()`; only the record id travels with it.
    pub async fn subscribe_table(&self, table: &str) -> Result<LiveTable> {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use surrealdb::Notification;
        use surrealdb::types::{Action, Uuid, Value};
        let mut response = self
            .db
            .query(format!("LIVE SELECT id FROM {table}"))
            .await?;
        let id = response
            .take::<Option<Uuid>>(0)?
            .context("LIVE SELECT returned no query id")?;
        let stream = response.stream::<Notification<Value>>(0)?;
        let table_owned: Arc<str> = Arc::from(table);
        let changes = stream
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
            .boxed();
        Ok(LiveTable {
            changes,
            db: self.db.clone(),
            id,
        })
    }
}

/// A table's live query. [`LiveTable::close`] ends it on the session that
/// created it. The SDK's own kill on drop runs from a session that never
/// selected a namespace, fails, and leaves the query to the server for as
/// long as the connection lives.
pub struct LiveTable {
    changes: futures_util::stream::BoxStream<'static, ()>,
    db: Surreal<Any>,
    id: surrealdb::types::Uuid,
}

impl LiveTable {
    /// Kill the query, then let the stream run to its `Killed` notification
    /// so the SDK side drops quietly; past the budget it drops as is.
    pub async fn close(mut self) {
        use futures_util::StreamExt;
        let killed = self
            .db
            .query("KILL $id")
            .bind(("id", self.id))
            .await
            .and_then(|response| response.check());
        if let Err(e) = killed {
            tracing::debug!(target: "rivers::storage", id = %self.id, error = %e, "live query kill failed");
        }
        let drained = async { while self.changes.next().await.is_some() {} };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), drained).await;
    }
}

impl futures_util::Stream for LiveTable {
    type Item = ();

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<()>> {
        self.get_mut().changes.as_mut().poll_next(cx)
    }
}

fn record_id_str(id: &RecordId) -> String {
    format!("{}:{:?}", id.table.as_str(), id.key)
}

/// Client-generated `events` record id — retried inserts replay the same id
/// so `INSERT IGNORE` deduplicates instead of appending.
fn new_event_record_id() -> RecordId {
    RecordId::new("events", uuid::Uuid::new_v4().simple().to_string())
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

#[derive(Debug, SurrealValue, serde::Deserialize)]
struct OptStringField {
    value: Option<String>,
}

impl SurrealStorage {
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

    /// Apply one Deletion event's state clearing.
    /// Partition-scoped deletions drop that partition's row; whole-asset
    /// deletions clear the asset's materialization state and every partition
    /// row. `last_event_id` points the timeline at the deletion, while
    /// `last_run_id`, `last_timestamp` and `last_data_version` are cleared with
    /// the rest of the materialization state — conditions read them as "the run
    /// whose data this asset holds" and "when it acquired that data", and a
    /// deleted asset holds none (so `Missing` fires again, and `NewlyUpdated`
    /// does not). A partition-scoped deletion leaves the asset's
    /// `last_data_version` alone — other partitions may still hold data.
    /// Rows apply by event time, not commit order: state materialized after
    /// the deletion stays.
    async fn consolidate_deletion(
        &self,
        cl: &str,
        asset_key: &str,
        event: &EventRecord,
        event_id: &str,
    ) -> Result<()> {
        match &event.partition_key {
            Some(pk) => {
                self.delete_partitions(cl, asset_key, vec![(pk.clone(), event.timestamp)])
                    .await?;
            }
            None => {
                // `last_timestamp` is cleared with the rest: downstream reads
                // it as "this dependency produced something new", so leaving it
                // to advance would make deleting an asset trigger a
                // materialization from data that no longer exists. The event id
                // still points at the deletion so timelines resolve. One
                // transaction, not separate statements: without it each statement
                // commits on its own, and a concurrent materialization can land
                // between them — leaving the asset row and its partition rows
                // disagreeing. A conflict aborts the whole transaction and the
                // caller's retry re-applies all of it. An unset time is NONE,
                // which sorts below every number.
                self.db
                    .query(format!(
                        "BEGIN TRANSACTION; {CLEAR_DELETED_ASSET} COMMIT TRANSACTION;"
                    ))
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.to_string()))
                    .bind(("event_id", event_id.to_string()))
                    .bind(("ts", event.timestamp))
                    .await?
                    .check()?;
            }
        }
        Ok(())
    }

    /// Apply partition-scoped deletions of one asset: rows no newer than
    /// their key's deletion gone, tombstones kept at each key's newest
    /// deletion time.
    async fn delete_partitions(
        &self,
        cl: &str,
        asset_key: &str,
        deletions: Vec<(PartitionKey, i64)>,
    ) -> Result<()> {
        let mut newest: HashMap<PartitionKey, i64> = HashMap::new();
        for (pk, ts) in deletions {
            newest
                .entry(pk)
                .and_modify(|t| *t = (*t).max(ts))
                .or_insert(ts);
        }
        let mut by_time: HashMap<i64, Vec<PartitionKey>> = HashMap::new();
        for (pk, ts) in &newest {
            by_time.entry(*ts).or_default().push(pk.clone());
        }
        let rows: Vec<DbPartitionDeletion> = newest
            .into_iter()
            .map(|(partition_key, timestamp)| DbPartitionDeletion {
                code_location_id: cl.to_string(),
                asset_key: asset_key.to_string(),
                partition_key,
                timestamp,
            })
            .collect();
        // One DELETE per deletion time, not a FOR loop, which costs more. The
        // members of one delete step share a time, so this is one statement.
        let mut sql = String::from("BEGIN TRANSACTION; ");
        for i in 0..by_time.len() {
            sql.push_str(&format!(
                "DELETE FROM asset_partitions WHERE code_location_id = $cl \
                 AND asset_key = $asset_key AND partition_key IN $keys_{i} \
                 AND last_timestamp <= $deleted_at_{i}; "
            ));
        }
        sql.push_str(RECORD_PARTITION_TOMBSTONES);
        let mut query = self
            .db
            .query(sql)
            .bind(("cl", cl.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("rows", rows));
        for (i, (deleted_at, keys)) in by_time.into_iter().enumerate() {
            query = query
                .bind((format!("keys_{i}"), keys))
                .bind((format!("deleted_at_{i}"), deleted_at));
        }
        query.await?.check()?;
        Ok(())
    }

    /// Which of `ids` belong to action runs. Events don't carry the verb, so
    /// the materialization consolidation reads it off the run record; a
    /// missing row reads as materialize (fail-open to the old behavior).
    async fn action_run_ids(&self, ids: Vec<String>) -> Result<std::collections::HashSet<String>> {
        if ids.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let mut result = self
            .db
            .query("SELECT VALUE run_id FROM runs WHERE run_id IN $ids AND action IS NOT NONE")
            .bind(("ids", ids))
            .await?;
        let rows: Vec<String> = result.take(0)?;
        Ok(rows.into_iter().collect())
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

impl SurrealStorage {
    /// Roll the `assets` row (and any partition rows, in one transaction)
    /// forward to a materialization event — the single consolidation shared
    /// by `store_event` and `store_events`. The data comes from the event at
    /// `timestamp`, the provenance (`code_version`, `idv`) from the one at
    /// `provenance_timestamp`: in a drain, the newest event that read inputs
    /// can be older than the newest event. `from_action`: the event's run is
    /// an action run AND no co-drained event supplied real provenance
    /// (`idv`) — then no provenance is written, so a pending Stale(Code)
    /// badge survives the verb.
    #[allow(clippy::too_many_arguments)]
    async fn apply_materialization(
        &self,
        cl: &str,
        asset_key: &str,
        event_id: &str,
        run_id: &str,
        timestamp: i64,
        data_version: Option<String>,
        provenance_timestamp: i64,
        code_version: Option<String>,
        idv: Option<Vec<(String, String)>>,
        from_action: bool,
        parts: Vec<DbAssetPartitionWrite>,
    ) -> Result<()> {
        let update_sql = format!("{MATERIALIZE_ASSET_DATA} {MATERIALIZATION_IS_NEWER}");
        let update_sql = if from_action {
            update_sql
        } else {
            let provenance_sql = if idv.is_some() {
                MATERIALIZE_ASSET_PROVENANCE
            } else {
                MATERIALIZE_ASSET_PROVENANCE_KEEP_IDV
            };
            format!("{update_sql}; {provenance_sql} {PROVENANCE_IS_NEWER}")
        };
        // The asset row and its partition rows commit together — a concurrent
        // whole-asset deletion lands wholly before or wholly after this
        // materialization, never between the two. The partition upsert reads
        // the asset row first, so it runs before the row's update. The data
        // and provenance updates commit together too: no reader sees new data
        // with the provenance of the materialization it replaced.
        let has_parts = !parts.is_empty();
        let sql = if has_parts {
            format!(
                "BEGIN TRANSACTION; {UPSERT_ASSET_PARTITIONS}; {update_sql}; COMMIT TRANSACTION;"
            )
        } else {
            format!("BEGIN TRANSACTION; {update_sql}; COMMIT TRANSACTION;")
        };
        let mut query = self
            .db
            .query(sql)
            .bind(("cl", cl.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("event_id", event_id.to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("timestamp", timestamp))
            .bind(("data_version", data_version));
        if !from_action {
            query = query
                .bind(("provenance_timestamp", provenance_timestamp))
                .bind(("mcv", code_version));
            if let Some(idv) = idv {
                query = query.bind(("idv", idv));
            }
        }
        if has_parts {
            let keys: Vec<PartitionKey> = parts.iter().map(|p| p.partition_key.clone()).collect();
            let oldest = parts.iter().map(|p| p.last_timestamp).min();
            query = query
                .bind(("keys", keys))
                .bind(("oldest", oldest))
                .bind(("rows", parts));
        }
        query.await?.check()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
