//! PostgreSQL storage backend.
//!
//! For a server only: there is no embedded PostgreSQL. Local development
//! uses the embedded SurrealDB backend.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

use super::event_batch::EventWithId;
use super::migration::{Capability, SchemaStamps, SchemaStatus};
use super::retry::{StorageRetryConfig, postgres_should_retry, with_retry_if};
#[allow(unused_imports)]
use super::surrealdb_backend::record_id_str;
use super::url::redact_password;
use super::*;
use crate::assets::remote::LocationRef;

mod keys;
mod migration;
#[cfg(test)]
mod probes;
mod test_schema;
mod traits;

mod asset_versions;
mod assets;
mod automation;
mod backfills;
mod directory;
mod event_store;
mod events;
mod pages;
mod partition_failures;
mod partitions;
mod pools;
mod runs;

#[allow(unused_imports)]
use keys::{bounds_ord, key_json, partition_ord};
#[allow(unused_imports)]
use partition_failures::{RunChange, RunOfFailures, StoredStatus};
pub use test_schema::create_test_schema;

/// Connections of one store. A run sends its writes one at a time; the
/// daemon and the UI server read from several tasks.
const POOL_SIZE: u32 = 8;

/// A connection without work for this long is closed: a server has a limit
/// of connections for all the processes of a deployment.
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Storage over a PostgreSQL server.
pub struct PostgresStorage {
    pool: PgPool,
    /// For `label()`: the URL with the password removed.
    endpoint: String,
    /// For `subscribe_table`: a listener holds a connection of its own for
    /// its whole life, so it does not take one from the pool.
    options: PgConnectOptions,
    /// The schema this store writes to. A listener ignores the notifications
    /// of other schemas of the same database.
    schema: String,
    retry_config: StorageRetryConfig,
    /// Declared last: the schema of a test store goes after its pool.
    #[cfg(test)]
    test_schema: Option<test_schema::TestSchema>,
}

impl PostgresStorage {
    /// Connect, then gate on the capability floors of the schema.
    pub async fn connect(url: &str, cap: Capability) -> Result<Self> {
        Self::connect_with_retry(url, StorageRetryConfig::default(), cap).await
    }

    pub async fn connect_with_retry(
        url: &str,
        retry_config: StorageRetryConfig,
        cap: Capability,
    ) -> Result<Self> {
        let endpoint = redact_password(url);
        let options: PgConnectOptions = url
            .parse()
            .with_context(|| format!("parsing the PostgreSQL URL {endpoint}"))?;
        Self::connect_with_options(options, endpoint, retry_config, cap).await
    }

    async fn connect_with_options(
        options: PgConnectOptions,
        endpoint: String,
        retry_config: StorageRetryConfig,
        cap: Capability,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(POOL_SIZE)
            .idle_timeout(IDLE_TIMEOUT)
            .connect_with(options.clone())
            .await
            .with_context(|| format!("connecting to PostgreSQL at {endpoint}"))?;
        migration::ensure_compatible(&pool, cap).await?;
        // Where the tables are found. With several schemas on the search
        // path this is not always the schema that new objects go to.
        let schema: String = sqlx::query_scalar(
            "SELECT COALESCE( \
                (SELECT n.nspname::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = to_regclass('migration_meta')), \
                current_schema()::text)",
        )
        .fetch_one(&pool)
        .await?;
        Ok(Self {
            pool,
            endpoint,
            options,
            schema,
            retry_config,
            #[cfg(test)]
            test_schema: None,
        })
    }

    /// The PostgreSQL server of the tests (`RIVERS_TEST_POSTGRES_URL`).
    /// `None`: the tests run on SurrealDB only.
    #[cfg(test)]
    pub(crate) fn test_url() -> Option<String> {
        std::env::var("RIVERS_TEST_POSTGRES_URL")
            .ok()
            .filter(|url| !url.is_empty())
    }

    /// A store in a new schema of the database at `url`, for a test. Each
    /// test store has a schema of its own, so tests do not see each other.
    #[cfg(test)]
    pub(crate) async fn connect_test_schema(url: &str) -> Result<Self> {
        Self::connect_test_schema_with_retry(url, StorageRetryConfig::default()).await
    }

    #[cfg(test)]
    pub(crate) async fn connect_test_schema_with_retry(
        url: &str,
        retry_config: StorageRetryConfig,
    ) -> Result<Self> {
        let options: PgConnectOptions = url.parse()?;
        let test_schema = test_schema::TestSchema::create(options.clone()).await?;
        let options = options.options([("search_path", test_schema.name())]);
        let mut storage = Self::connect_with_options(
            options,
            redact_password(url),
            retry_config,
            Capability::ReadWrite,
        )
        .await?;
        storage.test_schema = Some(test_schema);
        Ok(storage)
    }

    /// The pool, for a test that reads a table directly.
    #[cfg(test)]
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Backend label for logs and the UI. Never includes the password.
    pub fn label(&self) -> String {
        format!("PostgreSQL ({})", self.endpoint)
    }

    /// The stamps of the store. `None`: the store is not initialized.
    pub async fn schema_stamps(&self) -> Result<Option<SchemaStamps>> {
        migration::read_schema_stamps(&self.pool).await
    }

    /// The stamps of the store, and each code location and component with
    /// what a migration to this build does to it.
    pub async fn schema_status(&self) -> Result<SchemaStatus> {
        super::migration::schema_status(self, self.schema_stamps().await?).await
    }

    /// Fails with [`MigrationBlocked`](super::migration::MigrationBlocked)
    /// when a migration to this build stops a code location that uses the store.
    pub async fn ensure_migration_stops_nothing(&self) -> Result<()> {
        super::migration::ensure_migration_stops_nothing(self.schema_status().await?)
    }

    /// Migrate a database to this build (`rivers db migrate`). Unless
    /// `force`, a migration that stops a code location fails and changes
    /// nothing.
    pub async fn migrate_remote(url: &str, force: bool) -> Result<()> {
        let storage = Self::connect(url, Capability::Inspect).await?;
        if !force {
            storage.ensure_migration_stops_nothing().await?;
        }
        migration::migrate_to_current(&storage.pool).await
    }

    /// Run `f` again while it fails with an error that a new attempt can pass.
    pub(super) async fn retry<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        with_retry_if(&self.retry_config, postgres_should_retry, f).await
    }

    /// Subscribe to the change notifications of `table`. Each statement that
    /// changes the table yields one `()`. The stream ends when the listener
    /// loses its connection: the changes since then are lost, so the caller
    /// must subscribe again and read the table.
    pub async fn subscribe_table(&self, table: &str) -> Result<PgLiveTable> {
        use futures_util::StreamExt;
        let mut listener = sqlx::postgres::PgListener::connect_with(
            &PgPoolOptions::new()
                .max_connections(1)
                .connect_with(self.options.clone())
                .await?,
        )
        .await?;
        listener.listen(&format!("rivers_{table}")).await?;
        let schema: std::sync::Arc<str> = std::sync::Arc::from(self.schema.as_str());
        let table: std::sync::Arc<str> = std::sync::Arc::from(table);
        let changes = futures_util::stream::unfold(listener, move |mut listener| {
            let schema = schema.clone();
            let table = table.clone();
            async move {
                loop {
                    match listener.try_recv().await {
                        Ok(Some(change)) if change.payload() == &*schema => {
                            return Some(((), listener));
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            tracing::warn!(
                                target: "rivers::storage",
                                table = %table,
                                "the change listener lost its connection"
                            );
                            return None;
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "rivers::storage",
                                table = %table,
                                error = %e,
                                "the change listener failed"
                            );
                            return None;
                        }
                    }
                }
            }
        })
        .boxed();
        Ok(PgLiveTable { changes })
    }
}

/// The change notifications of a table. Dropping it ends the listener.
pub struct PgLiveTable {
    changes: futures_util::stream::BoxStream<'static, ()>,
}

impl futures_util::Stream for PgLiveTable {
    type Item = ();

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<()>> {
        self.get_mut().changes.as_mut().poll_next(cx)
    }
}

/// A value as a `jsonb` bind. The record types derive serde, so the stored
/// form is their serde form.
pub(super) fn to_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).context("serializing a storage field to jsonb")
}

/// A `jsonb` column as its record type.
pub(super) fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value).context("deserializing a jsonb storage field")
}

pub(super) fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// The error of an operation that this backend does not have yet.
#[allow(dead_code)]
pub(super) fn not_implemented(operation: &str) -> anyhow::Error {
    anyhow::anyhow!("the PostgreSQL backend does not implement `{operation}` yet")
}

#[cfg(test)]
mod tests;
