//! PostgreSQL backend: the key-value entries, the code location directory
//! and the components.
//!
//! A directory entry is the kv row `code_location:<namespace>/<name>`, as on
//! SurrealDB. A writer that read an entry writes it back only if its bytes
//! did not change in between.

use super::*;
use crate::storage::migration::SCHEMA_VERSION;

/// Attempts before giving up on an entry that other writers keep changing.
const ATTEMPTS: usize = 16;

impl PostgresStorage {
    pub(super) async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT value FROM kv WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub(super) async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO kv (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn list_code_locations(
        &self,
    ) -> Result<Vec<(LocationRef, CodeLocationDirectoryEntry)>> {
        let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT key, value FROM kv \
             WHERE key >= 'code_location:' AND key < 'code_location;' ORDER BY key",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(key, value)| {
                let location = key
                    .strip_prefix(crate::CODE_LOCATION_DIRECTORY_KEY_PREFIX)
                    .and_then(|spec| LocationRef::try_from(spec.to_string()).ok());
                let entry = serde_json::from_slice(&value).ok();
                if location.is_none() || entry.is_none() {
                    tracing::warn!(
                        target: "rivers::storage",
                        key = %key,
                        "skipping a code location directory entry that does not decode"
                    );
                }
                Some((location?, entry?))
            })
            .collect())
    }

    pub(super) async fn get_code_location_entry(
        &self,
        location: &LocationRef,
    ) -> Result<Option<CodeLocationDirectoryEntry>> {
        let value = self
            .kv_get(&crate::code_location_directory_key(location))
            .await?;
        Ok(value
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?)
    }

    pub(super) async fn upsert_code_location_registration(
        &self,
        location: &LocationRef,
        entry: CodeLocationDirectoryEntry,
    ) -> Result<()> {
        self.update_directory_entry(location, |current| {
            let mut next = entry.clone();
            if let Some(current) = current.filter(|c| c.identity == entry.identity) {
                next.grpc_endpoint = next.grpc_endpoint.or(current.grpc_endpoint);
                next.module = next.module.or(current.module);
                next.last_seen = next.last_seen.or(current.last_seen);
            }
            Some(next)
        })
        .await?;
        Ok(())
    }

    pub(super) async fn touch_code_location(
        &self,
        location: &LocationRef,
        identity: &str,
        grpc_endpoint: &str,
        now: i64,
    ) -> Result<bool> {
        self.update_directory_entry(location, |current| {
            let mut entry = current.filter(|c| c.identity == identity)?;
            entry.grpc_endpoint = Some(grpc_endpoint.to_string());
            entry.last_seen = Some(now);
            entry.set_build();
            Some(entry)
        })
        .await
    }

    pub(super) async fn delete_code_location_if(
        &self,
        location: &LocationRef,
        identity: &str,
    ) -> Result<bool> {
        let key = crate::code_location_directory_key(location);
        for _ in 0..ATTEMPTS {
            let Some(current) = self.kv_get(&key).await? else {
                return Ok(false);
            };
            let entry: CodeLocationDirectoryEntry = serde_json::from_slice(&current)?;
            if entry.identity != identity {
                return Ok(false);
            }
            let deleted = sqlx::query("DELETE FROM kv WHERE key = $1 AND value = $2")
                .bind(&key)
                .bind(&current)
                .execute(&self.pool)
                .await?;
            if deleted.rows_affected() == 1 {
                return Ok(true);
            }
        }
        anyhow::bail!("the directory entry of {location} kept changing")
    }

    /// Replace `location`'s entry with `update(current)`; `None` leaves it.
    /// Returns whether it wrote.
    async fn update_directory_entry(
        &self,
        location: &LocationRef,
        mut update: impl FnMut(Option<CodeLocationDirectoryEntry>) -> Option<CodeLocationDirectoryEntry>,
    ) -> Result<bool> {
        let key = crate::code_location_directory_key(location);
        for _ in 0..ATTEMPTS {
            let current = self.kv_get(&key).await?;
            let entry = current.as_deref().map(serde_json::from_slice).transpose()?;
            let Some(next) = update(entry) else {
                return Ok(false);
            };
            let next = serde_json::to_vec(&next)?;
            let written =
                match &current {
                    Some(current) => {
                        sqlx::query("UPDATE kv SET value = $3 WHERE key = $1 AND value = $2")
                            .bind(&key)
                            .bind(current)
                            .bind(&next)
                            .execute(&self.pool)
                            .await?
                    }
                    None => sqlx::query(
                        "INSERT INTO kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                    )
                    .bind(&key)
                    .bind(&next)
                    .execute(&self.pool)
                    .await?,
                };
            if written.rows_affected() == 1 {
                return Ok(true);
            }
        }
        anyhow::bail!("the directory entry of {location} kept changing")
    }

    pub(super) async fn record_component(&self, name: &str, writes: bool, now: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO components (name, schema_version, rivers_version, writes, started_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (name) DO UPDATE SET schema_version = EXCLUDED.schema_version, \
                 rivers_version = EXCLUDED.rivers_version, writes = EXCLUDED.writes, \
                 started_at = EXCLUDED.started_at",
        )
        .bind(name)
        .bind(i64::from(SCHEMA_VERSION))
        .bind(crate::RIVERS_VERSION)
        .bind(writes)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn list_components(&self) -> Result<Vec<(String, ComponentRecord)>> {
        let rows: Vec<(String, i64, String, bool, i64)> = sqlx::query_as(
            "SELECT name, schema_version, rivers_version, writes, started_at \
             FROM components ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(
                |(name, schema_version, rivers_version, writes, started_at)| {
                    let record = ComponentRecord {
                        schema_version: u32::try_from(schema_version)?,
                        rivers_version,
                        writes,
                        started_at,
                    };
                    Ok((name, record))
                },
            )
            .collect()
    }
}
