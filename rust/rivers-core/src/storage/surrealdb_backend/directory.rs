//! The code location directory: one kv row per location. Writers that read
//! an entry first write it back only if its bytes did not change in between.

use anyhow::Result;
use surrealdb::types::Bytes;

use crate::assets::remote::LocationRef;
use crate::storage::{CodeLocationDirectoryEntry, StorageBackend, retry};

use super::*;

/// Every directory row, and no other kv row. Literal bounds so the planner
/// range-scans `idx_kv_key` (pinned by `tests/directory.rs`).
pub(super) const DIRECTORY_RANGE: &str =
    "SELECT * FROM kv WHERE key >= 'code_location:' AND key < 'code_location;'";

/// Attempts before giving up on an entry that other writers keep changing.
const ATTEMPTS: usize = 16;

impl SurrealStorage {
    pub(super) async fn list_directory(
        &self,
    ) -> Result<Vec<(LocationRef, CodeLocationDirectoryEntry)>> {
        let rows: Vec<DbKv> = retry::with_retry(&self.retry_config, || async {
            Ok(self.db.query(DIRECTORY_RANGE).await?.take(0)?)
        })
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let location = row
                    .key
                    .strip_prefix(crate::CODE_LOCATION_DIRECTORY_KEY_PREFIX)
                    .and_then(|spec| LocationRef::try_from(spec.to_string()).ok());
                let entry = serde_json::from_slice(&row.value).ok();
                if location.is_none() || entry.is_none() {
                    tracing::warn!(
                        target: "rivers::storage",
                        key = %row.key,
                        "skipping a code location directory entry that does not decode"
                    );
                }
                Some((location?, entry?))
            })
            .collect())
    }

    /// Replace `location`'s entry with `update(current)`; `None` leaves it.
    /// Returns whether it wrote.
    pub(super) async fn update_directory_entry(
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
            if self
                .kv_compare_and_set(&key, current.as_deref(), &serde_json::to_vec(&next)?)
                .await?
            {
                return Ok(true);
            }
        }
        anyhow::bail!("the directory entry of {location} kept changing")
    }

    pub(super) async fn delete_directory_entry_if(
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
            if self.kv_compare_and_delete(&key, &current).await? {
                return Ok(true);
            }
        }
        anyhow::bail!("the directory entry of {location} kept changing")
    }

    /// Write `value` under `key` if the row still holds `expected` (`None`:
    /// no row). Returns whether the row now holds `value`.
    pub(super) async fn kv_compare_and_set(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let Some(expected) = expected else {
                let created: Result<()> = async {
                    self.db
                        .query("CREATE kv CONTENT { key: $key, value: $value }")
                        .bind(("key", key.to_string()))
                        .bind(("value", Bytes::from(value.to_vec())))
                        .await?
                        .check()?;
                    Ok(())
                }
                .await;
                return match created {
                    Ok(()) => Ok(true),
                    Err(error) if is_already_exists_error(&error) => Ok(false),
                    Err(error) => Err(error),
                };
            };

            // UPDATE returns no usable row count, so read the row back.
            let mut response = self
                .db
                .query(
                    "UPDATE kv SET value = $value WHERE key = $key AND value = $expected; \
                     SELECT count() AS total FROM kv \
                     WHERE key = $key AND value = $value GROUP ALL",
                )
                .bind(("key", key.to_string()))
                .bind(("expected", Bytes::from(expected.to_vec())))
                .bind(("value", Bytes::from(value.to_vec())))
                .await?
                .check()?;
            let holds: Option<u32> = response.take((1, "total"))?;
            Ok(holds.unwrap_or(0) > 0)
        })
        .await
    }

    /// Delete the row under `key` if it still holds `expected`. Returns
    /// whether the row is gone.
    pub(super) async fn kv_compare_and_delete(&self, key: &str, expected: &[u8]) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let mut response = self
                .db
                .query(
                    "DELETE kv WHERE key = $key AND value = $expected; \
                     SELECT count() AS total FROM kv WHERE key = $key GROUP ALL",
                )
                .bind(("key", key.to_string()))
                .bind(("expected", Bytes::from(expected.to_vec())))
                .await?
                .check()?;
            let left: Option<u32> = response.take((1, "total"))?;
            Ok(left.unwrap_or(0) == 0)
        })
        .await
    }
}
