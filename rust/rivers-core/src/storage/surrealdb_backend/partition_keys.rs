//! Key rows: one kv row per distinct static key list of a code location,
//! `partition_keys:<code location id>:<digest>`, written with its topology.

use std::collections::HashSet;

use anyhow::Result;
use surrealdb::engine::any::Any;
use surrealdb::method::Query;
use surrealdb::types::Bytes;

use crate::assets::partitions_fingerprint::KeyList;

use super::*;

/// The key rows of one code location: a range of the `kv.key` index, and a
/// length that leaves out the rows of a code location whose id extends this
/// one past a `:`.
pub(super) const KEY_ROWS: &str =
    "key >= $key_rows_from AND key < $key_rows_to AND string::len(key) = $key_row_len";

pub(super) const KEY_ROWS_CHANGED: &str = "partition key rows changed during the topology write";

/// Digests are 32 hex characters.
const DIGEST_LEN: usize = 32;

impl SurrealStorage {
    /// The keys of the key rows `code_location_id` has.
    pub(super) async fn stored_key_rows(&self, code_location_id: &str) -> Result<HashSet<String>> {
        let rows: Vec<String> = KeyRows::of(code_location_id)
            .bind(
                self.db
                    .query(format!("SELECT VALUE key FROM kv WHERE {KEY_ROWS}")),
            )
            .await?
            .take(0)?;
        Ok(rows.into_iter().collect())
    }

    /// Write `topology` and the key rows of `key_lists` that are not in
    /// `stored`, and delete this code location's other key rows, in one
    /// transaction. Fails with [`KEY_ROWS_CHANGED`] when a row of `stored`
    /// that `key_lists` names is gone.
    pub(super) async fn write_topology(
        &self,
        code_location_id: &str,
        topology: &Bytes,
        key_lists: &[KeyList<'_>],
        stored: &HashSet<String>,
    ) -> Result<()> {
        let mut used: Vec<String> = Vec::new();
        let mut new_rows: Vec<DbKv> = Vec::new();
        for list in key_lists {
            let key = crate::partition_keys_key(code_location_id, &list.digest);
            if used.contains(&key) {
                continue;
            }
            if !stored.contains(&key) {
                let value = Bytes::from(serde_json::to_vec(&list.keys)?);
                new_rows.push(DbKv {
                    key: key.clone(),
                    value,
                });
            }
            used.push(key);
        }

        let query = format!(
            "BEGIN TRANSACTION; \
             DELETE kv WHERE {KEY_ROWS} AND key NOTINSIDE $used; \
             INSERT INTO kv $new_rows ON DUPLICATE KEY UPDATE value = $input.value; \
             LET $stored = (SELECT VALUE key FROM kv WHERE {KEY_ROWS}); \
             IF array::len($stored) != array::len($used) {{ THROW '{KEY_ROWS_CHANGED}' }}; \
             INSERT INTO kv {{ key: $topology_key, value: $topology }} \
             ON DUPLICATE KEY UPDATE value = $input.value; \
             COMMIT TRANSACTION;"
        );
        let mut response = KeyRows::of(code_location_id)
            .bind(self.db.query(query))
            .bind(("used", used))
            .bind(("new_rows", new_rows))
            .bind(("topology_key", crate::graph_topology_key(code_location_id)))
            .bind(("topology", topology.clone()))
            .await?;
        // `check()` would report the first statement's "not executed".
        let errors = response.take_errors();
        if errors
            .values()
            .any(|error| error.message().contains(KEY_ROWS_CHANGED))
        {
            anyhow::bail!(KEY_ROWS_CHANGED);
        }
        match errors.into_iter().min_by_key(|(statement, _)| *statement) {
            Some((_, error)) => Err(error.into()),
            None => Ok(()),
        }
    }
}

struct KeyRows {
    from: String,
    to: String,
    len: usize,
}

impl KeyRows {
    fn of(code_location_id: &str) -> Self {
        let from = format!("{}{code_location_id}:", crate::PARTITION_KEYS_KEY_PREFIX);
        let to = format!("{}{code_location_id};", crate::PARTITION_KEYS_KEY_PREFIX);
        let len = from.chars().count() + DIGEST_LEN;
        Self { from, to, len }
    }

    fn bind<'r>(&self, query: Query<'r, Any>) -> Query<'r, Any> {
        query
            .bind(("key_rows_from", self.from.clone()))
            .bind(("key_rows_to", self.to.clone()))
            .bind(("key_row_len", self.len))
    }
}
