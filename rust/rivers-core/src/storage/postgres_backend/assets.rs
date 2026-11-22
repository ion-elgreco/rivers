//! PostgreSQL backend: assets.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use sqlx::AssertSqlSafe;
use sqlx::postgres::PgRow;

use super::*;
use crate::assets::graph::GraphTopology;
use crate::assets::partitions_fingerprint::KeyList;

/// The columns that [`asset_from_row`] reads.
pub(super) const ASSET_COLUMNS: &str = "code_location_id, asset_key, tags, kinds, asset_group, \
     code_version, last_event_id, last_run_id, last_timestamp, last_data_version, \
     last_materialization_code_version, last_input_data_versions, pool";

pub(super) fn asset_from_row(row: &PgRow) -> Result<AssetRecord> {
    Ok(AssetRecord {
        code_location_id: row.try_get("code_location_id")?,
        asset_key: row.try_get("asset_key")?,
        tags: row.try_get("tags")?,
        kinds: row.try_get("kinds")?,
        asset_group: row.try_get("asset_group")?,
        code_version: row.try_get("code_version")?,
        last_event_id: row.try_get("last_event_id")?,
        last_run_id: row.try_get("last_run_id")?,
        last_timestamp: row.try_get("last_timestamp")?,
        last_data_version: row.try_get("last_data_version")?,
        last_materialization_code_version: row.try_get("last_materialization_code_version")?,
        last_input_data_versions: from_json(row.try_get("last_input_data_versions")?)?,
        pool: from_json(row.try_get("pool")?)?,
    })
}

/// The assets of the code location `$1` that pass `filter`, by key.
fn assets_where(filter: &str) -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "SELECT {ASSET_COLUMNS} FROM assets WHERE code_location_id = $1 {filter} \
         ORDER BY asset_key COLLATE \"C\""
    ))
}

/// Inserts the new assets whole. Of a registered asset it updates only the
/// definition: the rest is its materialization history. An unchanged
/// definition is not written again, as a code location registers all its
/// assets each time it starts.
const REGISTER_ASSETS: &str = "\
    INSERT INTO assets (code_location_id, asset_key, tags, kinds, asset_group, code_version, \
        last_event_id, last_run_id, last_timestamp, last_data_version, \
        last_materialization_code_version, last_input_data_versions, pool) \
    SELECT $1, asset_key, tags, kinds, asset_group, code_version, last_event_id, last_run_id, \
        last_timestamp, last_data_version, last_materialization_code_version, \
        last_input_data_versions, pool \
    FROM jsonb_to_recordset($2) AS r(asset_key text, tags text[], kinds text[], \
        asset_group text, code_version text, last_event_id text, last_run_id text, \
        last_timestamp bigint, last_data_version text, last_materialization_code_version text, \
        last_input_data_versions jsonb, pool jsonb) \
    ON CONFLICT (code_location_id, asset_key) DO UPDATE SET \
        tags = EXCLUDED.tags, kinds = EXCLUDED.kinds, asset_group = EXCLUDED.asset_group, \
        code_version = EXCLUDED.code_version, pool = EXCLUDED.pool \
    WHERE (assets.tags, assets.kinds, assets.asset_group, assets.code_version, assets.pool) \
        IS DISTINCT FROM \
        (EXCLUDED.tags, EXCLUDED.kinds, EXCLUDED.asset_group, EXCLUDED.code_version, EXCLUDED.pool)";

/// One record per asset key, by key, so that two registrations lock the rows
/// in the same order. A repeated key keeps the materialization fields of its
/// first record and the definition of its last, as a registration of the
/// records one by one does.
fn one_per_key(records: Vec<AssetRecord>) -> Vec<AssetRecord> {
    let mut by_key: BTreeMap<String, AssetRecord> = BTreeMap::new();
    for record in records {
        match by_key.entry(record.asset_key.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(record);
            }
            Entry::Occupied(mut slot) => {
                let first = slot.get_mut();
                first.tags = record.tags;
                first.kinds = record.kinds;
                first.asset_group = record.asset_group;
                first.code_version = record.code_version;
                first.pool = record.pool;
            }
        }
    }
    by_key.into_values().collect()
}

impl PostgresStorage {
    pub(super) async fn register_assets(
        &self,
        code_location_id: &str,
        records: Vec<AssetRecord>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let rows = to_json(&one_per_key(records))?;
        self.retry(|| async {
            sqlx::query(REGISTER_ASSETS)
                .bind(code_location_id)
                .bind(&rows)
                .execute(&self.pool)
                .await?;
            Ok(())
        })
        .await
    }

    pub(super) async fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<AssetRecord>> {
        sqlx::query(assets_where("AND asset_key = $2"))
            .bind(code_location_id)
            .bind(asset_key)
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(asset_from_row)
            .transpose()
    }

    pub(super) async fn get_asset_records(
        &self,
        code_location_id: &str,
    ) -> Result<Vec<AssetRecord>> {
        sqlx::query(assets_where(""))
            .bind(code_location_id)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(asset_from_row)
            .collect()
    }

    pub(super) async fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> Result<Vec<AssetRecord>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        self.assets_with(code_location_id, "AND asset_key = ANY($2)", keys)
            .await
    }

    pub(super) async fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> Result<Vec<AssetRecord>> {
        self.assets_with(code_location_id, "AND $2 = ANY(tags)", tag)
            .await
    }

    pub(super) async fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> Result<Vec<AssetRecord>> {
        self.assets_with(code_location_id, "AND $2 = ANY(kinds)", kind)
            .await
    }

    pub(super) async fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> Result<Vec<AssetRecord>> {
        self.assets_with(code_location_id, "AND asset_group = $2", group)
            .await
    }

    /// The assets of `code_location_id` that pass `filter` on `value`.
    async fn assets_with<'t, T>(
        &self,
        code_location_id: &str,
        filter: &str,
        value: T,
    ) -> Result<Vec<AssetRecord>>
    where
        T: sqlx::Encode<'t, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    {
        sqlx::query(assets_where(filter))
            .bind(code_location_id)
            .bind(value)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(asset_from_row)
            .collect()
    }

    pub(super) async fn add_dynamic_partitions(
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
        // Sorted, so that two adds of the same keys wait on each other
        // instead of locking each other out.
        sqlx::query(
            "INSERT INTO dynamic_partitions \
                 (code_location_id, partitions_def_name, partition_key, create_timestamp) \
             SELECT $1, $2, key, $4 FROM UNNEST($3::text[]) AS key ORDER BY key \
             ON CONFLICT DO NOTHING",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .bind(partition_keys)
        .bind(now_nanos() / 1_000_000_000)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn delete_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2 AND partition_key = $3",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .bind(partition_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT partition_key FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2 ORDER BY partition_key",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .fetch_all(&self.pool)
        .await?)
    }

    pub(super) async fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2 AND partition_key = $3)",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .bind(partition_key)
        .fetch_one(&self.pool)
        .await?)
    }

    pub(super) async fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as u64)
    }

    pub(super) async fn get_asset_deletion_timestamps(
        &self,
        code_location_id: &str,
    ) -> Result<HashMap<String, i64>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT asset_key, last_deletion_timestamp FROM assets \
             WHERE code_location_id = $1 AND last_deletion_timestamp IS NOT NULL",
        )
        .bind(code_location_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }

    pub(super) async fn get_graph_topology(
        &self,
        code_location_id: &str,
    ) -> Result<Option<GraphTopology>> {
        let topology: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT topology FROM graph_topologies WHERE code_location_id = $1")
                .bind(code_location_id)
                .fetch_optional(&self.pool)
                .await?;
        topology.map(from_json).transpose()
    }

    /// Write `topology` with the key lists it names. A stored list is not
    /// written again; the lists it no longer names go.
    pub(super) async fn set_graph_topology(
        &self,
        code_location_id: &str,
        topology: &GraphTopology,
        key_lists: &[KeyList<'_>],
    ) -> Result<()> {
        let topology = to_json(topology)?;
        let digests: Vec<&str> = key_lists.iter().map(|list| list.digest.as_str()).collect();
        let mut tx = self.pool.begin().await?;
        // The topology row first: its lock holds off the other writers of
        // this code location until commit, so the stored lists stay stored.
        sqlx::query(
            "INSERT INTO graph_topologies (code_location_id, topology) VALUES ($1, $2) \
             ON CONFLICT (code_location_id) DO UPDATE SET topology = EXCLUDED.topology",
        )
        .bind(code_location_id)
        .bind(&topology)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM partition_key_lists WHERE code_location_id = $1 AND digest <> ALL($2)",
        )
        .bind(code_location_id)
        .bind(&digests)
        .execute(&mut *tx)
        .await?;
        let stored: Vec<String> = sqlx::query_scalar(
            "SELECT digest FROM partition_key_lists WHERE code_location_id = $1",
        )
        .bind(code_location_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut written: HashSet<&str> = stored.iter().map(String::as_str).collect();
        for list in key_lists {
            if !written.insert(list.digest.as_str()) {
                continue;
            }
            sqlx::query(
                "INSERT INTO partition_key_lists (code_location_id, digest, keys) \
                 VALUES ($1, $2, $3)",
            )
            .bind(code_location_id)
            .bind(&list.digest)
            .bind(&list.keys)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn get_partition_keys(
        &self,
        code_location_id: &str,
        digest: &str,
    ) -> Result<Option<Vec<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT keys FROM partition_key_lists WHERE code_location_id = $1 AND digest = $2",
        )
        .bind(code_location_id)
        .bind(digest)
        .fetch_optional(&self.pool)
        .await?)
    }
}

#[cfg(test)]
mod tests;
