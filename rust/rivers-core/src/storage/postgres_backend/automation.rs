//! PostgreSQL backend: schedule and sensor ticks, condition ticks and
//! evaluations, and the saved state of the condition daemon.

use sqlx::postgres::PgRow;
use surrealdb::types::RecordId;

use super::*;

impl PostgresStorage {
    pub(super) async fn get_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        limit: usize,
    ) -> Result<Vec<StoredTick>> {
        let rows = sqlx::query(
            "SELECT * FROM ticks WHERE code_location_id = $1 AND automation_name = $2 \
             ORDER BY timestamp DESC LIMIT $3",
        )
        .bind(code_location_id)
        .bind(automation_name)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(tick_from_row).collect()
    }

    pub(super) async fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let deleted = sqlx::query(
            "DELETE FROM ticks WHERE id IN ( \
                 SELECT id FROM ticks WHERE code_location_id = $1 AND automation_name = $2 \
                 ORDER BY timestamp DESC OFFSET $3)",
        )
        .bind(code_location_id)
        .bind(automation_name)
        .bind(max_ticks as i64)
        .execute(&self.pool)
        .await?;
        Ok(deleted.rows_affected() as usize)
    }

    pub(super) async fn get_condition_ticks(
        &self,
        code_location_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionTick>> {
        let rows = sqlx::query(
            "SELECT * FROM condition_ticks WHERE code_location_id = $1 \
             ORDER BY timestamp DESC LIMIT $2",
        )
        .bind(code_location_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(condition_tick_from_row).collect()
    }

    /// Drop the condition ticks past the newest `max_ticks`, and the
    /// evaluations that belong to them.
    pub(super) async fn prune_condition_history(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let dropped: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM condition_ticks WHERE code_location_id = $1 \
             ORDER BY timestamp DESC OFFSET $2",
        )
        .bind(code_location_id)
        .bind(max_ticks as i64)
        .fetch_all(&self.pool)
        .await?;
        if dropped.is_empty() {
            return Ok(0);
        }

        // By id, not by the window again: a tick written since the SELECT
        // would shift the window and drop a tick whose evaluations stay.
        sqlx::query(
            "WITH evals AS (DELETE FROM condition_evals WHERE tick_id = ANY($1)) \
             DELETE FROM condition_ticks WHERE id = ANY($2)",
        )
        .bind(record_ids("condition_ticks", &dropped))
        .bind(&dropped)
        .execute(&self.pool)
        .await?;
        Ok(dropped.len())
    }

    pub(super) async fn get_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        let rows = sqlx::query(
            "SELECT * FROM condition_evals WHERE code_location_id = $1 AND asset_key = $2 \
             ORDER BY timestamp DESC LIMIT $3",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(condition_eval_from_row).collect()
    }

    pub(super) async fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        let rows = sqlx::query(
            "SELECT * FROM condition_evals WHERE code_location_id = $1 AND tick_id = $2 \
             ORDER BY asset_key COLLATE \"C\" ASC",
        )
        .bind(code_location_id)
        .bind(tick_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(condition_eval_from_row).collect()
    }

    pub(super) async fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        let bytes = self
            .kv_get(&crate::condition_eval_state_key(code_location_id))
            .await?;
        Ok(bytes.map(|b| serde_json::from_slice(&b)).transpose()?)
    }

    pub(super) async fn set_condition_eval_state(
        &self,
        code_location_id: &str,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        let key = crate::condition_eval_state_key(code_location_id);
        self.kv_set(&key, &serde_json::to_vec(state)?).await
    }

    pub(super) async fn get_condition_pending_dispatch(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::PendingDispatch>> {
        let bytes = self
            .kv_get(&crate::condition_pending_dispatch_key(code_location_id))
            .await?;
        Ok(bytes.map(|b| serde_json::from_slice(&b)).transpose()?)
    }

    pub(super) async fn set_condition_pending_dispatch(
        &self,
        code_location_id: &str,
        pending: &crate::condition::PendingDispatch,
    ) -> Result<()> {
        let key = crate::condition_pending_dispatch_key(code_location_id);
        self.kv_set(&key, &serde_json::to_vec(pending)?).await
    }

    pub(super) async fn store_tick(&self, tick: &TickRecord) -> Result<String> {
        self.store_ticks_batch(std::slice::from_ref(tick))
            .await?
            .pop()
            .context("no tick id returned")
    }

    pub(super) async fn store_ticks_batch(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        if ticks.is_empty() {
            return Ok(vec![]);
        }
        let keys: Vec<String> = ticks.iter().map(|_| new_key()).collect();
        sqlx::query(
            "INSERT INTO ticks (id, code_location_id, automation_name, automation_type, status, \
                 timestamp, run_ids, backfill_ids, skip_reason, error, cursor) \
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], \
                 $6::bigint[], $7::jsonb[], $8::jsonb[], $9::text[], $10::text[], $11::text[])",
        )
        .bind(&keys)
        .bind(column(ticks, |t| t.code_location_id.as_str()))
        .bind(column(ticks, |t| t.automation_name.as_str()))
        .bind(column(ticks, |t| t.automation_type.as_str()))
        .bind(column(ticks, |t| t.status.as_str()))
        .bind(column(ticks, |t| t.timestamp))
        .bind(json_column(ticks, |t| &t.run_ids)?)
        .bind(json_column(ticks, |t| &t.backfill_ids)?)
        .bind(column(ticks, |t| t.skip_reason.as_deref()))
        .bind(column(ticks, |t| t.error.as_deref()))
        .bind(column(ticks, |t| t.cursor.as_deref()))
        .execute(&self.pool)
        .await
        .context("failed to store ticks")?;
        Ok(record_ids("ticks", &keys))
    }

    pub(super) async fn store_condition_tick(&self, tick: &ConditionTickRecord) -> Result<String> {
        let key = new_key();
        sqlx::query(
            "INSERT INTO condition_ticks (id, code_location_id, timestamp, total_evaluated, \
                 total_fired, eval_duration_us, run_ids, backfill_ids) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&key)
        .bind(&tick.code_location_id)
        .bind(tick.timestamp)
        .bind(i64::from(tick.total_evaluated))
        .bind(i64::from(tick.total_fired))
        .bind(tick.eval_duration_us as i64)
        .bind(to_json(&tick.run_ids)?)
        .bind(to_json(&tick.backfill_ids)?)
        .execute(&self.pool)
        .await?;
        Ok(record_id_str(&RecordId::new("condition_ticks", key)))
    }

    pub(super) async fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> Result<Vec<String>> {
        if evals.is_empty() {
            return Ok(vec![]);
        }
        let keys: Vec<String> = evals.iter().map(|_| new_key()).collect();
        sqlx::query(
            "INSERT INTO condition_evals (id, code_location_id, asset_key, tick_id, timestamp, \
                 fired, eval_duration_us, run_ids, tree_json, selection_json) \
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], \
                 $5::bigint[], $6::boolean[], $7::bigint[], $8::jsonb[], $9::bytea[], \
                 $10::bytea[])",
        )
        .bind(&keys)
        .bind(column(evals, |e| e.code_location_id.as_str()))
        .bind(column(evals, |e| e.asset_key.as_str()))
        .bind(column(evals, |e| e.tick_id.as_str()))
        .bind(column(evals, |e| e.timestamp))
        .bind(column(evals, |e| e.fired))
        .bind(column(evals, |e| e.eval_duration_us as i64))
        .bind(json_column(evals, |e| &e.run_ids)?)
        .bind(column(evals, |e| e.tree_json.as_slice()))
        .bind(column(evals, |e| e.selection_json.as_deref()))
        .execute(&self.pool)
        .await?;
        Ok(record_ids("condition_evals", &keys))
    }
}

/// The key of a new row. The storage API returns it as a record id.
fn new_key() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn record_ids(table: &str, keys: &[String]) -> Vec<String> {
    keys.iter()
        .map(|key| record_id_str(&RecordId::new(table, key.as_str())))
        .collect()
}

/// One field of each record, as an array bind for `UNNEST`.
fn column<'a, R, T>(records: &'a [R], field: impl Fn(&'a R) -> T) -> Vec<T> {
    records.iter().map(field).collect()
}

fn json_column<'a, R, T: serde::Serialize + 'a>(
    records: &'a [R],
    field: impl Fn(&'a R) -> &'a T,
) -> Result<Vec<serde_json::Value>> {
    records.iter().map(|r| to_json(field(r))).collect()
}

fn tick_from_row(row: &PgRow) -> Result<StoredTick> {
    Ok(StoredTick {
        id: RecordId::new("ticks", row.try_get::<String, _>("id")?),
        code_location_id: row.try_get("code_location_id")?,
        automation_name: row.try_get("automation_name")?,
        automation_type: row.try_get("automation_type")?,
        status: row.try_get("status")?,
        timestamp: row.try_get("timestamp")?,
        run_ids: from_json(row.try_get("run_ids")?)?,
        backfill_ids: from_json(row.try_get("backfill_ids")?)?,
        skip_reason: row.try_get("skip_reason")?,
        error: row.try_get("error")?,
        cursor: row.try_get("cursor")?,
    })
}

fn condition_tick_from_row(row: &PgRow) -> Result<StoredConditionTick> {
    Ok(StoredConditionTick {
        id: RecordId::new("condition_ticks", row.try_get::<String, _>("id")?),
        code_location_id: row.try_get("code_location_id")?,
        timestamp: row.try_get("timestamp")?,
        total_evaluated: row.try_get::<i64, _>("total_evaluated")? as u32,
        total_fired: row.try_get::<i64, _>("total_fired")? as u32,
        eval_duration_us: row.try_get::<i64, _>("eval_duration_us")? as u64,
        run_ids: from_json(row.try_get("run_ids")?)?,
        backfill_ids: from_json(row.try_get("backfill_ids")?)?,
    })
}

fn condition_eval_from_row(row: &PgRow) -> Result<StoredConditionEval> {
    Ok(StoredConditionEval {
        id: RecordId::new("condition_evals", row.try_get::<String, _>("id")?),
        code_location_id: row.try_get("code_location_id")?,
        asset_key: row.try_get("asset_key")?,
        tick_id: row.try_get("tick_id")?,
        timestamp: row.try_get("timestamp")?,
        fired: row.try_get("fired")?,
        eval_duration_us: row.try_get::<i64, _>("eval_duration_us")? as u64,
        run_ids: from_json(row.try_get("run_ids")?)?,
        tree_json: row.try_get("tree_json")?,
        selection_json: row.try_get("selection_json")?,
    })
}
