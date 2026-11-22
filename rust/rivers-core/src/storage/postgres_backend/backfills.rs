//! PostgreSQL backend: backfills. The header row, the requested keys in
//! pages, and the steps from `Requested` to a final status. The runs of a
//! backfill are in `backfills/runs.rs`.
//!
//! A step that depends on the status locks the row of the backfill first. A
//! cancel, a completion, a link and a batch of runs wait for each other, so
//! each one sees what the others wrote.

use futures_util::TryStreamExt;
use sqlx::postgres::PgRow;

use super::*;

mod runs;
#[cfg(test)]
mod tests;

/// The columns of `backfills`, named as the fields of `BackfillRecord`.
macro_rules! backfill_columns {
    () => {
        "backfill_id, code_location_id, status, strategy, failure_policy, asset_selection, \
         job_name, num_partitions, planned_runs, max_concurrency, tags, create_time, end_time, \
         error, launched_by, action, config"
    };
}

pub(super) const BACKFILL_COLUMNS: &str = backfill_columns!();

/// Largest size of one page of requested keys, in bytes of stored JSON.
const KEY_PAGE_BYTES: usize = 256 * 1024;

/// A row of [`BACKFILL_COLUMNS`] as its backfill, without the linked runs and
/// the counts: [`load_backfill_progress`] reads those.
pub(super) fn backfill_from_row(row: &PgRow) -> Result<BackfillRecord> {
    Ok(BackfillRecord {
        backfill_id: row.try_get("backfill_id")?,
        code_location_id: row.try_get("code_location_id")?,
        status: parse_backfill_status(row.try_get("status")?)?,
        strategy: from_json(row.try_get("strategy")?)?,
        failure_policy: parse_failure_policy(row.try_get("failure_policy")?)?,
        asset_selection: row.try_get("asset_selection")?,
        job_name: row.try_get("job_name")?,
        num_partitions: row.try_get::<i64, _>("num_partitions")? as u64,
        planned_runs: row.try_get::<i64, _>("planned_runs")? as u64,
        run_ids: Vec::new(),
        completed_partitions: 0,
        failed_partitions: 0,
        canceled_partitions: 0,
        max_concurrency: row.try_get("max_concurrency")?,
        tags: from_json(row.try_get("tags")?)?,
        create_time: row.try_get("create_time")?,
        end_time: row.try_get("end_time")?,
        error: row.try_get("error")?,
        launched_by: from_json(row.try_get("launched_by")?)?,
        action: row.try_get("action")?,
        config: row.try_get("config")?,
    })
}

/// Fill in the linked runs and the counts of `backfills`, read from their own
/// tables.
pub(super) async fn load_backfill_progress(
    pool: &PgPool,
    mut backfills: Vec<BackfillRecord>,
) -> Result<Vec<BackfillRecord>> {
    if backfills.is_empty() {
        return Ok(backfills);
    }
    let ids: Vec<&str> = backfills.iter().map(|b| b.backfill_id.as_str()).collect();
    let links = sqlx::query(
        "SELECT backfill_id, run_id FROM backfill_runs WHERE backfill_id = ANY($1) \
         ORDER BY linked_at, run_id COLLATE \"C\"",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    let counts = sqlx::query(
        "SELECT backfill_id, outcome, sum(member_count)::bigint AS members, \
             sum(failed_members)::bigint AS failed_members \
         FROM backfill_run_outcomes WHERE backfill_id = ANY($1) GROUP BY backfill_id, outcome",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;

    let index: HashMap<String, usize> = backfills
        .iter()
        .enumerate()
        .map(|(i, b)| (b.backfill_id.clone(), i))
        .collect();
    for row in &links {
        if let Some(&i) = index.get(row.try_get::<&str, _>("backfill_id")?) {
            backfills[i].run_ids.push(row.try_get("run_id")?);
        }
    }
    for row in &counts {
        let Some(&i) = index.get(row.try_get::<&str, _>("backfill_id")?) else {
            continue;
        };
        let members = row.try_get::<i64, _>("members")? as u64;
        let failed_members = row.try_get::<i64, _>("failed_members")? as u64;
        let backfill = &mut backfills[i];
        match row.try_get::<&str, _>("outcome")? {
            "Completed" => {
                backfill.completed_partitions += members.saturating_sub(failed_members);
                backfill.failed_partitions += failed_members;
            }
            "Failed" => backfill.failed_partitions += members,
            "Canceled" => backfill.canceled_partitions += members,
            other => anyhow::bail!("unknown backfill outcome '{other}'"),
        }
    }
    Ok(backfills)
}

/// The text of `status` in the `status` column.
pub(super) fn backfill_status_text(status: &BackfillStatus) -> &'static str {
    match status {
        BackfillStatus::Requested => "Requested",
        BackfillStatus::InProgress => "InProgress",
        BackfillStatus::CompletedSuccess => "CompletedSuccess",
        BackfillStatus::CompletedFailed => "CompletedFailed",
        BackfillStatus::Canceled => "Canceled",
    }
}

fn parse_backfill_status(text: &str) -> Result<BackfillStatus> {
    Ok(match text {
        "Requested" => BackfillStatus::Requested,
        "InProgress" => BackfillStatus::InProgress,
        "CompletedSuccess" => BackfillStatus::CompletedSuccess,
        "CompletedFailed" => BackfillStatus::CompletedFailed,
        "Canceled" => BackfillStatus::Canceled,
        other => anyhow::bail!("unknown BackfillStatus: {other}"),
    })
}

fn parse_failure_policy(text: &str) -> Result<BackfillFailurePolicy> {
    Ok(match text {
        "Continue" => BackfillFailurePolicy::Continue,
        "StopOnFailure" => BackfillFailurePolicy::StopOnFailure,
        other => anyhow::bail!("unknown BackfillFailurePolicy: {other}"),
    })
}

/// The requested keys as `jsonb` arrays of at most about [`KEY_PAGE_BYTES`],
/// in order. A key larger than a page is a page of its own. There is always
/// a page 0.
fn key_pages(keys: &[PartitionKey]) -> Result<Vec<serde_json::Value>> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    let mut page_bytes = 0;

    for key in keys {
        let key = key_json(key)?;
        let bytes = serde_json::to_vec(&key)?.len();
        if !page.is_empty() && page_bytes + bytes > KEY_PAGE_BYTES {
            pages.push(serde_json::Value::Array(std::mem::take(&mut page)));
            page_bytes = 0;
        }
        page_bytes += bytes;
        page.push(key);
    }
    pages.push(serde_json::Value::Array(page));
    Ok(pages)
}

/// The status and the planned runs of the backfill, with its row locked for
/// the rest of `tx`. `None`: no such backfill.
async fn lock_backfill(
    tx: &mut sqlx::PgConnection,
    backfill_id: &str,
) -> Result<Option<(BackfillStatus, i64)>> {
    let row =
        sqlx::query("SELECT status, planned_runs FROM backfills WHERE backfill_id = $1 FOR UPDATE")
            .bind(backfill_id)
            .fetch_optional(tx)
            .await?;
    row.map(|row| {
        Ok((
            parse_backfill_status(row.try_get("status")?)?,
            row.try_get("planned_runs")?,
        ))
    })
    .transpose()
}

/// The status of the backfill, with its row share-locked for the rest of
/// `tx`: a change of the status waits for `tx`. `None`: no such backfill.
async fn share_backfill(
    tx: &mut sqlx::PgConnection,
    backfill_id: &str,
) -> Result<Option<BackfillStatus>> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM backfills WHERE backfill_id = $1 FOR SHARE")
            .bind(backfill_id)
            .fetch_optional(tx)
            .await?;
    status.as_deref().map(parse_backfill_status).transpose()
}

/// With the row of the backfill locked by [`lock_backfill`]: set the final
/// status of an `InProgress` backfill when each of its planned runs has an
/// outcome. Returns the new status.
async fn complete_locked(
    tx: &mut sqlx::PgConnection,
    backfill_id: &str,
    status: &BackfillStatus,
    planned_runs: i64,
) -> Result<Option<BackfillStatus>> {
    if *status != BackfillStatus::InProgress || planned_runs <= 0 {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT count(*) AS runs, \
             COALESCE(bool_or(outcome = 'Failed' OR failed_members > 0), false) AS failed, \
             COALESCE(bool_or(outcome = 'Canceled'), false) AS canceled \
         FROM backfill_run_outcomes WHERE backfill_id = $1",
    )
    .bind(backfill_id)
    .fetch_one(&mut *tx)
    .await?;
    if row.try_get::<i64, _>("runs")? < planned_runs {
        return Ok(None);
    }
    let completed = if row.try_get("failed")? {
        BackfillStatus::CompletedFailed
    } else if row.try_get("canceled")? {
        BackfillStatus::Canceled
    } else {
        BackfillStatus::CompletedSuccess
    };
    sqlx::query("UPDATE backfills SET status = $2, end_time = $3 WHERE backfill_id = $1")
        .bind(backfill_id)
        .bind(backfill_status_text(&completed))
        .bind(now_nanos())
        .execute(&mut *tx)
        .await?;
    Ok(Some(completed))
}

impl PostgresStorage {
    pub(super) async fn get_backfills(
        &self,
        code_location_id: &str,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        let limit = limit.map(|limit| limit as i64);
        let query = match &status {
            Some(status) => sqlx::query(concat!(
                "SELECT ",
                backfill_columns!(),
                " FROM backfills WHERE code_location_id = $1 AND status = $3 \
                 ORDER BY create_time DESC, backfill_id COLLATE \"C\" DESC LIMIT $2"
            ))
            .bind(code_location_id)
            .bind(limit)
            .bind(backfill_status_text(status)),
            None => sqlx::query(concat!(
                "SELECT ",
                backfill_columns!(),
                " FROM backfills WHERE code_location_id = $1 \
                 ORDER BY create_time DESC, backfill_id COLLATE \"C\" DESC LIMIT $2"
            ))
            .bind(code_location_id)
            .bind(limit),
        };
        let rows = query.fetch_all(&self.pool).await?;
        let backfills = rows.iter().map(backfill_from_row).collect::<Result<_>>()?;
        load_backfill_progress(&self.pool, backfills).await
    }

    /// The header and every page of keys go in in one transaction, so a
    /// backfill that can be read has all its keys. A backfill that exists
    /// keeps its row and its keys.
    pub(super) async fn create_backfill(
        &self,
        backfill: &BackfillRecord,
        partition_keys: &[PartitionKey],
    ) -> Result<()> {
        let num_partitions = partition_keys
            .iter()
            .map(|key| key.member_count() as u64)
            .sum();
        let record = to_json(&BackfillRecord {
            num_partitions,
            ..backfill.clone()
        })?;
        let pages = key_pages(partition_keys)?;

        let mut tx = self.pool.begin().await?;
        let created = sqlx::query(concat!(
            "INSERT INTO backfills (",
            backfill_columns!(),
            ") SELECT ",
            backfill_columns!(),
            " FROM jsonb_populate_record(NULL::backfills, $1) ON CONFLICT (backfill_id) DO NOTHING"
        ))
        .bind(record)
        .execute(&mut *tx)
        .await
        .context("failed to create backfill")?
        .rows_affected()
            > 0;
        if !created {
            return Ok(());
        }
        for (page, keys) in pages.into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO backfill_key_pages (backfill_id, page, partition_keys) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&backfill.backfill_id)
            .bind(page as i32)
            .bind(keys)
            .execute(&mut *tx)
            .await
            .context("failed to store the requested keys of the backfill")?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// A row per page, so no message holds more than a page of keys.
    pub(super) async fn get_backfill_keys(&self, backfill_id: &str) -> Result<Vec<PartitionKey>> {
        let mut pages = sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT partition_keys FROM backfill_key_pages WHERE backfill_id = $1 ORDER BY page",
        )
        .bind(backfill_id)
        .fetch(&self.pool);
        let mut keys = Vec::new();
        while let Some(page) = pages.try_next().await? {
            keys.extend(from_json::<Vec<PartitionKey>>(page)?);
        }
        Ok(keys)
    }

    pub(super) async fn start_backfill(
        &self,
        backfill_id: &str,
        planned_runs: usize,
    ) -> Result<bool> {
        let started = sqlx::query(
            "UPDATE backfills SET status = 'InProgress', planned_runs = $2 \
             WHERE backfill_id = $1 AND status = 'Requested'",
        )
        .bind(backfill_id)
        .bind(planned_runs as i64)
        .execute(&self.pool)
        .await?;
        Ok(started.rows_affected() > 0)
    }

    pub(super) async fn get_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            backfill_columns!(),
            " FROM backfills WHERE backfill_id = $1"
        ))
        .bind(backfill_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(
            load_backfill_progress(&self.pool, vec![backfill_from_row(&row)?])
                .await?
                .pop(),
        )
    }

    pub(super) async fn try_complete_backfill(
        &self,
        backfill_id: &str,
    ) -> Result<Option<BackfillStatus>> {
        let mut tx = self.pool.begin().await?;
        let Some((status, planned_runs)) = lock_backfill(&mut tx, backfill_id).await? else {
            return Ok(None);
        };
        let completed = complete_locked(&mut tx, backfill_id, &status, planned_runs).await?;
        tx.commit().await?;
        Ok(completed)
    }

    pub(super) async fn cancel_backfill(&self, backfill_id: &str) -> Result<BackfillStatus> {
        let mut tx = self.pool.begin().await?;
        let (status, planned_runs) = lock_backfill(&mut tx, backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?;
        // Settle first: if every run already finished, the cancel prevented
        // nothing and the derived outcome wins.
        if let Some(settled) = complete_locked(&mut tx, backfill_id, &status, planned_runs).await? {
            tx.commit().await?;
            return Ok(settled);
        }
        if !matches!(
            status,
            BackfillStatus::Requested | BackfillStatus::InProgress
        ) {
            return Ok(status);
        }
        sqlx::query(
            "UPDATE backfills SET status = 'Canceled', end_time = $2 WHERE backfill_id = $1",
        )
        .bind(backfill_id)
        .bind(now_nanos())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(BackfillStatus::Canceled)
    }

    /// Flip a zero-run `InProgress` backfill back to `Requested` so the
    /// pickup loop runs it again. A backfill that gained runs or moved on is
    /// left alone. Returns whether the flip applied.
    pub async fn resume_stalled_backfill(&self, backfill_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        // The lock comes before the check of the runs: the check then sees
        // each run that a batch or a link committed while it waited.
        let Some((status, _)) = lock_backfill(&mut tx, backfill_id).await? else {
            return Ok(false);
        };
        if status != BackfillStatus::InProgress {
            return Ok(false);
        }
        let flipped = sqlx::query(
            "UPDATE backfills SET status = 'Requested' WHERE backfill_id = $1 \
             AND NOT EXISTS (SELECT 1 FROM backfill_runs WHERE backfill_id = $1) \
             AND NOT EXISTS (SELECT 1 FROM backfill_run_outcomes WHERE backfill_id = $1)",
        )
        .bind(backfill_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        tx.commit().await?;
        Ok(flipped)
    }

    /// Mark a backfill `CompletedFailed` with the submission error recorded.
    /// A backfill that already ended keeps its status.
    pub async fn fail_backfill(&self, backfill_id: &str, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE backfills SET status = 'CompletedFailed', end_time = $2, error = $3 \
             WHERE backfill_id = $1 AND status IN ('Requested', 'InProgress')",
        )
        .bind(backfill_id)
        .bind(now_nanos())
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
