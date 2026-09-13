//! PostgreSQL storage backend.
//!
//! Remote-only: there is no embedded PostgreSQL. Local development uses the
//! embedded SurrealDB backend instead.

mod migration;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use super::migration::Capability;
use super::url::redact_password as redact;
use super::*;

/// Storage over a PostgreSQL server.
pub struct PostgresStorage {
    pool: PgPool,
    /// Kept for `label()`, with any password stripped.
    endpoint: String,
    /// Unredacted, for `subscribe_table`: a listener holds its connection open
    /// for its whole life, so it must not take one from the query pool.
    url: String,
    /// Schema this handle writes to, so a listener ignores other schemas'
    /// notifications on the same database.
    schema: String,
    retry_config: super::retry::StorageRetryConfig,
}

impl PostgresStorage {
    /// Connect, then gate on the schema's capability floors.
    pub async fn connect(url: &str, cap: Capability) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .with_context(|| format!("connecting to PostgreSQL at {}", redact(url)))?;
        migration::ensure_compatible(&pool, cap).await?;
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await?;
        Ok(Self {
            pool,
            endpoint: redact(url),
            url: url.to_string(),
            schema,
            retry_config: super::retry::StorageRetryConfig::default(),
        })
    }

    /// Backend label for logs. Never includes the password.
    pub fn label(&self) -> String {
        format!("PostgreSQL ({})", self.endpoint)
    }
}

/// Serialize a value into a `jsonb` bind. The record types already derive
/// serde, so their stored shape matches the SurrealDB backend's.
fn json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).context("serializing a storage field to jsonb")
}

/// The stored text of an enum that serialises to a bare string.
fn enum_str<T: serde::Serialize>(value: &T) -> Result<String> {
    match json(value)? {
        serde_json::Value::String(s) => Ok(s),
        other => anyhow::bail!("expected a string enum, got {other}"),
    }
}

/// A unique violation on a client-supplied id means the row already landed —
/// the write committed and the retry only saw its own result.
fn swallow_phantom_commit(result: Result<()>, op: &'static str, id: &str) -> Result<()> {
    match result {
        Err(e) if super::retry::is_postgres_unique_violation(&e) => {
            tracing::warn!(op, id, "treating unique violation as a phantom commit");
            Ok(())
        }
        other => other,
    }
}

/// Decode a `jsonb` column, treating SQL NULL as the type's default.
fn from_json<T: serde::de::DeserializeOwned + Default>(
    row: &sqlx::postgres::PgRow,
    col: &str,
) -> Result<T> {
    match row.try_get::<Option<serde_json::Value>, _>(col)? {
        Some(v) => serde_json::from_value(v).with_context(|| format!("decoding column {col}")),
        None => Ok(T::default()),
    }
}

/// Decode a `NOT NULL` `jsonb` column.
fn req_json<T: serde::de::DeserializeOwned>(row: &sqlx::postgres::PgRow, col: &str) -> Result<T> {
    let value: serde_json::Value = row.try_get(col)?;
    serde_json::from_value(value).with_context(|| format!("decoding column {col}"))
}

fn run_from_row(row: &sqlx::postgres::PgRow) -> Result<RunRecord> {
    let status: String = row.try_get("status")?;
    Ok(RunRecord {
        run_id: row.try_get("run_id")?,
        code_location_id: row.try_get("code_location_id")?,
        job_name: row.try_get("job_name")?,
        status: RunStatus::from_stored(&status)
            .with_context(|| format!("unknown RunStatus: {status}"))?,
        start_time: row.try_get("start_time")?,
        end_time: row.try_get("end_time")?,
        tags: from_json(row, "tags")?,
        node_names: row.try_get("node_names")?,
        priority: row.try_get::<i64, _>("priority")? as i32,
        partition_key: from_json(row, "partition_key")?,
        block_reason: row.try_get("block_reason")?,
        launched_by: from_json(row, "launched_by")?,
    })
}

fn asset_from_row(row: &sqlx::postgres::PgRow) -> Result<AssetRecord> {
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
        last_input_data_versions: from_json(row, "last_input_data_versions")?,
        pool: from_json(row, "pool")?,
    })
}

/// Append `extra` to `base`, keeping the first occurrence of each value.
///
/// Stands in for SurrealQL's `array::union`: jsonb arrays have no set union,
/// so the merge happens here.
fn union<T: Clone + PartialEq>(mut base: Vec<T>, extra: &[T]) -> Vec<T> {
    for item in extra {
        if !base.contains(item) {
            base.push(item.clone());
        }
    }
    base
}

fn backfill_from_row(row: &sqlx::postgres::PgRow) -> Result<BackfillRecord> {
    let status: String = row.try_get("status")?;
    let failure_policy: String = row.try_get("failure_policy")?;
    Ok(BackfillRecord {
        backfill_id: row.try_get("backfill_id")?,
        code_location_id: row.try_get("code_location_id")?,
        status: serde_json::from_value(serde_json::Value::String(status))
            .context("decoding BackfillStatus")?,
        strategy: from_json(row, "strategy")?,
        failure_policy: serde_json::from_value(serde_json::Value::String(failure_policy))
            .context("decoding BackfillFailurePolicy")?,
        asset_selection: row.try_get("asset_selection")?,
        job_name: row.try_get("job_name")?,
        partition_keys: from_json(row, "partition_keys")?,
        run_ids: from_json(row, "run_ids")?,
        completed_partitions: from_json(row, "completed_partitions")?,
        failed_partitions: from_json(row, "failed_partitions")?,
        canceled_partitions: from_json(row, "canceled_partitions")?,
        max_concurrency: row.try_get("max_concurrency")?,
        tags: from_json(row, "tags")?,
        create_time: row.try_get("create_time")?,
        end_time: row.try_get("end_time")?,
        error: row.try_get("error")?,
        launched_by: from_json(row, "launched_by")?,
    })
}

fn tick_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredTick> {
    Ok(StoredTick {
        id: row.try_get("id")?,
        code_location_id: row.try_get("code_location_id")?,
        automation_name: row.try_get("automation_name")?,
        automation_type: row.try_get("automation_type")?,
        status: row.try_get("status")?,
        timestamp: row.try_get("timestamp")?,
        run_ids: row.try_get("run_ids")?,
        backfill_ids: row.try_get("backfill_ids")?,
        skip_reason: row.try_get("skip_reason")?,
        error: row.try_get("error")?,
        cursor: row.try_get("cursor")?,
    })
}

fn condition_tick_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredConditionTick> {
    Ok(StoredConditionTick {
        id: row.try_get("id")?,
        code_location_id: row.try_get("code_location_id")?,
        timestamp: row.try_get("timestamp")?,
        total_evaluated: row.try_get::<i64, _>("total_evaluated")? as u32,
        total_fired: row.try_get::<i64, _>("total_fired")? as u32,
        eval_duration_us: row.try_get::<i64, _>("eval_duration_us")? as u64,
        run_ids: row.try_get("run_ids")?,
        backfill_ids: row.try_get("backfill_ids")?,
    })
}

fn condition_eval_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredConditionEval> {
    Ok(StoredConditionEval {
        id: row.try_get("id")?,
        code_location_id: row.try_get("code_location_id")?,
        asset_key: row.try_get("asset_key")?,
        tick_id: row.try_get("tick_id")?,
        timestamp: row.try_get("timestamp")?,
        fired: row.try_get("fired")?,
        eval_duration_us: row.try_get::<i64, _>("eval_duration_us")? as u64,
        run_ids: row.try_get("run_ids")?,
        tree_json: row.try_get("tree_json")?,
        selection_json: row.try_get("selection_json")?,
    })
}

fn log_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredLog> {
    Ok(StoredLog {
        id: row.try_get("id")?,
        code_location_id: row.try_get("code_location_id")?,
        run_id: row.try_get("run_id")?,
        step_key: row.try_get("step_key")?,
        timestamp: row.try_get("timestamp")?,
        stdout: row.try_get("stdout")?,
        stderr: row.try_get("stderr")?,
        logs: row.try_get("logs")?,
    })
}

fn coordinator_info_from_row(row: &sqlx::postgres::PgRow) -> Result<CoordinatorRunInfo> {
    Ok(CoordinatorRunInfo {
        run_id: row.try_get("run_id")?,
        code_location_id: row.try_get("code_location_id")?,
        tags: from_json(row, "tags")?,
        node_names: row.try_get("node_names")?,
        job_name: row.try_get("job_name")?,
        priority: row.try_get::<i64, _>("priority")? as i32,
        partition_key: from_json(row, "partition_key")?,
        start_time: row.try_get("start_time")?,
    })
}

fn pool_limit_from_row(row: &sqlx::postgres::PgRow) -> Result<PoolLimit> {
    Ok(PoolLimit {
        code_location_id: row.try_get("code_location_id")?,
        pool_key: row.try_get("pool_key")?,
        slot_limit: row.try_get::<i64, _>("slot_limit")? as i32,
        lease_duration_secs: row.try_get::<i64, _>("lease_duration_secs")? as u32,
    })
}

fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredEvent> {
    let type_name: String = row.try_get("event_type")?;
    let data_version: Option<String> = row.try_get("data_version")?;
    Ok(StoredEvent {
        id: row.try_get("id")?,
        event_type: EventType::from_type_name(&type_name, data_version)
            .map_err(anyhow::Error::msg)?,
        asset_key: row.try_get("asset_key")?,
        run_id: row.try_get("run_id")?,
        partition_key: from_json(row, "partition_key")?,
        timestamp: row.try_get("timestamp")?,
        metadata: from_json(row, "metadata")?,
        code_version: row.try_get("code_version")?,
        input_data_versions: from_json(row, "input_data_versions")?,
    })
}

impl PostgresStorage {
    /// Run a write under the backend's retry policy.
    async fn retry<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        super::retry::with_retry_if(&self.retry_config, super::retry::postgres_should_retry, f)
            .await
    }
}

impl PostgresStorage {
    async fn kv_get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.kv_get(key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn kv_set_json<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.kv_set(key, &serde_json::to_vec(value)?).await
    }
}

impl PostgresStorage {
    /// Push the `WHERE` body shared by the runs page and its row count.
    ///
    /// Substring filters are case-insensitive, so each pattern arrives
    /// lowercased and `strpos` compares against `lower(...)`.
    fn push_runs_where(
        q: &mut sqlx::QueryBuilder<sqlx::Postgres>,
        code_location_id: Option<&str>,
        filter: &RunFilter,
    ) {
        let mut first = true;
        let mut sep = |q: &mut sqlx::QueryBuilder<sqlx::Postgres>| {
            q.push(if std::mem::take(&mut first) {
                " WHERE "
            } else {
                " AND "
            });
        };
        if let Some(cl) = code_location_id {
            sep(q);
            q.push("code_location_id = ").push_bind(cl.to_string());
        }
        // "Queued" is the whole queue system: waiting (Queued) plus
        // dequeued-but-launching (NotStarted) — the same bucket the runs
        // summary counts.
        match &filter.status {
            Some(RunStatus::Queued) => {
                sep(q);
                q.push("status IN ('Queued', 'NotStarted')");
            }
            Some(s) => {
                sep(q);
                q.push("status = ").push_bind(s.as_str());
            }
            None => {}
        }
        if let Some(name) = &filter.job_name {
            sep(q);
            q.push("job_name = ").push_bind(name.clone());
        }
        if let Some(pat) = &filter.job_substring {
            sep(q);
            q.push("job_name IS NOT NULL AND strpos(lower(job_name), ")
                .push_bind(pat.to_lowercase())
                .push(") > 0");
        }
        if let Some(pat) = &filter.asset_substring {
            sep(q);
            q.push("EXISTS (SELECT 1 FROM unnest(node_names) n WHERE strpos(lower(n), ")
                .push_bind(pat.to_lowercase())
                .push(") > 0)");
        }
        if let Some(pat) = &filter.partition_substring {
            sep(q);
            q.push(
                "EXISTS (SELECT 1 FROM jsonb_array_elements(tags) t \
                 WHERE (t->>0) IN ('partition', 'partition_key') \
                 AND strpos(lower(t->>1), ",
            )
            .push_bind(pat.to_lowercase())
            .push(") > 0)");
        }
    }

    async fn runs_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.retry(|| async {
            let mut rows_q = sqlx::QueryBuilder::new("SELECT * FROM runs");
            Self::push_runs_where(&mut rows_q, code_location_id, filter);
            rows_q.push(" ORDER BY start_time DESC LIMIT ");
            rows_q.push_bind(limit as i64);
            rows_q.push(" OFFSET ").push_bind(offset as i64);

            let mut count_q = sqlx::QueryBuilder::new("SELECT count(*)::bigint FROM runs");
            Self::push_runs_where(&mut count_q, code_location_id, filter);

            let rows = rows_q.build().fetch_all(&self.pool).await?;
            let total: i64 = count_q
                .build()
                .fetch_one(&self.pool)
                .await?
                .try_get::<i64, _>(0)?;
            Ok(RunsPage {
                rows: rows.iter().map(run_from_row).collect::<Result<Vec<_>>>()?,
                total: total as u64,
            })
        })
        .await
    }

    async fn runs_summary_impl(
        &self,
        code_location_id: Option<&str>,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.retry(|| async {
            let mut q = sqlx::QueryBuilder::new(
                "SELECT count(*)::bigint AS total, \
                 count(*) FILTER (WHERE status = 'Started')::bigint AS in_progress, \
                 count(*) FILTER (WHERE status IN ('Queued', 'NotStarted'))::bigint AS queued, \
                 count(*) FILTER (WHERE status = 'Failure')::bigint AS failure, \
                 count(*) FILTER (WHERE status = 'Success')::bigint AS success, \
                 count(*) FILTER (WHERE start_time > ",
            );
            q.push_bind(cutoff_24h_ns);
            q.push(")::bigint AS last_24h FROM runs");
            if let Some(cl) = code_location_id {
                q.push(" WHERE code_location_id = ")
                    .push_bind(cl.to_string());
            }
            let row = q.build().fetch_one(&self.pool).await?;
            Ok(RunsSummary {
                total: row.try_get::<i64, _>("total")? as u64,
                in_progress: row.try_get::<i64, _>("in_progress")? as u64,
                queued: row.try_get::<i64, _>("queued")? as u64,
                failure: row.try_get::<i64, _>("failure")? as u64,
                success: row.try_get::<i64, _>("success")? as u64,
                last_24h: row.try_get::<i64, _>("last_24h")? as u64,
            })
        })
        .await
    }

    async fn backfills_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.retry(|| async {
            let push_where = |q: &mut sqlx::QueryBuilder<sqlx::Postgres>| -> Result<()> {
                let mut first = true;
                if let Some(cl) = code_location_id {
                    first = false;
                    q.push(" WHERE code_location_id = ")
                        .push_bind(cl.to_string());
                }
                if let Some(s) = &filter.status {
                    q.push(if first { " WHERE " } else { " AND " });
                    q.push("status = ").push_bind(enum_str(s)?);
                }
                Ok(())
            };

            let mut rows_q = sqlx::QueryBuilder::new("SELECT * FROM backfills");
            push_where(&mut rows_q)?;
            rows_q.push(" ORDER BY create_time DESC LIMIT ");
            rows_q.push_bind(limit as i64);
            rows_q.push(" OFFSET ").push_bind(offset as i64);

            let mut count_q = sqlx::QueryBuilder::new("SELECT count(*)::bigint FROM backfills");
            push_where(&mut count_q)?;

            let rows = rows_q.build().fetch_all(&self.pool).await?;
            let total: i64 = count_q
                .build()
                .fetch_one(&self.pool)
                .await?
                .try_get::<i64, _>(0)?;
            Ok(BackfillsPage {
                rows: rows
                    .iter()
                    .map(backfill_from_row)
                    .collect::<Result<Vec<_>>>()?,
                total: total as u64,
            })
        })
        .await
    }

    async fn backfills_summary_impl(
        &self,
        code_location_id: Option<&str>,
    ) -> Result<BackfillsSummary> {
        self.retry(|| async {
            let mut q = sqlx::QueryBuilder::new(
                "SELECT count(*)::bigint AS total, \
                 count(*) FILTER (WHERE status = 'InProgress')::bigint AS in_progress, \
                 count(*) FILTER (WHERE status = 'CompletedSuccess')::bigint AS completed_success, \
                 count(*) FILTER (WHERE status = 'CompletedFailed')::bigint AS completed_failed, \
                 count(*) FILTER (WHERE status = 'Canceled')::bigint AS canceled \
                 FROM backfills",
            );
            if let Some(cl) = code_location_id {
                q.push(" WHERE code_location_id = ")
                    .push_bind(cl.to_string());
            }
            let row = q.build().fetch_one(&self.pool).await?;
            Ok(BackfillsSummary {
                total: row.try_get::<i64, _>("total")? as u64,
                in_progress: row.try_get::<i64, _>("in_progress")? as u64,
                completed_success: row.try_get::<i64, _>("completed_success")? as u64,
                completed_failed: row.try_get::<i64, _>("completed_failed")? as u64,
                canceled: row.try_get::<i64, _>("canceled")? as u64,
            })
        })
        .await
    }

    async fn last_run_per_job_impl(
        &self,
        code_location_id: Option<&str>,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        if job_names.is_empty() {
            return Ok(Vec::new());
        }
        self.retry(|| async {
            // One `DISTINCT ON` pass beats the SurrealDB backend's
            // one-statement-per-job fan-out.
            let mut q = sqlx::QueryBuilder::new(
                "SELECT DISTINCT ON (job_name) * FROM runs WHERE job_name = ANY(",
            );
            q.push_bind(job_names);
            q.push(")");
            if let Some(cl) = code_location_id {
                q.push(" AND code_location_id = ").push_bind(cl.to_string());
            }
            q.push(" ORDER BY job_name, start_time DESC");
            let rows = q.build().fetch_all(&self.pool).await?;

            let mut by_job: HashMap<String, RunRecord> = HashMap::new();
            for row in &rows {
                let run = run_from_row(row)?;
                if let Some(name) = run.job_name.clone() {
                    by_job.insert(name, run);
                }
            }
            // Caller order, not query order.
            Ok(job_names
                .iter()
                .filter_map(|name| by_job.remove(name).map(|run| (name.clone(), run)))
                .collect())
        })
        .await
    }
}

impl PostgresStorage {
    /// Insert runs in one statement. Shared by `create_run` / `create_runs`.
    async fn insert_runs<'e>(
        &self,
        exec: impl sqlx::PgExecutor<'e>,
        runs: &[RunRecord],
    ) -> Result<()> {
        let mut rows = Vec::with_capacity(runs.len());
        for r in runs {
            rows.push((
                json(&r.tags)?,
                match &r.partition_key {
                    Some(pk) => Some(json(pk)?),
                    None => None,
                },
                json(&r.launched_by)?,
            ));
        }
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO runs (run_id, code_location_id, job_name, status, start_time, \
             end_time, tags, node_names, priority, partition_key, block_reason, launched_by) ",
        );
        q.push_values(
            runs.iter().zip(rows),
            |mut b, (r, (tags, pk, launched_by))| {
                b.push_bind(r.run_id.clone())
                    .push_bind(r.code_location_id.clone())
                    .push_bind(r.job_name.clone())
                    .push_bind(r.status.as_str())
                    .push_bind(r.start_time)
                    .push_bind(r.end_time)
                    .push_bind(tags)
                    .push_bind(r.node_names.clone())
                    .push_bind(i64::from(r.priority))
                    .push_bind(pk)
                    .push_bind(r.block_reason.clone())
                    .push_bind(launched_by);
            },
        );
        q.build().execute(exec).await?;
        Ok(())
    }
}

impl PostgresStorage {
    /// Insert ticks and return their ids, in the order given.
    async fn insert_ticks(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO ticks (code_location_id, automation_name, automation_type, \
             status, timestamp, run_ids, backfill_ids, skip_reason, error, cursor) ",
        );
        q.push_values(ticks, |mut b, t| {
            b.push_bind(&t.code_location_id)
                .push_bind(&t.automation_name)
                .push_bind(&t.automation_type)
                .push_bind(&t.status)
                .push_bind(t.timestamp)
                .push_bind(&t.run_ids)
                .push_bind(&t.backfill_ids)
                .push_bind(&t.skip_reason)
                .push_bind(&t.error)
                .push_bind(&t.cursor);
        });
        q.push(" RETURNING id");
        let rows = q.build().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| r.get::<String, _>("id")).collect())
    }
}

impl PostgresStorage {
    /// Insert events and return their ids, in the order given.
    ///
    /// A single `INSERT ... VALUES ... RETURNING id` returns rows in the order
    /// they were supplied, which is what lets callers zip ids back onto events.
    async fn insert_events<'e>(
        &self,
        exec: impl sqlx::PgExecutor<'e>,
        events: &[EventRecord],
    ) -> Result<Vec<String>> {
        let mut encoded = Vec::with_capacity(events.len());
        for e in events {
            encoded.push((
                match &e.partition_key {
                    Some(pk) => Some(json(pk)?),
                    None => None,
                },
                json(&e.metadata)?,
                json(&e.input_data_versions)?,
            ));
        }
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO events (code_location_id, event_type, asset_key, run_id, \
             partition_key, timestamp, sort_order, metadata, data_version, \
             code_version, input_data_versions) ",
        );
        q.push_values(
            events.iter().zip(encoded),
            |mut b, (e, (pk, metadata, idv))| {
                b.push_bind(e.code_location_id.clone())
                    .push_bind(e.event_type.type_name())
                    .push_bind(e.asset_key.clone())
                    .push_bind(e.run_id.clone())
                    .push_bind(pk)
                    .push_bind(e.timestamp)
                    .push_bind(e.event_type.sort_order())
                    .push_bind(metadata)
                    .push_bind(e.event_type.data_version().map(|s| s.to_string()))
                    .push_bind(Option::<String>::None)
                    .push_bind(idv);
            },
        );
        q.push(" RETURNING id");
        let rows = q.build().fetch_all(exec).await?;
        Ok(rows.into_iter().map(|r| r.get::<String, _>("id")).collect())
    }

    /// Roll each materialization forward onto its `assets` row.
    ///
    /// `last_materialization_code_version` reads the asset's own `code_version`
    /// in the same statement, so there is no per-asset lookup first.
    async fn roll_up_materializations(&self, events: &[EventRecord], ids: &[String]) -> Result<()> {
        use std::collections::HashMap;
        // Latest materialization wins per (code location, asset).
        let mut latest: HashMap<(&str, &str), usize> = HashMap::new();
        for (idx, e) in events.iter().enumerate() {
            if let Some(asset_key) = &e.asset_key
                && e.event_type.is_materialization()
            {
                latest.insert((e.code_location_id.as_str(), asset_key.as_str()), idx);
            }
        }
        if latest.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(latest.len());
        for (&(cl, asset_key), &idx) in &latest {
            rows.push((cl, asset_key, idx, json(&events[idx].input_data_versions)?));
        }

        let mut q = sqlx::QueryBuilder::new(
            "UPDATE assets a SET last_event_id = v.event_id, last_run_id = v.run_id, \
             last_timestamp = v.ts, last_data_version = v.data_version, \
             last_materialization_code_version = a.code_version, \
             last_input_data_versions = v.idv FROM (",
        );
        q.push_values(rows, |mut b, (cl, asset_key, idx, idv)| {
            let e = &events[idx];
            b.push_bind(cl.to_string())
                .push_bind(asset_key.to_string())
                .push_bind(ids[idx].clone())
                .push_bind(e.run_id.clone())
                .push_bind(e.timestamp)
                .push_bind(e.event_type.data_version().map(|s| s.to_string()))
                .push_bind(idv);
        });
        q.push(
            ") AS v(cl, asset_key, event_id, run_id, ts, data_version, idv) \
             WHERE a.code_location_id = v.cl AND a.asset_key = v.asset_key",
        );
        q.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Roll each observation forward onto its `assets` row.
    ///
    /// Narrower than the materialization roll-up on purpose: an observation
    /// records that the asset was looked at, so it must not touch the
    /// materialization-only columns.
    async fn roll_up_observations(&self, events: &[EventRecord], ids: &[String]) -> Result<()> {
        use std::collections::HashMap;
        // Last observation in the batch wins, matching per-event application.
        let mut latest: HashMap<(&str, &str), usize> = HashMap::new();
        for (idx, e) in events.iter().enumerate() {
            if let Some(asset_key) = &e.asset_key
                && e.event_type.is_observation()
            {
                latest.insert((e.code_location_id.as_str(), asset_key.as_str()), idx);
            }
        }
        if latest.is_empty() {
            return Ok(());
        }
        let mut q = sqlx::QueryBuilder::new(
            "UPDATE assets a SET last_event_id = v.event_id, last_timestamp = v.ts, \
             last_data_version = v.data_version FROM (",
        );
        q.push_values(latest, |mut b, ((cl, asset_key), idx)| {
            let e = &events[idx];
            b.push_bind(cl.to_string())
                .push_bind(asset_key.to_string())
                .push_bind(ids[idx].clone())
                .push_bind(e.timestamp)
                .push_bind(e.event_type.data_version().map(|s| s.to_string()));
        });
        q.push(
            ") AS v(cl, asset_key, event_id, ts, data_version) \
             WHERE a.code_location_id = v.cl AND a.asset_key = v.asset_key",
        );
        q.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Upsert the `asset_partitions` row for each partitioned materialization.
    async fn roll_up_partitions(&self, events: &[EventRecord], ids: &[String]) -> Result<()> {
        use std::collections::HashMap;
        let mut latest: HashMap<(&str, &str, String), usize> = HashMap::new();
        for (idx, e) in events.iter().enumerate() {
            if let (Some(asset_key), Some(pk)) = (&e.asset_key, &e.partition_key)
                && e.event_type.is_materialization()
            {
                let key = json(pk)?.to_string();
                latest.insert((e.code_location_id.as_str(), asset_key.as_str(), key), idx);
            }
        }
        if latest.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(latest.len());
        for (&(cl, asset_key, _), &idx) in &latest {
            let e = &events[idx];
            rows.push((
                cl.to_string(),
                asset_key.to_string(),
                json(e.partition_key.as_ref().expect("partitioned"))?,
                ids[idx].clone(),
                e.run_id.clone(),
                e.timestamp,
            ));
        }
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO asset_partitions (code_location_id, asset_key, partition_key, \
             last_event_id, last_run_id, last_timestamp) ",
        );
        q.push_values(rows, |mut b, (cl, asset_key, pk, event_id, run_id, ts)| {
            b.push_bind(cl)
                .push_bind(asset_key)
                .push_bind(pk)
                .push_bind(event_id)
                .push_bind(run_id)
                .push_bind(ts);
        });
        q.push(
            " ON CONFLICT (code_location_id, asset_key, partition_key) DO UPDATE \
             SET last_event_id = EXCLUDED.last_event_id, \
                 last_run_id = EXCLUDED.last_run_id, \
                 last_timestamp = EXCLUDED.last_timestamp",
        );
        q.build().execute(&self.pool).await?;
        Ok(())
    }
}

impl StorageBackend for PostgresStorage {
    async fn store_event(&self, event: &EventRecord) -> Result<String> {
        let ids = self.store_events(std::slice::from_ref(event)).await?;
        ids.into_iter()
            .next()
            .context("insert returned no event id")
    }

    async fn store_events(&self, events: &[EventRecord]) -> Result<Vec<String>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let ids = self.insert_events(&self.pool, events).await?;
        self.roll_up_materializations(events, &ids).await?;
        self.roll_up_partitions(events, &ids).await?;
        // After materializations: an observation in the same batch is the more
        // recent look at the asset.
        self.roll_up_observations(events, &ids).await?;
        Ok(ids)
    }

    async fn get_events_for_run(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(
            "SELECT * FROM events WHERE run_id = $1 \
             ORDER BY timestamp ASC, sort_order ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(event_from_row).collect()
    }

    async fn store_run_logs(&self, logs: &[LogRecord]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO run_logs (code_location_id, run_id, step_key, timestamp, \
             stdout, stderr, logs) ",
        );
        q.push_values(logs, |mut b, l| {
            b.push_bind(l.code_location_id.clone())
                .push_bind(l.run_id.clone())
                .push_bind(l.step_key.clone())
                .push_bind(l.timestamp)
                .push_bind(l.stdout.clone())
                .push_bind(l.stderr.clone())
                .push_bind(l.logs.clone());
        });
        q.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn get_run_logs(&self, run_id: &str) -> Result<Vec<StoredLog>> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM run_logs WHERE run_id = $1 ORDER BY timestamp ASC, id ASC",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(log_from_row).collect()
        })
        .await
    }

    async fn step_completion(
        &self,
        asset_key: &str,
        run_ids: &[String],
    ) -> Result<(bool, Vec<String>)> {
        self.retry(|| async {
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

    async fn create_run(&self, run: &RunRecord) -> Result<()> {
        let result = super::retry::with_retry_if(
            &self.retry_config,
            super::retry::postgres_should_retry,
            || async {
                self.insert_runs(&self.pool, std::slice::from_ref(run))
                    .await
            },
        )
        .await;
        swallow_phantom_commit(result, "create_run", &run.run_id)
    }

    async fn create_runs(&self, runs: &[RunRecord]) -> Result<()> {
        if runs.is_empty() {
            return Ok(());
        }
        super::retry::with_retry_if(
            &self.retry_config,
            super::retry::postgres_should_retry,
            || async { self.insert_runs(&self.pool, runs).await },
        )
        .await
    }

    async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        self.retry(|| async {
            match end_time {
                Some(end) => {
                    sqlx::query("UPDATE runs SET status = $1, end_time = $2 WHERE run_id = $3")
                        .bind(status.as_str())
                        .bind(end)
                        .bind(run_id)
                        .execute(&self.pool)
                        .await?;
                }
                None => {
                    sqlx::query("UPDATE runs SET status = $1 WHERE run_id = $2")
                        .bind(status.as_str())
                        .bind(run_id)
                        .execute(&self.pool)
                        .await?;
                }
            }
            Ok(())
        })
        .await
    }

    async fn try_start_run(&self, run_id: &str) -> Result<bool> {
        self.retry(|| async {
            let mut tx = self.pool.begin().await?;
            let started = sqlx::query_scalar::<_, String>(
                "UPDATE runs SET status = 'Started' \
                 WHERE run_id = $1 AND status <> 'Canceled' RETURNING status",
            )
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await?;
            if started.is_some() {
                tx.commit().await?;
                return Ok(true);
            }
            // No row updated: the run is either Canceled or absent.
            let exists =
                sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE run_id = $1")
                    .bind(run_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            tx.commit().await?;
            match exists {
                Some(_) => Ok(false),
                None => anyhow::bail!("run {run_id} not found"),
            }
        })
        .await
    }

    async fn update_run_block_reason(&self, run_id: &str, reason: Option<&str>) -> Result<()> {
        self.retry(|| async {
            sqlx::query("UPDATE runs SET block_reason = $1 WHERE run_id = $2")
                .bind(reason)
                .bind(run_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let row = sqlx::query("SELECT * FROM runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(run_from_row).transpose()
    }

    async fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM runs WHERE run_id = ANY($1) \
             AND ($2::text IS NULL OR status = $2) \
             ORDER BY start_time ASC, run_id ASC",
        )
        .bind(run_ids)
        .bind(status.as_ref().map(|s| s.as_str()))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM runs WHERE ($1::text IS NULL OR status = $1) \
             ORDER BY start_time DESC LIMIT $2",
        )
        .bind(status.as_ref().map(|s| s.as_str()))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM runs WHERE start_time > $1 \
             AND ($2::text IS NULL OR status = $2) ORDER BY start_time DESC",
        )
        .bind(since_timestamp)
        .bind(status.as_ref().map(|s| s.as_str()))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_all_queued_runs(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query("SELECT * FROM runs WHERE status IN ('Queued', 'NotStarted')")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn count_in_progress_runs(&self) -> Result<usize> {
        let total: i64 = sqlx::query(
            "SELECT count(*)::bigint AS total FROM runs \
             WHERE status IN ('NotStarted', 'Started')",
        )
        .fetch_one(&self.pool)
        .await?
        .get("total");
        Ok(total as usize)
    }

    async fn get_in_progress_runs(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query("SELECT * FROM runs WHERE status IN ('NotStarted', 'Started')")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_observations_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
    ) -> Result<Vec<StoredEvent>> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events WHERE event_type = 'Observation' \
                 AND code_location_id = $1 AND timestamp > $2 ORDER BY timestamp DESC",
            )
            .bind(code_location_id)
            .bind(since_timestamp)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(event_from_row).collect()
        })
        .await
    }

    async fn get_latest_observation_ts(&self, code_location_id: &str) -> Result<Option<i64>> {
        self.retry(|| async {
            let ts = sqlx::query_scalar::<_, i64>(
                "SELECT timestamp FROM events WHERE event_type = 'Observation' \
                 AND code_location_id = $1 ORDER BY timestamp DESC LIMIT 1",
            )
            .bind(code_location_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(ts)
        })
        .await
    }

    async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT value FROM kv WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("value")))
    }

    async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        // Single-statement upsert: a crash must never leave the key missing.
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

    async fn store_tick(&self, tick: &TickRecord) -> Result<String> {
        self.retry(|| async {
            Ok(self
                .insert_ticks(std::slice::from_ref(tick))
                .await?
                .remove(0))
        })
        .await
    }

    async fn store_ticks_batch(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        if ticks.is_empty() {
            return Ok(vec![]);
        }
        self.retry(|| async { self.insert_ticks(ticks).await })
            .await
    }

    async fn store_condition_tick(&self, tick: &ConditionTickRecord) -> Result<String> {
        self.retry(|| async {
            let id = sqlx::query_scalar::<_, String>(
                "INSERT INTO condition_ticks (code_location_id, timestamp, total_evaluated, \
                     total_fired, eval_duration_us, run_ids, backfill_ids) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
            )
            .bind(&tick.code_location_id)
            .bind(tick.timestamp)
            .bind(i64::from(tick.total_evaluated))
            .bind(i64::from(tick.total_fired))
            .bind(tick.eval_duration_us as i64)
            .bind(&tick.run_ids)
            .bind(&tick.backfill_ids)
            .fetch_one(&self.pool)
            .await?;
            Ok(id)
        })
        .await
    }

    async fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> Result<Vec<String>> {
        if evals.is_empty() {
            return Ok(vec![]);
        }
        self.retry(|| async {
            let mut q = sqlx::QueryBuilder::new(
                "INSERT INTO condition_evals (code_location_id, asset_key, tick_id, \
                 timestamp, fired, eval_duration_us, run_ids, tree_json, selection_json) ",
            );
            q.push_values(evals, |mut b, e| {
                b.push_bind(&e.code_location_id)
                    .push_bind(&e.asset_key)
                    .push_bind(&e.tick_id)
                    .push_bind(e.timestamp)
                    .push_bind(e.fired)
                    .push_bind(e.eval_duration_us as i64)
                    .push_bind(&e.run_ids)
                    .push_bind(e.tree_json.as_slice())
                    .push_bind(e.selection_json.as_deref());
            });
            q.push(" RETURNING id");
            let rows = q.build().fetch_all(&self.pool).await?;
            Ok(rows.into_iter().map(|r| r.get::<String, _>("id")).collect())
        })
        .await
    }

    async fn create_backfill(&self, backfill: &BackfillRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO backfills (backfill_id, code_location_id, status, job_name, \
                 strategy, failure_policy, asset_selection, partition_keys, run_ids, \
                 completed_partitions, failed_partitions, canceled_partitions, \
                 max_concurrency, tags, create_time, end_time, error, launched_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                 $15, $16, $17, $18)",
        )
        .bind(&backfill.backfill_id)
        .bind(&backfill.code_location_id)
        .bind(enum_str(&backfill.status)?)
        .bind(&backfill.job_name)
        .bind(json(&backfill.strategy)?)
        .bind(enum_str(&backfill.failure_policy)?)
        .bind(&backfill.asset_selection)
        .bind(json(&backfill.partition_keys)?)
        .bind(json(&backfill.run_ids)?)
        .bind(json(&backfill.completed_partitions)?)
        .bind(json(&backfill.failed_partitions)?)
        .bind(json(&backfill.canceled_partitions)?)
        .bind(backfill.max_concurrency)
        .bind(json(&backfill.tags)?)
        .bind(backfill.create_time)
        .bind(backfill.end_time)
        .bind(&backfill.error)
        .bind(json(&backfill.launched_by)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_backfill_status(
        &self,
        backfill_id: &str,
        status: BackfillStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        self.retry(|| async {
            match end_time {
                Some(end) => {
                    sqlx::query(
                        "UPDATE backfills SET status = $1, end_time = $2 WHERE backfill_id = $3",
                    )
                    .bind(enum_str(&status)?)
                    .bind(end)
                    .bind(backfill_id)
                    .execute(&self.pool)
                    .await?;
                }
                None => {
                    sqlx::query("UPDATE backfills SET status = $1 WHERE backfill_id = $2")
                        .bind(enum_str(&status)?)
                        .bind(backfill_id)
                        .execute(&self.pool)
                        .await?;
                }
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
        self.retry(|| async {
            // jsonb has no set union, so the merge happens in Rust. `FOR UPDATE`
            // serialises concurrent progress reports on the same backfill.
            let mut tx = self.pool.begin().await?;
            let Some(row) = sqlx::query(
                "SELECT run_ids, completed_partitions, failed_partitions, canceled_partitions \
                 FROM backfills WHERE backfill_id = $1 FOR UPDATE",
            )
            .bind(backfill_id)
            .fetch_optional(&mut *tx)
            .await?
            else {
                tx.commit().await?;
                return Ok(());
            };

            let merged_runs = union(from_json::<Vec<String>>(&row, "run_ids")?, run_ids);
            let merged_completed = union(from_json(&row, "completed_partitions")?, completed);
            let merged_failed = union(from_json(&row, "failed_partitions")?, failed);
            let merged_canceled = union(from_json(&row, "canceled_partitions")?, canceled);

            sqlx::query(
                "UPDATE backfills SET run_ids = $1, completed_partitions = $2, \
                 failed_partitions = $3, canceled_partitions = $4 WHERE backfill_id = $5",
            )
            .bind(json(&merged_runs)?)
            .bind(json(&merged_completed)?)
            .bind(json(&merged_failed)?)
            .bind(json(&merged_canceled)?)
            .bind(backfill_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    async fn get_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        self.retry(|| async {
            let row = sqlx::query("SELECT * FROM backfills WHERE backfill_id = $1")
                .bind(backfill_id)
                .fetch_optional(&self.pool)
                .await?;
            row.as_ref().map(backfill_from_row).transpose()
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

        // A run can succeed overall while individual partition members failed,
        // so the per-member failures come from the event log, not the status.
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
            let rows = self
                .retry(|| async {
                    Ok(sqlx::query(
                        "SELECT DISTINCT run_id, partition_key FROM events \
                         WHERE run_id = ANY($1) AND event_type = 'StepFailure' \
                         AND partition_key IS NOT NULL",
                    )
                    .bind(&success_run_ids)
                    .fetch_all(&self.pool)
                    .await?)
                })
                .await?;
            for row in &rows {
                let pk: Option<PartitionKey> = from_json(row, "partition_key")?;
                if let Some(pk) = pk {
                    failed_by_run
                        .entry(row.try_get("run_id")?)
                        .or_default()
                        .insert(pk);
                }
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

        self.retry(|| async {
            sqlx::query(
                "UPDATE backfills SET completed_partitions = $1, failed_partitions = $2, \
                 canceled_partitions = $3 WHERE backfill_id = $4",
            )
            .bind(json(&completed_pks)?)
            .bind(json(&failed_pks)?)
            .bind(json(&canceled_pks)?)
            .bind(backfill_id)
            .execute(&self.pool)
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

        let now = super::now_nanos();
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
        let now = super::now_nanos();
        self.retry(|| async {
            sqlx::query(
                "UPDATE backfills SET status = 'Canceled', end_time = $1 \
                 WHERE backfill_id = $2 AND status IN ('Requested', 'InProgress')",
            )
            .bind(now)
            .bind(backfill_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await?;
        Ok(self
            .get_backfill(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?
            .status)
    }

    async fn free_concurrency_slots(&self, run_id: &str, step_key: &str) -> Result<()> {
        self.retry(|| async {
            let mut tx = self.pool.begin().await?;
            sqlx::query("DELETE FROM concurrency_slots WHERE run_id = $1 AND step_key = $2")
                .bind(run_id)
                .bind(step_key)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM pending_steps WHERE run_id = $1 AND step_key = $2")
                .bind(run_id)
                .bind(step_key)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    async fn free_concurrency_slots_for_run(&self, run_id: &str) -> Result<()> {
        self.retry(|| async {
            let mut tx = self.pool.begin().await?;
            sqlx::query("DELETE FROM concurrency_slots WHERE run_id = $1")
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM pending_steps WHERE run_id = $1")
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
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
        self.retry(|| async {
            let now_ns = super::now_nanos();
            let lease_exp = now_ns + i64::from(lease_duration_secs) * 1_000_000_000;
            let done = sqlx::query(
                "UPDATE concurrency_slots SET lease_expires_at = $1, last_heartbeat = $2 \
                 WHERE run_id = $3 AND step_key = $4",
            )
            .bind(lease_exp)
            .bind(now_ns)
            .bind(run_id)
            .bind(step_key)
            .execute(&self.pool)
            .await?;
            Ok(done.rows_affected() as u32)
        })
        .await
    }

    async fn free_expired_leases(&self) -> Result<u32> {
        self.retry(|| async {
            let now_ns = super::now_nanos();
            let done = sqlx::query("DELETE FROM concurrency_slots WHERE lease_expires_at <= $1")
                .bind(now_ns)
                .execute(&self.pool)
                .await?;
            Ok(done.rows_affected() as u32)
        })
        .await
    }

    async fn cancel_queued_run(&self, run_id: &str) -> Result<bool> {
        self.retry(|| async {
            let now_ns = super::now_nanos();
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "UPDATE runs SET status = $1, end_time = $2 \
                 WHERE run_id = $3 AND status IN ('Queued', 'NotStarted')",
            )
            .bind(RunStatus::Canceled.as_str())
            .bind(now_ns)
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM pending_steps WHERE run_id = $1")
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
            let cancelled = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM runs WHERE run_id = $1 AND status = $2",
            )
            .bind(run_id)
            .bind(RunStatus::Canceled.as_str())
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(cancelled > 0)
        })
        .await
    }

    async fn delete_run(&self, run_id: &str) -> Result<bool> {
        // Check-then-delete is race-free here: terminal statuses are
        // permanent (re-execution mints a new run_id), so a run observed
        // terminal can't be picked up by the coordinator afterwards.
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
        self.retry(|| async {
            let mut tx = self.pool.begin().await?;
            for sql in [
                "DELETE FROM events WHERE run_id = $1",
                "DELETE FROM run_logs WHERE run_id = $1",
                "DELETE FROM concurrency_slots WHERE run_id = $1",
                "DELETE FROM pending_steps WHERE run_id = $1",
                "DELETE FROM runs WHERE run_id = $1",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(run_id)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query("DELETE FROM kv WHERE key = $1")
                .bind(format!("cancel:{run_id}"))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await?;
        Ok(true)
    }

    async fn get_run_progress(&self, run_id: &str) -> Result<RunProgress> {
        self.retry(|| async {
            // Distinct steps, not raw events — a retried step re-emits
            // StepStart/StepFailure per attempt and must count once.
            let counts = sqlx::query(
                "SELECT \
                     count(DISTINCT asset_key) FILTER (WHERE event_type = 'StepStart')::bigint \
                         AS started, \
                     count(DISTINCT asset_key) FILTER ( \
                         WHERE event_type = 'StepSuccess' \
                            OR (event_type = 'StepFailure' AND partition_key IS NULL) \
                     )::bigint AS terminal \
                 FROM events WHERE run_id = $1 AND asset_key IS NOT NULL",
            )
            .bind(run_id)
            .fetch_one(&self.pool)
            .await?;

            let last = sqlx::query(
                "SELECT asset_key, timestamp FROM events \
                 WHERE run_id = $1 AND (event_type = 'StepSuccess' \
                     OR (event_type = 'StepFailure' AND partition_key IS NULL)) \
                 ORDER BY timestamp DESC LIMIT 1",
            )
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;

            Ok(RunProgress {
                completed_steps: counts.try_get::<i64, _>("terminal")? as u32,
                total_steps: counts.try_get::<i64, _>("started")? as u32,
                last_step_completed_at: last
                    .as_ref()
                    .map(|r| r.try_get::<i64, _>("timestamp"))
                    .transpose()?,
                last_completed_step: last
                    .as_ref()
                    .map(|r| r.try_get::<Option<String>, _>("asset_key"))
                    .transpose()?
                    .flatten(),
            })
        })
        .await
    }

    async fn get_run_outcome(&self, run_id: &str) -> Result<Option<RunOutcome>> {
        let key = format!("run_outcome:{run_id}");
        match self.kv_get(&key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
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
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events WHERE run_id = $1 AND asset_key = $2 \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(run_id)
            .bind(step_key)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(event_from_row).collect()
        })
        .await
    }

    async fn get_step_terminal_events(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> Result<Vec<StoredEvent>> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events WHERE run_id = $1 AND asset_key = $2 \
                 AND event_type IN ('StepSuccess', 'StepFailure') \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(run_id)
            .bind(step_key)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(event_from_row).collect()
        })
        .await
    }

    async fn get_completed_step_keys(&self, run_id: &str) -> Result<HashSet<String>> {
        self.retry(|| async {
            let keys = sqlx::query_scalar::<_, String>(
                "SELECT DISTINCT asset_key FROM events \
                 WHERE run_id = $1 AND event_type = 'StepSuccess' AND asset_key IS NOT NULL",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(keys.into_iter().collect())
        })
        .await
    }

    async fn get_step_data_versions(&self, run_id: &str) -> Result<HashMap<String, String>> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT asset_key, data_version FROM events \
                 WHERE run_id = $1 AND event_type = 'Materialization' \
                 AND asset_key IS NOT NULL AND data_version IS NOT NULL",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
            let mut out = HashMap::new();
            for row in &rows {
                out.insert(row.try_get("asset_key")?, row.try_get("data_version")?);
            }
            Ok(out)
        })
        .await
    }
}

impl PerCodeLocationStorage for PostgresStorage {
    async fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(
            "SELECT * FROM events WHERE code_location_id = $1 AND asset_key = $2 \
             ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $3",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(event_from_row).collect()
    }

    async fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let Some(display) = partition else {
            let row = sqlx::query(
                "SELECT * FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND event_type = 'Materialization' ORDER BY timestamp DESC LIMIT 1",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .fetch_optional(&self.pool)
            .await?;
            return row.as_ref().map(event_from_row).transpose();
        };
        // Older rows may hold a display string that parses several ways. Try the
        // most specific shape first; `display_candidates` orders them loosest-first.
        for cand in PartitionKey::display_candidates(display).into_iter().rev() {
            let row = sqlx::query(
                "SELECT * FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND partition_key = $3 AND event_type = 'Materialization' \
                 ORDER BY timestamp DESC LIMIT 1",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .bind(json(&cand)?)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(row) = row {
                return Ok(Some(event_from_row(&row)?));
            }
        }
        Ok(None)
    }

    async fn register_assets(&self, code_location_id: &str, records: &[AssetRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut pools = Vec::with_capacity(records.len());
        for r in records {
            pools.push(json(&r.pool)?);
        }
        // One statement for the batch. Only the definition columns update: the
        // last_* columns are materialization state, which re-registering an
        // asset must not clear.
        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO assets (code_location_id, asset_key, tags, kinds, \
             asset_group, code_version, pool) ",
        );
        q.push_values(records.iter().zip(pools), |mut b, (r, pool)| {
            b.push_bind(code_location_id.to_string())
                .push_bind(r.asset_key.clone())
                .push_bind(r.tags.clone())
                .push_bind(r.kinds.clone())
                .push_bind(r.asset_group.clone())
                .push_bind(r.code_version.clone())
                .push_bind(pool);
        });
        q.push(
            " ON CONFLICT (code_location_id, asset_key) DO UPDATE \
             SET tags = EXCLUDED.tags, kinds = EXCLUDED.kinds, \
                 asset_group = EXCLUDED.asset_group, \
                 code_version = EXCLUDED.code_version, pool = EXCLUDED.pool",
        );
        q.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<AssetRecord>> {
        let row =
            sqlx::query("SELECT * FROM assets WHERE code_location_id = $1 AND asset_key = $2")
                .bind(code_location_id)
                .bind(asset_key)
                .fetch_optional(&self.pool)
                .await?;
        row.as_ref().map(asset_from_row).transpose()
    }

    async fn get_asset_records(&self, code_location_id: &str) -> Result<Vec<AssetRecord>> {
        let rows = sqlx::query("SELECT * FROM assets WHERE code_location_id = $1")
            .bind(code_location_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> Result<Vec<AssetRecord>> {
        let rows =
            sqlx::query("SELECT * FROM assets WHERE code_location_id = $1 AND asset_key = ANY($2)")
                .bind(code_location_id)
                .bind(keys)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> Result<Vec<AssetRecord>> {
        let rows =
            sqlx::query("SELECT * FROM assets WHERE code_location_id = $1 AND $2 = ANY(tags)")
                .bind(code_location_id)
                .bind(tag)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> Result<Vec<AssetRecord>> {
        let rows =
            sqlx::query("SELECT * FROM assets WHERE code_location_id = $1 AND $2 = ANY(kinds)")
                .bind(code_location_id)
                .bind(kind)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> Result<Vec<AssetRecord>> {
        let rows =
            sqlx::query("SELECT * FROM assets WHERE code_location_id = $1 AND asset_group = $2")
                .bind(code_location_id)
                .bind(group)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(asset_from_row).collect()
    }

    async fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE runs SET block_reason = $1 WHERE status = $2 AND code_location_id = $3",
        )
        .bind(reason)
        .bind(status.as_str())
        .bind(code_location_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        let now_ns = super::now_nanos();
        let mut tx = self.pool.begin().await?;
        let expired = sqlx::query("DELETE FROM concurrency_slots WHERE lease_expires_at <= $1")
            .bind(now_ns)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let cols = "run_id, code_location_id, tags, node_names, job_name, priority, \
                    partition_key, start_time";
        let in_progress = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {cols} FROM runs \
             WHERE status IN ('NotStarted', 'Started') AND code_location_id = $1"
        )))
        .bind(code_location_id)
        .fetch_all(&mut *tx)
        .await?;
        let queued = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {cols} FROM runs WHERE status = 'Queued' AND code_location_id = $1"
        )))
        .bind(code_location_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok((
            expired as u32,
            in_progress
                .iter()
                .map(coordinator_info_from_row)
                .collect::<Result<Vec<_>>>()?,
            queued
                .iter()
                .map(coordinator_info_from_row)
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    async fn get_stalled_not_started_runs(
        &self,
        code_location_id: &str,
        cutoff_ns: i64,
    ) -> Result<Vec<String>> {
        self.retry(|| async {
            let candidates = sqlx::query(
                "SELECT run_id, start_time FROM runs \
                 WHERE code_location_id = $1 AND status = 'NotStarted'",
            )
            .bind(code_location_id)
            .fetch_all(&self.pool)
            .await?;
            if candidates.is_empty() {
                return Ok(vec![]);
            }
            let ids: Vec<String> = candidates
                .iter()
                .map(|r| r.try_get("run_id"))
                .collect::<std::result::Result<_, _>>()?;

            // A run that was dequeued restarts its stall clock from that event,
            // not from when it was first created.
            let dequeues = sqlx::query(
                "SELECT run_id, max(timestamp) AS ts FROM events \
                 WHERE event_type = 'RunDequeued' AND run_id = ANY($1) GROUP BY run_id",
            )
            .bind(&ids)
            .fetch_all(&self.pool)
            .await?;
            let mut dequeue_ts: HashMap<String, i64> = HashMap::new();
            for row in &dequeues {
                dequeue_ts.insert(row.try_get("run_id")?, row.try_get("ts")?);
            }

            let mut out = Vec::new();
            for row in &candidates {
                let run_id: String = row.try_get("run_id")?;
                let start_time: i64 = row.try_get("start_time")?;
                if *dequeue_ts.get(&run_id).unwrap_or(&start_time) < cutoff_ns {
                    out.push(run_id);
                }
            }
            Ok(out)
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
        if partition_keys.is_empty() {
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        // Dedup first: `ON CONFLICT` cannot resolve two identical keys inside
        // one `VALUES` list.
        let mut seen = std::collections::HashSet::new();
        let unique: Vec<&String> = partition_keys.iter().filter(|k| seen.insert(*k)).collect();

        let mut q = sqlx::QueryBuilder::new(
            "INSERT INTO dynamic_partitions \
             (code_location_id, partitions_def_name, partition_key, create_timestamp) ",
        );
        q.push_values(unique, |mut b, key| {
            b.push_bind(code_location_id)
                .push_bind(partitions_def_name)
                .push_bind(key.as_str())
                .push_bind(now);
        });
        q.push(" ON CONFLICT (code_location_id, partitions_def_name, partition_key) DO NOTHING");
        q.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn delete_dynamic_partition(
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

    async fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<Vec<String>> {
        let keys = sqlx::query_scalar::<_, String>(
            "SELECT partition_key FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2 \
             ORDER BY partition_key ASC",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .fetch_all(&self.pool)
        .await?;
        Ok(keys)
    }

    async fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        let found = sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM dynamic_partitions \
             WHERE code_location_id = $1 AND partitions_def_name = $2 AND partition_key = $3",
        )
        .bind(code_location_id)
        .bind(partitions_def_name)
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(found.is_some())
    }

    async fn get_ticks(
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

    async fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let done = sqlx::query(
            "DELETE FROM ticks WHERE code_location_id = $1 AND automation_name = $2 \
             AND id NOT IN ( \
                 SELECT id FROM ticks WHERE code_location_id = $1 AND automation_name = $2 \
                 ORDER BY timestamp DESC LIMIT $3 \
             )",
        )
        .bind(code_location_id)
        .bind(automation_name)
        .bind(max_ticks as i64)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() as usize)
    }

    async fn get_condition_ticks(
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

    async fn prune_condition_ticks(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let done = sqlx::query(
            "DELETE FROM condition_ticks WHERE code_location_id = $1 AND id NOT IN ( \
                 SELECT id FROM condition_ticks WHERE code_location_id = $1 \
                 ORDER BY timestamp DESC LIMIT $2 \
             )",
        )
        .bind(code_location_id)
        .bind(max_ticks as i64)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() as usize)
    }

    async fn get_condition_evals(
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

    async fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        let rows = sqlx::query(
            "SELECT * FROM condition_evals WHERE code_location_id = $1 AND tick_id = $2 \
             ORDER BY asset_key ASC",
        )
        .bind(code_location_id)
        .bind(tick_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(condition_eval_from_row).collect()
    }

    async fn prune_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        max_evals: usize,
    ) -> Result<usize> {
        let done = sqlx::query(
            "DELETE FROM condition_evals \
             WHERE code_location_id = $1 AND asset_key = $2 AND id NOT IN ( \
                 SELECT id FROM condition_evals \
                 WHERE code_location_id = $1 AND asset_key = $2 \
                 ORDER BY timestamp DESC LIMIT $3 \
             )",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(max_evals as i64)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() as usize)
    }

    async fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        // Older rows may hold a display string that parses several ways. Try the
        // most specific shape first; `display_candidates` orders them loosest-first.
        for cand in PartitionKey::display_candidates(partition_key)
            .into_iter()
            .rev()
        {
            let rows = sqlx::query(
                "SELECT * FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND partition_key = $3 ORDER BY timestamp DESC LIMIT $4",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .bind(json(&cand)?)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
            if !rows.is_empty() {
                return rows.iter().map(event_from_row).collect();
            }
        }
        Ok(Vec::new())
    }

    async fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let rows = sqlx::query(
            "SELECT partition_key FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(|r| req_json(r, "partition_key")).collect()
    }

    async fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<u64> {
        self.retry(|| async {
            let total = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM asset_partitions \
                 WHERE code_location_id = $1 AND asset_key = $2",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .fetch_one(&self.pool)
            .await?;
            Ok(total as u64)
        })
        .await
    }

    async fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<u64> {
        self.retry(|| async {
            let total = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM dynamic_partitions \
                 WHERE code_location_id = $1 AND partitions_def_name = $2",
            )
            .bind(code_location_id)
            .bind(partitions_def_name)
            .fetch_one(&self.pool)
            .await?;
            Ok(total as u64)
        })
        .await
    }

    async fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let rows = sqlx::query(
            "SELECT partition_key, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 AND last_timestamp IS NOT NULL",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| Ok((req_json(r, "partition_key")?, r.try_get("last_timestamp")?)))
            .collect()
    }

    async fn get_partition_timestamps_since(
        &self,
        code_location_id: &str,
        asset_key: &str,
        since_timestamp: i64,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let rows = sqlx::query(
            "SELECT partition_key, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 AND last_timestamp > $3",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(since_timestamp)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| Ok((req_json(r, "partition_key")?, r.try_get("last_timestamp")?)))
            .collect()
    }

    async fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let rows = sqlx::query(
            "SELECT DISTINCT e.partition_key FROM events e \
             WHERE e.code_location_id = $1 AND e.asset_key = $2 \
             AND e.event_type = 'StepStart' AND e.partition_key IS NOT NULL \
             AND e.run_id IN ( \
                 SELECT run_id FROM runs \
                 WHERE code_location_id = $1 AND status = 'Started' AND $2 = ANY(node_names) \
             )",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(|r| req_json(r, "partition_key")).collect()
    }

    async fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> Result<HashMap<PartitionKey, i64>> {
        let mut latest_failure: std::collections::HashMap<PartitionKey, i64> =
            std::collections::HashMap::new();

        let event_rows = sqlx::query(
            "SELECT partition_key, max(timestamp) AS ts FROM events \
             WHERE code_location_id = $1 AND asset_key = $2 \
             AND event_type = 'StepFailure' AND partition_key IS NOT NULL \
             GROUP BY partition_key",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;

        // A run's whole partition fails even when no per-step event named it,
        // so failed runs count too.
        let run_rows = sqlx::query(
            "SELECT partition_key, start_time FROM runs \
             WHERE code_location_id = $1 AND status = 'Failure' \
             AND $2 = ANY(node_names) AND partition_key IS NOT NULL",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;

        for (rows, ts_col) in [(&event_rows, "ts"), (&run_rows, "start_time")] {
            for row in rows {
                let pk: PartitionKey = req_json(row, "partition_key")?;
                let ts: i64 = row.try_get(ts_col)?;
                for member in pk.members() {
                    latest_failure
                        .entry(member)
                        .and_modify(|t| *t = (*t).max(ts))
                        .or_insert(ts);
                }
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
        let mut q = sqlx::QueryBuilder::new("SELECT * FROM backfills WHERE code_location_id = ");
        q.push_bind(code_location_id);
        if let Some(s) = &status {
            q.push(" AND status = ").push_bind(enum_str(s)?);
        }
        q.push(" ORDER BY create_time DESC");
        if let Some(lim) = limit {
            q.push(" LIMIT ").push_bind(lim as i64);
        }
        let rows = q.build().fetch_all(&self.pool).await?;
        rows.iter().map(backfill_from_row).collect()
    }

    async fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO concurrency_pools \
                 (code_location_id, pool_key, slot_limit, lease_duration_secs) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (code_location_id, pool_key) DO UPDATE \
             SET slot_limit = EXCLUDED.slot_limit, \
                 lease_duration_secs = EXCLUDED.lease_duration_secs",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .bind(i64::from(limit))
        .bind(i64::from(lease_duration_secs))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_pool_limits(&self, code_location_id: &str) -> Result<Vec<PoolLimit>> {
        let rows = sqlx::query(
            "SELECT code_location_id, pool_key, slot_limit, lease_duration_secs \
             FROM concurrency_pools WHERE code_location_id = $1 ORDER BY pool_key ASC",
        )
        .bind(code_location_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(pool_limit_from_row).collect()
    }

    async fn get_pool_info(&self, code_location_id: &str, pool_key: &str) -> Result<PoolInfo> {
        let now_ns = super::now_nanos();
        let row = sqlx::query(
            "SELECT code_location_id, pool_key, slot_limit, lease_duration_secs \
             FROM concurrency_pools WHERE code_location_id = $1 AND pool_key = $2",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("pool '{pool_key}' not configured"))?;
        let limit = pool_limit_from_row(&row)?;

        let usage = sqlx::query(
            "SELECT \
                 (SELECT COALESCE(SUM(slots_consumed), 0)::bigint FROM concurrency_slots \
                  WHERE code_location_id = $1 AND pool_key = $2 AND lease_expires_at > $3) \
                 AS claimed, \
                 (SELECT count(*)::bigint FROM pending_steps \
                  WHERE code_location_id = $1 AND pool_key = $2) AS pending",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .bind(now_ns)
        .fetch_one(&self.pool)
        .await?;

        Ok(PoolInfo {
            pool_key: limit.pool_key,
            slot_limit: limit.slot_limit,
            lease_duration_secs: limit.lease_duration_secs,
            claimed_count: usage.try_get::<i64, _>("claimed")? as u32,
            pending_count: usage.try_get::<i64, _>("pending")? as u32,
        })
    }

    async fn get_all_pool_infos(&self, code_location_id: &str) -> Result<Vec<PoolInfo>> {
        let now_ns = super::now_nanos();
        let rows = sqlx::query(
            "SELECT p.code_location_id, p.pool_key, p.slot_limit, p.lease_duration_secs, \
                 COALESCE(s.claimed, 0)::bigint AS claimed, \
                 COALESCE(q.pending, 0)::bigint AS pending \
             FROM concurrency_pools p \
             LEFT JOIN ( \
                 SELECT pool_key, SUM(slots_consumed) AS claimed FROM concurrency_slots \
                 WHERE code_location_id = $1 AND lease_expires_at > $2 GROUP BY pool_key \
             ) s ON s.pool_key = p.pool_key \
             LEFT JOIN ( \
                 SELECT pool_key, count(*) AS pending FROM pending_steps \
                 WHERE code_location_id = $1 GROUP BY pool_key \
             ) q ON q.pool_key = p.pool_key \
             WHERE p.code_location_id = $1 ORDER BY p.pool_key ASC",
        )
        .bind(code_location_id)
        .bind(now_ns)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|row| {
                let limit = pool_limit_from_row(row)?;
                Ok(PoolInfo {
                    pool_key: limit.pool_key,
                    slot_limit: limit.slot_limit,
                    lease_duration_secs: limit.lease_duration_secs,
                    claimed_count: row.try_get::<i64, _>("claimed")? as u32,
                    pending_count: row.try_get::<i64, _>("pending")? as u32,
                })
            })
            .collect()
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
        let now_ns = super::now_nanos();
        let lease_exp = now_ns + i64::from(lease_duration_secs) * 1_000_000_000;

        let mut tx = self.pool.begin().await?;

        // Lock the pool rows first. Every claimer for these pools serialises
        // here, so the usage read below cannot be overtaken between check and
        // insert — the write-skew the SurrealDB backend fences with
        // `claim_version` simply cannot happen.
        let keys: Vec<String> = pools.iter().map(|(k, _)| k.clone()).collect();
        let limits = sqlx::query(
            "SELECT pool_key, slot_limit FROM concurrency_pools \
             WHERE code_location_id = $1 AND pool_key = ANY($2) \
             ORDER BY pool_key FOR UPDATE",
        )
        .bind(code_location_id)
        .bind(&keys)
        .fetch_all(&mut *tx)
        .await?;
        let limits: std::collections::HashMap<String, i64> = limits
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>("pool_key"),
                    r.get::<i64, _>("slot_limit"),
                )
            })
            .collect();

        let used = sqlx::query(
            "SELECT pool_key, COALESCE(SUM(slots_consumed), 0)::bigint AS used \
             FROM concurrency_slots \
             WHERE code_location_id = $1 AND pool_key = ANY($2) AND lease_expires_at > $3 \
             GROUP BY pool_key",
        )
        .bind(code_location_id)
        .bind(&keys)
        .bind(now_ns)
        .fetch_all(&mut *tx)
        .await?;
        let used: std::collections::HashMap<String, i64> = used
            .into_iter()
            .map(|r| (r.get::<String, _>("pool_key"), r.get::<i64, _>("used")))
            .collect();

        let mut blocked = Vec::new();
        let mut limited: Vec<(&String, u32)> = Vec::new();
        for (pool_key, needed) in pools {
            let limit = *limits
                .get(pool_key)
                .with_context(|| format!("pool '{pool_key}' not configured"))?;
            // A negative limit means unlimited.
            if limit < 0 {
                continue;
            }
            limited.push((pool_key, *needed));
            let claimed = used.get(pool_key).copied().unwrap_or(0);
            if claimed + i64::from(*needed) > limit {
                blocked.push(PoolBlockDetail {
                    pool_key: pool_key.clone(),
                    claimed: claimed as u32,
                    limit: limit as i32,
                });
            }
        }

        if limited.is_empty() {
            tx.commit().await?;
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
            sqlx::query(
                "INSERT INTO pending_steps (code_location_id, pool_key, run_id, step_key, \
                     priority, enqueued_at, block_reason) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (run_id, step_key) DO UPDATE \
                 SET code_location_id = EXCLUDED.code_location_id, \
                     pool_key = EXCLUDED.pool_key, priority = EXCLUDED.priority, \
                     enqueued_at = EXCLUDED.enqueued_at, \
                     block_reason = EXCLUDED.block_reason",
            )
            .bind(code_location_id)
            .bind(&first_pool)
            .bind(run_id)
            .bind(step_key)
            .bind(i64::from(priority))
            .bind(now_ns)
            .bind(reason.to_string())
            .execute(&mut *tx)
            .await?;

            let position: i64 = sqlx::query(
                "SELECT count(*)::bigint AS total FROM pending_steps \
                 WHERE code_location_id = $1 AND pool_key = $2",
            )
            .bind(code_location_id)
            .bind(&first_pool)
            .fetch_one(&mut *tx)
            .await?
            .get("total");
            tx.commit().await?;
            return Ok(ConcurrencyClaimStatus::Pending {
                position: position as u32,
                reason,
            });
        }

        for (pool_key, needed) in limited {
            sqlx::query(
                "INSERT INTO concurrency_slots (code_location_id, pool_key, run_id, \
                     step_key, slots_consumed, claimed_at, lease_expires_at, last_heartbeat) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $6) \
                 ON CONFLICT (code_location_id, pool_key, run_id, step_key) DO UPDATE \
                 SET slots_consumed = EXCLUDED.slots_consumed, \
                     claimed_at = EXCLUDED.claimed_at, \
                     lease_expires_at = EXCLUDED.lease_expires_at, \
                     last_heartbeat = EXCLUDED.last_heartbeat",
            )
            .bind(code_location_id)
            .bind(pool_key)
            .bind(run_id)
            .bind(step_key)
            .bind(i64::from(needed))
            .bind(now_ns)
            .bind(lease_exp)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM pending_steps WHERE run_id = $1 AND step_key = $2")
            .bind(run_id)
            .bind(step_key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(ConcurrencyClaimStatus::Claimed)
    }

    async fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<Vec<SlotHolder>> {
        let now_ns = super::now_nanos();
        let rows = sqlx::query(
            "SELECT run_id, step_key, slots_consumed, claimed_at, lease_expires_at \
             FROM concurrency_slots \
             WHERE code_location_id = $1 AND pool_key = $2 AND lease_expires_at > $3 \
             ORDER BY claimed_at ASC",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .bind(now_ns)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(SlotHolder {
                    run_id: row.try_get("run_id")?,
                    step_key: row.try_get("step_key")?,
                    slots_consumed: row.try_get::<i64, _>("slots_consumed")? as u32,
                    claimed_at: row.try_get("claimed_at")?,
                    lease_expires_at: row.try_get("lease_expires_at")?,
                })
            })
            .collect()
    }

    async fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let mut q = sqlx::QueryBuilder::new("SELECT * FROM runs WHERE code_location_id = ");
        q.push_bind(code_location_id);
        if let Some(s) = status {
            q.push(" AND status = ").push_bind(s.as_str());
        }
        q.push(" ORDER BY start_time DESC LIMIT ")
            .push_bind(limit as i64);
        let rows = q.build().fetch_all(&self.pool).await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_queued_runs(&self, code_location_id: &str) -> Result<Vec<RunRecord>> {
        let rows =
            sqlx::query("SELECT * FROM runs WHERE code_location_id = $1 AND status = 'Queued'")
                .bind(code_location_id)
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: SortOrder,
    ) -> Result<Vec<RunRecord>> {
        let mut q = sqlx::QueryBuilder::new("SELECT * FROM runs WHERE code_location_id = ");
        q.push_bind(code_location_id);
        q.push(" AND start_time > ").push_bind(since_timestamp);
        if let Some(s) = status {
            q.push(" AND status = ").push_bind(s.as_str());
        }
        q.push(format!(" ORDER BY start_time {}", order.as_sql()));
        let rows = q.build().fetch_all(&self.pool).await?;
        rows.iter().map(run_from_row).collect()
    }

    async fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        // Retry: the caller resets to fresh state on error, discarding all latches.
        let key = crate::condition_eval_state_key(code_location_id);
        self.retry(|| async { self.kv_get_json(&key).await }).await
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
impl PostgresStorage {
    pub async fn enqueue_backfill_runs(
        &self,
        records: &[RunRecord],
        backfill_id: &str,
    ) -> Result<bool> {
        if records.is_empty() {
            return Ok(true);
        }
        let events: Vec<EventRecord> = records.iter().map(run_queued_event).collect();
        let run_ids: Vec<String> = records.iter().map(|r| r.run_id.clone()).collect();
        let result = self
            .retry(|| async {
                let mut tx = self.pool.begin().await?;
                self.insert_runs(&mut *tx, records).await?;
                self.insert_events(&mut *tx, &events).await?;
                let existing =
                    sqlx::query("SELECT run_ids FROM backfills WHERE backfill_id = $1 FOR UPDATE")
                        .bind(backfill_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                if let Some(row) = existing {
                    let merged = union(from_json::<Vec<String>>(&row, "run_ids")?, &run_ids);
                    sqlx::query("UPDATE backfills SET run_ids = $1 WHERE backfill_id = $2")
                        .bind(json(&merged)?)
                        .bind(backfill_id)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
                Ok(())
            })
            .await
            .context("failed to enqueue backfill runs batch");
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

    pub async fn enqueue_run(&self, record: &RunRecord) -> Result<()> {
        let event = run_queued_event(record);
        let result = self
            .retry(|| async {
                let mut tx = self.pool.begin().await?;
                self.insert_runs(&mut *tx, std::slice::from_ref(record))
                    .await?;
                self.insert_events(&mut *tx, std::slice::from_ref(&event))
                    .await?;
                tx.commit().await?;
                Ok(())
            })
            .await
            .context("failed to enqueue run");
        swallow_phantom_commit(result, "enqueue_run", &record.run_id)
    }

    pub async fn enqueue_runs(&self, records: &[RunRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let events: Vec<EventRecord> = records.iter().map(run_queued_event).collect();
        let result = self
            .retry(|| async {
                let mut tx = self.pool.begin().await?;
                self.insert_runs(&mut *tx, records).await?;
                self.insert_events(&mut *tx, &events).await?;
                tx.commit().await?;
                Ok(())
            })
            .await
            .context("failed to enqueue runs batch");
        swallow_phantom_commit(result, "enqueue_runs", &format!("batch[{}]", records.len()))
    }

    pub async fn fail_backfill(&self, backfill_id: &str, error: &str) -> Result<()> {
        self.retry(|| async {
            sqlx::query(
                "UPDATE backfills SET status = 'CompletedFailed', end_time = $1, error = $2 \
                 WHERE backfill_id = $3",
            )
            .bind(super::now_nanos())
            .bind(error)
            .bind(backfill_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }

    pub async fn get_all_backfills_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(None, offset, limit, filter).await
    }

    pub async fn get_all_backfills_summary(&self) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(None).await
    }

    pub async fn get_all_last_run_per_job(
        &self,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(None, job_names).await
    }

    pub async fn get_all_runs_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(None, offset, limit, filter).await
    }

    pub async fn get_all_runs_summary(&self, cutoff_24h_ns: i64) -> Result<RunsSummary> {
        self.runs_summary_impl(None, cutoff_24h_ns).await
    }

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

    pub async fn get_backfills_summary(&self, code_location_id: &str) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(Some(code_location_id)).await
    }

    pub async fn get_events_for_asset_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        event_types: &[String],
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events \
                 WHERE code_location_id = $1 AND asset_key = $2 AND event_type = ANY($3) \
                 ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $4 OFFSET $5",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .bind(event_types)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?;
            let total = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM events \
                 WHERE code_location_id = $1 AND asset_key = $2 AND event_type = ANY($3)",
            )
            .bind(code_location_id)
            .bind(asset_key)
            .bind(event_types)
            .fetch_one(&self.pool)
            .await?;
            Ok((
                rows.iter()
                    .map(event_from_row)
                    .collect::<Result<Vec<_>>>()?,
                total as u64,
            ))
        })
        .await
    }

    pub async fn get_last_run_per_job(
        &self,
        code_location_id: &str,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(Some(code_location_id), job_names)
            .await
    }

    pub async fn get_run_asset_events_page(
        &self,
        run_id: &str,
        asset_key: &str,
        event_type: &str,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events \
                 WHERE run_id = $1 AND asset_key = $2 AND event_type = $3 \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $4 OFFSET $5",
            )
            .bind(run_id)
            .bind(asset_key)
            .bind(event_type)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?;
            let total = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)::bigint FROM events \
                 WHERE run_id = $1 AND asset_key = $2 AND event_type = $3",
            )
            .bind(run_id)
            .bind(asset_key)
            .bind(event_type)
            .fetch_one(&self.pool)
            .await?;
            Ok((
                rows.iter()
                    .map(event_from_row)
                    .collect::<Result<Vec<_>>>()?,
                total as u64,
            ))
        })
        .await
    }

    pub async fn get_run_step_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        self.retry(|| async {
            let rows = sqlx::query(
                "SELECT * FROM events WHERE run_id = $1 \
                 AND event_type IN ('StepStart', 'StepSuccess', 'StepFailure') \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(event_from_row).collect()
        })
        .await
    }

    pub async fn get_run_structured_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        self.retry(|| async {
            let mut rows_q = sqlx::QueryBuilder::new("SELECT * FROM events WHERE run_id = ");
            rows_q.push_bind(run_id);
            let mut count_q =
                sqlx::QueryBuilder::new("SELECT count(*)::bigint FROM events WHERE run_id = ");
            count_q.push_bind(run_id);
            if let Some(ak) = asset_key {
                rows_q.push(" AND asset_key = ").push_bind(ak);
                count_q.push(" AND asset_key = ").push_bind(ak);
            }
            rows_q.push(" ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT ");
            rows_q.push_bind(limit as i64);
            rows_q.push(" OFFSET ").push_bind(offset as i64);

            let rows = rows_q.build().fetch_all(&self.pool).await?;
            let total: i64 = count_q
                .build()
                .fetch_one(&self.pool)
                .await?
                .try_get::<i64, _>(0)?;
            Ok((
                rows.iter()
                    .map(event_from_row)
                    .collect::<Result<Vec<_>>>()?,
                total as u64,
            ))
        })
        .await
    }

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

    pub async fn get_runs_summary(
        &self,
        code_location_id: &str,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.runs_summary_impl(Some(code_location_id), cutoff_24h_ns)
            .await
    }

    pub async fn link_backfill_run(&self, backfill_id: &str, run_id: &str) -> Result<bool> {
        self.retry(|| async {
            let mut tx = self.pool.begin().await?;
            let Some(row) = sqlx::query(
                "SELECT run_ids FROM backfills \
                 WHERE backfill_id = $1 AND status = 'InProgress' FOR UPDATE",
            )
            .bind(backfill_id)
            .fetch_optional(&mut *tx)
            .await?
            else {
                tx.commit().await?;
                return Ok(false);
            };
            let merged = union(
                from_json::<Vec<String>>(&row, "run_ids")?,
                std::slice::from_ref(&run_id.to_string()),
            );
            sqlx::query("UPDATE backfills SET run_ids = $1 WHERE backfill_id = $2")
                .bind(json(&merged)?)
                .bind(backfill_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(true)
        })
        .await
    }

    pub async fn resume_stalled_backfill(&self, backfill_id: &str) -> Result<bool> {
        self.retry(|| async {
            let flipped = sqlx::query_scalar::<_, String>(
                "UPDATE backfills SET status = 'Requested' \
                 WHERE backfill_id = $1 AND status = 'InProgress' \
                 AND jsonb_array_length(run_ids) = 0 RETURNING backfill_id",
            )
            .bind(backfill_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(flipped.is_some())
        })
        .await
    }

    pub async fn subscribe_table(
        &self,
        table: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, ()>> {
        use futures_util::StreamExt;
        let mut listener = sqlx::postgres::PgListener::connect(&self.url).await?;
        listener.listen(&format!("rivers_{table}")).await?;
        let schema = self.schema.clone();
        let table_owned: std::sync::Arc<str> = std::sync::Arc::from(table);
        Ok(listener
            .into_stream()
            .filter_map(move |result| {
                let table = std::sync::Arc::clone(&table_owned);
                let schema = schema.clone();
                async move {
                    match result {
                        Ok(n) if n.payload() == schema => Some(()),
                        Ok(_) => None,
                        Err(e) => {
                            tracing::warn!(
                                target: "rivers::storage",
                                table = %table,
                                error = %e,
                                "change notification stream yielded error"
                            );
                            None
                        }
                    }
                }
            })
            .boxed())
    }
}
