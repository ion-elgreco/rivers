//! PostgreSQL backend: the pages of the UI.
//!
//! A page reads its rows in the order of an index and skips the first
//! `offset` of them, as the `LIMIT .. START ..` of the SurrealDB backend.
//! Each query is built onto a `QueryBuilder` that the caller gives, so a
//! test can put `EXPLAIN` in front of the exact query of a page.

use sqlx::{Postgres, QueryBuilder};

use super::runs::{RUN_COLUMNS, parse_status, run_from_row, status_text};
use super::*;

mod backfills;
mod events;
#[cfg(test)]
mod tests;

/// The `LIMIT` and `OFFSET` of a page. Past `i64::MAX` is no limit.
fn push_limit(query: &mut QueryBuilder<Postgres>, offset: u64, limit: u64) {
    query
        .push(" LIMIT ")
        .push_bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .push(" OFFSET ")
        .push_bind(i64::try_from(offset).unwrap_or(i64::MAX));
}

/// The `WHERE` clause of the runs of `filter`.
fn push_run_filter(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    filter: &RunFilter,
) {
    query.push(" WHERE true");
    if let Some(cl) = code_location_id {
        query.push(" AND code_location_id = ").push_bind(cl);
    }
    match &filter.status {
        // The whole queue: the runs that wait and those dequeued but not
        // launched yet, as the runs summary counts them.
        Some(RunStatus::Queued) => {
            query.push(" AND status IN ('Queued', 'NotStarted')");
        }
        Some(status) => {
            query.push(" AND status = ").push_bind(status_text(status));
        }
        None => {}
    }
    if let Some(name) = &filter.job_name {
        query.push(" AND job_name = ").push_bind(name.as_str());
    }
    if let Some(pattern) = &filter.job_substring {
        query
            .push(" AND strpos(lower(job_name), ")
            .push_bind(pattern.to_lowercase())
            .push(") > 0");
    }
    if let Some(pattern) = &filter.asset_substring {
        query
            .push(
                " AND EXISTS (SELECT 1 FROM unnest(node_names) AS node \
                 WHERE strpos(lower(node), ",
            )
            .push_bind(pattern.to_lowercase())
            .push(") > 0)");
    }
    if let Some(pattern) = &filter.partition_substring {
        query
            .push(
                " AND EXISTS (SELECT 1 FROM jsonb_array_elements(tags) AS tag \
                 WHERE tag->>0 IN ('partition', 'partition_key') \
                 AND strpos(lower(tag->>1), ",
            )
            .push_bind(pattern.to_lowercase())
            .push(") > 0)");
    }
    match &filter.action {
        Some(Some(verb)) => {
            query.push(" AND action = ").push_bind(verb.as_str());
        }
        Some(None) => {
            query.push(" AND action IS NULL");
        }
        None => {}
    }
}

/// One page of the runs of `filter`, newest first.
fn push_runs_page(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    offset: u64,
    limit: u64,
    filter: &RunFilter,
) {
    query.push(format_args!("SELECT {RUN_COLUMNS} FROM runs"));
    push_run_filter(query, code_location_id, filter);
    query.push(" ORDER BY start_time DESC, run_id COLLATE \"C\" DESC");
    push_limit(query, offset, limit);
}

/// The number of runs of each status, and of those that started after
/// `cutoff`.
fn push_runs_summary(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    cutoff: i64,
) {
    query
        .push("SELECT status, count(*) AS runs, count(*) FILTER (WHERE start_time > ")
        .push_bind(cutoff)
        .push(") AS recent FROM runs");
    if let Some(cl) = code_location_id {
        query.push(" WHERE code_location_id = ").push_bind(cl);
    }
    query.push(" GROUP BY status");
}

/// The newest run of each of `job_names`, in the order of the names. A name
/// without a run has no row.
fn push_last_run_per_job(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    job_names: &[String],
) {
    query
        .push("SELECT requested.name AS requested_job, last_run.* FROM unnest(")
        .push_bind(job_names)
        .push(format_args!(
            "::text[]) WITH ORDINALITY AS requested(name, position) \
             CROSS JOIN LATERAL (SELECT {RUN_COLUMNS} FROM runs WHERE job_name = requested.name"
        ));
    if let Some(cl) = code_location_id {
        query.push(" AND code_location_id = ").push_bind(cl);
    }
    query.push(
        " ORDER BY start_time DESC, run_id COLLATE \"C\" DESC LIMIT 1) AS last_run \
         ORDER BY requested.position",
    );
}

impl PostgresStorage {
    pub async fn get_all_runs_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page(None, offset, limit, filter).await
    }

    pub async fn get_runs_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn runs_page(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        let mut page = QueryBuilder::new("");
        push_runs_page(&mut page, code_location_id, offset, limit, filter);
        let mut count = QueryBuilder::new("SELECT count(*) FROM runs");
        push_run_filter(&mut count, code_location_id, filter);
        let (rows, total) = tokio::try_join!(
            page.build().fetch_all(&self.pool),
            count.build_query_scalar::<i64>().fetch_one(&self.pool),
        )?;
        Ok(RunsPage {
            rows: rows.iter().map(run_from_row).collect::<Result<_>>()?,
            total: total as u64,
        })
    }

    pub async fn get_all_runs_summary(&self, cutoff_24h_ns: i64) -> Result<RunsSummary> {
        self.runs_summary(None, cutoff_24h_ns).await
    }

    pub async fn get_runs_summary(
        &self,
        code_location_id: &str,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.runs_summary(Some(code_location_id), cutoff_24h_ns)
            .await
    }

    async fn runs_summary(
        &self,
        code_location_id: Option<&str>,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        let mut query = QueryBuilder::new("");
        push_runs_summary(&mut query, code_location_id, cutoff_24h_ns);
        let rows: Vec<(String, i64, i64)> = query.build_query_as().fetch_all(&self.pool).await?;
        let mut summary = RunsSummary::default();
        for (status, runs, recent) in rows {
            let runs = runs as u64;
            summary.total += runs;
            summary.last_24h += recent as u64;
            match parse_status(&status)? {
                RunStatus::Started => summary.in_progress += runs,
                RunStatus::Queued | RunStatus::NotStarted => summary.queued += runs,
                RunStatus::Failure => summary.failure += runs,
                RunStatus::Success => summary.success += runs,
                RunStatus::Canceled => {}
            }
        }
        Ok(summary)
    }

    pub async fn get_all_last_run_per_job(
        &self,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job(None, job_names).await
    }

    pub async fn get_last_run_per_job(
        &self,
        code_location_id: &str,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job(Some(code_location_id), job_names)
            .await
    }

    async fn last_run_per_job(
        &self,
        code_location_id: Option<&str>,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        if job_names.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::new("");
        push_last_run_per_job(&mut query, code_location_id, job_names);
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| Ok((row.try_get("requested_job")?, run_from_row(row)?)))
            .collect()
    }
}
