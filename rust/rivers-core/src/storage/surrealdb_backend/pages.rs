use anyhow::Result;

use crate::storage::retry;
use crate::storage::{
    BackfillFilter, BackfillRecord, BackfillsPage, BackfillsSummary, RunFilter, RunRecord,
    RunStatus, RunsPage, RunsSummary, StoredEvent,
};

use super::*;

impl SurrealStorage {
    /// Paginated, filtered slice of runs plus total matching row count.
    pub async fn get_all_runs_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_runs_page`].
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

    async fn runs_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            // "Queued" is the whole queue system: waiting (Queued) plus
            // dequeued-but-launching (NotStarted) — same bucket the runs
            // summary counts.
            if filter.status == Some(RunStatus::Queued) {
                wheres.push("status IN ['Queued', 'NotStarted']");
            } else if filter.status.is_some() {
                wheres.push("status = $status");
            }
            if filter.job_name.is_some() {
                wheres.push("job_name = $job_exact");
            }
            if filter.job_substring.is_some() {
                wheres.push(
                    "job_name IS NOT NONE AND \
                     string::contains(string::lowercase(job_name), $job_pat)",
                );
            }
            if filter.asset_substring.is_some() {
                wheres.push(
                    "array::any(node_names, |$a| string::contains(string::lowercase($a), $asset_pat))",
                );
            }
            if filter.partition_substring.is_some() {
                wheres.push(
                    "array::any(tags, |$t| ($t[0] = 'partition' OR $t[0] = 'partition_key') \
                     AND string::contains(string::lowercase($t[1]), $partition_pat))",
                );
            }
            match &filter.action {
                Some(Some(_)) => wheres.push("action = $action"),
                Some(None) => wheres.push("action IS NONE"),
                None => {}
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM runs {where_clause} ORDER BY start_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM runs {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }
            if let Some(name) = &filter.job_name {
                q = q.bind(("job_exact", name.clone()));
            }
            if let Some(pat) = &filter.job_substring {
                q = q.bind(("job_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.asset_substring {
                q = q.bind(("asset_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.partition_substring {
                q = q.bind(("partition_pat", pat.to_lowercase()));
            }
            if let Some(Some(verb)) = &filter.action {
                q = q.bind(("action", verb.clone()));
            }

            let mut result = q.await?;
            let rows: Vec<RunRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(RunsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// A page of an asset's events (newest first) restricted to `event_types`, plus the total count.
    pub async fn get_events_for_asset_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        event_types: &[String],
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types \
                     ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types GROUP ALL;",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("types", event_types.to_vec()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A run's step events (`StepStart`/`Success`/`Failure`) — backs the timeline/DAG.
    pub async fn get_run_step_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE run_id = $id \
                     AND event_type IN ['StepStart', 'StepSuccess', 'StepFailure'] \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    /// A page of a run's events, optionally scoped to one asset, plus the total.
    pub async fn get_run_structured_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        retry::with_retry(&self.retry_config, || async {
            let asset_clause = if asset_key.is_some() {
                " AND asset_key = $ak"
            } else {
                ""
            };
            let sql = format!(
                "SELECT * FROM events WHERE run_id = $id{asset_clause} \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM events WHERE run_id = $id{asset_clause} GROUP ALL;"
            );
            let mut q = self
                .db
                .query(sql)
                .bind(("id", run_id.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(ak) = asset_key {
                q = q.bind(("ak", ak.to_string()));
            }
            let mut result = q.await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A page of one asset's events of a single type within a run + total.
    pub async fn get_run_asset_events_page(
        &self,
        run_id: &str,
        asset_key: &str,
        event_type: &str,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type GROUP ALL;",
                )
                .bind(("id", run_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("type", event_type.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// Aggregate run counts for the runs-list page header.
    pub async fn get_all_runs_summary(&self, cutoff_24h_ns: i64) -> Result<RunsSummary> {
        self.runs_summary_impl(None, cutoff_24h_ns).await
    }

    /// Per-CL variant of [`Self::get_all_runs_summary`].
    pub async fn get_runs_summary(
        &self,
        code_location_id: &str,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.runs_summary_impl(Some(code_location_id), cutoff_24h_ns)
            .await
    }

    async fn runs_summary_impl(
        &self,
        code_location_id: Option<&str>,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM runs {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Started' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status IN ['Queued', 'NotStarted'] GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Failure' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Success' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}start_time > $cutoff GROUP ALL;",
            );
            let mut q = self.db.query(sql).bind(("cutoff", cutoff_24h_ns));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(RunsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                queued: take(&mut result, 2)?,
                failure: take(&mut result, 3)?,
                success: take(&mut result, 4)?,
                last_24h: take(&mut result, 5)?,
            })
        })
        .await
    }

    /// For each requested job name, return the most recent run if any.
    pub async fn get_all_last_run_per_job(
        &self,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(None, job_names).await
    }

    /// Per-CL variant of [`Self::get_all_last_run_per_job`].
    pub async fn get_last_run_per_job(
        &self,
        code_location_id: &str,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(Some(code_location_id), job_names)
            .await
    }

    async fn last_run_per_job_impl(
        &self,
        code_location_id: Option<&str>,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        retry::with_retry(&self.retry_config, || async {
            use std::fmt::Write;

            if job_names.is_empty() {
                return Ok(Vec::new());
            }

            let cl_filter = if code_location_id.is_some() {
                " AND code_location_id = $cl"
            } else {
                ""
            };
            let mut sql = String::with_capacity(job_names.len() * 128);
            for i in 0..job_names.len() {
                let _ = writeln!(
                    sql,
                    "SELECT * FROM runs WHERE job_name = $job_{i}{cl_filter} \
                     ORDER BY start_time DESC LIMIT 1;"
                );
            }
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            for (i, name) in job_names.iter().enumerate() {
                q = q.bind((format!("job_{i}"), name.clone()));
            }
            let mut result = q.await?;

            let mut out = Vec::with_capacity(job_names.len());
            for (i, name) in job_names.iter().enumerate() {
                let rows: Vec<RunRecord> = result.take(i)?;
                if let Some(run) = rows.into_iter().next() {
                    out.push((name.clone(), run));
                }
            }
            Ok(out)
        })
        .await
    }

    /// Paginated + filtered backfills list. Mirrors `get_all_runs_page`.
    pub async fn get_all_backfills_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_page`].
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

    async fn backfills_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            if filter.status.is_some() {
                wheres.push("status = $status");
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM backfills {where_clause} ORDER BY create_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM backfills {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }

            let mut result = q.await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(BackfillsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// Aggregate backfill counts for the list-page status pills. Unfiltered.
    pub async fn get_all_backfills_summary(&self) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(None).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_summary`].
    pub async fn get_backfills_summary(&self, code_location_id: &str) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(Some(code_location_id)).await
    }

    async fn backfills_summary_impl(
        &self,
        code_location_id: Option<&str>,
    ) -> Result<BackfillsSummary> {
        retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM backfills {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'InProgress' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedSuccess' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedFailed' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'Canceled' GROUP ALL;",
            );
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(BackfillsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                completed_success: take(&mut result, 2)?,
                completed_failed: take(&mut result, 3)?,
                canceled: take(&mut result, 4)?,
            })
        })
        .await
    }
}
