//! PostgreSQL backend: the failure rows.
//!
//! A partition has a row in `asset_partition_failures` while its latest
//! failure is newer than its materialization, its deletion and the deletion
//! of its asset, and one row per run in `asset_partition_run_failures`. The
//! functions of `schema/partitions.sql` keep them. Each writer of failure
//! rows first locks the asset alone (`rivers_lock_assets`): no
//! materialization or deletion of the asset changes the rows it reads until
//! it commits.

use sqlx::postgres::PgRow;

use super::*;

/// The columns of a run that its failures need.
#[derive(Debug, Clone)]
pub(super) struct RunOfFailures {
    pub(super) run_id: String,
    pub(super) code_location_id: String,
    pub(super) status: String,
    pub(super) action: Option<String>,
    pub(super) partition_key: Option<PartitionKey>,
    pub(super) node_names: Vec<String>,
    pub(super) start_time: i64,
    pub(super) end_time: Option<i64>,
}

impl From<&RunRecord> for RunOfFailures {
    fn from(run: &RunRecord) -> Self {
        Self {
            run_id: run.run_id.clone(),
            code_location_id: run.code_location_id.clone(),
            status: format!("{:?}", run.status),
            action: run.action.clone(),
            partition_key: run.partition_key.clone(),
            node_names: run.node_names.clone(),
            start_time: run.start_time,
            end_time: run.end_time,
        }
    }
}

/// A change of a run that the failure rows of its partitions follow.
#[derive(Debug, Clone, Copy)]
pub(super) enum RunChange {
    /// The run is deleted.
    Deleted,
    /// The run ends in failure at this time, or not at all.
    FailedAt(Option<i64>),
}

/// The status and the times of a run, as the store holds them.
#[derive(Debug)]
pub(super) struct StoredStatus {
    pub(super) status: String,
    pub(super) start_time: i64,
    pub(super) end_time: Option<i64>,
}

/// The members that the run `run_id` materialized, per asset: of
/// `asset_key` only, when given.
async fn built_by_run<'e>(
    conn: impl sqlx::PgExecutor<'e>,
    run_id: &str,
    asset_key: Option<&str>,
) -> Result<HashMap<String, HashSet<PartitionKey>>> {
    let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT asset_key, partition_key FROM events WHERE run_id = $1 \
         AND event_type = 'Materialization' AND asset_key = COALESCE($2, asset_key) \
         AND partition_key IS NOT NULL",
    )
    .bind(run_id)
    .bind(asset_key)
    .fetch_all(conn)
    .await?;
    let mut built: HashMap<String, HashSet<PartitionKey>> = HashMap::new();
    for (asset_key, key) in rows {
        // As on SurrealDB: a key of an unknown form materialized nothing.
        if let Ok(key) = from_json::<PartitionKey>(key) {
            built.entry(asset_key).or_default().extend(key.members());
        }
    }
    Ok(built)
}

fn failure_from_row(row: &PgRow) -> Result<PartitionFailure> {
    Ok(PartitionFailure {
        key: from_json(row.try_get("partition_key")?)?,
        failed_at: row.try_get("failed_at")?,
        run_id: row.try_get("run_id")?,
        has_row: row.try_get("has_row")?,
    })
}

fn count_from_row(row: &PgRow) -> Result<FailureCount> {
    Ok(FailureCount {
        failed: row.try_get::<i64, _>("failed")? as u64,
        with_row: row.try_get::<i64, _>("with_row")? as u64,
    })
}

impl PostgresStorage {
    /// Record the failures `(partition, time, run)` of partitions of one
    /// asset; each partition is one member. A partition takes a failure that
    /// is newer than its materialization, its deletion and the deletion of
    /// the asset. `failed_run`: the failures are the end of this run in
    /// failure, which fails no partition it materialized; else keyed step
    /// failures.
    pub(super) async fn record_partition_failures(
        &self,
        cl: &str,
        asset_key: &str,
        failures: Vec<(PartitionKey, i64, &str)>,
        failed_run: Option<&str>,
    ) -> Result<()> {
        let mut latest: HashMap<(PartitionKey, &str), i64> = HashMap::new();
        for (key, at, run_id) in failures {
            let held = latest.entry((key, run_id)).or_insert(at);
            *held = (*held).max(at);
        }
        if latest.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT rivers_lock_assets(ARRAY[$1], ARRAY[$2], ARRAY[true])")
            .bind(cl)
            .bind(asset_key)
            .execute(&mut *tx)
            .await?;
        // Read after the lock: a materialization of the run that commits
        // later waits for this commit, and then takes back the failure.
        let built = match failed_run {
            Some(run_id) => built_by_run(&mut *tx, run_id, Some(asset_key))
                .await?
                .remove(asset_key)
                .unwrap_or_default(),
            None => HashSet::new(),
        };
        let (mut ords, mut keys, mut ats, mut runs) = (vec![], vec![], vec![], vec![]);
        for ((key, run_id), at) in &latest {
            if built.contains(key) {
                continue;
            }
            ords.push(partition_ord(key));
            keys.push(key_json(key)?);
            ats.push(*at);
            runs.push(*run_id);
        }
        sqlx::query("SELECT rivers_record_failures($1, $2, $3, $4, $5, $6, $7)")
            .bind(cl)
            .bind(asset_key)
            .bind(&ords)
            .bind(&keys)
            .bind(&ats)
            .bind(&runs)
            .bind(failed_run.is_some())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The run `run_id`, with the columns that its failures need.
    pub(super) async fn run_of_failures(&self, run_id: &str) -> Result<Option<RunOfFailures>> {
        let row = sqlx::query(
            "SELECT run_id, code_location_id, status, action, partition_key, node_names, \
             start_time, end_time FROM runs WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let partition_key: Option<serde_json::Value> = row.try_get("partition_key")?;
        Ok(Some(RunOfFailures {
            run_id: row.try_get("run_id")?,
            code_location_id: row.try_get("code_location_id")?,
            status: row.try_get("status")?,
            action: row.try_get("action")?,
            partition_key: partition_key.map(from_json).transpose()?,
            node_names: row.try_get("node_names")?,
            start_time: row.try_get("start_time")?,
            end_time: row.try_get("end_time")?,
        }))
    }

    /// Record the failures of `run`, which ends in failure at `at`: each
    /// member of its partition, for each asset of the run that it did not
    /// materialize. An action run and a run without a partition fail no
    /// partition.
    pub(super) async fn record_failures_of_run(&self, run: &RunOfFailures, at: i64) -> Result<()> {
        let Some(partition_key) = run.partition_key.as_ref().filter(|_| run.action.is_none())
        else {
            return Ok(());
        };
        // An asset of which the run built every member takes no lock. A
        // member the run built stays built, and `record_partition_failures`
        // reads them again under the lock.
        let built = built_by_run(&self.pool, &run.run_id, None).await?;
        let members = partition_key.members();
        for asset_key in &run.node_names {
            let done = built.get(asset_key);
            let failures: Vec<_> = members
                .iter()
                .filter(|member| done.is_none_or(|done| !done.contains(*member)))
                .map(|member| (member.clone(), at, run.run_id.as_str()))
                .collect();
            if !failures.is_empty() {
                self.record_partition_failures(
                    &run.code_location_id,
                    asset_key,
                    failures,
                    Some(&run.run_id),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Record the failures of the runs of `runs` that are created as failed.
    pub(super) async fn record_failures_of_created(&self, runs: &[RunRecord]) -> Result<()> {
        for run in runs.iter().filter(|run| run.status == RunStatus::Failure) {
            let at = run.end_time.unwrap_or(run.start_time);
            self.record_failures_of_run(&RunOfFailures::from(run), at)
                .await?;
        }
        Ok(())
    }

    /// Bring the failure rows of the partitions of the run `run_id` in line
    /// with `change`, in one transaction. A failure by the run that comes in
    /// after the change comes from an event after it.
    pub(super) async fn repair_failures_of_run(
        &self,
        run_id: &str,
        change: RunChange,
    ) -> Result<()> {
        let (deleted, end) = match change {
            RunChange::Deleted => (true, None),
            RunChange::FailedAt(end) => (false, end),
        };
        sqlx::query("SELECT rivers_repair_failures_of_run($1, $2, $3)")
            .bind(run_id)
            .bind(deleted)
            .bind(end)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// After a write of the status of the run `run_id`: bring the failure
    /// rows of its partitions in line with `stored`, the status that the
    /// store then holds, when the run is stored. A failed run fails them at
    /// its end time, else its start time; any other run not by its end. It
    /// follows the stored status, not the one before the write, so a write
    /// done again after a stop repairs too.
    pub(super) async fn repair_failures_after_status(
        &self,
        run_id: &str,
        stored: Option<&StoredStatus>,
    ) -> Result<()> {
        let Some(run) = stored else {
            return Ok(());
        };
        let end = (run.status == "Failure").then(|| run.end_time.unwrap_or(run.start_time));
        self.repair_failures_of_run(run_id, RunChange::FailedAt(end))
            .await
    }

    pub(super) async fn count_partition_failures(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<FailureCount> {
        let row = sqlx::query(
            "SELECT count(*) AS failed, count(*) FILTER (WHERE has_row) AS with_row \
             FROM asset_partition_failures WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_one(&self.pool)
        .await?;
        count_from_row(&row)
    }

    pub(super) async fn count_partition_failures_in(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<FailureCount>> {
        let (lowers, uppers): (Vec<Vec<u8>>, Vec<Vec<u8>>) = bounds.iter().map(bounds_ord).unzip();
        sqlx::query(
            "SELECT count(f.partition_ord) AS failed, \
             count(f.partition_ord) FILTER (WHERE f.has_row) AS with_row \
             FROM unnest($3::bytea[], $4::bytea[]) WITH ORDINALITY AS b(lower, upper, i) \
             LEFT JOIN asset_partition_failures f ON f.code_location_id = $1 \
             AND f.asset_key = $2 AND f.partition_ord BETWEEN b.lower AND b.upper \
             GROUP BY b.i ORDER BY b.i",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(&lowers)
        .bind(&uppers)
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(count_from_row)
        .collect()
    }

    pub(super) async fn get_partition_failures(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: Option<&PartitionBounds>,
        limit: u64,
    ) -> Result<Vec<PartitionFailure>> {
        let rows = match bounds {
            Some(bounds) => {
                let (lower, upper) = bounds_ord(bounds);
                sqlx::query(
                    "SELECT partition_key, failed_at, run_id, has_row \
                     FROM asset_partition_failures WHERE code_location_id = $1 \
                     AND asset_key = $2 AND partition_ord BETWEEN $3 AND $4 \
                     ORDER BY partition_ord LIMIT $5",
                )
                .bind(code_location_id)
                .bind(asset_key)
                .bind(lower)
                .bind(upper)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT partition_key, failed_at, run_id, has_row \
                     FROM asset_partition_failures WHERE code_location_id = $1 \
                     AND asset_key = $2 ORDER BY partition_ord LIMIT $3",
                )
                .bind(code_location_id)
                .bind(asset_key)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };
        rows.iter().map(failure_from_row).collect()
    }

    /// The failure row of each of `keys` that has one, in the order of
    /// `keys`.
    pub(super) async fn get_partition_failures_for_keys(
        &self,
        code_location_id: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<PartitionFailure>> {
        let ords: Vec<Vec<u8>> = keys.iter().map(partition_ord).collect();
        sqlx::query(
            "SELECT f.partition_key, f.failed_at, f.run_id, f.has_row \
             FROM unnest($3::bytea[]) WITH ORDINALITY AS k(ord, i) \
             JOIN asset_partition_failures f ON f.code_location_id = $1 \
             AND f.asset_key = $2 AND f.partition_ord = k.ord ORDER BY k.i",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(&ords)
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(failure_from_row)
        .collect()
    }
}
