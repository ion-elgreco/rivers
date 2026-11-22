//! The failure rows that the events and the runs give, for the tests that
//! compare them with the rows that the writers keep.

use std::cmp::Reverse;

use serde_json::Value;

use super::*;

/// A failure by run as the tests compare it: key, run, and the times of the
/// step failure and of the end of the run in failure.
pub(crate) type RunFailureRow = (PartitionKey, String, Option<i64>, Option<i64>);

/// The failure of a partition by one run.
#[derive(Debug, Clone, Copy, Default)]
struct RunFailure {
    step_failed_at: Option<i64>,
    run_failed_at: Option<i64>,
}

impl RunFailure {
    fn at(&self) -> Option<i64> {
        self.step_failed_at.max(self.run_failed_at)
    }
}

type ByRun = HashMap<String, RunFailure>;

/// A run that can fail partitions, or whose failures do not count: id,
/// status, action, partition key, assets, start and end.
type RunRow = (
    String,
    String,
    Option<String>,
    Option<Value>,
    Vec<String>,
    i64,
    Option<i64>,
);

impl PostgresStorage {
    /// The failure rows of `asset` that the events and the runs give, in the
    /// order of the store. The rows that the writers keep must be the same.
    pub(crate) async fn derived_failure_rows(
        &self,
        cl: &str,
        asset: &str,
    ) -> Result<Vec<PartitionFailure>> {
        let derived = self.derive_failures(cl, asset).await?;
        let built = self.built_at(cl, asset).await?;
        let mut ordered: Vec<(&PartitionKey, &ByRun)> = derived.iter().collect();
        ordered.sort_by(|a, b| stored_order(a.0, b.0));
        let mut rows = Vec::new();
        for (key, by_run) in ordered {
            let built_at = built.get(key).copied();
            let latest = by_run
                .iter()
                .filter_map(|(run_id, failure)| Some((run_id.as_str(), failure.at()?)))
                .filter(|&(_, at)| built_at.is_none_or(|built_at| built_at < at))
                .max_by_key(|&(run_id, at)| (at, Reverse(run_id)));
            if let Some((run_id, at)) = latest {
                rows.push(PartitionFailure {
                    key: key.clone(),
                    failed_at: at,
                    run_id: run_id.to_string(),
                    has_row: built_at.is_some(),
                });
            }
        }
        Ok(rows)
    }

    /// The failures by run of `asset` that the writers keep, and those that
    /// the events and the runs give, in the order of the store. A time at or
    /// before the materialization or a deletion of its partition reads as
    /// none in both: it fails nothing, now or later.
    pub(crate) async fn run_failure_rows(
        &self,
        cl: &str,
        asset: &str,
    ) -> Result<(Vec<RunFailureRow>, Vec<RunFailureRow>)> {
        let derived = self.derive_failures(cl, asset).await?;
        let built = self.built_at(cl, asset).await?;
        let (deleted, asset_deleted) = self.deletions(cl, asset).await?;
        let newer = |key: &PartitionKey, at: Option<i64>| {
            let replaced_at = built
                .get(key)
                .copied()
                .max(deleted.get(key).copied())
                .max(asset_deleted);
            at.filter(|at| replaced_at.is_none_or(|replaced_at| replaced_at < *at))
        };
        let order = |a: &RunFailureRow, b: &RunFailureRow| {
            stored_order(&a.0, &b.0).then_with(|| a.1.cmp(&b.1))
        };

        let stored: Vec<(Value, String, Option<i64>, Option<i64>)> = sqlx::query_as(
            "SELECT partition_key, run_id, step_failed_at, run_failed_at \
             FROM asset_partition_run_failures WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(cl)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        let mut kept = Vec::with_capacity(stored.len());
        for (key, run_id, step, run) in stored {
            let key: PartitionKey = from_json(key)?;
            let (step, run) = (newer(&key, step), newer(&key, run));
            kept.push((key, run_id, step, run));
        }
        kept.sort_by(order);

        let mut by_run: Vec<RunFailureRow> = derived
            .into_iter()
            .flat_map(|(key, by_run)| {
                by_run
                    .into_iter()
                    .map(move |(run_id, failure)| (key.clone(), run_id, failure))
            })
            .map(|(key, run_id, failure)| {
                let step = newer(&key, failure.step_failed_at);
                let run = newer(&key, failure.run_failed_at);
                (key, run_id, step, run)
            })
            .filter(|row| row.2.is_some() || row.3.is_some())
            .collect();
        by_run.sort_by(order);
        Ok((kept, by_run))
    }

    /// The failures of each partition (each member) of `asset`, by run,
    /// unless a deletion at or after a failure replaced it. A later
    /// materialization is not compared here.
    async fn derive_failures(&self, cl: &str, asset: &str) -> Result<HashMap<PartitionKey, ByRun>> {
        let runs: Vec<RunRow> = sqlx::query_as(
            "SELECT run_id, status, action, partition_key, node_names, start_time, end_time \
             FROM runs WHERE action IS NOT NULL OR (status = 'Failure' AND code_location_id = $1)",
        )
        .bind(cl)
        .fetch_all(&self.pool)
        .await?;
        let action_runs: HashSet<&str> = runs
            .iter()
            .filter(|run| run.2.is_some())
            .map(|run| run.0.as_str())
            .collect();

        let mut failures: HashMap<PartitionKey, ByRun> = HashMap::new();
        let step_failures: Vec<(Value, String, i64)> = sqlx::query_as(
            "SELECT partition_key, run_id, max(timestamp) FROM events \
             WHERE code_location_id = $1 AND asset_key = $2 AND event_type = 'StepFailure' \
             AND partition_key IS NOT NULL GROUP BY partition_key, run_id",
        )
        .bind(cl)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        for (key, run_id, at) in step_failures {
            if action_runs.contains(run_id.as_str()) {
                continue;
            }
            for member in from_json::<PartitionKey>(key)?.members() {
                let failure = failures
                    .entry(member)
                    .or_default()
                    .entry(run_id.clone())
                    .or_default();
                failure.step_failed_at = failure.step_failed_at.max(Some(at));
            }
        }

        // A failed run fails no partition that it materialized, read off its
        // own events.
        let failed_runs: Vec<&RunRow> = runs
            .iter()
            .filter(|run| run.1 == "Failure" && run.2.is_none())
            .filter(|run| run.4.iter().any(|name| name == asset))
            .collect();
        let ids: Vec<&str> = failed_runs.iter().map(|run| run.0.as_str()).collect();
        let materialized: Vec<(String, Value)> = sqlx::query_as(
            "SELECT run_id, partition_key FROM events WHERE run_id = ANY($1) \
             AND event_type = 'Materialization' AND asset_key = $2 AND partition_key IS NOT NULL",
        )
        .bind(&ids)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        let mut done: HashMap<String, HashSet<PartitionKey>> = HashMap::new();
        for (run_id, key) in materialized {
            let members = from_json::<PartitionKey>(key)?.members();
            done.entry(run_id).or_default().extend(members);
        }
        for (run_id, _, _, key, _, start_time, end_time) in failed_runs {
            let Some(key) = key else {
                continue;
            };
            let done = done.get(run_id);
            for member in from_json::<PartitionKey>(key.clone())?.members() {
                if done.is_some_and(|done| done.contains(&member)) {
                    continue;
                }
                let failure = failures
                    .entry(member)
                    .or_default()
                    .entry(run_id.clone())
                    .or_default();
                failure.run_failed_at = Some(end_time.unwrap_or(*start_time));
            }
        }

        let (deleted, asset_deleted) = self.deletions(cl, asset).await?;
        failures.retain(|member, by_run| {
            let deleted_at = deleted.get(member).copied().max(asset_deleted);
            by_run.retain(|_, failure| {
                failure
                    .at()
                    .is_some_and(|at| deleted_at.is_none_or(|deleted_at| deleted_at < at))
            });
            !by_run.is_empty()
        });
        Ok(failures)
    }

    /// The newest deletion of each partition (each member) of `asset`, and
    /// the newest deletion of the asset.
    async fn deletions(
        &self,
        cl: &str,
        asset: &str,
    ) -> Result<(HashMap<PartitionKey, i64>, Option<i64>)> {
        let tombstones: Vec<(Value, i64)> = sqlx::query_as(
            "SELECT partition_key, timestamp FROM asset_partition_deletions \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(cl)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        let mut deleted = HashMap::new();
        for (key, at) in tombstones {
            for member in from_json::<PartitionKey>(key)?.members() {
                let held = deleted.entry(member).or_insert(at);
                *held = (*held).max(at);
            }
        }
        let asset_deleted: Option<i64> = sqlx::query_scalar(
            "SELECT last_deletion_timestamp FROM assets \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(cl)
        .bind(asset)
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        Ok((deleted, asset_deleted))
    }

    /// The time of the row of each materialized partition of `asset`.
    async fn built_at(&self, cl: &str, asset: &str) -> Result<HashMap<PartitionKey, i64>> {
        let rows: Vec<(Value, Option<i64>)> = sqlx::query_as(
            "SELECT partition_key, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(cl)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        let mut built = HashMap::new();
        for (key, at) in rows {
            if let Some(at) = at {
                built.insert(from_json(key)?, at);
            }
        }
        Ok(built)
    }
}
