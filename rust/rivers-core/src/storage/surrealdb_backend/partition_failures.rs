//! The partitions whose latest result is a failure: one row each in
//! `asset_partition_failures`.
//!
//! A partition has a row while its latest failure is newer than its
//! materialization, its deletion and the deletion of its asset. The writers
//! keep the rows: a failure adds one, a materialization or a deletion at or
//! after the failure removes it. [`derive_failures`] gives the same failures
//! from the events and the runs. It fills the table on the migration, and
//! again for the partitions of a run that changed after it ended.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::method::{Query, Transaction};
use surrealdb::types::{RecordId, SurrealValue, Value};

use crate::storage::{
    EventRecord, EventType, FailureCount, PartitionBounds, PartitionFailure, PartitionKey,
    RunRecord, RunStatus, retry, stored_order,
};

use super::partition_reads::{
    KEYS_PER_QUERY, QUERIES_AT_ONCE, bounded_statements, keyed_statements,
};
use super::*;

/// Partitions that one transaction writes. A short transaction holds the
/// asset row for less time, and loses less work to a conflict.
const FAILURES_PER_TRANSACTION: usize = 1_000;

/// Up to this many keys are read one by one. The rows of more keys are read
/// as one range first.
const KEYS_READ_ONE_BY_ONE: usize = 16;

/// The rows per key that a read of a range may return before the keys are
/// read one by one: one read by key costs about as much as 50 rows of a range.
const RANGE_ROWS_PER_KEY: usize = 50;

/// Runs whose materializations one query reads.
const RUNS_PER_QUERY: usize = 1_000;

/// The error of [`failure_rows_guard`].
const FAILURE_ROWS_IN_RANGE: &str = "failure rows are in the range of the partitions";

/// Starts a transaction that changes the partition rows of an asset from
/// `$lowest` to `$highest`. It stops the transaction when failure rows of the
/// asset are in that range: the change then runs again in a transaction that
/// also changes those rows.
pub(super) fn failure_rows_guard() -> String {
    format!("IF ({ONE_FAILURE_IN_RANGE}) {{ THROW '{FAILURE_ROWS_IN_RANGE}' }}; ")
}

/// Run `query`, a transaction that [`failure_rows_guard`] starts. `false`:
/// the guard stopped it.
pub(super) async fn run_guarded(query: Query<'_, Any>) -> Result<bool> {
    let mut response = query.await?;
    let errors = response.take_errors();
    if errors
        .values()
        .any(|error| error.to_string().contains(FAILURE_ROWS_IN_RANGE))
    {
        return Ok(false);
    }
    match errors.into_iter().min_by_key(|(statement, _)| *statement) {
        Some((_, error)) => Err(error.into()),
        None => Ok(true),
    }
}

/// The asset and the partition of a keyed `StepFailure` event.
pub(super) fn keyed_failure(event: &EventRecord) -> Option<(&str, &PartitionKey)> {
    match (&event.event_type, &event.asset_key, &event.partition_key) {
        (EventType::StepFailure, Some(asset_key), Some(partition_key)) => {
            Some((asset_key.as_str(), partition_key))
        }
        _ => None,
    }
}

/// The first and the last of `keys` in the order of the store.
pub(super) fn key_range(keys: &[PartitionKey]) -> Option<(PartitionKey, PartitionKey)> {
    let lowest = keys.iter().min_by(|a, b| stored_order(a, b))?;
    let highest = keys.iter().max_by(|a, b| stored_order(a, b))?;
    Some((lowest.clone(), highest.clone()))
}

/// The store, or a transaction of it.
trait Statements: Sync {
    fn statements(&self, sql: String) -> Query<'_, Any>;
}

impl Statements for Surreal<Any> {
    fn statements(&self, sql: String) -> Query<'_, Any> {
        self.query(sql)
    }
}

impl Statements for Transaction<Any> {
    fn statements(&self, sql: String) -> Query<'_, Any> {
        self.query(sql)
    }
}

/// A row of a table with one row per partition of an asset, on a UNIQUE
/// index of `code_location_id`, `asset_key` and `partition_key`.
trait OfPartition: SurrealValue + Send {
    /// `SELECT <fields> FROM <table>`.
    const SELECT: &'static str;

    fn partition_key(&self) -> &PartitionKey;
}

#[derive(Debug, SurrealValue)]
struct BuiltRow {
    partition_key: PartitionKey,
    last_timestamp: Option<i64>,
}

impl OfPartition for BuiltRow {
    const SELECT: &'static str = "SELECT partition_key, last_timestamp FROM asset_partitions";

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

#[derive(Debug, SurrealValue)]
struct DeletedRow {
    partition_key: PartitionKey,
    timestamp: i64,
}

impl OfPartition for DeletedRow {
    const SELECT: &'static str = "SELECT partition_key, timestamp FROM asset_partition_deletions";

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

#[derive(Debug, SurrealValue)]
struct FailedRow {
    id: RecordId,
    partition_key: PartitionKey,
    failed_at: i64,
    run_id: String,
    built_at: Option<i64>,
}

impl OfPartition for FailedRow {
    const SELECT: &'static str =
        "SELECT id, partition_key, failed_at, run_id, built_at FROM asset_partition_failures";

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

/// The rows of the keys from `lower` to `upper`, at most `limit` of them.
async fn rows_in_range<R: OfPartition>(
    db: &impl Statements,
    cl: &str,
    asset_key: &str,
    (lower, upper): (&PartitionKey, &PartitionKey),
    limit: usize,
) -> Result<Vec<R>> {
    let mut result = db
        .statements(format!("{} {ROWS_IN_RANGE}", R::SELECT))
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .bind(("lower", lower.clone()))
        .bind(("upper", upper.clone()))
        .bind(("limit", limit as u64))
        .await?
        .check()?;
    Ok(result.take(0)?)
}

/// The rows of `keys`, which are in the order of the store. One read of
/// their range when it holds few other rows, else one read per key.
async fn rows_of_keys<R: OfPartition>(
    db: &impl Statements,
    cl: &str,
    asset_key: &str,
    keys: &[PartitionKey],
) -> Result<Vec<R>> {
    let (Some(first), Some(last)) = (keys.first(), keys.last()) else {
        return Ok(Vec::new());
    };

    if keys.len() > KEYS_READ_ONE_BY_ONE {
        let limit = keys.len().saturating_mul(RANGE_ROWS_PER_KEY);
        let rows: Vec<R> = rows_in_range(db, cl, asset_key, (first, last), limit).await?;
        if rows.len() < limit {
            let wanted: HashSet<&PartitionKey> = keys.iter().collect();
            return Ok(rows
                .into_iter()
                .filter(|row| wanted.contains(row.partition_key()))
                .collect());
        }
    }

    let by_key = format!("{} {ROW_OF_KEY}", R::SELECT);
    let mut rows = Vec::new();
    for chunk in keys.chunks(KEYS_PER_QUERY) {
        let mut query = db
            .statements(keyed_statements(&by_key, chunk.len()))
            .bind(("cl", cl.to_string()))
            .bind(("asset_key", asset_key.to_string()));
        for (i, key) in chunk.iter().enumerate() {
            query = query.bind((format!("partition_{i}"), key.clone()));
        }
        let mut result = query.await?.check()?;
        for statement in 0..chunk.len() {
            rows.extend(result.take::<Vec<R>>(statement)?);
        }
    }
    Ok(rows)
}

/// The time of the row of each of `keys` that has one.
async fn built_at_of(
    db: &impl Statements,
    cl: &str,
    asset_key: &str,
    keys: &[PartitionKey],
) -> Result<HashMap<PartitionKey, i64>> {
    Ok(rows_of_keys::<BuiltRow>(db, cl, asset_key, keys)
        .await?
        .into_iter()
        .filter_map(|row| Some((row.partition_key, row.last_timestamp?)))
        .collect())
}

/// What a writer of failure rows reads while it holds the asset row.
#[derive(Debug, SurrealValue)]
struct Held {
    /// The time of the latest deletion of the whole asset.
    deleted_at: Option<i64>,
    /// The asset has partition tombstones.
    tombstones: bool,
}

/// In `txn`: write the `assets` row and put it back (see
/// [`HOLD_ASSET_FOR_FAILURES`]).
async fn hold_asset(txn: &Transaction<Any>, cl: &str, asset_key: &str) -> Result<Held> {
    let mut result = txn
        .query(HOLD_ASSET_FOR_FAILURES)
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .await?
        .check()?;
    let last = result.num_statements() - 1;
    let held: Option<Held> = result.take(last)?;
    held.context("the hold of the asset row returns no row")
}

/// In `txn`, which holds the asset row: write a row for each of `failures`
/// (in the order of the store) that is newer than the partition's
/// materialization, its deletion and the deletion of the asset.
async fn write_failures(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    held: &Held,
    failures: &[(PartitionKey, i64, String)],
) -> Result<()> {
    let failures: Vec<&(PartitionKey, i64, String)> = failures
        .iter()
        .filter(|(_, at, _)| held.deleted_at.is_none_or(|deleted| deleted < *at))
        .collect();
    if failures.is_empty() {
        return Ok(());
    }
    let keys: Vec<PartitionKey> = failures.iter().map(|(key, _, _)| key.clone()).collect();

    let built = built_at_of(txn, cl, asset_key, &keys).await?;
    let deleted: HashMap<PartitionKey, i64> = match held.tombstones {
        true => rows_of_keys::<DeletedRow>(txn, cl, asset_key, &keys)
            .await?
            .into_iter()
            .map(|row| (row.partition_key, row.timestamp))
            .collect(),
        false => HashMap::new(),
    };

    let rows: Vec<DbPartitionFailure> = failures
        .into_iter()
        .filter(|(key, at, _)| {
            built.get(key).is_none_or(|built| built < at)
                && deleted.get(key).is_none_or(|deleted| deleted < at)
        })
        .map(|(key, at, run_id)| {
            let built_at = built.get(key).copied();
            DbPartitionFailure {
                code_location_id: cl.to_string(),
                asset_key: asset_key.to_string(),
                partition_key: key.clone(),
                failed_at: *at,
                run_id: run_id.clone(),
                built_at,
                has_row: built_at.is_some(),
            }
        })
        .collect();

    if !rows.is_empty() {
        txn.query(RECORD_PARTITION_FAILURES)
            .bind(("rows", rows))
            .await?
            .check()?;
    }
    Ok(())
}

/// In `txn`: delete the failure rows `ids`.
async fn delete_failure_rows(txn: &Transaction<Any>, ids: Vec<RecordId>) -> Result<()> {
    if !ids.is_empty() {
        txn.query("DELETE $ids RETURN NONE")
            .bind(("ids", ids))
            .await?
            .check()?;
    }
    Ok(())
}

/// In `txn`: give the failure rows `rows` the time of their partition's row.
async fn move_failure_rows(txn: &Transaction<Any>, rows: Vec<DbPartitionFailure>) -> Result<()> {
    if !rows.is_empty() {
        txn.query(MOVE_FAILURE_ROWS)
            .bind(("rows", rows))
            .await?
            .check()?;
    }
    Ok(())
}

/// `row` with the time `built_at` of its partition's row.
fn moved(cl: &str, asset_key: &str, row: FailedRow, built_at: Option<i64>) -> DbPartitionFailure {
    DbPartitionFailure {
        code_location_id: cl.to_string(),
        asset_key: asset_key.to_string(),
        partition_key: row.partition_key,
        failed_at: row.failed_at,
        run_id: row.run_id,
        built_at,
        has_row: built_at.is_some(),
    }
}

/// In `txn`, after the materialization of `parts`: remove the failure rows
/// that the materialization replaced, and give the others the time of the
/// partition's row. Returns, by run, the partitions whose row names the run
/// that materialized them: the failure can be the end of that run in failure,
/// which the materialization replaces when its event comes in late.
pub(super) async fn fix_failures_after_materialization(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    parts: &[DbAssetPartitionWrite],
) -> Result<HashMap<String, Vec<PartitionKey>>> {
    let mut keys: Vec<PartitionKey> = parts
        .iter()
        .map(|part| part.partition_key.clone())
        .collect();
    keys.sort_by(stored_order);
    keys.dedup();
    let failed: Vec<FailedRow> = rows_of_keys(txn, cl, asset_key, &keys).await?;
    if failed.is_empty() {
        return Ok(HashMap::new());
    }

    let failed_keys: Vec<PartitionKey> =
        failed.iter().map(|row| row.partition_key.clone()).collect();
    let built = built_at_of(txn, cl, asset_key, &failed_keys).await?;
    let run_of: HashMap<&PartitionKey, &str> = parts
        .iter()
        .map(|part| (&part.partition_key, part.last_run_id.as_str()))
        .collect();

    let mut replaced = Vec::new();
    let mut moved_rows = Vec::new();
    let mut late: HashMap<String, Vec<PartitionKey>> = HashMap::new();
    for row in failed {
        let built_at = built.get(&row.partition_key).copied();
        if built_at.is_some_and(|built| built >= row.failed_at) {
            replaced.push(row.id);
            continue;
        }
        if run_of.get(&row.partition_key) == Some(&row.run_id.as_str()) {
            late.entry(row.run_id.clone())
                .or_default()
                .push(row.partition_key.clone());
        }
        if built_at != row.built_at {
            moved_rows.push(moved(cl, asset_key, row, built_at));
        }
    }

    delete_failure_rows(txn, replaced).await?;
    move_failure_rows(txn, moved_rows).await?;
    Ok(late)
}

/// In `txn`, after the deletion of partitions (`deleted`: the time of each):
/// remove the failure rows that a deletion replaced, and clear the time of
/// the partition's row of the others whose row the deletion removed.
pub(super) async fn fix_failures_after_deletion(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    deleted: &HashMap<PartitionKey, i64>,
) -> Result<()> {
    let mut keys: Vec<PartitionKey> = deleted.keys().cloned().collect();
    keys.sort_by(stored_order);
    let failed: Vec<FailedRow> = rows_of_keys(txn, cl, asset_key, &keys).await?;

    let mut replaced = Vec::new();
    let mut moved_rows = Vec::new();
    for row in failed {
        let Some(deleted_at) = deleted.get(&row.partition_key).copied() else {
            continue;
        };
        if row.failed_at <= deleted_at {
            replaced.push(row.id);
        } else if row.built_at.is_some_and(|built| built <= deleted_at) {
            moved_rows.push(moved(cl, asset_key, row, None));
        }
    }

    delete_failure_rows(txn, replaced).await?;
    move_failure_rows(txn, moved_rows).await
}

/// The columns of a run that its failures need.
#[derive(Debug, Clone, SurrealValue)]
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

impl SurrealStorage {
    /// Record the failures `(partition, time, run)` of partitions of one
    /// asset; each partition is one member. A partition takes a failure that
    /// is newer than its materialization, its deletion, the deletion of the
    /// asset and the failure its row holds. `failed_run`: the failures are
    /// the end of this run in failure, which fails no partition it
    /// materialized.
    pub(super) async fn record_partition_failures(
        &self,
        cl: &str,
        asset_key: &str,
        failures: Vec<(PartitionKey, i64, String)>,
        failed_run: Option<&str>,
    ) -> Result<()> {
        let mut latest: HashMap<PartitionKey, (i64, String)> = HashMap::new();
        for (key, at, run_id) in failures {
            if latest.get(&key).is_none_or(|(held, _)| *held < at) {
                latest.insert(key, (at, run_id));
            }
        }
        let mut failures: Vec<(PartitionKey, i64, String)> = latest
            .into_iter()
            .map(|(key, (at, run_id))| (key, at, run_id))
            .collect();
        failures.sort_by(|a, b| stored_order(&a.0, &b.0));

        for chunk in failures.chunks(FAILURES_PER_TRANSACTION) {
            retry::with_retry(&self.retry_config.contended_batch(), || async {
                let txn = self.db.clone().begin().await?;
                let held = hold_asset(&txn, cl, asset_key).await?;

                // Read while the asset row is held: a materialization that
                // commits after this makes the transaction retry.
                let chunk = match failed_run {
                    Some(run_id) => not_built_by(&self.db, asset_key, run_id, chunk).await?,
                    None => chunk.to_vec(),
                };
                write_failures(&txn, cl, asset_key, &held, &chunk).await?;
                txn.commit().await?;
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    /// The run `run_id`, with the columns that its failures need.
    pub(super) async fn run_of_failures(&self, run_id: &str) -> Result<Option<RunOfFailures>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT run_id, code_location_id, status, action, partition_key, \
                     node_names, start_time, end_time FROM runs WHERE run_id = $run_id LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?
                .check()?;
            let runs: Vec<RunOfFailures> = result.take(0)?;
            Ok(runs.into_iter().next())
        })
        .await
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
        let built = materialized_by_runs(&self.db, None, std::slice::from_ref(&run.run_id)).await?;
        let members = partition_key.members();

        for asset_key in &run.node_names {
            let done = built.get(&(asset_key.clone(), run.run_id.clone()));
            let failures: Vec<(PartitionKey, i64, String)> = members
                .iter()
                .filter(|member| done.is_none_or(|done| !done.contains(*member)))
                .map(|member| (member.clone(), at, run.run_id.clone()))
                .collect();
            if !failures.is_empty() {
                let failed_run = Some(run.run_id.as_str());
                self.record_partition_failures(
                    &run.code_location_id,
                    asset_key,
                    failures,
                    failed_run,
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

    /// Derive again the failures of the partitions whose row names the run
    /// `run_id`: the run is deleted (`excluded`), no longer ends in failure,
    /// or materialized a partition after it ended. `only`: the asset and the
    /// partitions to look at; without it, every row of the run.
    pub(super) async fn rederive_failures_of_run(
        &self,
        run_id: &str,
        excluded: bool,
        only: Option<(&str, &str, Vec<PartitionKey>)>,
    ) -> Result<()> {
        let assets: Vec<(String, String, Vec<PartitionKey>)> = match only {
            Some((cl, asset_key, keys)) => vec![(cl.to_string(), asset_key.to_string(), keys)],
            None => self.failure_rows_of_run(run_id).await?,
        };

        for (cl, asset_key, mut keys) in assets {
            let scope = Scope::Asset {
                cl: &cl,
                asset_key: &asset_key,
            };
            keys.sort_by(stored_order);
            keys.dedup();

            for chunk in keys.chunks(FAILURES_PER_TRANSACTION) {
                retry::with_retry(&self.retry_config.contended_batch(), || async {
                    let txn = self.db.clone().begin().await?;
                    let held = hold_asset(&txn, &cl, &asset_key).await?;

                    // Derived while the asset row is held: a failure that
                    // commits after this makes the transaction retry.
                    let derived = derive_failures(&self.db, scope, excluded.then_some(run_id))
                        .await?
                        .remove(&(cl.clone(), asset_key.clone()))
                        .unwrap_or_default();
                    let rows: Vec<FailedRow> =
                        rows_of_keys::<FailedRow>(&txn, &cl, &asset_key, chunk)
                            .await?
                            .into_iter()
                            .filter(|row| row.run_id == run_id)
                            .collect();
                    let failures: Vec<(PartitionKey, i64, String)> = rows
                        .iter()
                        .filter_map(|row| {
                            let failure = derived.get(&row.partition_key)?;
                            Some((
                                row.partition_key.clone(),
                                failure.at,
                                failure.run_id.clone(),
                            ))
                        })
                        .collect();

                    delete_failure_rows(&txn, rows.into_iter().map(|row| row.id).collect()).await?;
                    write_failures(&txn, &cl, &asset_key, &held, &failures).await?;
                    txn.commit().await?;
                    Ok(())
                })
                .await?;
            }
        }
        Ok(())
    }

    /// The partitions whose failure row names the run `run_id`, by asset.
    async fn failure_rows_of_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<(String, String, Vec<PartitionKey>)>> {
        #[derive(SurrealValue)]
        struct Row {
            code_location_id: String,
            asset_key: String,
            partition_key: PartitionKey,
        }

        let rows: Vec<Row> = retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(FAILURE_ROWS_OF_RUN)
                .bind(("run_id", run_id.to_string()))
                .await?
                .check()?;
            Ok(result.take(0)?)
        })
        .await?;

        let mut by_asset: HashMap<(String, String), Vec<PartitionKey>> = HashMap::new();
        for row in rows {
            by_asset
                .entry((row.code_location_id, row.asset_key))
                .or_default()
                .push(row.partition_key);
        }
        Ok(by_asset
            .into_iter()
            .map(|((cl, asset_key), keys)| (cl, asset_key, keys))
            .collect())
    }

    pub(super) async fn count_failures(&self, cl: &str, asset_key: &str) -> Result<FailureCount> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(COUNT_FAILURES)
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .await?
                .check()?;
            let failed: Option<u64> = result.take((0, "total"))?;
            let with_row: Option<u64> = result.take((1, "total"))?;
            Ok(FailureCount {
                failed: failed.unwrap_or(0),
                with_row: with_row.unwrap_or(0),
            })
        })
        .await
    }

    pub(super) async fn count_failures_in(
        &self,
        cl: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<FailureCount>> {
        let queries: Vec<_> = bounds
            .chunks(KEYS_PER_QUERY / 2)
            .map(|chunk| self.count_failures_in_query(cl, asset_key, chunk))
            .collect();
        let counts: Vec<Vec<FailureCount>> = stream::iter(queries)
            .buffered(QUERIES_AT_ONCE)
            .try_collect()
            .await?;
        Ok(counts.into_iter().flatten().collect())
    }

    /// One query of [`Self::count_failures_in`].
    async fn count_failures_in_query(
        &self,
        cl: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<FailureCount>> {
        let statements = bounded_statements(COUNT_FAILURES_IN_BOUNDS, bounds.len());

        let mut result = retry::with_retry(&self.retry_config, || async {
            let mut query = self
                .db
                .query(statements.as_str())
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string()));
            for (i, bounds) in bounds.iter().enumerate() {
                query = query
                    .bind((format!("lower_{i}"), bounds.lower.clone()))
                    .bind((format!("upper_{i}"), bounds.upper.clone()));
            }
            Ok(query.await?.check()?)
        })
        .await?;

        (0..bounds.len())
            .map(|i| {
                let failed: Option<u64> = result.take((2 * i, "total"))?;
                let with_row: Option<u64> = result.take((2 * i + 1, "total"))?;
                Ok(FailureCount {
                    failed: failed.unwrap_or(0),
                    with_row: with_row.unwrap_or(0),
                })
            })
            .collect()
    }

    /// The failure rows of an asset, at most `limit` of them.
    pub(super) async fn failures_of_asset(
        &self,
        cl: &str,
        asset_key: &str,
        limit: Option<u64>,
    ) -> Result<Vec<PartitionFailure>> {
        let at_most = match limit {
            Some(_) => " LIMIT $limit",
            None => "",
        };
        let sql = format!(
            "{} WHERE code_location_id = $cl AND asset_key = $asset_key{at_most}",
            FailureRow::SELECT
        );

        let rows: Vec<FailureRow> = retry::with_retry(&self.retry_config, || async {
            let mut query = self
                .db
                .query(sql.as_str())
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string()));
            if let Some(limit) = limit {
                query = query.bind(("limit", limit));
            }
            Ok(query.await?.check()?.take(0)?)
        })
        .await?;
        Ok(rows.into_iter().map(PartitionFailure::from).collect())
    }

    /// The failure rows of the keys in `bounds`, at most `limit` of them.
    pub(super) async fn failures_in(
        &self,
        cl: &str,
        asset_key: &str,
        bounds: &PartitionBounds,
        limit: u64,
    ) -> Result<Vec<PartitionFailure>> {
        let range = (&bounds.lower, &bounds.upper);
        let rows: Vec<FailureRow> = retry::with_retry(&self.retry_config, || {
            rows_in_range(&self.db, cl, asset_key, range, limit as usize)
        })
        .await?;
        Ok(rows.into_iter().map(PartitionFailure::from).collect())
    }

    pub(super) async fn failures_of_keys(
        &self,
        cl: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<PartitionFailure>> {
        let rows: Vec<FailureRow> = self
            .select_by_partitions(
                &format!("{} {ROW_OF_KEY}", FailureRow::SELECT),
                &[("cl", cl), ("asset_key", asset_key)],
                keys,
            )
            .await?;
        Ok(rows.into_iter().map(PartitionFailure::from).collect())
    }
}

/// One failure row as the readers see it.
#[derive(Debug, SurrealValue)]
struct FailureRow {
    partition_key: PartitionKey,
    failed_at: i64,
    run_id: String,
    has_row: bool,
}

impl OfPartition for FailureRow {
    const SELECT: &'static str =
        "SELECT partition_key, failed_at, run_id, has_row FROM asset_partition_failures";

    fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

impl From<FailureRow> for PartitionFailure {
    fn from(row: FailureRow) -> Self {
        PartitionFailure {
            key: row.partition_key,
            failed_at: row.failed_at,
            run_id: row.run_id,
            has_row: row.has_row,
        }
    }
}

/// The assets whose failures to derive.
#[derive(Debug, Clone, Copy)]
pub(super) enum Scope<'a> {
    All,
    Asset { cl: &'a str, asset_key: &'a str },
}

impl Scope<'_> {
    /// The filter of the rows of the scope in a table with
    /// `code_location_id` and `asset_key`.
    fn rows(&self) -> &'static str {
        match self {
            Scope::All => "asset_key IS NOT NONE",
            Scope::Asset { .. } => "code_location_id = $cl AND asset_key = $asset_key",
        }
    }

    fn bind<'q>(&self, query: Query<'q, Any>) -> Query<'q, Any> {
        match self {
            Scope::All => query,
            Scope::Asset { cl, asset_key } => query
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string())),
        }
    }
}

/// The latest failure of a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LatestFailure {
    pub(super) at: i64,
    pub(super) run_id: String,
}

/// A code location and an asset key.
type AssetOf = (String, String);

/// The failures of each asset, by partition.
pub(super) type DerivedFailures = HashMap<AssetOf, HashMap<PartitionKey, LatestFailure>>;

/// The latest failure of each partition (each member) of the assets of
/// `scope`, unless a deletion at or after it replaced it. A later
/// materialization is not compared here. `excluded`: a run to leave out, as
/// if it was deleted.
///
/// A failure is a keyed `StepFailure` event of a run that is not an action
/// run. The end in failure of a run with a partition that is not an action
/// run is a failure too, at the end of the run: of each member of its
/// partition, for each asset of the run, unless the run materialized it.
pub(super) async fn derive_failures(
    db: &Surreal<Any>,
    scope: Scope<'_>,
    excluded: Option<&str>,
) -> Result<DerivedFailures> {
    #[derive(Debug, SurrealValue)]
    struct RunRow {
        run_id: String,
        code_location_id: String,
        status: String,
        action: Option<String>,
        partition_key: Option<Value>,
        node_names: Vec<String>,
        start_time: i64,
        end_time: Option<i64>,
    }

    #[derive(Debug, SurrealValue)]
    struct StepFailureRow {
        code_location_id: String,
        asset_key: String,
        partition_key: Value,
        run_id: String,
        at: i64,
    }

    #[derive(Debug, SurrealValue)]
    struct DeletionRow {
        code_location_id: String,
        asset_key: String,
        partition_key: Value,
        at: i64,
    }

    #[derive(Debug, SurrealValue)]
    struct AssetDeletionRow {
        code_location_id: String,
        asset_key: String,
        at: i64,
    }

    // `$asset_key IN node_names` defeats every index: one scan of the runs
    // serves both kinds of run. It keeps only action runs and failures.
    let of_runs = match scope {
        Scope::All => "",
        Scope::Asset { .. } => "code_location_id = $cl AND $asset_key IN node_names AND ",
    };
    let runs: Vec<RunRow> = scope
        .bind(db.query(format!(
            "SELECT run_id, code_location_id, status, action, partition_key, node_names, \
             start_time, end_time FROM runs \
             WHERE {of_runs}(action IS NOT NONE OR status = 'Failure')"
        )))
        .await?
        .check()?
        .take(0)?;
    let runs: Vec<RunRow> = runs
        .into_iter()
        .filter(|run| Some(run.run_id.as_str()) != excluded)
        .collect();

    // A failed action did not fail to materialize anything. Its keyed
    // StepFailure events stay in storage for the backfill accounting.
    let action_runs: HashSet<&str> = runs
        .iter()
        .filter(|run| run.action.is_some())
        .map(|run| run.run_id.as_str())
        .collect();

    let mut unknown = 0;
    let mut latest: DerivedFailures = HashMap::new();
    let mut fail = |asset: AssetOf, member: PartitionKey, at: i64, run_id: &str| {
        let failures = latest.entry(asset).or_default();
        if failures.get(&member).is_none_or(|held| held.at < at) {
            let run_id = run_id.to_string();
            failures.insert(member, LatestFailure { at, run_id });
        }
    };

    let step_failures: Vec<StepFailureRow> = scope
        .bind(db.query(format!(
            "SELECT code_location_id, asset_key, partition_key, run_id, \
             math::max(timestamp) AS at FROM events \
             WHERE {} AND event_type = 'StepFailure' AND partition_key IS NOT NONE \
             GROUP BY code_location_id, asset_key, partition_key, run_id",
            scope.rows()
        )))
        .await?
        .check()?
        .take(0)?;
    for row in step_failures {
        let run_id = row.run_id.as_str();
        if action_runs.contains(run_id) || Some(run_id) == excluded {
            continue;
        }
        let Some(partition_key) = stored_key(row.partition_key, &mut unknown) else {
            continue;
        };
        for member in partition_key.members() {
            let asset = (row.code_location_id.clone(), row.asset_key.clone());
            fail(asset, member, row.at, run_id);
        }
    }

    // A run that materialized a partition and then failed on another asset
    // did not fail that partition. Read off the run's own events, not the
    // partition row, which a deletion or a newer run moves off the run. The
    // failure happens when the run ends, so a deletion during the run does
    // not outrank it.
    let failed_runs: Vec<&RunRow> = runs
        .iter()
        .filter(|run| run.status == "Failure" && run.action.is_none())
        .filter(|run| run.partition_key.is_some())
        .collect();
    let failed_run_ids: Vec<String> = failed_runs.iter().map(|run| run.run_id.clone()).collect();
    let only_asset = match scope {
        Scope::All => None,
        Scope::Asset { asset_key, .. } => Some(asset_key),
    };
    let built = materialized_by_runs(db, only_asset, &failed_run_ids).await?;

    for run in failed_runs {
        let Some(partition_key) = run
            .partition_key
            .clone()
            .and_then(|key| stored_key(key, &mut unknown))
        else {
            continue;
        };
        let at = run.end_time.unwrap_or(run.start_time);
        let members = partition_key.members();
        let assets: Vec<&str> = match only_asset {
            Some(asset_key) => vec![asset_key],
            None => run.node_names.iter().map(String::as_str).collect(),
        };
        for asset_key in assets {
            let done = built.get(&(asset_key.to_string(), run.run_id.clone()));
            for member in &members {
                if done.is_some_and(|done| done.contains(member)) {
                    continue;
                }
                let asset = (run.code_location_id.clone(), asset_key.to_string());
                fail(asset, member.clone(), at, &run.run_id);
            }
        }
    }

    // A deletion replaces older failures the same way a newer
    // materialization does, the deletion of the whole asset for every key.
    // Read off the tombstones, which outlive the delete run's events.
    let mut result = scope
        .bind(db.query(format!(
            "SELECT code_location_id, asset_key, partition_key, timestamp AS at \
             FROM asset_partition_deletions WHERE {rows}; \
             SELECT code_location_id, asset_key, last_deletion_timestamp AS at FROM assets \
             WHERE {rows} AND last_deletion_timestamp IS NOT NONE;",
            rows = scope.rows()
        )))
        .await?
        .check()?;
    let deletions: Vec<DeletionRow> = result.take(0)?;
    let asset_deletions: Vec<AssetDeletionRow> = result.take(1)?;

    let mut deleted: HashMap<AssetOf, HashMap<PartitionKey, i64>> = HashMap::new();
    for row in deletions {
        let Some(partition_key) = stored_key(row.partition_key, &mut unknown) else {
            continue;
        };
        let of_asset = deleted
            .entry((row.code_location_id, row.asset_key))
            .or_default();
        for member in partition_key.members() {
            let at = of_asset.entry(member).or_insert(row.at);
            *at = (*at).max(row.at);
        }
    }
    let mut asset_deleted: HashMap<AssetOf, i64> = HashMap::new();
    for row in asset_deletions {
        let at = asset_deleted
            .entry((row.code_location_id, row.asset_key))
            .or_insert(row.at);
        *at = (*at).max(row.at);
    }

    for (asset, failures) in latest.iter_mut() {
        let of_asset = deleted.get(asset);
        let whole = asset_deleted.get(asset).copied();
        failures.retain(|member, failure| {
            let deleted_at = of_asset.and_then(|d| d.get(member)).copied().max(whole);
            deleted_at.is_none_or(|deleted_at| deleted_at < failure.at)
        });
    }
    latest.retain(|_, failures| !failures.is_empty());
    if unknown > 0 {
        tracing::warn!(
            target: "rivers::storage",
            rows = unknown,
            "rows whose partition key has an unknown form are left out of the failed partitions"
        );
    }
    Ok(latest)
}

/// A stored partition key, or `None` when the value is not one: it counts
/// in `unknown`.
fn stored_key(value: Value, unknown: &mut usize) -> Option<PartitionKey> {
    let key = PartitionKey::from_value(value).ok();
    *unknown += usize::from(key.is_none());
    key
}

/// The members that the runs `run_ids` materialized, by asset and run; with
/// `asset_key`, of that asset only. The index hint anchors the scan on the
/// runs, as in `materialized_by_runs` of the backend.
async fn materialized_by_runs(
    db: &Surreal<Any>,
    asset_key: Option<&str>,
    run_ids: &[String],
) -> Result<HashMap<AssetOf, HashSet<PartitionKey>>> {
    #[derive(Debug, SurrealValue)]
    struct BuiltByRun {
        asset_key: Option<String>,
        partition_key: Value,
        run_id: String,
    }

    let of_asset = match asset_key {
        Some(_) => " AND asset_key = $asset_key",
        None => "",
    };
    let sql = format!(
        "SELECT asset_key, partition_key, run_id FROM events WITH INDEX idx_events_run_type \
         WHERE run_id IN $runs AND event_type = 'Materialization'{of_asset} \
         AND partition_key IS NOT NONE"
    );

    let mut built: HashMap<AssetOf, HashSet<PartitionKey>> = HashMap::new();
    for runs in run_ids.chunks(RUNS_PER_QUERY) {
        let mut query = db.query(sql.as_str()).bind(("runs", runs.to_vec()));
        if let Some(asset_key) = asset_key {
            query = query.bind(("asset_key", asset_key.to_string()));
        }
        let rows: Vec<BuiltByRun> = query.await?.check()?.take(0)?;
        for row in rows {
            let (Some(asset_key), Ok(partition_key)) =
                (row.asset_key, PartitionKey::from_value(row.partition_key))
            else {
                continue;
            };
            built
                .entry((asset_key, row.run_id))
                .or_default()
                .extend(partition_key.members());
        }
    }
    Ok(built)
}

/// The failures of `failures` whose partition the run `run_id` did not
/// materialize.
async fn not_built_by(
    db: &Surreal<Any>,
    asset_key: &str,
    run_id: &str,
    failures: &[(PartitionKey, i64, String)],
) -> Result<Vec<(PartitionKey, i64, String)>> {
    let built = materialized_by_runs(db, Some(asset_key), &[run_id.to_string()]).await?;
    let done = built.get(&(asset_key.to_string(), run_id.to_string()));
    Ok(failures
        .iter()
        .filter(|(key, _, _)| done.is_none_or(|done| !done.contains(key)))
        .cloned()
        .collect())
}

/// The rows of the derived failures of one asset: those newer than the
/// partition's materialization.
async fn rows_of_derived(
    db: &Surreal<Any>,
    cl: &str,
    asset_key: &str,
    failures: HashMap<PartitionKey, LatestFailure>,
) -> Result<Vec<DbPartitionFailure>> {
    let mut failures: Vec<(PartitionKey, LatestFailure)> = failures.into_iter().collect();
    failures.sort_by(|a, b| stored_order(&a.0, &b.0));
    let keys: Vec<PartitionKey> = failures.iter().map(|(key, _)| key.clone()).collect();
    let built = built_at_of(db, cl, asset_key, &keys).await?;

    Ok(failures
        .into_iter()
        .filter(|(key, failure)| built.get(key).is_none_or(|built| *built < failure.at))
        .map(|(key, failure)| {
            let built_at = built.get(&key).copied();
            DbPartitionFailure {
                code_location_id: cl.to_string(),
                asset_key: asset_key.to_string(),
                partition_key: key,
                failed_at: failure.at,
                run_id: failure.run_id,
                built_at,
                has_row: built_at.is_some(),
            }
        })
        .collect())
}

/// Fill `asset_partition_failures` from the events and the runs, by the
/// rules the readers derived the failures with before the table.
pub(super) async fn fill_partition_failures(db: &Surreal<Any>) -> Result<()> {
    db.query("DELETE asset_partition_failures").await?.check()?;
    let derived = derive_failures(db, Scope::All, None).await?;

    for ((cl, asset_key), failures) in derived {
        let rows = rows_of_derived(db, &cl, &asset_key, failures).await?;
        for chunk in rows.chunks(FAILURES_PER_TRANSACTION) {
            db.query(RECORD_PARTITION_FAILURES)
                .bind(("rows", chunk.to_vec()))
                .await?
                .check()?;
        }
    }
    Ok(())
}

/// The failure rows of one asset that the derivation gives, in the order of
/// the store. The rows that the writers keep must be the same.
#[cfg(test)]
pub(super) async fn derived_rows(
    db: &Surreal<Any>,
    cl: &str,
    asset_key: &str,
) -> Result<Vec<PartitionFailure>> {
    let failures = derive_failures(db, Scope::Asset { cl, asset_key }, None)
        .await?
        .remove(&(cl.to_string(), asset_key.to_string()))
        .unwrap_or_default();
    let mut rows: Vec<PartitionFailure> = rows_of_derived(db, cl, asset_key, failures)
        .await?
        .into_iter()
        .map(|row| PartitionFailure {
            key: row.partition_key,
            failed_at: row.failed_at,
            run_id: row.run_id,
            has_row: row.has_row,
        })
        .collect();
    rows.sort_by(|a, b| stored_order(&a.key, &b.key));
    Ok(rows)
}
