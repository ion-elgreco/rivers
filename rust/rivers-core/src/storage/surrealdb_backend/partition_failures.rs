//! The partitions whose latest result is a failure: one row each in
//! `asset_partition_failures`, and one row per partition and run in
//! `asset_partition_run_failures`.
//!
//! A partition has rows while its latest failure is newer than its
//! materialization, its deletion and the deletion of its asset. The writers
//! keep the rows: a failure adds them, a materialization or a deletion at or
//! after the failure removes them. When a run is deleted or its end changes,
//! the rows of its partitions are computed again from the failures by run.
//! [`derive_failures`] gives the same failures from the events and the runs:
//! it fills the tables on the migration.

use std::cmp::Reverse;
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
/// asset row for less time, and loses less work to a conflict: at about 50 µs
/// per partition, 250 hold it for about 15 ms.
const FAILURES_PER_TRANSACTION: usize = 250;

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
pub(super) fn key_range<'a>(
    keys: impl Iterator<Item = &'a PartitionKey> + Clone,
) -> Option<(&'a PartitionKey, &'a PartitionKey)> {
    let lowest = keys.clone().min_by(|a, b| stored_order(a, b))?;
    let highest = keys.max_by(|a, b| stored_order(a, b))?;
    Some((lowest, highest))
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

/// A row of a table of the partitions of an asset, on an index that starts
/// with `code_location_id`, `asset_key` and `partition_key`.
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

#[derive(Debug, SurrealValue)]
struct RunFailedRow {
    id: RecordId,
    partition_key: PartitionKey,
    run_id: String,
    step_failed_at: Option<i64>,
    run_failed_at: Option<i64>,
}

impl RunFailedRow {
    fn failure(&self) -> RunFailure {
        RunFailure {
            step_failed_at: self.step_failed_at,
            run_failed_at: self.run_failed_at,
        }
    }
}

impl OfPartition for RunFailedRow {
    const SELECT: &'static str = "SELECT id, partition_key, run_id, step_failed_at, \
         run_failed_at FROM asset_partition_run_failures";

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

/// The deletions of an asset that its failures are compared with.
#[derive(Debug, Clone, Copy, SurrealValue)]
struct Deletions {
    /// The time of the latest deletion of the whole asset.
    deleted_at: Option<i64>,
    /// The asset has partition tombstones.
    tombstones: bool,
}

/// In `txn`: the deletions of the asset. `hold`: first write the `assets` row
/// and put it back (see [`HOLD_ASSET_FOR_FAILURES`]).
async fn deletions_of(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    hold: bool,
) -> Result<Deletions> {
    let hold = if hold { HOLD_ASSET_FOR_FAILURES } else { "" };
    let mut result = txn
        .query(format!("{hold}{DELETIONS_OF_ASSET}"))
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .await?
        .check()?;
    let last = result.num_statements() - 1;
    let deletions: Option<Deletions> = result.take(last)?;
    deletions.context("the deletions of the asset return no row")
}

/// In `txn`: hold the asset row, and read the deletions of the asset.
async fn hold_asset(txn: &Transaction<Any>, cl: &str, asset_key: &str) -> Result<Deletions> {
    deletions_of(txn, cl, asset_key, true).await
}

/// The time of the tombstone of each of `keys` that has one.
async fn deleted_at_of(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    deletions: &Deletions,
    keys: &[PartitionKey],
) -> Result<HashMap<PartitionKey, i64>> {
    if !deletions.tombstones {
        return Ok(HashMap::new());
    }
    Ok(rows_of_keys::<DeletedRow>(txn, cl, asset_key, keys)
        .await?
        .into_iter()
        .map(|row| (row.partition_key, row.timestamp))
        .collect())
}

/// In `txn`, which holds the asset row: write each of `failures` (in the
/// order of the store) that is newer than the partition's materialization,
/// its deletion and the deletion of the asset. `run_end`: the failures are
/// the end of their run in failure, else keyed step failures.
async fn write_failures(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    deletions: &Deletions,
    mut failures: Vec<&(PartitionKey, i64, &str)>,
    run_end: bool,
) -> Result<()> {
    failures.retain(|(_, at, _)| deletions.deleted_at.is_none_or(|deleted| deleted < *at));
    if failures.is_empty() {
        return Ok(());
    }
    let mut keys: Vec<PartitionKey> = failures.iter().map(|(key, _, _)| key.clone()).collect();
    keys.dedup();

    let built = built_at_of(txn, cl, asset_key, &keys).await?;
    let deleted = deleted_at_of(txn, cl, asset_key, deletions, &keys).await?;
    let failures: Vec<&(PartitionKey, i64, &str)> = failures
        .into_iter()
        .filter(|(key, at, _)| {
            built.get(key).is_none_or(|built| built < at)
                && deleted.get(key).is_none_or(|deleted| deleted < at)
        })
        .collect();
    if failures.is_empty() {
        return Ok(());
    }

    let mut latest: HashMap<&PartitionKey, &(PartitionKey, i64, &str)> = HashMap::new();
    for &failure in &failures {
        let (key, at, run_id) = failure;
        if latest
            .get(key)
            .is_none_or(|held| rank(held.1, held.2) < rank(*at, run_id))
        {
            latest.insert(key, failure);
        }
    }
    let rows: Vec<DbPartitionFailure> = latest
        .into_values()
        .map(|(key, at, run_id)| {
            let built_at = built.get(key).copied();
            DbPartitionFailure {
                code_location_id: cl.to_string(),
                asset_key: asset_key.to_string(),
                partition_key: key.clone(),
                failed_at: *at,
                run_id: run_id.to_string(),
                built_at,
                has_row: built_at.is_some(),
            }
        })
        .collect();
    let run_rows: Vec<DbPartitionRunFailure> = failures
        .into_iter()
        .map(|(key, at, run_id)| DbPartitionRunFailure {
            code_location_id: cl.to_string(),
            asset_key: asset_key.to_string(),
            partition_key: key.clone(),
            run_id: run_id.to_string(),
            step_failed_at: (!run_end).then_some(*at),
            run_failed_at: run_end.then_some(*at),
        })
        .collect();

    txn.query(format!(
        "{RECORD_RUN_FAILURES}; {RECORD_PARTITION_FAILURES};"
    ))
    .bind(("run_rows", run_rows))
    .bind(("rows", rows))
    .await?
    .check()?;
    Ok(())
}

/// In `txn`, which holds the asset row: give each failure by run of `keys`
/// (in the order of the store) the failure that `change` gives for it, keep
/// those that are newer than the partition's materialization and deletions,
/// and give each partition the failure row of the latest of them, or none.
/// `deletions`: of the asset, when the transaction read them. Returns whether
/// a row changed.
async fn refresh_partitions(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    deletions: Option<Deletions>,
    keys: &[PartitionKey],
    change: impl Fn(&RunFailedRow) -> RunFailure,
) -> Result<bool> {
    let run_rows: Vec<RunFailedRow> = rows_of_keys(txn, cl, asset_key, keys).await?;
    let failed: Vec<FailedRow> = rows_of_keys(txn, cl, asset_key, keys).await?;
    if run_rows.is_empty() && failed.is_empty() {
        return Ok(false);
    }

    let deletions = match deletions {
        Some(deletions) => deletions,
        None => deletions_of(txn, cl, asset_key, false).await?,
    };
    let mut keys: Vec<PartitionKey> = run_rows
        .iter()
        .map(|row| row.partition_key.clone())
        .chain(failed.iter().map(|row| row.partition_key.clone()))
        .collect();
    keys.sort_by(stored_order);
    keys.dedup();
    let built = built_at_of(txn, cl, asset_key, &keys).await?;
    let deleted = deleted_at_of(txn, cl, asset_key, &deletions, &keys).await?;
    let replaced_at = |key: &PartitionKey| {
        built
            .get(key)
            .copied()
            .max(deleted.get(key).copied())
            .max(deletions.deleted_at)
    };

    let mut gone: Vec<RecordId> = Vec::new();
    let mut changed: Vec<DbPartitionRunFailure> = Vec::new();
    let mut latest: HashMap<PartitionKey, (i64, String)> = HashMap::new();
    for row in run_rows {
        let failure = change(&row);
        let newer = failure
            .at()
            .filter(|at| replaced_at(&row.partition_key).is_none_or(|replaced| replaced < *at));
        let Some(at) = newer else {
            gone.push(row.id);
            continue;
        };
        if latest
            .get(&row.partition_key)
            .is_none_or(|(held, held_run)| rank(*held, held_run) < rank(at, &row.run_id))
        {
            latest.insert(row.partition_key.clone(), (at, row.run_id.clone()));
        }
        if failure != row.failure() {
            changed.push(DbPartitionRunFailure {
                code_location_id: cl.to_string(),
                asset_key: asset_key.to_string(),
                partition_key: row.partition_key,
                run_id: row.run_id,
                step_failed_at: failure.step_failed_at,
                run_failed_at: failure.run_failed_at,
            });
        }
    }

    let mut failed: HashMap<PartitionKey, FailedRow> = failed
        .into_iter()
        .map(|row| (row.partition_key.clone(), row))
        .collect();
    let mut rows: Vec<DbPartitionFailure> = Vec::new();
    for (key, (at, run_id)) in latest {
        let built_at = built.get(&key).copied();
        let held = failed.remove(&key);
        if held.is_some_and(|row| {
            row.failed_at == at && row.run_id == run_id && row.built_at == built_at
        }) {
            continue;
        }
        rows.push(DbPartitionFailure {
            code_location_id: cl.to_string(),
            asset_key: asset_key.to_string(),
            partition_key: key,
            failed_at: at,
            run_id,
            built_at,
            has_row: built_at.is_some(),
        });
    }
    gone.extend(failed.into_values().map(|row| row.id));

    let mut sql = String::new();
    if !gone.is_empty() {
        sql.push_str("DELETE $gone RETURN NONE; ");
    }
    if !changed.is_empty() {
        sql.push_str(&format!("{PUT_RUN_FAILURES}; "));
    }
    if !rows.is_empty() {
        sql.push_str(&format!("{PUT_PARTITION_FAILURES}; "));
    }
    if sql.is_empty() {
        return Ok(false);
    }
    txn.query(sql)
        .bind(("gone", gone))
        .bind(("run_rows", changed))
        .bind(("rows", rows))
        .await?
        .check()?;
    Ok(true)
}

/// In `txn`, after the materializations `built_by` (each partition and the
/// run that materialized it): the failures at or before the materialization
/// go, and a run that materialized a partition does not fail it by its end.
/// Its event can come in after the end of the run.
pub(super) async fn fix_failures_after_materialization(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    built_by: &[(&PartitionKey, &str)],
) -> Result<()> {
    let mut runs_of: HashMap<PartitionKey, HashSet<&str>> = HashMap::new();
    for &(partition_key, run_id) in built_by {
        for member in partition_key.members() {
            runs_of.entry(member).or_default().insert(run_id);
        }
    }
    let mut keys: Vec<PartitionKey> = runs_of.keys().cloned().collect();
    keys.sort_by(stored_order);

    refresh_partitions(txn, cl, asset_key, None, &keys, |row| {
        let failure = row.failure();
        let built = runs_of
            .get(&row.partition_key)
            .is_some_and(|runs| runs.contains(row.run_id.as_str()));
        match built {
            true => RunFailure {
                run_failed_at: None,
                ..failure
            },
            false => failure,
        }
    })
    .await?;
    Ok(())
}

/// In `txn`, after the deletion of the partitions `keys`: the failures at or
/// before their deletion go.
pub(super) async fn fix_failures_after_deletion(
    txn: &Transaction<Any>,
    cl: &str,
    asset_key: &str,
    keys: &[PartitionKey],
) -> Result<()> {
    let mut keys = keys.to_vec();
    keys.sort_by(stored_order);
    keys.dedup();
    refresh_partitions(txn, cl, asset_key, None, &keys, RunFailedRow::failure).await?;
    Ok(())
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

/// A change of a run that the failure rows of its partitions follow.
#[derive(Debug, Clone, Copy)]
pub(super) enum RunChange {
    /// The run is deleted.
    Deleted,
    /// The run ends in failure at this time, or not at all.
    FailedAt(Option<i64>),
}

impl RunChange {
    /// The failure by `row` after this change of the run `run_id`.
    fn apply(self, run_id: &str, row: &RunFailedRow) -> RunFailure {
        let failure = row.failure();
        match self {
            _ if row.run_id != run_id => failure,
            RunChange::Deleted => RunFailure::default(),
            RunChange::FailedAt(end) => RunFailure {
                run_failed_at: failure.run_failed_at.and(end),
                ..failure
            },
        }
    }
}

/// The status and the times of a run, as the store holds them.
#[derive(Debug, SurrealValue)]
pub(super) struct StoredStatus {
    pub(super) status: String,
    pub(super) start_time: i64,
    pub(super) end_time: Option<i64>,
}

impl SurrealStorage {
    /// Record the failures `(partition, time, run)` of partitions of one
    /// asset; each partition is one member. A partition takes a failure that
    /// is newer than its materialization, its deletion and the deletion of
    /// the asset: in the row of the partition and the run, and in the row of
    /// the partition when it is newer than the failure that row holds, or as
    /// new and of a run that sorts first.
    /// `failed_run`: the failures are the end of this run in failure, which
    /// fails no partition it materialized; else keyed step failures.
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
        let mut failures: Vec<(PartitionKey, i64, &str)> = latest
            .into_iter()
            .map(|((key, run_id), at)| (key, at, run_id))
            .collect();
        failures.sort_by(|a, b| stored_order(&a.0, &b.0));

        for chunk in failures.chunks(FAILURES_PER_TRANSACTION) {
            retry::with_retry(&self.retry_config.contended_batch(), || async {
                let txn = self.db.clone().begin().await?;
                let deletions = hold_asset(&txn, cl, asset_key).await?;

                // Read while the asset row is held: a materialization that
                // commits after this makes the transaction retry.
                let chunk = match failed_run {
                    Some(run_id) => not_built_by(&self.db, asset_key, run_id, chunk).await?,
                    None => chunk.iter().collect(),
                };
                let run_end = failed_run.is_some();
                write_failures(&txn, cl, asset_key, &deletions, chunk, run_end).await?;
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
            let failures: Vec<(PartitionKey, i64, &str)> = members
                .iter()
                .filter(|member| done.is_none_or(|done| !done.contains(*member)))
                .map(|member| (member.clone(), at, run.run_id.as_str()))
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

    /// Bring the failure rows of the partitions of the run `run_id` in line
    /// with `change`. The failures by the run that change are read once;
    /// their keys then go in the order of the store, at most
    /// [`FAILURES_PER_TRANSACTION`] of one asset per transaction. A failure
    /// by the run that comes in after the read comes from an event after the
    /// change.
    pub(super) async fn repair_failures_of_run(
        &self,
        run_id: &str,
        change: RunChange,
    ) -> Result<()> {
        #[derive(SurrealValue)]
        struct OfRun {
            code_location_id: String,
            asset_key: String,
            partition_key: PartitionKey,
        }

        let (select, end) = match change {
            RunChange::Deleted => (FAILURES_OF_RUN, None),
            RunChange::FailedAt(end) => (FAILED_ENDS_OF_RUN, end),
        };
        let rows: Vec<OfRun> = retry::with_retry(&self.retry_config, || async {
            Ok(self
                .db
                .query(select)
                .bind(("run_id", run_id.to_string()))
                .bind(("end", end))
                .await?
                .check()?
                .take(0)?)
        })
        .await?;
        let mut keys_of: HashMap<(String, String), Vec<PartitionKey>> = HashMap::new();
        for row in rows {
            keys_of
                .entry((row.code_location_id, row.asset_key))
                .or_default()
                .push(row.partition_key);
        }

        let retry_config = self.retry_config.contended_batch();
        for ((cl, asset_key), mut keys) in keys_of {
            keys.sort_by(stored_order);
            for chunk in keys.chunks(FAILURES_PER_TRANSACTION) {
                retry::with_retry(&retry_config, || async {
                    let txn = self.db.clone().begin().await?;
                    let deletions = hold_asset(&txn, &cl, &asset_key).await?;
                    let changed =
                        refresh_partitions(&txn, &cl, &asset_key, Some(deletions), chunk, |row| {
                            change.apply(run_id, row)
                        })
                        .await?;
                    if changed {
                        txn.commit().await?;
                    } else {
                        txn.cancel().await?;
                    }
                    Ok(())
                })
                .await?;
            }
        }
        Ok(())
    }

    /// After a write of the status of the run `run_id`: bring the failure
    /// rows of its partitions in line with `stored`, the status that the
    /// store then holds, when the run is stored. A failed run fails them at
    /// its end time, else its start time; any other run not by its end. It
    /// follows the stored status, not the one before the write, so a write
    /// done again after a stop repairs too. With nothing to change, it is one
    /// read of the run's failures: each write of a status pays it.
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

/// The failure of a partition by one run: the run's latest keyed step
/// failure of the partition, and the end of the run in failure when it fails
/// the partition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RunFailure {
    pub(super) step_failed_at: Option<i64>,
    pub(super) run_failed_at: Option<i64>,
}

impl RunFailure {
    /// The time of the failure: the later of the two.
    pub(super) fn at(&self) -> Option<i64> {
        self.step_failed_at.max(self.run_failed_at)
    }
}

/// A code location and an asset key.
type AssetOf = (String, String);

/// The failures of one partition, by run.
pub(super) type FailuresByRun = HashMap<String, RunFailure>;

/// The failures of each asset, by partition and run.
pub(super) type DerivedFailures = HashMap<AssetOf, HashMap<PartitionKey, FailuresByRun>>;

/// The order of the failures of one partition: the later one is greater,
/// and of two at the same time, the one of the run that sorts first.
fn rank(at: i64, run_id: &str) -> (i64, Reverse<&str>) {
    (at, Reverse(run_id))
}

/// The run and the time of the latest of `failures`.
fn latest(failures: &FailuresByRun) -> Option<(&str, i64)> {
    failures
        .iter()
        .filter_map(|(run_id, failure)| Some((run_id.as_str(), failure.at()?)))
        .max_by_key(|&(run_id, at)| rank(at, run_id))
}

/// The failures of each partition (each member) of each asset, by run,
/// unless a deletion at or after a failure replaced it. A later
/// materialization is not compared here.
///
/// A failure is a keyed `StepFailure` event of a run that is not an action
/// run. The end in failure of a run with a partition that is not an action
/// run is a failure too, at the end of the run: of each member of its
/// partition, for each asset of the run, unless the run materialized it.
pub(super) async fn derive_failures(db: &Surreal<Any>) -> Result<DerivedFailures> {
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

    // One scan of the runs serves both kinds of run.
    let runs: Vec<RunRow> = db
        .query(
            "SELECT run_id, code_location_id, status, action, partition_key, node_names, \
             start_time, end_time FROM runs WHERE action IS NOT NONE OR status = 'Failure'",
        )
        .await?
        .check()?
        .take(0)?;

    // A failed action did not fail to materialize anything. Its keyed
    // StepFailure events stay in storage for the backfill accounting.
    let action_runs: HashSet<&str> = runs
        .iter()
        .filter(|run| run.action.is_some())
        .map(|run| run.run_id.as_str())
        .collect();

    let mut unknown = 0;
    let mut failures: DerivedFailures = HashMap::new();

    let step_failures: Vec<StepFailureRow> = db
        .query(
            "SELECT code_location_id, asset_key, partition_key, run_id, \
             math::max(timestamp) AS at FROM events \
             WHERE asset_key IS NOT NONE AND event_type = 'StepFailure' \
             AND partition_key IS NOT NONE \
             GROUP BY code_location_id, asset_key, partition_key, run_id",
        )
        .await?
        .check()?
        .take(0)?;
    for row in step_failures {
        if action_runs.contains(row.run_id.as_str()) {
            continue;
        }
        let Some(partition_key) = stored_key(row.partition_key, &mut unknown) else {
            continue;
        };
        let of_asset = failures
            .entry((row.code_location_id, row.asset_key))
            .or_default();
        for member in partition_key.members() {
            let failure = of_asset
                .entry(member)
                .or_default()
                .entry(row.run_id.clone())
                .or_default();
            failure.step_failed_at = failure.step_failed_at.max(Some(row.at));
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
    let built = materialized_by_runs(db, None, &failed_run_ids).await?;

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
        for asset_key in &run.node_names {
            let done = built.get(&(asset_key.clone(), run.run_id.clone()));
            let of_asset = failures
                .entry((run.code_location_id.clone(), asset_key.clone()))
                .or_default();
            for member in &members {
                if done.is_some_and(|done| done.contains(member)) {
                    continue;
                }
                let failure = of_asset
                    .entry(member.clone())
                    .or_default()
                    .entry(run.run_id.clone())
                    .or_default();
                failure.run_failed_at = Some(at);
            }
        }
    }

    // A deletion replaces older failures the same way a newer
    // materialization does, the deletion of the whole asset for every key.
    // Read off the tombstones, which outlive the delete run's events.
    let mut result = db
        .query(
            "SELECT code_location_id, asset_key, partition_key, timestamp AS at \
             FROM asset_partition_deletions; \
             SELECT code_location_id, asset_key, last_deletion_timestamp AS at FROM assets \
             WHERE last_deletion_timestamp IS NOT NONE;",
        )
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

    for (asset, of_asset) in failures.iter_mut() {
        let deleted_of_asset = deleted.get(asset);
        let whole = asset_deleted.get(asset).copied();
        of_asset.retain(|member, by_run| {
            let deleted_at = deleted_of_asset
                .and_then(|deleted| deleted.get(member))
                .copied()
                .max(whole);
            by_run.retain(|_, failure| {
                failure
                    .at()
                    .is_some_and(|at| deleted_at.is_none_or(|deleted_at| deleted_at < at))
            });
            !by_run.is_empty()
        });
    }
    failures.retain(|_, of_asset| !of_asset.is_empty());
    if unknown > 0 {
        tracing::warn!(
            target: "rivers::storage",
            rows = unknown,
            "rows whose partition key has an unknown form are left out of the failed partitions"
        );
    }
    Ok(failures)
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
async fn not_built_by<'a>(
    db: &Surreal<Any>,
    asset_key: &str,
    run_id: &str,
    failures: &'a [(PartitionKey, i64, &'a str)],
) -> Result<Vec<&'a (PartitionKey, i64, &'a str)>> {
    let built = materialized_by_runs(db, Some(asset_key), &[run_id.to_string()]).await?;
    let done = built.get(&(asset_key.to_string(), run_id.to_string()));
    Ok(failures
        .iter()
        .filter(|(key, _, _)| done.is_none_or(|done| !done.contains(key)))
        .collect())
}

/// The rows of the derived failures of one asset: the failures by run that
/// are newer than the partition's materialization, and the failure row of
/// the latest of them for each partition.
async fn rows_of_derived(
    db: &Surreal<Any>,
    cl: &str,
    asset_key: &str,
    failures: HashMap<PartitionKey, FailuresByRun>,
) -> Result<(Vec<DbPartitionFailure>, Vec<DbPartitionRunFailure>)> {
    let mut failures: Vec<(PartitionKey, FailuresByRun)> = failures.into_iter().collect();
    failures.sort_by(|a, b| stored_order(&a.0, &b.0));
    let keys: Vec<PartitionKey> = failures.iter().map(|(key, _)| key.clone()).collect();
    let built = built_at_of(db, cl, asset_key, &keys).await?;

    let (mut rows, mut run_rows) = (Vec::new(), Vec::new());
    for (key, mut by_run) in failures {
        let built_at = built.get(&key).copied();
        by_run.retain(|_, failure| {
            failure
                .at()
                .is_some_and(|at| built_at.is_none_or(|built| built < at))
        });
        let Some((run_id, at)) = latest(&by_run).map(|(run_id, at)| (run_id.to_string(), at))
        else {
            continue;
        };
        rows.push(DbPartitionFailure {
            code_location_id: cl.to_string(),
            asset_key: asset_key.to_string(),
            partition_key: key.clone(),
            failed_at: at,
            run_id,
            built_at,
            has_row: built_at.is_some(),
        });
        run_rows.extend(
            by_run
                .into_iter()
                .map(|(run_id, failure)| DbPartitionRunFailure {
                    code_location_id: cl.to_string(),
                    asset_key: asset_key.to_string(),
                    partition_key: key.clone(),
                    run_id,
                    step_failed_at: failure.step_failed_at,
                    run_failed_at: failure.run_failed_at,
                }),
        );
    }
    Ok((rows, run_rows))
}

/// Fill `asset_partition_failures` and `asset_partition_run_failures` from
/// the events and the runs, by the rules the readers derived the failures
/// with before the tables.
pub(super) async fn fill_partition_failures(db: &Surreal<Any>) -> Result<()> {
    db.query("DELETE asset_partition_failures; DELETE asset_partition_run_failures;")
        .await?
        .check()?;
    let derived = derive_failures(db).await?;

    for ((cl, asset_key), failures) in derived {
        let (rows, run_rows) = rows_of_derived(db, &cl, &asset_key, failures).await?;
        for chunk in rows.chunks(FAILURES_PER_TRANSACTION) {
            db.query(PUT_PARTITION_FAILURES)
                .bind(("rows", chunk.to_vec()))
                .await?
                .check()?;
        }
        for chunk in run_rows.chunks(FAILURES_PER_TRANSACTION) {
            db.query(PUT_RUN_FAILURES)
                .bind(("run_rows", chunk.to_vec()))
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
    let failures = derive_failures(db)
        .await?
        .remove(&(cl.to_string(), asset_key.to_string()))
        .unwrap_or_default();
    let (rows, _) = rows_of_derived(db, cl, asset_key, failures).await?;
    let mut rows: Vec<PartitionFailure> = rows
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

/// A failure by run as the tests compare it.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunFailureRow {
    pub(super) key: PartitionKey,
    pub(super) run_id: String,
    pub(super) step_failed_at: Option<i64>,
    pub(super) run_failed_at: Option<i64>,
}

/// The failures by run of one asset that the writers keep, and those that
/// the derivation gives that are newer than the partition's materialization
/// and deletions, in the order of the store. A time at or before those reads
/// as none in both: it fails nothing, now or later, as they only move later.
#[cfg(test)]
pub(super) async fn run_failure_rows(
    db: &Surreal<Any>,
    cl: &str,
    asset_key: &str,
) -> Result<(Vec<RunFailureRow>, Vec<RunFailureRow>)> {
    #[derive(SurrealValue)]
    struct Kept {
        partition_key: PartitionKey,
        run_id: String,
        step_failed_at: Option<i64>,
        run_failed_at: Option<i64>,
    }

    let of_asset = "WHERE code_location_id = $cl AND asset_key = $asset_key";
    let mut result = db
        .query(format!(
            "{} {of_asset}; {} {of_asset}; \
             SELECT VALUE last_deletion_timestamp FROM assets {of_asset}; \
             SELECT partition_key, run_id, step_failed_at, run_failed_at \
             FROM asset_partition_run_failures {of_asset};",
            BuiltRow::SELECT,
            DeletedRow::SELECT
        ))
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .await?
        .check()?;
    let built: Vec<BuiltRow> = result.take(0)?;
    let tombstones: Vec<DeletedRow> = result.take(1)?;
    let asset_deleted: Option<i64> = result
        .take::<Vec<Option<i64>>>(2)?
        .into_iter()
        .flatten()
        .max();
    let kept: Vec<Kept> = result.take(3)?;

    let mut replaced: HashMap<PartitionKey, i64> = HashMap::new();
    let deleted = tombstones.into_iter().flat_map(|row| {
        row.partition_key
            .members()
            .into_iter()
            .map(move |member| (member, row.timestamp))
    });
    let built = built
        .into_iter()
        .filter_map(|row| Some((row.partition_key, row.last_timestamp?)));
    for (key, at) in deleted.chain(built) {
        let held = replaced.entry(key).or_insert(at);
        *held = (*held).max(at);
    }
    let newer = |key: &PartitionKey, at: Option<i64>| {
        let replaced_at = replaced.get(key).copied().max(asset_deleted);
        at.filter(|at| replaced_at.is_none_or(|replaced_at| replaced_at < *at))
    };
    let row = |key: PartitionKey, run_id: String, failure: RunFailure| RunFailureRow {
        step_failed_at: newer(&key, failure.step_failed_at),
        run_failed_at: newer(&key, failure.run_failed_at),
        key,
        run_id,
    };
    let order = |a: &RunFailureRow, b: &RunFailureRow| {
        stored_order(&a.key, &b.key).then_with(|| a.run_id.cmp(&b.run_id))
    };

    let mut kept: Vec<RunFailureRow> = kept
        .into_iter()
        .map(|kept| {
            let failure = RunFailure {
                step_failed_at: kept.step_failed_at,
                run_failed_at: kept.run_failed_at,
            };
            row(kept.partition_key, kept.run_id, failure)
        })
        .collect();
    kept.sort_by(order);

    let derived = derive_failures(db)
        .await?
        .remove(&(cl.to_string(), asset_key.to_string()))
        .unwrap_or_default();
    let mut derived: Vec<RunFailureRow> = derived
        .into_iter()
        .flat_map(|(key, by_run)| {
            by_run
                .into_iter()
                .map(move |(run_id, failure)| (key.clone(), run_id, failure))
        })
        .map(|(key, run_id, failure)| row(key, run_id, failure))
        .filter(|row| row.step_failed_at.is_some() || row.run_failed_at.is_some())
        .collect();
    derived.sort_by(order);
    Ok((kept, derived))
}
