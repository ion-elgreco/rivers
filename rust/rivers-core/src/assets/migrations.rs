//! The migration loop (RFC-048 §5). A daemon moves the stored data of each
//! asset of its code location from the applied version to the version that
//! its code declares, one version at a time: `pending` → `draining` →
//! `running` → `ready`.
//!
//! The daemon that takes a migration out of `pending` holds it with a lease,
//! and renews the lease while it works. When the lease runs out, another
//! daemon of the same code takes over. Each change of a row is a
//! compare-and-set on its revision, so two daemons never both move it.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Result, bail};

use super::versions::{CodeVersions, DeclaredMigration, registered_version};
use crate::storage::{
    AssetVersionHistoryEntry, AssetVersionRecord, AssetVersionState, BackfillStatus, EventRecord,
    EventType, MigrationKind, PartitionKey, RunRecord, RunStatus, ScopedStorage,
    ScopedStorageHandle, StorageBackend,
};
use crate::util::now_ts;

/// How long a daemon holds a migration after it last renewed its lease, in
/// nanoseconds.
pub const LEASE_NS: i64 = 60_000_000_000;

/// How many times [`retry_migration`] reads the row again when another
/// writer changed it first.
const RETRY_ATTEMPTS: usize = 10;

/// How many runs of a rebuild backfill run at once, as for the backfills of
/// automation conditions.
pub const REBUILD_CONCURRENCY: u32 = 4;

/// The work that moves the stored data of an asset by one version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationWork {
    /// A run of the built-in `migrate` verb on the asset: its convert.
    Convert { run_id: String },
    /// A run that materializes the asset again: the rebuild of an asset
    /// without partitions.
    RebuildRun { run_id: String },
    /// A backfill that materializes the partitions `keys` of the asset
    /// again.
    RebuildBackfill {
        backfill_id: String,
        keys: Vec<PartitionKey>,
    },
}

impl MigrationWork {
    fn ids(&self) -> (Option<String>, Option<String>) {
        match self {
            Self::Convert { run_id } | Self::RebuildRun { run_id } => (Some(run_id.clone()), None),
            Self::RebuildBackfill { backfill_id, .. } => (None, Some(backfill_id.clone())),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Convert { .. } => "convert",
            Self::RebuildRun { .. } | Self::RebuildBackfill { .. } => "rebuild",
        }
    }
}

/// Starts the work of a migration.
pub trait MigrationLauncher: Send + Sync {
    /// Start `work`, which moves the stored data of `asset_key` to version
    /// `to`. `Ok` once its run or backfill has a record, or once the caller
    /// of the pass will start the work before its next pass.
    fn launch(
        &self,
        asset_key: &str,
        to: u32,
        work: MigrationWork,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// What the loop does next with a migration.
enum Next {
    /// Write the row; once written, start the work, if any.
    Write(AssetVersionRecord, Option<(u32, MigrationWork)>),
    /// Nothing to do until the next pass.
    Wait,
}

/// The work that took the stored data to a version, for its history row.
#[derive(Default)]
struct Done {
    run_id: Option<String>,
    backfill_id: Option<String>,
    filled: Option<u64>,
    failed: Option<u64>,
}

/// The part of one daemon in the migrations of its code location.
pub struct MigrationLoop {
    /// The daemon, as the owner of the migrations that it holds.
    owner: String,
    code: Arc<CodeVersions>,
    /// The assets whose migrations the loop moves: all of the code's when
    /// `None`.
    only: Option<HashSet<String>>,
}

impl MigrationLoop {
    pub fn new(owner: String, code: Arc<CodeVersions>) -> Self {
        Self {
            owner,
            code,
            only: None,
        }
    }

    /// The same loop, for the migrations of `keys` only.
    pub fn only(self, keys: HashSet<String>) -> Self {
        Self {
            only: Some(keys),
            ..self
        }
    }

    /// Move each migration of an asset that this code owns as far as it can
    /// go now. `Ok(true)` while one of them is still on its way. An error
    /// with one asset is logged, and the pass goes on with the others.
    pub async fn pass<S: StorageBackend, L: MigrationLauncher>(
        &self,
        storage: &ScopedStorageHandle<S>,
        launcher: &L,
    ) -> Result<bool> {
        let rows = storage.scoped().get_asset_versions(None).await?;
        let mut on_its_way = false;
        for row in rows {
            if let Some(only) = &self.only
                && !only.contains(&row.asset_key)
            {
                continue;
            }
            let key = row.asset_key.clone();
            match self.advance(storage, launcher, row).await {
                Ok(moving) => on_its_way |= moving,
                Err(e) => {
                    on_its_way = true;
                    tracing::warn!(
                        target: "rivers::migrations",
                        asset = %key,
                        error = %format!("{e:#}"),
                        "the migration did not move on; the next pass tries again"
                    );
                }
            }
        }
        Ok(on_its_way)
    }

    /// Let go of each migration that this loop holds, so that the next pass
    /// of any loop takes it on at once: for a loop that stops before its
    /// migrations end. Each migration stays in its state.
    pub async fn release<S: StorageBackend>(&self, storage: &ScopedStorageHandle<S>) -> Result<()> {
        for row in storage.scoped().get_asset_versions(None).await? {
            if row.owner.as_deref() != Some(self.owner.as_str()) {
                continue;
            }
            let free = AssetVersionRecord {
                owner: None,
                lease_expires_at: None,
                ..row.clone()
            };
            // When another writer changed the row, this loop holds it no longer.
            storage
                .scoped()
                .put_asset_version(&free, Some(row.revision))
                .await?;
        }
        Ok(())
    }

    /// Move the migration in `row` on until it must wait. `Ok(true)` while it
    /// is still on its way.
    async fn advance<S: StorageBackend, L: MigrationLauncher>(
        &self,
        storage: &ScopedStorageHandle<S>,
        launcher: &L,
        mut row: AssetVersionRecord,
    ) -> Result<bool> {
        let Some(declared) = self.code.declared(&row.asset_key) else {
            return Ok(false);
        };
        loop {
            let now = now_ts();
            match self.next(storage, &row, declared, now).await? {
                Next::Wait => return Ok(on_its_way(&row, declared)),
                Next::Write(next, work) => {
                    // Another writer changed the row: the next pass reads it again.
                    if !storage
                        .scoped()
                        .put_asset_version(&next, Some(row.revision))
                        .await?
                    {
                        return Ok(true);
                    }
                    self.log_change(&row, &next);
                    row = AssetVersionRecord {
                        revision: row.revision + 1,
                        updated_at: now,
                        ..next
                    };
                    if let Some((to, work)) = work {
                        self.start(storage, launcher, &row, to, work).await?;
                        return Ok(true);
                    }
                }
            }
        }
    }

    async fn next<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        declared: u32,
        now: i64,
    ) -> Result<Next> {
        let target = row.target_version.unwrap_or(row.applied_version);
        match row.state {
            AssetVersionState::Ready => Ok(registered_version(row, declared)
                .map_or(Next::Wait, |pending| Next::Write(pending, None))),
            AssetVersionState::Failed | AssetVersionState::Blocked => Ok(Next::Wait),
            // The migration of newer code, or one that another daemon holds.
            _ if target > declared || self.held_by_other(row, now) => Ok(Next::Wait),
            AssetVersionState::Pending => Ok(Next::Write(
                self.hold(
                    AssetVersionRecord {
                        state: AssetVersionState::Draining,
                        reason: None,
                        started_at: None,
                        run_id: None,
                        backfill_id: None,
                        ..row.clone()
                    },
                    now,
                ),
                None,
            )),
            AssetVersionState::Draining => match self.drain_reason(storage, row).await? {
                Some(reason) => {
                    tracing::debug!(
                        target: "rivers::migrations",
                        asset = %row.asset_key,
                        reason = %reason,
                        "the migration waits"
                    );
                    Ok(self.renewed(row, now))
                }
                None => Ok(Next::Write(
                    self.hold(
                        AssetVersionRecord {
                            state: AssetVersionState::Running,
                            started_at: Some(now),
                            run_id: None,
                            backfill_id: None,
                            ..row.clone()
                        },
                        now,
                    ),
                    None,
                )),
            },
            AssetVersionState::Running => self.run(storage, row, declared, now).await,
        }
    }

    /// The next version step of a running migration.
    async fn run<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        declared: u32,
        now: i64,
    ) -> Result<Next> {
        let to = row.applied_version + 1;
        if to > row.target_version.unwrap_or(row.applied_version) {
            return Ok(self.finished(row, declared));
        }
        let Some(migration) = self.code.migration(&row.asset_key, to) else {
            return Ok(Next::Write(
                failed(row, format!("no migration to version {to}")),
                None,
            ));
        };
        match migration.kind {
            MigrationKind::VersionOnly => {
                self.applied(storage, row, migration, Done::default(), now)
                    .await
            }
            MigrationKind::Convert => self.convert(storage, row, migration, now).await,
            MigrationKind::Rebuild if self.code.is_partitioned(&row.asset_key) => {
                self.rebuild_partitions(storage, row, migration, now).await
            }
            MigrationKind::Rebuild => self.rebuild(storage, row, migration, now).await,
        }
    }

    async fn convert<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        migration: DeclaredMigration,
        now: i64,
    ) -> Result<Next> {
        let to = migration.to;
        let Some(run_id) = row.run_id.clone() else {
            let run_id = new_id();
            return Ok(self.launch(row, to, MigrationWork::Convert { run_id }, now));
        };
        // Without a record, the work never started: start it again, with its id.
        let Some(run) = storage.backend().get_run(&run_id).await? else {
            return Ok(self.launch(row, to, MigrationWork::Convert { run_id }, now));
        };
        match run.status {
            RunStatus::Success => {
                let done = Done {
                    run_id: Some(run_id),
                    ..Done::default()
                };
                self.applied(storage, row, migration, done, now).await
            }
            RunStatus::Failure | RunStatus::Canceled => {
                let reason =
                    run_failure(storage, &run, &format!("the convert to version {to}")).await?;
                Ok(Next::Write(failed(row, reason), None))
            }
            RunStatus::Queued | RunStatus::NotStarted | RunStatus::Started => {
                Ok(self.renewed(row, now))
            }
        }
    }

    /// The rebuild of an asset without partitions: one run. When the run
    /// fails, the stored data goes, so that no reader reads data of the
    /// version before.
    async fn rebuild<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        migration: DeclaredMigration,
        now: i64,
    ) -> Result<Next> {
        let to = migration.to;
        let Some(run_id) = row.run_id.clone() else {
            let records = storage
                .scoped()
                .get_asset_records_by_keys(std::slice::from_ref(&row.asset_key))
                .await?;
            if !records.iter().any(|record| record.last_timestamp.is_some()) {
                let done = Done {
                    filled: Some(0),
                    failed: Some(0),
                    ..Done::default()
                };
                return self.applied(storage, row, migration, done, now).await;
            }
            let run_id = new_id();
            return Ok(self.launch(row, to, MigrationWork::RebuildRun { run_id }, now));
        };
        let Some(run) = storage.backend().get_run(&run_id).await? else {
            return Ok(self.launch(row, to, MigrationWork::RebuildRun { run_id }, now));
        };
        let filled = match run.status {
            RunStatus::Success => 1,
            RunStatus::Failure | RunStatus::Canceled => {
                delete_older(storage, &row.asset_key, &run_id, run.start_time).await?;
                0
            }
            RunStatus::Queued | RunStatus::NotStarted | RunStatus::Started => {
                return Ok(self.renewed(row, now));
            }
        };
        let done = Done {
            run_id: Some(run_id),
            filled: Some(filled),
            failed: Some(1 - filled),
            ..Done::default()
        };
        self.applied(storage, row, migration, done, now).await
    }

    /// The rebuild of a partitioned asset: a backfill over its materialized
    /// partitions. Partitions that it did not write go when it ends, so that
    /// no reader reads data of the version before; failed keys stay failed.
    async fn rebuild_partitions<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        migration: DeclaredMigration,
        now: i64,
    ) -> Result<Next> {
        let to = migration.to;
        let nothing_stored = Done {
            filled: Some(0),
            failed: Some(0),
            ..Done::default()
        };
        let Some(backfill_id) = row.backfill_id.clone() else {
            let keys = storage
                .scoped()
                .get_materialized_partitions(&row.asset_key)
                .await?;
            if keys.is_empty() {
                return self
                    .applied(storage, row, migration, nothing_stored, now)
                    .await;
            }
            let backfill_id = new_id();
            let work = MigrationWork::RebuildBackfill { backfill_id, keys };
            return Ok(self.launch(row, to, work, now));
        };
        let Some(backfill) = storage.backend().get_backfill(&backfill_id).await? else {
            let keys = storage
                .scoped()
                .get_materialized_partitions(&row.asset_key)
                .await?;
            if keys.is_empty() {
                return self
                    .applied(storage, row, migration, nothing_stored, now)
                    .await;
            }
            let work = MigrationWork::RebuildBackfill { backfill_id, keys };
            return Ok(self.launch(row, to, work, now));
        };
        if matches!(
            backfill.status,
            BackfillStatus::Requested | BackfillStatus::InProgress
        ) {
            return Ok(self.renewed(row, now));
        }
        let filled = backfill.completed_partitions;
        let unfilled = backfill.num_partitions.saturating_sub(filled);
        if unfilled > 0 {
            delete_older(storage, &row.asset_key, &backfill_id, backfill.create_time).await?;
        }
        let done = Done {
            backfill_id: Some(backfill_id),
            filled: Some(filled),
            failed: Some(unfilled),
            ..Done::default()
        };
        self.applied(storage, row, migration, done, now).await
    }

    /// Record that the stored data of `row` reached the version of
    /// `migration` through `done`, and move the row to that version.
    async fn applied<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
        migration: DeclaredMigration,
        done: Done,
        now: i64,
    ) -> Result<Next> {
        storage
            .scoped()
            .add_asset_version_history(&AssetVersionHistoryEntry {
                code_location_id: storage.code_location_id().to_string(),
                asset_key: row.asset_key.clone(),
                version: migration.to,
                kind: migration.kind,
                breaking: migration.breaking,
                run_id: done.run_id,
                backfill_id: done.backfill_id,
                filled: done.filled,
                failed: done.failed,
                finished_at: now,
            })
            .await?;
        Ok(Next::Write(
            self.hold(
                AssetVersionRecord {
                    applied_version: migration.to,
                    run_id: None,
                    backfill_id: None,
                    ..row.clone()
                },
                now,
            ),
            None,
        ))
    }

    /// The row once its data is at the target: `ready`, or `pending` for a
    /// higher version that this code declares.
    fn finished(&self, row: &AssetVersionRecord, declared: u32) -> Next {
        let ready = AssetVersionRecord {
            state: AssetVersionState::Ready,
            target_version: None,
            reason: None,
            started_at: None,
            run_id: None,
            backfill_id: None,
            owner: None,
            lease_expires_at: None,
            ..row.clone()
        };
        Next::Write(registered_version(&ready, declared).unwrap_or(ready), None)
    }

    /// Hold the row with the id of `work` on it, then start the work. Of the
    /// loops that read the row, only the one that writes it starts the work.
    fn launch(&self, row: &AssetVersionRecord, to: u32, work: MigrationWork, now: i64) -> Next {
        let (run_id, backfill_id) = work.ids();
        let next = AssetVersionRecord {
            run_id,
            backfill_id,
            ..row.clone()
        };
        Next::Write(self.hold(next, now), Some((to, work)))
    }

    /// Start `work`. When it cannot start, the migration fails with the
    /// reason.
    async fn start<S: StorageBackend, L: MigrationLauncher>(
        &self,
        storage: &ScopedStorageHandle<S>,
        launcher: &L,
        row: &AssetVersionRecord,
        to: u32,
        work: MigrationWork,
    ) -> Result<()> {
        let name = work.name();
        tracing::info!(
            target: "rivers::migrations",
            asset = %row.asset_key,
            to_version = to,
            work = ?work.ids(),
            "starting the {name}"
        );
        let Err(e) = launcher.launch(&row.asset_key, to, work).await else {
            return Ok(());
        };
        let next = failed(
            row,
            format!("the {name} to version {to} did not start: {e:#}"),
        );
        if storage
            .scoped()
            .put_asset_version(&next, Some(row.revision))
            .await?
        {
            self.log_change(row, &next);
        }
        Ok(())
    }

    /// Why the migration in `row` cannot run yet: an asset that it reads
    /// migrates, or a started run touches the asset or a reader of it.
    async fn drain_reason<S: StorageBackend>(
        &self,
        storage: &ScopedStorageHandle<S>,
        row: &AssetVersionRecord,
    ) -> Result<Option<String>> {
        let reads = self.code.reads(&row.asset_key);
        if !reads.is_empty() {
            let upstream = storage.scoped().get_asset_versions(Some(reads)).await?;
            if let Some(up) = upstream.iter().find(|up| self.migrates(up)) {
                return Ok(Some(format!("{} migrates first", up.asset_key)));
            }
        }
        let runs = drain_runs(storage, &self.code, row).await?;
        Ok(runs
            .first()
            .map(|run| format!("run {} is started", run.run_id)))
    }

    /// Whether the stored data of `row` is not at the version that this code
    /// declares.
    fn migrates(&self, row: &AssetVersionRecord) -> bool {
        row.state != AssetVersionState::Ready
            || self
                .code
                .declared(&row.asset_key)
                .is_some_and(|declared| declared > row.applied_version)
    }

    fn held_by_other(&self, row: &AssetVersionRecord, now: i64) -> bool {
        row.owner
            .as_deref()
            .is_some_and(|owner| owner != self.owner)
            && row.lease_expires_at.is_some_and(|until| until > now)
    }

    /// `row`, held by this daemon until a lease from `now` runs out.
    fn hold(&self, row: AssetVersionRecord, now: i64) -> AssetVersionRecord {
        AssetVersionRecord {
            owner: Some(self.owner.clone()),
            lease_expires_at: Some(now + LEASE_NS),
            ..row
        }
    }

    /// For a migration that waits: renew the lease when half of it is gone,
    /// or take the migration over.
    fn renewed(&self, row: &AssetVersionRecord, now: i64) -> Next {
        let held = row.owner.as_deref() == Some(self.owner.as_str())
            && row
                .lease_expires_at
                .is_some_and(|until| until - now > LEASE_NS / 2);
        if held {
            Next::Wait
        } else {
            Next::Write(self.hold(row.clone(), now), None)
        }
    }

    fn log_change(&self, before: &AssetVersionRecord, after: &AssetVersionRecord) {
        if let Some(owner) = &before.owner
            && before.owner != after.owner
            && after.owner.as_deref() == Some(self.owner.as_str())
        {
            tracing::warn!(
                target: "rivers::migrations",
                asset = %after.asset_key,
                previous_owner = %owner,
                "took over the migration: the lease of its daemon ran out"
            );
        }
        if before.state == after.state && before.applied_version == after.applied_version {
            return;
        }
        if after.state == AssetVersionState::Failed {
            tracing::warn!(
                target: "rivers::migrations",
                asset = %after.asset_key,
                applied_version = after.applied_version,
                target_version = ?after.target_version,
                reason = after.reason.as_deref().unwrap_or_default(),
                "the migration failed"
            );
        } else {
            tracing::info!(
                target: "rivers::migrations",
                asset = %after.asset_key,
                state = after.state.as_str(),
                applied_version = after.applied_version,
                target_version = ?after.target_version,
                "the migration moved on"
            );
        }
    }
}

/// The started runs that the drain of the migration in `row` waits for: the
/// runs of its code location that touch the asset or a reader of it, other
/// than the migration's own work. A run without node names touches every
/// asset.
pub async fn drain_runs<S: StorageBackend>(
    storage: &ScopedStorageHandle<S>,
    code: &CodeVersions,
    row: &AssetVersionRecord,
) -> Result<Vec<RunRecord>> {
    let touching = code.touching(&row.asset_key);
    let runs = storage.backend().get_in_progress_runs().await?;
    Ok(runs
        .into_iter()
        .filter(|run| {
            run.code_location_id == storage.code_location_id()
                && run.status == RunStatus::Started
                && !is_own_work(row, run)
                && (run.node_names.is_empty()
                    || run
                        .node_names
                        .iter()
                        .any(|name| touching.contains(name.as_str())))
        })
        .collect())
}

/// Whether `run` is the convert or rebuild of the migration in `row`.
fn is_own_work(row: &AssetVersionRecord, run: &RunRecord) -> bool {
    row.run_id.as_deref() == Some(run.run_id.as_str())
        || row
            .backfill_id
            .as_deref()
            .is_some_and(|id| run.launched_by.backfill_id() == Some(id))
}

/// Run a failed migration again: `failed` → `draining`, held by no daemon.
/// `Ok(false)` when the migration of `asset_key` has not failed.
pub async fn retry_migration<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
) -> Result<bool> {
    for _ in 0..RETRY_ATTEMPTS {
        let Some(row) = storage
            .get_asset_versions(Some(&[asset_key.to_string()]))
            .await?
            .pop()
        else {
            return Ok(false);
        };
        if row.state != AssetVersionState::Failed {
            return Ok(false);
        }
        let next = AssetVersionRecord {
            state: AssetVersionState::Draining,
            reason: None,
            started_at: None,
            run_id: None,
            backfill_id: None,
            owner: None,
            lease_expires_at: None,
            ..row.clone()
        };
        if storage.put_asset_version(&next, Some(row.revision)).await? {
            return Ok(true);
        }
    }
    bail!("asset '{asset_key}': other writers changed its version row {RETRY_ATTEMPTS} times")
}

/// Stop a migration that has not started to run: `pending` or `draining` →
/// `ready` at the applied version. `Ok(false)` when the migration of
/// `asset_key` is not pending or draining. A daemon whose code declares a
/// higher version starts the migration again.
pub async fn cancel_migration<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
) -> Result<bool> {
    for _ in 0..RETRY_ATTEMPTS {
        let Some(row) = storage
            .get_asset_versions(Some(&[asset_key.to_string()]))
            .await?
            .pop()
        else {
            return Ok(false);
        };
        if !matches!(
            row.state,
            AssetVersionState::Pending | AssetVersionState::Draining
        ) {
            return Ok(false);
        }
        let next = AssetVersionRecord {
            state: AssetVersionState::Ready,
            target_version: None,
            reason: None,
            started_at: None,
            run_id: None,
            backfill_id: None,
            owner: None,
            lease_expires_at: None,
            ..row.clone()
        };
        if storage.put_asset_version(&next, Some(row.revision)).await? {
            return Ok(true);
        }
    }
    bail!("asset '{asset_key}': other writers changed its version row {RETRY_ATTEMPTS} times")
}

/// Whether the migration in `row` is on its way to a version that the code,
/// which declares `declared`, drives.
fn on_its_way(row: &AssetVersionRecord, declared: u32) -> bool {
    matches!(
        row.state,
        AssetVersionState::Pending | AssetVersionState::Draining | AssetVersionState::Running
    ) && row.target_version.is_some_and(|target| target <= declared)
}

/// `row`, failed with `reason`. It keeps the id of the work that failed, and
/// no daemon holds it.
fn failed(row: &AssetVersionRecord, reason: String) -> AssetVersionRecord {
    AssetVersionRecord {
        state: AssetVersionState::Failed,
        reason: Some(reason),
        owner: None,
        lease_expires_at: None,
        ..row.clone()
    }
}

/// Why the migration work `run` (`what`) failed: the error of its last
/// failure.
async fn run_failure<S: StorageBackend>(
    storage: &ScopedStorageHandle<S>,
    run: &RunRecord,
    what: &str,
) -> Result<String> {
    let run_id = &run.run_id;
    if run.status == RunStatus::Canceled {
        return Ok(format!("{what} was canceled in run {run_id}"));
    }
    let events = storage.backend().get_events_for_run(run_id).await?;
    let error = events
        .iter()
        .rev()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::StepFailure | EventType::RunLaunchFailed
            )
        })
        .find_map(|event| {
            event
                .metadata
                .iter()
                .find(|(key, _)| key == "error")
                .map(|(_, error)| error)
        });
    Ok(match error {
        Some(error) => format!("{what} failed in run {run_id}: {error}"),
        None => format!("{what} failed in run {run_id}"),
    })
}

/// Remove the state of each partition of `asset_key` that a rebuild did not
/// write: one whole-asset `Deletion` at `since`, the start of the rebuild,
/// under the id of the rebuild's run or backfill. Rows written later stay.
async fn delete_older<S: StorageBackend>(
    storage: &ScopedStorageHandle<S>,
    asset_key: &str,
    work_id: &str,
    since: i64,
) -> Result<()> {
    storage
        .backend()
        .store_event(&EventRecord {
            code_location_id: storage.code_location_id().to_string(),
            event_type: EventType::Deletion,
            asset_key: Some(asset_key.to_string()),
            run_id: work_id.to_string(),
            partition_key: None,
            timestamp: since,
            // As the executor writes the action of a `Deletion`.
            metadata: vec![(
                "action".to_string(),
                r#"{"Text":{"value":"migrate"}}"#.to_string(),
            )],
            input_data_versions: Vec::new(),
        })
        .await?;
    Ok(())
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use super::*;
    use crate::assets::graph::{AssetGraph, DagNode, NodeKind};
    use crate::assets::versions::first_version_row;
    use crate::storage::any::AnyStorage;
    use crate::storage::{
        AssetRecord, CodeLocationContext, DEFAULT_CODE_LOCATION_ID, LaunchedBy, RunRecord,
    };

    const CL: &str = DEFAULT_CODE_LOCATION_ID;

    /// Records each launch, and gives each run a `Started` record.
    struct Launches {
        storage: Arc<AnyStorage>,
        seen: Mutex<Vec<(String, u32, MigrationWork)>>,
        refuse: Option<&'static str>,
    }

    impl MigrationLauncher for Launches {
        async fn launch(&self, asset_key: &str, to: u32, work: MigrationWork) -> Result<()> {
            if let Some(error) = self.refuse {
                bail!("{error}");
            }
            if let MigrationWork::Convert { run_id } | MigrationWork::RebuildRun { run_id } = &work
            {
                let launched_by = LaunchedBy::Migration {
                    asset_key: asset_key.to_string(),
                    to_version: to,
                };
                let run = started_run(run_id, CL, &[asset_key], launched_by);
                self.storage.create_run(&run).await?;
            }
            self.seen
                .lock()
                .unwrap()
                .push((asset_key.to_string(), to, work));
            Ok(())
        }
    }

    impl Launches {
        fn taken(&self) -> Vec<(String, u32, MigrationWork)> {
            std::mem::take(&mut self.seen.lock().unwrap())
        }

        /// The id of the one run that the loop started since the last call.
        fn one_run(&self) -> String {
            match self.taken().as_slice() {
                [
                    (
                        _,
                        _,
                        MigrationWork::Convert { run_id } | MigrationWork::RebuildRun { run_id },
                    ),
                ] => run_id.clone(),
                other => panic!("expected one run, got {other:?}"),
            }
        }
    }

    struct Fixture {
        storage: Arc<AnyStorage>,
        handle: ScopedStorageHandle<AnyStorage>,
        launches: Launches,
    }

    impl Fixture {
        async fn new() -> Self {
            let storage = Arc::new(AnyStorage::new_memory().await.unwrap());
            Self {
                handle: ScopedStorageHandle::new(
                    Arc::clone(&storage),
                    CodeLocationContext::new(CL),
                ),
                launches: Launches {
                    storage: Arc::clone(&storage),
                    seen: Mutex::default(),
                    refuse: None,
                },
                storage,
            }
        }

        async fn put(&self, row: &AssetVersionRecord) {
            assert!(
                self.handle
                    .scoped()
                    .put_asset_version(row, None)
                    .await
                    .unwrap()
            );
        }

        async fn row(&self, key: &str) -> AssetVersionRecord {
            self.handle
                .scoped()
                .get_asset_versions(Some(&[key.to_string()]))
                .await
                .unwrap()
                .pop()
                .unwrap()
        }

        async fn pass(&self, daemon: &MigrationLoop) -> bool {
            daemon.pass(&self.handle, &self.launches).await.unwrap()
        }

        async fn end_run(&self, run_id: &str, status: RunStatus) {
            self.storage
                .update_run_status(run_id, status, Some(now_ts()))
                .await
                .unwrap();
        }

        async fn history(&self, key: &str) -> Vec<(u32, MigrationKind, Option<u64>, Option<u64>)> {
            self.handle
                .scoped()
                .get_asset_version_history(key)
                .await
                .unwrap()
                .into_iter()
                .map(|entry| (entry.version, entry.kind, entry.filled, entry.failed))
                .collect()
        }
    }

    /// `raw` ← `orders` ← `summary`: `summary` reads `orders`, which reads
    /// `raw`.
    fn graph() -> AssetGraph {
        let mut graph = AssetGraph::new();
        let mut node = |name: &str| {
            graph.add_node(DagNode {
                name: name.to_string(),
                kind: NodeKind::Asset,
                inputs: Vec::new(),
                outputs: Vec::new(),
                sub_graph: None,
            })
        };
        let raw = node("raw");
        let orders = node("orders");
        let summary = node("summary");
        graph.add_edge(orders, raw, ());
        graph.add_edge(summary, orders, ());
        graph
    }

    /// The loop of a daemon, `owner`, whose code declares each asset of
    /// `declared` at a version with its migrations, by kind from version 2.
    fn daemon(owner: &str, declared: &[(&str, &[MigrationKind])]) -> MigrationLoop {
        let versions = declared
            .iter()
            .map(|(key, steps)| (key.to_string(), 1 + steps.len() as u32))
            .collect();
        let migrations = declared
            .iter()
            .map(|(key, steps)| {
                let steps = (2..)
                    .zip(steps.iter())
                    .map(|(to, &kind)| DeclaredMigration {
                        to,
                        kind,
                        breaking: true,
                    })
                    .collect();
                (key.to_string(), steps)
            })
            .collect::<HashMap<_, _>>();
        let code =
            CodeVersions::new(&graph(), versions).with_migrations(migrations, HashSet::new());
        MigrationLoop::new(owner.to_string(), Arc::new(code))
    }

    fn pending(key: &str, applied: u32, target: u32) -> AssetVersionRecord {
        AssetVersionRecord {
            state: AssetVersionState::Pending,
            target_version: Some(target),
            ..first_version_row(CL, key, applied, false, None)
        }
    }

    fn started_run(
        run_id: &str,
        code_location_id: &str,
        node_names: &[&str],
        launched_by: LaunchedBy,
    ) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            code_location_id: code_location_id.to_string(),
            job_name: None,
            status: RunStatus::Started,
            start_time: now_ts(),
            end_time: None,
            execution_start_time: None,
            tags: Vec::new(),
            node_names: node_names.iter().map(|name| name.to_string()).collect(),
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by,
            action: None,
            config: None,
        }
    }

    use MigrationKind::{Convert, Rebuild, VersionOnly};

    #[tokio::test]
    async fn steps_that_only_record_the_version_apply_in_one_pass() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 3)).await;

        let on_its_way = f
            .pass(&daemon("a", &[("orders", &[VersionOnly, VersionOnly])]))
            .await;

        let row = f.row("orders").await;
        assert_eq!(
            (
                row.state,
                row.applied_version,
                row.target_version,
                row.owner
            ),
            (AssetVersionState::Ready, 3, None, None)
        );
        assert!(!on_its_way);
        assert_eq!(
            f.history("orders").await,
            [(2, VersionOnly, None, None), (3, VersionOnly, None, None)]
        );
        assert!(f.launches.taken().is_empty());
    }

    #[tokio::test]
    async fn a_convert_runs_once_and_its_success_applies_the_version() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let a = daemon("a", &[("orders", &[Convert])]);

        assert!(f.pass(&a).await);
        let run_id = f.launches.one_run();
        let row = f.row("orders").await;
        assert_eq!(row.state, AssetVersionState::Running);
        assert_eq!(row.run_id.as_deref(), Some(run_id.as_str()));
        assert_eq!(row.owner.as_deref(), Some("a"));
        assert!(row.started_at.is_some());

        // While the run is on its way, the loop starts nothing more.
        assert!(f.pass(&a).await);
        assert!(f.launches.taken().is_empty());

        f.end_run(&run_id, RunStatus::Success).await;
        assert!(!f.pass(&a).await);

        let row = f.row("orders").await;
        assert_eq!(
            (row.state, row.applied_version, row.run_id),
            (AssetVersionState::Ready, 2, None)
        );
        let history = f
            .handle
            .scoped()
            .get_asset_version_history("orders")
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].run_id.as_deref(), Some(run_id.as_str()));
        assert_eq!(history[0].kind, Convert);
    }

    #[tokio::test]
    async fn a_failed_convert_fails_the_migration_with_its_error_until_a_retry() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let a = daemon("a", &[("orders", &[Convert])]);
        f.pass(&a).await;
        let run_id = f.launches.one_run();
        f.storage
            .store_event(&EventRecord {
                code_location_id: CL.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("orders".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: now_ts(),
                metadata: vec![("error".to_string(), "the warehouse is down".to_string())],
                input_data_versions: Vec::new(),
            })
            .await
            .unwrap();
        f.end_run(&run_id, RunStatus::Failure).await;

        assert!(!f.pass(&a).await);
        let row = f.row("orders").await;
        assert_eq!(
            (row.state, row.applied_version),
            (AssetVersionState::Failed, 1)
        );
        assert_eq!(
            row.reason,
            Some(format!(
                "the convert to version 2 failed in run {run_id}: the warehouse is down"
            ))
        );
        assert_eq!(row.run_id.as_deref(), Some(run_id.as_str()));
        assert_eq!(row.owner, None);

        // A failed migration waits for a retry.
        assert!(!f.pass(&a).await);
        assert!(f.launches.taken().is_empty());

        assert!(retry_migration(&f.handle.scoped(), "orders").await.unwrap());
        assert!(!retry_migration(&f.handle.scoped(), "orders").await.unwrap());
        assert_eq!(f.row("orders").await.state, AssetVersionState::Draining);
        f.pass(&a).await;
        let retried = f.launches.one_run();
        assert_ne!(retried, run_id);
    }

    #[tokio::test]
    async fn a_migration_can_be_canceled_only_before_it_runs() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let rolled_back = daemon("a", &[("orders", &[])]);

        assert!(
            cancel_migration(&f.handle.scoped(), "orders")
                .await
                .unwrap()
        );

        let row = f.row("orders").await;
        assert_eq!(
            (row.state, row.applied_version, row.target_version),
            (AssetVersionState::Ready, 1, None)
        );
        assert!(!f.pass(&rolled_back).await);
        assert_eq!(f.row("orders").await.state, AssetVersionState::Ready);

        f.put(&pending("summary", 1, 2)).await;
        f.pass(&daemon("b", &[("summary", &[Convert])])).await;
        f.launches.one_run();
        assert_eq!(f.row("summary").await.state, AssetVersionState::Running);
        assert!(
            !cancel_migration(&f.handle.scoped(), "summary")
                .await
                .unwrap()
        );
        assert!(!cancel_migration(&f.handle.scoped(), "nope").await.unwrap());
    }

    #[tokio::test]
    async fn a_canceled_convert_fails_the_migration() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let a = daemon("a", &[("orders", &[Convert])]);
        f.pass(&a).await;
        let run_id = f.launches.one_run();
        f.end_run(&run_id, RunStatus::Canceled).await;

        f.pass(&a).await;

        assert_eq!(
            f.row("orders").await.reason,
            Some(format!(
                "the convert to version 2 was canceled in run {run_id}"
            ))
        );
    }

    #[tokio::test]
    async fn a_version_without_a_migration_fails() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 3)).await;
        let code = CodeVersions::new(&graph(), HashMap::from([("orders".to_string(), 3)]))
            .with_migrations(
                HashMap::from([(
                    "orders".to_string(),
                    vec![DeclaredMigration {
                        to: 3,
                        kind: VersionOnly,
                        breaking: true,
                    }],
                )]),
                HashSet::new(),
            );

        f.pass(&MigrationLoop::new("a".to_string(), Arc::new(code)))
            .await;

        let row = f.row("orders").await;
        assert_eq!(row.state, AssetVersionState::Failed);
        assert_eq!(row.reason.as_deref(), Some("no migration to version 2"));
    }

    #[tokio::test]
    async fn a_work_that_does_not_start_fails_the_migration() {
        let mut f = Fixture::new().await;
        f.launches.refuse = Some("the run queue is full");
        f.put(&pending("orders", 1, 2)).await;

        f.pass(&daemon("a", &[("orders", &[Convert])])).await;

        assert_eq!(
            f.row("orders").await.reason.as_deref(),
            Some("the convert to version 2 did not start: the run queue is full")
        );
    }

    #[tokio::test]
    async fn the_drain_waits_for_the_started_runs_of_the_asset_and_its_readers() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let runs = [
            ("summary-run", CL, "summary"),
            ("raw-run", CL, "raw"),
            ("elsewhere-run", "other-location", "summary"),
        ];
        for (run_id, location, asset) in runs {
            let run = started_run(run_id, location, &[asset], LaunchedBy::default());
            f.storage.create_run(&run).await.unwrap();
        }
        let a = daemon("a", &[("orders", &[Convert])]);

        assert!(f.pass(&a).await);
        assert_eq!(f.row("orders").await.state, AssetVersionState::Draining);
        assert!(f.launches.taken().is_empty());

        f.end_run("summary-run", RunStatus::Success).await;
        f.pass(&a).await;

        assert_eq!(f.row("orders").await.state, AssetVersionState::Running);
        f.launches.one_run();
    }

    #[tokio::test]
    async fn the_migration_of_an_asset_that_it_reads_runs_first() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        f.put(&pending("summary", 1, 2)).await;
        let a = daemon("a", &[("orders", &[Convert]), ("summary", &[Convert])]);

        f.pass(&a).await;
        let orders_run = f.launches.one_run();
        assert_eq!(f.row("summary").await.state, AssetVersionState::Draining);

        f.end_run(&orders_run, RunStatus::Success).await;
        f.pass(&a).await;

        assert_eq!(f.row("orders").await.state, AssetVersionState::Ready);
        assert_eq!(f.row("summary").await.state, AssetVersionState::Running);
        f.launches.one_run();
    }

    #[tokio::test]
    async fn a_daemon_takes_over_a_migration_only_when_its_lease_runs_out() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let reader = started_run("summary-run", CL, &["summary"], LaunchedBy::default());
        f.storage.create_run(&reader).await.unwrap();
        let declared: &[(&str, &[MigrationKind])] = &[("orders", &[Convert])];
        let (a, b) = (daemon("a", declared), daemon("b", declared));

        f.pass(&a).await;
        let held = f.row("orders").await;
        assert_eq!(held.owner.as_deref(), Some("a"));
        f.pass(&b).await;
        assert_eq!(f.row("orders").await, held);

        let expired = AssetVersionRecord {
            lease_expires_at: Some(now_ts() - 1),
            ..held.clone()
        };
        assert!(
            f.handle
                .scoped()
                .put_asset_version(&expired, Some(held.revision))
                .await
                .unwrap()
        );
        f.pass(&b).await;

        let taken = f.row("orders").await;
        assert_eq!(
            (taken.state, taken.owner.as_deref()),
            (AssetVersionState::Draining, Some("b"))
        );
        assert!(taken.lease_expires_at.unwrap() > now_ts());
    }

    #[tokio::test]
    async fn a_daemon_that_read_the_row_before_another_held_it_starts_nothing() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let read_by_b = f.row("orders").await;
        let declared: &[(&str, &[MigrationKind])] = &[("orders", &[Convert])];
        let (a, b) = (daemon("a", declared), daemon("b", declared));
        f.pass(&a).await;
        f.end_run(&f.launches.one_run(), RunStatus::Success).await;
        let held = f.row("orders").await;

        assert!(b.advance(&f.handle, &f.launches, read_by_b).await.unwrap());

        assert_eq!(f.row("orders").await, held);
        assert!(f.launches.taken().is_empty());
    }

    /// Records each launch, and gives no work a record.
    #[derive(Default)]
    struct Unrecorded(Mutex<Vec<MigrationWork>>);

    impl MigrationLauncher for Unrecorded {
        async fn launch(&self, _: &str, _: u32, work: MigrationWork) -> Result<()> {
            self.0.lock().unwrap().push(work);
            Ok(())
        }
    }

    #[tokio::test]
    async fn two_daemons_that_take_over_a_work_without_a_record_start_it_once() {
        let f = Fixture::new().await;
        let lost = AssetVersionRecord {
            state: AssetVersionState::Running,
            run_id: Some("lost".to_string()),
            owner: Some("gone".to_string()),
            lease_expires_at: Some(now_ts() - 1),
            ..pending("orders", 1, 2)
        };
        f.put(&lost).await;
        let read = f.row("orders").await;
        let declared: &[(&str, &[MigrationKind])] = &[("orders", &[Convert])];
        let (a, b) = (daemon("a", declared), daemon("b", declared));
        let launcher = Unrecorded::default();

        assert!(a.advance(&f.handle, &launcher, read.clone()).await.unwrap());
        assert!(b.advance(&f.handle, &launcher, read).await.unwrap());

        let started = launcher.0.into_inner().unwrap();
        let lost = MigrationWork::Convert {
            run_id: "lost".to_string(),
        };
        assert_eq!(started, [lost]);
        assert_eq!(f.row("orders").await.owner.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn a_released_migration_goes_on_in_the_next_loop_at_once() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;
        let declared: &[(&str, &[MigrationKind])] = &[("orders", &[Convert])];
        let (a, b) = (daemon("a", declared), daemon("b", declared));
        f.pass(&a).await;
        let run_id = f.launches.one_run();

        a.release(&f.handle).await.unwrap();

        let free = f.row("orders").await;
        assert_eq!(
            (free.state, free.owner, free.lease_expires_at),
            (AssetVersionState::Running, None, None)
        );
        f.end_run(&run_id, RunStatus::Success).await;
        assert!(!f.pass(&b).await);
        assert_eq!(f.row("orders").await.applied_version, 2);
    }

    #[tokio::test]
    async fn a_loop_for_some_assets_moves_only_their_migrations() {
        let f = Fixture::new().await;
        f.put(&pending("raw", 1, 2)).await;
        f.put(&pending("orders", 1, 2)).await;
        let declared: &[(&str, &[MigrationKind])] =
            &[("raw", &[VersionOnly]), ("orders", &[VersionOnly])];
        let raw_only = daemon("a", declared).only(HashSet::from(["raw".to_string()]));

        assert!(!f.pass(&raw_only).await);

        assert_eq!(f.row("raw").await.state, AssetVersionState::Ready);
        assert_eq!(f.row("orders").await.state, AssetVersionState::Pending);
    }

    #[tokio::test]
    async fn old_code_leaves_the_migration_of_newer_code_alone() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 3)).await;
        let before = f.row("orders").await;

        assert!(!f.pass(&daemon("old", &[("orders", &[Convert])])).await);

        assert_eq!(f.row("orders").await, before);
        assert!(f.launches.taken().is_empty());
    }

    #[tokio::test]
    async fn a_finished_migration_goes_on_to_the_version_that_the_code_declares() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;

        f.pass(&daemon("a", &[("orders", &[VersionOnly, VersionOnly])]))
            .await;

        let row = f.row("orders").await;
        assert_eq!(
            (row.state, row.applied_version),
            (AssetVersionState::Ready, 3)
        );
        assert_eq!(
            f.history("orders").await,
            [(2, VersionOnly, None, None), (3, VersionOnly, None, None)]
        );
    }

    #[tokio::test]
    async fn a_failed_rebuild_run_removes_the_data_of_the_version_before() {
        let f = Fixture::new().await;
        let record = AssetRecord {
            code_location_id: CL.to_string(),
            asset_key: "orders".to_string(),
            tags: Vec::new(),
            kinds: Vec::new(),
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: Vec::new(),
            pool: Vec::new(),
        };
        f.handle
            .scoped()
            .register_assets(vec![record])
            .await
            .unwrap();
        f.storage
            .store_event(&EventRecord {
                code_location_id: CL.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v1".to_string()),
                },
                asset_key: Some("orders".to_string()),
                run_id: "old-run".to_string(),
                partition_key: None,
                timestamp: now_ts() - 1_000,
                metadata: Vec::new(),
                input_data_versions: Vec::new(),
            })
            .await
            .unwrap();
        f.put(&pending("orders", 1, 2)).await;
        let a = daemon("a", &[("orders", &[Rebuild])]);

        f.pass(&a).await;
        let run_id = f.launches.one_run();
        f.end_run(&run_id, RunStatus::Failure).await;
        f.pass(&a).await;

        let row = f.row("orders").await;
        assert_eq!(
            (row.state, row.applied_version),
            (AssetVersionState::Ready, 2)
        );
        assert_eq!(f.history("orders").await, [(2, Rebuild, Some(0), Some(1))]);
        let stored = f
            .handle
            .scoped()
            .get_asset_record("orders")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.last_timestamp, None);
    }

    #[tokio::test]
    async fn a_rebuild_without_stored_data_has_nothing_to_do() {
        let f = Fixture::new().await;
        f.put(&pending("orders", 1, 2)).await;

        f.pass(&daemon("a", &[("orders", &[Rebuild])])).await;

        assert_eq!(f.row("orders").await.state, AssetVersionState::Ready);
        assert_eq!(f.history("orders").await, [(2, Rebuild, Some(0), Some(0))]);
        assert!(f.launches.taken().is_empty());
    }
}
