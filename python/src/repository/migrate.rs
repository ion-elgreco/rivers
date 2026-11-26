//! `repo.migrate()`: the asset migrations of this code, run in this process
//! (RFC-048 §12), for tests and scripts without a daemon.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use rivers_core::assets::migrations::{
    MigrationLauncher, MigrationLoop, MigrationWork, REBUILD_CONCURRENCY, retry_migration,
};
use rivers_core::assets::versions::VersionGate;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::{
    AssetVersionState, CodeLocationContext, LaunchedBy, ScopedStorageHandle,
};

use super::PyCodeRepository;
use crate::assets::action::MIGRATE;
use crate::daemon::RunType;
use crate::errors::{AssetMigrationPendingError, AssetNotFoundError, AssetVersionMismatchError};
use crate::partitions::PyPartitionKey;
use crate::runtime::io_rt;
use crate::storage::{PyAssetVersionRecord, to_py_err};

/// How long `repo.migrate()` waits before it looks again at a migration that
/// cannot move yet: one that waits for started runs, or that a daemon holds.
const WAIT_INTERVAL: Duration = Duration::from_millis(250);

/// Keeps the work that a pass starts, for `repo.migrate()` to run after the
/// pass: the work blocks until it ends.
#[derive(Default)]
struct Deferred(Mutex<Vec<(String, u32, MigrationWork)>>);

impl MigrationLauncher for Deferred {
    async fn launch(&self, asset_key: &str, to: u32, work: MigrationWork) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((asset_key.to_string(), to, work));
        Ok(())
    }
}

impl PyCodeRepository {
    /// Body of `repo.migrate()`. Blocks: call it without the GIL.
    pub(crate) fn migrate_inner(
        &self,
        selection: Option<Vec<String>>,
        raise_on_error: bool,
    ) -> PyResult<Vec<PyAssetVersionRecord>> {
        let resolved = self.ensure_resolved()?;
        let state = &*resolved;
        let code = Arc::clone(&state.code_versions);
        let mut selected: Vec<String> = match selection {
            Some(names) => {
                if let Some(name) = names
                    .iter()
                    .find(|name| !state.node_map.contains_key(*name))
                {
                    return Err(AssetNotFoundError::new_err(format!(
                        "Selection contains unknown asset: '{name}'"
                    )));
                }
                names
            }
            None => code.assets().map(str::to_string).collect(),
        };
        selected.sort_unstable();
        selected.dedup();
        // An asset migrates after the assets that it reads.
        let only: HashSet<String> = code
            .with_upstream(selected.iter().map(String::as_str))
            .into_iter()
            .map(str::to_string)
            .collect();

        let handle = ScopedStorageHandle::new(
            Arc::clone(&state.storage),
            CodeLocationContext::new(state.code_location_id.clone()),
        );
        let rows = io_rt()
            .block_on(handle.scoped().get_asset_versions(None))
            .map_err(to_py_err)?;
        for row in &rows {
            if row.state == AssetVersionState::Failed && only.contains(&row.asset_key) {
                io_rt()
                    .block_on(retry_migration(&handle.scoped(), &row.asset_key))
                    .map_err(to_py_err)?;
            }
        }

        let migrations = MigrationLoop::new(uuid::Uuid::new_v4().to_string(), Arc::clone(&code))
            .only(only)
            .at_once();
        let moved = self.move_migrations(&handle, &migrations);
        if moved.is_err()
            && let Err(e) = io_rt().block_on(migrations.release(&handle))
        {
            tracing::warn!(
                target: "rivers::migrations",
                error = %format!("{e:#}"),
                "repo.migrate() could not let go of its migrations; they wait for the lease to run out"
            );
        }
        moved?;

        let rows = io_rt()
            .block_on(handle.scoped().get_asset_versions(None))
            .map_err(to_py_err)?;
        if raise_on_error {
            for key in &selected {
                match code.gate(None, None, std::slice::from_ref(key), &rows) {
                    VersionGate::Go => {}
                    VersionGate::Wait(reason) => {
                        return Err(AssetMigrationPendingError::new_err(reason));
                    }
                    VersionGate::Refuse(reason) => {
                        return Err(AssetVersionMismatchError::new_err(reason));
                    }
                }
            }
        }
        let mut rows: Vec<PyAssetVersionRecord> = rows
            .into_iter()
            .filter(|row| selected.binary_search(&row.asset_key).is_ok())
            .map(PyAssetVersionRecord::from)
            .collect();
        rows.sort_unstable_by(|a, b| a.asset_key.cmp(&b.asset_key));
        Ok(rows)
    }

    /// Run passes of `migrations`, and the work that each pass starts, until
    /// no migration is on its way.
    fn move_migrations(
        &self,
        handle: &ScopedStorageHandle<AnyStorage>,
        migrations: &MigrationLoop,
    ) -> PyResult<()> {
        let deferred = Deferred::default();
        loop {
            let on_its_way = io_rt()
                .block_on(migrations.pass(handle, &deferred))
                .map_err(to_py_err)?;
            let works = std::mem::take(&mut *deferred.0.lock().unwrap());
            let started = !works.is_empty();
            for (asset_key, to, work) in works {
                self.run_migration_work(asset_key, to, work)?;
            }
            if !on_its_way {
                return Ok(());
            }
            Python::attach(|py| py.check_signals())?;
            if !started {
                std::thread::sleep(WAIT_INTERVAL);
            }
        }
    }

    /// Run `work`, which moves the stored data of `asset_key` to version
    /// `to`, in this process until it ends. A failed run is not an error:
    /// the next pass reads it from its record.
    fn run_migration_work(&self, asset_key: String, to: u32, work: MigrationWork) -> PyResult<()> {
        let launched_by = LaunchedBy::Migration {
            asset_key: asset_key.clone(),
            to_version: to,
        };
        let selection = vec![asset_key];
        match work {
            MigrationWork::Convert { run_id } => self
                .run_action_with_launcher(
                    MIGRATE.to_string(),
                    Some(selection),
                    None,
                    None,
                    None,
                    false,
                    None,
                    Some(run_id),
                    false,
                    launched_by,
                )
                .map(drop),
            MigrationWork::RebuildRun { run_id } => self
                .materialize_with_launcher(
                    Some(selection),
                    None,
                    None,
                    None,
                    false,
                    None,
                    Some(run_id),
                    false,
                    false,
                    None,
                    launched_by,
                )
                .map(drop),
            MigrationWork::RebuildBackfill { backfill_id, keys } => self
                .backfill_inner(
                    RunType::Materialization(selection),
                    Some(keys.iter().map(PyPartitionKey::from).collect()),
                    None,
                    None,
                    "continue",
                    REBUILD_CONCURRENCY,
                    None,
                    None,
                    true,
                    false,
                    Some(backfill_id),
                    launched_by,
                    None,
                )
                .map(drop),
        }
    }
}
