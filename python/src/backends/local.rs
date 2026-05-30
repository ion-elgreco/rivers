use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use pyo3::prelude::*;
use rivers_core::run_backend::{RunBackend, RunHealthStatus};
use rivers_core::storage::{CoordinatorRunInfo, LaunchedBy};

use crate::partitions::PyPartitionKey;
use crate::repository::PyCodeRepository;

pub struct LocalRunBackend {
    /// `run_id` → "finished" flag (for health / terminate). The `JoinHandle`
    /// lives in the global run-handle pool, joined before finalize via
    /// [`crate::shutdown::register_run_handle`].
    active_runs: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl Default for LocalRunBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalRunBackend {
    pub fn new() -> Self {
        Self {
            active_runs: Mutex::new(HashMap::new()),
        }
    }
}

impl RunBackend for LocalRunBackend {
    async fn launch(
        &self,
        run_info: &CoordinatorRunInfo,
        ctx: &(dyn std::any::Any + Send + Sync),
    ) -> Result<()> {
        let repo: &Arc<Py<PyCodeRepository>> = ctx
            .downcast_ref()
            .context("launch context must be Arc<Py<PyCodeRepository>>")?;

        let repo = repo.clone();
        let run_id = run_info.run_id.clone();
        let node_names = run_info.node_names.clone();
        let partition_key = run_info.partition_key.as_ref().map(PyPartitionKey::from);

        let run_id_for_key = run_id.clone();
        let done = Arc::new(AtomicBool::new(false));
        let done_for_thread = Arc::clone(&done);
        let handle = std::thread::spawn(move || {
            let selection = if node_names.is_empty() {
                None
            } else {
                Some(node_names)
            };
            // `Some(run_id)` makes `materialize_with_launcher` reuse the existing
            // queued record, so `LaunchedBy::Manual` is ignored.
            if let Err(e) = repo.get().materialize_with_launcher(
                selection,
                partition_key,
                None,
                false,
                None,
                Some(run_id.clone()),
                false,
                false,
                LaunchedBy::Manual,
            ) {
                tracing::error!(
                    target: "rivers::coordinator",
                    run_id = %run_id,
                    error = %e,
                    "dequeued run execution failed"
                );
            }
            done_for_thread.store(true, Ordering::Release);
        });

        // Joined before finalize (holds the GIL); health tracked via `done`.
        crate::shutdown::register_run_handle(handle);
        self.active_runs
            .lock()
            .unwrap()
            .insert(run_id_for_key, done);
        Ok(())
    }

    async fn terminate_run(&self, run_id: &str) -> Result<bool> {
        let entry = self.active_runs.lock().unwrap().remove(run_id);
        // TODO(ion): add ability to cancel runs for local runs.
        Ok(entry.is_some())
    }

    async fn check_run_health(&self, run_id: &str) -> Result<RunHealthStatus> {
        let mut active = self.active_runs.lock().unwrap();
        match active.get(run_id) {
            Some(done) if done.load(Ordering::Acquire) => {
                active.remove(run_id);
                Ok(RunHealthStatus::Exited)
            }
            Some(_) => Ok(RunHealthStatus::Healthy),
            None => Ok(RunHealthStatus::Missing),
        }
    }
}
