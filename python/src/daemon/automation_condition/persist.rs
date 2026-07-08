//! Single-write persistence for the global condition tick.
use rivers_core::condition::EvalResultRow;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{ConditionTickRecord, ScopedStorageHandle, StorageBackend};

/// Buffer for the run/backfill ids contributed by a single tick's materialization fan-out.
pub(super) struct ConditionTickHandle {
    code_location_id: String,
    timestamp: i64,
    total_evaluated: u32,
    total_fired: u32,
    eval_duration_us: u64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
}

impl ConditionTickHandle {
    pub(super) fn new(code_location_id: String, timestamp: i64, results: &[EvalResultRow]) -> Self {
        let total_evaluated = results.len() as u32;
        let total_fired = results.iter().filter(|r| r.result.fired).count() as u32;
        let eval_duration_us: u64 = results.iter().map(|r| r.duration_us).sum();
        Self {
            code_location_id,
            timestamp,
            total_evaluated,
            total_fired,
            eval_duration_us,
            run_ids: Vec::new(),
            backfill_ids: Vec::new(),
        }
    }

    pub(super) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Record a `run_id` minted synchronously during dispatch.
    pub(super) fn register_run(&mut self, run_id: String) {
        self.run_ids.push(run_id);
    }

    /// Drop a `run_id` whose dispatch failed before its run record reached storage.
    pub(super) fn unregister_run(&mut self, run_id: &str) {
        self.run_ids.retain(|id| id != run_id);
    }

    /// Record a `backfill_id` produced by `BackfillDispatcherKind::dispatch`.
    pub(super) fn register_backfill_id(&mut self, backfill_id: String) {
        self.backfill_ids.push(backfill_id);
    }

    /// Write the `ConditionTickRecord` once with all ids populated.
    pub(super) async fn finalize(self, storage: &ScopedStorageHandle<SurrealStorage>) -> String {
        let record = ConditionTickRecord {
            code_location_id: self.code_location_id,
            timestamp: self.timestamp,
            total_evaluated: self.total_evaluated,
            total_fired: self.total_fired,
            eval_duration_us: self.eval_duration_us,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
        };

        match storage.backend().store_condition_tick(&record).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(target: "rivers::daemon", error = %e, "failed to store condition tick");
                format!("tick_{}", record.timestamp)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConditionTickHandle;

    #[test]
    fn unregister_run_drops_only_the_named_id() {
        let mut handle = ConditionTickHandle::new("cl".to_string(), 0, &[]);
        handle.register_run("a".to_string());
        handle.register_run("b".to_string());
        handle.unregister_run("a");
        assert_eq!(
            handle.run_ids,
            vec!["b".to_string()],
            "the finalized tick must not reference a run that never persisted"
        );
    }
}
