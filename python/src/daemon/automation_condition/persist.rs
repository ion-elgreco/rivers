//! Single-write persistence for the global condition tick.
use rivers_core::condition::EvalResultRow;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{ConditionTickRecord, ScopedStorageHandle, StorageBackend};

/// Per-asset dispatch outcome, feeding the asset's tick-history row.
#[derive(Default, Clone)]
pub(super) struct AssetOutcome {
    pub(super) run_ids: Vec<String>,
    pub(super) backfill_ids: Vec<String>,
    pub(super) error: Option<String>,
}

/// Buffer for the run/backfill ids contributed by a single tick's materialization fan-out.
pub(super) struct ConditionTickHandle {
    code_location_id: String,
    timestamp: i64,
    total_evaluated: u32,
    total_fired: u32,
    eval_duration_us: u64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
    asset_outcomes: std::collections::HashMap<String, AssetOutcome>,
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
            asset_outcomes: std::collections::HashMap::new(),
        }
    }

    pub(super) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Ensure every planned asset gets a tick-history row, even if dispatch
    /// never reaches it.
    pub(super) fn seed_assets<'a>(&mut self, assets: impl Iterator<Item = &'a String>) {
        for asset in assets {
            self.asset_outcomes.entry(asset.clone()).or_default();
        }
    }

    /// Record a `run_id` minted synchronously during dispatch for `assets`.
    pub(super) fn note_run(&mut self, assets: &[String], run_id: &str) {
        self.run_ids.push(run_id.to_string());
        for asset in assets {
            self.asset_outcomes
                .entry(asset.clone())
                .or_default()
                .run_ids
                .push(run_id.to_string());
        }
    }

    /// Drop a `run_id` whose dispatch failed before its run record reached storage.
    pub(super) fn unnote_run(&mut self, assets: &[String], run_id: &str, error: &str) {
        self.run_ids.retain(|id| id != run_id);
        for asset in assets {
            let outcome = self.asset_outcomes.entry(asset.clone()).or_default();
            outcome.run_ids.retain(|id| id != run_id);
            outcome.error = Some(error.to_string());
        }
    }

    /// Record a `backfill_id` produced by `BackfillDispatcherKind::dispatch`.
    pub(super) fn note_backfill(&mut self, asset: &str, backfill_id: &str) {
        self.backfill_ids.push(backfill_id.to_string());
        self.asset_outcomes
            .entry(asset.to_string())
            .or_default()
            .backfill_ids
            .push(backfill_id.to_string());
    }

    /// Record a backfill dispatch failure for `asset`.
    pub(super) fn note_backfill_error(&mut self, asset: &str, error: &str) {
        self.asset_outcomes
            .entry(asset.to_string())
            .or_default()
            .error = Some(error.to_string());
    }

    /// Per-asset outcomes for the tick-history rows.
    pub(super) fn outcomes(&self) -> &std::collections::HashMap<String, AssetOutcome> {
        &self.asset_outcomes
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
    fn unnote_run_drops_only_the_named_id() {
        let assets = vec!["x".to_string()];
        let mut handle = ConditionTickHandle::new("cl".to_string(), 0, &[]);
        handle.note_run(&assets, "a");
        handle.note_run(&assets, "b");
        handle.unnote_run(&assets, "a", "dispatch failed");
        assert_eq!(
            handle.run_ids,
            vec!["b".to_string()],
            "the finalized tick must not reference a run that never persisted"
        );
        let outcome = &handle.outcomes()["x"];
        assert_eq!(outcome.run_ids, vec!["b".to_string()]);
        assert_eq!(outcome.error.as_deref(), Some("dispatch failed"));
    }
}
