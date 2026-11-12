//! The runs of a backfill, one page at a time.

use std::collections::HashMap;

use anyhow::Result;

use crate::storage::{BackfillRecord, RunRecord, StorageBackend};

/// The runs of `backfill` from `offset`, at most `limit`. The run linked last
/// comes first.
///
/// The start times do not order the runs: a daemon creates every run of a
/// backfill in one step.
pub async fn backfill_runs_page<S: StorageBackend>(
    storage: &S,
    backfill: &BackfillRecord,
    offset: u64,
    limit: u64,
) -> Result<Vec<RunRecord>> {
    let run_ids: Vec<String> = backfill
        .run_ids
        .iter()
        .rev()
        .skip(offset as usize)
        .take(limit as usize)
        .cloned()
        .collect();

    let mut runs: HashMap<String, RunRecord> = storage
        .get_runs_by_ids(&run_ids, None)
        .await?
        .into_iter()
        .map(|run| (run.run_id.clone(), run))
        .collect();
    Ok(run_ids
        .iter()
        .filter_map(|run_id| runs.remove(run_id))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillStatus, BackfillStrategy, LaunchedBy, PartitionKey,
        RunStatus, default_code_location_id,
    };

    fn planned_run(run_id: &str, day: &str) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            code_location_id: default_code_location_id(),
            job_name: None,
            status: RunStatus::Queued,
            start_time: 7,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: Some(PartitionKey::Single {
                keys: vec![day.to_string()],
            }),
            block_reason: None,
            launched_by: LaunchedBy::Backfill {
                backfill_id: "bf".to_string(),
            },
            action: None,
            config: None,
        }
    }

    /// A daemon creates every run of a backfill in one step: the runs share
    /// their start time, and their ids are random.
    #[tokio::test]
    async fn runs_with_one_start_time_come_in_the_order_of_their_links() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let plan = [
            planned_run("run-e", "d1"),
            planned_run("run-a", "d2"),
            planned_run("run-d", "d3"),
            planned_run("run-b", "d4"),
            planned_run("run-c", "d5"),
        ];
        let requested: Vec<PartitionKey> = plan
            .iter()
            .filter_map(|run| run.partition_key.clone())
            .collect();
        let record = BackfillRecord {
            backfill_id: "bf".to_string(),
            code_location_id: default_code_location_id(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["a".to_string()],
            job_name: None,
            num_partitions: 0,
            planned_runs: 0,
            run_ids: vec![],
            completed_partitions: 0,
            failed_partitions: 0,
            canceled_partitions: 0,
            max_concurrency: 1,
            tags: vec![],
            create_time: 1,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::default(),
            action: None,
            config: None,
        };
        storage.create_backfill(&record, &requested).await.unwrap();
        assert!(storage.start_backfill("bf", plan.len()).await.unwrap());
        assert!(storage.enqueue_backfill_runs(&plan, "bf").await.unwrap());

        let backfill = storage.get_backfill("bf").await.unwrap().unwrap();
        let mut pages: Vec<Vec<String>> = Vec::new();
        for offset in [0, 2, 4, 6] {
            let page = backfill_runs_page(&storage, &backfill, offset, 2)
                .await
                .unwrap();
            pages.push(page.into_iter().map(|run| run.run_id).collect());
        }

        assert_eq!(
            pages,
            vec![
                vec!["run-c".to_string(), "run-b".to_string()],
                vec!["run-d".to_string(), "run-a".to_string()],
                vec!["run-e".to_string()],
                vec![],
            ]
        );
    }
}
