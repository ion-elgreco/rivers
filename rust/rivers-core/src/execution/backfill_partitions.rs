//! The partitions of a backfill with what happened to each, one page at a time.

use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;

use crate::execution::backfill::RunNumbers;
use crate::storage::{
    BackfillRecord, BackfillRunOutcome, EndedBackfillRun, PartitionKey, StorageBackend,
};

/// What happened to one partition of a backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillPartitionState {
    Pending,
    Completed,
    Failed,
    Canceled,
}

impl From<BackfillRunOutcome> for BackfillPartitionState {
    fn from(outcome: BackfillRunOutcome) -> Self {
        match outcome {
            BackfillRunOutcome::Completed => Self::Completed,
            BackfillRunOutcome::Failed => Self::Failed,
            BackfillRunOutcome::Canceled => Self::Canceled,
        }
    }
}

/// The partitions of `backfill` from `offset`, at most `limit`, in the order
/// of its requested keys, each with what happened to it.
pub async fn backfill_partitions_page<S: StorageBackend>(
    storage: &S,
    backfill: &BackfillRecord,
    offset: u64,
    limit: u64,
) -> Result<Vec<(PartitionKey, BackfillPartitionState)>> {
    let requested = storage.get_backfill_keys(&backfill.backfill_id).await?;
    let partitions = partitions_at(&requested, offset, limit);
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    // A backfill that ended before outcomes were kept per run has no plan.
    let states = if backfill.planned_runs > 0 {
        states_from_runs(storage, backfill, &requested, &partitions).await?
    } else {
        states_from_partition_rows(storage, backfill, &partitions).await?
    };
    Ok(partitions
        .into_iter()
        .map(|(_, partition)| partition)
        .zip(states)
        .collect())
}

/// The partitions that `requested` names from `offset`, at most `limit`, each
/// with its place among all of them.
fn partitions_at(
    requested: &[PartitionKey],
    offset: u64,
    limit: u64,
) -> Vec<(usize, PartitionKey)> {
    let mut page: Vec<(usize, PartitionKey)> = Vec::new();
    let mut first_of_key = 0u64;

    for key in requested {
        let end_of_key = first_of_key.saturating_add(key.member_count() as u64);
        let mut place = offset.max(first_of_key);
        while place < end_of_key && (page.len() as u64) < limit {
            if let Some(partition) = key.member_at((place - first_of_key) as usize) {
                page.push((place as usize, partition));
            }
            place += 1;
        }
        if page.len() as u64 >= limit {
            break;
        }
        first_of_key = end_of_key;
    }
    page
}

async fn states_from_runs<S: StorageBackend>(
    storage: &S,
    backfill: &BackfillRecord,
    requested: &[PartitionKey],
    partitions: &[(usize, PartitionKey)],
) -> Result<Vec<BackfillPartitionState>> {
    let numbers = RunNumbers::new(&backfill.strategy, requested);
    let run_numbers: Vec<Option<usize>> = partitions
        .iter()
        .map(|(place, partition)| numbers.of(*place, partition))
        .collect();
    let on_page: BTreeSet<usize> = run_numbers.iter().flatten().copied().collect();
    let (Some(first), Some(last)) = (on_page.first(), on_page.last()) else {
        return Ok(vec![BackfillPartitionState::Pending; partitions.len()]);
    };

    let ended: HashMap<usize, EndedBackfillRun> = storage
        .get_ended_backfill_runs(&backfill.backfill_id, *first, *last)
        .await?
        .into_iter()
        .filter(|run| on_page.contains(&run.run_number))
        .map(|run| (run.run_number, run))
        .collect();

    // A run that succeeded can hold partitions that failed alone.
    let mut failed_alone: HashMap<usize, HashSet<PartitionKey>> = HashMap::new();
    for run in ended.values() {
        let some_failed = run.failed_members > 0 && run.failed_members < run.member_count;
        if run.outcome == BackfillRunOutcome::Completed
            && some_failed
            && let Some(run_id) = &run.run_id
        {
            let failed = storage.get_run_failed_partitions(run_id).await?;
            failed_alone.insert(run.run_number, failed.into_iter().collect());
        }
    }

    Ok(partitions
        .iter()
        .zip(&run_numbers)
        .map(|((_, partition), run_number)| {
            let Some(run) = run_number.and_then(|number| ended.get(&number)) else {
                return BackfillPartitionState::Pending;
            };
            let failed_in_a_good_run = run.outcome == BackfillRunOutcome::Completed
                && (run.failed_members >= run.member_count
                    || failed_alone
                        .get(&run.run_number)
                        .is_some_and(|failed| failed.contains(partition)));
            if failed_in_a_good_run {
                BackfillPartitionState::Failed
            } else {
                run.outcome.into()
            }
        })
        .collect())
}

async fn states_from_partition_rows<S: StorageBackend>(
    storage: &S,
    backfill: &BackfillRecord,
    partitions: &[(usize, PartitionKey)],
) -> Result<Vec<BackfillPartitionState>> {
    let keys: Vec<PartitionKey> = partitions
        .iter()
        .map(|(_, partition)| partition.clone())
        .collect();
    let outcomes: HashMap<PartitionKey, BackfillRunOutcome> = storage
        .get_backfill_partition_outcomes(&backfill.backfill_id, &keys)
        .await?
        .into_iter()
        .collect();

    Ok(keys
        .iter()
        .map(|partition| {
            outcomes
                .get(partition)
                .map_or(BackfillPartitionState::Pending, |outcome| (*outcome).into())
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillStatus, BackfillStrategy, EventRecord, EventType,
        LaunchedBy, RunRecord, RunStatus, UnstartedBackfillRun, default_code_location_id,
    };

    use BackfillPartitionState::{Canceled, Completed, Failed, Pending};

    fn single(values: &[&str]) -> PartitionKey {
        PartitionKey::Single {
            keys: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn multi(dims: &[(&str, &[&str])]) -> PartitionKey {
        PartitionKey::Multi {
            dims: dims
                .iter()
                .map(|(name, values)| {
                    (
                        name.to_string(),
                        values.iter().map(|v| v.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    /// Store backfill `bf` for `requested` and start it with its plan.
    async fn started_backfill(
        storage: &SurrealStorage,
        strategy: BackfillStrategy,
        requested: &[PartitionKey],
    ) -> Vec<PartitionKey> {
        let record = BackfillRecord {
            backfill_id: "bf".to_string(),
            code_location_id: default_code_location_id(),
            status: BackfillStatus::Requested,
            strategy: strategy.clone(),
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
        storage.create_backfill(&record, requested).await.unwrap();

        let plan = crate::execution::backfill::plan_runs(&strategy, requested);
        assert!(storage.start_backfill("bf", plan.len()).await.unwrap());
        plan
    }

    /// Link run `run_number` of the plan and end it with `status`.
    async fn ended_run(
        storage: &SurrealStorage,
        plan: &[PartitionKey],
        run_number: usize,
        status: RunStatus,
        failed_alone: &[PartitionKey],
    ) {
        let run_id = format!("run-{run_number}");
        let run_key = &plan[run_number];
        assert!(
            storage
                .link_backfill_run("bf", &run_id, run_number, run_key.member_count())
                .await
                .unwrap()
        );
        storage
            .create_run(&RunRecord {
                run_id: run_id.clone(),
                code_location_id: default_code_location_id(),
                job_name: None,
                status: RunStatus::Started,
                start_time: 1,
                end_time: None,
                execution_start_time: None,
                tags: vec![],
                node_names: vec!["a".to_string()],
                priority: 0,
                partition_key: Some(run_key.clone()),
                block_reason: None,
                launched_by: LaunchedBy::Backfill {
                    backfill_id: "bf".to_string(),
                },
                action: None,
                config: None,
            })
            .await
            .unwrap();
        for partition in failed_alone {
            storage
                .store_event(&EventRecord {
                    code_location_id: default_code_location_id(),
                    event_type: EventType::StepFailure,
                    asset_key: Some("a".to_string()),
                    run_id: run_id.clone(),
                    partition_key: Some(partition.clone()),
                    timestamp: 2,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        storage
            .update_run_status(&run_id, status, Some(3))
            .await
            .unwrap();
    }

    async fn page(
        storage: &SurrealStorage,
        offset: u64,
        limit: u64,
    ) -> Vec<(String, BackfillPartitionState)> {
        let backfill = storage.get_backfill("bf").await.unwrap().unwrap();
        backfill_partitions_page(storage, &backfill, offset, limit)
            .await
            .unwrap()
            .into_iter()
            .map(|(partition, state)| (partition.to_display(), state))
            .collect()
    }

    #[tokio::test]
    async fn each_partition_takes_the_outcome_of_its_run() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [multi(&[
            ("zone", &["z1", "z2", "z3"]),
            ("hour", &["h1", "h2"]),
        ])];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["zone".to_string()],
            single_run: vec!["hour".to_string()],
        };
        let plan = started_backfill(&storage, strategy, &requested).await;
        assert_eq!(plan.len(), 3, "one run per zone");

        // z1 succeeded with one partition failed alone, z2 failed, z3 is open.
        let failed = multi(&[("zone", &["z1"]), ("hour", &["h2"])]);
        ended_run(&storage, &plan, 0, RunStatus::Success, &[failed]).await;
        ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;

        let cell = |hour: &str, zone: &str, state| (format!("hour={hour}|zone={zone}"), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("h1", "z1", Completed),
                cell("h1", "z2", Failed),
                cell("h1", "z3", Pending),
                cell("h2", "z1", Failed),
                cell("h2", "z2", Failed),
                cell("h2", "z3", Pending),
            ]
        );
        assert_eq!(
            page(&storage, 4, 1).await,
            vec![cell("h2", "z2", Failed)],
            "a page reads its own partitions"
        );
        assert!(page(&storage, 6, 10).await.is_empty());

        // The run of z3 is canceled before it starts.
        storage
            .settle_unstarted_backfill_runs(
                "bf",
                &[UnstartedBackfillRun {
                    run_number: 2,
                    member_count: 2,
                    outcome: BackfillRunOutcome::Canceled,
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            page(&storage, 5, 10).await,
            vec![cell("h2", "z3", Canceled)]
        );
    }

    #[tokio::test]
    async fn a_run_per_partition_reads_the_runs_of_the_page() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [single(&["a", "b"]), single(&["c", "d"])];
        let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
        assert_eq!(plan.len(), 4);

        ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;
        // The one partition of this run is marked failed; the run succeeds.
        ended_run(&storage, &plan, 2, RunStatus::Success, &[single(&["c"])]).await;
        ended_run(&storage, &plan, 3, RunStatus::Canceled, &[]).await;

        let cell = |key: &str, state| (key.to_string(), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("a", Completed),
                cell("b", Pending),
                cell("c", Failed),
                cell("d", Canceled),
            ]
        );
        assert_eq!(
            page(&storage, 2, 2).await,
            vec![cell("c", Failed), cell("d", Canceled)]
        );
    }

    #[tokio::test]
    async fn one_run_gives_its_outcome_to_each_partition() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [single(&["a", "b", "c"])];
        let plan = started_backfill(&storage, BackfillStrategy::SingleRun, &requested).await;
        assert_eq!(plan.len(), 1);

        ended_run(&storage, &plan, 0, RunStatus::Success, &[single(&["b"])]).await;

        let cell = |key: &str, state| (key.to_string(), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("a", Completed),
                cell("b", Failed),
                cell("c", Completed)
            ]
        );
    }

    #[test]
    fn a_page_starts_inside_a_key_and_goes_on_into_the_next() {
        let requested = [single(&["a", "b", "c"]), single(&[]), single(&["d", "e"])];
        let places = |offset, limit| -> Vec<(usize, String)> {
            partitions_at(&requested, offset, limit)
                .into_iter()
                .map(|(place, partition)| (place, partition.to_display()))
                .collect()
        };

        assert_eq!(
            places(2, 2),
            vec![(2, "c".to_string()), (3, "d".to_string())]
        );
        assert_eq!(places(4, 10), vec![(4, "e".to_string())]);
        assert!(places(5, 10).is_empty());
        assert!(places(0, 0).is_empty());
    }
}
