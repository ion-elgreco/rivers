//! PostgreSQL only: the pages of the keys, a backfill read back, and races
//! of two writers.

use super::*;

fn key(values: &[&str]) -> PartitionKey {
    PartitionKey::Single {
        keys: values.iter().map(|v| v.to_string()).collect(),
    }
}

fn backfill(id: &str, status: BackfillStatus) -> BackfillRecord {
    BackfillRecord {
        backfill_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        status,
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
    }
}

fn run(run_id: &str, partition_key: Option<PartitionKey>, backfill_id: Option<&str>) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Queued,
        start_time: 1,
        end_time: None,
        execution_start_time: None,
        tags: vec![],
        node_names: vec!["a".to_string()],
        priority: 0,
        partition_key,
        block_reason: None,
        launched_by: backfill_id.map_or_else(LaunchedBy::default, |id| LaunchedBy::Backfill {
            backfill_id: id.to_string(),
        }),
        action: None,
        config: None,
    }
}

async fn store() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    Some(PostgresStorage::connect_test_schema(&url).await.unwrap())
}

#[test]
fn no_keys_are_one_empty_page() {
    assert_eq!(key_pages(&[]).unwrap(), vec![serde_json::json!([])]);
}

#[test]
fn keys_past_the_page_size_start_a_new_page() {
    let value = "v".repeat(KEY_PAGE_BYTES / 4);
    let keys: Vec<PartitionKey> = (0..9).map(|_| key(&[&value])).collect();

    let pages = key_pages(&keys).unwrap();

    assert_eq!(pages.len(), 3);
    assert!(pages.iter().all(|page| page.as_array().unwrap().len() == 3));
}

#[test]
fn a_key_larger_than_a_page_is_a_page_of_its_own() {
    let big = key(&["v".repeat(2 * KEY_PAGE_BYTES).as_str()]);
    let keys = vec![key(&["a"]), big.clone(), key(&["b"])];

    let pages: Vec<Vec<PartitionKey>> = key_pages(&keys)
        .unwrap()
        .into_iter()
        .map(|page| from_json(page).unwrap())
        .collect();

    assert_eq!(pages, vec![vec![key(&["a"])], vec![big], vec![key(&["b"])]]);
}

#[tokio::test]
async fn a_backfill_reads_back_as_it_was_stored() {
    let Some(storage) = store().await else {
        return;
    };
    let stored = BackfillRecord {
        strategy: BackfillStrategy::PerDimension {
            multi_run: vec!["date".to_string()],
            single_run: vec!["region".to_string()],
        },
        failure_policy: BackfillFailurePolicy::StopOnFailure,
        asset_selection: vec!["a".to_string(), "b".to_string()],
        job_name: Some("job".to_string()),
        num_partitions: 3,
        max_concurrency: 4,
        tags: vec![("team".to_string(), "data".to_string())],
        launched_by: LaunchedBy::Manual {
            user: Some(UserRef {
                subject: "sub-42".to_string(),
                email: Some("someone@example.com".to_string()),
                name: None,
            }),
        },
        action: Some("check".to_string()),
        config: Some(r#"{"a":{"mode":"full"}}"#.to_string()),
        ..backfill("bf", BackfillStatus::Requested)
    };
    let requested = vec![
        PartitionKey::Multi {
            dims: vec![
                (
                    "region".to_string(),
                    vec!["eu".to_string(), "us".to_string()],
                ),
                ("date".to_string(), vec!["2024-01-01".to_string()]),
            ],
        },
        key(&["x"]),
    ];

    storage.create_backfill(&stored, &requested).await.unwrap();

    assert_eq!(storage.get_backfill("bf").await.unwrap(), Some(stored));
    assert_eq!(
        storage.get_backfill_keys("bf").await.unwrap(),
        vec![
            PartitionKey::Multi {
                dims: vec![
                    ("date".to_string(), vec!["2024-01-01".to_string()]),
                    (
                        "region".to_string(),
                        vec!["eu".to_string(), "us".to_string()]
                    ),
                ],
            },
            key(&["x"]),
        ],
        "a key reads back with its dimensions in the order of their names"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_drivers_of_one_backfill_start_each_partition_once_and_complete_it_once() {
    let Some(storage) = store().await else {
        return;
    };
    // More than two statements of links per batch.
    const RUNS: usize = 2 * runs::ROWS_PER_WRITE + 10;

    for round in 0..4 {
        let id = format!("bf{round}");
        storage
            .create_backfill(&backfill(&id, BackfillStatus::Requested), &[])
            .await
            .unwrap();
        assert!(storage.start_backfill(&id, RUNS).await.unwrap());

        // Each driver plans the same partitions with run ids of its own.
        let plan = |driver: &str| -> Vec<RunRecord> {
            (0..RUNS)
                .map(|n| {
                    let partition = format!("p{n}");
                    let key = key(&[partition.as_str()]);
                    run(&format!("{id}-{driver}-{n}"), Some(key), Some(&id))
                })
                .collect()
        };
        let (a, b) = (plan("a"), plan("b"));
        let (live_a, live_b) = tokio::join!(
            storage.enqueue_backfill_runs(&a, &id),
            storage.enqueue_backfill_runs(&b, &id)
        );
        assert!(live_a.unwrap() && live_b.unwrap());

        let keys: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT partition_key FROM runs WHERE launched_by->>'backfill_id' = $1",
        )
        .bind(&id)
        .fetch_all(storage.pool())
        .await
        .unwrap();
        let mut runs_per_partition: HashMap<PartitionKey, usize> = HashMap::new();
        for key in keys {
            *runs_per_partition
                .entry(from_json(key).unwrap())
                .or_default() += 1;
        }
        assert_eq!(runs_per_partition.len(), RUNS, "each partition has a run");
        assert!(
            runs_per_partition.values().all(|&runs| runs == 1),
            "no partition has two runs"
        );
        let linked_runs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM backfill_runs JOIN runs USING (run_id) WHERE backfill_id = $1",
        )
        .bind(&id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(linked_runs, RUNS as i64, "each run is linked");

        // All planned runs but the last two ended. The last two end at the
        // same time, and each driver tries to complete the backfill after
        // the run it saw end.
        let ended: Vec<UnstartedBackfillRun> = (0..RUNS - 2)
            .map(|run_number| UnstartedBackfillRun {
                run_number,
                member_count: 1,
                outcome: BackfillRunOutcome::Completed,
            })
            .collect();
        storage
            .settle_unstarted_backfill_runs(&id, &ended)
            .await
            .unwrap();
        let last = storage.get_open_backfill_runs(&id).await.unwrap();
        assert_eq!(last.len(), 2);
        let end_and_complete = |run_id: String| {
            let storage = &storage;
            let id = &id;
            async move {
                storage
                    .update_run_status(&run_id, RunStatus::Success, Some(3))
                    .await
                    .unwrap();
                storage.try_complete_backfill(id).await.unwrap()
            }
        };
        let (first, second) = tokio::join!(
            end_and_complete(last[0].clone()),
            end_and_complete(last[1].clone())
        );

        let completions: Vec<BackfillStatus> = [first, second].into_iter().flatten().collect();
        assert_eq!(completions, vec![BackfillStatus::CompletedSuccess]);
        assert_eq!(storage.try_complete_backfill(&id).await.unwrap(), None);
        let read = storage.get_backfill(&id).await.unwrap().unwrap();
        assert_eq!(read.status, BackfillStatus::CompletedSuccess);
        assert_eq!(read.completed_partitions, RUNS as u64);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_that_a_cancel_lets_through_reaches_its_cascade() {
    let Some(storage) = store().await else {
        return;
    };
    for round in 0..20 {
        let id = format!("bf{round}");
        storage
            .create_backfill(&backfill(&id, BackfillStatus::InProgress), &[])
            .await
            .unwrap();

        let (linked, cascade) = tokio::join!(storage.link_backfill_run(&id, "r", 0, 1), async {
            let status = storage.cancel_backfill(&id).await.unwrap();
            assert_eq!(status, BackfillStatus::Canceled);
            storage.get_open_backfill_runs(&id).await.unwrap()
        });

        let linked = linked.unwrap();
        let expected: Vec<String> = if linked { vec!["r".into()] } else { vec![] };
        assert_eq!(cascade, expected, "round {round}");
        let read = storage.get_backfill(&id).await.unwrap().unwrap();
        assert_eq!(read.run_ids, expected, "round {round}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_that_a_cancel_lets_through_reaches_its_cascade() {
    let Some(storage) = store().await else {
        return;
    };
    for round in 0..20 {
        let id = format!("bf{round}");
        storage
            .create_backfill(&backfill(&id, BackfillStatus::InProgress), &[])
            .await
            .unwrap();
        let records: Vec<RunRecord> = ["a", "b"]
            .into_iter()
            .map(|partition| {
                run(
                    &format!("{id}-{partition}"),
                    Some(key(&[partition])),
                    Some(&id),
                )
            })
            .collect();
        let run_ids: Vec<String> = records.iter().map(|r| r.run_id.clone()).collect();

        let (live, cascade) = tokio::join!(storage.enqueue_backfill_runs(&records, &id), async {
            let status = storage.cancel_backfill(&id).await.unwrap();
            assert_eq!(status, BackfillStatus::Canceled);
            storage.get_open_backfill_runs(&id).await.unwrap()
        });

        if live.unwrap() {
            assert_eq!(cascade, run_ids, "round {round}");
        } else {
            for run_id in &run_ids {
                let run = storage.get_run(run_id).await.unwrap().unwrap();
                assert_eq!(run.status, RunStatus::Canceled, "round {round}");
            }
            let read = storage.get_backfill(&id).await.unwrap().unwrap();
            assert_eq!(read.canceled_partitions, 2, "round {round}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backfill_that_gets_a_run_while_it_resumes_is_not_resumed() {
    let Some(storage) = store().await else {
        return;
    };
    for round in 0..20 {
        let id = format!("bf{round}");
        storage
            .create_backfill(&backfill(&id, BackfillStatus::InProgress), &[])
            .await
            .unwrap();

        let (linked, resumed) = tokio::join!(
            storage.link_backfill_run(&id, "r", 0, 1),
            storage.resume_stalled_backfill(&id)
        );

        let (linked, resumed) = (linked.unwrap(), resumed.unwrap());
        assert!(
            linked != resumed,
            "round {round}: linked {linked}, resumed {resumed}"
        );
        let read = storage.get_backfill(&id).await.unwrap().unwrap();
        let expected = if linked {
            BackfillStatus::InProgress
        } else {
            BackfillStatus::Requested
        };
        assert_eq!(read.status, expected, "round {round}");
    }
}

#[tokio::test]
async fn a_run_linked_again_keeps_its_place() {
    let Some(storage) = store().await else {
        return;
    };
    storage
        .create_backfill(&backfill("bf", BackfillStatus::InProgress), &[])
        .await
        .unwrap();

    assert!(storage.link_backfill_run("bf", "r2", 0, 1).await.unwrap());
    assert!(storage.link_backfill_run("bf", "r1", 1, 1).await.unwrap());
    assert!(storage.link_backfill_run("bf", "r2", 0, 1).await.unwrap());
    assert!(
        !storage.link_backfill_run("bf", "r3", 1, 1).await.unwrap(),
        "a run number has one run"
    );

    assert_eq!(
        storage.link_rows("bf").await.unwrap(),
        vec![("r2".to_string(), 0, 1), ("r1".to_string(), 1, 1)]
    );
}
