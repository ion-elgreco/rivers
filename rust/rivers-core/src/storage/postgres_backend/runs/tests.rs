use std::collections::BTreeSet;

use super::*;

async fn test_storage() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    Some(PostgresStorage::connect_test_schema(&url).await.unwrap())
}

fn run(run_id: &str, status: RunStatus) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status,
        start_time: 1000,
        end_time: None,
        execution_start_time: None,
        tags: vec![],
        node_names: vec!["a".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::default(),
        action: None,
        config: None,
    }
}

/// Two writers that change the status of one run at the same time: exactly
/// one of them wins, and the run holds the status of the winner.
#[tokio::test]
async fn status_changes_of_one_run_have_one_winner() {
    let Some(storage) = test_storage().await else {
        return;
    };
    let runs: Vec<RunRecord> = (0..40)
        .map(|i| run(&format!("r{i}"), RunStatus::NotStarted))
        .collect();
    storage.create_runs(&runs).await.unwrap();

    let races = (0..20).map(|i| {
        let storage = &storage;
        async move {
            let run_id = format!("r{i}");
            let (canceled, started) = tokio::join!(
                storage.cancel_queued_run(&run_id),
                storage.try_start_run(&run_id)
            );
            let (canceled, started) = (canceled.unwrap(), started.unwrap());
            assert_ne!(canceled, started, "{run_id}: exactly one side must win");
            let expected = if canceled {
                RunStatus::Canceled
            } else {
                RunStatus::Started
            };
            let stored = storage.get_run(&run_id).await.unwrap().unwrap();
            assert_eq!(stored.status, expected, "{run_id}");
        }
    });
    futures_util::future::join_all(races).await;

    let races = (20..40).map(|i| {
        let storage = &storage;
        async move {
            let run_id = format!("r{i}");
            let (canceled, failed) = tokio::join!(
                storage.cancel_queued_run(&run_id),
                storage.fail_run_if_active(&run_id, 5000)
            );
            let (canceled, failed) = (canceled.unwrap(), failed.unwrap());
            assert_ne!(canceled, failed, "{run_id}: exactly one side must win");
            let expected = if canceled {
                RunStatus::Canceled
            } else {
                RunStatus::Failure
            };
            let stored = storage.get_run(&run_id).await.unwrap().unwrap();
            assert_eq!(stored.status, expected, "{run_id}");
        }
    });
    futures_util::future::join_all(races).await;
}

/// Each field of `RunRecord` has a column in `runs`, and the inserts name it.
/// The inserts read the runs through `jsonb_populate_recordset`, which drops
/// a field without a column: a new field would be lost without an error.
#[tokio::test]
async fn every_field_of_a_run_has_a_column() {
    let Some(storage) = test_storage().await else {
        return;
    };
    let full = RunRecord {
        end_time: Some(2000),
        execution_start_time: Some(1500),
        tags: vec![("team".to_string(), "data".to_string())],
        priority: 3,
        partition_key: Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        }),
        block_reason: Some("pool full".to_string()),
        launched_by: LaunchedBy::Backfill {
            backfill_id: "bf".to_string(),
        },
        action: Some("check".to_string()),
        config: Some("{}".to_string()),
        ..run("r1", RunStatus::Success)
    };
    let serde_json::Value::Object(fields) = serde_json::to_value(&full).unwrap() else {
        panic!("a run is a JSON object");
    };
    let fields: BTreeSet<String> = fields.into_iter().map(|(name, _)| name).collect();

    let inserted: BTreeSet<String> = RUN_COLUMNS
        .split(',')
        .map(|name| name.trim().to_string())
        .collect();
    assert_eq!(fields, inserted, "the inserts name each field of a run");
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'runs'",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();
    let columns: BTreeSet<String> = columns.into_iter().collect();
    let missing: Vec<&String> = fields.difference(&columns).collect();
    assert!(missing.is_empty(), "fields without a column: {missing:?}");

    storage.create_run(&full).await.unwrap();
    assert_eq!(storage.get_run("r1").await.unwrap(), Some(full));
}
