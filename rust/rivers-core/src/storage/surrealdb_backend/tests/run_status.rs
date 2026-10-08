use super::*;

fn ended_run(
    run_id: &str,
    cl: &str,
    job: Option<&str>,
    status: RunStatus,
    end_time: Option<i64>,
) -> RunRecord {
    RunRecord {
        code_location_id: cl.to_string(),
        job_name: job.map(str::to_string),
        end_time,
        ..minimal_run(run_id, status)
    }
}

fn ids(runs: &[RunRecord]) -> Vec<&str> {
    runs.iter().map(|r| r.run_id.as_str()).collect()
}

#[tokio::test]
async fn get_runs_ended_since_filters_and_orders() {
    let storage = make_storage().await;
    let cl = DEFAULT_CODE_LOCATION_ID;
    let runs = [
        ended_run("f300", cl, Some("j"), RunStatus::Failure, Some(300)),
        ended_run("f100", cl, Some("j"), RunStatus::Failure, Some(100)),
        ended_run("f200", cl, Some("k"), RunStatus::Failure, Some(200)),
        ended_run("adhoc", cl, None, RunStatus::Failure, Some(250)),
        ended_run("ok", cl, Some("j"), RunStatus::Success, Some(150)),
        ended_run("running", cl, Some("j"), RunStatus::Started, None),
        ended_run(
            "other_cl",
            "other",
            Some("j"),
            RunStatus::Failure,
            Some(220),
        ),
    ];
    storage.create_runs(&runs).await.unwrap();

    // `since` is inclusive; NONE end times, other statuses and other code
    // locations are excluded; oldest end first.
    let got = storage
        .get_runs_ended_since(cl, 100, RunStatus::Failure, None, 10)
        .await
        .unwrap();
    assert_eq!(ids(&got), ["f100", "f200", "adhoc", "f300"]);
    assert_eq!(got[0], runs[1]);

    let got = storage
        .get_runs_ended_since(cl, 101, RunStatus::Failure, None, 10)
        .await
        .unwrap();
    assert_eq!(ids(&got), ["f200", "adhoc", "f300"]);

    // A job filter drops other jobs and runs without a job.
    let jobs = ["j".to_string()];
    let got = storage
        .get_runs_ended_since(cl, 0, RunStatus::Failure, Some(&jobs), 10)
        .await
        .unwrap();
    assert_eq!(ids(&got), ["f100", "f300"]);

    let got = storage
        .get_runs_ended_since(cl, 0, RunStatus::Failure, None, 2)
        .await
        .unwrap();
    assert_eq!(ids(&got), ["f100", "f200"]);

    let got = storage
        .get_runs_ended_since(cl, 0, RunStatus::Success, None, 10)
        .await
        .unwrap();
    assert_eq!(ids(&got), ["ok"]);
}

#[tokio::test]
async fn get_run_failure_events_returns_failures_in_order() {
    let storage = make_storage().await;
    register(&storage, &["a", "b"]).await;
    let event = |event_type: EventType, asset: Option<&str>, run_id: &str, ts: i64| EventRecord {
        event_type,
        asset_key: asset.map(str::to_string),
        metadata: vec![("error".to_string(), format!("boom at {ts}"))],
        ..make_event("a", run_id, ts)
    };
    storage
        .store_events(&[
            event(EventType::StepStart, Some("a"), "r1", 10),
            event(EventType::StepFailure, Some("b"), "r1", 30),
            event(EventType::StepFailure, Some("a"), "r1", 20),
            event(EventType::StepSuccess, Some("a"), "r1", 25),
            event(EventType::RunLaunchFailed, None, "r1", 40),
            event(EventType::StepFailure, Some("a"), "r2", 50),
        ])
        .await
        .unwrap();

    let got = storage.get_run_failure_events("r1").await.unwrap();
    let got: Vec<(&str, Option<&str>, i64, &str)> = got
        .iter()
        .map(|e| {
            (
                e.event_type.type_name(),
                e.asset_key.as_deref(),
                e.timestamp,
                e.metadata[0].1.as_str(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("StepFailure", Some("a"), 20, "boom at 20"),
            ("StepFailure", Some("b"), 30, "boom at 30"),
            ("RunLaunchFailed", None, 40, "boom at 40"),
        ]
    );
}

/// The run-status read must walk `idx_runs_loc_status_end` in end_time order, not sort.
#[tokio::test]
async fn runs_ended_since_uses_end_time_index() {
    let temp = test_temp_dir::test_temp_dir!();
    let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
        .await
        .unwrap();
    let runs: Vec<RunRecord> = (0..2000i64)
        .map(|i| {
            let (status, end_time) = match i % 4 {
                0 => (RunStatus::Started, None),
                1 => (RunStatus::Success, Some(i)),
                _ => (RunStatus::Failure, Some(i)),
            };
            ended_run(&format!("r{i}"), "default", Some("j"), status, end_time)
        })
        .collect();
    s.create_runs(&runs).await.unwrap();

    for query in [
        "SELECT * FROM runs WHERE code_location_id = 'default' AND end_time >= 1000 \
             AND status = 'Failure' ORDER BY end_time ASC LIMIT 5 EXPLAIN",
        "SELECT * FROM runs WHERE code_location_id = 'default' AND end_time >= 1000 \
             AND status = 'Failure' AND job_name IN ['j'] ORDER BY end_time ASC LIMIT 5 EXPLAIN",
    ] {
        let plan: Vec<serde_json::Value> = s.db.query(query).await.unwrap().take(0).unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("idx_runs_loc_status_end"),
            "run-status read should scan idx_runs_loc_status_end: {plan}"
        );
        assert!(
            !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
            "run-status read should not sort — the index covers end_time order: {plan}"
        );
    }
}
