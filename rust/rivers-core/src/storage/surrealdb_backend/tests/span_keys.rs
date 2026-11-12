//! Runs whose key holds a span of time windows, as the planner of a backfill
//! writes them.

use jiff::civil::date;

use super::*;
use crate::execution::backfill::plan_runs;
use crate::execution::backfill_spans::with_spans;
use crate::timegrid::TimeGrid;

/// The hours of the first half of 2024.
pub(super) const HOURS: usize = 4368;

/// One run key for each zone, over half a year of hours: with every hour
/// written out, and with the hours as one span.
pub(super) fn zone_run_keys(zones: &[&str]) -> (Vec<PartitionKey>, Vec<PartitionKey>) {
    let grid = TimeGrid {
        cron_schedule: Some("0 * * * *".to_string()),
        interval_seconds: None,
        start: date(2024, 1, 1).at(0, 0, 0, 0),
        end: None,
        fmt: "%Y-%m-%dT%H:00".to_string(),
    };
    let hours = grid
        .keys_in_range(
            date(2024, 1, 1).at(0, 0, 0, 0),
            date(2024, 6, 30).at(23, 0, 0, 0),
        )
        .unwrap();
    assert_eq!(hours.len(), HOURS);

    let requested = [PartitionKey::Multi {
        dims: vec![
            ("hour".to_string(), hours),
            (
                "zone".to_string(),
                zones.iter().map(|zone| zone.to_string()).collect(),
            ),
        ],
    }];
    let strategy = BackfillStrategy::PerDimension {
        multi_run: vec!["zone".to_string()],
        single_run: vec!["hour".to_string()],
    };
    let written_out = plan_runs(&strategy, &requested);
    let with_spans = with_spans(written_out.clone(), &[(Some("hour".to_string()), grid)]);
    (written_out, with_spans)
}

/// One queued run for each key.
pub(super) fn queued_runs(keys: &[PartitionKey]) -> Vec<RunRecord> {
    keys.iter()
        .enumerate()
        .map(|(number, key)| {
            let mut run = minimal_run(&format!("run-{number}"), RunStatus::Queued);
            run.node_names = vec!["trips".to_string()];
            run.partition_key = Some(key.clone());
            run
        })
        .collect()
}

fn hour_of_zone(hour: &str, zone: &str) -> PartitionKey {
    PartitionKey::Multi {
        dims: vec![
            ("hour".to_string(), vec![hour.to_string()]),
            ("zone".to_string(), vec![zone.to_string()]),
        ],
    }
}

#[tokio::test]
async fn test_queued_backfill_runs_keep_their_span_keys() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf", BackfillStatus::InProgress, 100), &[])
        .await
        .unwrap();
    let (_, keys) = zone_run_keys(&["132", "7"]);
    let records = queued_runs(&keys);

    let live = storage.enqueue_backfill_runs(&records, "bf").await.unwrap();

    assert!(live);
    let backfill = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(backfill.run_ids, vec!["run-0", "run-1"]);

    let queued = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(queued.len(), 2);
    for (record, key) in records.iter().zip(&keys) {
        assert!(matches!(key, PartitionKey::MultiItems { .. }), "{key:?}");

        let run = storage.get_run(&record.run_id).await.unwrap().unwrap();
        assert_eq!(run.partition_key.as_ref(), Some(key));
        assert_eq!(run.partition_key.unwrap().member_count(), HOURS);

        let (events, total) = storage
            .get_run_structured_events_page(&record.run_id, None, 0, 10)
            .await
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(events[0].event_type, EventType::RunQueued);
        assert_eq!(events[0].partition_key.as_ref(), Some(key));
    }

    #[derive(SurrealValue)]
    struct Link {
        run_id: String,
        member_count: i64,
    }
    let mut links: Vec<Link> = storage
        .db
        .query("SELECT run_id, member_count FROM backfill_runs WHERE backfill_id = 'bf'")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    links.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    let links: Vec<(String, i64)> = links
        .into_iter()
        .map(|link| (link.run_id, link.member_count))
        .collect();
    assert_eq!(
        links,
        vec![
            ("run-0".to_string(), HOURS as i64),
            ("run-1".to_string(), HOURS as i64)
        ]
    );
}

#[tokio::test]
async fn test_failure_with_a_span_key_fails_each_window() {
    let storage = make_storage().await;
    register(&storage, &["trips"]).await;
    let (_, keys) = zone_run_keys(&["132"]);
    let mut run = minimal_run("failed", RunStatus::Failure);
    run.job_name = None;
    run.node_names = vec!["trips".to_string()];
    run.partition_key = Some(keys[0].clone());
    run.end_time = Some(1500);
    storage.create_run(&run).await.unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("trips".to_string()),
            run_id: "failed".to_string(),
            partition_key: Some(keys[0].clone()),
            timestamp: 1400,
            metadata: vec![("error".to_string(), "boom".to_string())],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "trips",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(failed.len(), HOURS);
    assert!(failed.contains_key(&hour_of_zone("2024-01-01T00:00", "132")));
    assert!(failed.contains_key(&hour_of_zone("2024-06-30T23:00", "132")));
    assert!(!failed.contains_key(&hour_of_zone("2024-07-01T00:00", "132")));
}
