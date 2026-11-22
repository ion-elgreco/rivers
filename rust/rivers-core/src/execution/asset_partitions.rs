//! The partitions of an asset by state: counts for a run of keys, and rows.

use std::borrow::Cow;
use std::collections::HashMap;

use anyhow::Result;

use crate::storage::{
    FailureCount, PartitionBounds, PartitionFailure, PartitionKey, ScopedStorage, StorageBackend,
    stored_order,
};

/// Up to this many failed partitions of an asset are read at once. With
/// more, the failures in a run of keys are counted, and those of a list of
/// partitions are read, when needed.
const FAILURES_READ_AT_ONCE: u64 = 10_000;

/// The state of one partition of an asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionState {
    Materialized,
    Failed,
    Missing,
}

/// The partitions of an asset that failed and were not materialized or
/// deleted again.
#[derive(Debug, Clone, Default)]
pub struct FailedPartitions {
    /// All the failed partitions of the asset.
    count: FailureCount,
    /// The failed partitions in the order of the store: all of them, or
    /// those in the bounds of the read. `None` when the asset has more than
    /// [`FAILURES_READ_AT_ONCE`].
    partitions: Option<Vec<PartitionFailure>>,
}

impl FailedPartitions {
    /// The failed partitions of `asset_key`: all of them, or with `bounds`
    /// those in one of the bounds. With more than [`FAILURES_READ_AT_ONCE`],
    /// only their number.
    pub async fn read<S: StorageBackend>(
        storage: &ScopedStorage<'_, S>,
        asset_key: &str,
        bounds: Option<&[PartitionBounds]>,
    ) -> Result<Self> {
        let count = storage.count_partition_failures(asset_key).await?;
        let partitions = match count.failed {
            0 => Some(Vec::new()),
            failed if failed <= FAILURES_READ_AT_ONCE => {
                let mut partitions = match bounds {
                    Some(bounds) => Self::read_in(storage, asset_key, bounds).await?,
                    None => {
                        storage
                            .get_partition_failures(asset_key, None, FAILURES_READ_AT_ONCE)
                            .await?
                    }
                };
                partitions.sort_by(|a, b| stored_order(&a.key, &b.key));
                Some(partitions)
            }
            _ => None,
        };

        Ok(Self { count, partitions })
    }

    /// The failed partitions in one of `bounds`, of an asset with at most
    /// [`FAILURES_READ_AT_ONCE`]. One read from the lowest key of the bounds
    /// to the highest: a read per bounds costs more than the rows of a page.
    async fn read_in<S: StorageBackend>(
        storage: &ScopedStorage<'_, S>,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<PartitionFailure>> {
        let lower = bounds
            .iter()
            .map(|bounds| &bounds.lower)
            .min_by(|a, b| stored_order(a, b));
        let upper = bounds
            .iter()
            .map(|bounds| &bounds.upper)
            .max_by(|a, b| stored_order(a, b));
        let (Some(lower), Some(upper)) = (lower, upper) else {
            return Ok(Vec::new());
        };
        let around = PartitionBounds {
            lower: lower.clone(),
            upper: upper.clone(),
        };

        let mut partitions = storage
            .get_partition_failures(asset_key, Some(&around), FAILURES_READ_AT_ONCE)
            .await?;
        partitions.retain(|failed| bounds.iter().any(|bounds| bounds.contains(&failed.key)));
        Ok(partitions)
    }

    /// All the failed partitions of the asset.
    pub fn count(&self) -> FailureCount {
        self.count
    }

    /// The failed partitions in `bounds` of those read.
    fn within<'a>(
        read: &'a [PartitionFailure],
        bounds: &PartitionBounds,
    ) -> &'a [PartitionFailure] {
        let start = read.partition_point(|failed| stored_order(&failed.key, &bounds.lower).is_lt());
        let end = read.partition_point(|failed| stored_order(&failed.key, &bounds.upper).is_le());
        &read[start..end.max(start)]
    }

    /// The failed partitions in each of `bounds`.
    async fn counts_in<S: StorageBackend>(
        &self,
        storage: &ScopedStorage<'_, S>,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<FailureCount>> {
        let Some(read) = &self.partitions else {
            return storage.count_partition_failures_in(asset_key, bounds).await;
        };
        Ok(bounds
            .iter()
            .map(|bounds| {
                let within = Self::within(read, bounds);
                FailureCount {
                    failed: within.len() as u64,
                    with_row: within.iter().filter(|failed| failed.has_row).count() as u64,
                }
            })
            .collect())
    }

    /// The failures of `partitions`, which are in `bounds` when given.
    async fn of<S: StorageBackend>(
        &self,
        storage: &ScopedStorage<'_, S>,
        asset_key: &str,
        bounds: Option<&PartitionBounds>,
        partitions: &[PartitionKey],
    ) -> Result<Vec<Cow<'_, PartitionFailure>>> {
        if let Some(read) = &self.partitions {
            return Ok(partitions
                .iter()
                .filter_map(|key| {
                    let at = read
                        .binary_search_by(|failed| stored_order(&failed.key, key))
                        .ok()?;
                    Some(Cow::Borrowed(&read[at]))
                })
                .collect());
        }

        let failures = match bounds {
            Some(bounds) => {
                let limit = rows_limit(partitions.len());
                let in_bounds = storage
                    .get_partition_failures(asset_key, Some(bounds), limit)
                    .await?;
                match in_bounds.len() as u64 == limit {
                    true => {
                        storage
                            .get_partition_failures_for_keys(asset_key, partitions)
                            .await?
                    }
                    false => in_bounds,
                }
            }
            None => {
                storage
                    .get_partition_failures_for_keys(asset_key, partitions)
                    .await?
            }
        };

        Ok(failures.into_iter().map(Cow::Owned).collect())
    }
}

/// The partitions of an asset, or of a run of its keys, by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StateCounts {
    pub materialized: u64,
    pub failed: u64,
}

impl StateCounts {
    /// The states of partitions with `rows` rows, of which `failures` failed
    /// later.
    fn of(rows: u64, failures: FailureCount) -> Self {
        StateCounts {
            materialized: rows.saturating_sub(failures.with_row),
            failed: failures.failed,
        }
    }
}

/// The partitions of `asset_key` by state.
pub async fn count_states<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
) -> Result<StateCounts> {
    let rows = storage.count_materialized_partitions(asset_key).await?;
    Ok(StateCounts::of(rows, failed.count))
}

/// The partitions in each of `bounds` by state, in the order of the bounds.
///
/// No read grows with the number of materialized or failed partitions.
pub async fn count_states_in<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
    bounds: &[PartitionBounds],
) -> Result<Vec<StateCounts>> {
    let (rows, failures) = tokio::try_join!(
        storage.count_materialized_partitions_in(asset_key, bounds),
        failed.counts_in(storage, asset_key, bounds),
    )?;
    Ok(rows
        .into_iter()
        .zip(failures)
        .map(|(rows, failures)| StateCounts::of(rows, failures))
        .collect())
}

/// One partition as a row of a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    pub state: PartitionState,
    /// The time of the last materialization. A failed partition can have one.
    pub materialized_at: Option<i64>,
    /// The run of the last materialization, or the run that failed a failed
    /// partition.
    pub run_id: Option<String>,
    /// The time of the failure of a failed partition.
    pub failed_at: Option<i64>,
}

/// The rows that a list of partitions has room for when the store has rows
/// of keys that the list does not hold.
const ROWS_SPARE: u64 = 100;

/// The rows that a read of the range of `partitions` returns before the
/// partitions are read by key.
fn rows_limit(partitions: usize) -> u64 {
    (partitions as u64)
        .saturating_mul(2)
        .saturating_add(ROWS_SPARE)
}

/// A row for each of `partitions`, which are the partitions in `bounds`.
pub async fn partition_rows_in<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
    bounds: &PartitionBounds,
    partitions: &[PartitionKey],
) -> Result<Vec<PartitionRow>> {
    let limit = rows_limit(partitions.len());
    let mut stored = storage
        .get_partition_timestamps_in(asset_key, bounds, limit)
        .await?;

    // The bounds hold more rows than the read returns: read by key.
    if stored.len() as u64 == limit {
        stored = storage
            .get_partition_timestamps_for_keys(asset_key, partitions)
            .await?;
    }

    let failures = failed
        .of(storage, asset_key, Some(bounds), partitions)
        .await?;
    Ok(rows_of(&failures, partitions, stored))
}

/// A row for each of `partitions`, each read by its key.
pub async fn partition_rows_of<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
    partitions: &[PartitionKey],
) -> Result<Vec<PartitionRow>> {
    let stored = storage
        .get_partition_timestamps_for_keys(asset_key, partitions)
        .await?;
    let failures = failed.of(storage, asset_key, None, partitions).await?;
    Ok(rows_of(&failures, partitions, stored))
}

fn rows_of(
    failures: &[Cow<'_, PartitionFailure>],
    partitions: &[PartitionKey],
    stored: Vec<(PartitionKey, i64, Option<String>)>,
) -> Vec<PartitionRow> {
    let failures: HashMap<&PartitionKey, &PartitionFailure> = failures
        .iter()
        .map(|failed| (&failed.key, failed.as_ref()))
        .collect();
    let stored: HashMap<PartitionKey, (i64, Option<String>)> = stored
        .into_iter()
        .map(|(key, time, run_id)| (key, (time, run_id)))
        .collect();

    partitions
        .iter()
        .map(|partition| {
            let built = stored.get(partition);
            let materialized_at = built.map(|(time, _)| *time);
            // The rows were read apart from the failures: a materialization
            // between the two reads replaced the failure.
            let failure = failures
                .get(partition)
                .filter(|failure| materialized_at.is_none_or(|built| built < failure.failed_at));

            match failure {
                Some(failure) => PartitionRow {
                    state: PartitionState::Failed,
                    materialized_at,
                    run_id: Some(failure.run_id.clone()),
                    failed_at: Some(failure.failed_at),
                },
                None => PartitionRow {
                    state: match built {
                        Some(_) => PartitionState::Materialized,
                        None => PartitionState::Missing,
                    },
                    materialized_at,
                    run_id: built.and_then(|(_, run_id)| run_id.clone()),
                    failed_at: None,
                },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::any::AnyStorage;
    use crate::storage::{
        AssetRecord, CodeLocationContext, DEFAULT_CODE_LOCATION_ID, EventRecord, EventType,
    };

    use PartitionState::{Failed, Materialized, Missing};

    fn single(key: &str) -> PartitionKey {
        PartitionKey::Single {
            keys: vec![key.to_string()],
        }
    }

    fn cell(hour: &str, zone: &str) -> PartitionKey {
        PartitionKey::Multi {
            dims: vec![
                ("hour".to_string(), vec![hour.to_string()]),
                ("zone".to_string(), vec![zone.to_string()]),
            ],
        }
    }

    fn event(asset: &str, event_type: EventType, partition: PartitionKey, at: i64) -> EventRecord {
        EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type,
            asset_key: Some(asset.to_string()),
            run_id: format!("run-{at}"),
            partition_key: Some(partition),
            timestamp: at,
            metadata: vec![],
            input_data_versions: vec![],
        }
    }

    fn built(asset: &str, partition: PartitionKey, at: i64) -> EventRecord {
        let event_type = EventType::Materialization { data_version: None };
        event(asset, event_type, partition, at)
    }

    fn failure(asset: &str, partition: PartitionKey, at: i64) -> EventRecord {
        event(asset, EventType::StepFailure, partition, at)
    }

    async fn storage_with(asset: &str, events: &[EventRecord]) -> AnyStorage {
        let storage = AnyStorage::new_memory().await.unwrap();
        let record = AssetRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: asset.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        storage
            .for_code_location(&CodeLocationContext::default_for_tests())
            .register_assets(vec![record])
            .await
            .unwrap();
        for event in events {
            storage.store_event(event).await.unwrap();
        }
        storage
    }

    /// The storage of `asset` after one drain of `events`.
    async fn storage_with_drain(asset: &str, events: &[EventRecord]) -> AnyStorage {
        let storage = storage_with(asset, &[]).await;
        storage.store_events(events).await.unwrap();
        storage
    }

    async fn failed_and_scoped(
        storage: &AnyStorage,
        asset: &str,
    ) -> (FailedPartitions, CodeLocationContext) {
        let ctx = CodeLocationContext::default_for_tests();
        let failed = FailedPartitions::read(&storage.for_code_location(&ctx), asset, None)
            .await
            .unwrap();
        (failed, ctx)
    }

    /// `00:00|b` was materialized and failed later, `00:00|c` failed only,
    /// `01:00|a` failed and was materialized again.
    async fn trips() -> AnyStorage {
        storage_with(
            "trips",
            &[
                built("trips", cell("00:00", "a"), 100),
                built("trips", cell("00:00", "b"), 100),
                failure("trips", cell("00:00", "b"), 200),
                failure("trips", cell("00:00", "c"), 200),
                failure("trips", cell("01:00", "a"), 200),
                built("trips", cell("01:00", "a"), 300),
                built("trips", cell("01:00", "c"), 300),
            ],
        )
        .await
    }

    fn counts(materialized: u64, failed: u64) -> StateCounts {
        StateCounts {
            materialized,
            failed,
        }
    }

    #[tokio::test]
    async fn states_are_counted_for_each_run_of_keys() {
        let storage = trips().await;
        let (failed, ctx) = failed_and_scoped(&storage, "trips").await;
        let scoped = storage.for_code_location(&ctx);
        let hour = ("hour".to_string(), "00:00".to_string());

        let of_asset = count_states(&scoped, "trips", &failed).await.unwrap();
        let of_runs = count_states_in(
            &scoped,
            "trips",
            &failed,
            &[
                PartitionBounds::of_dimension(&[], "hour", "00:00", "00:00"),
                PartitionBounds::of_dimension(&[], "hour", "01:00", "01:00"),
                PartitionBounds::of_dimension(&[], "hour", "00:00", "01:00"),
                PartitionBounds::of_dimension(&[], "hour", "02:00", "23:00"),
                PartitionBounds::of_dimension(std::slice::from_ref(&hour), "zone", "a", "b"),
                PartitionBounds::of_dimension(&[hour], "zone", "c", "c"),
            ],
        )
        .await
        .unwrap();

        assert_eq!(of_asset, counts(3, 2));
        assert_eq!(
            of_runs,
            vec![
                counts(1, 2),
                counts(2, 0),
                counts(3, 2),
                counts(0, 0),
                counts(1, 1),
                counts(0, 1),
            ]
        );
    }

    #[tokio::test]
    async fn rows_of_the_partitions_of_a_run_of_keys() {
        let storage = trips().await;
        let (failed, ctx) = failed_and_scoped(&storage, "trips").await;
        let scoped = storage.for_code_location(&ctx);
        let hour = ("hour".to_string(), "00:00".to_string());
        let partitions: Vec<PartitionKey> = ["a", "b", "c", "d"]
            .iter()
            .map(|zone| cell("00:00", zone))
            .collect();
        let bounds = PartitionBounds::of_dimension(&[hour], "zone", "a", "d");

        let in_bounds = partition_rows_in(&scoped, "trips", &failed, &bounds, &partitions)
            .await
            .unwrap();
        let by_key = partition_rows_of(&scoped, "trips", &failed, &partitions)
            .await
            .unwrap();

        let row = |state, materialized_at, run_id: Option<&str>, failed_at| PartitionRow {
            state,
            materialized_at,
            run_id: run_id.map(str::to_string),
            failed_at,
        };
        assert_eq!(
            in_bounds,
            vec![
                row(Materialized, Some(100), Some("run-100"), None),
                row(Failed, Some(100), Some("run-200"), Some(200)),
                row(Failed, None, Some("run-200"), Some(200)),
                row(Missing, None, None, None),
            ]
        );
        assert_eq!(by_key, in_bounds);
    }

    #[tokio::test]
    async fn rows_are_read_by_key_when_the_bounds_hold_more_rows_than_the_list() {
        let events: Vec<EventRecord> = (0..110)
            .map(|i| built("plain", single(&format!("p{i:03}")), 100 + i))
            .collect();
        let storage = storage_with("plain", &events).await;
        let (failed, ctx) = failed_and_scoped(&storage, "plain").await;
        let scoped = storage.for_code_location(&ctx);

        // The list holds two keys; the bounds hold the rows of 110.
        let partitions = vec![single("p000"), single("p109")];
        let bounds = PartitionBounds::of_keys("p000", "p109");
        let rows = partition_rows_in(&scoped, "plain", &failed, &bounds, &partitions)
            .await
            .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.state).collect::<Vec<_>>(),
            vec![Materialized, Materialized]
        );
        assert_eq!(rows[1].materialized_at, Some(209));
    }

    #[tokio::test]
    async fn failed_partitions_outside_the_bounds_are_left_out() {
        let storage = trips().await;
        let ctx = CodeLocationContext::default_for_tests();
        let scoped = storage.for_code_location(&ctx);
        let hour = ("hour".to_string(), "00:00".to_string());
        let of_zone_c = FailedPartitions::read(
            &scoped,
            "trips",
            Some(&[PartitionBounds::of_dimension(&[hour], "zone", "c", "c")]),
        )
        .await
        .unwrap();

        let rows = partition_rows_of(
            &scoped,
            "trips",
            &of_zone_c,
            &[cell("00:00", "b"), cell("00:00", "c")],
        )
        .await
        .unwrap();

        // `00:00|b` failed too, outside the bounds of the read.
        let states: Vec<PartitionState> = rows.iter().map(|row| row.state).collect();
        assert_eq!(states, vec![Materialized, Failed]);
        assert_eq!(of_zone_c.count().failed, 2);
    }

    #[tokio::test]
    async fn failed_partitions_between_the_bounds_are_left_out() {
        let events: Vec<EventRecord> = (0..6)
            .map(|i| failure("plain", single(&format!("p{i}")), 200))
            .collect();
        let storage = storage_with("plain", &events).await;
        let ctx = CodeLocationContext::default_for_tests();
        let scoped = storage.for_code_location(&ctx);
        let read = |bounds: Vec<PartitionBounds>| {
            let scoped = &scoped;
            async move {
                let failed = FailedPartitions::read(scoped, "plain", Some(&bounds))
                    .await
                    .unwrap();
                assert_eq!(failed.count().failed, 6);
                failed
                    .partitions
                    .unwrap()
                    .into_iter()
                    .map(|failed| failed.key)
                    .collect::<Vec<_>>()
            }
        };

        let scattered = read(vec![
            PartitionBounds::of_keys("p4", "p5"),
            PartitionBounds::of_keys("p0", "p0"),
            PartitionBounds::of_keys("p0", "p1"),
        ])
        .await;
        let none = read(vec![]).await;

        assert_eq!(scattered, ["p0", "p1", "p4", "p5"].map(single));
        assert!(none.is_empty());
    }

    #[test]
    fn a_partition_materialized_after_its_failure_is_not_failed() {
        // The rows are read apart from the failures: a materialization
        // between the two reads replaced the failure.
        let failures = [Cow::Owned(PartitionFailure {
            key: single("p1"),
            failed_at: 200,
            run_id: "run-200".to_string(),
            has_row: false,
        })];
        let stored = |at: i64| vec![(single("p1"), at, Some(format!("run-{at}")))];

        let built_later = rows_of(&failures, &[single("p1")], stored(300));
        let built_before = rows_of(&failures, &[single("p1")], stored(100));
        let not_built = rows_of(&failures, &[single("p1")], vec![]);

        assert_eq!(
            built_later,
            vec![PartitionRow {
                state: Materialized,
                materialized_at: Some(300),
                run_id: Some("run-300".to_string()),
                failed_at: None,
            }]
        );
        assert_eq!(built_before[0].state, Failed);
        assert_eq!(built_before[0].failed_at, Some(200));
        assert_eq!(built_before[0].run_id.as_deref(), Some("run-200"));
        assert_eq!(not_built[0].state, Failed);
    }

    #[tokio::test]
    async fn states_of_more_failed_partitions_than_are_read_at_once() {
        let names: Vec<String> = (0..10_050).map(|i| format!("p{i:05}")).collect();
        let mut events: Vec<EventRecord> = names
            .iter()
            .map(|name| built("plain", single(name), 100))
            .collect();
        events.extend(names.iter().map(|name| failure("plain", single(name), 200)));
        let storage = storage_with_drain("plain", &events).await;
        let (failed, ctx) = failed_and_scoped(&storage, "plain").await;
        let scoped = storage.for_code_location(&ctx);

        let of_asset = count_states(&scoped, "plain", &failed).await.unwrap();
        let of_runs = count_states_in(
            &scoped,
            "plain",
            &failed,
            &[
                PartitionBounds::of_keys("p00000", "p00049"),
                PartitionBounds::of_keys("p10000", "p10049"),
            ],
        )
        .await
        .unwrap();
        let rows = partition_rows_of(&scoped, "plain", &failed, &[single("p10049")])
            .await
            .unwrap();

        assert_eq!(of_asset, counts(0, 10_050));
        assert_eq!(of_runs, vec![counts(0, 50), counts(0, 50)]);
        assert_eq!(rows[0].state, Failed);
        assert_eq!(rows[0].run_id.as_deref(), Some("run-200"));
    }

    #[tokio::test]
    async fn states_of_keys_with_one_dimension() {
        let storage = storage_with(
            "plain",
            &[
                built("plain", single("2024-01-01"), 100),
                built("plain", single("2024-01-02"), 100),
                failure("plain", single("2024-01-02"), 200),
                failure("plain", single("2024-01-03"), 200),
                built("plain", single("2024-02-01"), 100),
            ],
        )
        .await;
        let (failed, ctx) = failed_and_scoped(&storage, "plain").await;
        let scoped = storage.for_code_location(&ctx);

        let of_runs = count_states_in(
            &scoped,
            "plain",
            &failed,
            &[
                PartitionBounds::of_keys("2024-01-01", "2024-01-31"),
                PartitionBounds::of_keys("2024-02-01", "2024-02-29"),
            ],
        )
        .await
        .unwrap();

        assert_eq!(of_runs, vec![counts(1, 2), counts(1, 0)]);
        assert_eq!(
            failed.count(),
            FailureCount {
                failed: 2,
                with_row: 1
            }
        );
    }
}
