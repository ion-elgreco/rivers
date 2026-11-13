//! The partitions of an asset by state: counts for a run of keys, and rows.

use std::collections::HashMap;

use anyhow::Result;

use crate::storage::{PartitionBounds, PartitionKey, ScopedStorage, StorageBackend, stored_order};

/// The failed partitions whose row is read to compare the time of the failure
/// with the time of the materialization. A failed partition above this number
/// counts as failed, and as materialized too when it has a row.
const FAILURES_CHECKED: usize = 10_000;

/// The state of one partition of an asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionState {
    Materialized,
    Failed,
    Missing,
}

/// A partition that failed and was not materialized again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedPartition {
    pub key: PartitionKey,
    pub failed_at: i64,
    /// The partition has a row: a materialization older than the failure.
    pub has_row: bool,
}

/// The partitions of an asset that failed and were not materialized again,
/// in the order of the store.
#[derive(Debug, Clone, Default)]
pub struct FailedPartitions {
    partitions: Vec<FailedPartition>,
}

impl FailedPartitions {
    /// The failed partitions of `asset_key`: all of them, or with `bounds`
    /// those in one of the bounds. The rows of the partitions outside the
    /// bounds are not read.
    pub async fn read<S: StorageBackend>(
        storage: &ScopedStorage<'_, S>,
        asset_key: &str,
        bounds: Option<&[PartitionBounds]>,
    ) -> Result<Self> {
        let mut failures = storage.get_partition_failures(asset_key).await?;
        if let Some(bounds) = bounds {
            failures.retain(|key, _| bounds.iter().any(|bounds| bounds.contains(key)));
        }

        // A failed partition can have a row: the row is newer (the partition
        // was materialized again) or older (it counts as failed).
        let checked: Vec<PartitionKey> = failures.keys().take(FAILURES_CHECKED).cloned().collect();
        let built_at: HashMap<PartitionKey, i64> = storage
            .get_partition_timestamps_for_keys(asset_key, &checked)
            .await?
            .into_iter()
            .map(|(key, time, _)| (key, time))
            .collect();

        let mut partitions: Vec<FailedPartition> = failures
            .into_iter()
            .filter_map(|(key, failed_at)| match built_at.get(&key) {
                Some(built) if *built >= failed_at => None,
                row => Some(FailedPartition {
                    has_row: row.is_some(),
                    key,
                    failed_at,
                }),
            })
            .collect();
        partitions.sort_by(|a, b| stored_order(&a.key, &b.key));

        Ok(Self { partitions })
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// The failed partitions in `bounds`.
    pub fn within(&self, bounds: &PartitionBounds) -> &[FailedPartition] {
        let start = self
            .partitions
            .partition_point(|failed| stored_order(&failed.key, &bounds.lower).is_lt());
        let end = self
            .partitions
            .partition_point(|failed| stored_order(&failed.key, &bounds.upper).is_le());
        &self.partitions[start..end.max(start)]
    }

    pub fn get(&self, key: &PartitionKey) -> Option<&FailedPartition> {
        self.partitions
            .binary_search_by(|failed| stored_order(&failed.key, key))
            .ok()
            .map(|at| &self.partitions[at])
    }

    fn states(failed: &[FailedPartition], rows: u64) -> StateCounts {
        let with_row = failed.iter().filter(|failed| failed.has_row).count() as u64;
        StateCounts {
            materialized: rows.saturating_sub(with_row),
            failed: failed.len() as u64,
        }
    }
}

/// The partitions of an asset, or of a run of its keys, by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StateCounts {
    pub materialized: u64,
    pub failed: u64,
}

/// The partitions of `asset_key` by state.
pub async fn count_states<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
) -> Result<StateCounts> {
    let rows = storage.count_materialized_partitions(asset_key).await?;
    Ok(FailedPartitions::states(&failed.partitions, rows))
}

/// The partitions in each of `bounds` by state, in the order of the bounds.
///
/// No read grows with the number of materialized partitions.
pub async fn count_states_in<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
    bounds: &[PartitionBounds],
) -> Result<Vec<StateCounts>> {
    let rows = storage
        .count_materialized_partitions_in(asset_key, bounds)
        .await?;
    Ok(bounds
        .iter()
        .zip(rows)
        .map(|(bounds, rows)| FailedPartitions::states(failed.within(bounds), rows))
        .collect())
}

/// One partition as a row of a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    pub state: PartitionState,
    /// The time of the last materialization. A failed partition can have one.
    pub materialized_at: Option<i64>,
    /// The run of the last materialization.
    pub run_id: Option<String>,
    /// The time of the failure of a failed partition.
    pub failed_at: Option<i64>,
}

/// The rows that a list of partitions has room for when the store has rows
/// of keys that the list does not hold.
const ROWS_SPARE: u64 = 100;

/// A row for each of `partitions`, which are the partitions in `bounds`.
pub async fn partition_rows_in<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    failed: &FailedPartitions,
    bounds: &PartitionBounds,
    partitions: &[PartitionKey],
) -> Result<Vec<PartitionRow>> {
    let limit = (partitions.len() as u64)
        .saturating_mul(2)
        .saturating_add(ROWS_SPARE);
    let mut stored = storage
        .get_partition_timestamps_in(asset_key, bounds, limit)
        .await?;

    // The bounds hold more rows than the read returns: read by key.
    if stored.len() as u64 == limit {
        stored = storage
            .get_partition_timestamps_for_keys(asset_key, partitions)
            .await?;
    }

    Ok(rows_of(failed, partitions, stored))
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
    Ok(rows_of(failed, partitions, stored))
}

fn rows_of(
    failed: &FailedPartitions,
    partitions: &[PartitionKey],
    stored: Vec<(PartitionKey, i64, Option<String>)>,
) -> Vec<PartitionRow> {
    let stored: HashMap<PartitionKey, (i64, Option<String>)> = stored
        .into_iter()
        .map(|(key, time, run_id)| (key, (time, run_id)))
        .collect();

    partitions
        .iter()
        .map(|partition| {
            let built = stored.get(partition);
            let materialized_at = built.map(|(time, _)| *time);
            let failure = failed
                .get(partition)
                .filter(|failure| materialized_at.is_none_or(|built| built < failure.failed_at));

            match failure {
                Some(failure) => PartitionRow {
                    state: PartitionState::Failed,
                    materialized_at,
                    run_id: None,
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
    use crate::storage::surrealdb_backend::SurrealStorage;
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

    async fn storage_with(asset: &str, events: &[EventRecord]) -> SurrealStorage {
        let storage = SurrealStorage::new_memory().await.unwrap();
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
            .register_assets(&[record])
            .await
            .unwrap();
        for event in events {
            storage.store_event(event).await.unwrap();
        }
        storage
    }

    async fn failed_and_scoped(
        storage: &SurrealStorage,
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
    async fn trips() -> SurrealStorage {
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
                row(Failed, Some(100), None, Some(200)),
                row(Failed, None, None, Some(200)),
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
        let of_the_next_hour = FailedPartitions::read(
            &scoped,
            "trips",
            Some(&[PartitionBounds::of_dimension(&[], "hour", "01:00", "01:00")]),
        )
        .await
        .unwrap();

        assert_eq!(of_zone_c.len(), 1);
        assert!(of_zone_c.get(&cell("00:00", "c")).is_some());
        assert!(of_zone_c.get(&cell("00:00", "b")).is_none());
        assert!(of_the_next_hour.is_empty());
    }

    #[test]
    fn a_partition_materialized_after_its_failure_is_not_failed() {
        // A failure whose row was not read, as above `FAILURES_CHECKED`.
        let failed = FailedPartitions {
            partitions: vec![FailedPartition {
                key: single("p1"),
                failed_at: 200,
                has_row: false,
            }],
        };
        let stored = |at: i64| vec![(single("p1"), at, Some(format!("run-{at}")))];

        let built_later = rows_of(&failed, &[single("p1")], stored(300));
        let built_before = rows_of(&failed, &[single("p1")], stored(100));
        let not_built = rows_of(&failed, &[single("p1")], vec![]);

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
        assert_eq!(not_built[0].state, Failed);
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
        assert_eq!(failed.len(), 2);
        assert_eq!(
            failed.get(&single("2024-01-03")).map(|f| f.has_row),
            Some(false)
        );
        assert_eq!(failed.get(&single("2024-01-01")), None);
    }
}
