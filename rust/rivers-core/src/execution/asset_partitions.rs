//! The partitions of an asset with their state, one page at a time.

use std::collections::HashMap;

use anyhow::Result;

use crate::storage::{PartitionKey, ScopedStorage, StorageBackend};

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

/// The state of the partitions of one page, and the totals of the asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetPartitionsPage {
    /// The partitions of the asset in the state `Materialized`.
    pub materialized: u64,
    /// The partitions of the asset in the state `Failed`.
    pub failed: u64,
    /// The state of each partition of the page, in the order given.
    pub states: Vec<PartitionState>,
}

/// The state of `partitions`, the display keys of one page, and the totals of
/// `asset_key`.
///
/// No read grows with the number of materialized partitions: a count, the
/// failures, and one row per partition of the page.
pub async fn asset_partitions_page<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    partitions: &[String],
) -> Result<AssetPartitionsPage> {
    let with_row = storage.count_materialized_partitions(asset_key).await?;
    let failures = storage.get_partition_failures(asset_key).await?;

    // A failed partition can have a row: the row is newer (the partition was
    // materialized again) or older (it counts as failed, not as materialized).
    let checked: Vec<PartitionKey> = failures.keys().take(FAILURES_CHECKED).cloned().collect();
    let (mut rebuilt, mut failed_later) = (0u64, 0u64);
    for (partition, time, _) in storage
        .get_partition_timestamps_for_keys(asset_key, &checked)
        .await?
    {
        match failures.get(&partition) {
            Some(failed) if time >= *failed => rebuilt += 1,
            Some(_) => failed_later += 1,
            None => {}
        }
    }
    let failed = failures.len() as u64 - rebuilt;
    let materialized = with_row.saturating_sub(failed_later);

    let times = times_of(storage, asset_key, partitions).await?;
    let failed_at: HashMap<String, i64> = failures
        .into_iter()
        .map(|(partition, time)| (partition.to_display(), time))
        .collect();
    let states = partitions
        .iter()
        .map(|partition| {
            let time = times.get(partition);
            match failed_at.get(partition) {
                Some(failed) if time.is_none_or(|time| time < failed) => PartitionState::Failed,
                _ if time.is_some() => PartitionState::Materialized,
                _ => PartitionState::Missing,
            }
        })
        .collect();

    Ok(AssetPartitionsPage {
        materialized,
        failed,
        states,
    })
}

/// The materialization time of each of `partitions`, display keys, that has
/// one.
///
/// A display key with the form of several dimensions can also be a key of
/// one dimension. The form with dimensions is read first; the other form is
/// read only for the partitions that have no row then.
async fn times_of<S: StorageBackend>(
    storage: &ScopedStorage<'_, S>,
    asset_key: &str,
    partitions: &[String],
) -> Result<HashMap<String, i64>> {
    let mut times: HashMap<String, i64> = HashMap::new();
    let mut forms: Vec<Vec<PartitionKey>> = partitions
        .iter()
        .map(|partition| PartitionKey::display_candidates(partition))
        .collect();

    loop {
        let keys: Vec<PartitionKey> = partitions
            .iter()
            .zip(forms.iter_mut())
            .filter(|(partition, _)| !times.contains_key(*partition))
            .filter_map(|(_, forms)| forms.pop())
            .collect();
        if keys.is_empty() {
            return Ok(times);
        }

        let rows = storage
            .get_partition_timestamps_for_keys(asset_key, &keys)
            .await?;
        for (key, time, _) in rows {
            times.insert(key.to_display(), time);
        }
    }
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

    async fn page(
        storage: &SurrealStorage,
        asset: &str,
        partitions: &[&str],
    ) -> AssetPartitionsPage {
        let ctx = CodeLocationContext::default_for_tests();
        let partitions: Vec<String> = partitions.iter().map(|p| p.to_string()).collect();
        asset_partitions_page(&storage.for_code_location(&ctx), asset, &partitions)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn page_of_an_asset_with_two_dimensions() {
        let storage = storage_with(
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
        .await;

        let page = page(
            &storage,
            "trips",
            &[
                "hour=00:00|zone=a",
                "hour=00:00|zone=b",
                "hour=00:00|zone=c",
                "hour=01:00|zone=a",
                "hour=01:00|zone=b",
            ],
        )
        .await;

        // `00:00|b` was materialized and failed later: it counts as failed only.
        assert_eq!(
            page,
            AssetPartitionsPage {
                materialized: 3,
                failed: 2,
                states: vec![Materialized, Failed, Failed, Materialized, Missing],
            }
        );
    }

    #[tokio::test]
    async fn totals_count_the_partitions_outside_the_page() {
        let storage = storage_with(
            "plain",
            &[
                built("plain", single("p1"), 100),
                built("plain", single("p2"), 100),
                failure("plain", single("p3"), 200),
            ],
        )
        .await;

        let page = page(&storage, "plain", &["p1"]).await;

        assert_eq!(
            page,
            AssetPartitionsPage {
                materialized: 2,
                failed: 1,
                states: vec![Materialized],
            }
        );
    }

    #[tokio::test]
    async fn key_of_one_dimension_that_reads_like_two() {
        let storage = storage_with(
            "plain",
            &[
                built("plain", single("region=eu"), 100),
                failure("plain", single("region=us"), 200),
            ],
        )
        .await;

        let page = page(&storage, "plain", &["region=eu", "region=us", "region=ap"]).await;

        assert_eq!(page.states, vec![Materialized, Failed, Missing]);
    }
}
