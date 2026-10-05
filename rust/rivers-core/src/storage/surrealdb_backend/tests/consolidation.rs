use crate::storage::retry;

use super::*;

/// Race partitioned materializations against whole-asset deletions per
/// iteration on the RocksDB backend (kv-mem misses write-write conflicts)
/// and return each iteration's final asset row + partition set. Retries
/// keep their full budget but near-zero backoff so contended iterations
/// stay fast. `mat{r}` writes partition `p{r}`; the seed writes `pseed`.
async fn race_deletion_against_partitioned_mat(
    iters: usize,
) -> Vec<(AssetRecord, Vec<PartitionKey>)> {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let retry = retry::StorageRetryConfig {
        max_retries: 10,
        initial_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_elapsed: None,
    };
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded_with_retry(
            temp_dir.as_path_untracked().to_str().unwrap(),
            retry,
            Capability::ReadWrite,
        )
        .await
        .expect("failed to create rocksdb storage"),
    );
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let single = |name: &str| PartitionKey::Single {
        keys: vec![name.to_string()],
    };
    let mut finals = Vec::with_capacity(iters);
    for i in 0..iters {
        let key = format!("orders_{i}");
        storage
            .register_assets(cl, &[make_asset_record(&key)])
            .await
            .unwrap();
        let mut seed = make_event(&key, "seed", 1);
        seed.partition_key = Some(single("pseed"));
        storage.store_event(&seed).await.unwrap();

        let mut racers = Vec::new();
        for r in 0..8 {
            let mut mat = make_event(&key, &format!("mat{r}"), 100 + r as i64);
            mat.partition_key = Some(single(&format!("p{r}")));
            racers.push(mat);
            let mut del = make_event(&key, &format!("del{r}"), 200 + r as i64);
            del.event_type = EventType::Deletion;
            racers.push(del);
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(racers.len()));
        let tasks: Vec<_> = racers
            .into_iter()
            .map(|event| {
                let (s, b) = (storage.clone(), barrier.clone());
                tokio::spawn(async move {
                    b.wait().await;
                    // Contention exhausting the retry budget is a loud,
                    // clean failure for the caller — only committed state
                    // is under test here.
                    let _ = s.store_event(&event).await;
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        let record = storage.get_asset_record(cl, &key).await.unwrap().unwrap();
        let parts = storage.get_materialized_partitions(cl, &key).await.unwrap();
        finals.push((record, parts));
    }
    finals
}

/// A whole-asset deletion and a partitioned materialization racing on the
/// same asset must serialize: afterwards the asset row and the partition
/// rows agree, whichever won. A torn state means one side's writes split
/// around the other's — the deletion's clear+delete or the materialize's
/// update+upsert landed as two independently-committed pieces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_and_materialization_consolidate_atomically() {
    for (i, (record, parts)) in race_deletion_against_partitioned_mat(300)
        .await
        .iter()
        .enumerate()
    {
        match record.last_run_id.as_deref() {
            Some(run) => {
                let own = PartitionKey::Single {
                    keys: vec![if run == "seed" {
                        "pseed".to_string()
                    } else {
                        format!("p{}", run.trim_start_matches("mat"))
                    }],
                };
                assert!(
                    parts.contains(&own),
                    "iteration {i}: asset row holds {run} but its partition \
                         row is gone — a deletion split around that materialization \
                         (partitions: {parts:?})"
                );
            }
            None => assert!(
                parts.is_empty(),
                "iteration {i}: asset row is cleared but partition rows \
                     survived — a materialization split around a deletion \
                     (partitions: {parts:?})"
            ),
        }
    }
}

/// What an asset holds after its write-backs: one key's `asset_partitions`
/// row and tombstone, and the `assets` row.
#[derive(Debug, PartialEq)]
struct AssetWrites {
    part_row: Option<(i64, Option<String>)>,
    part_deleted_at: Option<i64>,
    last_run_id: Option<String>,
    last_timestamp: Option<i64>,
    last_data_version: Option<String>,
    deleted_at: Option<i64>,
}

/// `last`: the run and time of the materialization the `assets` row holds.
fn writes(
    part_row: Option<(i64, &str)>,
    part_deleted_at: Option<i64>,
    last: Option<(&str, i64)>,
    deleted_at: Option<i64>,
) -> AssetWrites {
    AssetWrites {
        part_row: part_row.map(|(ts, run)| (ts, Some(run.to_string()))),
        part_deleted_at,
        last_run_id: last.map(|(run, _)| run.to_string()),
        last_timestamp: last.map(|(_, ts)| ts),
        last_data_version: last.map(|(_, ts)| format!("dv_{ts}")),
        deleted_at,
    }
}

fn order_key() -> PartitionKey {
    PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    }
}

fn mat_at(asset_key: &str, run_id: &str, ts: i64, pk: Option<&PartitionKey>) -> EventRecord {
    let mut event = make_event(asset_key, run_id, ts);
    event.event_type = EventType::Materialization {
        data_version: Some(format!("dv_{ts}")),
    };
    event.partition_key = pk.cloned();
    event
}

fn del_at(asset_key: &str, ts: i64, pk: Option<&PartitionKey>) -> EventRecord {
    let mut event = make_event(asset_key, "del", ts);
    event.event_type = EventType::Deletion;
    event.partition_key = pk.cloned();
    event
}

/// Commit `events` one call at a time, in the given order, through the
/// single-event or the batched write path.
async fn commit_one_by_one(
    storage: &SurrealStorage,
    asset_key: &str,
    batched: bool,
    events: &[EventRecord],
) -> AssetWrites {
    register(storage, &[asset_key]).await;
    for event in events {
        if batched {
            storage
                .store_events(std::slice::from_ref(event))
                .await
                .unwrap();
        } else {
            storage.store_event(event).await.unwrap();
        }
    }
    read_writes(storage, asset_key, &order_key()).await
}

async fn read_writes(storage: &SurrealStorage, asset_key: &str, pk: &PartitionKey) -> AssetWrites {
    let cl = DEFAULT_CODE_LOCATION_ID;
    let pk = pk.clone();
    let part_row = storage
        .get_partition_timestamps_for_keys(cl, asset_key, std::slice::from_ref(&pk))
        .await
        .unwrap()
        .into_iter()
        .next()
        .map(|(_, ts, run)| (ts, run));
    let mut result = storage
        .db
        .query(
            "SELECT VALUE timestamp FROM asset_partition_deletions \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND partition_key = $pk",
        )
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .bind(("pk", pk))
        .await
        .unwrap();
    let part_deleted_at: Vec<i64> = result.take(0).unwrap();
    let record = storage
        .get_asset_record(cl, asset_key)
        .await
        .unwrap()
        .unwrap();
    AssetWrites {
        part_row,
        part_deleted_at: part_deleted_at.first().copied(),
        last_run_id: record.last_run_id,
        last_timestamp: record.last_timestamp,
        last_data_version: record.last_data_version,
        deleted_at: storage
            .get_asset_deletion_timestamps(cl)
            .await
            .unwrap()
            .get(asset_key)
            .copied(),
    }
}

/// A keyed delete's members drain over many batches after its slot is
/// released, so a materialize of one member can commit first. The older
/// Deletion must not remove the newer partition row.
#[tokio::test]
async fn a_late_partition_deletion_keeps_the_newer_materialization() {
    let storage = make_storage().await;
    let p = order_key();
    for batched in [false, true] {
        let key = format!("orders_{batched}");
        let events = [
            mat_at(&key, "mat", 200, Some(&p)),
            del_at(&key, 100, Some(&p)),
        ];
        assert_eq!(
            commit_one_by_one(&storage, &key, batched, &events).await,
            writes(Some((200, "mat")), Some(100), Some(("mat", 200)), None),
            "batched={batched}"
        );
    }
}

#[tokio::test]
async fn a_late_materialization_does_not_recreate_a_deleted_partition() {
    let storage = make_storage().await;
    let p = order_key();
    for batched in [false, true] {
        let key = format!("orders_{batched}");
        let events = [
            del_at(&key, 200, Some(&p)),
            mat_at(&key, "old", 100, Some(&p)),
        ];
        assert_eq!(
            commit_one_by_one(&storage, &key, batched, &events).await,
            writes(None, Some(200), Some(("old", 100)), None),
            "batched={batched}"
        );
    }
}

#[tokio::test]
async fn a_late_materialization_keeps_the_newer_partition_row() {
    let storage = make_storage().await;
    let p = order_key();
    for batched in [false, true] {
        let key = format!("orders_{batched}");
        let events = [
            mat_at(&key, "new", 200, Some(&p)),
            mat_at(&key, "old", 100, Some(&p)),
        ];
        assert_eq!(
            commit_one_by_one(&storage, &key, batched, &events).await,
            writes(Some((200, "new")), None, Some(("new", 200)), None),
            "batched={batched}"
        );
    }
}

/// One drain can hold a newer event before an older one. The newest by
/// time decides the rows, and a later stale event must still find them.
#[tokio::test]
async fn a_drain_out_of_time_order_keeps_the_newest_rows() {
    let storage = make_storage().await;
    register(&storage, &["orders"]).await;
    let p1 = order_key();
    let p2 = PartitionKey::Single {
        keys: vec!["2024-01-02".to_string()],
    };
    storage
        .store_events(&[
            mat_at("orders", "new", 200, Some(&p1)),
            mat_at("orders", "old", 100, Some(&p2)),
        ])
        .await
        .unwrap();
    storage
        .store_events(&[mat_at("orders", "late", 150, Some(&p1))])
        .await
        .unwrap();
    assert_eq!(
        read_writes(&storage, "orders", &p1).await,
        writes(Some((200, "new")), None, Some(("new", 200)), None)
    );
    assert_eq!(
        read_writes(&storage, "orders", &p2).await.part_row,
        Some((100, Some("old".to_string())))
    );
}

#[tokio::test]
async fn a_late_asset_deletion_keeps_the_newer_materialization() {
    let storage = make_storage().await;
    for batched in [false, true] {
        for pk in [None, Some(order_key())] {
            let key = format!("orders_{batched}_{}", pk.is_some());
            let events = [
                mat_at(&key, "mat", 200, pk.as_ref()),
                del_at(&key, 100, None),
            ];
            let part_row = pk.as_ref().map(|_| (200, "mat"));
            assert_eq!(
                commit_one_by_one(&storage, &key, batched, &events).await,
                writes(part_row, None, Some(("mat", 200)), Some(100)),
                "batched={batched} partitioned={}",
                pk.is_some()
            );
        }
    }
}

#[tokio::test]
async fn a_late_materialization_does_not_undo_a_newer_asset_deletion() {
    let storage = make_storage().await;
    for batched in [false, true] {
        for pk in [None, Some(order_key())] {
            let key = format!("orders_{batched}_{}", pk.is_some());
            let events = [
                del_at(&key, 200, None),
                mat_at(&key, "old", 100, pk.as_ref()),
            ];
            assert_eq!(
                commit_one_by_one(&storage, &key, batched, &events).await,
                writes(None, None, None, Some(200)),
                "batched={batched} partitioned={}",
                pk.is_some()
            );
        }
    }
}

/// A whole-asset deletion and an older materialization of a new partition,
/// in flight together: neither snapshot holds the other's writes. They must
/// still serialize, so no partition row outlives the newer deletion. The
/// deletion's transaction stays open across the materialization to pin that
/// interleaving; a failed commit is retried as `with_retry` would.
#[tokio::test]
async fn a_concurrent_asset_deletion_leaves_no_older_partition_row() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let p0 = order_key();
    let p3 = PartitionKey::Single {
        keys: vec!["2024-01-04".to_string()],
    };
    for batched in [false, true] {
        let key = format!("orders_{batched}");
        register(&storage, &[&key]).await;
        // The newer partition holds the asset row, so the older one's
        // update of that row changes nothing.
        let newer_event_id = storage
            .store_event(&mat_at(&key, "r3", 120, Some(&p3)))
            .await
            .unwrap();

        let deletion = del_at(&key, 200, None);
        let tx = storage.db.clone().begin().await.unwrap();
        tx.query(CLEAR_DELETED_ASSET)
            .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
            .bind(("asset_key", key.clone()))
            .bind(("event_id", "deletion".to_string()))
            .bind(("ts", deletion.timestamp))
            .await
            .unwrap()
            .check()
            .unwrap();

        let older = mat_at(&key, "r0", 100, Some(&p0));
        if batched {
            storage
                .store_events(std::slice::from_ref(&older))
                .await
                .unwrap();
        } else {
            storage.store_event(&older).await.unwrap();
        }
        let record = storage
            .get_asset_record(DEFAULT_CODE_LOCATION_ID, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                record.last_event_id,
                record.last_run_id,
                record.last_timestamp
            ),
            (Some(newer_event_id), Some("r3".to_string()), Some(120)),
            "batched={batched}: the older materialization changed the asset row"
        );

        if let Err(e) = tx.commit().await {
            assert!(
                retry::is_transient_surrealdb_error(&e),
                "batched={batched}: {e}"
            );
            storage.store_event(&deletion).await.unwrap();
        }

        assert_eq!(
            read_writes(&storage, &key, &p0).await,
            writes(None, None, None, Some(200)),
            "batched={batched}"
        );
        assert_eq!(
            storage
                .get_materialized_partitions(DEFAULT_CODE_LOCATION_ID, &key)
                .await
                .unwrap(),
            Vec::<PartitionKey>::new(),
            "batched={batched}: a partition row outlived the newer whole-asset deletion"
        );
    }
}

#[tokio::test]
async fn in_order_deletes_and_materializations_apply_as_before() {
    let storage = make_storage().await;
    let p = order_key();
    type Events = fn(&str, &PartitionKey) -> Vec<EventRecord>;
    let cases: [(&str, Events, AssetWrites); 7] = [
        (
            "mat_then_part_del",
            |k, p| vec![mat_at(k, "mat", 100, Some(p)), del_at(k, 200, Some(p))],
            writes(None, Some(200), Some(("mat", 100)), None),
        ),
        (
            "part_del_then_mat",
            |k, p| vec![del_at(k, 100, Some(p)), mat_at(k, "mat", 200, Some(p))],
            writes(Some((200, "mat")), Some(100), Some(("mat", 200)), None),
        ),
        (
            "mat_then_mat",
            |k, p| {
                vec![
                    mat_at(k, "old", 100, Some(p)),
                    mat_at(k, "new", 200, Some(p)),
                ]
            },
            writes(Some((200, "new")), None, Some(("new", 200)), None),
        ),
        (
            "mat_then_asset_del",
            |k, p| vec![mat_at(k, "mat", 100, Some(p)), del_at(k, 200, None)],
            writes(None, None, None, Some(200)),
        ),
        (
            "asset_del_then_mat",
            |k, p| vec![del_at(k, 100, None), mat_at(k, "mat", 200, Some(p))],
            writes(Some((200, "mat")), None, Some(("mat", 200)), Some(100)),
        ),
        (
            "unpartitioned_mat_then_asset_del",
            |k, _| vec![mat_at(k, "mat", 100, None), del_at(k, 200, None)],
            writes(None, None, None, Some(200)),
        ),
        (
            "asset_del_then_unpartitioned_mat",
            |k, _| vec![del_at(k, 100, None), mat_at(k, "mat", 200, None)],
            writes(None, None, Some(("mat", 200)), Some(100)),
        ),
    ];
    for batched in [false, true] {
        for (case, events, expected) in &cases {
            let key = format!("{case}_{batched}");
            assert_eq!(
                &commit_one_by_one(&storage, &key, batched, &events(&key, &p)).await,
                expected,
                "{case} batched={batched}"
            );
        }
    }
}

/// `mat_at` for a materialization that read upstream `u` at `u_version`.
fn mat_reading(
    asset_key: &str,
    run_id: &str,
    ts: i64,
    u_version: Option<&str>,
    pk: Option<&PartitionKey>,
) -> EventRecord {
    let mut event = mat_at(asset_key, run_id, ts, pk);
    event.input_data_versions = u_version
        .map(|v| vec![("u".to_string(), v.to_string())])
        .unwrap_or_default();
    event
}

/// One write-back step of an asset: a deploy that registers its code
/// version, or one drain of events.
enum Step {
    Deploy(&'static str),
    Drain(Vec<EventRecord>),
}

/// What an `assets` row holds: the materialization its data comes from,
/// and its provenance — the code version and inputs the data was built
/// from, and the time of the materialization that recorded them.
#[derive(Debug, PartialEq)]
struct RowProvenance {
    last_run_id: Option<String>,
    last_timestamp: Option<i64>,
    last_data_version: Option<String>,
    code_version: Option<String>,
    inputs: Vec<(String, String)>,
    provenance_at: Option<i64>,
}

/// `data`: the run and time of the materialization the data comes from.
/// `built`: the code version, the version of `u` read, and the time of
/// the materialization the provenance comes from.
fn row(data: Option<(&str, i64)>, built: Option<(&str, Option<&str>, i64)>) -> RowProvenance {
    RowProvenance {
        last_run_id: data.map(|(run, _)| run.to_string()),
        last_timestamp: data.map(|(_, ts)| ts),
        last_data_version: data.map(|(_, ts)| format!("dv_{ts}")),
        code_version: built.map(|(cv, ..)| cv.to_string()),
        inputs: built
            .and_then(|(_, u, _)| u)
            .map(|u| vec![("u".to_string(), u.to_string())])
            .unwrap_or_default(),
        provenance_at: built.map(|(.., ts)| ts),
    }
}

/// Apply `steps` in order. `batched`: each drain is one `store_events`
/// call; otherwise its events are stored one `store_event` call at a time.
async fn apply_steps(
    storage: &SurrealStorage,
    asset_key: &str,
    batched: bool,
    steps: Vec<Step>,
) -> RowProvenance {
    let cl = DEFAULT_CODE_LOCATION_ID;
    for step in steps {
        match step {
            Step::Deploy(code_version) => storage
                .register_assets(
                    cl,
                    &[AssetRecord {
                        code_version: Some(code_version.to_string()),
                        ..make_asset_record(asset_key)
                    }],
                )
                .await
                .unwrap(),
            Step::Drain(events) if batched => {
                storage.store_events(&events).await.unwrap();
            }
            Step::Drain(events) => {
                for event in &events {
                    storage.store_event(event).await.unwrap();
                }
            }
        }
    }
    let record = storage
        .get_asset_record(cl, asset_key)
        .await
        .unwrap()
        .unwrap();
    let mut result = storage
        .db
        .query(
            "SELECT VALUE last_provenance_timestamp FROM assets \
                 WHERE code_location_id = $cl AND asset_key = $asset_key",
        )
        .bind(("cl", cl.to_string()))
        .bind(("asset_key", asset_key.to_string()))
        .await
        .unwrap();
    let provenance_at: Vec<Option<i64>> = result.take(0).unwrap();
    RowProvenance {
        last_run_id: record.last_run_id,
        last_timestamp: record.last_timestamp,
        last_data_version: record.last_data_version,
        code_version: record.last_materialization_code_version,
        inputs: record.last_input_data_versions,
        provenance_at: provenance_at.into_iter().flatten().next(),
    }
}

/// An action's `materialized()` moves an asset's data but writes no
/// provenance. A real materialization that lands after a newer action must
/// still record the code version and inputs it was built from, unless a
/// newer real materialization or whole-asset deletion holds them. Each
/// case must end as its events would applied in time order, however they
/// drain. `a*` runs are action runs.
#[tokio::test]
async fn provenance_applies_in_event_order_apart_from_the_data() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    for run_id in ["a2", "a4"] {
        let run = RunRecord {
            action: Some("merge".to_string()),
            ..minimal_run(run_id, RunStatus::Success)
        };
        storage.create_run(&run).await.unwrap();
    }
    use Step::{Deploy, Drain};
    type Steps = fn(&str, Option<&PartitionKey>) -> Vec<Step>;
    let cases: [(&str, Steps, RowProvenance); 9] = [
        (
            "real_lands_after_newer_action",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "a2", 200, None, p)]),
                    Drain(vec![mat_reading(k, "r1", 100, Some("u2"), p)]),
                ]
            },
            row(Some(("a2", 200)), Some(("v2", Some("u2"), 100))),
        ),
        (
            "real_without_inputs_lands_after_newer_action",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, None, p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "a2", 200, None, p)]),
                    Drain(vec![mat_reading(k, "r1", 100, None, p)]),
                ]
            },
            row(Some(("a2", 200)), Some(("v2", None, 100))),
        ),
        (
            "real_lands_after_newer_real",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "r3", 300, Some("u3"), p)]),
                    Deploy("v3"),
                    Drain(vec![mat_reading(k, "r1", 100, Some("u2"), p)]),
                ]
            },
            row(Some(("r3", 300)), Some(("v2", Some("u3"), 300))),
        ),
        (
            "action_and_older_real_drain_together",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![
                        mat_reading(k, "a2", 200, None, p),
                        mat_reading(k, "r1", 100, Some("u2"), p),
                    ]),
                ]
            },
            row(Some(("a2", 200)), Some(("v2", Some("u2"), 100))),
        ),
        (
            "drain_older_than_the_rows_data",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "a2", 200, None, p)]),
                    Drain(vec![
                        mat_reading(k, "r1", 100, Some("u2"), p),
                        mat_reading(k, "a4", 150, None, p),
                    ]),
                ]
            },
            row(Some(("a2", 200)), Some(("v2", Some("u2"), 100))),
        ),
        (
            "drain_inputs_older_than_the_rows_provenance",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "r3", 300, Some("u3"), p)]),
                    Deploy("v3"),
                    Drain(vec![
                        mat_reading(k, "r1", 100, Some("u2"), p),
                        mat_reading(k, "a4", 400, None, p),
                    ]),
                ]
            },
            row(Some(("a4", 400)), Some(("v2", Some("u3"), 300))),
        ),
        (
            "late_deletion_clears_older_provenance",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "a2", 300, None, p)]),
                    Drain(vec![del_at(k, 200, None)]),
                    Drain(vec![mat_reading(k, "r1", 100, Some("u2"), p)]),
                ]
            },
            row(Some(("a2", 300)), None),
        ),
        (
            "late_deletion_keeps_newer_provenance",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r3", 300, Some("u3"), p)]),
                    Drain(vec![del_at(k, 200, None)]),
                ]
            },
            row(Some(("r3", 300)), Some(("v1", Some("u3"), 300))),
        ),
        (
            "older_real_after_deletion_stays_deleted",
            |k, p| {
                vec![
                    Deploy("v1"),
                    Drain(vec![mat_reading(k, "r0", 50, Some("u1"), p)]),
                    Drain(vec![del_at(k, 200, None)]),
                    Deploy("v2"),
                    Drain(vec![mat_reading(k, "r1", 100, Some("u2"), p)]),
                ]
            },
            row(None, None),
        ),
    ];
    let mut wrong = Vec::new();
    for batched in [false, true] {
        for pk in [None, Some(order_key())] {
            for (case, steps, expected) in &cases {
                let key = format!("{case}_{batched}_{}", pk.is_some());
                let got = apply_steps(&storage, &key, batched, steps(&key, pk.as_ref())).await;
                if &got != expected {
                    wrong.push(format!(
                            "{case} batched={batched} partitioned={}:\n  got  {got:?}\n  want {expected:?}",
                            pk.is_some()
                        ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Concurrent unpartitioned materializations of one asset conflict on the
/// `assets` row; the retry re-runs the whole closure, so the event insert
/// must be idempotent — every stored materialization appears exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_retry_does_not_duplicate_events() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let retry = retry::StorageRetryConfig {
        max_retries: 10,
        initial_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_elapsed: None,
    };
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded_with_retry(
            temp_dir.as_path_untracked().to_str().unwrap(),
            retry,
            Capability::ReadWrite,
        )
        .await
        .expect("failed to create rocksdb storage"),
    );
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    for i in 0..100 {
        let key = format!("orders_{i}");
        storage
            .register_assets(cl, &[make_asset_record(&key)])
            .await
            .unwrap();
        let racers: Vec<EventRecord> = (0..8)
            .map(|r| make_event(&key, &format!("mat{r}"), 100 + r as i64))
            .collect();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(racers.len()));
        let tasks: Vec<_> = racers
            .into_iter()
            .map(|event| {
                let (s, b) = (storage.clone(), barrier.clone());
                tokio::spawn(async move {
                    b.wait().await;
                    s.store_event(&event).await.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        let events = storage.get_events_for_asset(cl, &key, 1000).await.unwrap();
        assert_eq!(
            events.len(),
            8,
            "iteration {i}: 8 materializations stored but {} event rows — \
                 a conflict retry re-inserted an already-committed event",
            events.len()
        );
    }
}

/// Concurrent observations race the same `assets` row. Whatever commits
/// last, the row must hold one observation's consistent (timestamp,
/// data_version) pair, and — once observation updates surface conflicts
/// and retry — the retried closure must not duplicate event rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_observations_stay_consistent() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let retry = retry::StorageRetryConfig {
        max_retries: 10,
        initial_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_elapsed: None,
    };
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded_with_retry(
            temp_dir.as_path_untracked().to_str().unwrap(),
            retry,
            Capability::ReadWrite,
        )
        .await
        .expect("failed to create rocksdb storage"),
    );
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    for i in 0..100 {
        let key = format!("sensor_{i}");
        storage
            .register_assets(cl, &[make_asset_record(&key)])
            .await
            .unwrap();
        let racers: Vec<EventRecord> = (0..8)
            .map(|r| {
                let mut e = make_event(&key, &format!("obs{r}"), 100 + r as i64);
                e.event_type = EventType::Observation {
                    data_version: Some(format!("dv{r}")),
                };
                e
            })
            .collect();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(racers.len()));
        let tasks: Vec<_> = racers
            .into_iter()
            .map(|event| {
                let (s, b) = (storage.clone(), barrier.clone());
                tokio::spawn(async move {
                    b.wait().await;
                    s.store_event(&event).await.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        let events = storage.get_events_for_asset(cl, &key, 1000).await.unwrap();
        assert_eq!(
            events.len(),
            8,
            "iteration {i}: 8 observations stored but {} event rows",
            events.len()
        );
        let record = storage.get_asset_record(cl, &key).await.unwrap().unwrap();
        let ts = record.last_timestamp.expect("an observation committed");
        let r = ts - 100;
        assert!((0..8).contains(&r), "iteration {i}: foreign timestamp {ts}");
        assert_eq!(
            record.last_data_version.as_deref(),
            Some(format!("dv{r}").as_str()),
            "iteration {i}: assets row mixes two observations \
                 (ts {ts} with {:?})",
            record.last_data_version
        );
    }
}
