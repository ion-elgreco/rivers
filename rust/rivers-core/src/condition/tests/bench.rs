use super::*;

fn make_wide_graph(
    n: usize,
    edges_per_node: usize,
) -> (HashMap<String, AssetRecord>, HashMap<String, Vec<String>>) {
    let mut records = HashMap::new();
    let mut upstream = HashMap::new();

    for i in 0..n {
        let key = format!("asset_{i}");
        let mut r = if i % 3 == 0 {
            make_record(&key) // some missing
        } else {
            make_materialized_record(&key, (i as i64) * 100)
        };
        if i % 5 == 0 {
            r.code_version = Some("v2".to_string());
            r.last_materialization_code_version = Some("v1".to_string());
        }
        records.insert(key.clone(), r);

        let mut deps = Vec::new();
        for j in 0..edges_per_node {
            let dep_idx = (i + j + 1) % n;
            if dep_idx != i {
                deps.push(format!("asset_{dep_idx}"));
            }
        }
        upstream.insert(key, deps);
    }
    (records, upstream)
}

fn make_linear_chain(n: usize) -> (HashMap<String, AssetRecord>, HashMap<String, Vec<String>>) {
    let mut records = HashMap::new();
    let mut upstream = HashMap::new();

    for i in 0..n {
        let key = format!("asset_{i}");
        let r = make_materialized_record(&key, (i as i64) * 100);
        records.insert(key.clone(), r);
        if i > 0 {
            upstream.insert(key, vec![format!("asset_{}", i - 1)]);
        } else {
            upstream.insert(key, vec![]);
        }
    }
    (records, upstream)
}

fn bench_eval(
    label: &str,
    records: &HashMap<String, AssetRecord>,
    upstream_deps: &HashMap<String, Vec<String>>,
    conditions: &[(String, ConditionNode)],
    iters: usize,
) {
    let in_progress = HashSet::new();
    let failed = HashSet::new();

    // Warm up
    for _ in 0..10 {
        for (key, cond) in conditions {
            let record = &records[key];
            let prev = AssetConditionState::default();
            let ctx = EvalContext {
                target_key: key,
                root_key: key,
                target_record: record,
                cache: CacheSnapshot {
                    records,
                    upstream_deps,
                    in_progress_assets: &in_progress,
                    failed_assets: &failed,
                    failed_asset_timestamps: &EMPTY_FAILED_TS,
                    backfill: &EMPTY_BACKFILL,
                },
                tags: empty_tag_snapshot(),
                prev_state: &prev,
                all_asset_states: &EMPTY_ASSET_STATES,
                requested_this_tick: &EMPTY_REQUESTED,
                now: 999_999_999_999,
                is_initial: false,
                partitions: None,
                root_partition_floor: None,
            };
            std::hint::black_box(evaluate(cond, &ctx));
        }
    }

    let start = std::time::Instant::now();
    for _ in 0..iters {
        for (key, cond) in conditions {
            let record = &records[key];
            let prev = AssetConditionState::default();
            let ctx = EvalContext {
                target_key: key,
                root_key: key,
                target_record: record,
                cache: CacheSnapshot {
                    records,
                    upstream_deps,
                    in_progress_assets: &in_progress,
                    failed_assets: &failed,
                    failed_asset_timestamps: &EMPTY_FAILED_TS,
                    backfill: &EMPTY_BACKFILL,
                },
                tags: empty_tag_snapshot(),
                prev_state: &prev,
                all_asset_states: &EMPTY_ASSET_STATES,
                requested_this_tick: &EMPTY_REQUESTED,
                now: 999_999_999_999,
                is_initial: false,
                partitions: None,
                root_partition_floor: None,
            };
            std::hint::black_box(evaluate(cond, &ctx));
        }
    }
    let elapsed = start.elapsed();
    let total_evals = iters * conditions.len();
    let per_eval = elapsed / total_evals as u32;
    eprintln!(
        "  {label:40} {iters:5} iters x {n:4} assets = {total:6} evals in {elapsed:?}  ({per_eval:?}/eval)",
        n = conditions.len(),
        total = total_evals,
    );
}

#[test]
fn bench_condition_eval_pure() {
    eprintln!("\n== Benchmark A: Pure condition evaluation (no storage) ==\n");

    // Shallow wide graph — Eager condition
    {
        let (records, upstream) = make_wide_graph(100, 2);
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .map(|k| (k.clone(), ConditionNode::eager()))
            .collect();
        bench_eval(
            "shallow_100 (Eager)",
            &records,
            &upstream,
            &conditions,
            1000,
        );
    }

    {
        let (records, upstream) = make_wide_graph(1000, 2);
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .map(|k| (k.clone(), ConditionNode::eager()))
            .collect();
        bench_eval("shallow_1k (Eager)", &records, &upstream, &conditions, 100);
    }

    // Deep linear chain — Eager
    {
        let (records, upstream) = make_linear_chain(100);
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .map(|k| (k.clone(), ConditionNode::eager()))
            .collect();
        bench_eval(
            "deep_chain_100 (Eager)",
            &records,
            &upstream,
            &conditions,
            1000,
        );
    }

    // Complex condition tree
    {
        let (records, upstream) = make_wide_graph(100, 2);
        let complex = ConditionNode::And(vec![ConditionNode::SinceLastHandled(Box::new(
            ConditionNode::And(vec![
                ConditionNode::NewlyTrue(Box::new(ConditionNode::any_deps_updated())),
                ConditionNode::Not(Box::new(ConditionNode::InProgress)),
                ConditionNode::Not(Box::new(ConditionNode::any_deps_missing())),
                ConditionNode::all_deps_match(!ConditionNode::ExecutionFailed),
            ]),
        ))]);
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .map(|k| (k.clone(), complex.clone()))
            .collect();
        bench_eval(
            "complex_condition_100",
            &records,
            &upstream,
            &conditions,
            1000,
        );
    }

    // Nested recursive deps match
    {
        let (records, upstream) = make_wide_graph(100, 2);
        let nested = ConditionNode::all_deps_match(ConditionNode::any_deps_match(
            ConditionNode::NewlyUpdated,
        ));
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .map(|k| (k.clone(), nested.clone()))
            .collect();
        bench_eval(
            "nested_deps_match_100",
            &records,
            &upstream,
            &conditions,
            1000,
        );
    }

    // Mixed workload at 1k scale
    {
        let (records, upstream) = make_wide_graph(1000, 3);
        let conditions: Vec<(String, ConditionNode)> = records
            .keys()
            .enumerate()
            .map(|(i, k)| {
                let cond = match i % 3 {
                    0 => ConditionNode::eager(),
                    1 => ConditionNode::on_missing(),
                    _ => ConditionNode::on_cron("0 * * * *".to_string(), None),
                };
                (k.clone(), cond)
            })
            .collect();
        bench_eval(
            "mixed_1k (Eager/OnMissing/OnCron)",
            &records,
            &upstream,
            &conditions,
            50,
        );
    }

    eprintln!();
}

#[test]
fn bench_selective_vs_full_eval() {
    eprintln!("\n== Benchmark: Selective (time-based + downstream) vs Full eval ==\n");

    for n in [1_000, 10_000, 100_000] {
        let n_cron = std::cmp::max(1, n / 100);
        let n_downstream = n / 10;

        let mut records = HashMap::new();
        let mut conditions: Vec<(String, ConditionNode)> = Vec::new();
        let mut cache =
            AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());

        for i in 0..n {
            let key = format!("asset_{i}");
            let r = make_materialized_record(&key, (i as i64) * 100);
            records.insert(key.clone(), r);

            if i < n_cron {
                conditions.push((key, ConditionNode::on_cron("0 * * * *".to_string(), None)));
            } else if i < n_cron + n_downstream {
                let dep = format!("asset_{}", i % n_cron);
                cache.edges.push((key.clone(), dep));
                conditions.push((key, ConditionNode::eager()));
            } else {
                let dep = format!("asset_{}", n_cron + n_downstream + (i % 10));
                cache.edges.push((key.clone(), dep));
                conditions.push((key, ConditionNode::eager()));
            }
        }

        cache.build_adjacency();
        let eval_set = cache.compute_time_based_eval_set(&conditions);

        let in_progress = HashSet::new();
        let failed = HashSet::new();
        let iters = if n <= 10_000 { 100 } else { 10 };

        let start = std::time::Instant::now();
        for _ in 0..iters {
            for (key, cond) in &conditions {
                let record = &records[key];
                let prev = AssetConditionState::default();
                let ctx = EvalContext {
                    target_key: key,
                    root_key: key,
                    target_record: record,
                    cache: CacheSnapshot {
                        records: &records,
                        upstream_deps: &cache.upstream_deps,
                        in_progress_assets: &in_progress,
                        failed_assets: &failed,
                        failed_asset_timestamps: &EMPTY_FAILED_TS,
                        backfill: &EMPTY_BACKFILL,
                    },
                    tags: empty_tag_snapshot(),
                    prev_state: &prev,
                    all_asset_states: &EMPTY_ASSET_STATES,
                    requested_this_tick: &EMPTY_REQUESTED,
                    now: 999_999_999_999,
                    is_initial: false,
                    partitions: None,
                    root_partition_floor: None,
                };
                std::hint::black_box(evaluate(cond, &ctx));
            }
        }
        let full_elapsed = start.elapsed();
        let full_total = iters * conditions.len();

        let start = std::time::Instant::now();
        for _ in 0..iters {
            for (key, cond) in &conditions {
                if !eval_set.contains(key) {
                    continue;
                }
                let record = &records[key];
                let prev = AssetConditionState::default();
                let ctx = EvalContext {
                    target_key: key,
                    root_key: key,
                    target_record: record,
                    cache: CacheSnapshot {
                        records: &records,
                        upstream_deps: &cache.upstream_deps,
                        in_progress_assets: &in_progress,
                        failed_assets: &failed,
                        failed_asset_timestamps: &EMPTY_FAILED_TS,
                        backfill: &EMPTY_BACKFILL,
                    },
                    tags: empty_tag_snapshot(),
                    prev_state: &prev,
                    all_asset_states: &EMPTY_ASSET_STATES,
                    requested_this_tick: &EMPTY_REQUESTED,
                    now: 999_999_999_999,
                    is_initial: false,
                    partitions: None,
                    root_partition_floor: None,
                };
                std::hint::black_box(evaluate(cond, &ctx));
            }
        }
        let sel_elapsed = start.elapsed();
        let sel_total = iters * eval_set.len();
        let speedup = full_elapsed.as_nanos() as f64 / sel_elapsed.as_nanos() as f64;
        eprintln!(
            "  n={n:>6}  cron={n_cron}  eval_set={sel:>5}/{n}  full={full_elapsed:>10?} ({full_total:>8} evals)  selective={sel_elapsed:>10?} ({sel_total:>8} evals)  speedup={speedup:.1}x",
            sel = eval_set.len(),
        );
    }

    eprintln!();
}

async fn setup_storage_bench<S: StorageBackend>(storage: &S, n_assets: usize) -> Vec<String> {
    use crate::storage::{EventRecord, EventType};

    let mut asset_records = Vec::with_capacity(n_assets);
    let mut asset_keys = Vec::with_capacity(n_assets);
    for i in 0..n_assets {
        let key = format!("bench_{i}");
        asset_keys.push(key.clone());
        asset_records.push(AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key,
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: Some("v1".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        });
    }
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&asset_records)
        .await
        .unwrap();

    let mut edges = Vec::new();
    for i in 1..n_assets {
        edges.push((format!("bench_{i}"), format!("bench_{}", i - 1)));
    }
    let topology = GraphTopology {
        nodes: asset_keys
            .iter()
            .map(|k| crate::assets::graph::TopologyNode {
                name: k.clone(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            })
            .collect(),
        edges,
    };
    let json = serde_json::to_vec(&topology).unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &json,
        )
        .await
        .unwrap();

    // Create a run + materialize half the assets
    let run = RunRecord {
        run_id: "bench_run_1".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(2000),
        tags: vec![],
        node_names: asset_keys.clone(),
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    for (i, key) in asset_keys.iter().enumerate() {
        if i % 2 == 0 {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::Materialization {
                        data_version: Some(format!("dv_{i}")),
                    },
                    asset_key: Some(key.clone()),
                    run_id: "bench_run_1".to_string(),
                    partition_key: None,
                    timestamp: 1500 + i as i64,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
    }

    asset_keys
}

async fn bench_cache_tick<S: StorageBackend>(
    label: &str,
    storage: &S,
    asset_keys: &[String],
    n_changed: usize,
) {
    use crate::storage::{EventRecord, EventType};

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());

    // Initial load
    let start = std::time::Instant::now();
    cache.refresh(storage, 0).await.unwrap();
    let initial_elapsed = start.elapsed();
    eprintln!("  {label:40} initial load:       {initial_elapsed:?}");

    // Warm-up refresh: initial_load parks the cursor 1ns before the newest run
    // (cache.rs::initial_load), so the first delta refresh re-includes it; then ticks settle.
    cache.refresh(storage, 0).await.unwrap();

    // No-change tick
    let start = std::time::Instant::now();
    let iters = 100;
    for _ in 0..iters {
        let changed = cache.refresh(storage, 0).await.unwrap();
        assert!(!changed);
    }
    let no_change = start.elapsed() / iters;
    eprintln!("  {label:40} tick (no change):   {no_change:?}");

    // Create a new run touching n_changed assets
    if n_changed > 0 {
        let touched: Vec<String> = asset_keys.iter().take(n_changed).cloned().collect();
        let run = RunRecord {
            run_id: format!("bench_run_change_{n_changed}"),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Success,
            start_time: 99_000_000_000,
            end_time: Some(99_500_000_000),
            tags: vec![],
            node_names: touched.clone(),
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        storage.create_run(&run).await.unwrap();
        for key in &touched {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::Materialization {
                        data_version: Some("new_dv".to_string()),
                    },
                    asset_key: Some(key.clone()),
                    run_id: format!("bench_run_change_{n_changed}"),
                    partition_key: None,
                    timestamp: 99_100_000_000,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        // Tick with changes
        let start = std::time::Instant::now();
        let changed = cache.refresh(storage, 0).await.unwrap();
        assert!(changed);
        let change_elapsed = start.elapsed();
        eprintln!("  {label:40} tick ({n_changed} changed):  {change_elapsed:?}");

        // Evaluate conditions on all assets (included in total tick time)
        let in_progress = HashSet::new();
        let failed = HashSet::new();
        let conditions: Vec<ConditionNode> =
            asset_keys.iter().map(|_| ConditionNode::eager()).collect();

        let start = std::time::Instant::now();
        for (key, cond) in asset_keys.iter().zip(conditions.iter()) {
            if let Some(record) = cache.records.get(key) {
                let prev = AssetConditionState::default();
                let ctx = EvalContext {
                    target_key: key,
                    root_key: key,
                    target_record: record,
                    cache: CacheSnapshot {
                        records: &cache.records,
                        upstream_deps: &cache.upstream_deps,
                        in_progress_assets: &in_progress,
                        failed_assets: &failed,
                        failed_asset_timestamps: &EMPTY_FAILED_TS,
                        backfill: &EMPTY_BACKFILL,
                    },
                    tags: empty_tag_snapshot(),
                    prev_state: &prev,
                    all_asset_states: &EMPTY_ASSET_STATES,
                    requested_this_tick: &EMPTY_REQUESTED,
                    now: 100_000_000_000,
                    is_initial: false,
                    partitions: None,
                    root_partition_floor: None,
                };
                std::hint::black_box(evaluate(cond, &ctx));
            }
        }
        let eval_elapsed = start.elapsed();
        eprintln!("  {label:40} eval all assets:   {eval_elapsed:?}");
        eprintln!(
            "  {label:40} total tick:        {:?}",
            change_elapsed + eval_elapsed
        );
    }
}

#[tokio::test]
async fn bench_condition_cache_memory() {
    eprintln!("\n== Benchmark B: Condition cache + eval (in-memory storage) ==\n");

    let storage = crate::storage::surrealdb_backend::SurrealStorage::new_memory()
        .await
        .unwrap();

    let keys_100 = setup_storage_bench(&storage, 100).await;
    bench_cache_tick("memory_100", &storage, &keys_100, 0).await;
    bench_cache_tick("memory_100", &storage, &keys_100, 3).await;

    eprintln!();
}

#[tokio::test]
async fn bench_condition_cache_embedded() {
    eprintln!("\n== Benchmark B: Condition cache + eval (embedded RocksDB) ==\n");

    // Unique per-run dir: embedded RocksDB takes an exclusive single-process
    // lock, so a fixed path collides across concurrent `cargo test` runs.
    let tmp = test_temp_dir::test_temp_dir!();
    let storage = crate::storage::surrealdb_backend::SurrealStorage::new_embedded(
        tmp.as_path_untracked().to_str().unwrap(),
    )
    .await
    .unwrap();

    let keys_100 = setup_storage_bench(&storage, 100).await;
    bench_cache_tick("embedded_100", &storage, &keys_100, 0).await;
    bench_cache_tick("embedded_100", &storage, &keys_100, 3).await;

    eprintln!();
}
