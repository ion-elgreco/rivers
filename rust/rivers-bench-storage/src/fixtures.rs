//! Record builders and the seeded dataset every scenario reads from.

use anyhow::Result;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::{
    AssetRecord, BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
    ConditionEvalRecord, ConditionTickRecord, EventRecord, EventType, LaunchedBy, LogRecord,
    PartitionKey, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend, TickRecord,
};

pub type Handle = ScopedStorageHandle<AnyStorage>;

/// Assets the seeded events and runs refer to. Read scenarios index into this
/// range, so it has to stay smaller than the seeded asset count.
pub const ASSETS: usize = 50;
/// Runs the seeded events are spread across.
pub const RUNS: usize = 500;
pub const POOL: &str = "bench_pool";
pub const PARTITIONS_DEF: &str = "daily";
pub const AUTOMATION: &str = "nightly";
/// Seeded ids the read scenarios target, so every read hits real rows.
pub const SEED_RUN: &str = "seed_run_0";
pub const SEED_ASSET: &str = "asset_1";
pub const SEED_BACKFILL: &str = "seed_backfill_0";
pub const SEED_TICK_ID: &str = "seed_tick";

pub struct Config {
    pub warmup: usize,
    pub iters: usize,
    /// Events per `store_events` call. Matches the executor's drain batch.
    pub event_batch: usize,
    /// Rows seeded before the read scenarios, so reads are not measured
    /// against an empty table where every backend looks identical.
    pub seed_rows: usize,
}

/// Wall-clock nanoseconds. `rivers_core`'s own helper is crate-private, and
/// the exact epoch does not matter here — only that timestamps advance.
pub fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

pub fn partition(i: usize) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![format!("2024-01-{:02}", (i % 28) + 1)],
    }
}

pub fn event(cl: &str, run_id: &str, asset: &str, i: usize) -> EventRecord {
    EventRecord {
        code_location_id: cl.to_string(),
        event_type: EventType::Materialization {
            data_version: Some(format!("v{i}")),
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(partition(i)),
        timestamp: now_nanos(),
        metadata: vec![("rows".to_string(), i.to_string())],
        input_data_versions: vec![],
    }
}

pub fn typed_event(cl: &str, run_id: &str, asset: &str, ty: EventType, i: usize) -> EventRecord {
    EventRecord {
        code_location_id: cl.to_string(),
        event_type: ty,
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(partition(i)),
        timestamp: now_nanos(),
        metadata: vec![],
        input_data_versions: vec![],
    }
}

pub fn run(cl: &str, run_id: &str, i: usize) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: cl.to_string(),
        job_name: Some(format!("job_{}", i % 20)),
        status: if i.is_multiple_of(3) {
            RunStatus::Success
        } else {
            RunStatus::Queued
        },
        start_time: now_nanos(),
        end_time: None,
        tags: vec![("team".to_string(), format!("t{}", i % 5))],
        node_names: vec![format!("asset_{}", i % ASSETS)],
        priority: (i % 7) as i32,
        partition_key: Some(partition(i)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
    }
}

pub fn asset(cl: &str, i: usize) -> AssetRecord {
    AssetRecord {
        code_location_id: cl.to_string(),
        asset_key: format!("asset_{i}"),
        tags: vec![format!("team:t{}", i % 5)],
        kinds: vec!["python".to_string()],
        asset_group: Some(format!("group_{}", i % 10)),
        code_version: Some("v1".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }
}

pub fn log(cl: &str, run_id: &str, step: &str) -> LogRecord {
    LogRecord {
        code_location_id: cl.to_string(),
        run_id: run_id.to_string(),
        step_key: step.to_string(),
        timestamp: now_nanos(),
        stdout: Some("hello from the step\n".repeat(10)),
        stderr: None,
        logs: None,
    }
}

pub fn tick(cl: &str, i: usize) -> TickRecord {
    TickRecord {
        code_location_id: cl.to_string(),
        automation_name: AUTOMATION.to_string(),
        automation_type: "Schedule".to_string(),
        status: "Success".to_string(),
        timestamp: now_nanos() + i as i64,
        run_ids: vec![format!("seed_run_{}", i % RUNS)],
        backfill_ids: vec![],
        skip_reason: None,
        error: None,
        cursor: Some(format!("c{i}")),
    }
}

pub fn condition_tick(cl: &str, i: usize) -> ConditionTickRecord {
    ConditionTickRecord {
        code_location_id: cl.to_string(),
        timestamp: now_nanos() + i as i64,
        total_evaluated: 50,
        total_fired: 3,
        eval_duration_us: 1_200,
        run_ids: vec![],
        backfill_ids: vec![],
    }
}

pub fn condition_eval(cl: &str, tick_id: &str, i: usize) -> ConditionEvalRecord {
    ConditionEvalRecord {
        code_location_id: cl.to_string(),
        asset_key: format!("asset_{}", i % ASSETS),
        tick_id: tick_id.to_string(),
        timestamp: now_nanos() + i as i64,
        fired: i.is_multiple_of(4),
        eval_duration_us: 90,
        run_ids: vec![],
        tree_json: br#"{"kind":"and","children":[]}"#.to_vec(),
        selection_json: None,
    }
}

pub fn backfill(cl: &str, id: &str, status: BackfillStatus) -> BackfillRecord {
    BackfillRecord {
        backfill_id: id.to_string(),
        code_location_id: cl.to_string(),
        status,
        strategy: BackfillStrategy::MultiRun,
        failure_policy: BackfillFailurePolicy::Continue,
        asset_selection: vec![SEED_ASSET.to_string()],
        job_name: Some("job_0".to_string()),
        partition_keys: (0..20).map(partition).collect(),
        run_ids: vec![],
        completed_partitions: vec![],
        failed_partitions: vec![],
        canceled_partitions: vec![],
        max_concurrency: 4,
        tags: vec![],
        create_time: now_nanos(),
        end_time: None,
        error: None,
        launched_by: LaunchedBy::Manual { user: None },
    }
}

/// Fill the store so the read scenarios have something to read.
///
/// Seeding is not timed. It runs once per backend before any measurement, so
/// a slow writer does not also pay for it inside a read result.
pub async fn seed(handle: &Handle, cfg: &Config) -> Result<()> {
    let cl = handle.code_location_id().to_string();
    let backend = handle.backend();

    let assets: Vec<AssetRecord> = (0..cfg.seed_rows.min(2_000))
        .map(|i| asset(&cl, i))
        .collect();
    handle.scoped().register_assets(&assets).await?;

    let runs: Vec<RunRecord> = (0..cfg.seed_rows)
        .map(|i| run(&cl, &format!("seed_run_{i}"), i))
        .collect();
    for chunk in runs.chunks(500) {
        backend.create_runs(chunk).await?;
    }

    // Materializations, plus the step events the run-detail queries read.
    let mut events: Vec<EventRecord> = (0..cfg.seed_rows)
        .map(|i| {
            event(
                &cl,
                &format!("seed_run_{}", i % RUNS),
                &format!("asset_{}", i % ASSETS),
                i,
            )
        })
        .collect();
    for i in 0..RUNS {
        let run_id = format!("seed_run_{i}");
        let asset_key = format!("asset_{}", i % ASSETS);
        for ty in [
            EventType::StepStart,
            EventType::StepSuccess,
            EventType::Observation {
                data_version: Some(format!("o{i}")),
            },
        ] {
            events.push(typed_event(&cl, &run_id, &asset_key, ty, i));
        }
    }
    for chunk in events.chunks(500) {
        backend.store_events(chunk).await?;
    }

    let logs: Vec<LogRecord> = (0..RUNS)
        .map(|i| {
            log(
                &cl,
                &format!("seed_run_{i}"),
                &format!("asset_{}", i % ASSETS),
            )
        })
        .collect();
    for chunk in logs.chunks(200) {
        backend.store_run_logs(chunk).await?;
    }

    let ticks: Vec<TickRecord> = (0..500).map(|i| tick(&cl, i)).collect();
    backend.store_ticks_batch(&ticks).await?;
    for i in 0..200 {
        backend
            .store_condition_tick(&condition_tick(&cl, i))
            .await?;
    }
    let evals: Vec<ConditionEvalRecord> = (0..500)
        .map(|i| condition_eval(&cl, SEED_TICK_ID, i))
        .collect();
    backend.store_condition_evals_batch(&evals).await?;

    for i in 0..50 {
        backend
            .create_backfill(&backfill(
                &cl,
                &format!("seed_backfill_{i}"),
                BackfillStatus::InProgress,
            ))
            .await?;
    }

    handle.scoped().set_pool_limit(POOL, 1_000_000, 300).await?;
    let keys: Vec<String> = (0..2_000).map(|i| format!("seed_p_{i}")).collect();
    handle
        .scoped()
        .add_dynamic_partitions(PARTITIONS_DEF, &keys)
        .await?;

    backend.kv_set("bench:key", b"bench-value").await?;
    Ok(())
}
