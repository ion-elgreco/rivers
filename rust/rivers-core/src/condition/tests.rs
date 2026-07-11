//! Tests for the condition evaluation engine.

#![allow(clippy::type_complexity)] // test scaffolding LazyLocks mirror cache types verbatim

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;
use crate::assets::graph::GraphTopology;
use crate::storage::{
    AssetRecord, DEFAULT_CODE_LOCATION_ID, LaunchedBy, PartitionKey, RunRecord, RunStatus,
    StorageBackend,
};

fn make_record(key: &str) -> AssetRecord {
    AssetRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: key.to_string(),
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
    }
}

fn make_materialized_record(key: &str, ts: i64) -> AssetRecord {
    let mut r = make_record(key);
    r.last_timestamp = Some(ts);
    r.last_data_version = Some(format!("dv_{key}"));
    r.last_run_id = Some(format!("run_{key}"));
    r
}

static EMPTY_SET: std::sync::LazyLock<HashSet<String>> = std::sync::LazyLock::new(HashSet::new);
static EMPTY_BACKFILL: std::sync::LazyLock<crate::condition::cache::BackfillState> =
    std::sync::LazyLock::new(crate::condition::cache::BackfillState::default);
static DEFAULT_STATE: std::sync::LazyLock<AssetConditionState> =
    std::sync::LazyLock::new(AssetConditionState::default);
static EMPTY_ASSET_STATES: std::sync::LazyLock<HashMap<String, AssetConditionState>> =
    std::sync::LazyLock::new(HashMap::new);
static EMPTY_RUN_TAGS: std::sync::LazyLock<HashMap<String, Arc<[(String, String)]>>> =
    std::sync::LazyLock::new(HashMap::new);
static EMPTY_PARTITION_RUN_TAGS: std::sync::LazyLock<
    HashMap<String, HashMap<PartitionKey, Arc<[(String, String)]>>>,
> = std::sync::LazyLock::new(HashMap::new);
static EMPTY_TICK_MAT_TAGS: std::sync::LazyLock<HashMap<String, Vec<Arc<[(String, String)]>>>> =
    std::sync::LazyLock::new(HashMap::new);
static EMPTY_TICK_PART_MAT_TAGS: std::sync::LazyLock<
    HashMap<String, HashMap<PartitionKey, Vec<Arc<[(String, String)]>>>>,
> = std::sync::LazyLock::new(HashMap::new);
static EMPTY_RUN_ASSET_NAMES: std::sync::LazyLock<HashMap<String, Arc<[String]>>> =
    std::sync::LazyLock::new(HashMap::new);
static EMPTY_PARTITION_RUN_ASSET_NAMES: std::sync::LazyLock<
    HashMap<String, HashMap<PartitionKey, Arc<[String]>>>,
> = std::sync::LazyLock::new(HashMap::new);

fn empty_tag_snapshot() -> RunTagSnapshot<'static> {
    RunTagSnapshot {
        last_run_tags: &EMPTY_RUN_TAGS,
        partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
        tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
        tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
        last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
        partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
    }
}

fn spk(s: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![s.to_string()],
    }
}

fn mpk(dims: &[(&str, &str)]) -> PartitionKey {
    PartitionKey::Multi {
        dims: dims
            .iter()
            .map(|(d, v)| (d.to_string(), vec![v.to_string()]))
            .collect(),
    }
}

static EMPTY_REQUESTED: std::sync::LazyLock<HashMap<String, PartitionSelection>> =
    std::sync::LazyLock::new(HashMap::new);

static EMPTY_FAILED_TS: std::sync::LazyLock<HashMap<String, i64>> =
    std::sync::LazyLock::new(HashMap::new);

fn make_ctx<'a>(
    target_key: &'a str,
    target_record: &'a AssetRecord,
    records: &'a HashMap<String, AssetRecord>,
    upstream_deps: &'a HashMap<String, Vec<String>>,
) -> EvalContext<'a> {
    EvalContext {
        target_key,
        root_key: target_key,
        target_record,
        cache: CacheSnapshot {
            records,
            upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000, // 1000s in nanos
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    }
}

#[test]
fn test_missing_true() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(result.fired);
}

#[test]
fn test_missing_false_when_materialized() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(!result.fired);
}

#[test]
fn test_missing_false_when_materialized_without_data_version() {
    // Missing keys off materialization presence (last_run_id), not last_data_version:
    // a materialized asset carrying no data version is still not Missing.
    let mut record = make_record("a");
    record.last_timestamp = Some(100);
    record.last_run_id = Some("run_a".to_string());
    record.last_data_version = None;
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::Missing, &ctx).fired);
}

#[test]
fn test_in_progress() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let in_progress = HashSet::from(["a".to_string()]);
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &in_progress,
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &AssetConditionState::default(),
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::InProgress, &ctx).fired);
}

#[test]
fn test_execution_failed() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let failed = HashSet::from(["a".to_string()]);
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &failed,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &AssetConditionState::default(),
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::ExecutionFailed, &ctx).fired);
}

#[test]
fn test_code_version_changed() {
    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v2".to_string());
    record.last_materialization_code_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(evaluate(&ConditionNode::CodeVersionChanged, &ctx).fired);
}

#[test]
fn test_code_version_same() {
    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v1".to_string());
    record.last_materialization_code_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::CodeVersionChanged, &ctx).fired);
}

#[test]
fn test_newly_requested_after_firing() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::NewlyRequested, &ctx).fired);
}

#[test]
fn test_newly_requested_not_fired_last_tick() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState {
        last_handled_timestamp: Some(500),
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::NewlyRequested, &ctx).fired);
}

#[test]
fn test_newly_requested_never_handled() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState {
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::NewlyRequested, &ctx).fired);
}

#[test]
fn test_newly_updated() {
    let record = make_materialized_record("a", 200);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::NewlyUpdated, &ctx).fired);
}

#[test]
fn test_newly_updated_no_change() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::NewlyUpdated, &ctx).fired);
}

#[test]
fn test_newly_updated_self_suppressed_on_initial_tick() {
    // A bare newly_updated() self-condition must not fire for an
    // already-materialized asset on the initial tick (empty prev_state).
    let record = make_materialized_record("a", 200);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    // Empty prev state: the daemon just (re)started — no last-tick baseline.
    let prev = AssetConditionState::default();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: true,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&ConditionNode::NewlyUpdated, &ctx).fired,
        "already-materialized asset must not re-fire newly_updated() on the initial tick"
    );
}

#[test]
fn test_newly_updated_self_fires_when_appears_between_ticks() {
    // On a non-initial tick a missing baseline means the asset appeared between ticks → fire.
    let record = make_materialized_record("a", 200);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev = AssetConditionState::default();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        evaluate(&ConditionNode::NewlyUpdated, &ctx).fired,
        "an asset appearing between non-initial ticks must fire newly_updated()"
    );
}

#[test]
fn test_initial_evaluation_true_on_first_tick() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    assert!(evaluate(&ConditionNode::InitialEvaluation, &ctx).fired);
}

#[test]
fn test_initial_evaluation_false_on_subsequent_tick() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps); // is_initial=false by default
    assert!(!evaluate(&ConditionNode::InitialEvaluation, &ctx).fired);
}

#[test]
fn test_initial_evaluation_composable_with_or() {
    // InitialEvaluation | Missing.newly_true() — fires on first tick even if Missing is false
    let record = make_materialized_record("a", 100); // NOT missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let cond = ConditionNode::Or(vec![
        ConditionNode::InitialEvaluation,
        ConditionNode::Missing.newly_true(),
    ]);
    assert!(evaluate(&cond, &ctx).fired);

    // On subsequent tick, InitialEvaluation=false and Missing=false → false
    let mut ctx2 = make_ctx("a", &record, &records, &deps);
    ctx2.is_initial = false;
    assert!(!evaluate(&cond, &ctx2).fired);
}

#[test]
fn test_initial_evaluation_composable_with_and() {
    // InitialEvaluation & Missing — only fires on first tick if also missing
    let record = make_materialized_record("a", 100); // NOT missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let cond = ConditionNode::And(vec![
        ConditionNode::InitialEvaluation,
        ConditionNode::Missing,
    ]);
    // is_initial=true but asset is not missing → false
    assert!(!evaluate(&cond, &ctx).fired);

    // With a missing asset on initial tick → true
    let missing_record = make_record("b");
    let records2 = HashMap::from([("b".to_string(), missing_record.clone())]);
    let mut ctx3 = make_ctx("b", &missing_record, &records2, &deps);
    ctx3.is_initial = true;
    assert!(evaluate(&cond, &ctx3).fired);
}

#[test]
fn test_initial_evaluation_since_last_handled() {
    // InitialEvaluation.since_last_handled() — fires once on first tick, debounced after
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // First tick: is_initial=true, never handled
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let cond = ConditionNode::InitialEvaluation.since_last_handled();
    assert!(evaluate(&cond, &ctx).fired);

    // Second tick: is_initial=false → InitialEvaluation=false → since_last_handled(false)=false
    let ctx2 = make_ctx("a", &record, &records, &deps); // is_initial=false
    assert!(!evaluate(&cond, &ctx2).fired);
}

#[test]
fn test_newly_true_pure_no_initial_hack() {
    // NewlyTrue is a pure rising-edge detector with no is_initial special-casing;
    // fires when child is true and previous defaults to false.
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // is_initial=true, child=true, previous=false (no prev results) → fires
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));
    assert!(evaluate(&cond, &ctx).fired);

    // is_initial=false, child=true, previous=false → also fires (NewlyTrue ignores is_initial)
    let ctx2 = make_ctx("a", &record, &records, &deps);
    assert!(evaluate(&cond, &ctx2).fired);
}

#[test]
fn test_newly_true_does_not_refire_with_previous_true() {
    // After child was true last tick, NewlyTrue must not fire regardless of is_initial.
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let mut prev = AssetConditionState::default();
    // NewlyTrue (index 0) stores the child's raw value at its own index.
    prev.previous_results.insert(0, true);

    // is_initial=true but previous=true → should NOT fire (pure rising-edge)
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: true,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));
    assert!(
        !evaluate(&cond, &ctx).fired,
        "pure NewlyTrue should not refire when previous=true, even on is_initial"
    );
}

#[test]
fn test_any_deps_updated_initial_heuristic_dep_newer() {
    // On initial tick with no dep baseline, AnyDepsUpdated fires only if dep_ts > target_ts;
    // dep a(200) newer than target b(100) → fires.
    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);

    let mut ctx = make_ctx("b", &b, &records, &deps);
    ctx.is_initial = true;
    assert!(
        evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "AnyDepsUpdated should fire on initial tick when dep is newer than target"
    );
}

#[test]
fn test_any_deps_updated_initial_heuristic_dep_same_age() {
    // On initial tick, dep "a" at ts=100 same age as target "b" at ts=100 → no fire.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);

    let mut ctx = make_ctx("b", &b, &records, &deps);
    ctx.is_initial = true;
    assert!(
        !evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "AnyDepsUpdated should NOT fire on initial tick when dep is same age as target"
    );
}

#[test]
fn test_any_deps_updated_fires_on_non_initial_without_prev_timestamps() {
    // On non-initial tick with no dep baseline (e.g. new dep), AnyDepsUpdated fires — dep has unseen data.
    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);

    let ctx = make_ctx("b", &b, &records, &deps); // is_initial=false
    assert!(
        evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "AnyDepsUpdated should fire on non-initial tick when dep has data but no baseline"
    );
}

#[test]
fn test_initial_evaluation_with_tree() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let (result, tree) = evaluate_with_tree(&ConditionNode::InitialEvaluation, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "initial_evaluation");
    assert_eq!(tree.status, NodeStatus::True);

    let ctx2 = make_ctx("a", &record, &records, &deps);
    let (result2, tree2) = evaluate_with_tree(&ConditionNode::InitialEvaluation, &ctx2);
    assert!(!result2.fired);
    assert_eq!(tree2.status, NodeStatus::False);
}

#[test]
fn test_data_version_changed_true_when_version_differs() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v2".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev_state = AssetConditionState {
        last_data_version: Some("v1".to_string()),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::DataVersionChanged, &ctx).fired);
}

#[test]
fn test_data_version_changed_false_when_same() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev_state = AssetConditionState {
        last_data_version: Some("v1".to_string()),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::DataVersionChanged, &ctx).fired);
}

#[test]
fn test_data_version_changed_true_first_version() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(evaluate(&ConditionNode::DataVersionChanged, &ctx).fired);
}

#[test]
fn test_data_version_changed_suppressed_on_initial_tick() {
    // On the initial tick a pre-existing version is not a change, so DataVersionChanged
    // suppresses (mirrors NewlyUpdated's `(Some, None) => !is_initial`).
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    assert!(
        !evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "first version observed on the initial tick must not count as a change"
    );
}

#[test]
fn test_data_version_changed_suppressed_when_baseline_predates_tracking() {
    // Baseline predates version tracking (state exists, timestamp matches record, only
    // version baseline missing): the version was already there, so not a change.
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let state = AssetConditionState {
        last_materialized_timestamp: record.last_timestamp,
        ..Default::default()
    };
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &state;
    assert!(
        !evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "missing baseline with no new materialization is not a version change"
    );

    // But a materialization past the state's last observation IS a change signal.
    let stale = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    ctx.prev_state = &stale;
    assert!(
        evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "version appearing alongside a new materialization must fire"
    );
}

#[test]
fn test_data_version_changed_false_no_version() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = None;
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::DataVersionChanged, &ctx).fired);
}

#[test]
fn test_data_version_changed_false_version_disappeared() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = None;
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev_state = AssetConditionState {
        last_data_version: Some("v1".to_string()),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::DataVersionChanged, &ctx).fired);
}

#[test]
fn test_data_version_changed_with_tree() {
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v2".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let prev_state = AssetConditionState {
        last_data_version: Some("v1".to_string()),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let (result, tree) = evaluate_with_tree(&ConditionNode::DataVersionChanged, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "data_version_changed");
    assert_eq!(tree.status, NodeStatus::True);
}

#[test]
fn test_data_version_changed_state_tracking() {
    // update_condition_state persists last_data_version, so a repeat version returns false.
    let mut record = make_materialized_record("a", 100);
    record.last_data_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Tick 1: no previous version → fires
    let ctx1 = make_ctx("a", &record, &records, &deps);
    let result1 = evaluate(&ConditionNode::DataVersionChanged, &ctx1);
    assert!(result1.fired);

    let mut state = AssetConditionState::default();
    let update_ctx = StateUpdateContext::from_eval_context(&ctx1);
    update_condition_state(&mut state, &update_ctx, &result1);
    assert_eq!(state.last_data_version, Some("v1".to_string()));

    // Tick 2: same version → does not fire
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result2 = evaluate(&ConditionNode::DataVersionChanged, &ctx2);
    assert!(!result2.fired);

    // Tick 3: version changes → fires again
    let mut record_v2 = record.clone();
    record_v2.last_data_version = Some("v2".to_string());
    let records_v2 = HashMap::from([("a".to_string(), record_v2.clone())]);
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record_v2,
        cache: CacheSnapshot {
            records: &records_v2,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &state,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 4_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result3 = evaluate(&ConditionNode::DataVersionChanged, &ctx3);
    assert!(result3.fired);
}

#[test]
fn test_backfill_in_progress_true_when_in_backfill() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::new(),
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::BackfillInProgress, &ctx).fired);
}

#[test]
fn test_backfill_in_progress_false_when_not_in_backfill() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::BackfillInProgress, &ctx).fired);
}

#[test]
fn test_backfill_in_progress_only_matches_selected_assets() {
    // "a" is in backfill, "b" is not
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::new(),
    };
    let ctx_a = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &a,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let ctx_b = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::BackfillInProgress, &ctx_a).fired);
    assert!(!evaluate(&ConditionNode::BackfillInProgress, &ctx_b).fired);
}

#[test]
fn test_backfill_in_progress_with_tree() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::new(),
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let (result, tree) = evaluate_with_tree(&ConditionNode::BackfillInProgress, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "backfill_in_progress");
    assert_eq!(tree.status, NodeStatus::True);
}

#[test]
fn test_backfill_in_progress_composition_with_in_progress() {
    // InProgress | BackfillInProgress — true if either is true
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::new(),
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::InProgress | ConditionNode::BackfillInProgress;
    assert!(evaluate(&cond, &ctx).fired);
    // InProgress alone is false
    assert!(!evaluate(&ConditionNode::InProgress, &ctx).fired);
}

#[test]
fn test_backfill_in_progress_dep_aggregate() {
    // any_deps_match(backfill_in_progress) — true if any upstream dep is in a backfill
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::new(),
    };
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::any_deps_match(ConditionNode::BackfillInProgress);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_backfill_in_progress_partitioned_targets_subset() {
    // Backfill targets only partitions "p1" and "p2" out of "p1","p2","p3"
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::from([("bf-1".to_string(), vec![spk("p1"), spk("p2")])]),
    };
    let data = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 10), ("p2", 20), ("p3", 30)],
    );
    let pctx = data.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::BackfillInProgress, &ctx);
    assert!(result.fired);
    // Only p1 and p2 should be selected, not p3
    let selection = result.selection.unwrap();
    match &selection {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&spk("p1")));
            assert!(keys.contains(&spk("p2")));
            assert!(!keys.contains(&spk("p3")));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_backfill_in_progress_partitioned_empty_keys_selects_all() {
    // Backfill with empty partition_keys targets the whole asset
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::from([("bf-1".to_string(), vec![])]),
    };
    let data = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 10), ("p2", 20), ("p3", 30)],
    );
    let pctx = data.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::BackfillInProgress, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::All,
        "a backfill with no recorded keys targets the whole universe"
    );
}

#[test]
fn test_backfill_in_progress_partitioned_disjoint_keys() {
    // Backfill targets partitions that don't exist in the asset's partition space → empty
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([("a".to_string(), vec!["bf-1".to_string()])]),
        partition_keys: HashMap::from([("bf-1".to_string(), vec![spk("x1"), spk("x2")])]),
    };
    let data = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 10), ("p2", 20), ("p3", 30)],
    );
    let pctx = data.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::BackfillInProgress, &ctx);
    assert!(!result.fired);
    let selection = result.selection.unwrap();
    assert!(matches!(selection, PartitionSelection::Empty));
}

#[test]
fn test_backfill_in_progress_multiple_backfills_union_partitions() {
    // Two backfills target different partition subsets → union
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([(
            "a".to_string(),
            vec!["bf-1".to_string(), "bf-2".to_string()],
        )]),
        partition_keys: HashMap::from([
            ("bf-1".to_string(), vec![spk("p1")]),
            ("bf-2".to_string(), vec![spk("p3")]),
        ]),
    };
    let data = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 10), ("p2", 20), ("p3", 30)],
    );
    let pctx = data.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::BackfillInProgress, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&spk("p1")));
            assert!(keys.contains(&spk("p3")));
            assert!(!keys.contains(&spk("p2")));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_backfill_in_progress_one_backfill_empty_keys_short_circuits() {
    // An empty-keys backfill short-circuits to all partitions regardless of the other.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let bf = crate::condition::cache::BackfillState {
        assets: HashMap::from([(
            "a".to_string(),
            vec!["bf-1".to_string(), "bf-2".to_string()],
        )]),
        partition_keys: HashMap::from([
            ("bf-1".to_string(), vec![spk("p1")]),
            ("bf-2".to_string(), vec![]),
        ]),
    };
    let data = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 10), ("p2", 20), ("p3", 30)],
    );
    let pctx = data.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &bf,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::BackfillInProgress, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::All,
        "a backfill with no recorded keys targets the whole universe"
    );
}

#[test]
fn test_last_executed_with_tags_values_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_values_mismatch() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "staging".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_false_when_no_run() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_key_only_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "staging".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string(), "team".to_string()],
        tag_values: vec![],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_key_missing() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_combined_keys_and_values() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);

    let cond2 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["missing".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond2, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_subset_containment() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
            ("priority".to_string(), "high".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_only_matches_target_asset() {
    let record_a = make_materialized_record("a", 100);
    let record_b = make_materialized_record("b", 100);
    let records = HashMap::from([
        ("a".to_string(), record_a.clone()),
        ("b".to_string(), record_b.clone()),
    ]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "b".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record_a, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_tree_output() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(tree.status, NodeStatus::True);
    assert_eq!(tree.label, "last_executed_with_tags(env=prod)");
}

#[test]
fn test_last_executed_with_tags_composition_with_not() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = !ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_partitioned_per_partition() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let _pk2 = spk("2024-01-02");
    let partition_run_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([(
            pk1.clone(),
            Arc::from(vec![("env".to_string(), "backfill".to_string())]),
        )]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.tags.partition_last_run_tags = &partition_run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_executed_with_tags_partitioned_no_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let data = OwnedPartitionData::new(&["2024-01-01"], &["2024-01-01"], &[("2024-01-01", 10)]);
    let pctx = data.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
}

#[test]
fn test_last_executed_with_tags_partitioned_key_only() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let partition_run_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec![("env".to_string(), "prod".to_string())]),
            ),
            (
                pk2.clone(),
                Arc::from(vec![("team".to_string(), "data".to_string())]),
            ),
        ]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.tags.partition_last_run_tags = &partition_run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_executed_with_tags_empty_tags_in_cache_does_not_match() {
    // A cached empty-tags entry must not vacuously match (.all() on empty iters is true);
    // the evaluator returns false when the run had no tags.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags: HashMap<String, Arc<[(String, String)]>> =
        HashMap::from([("a".to_string(), Arc::from(vec![]))]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);

    let cond2 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    assert!(!evaluate(&cond2, &ctx).fired);

    // BOTH empty: vacuous truth (.all() on empty iters); defended at the cache
    // layer, documented here for the bypassed-guard edge case.
    let cond3 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    };
    assert!(evaluate(&cond3, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_mismatch() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "staging".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_no_materializations() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_multiple_runs_one_matches() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![("env".to_string(), "staging".to_string())]),
            Arc::from(vec![("env".to_string(), "prod".to_string())]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_all_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "data".to_string()),
            ]),
            Arc::from(vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "infra".to_string()),
            ]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_one_missing() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![("env".to_string(), "prod".to_string())]),
            Arc::from(vec![("env".to_string(), "staging".to_string())]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_no_materializations() {
    // Not vacuously true.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_key_only() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "staging".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_combined_keys_and_values() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_new_update_tags_asset_selectivity() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
    )]);

    let mut ctx_a = make_ctx("a", &a, &records, &deps);
    ctx_a.tags.tick_materialization_tags = &tick_tags;
    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx_a).fired);

    let mut ctx_b = make_ctx("b", &b, &records, &deps);
    ctx_b.tags.tick_materialization_tags = &tick_tags;
    assert!(!evaluate(&cond, &ctx_b).fired);
}

#[test]
fn test_new_update_tags_in_any_deps_match_composition() {
    // Asset "c" depends on "a" and "b"; only "a" materialized this tick with the backfill tag.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let c = make_materialized_record("c", 150);
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::from([("c".to_string(), vec!["a".to_string(), "b".to_string()])]);
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![(
            "type".to_string(),
            "backfill".to_string(),
        )])],
    )]);

    let mut ctx = make_ctx("c", &c, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::any_deps_match(ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("type".to_string(), "backfill".to_string())],
    });
    assert!(evaluate(&cond, &ctx).fired);

    // all_deps_match → false because "b" has no tick materializations
    let cond_all = ConditionNode::all_deps_match(ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("type".to_string(), "backfill".to_string())],
    });
    assert!(!evaluate(&cond_all, &ctx).fired);
}

#[test]
fn test_new_update_tags_run_with_empty_tags() {
    // A run completed with no tags — both AnyNewUpdate and AllNewUpdates are false.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([("a".to_string(), vec![Arc::from(vec![])])]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond_any = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond_any, &ctx).fired);

    let cond_all = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond_all, &ctx).fired);
}

#[test]
fn test_new_update_tags_tree_output() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let (_result, tree) = evaluate_with_tree(&cond, &ctx);
    assert_eq!(tree.status, NodeStatus::True);
    assert!(tree.label.contains("has_run_with_tags"));
}

#[test]
fn test_has_run_with_tags_partitioned() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let tick_part_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([(
            pk1.clone(),
            vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
        )]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.tags.tick_partition_materialization_tags = &tick_part_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_all_runs_have_tags_partitioned() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let tick_part_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                vec![
                    Arc::from(vec![("env".to_string(), "prod".to_string())]),
                    Arc::from(vec![("env".to_string(), "staging".to_string())]),
                ],
            ),
            (
                pk2.clone(),
                vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
            ),
        ]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.tags.tick_partition_materialization_tags = &tick_part_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    // Only pk2 should match (all its runs have env=prod)
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk2));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_new_update_tags_partitioned_no_materializations() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let data = OwnedPartitionData::new(&["2024-01-01"], &["2024-01-01"], &[("2024-01-01", 10)]);
    let pctx = data.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
}

// LastRunIncludesTarget: dep's latest run also included the root asset in
// `asset_names`; always false when target_key == root_key (self-referential guard).

#[test]
fn test_last_run_includes_target_true_when_run_includes_root() {
    // Dep b's joint run [b, c] contains root "c" → true.
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string(), "c".to_string()]),
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c"; // evaluating on dep b, root is c
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_run_excludes_root() {
    // Dep b's solo run [b] excludes root "c" → false.
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([("b".to_string(), Arc::from(vec!["b".to_string()]))]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_no_cache_entry() {
    // No run data for this asset → false
    let record = make_record("b");
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_self_referential() {
    // target_key == root_key → always false (self-referential guard)
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([("a".to_string(), Arc::from(vec!["a".to_string()]))]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    // root_key defaults to target_key ("a") via make_ctx
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_not_composition() {
    // ~last_run_includes_target: true when dep's run did NOT include root
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string()]), // solo run, no root "c"
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    ctx.tags.last_run_asset_names = &asset_names;

    let cond = !ConditionNode::LastRunIncludesTarget;
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_in_any_deps_match() {
    // any_deps_match(newly_updated & ~last_run_includes_target) on root "a", deps [b,c]:
    // dep b (joint [a,b]) filtered, dep c (solo [c]) included.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 100);
    let c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string(), "c".to_string()])]);
    let asset_names = HashMap::from([
        (
            "b".to_string(),
            Arc::from(vec!["a".to_string(), "b".to_string()]),
        ), // joint run included root "a"
        ("c".to_string(), Arc::from(vec!["c".to_string()])), // solo run, no root "a"
    ]);
    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let dep_c_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let all_states = HashMap::from([
        ("b".to_string(), dep_b_state),
        ("c".to_string(), dep_c_state),
    ]);
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.last_run_asset_names = &asset_names;
    ctx.all_asset_states = &all_states;

    // b: newly_updated=true, last_run_includes_target=true (joint run [a,b]) → ~=false → AND=false
    // c: newly_updated=true, last_run_includes_target=false (solo [c]) → ~=true → AND=true
    let cond = ConditionNode::any_deps_match(
        ConditionNode::NewlyUpdated & !ConditionNode::LastRunIncludesTarget,
    );
    assert!(evaluate(&cond, &ctx).fired);

    // If we only had "b" as a dep (joint run with root), should be false
    let deps_b_only = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    ctx.cache.upstream_deps = &deps_b_only;
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_tree_output() {
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string(), "c".to_string()]),
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    ctx.tags.last_run_asset_names = &asset_names;

    let (result, tree) = evaluate_with_tree(&ConditionNode::LastRunIncludesTarget, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "last_run_includes_target");
    assert_eq!(tree.node_type, "Leaf");
    assert_eq!(tree.status, NodeStatus::True);
}

#[test]
fn test_last_run_includes_target_partitioned() {
    // Partition-level: pk1's run included root "c", pk2's did not (target b, root c).
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]);

    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["b".to_string(), "c".to_string()]),
            ), // joint run included root
            (pk2.clone(), Arc::from(vec!["b".to_string()])), // solo run
        ]),
    )]);

    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;

    let partition_status = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &partition_status,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::LastRunIncludesTarget, &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    match sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&pk1)); // joint run included root
            assert!(!keys.contains(&pk2)); // solo run did not
            assert_eq!(keys.len(), 1);
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_run_includes_target_only_checks_target_not_root() {
    // The check is on the dep's (target b) run, not the root's (c):
    // what matters is b's asset_names containing "c".
    let record_b = make_materialized_record("b", 100);
    let record_c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("b".to_string(), record_b.clone()),
        ("c".to_string(), record_c.clone()),
    ]);
    let deps = HashMap::new();
    // b's run does NOT include c; c's run includes b (irrelevant)
    let asset_names = HashMap::from([
        ("b".to_string(), Arc::from(vec!["b".to_string()])),
        (
            "c".to_string(),
            Arc::from(vec!["b".to_string(), "c".to_string()]),
        ),
    ]);
    let mut ctx = make_ctx("b", &record_b, &records, &deps);
    ctx.root_key = "c";
    ctx.tags.last_run_asset_names = &asset_names;

    // b's run = [b], does not contain root "c" → false
    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_partitioned_joint_run_suppresses_newly_updated() {
    // Dep b and root a co-materialized at pk1 in a joint run; next tick b:pk1 is
    // NewlyUpdated but LastRunIncludesTarget must suppress the re-fire.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200); // b was updated (ts > prev)
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 100)]);

    // b:pk1 joint run [a, b] includes root "a"; b:pk2 solo run [b].
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["a".to_string(), "b".to_string()]),
            ),
            (pk2.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    // b's prev state: saw b at ts=100 previously
    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    // Dep "b" partition status: both partitions materialized with their timestamps
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 100)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    // any_deps_updated = AnyDepsMatch(NewlyUpdated & !LastRunIncludesTarget | WillBeRequested)
    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1 NewlyUpdated but joint-run suppressed; pk2 not NewlyUpdated → neither fires.
    assert!(!result.fired, "joint run should suppress re-fire for pk1");
    match result.selection {
        Some(PartitionSelection::Empty) | None => {} // expected
        Some(PartitionSelection::Keys(ref keys)) if keys.is_empty() => {}
        other => panic!("expected empty selection, got {:?}", other),
    }
}

#[test]
fn dep_updated_requires_dep_newer_than_target_key() {
    // A dep key counts as updated only while strictly newer than the root's own
    // materialization of that key (self-suppressing once the root advances past it).
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    // Root "a" materialized pk1 and pk2 at 100.
    let a_timestamps = HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]);

    // Dep "b": pk1 ts equals the root's (nothing new), pk2 genuinely newer;
    // empty eval-state baseline (drain-lag shape).
    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    // Solo runs: no joint-run suppression in play.
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (pk1.clone(), Arc::from(vec!["b".to_string()])),
            (pk2.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    assert!(result.fired, "pk2 is genuinely newer than the root");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(keys.contains(&pk2));
            assert!(
                !keys.contains(&pk1),
                "a dep key no newer than the root's must not count as updated"
            );
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn partitioned_root_unpartitioned_dep_refires_stale_older_partitions() {
    // A partitioned root on an unpartitioned dep (bool fallback): the staleness
    // floor must be the root's oldest partition attempt (min), not the asset-level
    // max, or genuinely-stale older partitions never re-fire.
    let pk_old = spk("2024-01-01");
    let pk_mid = spk("2024-01-02");
    let pk_new = spk("2024-01-03");
    let all_keys = HashSet::from([pk_old.clone(), pk_mid.clone(), pk_new.clone()]);

    // Root "a": partitions at 10 / 30 / 50; the asset-level record carries the max (50).
    let a = make_materialized_record("a", 50);
    // Dep "b": unpartitioned, updated at 35 — newer than pk_old/pk_mid, older than pk_new.
    let b = make_materialized_record("b", 35);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([
        (pk_old.clone(), 10i64),
        (pk_mid.clone(), 30),
        (pk_new.clone(), 50),
    ]);
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("a".to_string(), a_partition_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    // "b" absent from upstream_partition_keys → unpartitioned dep → bool fallback.
    let no_upstream_keys = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    let covers = |sel: &Option<PartitionSelection>, k: &PartitionKey| match sel {
        Some(PartitionSelection::All) => true,
        Some(PartitionSelection::Keys(ks)) => ks.contains(k),
        _ => false,
    };
    // dep@35 newer than the older partition attempts (10, 30) → the edge must
    // fire and re-materialize those stale partitions.
    assert!(
        result.fired,
        "dep@35 newer than older partitions (10/30) → must re-fire, got {:?}",
        result.selection
    );
    assert!(
        covers(&result.selection, &pk_old),
        "stale pk_old (mat@10 < dep@35) must be selected, got {:?}",
        result.selection
    );
    assert!(
        covers(&result.selection, &pk_mid),
        "stale pk_mid (mat@30 < dep@35) must be selected, got {:?}",
        result.selection
    );
}

#[test]
fn all_partitions_dep_frontier_key_does_not_refire_whole_universe() {
    // AllPartitions floors the dep against the min effective ts over the universe;
    // the floor must ignore never-attempted frontier keys, else a new key refires
    // the whole universe with no upstream change.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // freshly minted, never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 90);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    // Root "a": d1/d2 at 100, d3 never attempted. Dep "b": u1 at 90, older than every attempted key.
    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 90i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    // Dep u1@90 older than every attempted key (100); new frontier d3 must not drag the floor to None and broadcast All.
    assert!(
        !result.fired,
        "a never-attempted frontier key must not refire the universe with no \
         upstream change, got {:?}",
        result.selection
    );
}

#[test]
fn empty_universe_all_selection_does_not_fire() {
    // `All` of an empty partition universe selects nothing; reporting
    // fired=true would leak a full WillBeRequested signal to downstreams
    // evaluated later in the same tick.
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), a.clone())]);
    let deps = HashMap::new();
    let all_keys: HashSet<PartitionKey> = HashSet::new();
    let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry::default(),
    )]);
    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;
    ctx.is_initial = true; // InitialEvaluation yields `All` independent of the universe

    let empty_mappings = HashMap::new();
    let no_upstream = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::InitialEvaluation, &ctx);
    assert!(
        !result.fired,
        "All over an empty universe must not fire; got {:?}",
        result.selection
    );
}

#[test]
fn unpartitioned_dep_frontier_key_does_not_refire_whole_universe() {
    // The bridged (unpartitioned-dep) path floors the dep against the whole
    // root universe like an AllPartitions edge; the floor must ignore
    // never-attempted frontier keys, else a freshly-minted key drags the
    // floor to None and the dep refires every partition each tick.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // freshly minted, never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 90); // older than every attempted key
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("a".to_string(), a_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    // "b" absent from upstream_partition_keys → unpartitioned dep → bridged bool path.
    let no_upstream_keys = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    assert!(
        !result.fired,
        "a never-attempted frontier key must not make the unpartitioned dep \
         (@90, older than every attempted key @100) refire the universe, got {:?}",
        result.selection
    );
}

#[test]
fn test_empty_partitioned_dep_universe_does_not_bridge_latch_to_all() {
    // An empty-universe partitioned dep (present with empty set, unlike an absent
    // unpartitioned dep) must take the partitioned path, not the bool fallback that
    // bridges a stateful latch to `All` and later fires the whole universe.
    let rk = spk("rk1");
    let all_keys = HashSet::from([rk.clone()]);
    let r = make_materialized_record("r", 100);
    let u = make_record("u"); // never materialized → Missing is true
    let records = HashMap::from([("r".to_string(), r.clone()), ("u".to_string(), u.clone())]);
    let deps = HashMap::from([("r".to_string(), vec!["u".to_string()])]);

    let partition_statuses = HashMap::from([("r".to_string(), Default::default())]);
    let all_states = HashMap::new();
    let mut ctx = make_ctx("r", &r, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(("r".into(), "u".into()), PartitionMappingKind::Identity)]);
    // u PRESENT with an EMPTY universe (partitioned, no keys yet).
    let upstream_u = HashMap::from([("u".to_string(), HashSet::<PartitionKey>::new())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &HashMap::new(),
        resolver: PartitionResolver::new(&mappings, &upstream_u),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let condition = ConditionNode::AnyDepsMatch {
        condition: Box::new(ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing))),
        label: None,
    };
    let result = evaluate(&condition, &ctx);

    if let Some(dep_sels) = &result.dep_sub_selections {
        for (dep, latch) in dep_sels {
            for (idx, sel) in latch {
                assert_ne!(
                    sel,
                    &PartitionSelection::All,
                    "empty-universe dep {dep} latched node {idx} as All \
                     (bool-fallback bridge); the partitioned path must be taken"
                );
            }
        }
    }
}

#[test]
fn all_partitions_dep_genuine_update_still_fires() {
    // When an upstream key is newer than an attempted downstream key, the AllPartitions edge must still fire.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 150);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 150i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);
    assert!(
        result.fired,
        "upstream u1@150 newer than attempted downstream (100) must fire, got {:?}",
        result.selection
    );
}

#[test]
fn all_partitions_dep_initial_population_fires() {
    // A never-materialized AllPartitions fan-out must still fire to populate itself:
    // a None per-key floor means "never materialized ⇒ updated" (fire), not exclude.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let all_keys = HashSet::from([d1.clone(), d2.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_record("a"); // never materialized
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    // Root "a": nothing attempted. Dep "b": u1 materialized at 100.
    let a_status = crate::condition::cache::PartitionStatusEntry::default();
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 100i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);
    assert!(
        result.fired,
        "a never-materialized fan-out downstream must fire to populate itself, got {:?}",
        result.selection
    );
}

#[test]
fn dep_updated_floor_compares_mapped_downstream_key() {
    // The staleness floor must compare a dep key against the root's materialization
    // of the mapped downstream key, not a same-named key. With time_window(offset=-1),
    // b@D drives a@(D+1) and self-suppresses once a@(D+1) is newer.
    let a = make_materialized_record("a", 400);
    let b = make_materialized_record("b", 300);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(86400.0),
        start: chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        end: Some(
            chrono::NaiveDate::from_ymd_opt(2024, 2, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        fmt: "%Y-%m-%d".to_string(),
    };

    let b_keys = HashSet::from([spk("2024-01-04"), spk("2024-01-05")]);
    let a_keys = HashSet::from([spk("2024-01-05"), spk("2024-01-06")]);
    // Root already consumed both dep updates: a@05 (from b@04) and a@06 (from b@05) newer than their driving dep keys.
    let a_timestamps = HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 400)]);

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    // Solo runs: no joint-run suppression in play.
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (spk("2024-01-04"), Arc::from(vec!["b".to_string()])),
            (spk("2024-01-05"), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".to_string(), "b".to_string()),
        PartitionMappingKind::TimeWindow {
            offset: -1,
            grid: Some(grid),
        },
    )]);
    let upstream_b = HashMap::from([("b".to_string(), b_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "every dep key's mapped downstream key is already newer; got {:?}",
        result.selection
    );

    // Control: root's mapped key a@06 older than b@05 → that downstream key fires.
    let a_timestamps_stale = HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 200)]);
    let statuses_stale = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: a_timestamps_stale.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_stale = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps_stale,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_stale,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_stale);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired, "a@06 is older than b@05 now");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert_eq!(keys.len(), 1);
            assert!(
                keys.contains(&spk("2024-01-06")),
                "the fire must target the mapped downstream key"
            );
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn will_be_requested_carries_the_upstream_fired_selection() {
    // A partitioned upstream that fired for one key this tick must make only the
    // mapped downstream key eligible via any_deps_updated's WillBeRequested branch,
    // not the whole universe.
    let down = make_materialized_record("down", 100);
    let up = make_materialized_record("up", 100);
    let records = HashMap::from([
        ("down".to_string(), down.clone()),
        ("up".to_string(), up.clone()),
    ]);
    let deps = HashMap::from([("down".to_string(), vec!["up".to_string()])]);

    let pa = spk("a");
    let pb = spk("b");
    let pc = spk("c");
    let keys = HashSet::from([pa.clone(), pb.clone(), pc.clone()]);
    // Equal timestamps on both sides keep NewlyUpdated quiet, isolating WillBeRequested.
    let ts: HashMap<PartitionKey, i64> = keys.iter().map(|k| (k.clone(), 100i64)).collect();

    let all_states = HashMap::new();
    let partition_statuses = HashMap::from([
        (
            "down".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: ts.clone(),
                ..Default::default()
            },
        ),
        (
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: ts.clone(),
                ..Default::default()
            },
        ),
    ]);
    let partition_asset_names = HashMap::from([(
        "up".to_string(),
        keys.iter()
            .map(|k| (k.clone(), Arc::from(vec!["up".to_string()])))
            .collect::<HashMap<_, _>>(),
    )]);

    // Upstream's own condition fired for 'a' only, earlier this tick.
    let requested = HashMap::from([(
        "up".to_string(),
        PartitionSelection::Keys(HashSet::from([pa.clone()])),
    )]);

    let mut ctx = make_ctx("down", &down, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;
    ctx.requested_this_tick = &requested;

    let empty_mappings = HashMap::new();
    let upstream_up = HashMap::from([("up".to_string(), keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &ts,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_up),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref sel)) => {
            assert_eq!(
                sel,
                &HashSet::from([pa.clone()]),
                "only the upstream's fired key may cascade, not the universe"
            );
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn dep_updated_retries_once_per_dep_update_after_failure() {
    // A failed run never advances the materialization floor; the failure timestamp
    // must raise it — suppressed while the failure postdates the dep update, retried
    // when the dep lands something newer.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 300);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let k = spk("2024-01-01");
    let keys = HashSet::from([k.clone()]);
    // Root "a" never succeeded for k; its run at 400 failed.
    let a_timestamps: HashMap<PartitionKey, i64> = HashMap::new();

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(k.clone(), 300i64)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        failed: HashSet::from([k.clone()]),
        failed_timestamps: HashMap::from([(k.clone(), 400i64)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([(k.clone(), Arc::from(vec!["b".to_string()]))]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "the failed attempt at 400 already consumed the dep update at 300; \
         re-firing every tick is an unbounded retry loop; got {:?}",
        result.selection
    );

    // Control: dep lands new data (500 > failure 400) → exactly one retry becomes due.
    let statuses_retry = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                failed: HashSet::from([k.clone()]),
                failed_timestamps: HashMap::from([(k.clone(), 400i64)]),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(k.clone(), 500i64)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_retry = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_retry,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_retry);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired, "a newer dep update must retry the failed key");
    match result.selection {
        Some(PartitionSelection::Keys(ref sel)) => {
            assert_eq!(sel.len(), 1);
            assert!(sel.contains(&k));
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn dep_updated_ignores_dep_keys_outside_root_universe() {
    // Identity dep whose upstream range is a superset of the root's (upstream since
    // 2020, downstream start=2024): upstream-only keys can never be dispatched
    // downstream, so must not count as updated.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 500);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let old = spk("2020-01-01");
    let shared = spk("2024-06-01");
    let b_keys = HashSet::from([old.clone(), shared.clone()]);
    let a_keys = HashSet::from([shared.clone()]);
    let a_timestamps = HashMap::from([(shared.clone(), 100i64)]);

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 100)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (old.clone(), Arc::from(vec!["b".to_string()])),
            (shared.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), b_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "an upstream-only key must not fire the condition; got {:?}",
        result.selection
    );

    // Control: a genuine update of the shared key still fires it alone.
    let statuses_new = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: a_timestamps.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 150)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_new = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_new,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_new);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert_eq!(
                keys.len(),
                1,
                "only the shared key may fire, never the phantom: {keys:?}"
            );
            assert!(keys.contains(&shared));
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn test_last_run_includes_target_partitioned_solo_run_allows_newly_updated() {
    // Dep b in a solo run (not with root a): next tick b:pk1 is NewlyUpdated and must not be suppressed.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let all_keys = HashSet::from([pk1.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64)]);

    // Solo run: b:pk1 was in a run with [b] only → does NOT include root "a"
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([(pk1.clone(), Arc::from(vec!["b".to_string()]))]),
    )]);

    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1 NewlyUpdated and solo-run (not suppressed) → fires.
    assert!(result.fired, "solo run should allow NewlyUpdated to fire");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(keys.contains(&pk1));
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys selection with pk1, got {:?}", other),
    }
}

#[test]
fn test_last_run_includes_target_partitioned_mixed_joint_and_solo() {
    // Dep b: pk1 joint run with root a, pk2 solo → only pk2 fires any_deps_updated.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 200)]);

    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["a".to_string(), "b".to_string()]),
            ), // joint
            (pk2.clone(), Arc::from(vec!["b".to_string()])), // solo
        ]),
    )]);

    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.partition_last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1: joint run → suppressed. pk2: solo run → fires.
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(!keys.contains(&pk1), "pk1 should be suppressed (joint run)");
            assert!(keys.contains(&pk2), "pk2 should fire (solo run)");
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys with only pk2, got {:?}", other),
    }
}

#[allow(dead_code)] // Debugging helper kept for ad-hoc test inspection.
fn print_tree(tree: &EvalNodeResult, indent: usize) {
    let pad = " ".repeat(indent);
    let status = match tree.status {
        NodeStatus::True => "TRUE",
        NodeStatus::False => "FALSE",
        NodeStatus::Skipped => "SKIP",
    };
    eprintln!("{pad}{} [{status}]", tree.label);
    for child in &tree.children {
        print_tree(child, indent + 2);
    }
}

#[test]
fn test_asset_matches_evaluates_condition_on_named_asset() {
    // asset_matches("b", Missing) should be true when "b" is missing
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // missing
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &a, &records, &deps);

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_false_when_condition_not_met() {
    // asset_matches("b", Missing) should be false when "b" is materialized
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &a, &records, &deps);

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing);
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_false_when_key_not_in_records() {
    // asset_matches for a non-existent asset should be false
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), a.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &a, &records, &deps);

    let cond = ConditionNode::asset_matches(vec!["nonexistent".into()], ConditionNode::Missing);
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_newly_updated_on_non_dep() {
    // asset_matches can target a non-dep asset (cross-graph checks, e.g. "fire when sibling updated").
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200); // updated
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new(); // b is NOT a dep of a
    let b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), b_state)]);
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::NewlyUpdated);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_preserves_root_key() {
    // asset_matches preserves root_key so LastRunIncludesTarget still checks against the original root.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["a".to_string(), "b".to_string()]),
    )]);
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.tags.last_run_asset_names = &asset_names;

    // LastRunIncludesTarget on "b" should check if root "a" is in b's run
    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::LastRunIncludesTarget);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_in_boolean_composition() {
    // asset_matches composes with boolean operators
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // missing
    let c = make_materialized_record("c", 100); // not missing
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &a, &records, &deps);

    // b is missing AND c is not missing
    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing)
        & !ConditionNode::asset_matches(vec!["c".into()], ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_asset_matches_multi_key_any_semantics() {
    // asset_matches(["b", "c"], Missing) — true if ANY of them is missing
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100); // not missing
    let c = make_record("c"); // missing
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &a, &records, &deps);

    // One of [b, c] is missing → true
    let cond = ConditionNode::asset_matches(vec!["b".into(), "c".into()], ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);

    // Neither is missing → false
    let d = make_materialized_record("d", 100);
    let records2 = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("d".to_string(), d.clone()),
    ]);
    let ctx2 = make_ctx("a", &a, &records2, &deps);
    let cond2 = ConditionNode::asset_matches(vec!["b".into(), "d".into()], ConditionNode::Missing);
    assert!(!evaluate(&cond2, &ctx2).fired);
}

#[test]
fn test_bitand_flattens_nested_and() {
    // (a & b) & c should produce And([a, b, c]), not And([And([a, b]), c])
    let tree = ConditionNode::Missing & ConditionNode::InProgress & ConditionNode::ExecutionFailed;
    match tree {
        ConditionNode::And(children) => {
            assert_eq!(children.len(), 3);
            assert!(matches!(children[0], ConditionNode::Missing));
            assert!(matches!(children[1], ConditionNode::InProgress));
            assert!(matches!(children[2], ConditionNode::ExecutionFailed));
        }
        _ => panic!("expected flat And"),
    }
}

#[test]
fn test_bitor_flattens_nested_or() {
    let tree = ConditionNode::Missing | ConditionNode::InProgress | ConditionNode::ExecutionFailed;
    match tree {
        ConditionNode::Or(children) => {
            assert_eq!(children.len(), 3);
            assert!(matches!(children[0], ConditionNode::Missing));
            assert!(matches!(children[1], ConditionNode::InProgress));
            assert!(matches!(children[2], ConditionNode::ExecutionFailed));
        }
        _ => panic!("expected flat Or"),
    }
}

#[test]
fn test_without_matching_removes_matching_child() {
    // eager = SinceLastHandled(...) & !any_deps_missing & !any_deps_in_progress & !in_flight & !ExecutionFailed.
    // Strip the in-progress guard operand Not(any_deps_in_progress()) by structural match.
    let eager = ConditionNode::eager();
    let result = eager.without_matching(&|c| *c == !ConditionNode::any_deps_in_progress());
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_matching_removes_only_exact_operand() {
    // Operands match structurally: bare any_deps_missing() does not match the
    // Not(...) guard (no-op); the exact negated operand strips it.
    let eager = ConditionNode::eager();
    assert_eq!(
        eager.without_matching(&|c| *c == ConditionNode::any_deps_missing()),
        eager,
        "bare any_deps_missing must not match the Not(...) guard"
    );
    let result = eager.without_matching(&|c| *c == !ConditionNode::any_deps_missing());
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_matching_on_non_and_is_identity() {
    let leaf = ConditionNode::Missing;
    assert_eq!(
        leaf.without_matching(&|c| *c == ConditionNode::InProgress),
        ConditionNode::Missing
    );
}

#[test]
fn test_replace_swaps_matching_node() {
    let eager = ConditionNode::eager();
    let result = eager.replace_by_label("any_deps_updated", &ConditionNode::NewlyUpdated);
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::NewlyUpdated)
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_replace_no_match_is_identity() {
    let tree = ConditionNode::Missing & ConditionNode::InProgress;
    let result = tree.replace_by_label("nonexistent", &ConditionNode::ExecutionFailed);
    assert_eq!(result, ConditionNode::Missing & ConditionNode::InProgress);
}

#[test]
fn test_replace_on_leaf() {
    assert_eq!(
        ConditionNode::Missing.replace_by_label("missing", &ConditionNode::InProgress),
        ConditionNode::InProgress,
    );
}

#[test]
fn test_replace_by_node_structural_match() {
    let eager = ConditionNode::eager();
    let result = eager.replace_by_node(
        &ConditionNode::any_deps_in_progress(),
        &ConditionNode::InProgress,
    );
    // Not(any_deps_in_progress) becomes Not(InProgress); the InProgress inside
    // Not(in_flight()) isn't a structural match, so the in-flight guard is untouched.
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::InProgress
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_replace_by_node_no_match() {
    let tree = ConditionNode::Missing & ConditionNode::InProgress;
    let result = tree.replace_by_node(&ConditionNode::ExecutionFailed, &ConditionNode::Missing);
    assert_eq!(result, tree);
}

#[test]
fn test_any_deps_missing() {
    let a = make_record("a"); // missing
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_any_deps_missing_none_missing() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(!evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_any_deps_in_progress() {
    // "b" depends on "a", and "a" is in progress → true
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let in_progress = HashSet::from(["a".to_string()]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &in_progress,
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_in_progress(), &ctx).fired);
}

#[test]
fn test_any_deps_in_progress_none() {
    // "b" depends on "a", but "a" is not in progress → false
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::any_deps_in_progress(), &ctx).fired);
}

#[test]
fn test_any_deps_updated() {
    let a = make_materialized_record("a", 200); // updated
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx).fired);
}

#[test]
fn unpartitioned_dep_updated_compares_against_root_record() {
    // The root ran at 110 reading b@99; b drains a tick later with a stale baseline (50).
    // The staleness floor must see the root is already newer and suppress.
    let r = make_materialized_record("R", 110);
    let b = make_materialized_record("b", 99);
    let records = HashMap::from([("R".to_string(), r.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("R".to_string(), vec!["b".to_string()])]);
    let b_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), b_state)]);

    let mut ctx = make_ctx("R", &r, &records, &deps);
    ctx.all_asset_states = &all_states;
    assert!(
        !evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "the root's run at 110 already consumed b@99"
    );

    // Control: b lands genuinely newer than the root -> fires.
    let b_new = make_materialized_record("b", 120);
    let records_new = HashMap::from([
        ("R".to_string(), r.clone()),
        ("b".to_string(), b_new.clone()),
    ]);
    let mut ctx2 = make_ctx("R", &r, &records_new, &deps);
    ctx2.all_asset_states = &all_states;
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx2).fired);
}

#[test]
fn unpartitioned_dep_updated_failed_root_retries_once() {
    // A failed root run consumes the dep update that triggered it; suppressed while
    // the failure postdates the dep, one retry when a newer dep lands.
    let r = make_materialized_record("R", 100);
    let b = make_materialized_record("b", 120);
    let records = HashMap::from([("R".to_string(), r.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("R".to_string(), vec!["b".to_string()])]);
    let failed_ts = HashMap::from([("R".to_string(), 130i64)]);
    let all_states = HashMap::new();

    let mut ctx = make_ctx("R", &r, &records, &deps);
    ctx.all_asset_states = &all_states;
    ctx.cache.failed_asset_timestamps = &failed_ts;
    assert!(
        !evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "the failed attempt at 130 already consumed b@120"
    );

    // Control: b lands after the failure -> one retry becomes due.
    let b_new = make_materialized_record("b", 140);
    let records_new = HashMap::from([
        ("R".to_string(), r.clone()),
        ("b".to_string(), b_new.clone()),
    ]);
    let mut ctx2 = make_ctx("R", &r, &records_new, &deps);
    ctx2.all_asset_states = &all_states;
    ctx2.cache.failed_asset_timestamps = &failed_ts;
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx2).fired);
}

#[test]
fn test_any_deps_updated_no_change() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        ..Default::default()
    };
    // Dep A needs state so NewlyUpdated sees no change (100 == 100)
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([("a".to_string(), a_state)]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::any_deps_updated(), &ctx).fired);
}

#[test]
fn test_and() {
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    // Missing AND Missing → true
    let cond = ConditionNode::And(vec![ConditionNode::Missing, ConditionNode::Missing]);
    assert!(evaluate(&cond, &ctx).fired);

    // Missing AND NOT Missing → false
    let cond = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::Not(Box::new(ConditionNode::Missing)),
    ]);
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_or() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    // Missing OR CodeVersionChanged → false (neither true)
    let cond = ConditionNode::Or(vec![
        ConditionNode::Missing,
        ConditionNode::CodeVersionChanged,
    ]);
    assert!(!evaluate(&cond, &ctx).fired);

    // Missing OR NOT Missing → true
    let cond = ConditionNode::Or(vec![
        ConditionNode::Missing,
        ConditionNode::Not(Box::new(ConditionNode::Missing)),
    ]);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_not() {
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::Not(Box::new(ConditionNode::Missing)), &ctx,).fired);
}

#[test]
fn test_newly_true_transition() {
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // First tick: Missing is true, previous was false → NewlyTrue fires
    let prev = AssetConditionState::default(); // no previous results
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: true,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));
    assert!(evaluate(&cond, &ctx).fired);

    // Second tick: Missing is still true, previous was true → NewlyTrue does NOT fire
    let mut prev2 = AssetConditionState::default();
    prev2.previous_results.insert(0, true); // node index 0 = the NewlyTrue node
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&cond, &ctx2).fired);

    // Third tick: Missing still true, inner was true tick 2 → still does not fire
    // (must store `current`, not `result`, or it re-fires every other tick).
    let mut prev3 = AssetConditionState::default();
    prev3.previous_results.insert(0, true); // inner was true last tick
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev3,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&cond, &ctx3).fired);
}

#[test]
fn test_counter_stability_and_short_circuit() {
    // And short-circuit must not shift node indices: in And(InProgress, NewlyTrue(Missing)),
    // NewlyTrue keeps a fixed index whether or not And short-circuits.
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::And(vec![
        ConditionNode::InProgress,
        ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing)),
    ]);

    // Tick 1: InProgress=false → And short-circuits, but NewlyTrue's index must still be assigned; result false.
    let ctx1 = make_ctx("a", &record, &records, &deps);
    let r1 = evaluate(&cond, &ctx1);
    assert!(!r1.fired);
    // NewlyTrue's index is 2 (And=0, InProgress=1, NewlyTrue=2, Missing=3); the
    // short-circuited And still evaluates the stateful child, so Missing=true records
    // current=true at stable index 2.
    assert_eq!(r1.sub_results.get(&2), Some(&true));
    assert_eq!(r1.sub_results.len(), 1);

    // Tick 2: InProgress=true (no short-circuit) → NewlyTrue(Missing) fires (inner=true, previous=false).
    let in_progress = HashSet::from(["a".to_string()]);
    let mut prev2 = AssetConditionState::default();
    // No previous results → NewlyTrue fires
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &in_progress,
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    // And(InProgress=true, NewlyTrue(Missing=true, prev=false)=true) → true
    assert!(r2.fired);
    // NewlyTrue should have stored its inner value (true) at index 2
    assert_eq!(r2.sub_results.get(&2), Some(&true));

    // Tick 3: Same state. NewlyTrue should NOT fire (inner was true last tick).
    prev2.previous_results = r2.sub_results;
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &in_progress,
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r3 = evaluate(&cond, &ctx3);
    // NewlyTrue(Missing=true, prev=true) → false, so And → false
    assert!(!r3.fired);
}

#[test]
fn test_on_missing_fires_when_missing_and_deps_present() {
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // missing
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(evaluate(&ConditionNode::on_missing(), &ctx).fired);
}

#[test]
fn test_on_missing_does_not_fire_when_dep_missing() {
    let a = make_record("a"); // missing
    let b = make_record("b"); // missing
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    // b is missing but a is also missing → AllDepsMatch(~Missing) fails
    assert!(!evaluate(&ConditionNode::on_missing(), &ctx).fired);
}

#[test]
fn test_on_missing_does_not_fire_when_in_progress() {
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // missing
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let in_progress = HashSet::from(["b".to_string()]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &in_progress,
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &AssetConditionState::default(),
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::on_missing(), &ctx).fired);
}

#[test]
fn test_eager_fires_on_missing() {
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // missing
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(evaluate(&ConditionNode::eager(), &ctx).fired);
}

#[test]
fn test_eager_fires_on_deps_updated() {
    let a = make_materialized_record("a", 200); // updated
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        ..Default::default()
    };
    // Dep A needs state so NewlyUpdated on A detects the change (200 > 100)
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([("a".to_string(), a_state)]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::eager(), &ctx).fired);
}

#[test]
fn test_eager_does_not_fire_when_up_to_date() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    // Dep A needs state (daemon seeds this on initial tick)
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([("a".to_string(), a_state)]);
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::eager(), &ctx).fired);
}

#[test]
fn test_all_deps_match_with_no_deps() {
    let a = make_record("a");
    let records = HashMap::from([("a".to_string(), a.clone())]);
    let deps = HashMap::new(); // no deps
    let ctx = make_ctx("a", &a, &records, &deps);
    // AllDepsMatch with empty deps → true (vacuous truth)
    let cond = ConditionNode::all_deps_match(ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_any_deps_match_recursive() {
    let a = make_record("a"); // missing
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    // AnyDepsMatch(Missing) → true because dep a is missing
    let cond = ConditionNode::any_deps_match(ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_bug_cron_tick_always_fires_on_first_eval() {
    // CronTickPassed with no baseline must not make the window [epoch, now] and always
    // match: "30 16 * * 1-5" must not fire at ~22:13 UTC on first eval.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);
    // ~2023-11-14 22:13 UTC (a Tuesday) — NOT 16:30
    let now_nanos = 1_700_000_000_000_000_000_i64;

    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &AssetConditionState::default(),
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now_nanos,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    assert!(
        !evaluate(&cond, &ctx).fired,
        "on_cron should not fire on first eval when no cron tick boundary is known"
    );
}

#[test]
fn test_cron_tick_fires_when_tick_passes_between_evals() {
    // Positive test: on_cron fires when a cron tick occurs between two evals.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    // Prev eval 16:00, current 16:31 UTC (Tue 2023-11-14); the 16:30 cron tick falls between → fires.
    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_979_460_000_000_000_i64;

    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick_nanos),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now_nanos,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    assert!(
        evaluate(&cond, &ctx).fired,
        "on_cron should fire when cron tick passes between evals"
    );
}

//
#[test]
fn test_in_latest_time_window_unpartitioned_always_true() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    assert!(
        evaluate(&cond, &ctx).fired,
        "unpartitioned assets should always be in the latest time window"
    );
}

#[test]
fn test_in_latest_time_window_unpartitioned_no_lookback() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: None,
    };
    assert!(
        evaluate(&cond, &ctx).fired,
        "unpartitioned assets should always be in the latest time window"
    );
}

#[test]
fn test_in_latest_time_window_unpartitioned_materialized() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    assert!(
        evaluate(&cond, &ctx).fired,
        "unpartitioned materialized assets should always be in the latest time window"
    );
}

#[test]
fn test_has_root_scope_latest_time_window() {
    assert!(!ConditionNode::eager().has_root_scope_latest_time_window());

    let root_level = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(7200.0),
        },
    ]);
    assert!(root_level.has_root_scope_latest_time_window());

    // Inside a dep aggregate the node filters the DEP's partitions.
    let dep_scoped = ConditionNode::any_deps_match(ConditionNode::And(vec![
        ConditionNode::NewlyUpdated,
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        },
    ]));
    assert!(!dep_scoped.has_root_scope_latest_time_window());
}

#[test]
fn test_bug_since_last_handled_refires_after_own_materialization() {
    // SinceLastHandled checks target.last_timestamp > last_handled_timestamp; the asset's
    // own completed materialization must not read as a spurious re-fire.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Child Not(Missing) stays true across ticks for a materialized asset.
    let cond = ConditionNode::SinceLastHandled(Box::new(ConditionNode::Not(Box::new(
        ConditionNode::Missing,
    ))));

    // Tick 1: child=true, last_handled_timestamp=None → fires
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev1,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&cond, &ctx1).fired, "Tick 1 should fire");

    // Simulate: materialization completes, target.last_timestamp updated
    let mut record2 = record.clone();
    record2.last_timestamp = Some(1500); // Updated by materialization
    let records2 = HashMap::from([("a".to_string(), record2.clone())]);

    // Tick 2: child still true, last_handled_timestamp=1000, target.last_timestamp=1500>1000
    let prev2 = AssetConditionState {
        last_handled_timestamp: Some(1000), // Set when we fired on tick 1
        last_materialized_timestamp: Some(100), // Target's ts at tick 1
        last_tick_timestamp: Some(1000),    // Previous tick's now
        ..Default::default()
    };
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record2,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&cond, &ctx2).fired,
        "Tick 2 should NOT re-fire — target update was from our own materialization"
    );
}

#[test]
fn test_newly_requested_any_deps_match_cross_asset_signaling() {
    // any_deps_match(newly_requested()): downstream fires the tick after upstream was requested.
    // Tick 1: not requested → no fire; Tick 2: requested last tick → fires; Tick 3: no longer newly_requested → no fire.

    let upstream_record = make_materialized_record("upstream", 100);
    let downstream_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), upstream_record.clone()),
        ("downstream".to_string(), downstream_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);

    let cond = ConditionNode::any_deps_match(ConditionNode::NewlyRequested);

    // Tick 1: no prior state → upstream not requested → downstream doesn't fire
    let all_states_1 = HashMap::new();
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &downstream_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev1,
        all_asset_states: &all_states_1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&cond, &ctx1).fired,
        "Tick 1: upstream not requested yet"
    );

    // After tick 1 upstream has last_handled=1000, last_tick=1000 (fired at now=1000).
    let all_states_2 = HashMap::from([
        (
            "upstream".to_string(),
            AssetConditionState {
                last_handled_timestamp: Some(1000),
                last_tick_timestamp: Some(1000),
                ..Default::default()
            },
        ),
        (
            "downstream".to_string(),
            AssetConditionState {
                last_tick_timestamp: Some(1000),
                ..Default::default()
            },
        ),
    ]);

    // Tick 2: eval_on_dep looks up upstream's state → NewlyRequested is true → fires
    let ctx2 = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &downstream_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &all_states_2["downstream"],
        all_asset_states: &all_states_2,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        evaluate(&cond, &ctx2).fired,
        "Tick 2: upstream was requested last tick → downstream fires"
    );

    // Tick 3: upstream last_handled=1000 != last_tick=2000 → no longer newly_requested → no fire.
    let all_states_3 = HashMap::from([(
        "upstream".to_string(),
        AssetConditionState {
            last_handled_timestamp: Some(1000),
            last_tick_timestamp: Some(2000),
            ..Default::default()
        },
    )]);
    let prev3 = AssetConditionState {
        last_tick_timestamp: Some(2000),
        ..Default::default()
    };
    let ctx3 = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &downstream_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev3,
        all_asset_states: &all_states_3,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&cond, &ctx3).fired,
        "Tick 3: upstream no longer newly_requested → downstream doesn't fire"
    );
}

#[test]
fn test_newly_requested_as_since_reset() {
    // code_version_changed().since(newly_requested()) with materialization completing between ticks:
    // Tick 1 (v2 vs v1) fires; after materialization (v2==v2) Tick 2 doesn't fire.

    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v2".to_string());
    record.last_materialization_code_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::Since {
        trigger: Box::new(ConditionNode::CodeVersionChanged),
        reset: Box::new(ConditionNode::NewlyRequested),
    };

    // Tick 1: code changed, never requested → fires
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev1,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert!(r1.fired, "Tick 1: code changed → fires");

    // Between ticks: materialization completes, code versions now match
    let mut record2 = record.clone();
    record2.last_materialization_code_version = Some("v2".to_string());
    record2.last_timestamp = Some(1500);
    let records2 = HashMap::from([("a".to_string(), record2.clone())]);

    // Tick 2: CodeVersionChanged is false (versions match) → doesn't fire
    let mut prev2 = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    prev2.previous_results = r1.sub_results;
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record2,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    assert!(
        !r2.fired,
        "Tick 2: materialization completed, code versions match → doesn't fire"
    );
}

#[test]
fn test_newly_requested_as_since_reset_fast_ticks() {
    // With ticks faster than materialization, CodeVersionChanged stays true and the condition re-fires (add & ~InProgress to guard):
    // Tick 1 fires; Tick 2 newly_requested resets latch → no fire; Tick 3 no longer newly_requested, code still changed → re-fires.

    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v2".to_string());
    record.last_materialization_code_version = Some("v1".to_string());
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::Since {
        trigger: Box::new(ConditionNode::CodeVersionChanged),
        reset: Box::new(ConditionNode::NewlyRequested),
    };

    // Tick 1: fires
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev1,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert!(r1.fired, "Tick 1: code changed → fires");

    // Tick 2: newly_requested resets the latch (record unchanged — still materializing)
    let mut prev2 = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    prev2.previous_results = r1.sub_results;
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    assert!(
        !r2.fired,
        "Tick 2: newly_requested resets latch → doesn't fire"
    );

    // Tick 3: no longer newly_requested, code still changed → re-fires (what & ~InProgress would prevent).
    let mut prev3 = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(2000),
        ..Default::default()
    };
    prev3.previous_results = r2.sub_results;
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev3,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let r3 = evaluate(&cond, &ctx3);
    assert!(
        r3.fired,
        "Tick 3: code still changed, no ~InProgress guard → re-fires"
    );
}

#[test]
fn test_newly_requested_in_since_last_handled() {
    // SinceLastHandled(Not(Missing)) debounces: fires once, then suppresses until
    // last_handled_timestamp < last_tick_timestamp.

    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::SinceLastHandled(Box::new(ConditionNode::Not(Box::new(
        ConditionNode::Missing,
    ))));

    // Tick 1: never handled → fires
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev1,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&cond, &ctx1).fired, "Tick 1: should fire");

    // Tick 2: handled on tick 1 (last_handled=1000=last_tick) → suppressed
    let prev2 = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&cond, &ctx2).fired,
        "Tick 2: just handled → suppressed"
    );

    // Tick 3: last_handled=1000, last_tick=2000 → handled before last tick → can fire again
    let prev3 = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(2000),
        ..Default::default()
    };
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &HashSet::new(),
            failed_assets: &HashSet::new(),
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev3,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        evaluate(&cond, &ctx3).fired,
        "Tick 3: handled before last tick → fires again"
    );
}

#[test]
fn test_tree_matches_eval_result() {
    // evaluate() and evaluate_with_tree() should agree on fired
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::eager();
    let eval = evaluate(&cond, &ctx);
    let (tree_eval, tree) = evaluate_with_tree(&cond, &ctx);
    assert_eq!(eval.fired, tree_eval.fired);
    assert_eq!(tree_eval.fired, tree.status == NodeStatus::True);
}

#[test]
fn test_tree_short_circuit_skipped() {
    // And([false_leaf, other]) → other should be Skipped
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    // Missing is false (asset is materialized), so second child should be skipped
    let cond = ConditionNode::And(vec![ConditionNode::Missing, ConditionNode::InProgress]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(!result.fired);
    assert_eq!(tree.status, NodeStatus::False);
    assert_eq!(tree.children.len(), 2);
    assert_eq!(tree.children[0].status, NodeStatus::False); // Missing=false
    assert_eq!(tree.children[1].status, NodeStatus::Skipped); // short-circuited
}

#[test]
fn test_tree_indices_stable() {
    // Node indices from evaluate_with_tree must match evaluate's sub_results keys
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::eager();
    let eval = evaluate(&cond, &ctx);
    let (tree_eval, _tree) = evaluate_with_tree(&cond, &ctx);

    // Both should produce identical sub_results
    assert_eq!(eval.sub_results, tree_eval.sub_results);
}

#[test]
fn test_tree_leaf_labels() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let (_, tree) = evaluate_with_tree(&ConditionNode::Missing, &ctx);
    assert_eq!(tree.label, "missing");
    assert_eq!(tree.node_type, "Leaf");
    assert_eq!(tree.status, NodeStatus::True); // Missing asset
}

#[test]
fn test_tree_or_short_circuit() {
    // Or([true_leaf, other]) → other should be Skipped
    let record = make_record("a"); // Missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::Or(vec![
        ConditionNode::Missing,    // true
        ConditionNode::InProgress, // should be skipped
    ]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(tree.children[0].status, NodeStatus::True);
    assert_eq!(tree.children[1].status, NodeStatus::Skipped);
}

#[test]
fn test_tree_serialization() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let (_, tree) = evaluate_with_tree(&ConditionNode::eager(), &ctx);
    let json = serde_json::to_vec(&tree).unwrap();
    let deserialized: EvalNodeResult = serde_json::from_slice(&json).unwrap();
    assert_eq!(tree.node_idx, deserialized.node_idx);
    assert_eq!(tree.status, deserialized.status);
}

// ── Benchmark A: Pure evaluation speed (no storage) ──

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
                tags: RunTagSnapshot {
                    last_run_tags: &EMPTY_RUN_TAGS,
                    partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
                    tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
                    tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
                    last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
                    partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
                },
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
                tags: RunTagSnapshot {
                    last_run_tags: &EMPTY_RUN_TAGS,
                    partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
                    tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
                    tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
                    last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
                    partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
                },
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
fn test_time_based_eval_set_single_cron_chain() {
    // A (cron) → B (eager) → C (eager) — all three in eval set
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("c".to_string(), "b".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 3);
    assert!(eval_set.contains("a"));
    assert!(eval_set.contains("b"));
    assert!(eval_set.contains("c"));
}

#[test]
fn test_time_based_eval_set_isolated_subgraph_excluded() {
    // A (cron) → B (eager)  |  C (eager) → D (eager) — only {A, B}
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("d".to_string(), "c".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
        ("d".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 2);
    assert!(eval_set.contains("a"));
    assert!(eval_set.contains("b"));
    assert!(!eval_set.contains("c"));
    assert!(!eval_set.contains("d"));
}

#[test]
fn test_time_based_eval_set_multiple_cron_overlapping() {
    // A (cron) → C (eager), B (cron) → C (eager) — all three
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("c".to_string(), "a".to_string()),
        ("c".to_string(), "b".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        (
            "b".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("c".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 3);
}

#[test]
fn test_time_based_eval_set_cron_no_downstream() {
    // A (cron) standalone, B (eager) standalone — only {A}
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 1);
    assert!(eval_set.contains("a"));
}

#[test]
fn test_time_based_eval_set_diamond() {
    // A (cron) → B, A → C, B → D, C → D — all four
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("c".to_string(), "a".to_string()),
        ("d".to_string(), "b".to_string()),
        ("d".to_string(), "c".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
        ("d".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 4);
}

// ── Benchmark: Selective eval (time-based + downstream) vs evaluate-all ──

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
                    tags: RunTagSnapshot {
                        last_run_tags: &EMPTY_RUN_TAGS,
                        partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
                        tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
                        tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
                        last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
                        partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
                    },
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
                    tags: RunTagSnapshot {
                        last_run_tags: &EMPTY_RUN_TAGS,
                        partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
                        tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
                        tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
                        last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
                        partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
                    },
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

// ── Benchmark B: With storage (in-memory + embedded) ──

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
        launched_by: LaunchedBy::Manual,
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
            launched_by: LaunchedBy::Manual,
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
                    tags: RunTagSnapshot {
                        last_run_tags: &EMPTY_RUN_TAGS,
                        partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
                        tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
                        tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
                        last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
                        partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
                    },
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

// ── Fingerprinting tests ─────────────────────────────────────────────

#[test]
fn test_fingerprint_stability() {
    let tree = ConditionNode::eager();
    assert_eq!(tree.fingerprint(), tree.fingerprint());
    assert_eq!(tree.fingerprint_hex(), tree.fingerprint_hex());
    assert_eq!(tree.fingerprint_hex().len(), 16);
}

#[test]
fn test_fingerprint_sensitivity() {
    let eager = ConditionNode::eager();
    let on_missing = ConditionNode::on_missing();
    assert_ne!(eager.fingerprint(), on_missing.fingerprint());

    let cron1 = ConditionNode::on_cron("0 * * * *".into(), None);
    let cron2 = ConditionNode::on_cron("*/5 * * * *".into(), None);
    assert_ne!(cron1.fingerprint(), cron2.fingerprint());

    // Adding a node changes the fingerprint
    let base = ConditionNode::Missing;
    let extended = ConditionNode::Missing & !ConditionNode::InProgress;
    assert_ne!(base.fingerprint(), extended.fingerprint());
}

#[test]
fn test_reset_for_new_tree() {
    let mut state = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        last_handled_timestamp: Some(1000),
        last_materialized_timestamp: Some(500),

        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    state.reset_for_new_tree("abc123".into());

    assert!(state.previous_results.is_empty());
    assert!(state.last_handled_timestamp.is_none());
    assert_eq!(state.last_materialized_timestamp, Some(500)); // preserved

    assert!(state.last_tick_timestamp.is_none());
    assert_eq!(state.condition_fingerprint, "abc123");
    assert!(state.is_initial);
}

#[test]
fn test_default_fingerprint_never_matches() {
    let default = AssetConditionState::default();
    let real_fp = ConditionNode::eager().fingerprint_hex();
    assert_ne!(default.condition_fingerprint, real_fp);
    assert!(default.condition_fingerprint.is_empty());
}

/// Helper: run the same invalidation logic used by the daemon at startup.
fn run_invalidation(eval_state: &mut ConditionEvalState, conditions: &[(String, ConditionNode)]) {
    for (asset_key, condition) in conditions {
        let current_fp = condition.fingerprint_hex();
        let state = eval_state.assets.entry(asset_key.clone()).or_default();

        if state.condition_fingerprint == current_fp {
            continue;
        }
        state.reset_for_new_tree(current_fp);
    }

    let active: std::collections::HashSet<&str> = conditions.iter().map(|c| c.0.as_str()).collect();
    eval_state
        .assets
        .retain(|k, v| active.contains(k.as_str()) || v.last_materialized_timestamp.is_some());
}

#[test]
fn test_invalidation_on_tree_change() {
    // Simulate: daemon ran with eager(), persisted state, restarted with on_missing()
    let eager = ConditionNode::eager();
    let eager_fp = eager.fingerprint_hex();

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([(
            "asset_a".into(),
            AssetConditionState {
                previous_results: HashMap::from([(0, true), (1, false), (2, true)]),
                dep_previous_results: HashMap::new(),
                dep_baselines: HashMap::new(),
                last_handled_timestamp: Some(5000),
                last_materialized_timestamp: Some(3000),
                last_data_version: None,

                last_tick_timestamp: Some(5000),
                condition_fingerprint: eager_fp,
                is_initial: false,
                partition_state: None,
            },
        )]),
        is_initial: false,
        ..Default::default()
    };

    let on_missing = ConditionNode::on_missing();
    run_invalidation(&mut eval_state, &[("asset_a".into(), on_missing)]);

    let state = &eval_state.assets["asset_a"];
    assert!(
        state.previous_results.is_empty(),
        "previous_results should be cleared"
    );
    assert!(
        state.last_handled_timestamp.is_none(),
        "last_handled should be cleared"
    );
    assert_eq!(
        state.last_materialized_timestamp,
        Some(3000),
        "last_materialized should be preserved"
    );
    assert!(
        state.last_tick_timestamp.is_none(),
        "last_tick should be cleared"
    );
    assert!(state.is_initial, "should be marked as initial");
    assert_eq!(
        state.condition_fingerprint,
        ConditionNode::on_missing().fingerprint_hex(),
        "fingerprint should be updated to new tree"
    );
}

#[test]
fn test_invalidation_noop_on_unchanged_tree() {
    // Simulate: daemon restarts with the same condition tree — state preserved
    let eager = ConditionNode::eager();
    let eager_fp = eager.fingerprint_hex();

    let original_state = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        dep_previous_results: HashMap::new(),
        dep_baselines: HashMap::new(),
        last_handled_timestamp: Some(5000),
        last_materialized_timestamp: Some(3000),
        last_data_version: None,
        last_tick_timestamp: Some(5000),
        condition_fingerprint: eager_fp,
        is_initial: false,
        partition_state: None,
    };

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([("asset_a".into(), original_state.clone())]),
        is_initial: false,
        ..Default::default()
    };

    run_invalidation(
        &mut eval_state,
        &[("asset_a".into(), ConditionNode::eager())],
    );

    let state = &eval_state.assets["asset_a"];
    assert_eq!(state.previous_results, original_state.previous_results);
    assert_eq!(
        state.last_handled_timestamp,
        original_state.last_handled_timestamp
    );
    assert_eq!(
        state.last_materialized_timestamp,
        original_state.last_materialized_timestamp
    );
    assert_eq!(
        state.last_tick_timestamp,
        original_state.last_tick_timestamp
    );
    assert!(!state.is_initial, "should NOT be marked as initial");
}

#[test]
fn test_invalidation_prunes_removed_assets() {
    let eager_fp = ConditionNode::eager().fingerprint_hex();

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([
            (
                "asset_a".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp.clone(),
                    ..Default::default()
                },
            ),
            (
                "asset_b".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp.clone(),
                    ..Default::default()
                },
            ),
            (
                "asset_c".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp,
                    ..Default::default()
                },
            ),
        ]),
        is_initial: false,
        ..Default::default()
    };

    run_invalidation(
        &mut eval_state,
        &[("asset_a".into(), ConditionNode::eager())],
    );

    assert!(
        eval_state.assets.contains_key("asset_a"),
        "active asset should be kept"
    );
    assert!(
        !eval_state.assets.contains_key("asset_b"),
        "removed asset should be pruned"
    );
    assert!(
        !eval_state.assets.contains_key("asset_c"),
        "removed asset should be pruned"
    );
    assert_eq!(eval_state.assets.len(), 1);
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

// ── Cache: in-progress completion detected as change ────────────────────

#[tokio::test]
async fn test_cache_detects_in_progress_completion_as_change() {
    // a → b, both materialized; a run re-materializes a. Tick 1 detects Started (a in-progress);
    // Tick 2 a completes → cache.refresh must return true so b's eval isn't skipped.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    // Register assets
    let rec_a = make_materialized_record("a", 1000);
    let rec_b = make_materialized_record("b", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();

    // Store graph topology so cache knows a → b
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "a".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "b".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("b".to_string(), "a".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    // Initial cache load
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed, "initial load should report changes");

    // Verify baseline: no in-progress assets
    assert!(cache.in_progress_assets.is_empty());

    // Create a Started run for a
    let run_id = "run-1".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    // Tick 1: detect the new run
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        changed,
        "tick 1: new Started run should be detected as change"
    );
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "tick 1: a should be in-progress"
    );

    // Complete the run: update status and a's record with new timestamp
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();

    // Simulate a's materialization updating its record (the executor writes a
    // Materialization event that updates last_timestamp via recompute_staleness).
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("new-dv".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: detect a's completion
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        changed,
        "tick 2: a's run completing should be detected as change. \
         Without this, the eval loop skips and b never fires."
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "tick 2: a should no longer be in-progress"
    );

    // Verify a's record was refreshed with new timestamp
    let a_record = cache.records.get("a").unwrap();
    assert!(
        a_record.last_timestamp.is_some(),
        "a should have updated timestamp in cache"
    );
}

#[tokio::test]
async fn test_cache_keeps_sibling_backfill_runs_in_progress_on_partial_completion() {
    // A backfill registers one run per partition on the same asset; when the first
    // completes, refresh must clear only that run — wiping the whole asset reopens
    // the dispatch gate for still-running siblings.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);

    // Downstream asset with a baseline materialization.
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.is_empty(),
        "baseline: nothing in flight"
    );

    // Backfill dispatch: one Started run per partition (a, b, c), all on `dst`.
    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("backfill".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };
    storage
        .create_runs(&[
            mk_run("run_a", "a"),
            mk_run("run_b", "b"),
            mk_run("run_c", "c"),
        ])
        .await
        .unwrap();

    // Tick 1: all three observed Started → tracked under `dst`.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst should be in-progress after dispatch");
    assert_eq!(tracked.len(), 3, "all three backfill runs are tracked");

    // Partition a finishes: its run flips to Success and its materialization lands (advancing dst's timestamp).
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("dv_a".to_string()),
            },
            asset_key: Some("dst".to_string()),
            run_id: "run_a".to_string(),
            partition_key: Some(part("a")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: a's completion detected; only run_a clears — b and c still running, so dst stays gated.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst must stay in-progress while runs b and c are still running");
    assert!(
        !tracked.contains_key("run_a"),
        "the completed run a should be cleared"
    );
    assert!(
        tracked.contains_key("run_b"),
        "still-running run b must stay tracked"
    );
    assert!(
        tracked.contains_key("run_c"),
        "still-running run c must stay tracked"
    );
}

#[tokio::test]
async fn test_observation_committing_after_load_is_still_seen() {
    // The observation cursor must derive from storage, not wall clock: an
    // observation stamped before daemon start whose write commits only after
    // the initial load must still trigger a refresh (re-processing is an
    // idempotent record re-fetch).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("ext")])
        .await
        .unwrap();

    let mk_obs = |run_id: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation {
            data_version: Some(format!("dv-{ts}")),
        },
        asset_key: Some("ext".to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    // Prior observation history, committed before the load.
    storage
        .store_events(&[mk_obs("obs-1", 1_000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 5_000).await.unwrap(); // wall clock well past the stamp

    // An equal-stamped observation carrying a NEW data version (batched
    // `now`, lagging write) commits only after the load.
    let mut late = mk_obs("obs-2", 1_000);
    late.event_type = EventType::Observation {
        data_version: Some("dv-late".to_string()),
    };
    storage.store_events(&[late]).await.unwrap();

    let changed = cache.refresh(&storage, 6_000).await.unwrap();
    assert!(
        changed,
        "an observation whose write landed after the initial load must still be seen"
    );
    assert_eq!(
        cache
            .records
            .get("ext")
            .and_then(|r| r.last_data_version.as_deref()),
        Some("dv-late"),
        "the late observation's data version must reach the cached record"
    );
}

#[tokio::test]
async fn test_incremental_partition_refresh_keeps_equal_timestamp_partitions() {
    // Materialization events can share one stamped `now`. A partition whose
    // row lands in a later refresh with a timestamp EQUAL to the cache's
    // current max must still be picked up — the incremental cursor has to
    // trail the max like the run cursor does, not query strictly past it.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };
    let mk_event = |run_id: &str, k: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("dst".to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(part(k)),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["dst".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    // Partition a: run + materialization stamped 3000, observed by one refresh.
    storage
        .create_run(&mk_run("run_a", "a", 2000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run_a", "a", 3000)])
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(
        cache.partition_status["dst"].timestamps.get(&part("a")),
        Some(&3000),
        "precondition: partition a's timestamp is cached"
    );

    // Partition b: a separate run whose materialization carries the SAME
    // stamped timestamp, landing in a later refresh.
    storage
        .create_run(&mk_run("run_b", "b", 4000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run_b", RunStatus::Success, Some(5000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run_b", "b", 3000)])
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert_eq!(
        cache.partition_status["dst"].timestamps.get(&part("b")),
        Some(&3000),
        "a partition update equal to the cached max timestamp must not be dropped"
    );
}

#[test]
fn test_clear_predispatch_mark_drops_empty_entry_only() {
    // A multi-partition backfill pre-marks an empty in_progress entry (classify) with no
    // phantom-eviction net; on dispatch failure it must be cleared, but never when real runs exist.
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());

    // Pre-mark (empty placeholder, as classify does) → cleared.
    cache
        .in_progress_assets
        .entry("dst".to_string())
        .or_default();
    assert!(cache.in_progress_assets.contains_key("dst"));
    cache.clear_predispatch_mark("dst");
    assert!(
        !cache.in_progress_assets.contains_key("dst"),
        "empty pre-dispatch placeholder must be cleared"
    );

    // A real run was registered → clear is a no-op (must not wipe live runs).
    cache.register_dispatched_run("dst".to_string(), "run-1".to_string(), 0, None);
    cache.clear_predispatch_mark("dst");
    assert!(
        cache
            .in_progress_assets
            .get("dst")
            .is_some_and(|r| r.contains_key("run-1")),
        "an entry with a real run must NOT be cleared"
    );
}

#[tokio::test]
async fn test_cache_completion_fallback_skips_still_started_sibling_effects() {
    // In the ts-unchanged completion fallback the effects loop must skip still-Started
    // siblings — applying a still-running run's effects would record its incomplete
    // tags as that partition's last-run tags prematurely.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["dst".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("backfill".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![("batch".to_string(), k.to_string())],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };
    storage
        .create_runs(&[mk_run("run_a", "a"), mk_run("run_b", "b")])
        .await
        .unwrap();

    // Tick 1: both observed Started, tracked, no tags recorded yet.
    cache.refresh(&storage, 0).await.unwrap();

    // Partition a finishes (StepSuccess) but dst's timestamp is unchanged (idempotent)
    // → forces the ts-unchanged fallback; run_b stays Started.
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("dst".to_string()),
            run_id: "run_a".to_string(),
            partition_key: Some(part("a")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: fallback fires; run_a's tags recorded for partition a, but run_b still Started → its tags must not be recorded.
    cache.refresh(&storage, 0).await.unwrap();

    let tracked = cache.in_progress_assets.get("dst").unwrap();
    assert!(!tracked.contains_key("run_a"), "completed run a is cleared");
    assert!(tracked.contains_key("run_b"), "still-running run b stays");

    let dst_tags = cache.partition_last_run_tags.get("dst");
    assert!(
        dst_tags.is_some_and(|m| m.contains_key(&part("a"))),
        "partition a (completed) should have recorded tags"
    );
    assert!(
        dst_tags.is_none_or(|m| !m.contains_key(&part("b"))),
        "partition b is still running — its tags must NOT be recorded yet, got: {:?}",
        dst_tags.and_then(|m| m.get(&part("b"))),
    );
}

#[tokio::test]
async fn test_cache_clears_in_progress_when_run_succeeds_but_timestamp_unchanged() {
    // a → b materialized at 1000; a schedule re-materializes a with identical output
    // (last_timestamp unchanged). The in-progress check must also detect the
    // Started→Success status change and clear a, or b never fires.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    // Register assets with same timestamp
    let rec_a = make_materialized_record("a", 1000);
    let rec_b = make_materialized_record("b", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();

    // Store graph topology
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "a".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "b".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("b".to_string(), "a".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    // Initial cache load
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.is_empty());

    // Create a Started run for a
    let run_id = "run-idem".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    // Tick: detect the new run → a is in-progress
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed);
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run completes Success but a's timestamp is not updated (idempotent);
    // the executor writes a StepSuccess event for a.
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::StepSuccess,
            asset_key: Some("a".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    // Deliberately not updating a's last_timestamp (identical output).

    // Next tick: cache detects a's StepSuccess and removes it from in_progress, even though last_timestamp didn't change.
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a should be removed from in_progress_assets when its StepSuccess event exists, \
         even if last_timestamp didn't change. Got in_progress={:?}",
        cache.in_progress_assets,
    );
    assert!(
        changed,
        "cache should report changes when an in-progress asset's step completes"
    );
}

#[tokio::test]
async fn test_step_success_clears_floor_for_lagging_record_in_joint_failed_run() {
    // A joint run R=[x,y] fails on y but x materialized (StepSuccess); with x's record
    // write lagging, the step-completion fallback must treat x as materialized-here
    // (no floor) while y (StepFailure) is floored.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-joint".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));
    assert!(cache.in_progress_assets.contains_key("y"));

    // Run fails: x StepSuccess, y StepFailure; neither record updated this tick (write lag, ts stays 1000).
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("x".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("y".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.failed_assets.contains("x"),
        "x materialized (StepSuccess) in the failed joint run → must not be floored \
         despite the lagging record; got failed_assets={:?}",
        cache.failed_assets,
    );
    assert!(
        cache.failed_assets.contains("y"),
        "y failed (StepFailure) → must be floored"
    );
    assert_eq!(cache.failed_asset_timestamps.get("y"), Some(&3000));
}

#[tokio::test]
async fn test_cache_clears_in_progress_when_run_canceled_after_cursor_advanced() {
    // A run reported once advances the cursor past its start_time; a later CANCEL changes
    // neither the record nor start_time, so get_runs_since (`>`) never re-delivers it.
    // The cache must re-check tracked in-progress runs by id so a missed terminal transition still clears them.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.is_empty());

    // Started run for a → observed, in_progress set, cursor advances to 2000.
    let run_id = "run-cancel".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed);
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run canceled; start_time stays 2000 → the cursor `> 2000` never re-delivers it.
    storage
        .update_run_status(&run_id, RunStatus::Canceled, Some(3000))
        .await
        .unwrap();

    // Next refresh must clear a from in_progress despite the cursor miss.
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a should be cleared from in_progress when its run is canceled, even though \
         the run cursor never re-delivers the cancel. Got in_progress={:?}",
        cache.in_progress_assets,
    );
    // The sweep mutated eval-visible state, so refresh must report a change, or
    // should_skip suppresses evaluation and the un-wedged asset never re-fires.
    assert!(
        changed,
        "a sweep-only terminal transition must report the refresh as changed"
    );
}

#[tokio::test]
async fn test_queued_run_from_scheduler_is_tracked_and_applies_effects() {
    // A run first observed while Queued (schedule/sensor dispatch — never
    // registered via register_dispatched_run) must be tracked as in-flight and
    // its completion effects applied, even though the cursor advances past its
    // immutable start_time on first sight.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-queued".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
            start_time: 2000,
            end_time: None,
            tags: vec![("team".to_string(), "x".to_string())],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "a queued run must suppress in_flight-gated conditions; got {:?}",
        cache.in_progress_assets
    );

    // The run completes; start_time never changes, so only the tracked-run
    // sweep can observe the transition. The materialization event credits the
    // record with the run.
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dvq".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-queued".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "completion must clear in-flight tracking; got {:?}",
        cache.in_progress_assets
    );
    assert!(
        cache
            .last_run_tags
            .get("a")
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "completion effects (run tags) must be applied; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_initial_load_tracks_queued_and_not_started_runs() {
    // Runs alive as Queued/NotStarted at daemon restart must be reloaded into
    // in-flight tracking; loading only Started runs re-dispatches their assets.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
        make_materialized_record("c", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, status: RunStatus, start: i64, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };
    // run-s is the newest, so the seeded cursor sits above run-q / run-n.
    storage
        .create_run(&mk_run("run-q", RunStatus::Queued, 2000, "a"))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-n", RunStatus::NotStarted, 2100, "b"))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-s", RunStatus::Started, 3000, "c"))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    for asset in ["a", "b", "c"] {
        assert!(
            cache.in_progress_assets.contains_key(asset),
            "{asset} has a live run at restart and must be tracked in-flight; got {:?}",
            cache.in_progress_assets
        );
    }

    // The queued run's completion must be observed via the tracked-run sweep.
    storage
        .update_run_status("run-q", RunStatus::Success, Some(4000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "queued run's completion must clear tracking; got {:?}",
        cache.in_progress_assets
    );
    assert!(cache.in_progress_assets.contains_key("b"));
    assert!(cache.in_progress_assets.contains_key("c"));
}

#[tokio::test]
async fn test_foreign_code_location_observations_do_not_clear_in_flight() {
    // Code locations can share one SurrealDB; another location observing a
    // SAME-NAMED asset must not wipe this location's in-flight run tracking.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_x = make_materialized_record("x", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new("cl-a"))
        .register_assets(&[rec_x])
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "run-x".to_string(),
            code_location_id: "cl-a".to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new("cl-a".to_string());
    cache.refresh(&storage, 3_000).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));

    // The OTHER code location observes its own asset named "x".
    storage
        .store_event(&EventRecord {
            code_location_id: "cl-b".to_string(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 4_000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 5_000).await.unwrap();

    assert!(
        cache.in_progress_assets.contains_key("x"),
        "a foreign location's observation must not clear this location's tracking; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_backfill_terminal_clears_predispatch_placeholder() {
    // A backfill-shaped dispatch inserts an empty in-flight placeholder; when
    // the backfill ends without any observed sub-run (e.g. canceled before its
    // first wave) the asset must not stay in-flight forever.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
        DEFAULT_CODE_LOCATION_ID, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_p = make_materialized_record("P", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p])
        .await
        .unwrap();
    storage
        .create_backfill(&BackfillRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "bf1".to_string(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["P".to_string()],
            job_name: None,
            partition_keys: vec![spk("k1"), spk("k2")],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time: 1000,
            end_time: None,
            error: None,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.backfill.assets.contains_key("P"), "precondition");

    // The dispatch path's pre-dispatch placeholder.
    cache.in_progress_assets.entry("P".to_string()).or_default();

    // Canceled before any sub-run was ever observed.
    storage
        .update_backfill_status("bf1", BackfillStatus::Canceled, Some(2000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.backfill.assets.contains_key("P"),
        "terminal backfill untracked"
    );
    assert!(
        !cache.in_progress_assets.contains_key("P"),
        "the empty placeholder must be cleared when the backfill ends; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_joint_partitioned_run_updates_unpartitioned_assets_scalar_tags() {
    // A partition-keyed joint run spanning a partitioned and an unpartitioned
    // asset must write the unpartitioned asset's tags into the SCALAR maps the
    // unpartitioned eval path reads — not only into the partition maps.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_p = make_materialized_record("P", 1000);
    rec_p.last_run_id = Some("run-joint".to_string());
    let mut rec_d = make_materialized_record("D", 1000);
    rec_d.last_run_id = Some("run-joint".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p, rec_d])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["P".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-joint".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![("team".to_string(), "x".to_string())],
            node_names: vec!["P".to_string(), "D".to_string()],
            priority: 0,
            partition_key: Some(spk("2024-01-01")),
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-joint", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .partition_last_run_tags
            .get("P")
            .and_then(|m| m.get(&spk("2024-01-01")))
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "partitioned asset keeps per-partition tags; got {:?}",
        cache.partition_last_run_tags
    );
    assert!(
        cache
            .last_run_tags
            .get("D")
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "unpartitioned asset must get scalar tags; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_failed_run_does_not_clobber_latest_materializing_tags() {
    // LastExecutedWithTags reflects the latest run that MATERIALIZED the
    // asset; a later run that failed without materializing it must not
    // overwrite the tags (and must match what a restart rebuilds).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_a = make_materialized_record("A", 1000);
    rec_a.last_run_id = Some("r1".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, tags: Vec<(String, String)>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags,
        node_names: vec!["A".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    // r1 materializes A with env=prod.
    storage
        .create_run(&mk_run("r1", 2000, vec![("env".into(), "prod".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("r1", RunStatus::Success, Some(2100))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache
            .last_run_tags
            .get("A")
            .is_some_and(|t| t.contains(&("env".to_string(), "prod".to_string()))),
        "precondition: r1's tags recorded"
    );

    // r2 covers A but FAILS without materializing it (record.last_run_id stays r1).
    storage
        .create_run(&mk_run("r2", 3000, vec![("env".into(), "dev".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("r2", RunStatus::Failure, Some(3100))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("A")
            .is_some_and(|t| t.contains(&("env".to_string(), "prod".to_string()))),
        "a failed non-materializing run must not clobber the tags; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_later_finishing_run_keeps_latest_tags() {
    // Overlapping runs: once the later-finishing materializing run's tags are
    // recorded, an earlier-finishing run applied afterwards must not win.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_x = make_materialized_record("X", 1000);
    rec_x.last_run_id = Some("run-a".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_x])
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, tags: Vec<(String, String)>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags,
        node_names: vec!["X".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&mk_run("run-a", 100, vec![("who".into(), "a".into())]))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-b", 200, vec![("who".into(), "b".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // run-a (the run the record credits with the materialization) finishes
    // later (305) and is applied first; run-b (300) applied in a later refresh
    // must not overwrite.
    storage
        .update_run_status("run-a", RunStatus::Success, Some(305))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-b", RunStatus::Success, Some(300))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("X")
            .is_some_and(|t| t.contains(&("who".to_string(), "a".to_string()))),
        "the later-finishing materializing run's tags must win; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_stale_eval_state_with_live_queued_run_does_not_redispatch() {
    // Crash window: a condition fired and its run was durably enqueued, but
    // the daemon died before persisting eval state. On restart with the stale
    // (pre-fire) latches, the live queued run must suppress a re-dispatch.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_record("a"); // missing → eager's missing arm fires
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::eager(),
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(&storage, 1_000).await.unwrap();
    let out = pass1.run(1_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "precondition: eager fires for the missing asset"
    );

    // Dispatch durably enqueued the run; the daemon crashed before
    // set_condition_eval_state, so pass1.eval_state is never persisted.
    storage
        .create_run(&RunRecord {
            run_id: "run-crash".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
            start_time: 2_000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    // Restart with the STALE (pre-fire) eval state.
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 3_000).await.unwrap();
    let out2 = pass2.run(3_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "the live queued run must suppress a duplicate dispatch; got {:?}",
        out2.plan.unpartitioned
    );
}

#[tokio::test]
async fn test_dispatch_failure_preserves_edge_trigger_for_retry() {
    // A fired root's edge-trigger latches (dep baselines, previous_results,
    // handled cursor) must survive a failed dispatch: the tick commits only
    // for assets that actually dispatched, and the failure forces a retry
    // evaluation on the next tick even with no new upstream changes.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );

    // Initial tick seeds the dep baselines; nothing fires.
    pass.refresh_cache(&storage, 2_000).await.unwrap();
    let out = pass.run(2_000, false);
    assert!(out.plan.is_empty(), "initial tick must not fire");

    // raw re-materializes.
    storage
        .create_run(&RunRecord {
            run_id: "run-raw".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2_500,
            end_time: None,
            tags: vec![],
            node_names: vec!["raw".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    storage
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("raw".to_string()),
            run_id: "run-raw".to_string(),
            partition_key: None,
            timestamp: 3_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    pass.refresh_cache(&storage, 4_000).await.unwrap();

    // The dep update fires dst, but its dispatch fails.
    let out = pass.plan_tick(4_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"dst".to_string()),
        "precondition: the dep update must fire dst"
    );
    // Mimic the daemon's failure path on the shared cache…
    pass.cache
        .register_dispatched_run("dst".to_string(), "run-fail".to_string(), 4_000, None);
    pass.cache.clear_dispatched_run("dst", "run-fail");
    // …and commit the tick with dst's dispatch marked failed.
    let failed: HashSet<String> = ["dst".to_string()].into_iter().collect();
    let dirty = pass.commit_tick(&out, &failed, 4_000);
    assert!(
        !dirty,
        "an all-failed tick leaves no latch state to persist"
    );

    assert!(
        !pass.should_skip(false),
        "a failed dispatch must force a retry evaluation even with no new changes"
    );
    let retry = pass.plan_tick(5_000, false);
    assert!(
        retry.plan.unpartitioned.contains(&"dst".to_string()),
        "the un-consumed dep trigger must re-fire on the retry tick"
    );
    let dirty = pass.commit_tick(&retry, &HashSet::new(), 5_000);
    assert!(dirty, "a committed fire consumes latches and must persist");

    assert!(
        pass.should_skip(false),
        "after a successful commit the pass may skip no-change ticks again"
    );
    let done = pass.plan_tick(6_000, false);
    assert!(
        done.plan.is_empty(),
        "the trigger must be consumed exactly once after a successful dispatch; got {:?}",
        done.plan.unpartitioned
    );
    let dirty = pass.commit_tick(&done, &HashSet::new(), 6_000);
    assert!(
        !dirty,
        "a passive tick has nothing latch-bearing to persist"
    );
}

#[tokio::test]
async fn test_initial_evaluation_fires_once_not_per_restart() {
    // Pins initial_evaluation()'s documented semantics: it fires on the very
    // first evaluation tick (fresh eval state) and NOT again after a normal
    // restart with intact persisted state.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("a", 1_000)])
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::InitialEvaluation,
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(&storage, 2_000).await.unwrap();
    let out = pass1.run(2_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "the very first evaluation must fire initial_evaluation()"
    );
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    // Normal restart: intact persisted state, same condition tree.
    let mut eval_state = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("persisted");
    eval_state.migrate_loaded();
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 3_000).await.unwrap();
    let out2 = pass2.run(3_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "a restart with intact persisted state must not re-fire initial_evaluation(); got {:?}",
        out2.plan.unpartitioned
    );
}

#[tokio::test]
async fn test_initial_load_derives_failure_floor_from_run_history() {
    // First-ever daemon start (no persisted eval state to rehydrate from):
    // an asset whose most recent run failed must still be visible to
    // ExecutionFailed — the floor has to come from run history, not only
    // from persisted eval-state. A failure outranked by a newer
    // materialization must NOT floor.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("a"), make_materialized_record("b", 5_000)])
        .await
        .unwrap();

    let mk_run = |id: &str, asset: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };
    // a: failed, never materialized afterwards → floor stands.
    storage
        .create_run(&mk_run("run-fail-a", "a", 2_000))
        .await
        .unwrap();
    storage
        .update_run_status("run-fail-a", RunStatus::Failure, Some(2_500))
        .await
        .unwrap();
    // b: failed at 2_500 but re-materialized at 5_000 → floor cleared.
    storage
        .create_run(&mk_run("run-fail-b", "b", 2_000))
        .await
        .unwrap();
    storage
        .update_run_status("run-fail-b", RunStatus::Failure, Some(2_500))
        .await
        .unwrap();

    let conditions = vec![
        AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::ExecutionFailed,
            partition_info: None,
            backfill_strategy: None,
        },
        AssetConditionInfo {
            asset_key: "b".to_string(),
            condition: ConditionNode::ExecutionFailed,
            partition_info: None,
            backfill_strategy: None,
        },
    ];
    // b also carries a stale PERSISTED floor (from an eval-state snapshot
    // taken before it recovered while the daemon was down) — the load must
    // drop it, not trust it.
    let mut eval_state = ConditionEvalState {
        is_initial: true,
        ..Default::default()
    };
    eval_state.failed_assets.insert("b".to_string(), 2_500);
    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state,
        conditions,
        HashMap::new(),
    );
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "a pre-existing failure must fire ExecutionFailed on first start; got {:?}",
        out.plan.unpartitioned
    );
    assert!(
        !out.plan.unpartitioned.contains(&"b".to_string()),
        "a failure outranked by a newer materialization must not fire, even when \
         a stale persisted floor rehydrated it"
    );
}

#[tokio::test]
async fn test_crash_after_dispatch_recovers_latches_from_intent() {
    // Crash window: a tick's run was durably dispatched (and even completed),
    // but the daemon died before persisting eval state. The pre-dispatch
    // intent must replay the consumed latches on restart so the tick's
    // trigger doesn't re-fire and double-materialize.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass, recover_pending_dispatch};
    use crate::condition::state::PendingDispatch;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }]
    };
    let mk_run = |id: &str, asset: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Condition,
    };
    let mk_event = |run_id: &str, asset: &str, dv: &str, ts: i64| crate::storage::EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: crate::storage::EventType::Materialization {
            data_version: Some(dv.to_string()),
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(storage.as_ref(), 2_000).await.unwrap();
    let out0 = pass1.run(2_000, false);
    assert!(out0.plan.is_empty(), "initial tick must not fire");
    // The pre-fire eval state is what a restart will load.
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    // raw's data version changes → dst fires.
    storage
        .create_run(&mk_run("run-raw", "raw", 2_500))
        .await
        .unwrap();
    storage
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run-raw", "raw", "v2", 3_000)])
        .await
        .unwrap();
    pass1.refresh_cache(storage.as_ref(), 4_000).await.unwrap();
    let out = pass1.plan_tick(4_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"dst".to_string()),
        "precondition: the dep dv change must fire dst"
    );

    // The engine persists the intent, then dispatch goes out durably and the
    // run even completes, materializing dst…
    let pending = PendingDispatch {
        tick_timestamp: 4_000,
        entries: pass1
            .pending_dispatch_states(&out, 4_000)
            .into_iter()
            .map(
                |(asset_key, committed)| crate::condition::state::PendingDispatchEntry {
                    asset_key,
                    run_ids: vec!["run-dst".to_string()],
                    committed,
                },
            )
            .collect(),
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-dst", "dst", 4_500))
        .await
        .unwrap();
    storage
        .update_run_status("run-dst", RunStatus::Success, Some(5_000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run-dst", "dst", "dst-v2", 5_000)])
        .await
        .unwrap();
    // …and the daemon dies before set_condition_eval_state. pass1 is gone.

    // Restart: load the STALE eval state, recover from the intent.
    let mut eval_state2 = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("pre-fire state was persisted");
    eval_state2.migrate_loaded();
    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state2, &handle)
        .await
        .unwrap();

    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state2,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(storage.as_ref(), 6_000).await.unwrap();
    let out2 = pass2.plan_tick(6_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "recovered latches must suppress the replayed fire; got {:?}",
        out2.plan.unpartitioned
    );

    let cleared = storage
        .for_code_location(&ctx)
        .get_condition_pending_dispatch()
        .await
        .unwrap()
        .unwrap_or_default();
    assert!(
        cleared.entries.is_empty(),
        "the intent must be cleared after recovery"
    );
}

#[tokio::test]
async fn test_crash_before_dispatch_leaves_trigger_armed() {
    // The symmetric case: the intent was written but the run never reached
    // storage (crash before dispatch). Recovery must NOT consume the latches
    // — the next tick re-fires as the retry.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass, recover_pending_dispatch};
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(storage.as_ref(), 2_000).await.unwrap();
    pass1.run(2_000, false);
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-raw".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2_500,
            end_time: None,
            tags: vec![],
            node_names: vec!["raw".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    storage
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("raw".to_string()),
            run_id: "run-raw".to_string(),
            partition_key: None,
            timestamp: 3_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    pass1.refresh_cache(storage.as_ref(), 4_000).await.unwrap();
    let out = pass1.plan_tick(4_000, false);
    assert!(out.plan.unpartitioned.contains(&"dst".to_string()));

    // Intent written; the run never reached storage (crash before dispatch).
    let pending = PendingDispatch {
        tick_timestamp: 4_000,
        entries: pass1
            .pending_dispatch_states(&out, 4_000)
            .into_iter()
            .map(|(asset_key, committed)| PendingDispatchEntry {
                asset_key,
                run_ids: vec!["run-never-created".to_string()],
                committed,
            })
            .collect(),
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    let mut eval_state2 = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("pre-fire state was persisted");
    eval_state2.migrate_loaded();
    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state2, &handle)
        .await
        .unwrap();

    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state2,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(storage.as_ref(), 6_000).await.unwrap();
    let out2 = pass2.plan_tick(6_000, false);
    assert!(
        out2.plan.unpartitioned.contains(&"dst".to_string()),
        "a dispatch that never happened must stay armed and re-fire"
    );
}

#[tokio::test]
async fn test_restart_does_not_replay_newest_run_tick_tags() {
    // The newest pre-restart run must not repopulate the tick-scoped tag
    // accumulators on the first steady refresh — HasRunWithTags would report
    // a days-old run as "completed this tick" and spuriously fire.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "run-old".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![("team".to_string(), "x".to_string())],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("team".to_string(), "x".to_string())],
    }]);
    cache.refresh(&storage, 4000).await.unwrap();
    cache.refresh(&storage, 4001).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty(),
        "a pre-restart run must not be reported as completed this tick; got {:?}",
        cache.tick_materialization_tags
    );
}

#[tokio::test]
async fn test_same_timestamp_run_committed_after_refresh_is_seen() {
    // Dispatchers stamp one `now` across a batch committed record-by-record;
    // a refresh landing mid-batch must not permanently lose the runs that
    // commit afterward with the same start_time.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![("team".to_string(), "x".to_string())],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("team".to_string(), "x".to_string())],
    }]);
    cache.refresh(&storage, 0).await.unwrap();

    storage.create_run(&mk_run("run-1", "a")).await.unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("a"));

    // run-2 commits after the refresh with the SAME start_time.
    storage.create_run(&mk_run("run-2", "b")).await.unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("b"),
        "a same-timestamp run committed after the refresh must still be seen; got {:?}",
        cache.in_progress_assets
    );

    // Completion effects apply exactly once despite re-delivery.
    storage
        .update_run_status("run-1", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(!cache.in_progress_assets.contains_key("a"));
    let first_report = cache.tick_materialization_tags.clone();
    assert!(
        !first_report.is_empty(),
        "completion reports tick tags once"
    );
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty(),
        "re-delivered runs must not double-report tick tags; got {:?}",
        cache.tick_materialization_tags
    );
}

#[tokio::test]
async fn test_failure_floor_survives_daemon_restart() {
    // ExecutionFailed state is maintained by the steady-state refresh only;
    // it must survive a restart via the persisted eval state or failed assets
    // silently auto-retry after every daemon restart.
    use crate::condition::pass::ConditionPass;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    };

    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        vec![],
        HashMap::new(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();

    // a's run fails without materializing it → floor set by the steady path.
    storage
        .create_run(&mk_run("run-fail", 2000, "a"))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-fail", RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    // A later unrelated run makes a's failure non-newest.
    storage
        .create_run(&mk_run("run-b", 4000, "b"))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    assert!(
        pass.cache.failed_assets.contains("a"),
        "precondition: floor set"
    );

    pass.run(5000, false);

    // Restart: fresh cache, persisted eval state.
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        pass.eval_state.clone(),
        vec![],
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 6000).await.unwrap();
    assert!(
        pass2.cache.failed_assets.contains("a"),
        "failure floor must survive a daemon restart; got {:?}",
        pass2.cache.failed_assets
    );
    assert_eq!(pass2.cache.failed_asset_timestamps.get("a"), Some(&3000));
}

#[tokio::test]
async fn test_initial_load_seeds_observation_cursor() {
    // Historical observation events must not be replayed by the first
    // steady-state refresh — the replay's AssetClear wipes live Started-run
    // tracking established at initial_load.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_x = make_materialized_record("x", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_x])
        .await
        .unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 500,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-x".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 3_000).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));

    cache.refresh(&storage, 3_001).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("x"),
        "a historical observation must not wipe live in-flight tracking; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_clearable_sweep_sets_failure_floor_on_missed_terminal_failure() {
    // A Started run reported once fails with no materialization and no StepFailure event;
    // only the clearable sweep catches it. That sweep must also set the failure floor,
    // or eager/on_missing re-dispatches the failing run every tick.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    // Started run for a → observed, in_progress set, cursor advances to 2000.
    let run_id = "run-fail".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run fails; start_time stays 2000, no StepFailure event → only the clearable sweep sees the failure.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a must be cleared from in_progress after the failure"
    );
    assert!(
        cache.failed_assets.contains("a"),
        "the clearable sweep must set the failure floor for a terminal failure \
         caught only there; got failed_assets={:?}",
        cache.failed_assets,
    );
    assert_eq!(cache.failed_asset_timestamps.get("a"), Some(&3000));
}

#[tokio::test]
async fn test_clearable_sweep_records_partitioned_failure_in_partition_status() {
    // Partitioned sibling of the sweep-floor case: a partitioned run fails with no
    // StepFailure event or record change; only the clearable sweep sees it. Since the
    // asset-level floor isn't set for partitioned runs, the failure must land in partition_status.failed.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec_p = make_materialized_record("p", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["p".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-part-fail".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["p".to_string()],
            priority: 0,
            partition_key: Some(spk("2024-01-01")),
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("p"));

    // Run fails; start_time stays 2000, no StepFailure event → only the clearable sweep sees the terminal transition.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("p"),
        "p must be cleared from in_progress after the failure"
    );
    let status = cache
        .partition_status
        .get("p")
        .expect("p is a registered partitioned asset");
    assert!(
        status.failed.contains(&spk("2024-01-01")),
        "a partitioned terminal failure caught only by the sweep must surface in \
         partition_status.failed; got failed={:?}",
        status.failed,
    );
    assert!(
        status.failed_timestamps.contains_key(&spk("2024-01-01")),
        "the failed partition needs a floor timestamp for root_floor comparisons"
    );
    assert!(
        !cache.failed_assets.contains("p"),
        "the asset-level floor stays scoped to unpartitioned runs"
    );
}

#[tokio::test]
async fn test_queued_run_is_not_cleared_by_sweep() {
    // A run dispatched in run_queue mode is written as Queued; the clearable sweep must
    // not treat Queued as terminal, or eager/on_missing re-enqueues a duplicate every
    // tick. Only Success/Failure/Canceled are terminal.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    // Dispatch registers the asset; the queued dispatcher writes a Queued record.
    let run_id = "run-queued".to_string();
    cache.register_dispatched_run("a".to_string(), run_id.clone(), 0, None);
    assert!(cache.in_progress_assets.contains_key("a"));
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    // Refresh must keep `a` gated — its run is still queued, not terminal.
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "a queued run must keep the asset in_progress; got in_progress={:?}",
        cache.in_progress_assets,
    );
}

#[tokio::test]
async fn test_cache_does_not_store_empty_run_tags() {
    // The cache must not store empty tag vecs: an empty entry makes run_tags_match(&[],&[],&[])
    // vacuously true, so a no-arg LastExecutedWithTags would fire on every asset with any completed run.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.last_run_tags.is_empty());

    // Create a completed run with no tags (arrives Success, a fast run completing between ticks).
    storage
        .create_run(&RunRecord {
            run_id: "run-no-tags".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    // The materialization event credits the record with the run.
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv1".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-no-tags".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // Cache must NOT have an entry for "a" — empty tags should be skipped
    assert!(
        !cache.last_run_tags.contains_key("a"),
        "cache should not store empty tags; got: {:?}",
        cache.last_run_tags.get("a"),
    );

    // Now create a run with tags arriving already-completed (Success), a fast run between ticks.
    storage
        .create_run(&RunRecord {
            run_id: "run-tagged".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 4000,
            end_time: Some(5000),
            tags: vec![("env".to_string(), "prod".to_string())],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv2".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-tagged".to_string(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert_eq!(
        cache.last_run_tags.get("a"),
        Some(&Arc::from(vec![("env".to_string(), "prod".to_string())])),
    );
}

#[tokio::test]
async fn test_cache_tick_materialization_tags() {
    // Verify that tick_materialization_tags is populated on refresh and cleared on next refresh.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    }]);
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.tick_materialization_tags.is_empty());

    // Create a completed run with tags
    storage
        .create_run(&RunRecord {
            run_id: "run-tagged".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![("env".to_string(), "prod".to_string())],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // tick_materialization_tags should have the run's tags
    assert_eq!(
        cache.tick_materialization_tags.get("a"),
        Some(&vec![Arc::from(vec![(
            "env".to_string(),
            "prod".to_string()
        )])]),
    );

    // On next refresh with no new runs, tick tags should be cleared
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty()
            || !cache.tick_materialization_tags.contains_key("a"),
        "tick_materialization_tags should be cleared on next refresh"
    );
}

#[tokio::test]
async fn test_cache_tick_materialization_tags_includes_empty_tags() {
    // A run with no tags is still recorded (empty vec) so AllRunsHaveTags returns false.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    }]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-no-tags".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // tick_materialization_tags should have an entry with empty tags
    let tick_tags = cache.tick_materialization_tags.get("a");
    assert!(
        tick_tags.is_some(),
        "should record materializations even with empty tags"
    );
    assert_eq!(tick_tags.unwrap(), &vec![Arc::from(vec![])]);
}

#[tokio::test]
async fn test_step_completion_sql_query() {
    // Direct test of the step_completion event scan against SurrealDB.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let run_id = "run-query-test".to_string();
    let other_run = "run-other".to_string();

    // Write events for asset "a" in run-query-test
    storage
        .store_events(&[
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv1".to_string()),
                },
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 2000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();

    // Write StepSuccess for different asset "b" in same run
    storage
        .store_events(&[EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("b".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Write StepSuccess for "a" in a DIFFERENT run
    storage
        .store_events(&[EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("a".to_string()),
            run_id: other_run.clone(),
            partition_key: None,
            timestamp: 4000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Test 1: asset "a" in run-query-test → true
    assert!(
        storage
            .step_completion("a", std::slice::from_ref(&run_id))
            .await
            .unwrap()
            .0,
        "should find StepSuccess for 'a' in run-query-test"
    );

    // Test 2: asset "b" in run-query-test → true
    assert!(
        storage
            .step_completion("b", std::slice::from_ref(&run_id))
            .await
            .unwrap()
            .0,
        "should find StepSuccess for 'b' in run-query-test"
    );

    // Test 3: asset "a" in other-run → true
    assert!(
        storage
            .step_completion("a", std::slice::from_ref(&other_run))
            .await
            .unwrap()
            .0,
        "should find StepSuccess for 'a' in run-other"
    );

    // Test 4: asset "c" (doesn't exist) → false
    assert!(
        !storage
            .step_completion("c", std::slice::from_ref(&run_id))
            .await
            .unwrap()
            .0,
        "should NOT find StepSuccess for 'c'"
    );

    // Test 5: asset "a" in non-existent run → false
    assert!(
        !storage
            .step_completion("a", &["run-nonexistent".to_string()])
            .await
            .unwrap()
            .0,
        "should NOT find StepSuccess in non-existent run"
    );

    // Test 6: asset "a" in multiple run_ids → true (matches first)
    assert!(
        storage
            .step_completion("a", &[run_id.clone(), other_run.clone()])
            .await
            .unwrap()
            .0,
        "should find StepSuccess for 'a' across multiple run_ids"
    );

    // Test 7: asset "b" in other_run only → false (b only has events in run_id)
    assert!(
        !storage
            .step_completion("b", std::slice::from_ref(&other_run))
            .await
            .unwrap()
            .0,
        "should NOT find StepSuccess for 'b' in run-other"
    );
}

#[tokio::test]
async fn test_step_completion_single_pass() {
    // One storage call answers both "did any step complete" and "which run succeeded"
    // (avoids the per-run N+1 during backfills).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ev = |run: &str, asset: &str, event_type: EventType| EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: run.to_string(),
        partition_key: None,
        timestamp: 1000,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage
        .store_events(&[
            ev("run-fail", "a", EventType::StepFailure),
            ev("run-ok", "a", EventType::StepSuccess),
            ev("run-ok", "b", EventType::StepFailure),
        ])
        .await
        .unwrap();

    let runs = vec!["run-fail".to_string(), "run-ok".to_string()];
    let (completed, succeeded) = storage.step_completion("a", &runs).await.unwrap();
    assert!(completed, "a completed a step in the given runs");
    assert_eq!(
        succeeded,
        vec!["run-ok".to_string()],
        "every succeeding run must be identified"
    );

    let (completed, succeeded) = storage.step_completion("b", &runs).await.unwrap();
    assert!(completed, "a failure is still a completion");
    assert!(
        succeeded.is_empty(),
        "no success run for a failed-only asset"
    );

    let (completed, succeeded) = storage.step_completion("c", &runs).await.unwrap();
    assert!(!completed, "no events for 'c' in the given runs");
    assert!(succeeded.is_empty());
}

// ── Partition-aware tests ───────────────────────────────────────────────

/// Owned partition data for tests. Build this, then borrow from it to create PartitionEvalContext.
struct OwnedPartitionData {
    all_keys: HashSet<PartitionKey>,
    in_progress: HashSet<PartitionKey>,
    failed: HashSet<PartitionKey>,
    timestamps: HashMap<PartitionKey, i64>,
    all_partition_statuses: HashMap<String, crate::condition::cache::PartitionStatusEntry>,
}

impl OwnedPartitionData {
    fn new(all_keys: &[&str], materialized: &[&str], timestamps: &[(&str, i64)]) -> Self {
        // Materialized == has a timestamp (the cache keeps them in lockstep);
        // keys listed only in `materialized` get a placeholder ts.
        let mut ts: HashMap<PartitionKey, i64> =
            timestamps.iter().map(|(k, v)| (spk(k), *v)).collect();
        for k in materialized {
            ts.entry(spk(k)).or_insert(1);
        }
        Self {
            all_keys: all_keys.iter().map(|s| spk(s)).collect(),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            timestamps: ts,
            all_partition_statuses: HashMap::new(),
        }
    }

    fn as_eval_ctx(&self) -> PartitionEvalContext<'_> {
        PartitionEvalContext {
            all_keys: &self.all_keys,
            in_progress: &self.in_progress,
            failed: &self.failed,
            timestamps: &self.timestamps,
            resolver: PartitionResolver::empty(),
            time_windows: None,
            all_partition_statuses: &self.all_partition_statuses,
            dep_root_floor: None,
        }
    }
}

fn make_partitioned_ctx<'a>(
    target_key: &'a str,
    target_record: &'a AssetRecord,
    records: &'a HashMap<String, AssetRecord>,
    upstream_deps: &'a HashMap<String, Vec<String>>,
    pctx: &'a PartitionEvalContext<'a>,
) -> EvalContext<'a> {
    EvalContext {
        target_key,
        root_key: target_key,
        target_record,
        cache: CacheSnapshot {
            records,
            upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(pctx),
        root_partition_floor: None,
    }
}

#[test]
fn test_partition_selection_union() {
    let a = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    let b = PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]));
    assert_eq!(
        a.union(&b),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2"), spk("p3")]))
    );

    assert_eq!(a.union(&PartitionSelection::All), PartitionSelection::All);
    assert_eq!(a.union(&PartitionSelection::Empty), a);
    assert_eq!(
        PartitionSelection::Empty.union(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
}

#[test]
fn test_partition_selection_intersect() {
    let a = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    let b = PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]));
    assert_eq!(
        a.intersect(&b),
        PartitionSelection::Keys(HashSet::from([spk("p2")]))
    );

    assert_eq!(a.intersect(&PartitionSelection::All), a);
    assert_eq!(
        a.intersect(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
}

#[test]
fn test_partition_selection_complement() {
    let universe: HashSet<PartitionKey> = ["p1", "p2", "p3"].iter().map(|s| spk(s)).collect();
    let a = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    assert_eq!(
        a.complement(&universe),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );

    assert_eq!(
        PartitionSelection::All.complement(&universe),
        PartitionSelection::Empty
    );
    assert_eq!(
        PartitionSelection::Empty.complement(&universe),
        PartitionSelection::Keys(universe.clone())
    );
}

#[test]
fn test_partition_selection_complement_empty_universe() {
    // With no partitions, the complement of anything is nothing (not `All`, which would falsely report firing).
    let empty: HashSet<PartitionKey> = HashSet::new();
    assert_eq!(
        PartitionSelection::Empty.complement(&empty),
        PartitionSelection::Empty
    );
    assert_eq!(
        PartitionSelection::All.complement(&empty),
        PartitionSelection::Empty
    );
}

#[test]
fn test_partition_selection_difference() {
    let universe: HashSet<PartitionKey> = ["p1", "p2", "p3"].iter().map(|s| spk(s)).collect();
    let a = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2"), spk("p3")]));
    let b = PartitionSelection::Keys(HashSet::from([spk("p2")]));
    assert_eq!(
        a.difference(&b, &universe),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p3")]))
    );

    assert_eq!(
        a.difference(&PartitionSelection::All, &universe),
        PartitionSelection::Empty
    );
    assert_eq!(a.difference(&PartitionSelection::Empty, &universe), a);
    assert_eq!(
        PartitionSelection::Empty.difference(&b, &universe),
        PartitionSelection::Empty
    );
}

/// `time_window(offset)` must shift in condition eval exactly as the runtime IO
/// path does, or it fires the partition that never read the updated upstream.
#[test]
fn test_time_window_mapping_shifts_selections_by_offset() {
    use crate::timegrid::TimeGrid;
    let grid = TimeGrid {
        cron_schedule: Some("0 0 * * *".into()),
        interval_seconds: None,
        start: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
        end: Some(chrono::NaiveDate::from_ymd_opt(2024, 2, 1).unwrap().into()),
        fmt: "%Y-%m-%d".into(),
    };
    let m = PartitionMappingKind::TimeWindow {
        offset: -1,
        grid: Some(grid),
    };

    let d = PartitionSelection::Keys(HashSet::from([spk("2024-01-05")]));
    // Upstream 2024-01-05 updating affects downstream 2024-01-06.
    assert_eq!(
        m.map_to_downstream(&d),
        PartitionSelection::Keys(HashSet::from([spk("2024-01-06")]))
    );
    // A shift outside [start, end) has no counterpart partition.
    let last = PartitionSelection::Keys(HashSet::from([spk("2024-01-31")]));
    assert_eq!(m.map_to_downstream(&last), PartitionSelection::Empty);
}

/// Mappings serialized before the grid existed degrade to pass-through.
#[test]
fn test_time_window_mapping_without_grid_passes_through() {
    let m = PartitionMappingKind::TimeWindow {
        offset: -1,
        grid: None,
    };
    let sel = PartitionSelection::Keys(HashSet::from([spk("2024-01-05")]));
    assert_eq!(m.map_to_downstream(&sel), sel);
}

/// `All - Keys` must resolve to the complement, not fall back to `All` (which would
/// re-select the dropped keys, e.g. handled keys in newly_requested().since_last_handled()).
#[test]
fn test_partition_selection_difference_all_minus_keys() {
    let universe: HashSet<PartitionKey> = ["p1", "p2", "p3"].iter().map(|s| spk(s)).collect();
    let handled = PartitionSelection::Keys(HashSet::from([spk("p2")]));
    assert_eq!(
        PartitionSelection::All.difference(&handled, &universe),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p3")]))
    );
}

#[test]
fn test_partition_selection_from_to_bool() {
    assert_eq!(PartitionSelection::from_bool(true), PartitionSelection::All);
    assert_eq!(
        PartitionSelection::from_bool(false),
        PartitionSelection::Empty
    );
    assert!(PartitionSelection::All.to_bool());
    assert!(!PartitionSelection::Empty.to_bool());
    assert!(PartitionSelection::Keys(HashSet::from([spk("p1")])).to_bool());
    assert!(!PartitionSelection::Keys(HashSet::new()).to_bool());
}

#[test]
fn test_partitioned_missing() {
    // 3 partitions, only p1 materialized → p2, p3 missing
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_partitioned_missing_all_materialized() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2"], &["p1", "p2"], &[("p1", 100), ("p2", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(!result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::Empty);
}

#[test]
fn test_partitioned_in_latest_time_window_selects_recent_keys() {
    let empty_partition_statuses = HashMap::new();
    // 5 daily partitions; a 1-day lookback selects the latest two.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(
        &[
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ],
        &["2020-01-01", "2020-01-02"],
        &[("2020-01-01", 100), ("2020-01-02", 100)],
    );
    let fmts = HashMap::from([(
        "a".to_string(),
        TimeWindowSource {
            fmt: "%Y-%m-%d".to_string(),
            grid: None,
        },
    )]);
    let now_local = chrono::NaiveDate::from_ymd_opt(2020, 1, 5)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let tw = TimeWindowResolver::new(&fmts, now_local);
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: Some(&tw),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(86_400.0),
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("2020-01-04"), spk("2020-01-05")]))
    );
}

#[test]
fn test_partitioned_in_latest_time_window_empty_when_no_recent() {
    let empty_partition_statuses = HashMap::new();
    // All partitions are in the future relative to `now` → nothing selected.
    let record = make_record("a");
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["2020-02-01", "2020-02-02", "2020-02-03"], &[], &[]);
    let fmts = HashMap::from([(
        "a".to_string(),
        TimeWindowSource {
            fmt: "%Y-%m-%d".to_string(),
            grid: None,
        },
    )]);
    let now_local = chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let tw = TimeWindowResolver::new(&fmts, now_local);
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: Some(&tw),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(86400.0),
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::Empty);
}

#[test]
fn test_partitioned_in_latest_time_window_static_partitions_selects_none() {
    // Static (non-time) partitions have no latest window: the filter selects
    // nothing instead of silently selecting every partition.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["us", "eu", "ap"], &["us"], &[("us", 100)]);
    let fmts: HashMap<String, TimeWindowSource> = HashMap::new(); // "a" is not time-partitioned
    let now_local = chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let tw = TimeWindowResolver::new(&fmts, now_local);
    let empty_partition_statuses = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: Some(&tw),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::Empty);
}

#[test]
fn test_partitioned_in_latest_time_window_combined_with_missing() {
    let empty_partition_statuses = HashMap::new();
    // InLatestTimeWindow(1d) & Missing over 5 daily partitions (01 materialized):
    // Missing={02..05} ∩ latest={04,05} → {04,05}.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(
        &[
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ],
        &["2020-01-01"],
        &[("2020-01-01", 100)],
    );
    let fmts = HashMap::from([(
        "a".to_string(),
        TimeWindowSource {
            fmt: "%Y-%m-%d".to_string(),
            grid: None,
        },
    )]);
    let now_local = chrono::NaiveDate::from_ymd_opt(2020, 1, 5)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let tw = TimeWindowResolver::new(&fmts, now_local);
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: Some(&tw),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::And(vec![
        ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(86_400.0),
        },
        ConditionNode::Missing,
    ]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("2020-01-04"), spk("2020-01-05")]))
    );
}

#[test]
fn test_partitioned_in_progress() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let mut pdata = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 100), ("p2", 100), ("p3", 100)],
    );
    pdata.in_progress = HashSet::from([spk("p2")]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::InProgress, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2")]))
    );
}

#[test]
fn test_partitioned_execution_failed() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let mut pdata = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2"],
        &[("p1", 100), ("p2", 100)],
    );
    pdata.failed = HashSet::from([spk("p3")]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::ExecutionFailed, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p3")]))
    );
}

#[test]
fn test_partitioned_code_version_changed() {
    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v2".into());
    record.last_materialization_code_version = Some("v1".into());
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2"], &["p1", "p2"], &[("p1", 100), ("p2", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::CodeVersionChanged, &ctx);
    assert!(result.fired);
    // Code version change affects ALL partitions (the All sentinel).
    assert_eq!(result.selection.unwrap(), PartitionSelection::All);
}

#[test]
fn test_partitioned_newly_updated() {
    let record = make_materialized_record("a", 200);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p2", "p3"],
        &[("p1", 100), ("p2", 200), ("p3", 200)],
    );
    // Previous state: p1=100, p2=100 (so p2 is updated), p3 not tracked (newly appeared)
    let prev = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let pctx = pdata.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let result = evaluate(&ConditionNode::NewlyUpdated, &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    // p2 updated (200 > 100), p3 newly appeared (no prev), p1 unchanged
    match &sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&spk("p2")), "p2 should be updated");
            assert!(keys.contains(&spk("p3")), "p3 should be newly appeared");
            assert!(!keys.contains(&spk("p1")), "p1 should not be updated");
        }
        _ => panic!("expected Keys, got {:?}", sel),
    }
}

#[test]
fn test_partitioned_newly_updated_suppressed_on_initial_tick() {
    // On the initial tick, pre-existing partitions with no baseline must not count as
    // newly updated (mirror of the unpartitioned guard).
    let record = make_materialized_record("a", 200);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2"], &["p1", "p2"], &[("p1", 100), ("p2", 200)]);
    let prev = AssetConditionState::default(); // no partition_state → no baselines
    let pctx = pdata.as_eval_ctx();
    let mut ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&ConditionNode::NewlyUpdated, &ctx);
    assert!(
        !result.fired,
        "initial tick: pre-existing partitions must not be newly updated, got {:?}",
        result.selection
    );

    // On a non-initial tick the same baseline-less partitions appeared between ticks and do fire.
    ctx.is_initial = false;
    let result2 = evaluate(&ConditionNode::NewlyUpdated, &ctx);
    assert!(
        result2.fired,
        "non-initial tick: baseline-less partitions appeared between ticks and should fire"
    );
}

#[test]
fn test_partitioned_and() {
    // And(Missing, Not(InProgress)); p1 materialized, p2 missing, p3 missing+in_progress.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let mut pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    pdata.in_progress = HashSet::from([spk("p3")]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::Not(Box::new(ConditionNode::InProgress)),
    ]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    // Missing = {p2, p3}, Not(InProgress) = {p1, p2}, intersection = {p2}
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2")]))
    );
}

#[test]
fn test_partitioned_or() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let mut pdata = OwnedPartitionData::new(
        &["p1", "p2", "p3"],
        &["p1", "p3"],
        &[("p1", 100), ("p3", 100)],
    );
    pdata.failed = HashSet::from([spk("p1")]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    // Or(Missing, ExecutionFailed) → {p2} ∪ {p1} = {p1, p2}
    let cond = ConditionNode::Or(vec![ConditionNode::Missing, ConditionNode::ExecutionFailed]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]))
    );
}

#[test]
fn test_partitioned_not() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    // Not(Missing) → complement of {p2, p3} = {p1}
    let cond = ConditionNode::Not(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1")]))
    );
}

#[test]
fn test_partitioned_not_over_empty_universe_does_not_fire() {
    // A partitioned asset with an empty universe must not report fired for a Not(...) clause.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&[], &[], &[]); // empty universe
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::Not(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "Not(...) over an empty partition universe must not fire, got {:?}",
        result.selection
    );
}

#[test]
fn test_partitioned_newly_true() {
    // NewlyTrue(Missing): fires for partitions that became missing this tick
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);

    // First tick (initial): NewlyTrue fires for all currently-true partitions
    let pctx = pdata.as_eval_ctx();
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    assert_eq!(
        sel,
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );

    // Second tick: Missing is still {p2, p3}, previous was {p2, p3} → NewlyTrue = Empty
    let prev = AssetConditionState {
        partition_state: Some(PartitionState {
            // node 0 = NewlyTrue, stores inner (Missing) result
            previous_selections: HashMap::from([(
                0,
                PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")])),
            )]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let pctx2 = pdata.as_eval_ctx();
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let result2 = evaluate(&cond, &ctx2);
    assert!(!result2.fired);
    assert_eq!(result2.selection.unwrap(), PartitionSelection::Empty);
}

#[test]
fn test_partitioned_since() {
    // Since { trigger: Missing, reset: NewlyUpdated }
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::Since {
        trigger: Box::new(ConditionNode::Missing),
        reset: Box::new(ConditionNode::NewlyUpdated),
    };

    // Tick 1: trigger = {p2, p3}, reset = Empty → {p2, p3}
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.as_ref().unwrap(),
        &PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_partitioned_any_deps_missing_with_identity() {
    // b depends on a (3 partitions, all materialized, identity mapping); AnyDepsMissing on b must not fire.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2"), spk("p3")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let a_state = AssetConditionState {
        ..Default::default()
    };
    let asset_states = HashMap::from([("a".into(), a_state)]);

    // Upstream "a" partition status: all 3 partitions materialized
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100), (spk("p3"), 100)]),
        },
    )]);

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _ip = HashSet::new();
    let _fail = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100), (spk("p2"), 100), (spk("p3"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    // AnyDepsMissing evaluates Missing on upstream a (all keys materialized via the resolver), so the result is empty.
    let result = evaluate(&ConditionNode::any_deps_missing(), &ctx);
    // a isn't Missing and eval_partitioned_on_dep builds a pctx with all upstream keys materialized → no missing partitions.
    assert!(!result.fired);
}

#[test]
fn test_partitioned_all_deps_match_not_missing() {
    // b depends on a (both partitioned), a fully materialized → AllDepsMatch(Not(Missing)) on b holds.
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // b is missing
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let asset_states = HashMap::from([("a".into(), AssetConditionState::default())]);

    // Upstream "a" partition status: both partitions materialized
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
        },
    )]);

    let _ak2 = HashSet::from([spk("p1"), spk("p2")]);
    let _mat2: HashSet<PartitionKey> = HashSet::new();
    let _ip2: HashSet<PartitionKey> = HashSet::new();
    let _fail2: HashSet<PartitionKey> = HashSet::new();
    let _ts2: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &_ak2,
        in_progress: &_ip2,
        failed: &_fail2,
        timestamps: &_ts2,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let cond = ConditionNode::all_deps_match(!ConditionNode::Missing);
    let result = evaluate(&cond, &ctx);
    // a is materialized (not missing), so Not(Missing) is true for all partitions
    assert!(result.fired);
}

#[test]
fn test_partition_mapping_identity() {
    let m = PartitionMappingKind::Identity;
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_all_partitions() {
    let m = PartitionMappingKind::AllPartitions;
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    // map_to_downstream: any upstream change affects all downstream
    assert_eq!(m.map_to_downstream(&sel), PartitionSelection::All);
    // Empty input → Empty
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
}

#[test]
fn test_partition_mapping_static() {
    let m = PartitionMappingKind::Static {
        mapping: HashMap::from([("d1".into(), "u1".into()), ("d2".into(), "u2".into())]),
    };
    // Upstream u2 maps to downstream d2 plus its identity image u2 (a downstream key named
    // u2 forward-reads upstream u2); phantoms filtered against the universe.
    let sel2 = PartitionSelection::Keys(HashSet::from([spk("u2")]));
    assert_eq!(
        m.map_to_downstream(&sel2),
        PartitionSelection::Keys(HashSet::from([spk("d2"), spk("u2")]))
    );
}

#[test]
fn test_partition_mapping_static_identity_fallback() {
    // Partial Static map (d1 explicit, d2 unmapped): the runtime reads upstream d2 for
    // unmapped downstream d2 via identity, so an upstream d2 update must trigger downstream d2.
    let m = PartitionMappingKind::Static {
        mapping: HashMap::from([("d1".to_string(), "u1".to_string())]),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Keys(HashSet::from([spk("d2")]))),
        PartitionSelection::Keys(HashSet::from([spk("d2")])),
        "unmapped upstream d2 must identity-map to downstream d2"
    );
    // Both the explicit reverse mapping and the identity image fire: forward map_key applies
    // identity to any downstream key absent from the mapping keys, so a downstream named u1
    // reads upstream u1 too (spurious keys filtered against the universe).
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Keys(HashSet::from([spk("u1")]))),
        PartitionSelection::Keys(HashSet::from([spk("d1"), spk("u1")])),
        "explicit u1 -> d1 plus the identity image u1 -> u1"
    );
}

#[test]
fn test_partition_mapping_specific() {
    let m = PartitionMappingKind::SpecificPartitions {
        keys: vec!["latest".into()],
    };
    // Any upstream change affects all downstream
    let up = PartitionSelection::Keys(HashSet::from([spk("latest")]));
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_resolver_identity_passthrough() {
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let upstream_keys =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2"), spk("p3")]))]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    assert_eq!(resolver.map_downstream("a", "b", &sel, None), sel);
}

#[test]
fn test_partition_resolver_no_mapping_is_identity() {
    // No mapping registered for edge (c, d) → identity passthrough
    let resolver = PartitionResolver::empty();
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    assert_eq!(resolver.map_downstream("d", "c", &sel, None), sel);
}

#[test]
fn test_unpartitioned_result_has_no_selection() {
    // When partitions is None, result.selection should be None
    let record = make_record("a");
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(result.fired);
    assert!(result.selection.is_none());
    assert!(result.sub_selections.is_none());
}

#[test]
fn test_partitioned_eager_selects_new_partition() {
    // raw_events → cleaned_events (eager): raw has p1/p2/p3, cleaned has p1/p2 → eager selects p3 only.
    let raw = make_materialized_record("raw", 200);
    let cleaned = make_materialized_record("cleaned", 100);
    let records = HashMap::from([
        ("raw".into(), raw.clone()),
        ("cleaned".into(), cleaned.clone()),
    ]);
    let deps = HashMap::from([("cleaned".into(), vec!["raw".into()])]);

    let upstream_keys = HashMap::from([(
        "raw".into(),
        HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
    )]);
    let mappings = HashMap::from([(
        ("cleaned".into(), "raw".into()),
        PartitionMappingKind::Identity,
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let raw_state = AssetConditionState::default();
    let asset_states = HashMap::from([("raw".into(), raw_state)]);

    // Upstream "raw" partition status: all 3 partitions materialized
    let partition_statuses = HashMap::from([(
        "raw".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200), (spk("p3"), 200)]),
        },
    )]);

    let _ak3 = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat3 = HashSet::from([spk("p1"), spk("p2")]);
    let _ip3: HashSet<PartitionKey> = HashSet::new();
    let _fail3: HashSet<PartitionKey> = HashSet::new();
    let _ts3 = HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak3,
        in_progress: &_ip3,
        failed: &_fail3,
        timestamps: &_ts3,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "cleaned",
        root_key: "cleaned",
        target_record: &cleaned,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true, // first tick,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&ConditionNode::eager(), &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    // p3 should be in the selection (it's missing)
    match &sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&spk("p3")), "p3 should be selected (missing)");
        }
        _ => panic!("expected Keys, got {:?}", sel),
    }
}

// ── Multi and MultiToSingle mapping tests ───────────────────────────────

#[test]
fn test_partition_mapping_multi_identity_dims() {
    // Multi mapping with identity per dimension: same-named dims
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let sel = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_multi_dimension_rename() {
    // Multi mapping with dimension rename: upstream "src_date" → downstream "date"
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "src_date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "src_region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("src_date", "2024-01-01"),
        ("src_region", "us"),
    ])]));
    let down = m.map_to_downstream(&upstream);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([mpk(&[
            ("date", "2024-01-01"),
            ("region", "us")
        ])]))
    );
}

#[test]
fn test_partition_mapping_multi_with_static_sub() {
    // Multi mapping with static per-dimension mapping on one dimension
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::Static {
                        mapping: HashMap::from([
                            ("north".into(), "us".into()),
                            ("europe".into(), "eu".into()),
                        ]),
                    }),
                ),
            ),
        ]),
    };
    // Upstream "eu" → downstream "europe" (explicit) plus the identity image "eu"
    // (a downstream region named "eu" reads upstream "eu"); spurious combos filtered against the universe.
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "eu"),
    ])]));
    let down = m.map_to_downstream(&up);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "europe")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ]))
    );
}

#[test]
fn test_partition_mapping_multi_many_to_one_sub_keeps_all_downstream_keys() {
    // A many-to-one per-dimension Static sub-mapping must reverse-map one upstream value
    // to both downstream keys (cartesian product), not just the first.
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::Static {
                        mapping: HashMap::from([
                            ("north".into(), "shared".into()),
                            ("south".into(), "shared".into()),
                        ]),
                    }),
                ),
            ),
        ]),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "shared"),
    ])]));
    let down = m.map_to_downstream(&up);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "north")]),
            mpk(&[("date", "2024-01-01"), ("region", "south")]),
            // Identity image: a downstream region named "shared" would forward-read upstream "shared" too.
            mpk(&[("date", "2024-01-01"), ("region", "shared")]),
        ]))
    );
}

#[test]
fn test_partition_mapping_multi_empty_and_all() {
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([(
            "d".into(),
            ("d".into(), Box::new(PartitionMappingKind::Identity)),
        )]),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::All),
        PartitionSelection::All
    );
}

#[test]
fn test_partition_mapping_multi_all_sub_expands_against_universe() {
    // A per-dimension AllPartitions sub-mapping fans that dimension out; with
    // the downstream universe available the mapping expands precisely instead
    // of escalating the whole selection to `All` (which over-materialized
    // every unrelated date).
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::AllPartitions),
                ),
            ),
        ]),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let universe = HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        mpk(&[("date", "2024-01-02"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]);
    assert_eq!(
        m.map_to_downstream_in(&up, Some(&universe)),
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "us")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ])),
        "the constrained date must limit the fan-out to that date's regions"
    );
    // Without a universe the over-approximation remains (never drop the key).
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_mapping_multi_to_single_all_sub_overapproximates_to_all() {
    // MultiToSingle whose inner fans the dimension in must over-approximate to `All`, not drop the key.
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "region".into(),
        inner: Box::new(PartitionMappingKind::AllPartitions),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_mapping_multi_multiple_keys() {
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let sel = PartitionSelection::Keys(HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]));
    // Identity per-dim → same keys
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_multi_to_single_extract_dimension() {
    // MultiToSingle: extract "date" from multi key
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };

    // Upstream is multi "date=2024-01-01|region=us" → downstream extracts "date" → "2024-01-01"
    let upstream = PartitionSelection::Keys(HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]));
    let downstream = m.map_to_downstream(&upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("2024-01-01"), spk("2024-01-02")]))
    );
}

#[test]
fn test_partition_mapping_multi_to_single_with_static_inner() {
    // MultiToSingle with static inner mapping
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "region".into(),
        inner: Box::new(PartitionMappingKind::Static {
            mapping: HashMap::from([("north".into(), "us".into())]),
        }),
    };

    // Upstream region=us → downstream "north" (reverse of the explicit entry) plus the
    // identity image "us"; phantoms filtered against the universe.
    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let downstream = m.map_to_downstream(&upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("north"), spk("us")]))
    );
}

#[test]
fn test_partition_mapping_multi_to_single_empty_and_all() {
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::All),
        PartitionSelection::All
    );
}

#[test]
fn test_partition_mapping_single_to_multi_expands_against_universe() {
    // MultiToSingle's other orientation: a Single-partitioned upstream feeding
    // a Multi-partitioned downstream. The upstream key constrains the named
    // dimension; the remaining dimensions expand against the downstream
    // universe.
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };
    let universe = HashSet::from([
        mpk(&[("date", "2024-01-05"), ("region", "us")]),
        mpk(&[("date", "2024-01-05"), ("region", "eu")]),
        mpk(&[("date", "2024-01-06"), ("region", "us")]),
    ]);
    let upstream = PartitionSelection::Keys(HashSet::from([spk("2024-01-05")]));
    assert_eq!(
        m.map_to_downstream_in(&upstream, Some(&universe)),
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-05"), ("region", "us")]),
            mpk(&[("date", "2024-01-05"), ("region", "eu")]),
        ])),
        "a Single upstream key must select every downstream Multi key matching the named dimension"
    );
    // Without a universe the fan-out must over-approximate, not drop the update.
    assert_eq!(m.map_to_downstream(&upstream), PartitionSelection::All);
    // An upstream key matching nothing downstream maps to Empty.
    assert_eq!(
        m.map_to_downstream_in(
            &PartitionSelection::Keys(HashSet::from([spk("2020-12-31")])),
            Some(&universe)
        ),
        PartitionSelection::Empty
    );
}

// ── Partition resolver with Multi/MultiToSingle ─────────────────────────

#[test]
fn test_resolver_multi_mapping() {
    let mappings = HashMap::from([(
        ("down".into(), "up".into()),
        PartitionMappingKind::Multi {
            dimension_mappings: HashMap::from([
                (
                    "date".into(),
                    ("date".into(), Box::new(PartitionMappingKind::Identity)),
                ),
                (
                    "region".into(),
                    ("region".into(), Box::new(PartitionMappingKind::Identity)),
                ),
            ]),
        },
    )]);
    let upstream_keys = HashMap::from([(
        "up".to_string(),
        HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "us")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ]),
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let sel = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(resolver.map_downstream("up", "down", &sel, None), sel);
}

#[test]
fn test_resolver_multi_to_single_mapping() {
    let mappings = HashMap::from([(
        ("single_down".into(), "multi_up".into()),
        PartitionMappingKind::MultiToSingle {
            dimension_name: "date".into(),
            inner: Box::new(PartitionMappingKind::Identity),
        },
    )]);
    let upstream_keys = HashMap::from([(
        "multi_up".to_string(),
        HashSet::from([mpk(&[("date", "2024-01-01"), ("region", "us")])]),
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let downstream = resolver.map_downstream("multi_up", "single_down", &upstream, None);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("2024-01-01")]))
    );
}

// ── Comprehensive partition evaluation scenarios ────────────────────────

#[test]
fn test_partitioned_eager_partial_upstream_update() {
    // raw → processed (eager): raw has p1/p2/p3, processed has p1/p2; raw p3 just materialized → eager selects p3.
    let raw = make_materialized_record("raw", 200);
    let processed = make_materialized_record("processed", 100);
    let records = HashMap::from([
        ("raw".into(), raw.clone()),
        ("processed".into(), processed.clone()),
    ]);
    let deps = HashMap::from([("processed".into(), vec!["raw".into()])]);

    let upstream_keys = HashMap::from([(
        "raw".into(),
        HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
    )]);
    let mappings = HashMap::from([(
        ("processed".into(), "raw".into()),
        PartitionMappingKind::Identity,
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    // raw_state knows p1,p2 at ts=200, so only p3 (new at 200) is NewlyUpdated.
    let raw_state = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let asset_states = HashMap::from([("raw".into(), raw_state)]);

    // Upstream "raw" partition status: all 3 partitions materialized
    let partition_statuses = HashMap::from([(
        "raw".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200), (spk("p3"), 200)]),
        },
    )]);

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "processed",
        root_key: "processed",
        target_record: &processed,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&ConditionNode::eager(), &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    match &sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&spk("p3")), "p3 should be selected (missing)");
            assert!(!keys.contains(&spk("p1")), "p1 already materialized");
            assert!(!keys.contains(&spk("p2")), "p2 already materialized");
        }
        _ => panic!("expected Keys, got {:?}", sel),
    }
}

#[test]
fn test_partitioned_eager_only_fires_for_partitions_with_upstream_data() {
    // Upstream has 3 of 5 partitions materialized; eager() on the never-materialized downstream
    // fires only for those 3 (the !AnyDepsMissing clause uses per-partition upstream status).
    let raw = make_materialized_record("raw", 200);
    let processed = make_record("processed"); // Missing by default
    let records = HashMap::from([("raw".into(), raw), ("processed".into(), processed.clone())]);
    let deps = HashMap::from([("processed".into(), vec!["raw".into()])]);

    let all_partitions = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4"), spk("p5")]);
    let upstream_keys = HashMap::from([("raw".into(), all_partitions.clone())]);
    let mappings = HashMap::from([(
        ("processed".into(), "raw".into()),
        PartitionMappingKind::Identity,
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    // Upstream "raw" only has p1, p2, p3 materialized (p4, p5 are missing)
    let partition_statuses = HashMap::from([(
        "raw".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200), (spk("p3"), 200)]),
        },
    )]);

    // Downstream "processed" has never been materialized
    let empty_ip: HashSet<PartitionKey> = HashSet::new();
    let empty_fail: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_partitions,
        in_progress: &empty_ip,
        failed: &empty_fail,
        timestamps: &empty_ts,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "processed",
        root_key: "processed",
        target_record: &processed,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &HashMap::new(),
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    // evaluate() should use partition-aware path
    let result = evaluate(&ConditionNode::eager(), &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    match &sel {
        PartitionSelection::Keys(keys) => {
            assert_eq!(
                keys.len(),
                3,
                "should fire for exactly 3 partitions (those with upstream data)"
            );
            assert!(keys.contains(&spk("p1")));
            assert!(keys.contains(&spk("p2")));
            assert!(keys.contains(&spk("p3")));
            assert!(!keys.contains(&spk("p4")), "p4 has no upstream data");
            assert!(!keys.contains(&spk("p5")), "p5 has no upstream data");
        }
        _ => panic!("expected Keys, got {:?}", sel),
    }

    // evaluate_with_tree() should produce the same selection
    let (result2, tree) = evaluate_with_tree(&ConditionNode::eager(), &ctx);
    assert!(result2.fired);
    assert_eq!(
        result2.selection,
        Some(sel.clone()),
        "evaluate_with_tree must match evaluate"
    );
    assert!(
        tree.num_partitions.is_some(),
        "tree should have partition counts"
    );
}

#[test]
fn test_partitioned_on_missing_only_missing_partitions() {
    // on_missing() fires only for missing partitions whose upstream deps are not missing.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let upstream_keys =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2"), spk("p3")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let asset_states = HashMap::from([("a".into(), AssetConditionState::default())]);

    // Upstream "a" partition status: all 3 partitions materialized
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100), (spk("p3"), 100)]),
        },
    )]);

    // b: p1 materialized, p2 and p3 missing
    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&ConditionNode::on_missing(), &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    match &sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&spk("p2")));
            assert!(keys.contains(&spk("p3")));
            assert!(!keys.contains(&spk("p1")));
        }
        _ => panic!("expected Keys, got {:?}", sel),
    }
}

#[test]
fn test_partitioned_in_progress_excludes_from_and() {
    let empty_partition_statuses = HashMap::new();
    // And(Missing, Not(InProgress)): p2 missing, p3 missing+in_progress → {p2}.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1")]);
    let _ip = HashSet::from([spk("p3")]);
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let cond = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::Not(Box::new(ConditionNode::InProgress)),
    ]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2")]))
    );
}

#[test]
fn test_partitioned_code_version_changed_all_partitions() {
    let empty_partition_statuses = HashMap::new();
    // Code version change affects ALL partitions uniformly
    let mut record = make_materialized_record("a", 100);
    record.code_version = Some("v2".into());
    record.last_materialization_code_version = Some("v1".into());
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4")]);
    let _mat = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let pctx_ref = &pctx;
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, pctx_ref);
    let result = evaluate(&ConditionNode::CodeVersionChanged, &ctx);
    assert!(result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::All);
}

#[test]
fn test_partitioned_since_latch_per_partition() {
    let empty_partition_statuses = HashMap::new();
    // Since{Missing, reset NewlyUpdated}. Tick 1: p2,p3 missing → latch {p2,p3}.
    // Tick 2: p2 materialized (reset), p3 latched → {p3}.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    // Tick 1
    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat1 = HashSet::from([spk("p1")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts1 = HashMap::from([(spk("p1"), 100_i64)]);
    let pctx1 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts1,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx1),
        root_partition_floor: None,
    };
    let cond = ConditionNode::Since {
        trigger: Box::new(ConditionNode::Missing),
        reset: Box::new(ConditionNode::NewlyUpdated),
    };
    let r1 = evaluate(&cond, &ctx1);
    assert_eq!(
        r1.selection.as_ref().unwrap(),
        &PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );

    // Tick 2: p2 now materialized with new timestamp
    let _mat2 = HashSet::from([spk("p1"), spk("p2")]);
    let _ts2 = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 200)]);
    let pctx2 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts2,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let prev2 = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: r1.sub_selections.unwrap(),
            timestamps: _ts1.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    // p2 was reset (NewlyUpdated), p3 still latched
    assert_eq!(
        r2.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p3")]))
    );
}

#[test]
fn test_partitioned_newly_true_only_new_partitions() {
    // NewlyTrue(Missing): only partitions that became missing this tick.
    // Tick 1 {p2,p3}, Tick 2 {} (no change), Tick 3 p4 added → {p4}.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));

    // Tick 1
    let pdata1 = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx1 = pdata1.as_eval_ctx();
    let ctx1 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: true,
        partitions: Some(&pctx1),
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert_eq!(
        r1.selection.as_ref().unwrap(),
        &PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );

    // Tick 2: same state → no newly true
    let prev2 = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: r1.sub_selections.unwrap(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let pctx2 = pdata1.as_eval_ctx();
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    assert_eq!(r2.selection.unwrap(), PartitionSelection::Empty);

    // Tick 3: p4 added as new partition (missing)
    let pdata3 = OwnedPartitionData::new(&["p1", "p2", "p3", "p4"], &["p1"], &[("p1", 100)]);
    let prev3 = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: r2.sub_selections.unwrap(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let pctx3 = pdata3.as_eval_ctx();
    let ctx3 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev3,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 3_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx3),
        root_partition_floor: None,
    };
    let r3 = evaluate(&cond, &ctx3);
    // Only p4 is newly missing (p2,p3 were already missing last tick)
    assert_eq!(
        r3.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p4")]))
    );
}

#[test]
fn test_partitioned_execution_failed_subset() {
    let empty_partition_statuses = HashMap::new();
    // ExecutionFailed returns only the failed partition keys
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4")]);
    let _mat = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail = HashSet::from([spk("p3"), spk("p4")]);
    let _ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let result = evaluate(&ConditionNode::ExecutionFailed, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p3"), spk("p4")]))
    );
}

#[test]
fn test_partitioned_complex_or_and_not() {
    let empty_partition_statuses = HashMap::new();
    // Or(Missing, ExecutionFailed) & Not(InProgress); p1 materialized, p2 missing, p3 failed, p4 missing+in_progress.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4")]);
    let _ip = HashSet::from([spk("p4")]);
    let _fail = HashSet::from([spk("p3")]);
    // p1 and p3 materialized (timestamps ARE the materialized set).
    let _ts: HashMap<PartitionKey, i64> = HashMap::from([(spk("p1"), 50), (spk("p3"), 50)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::And(vec![
        ConditionNode::Or(vec![ConditionNode::Missing, ConditionNode::ExecutionFailed]),
        ConditionNode::Not(Box::new(ConditionNode::InProgress)),
    ]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    // Missing={p2,p4}, Failed={p3}, Or={p2,p3,p4}; Not(InProgress)={p1,p2,p3}; And={p2,p3}.
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

// ── eager must not fire on first daemon tick when all assets are UpToDate ──

#[test]
fn test_eager_does_not_fire_when_all_up_to_date_first_tick() {
    // a → b → c all materialized (UpToDate); on a fresh daemon start (is_initial=true),
    // eager on b/c must not fire.
    let ts = 1_000_000_000;
    let rec_a = make_materialized_record("a", ts);
    let rec_b = make_materialized_record("b", ts);
    let rec_c = make_materialized_record("c", ts);

    let records = HashMap::from([
        ("a".to_string(), rec_a),
        ("b".to_string(), rec_b.clone()),
        ("c".to_string(), rec_c.clone()),
    ]);
    let upstream_deps = HashMap::from([
        ("b".to_string(), vec!["a".to_string()]),
        ("c".to_string(), vec!["b".to_string()]),
    ]);

    let cond = ConditionNode::eager();

    // Dep states: all materialized at the same timestamp (nothing changed)
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(ts),
        ..Default::default()
    };
    let b_state_init = AssetConditionState {
        last_materialized_timestamp: Some(ts),
        ..Default::default()
    };
    let all_states = HashMap::from([("a".to_string(), a_state), ("b".to_string(), b_state_init)]);

    // First tick: is_initial=true, prev_state is empty (default)
    let mut ctx_b = make_ctx("b", &rec_b, &records, &upstream_deps);
    ctx_b.is_initial = true;
    ctx_b.all_asset_states = &all_states;

    let (result_b, tree_b) = evaluate_with_tree(&cond, &ctx_b);

    fn print_tree(node: &EvalNodeResult, indent: usize) {
        let pad = " ".repeat(indent);
        eprintln!(
            "{pad}{} [{}] → {:?}",
            node.label, node.node_type, node.status
        );
        for child in &node.children {
            print_tree(child, indent + 2);
        }
    }
    eprintln!("=== Eval tree for b ===");
    print_tree(&tree_b, 0);

    assert!(
        !result_b.fired,
        "b should NOT fire on first tick when all assets are up-to-date"
    );

    // Simulate what the daemon does after tick 1: update_condition_state
    let mut state_b = AssetConditionState::default();
    update_condition_state(
        &mut state_b,
        &StateUpdateContext::from_eval_context(&ctx_b),
        &result_b,
    );

    // Second tick: is_initial=false, nothing changed → still no fire.
    let tick2_now = ctx_b.now + 1_000_000_000; // 1s later
    let ctx_b2 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &rec_b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &state_b,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: tick2_now,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    let (result_b2, tree_b2) = evaluate_with_tree(&cond, &ctx_b2);
    eprintln!("=== Eval tree for b (tick 2) ===");
    print_tree(&tree_b2, 0);

    assert!(
        !result_b2.fired,
        "b should NOT fire on second tick when nothing changed"
    );
}

#[test]
fn test_eager_fires_after_upstream_observed_on_second_tick() {
    // ext_feed (observed) → aggregated (eager) → report (eager). Tick 1 (initial) no fire;
    // ext_feed re-observed at 500 → Tick 2 aggregated fires; aggregated materializes at 600 → Tick 3 report fires.

    fn print_tree(node: &EvalNodeResult, indent: usize) {
        let pad = " ".repeat(indent);
        eprintln!(
            "{pad}{} [{}] → {:?}",
            node.label, node.node_type, node.status
        );
        for child in &node.children {
            print_tree(child, indent + 2);
        }
    }

    let rec_ext = make_materialized_record("ext_feed", 100);
    let rec_agg = make_materialized_record("aggregated", 200);
    let rec_rep = make_materialized_record("report", 300);

    let records = HashMap::from([
        ("ext_feed".to_string(), rec_ext.clone()),
        ("aggregated".to_string(), rec_agg.clone()),
        ("report".to_string(), rec_rep.clone()),
    ]);
    let upstream_deps = HashMap::from([
        ("aggregated".to_string(), vec!["ext_feed".to_string()]),
        ("report".to_string(), vec!["aggregated".to_string()]),
    ]);

    let cond = ConditionNode::eager();

    // Dep states: ext_feed seen at ts=100, aggregated at ts=200
    let ext_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let agg_state_init = AssetConditionState {
        last_materialized_timestamp: Some(200),
        ..Default::default()
    };
    let all_states_tick1 = HashMap::from([
        ("ext_feed".to_string(), ext_state),
        ("aggregated".to_string(), agg_state_init),
        (
            "report".to_string(),
            AssetConditionState {
                last_materialized_timestamp: Some(300),
                ..Default::default()
            },
        ),
    ]);

    // ── Tick 1 (is_initial=true): nothing changed, should NOT fire ──
    let now1 = 1000;
    let mut ctx_agg = make_ctx("aggregated", &rec_agg, &records, &upstream_deps);
    ctx_agg.is_initial = true;
    ctx_agg.now = now1;
    ctx_agg.all_asset_states = &all_states_tick1;

    let (result_agg1, tree_agg1) = evaluate_with_tree(&cond, &ctx_agg);
    eprintln!("=== Tick 1: aggregated ===");
    print_tree(&tree_agg1, 0);
    assert!(
        !result_agg1.fired,
        "aggregated should NOT fire on tick 1 (initial, all up-to-date)"
    );

    // Update state after tick 1
    let mut state_agg = AssetConditionState::default();
    update_condition_state(
        &mut state_agg,
        &StateUpdateContext::from_eval_context(&ctx_agg),
        &result_agg1,
    );

    // ── ext_feed gets re-observed → new timestamp ──
    let rec_ext_new = make_materialized_record("ext_feed", 500);
    let records2 = HashMap::from([
        ("ext_feed".to_string(), rec_ext_new),
        ("aggregated".to_string(), rec_agg.clone()),
        ("report".to_string(), rec_rep.clone()),
    ]);

    // ── Tick 2 (is_initial=false): ext_feed updated, aggregated should fire ──
    let now2 = 2000;
    let ctx_agg2 = EvalContext {
        target_key: "aggregated",
        root_key: "aggregated",
        target_record: &rec_agg,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &state_agg,
        all_asset_states: &all_states_tick1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now2,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    let (result_agg2, tree_agg2) = evaluate_with_tree(&cond, &ctx_agg2);
    eprintln!("=== Tick 2: aggregated (after ext_feed re-observed) ===");
    print_tree(&tree_agg2, 0);
    assert!(
        result_agg2.fired,
        "aggregated SHOULD fire on tick 2 (ext_feed was re-observed)"
    );

    // Update state, simulate aggregated materialization at ts=600
    let mut state_agg2 = state_agg.clone();
    update_condition_state(
        &mut state_agg2,
        &StateUpdateContext::from_eval_context(&ctx_agg2),
        &result_agg2,
    );
    state_agg2.last_handled_timestamp = Some(now2);

    let rec_agg_new = make_materialized_record("aggregated", 600);
    let records3 = HashMap::from([
        (
            "ext_feed".to_string(),
            make_materialized_record("ext_feed", 500),
        ),
        ("aggregated".to_string(), rec_agg_new.clone()),
        ("report".to_string(), rec_rep.clone()),
    ]);

    // ── Tick 3: report should fire (aggregated updated) ──
    let now3 = 3000;
    let mut state_rep = AssetConditionState::default();
    // Simulate tick 1 for report too
    let mut ctx_rep1 = make_ctx("report", &rec_rep, &records, &upstream_deps);
    ctx_rep1.is_initial = true;
    ctx_rep1.now = now1;
    ctx_rep1.all_asset_states = &all_states_tick1;
    let (result_rep1, _) = evaluate_with_tree(&cond, &ctx_rep1);
    update_condition_state(
        &mut state_rep,
        &StateUpdateContext::from_eval_context(&ctx_rep1),
        &result_rep1,
    );

    // Tick 2 for report (aggregated not yet re-materialized)
    let prev_state_rep = state_rep.clone();
    let ctx_rep2 = EvalContext {
        target_key: "report",
        root_key: "report",
        target_record: &rec_rep,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state_rep,
        all_asset_states: &all_states_tick1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now2,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let (result_rep2, _) = evaluate_with_tree(&cond, &ctx_rep2);
    update_condition_state(
        &mut state_rep,
        &StateUpdateContext::from_eval_context(&ctx_rep2),
        &result_rep2,
    );

    // Tick 3 for report (aggregated now re-materialized at ts=600)
    let ctx_rep3 = EvalContext {
        target_key: "report",
        root_key: "report",
        target_record: &rec_rep,
        cache: CacheSnapshot {
            records: &records3,
            upstream_deps: &upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &state_rep,
        all_asset_states: &all_states_tick1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now3,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    let (result_rep3, tree_rep3) = evaluate_with_tree(&cond, &ctx_rep3);
    eprintln!("=== Tick 3: report (after aggregated re-materialized) ===");
    print_tree(&tree_rep3, 0);
    assert!(
        result_rep3.fired,
        "report SHOULD fire on tick 3 (aggregated was re-materialized)"
    );
}

#[test]
fn test_eager_fires_after_dep_in_progress_clears() {
    // a → b, both materialized. While a is in-progress b must not fire (AnyDepsInProgress);
    // after a completes with a new timestamp b fires next tick. The dep-state update while
    // in-progress must not silently consume the change.

    fn print_tree(node: &EvalNodeResult, indent: usize) {
        let pad = " ".repeat(indent);
        eprintln!(
            "{pad}{} [{}] → {:?}",
            node.label, node.node_type, node.status
        );
        for child in &node.children {
            print_tree(child, indent + 2);
        }
    }

    let ts = 1_000_000_000;
    let rec_a = make_materialized_record("a", ts);
    let rec_b = make_materialized_record("b", ts);

    let records = HashMap::from([
        ("a".to_string(), rec_a.clone()),
        ("b".to_string(), rec_b.clone()),
    ]);
    let upstream_deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let cond = ConditionNode::eager();

    // Dep state: a materialized at ts
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(ts),
        ..Default::default()
    };
    let all_states = HashMap::from([
        ("a".to_string(), a_state),
        (
            "b".to_string(),
            AssetConditionState {
                last_materialized_timestamp: Some(ts),
                ..Default::default()
            },
        ),
    ]);

    // ── Tick 1 (initial): nothing fires ──
    let now1 = 2_000_000_000;
    let mut ctx1 = make_ctx("b", &rec_b, &records, &upstream_deps);
    ctx1.is_initial = true;
    ctx1.now = now1;
    ctx1.all_asset_states = &all_states;

    let (result1, _) = evaluate_with_tree(&cond, &ctx1);
    assert!(
        !result1.fired,
        "tick 1: b should NOT fire (initial, all up-to-date)"
    );

    let mut state_b = AssetConditionState::default();
    update_condition_state(
        &mut state_b,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // ── a starts re-materializing (in-progress) with new timestamp ──
    let new_a_ts = 3_000_000_000;
    let rec_a_new = make_materialized_record("a", new_a_ts);
    let records2 = HashMap::from([
        ("a".to_string(), rec_a_new.clone()),
        ("b".to_string(), rec_b.clone()),
    ]);
    let in_progress = HashSet::from(["a".to_string()]);

    // ── Tick 2: a is in-progress, b should NOT fire ──
    let now2 = 4_000_000_000;
    let prev_state2 = state_b.clone();
    let ctx2 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &rec_b,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &upstream_deps,
            in_progress_assets: &in_progress,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state2,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now2,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    let (result2, tree2) = evaluate_with_tree(&cond, &ctx2);
    eprintln!("=== Tick 2: b (a is in-progress) ===");
    print_tree(&tree2, 0);
    assert!(
        !result2.fired,
        "tick 2: b should NOT fire (a is in-progress)"
    );

    // Update state after tick 2
    update_condition_state(
        &mut state_b,
        &StateUpdateContext::from_eval_context(&ctx2),
        &result2,
    );

    // ── a completes (no longer in-progress) ──
    let empty_in_progress: HashSet<String> = HashSet::new();

    // ── Tick 3: a completed, b SHOULD fire ──
    let now3 = 5_000_000_000;
    let prev_state3 = state_b.clone();
    let ctx3 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &rec_b,
        cache: CacheSnapshot {
            records: &records2,
            upstream_deps: &upstream_deps,
            in_progress_assets: &empty_in_progress,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: RunTagSnapshot {
            last_run_tags: &EMPTY_RUN_TAGS,
            partition_last_run_tags: &EMPTY_PARTITION_RUN_TAGS,
            tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
            tick_partition_materialization_tags: &EMPTY_TICK_PART_MAT_TAGS,
            last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
            partition_last_run_asset_names: &EMPTY_PARTITION_RUN_ASSET_NAMES,
        },
        prev_state: &prev_state3,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now3,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };

    let (result3, tree3) = evaluate_with_tree(&cond, &ctx3);
    eprintln!("=== Tick 3: b (a completed) ===");
    print_tree(&tree3, 0);

    assert!(
        result3.fired,
        "tick 3: b SHOULD fire (a completed with new timestamp, no longer in-progress)"
    );
}

#[test]
fn test_has_time_based_conditions() {
    // Leaf: CronTickPassed is time-based
    assert!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .has_time_based_conditions()
    );

    // Non-time-based leaves
    assert!(!ConditionNode::Missing.has_time_based_conditions());
    assert!(!ConditionNode::InProgress.has_time_based_conditions());
    assert!(!ConditionNode::ExecutionFailed.has_time_based_conditions());
    assert!(!ConditionNode::NewlyUpdated.has_time_based_conditions());
    assert!(!ConditionNode::CodeVersionChanged.has_time_based_conditions());
    assert!(!ConditionNode::any_deps_missing().has_time_based_conditions());
    assert!(!ConditionNode::any_deps_in_progress().has_time_based_conditions());
    assert!(!ConditionNode::any_deps_updated().has_time_based_conditions());

    // Nested in And
    let and_with_cron = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::CronTickPassed {
            cron_schedule: "0 0 * * *".to_string(),
            timezone: None,
        },
    ]);
    assert!(and_with_cron.has_time_based_conditions());

    // Nested in Or
    let or_without_cron =
        ConditionNode::Or(vec![ConditionNode::Missing, ConditionNode::InProgress]);
    assert!(!or_without_cron.has_time_based_conditions());

    // Nested in Not
    let not_cron = !ConditionNode::CronTickPassed {
        cron_schedule: "* * * * *".to_string(),
        timezone: None,
    };
    assert!(not_cron.has_time_based_conditions());

    // Nested in NewlyTrue
    assert!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .newly_true()
        .has_time_based_conditions()
    );

    // Nested in Since (trigger)
    let since_cron = ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    }
    .since(ConditionNode::NewlyRequested);
    assert!(since_cron.has_time_based_conditions());

    // Nested in Since (reset)
    let since_reset_cron = ConditionNode::Missing.since(ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    });
    assert!(since_reset_cron.has_time_based_conditions());

    // Nested in SinceLastHandled
    assert!(
        !ConditionNode::Missing
            .since_last_handled()
            .has_time_based_conditions()
    );

    // Nested in AnyDepsMatch
    let any_deps = ConditionNode::any_deps_match(ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    });
    assert!(any_deps.has_time_based_conditions());

    // Nested in AllDepsMatch
    let all_deps = ConditionNode::all_deps_match(ConditionNode::Missing);
    assert!(!all_deps.has_time_based_conditions());

    // InLatestTimeWindow is NOT time-based (it's partition-based)
    assert!(
        !ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(86400.0)
        }
        .has_time_based_conditions()
    );
}

#[test]
fn test_node_type_str() {
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).node_type_str(),
        "And"
    );
    assert_eq!(
        ConditionNode::Or(vec![ConditionNode::Missing]).node_type_str(),
        "Or"
    );
    assert_eq!((!ConditionNode::Missing).node_type_str(), "Not");
    assert_eq!(
        ConditionNode::Missing.newly_true().node_type_str(),
        "NewlyTrue"
    );
    assert_eq!(
        ConditionNode::Missing
            .since(ConditionNode::NewlyRequested)
            .node_type_str(),
        "Since"
    );
    assert_eq!(
        ConditionNode::Missing.since_last_handled().node_type_str(),
        "SinceLastHandled"
    );
    assert_eq!(
        ConditionNode::any_deps_match(ConditionNode::Missing).node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::all_deps_match(ConditionNode::Missing).node_type_str(),
        "AllDepsMatch"
    );

    // All leaf variants return "Leaf"
    assert_eq!(ConditionNode::Missing.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::InProgress.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::ExecutionFailed.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::NewlyUpdated.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::NewlyRequested.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::CodeVersionChanged.node_type_str(), "Leaf");
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .node_type_str(),
        "Leaf"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        }
        .node_type_str(),
        "Leaf"
    );
    assert_eq!(
        ConditionNode::any_deps_missing().node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::any_deps_in_progress().node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::any_deps_updated().node_type_str(),
        "AnyDepsMatch"
    );
}

#[test]
fn test_node_label_exhaustive() {
    assert_eq!(ConditionNode::Missing.node_label(), "missing");
    assert_eq!(ConditionNode::InProgress.node_label(), "in_progress");
    assert_eq!(
        ConditionNode::ExecutionFailed.node_label(),
        "execution_failed"
    );
    assert_eq!(ConditionNode::NewlyUpdated.node_label(), "newly_updated");
    assert_eq!(
        ConditionNode::NewlyRequested.node_label(),
        "newly_requested"
    );
    assert_eq!(
        ConditionNode::CodeVersionChanged.node_label(),
        "code_version_changed"
    );
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 */5 * * *".to_string(),
            timezone: Some("UTC".to_string()),
        }
        .node_label(),
        // tz is load-bearing → must appear in the label (else two crons differing only by zone collapse)
        "cron_tick_passed('0 */5 * * *', tz='UTC')"
    );
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 */5 * * *".to_string(),
            timezone: None,
        }
        .node_label(),
        "cron_tick_passed('0 */5 * * *')"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(3600.0),
        }
        .node_label(),
        "in_latest_time_window(3600s)"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        }
        .node_label(),
        "in_latest_time_window"
    );
    assert_eq!(
        ConditionNode::any_deps_missing().node_label(),
        "any_deps_missing"
    );
    assert_eq!(
        ConditionNode::any_deps_in_progress().node_label(),
        "any_deps_in_progress"
    );
    assert_eq!(
        ConditionNode::any_deps_updated().node_label(),
        "any_deps_updated"
    );
    assert_eq!(
        ConditionNode::any_deps_match(ConditionNode::Missing).node_label(),
        format!(
            "any_deps_match({})",
            ConditionNode::Missing.fingerprint_hex()
        )
    );
    assert_eq!(
        ConditionNode::all_deps_match(ConditionNode::Missing).node_label(),
        format!(
            "all_deps_match({})",
            ConditionNode::Missing.fingerprint_hex()
        )
    );
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).node_label(),
        "All of"
    );
    assert_eq!(
        ConditionNode::Or(vec![ConditionNode::Missing]).node_label(),
        "Any of"
    );
    assert_eq!((!ConditionNode::Missing).node_label(), "Not");
    assert_eq!(
        ConditionNode::Missing.newly_true().node_label(),
        "newly_true"
    );
    assert_eq!(
        ConditionNode::Missing
            .since(ConditionNode::NewlyRequested)
            .node_label(),
        "since"
    );
    assert_eq!(
        ConditionNode::Missing.since_last_handled().node_label(),
        "since_last_handled"
    );
}

#[test]
fn test_display_label_is_readable_not_a_fingerprint() {
    // node_label folds a fingerprint into unlabeled dep-aggregates/asset_matches for
    // replace-by-label, but that hex must not leak into the UI tree — display_label renders it readably.
    let dep = ConditionNode::any_deps_match(ConditionNode::NewlyUpdated);
    assert_eq!(dep.display_label(), "any_deps_match(newly_updated)");
    assert!(
        !dep.display_label()
            .contains(&ConditionNode::NewlyUpdated.fingerprint_hex()),
        "display_label must not contain the raw fingerprint"
    );

    let am =
        ConditionNode::asset_matches(vec!["upstream_feed".to_string()], ConditionNode::Missing);
    assert_eq!(
        am.display_label(),
        "asset_matches('upstream_feed', missing)"
    );

    // A user-provided label on a dep-aggregate is already readable — keep it.
    let labeled = ConditionNode::AnyDepsMatch {
        condition: Box::new(ConditionNode::Missing),
        label: Some("any_deps_missing".to_string()),
    };
    assert_eq!(labeled.display_label(), "any_deps_missing");

    // Leaf and composite nodes keep their existing labels.
    assert_eq!(ConditionNode::Missing.display_label(), "missing");
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).display_label(),
        "All of"
    );

    // The eval tree (rendered verbatim by the UI) must carry the readable label, not the fingerprint.
    let tree_node = crate::condition::state::EvalNodeResult::new(
        &dep,
        0,
        crate::condition::state::NodeStatus::True,
        vec![],
        None,
    );
    assert_eq!(tree_node.label, "any_deps_match(newly_updated)");
}

#[test]
fn test_node_label_distinguishes_unlabeled_aggregate_inner_condition() {
    // node_label for unlabeled any/all_deps_match and asset_matches must include the inner
    // condition, else structurally-distinct siblings collapse to one label and replace_by_label hits the wrong subtree.
    let a = ConditionNode::any_deps_match(ConditionNode::Missing);
    let b = ConditionNode::any_deps_match(ConditionNode::NewlyUpdated);
    assert_ne!(
        a.node_label(),
        b.node_label(),
        "distinct inner conditions must yield distinct labels"
    );

    // asset_matches with identical keys but different inner conditions.
    let am1 = ConditionNode::asset_matches(vec!["x".into()], ConditionNode::Missing);
    let am2 = ConditionNode::asset_matches(vec!["x".into()], ConditionNode::InProgress);
    assert_ne!(am1.node_label(), am2.node_label());

    // replace_by_label must touch only the matching sibling, preserving the other's inner condition.
    let tree = a.clone() | b.clone();
    let replaced = tree.replace_by_label(&a.node_label(), &ConditionNode::ExecutionFailed);
    if let ConditionNode::Or(children) = &replaced {
        assert!(
            children
                .iter()
                .any(|c| matches!(c, ConditionNode::ExecutionFailed)),
            "the matched sibling must be replaced; got {replaced:?}"
        );
        assert!(
            children.iter().any(|c| c.node_label() == b.node_label()),
            "the non-matching sibling must be preserved; got {replaced:?}"
        );
    } else {
        panic!("expected Or, got {replaced:?}");
    }
}

#[test]
fn root_scope_latest_time_window_stops_at_asset_matches() {
    // AssetMatches pivots evaluation onto OTHER assets, so a nested
    // InLatestTimeWindow doesn't constrain the root's own partitioning.
    let inner = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    let tree = ConditionNode::asset_matches(vec!["x".into()], inner);
    assert!(!tree.has_root_scope_latest_time_window());
}

#[test]
fn test_partition_selection_is_empty() {
    assert!(PartitionSelection::Empty.is_empty());
    assert!(!PartitionSelection::All.is_empty());
    assert!(PartitionSelection::Keys(HashSet::new()).is_empty());
    assert!(!PartitionSelection::Keys(HashSet::from([spk("p1")])).is_empty());
}

#[test]
fn test_partition_selection_is_all() {
    assert!(PartitionSelection::All.is_all());
    assert!(!PartitionSelection::Empty.is_all());
    assert!(!PartitionSelection::Keys(HashSet::from([spk("p1")])).is_all());
}

#[test]
fn test_evaluate_full_result_missing() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    let expected = EvalResult {
        fired: true,
        sub_results: HashMap::new(),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    assert_eq!(result, expected);
}

#[test]
fn test_evaluate_full_result_and() {
    // And([Missing, Not(InProgress)]) on missing, not-in-progress asset
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let cond = ConditionNode::And(vec![ConditionNode::Missing, !ConditionNode::InProgress]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(result.selection, None);
    assert_eq!(result.sub_selections, None);
    // sub_results should be empty since no stateful operators
    assert_eq!(result.sub_results, HashMap::new());
}

#[test]
fn test_evaluate_full_result_not_fired() {
    // InProgress on a non-in-progress asset
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::InProgress, &ctx);
    let expected = EvalResult {
        fired: false,
        sub_results: HashMap::new(),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    assert_eq!(result, expected);
}

#[test]
fn test_evaluate_with_tree_full_result() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::And(vec![ConditionNode::Missing, !ConditionNode::InProgress]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);

    assert!(result.fired);

    let expected_tree = EvalNodeResult {
        node_idx: 0,
        label: "All of".to_string(),
        node_type: "And".to_string(),
        status: NodeStatus::True,
        children: vec![
            EvalNodeResult {
                node_idx: 1,
                label: "missing".to_string(),
                node_type: "Leaf".to_string(),
                status: NodeStatus::True,
                children: vec![],
                num_partitions: None,
            },
            EvalNodeResult {
                node_idx: 2,
                label: "Not".to_string(),
                node_type: "Not".to_string(),
                status: NodeStatus::True,
                children: vec![EvalNodeResult {
                    node_idx: 3,
                    label: "in_progress".to_string(),
                    node_type: "Leaf".to_string(),
                    status: NodeStatus::False,
                    children: vec![],
                    num_partitions: None,
                }],
                num_partitions: None,
            },
        ],
        num_partitions: None,
    };
    assert_eq!(tree, expected_tree);
}

#[test]
fn test_update_condition_state_basic() {
    let record = make_materialized_record("a", 500);

    let mut state = AssetConditionState::default();
    let result = EvalResult {
        fired: true,
        sub_results: HashMap::from([(0, true), (1, false)]),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    let ctx = StateUpdateContext {
        target_record_timestamp: record.last_timestamp,
        target_data_version: record.last_data_version.as_ref(),
        now: 2000,
        is_initial: false,
        partition_timestamps: None,
    };
    update_condition_state(&mut state, &ctx, &result);

    let expected = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        dep_previous_results: HashMap::new(),
        dep_baselines: HashMap::new(),
        last_handled_timestamp: None,
        last_materialized_timestamp: Some(500),
        last_data_version: Some("dv_a".to_string()),
        last_tick_timestamp: Some(2000),
        condition_fingerprint: String::new(),
        is_initial: false,
        partition_state: None,
    };
    assert_eq!(state, expected);
}

#[test]
fn test_node_status_from_bool() {
    assert_eq!(NodeStatus::from_bool(true), NodeStatus::True);
    assert_eq!(NodeStatus::from_bool(false), NodeStatus::False);
}

#[test]
fn test_will_be_requested_false_when_not_in_set() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::WillBeRequested, &ctx).fired);
}

#[test]
fn test_will_be_requested_true_when_in_set() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let requested = HashMap::from([("a".to_string(), PartitionSelection::All)]);
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::WillBeRequested, &ctx).fired);
}

#[test]
fn test_will_be_requested_in_dep_pivot() {
    // upstream is in requested_this_tick → WillBeRequested fires in any_deps_match(WillBeRequested) on downstream.
    let up_record = make_materialized_record("upstream", 100);
    let down_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), up_record.clone()),
        ("downstream".to_string(), down_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);
    let requested = HashMap::from([("upstream".to_string(), PartitionSelection::All)]);
    let ctx = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &down_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::any_deps_match(ConditionNode::WillBeRequested);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_will_be_requested_not_in_dep_pivot_when_dep_not_requested() {
    // "upstream" is NOT in requested_this_tick → should not fire
    let up_record = make_materialized_record("upstream", 100);
    let down_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), up_record.clone()),
        ("downstream".to_string(), down_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);
    let ctx = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &down_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let cond = ConditionNode::any_deps_match(ConditionNode::WillBeRequested);
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_any_deps_updated_fires_via_will_be_requested() {
    // any_deps_updated() includes WillBeRequested: an upstream in requested_this_tick
    // fires the composite even without a new dep update (same-tick cascading).
    let up_record = make_materialized_record("upstream", 100);
    let down_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), up_record.clone()),
        ("downstream".to_string(), down_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);
    let requested = HashMap::from([("upstream".to_string(), PartitionSelection::All)]);
    // upstream has prev_state with same timestamp → NewlyUpdated is false
    let up_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([
        ("upstream".to_string(), up_state),
        ("downstream".to_string(), AssetConditionState::default()),
    ]);
    let ctx = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &down_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &all_states,
        requested_this_tick: &requested,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx).fired);
}

#[test]
fn test_any_deps_missing_suppressed_by_will_be_requested() {
    // any_deps_missing() includes & !WillBeRequested: a missing upstream in
    // requested_this_tick does not fire (about to be materialized).
    let up_record = make_record("upstream"); // missing
    let down_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), up_record.clone()),
        ("downstream".to_string(), down_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);
    let requested = HashMap::from([("upstream".to_string(), PartitionSelection::All)]);
    let ctx = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &down_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    // Missing dep, but will be requested → should be false
    assert!(!evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_any_deps_missing_fires_when_dep_not_requested() {
    // When dep is missing and NOT in requested_this_tick, any_deps_missing fires.
    let up_record = make_record("upstream"); // missing
    let down_record = make_materialized_record("downstream", 100);
    let records = HashMap::from([
        ("upstream".to_string(), up_record.clone()),
        ("downstream".to_string(), down_record.clone()),
    ]);
    let deps = HashMap::from([("downstream".to_string(), vec!["upstream".to_string()])]);
    let ctx = EvalContext {
        target_key: "downstream",
        root_key: "downstream",
        target_record: &down_record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_will_be_requested_tree_output() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let requested = HashMap::from([("a".to_string(), PartitionSelection::All)]);
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let (result, tree) = evaluate_with_tree(&ConditionNode::WillBeRequested, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "will_be_requested");
    assert_eq!(tree.status, NodeStatus::True);
}

#[test]
fn test_partitioned_on_cron_no_deps_fires_all_partitions() {
    // Asset with no deps, partitioned, cron tick passes → fires all partitions.
    let empty_partition_statuses = HashMap::new();
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    // Prev eval 16:00, current 16:31 UTC → cron tick at 16:30.
    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_979_460_000_000_000_i64;

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100), (spk("p3"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick_nanos),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now_nanos,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&cond, &ctx);
    assert!(
        result.fired,
        "partitioned on_cron with no deps should fire on cron tick"
    );
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_cron_tick_respects_timezone() {
    use chrono::{TimeZone, Utc};
    // "0 9 * * *" in America/New_York must fire when the NY wall clock crosses 09:00
    // (= 13:00 UTC in EDT), not 09:00 UTC.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let cond = ConditionNode::CronTickPassed {
        cron_schedule: "0 9 * * *".to_string(),
        timezone: Some("America/New_York".to_string()),
    };

    // Window 12:30→13:30 UTC (08:30→09:30 EDT); the 09:00 EDT tick (13:00 UTC) lies inside.
    let prev_tick = Utc
        .with_ymd_and_hms(2026, 6, 16, 12, 30, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let now = Utc
        .with_ymd_and_hms(2026, 6, 16, 13, 30, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick),
        ..Default::default()
    };
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;
    ctx.now = now;

    let result = evaluate(&cond, &ctx);
    assert!(
        result.fired,
        "cron '0 9' in America/New_York must fire at 09:00 EDT (13:00 UTC), not 09:00 UTC"
    );

    // Control: no 09:00 UTC tick in the window → the UTC schedule must not fire (the fire came from the tz).
    let cond_utc = ConditionNode::CronTickPassed {
        cron_schedule: "0 9 * * *".to_string(),
        timezone: None,
    };
    let result_utc = evaluate(&cond_utc, &ctx);
    assert!(
        !result_utc.fired,
        "same window has no 09:00 UTC tick, so the UTC schedule must not fire"
    );
}

#[test]
fn test_cron_tick_across_dst_fallback_terminates_and_fires() {
    use chrono::{TimeZone, Utc};
    // On the DST fall-back day (2025-11-02), a noon schedule must still fire and the call
    // must terminate (the naive-as-UTC croner path can't spin on the fall-back).
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let cond = ConditionNode::CronTickPassed {
        cron_schedule: "0 12 * * *".to_string(),
        timezone: Some("America/New_York".to_string()),
    };
    // 16:00 UTC (11:00 EST) → 17:30 UTC (12:30 EST); noon EST = 17:00 UTC.
    let prev_tick = Utc
        .with_ymd_and_hms(2025, 11, 2, 16, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let now = Utc
        .with_ymd_and_hms(2025, 11, 2, 17, 30, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick),
        ..Default::default()
    };
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;
    ctx.now = now;

    assert!(
        evaluate(&cond, &ctx).fired,
        "noon schedule must fire on the DST fall-back day"
    );
}

#[test]
fn test_partitioned_newly_requested_is_per_partition() {
    // NewlyRequested on a partitioned asset must select only the partitions requested
    // last tick (prev `handled` set), not widen the scalar to every partition.
    let empty_partition_statuses = HashMap::new();
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let cond = ConditionNode::NewlyRequested;

    let p1 = spk("p1");
    let p2 = spk("p2");
    let all_keys = HashSet::from([p1.clone(), p2.clone()]);
    let ts = HashMap::from([(p1.clone(), 100i64), (p2.clone(), 100)]);
    let empty_pk: HashSet<PartitionKey> = HashSet::new();
    let empty_mappings = HashMap::new();
    let no_upstream_keys = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_pk,
        failed: &empty_pk,
        timestamps: &ts,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    // Last tick requested only p1 (handled set), asset-level cursor on the previous tick.
    let prev = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        partition_state: Some(PartitionState {
            handled: HashSet::from([p1.clone()]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;
    ctx.now = 2000;
    ctx.partitions = Some(&pctx);

    let result = evaluate(&cond, &ctx);
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(keys.contains(&p1), "p1 was requested last tick → selected");
            assert!(
                !keys.contains(&p2),
                "p2 was NOT requested last tick → must not be selected (no widening)"
            );
        }
        other => panic!("expected Keys({{p1}}), got {other:?}"),
    }
}

#[test]
fn test_partitioned_on_cron_does_not_fire_without_tick() {
    // No cron tick between evals → on_cron should not fire.
    let empty_partition_statuses = HashMap::new();
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    // Prev eval 16:00, current 16:20 UTC → no cron tick yet.
    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_978_800_000_000_000_i64;

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick_nanos),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &record,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now_nanos,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "partitioned on_cron should not fire without a cron tick"
    );
}

#[test]
fn test_partitioned_on_cron_waits_for_dep_update() {
    // b depends on a (both partitioned); cron tick passes but dep a not updated → on_cron does not fire.
    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let prev_tick_nanos = 1_699_977_600_000_000_000_i64; // 16:00
    let now_nanos = 1_699_979_460_000_000_000_i64; // 16:31

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    // a's partition timestamps haven't changed (dep not updated)
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
        },
    )]);

    // Dep "a" has previous partition state with same timestamps → NewlyUpdated = false
    let a_state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let asset_states = HashMap::from([
        ("a".into(), a_state),
        (
            "b".into(),
            AssetConditionState {
                last_tick_timestamp: Some(prev_tick_nanos),
                last_materialized_timestamp: Some(100),
                ..Default::default()
            },
        ),
    ]);

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    let prev = AssetConditionState {
        last_tick_timestamp: Some(prev_tick_nanos),
        ..Default::default()
    };
    let ctx = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev,
        all_asset_states: &asset_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: now_nanos,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };

    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "partitioned on_cron should wait for dep to be updated"
    );
}

#[test]
fn test_partitioned_on_cron_fires_after_dep_update() {
    // Production-shaped: BOTH root and dep are seeded in `all_asset_states`.
    // Tick 1 crosses the boundary with the dep unchanged (no fire); the dep
    // then updates and tick 2 fires for the updated partitions off the
    // still-armed gate.
    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let t0 = 1_699_977_600_000_000_000_i64; // 16:00
    let t1 = 1_699_979_460_000_000_000_i64; // 16:31 — boundary tick
    let t2 = t1 + 60_000_000_000; // 16:32

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);

    let a_state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut state_b = AssetConditionState {
        last_tick_timestamp: Some(t0),
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);

    let statuses = |ts: i64| {
        HashMap::from([(
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("p1"), ts), (spk("p2"), ts)]),
            },
        )])
    };

    // Tick 1 (boundary): a's partitions unchanged → no fire.
    let statuses_t1 = statuses(100);
    let pctx1 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses_t1,
        dep_root_floor: None,
    };
    let all1 = HashMap::from([
        ("a".to_string(), a_state.clone()),
        ("b".to_string(), state_b.clone()),
    ]);
    let ctx1 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &state_b,
        all_asset_states: &all1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t1,
        is_initial: false,
        partitions: Some(&pctx1),
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert!(
        !r1.fired,
        "boundary tick: dep not updated since the boundary"
    );
    update_condition_state(
        &mut state_b,
        &StateUpdateContext {
            target_record_timestamp: b.last_timestamp,
            target_data_version: b.last_data_version.as_ref(),
            now: t1,
            is_initial: false,
            partition_timestamps: Some(&_ts),
        },
        &r1,
    );

    // Tick 2: a's partitions update after the boundary → fire for p1 and p2.
    let statuses_t2 = statuses(200);
    let pctx2 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses_t2,
        dep_root_floor: None,
    };
    let all2 = HashMap::from([
        ("a".to_string(), a_state.clone()),
        ("b".to_string(), state_b.clone()),
    ]);
    let ctx2 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &state_b,
        all_asset_states: &all2,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t2,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    assert!(
        r2.fired,
        "gate stays armed past the boundary; dep update fires on_cron"
    );
    assert_eq!(
        r2.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]))
    );
}

/// Production-shaped on_cron with deps: BOTH root and dep are seeded in
/// `all_asset_states` and the root's `last_tick_timestamp` advances every tick,
/// exactly as `ConditionPass` wires it. The cron gate must stay armed after the
/// boundary tick so a dep update later in the period fires the condition, then
/// disarm once the root is requested/updated (once per period), and re-arm at
/// the next boundary.
#[test]
fn test_on_cron_with_deps_fires_when_dep_updates_after_boundary() {
    let tree = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    // b (root, on_cron) depends on a. b last ran at ts=100, a at ts=90 → b starts up to date.
    let mut a = make_materialized_record("a", 90);
    let mut b = make_materialized_record("b", 100);
    let deps: HashMap<String, Vec<String>> = HashMap::from([("b".into(), vec!["a".into()])]);

    let t0 = 1_699_977_600_000_000_000_i64; // 2023-11-14 16:00 — before the 16:30 boundary
    let t1 = 1_699_979_460_000_000_000_i64; // 16:31 — first tick past the boundary
    let step = 60_000_000_000_i64;
    let t2 = t1 + step; // 16:32
    let t3 = t2 + step; // 16:33
    let day = 86_400_000_000_000_i64;
    let t4 = t1 + day; // next day 16:31 — next boundary tick
    let t5 = t4 + step; // next day 16:32

    let mut state_b = AssetConditionState {
        last_tick_timestamp: Some(t0),
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let state_a = AssetConditionState {
        last_materialized_timestamp: Some(90),
        ..Default::default()
    };

    let eval_tick = |a: &AssetRecord, b: &AssetRecord, state_b: &AssetConditionState, now: i64| {
        let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
        let all = HashMap::from([
            ("a".to_string(), state_a.clone()),
            ("b".to_string(), state_b.clone()),
        ]);
        let ctx = EvalContext {
            target_key: "b",
            root_key: "b",
            target_record: b,
            cache: CacheSnapshot {
                records: &records,
                upstream_deps: &deps,
                in_progress_assets: &EMPTY_SET,
                failed_assets: &EMPTY_SET,
                failed_asset_timestamps: &EMPTY_FAILED_TS,
                backfill: &EMPTY_BACKFILL,
            },
            tags: empty_tag_snapshot(),
            prev_state: state_b,
            all_asset_states: &all,
            requested_this_tick: &EMPTY_REQUESTED,
            now,
            is_initial: false,
            partitions: None,
            root_partition_floor: None,
        };
        evaluate(&tree, &ctx)
    };
    let advance = |state_b: &mut AssetConditionState, b: &AssetRecord, now: i64, r: &EvalResult| {
        update_condition_state(
            state_b,
            &StateUpdateContext {
                target_record_timestamp: b.last_timestamp,
                target_data_version: b.last_data_version.as_ref(),
                now,
                is_initial: false,
                partition_timestamps: None,
            },
            r,
        );
    };

    // Tick 1 (boundary): the gate arms, but a hasn't updated since b's last run.
    let r1 = eval_tick(&a, &b, &state_b, t1);
    assert!(!r1.fired, "tick 1 (boundary): deps not updated yet");
    advance(&mut state_b, &b, t1, &r1);

    // Tick 2: a materializes after the boundary → the still-armed gate + dep update fire.
    a.last_timestamp = Some(200);
    let r2 = eval_tick(&a, &b, &state_b, t2);
    assert!(
        r2.fired,
        "tick 2: the cron gate must stay armed past the boundary tick so the dep update fires"
    );
    advance(&mut state_b, &b, t2, &r2);
    // Production stamps the handled cursor when the fire is dispatched.
    state_b.last_handled_timestamp = Some(t2);

    // Tick 3: b's run completed (record advanced) → once per period, no re-fire.
    b.last_timestamp = Some(300);
    let r3 = eval_tick(&a, &b, &state_b, t3);
    assert!(
        !r3.fired,
        "tick 3: period already handled — on_cron fires once per cron tick"
    );
    advance(&mut state_b, &b, t3, &r3);

    // Next period: a updates again before the boundary tick.
    a.last_timestamp = Some(400);

    // Tick 4 (next boundary): dep evidence from before the boundary is reset → no fire yet.
    let r4 = eval_tick(&a, &b, &state_b, t4);
    assert!(
        !r4.fired,
        "tick 4 (next boundary): dep must update since THIS boundary before firing"
    );
    advance(&mut state_b, &b, t4, &r4);

    // Tick 5: a is still newer than b → the latch re-arms and the new period fires.
    let r5 = eval_tick(&a, &b, &state_b, t5);
    assert!(
        r5.fired,
        "tick 5: gate re-armed at the new boundary must fire"
    );
}

/// A wall time that repeats during a DST fall-back must fire once, at its
/// first real instant — not again an hour later when the wall clock repeats.
#[test]
fn test_cron_tick_fall_back_repeated_hour_fires_once() {
    let tree = ConditionNode::CronTickPassed {
        cron_schedule: "30 1 * * *".to_string(),
        timezone: Some("America/New_York".to_string()),
    };
    let r = make_materialized_record("r", 100);
    let records = HashMap::from([("r".to_string(), r.clone())]);
    let deps: HashMap<String, Vec<String>> = HashMap::new();

    let eval_window = |prev_secs: i64, now_secs: i64| {
        let prev = AssetConditionState {
            last_tick_timestamp: Some(prev_secs * 1_000_000_000),
            ..Default::default()
        };
        let states: HashMap<String, AssetConditionState> = HashMap::new();
        let ctx = EvalContext {
            target_key: "r",
            root_key: "r",
            target_record: &r,
            cache: CacheSnapshot {
                records: &records,
                upstream_deps: &deps,
                in_progress_assets: &EMPTY_SET,
                failed_assets: &EMPTY_SET,
                failed_asset_timestamps: &EMPTY_FAILED_TS,
                backfill: &EMPTY_BACKFILL,
            },
            tags: empty_tag_snapshot(),
            prev_state: &prev,
            all_asset_states: &states,
            requested_this_tick: &EMPTY_REQUESTED,
            now: now_secs * 1_000_000_000,
            is_initial: false,
            partitions: None,
            root_partition_floor: None,
        };
        evaluate(&tree, &ctx).fired
    };

    // 2024-11-03 America/New_York: clocks fall back 02:00 EDT → 01:00 EST at
    // 06:00Z; wall 01:30 maps to both 05:30Z (EDT) and 06:30Z (EST).
    let first_pass_prev = 1_730_611_785; // 05:29:45Z = 01:29:45 EDT
    assert!(
        eval_window(first_pass_prev, first_pass_prev + 30),
        "the first real instant of wall 01:30 must fire"
    );
    let second_pass_prev = first_pass_prev + 3600; // 06:29:45Z = 01:29:45 EST
    assert!(
        !eval_window(second_pass_prev, second_pass_prev + 30),
        "the repeated wall 01:30 an hour later must not fire again"
    );
}

/// A wall time skipped by spring-forward maps to the first valid instant
/// after the gap instead of silently never firing.
#[test]
fn test_cron_tick_spring_forward_gap_fires_after_gap() {
    let tree = ConditionNode::CronTickPassed {
        cron_schedule: "30 2 * * *".to_string(),
        timezone: Some("America/New_York".to_string()),
    };
    let r = make_materialized_record("r", 100);
    let records = HashMap::from([("r".to_string(), r.clone())]);
    let deps: HashMap<String, Vec<String>> = HashMap::new();

    let eval_window = |prev_secs: i64, now_secs: i64| {
        let prev = AssetConditionState {
            last_tick_timestamp: Some(prev_secs * 1_000_000_000),
            ..Default::default()
        };
        let states: HashMap<String, AssetConditionState> = HashMap::new();
        let ctx = EvalContext {
            target_key: "r",
            root_key: "r",
            target_record: &r,
            cache: CacheSnapshot {
                records: &records,
                upstream_deps: &deps,
                in_progress_assets: &EMPTY_SET,
                failed_assets: &EMPTY_SET,
                failed_asset_timestamps: &EMPTY_FAILED_TS,
                backfill: &EMPTY_BACKFILL,
            },
            tags: empty_tag_snapshot(),
            prev_state: &prev,
            all_asset_states: &states,
            requested_this_tick: &EMPTY_REQUESTED,
            now: now_secs * 1_000_000_000,
            is_initial: false,
            partitions: None,
            root_partition_floor: None,
        };
        evaluate(&tree, &ctx).fired
    };

    // 2024-03-10 America/New_York: 02:00 EST → 03:00 EDT at 07:00Z; wall 02:30
    // does not exist — the occurrence lands at 03:00 EDT = 07:00Z.
    let prev = 1_710_053_985; // 06:59:45Z = 01:59:45 EST
    assert!(
        eval_window(prev, prev + 30),
        "the gap occurrence must fire at the first valid instant after the gap"
    );
    assert!(
        !eval_window(prev - 3600, prev - 3600 + 30),
        "no occurrence lands in the pre-gap window"
    );
}

#[test]
fn test_partitioned_on_cron_partial_dep_update() {
    // Production-shaped: after the boundary tick, only a:p1 updates → on_cron
    // fires for p1 only (identity mapping b:pN ↔ a:pN).
    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let t0 = 1_699_977_600_000_000_000_i64; // 16:00
    let t1 = 1_699_979_460_000_000_000_i64; // 16:31 — boundary tick
    let t2 = t1 + 60_000_000_000; // 16:32

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);

    let a_state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut state_b = AssetConditionState {
        last_tick_timestamp: Some(t0),
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);

    let statuses = |p1_ts: i64, p2_ts: i64| {
        HashMap::from([(
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("p1"), p1_ts), (spk("p2"), p2_ts)]),
            },
        )])
    };

    // Tick 1 (boundary): nothing updated yet.
    let statuses_t1 = statuses(100, 100);
    let pctx1 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses_t1,
        dep_root_floor: None,
    };
    let all1 = HashMap::from([
        ("a".to_string(), a_state.clone()),
        ("b".to_string(), state_b.clone()),
    ]);
    let ctx1 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &state_b,
        all_asset_states: &all1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t1,
        is_initial: false,
        partitions: Some(&pctx1),
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert!(!r1.fired, "boundary tick: nothing updated yet");
    update_condition_state(
        &mut state_b,
        &StateUpdateContext {
            target_record_timestamp: b.last_timestamp,
            target_data_version: b.last_data_version.as_ref(),
            now: t1,
            is_initial: false,
            partition_timestamps: Some(&_ts),
        },
        &r1,
    );

    // Tick 2: only a:p1 updated (ts=200); p2 unchanged.
    let statuses_t2 = statuses(200, 100);
    let pctx2 = PartitionEvalContext {
        all_keys: &_ak,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses_t2,
        dep_root_floor: None,
    };
    let all2 = HashMap::from([
        ("a".to_string(), a_state.clone()),
        ("b".to_string(), state_b.clone()),
    ]);
    let ctx2 = EvalContext {
        target_key: "b",
        root_key: "b",
        target_record: &b,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &state_b,
        all_asset_states: &all2,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t2,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired, "on_cron should fire for p1 whose dep updated");
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1")]))
    );
}

/// Nested dep-of-dep pivot with mismatched key spaces: the bridge floor for an
/// unpartitioned dep must come from the ROOT's own universe, not from the
/// intermediate dep's keys looked up in the root's status (which collapses the
/// floor to None and fires forever).
#[test]
fn test_nested_dep_pivot_floor_uses_root_universe() {
    let cond =
        ConditionNode::any_deps_match(ConditionNode::any_deps_match(ConditionNode::NewlyUpdated));

    // r (date keys) ← m (region keys, AllPartitions mapping) ← u (unpartitioned).
    let r = make_materialized_record("r", 500);
    let m = make_materialized_record("m", 400);
    let mut u = make_materialized_record("u", 100); // older than r's floor of 500
    let deps: HashMap<String, Vec<String>> = HashMap::from([
        ("r".into(), vec!["m".into()]),
        ("m".into(), vec!["u".into()]),
    ]);

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("m".into(), HashSet::from([spk("eu"), spk("us")]))]);
    let mappings = HashMap::from([(
        ("r".into(), "m".into()),
        PartitionMappingKind::AllPartitions,
    )]);

    let statuses = HashMap::from([
        (
            "r".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("d1"), 500), (spk("d2"), 500)]),
            },
        ),
        (
            "m".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("eu"), 400), (spk("us"), 400)]),
            },
        ),
    ]);

    let _ak = HashSet::from([spk("d1"), spk("d2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("d1"), spk("d2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("d1"), 500_i64), (spk("d2"), 500)]);

    let eval_with = |records: HashMap<String, AssetRecord>| {
        let pctx = PartitionEvalContext {
            all_keys: &_ak,
            in_progress: &_ip,
            failed: &_fail,
            timestamps: &_ts,
            resolver: PartitionResolver::new(&mappings, &upstream_keys),
            time_windows: None,
            all_partition_statuses: &statuses,
            dep_root_floor: None,
        };
        let prev = AssetConditionState::default();
        let states: HashMap<String, AssetConditionState> = HashMap::new();
        let ctx = EvalContext {
            target_key: "r",
            root_key: "r",
            target_record: records.get("r").unwrap(),
            cache: CacheSnapshot {
                records: &records,
                upstream_deps: &deps,
                in_progress_assets: &EMPTY_SET,
                failed_assets: &EMPTY_SET,
                failed_asset_timestamps: &EMPTY_FAILED_TS,
                backfill: &EMPTY_BACKFILL,
            },
            tags: empty_tag_snapshot(),
            prev_state: &prev,
            all_asset_states: &states,
            requested_this_tick: &EMPTY_REQUESTED,
            now: 1_000_000_000,
            is_initial: false,
            partitions: Some(&pctx),
            root_partition_floor: None,
        };
        evaluate(&cond, &ctx)
    };

    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("m".to_string(), m.clone()),
        ("u".to_string(), u.clone()),
    ]);
    let result = eval_with(records);
    assert!(
        !result.fired,
        "u (ts=100) is older than r's floor (500): nested newly_updated must not fire"
    );

    // Positive control: u genuinely newer than the root's floor must fire.
    u.last_timestamp = Some(600);
    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("m".to_string(), m.clone()),
        ("u".to_string(), u.clone()),
    ]);
    let result = eval_with(records);
    assert!(
        result.fired,
        "u (ts=600) newer than r's floor (500) must fire the nested condition"
    );
}

#[test]
fn test_update_dep_baselines_stores_partition_timestamps() {
    // update_dep_baselines populates partition_state.timestamps for non-conditioned deps, so NewlyUpdated has a baseline next tick.
    let pk1 = spk("2025-01-01");
    let pk2 = spk("2025-01-02");

    let mut eval_state: HashMap<String, AssetConditionState> = HashMap::new();
    let upstream_deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    let conditioned = HashSet::from(["a".to_string()]); // "a" has condition, "b" does not
    let partition_statuses = HashMap::from([(
        "b".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 300)]),
            ..Default::default()
        },
    )]);
    let records = HashMap::from([("b".to_string(), make_materialized_record("b", 200))]);

    // Before: "b" has no state at all
    assert!(!eval_state.contains_key("b"));

    update_dep_baselines(
        &mut eval_state,
        &["a".to_string()],
        &upstream_deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    // After: "b" has partition timestamps and last_materialized_timestamp
    let b_state = eval_state
        .get("b")
        .expect("b should have state after baseline");
    assert_eq!(b_state.last_materialized_timestamp, Some(200));
    let ps = b_state
        .partition_state
        .as_ref()
        .expect("partition_state should be set");
    assert_eq!(ps.timestamps.get(&pk1), Some(&200));
    assert_eq!(ps.timestamps.get(&pk2), Some(&300));
}

#[test]
fn test_update_dep_baselines_skips_conditioned_assets() {
    // Deps that have their own condition should NOT be touched by update_dep_baselines.
    let mut eval_state: HashMap<String, AssetConditionState> = HashMap::new();
    let upstream_deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    let conditioned = HashSet::from(["a".to_string(), "b".to_string()]); // both conditioned
    let partition_statuses = HashMap::from([(
        "b".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("p1"), 100_i64)]),
            ..Default::default()
        },
    )]);
    let records = HashMap::from([("b".to_string(), make_materialized_record("b", 100))]);

    update_dep_baselines(
        &mut eval_state,
        &["a".to_string()],
        &upstream_deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    // "b" is conditioned → should not have state from baseline
    assert!(!eval_state.contains_key("b"));
}

#[test]
fn test_update_dep_baselines_prevents_newly_updated_false_positive() {
    // End-to-end: without baseline, NewlyUpdated fires; with baseline, it doesn't.
    let pk1 = spk("2025-01-01");
    let pk2 = spk("2025-01-02");

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("a".into(), vec!["b".into()])]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".into(), b_partition_status)]);

    // No baseline → NewlyUpdated false-positives.
    let empty_states: HashMap<String, AssetConditionState> = HashMap::new();

    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 100_i64), (pk2.clone(), 100)]);
    let upstream_keys = HashMap::from([("b".into(), HashSet::from([pk1.clone(), pk2.clone()]))]);

    let mappings = HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::Identity)]);
    let build_ctx = |_states: &HashMap<String, AssetConditionState>| {
        let resolver = PartitionResolver::new(&mappings, &upstream_keys);
        let prev = AssetConditionState {
            last_tick_timestamp: Some(50),
            ..Default::default()
        };
        (resolver, prev)
    };

    // Without baseline
    let (resolver, prev) = build_ctx(&empty_states);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &a,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev,
        all_asset_states: &empty_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        result.fired,
        "without baseline, NewlyUpdated should false-positive"
    );

    // Now apply update_dep_baselines and re-evaluate
    let mut baselined_states: HashMap<String, AssetConditionState> = HashMap::new();
    let conditioned = HashSet::from(["a".to_string()]);
    update_dep_baselines(
        &mut baselined_states,
        &["a".to_string()],
        &deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    let (resolver2, prev2) = build_ctx(&baselined_states);
    let pctx2 = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver: resolver2,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &a,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev2,
        all_asset_states: &baselined_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let result2 = evaluate(&cond, &ctx2);
    assert!(
        !result2.fired,
        "after update_dep_baselines, NewlyUpdated should not false-positive"
    );
}

// ── Phantom run eviction: dispatch inserts run_ids; refresh confirms from storage or evicts after grace ─

/// Helper: memory-backed storage with asset `a` registered and an initialized cache.
async fn pending_test_setup() -> (
    crate::storage::surrealdb_backend::SurrealStorage,
    AssetConditionCache,
) {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[make_materialized_record("a", 1000)])
        .await
        .unwrap();
    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    (storage, cache)
}

#[tokio::test]
async fn test_register_dispatched_run_populates_pending_and_in_progress() {
    let (_storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000, None);
    assert_eq!(
        cache.in_progress_assets.get("a"),
        Some(&HashMap::from([(
            "run-1".to_string(),
            None::<PartitionKey>
        )])),
        "in_progress_assets should hold the run_id with no partition key"
    );
    assert!(
        cache.pending_runs.contains_key("run-1"),
        "pending_runs should hold the run_id"
    );
    assert_eq!(
        cache.pending_runs.get("run-1").unwrap().asset_keys,
        vec!["a".to_string()],
        "pending entry should track the asset"
    );
}

#[tokio::test]
async fn test_pending_run_confirmed_by_storage_clears_pending() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000, None);

    // Storage now reports the run as Started — phantom is no longer phantom.
    storage
        .create_run(&RunRecord {
            run_id: "run-1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    cache.refresh(&storage, 1_000_000).await.unwrap();
    assert!(
        !cache.pending_runs.contains_key("run-1"),
        "pending should be cleared once storage confirms the run"
    );
    // The Started run propagates a fresh in-progress entry, so the asset stays in-progress.
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "asset should remain in-progress while the Started run is live"
    );
}

#[tokio::test]
async fn test_pending_run_evicted_after_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000; // 10 microseconds, easy to exceed
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);

    // Refresh well past the grace window with NO matching run in storage.
    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("phantom-run"),
        "phantom past grace should be removed from pending_runs"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "phantom past grace should also be removed from in_progress_assets so the asset can re-fire"
    );
}

#[tokio::test]
async fn test_pending_eviction_untracks_all_assets_of_a_multi_asset_run() {
    // A phantom joint run must untrack every asset it covered, not just the last.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("b".into(), "joint-run".into(), 1_000_000, None);

    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("joint-run"),
        "phantom joint run should be removed from pending_runs"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "asset a (registered first) must be untracked on phantom eviction"
    );
    assert!(
        !cache.in_progress_assets.contains_key("b"),
        "asset b (registered second) must be untracked on phantom eviction"
    );
}

#[tokio::test]
async fn test_pending_run_not_evicted_within_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 1_000_000_000; // 1 second
    cache.register_dispatched_run("a".into(), "pending-run".into(), 1_000_000, None);

    // Refresh well within the grace window — no eviction yet.
    cache.refresh(&storage, 1_000_000 + 500).await.unwrap();
    assert!(
        cache.pending_runs.contains_key("pending-run"),
        "pending entry within grace should still be tracked"
    );
    assert!(
        cache
            .in_progress_assets
            .get("a")
            .is_some_and(|v| v.contains_key("pending-run")),
        "in_progress entry within grace should remain"
    );
}

#[tokio::test]
async fn test_clear_dispatched_run_rolls_back_failed_dispatch() {
    // A synchronous dispatch failure never reaches storage; the mark must drop
    // immediately, not wait out the phantom-eviction grace.
    let (_storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("b".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("c".into(), "solo-run".into(), 1_000_000, None);

    cache.clear_dispatched_run("a", "joint-run");
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "cleared asset must be untracked immediately"
    );
    assert!(
        cache
            .in_progress_assets
            .get("b")
            .is_some_and(|v| v.contains_key("joint-run")),
        "other assets of the run keep their mark until cleared themselves"
    );
    assert!(
        cache
            .pending_runs
            .get("joint-run")
            .is_some_and(|p| p.asset_keys == vec!["b".to_string()]),
        "pending entry drops only the cleared asset"
    );

    cache.clear_dispatched_run("b", "joint-run");
    assert!(
        !cache.pending_runs.contains_key("joint-run"),
        "pending entry is removed once its last asset is cleared"
    );
    assert!(!cache.in_progress_assets.contains_key("b"));

    cache.clear_dispatched_run("c", "solo-run");
    assert!(!cache.in_progress_assets.contains_key("c"));
    assert!(!cache.pending_runs.contains_key("solo-run"));
}

#[tokio::test]
async fn test_pending_eviction_only_drops_phantom_run_id_not_other_runs() {
    // Two run_ids on one asset — one phantom (grace expired), one real (storage-confirmed);
    // eviction drops only the phantom.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);
    cache.register_dispatched_run("a".into(), "real-run".into(), 1_000_000, None);

    // Confirm `real-run` via storage.
    storage
        .create_run(&RunRecord {
            run_id: "real-run".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();

    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("phantom-run"),
        "phantom-run evicted from pending"
    );
    assert!(
        !cache.pending_runs.contains_key("real-run"),
        "real-run confirmed (cleared from pending)"
    );
    let in_progress = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert!(
        in_progress.contains_key("real-run"),
        "real-run survives in in_progress_assets"
    );
    assert!(
        !in_progress.contains_key("phantom-run"),
        "phantom-run evicted from in_progress_assets"
    );
}

#[tokio::test]
async fn test_pending_eviction_reports_changed_so_eval_runs() {
    // After phantom eviction, refresh must return `true` so the unblocked asset is re-evaluated.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);

    let changed = cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();
    assert!(
        changed,
        "phantom eviction should flip `changed` so the eval pass runs"
    );
}

// ── Tests for the daemon-race fix: cursor backoff + ASC ordering ────────────

/// Helper: memory-backed storage with `a` and `b` registered at `ts`, `b` depending on `a`.
async fn race_test_setup(ts: i64) -> crate::storage::surrealdb_backend::SurrealStorage {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("a", ts),
            make_materialized_record("b", ts),
        ])
        .await
        .unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&GraphTopology {
                nodes: vec![
                    crate::assets::graph::TopologyNode {
                        name: "a".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                    crate::assets::graph::TopologyNode {
                        name: "b".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                ],
                edges: vec![("b".to_string(), "a".to_string())],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    storage
}

fn run_record(
    run_id: &str,
    status: RunStatus,
    start_time: i64,
    node_names: Vec<&str>,
) -> RunRecord {
    RunRecord {
        run_id: run_id.into(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.into(),
        job_name: Some("test".into()),
        status,
        start_time,
        end_time: None,
        tags: vec![],
        node_names: node_names.into_iter().map(String::from).collect(),
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual,
    }
}

/// initial_load backs the cursor off 1ns so the next `get_runs_since(>cursor)` re-includes
/// the run; a Started run must not pile up duplicate in-progress run_ids across both queries.
#[tokio::test]
async fn test_cursor_backoff_doesnt_duplicate_in_progress_entries() {
    let storage = race_test_setup(1000).await;
    let started = run_record("r1", RunStatus::Started, 2000, vec!["a"]);
    storage.create_run(&started).await.unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 5000).await.unwrap();

    let entries = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        entries.keys().filter(|id| id.as_str() == "r1").count(),
        1,
        "run_id should appear exactly once in in_progress_assets despite \
         both initial_load and the cursor-rewound refresh observing the run"
    );
}

/// With two runs for one asset in a single delta, `apply_run_effects_to_delta` does
/// last-write-wins on `last_run_asset_names`; ASC iteration lands the newest run's state
/// last, as `LastRunIncludesTarget` needs.
#[tokio::test]
async fn test_asc_iteration_makes_newest_run_win_per_asset_state() {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();

    // Older "manual bulk" run materializes [a,b]; stamp it on both records so initial_load picks up its state.
    storage
        .create_run(&run_record("old", RunStatus::Success, 2000, vec!["a", "b"]))
        .await
        .unwrap();
    let mut rec_a = make_materialized_record("a", 1000);
    rec_a.last_run_id = Some("old".into());
    let mut rec_b = make_materialized_record("b", 1000);
    rec_b.last_run_id = Some("old".into());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&GraphTopology {
                nodes: vec![
                    crate::assets::graph::TopologyNode {
                        name: "a".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                    crate::assets::graph::TopologyNode {
                        name: "b".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                ],
                edges: vec![("b".to_string(), "a".to_string())],
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(
        cache
            .last_run_asset_names
            .get("a")
            .map(|n| n.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
        vec!["a".to_string(), "b".to_string()],
        "sanity: initial_load reflects the manual bulk run for asset 'a'"
    );

    // After init, a newer schedule run for 'a' lands; the cursor backoff makes both visible to the next refresh.
    storage
        .create_run(&run_record("new", RunStatus::Success, 3000, vec!["a"]))
        .await
        .unwrap();
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv-new".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "new".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 5000).await.unwrap();

    let names_for_a = cache
        .last_run_asset_names
        .get("a")
        .map(|n| n.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    assert_eq!(
        names_for_a,
        vec!["a".to_string()],
        "after both runs are processed in ASC order, the newer schedule \
         run must win the per-asset overwrite; if this fails, \
         LastRunIncludesTarget wrongly reports 'b' was included in a's \
         last run and eager() never fires"
    );
}

/// An older Success + newer Started run in the same refresh: ASC iteration (old first)
/// leaves the asset in-progress — Success clears it, then Started re-adds it.
#[tokio::test]
async fn test_mixed_status_order_started_after_success_lands_in_progress() {
    let storage = race_test_setup(1000).await;
    storage
        .create_run(&run_record(
            "old-success",
            RunStatus::Success,
            2000,
            vec!["a"],
        ))
        .await
        .unwrap();
    storage
        .create_run(&run_record(
            "new-started",
            RunStatus::Started,
            3000,
            vec!["a"],
        ))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 0).await.unwrap();

    let ids = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        ids.keys().cloned().collect::<Vec<_>>(),
        vec!["new-started".to_string()],
        "newer Started run must leave 'a' in-progress after older Success \
         run cleared it; if iteration order flipped, 'a' would incorrectly \
         be reported as not-in-progress"
    );
}

/// A run racing daemon init: `initial_load` sees it as the newest, so without the cursor
/// backoff `get_runs_since(>newest)` excludes it forever; the backoff re-includes it so its terminal state is picked up.
#[tokio::test]
async fn test_cursor_backoff_lets_init_racing_run_be_observed_terminal() {
    let storage = race_test_setup(1000).await;
    storage
        .create_run(&run_record("racing", RunStatus::Started, 2000, vec!["a"]))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 5000).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "after initial load, the racing Started run leaves 'a' in-progress"
    );

    // Run completes — flip to Success and update the asset record.
    storage
        .update_run_status("racing", RunStatus::Success, Some(6000))
        .await
        .unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[make_materialized_record("a", 6000)])
        .await
        .unwrap();

    let changed = cache.refresh(&storage, 7000).await.unwrap();
    assert!(
        changed,
        "completion of the init-racing run must surface as a delta change"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "'a' should be cleared from in-progress after its terminal status \
         is observed; without the cursor backoff + ASC ordering this fails"
    );
}

/// A dep-aggregate must consume a fixed number of node-index slots regardless of dep count,
/// or a trailing stateful node (`NewlyTrue`) drifts index — and since dep changes don't change
/// the fingerprint, the persisted latch is read from the wrong key.
#[test]
fn test_any_deps_aggregate_index_stable_across_dep_count() {
    // `Or` (not `And`): the false aggregate would let `And` short-circuit and skip NewlyTrue;
    // `Or` keeps evaluating so the trailing stateful node records its index.
    let tree = ConditionNode::Or(vec![
        ConditionNode::any_deps_match(ConditionNode::NewlyUpdated),
        ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing)),
    ]);

    // Root newer than its deps so NewlyUpdated is false for every dep, forcing `.any()`
    // to scan all deps (no short-circuit) and expose per-dep counter growth.
    let d = make_materialized_record("d", 200);
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);

    // Two deps.
    let records2 = HashMap::from([
        ("d".to_string(), d.clone()),
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
    ]);
    let deps2 = HashMap::from([("d".to_string(), vec!["a".to_string(), "b".to_string()])]);
    let ctx2 = make_ctx("d", &d, &records2, &deps2);
    let mut keys2: Vec<u32> = evaluate(&tree, &ctx2).sub_results.into_keys().collect();
    keys2.sort_unstable();

    // One dep.
    let records1 = HashMap::from([("d".to_string(), d.clone()), ("a".to_string(), a.clone())]);
    let deps1 = HashMap::from([("d".to_string(), vec!["a".to_string()])]);
    let ctx1 = make_ctx("d", &d, &records1, &deps1);
    let mut keys1: Vec<u32> = evaluate(&tree, &ctx1).sub_results.into_keys().collect();
    keys1.sort_unstable();

    assert_eq!(
        keys1, keys2,
        "NewlyTrue index drifted with dep count: {keys1:?} (1 dep) vs {keys2:?} (2 deps)"
    );
}

/// A zero-dep dep-aggregate must still advance the counter past its inner condition,
/// or a trailing stateful node shifts index when the asset gains its first dep.
#[test]
fn test_all_deps_aggregate_index_stable_with_zero_deps() {
    let tree = ConditionNode::And(vec![
        ConditionNode::all_deps_match(ConditionNode::NewlyUpdated),
        ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing)),
    ]);

    let d = make_record("d");
    let a = make_materialized_record("a", 100);

    // Zero deps.
    let records0 = HashMap::from([("d".to_string(), d.clone())]);
    let deps0 = HashMap::new();
    let ctx0 = make_ctx("d", &d, &records0, &deps0);
    let mut keys0: Vec<u32> = evaluate(&tree, &ctx0).sub_results.into_keys().collect();
    keys0.sort_unstable();

    // One dep.
    let records1 = HashMap::from([("d".to_string(), d.clone()), ("a".to_string(), a.clone())]);
    let deps1 = HashMap::from([("d".to_string(), vec!["a".to_string()])]);
    let ctx1 = make_ctx("d", &d, &records1, &deps1);
    let mut keys1: Vec<u32> = evaluate(&tree, &ctx1).sub_results.into_keys().collect();
    keys1.sort_unstable();

    assert_eq!(
        keys0, keys1,
        "NewlyTrue index drifted: {keys0:?} (0 deps) vs {keys1:?} (1 dep)"
    );
}

/// `on_cron` puts an SR latch inside a dep-aggregate, so each dep's "updated since reset"
/// state must persist per-dep across ticks, or the latch stops firing once the trigger goes false.
#[test]
fn test_dep_aggregate_since_latch_persists_per_dep() {
    let tree = ConditionNode::all_deps_match(
        ConditionNode::NewlyUpdated.since(ConditionNode::ExecutionFailed),
    );

    // dep "a" materialized at 100, with no latch state of its own.
    let a_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        ..Default::default()
    };
    let all_states = HashMap::from([("a".to_string(), a_state)]);
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string()])]);
    let a = make_materialized_record("a", 100);

    // ── Tick 1: root unmaterialized → NewlyUpdated(a) true → latch sets true ──
    let r1 = make_record("r");
    let records1 = HashMap::from([("r".to_string(), r1.clone()), ("a".to_string(), a.clone())]);
    let mut ctx1 = make_ctx("r", &r1, &records1, &deps);
    ctx1.all_asset_states = &all_states;
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: a is newly updated, latch fires");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // Tick 2: root newer than a → NewlyUpdated(a) false, reset false → latch must stay true from tick 1.
    let r2 = make_materialized_record("r", 200);
    let records2 = HashMap::from([("r".to_string(), r2.clone()), ("a".to_string(), a.clone())]);
    let mut ctx2 = make_ctx("r", &r2, &records2, &deps);
    ctx2.prev_state = &state_r;
    ctx2.all_asset_states = &all_states;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        result2.fired,
        "tick 2: the per-dep latch set on tick 1 must persist so the aggregate keeps firing"
    );
}

/// Partitioned twin: each dep's `Since` latch must persist per-partition under the root's
/// partition state; tick 1 latches, tick 2 (trigger/reset false) fires from the persisted latch.
#[test]
fn test_dep_aggregate_partitioned_since_latch_persists_per_dep() {
    let tree = ConditionNode::all_deps_match(
        ConditionNode::NewlyUpdated.since(ConditionNode::ExecutionFailed),
    );

    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let all_keys = HashSet::from([spk("p1"), spk("p2")]);

    // dep "a" materialized @200 on both ticks; "a" itself failing never (reset off).
    let a_status = || crate::condition::cache::PartitionStatusEntry {
        in_progress: HashSet::new(),
        failed: HashSet::new(),
        failed_timestamps: HashMap::new(),
        timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200)]),
    };

    // Tick 1: root b never materialized → NewlyUpdated(a) fires, latch true for both partitions.
    let statuses1 = HashMap::from([("a".to_string(), a_status())]);
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let empty_set: HashSet<PartitionKey> = HashSet::new();
    let pctx1 = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses1,
        dep_root_floor: None,
    };
    let prev1 = AssetConditionState::default();
    let mut ctx1 = make_ctx("b", &b, &records, &deps);
    ctx1.prev_state = &prev1;
    ctx1.partitions = Some(&pctx1);
    ctx1.now = 1000;
    let result1 = evaluate(&tree, &ctx1);
    assert!(
        result1.fired,
        "tick 1: deps updated since reset → latch fires"
    );

    let mut state_b = AssetConditionState::default();
    update_condition_state(
        &mut state_b,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // Tick 2: root b @300 (newer than a@200) → NewlyUpdated(a) false, reset false → per-partition latch must persist.
    let b_status = crate::condition::cache::PartitionStatusEntry {
        in_progress: HashSet::new(),
        failed: HashSet::new(),
        failed_timestamps: HashMap::new(),
        timestamps: HashMap::from([(spk("p1"), 300), (spk("p2"), 300)]),
    };
    let statuses2 = HashMap::from([("a".to_string(), a_status()), ("b".to_string(), b_status)]);
    let b_ts = HashMap::from([(spk("p1"), 300i64), (spk("p2"), 300)]);
    let pctx2 = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &b_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses2,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("b", &b, &records, &deps);
    ctx2.prev_state = &state_b;
    ctx2.partitions = Some(&pctx2);
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        result2.fired,
        "tick 2: per-partition dep latch must persist so the aggregate keeps firing"
    );
    assert_eq!(
        result2.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")])),
        "both partitions stay latched"
    );
}

/// A stateful op two dep-hops deep (`r → x → y`, latch on `y`) must persist under the
/// root's per-dep state (read from the root's dep map, not x's).
#[test]
fn test_nested_dep_aggregate_since_latch_persists() {
    let tree = ConditionNode::any_deps_match(ConditionNode::any_deps_match(
        ConditionNode::NewlyUpdated.since(ConditionNode::ExecutionFailed),
    ));

    let x = make_materialized_record("x", 100);
    let y = make_materialized_record("y", 100);
    let deps = HashMap::from([
        ("r".to_string(), vec!["x".to_string()]),
        ("x".to_string(), vec!["y".to_string()]),
    ]);
    // x and y carry no latch state of their own.
    let all_states = HashMap::from([
        (
            "x".to_string(),
            AssetConditionState {
                last_materialized_timestamp: Some(100),
                ..Default::default()
            },
        ),
        (
            "y".to_string(),
            AssetConditionState {
                last_materialized_timestamp: Some(100),
                ..Default::default()
            },
        ),
    ]);

    // ── Tick 1: root unmaterialized → NewlyUpdated(y) true → latch sets true ──
    let r1 = make_record("r");
    let records1 = HashMap::from([
        ("r".to_string(), r1.clone()),
        ("x".to_string(), x.clone()),
        ("y".to_string(), y.clone()),
    ]);
    let mut ctx1 = make_ctx("r", &r1, &records1, &deps);
    ctx1.all_asset_states = &all_states;
    let result1 = evaluate(&tree, &ctx1);
    assert!(
        result1.fired,
        "tick 1: grandparent y is newly updated, latch fires"
    );

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // Tick 2: root newer than y → NewlyUpdated(y) false, reset false → the grandparent latch
    // must persist (read from the root's per-dep state keyed by y).
    let r2 = make_materialized_record("r", 200);
    let records2 = HashMap::from([
        ("r".to_string(), r2.clone()),
        ("x".to_string(), x.clone()),
        ("y".to_string(), y.clone()),
    ]);
    let mut ctx2 = make_ctx("r", &r2, &records2, &deps);
    ctx2.prev_state = &state_r;
    ctx2.all_asset_states = &all_states;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        result2.fired,
        "tick 2: a latch nested two dep-hops deep must persist under the root's state"
    );
}

/// A dep-aggregate with a stateful inner condition must evaluate every dep even when a
/// sibling short-circuits it, or the skipped dep drops from `dep_sub_results` and
/// `update_condition_state` (full replace) loses its latch. Multi-dep `on_cron` needs each dep's latch independent.
#[test]
fn test_dep_aggregate_short_circuit_preserves_skipped_dep_latch() {
    // `.all()` short-circuits on the first false dep; `a` is first.
    let tree = ConditionNode::all_deps_match(
        ConditionNode::InProgress.since(ConditionNode::ExecutionFailed),
    );
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string(), "b".to_string()])]);

    let both = HashSet::from(["a".to_string(), "b".to_string()]);
    let none: HashSet<String> = HashSet::new();
    let only_a = HashSet::from(["a".to_string()]);

    let r = make_record("r");
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
    ]);

    // ── Tick 1: a and b both in-progress → both deps' Since latch true ──
    let mut ctx1 = make_ctx("r", &r, &records, &deps);
    ctx1.cache.in_progress_assets = &both;
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: both deps in-progress → latch fires");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // Tick 2: a fails → its reset fires → Since(a) false, `.all()` short-circuits at a
    // and skips b; b's latch must survive the skipped tick.
    let mut ctx2 = make_ctx("r", &r, &records, &deps);
    ctx2.prev_state = &state_r;
    ctx2.cache.failed_assets = &only_a;
    ctx2.cache.in_progress_assets = &none;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        !result2.fired,
        "tick 2: a's latch reset by its failure → aggregate false this tick"
    );

    // Built manually (not from_eval_context) to avoid borrowing state_r through ctx2, which would block the &mut below.
    update_condition_state(
        &mut state_r,
        &StateUpdateContext {
            target_record_timestamp: r.last_timestamp,
            target_data_version: r.last_data_version.as_ref(),
            now: 2000,
            is_initial: false,
            partition_timestamps: None,
        },
        &result2,
    );

    // Tick 3: a in-progress again (re-latches), b neither in-progress nor failed → b fires
    // only from its persisted latch, which must survive the tick 2 skip.
    let mut ctx3 = make_ctx("r", &r, &records, &deps);
    ctx3.prev_state = &state_r;
    ctx3.cache.in_progress_assets = &only_a;
    ctx3.now = 3000;
    let result3 = evaluate(&tree, &ctx3);
    assert!(
        result3.fired,
        "tick 3: b's per-dep latch must persist across the tick where a \
         short-circuited the aggregate"
    );
}

/// Two sibling dep-aggregates pivoting on the same dep must merge their per-dep latch maps
/// (each at a distinct node index), not clobber each other via a wholesale insert.
#[test]
fn test_sibling_dep_aggregates_do_not_clobber_shared_dep_latch() {
    // `And` so both aggregates evaluate on tick 1 (Or would short-circuit). Both pivot
    // dep `a` (Agg1 idx 2, Agg2 idx 6), so the per-dep map needs both keys.
    let agg = || {
        ConditionNode::any_deps_match(
            ConditionNode::InProgress.since(ConditionNode::ExecutionFailed),
        )
    };
    let tree = ConditionNode::And(vec![agg(), agg()]);
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string()])]);

    let only_a = HashSet::from(["a".to_string()]);
    let none: HashSet<String> = HashSet::new();
    let r = make_record("r");
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("r".to_string(), r.clone()), ("a".to_string(), a.clone())]);

    // Tick 1: a in-progress → both aggregates latch true; the second write must not drop the first.
    let mut ctx1 = make_ctx("r", &r, &records, &deps);
    ctx1.cache.in_progress_assets = &only_a;
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: a in-progress → both latches set");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // Tick 2: a no longer in-progress, never failed → both aggregates fire only from their
    // persisted latch; Agg1's latch (idx 2) must not be overwritten.
    let mut ctx2 = make_ctx("r", &r, &records, &deps);
    ctx2.prev_state = &state_r;
    ctx2.cache.in_progress_assets = &none;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        result2.fired,
        "tick 2: both sibling aggregates' latches on the shared dep must persist"
    );
}

/// A dep-aggregate nested inside a partitioned root's unpartitioned-dep bool fallback must
/// persist its per-dep latch across ticks (not land in a throwaway accumulator).
#[test]
fn test_nested_dep_aggregate_under_unpartitioned_dep_persists_latch() {
    // Partitioned root r ← unpartitioned b ← c; any_deps_match(any_deps_match(InProgress.since(ExecutionFailed))).
    // Outer pivots to b (bool fallback), inner pivots to c whose Since latch is under test.
    let tree = ConditionNode::any_deps_match(ConditionNode::any_deps_match(
        ConditionNode::InProgress.since(ConditionNode::ExecutionFailed),
    ));
    let p1 = spk("p1");
    let all_keys = HashSet::from([p1.clone()]);

    let r = make_record("r");
    let b = make_materialized_record("b", 100);
    let c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::from([
        ("r".to_string(), vec!["b".to_string()]),
        ("b".to_string(), vec!["c".to_string()]),
    ]);

    let only_c = HashSet::from(["c".to_string()]);
    let none: HashSet<String> = HashSet::new();

    let r_timestamps = HashMap::from([(p1.clone(), 50i64)]);
    let r_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: r_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("r".to_string(), r_status)]);
    let empty_mappings = HashMap::new();
    // `b` absent from upstream_partition_keys → unpartitioned dep → bool fallback.
    let no_upstream_keys = HashMap::new();
    let empty_pk: HashSet<PartitionKey> = HashSet::new();

    let make_pctx = || PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_pk,
        failed: &empty_pk,
        timestamps: &r_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };

    // ── Tick 1: c in-progress → inner Since latches true for c ──
    let mut ctx1 = make_ctx("r", &r, &records, &deps);
    ctx1.cache.in_progress_assets = &only_c;
    let pctx1 = make_pctx();
    ctx1.partitions = Some(&pctx1);
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: c in-progress → nested Since fires");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext {
            target_record_timestamp: r.last_timestamp,
            target_data_version: r.last_data_version.as_ref(),
            now: 1000,
            is_initial: false,
            partition_timestamps: Some(&r_timestamps),
        },
        &result1,
    );

    // Tick 2: c no longer in-progress, never failed → the inner Since fires only from its persisted latch, which must survive.
    let mut ctx2 = make_ctx("r", &r, &records, &deps);
    ctx2.prev_state = &state_r;
    ctx2.cache.in_progress_assets = &none;
    ctx2.now = 2000;
    let pctx2 = make_pctx();
    ctx2.partitions = Some(&pctx2);
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        result2.fired,
        "tick 2: a dep-aggregate nested inside the unpartitioned-dep fallback \
         must persist its per-dep latch"
    );
}

/// A stateful child (Since/NewlyTrue) skipped by an `And`/`Or` short-circuit must keep its
/// latch, or `update_condition_state` (full replace of previous_results) drops it.
#[test]
fn test_and_short_circuit_preserves_stateful_child_latch() {
    // And([gate, trigger.since(reset)]); a false gate short-circuits the And and skips the stateful child.
    let tree = ConditionNode::And(vec![
        ConditionNode::InProgress,
        ConditionNode::ExecutionFailed.since(ConditionNode::Missing),
    ]);
    let deps = HashMap::new();
    let r = make_materialized_record("r", 100); // materialized → Missing (reset) false
    let records = HashMap::from([("r".to_string(), r.clone())]);
    let only_r = HashSet::from(["r".to_string()]);
    let none: HashSet<String> = HashSet::new();

    // Tick 1: gate (InProgress) and trigger (ExecutionFailed) true → Since latches, And fires.
    let mut ctx1 = make_ctx("r", &r, &records, &deps);
    ctx1.cache.in_progress_assets = &only_r;
    ctx1.cache.failed_assets = &only_r;
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: gate + trigger true → And fires");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // ── Tick 2: gate false → And short-circuits and the Since is skipped.
    //    trigger also false this tick ──
    let mut ctx2 = make_ctx("r", &r, &records, &deps);
    ctx2.prev_state = &state_r;
    ctx2.cache.in_progress_assets = &none;
    ctx2.cache.failed_assets = &none;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(!result2.fired, "tick 2: gate false → And false");

    update_condition_state(
        &mut state_r,
        &StateUpdateContext {
            target_record_timestamp: r.last_timestamp,
            target_data_version: r.last_data_version.as_ref(),
            now: 2000,
            is_initial: false,
            partition_timestamps: None,
        },
        &result2,
    );

    // ── Tick 3: gate true again, trigger false → the Since can fire only from
    //    its persisted latch. Pre-fix the latch was dropped on tick 2 ──
    let mut ctx3 = make_ctx("r", &r, &records, &deps);
    ctx3.prev_state = &state_r;
    ctx3.cache.in_progress_assets = &only_r;
    ctx3.cache.failed_assets = &none;
    ctx3.now = 3000;
    let result3 = evaluate(&tree, &ctx3);
    assert!(
        result3.fired,
        "tick 3: a stateful child's latch must persist across a tick where \
         And short-circuited and skipped it"
    );
}

/// A per-dep `Since` whose reset is `CronTickPassed`, evaluated in a dep pivot
/// over an UNCONDITIONED dep, must use the ROOT's last tick as the cron-window
/// boundary. Unconditioned deps never get `last_tick_timestamp` set, so reading
/// the dep's tick gives a zero-width window — the reset never fires, the latch
/// sticks true, and `on_cron` re-fires every cron tick even without fresh data.
#[test]
fn test_cron_reset_in_dep_pivot_uses_root_tick() {
    let cron = ConditionNode::CronTickPassed {
        cron_schedule: "30 16 * * 1-5".to_string(),
        timezone: None,
    };
    let tree = ConditionNode::all_deps_match(ConditionNode::InProgress.since(cron));
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string()])]);
    let r = make_materialized_record("r", 100);
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("r".to_string(), r.clone()), ("a".to_string(), a.clone())]);

    let only_a = HashSet::from(["a".to_string()]);
    let none: HashSet<String> = HashSet::new();

    // Tue 2023-11-14: 16:00 (tick 1) → 16:31 (tick 2); cron tick at 16:30.
    let t1: i64 = 1_699_977_600_000_000_000;
    let t2: i64 = 1_699_979_460_000_000_000;

    // ── Tick 1: dep `a` in-progress → the per-dep Since latches true ──
    let prev1 = AssetConditionState::default();
    let all1: HashMap<String, AssetConditionState> = HashMap::new();
    let ctx1 = EvalContext {
        target_key: "r",
        root_key: "r",
        target_record: &r,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &only_a,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev1,
        all_asset_states: &all1,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t1,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: dep in-progress latches the Since");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext {
            target_record_timestamp: r.last_timestamp,
            target_data_version: r.last_data_version.as_ref(),
            now: t1,
            is_initial: false,
            partition_timestamps: None,
        },
        &result1,
    );
    // The root's own state as seen on the next tick (last_tick = t1).
    let all2: HashMap<String, AssetConditionState> =
        HashMap::from([("r".to_string(), state_r.clone())]);

    // Tick 2: a cron tick (16:30) passed since t1, dep no longer in-progress → the reset
    // must clear the latch → all_deps_match false.
    let ctx2 = EvalContext {
        target_key: "r",
        root_key: "r",
        target_record: &r,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &none,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &state_r,
        all_asset_states: &all2,
        requested_this_tick: &EMPTY_REQUESTED,
        now: t2,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        !result2.fired,
        "tick 2: the cron reset (root's tick boundary) must clear the per-dep \
         Since latch over an unconditioned dep"
    );
}

/// The eval-state blob carries a schema stamp for an explicit migration point (`migrate_loaded`);
/// a pre-versioning blob (no stamp) must read as version 0, not the current version.
#[test]
fn test_condition_eval_state_schema_version_stamps_and_migrates() {
    assert_eq!(
        ConditionEvalState::default().schema_version,
        EVAL_STATE_SCHEMA_VERSION,
        "fresh state must carry the current schema version"
    );

    let mut old: ConditionEvalState = serde_json::from_str("{}").unwrap();
    assert_eq!(
        old.schema_version, 0,
        "a blob written before versioning must load as version 0"
    );
    old.migrate_loaded();
    assert_eq!(old.schema_version, EVAL_STATE_SCHEMA_VERSION);

    let bytes = serde_json::to_vec(&ConditionEvalState::default()).unwrap();
    let round: ConditionEvalState = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        round.schema_version, EVAL_STATE_SCHEMA_VERSION,
        "the stamp must survive a persist round-trip"
    );
}

/// When an asset flips partitioned→unpartitioned with the same tree, the fingerprint is
/// unchanged so `reset_for_new_tree` never runs; `update_condition_state` must itself drop
/// the stale `partition_state` on an unpartitioned eval.
#[test]
fn test_update_condition_state_clears_stale_partition_state_when_unpartitioned() {
    let pk = PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    };
    let mut state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(pk, 100i64)]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // An unpartitioned evaluation result: `sub_selections` is None.
    let result = EvalResult {
        fired: false,
        ..Default::default()
    };
    let ctx = StateUpdateContext {
        target_record_timestamp: Some(200),
        target_data_version: None,
        now: 300,
        is_initial: false,
        partition_timestamps: None,
    };
    update_condition_state(&mut state, &ctx, &result);

    assert!(
        state.partition_state.is_none(),
        "stale partition_state must be cleared on an unpartitioned eval"
    );
}

/// The baseline is bounded by the partition_status snapshot, not the universe: a snapshot
/// key outside the universe (retired/def-change/future-cap) must keep its baseline, or it
/// reads newly-updated forever and re-adding it fires a spurious materialization.
#[test]
fn test_update_condition_state_keeps_baseline_for_snapshot_keys_outside_universe() {
    let live = spk("2024-01-02");
    let stale = spk("2023-12-31");
    let timestamps = HashMap::from([(live.clone(), 100i64), (stale.clone(), 50)]);

    let mut state = AssetConditionState::default();
    let result = EvalResult {
        fired: false,
        sub_selections: Some(HashMap::new()),
        ..Default::default()
    };
    update_condition_state(
        &mut state,
        &StateUpdateContext {
            target_record_timestamp: Some(100),
            target_data_version: None,
            now: 1_000,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        },
        &result,
    );

    let ps = state
        .partition_state
        .expect("partitioned eval must store partition_state");
    assert_eq!(
        ps.timestamps,
        HashMap::from([(live, 100i64), (stale, 50)]),
        "every snapshot key keeps its baseline, in or out of the universe"
    );
}

/// A snapshot key outside the universe (retired/def-change/future-cap) is not evaluable;
/// partitioned `NewlyUpdated` must filter to the universe, or it selects a baseline-less key every tick forever.
#[test]
fn test_partitioned_newly_updated_ignores_keys_outside_universe() {
    let live = spk("2024-01-02");
    let retired = spk("2020-01-01");
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Baseline covers only the live key; the retired key's baseline was never established.
    let mut prev = AssetConditionState::default();
    prev.partition_state = Some(PartitionState {
        timestamps: HashMap::from([(live.clone(), 100i64)]),
        ..Default::default()
    });

    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;

    let all_keys = HashSet::from([live.clone()]);
    let timestamps = HashMap::from([(live.clone(), 100i64), (retired.clone(), 50)]);
    let partition_status = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &partition_status,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::NewlyUpdated, &ctx);
    assert!(
        !result.fired,
        "a snapshot key outside the universe must not fire NewlyUpdated; got {:?}",
        result.selection
    );
}

/// `ConditionEvalState` is persisted via `serde_json`, which can't use a `PartitionKey` as a
/// JSON map key; `PartitionState` timestamps are so keyed, so the state must still round-trip
/// or every restart wipes all latches.
#[test]
fn test_condition_eval_state_round_trips_with_partition_timestamps() {
    let pk = PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    };
    let mut asset = AssetConditionState::default();
    asset.partition_state = Some(PartitionState {
        timestamps: HashMap::from([(pk.clone(), 100i64)]),
        ..Default::default()
    });
    let mut state = ConditionEvalState::default();
    state.assets.insert("a".to_string(), asset);

    let bytes = serde_json::to_vec(&state)
        .expect("ConditionEvalState must serialize via serde_json (storage uses kv_set_json)");
    let round: ConditionEvalState =
        serde_json::from_slice(&bytes).expect("ConditionEvalState must round-trip");
    let ts = round.assets["a"]
        .partition_state
        .as_ref()
        .unwrap()
        .timestamps
        .get(&pk);
    assert_eq!(
        ts,
        Some(&100),
        "partition timestamp must survive the round-trip"
    );
}

/// An old blob missing newer fields must still load with defaults, not fail to deserialize
/// (which would silently reset all latches); guards `#[serde(default)]` coverage.
#[test]
fn test_condition_eval_state_tolerates_missing_fields() {
    // Top-level `is_initial` omitted; asset "a" carries only `is_initial`, every other field absent.
    let json = r#"{"assets":{"a":{"is_initial":true}}}"#;
    let state: ConditionEvalState =
        serde_json::from_str(json).expect("a partial/old blob must load with defaults");
    assert!(
        !state.is_initial,
        "missing top-level is_initial → default false"
    );
    let a = &state.assets["a"];
    assert!(a.is_initial);
    assert!(a.previous_results.is_empty());
    assert_eq!(a.condition_fingerprint, "");
    assert!(a.last_handled_timestamp.is_none());
    assert!(a.partition_state.is_none());
}

/// A pre-pairs-format blob stored `timestamps` as a JSON map (always empty `{}`); the
/// deserializer must accept that legacy shape, or the whole load fails and wipes every latch on upgrade.
#[test]
fn test_condition_eval_state_loads_legacy_map_shaped_timestamps() {
    let json =
        r#"{"assets":{"a":{"previous_results":{"3":true},"partition_state":{"timestamps":{}}}}}"#;
    let state: ConditionEvalState = serde_json::from_str(json)
        .expect("legacy blob with map-shaped empty timestamps must load, not reset all latches");
    let a = &state.assets["a"];
    assert_eq!(
        a.previous_results.get(&3),
        Some(&true),
        "latches must survive the legacy-shape load"
    );
    let ps = a
        .partition_state
        .as_ref()
        .expect("partition_state present in the blob must load");
    assert!(ps.timestamps.is_empty());
}

/// Upgrading past pre-data-version state (baseline last_data_version None, record Some,
/// is_initial false): the partitioned arm must gate on a materialization landing since the
/// last observation, not fire its whole universe.
#[test]
fn test_partitioned_data_version_changed_suppressed_for_pre_versioning_state() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Pre-versioning blob: baseline ts matches the record, version None.
    let mut prev = AssetConditionState::default();
    prev.last_materialized_timestamp = Some(100);
    prev.last_tick_timestamp = Some(900);
    prev.last_data_version = None;

    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;

    let k1 = spk("2024-01-01");
    let all_keys = HashSet::from([k1.clone()]);
    let timestamps = HashMap::from([(k1.clone(), 100i64)]);
    let partition_status = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &partition_status,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    assert!(
        !evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "a missing baseline with no new materialization must not fire the whole universe"
    );

    // A version that appears WITH a fresh materialization is a real change.
    let record2 = make_materialized_record("a", 200);
    let records2 = HashMap::from([("a".to_string(), record2.clone())]);
    let mut ctx = make_ctx("a", &record2, &records2, &deps);
    ctx.prev_state = &prev; // baseline still at 100, version None
    ctx.partitions = Some(&pctx);
    assert!(
        evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "a version appearing with a new materialization must fire"
    );
}

/// An Observation bumps `last_timestamp` (and maybe a data version) without materializing;
/// a never-materialized-but-observed asset must still read as Missing (Missing keys off
/// `last_run_id`, written only by materializations).
#[test]
fn test_missing_true_for_observed_never_materialized_asset() {
    let mut record = make_record("a");
    record.last_timestamp = Some(500); // bumped by the observation
    record.last_data_version = Some("obs-v1".to_string()); // observation-carried
    record.last_run_id = None; // no materialization ever
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(
        evaluate(&ConditionNode::Missing, &ctx).fired,
        "an observed-but-never-materialized asset must still be Missing"
    );

    // And a real materialization (which always records its run) clears it.
    let record = make_materialized_record("a", 600);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let ctx = make_ctx("a", &record, &records, &deps);
    assert!(!evaluate(&ConditionNode::Missing, &ctx).fired);
}

/// `DataVersionChanged` over an unconditioned dep must not re-fire every tick:
/// `update_dep_baselines` must record the dep's `last_data_version`, or the pivot reads
/// prev=None forever and fires despite a stable version.
#[test]
fn test_data_version_changed_baselines_unconditioned_dep() {
    let tree = ConditionNode::any_deps_match(ConditionNode::DataVersionChanged);
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string()])]);
    let r = make_materialized_record("r", 100);
    let a = make_materialized_record("a", 100); // last_data_version = Some("dv_a")
    let records = HashMap::from([("r".to_string(), r.clone()), ("a".to_string(), a.clone())]);
    let no_conditioned: HashSet<String> = HashSet::new();
    let empty_ps = HashMap::new();

    // ── Tick 1: dep `a`'s version is first-seen (prev None) → fires ──
    let mut assets: HashMap<String, AssetConditionState> = HashMap::new();
    let prev1 = AssetConditionState::default();
    let ctx1 = EvalContext {
        target_key: "r",
        root_key: "r",
        target_record: &r,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev1,
        all_asset_states: &assets,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: first-seen dep data version → fires");

    // Baseline deps, as the daemon does after a fired/initial tick.
    update_dep_baselines(
        &mut assets,
        &["r".to_string()],
        &deps,
        &no_conditioned,
        &empty_ps,
        &records,
    );

    // ── Tick 2: `a`'s version is unchanged → must NOT re-fire ──
    let prev2 = AssetConditionState::default();
    let ctx2 = EvalContext {
        target_key: "r",
        root_key: "r",
        target_record: &r,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev2,
        all_asset_states: &assets,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result2 = evaluate(&tree, &ctx2);
    assert!(
        !result2.fired,
        "tick 2: a stable dep data version must not re-fire (baseline missing)"
    );
}

/// The `Or` counterpart: a stateful child skipped because an earlier child made the Or true must keep its latch.
#[test]
fn test_or_short_circuit_preserves_stateful_child_latch() {
    // Or([gate, trigger.since(reset)]); a true gate short-circuits the Or and skips the stateful child.
    let tree = ConditionNode::Or(vec![
        ConditionNode::InProgress,
        ConditionNode::ExecutionFailed.since(ConditionNode::Missing),
    ]);
    let deps = HashMap::new();
    let r = make_materialized_record("r", 100); // materialized → Missing (reset) false
    let records = HashMap::from([("r".to_string(), r.clone())]);
    let only_r = HashSet::from(["r".to_string()]);
    let none: HashSet<String> = HashSet::new();

    // ── Tick 1: gate false, trigger true → Or evaluates the Since (latches true) ──
    let mut ctx1 = make_ctx("r", &r, &records, &deps);
    ctx1.cache.in_progress_assets = &none;
    ctx1.cache.failed_assets = &only_r;
    let result1 = evaluate(&tree, &ctx1);
    assert!(result1.fired, "tick 1: trigger true → Or fires");

    let mut state_r = AssetConditionState::default();
    update_condition_state(
        &mut state_r,
        &StateUpdateContext::from_eval_context(&ctx1),
        &result1,
    );

    // ── Tick 2: gate true → Or short-circuits and skips the Since. trigger false ──
    let mut ctx2 = make_ctx("r", &r, &records, &deps);
    ctx2.prev_state = &state_r;
    ctx2.cache.in_progress_assets = &only_r;
    ctx2.cache.failed_assets = &none;
    ctx2.now = 2000;
    let result2 = evaluate(&tree, &ctx2);
    assert!(result2.fired, "tick 2: gate true → Or fires (from gate)");

    update_condition_state(
        &mut state_r,
        &StateUpdateContext {
            target_record_timestamp: r.last_timestamp,
            target_data_version: r.last_data_version.as_ref(),
            now: 2000,
            is_initial: false,
            partition_timestamps: None,
        },
        &result2,
    );

    // Tick 3: gate false, trigger false → Or fires only if the Since latch persisted across the skipped tick.
    let mut ctx3 = make_ctx("r", &r, &records, &deps);
    ctx3.prev_state = &state_r;
    ctx3.cache.in_progress_assets = &none;
    ctx3.cache.failed_assets = &none;
    ctx3.now = 3000;
    let result3 = evaluate(&tree, &ctx3);
    assert!(
        result3.fired,
        "tick 3: a stateful child's latch must persist across a tick where \
         Or short-circuited and skipped it"
    );
}

/// The Schedule loop stores `next_occurrence` as a UTC instant; a tz-qualified schedule must
/// fire at the declared wall time, and the UTC instant shifts across DST while the wall time stays fixed.
#[test]
fn test_next_cron_occurrence_utc_respects_timezone_and_dst() {
    use chrono::{TimeZone, Utc};
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("0 9 * * *")
        .unwrap();

    // No timezone → evaluated in UTC: next 09:00 UTC.
    let after = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, None),
        Some(Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap()),
    );

    // America/New_York, winter (EST = UTC-5): 09:00 local → 14:00 UTC.
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, Some("America/New_York")),
        Some(Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap()),
        "09:00 EST must be 14:00 UTC, not 09:00 UTC"
    );

    // Same schedule, summer (EDT = UTC-4): 09:00 local → 13:00 UTC (shifts an hour across DST, wall time fixed).
    let summer = Utc.with_ymd_and_hms(2024, 7, 15, 0, 0, 0).unwrap();
    assert_eq!(
        next_cron_occurrence_utc(&cron, summer, Some("America/New_York")),
        Some(Utc.with_ymd_and_hms(2024, 7, 15, 13, 0, 0).unwrap()),
        "09:00 EDT must be 13:00 UTC"
    );
}

/// A spring-forward-skipped wall time (02:30 NY doesn't exist on 2026-03-08) must fire at the
/// first valid instant after the gap (03:00 EDT = 07:00 UTC), not the skipped wall time misread as UTC.
#[test]
fn test_next_cron_occurrence_utc_spring_forward_gap_advances_to_gap_end() {
    use chrono::{TimeZone, Utc};
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("30 2 * * *")
        .unwrap();

    // 2026-03-08 01:00 EST = 06:00 UTC, one wall-clock hour before the gap.
    let after = Utc.with_ymd_and_hms(2026, 3, 8, 6, 0, 0).unwrap();
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, Some("America/New_York")),
        Some(Utc.with_ymd_and_hms(2026, 3, 8, 7, 0, 0).unwrap()),
        "gap occurrence must fire at the first valid wall time after the gap (03:00 EDT)"
    );
}

/// Fall-back repeated hour: when `after` is in the second pass, resolving to the earliest
/// ambiguous instant lands in the past and (no in-flight guard) re-fires every loop;
/// the next occurrence must be strictly after `after`.
#[test]
fn test_next_cron_occurrence_utc_fall_back_never_returns_past_instant() {
    use chrono::{TimeZone, Utc};
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("30 1 * * *")
        .unwrap();

    // 06:05 UTC = 01:05 EST (second pass of the repeated 01:00-02:00 hour); next 01:30 is
    // ambiguous: 01:30 EDT = 05:30 UTC (past) vs 01:30 EST = 06:30 UTC.
    let after = Utc.with_ymd_and_hms(2025, 11, 2, 6, 5, 0).unwrap();
    let next = next_cron_occurrence_utc(&cron, after, Some("America/New_York"))
        .expect("occurrence must exist");
    assert!(
        next > after,
        "next occurrence must be strictly after `after`; got {next} <= {after} \
         (schedule would re-fire on every daemon loop pass)"
    );
    assert_eq!(
        next,
        Utc.with_ymd_and_hms(2025, 11, 2, 6, 30, 0).unwrap(),
        "the first 01:30 wall time after 01:05 EST is 01:30 EST"
    );
}

/// On the root's first tick (`last_tick_timestamp` None), a `CronTickPassed` in a dep pivot
/// must default to a zero-width window (`ctx.now`), not bleed the dep's own `last_tick`, which
/// could span a cron boundary and spuriously fire.
#[test]
fn test_cron_in_dep_pivot_does_not_use_dep_tick_on_root_first_eval() {
    let cron = ConditionNode::CronTickPassed {
        cron_schedule: "30 16 * * 1-5".to_string(),
        timezone: None,
    };
    let tree = ConditionNode::all_deps_match(cron);
    let deps = HashMap::from([("r".to_string(), vec!["a".to_string()])]);
    let r = make_materialized_record("r", 100);
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("r".to_string(), r.clone()), ("a".to_string(), a.clone())]);

    // Tue 2023-11-14: 16:00 (dep's old tick) → 16:31 (now); cron tick at 16:30.
    let t_old: i64 = 1_699_977_600_000_000_000;
    let now: i64 = 1_699_979_460_000_000_000;

    // Dep a is conditioned and last evaluated at t_old; root r has never ticked (last_tick None).
    let dep_state = AssetConditionState {
        last_tick_timestamp: Some(t_old),
        ..Default::default()
    };
    let all_states: HashMap<String, AssetConditionState> =
        HashMap::from([("a".to_string(), dep_state)]);
    let prev_r = AssetConditionState::default();

    let ctx = EvalContext {
        target_key: "r",
        root_key: "r",
        target_record: &r,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev_r,
        all_asset_states: &all_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    let result = evaluate(&tree, &ctx);
    assert!(
        !result.fired,
        "root's first tick: cron window is zero-width (ctx.now), so no cron tick \
         has passed; the dep's old tick must NOT widen the window and fire"
    );
}

/// The baseline must mirror the partition_status snapshot exactly: keys the snapshot no longer
/// contains must leave the persisted baseline too (in-place delta can't drift from replace semantics).
#[test]
fn test_update_condition_state_drops_baseline_keys_missing_from_snapshot() {
    let kept = spk("2024-01-02");
    let gone = spk("2024-01-01");

    let mut state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(kept.clone(), 50i64), (gone.clone(), 40)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    // New snapshot: `gone` vanished, `kept` advanced.
    let timestamps = HashMap::from([(kept.clone(), 100i64)]);
    update_condition_state(
        &mut state,
        &StateUpdateContext {
            target_record_timestamp: Some(100),
            target_data_version: None,
            now: 1_000,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        },
        &EvalResult {
            fired: false,
            sub_selections: Some(HashMap::new()),
            ..Default::default()
        },
    );

    assert_eq!(
        state.partition_state.unwrap().timestamps,
        HashMap::from([(kept, 100i64)]),
        "baseline must equal the snapshot: stale key dropped, kept key advanced"
    );
}

#[tokio::test]
async fn test_initial_load_does_not_floor_asset_materialized_in_failed_joint_run() {
    // A joint run R=[x,y] fails on y but x materialized (its last_run_id → R). On restart,
    // initial_load rebuilds floors from last_run_id; since last_run_id is written only by
    // materializations, an asset whose last_run_id names a failed run materialized in it and must not be floored.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let run_id = "run-joint-fail".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Failure,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    // x materialized in R (advances x.last_run_id to R); y's step failed.
    storage
        .store_events(&[
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv-x2".to_string()),
                },
                asset_key: Some("x".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("y".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();

    // Fresh cache = daemon restart → initial_load.
    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.failed_assets.contains("x"),
        "x materialized in the failed joint run → must not be floored on restart; \
         got failed_assets={:?}",
        cache.failed_assets,
    );
}

/// `SinceLastHandled` debounces the root's dispatch cycle; inside a dep pivot `prev_state` is
/// the dep's state (never written for unconditioned deps), so it must read the root's handled
/// state, not vacuously pass and re-dispatch the tick after the root fired.
#[test]
fn test_since_last_handled_in_dep_pivot_debounces_root_cycle() {
    let record_down = make_record("down");
    let record_up = make_record("up"); // never materialized → Missing is true
    let records = HashMap::from([
        ("down".to_string(), record_down.clone()),
        ("up".to_string(), record_up.clone()),
    ]);
    let deps = HashMap::from([("down".to_string(), vec!["up".to_string()])]);

    let condition = ConditionNode::AnyDepsMatch {
        condition: Box::new(ConditionNode::SinceLastHandled(Box::new(
            ConditionNode::Missing,
        ))),
        label: None,
    };

    // Root fired AND was dispatched on the previous tick: handled == tick.
    let mut root_state = AssetConditionState::default();
    root_state.last_handled_timestamp = Some(1000);
    root_state.last_tick_timestamp = Some(1000);
    let states = HashMap::from([("down".to_string(), root_state.clone())]);

    let mut ctx = make_ctx("down", &record_down, &records, &deps);
    ctx.prev_state = &root_state;
    ctx.all_asset_states = &states;
    assert!(
        !evaluate(&condition, &ctx).fired,
        "the tick after the root was handled must be debounced — a vacuous \
         pass here re-dispatches the root every tick until its run lands"
    );

    // An OLDER handled cycle (handled < last tick) must pass again.
    let mut stale_state = AssetConditionState::default();
    stale_state.last_handled_timestamp = Some(500);
    stale_state.last_tick_timestamp = Some(1000);
    let states = HashMap::from([("down".to_string(), stale_state.clone())]);
    let mut ctx = make_ctx("down", &record_down, &records, &deps);
    ctx.prev_state = &stale_state;
    ctx.all_asset_states = &states;
    assert!(
        evaluate(&condition, &ctx).fired,
        "an older handled cycle must not suppress the trigger"
    );
}

#[tokio::test]
async fn test_completed_run_invalidates_event_less_partitioned_sibling() {
    // Joint keyed run R=[x,y] over p: x materializes p (R enters completed_run_ids), y dies
    // event-less. The completed_run_ids path handles R and must invalidate y so its partition
    // failure (run-status union) reaches partition_status[y].
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["x".to_string(), "y".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-joint-part".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: Some(spk("p")),
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));
    assert!(cache.in_progress_assets.contains_key("y"));

    // R fails: x materialized p (R enters completed_run_ids), y is event-less.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("dv-xp".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: run_id.clone(),
            partition_key: Some(spk("p")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();

    let y_status = cache
        .partition_status
        .get("y")
        .expect("y is a registered partitioned asset");
    assert!(
        y_status.failed.contains(&spk("p")),
        "y's event-less partition failure in the joint run must surface in \
         partition_status via completed-path invalidation; got failed={:?}",
        y_status.failed,
    );
}

#[test]
fn test_partitioned_execution_failed_ignores_keys_outside_universe() {
    // A failed partition retired from the universe stays in partition_status.failed forever but
    // is no longer evaluable; ExecutionFailed/InProgress must filter to all_keys (like NewlyUpdated),
    // or it spams requested_this_tick every tick.
    let live = spk("2024-01-02");
    let retired = spk("2020-01-01");
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let all_keys = HashSet::from([live.clone()]);
    let partition_status = HashMap::new();
    let failed = HashSet::from([retired.clone()]);
    let in_progress = HashSet::from([retired.clone()]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &in_progress,
        failed: &failed,
        timestamps: &HashMap::new(),
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &partition_status,
        dep_root_floor: None,
    };
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.partitions = Some(&pctx);

    assert!(
        !evaluate(&ConditionNode::ExecutionFailed, &ctx).fired,
        "a failed partition outside the universe must not fire ExecutionFailed"
    );
    assert!(
        !evaluate(&ConditionNode::InProgress, &ctx).fired,
        "an in-progress partition outside the universe must not fire InProgress"
    );
}
