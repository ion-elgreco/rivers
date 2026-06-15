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
    // A bare newly_updated() self-condition must NOT fire for an
    // already-materialized asset on the daemon's first/restart tick, where
    // prev_state is empty (no baseline). Pre-fix the (Some, None) self arm
    // returned true unconditionally and re-materialized up-to-date data once
    // per (re)start.
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
    // The boundary the is_initial guard must preserve: on a NON-initial tick a
    // missing baseline means the asset genuinely appeared between ticks → fire.
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
    // NewlyTrue is a pure rising-edge detector — it does NOT have special
    // is_initial behavior. On first tick with no previous results, it fires
    // if the child is true (because previous defaults to false).
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // is_initial=true, child=true, previous=false (no prev results) → fires
    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.is_initial = true;
    let cond = ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing));
    assert!(evaluate(&cond, &ctx).fired);

    // is_initial=false, child=true, previous=false (no prev results) → also fires
    // (this is the key: NewlyTrue doesn't care about is_initial)
    let ctx2 = make_ctx("a", &record, &records, &deps);
    assert!(evaluate(&cond, &ctx2).fired);
}

#[test]
fn test_newly_true_does_not_refire_with_previous_true() {
    // After child was true on previous tick, NewlyTrue should NOT fire
    // regardless of is_initial
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let mut prev = AssetConditionState::default();
    // NewlyTrue is at index 0, child (Missing) at index 1
    // NewlyTrue stores the child's raw value at its own index
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
    // On initial tick with no previous dep timestamps, AnyDepsUpdated uses a
    // heuristic: fire only if dep_ts > target_ts (genuine new data).
    // Here dep "a" at ts=200 is NEWER than target "b" at ts=100 → fires.
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
    // On non-initial tick with no previous dep timestamps (e.g., new dep added),
    // AnyDepsUpdated SHOULD fire — the dep has data but we've never seen it.
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
    // Verify that update_condition_state persists last_data_version,
    // so a subsequent tick with the same version returns false.
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
    let selection = result.selection.unwrap();
    match &selection {
        PartitionSelection::Keys(keys) => assert_eq!(keys.len(), 3),
        _ => panic!("expected Keys selection"),
    }
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
    // Two backfills: one with specific partitions, one with empty (whole-asset).
    // Empty-keys backfill should short-circuit to all partitions regardless of the other.
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
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => assert_eq!(keys.len(), 3),
        _ => panic!("expected Keys selection"),
    }
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
    // Regression: if empty tags were stored in the cache (e.g. from a run with no tags),
    // run_tags_match(&[], &[], &[]) returns true because .all() on empty iterators
    // is vacuously true. The evaluator must return false when the run had no tags,
    // even if a cache entry exists with an empty vec.
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

    // BOTH empty: vacuous truth bug — .all() on empty iters returns true.
    // The defense is at the cache layer (never store empty tags); this
    // documents the known edge case if the guard were ever bypassed.
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

// New semantics: checks if the dep's latest run also included the root asset
// (the top-level asset being evaluated) in its `asset_names`.
// Always false when target_key == root_key (self-referential guard).

#[test]
fn test_last_run_includes_target_true_when_run_includes_root() {
    // Dep "b" was materialized by a joint run [b, c]. root_key is "c".
    // b's latest run asset_names = [b, c] → contains root "c" → true
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
    // Dep "b" was materialized by a solo run [b]. root_key is "c".
    // b's latest run asset_names = [b] → does NOT contain "c" → false
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
    // Primary use case: any_deps_match(newly_updated & ~last_run_includes_target)
    // Evaluating root "a" with deps [b, c].
    // Dep "b": latest run was joint [a, b] → last_run_includes_target=true → filtered
    // Dep "c": latest run was solo [c] → last_run_includes_target=false → included
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
    // Partition-level: pk1's run included root "c", pk2's run did not.
    // target_key="b", root_key="c"
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let materialized = HashSet::from([pk1.clone(), pk2.clone()]);
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
        materialized: &materialized,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
    // The check is on the dep's (target_key's) run, not the root's run.
    // target_key="b", root_key="c". Even if "c" has asset_names containing "b",
    // what matters is "b"'s run asset_names containing "c".
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
    // Scenario: partitioned dep "b" and root "a" were co-materialized at partition
    // pk1 in a joint run. On the next tick, b:pk1 shows as NewlyUpdated but
    // LastRunIncludesTarget should suppress it.
    //
    // This is the core correctness case: without grouping partitioned condition
    // materializations by partition key, each asset gets its own run, and
    // LastRunIncludesTarget would fail to suppress the re-fire.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200); // b was updated (ts > prev)
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 100)]);

    // Joint run: b:pk1 was in a run with [a, b] → includes root "a"
    // Solo run: b:pk2 was in a run with [b] only
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
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    // Dep "b" partition status: both partitions materialized with their timestamps
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: HashSet::from([pk1.clone(), pk2.clone()]),
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
        materialized: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    // any_deps_updated = AnyDepsMatch(NewlyUpdated & !LastRunIncludesTarget | WillBeRequested)
    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1: b:pk1 is NewlyUpdated (200 > 100) BUT LastRunIncludesTarget = true
    //       (joint run [a, b]) → NewlyUpdated & !true = false → suppressed
    // pk2: b:pk2 is NOT NewlyUpdated (100 == 100) → false
    // Neither partition fires any_deps_updated
    assert!(!result.fired, "joint run should suppress re-fire for pk1");
    match result.selection {
        Some(PartitionSelection::Empty) | None => {} // expected
        Some(PartitionSelection::Keys(ref keys)) if keys.is_empty() => {}
        other => panic!("expected empty selection, got {:?}", other),
    }
}

#[test]
fn dep_updated_requires_dep_newer_than_target_key() {
    // Observation baselines lag async event drains: a fire can baseline stale
    // timestamps and the same events re-surface as "updated" a tick later
    // (or, with no baseline at all, every key reads as newly appeared). A dep
    // key counts as updated only while it is strictly newer than the root's
    // own materialization of that key — the dispatched run then advances the
    // root past the dep, making the trigger self-suppressing.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    // Root "a" materialized pk1 at 100 (same joint baseline run as the dep)
    // and pk2 at 100.
    let a_timestamps = HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]);

    // Dep "b": pk1 ts equals the root's (nothing new); pk2 genuinely newer.
    // Its eval-state baseline is EMPTY — the drain-lag shape where every key
    // would read as "newly appeared".
    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: HashSet::from([pk1.clone(), pk2.clone()]),
        timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: HashSet::from([pk1.clone(), pk2.clone()]),
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
        materialized: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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
    // A partitioned root reading an UNPARTITIONED dep takes the bool fallback
    // in eval_partitioned_on_dep. The staleness floor there must be the root's
    // OLDEST partition attempt (min across partitions), not the asset-level max
    // — otherwise a dep update older than the newest partition floors against
    // that newest ts and the genuinely-stale older partitions never re-fire.
    let pk_old = spk("2024-01-01");
    let pk_mid = spk("2024-01-02");
    let pk_new = spk("2024-01-03");
    let all_keys = HashSet::from([pk_old.clone(), pk_mid.clone(), pk_new.clone()]);

    // Root "a": partitions materialized at 10 / 30 / 50; the asset-level record
    // carries the MAX (50) — exactly the value the buggy floor compared against.
    let a = make_materialized_record("a", 50);
    // Dep "b": UNPARTITIONED, updated at 35 — newer than pk_old/pk_mid, older
    // than pk_new.
    let b = make_materialized_record("b", 35);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([
        (pk_old.clone(), 10i64),
        (pk_mid.clone(), 30),
        (pk_new.clone(), 50),
    ]);
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: all_keys.clone(),
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
        materialized: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        latest_time_window_keys: None,
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
    // dep@35 is newer than the older partition attempts (10, 30), so the edge
    // must fire and re-materialize those stale partitions. Pre-fix it floored
    // against the asset-level max (50), got 35 > 50 = false, and starved them.
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
    // AllPartitions floors the dep against the MIN effective ts over the whole
    // downstream universe. A freshly-minted, never-attempted frontier key made
    // root_floor_over return None → the dep counted as "updated" with no
    // upstream change and re-dispatched the entire universe. The floor must
    // ignore never-attempted keys.
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

    // Root "a": d1/d2 materialized at 100, d3 never attempted. Dep "b": u1 at
    // 90 — older than every attempted downstream key, already consumed.
    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_mat = HashSet::from([d1.clone(), d2.clone()]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        materialized: a_mat.clone(),
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        materialized: up_keys.clone(),
        timestamps: HashMap::from([(u1.clone(), 90i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings =
        HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::AllPartitions)]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        materialized: &a_mat,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        latest_time_window_keys: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    // Dep u1@90 is older than every attempted downstream key (100); the new
    // frontier key d3 must not drag the floor to None and broadcast All.
    assert!(
        !result.fired,
        "a never-attempted frontier key must not refire the universe with no \
         upstream change, got {:?}",
        result.selection
    );
}

#[test]
fn all_partitions_dep_genuine_update_still_fires() {
    // The boundary the floor must preserve: when an upstream key IS newer than
    // an attempted downstream key, the AllPartitions edge must still fire.
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
    let a_mat = HashSet::from([d1.clone(), d2.clone()]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        materialized: a_mat.clone(),
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        materialized: up_keys.clone(),
        timestamps: HashMap::from([(u1.clone(), 150i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings =
        HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::AllPartitions)]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        materialized: &a_mat,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        latest_time_window_keys: None,
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
fn dep_updated_floor_compares_mapped_downstream_key() {
    // The staleness floor must compare a dep key against the root's
    // materialization of the DOWNSTREAM key the mapping resolves it to —
    // not the root's unrelated same-named key. With time_window(offset=-1)
    // ("read yesterday's partition"), b@D drives a@(D+1): once a@(D+1) has
    // landed newer than b@D, the trigger must self-suppress.
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
    // Root already consumed both dep updates: a@05 (from b@04) and a@06
    // (from b@05) are newer than their driving dep keys.
    let a_timestamps = HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 400)]);

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: b_keys.clone(),
        timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: a_keys.clone(),
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
        materialized: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        latest_time_window_keys: None,
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

    // Control: the root's mapped key a@06 older than b@05 -> exactly that
    // downstream key fires.
    let a_timestamps_stale =
        HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 200)]);
    let statuses_stale = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: a_keys.clone(),
                timestamps: a_timestamps_stale.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: b_keys.clone(),
                timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_stale = PartitionEvalContext {
        all_keys: &a_keys,
        materialized: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps_stale,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        latest_time_window_keys: None,
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
    // A partitioned upstream whose own condition fired for ONE key this tick
    // must make only the mapped downstream key eligible — the old arm
    // broadcast the whole universe (`requested_this_tick` carried bare asset
    // names), re-introducing the per-partition-contract violation through
    // any_deps_updated's WillBeRequested branch.
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
    // Equal timestamps on both sides: the NewlyUpdated branch stays quiet,
    // isolating WillBeRequested.
    let ts: HashMap<PartitionKey, i64> =
        keys.iter().map(|k| (k.clone(), 100i64)).collect();

    let all_states = HashMap::new();
    let partition_statuses = HashMap::from([
        (
            "down".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys.clone(),
                timestamps: ts.clone(),
                ..Default::default()
            },
        ),
        (
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys.clone(),
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
        materialized: &keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &ts,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_up),
        latest_time_window_keys: None,
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
    // A failed run never advances the root's materialization floor, so
    // without failure awareness a deterministically failing partition is
    // re-dispatched every tick forever. The failure timestamp must raise the
    // floor: suppressed while the failure postdates the dep update, retried
    // exactly when the dep lands something newer.
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
        materialized: keys.clone(),
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
        materialized: &HashSet::new(),
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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

    // Control: the dep lands NEW data (500 > failure 400) -> exactly one
    // retry becomes due again.
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
                materialized: keys.clone(),
                timestamps: HashMap::from([(k.clone(), 500i64)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_retry = PartitionEvalContext {
        all_keys: &keys,
        materialized: &HashSet::new(),
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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
    // Identity dep whose upstream range is a superset of the root's
    // (upstream daily since 2020, downstream added with start=2024): the
    // upstream-only keys can never be dispatched downstream, so they must
    // not count as updated — pre-fix they kept the condition permanently
    // firing phantom selections.
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
        materialized: b_keys.clone(),
        timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 100)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: a_keys.clone(),
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
        materialized: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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
                materialized: a_keys.clone(),
                timestamps: a_timestamps.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: b_keys.clone(),
                timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 150)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_new = PartitionEvalContext {
        all_keys: &a_keys,
        materialized: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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
    // Scenario: partitioned dep "b" was materialized in a solo run (not with root "a").
    // On the next tick, b:pk1 shows NewlyUpdated and should NOT be suppressed.
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
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: HashSet::from([pk1.clone()]),
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
        materialized: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1: b:pk1 is NewlyUpdated (200 > 100) AND LastRunIncludesTarget = false
    //       (solo run [b]) → NewlyUpdated & !false = true → fires
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
    // Two partitions of dep "b": pk1 was in a joint run with root "a",
    // pk2 was in a solo run. Only pk2 should fire any_deps_updated.
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
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        materialized: HashSet::from([pk1.clone(), pk2.clone()]),
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
        materialized: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        latest_time_window_keys: None,
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
    // asset_matches can target an asset that isn't a dep — useful for
    // cross-graph condition checks (e.g. "fire when sibling asset updated")
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
    // asset_matches should preserve root_key (like eval_on_dep does)
    // so LastRunIncludesTarget still checks against the original root
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
        }
        _ => panic!("expected flat Or"),
    }
}

#[test]
fn test_without_removes_matching_child() {
    // eager = SinceLastHandled(...) & !any_deps_missing & !any_deps_in_progress
    //         & !InProgress & !ExecutionFailed
    let eager = ConditionNode::eager();
    let result = eager.without("any_deps_in_progress");
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::InProgress
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_removes_only_exact_match() {
    // "in_progress" should only remove Not(InProgress), not Not(any_deps_in_progress)
    let eager = ConditionNode::eager();
    let result = eager.without("in_progress");
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_on_non_and_is_identity() {
    let leaf = ConditionNode::Missing;
    assert_eq!(leaf.without("in_progress"), ConditionNode::Missing);
}

#[test]
fn test_replace_swaps_matching_node() {
    let eager = ConditionNode::eager();
    let result = eager.replace_by_label("any_deps_updated", &ConditionNode::NewlyUpdated);
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::NewlyUpdated)
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::InProgress
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
    // Not(any_deps_in_progress) becomes Not(InProgress)
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::InProgress
        & !ConditionNode::InProgress
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
    // Drain-lag double fire: the root already ran at 110 reading b@99's
    // durably-written data; b's completion drains into the cache a tick
    // later with a stale fire-time baseline (50). Baselines re-fire; the
    // staleness floor must see the root is already newer and suppress.
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
    // A failed root run consumes the dep update that triggered it: while the
    // failure postdates the dep, re-firing every tick is an unbounded retry
    // loop. A newer dep update retries exactly once more.
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

    // Third tick: Missing still true, inner was true on tick 2 → still does NOT fire
    // (Regression test: previously stored `result` instead of `current`, causing
    //  re-fire every other tick when inner stays continuously true.)
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
    // Regression test: And short-circuit must not shift node indices.
    // Tree: And(InProgress, NewlyTrue(Missing))
    // NewlyTrue is at a fixed index regardless of whether And short-circuits.
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::And(vec![
        ConditionNode::InProgress,
        ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing)),
    ]);

    // Tick 1: InProgress=false → And short-circuits, but NewlyTrue's index
    // must still be assigned (not shifted). Result: false (short-circuited)
    let ctx1 = make_ctx("a", &record, &records, &deps);
    let r1 = evaluate(&cond, &ctx1);
    assert!(!r1.fired);
    // NewlyTrue's index should be 2 (And=0, InProgress=1, NewlyTrue=2, Missing=3)
    // Since And short-circuited, NewlyTrue wasn't evaluated, so no sub_results entry
    assert!(r1.sub_results.is_empty());

    // Tick 2: Make InProgress=true so And doesn't short-circuit.
    // NewlyTrue(Missing) should fire (inner=true, previous=false).
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
    // Regression: CronTickPassed uses last_materialized_timestamp.unwrap_or(0),
    // making the cron window [epoch, now] on first eval — always matches.
    // A "30 16 * * 1-5" schedule should NOT fire at ~22:13 UTC on first eval.
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

    // Previous eval: Tuesday 2023-11-14 16:00 UTC
    // Current eval:  Tuesday 2023-11-14 16:31 UTC
    // The 16:30 cron tick falls between these → should fire.
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
fn test_find_lookback_delta() {
    let cond = ConditionNode::eager(); // eager() doesn't include InLatestTimeWindow
    assert!(cond.find_lookback_delta().is_none());

    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    assert_eq!(cond.find_lookback_delta(), Some(Some(3600.0)));

    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: None,
    };
    assert_eq!(cond.find_lookback_delta(), Some(None));

    let nested = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(7200.0),
        },
    ]);
    assert_eq!(nested.find_lookback_delta(), Some(Some(7200.0)));
}

#[test]
fn test_bug_since_last_handled_refires_after_own_materialization() {
    // Regression: SinceLastHandled checks target.last_timestamp > last_handled_timestamp.
    // After the triggered materialization completes, target.last_timestamp is updated
    // to a time after last_handled_timestamp, causing a spurious re-fire.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Use a child that stays true across ticks:
    // Not(Missing) is always true for a materialized asset.
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
    // Use case: downstream triggers when upstream was requested last tick.
    // Condition: any_deps_match(newly_requested())
    //
    // Tick 1: upstream hasn't been requested → downstream doesn't fire
    // Tick 2: upstream was requested on tick 1 → downstream fires
    // Tick 3: upstream no longer newly_requested → downstream doesn't fire

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

    // Simulate: upstream's condition fired on tick 1 (now=1000)
    // After tick 1: upstream has last_handled_timestamp=1000, last_tick_timestamp=1000
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

    // Tick 3: upstream's last_handled_timestamp=1000 != last_tick_timestamp=2000
    // → no longer newly_requested → downstream doesn't fire
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
    // Use case: code_version_changed().since(newly_requested())
    // Realistic timeline where materialization completes between ticks.
    //
    // Tick 1: code_version=v2, last_materialization_code_version=v1 → fires
    // Between ticks: materialization completes → last_materialization_code_version=v2
    // Tick 2: code versions now match → CodeVersionChanged is false → doesn't fire

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
    // When ticks are faster than materialization, the condition re-fires
    // because CodeVersionChanged is still true (materialization hasn't completed).
    // Users should add & ~InProgress to guard against this.
    //
    // Tick 1: code changed → fires, materialization starts
    // Tick 2: still materializing, newly_requested resets latch → doesn't fire
    // Tick 3: still materializing, no longer newly_requested, code still changed → re-fires

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

    // Tick 3: no longer newly_requested, code still changed → re-fires
    // This spurious re-fire is what & ~InProgress would prevent.
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
    // Use case: since_last_handled() uses newly_requested as part of its
    // debounce logic. Verify that after firing and being handled,
    // the condition doesn't re-fire on the next tick.
    //
    // SinceLastHandled(Not(Missing)):
    // - child is true when asset is materialized
    // - should fire once, then suppress until last_handled_timestamp < last_tick_timestamp

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

    // Warm-up refresh: `initial_load` parks the cursor 1ns before the
    // newest known run so a daemon-startup race doesn't lose its
    // terminal state (see cache.rs::initial_load). That makes the very
    // first delta refresh re-include that run; subsequent ticks settle.
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

    let tmp = std::env::temp_dir().join("rivers_bench_condition_cache");
    let _ = std::fs::remove_dir_all(&tmp);
    let storage =
        crate::storage::surrealdb_backend::SurrealStorage::new_embedded(tmp.to_str().unwrap())
            .await
            .unwrap();

    let keys_100 = setup_storage_bench(&storage, 100).await;
    bench_cache_tick("embedded_100", &storage, &keys_100, 0).await;
    bench_cache_tick("embedded_100", &storage, &keys_100, 3).await;

    let _ = std::fs::remove_dir_all(&tmp);
    eprintln!();
}

// ── Cache: in-progress completion detected as change ────────────────────

#[tokio::test]
async fn test_cache_detects_in_progress_completion_as_change() {
    // Scenario: a → b. Both materialized. A run re-materializes a.
    // Tick 1: cache.refresh detects the run (Started) → has_changes=true, a in in_progress
    // Tick 2: run completes (Success), a's record updated → cache.refresh must return true
    //
    // This tests the bug where b's condition eval was skipped because
    // cache.refresh returned false after a's run completed.

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

    // Simulate a's materialization updating its record
    // (In the real system, the executor writes a new Materialization event
    //  which updates last_timestamp via recompute_staleness)
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
    // Regression: a backfill registers one run per partition, all on the same
    // asset. When the first run completes, refresh must clear only that run —
    // wiping the whole asset reopens the dispatch gate for the still-running
    // siblings, re-firing them as duplicate runs (the flaky "expected 3, got 4
    // successful runs").

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
        .create_runs(&[mk_run("run_a", "a"), mk_run("run_b", "b"), mk_run("run_c", "c")])
        .await
        .unwrap();

    // Tick 1: all three observed Started → tracked under `dst`.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst should be in-progress after dispatch");
    assert_eq!(tracked.len(), 3, "all three backfill runs are tracked");

    // Partition a finishes: its run flips to Success and its materialization
    // lands (advancing `dst`'s asset-record timestamp).
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

    // Tick 2: a's completion is detected. Only run_a may clear — b and c are
    // still running, so `dst` must stay gated against re-dispatch.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst must stay in-progress while runs b and c are still running");
    assert!(
        !tracked.contains(&"run_a".to_string()),
        "the completed run a should be cleared"
    );
    assert!(
        tracked.contains(&"run_b".to_string()),
        "still-running run b must stay tracked"
    );
    assert!(
        tracked.contains(&"run_c".to_string()),
        "still-running run c must stay tracked"
    );
}

#[tokio::test]
async fn test_cache_clears_in_progress_when_run_succeeds_but_timestamp_unchanged() {
    // Root cause of flaky test_schedule_chain_three_layers.
    //
    // Scenario: a → b. Both materialized at ts=1000. A schedule re-materializes
    // a, but a's output is identical (same data), so recompute_staleness does NOT
    // update a's last_timestamp in the asset record.
    //
    // Timeline:
    //   1. cache.refresh() detects the new Started run → a added to in_progress_assets
    //   2. Condition eval: b sees AnyDepsInProgress=true → blocked, doesn't fire
    //   3. Run completes (Success). a's last_timestamp is unchanged (idempotent output).
    //   4. cache.refresh() checks in_progress_assets: re-fetches a's record,
    //      compares last_timestamp → unchanged → a stays in in_progress_assets FOREVER
    //   5. has_changes=false → skip evaluation → b never fires
    //
    // The bug: the in-progress check only looks at last_timestamp changes on the
    // asset record. It should also check if the run status changed from Started
    // to Success/Failure, and remove the asset from in_progress_assets accordingly.

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

    // Run completes with Success, BUT a's record timestamp is NOT updated
    // (idempotent output — same data, recompute_staleness sees no change).
    // The executor DOES write a StepSuccess event for a.
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
    // Deliberately NOT updating a's asset record last_timestamp.
    // This simulates the case where the output is identical.

    // Next tick: cache should detect that a's step completed (via StepSuccess event)
    // and remove it from in_progress_assets, even though last_timestamp didn't change.
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
async fn test_cache_does_not_store_empty_run_tags() {
    // Regression: update_run_tags previously stored empty tag vecs in the cache.
    // With an empty entry, run_tags_match(&[], &[], &[]) returns true (vacuous
    // .all()), so a no-arg LastExecutedWithTags condition would fire on every
    // asset that ever had a completed run — even untagged ones.

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

    // Create a completed run with NO tags (arrives as Success in one tick,
    // simulating a fast run that completes between daemon ticks).
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

    // Cache must NOT have an entry for "a" — empty tags should be skipped
    assert!(
        !cache.last_run_tags.contains_key("a"),
        "cache should not store empty tags; got: {:?}",
        cache.last_run_tags.get("a"),
    );

    // Now create a run WITH tags that arrives as already-completed (Success).
    // This simulates a fast run that completes between ticks.
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
    // A run with no tags should still be recorded in tick_materialization_tags
    // (empty vec), so AllRunsHaveTags can correctly return false.
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
async fn test_has_step_completed_sql_query() {
    // Direct test of has_step_completed SQL query against SurrealDB.
    // Populates events, then verifies the query finds them correctly.

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
            .has_step_completed("a", std::slice::from_ref(&run_id))
            .await
            .unwrap(),
        "should find StepSuccess for 'a' in run-query-test"
    );

    // Test 2: asset "b" in run-query-test → true
    assert!(
        storage
            .has_step_completed("b", std::slice::from_ref(&run_id))
            .await
            .unwrap(),
        "should find StepSuccess for 'b' in run-query-test"
    );

    // Test 3: asset "a" in other-run → true
    assert!(
        storage
            .has_step_completed("a", std::slice::from_ref(&other_run))
            .await
            .unwrap(),
        "should find StepSuccess for 'a' in run-other"
    );

    // Test 4: asset "c" (doesn't exist) → false
    assert!(
        !storage
            .has_step_completed("c", std::slice::from_ref(&run_id))
            .await
            .unwrap(),
        "should NOT find StepSuccess for 'c'"
    );

    // Test 5: asset "a" in non-existent run → false
    assert!(
        !storage
            .has_step_completed("a", &["run-nonexistent".to_string()])
            .await
            .unwrap(),
        "should NOT find StepSuccess in non-existent run"
    );

    // Test 6: asset "a" in multiple run_ids → true (matches first)
    assert!(
        storage
            .has_step_completed("a", &[run_id.clone(), other_run.clone()])
            .await
            .unwrap(),
        "should find StepSuccess for 'a' across multiple run_ids"
    );

    // Test 7: asset "b" in other_run only → false (b only has events in run_id)
    assert!(
        !storage
            .has_step_completed("b", std::slice::from_ref(&other_run))
            .await
            .unwrap(),
        "should NOT find StepSuccess for 'b' in run-other"
    );
}

// ── Partition-aware tests ───────────────────────────────────────────────

/// Owned partition data for tests. Build this, then borrow from it to create PartitionEvalContext.
struct OwnedPartitionData {
    all_keys: HashSet<PartitionKey>,
    materialized: HashSet<PartitionKey>,
    in_progress: HashSet<PartitionKey>,
    failed: HashSet<PartitionKey>,
    timestamps: HashMap<PartitionKey, i64>,
    all_partition_statuses: HashMap<String, crate::condition::cache::PartitionStatusEntry>,
}

impl OwnedPartitionData {
    fn new(all_keys: &[&str], materialized: &[&str], timestamps: &[(&str, i64)]) -> Self {
        Self {
            all_keys: all_keys.iter().map(|s| spk(s)).collect(),
            materialized: materialized.iter().map(|s| spk(s)).collect(),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            timestamps: timestamps.iter().map(|(k, v)| (spk(k), *v)).collect(),
            all_partition_statuses: HashMap::new(),
        }
    }

    fn as_eval_ctx(&self) -> PartitionEvalContext<'_> {
        PartitionEvalContext {
            all_keys: &self.all_keys,
            materialized: &self.materialized,
            in_progress: &self.in_progress,
            failed: &self.failed,
            timestamps: &self.timestamps,
            resolver: PartitionResolver::empty(),
            latest_time_window_keys: None,
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

/// `time_window(offset)` must shift in condition eval exactly as the
/// runtime IO path does — a pass-through here fires conditions on the
/// partition that does NOT read the updated upstream.
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

/// `All - Keys` must resolve to the complement, not fall back to `All` —
/// the fallback re-selects the very partitions the subtraction was meant to
/// drop (e.g. `newly_requested().since_last_handled()` re-selecting handled
/// keys).
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
    // Time-partitioned asset with 5 daily partitions.
    // latest_time_window_keys = {p4, p5} (the 2 most recent).
    // InLatestTimeWindow should select only those.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(
        &["p1", "p2", "p3", "p4", "p5"],
        &["p1", "p2"],
        &[("p1", 100), ("p2", 100)],
    );
    let latest = HashSet::from([spk("p4"), spk("p5")]);
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        materialized: &pdata.materialized,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: Some(&latest),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: None,
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p4"), spk("p5")]))
    );
}

#[test]
fn test_partitioned_in_latest_time_window_empty_when_no_recent() {
    let empty_partition_statuses = HashMap::new();
    // Time-partitioned asset but latest_time_window_keys is empty
    // (e.g., all partitions are in the future).
    let record = make_record("a");
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &[], &[]);
    let latest: HashSet<PartitionKey> = HashSet::new();
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        materialized: &pdata.materialized,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: Some(&latest),
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
fn test_partitioned_in_latest_time_window_static_partitions_selects_all() {
    // Static (non-time) partitions: latest_time_window_keys is None → all keys.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["us", "eu", "ap"], &["us"], &[("us", 100)]);
    let pctx = pdata.as_eval_ctx(); // latest_time_window_keys = None
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let cond = ConditionNode::InLatestTimeWindow {
        lookback_delta: Some(3600.0),
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(pdata.all_keys.clone())
    );
}

#[test]
fn test_partitioned_in_latest_time_window_combined_with_missing() {
    let empty_partition_statuses = HashMap::new();
    // Realistic pattern: InLatestTimeWindow & Missing
    // 5 daily partitions, latest 2 in window, p1 materialized, p4+p5 in window.
    // Missing = {p2, p3, p4, p5}, InLatestTimeWindow = {p4, p5}
    // AND → {p4, p5} (only missing partitions that are also in the latest window)
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3", "p4", "p5"], &["p1"], &[("p1", 100)]);
    let latest = HashSet::from([spk("p4"), spk("p5")]);
    let pctx = PartitionEvalContext {
        all_keys: &pdata.all_keys,
        materialized: &pdata.materialized,
        in_progress: &pdata.in_progress,
        failed: &pdata.failed,
        timestamps: &pdata.timestamps,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: Some(&latest),
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    // InLatestTimeWindow & Missing — only the latest missing partitions
    let cond = ConditionNode::And(vec![
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        },
        ConditionNode::Missing,
    ]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    // p4 and p5 are both in the latest window AND missing
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p4"), spk("p5")]))
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
    // Code version change affects ALL partitions
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]))
    );
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
fn test_partitioned_and() {
    // And(Missing, Not(InProgress))
    // 3 partitions: p1 materialized, p2 missing, p3 missing+in_progress
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
    // b depends on a. a has 3 partitions, all materialized.
    // b has same 3 partitions with identity mapping.
    // AnyDepsMissing on b should NOT fire (a is fully materialized).
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
            materialized: HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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

    // AnyDepsMissing evaluates Missing on upstream "a" which has
    // all_keys = {p1, p2, p3} and materialized via the resolver's upstream_partition_keys.
    // Since a is not StaleStatus::Missing, the upstream pctx has all keys materialized,
    // so the result should be empty.
    let result = evaluate(&ConditionNode::any_deps_missing(), &ctx);
    // The upstream "a" record is not Missing at the asset level, and the
    // eval_partitioned_on_dep builds a pctx with all upstream keys materialized
    // (since stale_status != Missing). So no missing partitions.
    assert!(!result.fired);
}

#[test]
fn test_partitioned_all_deps_match_not_missing() {
    // b depends on a (both partitioned). AllDepsMatch(Not(Missing)) on b.
    // a is fully materialized → all partitions satisfy Not(Missing).
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
            materialized: HashSet::from([spk("p1"), spk("p2")]),
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
        materialized: &_mat2,
        in_progress: &_ip2,
        failed: &_fail2,
        timestamps: &_ts2,
        resolver,
        latest_time_window_keys: None,
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
    // Upstream u2 maps to downstream d2
    let sel2 = PartitionSelection::Keys(HashSet::from([spk("u2")]));
    assert_eq!(
        m.map_to_downstream(&sel2),
        PartitionSelection::Keys(HashSet::from([spk("d2")]))
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
    assert_eq!(resolver.map_downstream("a", "b", &sel), sel);
}

#[test]
fn test_partition_resolver_no_mapping_is_identity() {
    // No mapping registered for edge (c, d) → identity passthrough
    let resolver = PartitionResolver::empty();
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    assert_eq!(resolver.map_downstream("d", "c", &sel), sel);
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
    // Daily pipeline: raw_events → cleaned_events (eager)
    // raw_events has p1, p2, p3 materialized. cleaned_events has p1, p2 materialized.
    // eager() on cleaned_events should select p3 only.
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
            materialized: HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
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
        materialized: &_mat3,
        in_progress: &_ip3,
        failed: &_fail3,
        timestamps: &_ts3,
        resolver,
        latest_time_window_keys: None,
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
    // Upstream "eu" → downstream "europe"
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "eu"),
    ])]));
    let down = m.map_to_downstream(&up);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([mpk(&[
            ("date", "2024-01-01"),
            ("region", "europe")
        ])]))
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

    // Upstream multi key has region=us → downstream single "north" (reverse of static)
    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let downstream = m.map_to_downstream(&upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("north")]))
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
    assert_eq!(resolver.map_downstream("up", "down", &sel), sel);
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
    let downstream = resolver.map_downstream("multi_up", "single_down", &upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("2024-01-01")]))
    );
}

// ── Comprehensive partition evaluation scenarios ────────────────────────

#[test]
fn test_partitioned_eager_partial_upstream_update() {
    // Daily pipeline: raw → processed (eager)
    // raw has p1,p2,p3 materialized. processed has p1,p2 materialized.
    // raw p3 was just materialized (timestamp newer than prev).
    // eager() should select p3 for processed.
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

    // raw_state has partition_state with p1, p2 already known at ts=200
    // so only p3 (newly materialized at ts=200) will be "NewlyUpdated"
    let raw_state = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200)]),
            handled: HashSet::new(),
        }),
        ..Default::default()
    };
    let asset_states = HashMap::from([("raw".into(), raw_state)]);

    // Upstream "raw" partition status: all 3 partitions materialized
    let partition_statuses = HashMap::from([(
        "raw".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            materialized: HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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
    // Regression test: upstream has 3 of 5 partitions materialized.
    // eager() on the downstream (never materialized) should ONLY fire for
    // the 3 partitions that have upstream data, NOT all 5.
    // The !AnyDepsMissing clause must use actual per-partition upstream status.
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
            materialized: HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200), (spk("p3"), 200)]),
        },
    )]);

    // Downstream "processed" has never been materialized
    let empty_mat: HashSet<PartitionKey> = HashSet::new();
    let empty_ip: HashSet<PartitionKey> = HashSet::new();
    let empty_fail: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_partitions,
        materialized: &empty_mat,
        in_progress: &empty_ip,
        failed: &empty_fail,
        timestamps: &empty_ts,
        resolver,
        latest_time_window_keys: None,
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
    // on_missing() should only fire for partitions that are missing
    // AND all upstream deps are not missing for those partitions.
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
            materialized: HashSet::from([spk("p1"), spk("p2"), spk("p3")]),
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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
    // And(Missing, Not(InProgress)): p2 is missing, p3 is missing+in_progress
    // Result should be {p2} only.
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };

    let pctx_ref = &pctx;
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, pctx_ref);
    let result = evaluate(&ConditionNode::CodeVersionChanged, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(_ak.clone())
    );
}

#[test]
fn test_partitioned_since_latch_per_partition() {
    let empty_partition_statuses = HashMap::new();
    // Since { trigger: Missing, reset: NewlyUpdated }
    // Tick 1: p2,p3 missing → trigger fires for {p2,p3}, latch = {p2,p3}
    // Tick 2: p2 materialized (NewlyUpdated), p3 still missing →
    //   trigger = {p3}, reset = {p2}, latch = ({p2,p3} ∪ {p3}) - {p2} = {p3}
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
        materialized: &_mat1,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts1,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
        materialized: &_mat2,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts2,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
    // NewlyTrue(Missing): only partitions that BECAME missing this tick.
    // Tick 1: p2,p3 missing → NewlyTrue = {p2,p3} (first tick)
    // Tick 2: still p2,p3 missing → NewlyTrue = {} (no change)
    // Tick 3: p4 added to all_keys and is missing → NewlyTrue = {p4}
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
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
    // Or(Missing, ExecutionFailed) & Not(InProgress)
    // p1: materialized, p2: missing, p3: failed, p4: missing+in_progress
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3"), spk("p4")]);
    let _mat = HashSet::from([spk("p1"), spk("p3")]);
    let _ip = HashSet::from([spk("p4")]);
    let _fail = HashSet::from([spk("p3")]);
    let _ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
    // Missing = {p2, p4}, Failed = {p3}, Or = {p2, p3, p4}
    // Not(InProgress) = {p1, p2, p3}
    // And = {p2, p3}
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

// ── Bug: eager should not fire on first daemon tick when all assets are UpToDate ──

#[test]
fn test_eager_does_not_fire_when_all_up_to_date_first_tick() {
    // Scenario: a → b → c, all materialized (UpToDate).
    // Daemon starts fresh (no previous state, is_initial=true).
    // Eager condition on b and c should NOT fire — nothing changed.
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

    // Second tick: is_initial=false. Nothing changed — a's timestamp is the same.
    // Should still not fire.
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
    // Scenario: ext_feed (observed) → aggregated (eager) → report (eager)
    // Initial state: ext_feed observed at ts=100, aggregated materialized at ts=200, report at ts=300.
    // Daemon starts (tick 1, is_initial=true): nothing should fire (all up-to-date).
    // Then ext_feed gets re-observed at ts=500 (on_cron fires).
    // Tick 2 (is_initial=false): aggregated should fire (ext_feed updated).
    // After aggregated materializes at ts=600:
    // Tick 3: report should fire (aggregated updated).

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
    // Scenario: a → b, both materialized at same time.
    // Schedule re-materializes a. While a is in-progress, b should NOT fire
    // (blocked by AnyDepsInProgress). After a completes with a new timestamp,
    // b SHOULD fire on the next tick.
    //
    // This tests the bug where dep state gets updated while in-progress,
    // silently consuming the change so it's not detected on the next tick.

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
        "any_deps_match(...)"
    );
    assert_eq!(
        ConditionNode::all_deps_match(ConditionNode::Missing).node_label(),
        "all_deps_match(...)"
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
    // Asset "downstream" depends on "upstream".
    // "upstream" is in requested_this_tick → WillBeRequested should fire
    // when evaluating any_deps_match(WillBeRequested) on "downstream".
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
    // any_deps_updated() includes WillBeRequested: when upstream is in
    // requested_this_tick, the composite fires even if the dep wasn't
    // newly updated (same-tick cascading).
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
    // any_deps_missing() includes & !WillBeRequested: when upstream is
    // missing but in requested_this_tick, the composite does NOT fire
    // (the dep is about to be materialized).
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

    // Previous eval: Tue 2023-11-14 16:00 UTC
    // Current eval:  Tue 2023-11-14 16:31 UTC → cron tick at 16:30
    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_979_460_000_000_000_i64;

    let _ak = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _mat = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100), (spk("p3"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
fn test_partitioned_on_cron_does_not_fire_without_tick() {
    // No cron tick between evals → on_cron should not fire.
    let empty_partition_statuses = HashMap::new();
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    // Previous eval: Tue 2023-11-14 16:00 UTC
    // Current eval:  Tue 2023-11-14 16:20 UTC → no cron tick yet
    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_978_800_000_000_000_i64;

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver: PartitionResolver::empty(),
        latest_time_window_keys: None,
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
    // b depends on a (both partitioned). Cron tick passes but dep "a" has NOT
    // been updated since the cron tick → on_cron should NOT fire.
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
            materialized: HashSet::from([spk("p1"), spk("p2")]),
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
    let asset_states = HashMap::from([("a".into(), a_state)]);

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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
    // b depends on a (both partitioned). Cron tick passes AND dep "a" has been
    // updated since the cron tick → on_cron fires for updated partitions.
    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let prev_tick_nanos = 1_699_977_600_000_000_000_i64; // 16:00
    let now_nanos = 1_699_979_460_000_000_000_i64; // 16:31

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    // a's partitions have new timestamps (updated after cron tick)
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            materialized: HashSet::from([spk("p1"), spk("p2")]),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 200)]),
        },
    )]);

    // Dep "a" has previous partition state with old timestamps → NewlyUpdated detects change
    let a_state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let asset_states = HashMap::from([("a".into(), a_state)]);

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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
        result.fired,
        "partitioned on_cron should fire when dep updated after cron tick"
    );
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]))
    );
}

#[test]
fn test_partitioned_on_cron_partial_dep_update() {
    // b depends on a (both partitioned, p1 and p2). Cron tick passes but only
    // a:p1 is updated → on_cron should NOT fire (AllDepsMatch requires all partitions).
    let cond = ConditionNode::on_cron("30 16 * * 1-5".to_string(), None);

    let a = make_materialized_record("a", 200);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("b".into(), vec!["a".into()])]);

    let prev_tick_nanos = 1_699_977_600_000_000_000_i64;
    let now_nanos = 1_699_979_460_000_000_000_i64;

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2")]))]);
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    // Only p1 updated (ts=200), p2 not updated (ts=100, same as prev)
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            materialized: HashSet::from([spk("p1"), spk("p2")]),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            failed_timestamps: HashMap::new(),
            timestamps: HashMap::from([(spk("p1"), 200), (spk("p2"), 100)]),
        },
    )]);

    // Dep "a" has previous partition state with old timestamps
    let a_state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(spk("p1"), 100), (spk("p2"), 100)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let asset_states = HashMap::from([("a".into(), a_state)]);

    let _ak = HashSet::from([spk("p1"), spk("p2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("p1"), spk("p2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("p1"), 100_i64), (spk("p2"), 100)]);
    let pctx = PartitionEvalContext {
        all_keys: &_ak,
        materialized: &_mat,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &_ts,
        resolver,
        latest_time_window_keys: None,
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
    // AllDepsMatch uses identity mapping: b:p1 ↔ a:p1, b:p2 ↔ a:p2.
    // a:p1 updated, a:p2 not → b:p1 fires, b:p2 does not.
    // on_cron fires for p1 only.
    assert!(result.fired, "on_cron should fire for p1 whose dep updated");
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p1")]))
    );
}

#[test]
fn test_update_dep_baselines_stores_partition_timestamps() {
    // Verify that update_dep_baselines populates partition_state.timestamps
    // for non-conditioned deps, so NewlyUpdated has a baseline on the next tick.
    let pk1 = spk("2025-01-01");
    let pk2 = spk("2025-01-02");

    let mut eval_state: HashMap<String, AssetConditionState> = HashMap::new();
    let upstream_deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    let conditioned = HashSet::from(["a".to_string()]); // "a" has condition, "b" does not
    let partition_statuses = HashMap::from([(
        "b".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            materialized: HashSet::from([pk1.clone(), pk2.clone()]),
            timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 300)]),
            ..Default::default()
        },
    )]);
    let records = HashMap::from([("b".to_string(), make_materialized_record("b", 200))]);

    // Before: "b" has no state at all
    assert!(!eval_state.contains_key("b"));

    update_dep_baselines(
        &mut eval_state,
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
        materialized: HashSet::from([pk1.clone(), pk2.clone()]),
        timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".into(), b_partition_status)]);

    // Simulate: no baseline → NewlyUpdated fires (the bug)
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
        materialized: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver,
        latest_time_window_keys: None,
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
        &deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    let (resolver2, prev2) = build_ctx(&baselined_states);
    let pctx2 = PartitionEvalContext {
        all_keys: &all_keys,
        materialized: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver: resolver2,
        latest_time_window_keys: None,
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

// ── Phantom run eviction: dispatch eagerly inserts run_ids; refresh confirms ─
//                         from storage or evicts after grace period.

/// Helper: build a memory-backed storage with one asset record `a`
/// already registered, and an initialized cache.
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
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000);
    assert_eq!(
        cache.in_progress_assets.get("a").map(|v| v.as_slice()),
        Some(["run-1".to_string()].as_slice()),
        "in_progress_assets should hold the run_id"
    );
    assert!(
        cache.pending_runs.contains_key("run-1"),
        "pending_runs should hold the run_id"
    );
    assert_eq!(
        cache.pending_runs.get("run-1").unwrap().asset_key,
        "a",
        "pending entry should track the asset"
    );
}

#[tokio::test]
async fn test_pending_run_confirmed_by_storage_clears_pending() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000);

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
    // The Started run propagates a fresh in-progress entry from refresh, so
    // the asset stays marked in-progress (not the same data, but same shape).
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "asset should remain in-progress while the Started run is live"
    );
}

#[tokio::test]
async fn test_pending_run_evicted_after_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000; // 10 microseconds, easy to exceed
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000);

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
async fn test_pending_run_not_evicted_within_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 1_000_000_000; // 1 second
    cache.register_dispatched_run("a".into(), "pending-run".into(), 1_000_000);

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
            .is_some_and(|v| v.contains(&"pending-run".to_string())),
        "in_progress entry within grace should remain"
    );
}

#[tokio::test]
async fn test_pending_eviction_only_drops_phantom_run_id_not_other_runs() {
    // Two run_ids on the same asset: one phantom (registered, never confirmed,
    // grace expired), one real (registered, then confirmed by storage). The
    // eviction must drop only the phantom, leaving the real run untouched.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000);
    cache.register_dispatched_run("a".into(), "real-run".into(), 1_000_000);

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
        in_progress.contains(&"real-run".to_string()),
        "real-run survives in in_progress_assets"
    );
    assert!(
        !in_progress.contains(&"phantom-run".to_string()),
        "phantom-run evicted from in_progress_assets"
    );
}

#[tokio::test]
async fn test_pending_eviction_reports_changed_so_eval_runs() {
    // After phantom eviction, refresh must return `true` so the engine
    // re-evaluates conditions on the asset that just unblocked.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000);

    let changed = cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();
    assert!(
        changed,
        "phantom eviction should flip `changed` so the eval pass runs"
    );
}

// ── Tests for the daemon-race fix: cursor backoff + ASC ordering ────────────

/// Helper: build memory-backed storage + an asset record `a` and `b`
/// already registered at the given timestamp, with `b` depending on `a`.
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

/// When `initial_load` finds an existing run, it backs the cursor off by
/// 1ns so the *next* `get_runs_since(>cursor)` re-includes that run. If
/// the run was Started, the in-progress entry from `initial_load`'s
/// Started-filter query and the entry from the subsequent
/// `get_runs_since` must not pile up duplicate run_ids.
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
        entries.iter().filter(|id| id.as_str() == "r1").count(),
        1,
        "run_id should appear exactly once in in_progress_assets despite \
         both initial_load and the cursor-rewound refresh observing the run"
    );
}

/// The actual bug: when the cache picks up two runs for the same asset
/// in a single delta (e.g. an old multi-asset run already known at init
/// + a newer single-asset run created concurrently with init),
/// `apply_run_effects_to_delta` does last-write-wins per-asset on
/// `last_run_asset_names`. Iterating ASC means the NEWEST run's state
/// lands last — which is what `LastRunIncludesTarget` needs.
#[tokio::test]
async fn test_asc_iteration_makes_newest_run_win_per_asset_state() {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();

    // Older "manual bulk" run materializes [a, b] together. Stamp it on
    // both asset records so initial_load picks up its state.
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

    // After init, a newer schedule run for just 'a' lands. The cursor
    // backoff makes both runs visible to the next delta refresh.
    storage
        .create_run(&run_record("new", RunStatus::Success, 3000, vec!["a"]))
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

/// When an older Success run and a newer Started run for the same asset
/// land in the same refresh, ASC iteration (old first, new last) leaves
/// the asset correctly *in* in-progress: the Success run clears it, then
/// the Started run re-adds it. The opposite order would leave the asset
/// not-in-progress while a run is still executing.
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
        ids.as_slice(),
        &["new-started".to_string()],
        "newer Started run must leave 'a' in-progress after older Success \
         run cleared it; if iteration order flipped, 'a' would incorrectly \
         be reported as not-in-progress"
    );
}

/// A run created concurrently with daemon init (the original failing
/// scenario): `initial_load`'s `get_runs(1)` sees the new Started run as
/// "the newest", so without the cursor backoff the next refresh's
/// `get_runs_since(>newest.start_time)` excludes it forever. With the
/// backoff, the next refresh re-includes it — and (if it's already
/// terminal) the cache picks up its terminal state.
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
