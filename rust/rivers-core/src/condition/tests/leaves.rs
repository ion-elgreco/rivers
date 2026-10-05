use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
fn test_skipped_dep_aggregate_is_leaf_like_evaluated() {
    // V-34: a short-circuited dep-aggregate must render as a childless leaf in
    // the eval tree, exactly like an evaluated one — otherwise the persisted UI
    // tree's arity flips between ticks (evaluated=leaf vs skipped=expanded).
    let record = make_materialized_record("a", 100); // materialized → Missing is false
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    // Missing is false and the inner condition is non-stateful, so And
    // short-circuits and the dep-aggregate is emitted via build_skipped_subtree.
    let cond = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::AnyDepsMatch {
            condition: Box::new(ConditionNode::NewlyUpdated),
            label: None,
        },
    ]);
    let (_result, tree) = evaluate_with_tree(&cond, &ctx);
    assert_eq!(
        tree.children.len(),
        2,
        "And keeps both children in the tree"
    );
    assert_eq!(tree.children[1].status, NodeStatus::Skipped);
    assert!(
        tree.children[1].children.is_empty(),
        "a skipped dep-aggregate must be a childless leaf like an evaluated one; got {} children",
        tree.children[1].children.len()
    );
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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

/// An unpartitioned asset whose data was deleted must read Missing again:
/// the deletion clears the record's materialization state, so `on_missing`
/// can re-fire. Regression: the deletion stamped its own run onto
/// `last_run_id`, which `Missing` keys off — the asset stayed "materialized"
/// forever and automation never rebuilt it.
#[tokio::test]
async fn deleted_asset_reads_missing_again() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = DEFAULT_CODE_LOCATION_ID.to_string();
    let ctx_cl = crate::storage::CodeLocationContext::new(cl.clone());
    let scoped = storage.for_code_location(&ctx_cl);

    scoped
        .register_assets(&[make_record("table")])
        .await
        .unwrap();
    let event = |event_type, ts| crate::storage::EventRecord {
        code_location_id: cl.clone(),
        event_type,
        asset_key: Some("table".to_string()),
        run_id: format!("run-{ts}"),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    storage
        .store_events(&[event(
            crate::storage::EventType::Materialization {
                data_version: Some("dv1".to_string()),
            },
            1000,
        )])
        .await
        .unwrap();
    let record = scoped.get_asset_record("table").await.unwrap().unwrap();
    let mat_event_id = record.last_event_id.clone();
    let records = HashMap::from([("table".to_string(), record.clone())]);
    let deps = HashMap::new();
    assert!(
        !evaluate(
            &ConditionNode::Missing,
            &make_ctx("table", &record, &records, &deps)
        )
        .fired,
        "a materialized asset must not read Missing"
    );

    storage
        .store_events(&[event(crate::storage::EventType::Deletion, 2000)])
        .await
        .unwrap();
    let record = scoped.get_asset_record("table").await.unwrap().unwrap();
    let records = HashMap::from([("table".to_string(), record.clone())]);
    assert!(
        evaluate(
            &ConditionNode::Missing,
            &make_ctx("table", &record, &records, &deps)
        )
        .fired,
        "a deleted asset must read Missing so automation can rebuild it"
    );
    // `is_some()` alone is vacuous — the materialization already set it. The
    // deletion event itself must own the pointer for timelines to resolve.
    assert!(record.last_event_id.is_some());
    assert_ne!(
        record.last_event_id, mat_event_id,
        "the deletion, not the prior materialization, owns the last event"
    );
}

/// Deleting an asset must not read to downstream as "the dependency produced
/// something new" — `NewlyUpdated` compares `last_timestamp` against the
/// downstream's own, so a deletion that moves it forward triggers a
/// materialization from data that no longer exists.
#[tokio::test]
async fn deleted_asset_does_not_read_newly_updated_downstream() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = DEFAULT_CODE_LOCATION_ID.to_string();
    let ctx_cl = crate::storage::CodeLocationContext::new(cl.clone());
    let scoped = storage.for_code_location(&ctx_cl);

    scoped
        .register_assets(&[make_record("events"), make_record("rollups")])
        .await
        .unwrap();
    let event = |asset: &str, event_type, ts| crate::storage::EventRecord {
        code_location_id: cl.clone(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: format!("run-{asset}-{ts}"),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    let mat = |dv: &str| crate::storage::EventType::Materialization {
        data_version: Some(dv.to_string()),
    };

    storage
        .store_events(&[event("events", mat("dv-e"), 1000)])
        .await
        .unwrap();
    storage
        .store_events(&[event("rollups", mat("dv-r"), 1100)])
        .await
        .unwrap();
    storage
        .store_events(&[event("events", crate::storage::EventType::Deletion, 2000)])
        .await
        .unwrap();

    let events = scoped.get_asset_record("events").await.unwrap().unwrap();
    let rollups = scoped.get_asset_record("rollups").await.unwrap().unwrap();
    let records = HashMap::from([
        ("events".to_string(), events.clone()),
        ("rollups".to_string(), rollups.clone()),
    ]);
    let deps = HashMap::from([("rollups".to_string(), vec!["events".to_string()])]);

    // NewlyUpdated(events) as evaluated for rollups: target is the dep, root
    // is the asset whose automation would fire.
    let ctx = EvalContext {
        target_key: "events",
        root_key: "rollups",
        target_record: &events,
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
        now: 3_000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(
        !evaluate(&ConditionNode::NewlyUpdated, &ctx).fired,
        "a deleted upstream must not look freshly materialized to downstream"
    );
}
