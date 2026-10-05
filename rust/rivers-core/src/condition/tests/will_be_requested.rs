use super::*;

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
