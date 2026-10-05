use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
