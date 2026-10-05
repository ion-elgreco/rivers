use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
fn test_partitioned_since_latch_drops_retired_universe_key() {
    // Regression (V-05): a Since latch must not re-emit a partition key that has
    // been removed from the universe. Otherwise the retired key re-fires every
    // tick, is dropped by classify, and drives a permanent needs_retry loop.
    let empty_partition_statuses = HashMap::new();
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let cond = ConditionNode::Since {
        trigger: Box::new(ConditionNode::Missing),
        reset: Box::new(ConditionNode::NewlyUpdated),
    };

    // Tick 1: universe {p1,p2,p3}, only p1 materialized → Missing latches {p2,p3}.
    let ak1 = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let ip: HashSet<PartitionKey> = HashSet::new();
    let fail: HashSet<PartitionKey> = HashSet::new();
    let ts1 = HashMap::from([(spk("p1"), 100_i64)]);
    let pctx1 = PartitionEvalContext {
        all_keys: &ak1,
        in_progress: &ip,
        failed: &fail,
        timestamps: &ts1,
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
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx1),
        root_partition_floor: None,
    };
    let r1 = evaluate(&cond, &ctx1);
    assert_eq!(
        r1.selection.as_ref().unwrap(),
        &PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );

    // Tick 2: p3 retired from the universe (now {p1,p2}); nothing resets.
    let ak2 = HashSet::from([spk("p1"), spk("p2")]);
    let pctx2 = PartitionEvalContext {
        all_keys: &ak2,
        in_progress: &ip,
        failed: &fail,
        timestamps: &ts1,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_partition_statuses,
        dep_root_floor: None,
    };
    let prev2 = AssetConditionState {
        partition_state: Some(PartitionState {
            previous_selections: r1.sub_selections.unwrap(),
            timestamps: ts1.clone(),
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
        tags: empty_tag_snapshot(),
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2_000_000_000_000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let r2 = evaluate(&cond, &ctx2);
    // p3 is gone from the universe → the latch must not re-emit it; only p2
    // (still missing) survives.
    assert_eq!(
        r2.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2")]))
    );
    // And the stored latch must be pruned so the loop cannot persist.
    for sel in r2.sub_selections.unwrap().values() {
        if let PartitionSelection::Keys(ks) = sel {
            assert!(
                !ks.contains(&spk("p3")),
                "retired key p3 must not remain latched"
            );
        }
    }
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
