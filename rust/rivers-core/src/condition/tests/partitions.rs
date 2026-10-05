use super::*;

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
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: Some(jiff::civil::date(2024, 2, 1).at(0, 0, 0, 0)),
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
    let now_local = jiff::civil::date(2020, 1, 5).at(12, 0, 0, 0);
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
    let now_local = jiff::civil::date(2020, 1, 1).at(0, 0, 0, 0);
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
    let now_local = jiff::civil::date(2020, 1, 1).at(0, 0, 0, 0);
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
    let now_local = jiff::civil::date(2020, 1, 5).at(12, 0, 0, 0);
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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

// These two arms had full bool-side coverage (7 and 4 tests) but *zero*
// partitioned tests. They pinned partition-aware behavior for the evaluator
// merge and now guard the PartitionDomain impl of these arms.

/// A partition status entry carrying only materialization timestamps.
fn pstatus_materialized(ts: &[(&str, i64)]) -> crate::condition::cache::PartitionStatusEntry {
    crate::condition::cache::PartitionStatusEntry {
        in_progress: HashSet::new(),
        failed: HashSet::new(),
        failed_timestamps: HashMap::new(),
        timestamps: ts.iter().map(|(k, v)| (spk(k), *v)).collect(),
    }
}

#[test]
fn test_partitioned_asset_matches_selects_missing_dep_partitions() {
    // asset_matches(["b"], Missing) over an identity-mapped b: the fired
    // selection is exactly b's missing partitions, mapped back to the root.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::new();

    let all_keys = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let upstream_keys = HashMap::from([("b".to_string(), all_keys.clone())]);
    let mappings = HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::Identity)]);
    // Only p1 materialized on b → p2, p3 are Missing.
    let statuses = HashMap::from([("b".to_string(), pstatus_materialized(&[("p1", 100)]))]);

    let empty_set: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &a, &records, &deps, &pctx);

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_partitioned_asset_matches_empty_when_dep_fully_materialized() {
    // Every partition of b materialized → Missing selects nothing → not fired.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::new();

    let all_keys = HashSet::from([spk("p1"), spk("p2")]);
    let upstream_keys = HashMap::from([("b".to_string(), all_keys.clone())]);
    let mappings = HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::Identity)]);
    let statuses = HashMap::from([(
        "b".to_string(),
        pstatus_materialized(&[("p1", 100), ("p2", 100)]),
    )]);

    let empty_set: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &a, &records, &deps, &pctx);

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing);
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::Empty);
}

#[test]
fn test_partitioned_asset_matches_multi_key_unions_dep_selections() {
    // asset_matches(["b","c"], Missing): the union of each named asset's
    // missing partitions (b missing p2, c missing p3 → {p2, p3}).
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("a".into(), a.clone()),
        ("b".into(), b.clone()),
        ("c".into(), c.clone()),
    ]);
    let deps = HashMap::new();

    let all_keys = HashSet::from([spk("p1"), spk("p2"), spk("p3")]);
    let upstream_keys = HashMap::from([
        ("b".to_string(), all_keys.clone()),
        ("c".to_string(), all_keys.clone()),
    ]);
    let mappings = HashMap::from([
        (("a".into(), "b".into()), PartitionMappingKind::Identity),
        (("a".into(), "c".into()), PartitionMappingKind::Identity),
    ]);
    let statuses = HashMap::from([
        (
            "b".to_string(),
            pstatus_materialized(&[("p1", 100), ("p3", 100)]),
        ),
        (
            "c".to_string(),
            pstatus_materialized(&[("p1", 100), ("p2", 100)]),
        ),
    ]);

    let empty_set: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &a, &records, &deps, &pctx);

    let cond = ConditionNode::asset_matches(vec!["b".into(), "c".into()], ConditionNode::Missing);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_partitioned_asset_matches_unmapped_key_bridges_to_bool() {
    // A named asset with no partition mapping bridges into the bool evaluator:
    // an unmaterialized b makes Missing true → from_bool(true) = All. Pins the
    // bridge path retained inside the partitioned dep pivot (`eval::<BoolDomain>`
    // for an unpartitioned dep under a partitioned root).
    let a = make_materialized_record("a", 100);
    let b = make_record("b"); // unmaterialized → last_run_id None → Missing true in bool world
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::new();

    let all_keys = HashSet::from([spk("p1"), spk("p2")]);
    // No upstream_keys entry for b → eval_partitioned_on_dep bridges to eval_inner.
    let upstream_keys: HashMap<String, HashSet<PartitionKey>> = HashMap::new();
    let mappings: HashMap<(String, String), PartitionMappingKind> = HashMap::new();
    let statuses = HashMap::new();

    let empty_set: HashSet<PartitionKey> = HashSet::new();
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &empty_set,
        failed: &empty_set,
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_keys),
        time_windows: None,
        all_partition_statuses: &statuses,
        dep_root_floor: None,
    };
    let ctx = make_partitioned_ctx("a", &a, &records, &deps, &pctx);

    let cond = ConditionNode::asset_matches(vec!["b".into()], ConditionNode::Missing);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(result.selection.unwrap(), PartitionSelection::All);
}

#[test]
fn test_partitioned_since_last_handled_passes_through_on_first_tick() {
    // No prior handled state (last_handled/last_tick both None) → not "just
    // handled" → SinceLastHandled passes the child selection through unchanged.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);

    let cond = ConditionNode::SinceLastHandled(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")]))
    );
}

#[test]
fn test_partitioned_since_last_handled_subtracts_handled_keys_when_just_handled() {
    // Just handled last tick (last_handled == last_tick) with p2 in the handled
    // set → SinceLastHandled drops p2 from the {p2,p3} missing selection (the
    // partition_state.handled branch at eval/mod.rs:953-968).
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let prev = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(1000),
        partition_state: Some(PartitionState {
            handled: HashSet::from([spk("p2")]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.prev_state = &prev;

    let cond = ConditionNode::SinceLastHandled(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p3")])),
        "p2 was handled last tick → suppressed; p3 still fires"
    );
}

#[test]
fn test_partitioned_since_last_handled_passes_through_when_handled_before_last_tick() {
    // last_handled (1000) strictly before last_tick (2000) → debounce released →
    // the full child selection passes, ignoring the (stale) handled set.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let pdata = OwnedPartitionData::new(&["p1", "p2", "p3"], &["p1"], &[("p1", 100)]);
    let pctx = pdata.as_eval_ctx();
    let prev = AssetConditionState {
        last_handled_timestamp: Some(1000),
        last_tick_timestamp: Some(2000),
        partition_state: Some(PartitionState {
            handled: HashSet::from([spk("p2")]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    ctx.prev_state = &prev;

    let cond = ConditionNode::SinceLastHandled(Box::new(ConditionNode::Missing));
    let result = evaluate(&cond, &ctx);
    assert_eq!(
        result.selection.unwrap(),
        PartitionSelection::Keys(HashSet::from([spk("p2"), spk("p3")])),
        "handled before last tick → nothing suppressed"
    );
}

// The load-bearing guard on the unified `eval<D>`: its two `EvalDomain` impls
// must never diverge. The claim (verified against Dagster's single-evaluator
// design) is that bool is exactly a `PartitionSelection` over a one-partition
// universe. This harness evaluates a corpus of trees BOTH ways over
// field-for-field mirrored fixtures and asserts
//   (1) `fired` parity every tick, and
//   (2) stateful-latch node indices stay aligned (bookkeeping is load-bearing —
//       persisted latches key off node index, so drift corrupts silently).
//
// Excluded from the auto-corpus (their bool/partition fixtures don't correspond
// under a trivial unit universe; covered by targeted tests instead): tag leaves,
// InLatestTimeWindow (bool is unconditionally true), dep-aggregates, and cron.

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
