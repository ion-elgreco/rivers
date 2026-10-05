use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
