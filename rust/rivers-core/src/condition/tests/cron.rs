use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
    let prev_tick = utc(2026, 6, 16, 12, 30, 0).as_nanosecond() as i64;
    let now = utc(2026, 6, 16, 13, 30, 0).as_nanosecond() as i64;
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
    let prev_tick = utc(2025, 11, 2, 16, 0, 0).as_nanosecond() as i64;
    let now = utc(2025, 11, 2, 17, 30, 0).as_nanosecond() as i64;
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

/// The Schedule loop stores `next_occurrence` as a UTC instant; a tz-qualified schedule must
/// fire at the declared wall time, and the UTC instant shifts across DST while the wall time stays fixed.
#[test]
fn test_next_cron_occurrence_utc_respects_timezone_and_dst() {
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("0 9 * * *")
        .unwrap();

    // No timezone → evaluated in UTC: next 09:00 UTC.
    let after = utc(2024, 1, 15, 0, 0, 0);
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, None),
        Some(utc(2024, 1, 15, 9, 0, 0)),
    );

    // America/New_York, winter (EST = UTC-5): 09:00 local → 14:00 UTC.
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, Some("America/New_York")),
        Some(utc(2024, 1, 15, 14, 0, 0)),
        "09:00 EST must be 14:00 UTC, not 09:00 UTC"
    );

    // Same schedule, summer (EDT = UTC-4): 09:00 local → 13:00 UTC (shifts an hour across DST, wall time fixed).
    let summer = utc(2024, 7, 15, 0, 0, 0);
    assert_eq!(
        next_cron_occurrence_utc(&cron, summer, Some("America/New_York")),
        Some(utc(2024, 7, 15, 13, 0, 0)),
        "09:00 EDT must be 13:00 UTC"
    );
}

/// A spring-forward-skipped wall time (02:30 NY doesn't exist on 2026-03-08) must fire at the
/// first valid instant after the gap (03:00 EDT = 07:00 UTC), not the skipped wall time misread as UTC.
#[test]
fn test_next_cron_occurrence_utc_spring_forward_gap_advances_to_gap_end() {
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("30 2 * * *")
        .unwrap();

    // 2026-03-08 01:00 EST = 06:00 UTC, one wall-clock hour before the gap.
    let after = utc(2026, 3, 8, 6, 0, 0);
    assert_eq!(
        next_cron_occurrence_utc(&cron, after, Some("America/New_York")),
        Some(utc(2026, 3, 8, 7, 0, 0)),
        "gap occurrence must fire at the first valid wall time after the gap (03:00 EDT)"
    );
}

/// Fall-back repeated hour: when `after` is in the second pass, resolving to the earliest
/// ambiguous instant lands in the past and (no in-flight guard) re-fires every loop;
/// the next occurrence must be strictly after `after`.
#[test]
fn test_next_cron_occurrence_utc_fall_back_never_returns_past_instant() {
    let cron = croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse("30 1 * * *")
        .unwrap();

    // 06:05 UTC = 01:05 EST (second pass of the repeated 01:00-02:00 hour); next 01:30 is
    // ambiguous: 01:30 EDT = 05:30 UTC (past) vs 01:30 EST = 06:30 UTC.
    let after = utc(2025, 11, 2, 6, 5, 0);
    let next = next_cron_occurrence_utc(&cron, after, Some("America/New_York"))
        .expect("occurrence must exist");
    assert!(
        next > after,
        "next occurrence must be strictly after `after`; got {next} <= {after} \
         (schedule would re-fire on every daemon loop pass)"
    );
    assert_eq!(
        next,
        utc(2025, 11, 2, 6, 30, 0),
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
