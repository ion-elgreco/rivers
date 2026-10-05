use super::*;

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
