use super::*;

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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
        tags: empty_tag_snapshot(),
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
