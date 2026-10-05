use super::*;

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
    // asset_matches can target a non-dep asset (cross-graph checks, e.g. "fire when sibling updated").
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
    // asset_matches preserves root_key so LastRunIncludesTarget still checks against the original root.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["a".to_string(), "b".to_string()]),
    )]);
    let mut ctx = make_ctx("a", &a, &records, &deps);
    let asset_names = slotted(asset_names);
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
            assert!(matches!(children[0], ConditionNode::Missing));
            assert!(matches!(children[1], ConditionNode::InProgress));
            assert!(matches!(children[2], ConditionNode::ExecutionFailed));
        }
        _ => panic!("expected flat Or"),
    }
}

#[test]
fn test_without_matching_removes_matching_child() {
    // eager = SinceLastHandled(...) & !any_deps_missing & !any_deps_in_progress & !in_flight & !ExecutionFailed.
    // Strip the in-progress guard operand Not(any_deps_in_progress()) by structural match.
    let eager = ConditionNode::eager();
    let result = eager.without_matching(&|c| *c == !ConditionNode::any_deps_in_progress());
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_matching_removes_only_exact_operand() {
    // Operands match structurally: bare any_deps_missing() does not match the
    // Not(...) guard (no-op); the exact negated operand strips it.
    let eager = ConditionNode::eager();
    assert_eq!(
        eager.without_matching(&|c| *c == ConditionNode::any_deps_missing()),
        eager,
        "bare any_deps_missing must not match the Not(...) guard"
    );
    let result = eager.without_matching(&|c| *c == !ConditionNode::any_deps_missing());
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::in_flight()
        & !ConditionNode::ExecutionFailed;
    assert_eq!(result, expected);
}

#[test]
fn test_without_matching_on_non_and_is_identity() {
    let leaf = ConditionNode::Missing;
    assert_eq!(
        leaf.without_matching(&|c| *c == ConditionNode::InProgress),
        ConditionNode::Missing
    );
}

#[test]
fn test_replace_swaps_matching_node() {
    let eager = ConditionNode::eager();
    let result = eager.replace_by_label("any_deps_updated", &ConditionNode::NewlyUpdated);
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::NewlyUpdated)
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::any_deps_in_progress()
        & !ConditionNode::in_flight()
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
    // Not(any_deps_in_progress) becomes Not(InProgress); the InProgress inside
    // Not(in_flight()) isn't a structural match, so the in-flight guard is untouched.
    let expected = (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
        .since_last_handled()
        & !ConditionNode::any_deps_missing()
        & !ConditionNode::InProgress
        & !ConditionNode::in_flight()
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
        tags: empty_tag_snapshot(),
        prev_state: &prev2,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 2000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&cond, &ctx2).fired);

    // Third tick: Missing still true, inner was true tick 2 → still does not fire
    // (must store `current`, not `result`, or it re-fires every other tick).
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
        tags: empty_tag_snapshot(),
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
    // And short-circuit must not shift node indices: in And(InProgress, NewlyTrue(Missing)),
    // NewlyTrue keeps a fixed index whether or not And short-circuits.
    let record = make_record("a"); // missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    let cond = ConditionNode::And(vec![
        ConditionNode::InProgress,
        ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing)),
    ]);

    // Tick 1: InProgress=false → And short-circuits, but NewlyTrue's index must still be assigned; result false.
    let ctx1 = make_ctx("a", &record, &records, &deps);
    let r1 = evaluate(&cond, &ctx1);
    assert!(!r1.fired);
    // NewlyTrue's index is 2 (And=0, InProgress=1, NewlyTrue=2, Missing=3); the
    // short-circuited And still evaluates the stateful child, so Missing=true records
    // current=true at stable index 2.
    assert_eq!(r1.sub_results.get(&2), Some(&true));
    assert_eq!(r1.sub_results.len(), 1);

    // Tick 2: InProgress=true (no short-circuit) → NewlyTrue(Missing) fires (inner=true, previous=false).
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
        tags: empty_tag_snapshot(),
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
