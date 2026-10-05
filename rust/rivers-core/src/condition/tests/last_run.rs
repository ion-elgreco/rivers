use super::*;

#[test]
fn test_last_run_includes_target_true_when_run_includes_root() {
    // Dep b's joint run [b, c] contains root "c" → true.
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string(), "c".to_string()]),
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c"; // evaluating on dep b, root is c
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_run_excludes_root() {
    // Dep b's solo run [b] excludes root "c" → false.
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([("b".to_string(), Arc::from(vec!["b".to_string()]))]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_no_cache_entry() {
    // No run data for this asset → false
    let record = make_record("b");
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_false_when_self_referential() {
    // target_key == root_key → always false (self-referential guard)
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([("a".to_string(), Arc::from(vec!["a".to_string()]))]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    // root_key defaults to target_key ("a") via make_ctx
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_not_composition() {
    // ~last_run_includes_target: true when dep's run did NOT include root
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string()]), // solo run, no root "c"
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    let cond = !ConditionNode::LastRunIncludesTarget;
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_in_any_deps_match() {
    // any_deps_match(newly_updated & ~last_run_includes_target) on root "a", deps [b,c]:
    // dep b (joint [a,b]) filtered, dep c (solo [c]) included.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 100);
    let c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string(), "c".to_string()])]);
    let asset_names = HashMap::from([
        (
            "b".to_string(),
            Arc::from(vec!["a".to_string(), "b".to_string()]),
        ), // joint run included root "a"
        ("c".to_string(), Arc::from(vec!["c".to_string()])), // solo run, no root "a"
    ]);
    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let dep_c_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let all_states = HashMap::from([
        ("b".to_string(), dep_b_state),
        ("c".to_string(), dep_c_state),
    ]);
    let mut ctx = make_ctx("a", &a, &records, &deps);
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;
    ctx.all_asset_states = &all_states;

    // b: newly_updated=true, last_run_includes_target=true (joint run [a,b]) → ~=false → AND=false
    // c: newly_updated=true, last_run_includes_target=false (solo [c]) → ~=true → AND=true
    let cond = ConditionNode::any_deps_match(
        ConditionNode::NewlyUpdated & !ConditionNode::LastRunIncludesTarget,
    );
    assert!(evaluate(&cond, &ctx).fired);

    // If we only had "b" as a dep (joint run with root), should be false
    let deps_b_only = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    ctx.cache.upstream_deps = &deps_b_only;
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_tree_output() {
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();
    let asset_names = HashMap::from([(
        "b".to_string(),
        Arc::from(vec!["b".to_string(), "c".to_string()]),
    )]);
    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    let (result, tree) = evaluate_with_tree(&ConditionNode::LastRunIncludesTarget, &ctx);
    assert!(result.fired);
    assert_eq!(tree.label, "last_run_includes_target");
    assert_eq!(tree.node_type, "Leaf");
    assert_eq!(tree.status, NodeStatus::True);
}

#[test]
fn test_last_run_includes_target_partitioned() {
    // Partition-level: pk1's run included root "c", pk2's did not (target b, root c).
    let record = make_materialized_record("b", 100);
    let records = HashMap::from([("b".to_string(), record.clone())]);
    let deps = HashMap::new();

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]);

    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["b".to_string(), "c".to_string()]),
            ), // joint run included root
            (pk2.clone(), Arc::from(vec!["b".to_string()])), // solo run
        ]),
    )]);

    let mut ctx = make_ctx("b", &record, &records, &deps);
    ctx.root_key = "c";
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;

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

    let result = evaluate(&ConditionNode::LastRunIncludesTarget, &ctx);
    assert!(result.fired);
    let sel = result.selection.unwrap();
    match sel {
        PartitionSelection::Keys(keys) => {
            assert!(keys.contains(&pk1)); // joint run included root
            assert!(!keys.contains(&pk2)); // solo run did not
            assert_eq!(keys.len(), 1);
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_run_includes_target_only_checks_target_not_root() {
    // The check is on the dep's (target b) run, not the root's (c):
    // what matters is b's asset_names containing "c".
    let record_b = make_materialized_record("b", 100);
    let record_c = make_materialized_record("c", 100);
    let records = HashMap::from([
        ("b".to_string(), record_b.clone()),
        ("c".to_string(), record_c.clone()),
    ]);
    let deps = HashMap::new();
    // b's run does NOT include c; c's run includes b (irrelevant)
    let asset_names = HashMap::from([
        ("b".to_string(), Arc::from(vec!["b".to_string()])),
        (
            "c".to_string(),
            Arc::from(vec!["b".to_string(), "c".to_string()]),
        ),
    ]);
    let mut ctx = make_ctx("b", &record_b, &records, &deps);
    ctx.root_key = "c";
    let asset_names = slotted(asset_names);
    ctx.tags.last_run_asset_names = &asset_names;

    // b's run = [b], does not contain root "c" → false
    assert!(!evaluate(&ConditionNode::LastRunIncludesTarget, &ctx).fired);
}

#[test]
fn test_last_run_includes_target_partitioned_joint_run_suppresses_newly_updated() {
    // Dep b and root a co-materialized at pk1 in a joint run; next tick b:pk1 is
    // NewlyUpdated but LastRunIncludesTarget must suppress the re-fire.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200); // b was updated (ts > prev)
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 100)]);

    // b:pk1 joint run [a, b] includes root "a"; b:pk2 solo run [b].
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["a".to_string(), "b".to_string()]),
            ),
            (pk2.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    // b's prev state: saw b at ts=100 previously
    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    // Dep "b" partition status: both partitions materialized with their timestamps
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 100)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    // any_deps_updated = AnyDepsMatch(NewlyUpdated & !LastRunIncludesTarget | WillBeRequested)
    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1 NewlyUpdated but joint-run suppressed; pk2 not NewlyUpdated → neither fires.
    assert!(!result.fired, "joint run should suppress re-fire for pk1");
    match result.selection {
        Some(PartitionSelection::Empty) | None => {} // expected
        Some(PartitionSelection::Keys(ref keys)) if keys.is_empty() => {}
        other => panic!("expected empty selection, got {:?}", other),
    }
}

#[test]
fn test_last_run_includes_target_partitioned_solo_run_allows_newly_updated() {
    // Dep b in a solo run (not with root a): next tick b:pk1 is NewlyUpdated and must not be suppressed.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let all_keys = HashSet::from([pk1.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64)]);

    // Solo run: b:pk1 was in a run with [b] only → does NOT include root "a"
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([(pk1.clone(), Arc::from(vec!["b".to_string()]))]),
    )]);

    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1 NewlyUpdated and solo-run (not suppressed) → fires.
    assert!(result.fired, "solo run should allow NewlyUpdated to fire");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(keys.contains(&pk1));
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys selection with pk1, got {:?}", other),
    }
}

#[test]
fn test_last_run_includes_target_partitioned_mixed_joint_and_solo() {
    // Dep b: pk1 joint run with root a, pk2 solo → only pk2 fires any_deps_updated.
    let a = make_materialized_record("a", 50);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 200)]);

    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec!["a".to_string(), "b".to_string()]),
            ), // joint
            (pk2.clone(), Arc::from(vec!["b".to_string()])), // solo
        ]),
    )]);

    let dep_b_state = AssetConditionState {
        last_materialized_timestamp: Some(100),
        partition_state: Some(PartitionState {
            previous_selections: HashMap::new(),
            timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]),
            handled: HashSet::new(),
            dep_previous_selections: HashMap::new(),
        }),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), dep_b_state)]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".to_string(), b_partition_status)]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), all_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    // pk1: joint run → suppressed. pk2: solo run → fires.
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(!keys.contains(&pk1), "pk1 should be suppressed (joint run)");
            assert!(keys.contains(&pk2), "pk2 should fire (solo run)");
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys with only pk2, got {:?}", other),
    }
}
