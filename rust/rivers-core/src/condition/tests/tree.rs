use super::*;

#[test]
fn test_tree_matches_eval_result() {
    // evaluate() and evaluate_with_tree() should agree on fired
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::eager();
    let eval = evaluate(&cond, &ctx);
    let (tree_eval, tree) = evaluate_with_tree(&cond, &ctx);
    assert_eq!(eval.fired, tree_eval.fired);
    assert_eq!(tree_eval.fired, tree.status == NodeStatus::True);
}

#[test]
fn test_tree_short_circuit_skipped() {
    // And([false_leaf, other]) → other should be Skipped
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    // Missing is false (asset is materialized), so second child should be skipped
    let cond = ConditionNode::And(vec![ConditionNode::Missing, ConditionNode::InProgress]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(!result.fired);
    assert_eq!(tree.status, NodeStatus::False);
    assert_eq!(tree.children.len(), 2);
    assert_eq!(tree.children[0].status, NodeStatus::False); // Missing=false
    assert_eq!(tree.children[1].status, NodeStatus::Skipped); // short-circuited
}

#[test]
fn test_tree_indices_stable() {
    // Node indices from evaluate_with_tree must match evaluate's sub_results keys
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::eager();
    let eval = evaluate(&cond, &ctx);
    let (tree_eval, _tree) = evaluate_with_tree(&cond, &ctx);

    // Both should produce identical sub_results
    assert_eq!(eval.sub_results, tree_eval.sub_results);
}

#[test]
fn test_tree_leaf_labels() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let (_, tree) = evaluate_with_tree(&ConditionNode::Missing, &ctx);
    assert_eq!(tree.label, "missing");
    assert_eq!(tree.node_type, "Leaf");
    assert_eq!(tree.status, NodeStatus::True); // Missing asset
}

#[test]
fn test_tree_or_short_circuit() {
    // Or([true_leaf, other]) → other should be Skipped
    let record = make_record("a"); // Missing
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::Or(vec![
        ConditionNode::Missing,    // true
        ConditionNode::InProgress, // should be skipped
    ]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(tree.children[0].status, NodeStatus::True);
    assert_eq!(tree.children[1].status, NodeStatus::Skipped);
}

#[test]
fn test_tree_serialization() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let (_, tree) = evaluate_with_tree(&ConditionNode::eager(), &ctx);
    let json = serde_json::to_vec(&tree).unwrap();
    let deserialized: EvalNodeResult = serde_json::from_slice(&json).unwrap();
    assert_eq!(tree.node_idx, deserialized.node_idx);
    assert_eq!(tree.status, deserialized.status);
}

#[test]
fn test_time_based_eval_set_single_cron_chain() {
    // A (cron) → B (eager) → C (eager) — all three in eval set
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("c".to_string(), "b".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 3);
    assert!(eval_set.contains("a"));
    assert!(eval_set.contains("b"));
    assert!(eval_set.contains("c"));
}

#[test]
fn test_time_based_eval_set_isolated_subgraph_excluded() {
    // A (cron) → B (eager)  |  C (eager) → D (eager) — only {A, B}
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("d".to_string(), "c".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
        ("d".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 2);
    assert!(eval_set.contains("a"));
    assert!(eval_set.contains("b"));
    assert!(!eval_set.contains("c"));
    assert!(!eval_set.contains("d"));
}

#[test]
fn test_time_based_eval_set_multiple_cron_overlapping() {
    // A (cron) → C (eager), B (cron) → C (eager) — all three
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("c".to_string(), "a".to_string()),
        ("c".to_string(), "b".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        (
            "b".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("c".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 3);
}

#[test]
fn test_time_based_eval_set_cron_no_downstream() {
    // A (cron) standalone, B (eager) standalone — only {A}
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 1);
    assert!(eval_set.contains("a"));
}

#[test]
fn test_time_based_eval_set_diamond() {
    // A (cron) → B, A → C, B → D, C → D — all four
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.edges = vec![
        ("b".to_string(), "a".to_string()),
        ("c".to_string(), "a".to_string()),
        ("d".to_string(), "b".to_string()),
        ("d".to_string(), "c".to_string()),
    ];
    cache.build_adjacency();

    let conditions = vec![
        (
            "a".to_string(),
            ConditionNode::on_cron("0 * * * *".to_string(), None),
        ),
        ("b".to_string(), ConditionNode::eager()),
        ("c".to_string(), ConditionNode::eager()),
        ("d".to_string(), ConditionNode::eager()),
    ];

    let eval_set = cache.compute_time_based_eval_set(&conditions);
    assert_eq!(eval_set.len(), 4);
}

#[test]
fn test_has_time_based_conditions() {
    // Leaf: CronTickPassed is time-based
    assert!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .has_time_based_conditions()
    );

    // Non-time-based leaves
    assert!(!ConditionNode::Missing.has_time_based_conditions());
    assert!(!ConditionNode::InProgress.has_time_based_conditions());
    assert!(!ConditionNode::ExecutionFailed.has_time_based_conditions());
    assert!(!ConditionNode::NewlyUpdated.has_time_based_conditions());
    assert!(!ConditionNode::CodeVersionChanged.has_time_based_conditions());
    assert!(!ConditionNode::any_deps_missing().has_time_based_conditions());
    assert!(!ConditionNode::any_deps_in_progress().has_time_based_conditions());
    assert!(!ConditionNode::any_deps_updated().has_time_based_conditions());

    // Nested in And
    let and_with_cron = ConditionNode::And(vec![
        ConditionNode::Missing,
        ConditionNode::CronTickPassed {
            cron_schedule: "0 0 * * *".to_string(),
            timezone: None,
        },
    ]);
    assert!(and_with_cron.has_time_based_conditions());

    // Nested in Or
    let or_without_cron =
        ConditionNode::Or(vec![ConditionNode::Missing, ConditionNode::InProgress]);
    assert!(!or_without_cron.has_time_based_conditions());

    // Nested in Not
    let not_cron = !ConditionNode::CronTickPassed {
        cron_schedule: "* * * * *".to_string(),
        timezone: None,
    };
    assert!(not_cron.has_time_based_conditions());

    // Nested in NewlyTrue
    assert!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .newly_true()
        .has_time_based_conditions()
    );

    // Nested in Since (trigger)
    let since_cron = ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    }
    .since(ConditionNode::NewlyRequested);
    assert!(since_cron.has_time_based_conditions());

    // Nested in Since (reset)
    let since_reset_cron = ConditionNode::Missing.since(ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    });
    assert!(since_reset_cron.has_time_based_conditions());

    // Nested in SinceLastHandled
    assert!(
        !ConditionNode::Missing
            .since_last_handled()
            .has_time_based_conditions()
    );

    // Nested in AnyDepsMatch
    let any_deps = ConditionNode::any_deps_match(ConditionNode::CronTickPassed {
        cron_schedule: "0 * * * *".to_string(),
        timezone: None,
    });
    assert!(any_deps.has_time_based_conditions());

    // Nested in AllDepsMatch
    let all_deps = ConditionNode::all_deps_match(ConditionNode::Missing);
    assert!(!all_deps.has_time_based_conditions());

    // InLatestTimeWindow is NOT time-based (it's partition-based)
    assert!(
        !ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(86400.0)
        }
        .has_time_based_conditions()
    );
}

#[test]
fn test_node_type_str() {
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).node_type_str(),
        "And"
    );
    assert_eq!(
        ConditionNode::Or(vec![ConditionNode::Missing]).node_type_str(),
        "Or"
    );
    assert_eq!((!ConditionNode::Missing).node_type_str(), "Not");
    assert_eq!(
        ConditionNode::Missing.newly_true().node_type_str(),
        "NewlyTrue"
    );
    assert_eq!(
        ConditionNode::Missing
            .since(ConditionNode::NewlyRequested)
            .node_type_str(),
        "Since"
    );
    assert_eq!(
        ConditionNode::Missing.since_last_handled().node_type_str(),
        "SinceLastHandled"
    );
    assert_eq!(
        ConditionNode::any_deps_match(ConditionNode::Missing).node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::all_deps_match(ConditionNode::Missing).node_type_str(),
        "AllDepsMatch"
    );

    // All leaf variants return "Leaf"
    assert_eq!(ConditionNode::Missing.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::InProgress.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::ExecutionFailed.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::NewlyUpdated.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::NewlyRequested.node_type_str(), "Leaf");
    assert_eq!(ConditionNode::CodeVersionChanged.node_type_str(), "Leaf");
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 * * * *".to_string(),
            timezone: None,
        }
        .node_type_str(),
        "Leaf"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        }
        .node_type_str(),
        "Leaf"
    );
    assert_eq!(
        ConditionNode::any_deps_missing().node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::any_deps_in_progress().node_type_str(),
        "AnyDepsMatch"
    );
    assert_eq!(
        ConditionNode::any_deps_updated().node_type_str(),
        "AnyDepsMatch"
    );
}

#[test]
fn test_node_label_exhaustive() {
    assert_eq!(ConditionNode::Missing.node_label(), "missing");
    assert_eq!(ConditionNode::InProgress.node_label(), "in_progress");
    assert_eq!(
        ConditionNode::ExecutionFailed.node_label(),
        "execution_failed"
    );
    assert_eq!(ConditionNode::NewlyUpdated.node_label(), "newly_updated");
    assert_eq!(
        ConditionNode::NewlyRequested.node_label(),
        "newly_requested"
    );
    assert_eq!(
        ConditionNode::CodeVersionChanged.node_label(),
        "code_version_changed"
    );
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 */5 * * *".to_string(),
            timezone: Some("UTC".to_string()),
        }
        .node_label(),
        // tz is load-bearing → must appear in the label (else two crons differing only by zone collapse)
        "cron_tick_passed('0 */5 * * *', tz='UTC')"
    );
    assert_eq!(
        ConditionNode::CronTickPassed {
            cron_schedule: "0 */5 * * *".to_string(),
            timezone: None,
        }
        .node_label(),
        "cron_tick_passed('0 */5 * * *')"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: Some(3600.0),
        }
        .node_label(),
        "in_latest_time_window(lookback=3600)"
    );
    assert_eq!(
        ConditionNode::InLatestTimeWindow {
            lookback_delta: None,
        }
        .node_label(),
        "in_latest_time_window"
    );
    assert_eq!(
        ConditionNode::any_deps_missing().node_label(),
        "any_deps_missing"
    );
    assert_eq!(
        ConditionNode::any_deps_in_progress().node_label(),
        "any_deps_in_progress"
    );
    assert_eq!(
        ConditionNode::any_deps_updated().node_label(),
        "any_deps_updated"
    );
    assert_eq!(
        ConditionNode::any_deps_match(ConditionNode::Missing).node_label(),
        format!(
            "any_deps_match({})",
            ConditionNode::Missing.fingerprint_hex()
        )
    );
    assert_eq!(
        ConditionNode::all_deps_match(ConditionNode::Missing).node_label(),
        format!(
            "all_deps_match({})",
            ConditionNode::Missing.fingerprint_hex()
        )
    );
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).node_label(),
        "All of"
    );
    assert_eq!(
        ConditionNode::Or(vec![ConditionNode::Missing]).node_label(),
        "Any of"
    );
    assert_eq!((!ConditionNode::Missing).node_label(), "Not");
    assert_eq!(
        ConditionNode::Missing.newly_true().node_label(),
        "newly_true"
    );
    assert_eq!(
        ConditionNode::Missing
            .since(ConditionNode::NewlyRequested)
            .node_label(),
        "since"
    );
    assert_eq!(
        ConditionNode::Missing.since_last_handled().node_label(),
        "since_last_handled"
    );
}

#[test]
fn test_display_label_is_readable_not_a_fingerprint() {
    // node_label folds a fingerprint into unlabeled dep-aggregates/asset_matches for
    // replace-by-label, but that hex must not leak into the UI tree — display_label renders it readably.
    let dep = ConditionNode::any_deps_match(ConditionNode::NewlyUpdated);
    assert_eq!(dep.display_label(), "any_deps_match(newly_updated)");
    assert!(
        !dep.display_label()
            .contains(&ConditionNode::NewlyUpdated.fingerprint_hex()),
        "display_label must not contain the raw fingerprint"
    );

    let am =
        ConditionNode::asset_matches(vec!["upstream_feed".to_string()], ConditionNode::Missing);
    assert_eq!(
        am.display_label(),
        "asset_matches('upstream_feed', missing)"
    );

    // A user-provided label on a dep-aggregate is already readable — keep it.
    let labeled = ConditionNode::AnyDepsMatch {
        condition: Box::new(ConditionNode::Missing),
        label: Some("any_deps_missing".to_string()),
    };
    assert_eq!(labeled.display_label(), "any_deps_missing");

    // Leaf and composite nodes keep their existing labels.
    assert_eq!(ConditionNode::Missing.display_label(), "missing");
    assert_eq!(
        ConditionNode::And(vec![ConditionNode::Missing]).display_label(),
        "All of"
    );

    // The eval tree (rendered verbatim by the UI) must carry the readable label, not the fingerprint.
    let tree_node = crate::condition::state::EvalNodeResult::new(
        &dep,
        0,
        crate::condition::state::NodeStatus::True,
        vec![],
        None,
    );
    assert_eq!(tree_node.label, "any_deps_match(newly_updated)");
}

#[test]
fn test_node_label_distinguishes_unlabeled_aggregate_inner_condition() {
    // node_label for unlabeled any/all_deps_match and asset_matches must include the inner
    // condition, else structurally-distinct siblings collapse to one label and replace_by_label hits the wrong subtree.
    let a = ConditionNode::any_deps_match(ConditionNode::Missing);
    let b = ConditionNode::any_deps_match(ConditionNode::NewlyUpdated);
    assert_ne!(
        a.node_label(),
        b.node_label(),
        "distinct inner conditions must yield distinct labels"
    );

    // asset_matches with identical keys but different inner conditions.
    let am1 = ConditionNode::asset_matches(vec!["x".into()], ConditionNode::Missing);
    let am2 = ConditionNode::asset_matches(vec!["x".into()], ConditionNode::InProgress);
    assert_ne!(am1.node_label(), am2.node_label());

    // replace_by_label must touch only the matching sibling, preserving the other's inner condition.
    let tree = a.clone() | b.clone();
    let replaced = tree.replace_by_label(&a.node_label(), &ConditionNode::ExecutionFailed);
    if let ConditionNode::Or(children) = &replaced {
        assert!(
            children
                .iter()
                .any(|c| matches!(c, ConditionNode::ExecutionFailed)),
            "the matched sibling must be replaced; got {replaced:?}"
        );
        assert!(
            children.iter().any(|c| c.node_label() == b.node_label()),
            "the non-matching sibling must be preserved; got {replaced:?}"
        );
    } else {
        panic!("expected Or, got {replaced:?}");
    }
}

#[test]
fn test_evaluate_full_result_missing() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    let expected = EvalResult {
        fired: true,
        sub_results: HashMap::new(),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    assert_eq!(result, expected);
}

#[test]
fn test_evaluate_full_result_and() {
    // And([Missing, Not(InProgress)]) on missing, not-in-progress asset
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let cond = ConditionNode::And(vec![ConditionNode::Missing, !ConditionNode::InProgress]);
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(result.selection, None);
    assert_eq!(result.sub_selections, None);
    // sub_results should be empty since no stateful operators
    assert_eq!(result.sub_results, HashMap::new());
}

#[test]
fn test_evaluate_full_result_not_fired() {
    // InProgress on a non-in-progress asset
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::InProgress, &ctx);
    let expected = EvalResult {
        fired: false,
        sub_results: HashMap::new(),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    assert_eq!(result, expected);
}

#[test]
fn test_evaluate_with_tree_full_result() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::And(vec![ConditionNode::Missing, !ConditionNode::InProgress]);
    let (result, tree) = evaluate_with_tree(&cond, &ctx);

    assert!(result.fired);

    let expected_tree = EvalNodeResult {
        node_idx: 0,
        label: "All of".to_string(),
        node_type: "And".to_string(),
        status: NodeStatus::True,
        children: vec![
            EvalNodeResult {
                node_idx: 1,
                label: "missing".to_string(),
                node_type: "Leaf".to_string(),
                status: NodeStatus::True,
                children: vec![],
                num_partitions: None,
            },
            EvalNodeResult {
                node_idx: 2,
                label: "Not".to_string(),
                node_type: "Not".to_string(),
                status: NodeStatus::True,
                children: vec![EvalNodeResult {
                    node_idx: 3,
                    label: "in_progress".to_string(),
                    node_type: "Leaf".to_string(),
                    status: NodeStatus::False,
                    children: vec![],
                    num_partitions: None,
                }],
                num_partitions: None,
            },
        ],
        num_partitions: None,
    };
    assert_eq!(tree, expected_tree);
}

#[test]
fn test_update_condition_state_basic() {
    let record = make_materialized_record("a", 500);

    let mut state = AssetConditionState::default();
    let result = EvalResult {
        fired: true,
        sub_results: HashMap::from([(0, true), (1, false)]),
        selection: None,
        sub_selections: None,
        dep_sub_results: HashMap::new(),
        dep_sub_selections: None,
    };
    let ctx = StateUpdateContext {
        target_record_timestamp: record.last_timestamp,
        target_data_version: record.last_data_version.as_ref(),
        now: 2000,
        is_initial: false,
        partition_timestamps: None,
    };
    update_condition_state(&mut state, &ctx, &result);

    let expected = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        dep_previous_results: HashMap::new(),
        dep_baselines: HashMap::new(),
        last_handled_timestamp: None,
        last_materialized_timestamp: Some(500),
        last_data_version: Some("dv_a".to_string()),
        last_tick_timestamp: Some(2000),
        condition_fingerprint: String::new(),
        is_initial: false,
        partition_state: None,
    };
    assert_eq!(state, expected);
}

#[test]
fn test_node_status_from_bool() {
    assert_eq!(NodeStatus::from_bool(true), NodeStatus::True);
    assert_eq!(NodeStatus::from_bool(false), NodeStatus::False);
}
