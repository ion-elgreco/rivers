use super::*;

#[test]
fn test_last_executed_with_tags_values_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_values_mismatch() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "staging".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_false_when_no_run() {
    let record = make_record("a");
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_key_only_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "staging".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string(), "team".to_string()],
        tag_values: vec![],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_key_missing() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_combined_keys_and_values() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);

    let cond2 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["missing".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond2, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_subset_containment() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
            ("priority".to_string(), "high".to_string()),
        ]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_only_matches_target_asset() {
    let record_a = make_materialized_record("a", 100);
    let record_b = make_materialized_record("b", 100);
    let records = HashMap::from([
        ("a".to_string(), record_a.clone()),
        ("b".to_string(), record_b.clone()),
    ]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "b".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record_a, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_tree_output() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let (result, tree) = evaluate_with_tree(&cond, &ctx);
    assert!(result.fired);
    assert_eq!(tree.status, NodeStatus::True);
    assert_eq!(tree.label, "last_executed_with_tags(env=prod)");
}

#[test]
fn test_last_executed_with_tags_composition_with_not() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags = HashMap::from([(
        "a".to_string(),
        Arc::from(vec![("env".to_string(), "prod".to_string())]),
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = !ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_last_executed_with_tags_partitioned_per_partition() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let _pk2 = spk("2024-01-02");
    let partition_run_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([(
            pk1.clone(),
            Arc::from(vec![("env".to_string(), "backfill".to_string())]),
        )]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let partition_run_tags = slotted_parts(partition_run_tags);
    ctx.tags.last_run_tags = &partition_run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_executed_with_tags_partitioned_no_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let data = OwnedPartitionData::new(&["2024-01-01"], &["2024-01-01"], &[("2024-01-01", 10)]);
    let pctx = data.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "backfill".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
}

#[test]
fn test_last_executed_with_tags_partitioned_key_only() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let partition_run_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                Arc::from(vec![("env".to_string(), "prod".to_string())]),
            ),
            (
                pk2.clone(),
                Arc::from(vec![("team".to_string(), "data".to_string())]),
            ),
        ]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let partition_run_tags = slotted_parts(partition_run_tags);
    ctx.tags.last_run_tags = &partition_run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_last_executed_with_tags_empty_tags_in_cache_does_not_match() {
    // A cached empty-tags entry must not vacuously match (.all() on empty iters is true);
    // the evaluator returns false when the run had no tags.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let run_tags: HashMap<String, Arc<[(String, String)]>> =
        HashMap::from([("a".to_string(), Arc::from(vec![]))]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let run_tags = slotted(run_tags);
    ctx.tags.last_run_tags = &run_tags;

    let cond = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);

    let cond2 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    assert!(!evaluate(&cond2, &ctx).fired);

    // BOTH empty: vacuous truth (.all() on empty iters); defended at the cache
    // layer, documented here for the bypassed-guard edge case.
    let cond3 = ConditionNode::LastExecutedWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    };
    assert!(evaluate(&cond3, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_mismatch() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "staging".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_no_materializations() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_multiple_runs_one_matches() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![("env".to_string(), "staging".to_string())]),
            Arc::from(vec![("env".to_string(), "prod".to_string())]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_all_match() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "data".to_string()),
            ]),
            Arc::from(vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "infra".to_string()),
            ]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_one_missing() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![
            Arc::from(vec![("env".to_string(), "prod".to_string())]),
            Arc::from(vec![("env".to_string(), "staging".to_string())]),
        ],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_all_runs_have_tags_no_materializations() {
    // Not vacuously true.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_key_only() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "staging".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec!["env".to_string()],
        tag_values: vec![],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_has_run_with_tags_combined_keys_and_values() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "data".to_string()),
        ])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec!["team".to_string()],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_new_update_tags_asset_selectivity() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
    )]);

    let mut ctx_a = make_ctx("a", &a, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx_a.tags.tick_materialization_tags = &tick_tags;
    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(evaluate(&cond, &ctx_a).fired);

    let mut ctx_b = make_ctx("b", &b, &records, &deps);
    ctx_b.tags.tick_materialization_tags = &tick_tags;
    assert!(!evaluate(&cond, &ctx_b).fired);
}

#[test]
fn test_new_update_tags_in_any_deps_match_composition() {
    // Asset "c" depends on "a" and "b"; only "a" materialized this tick with the backfill tag.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let c = make_materialized_record("c", 150);
    let records = HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
        ("c".to_string(), c.clone()),
    ]);
    let deps = HashMap::from([("c".to_string(), vec!["a".to_string(), "b".to_string()])]);
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![(
            "type".to_string(),
            "backfill".to_string(),
        )])],
    )]);

    let mut ctx = make_ctx("c", &c, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::any_deps_match(ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("type".to_string(), "backfill".to_string())],
    });
    assert!(evaluate(&cond, &ctx).fired);

    // all_deps_match → false because "b" has no tick materializations
    let cond_all = ConditionNode::all_deps_match(ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("type".to_string(), "backfill".to_string())],
    });
    assert!(!evaluate(&cond_all, &ctx).fired);
}

#[test]
fn test_new_update_tags_run_with_empty_tags() {
    // A run completed with no tags — both AnyNewUpdate and AllNewUpdates are false.
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([("a".to_string(), vec![Arc::from(vec![])])]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond_any = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond_any, &ctx).fired);

    let cond_all = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    assert!(!evaluate(&cond_all, &ctx).fired);
}

#[test]
fn test_new_update_tags_tree_output() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let tick_tags = HashMap::from([(
        "a".to_string(),
        vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
    )]);
    let mut ctx = make_ctx("a", &record, &records, &deps);
    let tick_tags = slotted(tick_tags);
    ctx.tags.tick_materialization_tags = &tick_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let (_result, tree) = evaluate_with_tree(&cond, &ctx);
    assert_eq!(tree.status, NodeStatus::True);
    assert!(tree.label.contains("has_run_with_tags"));
}

#[test]
fn test_has_run_with_tags_partitioned() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let tick_part_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([(
            pk1.clone(),
            vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
        )]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let tick_part_tags = slotted_parts(tick_part_tags);
    ctx.tags.tick_materialization_tags = &tick_part_tags;

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk1));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_all_runs_have_tags_partitioned() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let tick_part_tags = HashMap::from([(
        "a".to_string(),
        HashMap::from([
            (
                pk1.clone(),
                vec![
                    Arc::from(vec![("env".to_string(), "prod".to_string())]),
                    Arc::from(vec![("env".to_string(), "staging".to_string())]),
                ],
            ),
            (
                pk2.clone(),
                vec![Arc::from(vec![("env".to_string(), "prod".to_string())])],
            ),
        ]),
    )]);
    let data = OwnedPartitionData::new(
        &["2024-01-01", "2024-01-02"],
        &["2024-01-01", "2024-01-02"],
        &[("2024-01-01", 10), ("2024-01-02", 20)],
    );
    let pctx = data.as_eval_ctx();
    let mut ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);
    let tick_part_tags = slotted_parts(tick_part_tags);
    ctx.tags.tick_materialization_tags = &tick_part_tags;

    let cond = ConditionNode::AllRunsHaveTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    // Only pk2 should match (all its runs have env=prod)
    match result.selection.unwrap() {
        PartitionSelection::Keys(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys.contains(&pk2));
        }
        _ => panic!("expected Keys selection"),
    }
}

#[test]
fn test_new_update_tags_partitioned_no_materializations() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();
    let data = OwnedPartitionData::new(&["2024-01-01"], &["2024-01-01"], &[("2024-01-01", 10)]);
    let pctx = data.as_eval_ctx();
    let ctx = make_partitioned_ctx("a", &record, &records, &deps, &pctx);

    let cond = ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("env".to_string(), "prod".to_string())],
    };
    let result = evaluate(&cond, &ctx);
    assert!(!result.fired);
}

// LastRunIncludesTarget: dep's latest run also included the root asset in
// `asset_names`; always false when target_key == root_key (self-referential guard).
