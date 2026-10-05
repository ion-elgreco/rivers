use super::*;

#[test]
fn test_fingerprint_stability() {
    let tree = ConditionNode::eager();
    assert_eq!(tree.fingerprint(), tree.fingerprint());
    assert_eq!(tree.fingerprint_hex(), tree.fingerprint_hex());
    assert_eq!(tree.fingerprint_hex().len(), 16);
}

#[test]
fn test_fingerprint_sensitivity() {
    let eager = ConditionNode::eager();
    let on_missing = ConditionNode::on_missing();
    assert_ne!(eager.fingerprint(), on_missing.fingerprint());

    let cron1 = ConditionNode::on_cron("0 * * * *".into(), None);
    let cron2 = ConditionNode::on_cron("*/5 * * * *".into(), None);
    assert_ne!(cron1.fingerprint(), cron2.fingerprint());

    // Adding a node changes the fingerprint
    let base = ConditionNode::Missing;
    let extended = ConditionNode::Missing & !ConditionNode::InProgress;
    assert_ne!(base.fingerprint(), extended.fingerprint());
}

#[test]
fn test_reset_for_new_tree() {
    let mut state = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        last_handled_timestamp: Some(1000),
        last_materialized_timestamp: Some(500),

        last_tick_timestamp: Some(1000),
        ..Default::default()
    };
    state.reset_for_new_tree("abc123".into());

    assert!(state.previous_results.is_empty());
    assert!(state.last_handled_timestamp.is_none());
    assert_eq!(state.last_materialized_timestamp, Some(500)); // preserved

    assert!(state.last_tick_timestamp.is_none());
    assert_eq!(state.condition_fingerprint, "abc123");
    assert!(state.is_initial);
}

#[test]
fn test_default_fingerprint_never_matches() {
    let default = AssetConditionState::default();
    let real_fp = ConditionNode::eager().fingerprint_hex();
    assert_ne!(default.condition_fingerprint, real_fp);
    assert!(default.condition_fingerprint.is_empty());
}

/// Helper: run the same invalidation logic used by the daemon at startup.
fn run_invalidation(eval_state: &mut ConditionEvalState, conditions: &[(String, ConditionNode)]) {
    for (asset_key, condition) in conditions {
        let current_fp = condition.fingerprint_hex();
        let state = eval_state.assets.entry(asset_key.clone()).or_default();

        if state.condition_fingerprint == current_fp {
            continue;
        }
        state.reset_for_new_tree(current_fp);
    }

    let active: std::collections::HashSet<&str> = conditions.iter().map(|c| c.0.as_str()).collect();
    eval_state
        .assets
        .retain(|k, v| active.contains(k.as_str()) || v.last_materialized_timestamp.is_some());
}

#[test]
fn test_invalidation_on_tree_change() {
    // Simulate: daemon ran with eager(), persisted state, restarted with on_missing()
    let eager = ConditionNode::eager();
    let eager_fp = eager.fingerprint_hex();

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([(
            "asset_a".into(),
            AssetConditionState {
                previous_results: HashMap::from([(0, true), (1, false), (2, true)]),
                dep_previous_results: HashMap::new(),
                dep_baselines: HashMap::new(),
                last_handled_timestamp: Some(5000),
                last_materialized_timestamp: Some(3000),
                last_data_version: None,

                last_tick_timestamp: Some(5000),
                condition_fingerprint: eager_fp,
                is_initial: false,
                partition_state: None,
            },
        )]),
        is_initial: false,
        ..Default::default()
    };

    let on_missing = ConditionNode::on_missing();
    run_invalidation(&mut eval_state, &[("asset_a".into(), on_missing)]);

    let state = &eval_state.assets["asset_a"];
    assert!(
        state.previous_results.is_empty(),
        "previous_results should be cleared"
    );
    assert!(
        state.last_handled_timestamp.is_none(),
        "last_handled should be cleared"
    );
    assert_eq!(
        state.last_materialized_timestamp,
        Some(3000),
        "last_materialized should be preserved"
    );
    assert!(
        state.last_tick_timestamp.is_none(),
        "last_tick should be cleared"
    );
    assert!(state.is_initial, "should be marked as initial");
    assert_eq!(
        state.condition_fingerprint,
        ConditionNode::on_missing().fingerprint_hex(),
        "fingerprint should be updated to new tree"
    );
}

#[test]
fn test_invalidation_noop_on_unchanged_tree() {
    // Simulate: daemon restarts with the same condition tree — state preserved
    let eager = ConditionNode::eager();
    let eager_fp = eager.fingerprint_hex();

    let original_state = AssetConditionState {
        previous_results: HashMap::from([(0, true), (1, false)]),
        dep_previous_results: HashMap::new(),
        dep_baselines: HashMap::new(),
        last_handled_timestamp: Some(5000),
        last_materialized_timestamp: Some(3000),
        last_data_version: None,
        last_tick_timestamp: Some(5000),
        condition_fingerprint: eager_fp,
        is_initial: false,
        partition_state: None,
    };

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([("asset_a".into(), original_state.clone())]),
        is_initial: false,
        ..Default::default()
    };

    run_invalidation(
        &mut eval_state,
        &[("asset_a".into(), ConditionNode::eager())],
    );

    let state = &eval_state.assets["asset_a"];
    assert_eq!(state.previous_results, original_state.previous_results);
    assert_eq!(
        state.last_handled_timestamp,
        original_state.last_handled_timestamp
    );
    assert_eq!(
        state.last_materialized_timestamp,
        original_state.last_materialized_timestamp
    );
    assert_eq!(
        state.last_tick_timestamp,
        original_state.last_tick_timestamp
    );
    assert!(!state.is_initial, "should NOT be marked as initial");
}

#[test]
fn test_invalidation_prunes_removed_assets() {
    let eager_fp = ConditionNode::eager().fingerprint_hex();

    let mut eval_state = ConditionEvalState {
        assets: HashMap::from([
            (
                "asset_a".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp.clone(),
                    ..Default::default()
                },
            ),
            (
                "asset_b".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp.clone(),
                    ..Default::default()
                },
            ),
            (
                "asset_c".into(),
                AssetConditionState {
                    condition_fingerprint: eager_fp,
                    ..Default::default()
                },
            ),
        ]),
        is_initial: false,
        ..Default::default()
    };

    run_invalidation(
        &mut eval_state,
        &[("asset_a".into(), ConditionNode::eager())],
    );

    assert!(
        eval_state.assets.contains_key("asset_a"),
        "active asset should be kept"
    );
    assert!(
        !eval_state.assets.contains_key("asset_b"),
        "removed asset should be pruned"
    );
    assert!(
        !eval_state.assets.contains_key("asset_c"),
        "removed asset should be pruned"
    );
    assert_eq!(eval_state.assets.len(), 1);
}
