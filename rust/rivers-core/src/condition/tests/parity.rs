use super::*;

#[derive(Clone, Debug, Default)]
struct ParityScenario {
    /// Some(ts) => materialized at ts (bool: last_run_id + last_timestamp;
    /// partition: the single key present in `timestamps`).
    materialized_ts: Option<i64>,
    data_version: Option<String>,
    code_version: Option<String>,
    last_mat_code_version: Option<String>,
    in_progress: bool,
    failed: bool,
    is_initial: bool,
    requested: bool,
    now: i64,
}

/// Evaluate `tree` under `scn` in both modes, then evolve each mode's persisted
/// state exactly as production would. Returns each mode's result paired with its
/// next-tick state.
fn parity_eval_pair(
    scn: &ParityScenario,
    tree: &ConditionNode,
    prev_bool: &AssetConditionState,
    prev_part: &AssetConditionState,
) -> (
    (EvalResult, AssetConditionState),
    (EvalResult, AssetConditionState),
) {
    let key = "a";
    let mut record = make_record(key);
    if let Some(ts) = scn.materialized_ts {
        record.last_run_id = Some("run_a".to_string());
        record.last_timestamp = Some(ts);
    }
    record.last_data_version = scn.data_version.clone();
    record.code_version = scn.code_version.clone();
    record.last_materialization_code_version = scn.last_mat_code_version.clone();
    let records = HashMap::from([(key.to_string(), record.clone())]);
    let deps = HashMap::new();

    let in_progress_assets: HashSet<String> = if scn.in_progress {
        HashSet::from([key.to_string()])
    } else {
        HashSet::new()
    };
    let failed_assets: HashSet<String> = if scn.failed {
        HashSet::from([key.to_string()])
    } else {
        HashSet::new()
    };
    let requested: HashMap<String, PartitionSelection> = if scn.requested {
        HashMap::from([(key.to_string(), PartitionSelection::All)])
    } else {
        HashMap::new()
    };
    let cache = CacheSnapshot {
        records: &records,
        upstream_deps: &deps,
        in_progress_assets: &in_progress_assets,
        failed_assets: &failed_assets,
        failed_asset_timestamps: &EMPTY_FAILED_TS,
        backfill: &EMPTY_BACKFILL,
    };

    // ── bool mode ──
    let bool_ctx = EvalContext {
        target_key: key,
        root_key: key,
        target_record: &record,
        cache,
        tags: empty_tag_snapshot(),
        prev_state: prev_bool,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: scn.now,
        is_initial: scn.is_initial,
        partitions: None,
        root_partition_floor: None,
    };
    let bool_result = evaluate(tree, &bool_ctx);
    let mut next_bool = prev_bool.clone();
    update_condition_state(
        &mut next_bool,
        &StateUpdateContext::from_eval_context(&bool_ctx),
        &bool_result,
    );

    // ── partition mode: a one-key universe mirroring the scalar status ──
    let p = spk("p");
    let all_keys = HashSet::from([p.clone()]);
    let mut timestamps = HashMap::new();
    if let Some(ts) = scn.materialized_ts {
        timestamps.insert(p.clone(), ts);
    }
    let in_progress_p: HashSet<PartitionKey> = if scn.in_progress {
        HashSet::from([p.clone()])
    } else {
        HashSet::new()
    };
    let failed_p: HashSet<PartitionKey> = if scn.failed {
        HashSet::from([p.clone()])
    } else {
        HashSet::new()
    };
    let empty_statuses = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &in_progress_p,
        failed: &failed_p,
        timestamps: &timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: None,
        all_partition_statuses: &empty_statuses,
        dep_root_floor: None,
    };
    let part_ctx = EvalContext {
        target_key: key,
        root_key: key,
        target_record: &record,
        cache,
        tags: empty_tag_snapshot(),
        prev_state: prev_part,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &requested,
        now: scn.now,
        is_initial: scn.is_initial,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let part_result = evaluate(tree, &part_ctx);
    let mut next_part = prev_part.clone();
    update_condition_state(
        &mut next_part,
        &StateUpdateContext::from_eval_context(&part_ctx),
        &part_result,
    );

    ((bool_result, next_bool), (part_result, next_part))
}

/// Returns the per-tick `fired` outcomes (identical across both modes once parity
/// is asserted) so the caller can prove the corpus is non-vacuous.
fn assert_tree_parity_over_ticks(tree: &ConditionNode, seq: &[ParityScenario]) -> Vec<bool> {
    let mut bool_state = AssetConditionState::default();
    let mut part_state = AssetConditionState::default();
    let mut fired = Vec::with_capacity(seq.len());
    for (tick, scn) in seq.iter().enumerate() {
        let ((b, next_bool), (p, next_part)) =
            parity_eval_pair(scn, tree, &bool_state, &part_state);
        assert_eq!(
            b.fired, p.fired,
            "fired parity broke at tick {tick}: bool={} part={}\n  tree: {tree:?}\n  scenario: {scn:?}",
            b.fired, p.fired
        );
        let b_keys: HashSet<u32> = b.sub_results.keys().copied().collect();
        let p_keys: HashSet<u32> = p
            .sub_selections
            .as_ref()
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default();
        assert_eq!(
            b_keys, p_keys,
            "stateful-latch index drift at tick {tick}\n  tree: {tree:?}\n  scenario: {scn:?}"
        );
        fired.push(b.fired);
        bool_state = next_bool;
        part_state = next_part;
    }
    fired
}

/// Trees exercising every combinator and stateful op over the leaves whose
/// bool and partition readings correspond under a unit universe.
fn parity_corpus() -> Vec<ConditionNode> {
    use ConditionNode as C;
    let leaves = [
        C::Missing,
        C::InProgress,
        C::ExecutionFailed,
        C::NewlyUpdated,
        C::WillBeRequested,
        C::DataVersionChanged,
        C::CodeVersionChanged,
        C::InitialEvaluation,
        C::NewlyRequested,
    ];
    let mut corpus = Vec::new();
    for l in &leaves {
        corpus.push(l.clone());
        corpus.push(C::Not(Box::new(l.clone())));
        corpus.push(C::NewlyTrue(Box::new(l.clone())));
        corpus.push(C::SinceLastHandled(Box::new(l.clone())));
    }
    corpus.push(C::And(vec![C::Missing, C::InProgress]));
    corpus.push(C::Or(vec![C::Missing, C::InProgress]));
    corpus.push(C::And(vec![
        C::NewlyUpdated,
        C::Not(Box::new(C::ExecutionFailed)),
        C::Not(Box::new(C::InProgress)),
    ]));
    corpus.push(C::Or(vec![C::Missing, C::NewlyUpdated, C::WillBeRequested]));
    // Stateful ops at varying tree positions (indices must line up in both modes).
    corpus.push(C::And(vec![
        C::Missing,
        C::NewlyTrue(Box::new(C::InProgress)),
        C::Since {
            trigger: Box::new(C::NewlyUpdated),
            reset: Box::new(C::ExecutionFailed),
        },
    ]));
    corpus.push(C::Since {
        trigger: Box::new(C::NewlyUpdated),
        reset: Box::new(C::ExecutionFailed),
    });
    corpus.push(C::SinceLastHandled(Box::new(C::And(vec![
        C::NewlyUpdated,
        C::Not(Box::new(C::InProgress)),
    ]))));
    corpus.push(C::NewlyTrue(Box::new(C::Or(vec![
        C::Missing,
        C::NewlyUpdated,
    ]))));
    corpus.push(C::And(vec![
        C::Or(vec![C::NewlyTrue(Box::new(C::Missing)), C::WillBeRequested]),
        C::Not(Box::new(C::Since {
            trigger: Box::new(C::InProgress),
            reset: Box::new(C::Missing),
        })),
    ]));
    corpus.push(C::eager());
    corpus
}

/// Multi-tick scenario sequences; state threads across ticks within a sequence.
fn parity_scenarios() -> Vec<Vec<ParityScenario>> {
    vec![
        // Initial → materialized → updated: Missing/NewlyUpdated transitions.
        vec![
            ParityScenario {
                is_initial: true,
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(100),
                now: 2000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(200),
                now: 3000,
                ..Default::default()
            },
        ],
        // in_progress toggling.
        vec![
            ParityScenario {
                materialized_ts: Some(100),
                in_progress: true,
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(100),
                now: 2000,
                ..Default::default()
            },
        ],
        // failure toggling.
        vec![
            ParityScenario {
                materialized_ts: Some(100),
                failed: true,
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(100),
                now: 2000,
                ..Default::default()
            },
        ],
        // requested toggling (WillBeRequested).
        vec![
            ParityScenario {
                materialized_ts: Some(100),
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(100),
                requested: true,
                now: 2000,
                ..Default::default()
            },
        ],
        // data-version churn.
        vec![
            ParityScenario {
                materialized_ts: Some(100),
                data_version: Some("v1".into()),
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(200),
                data_version: Some("v2".into()),
                now: 2000,
                ..Default::default()
            },
        ],
        // code-version change.
        vec![ParityScenario {
            materialized_ts: Some(100),
            code_version: Some("c2".into()),
            last_mat_code_version: Some("c1".into()),
            now: 1000,
            ..Default::default()
        }],
        // A rich sequence toggling many fields across five ticks.
        vec![
            ParityScenario {
                is_initial: true,
                now: 1000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(100),
                in_progress: true,
                now: 2000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(200),
                failed: true,
                requested: true,
                now: 3000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(200),
                now: 4000,
                ..Default::default()
            },
            ParityScenario {
                materialized_ts: Some(300),
                requested: true,
                now: 5000,
                ..Default::default()
            },
        ],
    ]
}

#[test]
fn test_differential_parity_bool_vs_unit_partition() {
    let corpus = parity_corpus();
    let scenarios = parity_scenarios();
    let mut saw_fired = false;
    let mut saw_not_fired = false;
    for tree in &corpus {
        for seq in &scenarios {
            for outcome in assert_tree_parity_over_ticks(tree, seq) {
                saw_fired |= outcome;
                saw_not_fired |= !outcome;
            }
        }
    }
    // Guard against the harness silently going vacuous (e.g. every tree always
    // false would make parity trivially hold): the corpus must exercise both.
    assert!(saw_fired, "corpus never fired — parity check is vacuous");
    assert!(
        saw_not_fired,
        "corpus always fired — parity check is vacuous"
    );
}
