use super::*;

/// The eval-state blob carries a schema stamp for an explicit migration point (`migrate_loaded`);
/// a pre-versioning blob (no stamp) must read as version 0, not the current version.
#[test]
fn test_condition_eval_state_schema_version_stamps_and_migrates() {
    assert_eq!(
        ConditionEvalState::default().schema_version,
        EVAL_STATE_SCHEMA_VERSION,
        "fresh state must carry the current schema version"
    );

    let mut old: ConditionEvalState = serde_json::from_str("{}").unwrap();
    assert_eq!(
        old.schema_version, 0,
        "a blob written before versioning must load as version 0"
    );
    old.migrate_loaded();
    assert_eq!(old.schema_version, EVAL_STATE_SCHEMA_VERSION);

    let bytes = serde_json::to_vec(&ConditionEvalState::default()).unwrap();
    let round: ConditionEvalState = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        round.schema_version, EVAL_STATE_SCHEMA_VERSION,
        "the stamp must survive a persist round-trip"
    );
}

/// When an asset flips partitioned→unpartitioned with the same tree, the fingerprint is
/// unchanged so `reset_for_new_tree` never runs; `update_condition_state` must itself drop
/// the stale `partition_state` on an unpartitioned eval.
#[test]
fn test_update_condition_state_clears_stale_partition_state_when_unpartitioned() {
    let pk = PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    };
    let mut state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(pk, 100i64)]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // An unpartitioned evaluation result: `sub_selections` is None.
    let result = EvalResult {
        fired: false,
        ..Default::default()
    };
    let ctx = StateUpdateContext {
        target_record_timestamp: Some(200),
        target_data_version: None,
        now: 300,
        is_initial: false,
        partition_timestamps: None,
    };
    update_condition_state(&mut state, &ctx, &result);

    assert!(
        state.partition_state.is_none(),
        "stale partition_state must be cleared on an unpartitioned eval"
    );
}

/// The baseline is bounded by the partition_status snapshot, not the universe: a snapshot
/// key outside the universe (retired/def-change/future-cap) must keep its baseline, or it
/// reads newly-updated forever and re-adding it fires a spurious materialization.
#[test]
fn test_update_condition_state_keeps_baseline_for_snapshot_keys_outside_universe() {
    let live = spk("2024-01-02");
    let stale = spk("2023-12-31");
    let timestamps = HashMap::from([(live.clone(), 100i64), (stale.clone(), 50)]);

    let mut state = AssetConditionState::default();
    let result = EvalResult {
        fired: false,
        sub_selections: Some(HashMap::new()),
        ..Default::default()
    };
    update_condition_state(
        &mut state,
        &StateUpdateContext {
            target_record_timestamp: Some(100),
            target_data_version: None,
            now: 1_000,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        },
        &result,
    );

    let ps = state
        .partition_state
        .expect("partitioned eval must store partition_state");
    assert_eq!(
        ps.timestamps,
        HashMap::from([(live, 100i64), (stale, 50)]),
        "every snapshot key keeps its baseline, in or out of the universe"
    );
}

/// `ConditionEvalState` is persisted via `serde_json`, which can't use a `PartitionKey` as a
/// JSON map key; `PartitionState` timestamps are so keyed, so the state must still round-trip
/// or every restart wipes all latches.
#[test]
fn test_condition_eval_state_round_trips_with_partition_timestamps() {
    let pk = PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    };
    let mut asset = AssetConditionState::default();
    asset.partition_state = Some(PartitionState {
        timestamps: HashMap::from([(pk.clone(), 100i64)]),
        ..Default::default()
    });
    let mut state = ConditionEvalState::default();
    state.assets.insert("a".to_string(), asset);

    let bytes = serde_json::to_vec(&state)
        .expect("ConditionEvalState must serialize via serde_json (storage uses kv_set_json)");
    let round: ConditionEvalState =
        serde_json::from_slice(&bytes).expect("ConditionEvalState must round-trip");
    let ts = round.assets["a"]
        .partition_state
        .as_ref()
        .unwrap()
        .timestamps
        .get(&pk);
    assert_eq!(
        ts,
        Some(&100),
        "partition timestamp must survive the round-trip"
    );
}

/// An old blob missing newer fields must still load with defaults, not fail to deserialize
/// (which would silently reset all latches); guards `#[serde(default)]` coverage.
#[test]
fn test_condition_eval_state_tolerates_missing_fields() {
    // Top-level `is_initial` omitted; asset "a" carries only `is_initial`, every other field absent.
    let json = r#"{"assets":{"a":{"is_initial":true}}}"#;
    let state: ConditionEvalState =
        serde_json::from_str(json).expect("a partial/old blob must load with defaults");
    assert!(
        !state.is_initial,
        "missing top-level is_initial → default false"
    );
    let a = &state.assets["a"];
    assert!(a.is_initial);
    assert!(a.previous_results.is_empty());
    assert_eq!(a.condition_fingerprint, "");
    assert!(a.last_handled_timestamp.is_none());
    assert!(a.partition_state.is_none());
}

/// A pre-pairs-format blob stored `timestamps` as a JSON map (always empty `{}`); the
/// deserializer must accept that legacy shape, or the whole load fails and wipes every latch on upgrade.
#[test]
fn test_condition_eval_state_loads_legacy_map_shaped_timestamps() {
    let json =
        r#"{"assets":{"a":{"previous_results":{"3":true},"partition_state":{"timestamps":{}}}}}"#;
    let state: ConditionEvalState = serde_json::from_str(json)
        .expect("legacy blob with map-shaped empty timestamps must load, not reset all latches");
    let a = &state.assets["a"];
    assert_eq!(
        a.previous_results.get(&3),
        Some(&true),
        "latches must survive the legacy-shape load"
    );
    let ps = a
        .partition_state
        .as_ref()
        .expect("partition_state present in the blob must load");
    assert!(ps.timestamps.is_empty());
}

/// Upgrading past pre-data-version state (baseline last_data_version None, record Some,
/// is_initial false): the partitioned arm must gate on a materialization landing since the
/// last observation, not fire its whole universe.
#[test]
fn test_partitioned_data_version_changed_suppressed_for_pre_versioning_state() {
    let record = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), record.clone())]);
    let deps = HashMap::new();

    // Pre-versioning blob: baseline ts matches the record, version None.
    let mut prev = AssetConditionState::default();
    prev.last_materialized_timestamp = Some(100);
    prev.last_tick_timestamp = Some(900);
    prev.last_data_version = None;

    let mut ctx = make_ctx("a", &record, &records, &deps);
    ctx.prev_state = &prev;

    let k1 = spk("2024-01-01");
    let all_keys = HashSet::from([k1.clone()]);
    let timestamps = HashMap::from([(k1.clone(), 100i64)]);
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

    assert!(
        !evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "a missing baseline with no new materialization must not fire the whole universe"
    );

    // A version that appears WITH a fresh materialization is a real change.
    let record2 = make_materialized_record("a", 200);
    let records2 = HashMap::from([("a".to_string(), record2.clone())]);
    let mut ctx = make_ctx("a", &record2, &records2, &deps);
    ctx.prev_state = &prev; // baseline still at 100, version None
    ctx.partitions = Some(&pctx);
    assert!(
        evaluate(&ConditionNode::DataVersionChanged, &ctx).fired,
        "a version appearing with a new materialization must fire"
    );
}

/// The baseline must mirror the partition_status snapshot exactly: keys the snapshot no longer
/// contains must leave the persisted baseline too (in-place delta can't drift from replace semantics).
#[test]
fn test_update_condition_state_drops_baseline_keys_missing_from_snapshot() {
    let kept = spk("2024-01-02");
    let gone = spk("2024-01-01");

    let mut state = AssetConditionState {
        partition_state: Some(PartitionState {
            timestamps: HashMap::from([(kept.clone(), 50i64), (gone.clone(), 40)]),
            ..Default::default()
        }),
        ..Default::default()
    };
    // New snapshot: `gone` vanished, `kept` advanced.
    let timestamps = HashMap::from([(kept.clone(), 100i64)]);
    update_condition_state(
        &mut state,
        &StateUpdateContext {
            target_record_timestamp: Some(100),
            target_data_version: None,
            now: 1_000,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        },
        &EvalResult {
            fired: false,
            sub_selections: Some(HashMap::new()),
            ..Default::default()
        },
    );

    assert_eq!(
        state.partition_state.unwrap().timestamps,
        HashMap::from([(kept, 100i64)]),
        "baseline must equal the snapshot: stale key dropped, kept key advanced"
    );
}
