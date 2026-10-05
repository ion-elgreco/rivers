use super::*;

#[test]
fn dep_updated_requires_dep_newer_than_target_key() {
    // A dep key counts as updated only while strictly newer than the root's own
    // materialization of that key (self-suppressing once the root advances past it).
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let pk1 = spk("2024-01-01");
    let pk2 = spk("2024-01-02");
    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    // Root "a" materialized pk1 and pk2 at 100.
    let a_timestamps = HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 100)]);

    // Dep "b": pk1 ts equals the root's (nothing new), pk2 genuinely newer;
    // empty eval-state baseline (drain-lag shape).
    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 100i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    // Solo runs: no joint-run suppression in play.
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (pk1.clone(), Arc::from(vec!["b".to_string()])),
            (pk2.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

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
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);

    assert!(result.fired, "pk2 is genuinely newer than the root");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert!(keys.contains(&pk2));
            assert!(
                !keys.contains(&pk1),
                "a dep key no newer than the root's must not count as updated"
            );
            assert_eq!(keys.len(), 1);
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn partitioned_root_unpartitioned_dep_refires_stale_older_partitions() {
    // A partitioned root on an unpartitioned dep (bool fallback): the staleness
    // floor must be the root's oldest partition attempt (min), not the asset-level
    // max, or genuinely-stale older partitions never re-fire.
    let pk_old = spk("2024-01-01");
    let pk_mid = spk("2024-01-02");
    let pk_new = spk("2024-01-03");
    let all_keys = HashSet::from([pk_old.clone(), pk_mid.clone(), pk_new.clone()]);

    // Root "a": partitions at 10 / 30 / 50; the asset-level record carries the max (50).
    let a = make_materialized_record("a", 50);
    // Dep "b": unpartitioned, updated at 35 — newer than pk_old/pk_mid, older than pk_new.
    let b = make_materialized_record("b", 35);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([
        (pk_old.clone(), 10i64),
        (pk_mid.clone(), 30),
        (pk_new.clone(), 50),
    ]);
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("a".to_string(), a_partition_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    // "b" absent from upstream_partition_keys → unpartitioned dep → bool fallback.
    let no_upstream_keys = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    let covers = |sel: &Option<PartitionSelection>, k: &PartitionKey| match sel {
        Some(PartitionSelection::All) => true,
        Some(PartitionSelection::Keys(ks)) => ks.contains(k),
        _ => false,
    };
    // dep@35 newer than the older partition attempts (10, 30) → the edge must
    // fire and re-materialize those stale partitions.
    assert!(
        result.fired,
        "dep@35 newer than older partitions (10/30) → must re-fire, got {:?}",
        result.selection
    );
    assert!(
        covers(&result.selection, &pk_old),
        "stale pk_old (mat@10 < dep@35) must be selected, got {:?}",
        result.selection
    );
    assert!(
        covers(&result.selection, &pk_mid),
        "stale pk_mid (mat@30 < dep@35) must be selected, got {:?}",
        result.selection
    );
}

#[test]
fn all_partitions_dep_frontier_key_does_not_refire_whole_universe() {
    // AllPartitions floors the dep against the min effective ts over the universe;
    // the floor must ignore never-attempted frontier keys, else a new key refires
    // the whole universe with no upstream change.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // freshly minted, never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 90);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    // Root "a": d1/d2 at 100, d3 never attempted. Dep "b": u1 at 90, older than every attempted key.
    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 90i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    // Dep u1@90 older than every attempted key (100); new frontier d3 must not drag the floor to None and broadcast All.
    assert!(
        !result.fired,
        "a never-attempted frontier key must not refire the universe with no \
         upstream change, got {:?}",
        result.selection
    );
}

#[test]
fn empty_universe_all_selection_does_not_fire() {
    // `All` of an empty partition universe selects nothing; reporting
    // fired=true would leak a full WillBeRequested signal to downstreams
    // evaluated later in the same tick.
    let a = make_materialized_record("a", 100);
    let records = HashMap::from([("a".to_string(), a.clone())]);
    let deps = HashMap::new();
    let all_keys: HashSet<PartitionKey> = HashSet::new();
    let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
    let partition_statuses = HashMap::from([(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry::default(),
    )]);
    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;
    ctx.is_initial = true; // InitialEvaluation yields `All` independent of the universe

    let empty_mappings = HashMap::new();
    let no_upstream = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::InitialEvaluation, &ctx);
    assert!(
        !result.fired,
        "All over an empty universe must not fire; got {:?}",
        result.selection
    );
}

#[test]
fn unpartitioned_dep_frontier_key_does_not_refire_whole_universe() {
    // The bridged (unpartitioned-dep) path floors the dep against the whole
    // root universe like an AllPartitions edge; the floor must ignore
    // never-attempted frontier keys, else a freshly-minted key drags the
    // floor to None and the dep refires every partition each tick.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // freshly minted, never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 90); // older than every attempted key
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("a".to_string(), a_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    // "b" absent from upstream_partition_keys → unpartitioned dep → bridged bool path.
    let no_upstream_keys = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &no_upstream_keys),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);

    assert!(
        !result.fired,
        "a never-attempted frontier key must not make the unpartitioned dep \
         (@90, older than every attempted key @100) refire the universe, got {:?}",
        result.selection
    );
}

#[test]
fn test_empty_partitioned_dep_universe_does_not_bridge_latch_to_all() {
    // An empty-universe partitioned dep (present with empty set, unlike an absent
    // unpartitioned dep) must take the partitioned path, not the bool fallback that
    // bridges a stateful latch to `All` and later fires the whole universe.
    let rk = spk("rk1");
    let all_keys = HashSet::from([rk.clone()]);
    let r = make_materialized_record("r", 100);
    let u = make_record("u"); // never materialized → Missing is true
    let records = HashMap::from([("r".to_string(), r.clone()), ("u".to_string(), u.clone())]);
    let deps = HashMap::from([("r".to_string(), vec!["u".to_string()])]);

    let partition_statuses = HashMap::from([("r".to_string(), Default::default())]);
    let all_states = HashMap::new();
    let mut ctx = make_ctx("r", &r, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(("r".into(), "u".into()), PartitionMappingKind::Identity)]);
    // u PRESENT with an EMPTY universe (partitioned, no keys yet).
    let upstream_u = HashMap::from([("u".to_string(), HashSet::<PartitionKey>::new())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &HashMap::new(),
        resolver: PartitionResolver::new(&mappings, &upstream_u),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let condition = ConditionNode::AnyDepsMatch {
        condition: Box::new(ConditionNode::NewlyTrue(Box::new(ConditionNode::Missing))),
        label: None,
    };
    let result = evaluate(&condition, &ctx);

    if let Some(dep_sels) = &result.dep_sub_selections {
        for (dep, latch) in dep_sels {
            for (idx, sel) in latch {
                assert_ne!(
                    sel,
                    &PartitionSelection::All,
                    "empty-universe dep {dep} latched node {idx} as All \
                     (bool-fallback bridge); the partitioned path must be taken"
                );
            }
        }
    }
}

#[test]
fn all_partitions_dep_genuine_update_still_fires() {
    // When an upstream key is newer than an attempted downstream key, the AllPartitions edge must still fire.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let d3 = spk("d3"); // never attempted
    let all_keys = HashSet::from([d1.clone(), d2.clone(), d3.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 150);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let a_timestamps = HashMap::from([(d1.clone(), 100i64), (d2.clone(), 100)]);
    let a_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 150i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);
    assert!(
        result.fired,
        "upstream u1@150 newer than attempted downstream (100) must fire, got {:?}",
        result.selection
    );
}

#[test]
fn all_partitions_dep_initial_population_fires() {
    // A never-materialized AllPartitions fan-out must still fire to populate itself:
    // a None per-key floor means "never materialized ⇒ updated" (fire), not exclude.
    let d1 = spk("d1");
    let d2 = spk("d2");
    let all_keys = HashSet::from([d1.clone(), d2.clone()]);
    let u1 = spk("u1");
    let up_keys = HashSet::from([u1.clone()]);

    let a = make_record("a"); // never materialized
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    // Root "a": nothing attempted. Dep "b": u1 materialized at 100.
    let a_status = crate::condition::cache::PartitionStatusEntry::default();
    let b_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(u1.clone(), 100i64)]),
        ..Default::default()
    };
    let partition_statuses =
        HashMap::from([("a".to_string(), a_status), ("b".to_string(), b_status)]);

    let all_states = HashMap::new();
    let mut ctx = make_ctx("a", &a, &records, &deps);
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".into(), "b".into()),
        PartitionMappingKind::AllPartitions,
    )]);
    let upstream_b = HashMap::from([("b".to_string(), up_keys.clone())]);
    let empty_ts: HashMap<PartitionKey, i64> = HashMap::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &empty_ts,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let result = evaluate(&ConditionNode::any_deps_updated(), &ctx);
    assert!(
        result.fired,
        "a never-materialized fan-out downstream must fire to populate itself, got {:?}",
        result.selection
    );
}

#[test]
fn dep_updated_floor_compares_mapped_downstream_key() {
    // The staleness floor must compare a dep key against the root's materialization
    // of the mapped downstream key, not a same-named key. With time_window(offset=-1),
    // b@D drives a@(D+1) and self-suppresses once a@(D+1) is newer.
    let a = make_materialized_record("a", 400);
    let b = make_materialized_record("b", 300);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(86400.0),
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: Some(jiff::civil::date(2024, 2, 1).at(0, 0, 0, 0)),
        fmt: "%Y-%m-%d".to_string(),
    };

    let b_keys = HashSet::from([spk("2024-01-04"), spk("2024-01-05")]);
    let a_keys = HashSet::from([spk("2024-01-05"), spk("2024-01-06")]);
    // Root already consumed both dep updates: a@05 (from b@04) and a@06 (from b@05) newer than their driving dep keys.
    let a_timestamps = HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 400)]);

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    // Solo runs: no joint-run suppression in play.
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (spk("2024-01-04"), Arc::from(vec!["b".to_string()])),
            (spk("2024-01-05"), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let mappings = HashMap::from([(
        ("a".to_string(), "b".to_string()),
        PartitionMappingKind::TimeWindow {
            offset: -1,
            grid: Some(grid),
        },
    )]);
    let upstream_b = HashMap::from([("b".to_string(), b_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "every dep key's mapped downstream key is already newer; got {:?}",
        result.selection
    );

    // Control: root's mapped key a@06 older than b@05 → that downstream key fires.
    let a_timestamps_stale = HashMap::from([(spk("2024-01-05"), 100i64), (spk("2024-01-06"), 200)]);
    let statuses_stale = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: a_timestamps_stale.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(spk("2024-01-04"), 50i64), (spk("2024-01-05"), 300)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_stale = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps_stale,
        resolver: PartitionResolver::new(&mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_stale,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_stale);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired, "a@06 is older than b@05 now");
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert_eq!(keys.len(), 1);
            assert!(
                keys.contains(&spk("2024-01-06")),
                "the fire must target the mapped downstream key"
            );
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn will_be_requested_carries_the_upstream_fired_selection() {
    // A partitioned upstream that fired for one key this tick must make only the
    // mapped downstream key eligible via any_deps_updated's WillBeRequested branch,
    // not the whole universe.
    let down = make_materialized_record("down", 100);
    let up = make_materialized_record("up", 100);
    let records = HashMap::from([
        ("down".to_string(), down.clone()),
        ("up".to_string(), up.clone()),
    ]);
    let deps = HashMap::from([("down".to_string(), vec!["up".to_string()])]);

    let pa = spk("a");
    let pb = spk("b");
    let pc = spk("c");
    let keys = HashSet::from([pa.clone(), pb.clone(), pc.clone()]);
    // Equal timestamps on both sides keep NewlyUpdated quiet, isolating WillBeRequested.
    let ts: HashMap<PartitionKey, i64> = keys.iter().map(|k| (k.clone(), 100i64)).collect();

    let all_states = HashMap::new();
    let partition_statuses = HashMap::from([
        (
            "down".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: ts.clone(),
                ..Default::default()
            },
        ),
        (
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: ts.clone(),
                ..Default::default()
            },
        ),
    ]);
    let partition_asset_names = HashMap::from([(
        "up".to_string(),
        keys.iter()
            .map(|k| (k.clone(), Arc::from(vec!["up".to_string()])))
            .collect::<HashMap<_, _>>(),
    )]);

    // Upstream's own condition fired for 'a' only, earlier this tick.
    let requested = HashMap::from([(
        "up".to_string(),
        PartitionSelection::Keys(HashSet::from([pa.clone()])),
    )]);

    let mut ctx = make_ctx("down", &down, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;
    ctx.requested_this_tick = &requested;

    let empty_mappings = HashMap::new();
    let upstream_up = HashMap::from([("up".to_string(), keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &ts,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_up),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref sel)) => {
            assert_eq!(
                sel,
                &HashSet::from([pa.clone()]),
                "only the upstream's fired key may cascade, not the universe"
            );
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn dep_updated_retries_once_per_dep_update_after_failure() {
    // A failed run never advances the materialization floor; the failure timestamp
    // must raise it — suppressed while the failure postdates the dep update, retried
    // when the dep lands something newer.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 300);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let k = spk("2024-01-01");
    let keys = HashSet::from([k.clone()]);
    // Root "a" never succeeded for k; its run at 400 failed.
    let a_timestamps: HashMap<PartitionKey, i64> = HashMap::new();

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(k.clone(), 300i64)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        failed: HashSet::from([k.clone()]),
        failed_timestamps: HashMap::from([(k.clone(), 400i64)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([(k.clone(), Arc::from(vec!["b".to_string()]))]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "the failed attempt at 400 already consumed the dep update at 300; \
         re-firing every tick is an unbounded retry loop; got {:?}",
        result.selection
    );

    // Control: dep lands new data (500 > failure 400) → exactly one retry becomes due.
    let statuses_retry = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                failed: HashSet::from([k.clone()]),
                failed_timestamps: HashMap::from([(k.clone(), 400i64)]),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(k.clone(), 500i64)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_retry = PartitionEvalContext {
        all_keys: &keys,
        in_progress: &HashSet::new(),
        failed: &keys,
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_retry,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_retry);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired, "a newer dep update must retry the failed key");
    match result.selection {
        Some(PartitionSelection::Keys(ref sel)) => {
            assert_eq!(sel.len(), 1);
            assert!(sel.contains(&k));
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn dep_updated_ignores_dep_keys_outside_root_universe() {
    // Identity dep whose upstream range is a superset of the root's (upstream since
    // 2020, downstream start=2024): upstream-only keys can never be dispatched
    // downstream, so must not count as updated.
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 500);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);

    let old = spk("2020-01-01");
    let shared = spk("2024-06-01");
    let b_keys = HashSet::from([old.clone(), shared.clone()]);
    let a_keys = HashSet::from([shared.clone()]);
    let a_timestamps = HashMap::from([(shared.clone(), 100i64)]);

    let all_states = HashMap::new();
    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 100)]),
        ..Default::default()
    };
    let a_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: a_timestamps.clone(),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([
        ("a".to_string(), a_partition_status),
        ("b".to_string(), b_partition_status),
    ]);
    let partition_asset_names = HashMap::from([(
        "b".to_string(),
        HashMap::from([
            (old.clone(), Arc::from(vec!["b".to_string()])),
            (shared.clone(), Arc::from(vec!["b".to_string()])),
        ]),
    )]);

    let mut ctx = make_ctx("a", &a, &records, &deps);
    let partition_asset_names = slotted_parts(partition_asset_names);
    ctx.tags.last_run_asset_names = &partition_asset_names;
    ctx.all_asset_states = &all_states;

    let empty_mappings = HashMap::new();
    let upstream_b = HashMap::from([("b".to_string(), b_keys.clone())]);
    let pctx = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    ctx.partitions = Some(&pctx);

    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        !result.fired,
        "an upstream-only key must not fire the condition; got {:?}",
        result.selection
    );

    // Control: a genuine update of the shared key still fires it alone.
    let statuses_new = HashMap::from([
        (
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: a_timestamps.clone(),
                ..Default::default()
            },
        ),
        (
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: HashMap::from([(old.clone(), 500i64), (shared.clone(), 150)]),
                ..Default::default()
            },
        ),
    ]);
    let pctx_new = PartitionEvalContext {
        all_keys: &a_keys,
        in_progress: &HashSet::new(),
        failed: &HashSet::new(),
        timestamps: &a_timestamps,
        resolver: PartitionResolver::new(&empty_mappings, &upstream_b),
        time_windows: None,
        all_partition_statuses: &statuses_new,
        dep_root_floor: None,
    };
    let mut ctx2 = make_ctx("a", &a, &records, &deps);
    ctx2.tags.last_run_asset_names = &partition_asset_names;
    ctx2.all_asset_states = &all_states;
    ctx2.partitions = Some(&pctx_new);
    let result = evaluate(&cond, &ctx2);
    assert!(result.fired);
    match result.selection {
        Some(PartitionSelection::Keys(ref keys)) => {
            assert_eq!(
                keys.len(),
                1,
                "only the shared key may fire, never the phantom: {keys:?}"
            );
            assert!(keys.contains(&shared));
        }
        other => panic!("expected Keys selection, got {other:?}"),
    }
}

#[test]
fn test_any_deps_missing() {
    let a = make_record("a"); // missing
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_any_deps_missing_none_missing() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    assert!(!evaluate(&ConditionNode::any_deps_missing(), &ctx).fired);
}

#[test]
fn test_any_deps_in_progress() {
    // "b" depends on "a", and "a" is in progress → true
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let in_progress = HashSet::from(["a".to_string()]);
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
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_in_progress(), &ctx).fired);
}

#[test]
fn test_any_deps_in_progress_none() {
    // "b" depends on "a", but "a" is not in progress → false
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
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
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(!evaluate(&ConditionNode::any_deps_in_progress(), &ctx).fired);
}

#[test]
fn test_any_deps_updated() {
    let a = make_materialized_record("a", 200); // updated
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        ..Default::default()
    };
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
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    };
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx).fired);
}

#[test]
fn unpartitioned_dep_updated_compares_against_root_record() {
    // The root ran at 110 reading b@99; b drains a tick later with a stale baseline (50).
    // The staleness floor must see the root is already newer and suppress.
    let r = make_materialized_record("R", 110);
    let b = make_materialized_record("b", 99);
    let records = HashMap::from([("R".to_string(), r.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("R".to_string(), vec!["b".to_string()])]);
    let b_state = AssetConditionState {
        last_materialized_timestamp: Some(50),
        ..Default::default()
    };
    let all_states = HashMap::from([("b".to_string(), b_state)]);

    let mut ctx = make_ctx("R", &r, &records, &deps);
    ctx.all_asset_states = &all_states;
    assert!(
        !evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "the root's run at 110 already consumed b@99"
    );

    // Control: b lands genuinely newer than the root -> fires.
    let b_new = make_materialized_record("b", 120);
    let records_new = HashMap::from([
        ("R".to_string(), r.clone()),
        ("b".to_string(), b_new.clone()),
    ]);
    let mut ctx2 = make_ctx("R", &r, &records_new, &deps);
    ctx2.all_asset_states = &all_states;
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx2).fired);
}

#[test]
fn unpartitioned_dep_updated_failed_root_retries_once() {
    // A failed root run consumes the dep update that triggered it; suppressed while
    // the failure postdates the dep, one retry when a newer dep lands.
    let r = make_materialized_record("R", 100);
    let b = make_materialized_record("b", 120);
    let records = HashMap::from([("R".to_string(), r.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("R".to_string(), vec!["b".to_string()])]);
    let failed_ts = HashMap::from([("R".to_string(), 130i64)]);
    let all_states = HashMap::new();

    let mut ctx = make_ctx("R", &r, &records, &deps);
    ctx.all_asset_states = &all_states;
    ctx.cache.failed_asset_timestamps = &failed_ts;
    assert!(
        !evaluate(&ConditionNode::any_deps_updated(), &ctx).fired,
        "the failed attempt at 130 already consumed b@120"
    );

    // Control: b lands after the failure -> one retry becomes due.
    let b_new = make_materialized_record("b", 140);
    let records_new = HashMap::from([
        ("R".to_string(), r.clone()),
        ("b".to_string(), b_new.clone()),
    ]);
    let mut ctx2 = make_ctx("R", &r, &records_new, &deps);
    ctx2.all_asset_states = &all_states;
    ctx2.cache.failed_asset_timestamps = &failed_ts;
    assert!(evaluate(&ConditionNode::any_deps_updated(), &ctx2).fired);
}

#[test]
fn test_any_deps_updated_no_change() {
    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let prev = AssetConditionState {
        ..Default::default()
    };
    // Dep A needs state so NewlyUpdated sees no change (100 == 100)
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
    assert!(!evaluate(&ConditionNode::any_deps_updated(), &ctx).fired);
}

#[test]
fn test_all_deps_match_with_no_deps() {
    let a = make_record("a");
    let records = HashMap::from([("a".to_string(), a.clone())]);
    let deps = HashMap::new(); // no deps
    let ctx = make_ctx("a", &a, &records, &deps);
    // AllDepsMatch with empty deps → true (vacuous truth)
    let cond = ConditionNode::all_deps_match(ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

#[test]
fn test_any_deps_match_recursive() {
    let a = make_record("a"); // missing
    let b = make_materialized_record("b", 100);
    let records = HashMap::from([("a".to_string(), a.clone()), ("b".to_string(), b.clone())]);
    let deps = HashMap::from([("b".to_string(), vec!["a".to_string()])]);
    let ctx = make_ctx("b", &b, &records, &deps);
    // AnyDepsMatch(Missing) → true because dep a is missing
    let cond = ConditionNode::any_deps_match(ConditionNode::Missing);
    assert!(evaluate(&cond, &ctx).fired);
}

/// Nested dep-of-dep pivot with mismatched key spaces: the bridge floor for an
/// unpartitioned dep must come from the ROOT's own universe, not from the
/// intermediate dep's keys looked up in the root's status (which collapses the
/// floor to None and fires forever).
#[test]
fn test_nested_dep_pivot_floor_uses_root_universe() {
    let cond =
        ConditionNode::any_deps_match(ConditionNode::any_deps_match(ConditionNode::NewlyUpdated));

    // r (date keys) ← m (region keys, AllPartitions mapping) ← u (unpartitioned).
    let r = make_materialized_record("r", 500);
    let m = make_materialized_record("m", 400);
    let mut u = make_materialized_record("u", 100); // older than r's floor of 500
    let deps: HashMap<String, Vec<String>> = HashMap::from([
        ("r".into(), vec!["m".into()]),
        ("m".into(), vec!["u".into()]),
    ]);

    let upstream_keys: HashMap<String, HashSet<PartitionKey>> =
        HashMap::from([("m".into(), HashSet::from([spk("eu"), spk("us")]))]);
    let mappings = HashMap::from([(
        ("r".into(), "m".into()),
        PartitionMappingKind::AllPartitions,
    )]);

    let statuses = HashMap::from([
        (
            "r".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("d1"), 500), (spk("d2"), 500)]),
            },
        ),
        (
            "m".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("eu"), 400), (spk("us"), 400)]),
            },
        ),
    ]);

    let _ak = HashSet::from([spk("d1"), spk("d2")]);
    let _mat: HashSet<PartitionKey> = HashSet::from([spk("d1"), spk("d2")]);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let _ts = HashMap::from([(spk("d1"), 500_i64), (spk("d2"), 500)]);

    let eval_with = |records: HashMap<String, AssetRecord>| {
        let pctx = PartitionEvalContext {
            all_keys: &_ak,
            in_progress: &_ip,
            failed: &_fail,
            timestamps: &_ts,
            resolver: PartitionResolver::new(&mappings, &upstream_keys),
            time_windows: None,
            all_partition_statuses: &statuses,
            dep_root_floor: None,
        };
        let prev = AssetConditionState::default();
        let states: HashMap<String, AssetConditionState> = HashMap::new();
        let ctx = EvalContext {
            target_key: "r",
            root_key: "r",
            target_record: records.get("r").unwrap(),
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
            now: 1_000_000_000,
            is_initial: false,
            partitions: Some(&pctx),
            root_partition_floor: None,
        };
        evaluate(&cond, &ctx)
    };

    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("m".to_string(), m.clone()),
        ("u".to_string(), u.clone()),
    ]);
    let result = eval_with(records);
    assert!(
        !result.fired,
        "u (ts=100) is older than r's floor (500): nested newly_updated must not fire"
    );

    // Positive control: u genuinely newer than the root's floor must fire.
    u.last_timestamp = Some(600);
    let records = HashMap::from([
        ("r".to_string(), r.clone()),
        ("m".to_string(), m.clone()),
        ("u".to_string(), u.clone()),
    ]);
    let result = eval_with(records);
    assert!(
        result.fired,
        "u (ts=600) newer than r's floor (500) must fire the nested condition"
    );
}

#[test]
fn test_update_dep_baselines_stores_partition_timestamps() {
    // update_dep_baselines populates partition_state.timestamps for non-conditioned deps, so NewlyUpdated has a baseline next tick.
    let pk1 = spk("2025-01-01");
    let pk2 = spk("2025-01-02");

    let mut eval_state: HashMap<String, AssetConditionState> = HashMap::new();
    let upstream_deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    let conditioned = HashSet::from(["a".to_string()]); // "a" has condition, "b" does not
    let partition_statuses = HashMap::from([(
        "b".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 300)]),
            ..Default::default()
        },
    )]);
    let records = HashMap::from([("b".to_string(), make_materialized_record("b", 200))]);

    // Before: "b" has no state at all
    assert!(!eval_state.contains_key("b"));

    update_dep_baselines(
        &mut eval_state,
        &["a".to_string()],
        &upstream_deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    // After: "b" has partition timestamps and last_materialized_timestamp
    let b_state = eval_state
        .get("b")
        .expect("b should have state after baseline");
    assert_eq!(b_state.last_materialized_timestamp, Some(200));
    let ps = b_state
        .partition_state
        .as_ref()
        .expect("partition_state should be set");
    assert_eq!(ps.timestamps.get(&pk1), Some(&200));
    assert_eq!(ps.timestamps.get(&pk2), Some(&300));
}

#[test]
fn test_update_dep_baselines_skips_conditioned_assets() {
    // Deps that have their own condition should NOT be touched by update_dep_baselines.
    let mut eval_state: HashMap<String, AssetConditionState> = HashMap::new();
    let upstream_deps = HashMap::from([("a".to_string(), vec!["b".to_string()])]);
    let conditioned = HashSet::from(["a".to_string(), "b".to_string()]); // both conditioned
    let partition_statuses = HashMap::from([(
        "b".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("p1"), 100_i64)]),
            ..Default::default()
        },
    )]);
    let records = HashMap::from([("b".to_string(), make_materialized_record("b", 100))]);

    update_dep_baselines(
        &mut eval_state,
        &["a".to_string()],
        &upstream_deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    // "b" is conditioned → should not have state from baseline
    assert!(!eval_state.contains_key("b"));
}

#[test]
fn test_update_dep_baselines_prevents_newly_updated_false_positive() {
    // End-to-end: without baseline, NewlyUpdated fires; with baseline, it doesn't.
    let pk1 = spk("2025-01-01");
    let pk2 = spk("2025-01-02");

    let a = make_materialized_record("a", 100);
    let b = make_materialized_record("b", 200);
    let records = HashMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]);
    let deps = HashMap::from([("a".into(), vec!["b".into()])]);

    let b_partition_status = crate::condition::cache::PartitionStatusEntry {
        timestamps: HashMap::from([(pk1.clone(), 200_i64), (pk2.clone(), 200)]),
        ..Default::default()
    };
    let partition_statuses = HashMap::from([("b".into(), b_partition_status)]);

    // No baseline → NewlyUpdated false-positives.
    let empty_states: HashMap<String, AssetConditionState> = HashMap::new();

    let all_keys = HashSet::from([pk1.clone(), pk2.clone()]);
    let timestamps = HashMap::from([(pk1.clone(), 100_i64), (pk2.clone(), 100)]);
    let upstream_keys = HashMap::from([("b".into(), HashSet::from([pk1.clone(), pk2.clone()]))]);

    let mappings = HashMap::from([(("a".into(), "b".into()), PartitionMappingKind::Identity)]);
    let build_ctx = |_states: &HashMap<String, AssetConditionState>| {
        let resolver = PartitionResolver::new(&mappings, &upstream_keys);
        let prev = AssetConditionState {
            last_tick_timestamp: Some(50),
            ..Default::default()
        };
        (resolver, prev)
    };

    // Without baseline
    let (resolver, prev) = build_ctx(&empty_states);
    let _ip: HashSet<PartitionKey> = HashSet::new();
    let _fail: HashSet<PartitionKey> = HashSet::new();
    let pctx = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    let ctx = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &a,
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
        all_asset_states: &empty_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: Some(&pctx),
        root_partition_floor: None,
    };
    let cond = ConditionNode::any_deps_updated();
    let result = evaluate(&cond, &ctx);
    assert!(
        result.fired,
        "without baseline, NewlyUpdated should false-positive"
    );

    // Now apply update_dep_baselines and re-evaluate
    let mut baselined_states: HashMap<String, AssetConditionState> = HashMap::new();
    let conditioned = HashSet::from(["a".to_string()]);
    update_dep_baselines(
        &mut baselined_states,
        &["a".to_string()],
        &deps,
        &conditioned,
        &partition_statuses,
        &records,
    );

    let (resolver2, prev2) = build_ctx(&baselined_states);
    let pctx2 = PartitionEvalContext {
        all_keys: &all_keys,
        in_progress: &_ip,
        failed: &_fail,
        timestamps: &timestamps,
        resolver: resolver2,
        time_windows: None,
        all_partition_statuses: &partition_statuses,
        dep_root_floor: None,
    };
    let ctx2 = EvalContext {
        target_key: "a",
        root_key: "a",
        target_record: &a,
        cache: CacheSnapshot {
            records: &records,
            upstream_deps: &deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &prev2,
        all_asset_states: &baselined_states,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1000,
        is_initial: false,
        partitions: Some(&pctx2),
        root_partition_floor: None,
    };
    let result2 = evaluate(&cond, &ctx2);
    assert!(
        !result2.fired,
        "after update_dep_baselines, NewlyUpdated should not false-positive"
    );
}

/// The root-universe staleness floor must reach a `NewlyUpdated` leaf nested
/// behind ANY finite number of dep-aggregate levels, including a run of
/// consecutive unpartitioned (bool-world) deps.
///
/// Two shapes, each parametrized by `n` unpartitioned tail levels:
///   via_m=false: `r(part) ← u1(unpart) ← … ← u{n}` — bridge (B) recomputes the
///                floor over the root's own keys, then `n−1` bool pivots (C).
///   via_m=true:  `r(part) ← m(part, AllPartitions) ← u1 ← … ← u{n}` — a
///                partitioned pivot (A) carries the floor into the bridge (B),
///                then `n−1` bool pivots (C).
/// Root `r` has STAGGERED timestamps, so its floor (min = 500) differs from its
/// record scalar (max = 800). A leaf at ts=600 is newer than the floor (some
/// root partition is stale) but older than the record scalar, so only a
/// correctly-propagated floor fires it; a leaf at ts=400 must never fire.
#[test]
fn test_nested_dep_floor_propagates_to_every_depth() {
    let eval_chain = |via_m: bool, n_unpart: usize, leaf_ts: i64| -> bool {
        assert!(n_unpart >= 1);
        // One aggregate per dependency edge in the chain.
        let edges = n_unpart + usize::from(via_m);
        let mut cond = ConditionNode::NewlyUpdated;
        for _ in 0..edges {
            cond = ConditionNode::any_deps_match(cond);
        }

        let mut records: HashMap<String, AssetRecord> = HashMap::new();
        records.insert("r".into(), make_materialized_record("r", 800));
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        if via_m {
            records.insert("m".into(), make_materialized_record("m", 400));
            deps.insert("r".into(), vec!["m".into()]);
            deps.insert("m".into(), vec!["u1".into()]);
        } else {
            deps.insert("r".into(), vec!["u1".into()]);
        }
        for i in 1..=n_unpart {
            let node = format!("u{i}");
            // Only the leaf's timestamp feeds NewlyUpdated; intermediates just aggregate.
            let ts = if i == n_unpart { leaf_ts } else { 100 };
            records.insert(node.clone(), make_materialized_record(&node, ts));
            if i < n_unpart {
                deps.insert(node, vec![format!("u{}", i + 1)]);
            }
        }

        // `m` (when present) is the only partitioned dep; every `u*` is absent
        // from `upstream_keys` and so treated as unpartitioned (bridged).
        let upstream_keys: HashMap<String, HashSet<PartitionKey>> = if via_m {
            HashMap::from([("m".into(), HashSet::from([spk("eu"), spk("us")]))])
        } else {
            HashMap::new()
        };
        let mappings = if via_m {
            HashMap::from([(
                ("r".into(), "m".into()),
                PartitionMappingKind::AllPartitions,
            )])
        } else {
            HashMap::new()
        };
        let mut statuses = HashMap::from([(
            "r".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                in_progress: HashSet::new(),
                failed: HashSet::new(),
                failed_timestamps: HashMap::new(),
                timestamps: HashMap::from([(spk("d1"), 500), (spk("d2"), 800)]),
            },
        )]);
        if via_m {
            statuses.insert(
                "m".to_string(),
                crate::condition::cache::PartitionStatusEntry {
                    in_progress: HashSet::new(),
                    failed: HashSet::new(),
                    failed_timestamps: HashMap::new(),
                    timestamps: HashMap::from([(spk("eu"), 400), (spk("us"), 400)]),
                },
            );
        }
        let ak = HashSet::from([spk("d1"), spk("d2")]);
        let ip: HashSet<PartitionKey> = HashSet::new();
        let fail: HashSet<PartitionKey> = HashSet::new();
        let ts = HashMap::from([(spk("d1"), 500_i64), (spk("d2"), 800)]);
        let pctx = PartitionEvalContext {
            all_keys: &ak,
            in_progress: &ip,
            failed: &fail,
            timestamps: &ts,
            resolver: PartitionResolver::new(&mappings, &upstream_keys),
            time_windows: None,
            all_partition_statuses: &statuses,
            dep_root_floor: None,
        };
        let prev = AssetConditionState::default();
        let states: HashMap<String, AssetConditionState> = HashMap::new();
        let ctx = EvalContext {
            target_key: "r",
            root_key: "r",
            target_record: records.get("r").unwrap(),
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
            now: 1_000_000_000,
            is_initial: false,
            partitions: Some(&pctx),
            root_partition_floor: None,
        };
        evaluate(&cond, &ctx).fired
    };

    for via_m in [false, true] {
        for n in 1..=5 {
            assert!(
                eval_chain(via_m, n, 600),
                "via_m={via_m}, {n} unpartitioned levels: leaf ts=600 > floor(500) → stale, must fire"
            );
            assert!(
                !eval_chain(via_m, n, 400),
                "via_m={via_m}, {n} unpartitioned levels: leaf ts=400 < floor(500) → not stale, must not fire"
            );
        }
    }
}
