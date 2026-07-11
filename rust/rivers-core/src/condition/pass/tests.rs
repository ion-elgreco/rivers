use super::*;
use crate::timegrid::TimeGrid;
use ordermap::OrderSet;

fn spk(s: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![s.to_string()],
    }
}

fn make_daily_keys(keys: &[&str]) -> HashSet<PartitionKey> {
    keys.iter().map(|k| spk(k)).collect()
}

/// Helper: parse "YYYY-MM-DD HH:MM" as a wall-clock naive datetime.
fn to_wall(dt_str: &str) -> NaiveDateTime {
    chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M").unwrap()
}

fn test_record(key: &str) -> crate::storage::AssetRecord {
    crate::storage::AssetRecord {
        code_location_id: crate::storage::default_code_location_id(),
        asset_key: key.to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }
}

/// Build a one-partitioned-asset pass and run a full tick with the given fired selection.
fn handled_after_fired_selection(selection: PartitionSelection) -> Option<i64> {
    let mut cache = AssetConditionCache::default();
    cache
        .records
        .insert("down".to_string(), test_record("down"));
    let mut pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.eval_state.assets.insert(
        "down".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );
    let row = EvalResultRow {
        info_idx: 0,
        result: EvalResult {
            fired: true,
            selection: Some(selection),
            ..Default::default()
        },
        tree: EvalNodeResult::new(
            &ConditionNode::InProgress,
            0,
            crate::condition::state::NodeStatus::True,
            vec![],
            None,
        ),
        duration_us: 0,
    };
    let to_mat = vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: row.result.selection.clone(),
    }];
    let plan = pass.classify_materializations(to_mat);
    let output = PassOutput {
        results: vec![row],
        plan,
    };
    pass.commit_tick(&output, &HashSet::new(), 5000);
    pass.eval_state.assets["down"].last_handled_timestamp
}

/// Sub-daily grids expose off-grid stepping: a lookback that isn't a
/// whole multiple of the interval must still mirror the scan (daily
/// grids mask this because the date-only fmt truncates the misaligned
/// time-of-day away).
#[test]
fn grid_derivation_matches_the_scan_sub_daily_lookback() {
    let fmt = "%Y-%m-%dT%H:%M:%S";
    let parse = |s: &str| chrono::NaiveDateTime::parse_from_str(s, fmt).unwrap();
    let keys = make_daily_keys(&[
        "2024-01-01T07:00:00",
        "2024-01-01T08:00:00",
        "2024-01-01T09:00:00",
        "2024-01-01T10:00:00",
    ]);
    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(3600.0),
        start: parse("2024-01-01T00:00:00"),
        end: None,
        fmt: fmt.to_string(),
    };
    let now = parse("2024-01-01T10:30:00");
    // 90-minute lookback: not a multiple of the hourly interval.
    let scanned = compute_latest_time_window_keys(&keys, fmt, now, Some(5400.0));
    let derived =
        derive_window_keys_from_grid(&grid, &keys, now, Some(5400.0)).expect("grid derivation");
    assert_eq!(
        derived, scanned,
        "grid fast-path must mirror the scan for non-multiple lookbacks"
    );
    assert_eq!(
        scanned,
        make_daily_keys(&["2024-01-01T09:00:00", "2024-01-01T10:00:00"])
    );
}

/// The O(window) grid derivation must produce exactly what the full-scan
/// fallback produces for grid-enumerated universes.
#[test]
fn grid_derivation_matches_the_scan() {
    let keys = make_daily_keys(&[
        "2020-01-01",
        "2020-01-02",
        "2020-01-03",
        "2020-01-04",
        "2020-01-05",
    ]);
    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(86_400.0),
        start: to_wall("2020-01-01 00:00"),
        end: Some(to_wall("2020-01-06 00:00")),
        fmt: "%Y-%m-%d".to_string(),
    };
    let now = to_wall("2020-01-05 12:00");
    for lookback in [None, Some(86_400.0), Some(2.5 * 86_400.0)] {
        let scanned = compute_latest_time_window_keys(&keys, "%Y-%m-%d", now, lookback);
        let sources = HashMap::from([(
            "a".to_string(),
            TimeWindowSource {
                fmt: "%Y-%m-%d".to_string(),
                grid: Some(grid.clone()),
            },
        )]);
        let tw = TimeWindowResolver::new(&sources, now);
        let derived = tw.keys_for("a", &keys, lookback).unwrap();
        assert_eq!(*derived, scanned, "lookback {lookback:?}");
    }
}

/// Each InLatestTimeWindow node must select against its OWN lookback — not
/// a single set computed from the first node found in the tree.
#[test]
fn latest_time_window_respects_each_nodes_lookback() {
    let mut cache = AssetConditionCache::default();
    let mut rec = test_record("a");
    rec.last_timestamp = Some(100);
    cache.records.insert("a".to_string(), rec);
    cache.partition_status.insert(
        "a".to_string(),
        crate::condition::cache::PartitionStatusEntry::default(),
    );
    let keys = make_daily_keys(&[
        "2020-01-01",
        "2020-01-02",
        "2020-01-03",
        "2020-01-04",
        "2020-01-05",
    ]);

    let tree = ConditionNode::Or(vec![
        ConditionNode::And(vec![
            ConditionNode::InProgress,
            ConditionNode::InLatestTimeWindow {
                lookback_delta: None,
            },
        ]),
        ConditionNode::And(vec![
            ConditionNode::Missing,
            ConditionNode::InLatestTimeWindow {
                lookback_delta: Some(2.0 * 86_400.0),
            },
        ]),
    ]);
    let pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: tree,
            partition_info: Some(PartitionInfo {
                all_keys: keys,
                mappings: HashMap::new(),
                time_window_fmt: Some("%Y-%m-%d".to_string()),
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let rows = pass.evaluate(1_700_000_000_000_000_000, false);
    // Missing = all 5 keys; the 2-day lookback selects the latest 3.
    assert_eq!(
        rows[0].result.selection.clone().unwrap(),
        PartitionSelection::Keys(make_daily_keys(&["2020-01-03", "2020-01-04", "2020-01-05"])),
        "the 2-day-lookback node must select its own window, not the first node's"
    );
}

/// in_latest_time_window() inside a dep pivot must filter against the
/// DEP's latest window, not silently select every dep partition.
#[test]
fn dep_pivot_latest_time_window_filters_dep_keys() {
    let day_keys = make_daily_keys(&["2020-01-01", "2020-01-02", "2020-01-03"]);
    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(86_400.0),
        start: to_wall("2020-01-01 00:00"),
        end: Some(to_wall("2020-01-04 00:00")),
        fmt: "%Y-%m-%d".to_string(),
    };

    let build_pass = |a_ts: &[(&str, i64)]| {
        let mut cache = AssetConditionCache::default();
        let mut rec_b = test_record("b");
        rec_b.last_timestamp = Some(100);
        cache.records.insert("b".to_string(), rec_b);
        let mut rec_a = test_record("a");
        rec_a.last_timestamp = Some(200);
        cache.records.insert("a".to_string(), rec_a);
        cache
            .upstream_deps
            .insert("b".to_string(), vec!["a".to_string()]);
        cache.partition_status.insert(
            "b".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: day_keys.iter().map(|k| (k.clone(), 100)).collect(),
                ..Default::default()
            },
        );
        cache.partition_status.insert(
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                timestamps: a_ts.iter().map(|(k, ts)| (spk(k), *ts)).collect(),
                ..Default::default()
            },
        );
        let tree = ConditionNode::any_deps_match(
            ConditionNode::NewlyUpdated
                & ConditionNode::InLatestTimeWindow {
                    lookback_delta: None,
                },
        );
        ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "b".to_string(),
                condition: tree,
                partition_info: Some(PartitionInfo {
                    all_keys: day_keys.clone(),
                    mappings: HashMap::new(),
                    time_window_fmt: Some("%Y-%m-%d".to_string()),
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::from([(
                "a".to_string(),
                (
                    day_keys.clone(),
                    PartitionUniverse::TimeWindow {
                        grid: grid.clone(),
                        enumerated_to: to_wall("2020-01-04 00:00"),
                    },
                ),
            )]),
        )
    };

    // Only a's OLDEST key was re-materialized (200 > b's floor of 100).
    let pass = build_pass(&[
        ("2020-01-01", 200),
        ("2020-01-02", 100),
        ("2020-01-03", 100),
    ]);
    let rows = pass.evaluate(1_700_000_000_000_000_000, false);
    assert!(
        !rows[0].result.fired,
        "an update to a NON-latest dep partition must not pass in_latest_time_window; got {:?}",
        rows[0].result.selection
    );

    // Positive control: the dep's LATEST key updating passes the filter.
    let pass = build_pass(&[
        ("2020-01-01", 100),
        ("2020-01-02", 100),
        ("2020-01-03", 200),
    ]);
    let rows = pass.evaluate(1_700_000_000_000_000_000, false);
    assert!(rows[0].result.fired, "latest-key update must fire");
    assert_eq!(
        rows[0].result.selection.clone().unwrap(),
        PartitionSelection::Keys(make_daily_keys(&["2020-01-03"]))
    );
}

/// One asset's fire must not consume dep-change evidence a gated sibling
/// hasn't acted on: dep baselines are per (downstream, dep), not global.
#[test]
fn dep_baseline_survives_unrelated_asset_fire() {
    let mut cache = AssetConditionCache::default();
    let mut c = test_record("c");
    c.last_timestamp = Some(100);
    cache.records.insert("c".to_string(), c);
    let mut y = test_record("y");
    y.last_timestamp = Some(50);
    y.last_data_version = Some("v1".to_string());
    cache.records.insert("y".to_string(), y.clone());
    // d is missing → its condition fires every tick.
    cache.records.insert("d".to_string(), test_record("d"));
    cache
        .upstream_deps
        .insert("c".to_string(), vec!["y".to_string()]);

    let mut pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![
            AssetConditionInfo {
                asset_key: "c".to_string(),
                condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged)
                    & !ConditionNode::InProgress,
                partition_info: None,
                backfill_strategy: None,
            },
            AssetConditionInfo {
                asset_key: "d".to_string(),
                condition: ConditionNode::Missing,
                partition_info: None,
                backfill_strategy: None,
            },
        ],
        HashMap::new(),
    );

    let fired = |out: &PassOutput, pass: &ConditionPass, key: &str| {
        out.results
            .iter()
            .any(|row| pass.conditions[row.info_idx].asset_key == key && row.result.fired)
    };

    // Tick 1: y's version is first-seen → c fires and consumes it.
    let out = pass.run(1000, false);
    assert!(
        fired(&out, &pass, "c"),
        "tick 1: first-seen version fires c"
    );

    // Tick 2: y unchanged → no re-fire.
    let out = pass.run(2000, false);
    assert!(
        !fired(&out, &pass, "c"),
        "tick 2: stable version must not re-fire"
    );

    // y's version changes while c is gated; unrelated d keeps firing.
    y.last_data_version = Some("v2".to_string());
    y.last_timestamp = Some(60);
    pass.cache.records.insert("y".to_string(), y);
    pass.cache
        .in_progress_assets
        .entry("c".to_string())
        .or_default();
    let out = pass.run(3000, false);
    assert!(!fired(&out, &pass, "c"), "tick 3: c is gated");
    assert!(fired(&out, &pass, "d"), "tick 3: unrelated d fires");

    // Tick 4: c ungated — the pending version change must still be visible.
    pass.cache.in_progress_assets.remove("c");
    let out = pass.run(4000, false);
    assert!(
        fired(&out, &pass, "c"),
        "tick 4: an unrelated asset's fire must not consume c's dep-change trigger"
    );

    // Tick 5: c acted on it → consumed.
    let out = pass.run(5000, false);
    assert!(!fired(&out, &pass, "c"), "tick 5: c consumed the change");
}

#[test]
fn unpartitioned_watcher_sees_partition_failure_of_dep() {
    let mut cache = AssetConditionCache::default();
    let mut down = test_record("down");
    down.last_timestamp = Some(100);
    cache.records.insert("down".to_string(), down);
    let mut up = test_record("up");
    up.last_timestamp = Some(100);
    cache.records.insert("up".to_string(), up);
    cache
        .upstream_deps
        .insert("down".to_string(), vec!["up".to_string()]);
    cache.partition_status.insert(
        "up".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            failed: HashSet::from([spk("2024-01-01")]),
            failed_timestamps: HashMap::from([(spk("2024-01-01"), 200i64)]),
            ..Default::default()
        },
    );

    let pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::AnyDepsMatch {
                condition: Box::new(ConditionNode::ExecutionFailed),
                label: None,
            },
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let rows = pass.evaluate(5000, false);
    assert!(
        rows[0].result.fired,
        "an unpartitioned watcher must see a partitioned dep's failed partition"
    );
}

#[test]
fn observation_and_sibling_success_do_not_unsurface_partition_failure() {
    let mut cache = AssetConditionCache::default();
    let mut down = test_record("down");
    down.last_timestamp = Some(100);
    cache.records.insert("down".to_string(), down);
    let mut up = test_record("up");
    up.last_timestamp = Some(300);
    cache.records.insert("up".to_string(), up);
    cache
        .upstream_deps
        .insert("down".to_string(), vec!["up".to_string()]);
    cache.partition_status.insert(
        "up".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            failed: HashSet::from([spk("p")]),
            failed_timestamps: HashMap::from([(spk("p"), 150i64)]),
            timestamps: HashMap::from([(spk("q"), 400i64)]),
            ..Default::default()
        },
    );

    let pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::AnyDepsMatch {
                condition: Box::new(ConditionNode::ExecutionFailed),
                label: None,
            },
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let rows = pass.evaluate(5000, false);
    assert!(
        rows[0].result.fired,
        "a still-failed partition must stay surfaced despite an observation \
         or a sibling partition's success bumping the asset-level timestamp"
    );
}

#[test]
fn recovered_dep_partition_failure_does_not_poison_unpartitioned_watcher() {
    let mut cache = AssetConditionCache::default();
    let mut down = test_record("down");
    down.last_timestamp = Some(100);
    cache.records.insert("down".to_string(), down);
    let mut up = test_record("up");
    up.last_timestamp = Some(300);
    cache.records.insert("up".to_string(), up);
    cache
        .upstream_deps
        .insert("down".to_string(), vec!["up".to_string()]);
    cache.partition_status.insert(
        "up".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            failed: HashSet::new(),
            timestamps: HashMap::from([(spk("2024-01-01"), 300i64)]),
            ..Default::default()
        },
    );

    let pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::AnyDepsMatch {
                condition: Box::new(ConditionNode::ExecutionFailed),
                label: None,
            },
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let rows = pass.evaluate(5000, false);
    assert!(
        !rows[0].result.fired,
        "a dep whose failed partition re-materialized (empty failed set) \
         must not surface to unpartitioned watchers"
    );
}

#[test]
fn run_seeds_missing_eval_state_instead_of_panicking() {
    let mut cache = AssetConditionCache::default();
    cache.records.insert("a".to_string(), test_record("a"));
    let mut pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::eager(),
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let out = pass.run(1000, false);
    assert_eq!(
        out.results.len(),
        1,
        "conditioned asset must evaluate without panicking on a missing eval_state entry"
    );
}

#[test]
fn handled_cursor_skips_fully_dropped_selection() {
    let handled = handled_after_fired_selection(PartitionSelection::Keys(
        [spk("2099-12-31")].into_iter().collect(),
    ));
    assert_eq!(
        handled, None,
        "a fully-dropped selection must not advance the handled cursor"
    );
}

#[test]
fn handled_cursor_advances_for_surviving_selection() {
    let handled = handled_after_fired_selection(PartitionSelection::Keys(
        [spk("2024-01-01")].into_iter().collect(),
    ));
    assert_eq!(
        handled,
        Some(5000),
        "a dispatched selection must advance the handled cursor"
    );
}

#[test]
fn classify_drops_mapped_keys_the_asset_does_not_have() {
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::Keys(
            [spk("2024-01-02"), spk("2024-08-16")].into_iter().collect(),
        )),
    }]);
    assert_eq!(
        plan.single_partition_groups,
        HashMap::from([(spk("2024-01-02"), vec!["down".to_string()])])
    );
    assert!(plan.multi_partition_backfills.is_empty());
    assert!(plan.unpartitioned.is_empty());
}

#[test]
fn classify_skips_asset_when_no_mapped_key_survives() {
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::Keys(
            [spk("2024-08-16")].into_iter().collect(),
        )),
    }]);
    assert!(plan.is_empty());
}

fn naive(s: &str) -> NaiveDateTime {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
}

fn hourly_grid() -> TimeGrid {
    TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(3600.0),
        start: naive("2024-01-01T00:00:00"),
        end: None,
        fmt: "%Y-%m-%dT%H:%M:%S".into(),
    }
}

#[test]
fn time_window_universe_gains_new_windows() {
    let mut universe = PartitionUniverse::TimeWindow {
        grid: hourly_grid(),
        enumerated_to: naive("2024-01-01T01:30:00"),
    };
    let mut all_keys: HashSet<PartitionKey> =
        [spk("2024-01-01T00:00:00"), spk("2024-01-01T01:00:00")]
            .into_iter()
            .collect();
    refresh_universe(
        &mut universe,
        &mut all_keys,
        naive("2024-01-01T03:10:00"),
        &HashMap::new(),
    );
    assert!(all_keys.contains(&spk("2024-01-01T02:00:00")));
    assert!(all_keys.contains(&spk("2024-01-01T03:00:00")));
    assert_eq!(all_keys.len(), 4);
    refresh_universe(
        &mut universe,
        &mut all_keys,
        naive("2024-01-01T03:10:00"),
        &HashMap::new(),
    );
    assert_eq!(all_keys.len(), 4);
}

#[test]
fn refresh_universe_holds_watermark_on_enumeration_error() {
    let broken = TimeGrid {
        cron_schedule: None,
        interval_seconds: None,
        start: naive("2024-01-01T00:00:00"),
        end: None,
        fmt: "%Y-%m-%dT%H:%M:%S".into(),
    };
    let t0 = naive("2024-01-01T00:00:00");
    let mut universe = PartitionUniverse::TimeWindow {
        grid: broken.clone(),
        enumerated_to: t0,
    };
    let mut all_keys: HashSet<PartitionKey> = HashSet::new();
    let changed = refresh_universe(
        &mut universe,
        &mut all_keys,
        naive("2024-01-01T05:00:00"),
        &HashMap::new(),
    );
    assert!(!changed);
    assert!(all_keys.is_empty());
    let PartitionUniverse::TimeWindow { enumerated_to, .. } = &universe else {
        unreachable!()
    };
    assert_eq!(*enumerated_to, t0, "failed ranges must be retried");

    let mut multi = PartitionUniverse::Multi {
        dims: vec![(
            "date".to_string(),
            DimensionUniverse {
                keys: OrderSet::new(),
                kind: DimensionKind::TimeWindow {
                    grid: broken,
                    enumerated_to: t0,
                },
            },
        )],
    };
    let changed = refresh_universe(
        &mut multi,
        &mut all_keys,
        naive("2024-01-01T05:00:00"),
        &HashMap::new(),
    );
    assert!(!changed);
    let PartitionUniverse::Multi { dims } = &multi else {
        unreachable!()
    };
    let DimensionKind::TimeWindow { enumerated_to, .. } = &dims[0].1.kind else {
        unreachable!()
    };
    assert_eq!(*enumerated_to, t0, "failed dim ranges must be retried");
}

#[test]
fn dynamic_universe_mirrors_storage_including_retirement() {
    let mut universe = PartitionUniverse::Dynamic {
        namespace: "colors".into(),
    };
    let mut all_keys: HashSet<PartitionKey> = HashSet::new();
    let now = naive("2024-01-01T00:00:00");
    let registered: HashMap<String, HashSet<String>> = [(
        "colors".to_string(),
        ["red".to_string(), "blue".to_string()]
            .into_iter()
            .collect(),
    )]
    .into_iter()
    .collect();
    refresh_universe(&mut universe, &mut all_keys, now, &registered);
    assert_eq!(all_keys, [spk("red"), spk("blue")].into_iter().collect());
    let shrunk: HashMap<String, HashSet<String>> = [(
        "colors".to_string(),
        ["blue".to_string()].into_iter().collect(),
    )]
    .into_iter()
    .collect();
    refresh_universe(&mut universe, &mut all_keys, now, &shrunk);
    assert_eq!(all_keys, [spk("blue")].into_iter().collect());
    refresh_universe(&mut universe, &mut all_keys, now, &HashMap::new());
    assert_eq!(all_keys, [spk("blue")].into_iter().collect());
}

#[test]
fn multi_universe_recomputes_cartesian_on_dimension_change() {
    let mut universe = PartitionUniverse::Multi {
        dims: vec![
            (
                "date".to_string(),
                DimensionUniverse {
                    keys: ["2024-01-01T00:00:00".to_string()].into_iter().collect(),
                    kind: DimensionKind::TimeWindow {
                        grid: hourly_grid(),
                        enumerated_to: naive("2024-01-01T00:30:00"),
                    },
                },
            ),
            (
                "region".to_string(),
                DimensionUniverse {
                    keys: ["eu".to_string(), "us".to_string()].into_iter().collect(),
                    kind: DimensionKind::Frozen,
                },
            ),
        ],
    };
    let mut all_keys: HashSet<PartitionKey> = HashSet::new();
    refresh_universe(
        &mut universe,
        &mut all_keys,
        naive("2024-01-01T01:10:00"),
        &HashMap::new(),
    );
    assert_eq!(all_keys.len(), 4);
    let expected = PartitionKey::Multi {
        dims: vec![
            ("date".to_string(), vec!["2024-01-01T01:00:00".to_string()]),
            ("region".to_string(), vec!["eu".to_string()]),
        ],
    };
    assert!(all_keys.contains(&expected));
}

#[test]
fn multi_dim_refresh_never_duplicates_seeded_window_starts() {
    let grid = TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(3600.0),
        start: naive("2024-01-01T00:00:00"),
        end: Some(naive("2024-01-02T00:00:00")),
        fmt: "%Y-%m-%dT%H:%M:%S".into(),
    };
    let seeded: Vec<String> = (0..24)
        .map(|h| format!("2024-01-01T{h:02}:00:00"))
        .collect();
    let mut universe = PartitionUniverse::Multi {
        dims: vec![(
            "date".to_string(),
            DimensionUniverse {
                keys: seeded.iter().cloned().collect(),
                kind: DimensionKind::TimeWindow {
                    grid,
                    enumerated_to: naive("2024-01-01T01:30:00"),
                },
            },
        )],
    };
    let mut all_keys: HashSet<PartitionKey> = HashSet::new();
    let changed = refresh_universe(
        &mut universe,
        &mut all_keys,
        naive("2024-01-01T03:10:00"),
        &HashMap::new(),
    );
    let PartitionUniverse::Multi { dims } = &universe else {
        unreachable!()
    };
    assert!(
        !changed,
        "re-yielding already-seeded starts is not a change"
    );
    assert!(
        dims[0].1.keys.iter().eq(seeded.iter()),
        "no duplicate dim keys"
    );
}

#[test]
fn update_state_leaves_handled_to_classification() {
    let mut state = crate::condition::state::AssetConditionState::default();
    let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
    let ctx = StateUpdateContext {
        target_record_timestamp: None,
        target_data_version: None,
        now: 1,
        is_initial: false,
        partition_timestamps: Some(&timestamps),
    };
    let result = EvalResult {
        fired: true,
        selection: Some(PartitionSelection::Keys(
            [spk("2024-08-16")].into_iter().collect(),
        )),
        sub_selections: Some(HashMap::new()),
        ..Default::default()
    };
    update_condition_state(&mut state, &ctx, &result);
    let ps = state.partition_state.as_ref().expect("partition state");
    assert!(
        ps.handled.is_empty(),
        "handled must be extended from the classified plan, not the raw selection"
    );
}

#[test]
fn update_state_resets_handled_each_tick() {
    let mut state = crate::condition::state::AssetConditionState {
        partition_state: Some(crate::condition::partition::PartitionState {
            handled: [spk("2024-01-01")].into_iter().collect(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
    let ctx = StateUpdateContext {
        target_record_timestamp: None,
        target_data_version: None,
        now: 2,
        is_initial: false,
        partition_timestamps: Some(&timestamps),
    };
    let result = EvalResult {
        fired: false,
        selection: Some(PartitionSelection::Empty),
        sub_selections: Some(HashMap::new()),
        ..Default::default()
    };
    update_condition_state(&mut state, &ctx, &result);
    let ps = state.partition_state.as_ref().expect("partition state");
    assert!(
        ps.handled.is_empty(),
        "stale handled keys from a prior tick must be reset, got {:?}",
        ps.handled
    );
}

#[test]
fn commit_marks_only_surviving_keys_handled() {
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.eval_state.assets.insert(
        "down".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::Keys(
            [spk("2024-01-02"), spk("2024-08-16")].into_iter().collect(),
        )),
    }]);
    pass.commit_tick(
        &PassOutput {
            results: vec![],
            plan,
        },
        &HashSet::new(),
        2,
    );
    let handled = pass.eval_state.assets["down"]
        .partition_state
        .as_ref()
        .expect("partition state")
        .handled
        .clone();
    assert!(handled.contains(&spk("2024-01-02")));
    assert!(
        !handled.contains(&spk("2024-08-16")),
        "dropped keys must not be marked handled"
    );
}

#[test]
fn classify_orders_partition_keys_canonically() {
    let days: Vec<String> = (1..=12).map(|d| format!("2024-01-{d:02}")).collect();
    let day_refs: Vec<&str> = days.iter().map(String::as_str).collect();
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&day_refs),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.eval_state.assets.insert(
        "down".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::All),
    }]);
    let mut backfills = plan.multi_partition_backfills;
    let (_, keys) = backfills.pop().unwrap();
    let display: Vec<String> = keys.iter().map(|k| k.to_display()).collect();
    let mut sorted = display.clone();
    sorted.sort_unstable();
    assert_eq!(display, sorted, "dispatched keys must be display-ordered");
}

#[test]
fn classify_fully_dropped_selection_leaves_no_in_progress_entry() {
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.eval_state.assets.insert(
        "down".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::Keys(make_daily_keys(&["1999-01-01"]))),
    }]);
    assert!(plan.unpartitioned.is_empty());
    assert!(plan.single_partition_groups.is_empty());
    assert!(plan.multi_partition_backfills.is_empty());
    assert!(
        !pass.cache.in_progress_assets.contains_key("down"),
        "a fully-dropped selection must not wedge the asset behind an \
         in-progress entry nothing will ever clear"
    );

    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::Keys(make_daily_keys(&["2024-01-01"]))),
    }]);
    assert_eq!(plan.single_partition_groups.len(), 1);
    assert!(pass.cache.in_progress_assets.contains_key("down"));
}

#[test]
fn eager_does_not_fire_for_a_partition_an_active_backfill_covers() {
    let keys = |ks: &[&str]| ks.iter().map(|k| spk(k)).collect::<HashSet<_>>();

    let mut cache = AssetConditionCache::default();
    cache.records.insert("src".to_string(), test_record("src"));
    cache.records.insert("dst".to_string(), test_record("dst"));
    cache
        .upstream_deps
        .insert("dst".to_string(), vec!["src".to_string()]);

    cache.partition_status.insert(
        "src".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("a"), 100i64), (spk("b"), 100), (spk("c"), 100)]),
            ..Default::default()
        },
    );
    cache.partition_status.insert(
        "dst".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("a"), 200i64), (spk("b"), 200)]),
            ..Default::default()
        },
    );

    cache
        .backfill
        .assets
        .insert("dst".to_string(), vec!["bf1".to_string()]);
    cache
        .backfill
        .partition_keys
        .insert("bf1".to_string(), vec![spk("a"), spk("b"), spk("c")]);
    assert!(
        cache.in_progress_assets.is_empty(),
        "precondition: the gap tick has no tracked sub-run"
    );

    let mut pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::eager(),
            partition_info: Some(PartitionInfo {
                all_keys: keys(&["a", "b", "c", "d", "e"]),
                mappings: HashMap::from([(
                    ("dst".to_string(), "src".to_string()),
                    PartitionMappingKind::Identity,
                )]),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::from([(
            "src".to_string(),
            (keys(&["a", "b", "c", "d", "e"]), PartitionUniverse::Frozen),
        )]),
    );
    pass.eval_state.assets.insert(
        "dst".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );

    let out = pass.run(1000, false);

    let sel = &out.results[0].result.selection;
    let selects_c = matches!(sel, Some(PartitionSelection::Keys(ks)) if ks.contains(&spk("c")));
    assert!(
        !selects_c,
        "eager must not select a partition an active backfill already covers; got {sel:?}"
    );

    assert!(
        out.plan.is_empty(),
        "nothing may dispatch for a backfill-covered partition; plan dispatched \
         unpartitioned={:?} single={:?} backfills={:?}",
        out.plan.unpartitioned,
        out.plan.single_partition_groups,
        out.plan.multi_partition_backfills,
    );
}

#[test]
fn eager_does_not_redispatch_a_just_dispatched_partition_before_storage_catches_up() {
    let keys = |ks: &[&str]| ks.iter().map(|k| spk(k)).collect::<HashSet<_>>();

    let mut cache = AssetConditionCache::default();
    cache.records.insert("src".to_string(), test_record("src"));
    cache.records.insert("dst".to_string(), test_record("dst"));
    cache
        .upstream_deps
        .insert("dst".to_string(), vec!["src".to_string()]);
    cache.partition_status.insert(
        "src".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("a"), 100i64), (spk("b"), 100), (spk("c"), 100)]),
            ..Default::default()
        },
    );
    cache.partition_status.insert(
        "dst".to_string(),
        crate::condition::cache::PartitionStatusEntry {
            timestamps: HashMap::from([(spk("a"), 200i64), (spk("b"), 200)]),
            ..Default::default()
        },
    );

    cache.register_dispatched_run("dst".into(), "run_c".into(), 1000, Some(spk("c")));
    assert!(
        cache.partition_status["dst"].in_progress.is_empty(),
        "precondition: storage's get_in_progress_partitions hasn't caught up"
    );

    let mut pass = ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::eager(),
            partition_info: Some(PartitionInfo {
                all_keys: keys(&["a", "b", "c", "d", "e"]),
                mappings: HashMap::from([(
                    ("dst".to_string(), "src".to_string()),
                    PartitionMappingKind::Identity,
                )]),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::from([(
            "src".to_string(),
            (keys(&["a", "b", "c", "d", "e"]), PartitionUniverse::Frozen),
        )]),
    );
    pass.eval_state.assets.insert(
        "dst".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );

    let out = pass.run(1000, false);

    let sel = &out.results[0].result.selection;
    let selects_c = matches!(sel, Some(PartitionSelection::Keys(ks)) if ks.contains(&spk("c")));
    assert!(
        !selects_c,
        "eager must not re-dispatch a partition whose run was just dispatched; got {sel:?}"
    );
    assert!(
        out.plan.is_empty(),
        "nothing may dispatch; got single={:?}",
        out.plan.single_partition_groups
    );
}

#[test]
fn commit_marks_all_selection_keys_handled() {
    let mut pass = ConditionPass::new(
        AssetConditionCache::default(),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "down".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: Some(PartitionInfo {
                all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                mappings: HashMap::new(),
                time_window_fmt: None,
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.eval_state.assets.insert(
        "down".to_string(),
        crate::condition::state::AssetConditionState::default(),
    );
    let plan = pass.classify_materializations(vec![ToMaterialize {
        asset_key: "down".to_string(),
        selection: Some(PartitionSelection::All),
    }]);
    let output = PassOutput {
        results: vec![],
        plan,
    };
    pass.commit_tick(&output, &HashSet::new(), 2);
    let mut backfills = output.plan.multi_partition_backfills;
    assert_eq!(
        backfills.len(),
        1,
        "All must dispatch the asset's partitions"
    );
    let (asset, mut keys) = backfills.pop().unwrap();
    assert_eq!(asset, "down");
    keys.sort_by_key(|k| format!("{k:?}"));
    assert_eq!(keys, vec![spk("2024-01-01"), spk("2024-01-02")]);
    let handled = pass.eval_state.assets["down"]
        .partition_state
        .as_ref()
        .expect("partition state")
        .handled
        .clone();
    assert!(handled.contains(&spk("2024-01-01")));
    assert!(handled.contains(&spk("2024-01-02")));
}

#[test]
fn latest_window_tracks_wall_clock_on_non_utc_hosts() {
    let all_keys = make_daily_keys(&["2026-06-11T08:00", "2026-06-11T09:00", "2026-06-11T10:00"]);
    let now_local = to_wall("2026-06-11 10:30");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%dT%H:%M", now_local, None);
    assert_eq!(result.len(), 1);
    assert!(result.contains(&spk("2026-06-11T10:00")));
}

#[test]
fn test_compute_at_start_date_includes_current() {
    let all_keys = make_daily_keys(&["2026-03-01"]);
    let now = to_wall("2026-03-01 00:00");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
    assert_eq!(result.len(), 1);
    assert!(result.contains(&spk("2026-03-01")));
}

#[test]
fn test_compute_latest_single_partition_no_lookback() {
    let all_keys = make_daily_keys(&["2026-03-20", "2026-03-21", "2026-03-22", "2026-03-23"]);
    let now = to_wall("2026-03-23 01:00");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
    assert_eq!(result.len(), 1);
    assert!(result.contains(&spk("2026-03-23")));
}

#[test]
fn test_compute_latest_advances_with_time() {
    let all_keys = make_daily_keys(&[
        "2026-03-20",
        "2026-03-21",
        "2026-03-22",
        "2026-03-23",
        "2026-03-24",
        "2026-03-25",
        "2026-03-26",
    ]);
    let now1 = to_wall("2026-03-22 01:00");
    let r1 = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now1, None);
    assert_eq!(r1.len(), 1);
    assert!(r1.contains(&spk("2026-03-22")));

    let now2 = to_wall("2026-03-26 01:00");
    let r2 = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now2, None);
    assert_eq!(r2.len(), 1);
    assert!(r2.contains(&spk("2026-03-26")));
}

#[test]
fn test_compute_lookback_3_days() {
    let all_keys = make_daily_keys(&[
        "2026-03-20",
        "2026-03-21",
        "2026-03-22",
        "2026-03-23",
        "2026-03-24",
        "2026-03-25",
        "2026-03-26",
    ]);
    let now = to_wall("2026-03-26 01:00");
    let lookback_secs = 3.0 * 86400.0;
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(lookback_secs));
    assert_eq!(result.len(), 4);
    assert!(result.contains(&spk("2026-03-23")));
    assert!(result.contains(&spk("2026-03-24")));
    assert!(result.contains(&spk("2026-03-25")));
    assert!(result.contains(&spk("2026-03-26")));
}

#[test]
fn test_compute_lookback_advances_with_time() {
    let all_keys = make_daily_keys(&[
        "2026-03-15",
        "2026-03-16",
        "2026-03-17",
        "2026-03-18",
        "2026-03-19",
        "2026-03-20",
        "2026-03-21",
    ]);
    let now = to_wall("2026-03-21 01:00");
    let lookback_secs = 3.0 * 86400.0;
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(lookback_secs));
    assert_eq!(result.len(), 4);
    assert!(result.contains(&spk("2026-03-18")));
    assert!(result.contains(&spk("2026-03-19")));
    assert!(result.contains(&spk("2026-03-20")));
    assert!(result.contains(&spk("2026-03-21")));
}

#[test]
fn lookback_smaller_than_period_still_selects_latest_window() {
    let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2026-03-26"]);
    let now = to_wall("2026-03-26 12:00");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(3600.0));
    assert_eq!(result.len(), 1, "1h lookback at 12:00 must not go empty");
    assert!(result.contains(&spk("2026-03-26")));
}

#[test]
fn lookback_of_one_period_selects_previous_window_all_day() {
    let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2026-03-26"]);
    let now = to_wall("2026-03-26 23:59");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(86400.0));
    assert_eq!(result.len(), 2);
    assert!(result.contains(&spk("2026-03-25")));
    assert!(result.contains(&spk("2026-03-26")));
}

#[test]
fn test_compute_future_partitions_excluded() {
    let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2027-12-31"]);
    let now = to_wall("2026-03-25 12:00");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
    assert_eq!(result.len(), 1);
    assert!(result.contains(&spk("2026-03-25")));
    assert!(!result.contains(&spk("2027-12-31")));
}

#[test]
fn test_compute_empty_keys() {
    let all_keys: HashSet<PartitionKey> = HashSet::new();
    let now = to_wall("2026-03-25 12:00");
    let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
    assert!(result.is_empty());
}

#[test]
fn test_compute_hourly_partitions() {
    let all_keys = make_daily_keys(&[
        "2026-03-25T08:00",
        "2026-03-25T09:00",
        "2026-03-25T10:00",
        "2026-03-25T11:00",
        "2026-03-25T12:00",
    ]);
    let now = to_wall("2026-03-25 12:30");
    let lookback_secs = 3.0 * 3600.0;
    let result =
        compute_latest_time_window_keys(&all_keys, "%Y-%m-%dT%H:%M", now, Some(lookback_secs));
    assert_eq!(result.len(), 4);
    assert!(result.contains(&spk("2026-03-25T09:00")));
    assert!(result.contains(&spk("2026-03-25T10:00")));
    assert!(result.contains(&spk("2026-03-25T11:00")));
    assert!(result.contains(&spk("2026-03-25T12:00")));
}

#[test]
fn test_classify_unpartitioned_only() {
    let mut pass = empty_pass();
    let to_mat = vec![
        ToMaterialize {
            asset_key: "a".into(),
            selection: None,
        },
        ToMaterialize {
            asset_key: "b".into(),
            selection: None,
        },
    ];
    let plan = pass.classify_materializations(to_mat);
    assert_eq!(plan.unpartitioned, vec!["a".to_string(), "b".to_string()]);
    assert!(plan.single_partition_groups.is_empty());
    assert!(plan.multi_partition_backfills.is_empty());
    assert!(pass.cache.in_progress_assets.contains_key("a"));
    assert!(pass.cache.in_progress_assets.contains_key("b"));
}

#[test]
fn test_classify_single_partition_groups_share_run() {
    let mut pass = empty_pass();
    let pk = spk("2026-03-26");
    let to_mat = vec![
        ToMaterialize {
            asset_key: "a".into(),
            selection: Some(PartitionSelection::Keys([pk.clone()].into_iter().collect())),
        },
        ToMaterialize {
            asset_key: "b".into(),
            selection: Some(PartitionSelection::Keys([pk.clone()].into_iter().collect())),
        },
    ];
    let plan = pass.classify_materializations(to_mat);
    assert!(plan.unpartitioned.is_empty());
    assert_eq!(plan.single_partition_groups.len(), 1);
    let assets = plan.single_partition_groups.get(&pk).unwrap();
    assert_eq!(assets.len(), 2);
    assert!(assets.contains(&"a".to_string()));
    assert!(assets.contains(&"b".to_string()));
    assert!(plan.multi_partition_backfills.is_empty());
}

#[test]
fn test_classify_multi_partition_becomes_backfill() {
    let mut pass = empty_pass();
    let keys: HashSet<PartitionKey> = [spk("2026-03-25"), spk("2026-03-26")].into_iter().collect();
    let to_mat = vec![ToMaterialize {
        asset_key: "a".into(),
        selection: Some(PartitionSelection::Keys(keys)),
    }];
    let plan = pass.classify_materializations(to_mat);
    assert!(plan.unpartitioned.is_empty());
    assert!(plan.single_partition_groups.is_empty());
    assert_eq!(plan.multi_partition_backfills.len(), 1);
    let (asset, pks) = &plan.multi_partition_backfills[0];
    assert_eq!(asset, "a");
    assert_eq!(pks.len(), 2);
}

fn empty_pass() -> ConditionPass {
    ConditionPass::new(
        AssetConditionCache::new("test_cl".into()),
        ConditionEvalState::default(),
        Vec::new(),
        HashMap::new(),
    )
}
