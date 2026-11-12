//! Backfill planning — split the requested partition keys into runs per strategy.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::storage::{BackfillStrategy, PartitionKey};

/// One key per run of a backfill, in run order: the place of a key in the
/// list is its run number. A batched key is planned as its members, without
/// building them where the strategy keeps them together.
pub fn plan_runs(strategy: &BackfillStrategy, requested: &[PartitionKey]) -> Vec<PartitionKey> {
    let requested = without_sets(requested);

    match strategy {
        BackfillStrategy::MultiRun => requested.iter().flat_map(PartitionKey::members).collect(),
        BackfillStrategy::SingleRun => {
            if requested.is_empty() {
                vec![]
            } else {
                vec![bundle_keys(&requested)]
            }
        }
        BackfillStrategy::PerDimension { multi_run, .. } => {
            // A sorted map: the run order does not depend on the input order,
            // so stop-on-failure cancels the same runs each time.
            let mut groups: BTreeMap<Vec<(String, String)>, Vec<PartitionKey>> = BTreeMap::new();
            for key in &requested {
                for (group, part) in split_along(key, multi_run) {
                    groups.entry(group).or_default().push(part);
                }
            }
            groups
                .into_values()
                .map(|parts| bundle_keys(&parts))
                .collect()
        }
    }
}

/// The run number of each partition of a backfill: the place of its run in
/// [`plan_runs`], without the keys of the plan.
pub enum RunNumbers {
    /// One run holds every partition.
    OneRun,
    /// One run per partition, in the order of the requested partitions.
    RunPerPartition,
    /// One run per combination of values on the `multi_run` dimensions.
    RunPerGroup {
        multi_run: Vec<String>,
        numbers: HashMap<Vec<(String, String)>, usize>,
    },
}

impl RunNumbers {
    pub fn new(strategy: &BackfillStrategy, requested: &[PartitionKey]) -> Self {
        match strategy {
            BackfillStrategy::SingleRun => Self::OneRun,
            BackfillStrategy::MultiRun => Self::RunPerPartition,
            BackfillStrategy::PerDimension { multi_run, .. } => {
                let groups: BTreeSet<Vec<(String, String)>> = without_sets(requested)
                    .iter()
                    .flat_map(|key| groups_along(key, multi_run))
                    .collect();
                Self::RunPerGroup {
                    multi_run: multi_run.clone(),
                    numbers: groups
                        .into_iter()
                        .enumerate()
                        .map(|(number, group)| (group, number))
                        .collect(),
                }
            }
        }
    }

    /// The run number of `partition`, the one at `index` among the requested
    /// partitions. `None` for a partition that the backfill does not hold.
    pub fn of(&self, index: usize, partition: &PartitionKey) -> Option<usize> {
        match self {
            Self::OneRun => Some(0),
            Self::RunPerPartition => Some(index),
            Self::RunPerGroup { multi_run, numbers } => {
                let group = groups_along(partition, multi_run).pop()?;
                numbers.get(&group).copied()
            }
        }
    }
}

/// Collapse the keys of one run into one key: the tightest *exact* form —
/// `Single` (single-dim) or `Multi` (clean cartesian), falling back to an
/// explicit `Set` for a sparse multi-dim group no cartesian Multi can express.
/// One batched key stays as it is, with its values in sorted order.
pub fn bundle_keys(keys: &[PartitionKey]) -> PartitionKey {
    let keys = without_sets(keys);
    let candidate = union_per_dimension(&keys);

    if covers_exactly(&candidate, &keys) {
        candidate
    } else {
        let mut members = keys;
        members.sort_by_key(|a| a.to_json());
        PartitionKey::Set { keys: members }
    }
}

/// `keys` with each `Set` replaced by the keys it holds.
fn without_sets(keys: &[PartitionKey]) -> Vec<PartitionKey> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        match key {
            PartitionKey::Set { keys } => out.extend(without_sets(keys)),
            key => out.push(key.clone()),
        }
    }
    out
}

/// The combinations of the values of `key` on the `multi_run` dimensions:
/// each one is a run of its own. A key with none of those dimensions has one
/// combination, with no values.
fn groups_along(key: &PartitionKey, multi_run: &[String]) -> Vec<Vec<(String, String)>> {
    let PartitionKey::Multi { dims } = key else {
        return vec![Vec::new()];
    };
    let mut grouping: Vec<&(String, Vec<String>)> = dims
        .iter()
        .filter(|(name, _)| multi_run.contains(name))
        .collect();
    grouping.sort_by(|a, b| a.0.cmp(&b.0));

    let mut groups: Vec<Vec<(String, String)>> = vec![Vec::new()];
    for (name, values) in grouping {
        let values: BTreeSet<&String> = values.iter().collect();
        groups = groups
            .into_iter()
            .flat_map(|group| {
                values.iter().map(move |value| {
                    let mut group = group.clone();
                    group.push((name.clone(), (*value).clone()));
                    group
                })
            })
            .collect();
    }
    groups
}

/// The parts of `key` that go into different runs: one per combination of
/// its values on the `multi_run` dimensions, each with that combination.
fn split_along(
    key: &PartitionKey,
    multi_run: &[String],
) -> Vec<(Vec<(String, String)>, PartitionKey)> {
    let PartitionKey::Multi { dims } = key else {
        return vec![(Vec::new(), key.clone())];
    };

    groups_along(key, multi_run)
        .into_iter()
        .map(|group| {
            let dims = dims
                .iter()
                .map(
                    |(name, values)| match group.iter().find(|(n, _)| n == name) {
                        Some((_, value)) => (name.clone(), vec![value.clone()]),
                        None => (name.clone(), values.clone()),
                    },
                )
                .collect();
            (group, PartitionKey::Multi { dims })
        })
        .collect()
}

/// The values of `keys` joined per dimension, sorted: the smallest batched
/// key that holds every member of `keys`. Keys of another kind than the
/// first are left out, so the result then does not cover `keys`.
fn union_per_dimension(keys: &[PartitionKey]) -> PartitionKey {
    match keys.first() {
        Some(PartitionKey::Multi { .. }) => {
            // Dimensions in first-seen order.
            let mut order: Vec<String> = Vec::new();
            let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for key in keys {
                if let PartitionKey::Multi { dims } = key {
                    for (name, vals) in dims {
                        values
                            .entry(name.clone())
                            .or_insert_with(|| {
                                order.push(name.clone());
                                BTreeSet::new()
                            })
                            .extend(vals.iter().cloned());
                    }
                }
            }
            let dims = order
                .into_iter()
                .map(|name| {
                    let vals = values.remove(&name).unwrap_or_default();
                    (name, vals.into_iter().collect())
                })
                .collect();
            PartitionKey::Multi { dims }
        }
        _ => {
            let union: BTreeSet<String> = keys
                .iter()
                .filter_map(|k| match k {
                    PartitionKey::Single { keys } => Some(keys.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            PartitionKey::Single {
                keys: union.into_iter().collect(),
            }
        }
    }
}

/// Above this many batched keys, [`covers_exactly`] does not compare them
/// pair by pair.
const MAX_KEYS_COMPARED_IN_PAIRS: usize = 1024;

/// True if the members of `keys` are exactly the members of `candidate`,
/// each one time. `candidate` holds every member of `keys` of its own kind,
/// so the keys must only add up to its size and not overlap.
fn covers_exactly(candidate: &PartitionKey, keys: &[PartitionKey]) -> bool {
    let total = keys
        .iter()
        .map(PartitionKey::member_count)
        .fold(0usize, usize::saturating_add);
    if total != candidate.member_count() {
        return false;
    }

    if keys.iter().all(|k| k.member_count() == 1) {
        let mut seen: HashSet<&PartitionKey> = HashSet::with_capacity(keys.len());
        return keys.iter().all(|k| seen.insert(k));
    }
    if keys.len() > MAX_KEYS_COMPARED_IN_PAIRS {
        return false;
    }
    keys.iter()
        .enumerate()
        .all(|(i, a)| keys[i + 1..].iter().all(|b| !overlap(a, b)))
}

/// True if two value lists of one dimension have a value in common.
fn shares_a_value(x: &[String], y: &[String]) -> bool {
    if x.len().saturating_mul(y.len()) <= 4096 {
        return x.iter().any(|value| y.contains(value));
    }
    let y: HashSet<&String> = y.iter().collect();
    x.iter().any(|value| y.contains(value))
}

/// True if two batched keys can share a member. Two keys of different kinds
/// or dimensions count as overlapping: they do not join into one key.
fn overlap(a: &PartitionKey, b: &PartitionKey) -> bool {
    match (a, b) {
        (PartitionKey::Single { keys: x }, PartitionKey::Single { keys: y }) => {
            shares_a_value(x, y)
        }
        (PartitionKey::Multi { dims: x }, PartitionKey::Multi { dims: y }) => {
            x.len() != y.len()
                || x.iter().all(|(name, values)| {
                    y.iter()
                        .find(|(n, _)| n == name)
                        .is_none_or(|(_, other)| shares_a_value(values, other))
                })
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_key(s: &str) -> PartitionKey {
        PartitionKey::Single {
            keys: vec![s.to_string()],
        }
    }

    fn multi_key(dims: &[(&str, &str)]) -> PartitionKey {
        PartitionKey::Multi {
            dims: dims
                .iter()
                .map(|(k, v)| (k.to_string(), vec![v.to_string()]))
                .collect(),
        }
    }

    fn batched_key(dims: &[(&str, &[&str])]) -> PartitionKey {
        PartitionKey::Multi {
            dims: dims
                .iter()
                .map(|(k, vs)| (k.to_string(), vs.iter().map(|v| v.to_string()).collect()))
                .collect(),
        }
    }

    fn per_dimension(multi_run: &[&str], single_run: &[&str]) -> BackfillStrategy {
        BackfillStrategy::PerDimension {
            multi_run: multi_run.iter().map(|d| d.to_string()).collect(),
            single_run: single_run.iter().map(|d| d.to_string()).collect(),
        }
    }

    #[test]
    fn test_plan_multi_run() {
        let keys = vec![single_key("a"), single_key("b"), single_key("c")];
        let runs = plan_runs(&BackfillStrategy::MultiRun, &keys);
        assert_eq!(runs, keys);
    }

    #[test]
    fn test_plan_single_run() {
        let keys = vec![single_key("a"), single_key("b"), single_key("c")];
        let runs = plan_runs(&BackfillStrategy::SingleRun, &keys);
        assert_eq!(
            runs,
            vec![PartitionKey::Single {
                keys: vec!["a".into(), "b".into(), "c".into()]
            }]
        );
    }

    #[test]
    fn test_plan_per_dimension() {
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "us"), ("date", "2024-01-02")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-02")]),
        ];
        let runs = plan_runs(&per_dimension(&["region"], &["date"]), &keys);
        assert_eq!(
            runs,
            vec![
                batched_key(&[("region", &["eu"]), ("date", &["2024-01-01", "2024-01-02"])]),
                batched_key(&[("region", &["us"]), ("date", &["2024-01-01", "2024-01-02"])]),
            ]
        );
    }

    #[test]
    fn test_plan_per_dimension_deterministic_order() {
        // Deterministic order regardless of input order (not HashMap iteration).
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "ap"), ("date", "2024-01-01")]),
        ];
        let runs = plan_runs(&per_dimension(&["region"], &["date"]), &keys);
        assert_eq!(
            runs,
            vec![
                multi_key(&[("region", "ap"), ("date", "2024-01-01")]),
                multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
                multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            ]
        );
    }

    #[test]
    fn test_plan_empty() {
        for strategy in [
            BackfillStrategy::MultiRun,
            BackfillStrategy::SingleRun,
            per_dimension(&["region"], &["date"]),
        ] {
            assert!(plan_runs(&strategy, &[]).is_empty());
        }
    }

    /// `zones` × `hours` partitions as one batched key.
    fn zone_hour_key(zones: usize, hours: usize) -> PartitionKey {
        PartitionKey::Multi {
            dims: vec![
                (
                    "hour".to_string(),
                    (0..hours)
                        .map(|h| format!("2024-01-{:02}T{:02}:00", h / 24 + 1, h % 24))
                        .collect(),
                ),
                (
                    "zone".to_string(),
                    (0..zones).map(|z| format!("{z:03}")).collect(),
                ),
            ],
        }
    }

    #[test]
    fn test_per_dimension_splits_one_batched_key_into_one_run_per_value() {
        let requested = zone_hour_key(265, 480);

        let runs = plan_runs(
            &per_dimension(&["zone"], &["hour"]),
            std::slice::from_ref(&requested),
        );
        assert_eq!(runs.len(), 265);

        assert_eq!(runs[0], zone_hour_key(1, 480));
        for (zone, run_key) in runs.iter().enumerate() {
            let PartitionKey::Multi { dims } = run_key else {
                panic!("run {zone} is not a batched key: {run_key:?}");
            };
            let values = |name: &str| &dims.iter().find(|(n, _)| n == name).unwrap().1;
            assert_eq!(values("zone"), &vec![format!("{zone:03}")]);
            assert_eq!(values("hour").len(), 480);
        }
        assert_eq!(
            runs.iter().map(PartitionKey::member_count).sum::<usize>(),
            requested.member_count()
        );
    }

    /// A batched key and the list of its members are the same request.
    #[test]
    fn test_batched_key_plans_like_its_members() {
        let requested = batched_key(&[
            ("zone", &["z2", "z1", "z3"]),
            ("hour", &["h2", "h1"]),
            ("kind", &["x", "y"]),
        ]);
        for strategy in [
            BackfillStrategy::MultiRun,
            BackfillStrategy::SingleRun,
            per_dimension(&["zone"], &["hour", "kind"]),
            per_dimension(&["zone", "kind"], &["hour"]),
            per_dimension(&["kind", "zone", "hour"], &[]),
        ] {
            let from_batched = plan_runs(&strategy, std::slice::from_ref(&requested));
            let from_members = plan_runs(&strategy, &requested.members());
            assert_eq!(from_batched, from_members, "{strategy:?}");
        }
    }

    #[test]
    fn test_multi_run_makes_one_run_per_member_of_a_batched_key() {
        let requested = vec![
            PartitionKey::Single {
                keys: vec!["a".into(), "b".into()],
            },
            single_key("c"),
        ];
        let runs = plan_runs(&BackfillStrategy::MultiRun, &requested);
        assert_eq!(
            runs,
            vec![single_key("a"), single_key("b"), single_key("c")]
        );
    }

    #[test]
    fn test_single_run_keeps_one_batched_key() {
        let requested = zone_hour_key(3, 48);
        let runs = plan_runs(
            &BackfillStrategy::SingleRun,
            std::slice::from_ref(&requested),
        );
        assert_eq!(runs, vec![requested]);
    }

    #[test]
    fn test_batched_keys_of_one_run_join_when_they_form_a_product() {
        let requested = vec![
            batched_key(&[("zone", &["z1"]), ("hour", &["h1", "h2"])]),
            batched_key(&[("zone", &["z1"]), ("hour", &["h3"])]),
            batched_key(&[("zone", &["z2"]), ("hour", &["h1"])]),
        ];
        let runs = plan_runs(&per_dimension(&["zone"], &["hour"]), &requested);
        assert_eq!(
            runs,
            vec![
                batched_key(&[("zone", &["z1"]), ("hour", &["h1", "h2", "h3"])]),
                batched_key(&[("zone", &["z2"]), ("hour", &["h1"])]),
            ]
        );
    }

    #[test]
    fn test_batched_keys_that_are_no_product_stay_a_set() {
        let a = batched_key(&[("zone", &["z1", "z2"]), ("hour", &["h1"])]);
        let b = batched_key(&[("zone", &["z3"]), ("hour", &["h2"])]);
        let runs = plan_runs(&BackfillStrategy::SingleRun, &[a.clone(), b.clone()]);
        assert_eq!(runs.len(), 1);
        let PartitionKey::Set { keys } = &runs[0] else {
            panic!("expected a set, got {:?}", runs[0]);
        };
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&a) && keys.contains(&b));
        assert_eq!(runs[0].member_count(), 3);
    }

    #[test]
    fn test_batched_keys_that_overlap_stay_a_set() {
        // Sizes add up to the product z{1,2} × h{1,2}, but (z1, h1) is in both keys.
        let a = batched_key(&[("zone", &["z1"]), ("hour", &["h1", "h2"])]);
        let b = batched_key(&[("zone", &["z1", "z2"]), ("hour", &["h1"])]);
        let runs = plan_runs(&BackfillStrategy::SingleRun, &[a, b]);
        assert!(matches!(runs[0], PartitionKey::Set { .. }), "{:?}", runs[0]);
    }

    /// Canonical (dim-order-insensitive) set of keys for comparison, since core
    /// `Multi` equality is Vec-order-sensitive but dimension order is incidental.
    fn canon_set(keys: &[PartitionKey]) -> std::collections::BTreeSet<String> {
        keys.iter()
            .map(|k| {
                let mut k = k.clone();
                if let PartitionKey::Multi { dims } = &mut k {
                    dims.sort_by(|a, b| a.0.cmp(&b.0));
                }
                format!("{k:?}")
            })
            .collect()
    }

    #[test]
    fn test_bundle_single_run_round_trips_to_members() {
        let keys = vec![single_key("a"), single_key("b"), single_key("c")];
        let bundled = bundle_keys(&keys);
        assert_eq!(
            bundled,
            PartitionKey::Single {
                keys: vec!["a".into(), "b".into(), "c".into()]
            }
        );
        // The execution boundary expands the bundle back into its members.
        assert_eq!(bundled.members(), keys);
    }

    #[test]
    fn test_per_dimension_run_expands_to_its_group() {
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "us"), ("date", "2024-01-02")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-02")]),
        ];
        let runs = plan_runs(&per_dimension(&["region"], &["date"]), &keys);
        // Each run is one Multi key whose members are exactly the keys of
        // its region.
        assert_eq!(canon_set(&runs[0].members()), canon_set(&keys[2..]));
        assert_eq!(canon_set(&runs[1].members()), canon_set(&keys[..2]));
    }

    #[test]
    fn test_bundle_single_key_group_is_identity() {
        let group = vec![single_key("a")];
        assert_eq!(bundle_keys(&group), single_key("a"));
    }

    #[test]
    fn test_members_single_valued_key_is_self() {
        assert_eq!(single_key("x").members(), vec![single_key("x")]);
        let m = multi_key(&[("region", "us"), ("date", "d1")]);
        assert_eq!(m.members(), vec![m]);
    }

    #[test]
    fn test_bundle_sparse_multi_dim_uses_set() {
        // region × date × hour, sparse: no cartesian Multi reproduces this group.
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "d1"), ("hour", "h1")]),
            multi_key(&[("region", "us"), ("date", "d2"), ("hour", "h2")]),
        ];
        let bundled = bundle_keys(&keys);
        assert!(matches!(bundled, PartitionKey::Set { .. }));
        // The explicit Set still expands back to exactly the group (no over-include).
        assert_eq!(canon_set(&bundled.members()), canon_set(&keys));
    }

    #[test]
    fn test_bundle_repeated_key_uses_set() {
        let keys = vec![single_key("a"), single_key("a"), single_key("b")];
        let bundled = bundle_keys(&keys);
        assert_eq!(bundled, PartitionKey::Set { keys });
    }

    #[test]
    fn test_set_json_round_trip() {
        let set = PartitionKey::Set {
            keys: vec![single_key("a"), single_key("b")],
        };
        let back = PartitionKey::from_json(&set.to_json()).unwrap();
        assert_eq!(canon_set(&back.members()), canon_set(&set.members()));
    }

    #[test]
    fn test_bundle_large_cartesian_is_compact_multi() {
        // A clean region×sku cartesian must bundle into a compact Multi, not a Set.
        let mut keys = Vec::new();
        for r in ["us", "eu", "apac"] {
            for i in 0..2000 {
                keys.push(PartitionKey::Multi {
                    dims: vec![
                        ("region".to_string(), vec![r.to_string()]),
                        ("sku".to_string(), vec![format!("sku{i:05}")]),
                    ],
                });
            }
        }
        let runs = plan_runs(&BackfillStrategy::SingleRun, &keys);
        let bundled = &runs[0];
        // Compact cartesian form, not an explicit Set.
        assert!(matches!(bundled, PartitionKey::Multi { .. }));
        assert_eq!(bundled.members().len(), keys.len());
        assert_eq!(canon_set(&bundled.members()), canon_set(&keys));
    }

    /// Each requested partition is in the run of the plan that its run
    /// number names, and each run of the plan has a number.
    fn assert_run_numbers_match_the_plan(strategy: &BackfillStrategy, requested: &[PartitionKey]) {
        let plan = plan_runs(strategy, requested);
        let numbers = RunNumbers::new(strategy, requested);
        let partitions: Vec<PartitionKey> =
            requested.iter().flat_map(PartitionKey::members).collect();

        let mut used: BTreeSet<usize> = BTreeSet::new();
        for (index, partition) in partitions.iter().enumerate() {
            let run_number = numbers
                .of(index, partition)
                .expect("a run for each requested partition");
            assert!(
                plan[run_number].members().contains(partition),
                "{} is in run {run_number} under {strategy:?}",
                partition.to_display()
            );
            used.insert(run_number);
        }
        assert_eq!(used.len(), plan.len(), "{strategy:?}");
    }

    #[test]
    fn test_run_numbers_match_the_plan() {
        let requested = vec![
            batched_key(&[("zone", &["z2", "z1", "z3"]), ("hour", &["h1", "h2"])]),
            batched_key(&[("zone", &["z0"]), ("hour", &["h9"])]),
        ];
        for strategy in [
            BackfillStrategy::MultiRun,
            BackfillStrategy::SingleRun,
            per_dimension(&["zone"], &["hour"]),
            per_dimension(&["zone", "hour"], &[]),
            per_dimension(&[], &["zone", "hour"]),
        ] {
            assert_run_numbers_match_the_plan(&strategy, &requested);
            assert_run_numbers_match_the_plan(
                &strategy,
                &[PartitionKey::Set {
                    keys: requested.clone(),
                }],
            );
        }

        let one_dimension = [
            single_key("a"),
            PartitionKey::Single {
                keys: vec!["b".to_string(), "c".to_string()],
            },
        ];
        for strategy in [
            BackfillStrategy::MultiRun,
            BackfillStrategy::SingleRun,
            per_dimension(&["zone"], &[]),
        ] {
            assert_run_numbers_match_the_plan(&strategy, &one_dimension);
        }
    }

    #[test]
    fn test_run_number_of_a_partition_outside_the_backfill_is_none() {
        let requested = [batched_key(&[("zone", &["z1"]), ("hour", &["h1"])])];
        let numbers = RunNumbers::new(&per_dimension(&["zone"], &["hour"]), &requested);

        assert_eq!(
            numbers.of(0, &multi_key(&[("zone", "z9"), ("hour", "h1")])),
            None
        );
    }

    #[test]
    fn test_member_count_and_preview() {
        // Multi cartesian: count is the product (no materialization); preview is
        // the first N in `members()` order without building the other ~15k.
        let key = PartitionKey::Multi {
            dims: vec![
                (
                    "region".to_string(),
                    vec!["us".into(), "eu".into(), "apac".into()],
                ),
                (
                    "sku".to_string(),
                    (0..5000).map(|i| format!("sku{i:05}")).collect(),
                ),
            ],
        };
        assert_eq!(key.member_count(), 15_000);
        let preview = key.members_preview(3);
        assert_eq!(
            preview,
            key.members().into_iter().take(3).collect::<Vec<_>>()
        );

        // Single and Set.
        let single = PartitionKey::Single {
            keys: vec!["a".into(), "b".into(), "c".into()],
        };
        assert_eq!(single.member_count(), 3);
        assert_eq!(single.members_preview(2).len(), 2);
        let set = PartitionKey::Set {
            keys: vec![single_key("x"), single_key("y")],
        };
        assert_eq!(set.member_count(), 2);
        assert_eq!(set.members_preview(10).len(), 2);
    }
}
