//! Backfill grouping — split partition keys into runs per strategy.

use std::collections::HashMap;

use crate::storage::{BackfillStrategy, PartitionKey};

/// Group partition keys into run groups per backfill strategy.
pub fn group_into_runs(
    strategy: &BackfillStrategy,
    partition_keys: &[PartitionKey],
) -> Vec<Vec<PartitionKey>> {
    match strategy {
        BackfillStrategy::MultiRun => partition_keys.iter().map(|pk| vec![pk.clone()]).collect(),
        BackfillStrategy::SingleRun => {
            if partition_keys.is_empty() {
                vec![]
            } else {
                vec![partition_keys.to_vec()]
            }
        }
        BackfillStrategy::PerDimension { multi_run, .. } => {
            let mut groups: HashMap<Vec<(String, Vec<String>)>, Vec<PartitionKey>> = HashMap::new();
            for pk in partition_keys {
                let group_key = extract_multi_run_dims(pk, multi_run);
                groups.entry(group_key).or_default().push(pk.clone());
            }
            // Sort for deterministic order (HashMap iteration is randomized) so
            // execution / stop-on-failure cancellation is reproducible.
            let mut ordered: Vec<_> = groups.into_iter().collect();
            ordered.sort_by(|a, b| a.0.cmp(&b.0));
            ordered.into_iter().map(|(_, g)| g).collect()
        }
    }
}

/// Collapse a run group into one batched key: the tightest *exact* form —
/// `Single` (single-dim) or `Multi` (clean cartesian), falling back to an
/// explicit `Set` for a sparse multi-dim group no cartesian Multi can express.
pub fn bundle_keys(keys: &[PartitionKey]) -> PartitionKey {
    let candidate = match keys.first() {
        Some(PartitionKey::Multi { .. }) => {
            // Union values per dimension, preserving first-seen dimension order.
            let mut order: Vec<String> = Vec::new();
            let mut values: HashMap<String, Vec<String>> = HashMap::new();
            for key in keys {
                if let PartitionKey::Multi { dims } = key {
                    for (name, vals) in dims {
                        let entry = values.entry(name.clone()).or_insert_with(|| {
                            order.push(name.clone());
                            Vec::new()
                        });
                        for v in vals {
                            if !entry.contains(v) {
                                entry.push(v.clone());
                            }
                        }
                    }
                }
            }
            let dims = order
                .into_iter()
                .map(|name| {
                    let mut vals = values.remove(&name).unwrap_or_default();
                    vals.sort();
                    (name, vals)
                })
                .collect();
            PartitionKey::Multi { dims }
        }
        _ => {
            let mut union: Vec<String> = keys
                .iter()
                .filter_map(|k| match k {
                    PartitionKey::Single { keys } => Some(keys.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            union.sort();
            union.dedup();
            PartitionKey::Single { keys: union }
        }
    };

    // Keep the compact cartesian form only when it reproduces the group exactly;
    // otherwise an explicit Set preserves a sparse multi-dim selection.
    if same_set(&candidate.members(), keys) {
        candidate
    } else {
        let mut members = keys.to_vec();
        members.sort_by(|a, b| a.to_json().cmp(&b.to_json()));
        PartitionKey::Set { keys: members }
    }
}

/// Order-independent set equality.
fn same_set(a: &[PartitionKey], b: &[PartitionKey]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let b_set: std::collections::HashSet<&PartitionKey> = b.iter().collect();
    a.iter().all(|k| b_set.contains(k))
}

fn extract_multi_run_dims(pk: &PartitionKey, multi_run: &[String]) -> Vec<(String, Vec<String>)> {
    match pk {
        PartitionKey::Multi { dims } => {
            let mut result: Vec<(String, Vec<String>)> = dims
                .iter()
                .filter(|(name, _)| multi_run.contains(name))
                .cloned()
                .collect();
            result.sort_by(|a, b| a.0.cmp(&b.0));
            result
        }
        _ => vec![],
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

    #[test]
    fn test_group_multi_run() {
        let keys = vec![single_key("a"), single_key("b"), single_key("c")];
        let groups = group_into_runs(&BackfillStrategy::MultiRun, &keys);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 1);
    }

    #[test]
    fn test_group_single_run() {
        let keys = vec![single_key("a"), single_key("b"), single_key("c")];
        let groups = group_into_runs(&BackfillStrategy::SingleRun, &keys);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn test_group_per_dimension() {
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "us"), ("date", "2024-01-02")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-02")]),
        ];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["region".to_string()],
            single_run: vec!["date".to_string()],
        };
        let groups = group_into_runs(&strategy, &keys);
        assert_eq!(groups.len(), 2);
        for group in &groups {
            assert_eq!(group.len(), 2);
        }
    }

    #[test]
    fn test_group_per_dimension_deterministic_order() {
        // Deterministic order regardless of input order (not HashMap iteration).
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "ap"), ("date", "2024-01-01")]),
        ];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["region".to_string()],
            single_run: vec!["date".to_string()],
        };
        let regions: Vec<String> = group_into_runs(&strategy, &keys)
            .iter()
            .map(|g| match &g[0] {
                PartitionKey::Multi { dims } => dims
                    .iter()
                    .find(|(n, _)| n == "region")
                    .map(|(_, v)| v[0].clone())
                    .unwrap(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(regions, vec!["ap", "eu", "us"]);
    }

    #[test]
    fn test_group_empty() {
        let groups = group_into_runs(&BackfillStrategy::SingleRun, &[]);
        assert!(groups.is_empty());
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
        let group = &group_into_runs(&BackfillStrategy::SingleRun, &keys)[0];
        let bundled = bundle_keys(group);
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
    fn test_bundle_per_dimension_group_expands_to_group() {
        let keys = vec![
            multi_key(&[("region", "us"), ("date", "2024-01-01")]),
            multi_key(&[("region", "us"), ("date", "2024-01-02")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-01")]),
            multi_key(&[("region", "eu"), ("date", "2024-01-02")]),
        ];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["region".to_string()],
            single_run: vec!["date".to_string()],
        };
        for group in group_into_runs(&strategy, &keys) {
            // Each region-group bundles into one Multi key whose members are
            // exactly that group's keys (the dates for that region).
            let members = bundle_keys(&group).members();
            assert_eq!(members.len(), group.len());
            assert_eq!(canon_set(&members), canon_set(&group));
        }
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
        let group = &group_into_runs(&BackfillStrategy::SingleRun, &keys)[0];
        let bundled = bundle_keys(group);
        // Compact cartesian form, not an explicit Set.
        assert!(matches!(bundled, PartitionKey::Multi { .. }));
        assert_eq!(bundled.members().len(), keys.len());
        assert_eq!(canon_set(&bundled.members()), canon_set(&keys));
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
