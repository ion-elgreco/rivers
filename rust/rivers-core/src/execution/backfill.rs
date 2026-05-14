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
            groups.into_values().collect()
        }
    }
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
        PartitionKey::Single { .. } => vec![],
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
    fn test_group_empty() {
        let groups = group_into_runs(&BackfillStrategy::SingleRun, &[]);
        assert!(groups.is_empty());
    }
}
