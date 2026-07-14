use std::collections::HashMap;
use std::fmt;

use crate::concurrency::config::TagConcurrencyLimit;

/// Anything that carries run tags.
pub trait Tagged {
    fn tags(&self) -> &[(String, String)];
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum BlockReason {
    GlobalRunLimit {
        current: u32,
        limit: u32,
    },
    TagLimit {
        key: String,
        value: Option<String>,
        current: u32,
        limit: u32,
    },
    PoolFull {
        pool_key: String,
        claimed: u32,
        limit: u32,
    },
    PoolsFull {
        pools: Vec<PoolBlockDetail>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolBlockDetail {
    pub pool_key: String,
    pub claimed: u32,
    pub limit: u32,
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockReason::GlobalRunLimit { current, limit } => {
                write!(f, "global run limit ({current}/{limit})")
            }
            BlockReason::TagLimit {
                key,
                value,
                current,
                limit,
            } => {
                if let Some(v) = value {
                    write!(f, "tag limit: {key}={v} ({current}/{limit})")
                } else {
                    write!(f, "tag limit: {key} ({current}/{limit})")
                }
            }
            BlockReason::PoolFull {
                pool_key,
                claimed,
                limit,
            } => {
                write!(f, "pool '{pool_key}' full ({claimed}/{limit} slots)")
            }
            BlockReason::PoolsFull { pools } => {
                let parts: Vec<String> = pools
                    .iter()
                    .map(|p| format!("'{}' ({}/{})", p.pool_key, p.claimed, p.limit))
                    .collect();
                write!(f, "pools full: {}", parts.join(", "))
            }
        }
    }
}

pub struct TagConcurrencyCounter<'a> {
    key_counts: HashMap<String, u32>,
    key_value_counts: HashMap<(String, String), u32>,
    /// key -> (value -> count); used for per_unique_value limits.
    unique_value_counts: HashMap<String, HashMap<String, u32>>,
    limits: &'a [TagConcurrencyLimit],
}

impl<'a> TagConcurrencyCounter<'a> {
    pub fn from_runs(runs: &[impl Tagged], limits: &'a [TagConcurrencyLimit]) -> Self {
        let mut key_counts: HashMap<String, u32> = HashMap::new();
        let mut key_value_counts: HashMap<(String, String), u32> = HashMap::new();
        let mut unique_value_counts: HashMap<String, HashMap<String, u32>> = HashMap::new();

        // Only count tags for keys that appear in limits.
        let limit_keys: std::collections::HashSet<&str> =
            limits.iter().map(|l| l.key.as_str()).collect();

        for run in runs {
            for (k, v) in run.tags() {
                if !limit_keys.contains(k.as_str()) {
                    continue;
                }
                *key_counts.entry(k.clone()).or_default() += 1;
                *key_value_counts.entry((k.clone(), v.clone())).or_default() += 1;
                *unique_value_counts
                    .entry(k.clone())
                    .or_default()
                    .entry(v.clone())
                    .or_default() += 1;
            }
        }

        Self {
            key_counts,
            key_value_counts,
            unique_value_counts,
            limits,
        }
    }

    /// Returns `Some(reason)` if launching this run would violate a limit.
    pub fn is_blocked(&self, run: &impl Tagged) -> Option<BlockReason> {
        for limit in self.limits {
            if limit.per_unique_value {
                let counts = self.unique_value_counts.get(&limit.key);
                for (k, v) in run.tags() {
                    if k != &limit.key {
                        continue;
                    }
                    let current = counts.and_then(|m| m.get(v.as_str())).copied().unwrap_or(0);
                    if current >= limit.limit {
                        return Some(BlockReason::TagLimit {
                            key: limit.key.clone(),
                            value: Some(v.clone()),
                            current,
                            limit: limit.limit,
                        });
                    }
                }
            } else if let Some(ref required_value) = limit.value {
                if run
                    .tags()
                    .iter()
                    .any(|(k, v)| k == &limit.key && v == required_value)
                {
                    let current = self
                        .key_value_counts
                        .get(&(limit.key.clone(), required_value.clone()))
                        .copied()
                        .unwrap_or(0);
                    if current >= limit.limit {
                        return Some(BlockReason::TagLimit {
                            key: limit.key.clone(),
                            value: Some(required_value.clone()),
                            current,
                            limit: limit.limit,
                        });
                    }
                }
            } else if run.tags().iter().any(|(k, _)| k == &limit.key) {
                let current = self.key_counts.get(&limit.key).copied().unwrap_or(0);
                if current >= limit.limit {
                    return Some(BlockReason::TagLimit {
                        key: limit.key.clone(),
                        value: None,
                        current,
                        limit: limit.limit,
                    });
                }
            }
        }
        None
    }

    pub fn record_launch(&mut self, run: &impl Tagged) {
        for (k, v) in run.tags() {
            *self.key_counts.entry(k.clone()).or_default() += 1;
            *self
                .key_value_counts
                .entry((k.clone(), v.clone()))
                .or_default() += 1;
            *self
                .unique_value_counts
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_default() += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, LaunchedBy, RunRecord, RunStatus};

    fn make_run(id: &str, tags: &[(&str, &str)]) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Started,
            start_time: 1000,
            end_time: None,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        }
    }

    #[test]
    fn test_key_level_limit() {
        let in_progress = vec![
            make_run("r1", &[("team", "alpha")]),
            make_run("r2", &[("team", "beta")]),
            make_run("r3", &[("team", "alpha")]),
        ];
        let limits = vec![TagConcurrencyLimit {
            key: "team".to_string(),
            value: None,
            per_unique_value: false,
            limit: 3,
        }];
        let counter = TagConcurrencyCounter::from_runs(&in_progress, &limits);

        let candidate = make_run("r4", &[("team", "gamma")]);
        let reason = counter.is_blocked(&candidate);
        assert!(reason.is_some());
        match reason.unwrap() {
            BlockReason::TagLimit {
                key,
                value,
                current,
                limit,
            } => {
                assert_eq!(key, "team");
                assert!(value.is_none());
                assert_eq!(current, 3);
                assert_eq!(limit, 3);
            }
            _ => panic!("wrong reason type"),
        }

        let no_tag = make_run("r5", &[("other", "val")]);
        assert!(counter.is_blocked(&no_tag).is_none());
    }

    #[test]
    fn test_key_value_limit() {
        let in_progress = vec![make_run("r1", &[("db", "warehouse")])];
        let limits = vec![TagConcurrencyLimit {
            key: "db".to_string(),
            value: Some("warehouse".to_string()),
            per_unique_value: false,
            limit: 1,
        }];
        let counter = TagConcurrencyCounter::from_runs(&in_progress, &limits);

        let warehouse = make_run("r2", &[("db", "warehouse")]);
        assert!(counter.is_blocked(&warehouse).is_some());

        let postgres = make_run("r3", &[("db", "postgres")]);
        assert!(counter.is_blocked(&postgres).is_none());
    }

    #[test]
    fn test_per_unique_value_limit() {
        let in_progress = vec![
            make_run("r1", &[("env", "prod")]),
            make_run("r2", &[("env", "staging")]),
        ];
        let limits = vec![TagConcurrencyLimit {
            key: "env".to_string(),
            value: None,
            per_unique_value: true,
            limit: 1,
        }];
        let counter = TagConcurrencyCounter::from_runs(&in_progress, &limits);

        let prod = make_run("r3", &[("env", "prod")]);
        assert!(counter.is_blocked(&prod).is_some());

        let dev = make_run("r4", &[("env", "dev")]);
        assert!(counter.is_blocked(&dev).is_none());
    }

    #[test]
    fn test_record_launch_updates_counters() {
        let limits = vec![TagConcurrencyLimit {
            key: "team".to_string(),
            value: None,
            per_unique_value: false,
            limit: 2,
        }];
        let mut counter = TagConcurrencyCounter::from_runs(&[] as &[RunRecord], &limits);

        let r1 = make_run("r1", &[("team", "alpha")]);
        assert!(counter.is_blocked(&r1).is_none());
        counter.record_launch(&r1);

        let r2 = make_run("r2", &[("team", "beta")]);
        assert!(counter.is_blocked(&r2).is_none());
        counter.record_launch(&r2);

        let r3 = make_run("r3", &[("team", "gamma")]);
        assert!(counter.is_blocked(&r3).is_some());
    }

    #[test]
    fn test_no_limits_nothing_blocked() {
        let in_progress = vec![
            make_run("r1", &[("team", "a")]),
            make_run("r2", &[("team", "b")]),
        ];
        let counter = TagConcurrencyCounter::from_runs(&in_progress, &[]);
        let candidate = make_run("r3", &[("team", "c")]);
        assert!(counter.is_blocked(&candidate).is_none());
    }

    #[test]
    fn test_block_reason_display() {
        let reason = BlockReason::TagLimit {
            key: "team".to_string(),
            value: None,
            current: 3,
            limit: 3,
        };
        assert_eq!(reason.to_string(), "tag limit: team (3/3)");

        let reason = BlockReason::TagLimit {
            key: "db".to_string(),
            value: Some("warehouse".to_string()),
            current: 1,
            limit: 1,
        };
        assert_eq!(reason.to_string(), "tag limit: db=warehouse (1/1)");
    }
}
