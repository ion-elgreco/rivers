//! A partitions definition as storage keeps it, and how a remote asset's
//! mirror compares with the owner's definition.

use std::collections::BTreeMap;
use std::fmt;

use jiff::civil;
use jiff::fmt::strtime;
use serde::{Deserialize, Serialize};

use super::graph::GraphTopology;
use crate::timegrid::TimeGrid;

/// A partitions definition in a form two code locations can compare.
/// Static keys are stored, not hashed, so added and removed keys can be told
/// apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionsFingerprint {
    /// Keys sorted, so the order they were declared in does not count.
    Static {
        keys: Vec<String>,
    },
    TimeWindow(TimeGrid),
    /// Dimensions sorted by name.
    Multi {
        dimensions: Vec<(String, PartitionsFingerprint)>,
    },
    Dynamic {
        name: String,
    },
}

impl PartitionsFingerprint {
    pub fn static_keys(keys: impl IntoIterator<Item = String>) -> Self {
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();
        Self::Static { keys }
    }

    pub fn multi(dimensions: impl IntoIterator<Item = (String, Self)>) -> Self {
        let mut dimensions: Vec<(String, Self)> = dimensions.into_iter().collect();
        dimensions.sort_by(|a, b| a.0.cmp(&b.0));
        Self::Multi { dimensions }
    }

    fn has_dynamic(&self) -> bool {
        match self {
            Self::Dynamic { .. } => true,
            Self::Multi { dimensions } => dimensions.iter().any(|(_, d)| d.has_dynamic()),
            _ => false,
        }
    }
}

/// `"daily from 2024-01-01"`: a time window names its first key, so a
/// changed key format shows.
impl fmt::Display for PartitionsFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static { keys } => {
                let noun = if keys.len() == 1 { "key" } else { "keys" };
                write!(f, "static ({} {noun})", keys.len())
            }
            Self::TimeWindow(grid) => {
                match (grid.cron_schedule.as_deref(), grid.interval_seconds) {
                    (Some("0 0 * * *"), _) => f.write_str("daily")?,
                    (Some("0 * * * *"), _) => f.write_str("hourly")?,
                    (Some(cron), _) => write!(f, "cron '{cron}'")?,
                    (None, Some(seconds)) => write!(f, "every {seconds} s")?,
                    (None, None) => f.write_str("time window")?,
                }
                let key = |t: civil::DateTime| {
                    strtime::format(&grid.fmt, t).unwrap_or_else(|_| t.to_string())
                };
                write!(f, " from {}", key(grid.start))?;
                if let Some(end) = grid.end {
                    write!(f, " to {}", key(end))?;
                }
                Ok(())
            }
            Self::Multi { dimensions } => {
                f.write_str("multi (")?;
                for (i, (name, dimension)) in dimensions.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{name}: {dimension}")?;
                }
                f.write_str(")")
            }
            Self::Dynamic { name } => write!(f, "dynamic '{name}'"),
        }
    }
}

/// The partitions definitions of a code location's nodes. Storage keeps them
/// apart from the topology: static keys make them large, and only partition
/// drift checks read them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionDefinitions {
    /// Node name → index into [`Self::definitions`]. An unpartitioned node
    /// has no entry.
    pub nodes: BTreeMap<String, usize>,
    /// Each distinct definition once.
    pub definitions: Vec<PartitionsFingerprint>,
}

impl PartitionDefinitions {
    /// The definition of the node `name`.
    pub fn get(&self, name: &str) -> Option<&PartitionsFingerprint> {
        self.definitions.get(*self.nodes.get(name)?)
    }

    /// The index of `fingerprint` in [`Self::definitions`], added if new.
    pub fn intern(&mut self, fingerprint: PartitionsFingerprint) -> usize {
        if let Some(index) = self.definitions.iter().position(|d| *d == fingerprint) {
            return index;
        }
        self.definitions.push(fingerprint);
        self.definitions.len() - 1
    }
}

/// [`PartitionsFingerprint`]'s description, `"unpartitioned"` for `None`.
pub fn describe_partitions(fingerprint: Option<&PartitionsFingerprint>) -> String {
    fingerprint.map_or_else(|| "unpartitioned".to_string(), ToString::to_string)
}

/// How a remote asset's mirrored partitions definition compares with the
/// owner's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionDrift {
    Same,
    /// The owner moved `start`/`end` on the same grid or added static keys.
    /// A mirror key names an owner partition when it lies in the owner's
    /// range; the others stay missing.
    Compatible,
    /// Another kind, schedule or key format, an interval `start` moved by
    /// part of the interval, or removed static keys.
    Incompatible,
    /// The owner has no asset of that key.
    OwnerNodeMissing,
}

/// How `mirror` compares with the owner's definition of `asset_key`. `owner`
/// is the owner's topology, `definitions` its partitions definitions.
pub fn partition_drift(
    owner: &GraphTopology,
    definitions: &PartitionDefinitions,
    asset_key: &str,
    mirror: Option<&PartitionsFingerprint>,
) -> PartitionDrift {
    if owner.owned_node(asset_key).is_none() {
        return PartitionDrift::OwnerNodeMissing;
    }
    match (mirror, definitions.get(asset_key)) {
        (None, None) => PartitionDrift::Same,
        (Some(mirror), Some(owner)) => compare(mirror, owner),
        _ => PartitionDrift::Incompatible,
    }
}

fn compare(mirror: &PartitionsFingerprint, owner: &PartitionsFingerprint) -> PartitionDrift {
    use PartitionsFingerprint::*;

    if mirror.has_dynamic() || owner.has_dynamic() {
        return PartitionDrift::Incompatible;
    }
    if mirror == owner {
        return PartitionDrift::Same;
    }
    let compatible = match (mirror, owner) {
        (Static { keys: mirror }, Static { keys: owner }) => {
            let mut owner = owner.iter();
            mirror.iter().all(|key| owner.any(|k| k == key))
        }
        (TimeWindow(mirror), TimeWindow(owner)) => mirror.aligns_with(owner),
        (Multi { dimensions: mirror }, Multi { dimensions: owner }) => {
            mirror.len() == owner.len()
                && mirror.iter().zip(owner).all(|((m_name, m), (o_name, o))| {
                    m_name == o_name && compare(m, o) != PartitionDrift::Incompatible
                })
        }
        _ => false,
    };
    if compatible {
        PartitionDrift::Compatible
    } else {
        PartitionDrift::Incompatible
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::assets::remote::{LocationRef, RemoteRef};
    use jiff::civil::date;

    fn statics(keys: &[&str]) -> PartitionsFingerprint {
        PartitionsFingerprint::static_keys(keys.iter().map(|k| k.to_string()))
    }

    fn daily() -> TimeGrid {
        TimeGrid {
            cron_schedule: Some("0 0 * * *".into()),
            interval_seconds: None,
            start: date(2026, 1, 1).at(0, 0, 0, 0),
            end: None,
            fmt: "%Y-%m-%d".into(),
        }
    }

    fn window(grid: TimeGrid) -> PartitionsFingerprint {
        PartitionsFingerprint::TimeWindow(grid)
    }

    fn owner_with(
        fingerprint: Option<PartitionsFingerprint>,
    ) -> (GraphTopology, PartitionDefinitions) {
        let mut owner = GraphTopology::default();
        owner
            .nodes
            .push(TopologyNode::new("raw_orders", NodeKind::Asset));
        let mut definitions = PartitionDefinitions::default();
        if let Some(fingerprint) = fingerprint {
            let index = definitions.intern(fingerprint);
            definitions.nodes.insert("raw_orders".into(), index);
        }
        (owner, definitions)
    }

    fn drift(
        mirror: Option<PartitionsFingerprint>,
        owner: Option<PartitionsFingerprint>,
    ) -> PartitionDrift {
        let (owner, definitions) = owner_with(owner);
        partition_drift(&owner, &definitions, "raw_orders", mirror.as_ref())
    }

    #[test]
    fn static_keys_ignore_declaration_order() {
        assert_eq!(statics(&["b", "a", "b"]), statics(&["a", "b"]));
        assert_eq!(
            drift(Some(statics(&["eu", "us"])), Some(statics(&["us", "eu"]))),
            PartitionDrift::Same
        );
    }

    #[test]
    fn an_added_static_key_is_compatible_and_a_removed_one_is_not() {
        let mirror = || Some(statics(&["eu", "us"]));
        assert_eq!(
            drift(mirror(), Some(statics(&["apac", "eu", "us"]))),
            PartitionDrift::Compatible
        );
        assert_eq!(
            drift(mirror(), Some(statics(&["eu"]))),
            PartitionDrift::Incompatible
        );
        assert_eq!(
            drift(mirror(), Some(statics(&["eu", "uk"]))),
            PartitionDrift::Incompatible
        );
    }

    #[test]
    fn a_moved_range_is_compatible_and_another_grid_is_not() {
        let moved = TimeGrid {
            start: date(2025, 1, 1).at(0, 0, 0, 0),
            end: Some(date(2027, 1, 1).at(0, 0, 0, 0)),
            ..daily()
        };
        assert_eq!(
            drift(Some(window(daily())), Some(window(daily()))),
            PartitionDrift::Same
        );
        assert_eq!(
            drift(Some(window(daily())), Some(window(moved))),
            PartitionDrift::Compatible
        );

        let hourly = TimeGrid {
            cron_schedule: Some("0 * * * *".into()),
            ..daily()
        };
        let other_format = TimeGrid {
            fmt: "%Y%m%d".into(),
            ..daily()
        };
        let interval = TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(86_400.0),
            ..daily()
        };
        for owner in [hourly, other_format, interval] {
            assert_eq!(
                drift(Some(window(daily())), Some(window(owner))),
                PartitionDrift::Incompatible
            );
        }
    }

    #[test]
    fn an_interval_start_off_the_mirrors_grid_is_incompatible() {
        let hourly = |start: civil::DateTime, end: Option<civil::DateTime>| TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(3_600.0),
            start,
            end,
            fmt: "%Y-%m-%dT%H:%M".into(),
        };
        let at = |hour, minute| date(2026, 1, 1).at(hour, minute, 0, 0);
        let mirror = || Some(window(hourly(at(0, 0), None)));

        let compatible = [
            hourly(at(3, 0), None),
            hourly(date(2025, 12, 31).at(21, 0, 0, 0), None),
            hourly(at(0, 0), Some(at(12, 0))),
        ];
        for owner in compatible {
            assert_eq!(
                drift(mirror(), Some(window(owner.clone()))),
                PartitionDrift::Compatible,
                "{owner:?}"
            );
        }
        let incompatible = [
            hourly(at(0, 30), None),
            hourly(date(2025, 12, 31).at(23, 59, 59, 0), Some(at(12, 0))),
        ];
        for owner in incompatible {
            assert_eq!(
                drift(mirror(), Some(window(owner.clone()))),
                PartitionDrift::Incompatible,
                "{owner:?}"
            );
        }

        let too_short = |start| TimeGrid {
            interval_seconds: Some(1e-12),
            ..hourly(start, None)
        };
        assert_eq!(
            drift(
                Some(window(too_short(at(0, 0)))),
                Some(window(too_short(at(3, 0))))
            ),
            PartitionDrift::Incompatible
        );
        assert_eq!(
            drift(
                mirror(),
                Some(window(hourly(date(1700, 1, 1).at(0, 0, 0, 0), None)))
            ),
            PartitionDrift::Incompatible,
            "a distance past i64 nanoseconds counts as not aligned"
        );

        let cron_moved = TimeGrid {
            start: at(0, 30),
            ..daily()
        };
        assert_eq!(
            drift(Some(window(daily())), Some(window(cron_moved))),
            PartitionDrift::Compatible
        );

        let multi = |grid| {
            PartitionsFingerprint::multi([
                ("region".to_string(), statics(&["eu"])),
                ("hour".to_string(), window(grid)),
            ])
        };
        assert_eq!(
            drift(
                Some(multi(hourly(at(0, 0), None))),
                Some(multi(hourly(at(3, 0), None)))
            ),
            PartitionDrift::Compatible
        );
        assert_eq!(
            drift(
                Some(multi(hourly(at(0, 0), None))),
                Some(multi(hourly(at(0, 30), None)))
            ),
            PartitionDrift::Incompatible
        );
    }

    #[test]
    fn multi_is_as_compatible_as_its_least_compatible_dimension() {
        let multi = |regions: &[&str], grid: TimeGrid| {
            PartitionsFingerprint::multi([
                ("region".to_string(), statics(regions)),
                ("date".to_string(), window(grid)),
            ])
        };
        let mirror = || Some(multi(&["eu"], daily()));
        let later = TimeGrid {
            end: Some(date(2027, 1, 1).at(0, 0, 0, 0)),
            ..daily()
        };

        assert_eq!(
            drift(mirror(), Some(multi(&["eu"], daily()))),
            PartitionDrift::Same
        );
        assert_eq!(
            drift(mirror(), Some(multi(&["eu", "us"], later))),
            PartitionDrift::Compatible
        );
        assert_eq!(
            drift(mirror(), Some(multi(&["us"], daily()))),
            PartitionDrift::Incompatible
        );
        let renamed = PartitionsFingerprint::multi([
            ("area".to_string(), statics(&["eu"])),
            ("date".to_string(), window(daily())),
        ]);
        assert_eq!(drift(mirror(), Some(renamed)), PartitionDrift::Incompatible);
    }

    #[test]
    fn dynamic_on_either_side_is_incompatible() {
        let dynamic = || PartitionsFingerprint::Dynamic {
            name: "customers".into(),
        };
        assert_eq!(
            drift(Some(dynamic()), Some(dynamic())),
            PartitionDrift::Incompatible
        );
        assert_eq!(
            drift(Some(statics(&["a"])), Some(dynamic())),
            PartitionDrift::Incompatible
        );
        let multi = PartitionsFingerprint::multi([
            ("customer".to_string(), dynamic()),
            ("date".to_string(), window(daily())),
        ]);
        assert_eq!(
            drift(Some(multi.clone()), Some(multi)),
            PartitionDrift::Incompatible
        );
    }

    #[test]
    fn partitioned_on_one_side_only_is_incompatible() {
        assert_eq!(drift(None, None), PartitionDrift::Same);
        assert_eq!(
            drift(None, Some(window(daily()))),
            PartitionDrift::Incompatible
        );
        assert_eq!(
            drift(Some(window(daily())), None),
            PartitionDrift::Incompatible
        );
    }

    #[test]
    fn the_owner_node_must_be_an_asset_it_owns() {
        let (mut owner, definitions) = owner_with(None);
        assert_eq!(
            partition_drift(&owner, &definitions, "elsewhere", None),
            PartitionDrift::OwnerNodeMissing
        );

        owner.nodes[0].remote = Some(RemoteRef {
            code_location: LocationRef::new("dev", "lake").unwrap(),
            asset_key: "raw_orders".into(),
        });
        assert_eq!(
            partition_drift(&owner, &definitions, "raw_orders", None),
            PartitionDrift::OwnerNodeMissing
        );
    }

    #[test]
    fn descriptions_name_the_schedule_and_the_first_key() {
        let hourly_to = TimeGrid {
            cron_schedule: Some("0 * * * *".into()),
            end: Some(date(2026, 2, 1).at(0, 0, 0, 0)),
            fmt: "%Y-%m-%dT%H:00".into(),
            ..daily()
        };
        let cases = [
            (window(daily()), "daily from 2026-01-01"),
            (
                window(hourly_to),
                "hourly from 2026-01-01T00:00 to 2026-02-01T00:00",
            ),
            (
                window(TimeGrid {
                    cron_schedule: Some("0 0 * * 1".into()),
                    fmt: "%Y%m%d".into(),
                    ..daily()
                }),
                "cron '0 0 * * 1' from 20260101",
            ),
            (
                window(TimeGrid {
                    cron_schedule: None,
                    interval_seconds: Some(900.0),
                    ..daily()
                }),
                "every 900 s from 2026-01-01",
            ),
            (statics(&["eu"]), "static (1 key)"),
            (
                PartitionsFingerprint::multi([
                    ("region".to_string(), statics(&["eu", "us"])),
                    ("date".to_string(), window(daily())),
                ]),
                "multi (date: daily from 2026-01-01, region: static (2 keys))",
            ),
            (
                PartitionsFingerprint::Dynamic {
                    name: "customers".into(),
                },
                "dynamic 'customers'",
            ),
        ];
        for (fingerprint, description) in cases {
            assert_eq!(describe_partitions(Some(&fingerprint)), description);
        }
        assert_eq!(describe_partitions(None), "unpartitioned");
    }

    #[test]
    fn nodes_share_one_entry_per_definition() {
        let mut definitions = PartitionDefinitions::default();
        let a = definitions.intern(statics(&["x", "y"]));
        let b = definitions.intern(window(daily()));
        let c = definitions.intern(statics(&["y", "x"]));

        assert_eq!((a, b, c), (0, 1, 0));
        assert_eq!(definitions.definitions.len(), 2);
    }

    #[test]
    fn the_definitions_round_trip() {
        let mut definitions = PartitionDefinitions::default();
        let multi = PartitionsFingerprint::multi([
            ("region".to_string(), statics(&["eu"])),
            ("date".to_string(), window(daily())),
        ]);
        let index = definitions.intern(multi.clone());
        definitions.nodes.insert("store_days".into(), index);

        let json = serde_json::to_string(&definitions).unwrap();
        let back: PartitionDefinitions = serde_json::from_str(&json).unwrap();
        assert_eq!(back, definitions);
        assert_eq!(back.get("store_days"), Some(&multi));
        assert_eq!(back.get("summary"), None);
    }
}
