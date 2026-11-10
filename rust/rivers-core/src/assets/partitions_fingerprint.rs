//! A partitions definition as storage keeps it, and how a remote asset's
//! mirror compares with the owner's definition.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hasher;
use std::sync::{Mutex, PoisonError};

use jiff::civil;
use jiff::fmt::strtime;
use serde::{Deserialize, Serialize};
use siphasher::sip128::{Hasher128, SipHasher24};

use super::graph::GraphTopology;
use crate::storage::{CodeLocationContext, StorageBackend};
use crate::timegrid::TimeGrid;

/// A partitions definition in a form two code locations can compare.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionsFingerprint {
    /// The keys are stored apart, in a key row of the code location under
    /// `digest` (see [`KeyList`]).
    Static {
        count: u64,
        digest: String,
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
    /// `keys` in any order, with repeats.
    pub fn static_keys<'a>(keys: impl IntoIterator<Item = &'a str>) -> Self {
        KeyList::new(keys).fingerprint()
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

/// Static keys as a key row stores them: sorted, each once, under their
/// digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyList<'a> {
    pub digest: String,
    pub keys: Vec<&'a str>,
}

impl<'a> KeyList<'a> {
    /// `keys` in any order, with repeats.
    pub fn new(keys: impl IntoIterator<Item = &'a str>) -> Self {
        let mut keys: Vec<&str> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();
        Self {
            digest: digest(&keys),
            keys,
        }
    }

    pub fn fingerprint(&self) -> PartitionsFingerprint {
        PartitionsFingerprint::Static {
            count: self.keys.len() as u64,
            digest: self.digest.clone(),
        }
    }
}

/// 128-bit SipHash-2-4 with fixed keys over the sorted keys, each after its
/// length in bytes as a little-endian u64; lower-case hex. Code locations on
/// other builds and platforms compare these, so the bytes are spelled out.
fn digest(sorted: &[&str]) -> String {
    let mut hasher = SipHasher24::new_with_keys(0, 0);
    for key in sorted {
        hasher.write(&(key.len() as u64).to_le_bytes());
        hasher.write(key.as_bytes());
    }
    format!("{:032x}", hasher.finish128().as_u128())
}

/// `"daily from 2024-01-01"`: a time window names its first key, so a
/// changed key format shows.
impl fmt::Display for PartitionsFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static { count, .. } => {
                let noun = if *count == 1 { "key" } else { "keys" };
                write!(f, "static ({count} {noun})")
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

/// [`PartitionsFingerprint`]'s description, `"unpartitioned"` for `None`.
pub fn describe_partitions(fingerprint: Option<&PartitionsFingerprint>) -> String {
    fingerprint.map_or_else(|| "unpartitioned".to_string(), ToString::to_string)
}

/// How a remote asset's mirrored partitions definition compares with the
/// owner's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// The digests of a mirror's and an owner's static keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KeyListPair {
    mirror: String,
    owner: String,
}

/// What the fingerprints alone tell about a mirror.
#[derive(Debug, Clone, PartialEq)]
enum DriftCheck {
    Decided(PartitionDrift),
    /// The owner has more static keys than the mirror: `Compatible` when
    /// each mirror key of every pair is an owner key, else `Incompatible`.
    KeysNeeded(Vec<KeyListPair>),
}

/// How `mirror` compares with the owner's definition of `asset_key`, from
/// the owner's topology.
fn partition_drift(
    owner: &GraphTopology,
    asset_key: &str,
    mirror: Option<&PartitionsFingerprint>,
) -> DriftCheck {
    let Some(node) = owner.owned_node(asset_key) else {
        return DriftCheck::Decided(PartitionDrift::OwnerNodeMissing);
    };
    match (mirror, node.partitions.as_ref()) {
        (None, None) => DriftCheck::Decided(PartitionDrift::Same),
        (Some(mirror), Some(owner)) => compare(mirror, owner),
        _ => DriftCheck::Decided(PartitionDrift::Incompatible),
    }
}

fn compare(mirror: &PartitionsFingerprint, owner: &PartitionsFingerprint) -> DriftCheck {
    use PartitionsFingerprint::*;

    let decided = |compatible: bool| {
        DriftCheck::Decided(if compatible {
            PartitionDrift::Compatible
        } else {
            PartitionDrift::Incompatible
        })
    };

    if mirror.has_dynamic() || owner.has_dynamic() {
        return decided(false);
    }
    if mirror == owner {
        return DriftCheck::Decided(PartitionDrift::Same);
    }
    match (mirror, owner) {
        (
            Static {
                count: mirror_count,
                digest: mirror,
            },
            Static {
                count: owner_count,
                digest: owner,
            },
        ) => {
            if owner_count <= mirror_count {
                return decided(false);
            }
            DriftCheck::KeysNeeded(vec![KeyListPair {
                mirror: mirror.clone(),
                owner: owner.clone(),
            }])
        }
        (TimeWindow(mirror), TimeWindow(owner)) => decided(mirror.aligns_with(owner)),
        (Multi { dimensions: mirror }, Multi { dimensions: owner }) => {
            if mirror.len() != owner.len() {
                return decided(false);
            }
            let mut pairs = Vec::new();
            for ((m_name, m), (o_name, o)) in mirror.iter().zip(owner) {
                if m_name != o_name {
                    return decided(false);
                }
                match compare(m, o) {
                    DriftCheck::Decided(PartitionDrift::Incompatible) => return decided(false),
                    DriftCheck::Decided(_) => {}
                    DriftCheck::KeysNeeded(more) => pairs.extend(more),
                }
            }
            if pairs.is_empty() {
                decided(true)
            } else {
                DriftCheck::KeysNeeded(pairs)
            }
        }
        _ => decided(false),
    }
}

/// Answers kept before they are all dropped.
const MAX_KEY_ANSWERS: usize = 1_024;

/// Compares a mirror with its owner. The fingerprints decide, except when
/// the owner has more static keys: then the key rows decide, and the answer
/// of each pair of digests is kept, as the keys under a digest never change.
#[derive(Debug, Default)]
pub struct KeyComparisons {
    answers: Mutex<HashMap<KeyListPair, bool>>,
}

impl KeyComparisons {
    /// How `mirror`, a node of `consumer`, compares with the definition of
    /// `asset_key` in `owner_topology`.
    pub async fn drift<S: StorageBackend>(
        &self,
        storage: &S,
        consumer: &CodeLocationContext,
        owner: &CodeLocationContext,
        owner_topology: &GraphTopology,
        asset_key: &str,
        mirror: Option<&PartitionsFingerprint>,
    ) -> anyhow::Result<PartitionDrift> {
        let check = partition_drift(owner_topology, asset_key, mirror);
        self.settle(storage, consumer, owner, check).await
    }

    /// The drift `check` leads to. A pair not answered before reads the
    /// mirror's keys from the key rows of `consumer` and the owner's from
    /// those of `owner`. A missing key row gives `Incompatible`.
    async fn settle<S: StorageBackend>(
        &self,
        storage: &S,
        consumer: &CodeLocationContext,
        owner: &CodeLocationContext,
        check: DriftCheck,
    ) -> anyhow::Result<PartitionDrift> {
        let pairs = match check {
            DriftCheck::Decided(drift) => return Ok(drift),
            DriftCheck::KeysNeeded(pairs) => pairs,
        };
        for pair in pairs {
            if !self.contains(storage, consumer, owner, pair).await? {
                return Ok(PartitionDrift::Incompatible);
            }
        }
        Ok(PartitionDrift::Compatible)
    }

    /// Whether each key of the mirror's list is in the owner's.
    async fn contains<S: StorageBackend>(
        &self,
        storage: &S,
        consumer: &CodeLocationContext,
        owner: &CodeLocationContext,
        pair: KeyListPair,
    ) -> anyhow::Result<bool> {
        if let Some(&answer) = self.lock().get(&pair) {
            return Ok(answer);
        }

        let mirror_keys = storage
            .for_code_location(consumer)
            .get_partition_keys(&pair.mirror)
            .await?;
        let owner_keys = storage
            .for_code_location(owner)
            .get_partition_keys(&pair.owner)
            .await?;
        let (Some(mirror_keys), Some(owner_keys)) = (mirror_keys, owner_keys) else {
            tracing::warn!(
                target: "rivers::partitions",
                consumer = consumer.id(),
                mirror = %pair.mirror,
                owner_location = owner.id(),
                owner = %pair.owner,
                "a static key list is not stored; the mirror counts as incompatible"
            );
            return Ok(false);
        };

        let mut owned = owner_keys.iter();
        let answer = mirror_keys.iter().all(|key| owned.any(|k| k == key));
        self.remember(pair, answer);
        Ok(answer)
    }

    fn remember(&self, pair: KeyListPair, answer: bool) {
        let mut answers = self.lock();
        if answers.len() >= MAX_KEY_ANSWERS {
            answers.clear();
        }
        answers.insert(pair, answer);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<KeyListPair, bool>> {
        self.answers.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::assets::remote::{LocationRef, RemoteRef};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use jiff::civil::date;

    thread_local! {
        /// The keys of each digest [`statics`] made, for [`drift`].
        static KEYS: RefCell<HashMap<String, Vec<String>>> = RefCell::new(HashMap::new());
    }

    fn statics(keys: &[&str]) -> PartitionsFingerprint {
        let list = KeyList::new(keys.iter().copied());
        let owned = list.keys.iter().map(|k| k.to_string()).collect();
        KEYS.with(|known| known.borrow_mut().insert(list.digest.clone(), owned));
        list.fingerprint()
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

    fn owner_with(fingerprint: Option<PartitionsFingerprint>) -> GraphTopology {
        let mut node = TopologyNode::new("raw_orders", NodeKind::Asset);
        node.partitions = fingerprint;
        GraphTopology {
            nodes: vec![node],
            edges: vec![],
        }
    }

    /// The drift, with the keys of [`statics`] where the check needs them.
    fn drift(
        mirror: Option<PartitionsFingerprint>,
        owner: Option<PartitionsFingerprint>,
    ) -> PartitionDrift {
        let pairs = match partition_drift(&owner_with(owner), "raw_orders", mirror.as_ref()) {
            DriftCheck::Decided(drift) => return drift,
            DriftCheck::KeysNeeded(pairs) => pairs,
        };
        let contained = KEYS.with(|known| {
            let known = known.borrow();
            pairs.iter().all(|pair| {
                let owner = &known[&pair.owner];
                known[&pair.mirror].iter().all(|key| owner.contains(key))
            })
        });
        if contained {
            PartitionDrift::Compatible
        } else {
            PartitionDrift::Incompatible
        }
    }

    /// The check alone, without keys.
    fn check_of(
        mirror: Option<PartitionsFingerprint>,
        owner: Option<PartitionsFingerprint>,
    ) -> DriftCheck {
        partition_drift(&owner_with(owner), "raw_orders", mirror.as_ref())
    }

    fn pair(mirror: &PartitionsFingerprint, owner: &PartitionsFingerprint) -> KeyListPair {
        let digest = |fingerprint: &PartitionsFingerprint| match fingerprint {
            PartitionsFingerprint::Static { digest, .. } => digest.clone(),
            other => panic!("not static: {other:?}"),
        };
        KeyListPair {
            mirror: digest(mirror),
            owner: digest(owner),
        }
    }

    /// One key row of `identity` per list of `keys`, and no other.
    async fn key_rows(storage: &SurrealStorage, identity: &str, keys: &[&[&str]]) {
        let key_lists: Vec<KeyList> = keys
            .iter()
            .map(|keys| KeyList::new(keys.iter().copied()))
            .collect();
        storage
            .for_code_location(&CodeLocationContext::new(identity))
            .set_graph_topology(&GraphTopology::default(), &key_lists)
            .await
            .unwrap();
    }

    async fn settle(
        comparisons: &KeyComparisons,
        storage: &SurrealStorage,
        check: DriftCheck,
    ) -> PartitionDrift {
        let consumer = CodeLocationContext::new("analytics");
        let owner = CodeLocationContext::new("warehouse");
        comparisons
            .settle(storage, &consumer, &owner, check)
            .await
            .unwrap()
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
        let mut owner = owner_with(None);
        assert_eq!(
            partition_drift(&owner, "elsewhere", None),
            DriftCheck::Decided(PartitionDrift::OwnerNodeMissing)
        );

        owner.nodes[0].remote = Some(RemoteRef {
            code_location: LocationRef::new("dev", "lake").unwrap(),
            asset_key: "raw_orders".into(),
        });
        assert_eq!(
            partition_drift(&owner, "raw_orders", None),
            DriftCheck::Decided(PartitionDrift::OwnerNodeMissing)
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
    fn the_digest_ignores_order_and_repeats() {
        let digest = |keys: &[&str]| KeyList::new(keys.iter().copied()).digest;

        assert_eq!(digest(&["us", "eu", "us"]), digest(&["eu", "us"]));
        assert_eq!(KeyList::new(["us", "eu", "us"]).keys, ["eu", "us"]);
        assert_eq!(
            digest(&["eu", "us"]),
            "7466a968ad7667ffa346774108fbcf30",
            "a changed digest makes every stored key row unreachable"
        );
        assert_eq!(digest(&[]), "f4f2ced447ab02427de0a38047d74950");
        assert_ne!(
            digest(&["ab", "c"]),
            digest(&["a", "bc"]),
            "lengths separate keys"
        );
        assert_ne!(digest(&["eu"]), digest(&["eu", "us"]));
    }

    #[test]
    fn a_static_fingerprint_holds_the_count_and_digest() {
        let fingerprint = statics(&["us", "eu"]);
        assert_eq!(
            serde_json::to_value(&fingerprint).unwrap(),
            serde_json::json!({
                "kind": "static",
                "count": 2,
                "digest": KeyList::new(["eu", "us"]).digest,
            })
        );
        let back: PartitionsFingerprint =
            serde_json::from_value(serde_json::to_value(&fingerprint).unwrap()).unwrap();
        assert_eq!(back, fingerprint);
    }

    /// Equal definitions need no key row: none exists here.
    #[tokio::test]
    async fn equal_definitions_are_the_same_without_key_rows() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let comparisons = KeyComparisons::default();
        let multi = || {
            PartitionsFingerprint::multi([
                ("region".to_string(), statics(&["eu", "us"])),
                ("date".to_string(), window(daily())),
            ])
        };

        for (mirror, owner) in [
            (statics(&["eu", "us"]), statics(&["us", "eu"])),
            (window(daily()), window(daily())),
            (multi(), multi()),
        ] {
            let check = check_of(Some(mirror), Some(owner));
            assert_eq!(check, DriftCheck::Decided(PartitionDrift::Same));
            assert_eq!(
                settle(&comparisons, &storage, check).await,
                PartitionDrift::Same
            );
        }
    }

    /// A different set of the same or a smaller size cannot hold the mirror:
    /// the counts decide, with no key row stored.
    #[tokio::test]
    async fn a_different_set_of_no_more_keys_is_incompatible_without_key_rows() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let comparisons = KeyComparisons::default();
        let multi = |regions: &[&str]| {
            PartitionsFingerprint::multi([
                ("region".to_string(), statics(regions)),
                ("date".to_string(), window(daily())),
            ])
        };

        for (mirror, owner) in [
            (statics(&["eu", "us"]), statics(&["eu", "uk"])),
            (statics(&["eu", "us"]), statics(&["eu"])),
            (multi(&["eu", "us"]), multi(&["us"])),
        ] {
            let check = check_of(Some(mirror), Some(owner));
            assert_eq!(check, DriftCheck::Decided(PartitionDrift::Incompatible));
            assert_eq!(
                settle(&comparisons, &storage, check).await,
                PartitionDrift::Incompatible
            );
        }
    }

    /// With more owner keys, the key rows of both code locations decide.
    #[tokio::test]
    async fn an_owner_with_more_keys_is_decided_by_the_key_rows() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let mirror = statics(&["eu", "us"]);
        let added = statics(&["apac", "eu", "us"]);
        let replaced = statics(&["apac", "eu", "uk"]);
        let unstored = statics(&["eu", "uk", "us"]);
        key_rows(&storage, "analytics", &[&["eu", "us"]]).await;
        key_rows(
            &storage,
            "warehouse",
            &[&["apac", "eu", "us"], &["apac", "eu", "uk"]],
        )
        .await;

        let cases = [
            (&mirror, &added, PartitionDrift::Compatible),
            (&mirror, &replaced, PartitionDrift::Incompatible),
            (&mirror, &unstored, PartitionDrift::Incompatible),
        ];
        for (mirror, owner, expected) in cases {
            let check = check_of(Some(mirror.clone()), Some(owner.clone()));
            assert_eq!(check, DriftCheck::KeysNeeded(vec![pair(mirror, owner)]));
            let comparisons = KeyComparisons::default();
            assert_eq!(
                settle(&comparisons, &storage, check).await,
                expected,
                "{owner}"
            );
        }

        let missing_mirror = statics(&["eu"]);
        let check = check_of(Some(missing_mirror), Some(added.clone()));
        assert_eq!(
            settle(&KeyComparisons::default(), &storage, check).await,
            PartitionDrift::Incompatible,
            "the mirror's key row is missing"
        );

        let multi = |regions: &PartitionsFingerprint| {
            PartitionsFingerprint::multi([
                ("region".to_string(), regions.clone()),
                ("date".to_string(), window(daily())),
            ])
        };
        let check = check_of(Some(multi(&mirror)), Some(multi(&added)));
        assert_eq!(check, DriftCheck::KeysNeeded(vec![pair(&mirror, &added)]));
        assert_eq!(
            settle(&KeyComparisons::default(), &storage, check).await,
            PartitionDrift::Compatible
        );
    }

    /// The answer for a pair of digests is kept: the second check reads no
    /// key row, here because none is left.
    #[tokio::test]
    async fn a_compared_pair_is_not_read_again() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let mirror = statics(&["eu", "us"]);
        let owner = statics(&["apac", "eu", "us"]);
        key_rows(&storage, "analytics", &[&["eu", "us"]]).await;
        key_rows(&storage, "warehouse", &[&["apac", "eu", "us"]]).await;
        let comparisons = KeyComparisons::default();
        let check = || check_of(Some(mirror.clone()), Some(owner.clone()));
        assert_eq!(
            settle(&comparisons, &storage, check()).await,
            PartitionDrift::Compatible
        );

        key_rows(&storage, "analytics", &[]).await;
        key_rows(&storage, "warehouse", &[]).await;
        assert_eq!(
            settle(&comparisons, &storage, check()).await,
            PartitionDrift::Compatible
        );
        assert_eq!(
            settle(&KeyComparisons::default(), &storage, check()).await,
            PartitionDrift::Incompatible,
            "the key rows are gone"
        );
    }

    #[test]
    fn the_answers_are_dropped_at_the_bound() {
        let comparisons = KeyComparisons::default();
        let pair = |i: usize| KeyListPair {
            mirror: format!("m{i}"),
            owner: format!("o{i}"),
        };
        for i in 0..MAX_KEY_ANSWERS {
            comparisons.remember(pair(i), i % 2 == 0);
        }
        assert_eq!(comparisons.lock().len(), MAX_KEY_ANSWERS);
        assert_eq!(comparisons.lock().get(&pair(2)), Some(&true));

        comparisons.remember(pair(MAX_KEY_ANSWERS), false);
        let answers = comparisons.lock();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers.get(&pair(MAX_KEY_ANSWERS)), Some(&false));
    }
}
