//! Partition-aware condition types.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::storage::PartitionKey;

/// The result of evaluating a condition — which partitions satisfy it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionSelection {
    /// All partitions (or "true" for unpartitioned assets).
    All,
    /// No partitions (or "false" for unpartitioned assets).
    Empty,
    /// Specific partition keys that satisfy the condition.
    Keys(HashSet<PartitionKey>),
}

impl PartitionSelection {
    /// Set union (OR).
    pub fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::Empty, x) | (x, Self::Empty) => x.clone(),
            (Self::Keys(a), Self::Keys(b)) => {
                let merged: HashSet<PartitionKey> = a.union(b).cloned().collect();
                if merged.is_empty() {
                    Self::Empty
                } else {
                    Self::Keys(merged)
                }
            }
        }
    }

    /// Set intersection (AND).
    pub fn intersect(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::All, x) | (x, Self::All) => x.clone(),
            (Self::Keys(a), Self::Keys(b)) => {
                let common: HashSet<PartitionKey> = a.intersection(b).cloned().collect();
                if common.is_empty() {
                    Self::Empty
                } else {
                    Self::Keys(common)
                }
            }
        }
    }

    /// Set complement (NOT) — requires the universe of all valid keys.
    pub fn complement(&self, all_keys: &HashSet<PartitionKey>) -> Self {
        match self {
            Self::All => Self::Empty,
            Self::Empty => {
                if all_keys.is_empty() {
                    Self::Empty
                } else {
                    Self::Keys(all_keys.clone())
                }
            }
            Self::Keys(keys) => {
                let diff: HashSet<PartitionKey> = all_keys.difference(keys).cloned().collect();
                if diff.is_empty() {
                    Self::Empty
                } else {
                    Self::Keys(diff)
                }
            }
        }
    }

    /// Set difference (self - other).
    pub fn difference(&self, other: &Self, all_keys: &HashSet<PartitionKey>) -> Self {
        match (self, other) {
            (Self::Empty, _) => Self::Empty,
            (_, Self::All) => Self::Empty,
            (x, Self::Empty) => x.clone(),
            (Self::All, Self::Keys(_)) => other.complement(all_keys),
            (Self::Keys(a), Self::Keys(b)) => {
                let diff: HashSet<PartitionKey> = a.difference(b).cloned().collect();
                if diff.is_empty() {
                    Self::Empty
                } else {
                    Self::Keys(diff)
                }
            }
        }
    }

    /// True if this selection contains zero partitions.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Keys(keys) => keys.is_empty(),
            Self::All => false,
        }
    }

    /// True if this selection covers all partitions.
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Convert a bool to a PartitionSelection (for unpartitioned assets).
    pub fn from_bool(val: bool) -> Self {
        if val { Self::All } else { Self::Empty }
    }

    /// Number of partition keys in this selection.
    pub fn key_count(&self, total: usize) -> usize {
        match self {
            Self::All => total,
            Self::Empty => 0,
            Self::Keys(keys) => keys.len(),
        }
    }

    /// Convert to bool (for unpartitioned assets).
    pub fn to_bool(&self) -> bool {
        !self.is_empty()
    }
}

/// Serializable partition mapping for rivers-core (no PyO3 dependency).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionMappingKind {
    Identity,
    AllPartitions,
    Static {
        mapping: HashMap<String, String>,
    },
    TimeWindow {
        offset: i64,
        /// The upstream definition's time grid, captured at conversion.
        #[serde(default)]
        grid: Option<crate::timegrid::TimeGrid>,
    },
    SpecificPartitions {
        keys: Vec<String>,
    },
    /// Multi-dimension mapping: maps each dimension independently.
    Multi {
        dimension_mappings: HashMap<String, (String, Box<PartitionMappingKind>)>,
    },
    /// Maps one dimension of a multi-partitioned asset to/from a single-dimension asset.
    MultiToSingle {
        dimension_name: String,
        inner: Box<PartitionMappingKind>,
    },
    /// Maps an unpartitioned upstream to specific downstream partition keys.
    ForKeys,
    /// Subset mapping: upstream has a subset of downstream's partition keys.
    Subset,
}

/// Shift every single-dimension key in a selection by `offset` grid windows.
fn shift_selection(
    sel: &PartitionSelection,
    offset: i64,
    grid: Option<&crate::timegrid::TimeGrid>,
) -> PartitionSelection {
    let Some(grid) = grid else {
        return sel.clone();
    };
    if offset == 0 {
        return sel.clone();
    }
    match sel {
        PartitionSelection::All | PartitionSelection::Empty => sel.clone(),
        PartitionSelection::Keys(keys) => {
            let shifted: HashSet<PartitionKey> = keys
                .iter()
                .filter_map(|k| match k {
                    PartitionKey::Single { keys } if keys.len() == 1 => grid
                        .shift_key(&keys[0], offset)
                        .ok()
                        .map(|s| PartitionKey::Single { keys: vec![s] }),
                    _ => None,
                })
                .collect();
            if shifted.is_empty() {
                PartitionSelection::Empty
            } else {
                PartitionSelection::Keys(shifted)
            }
        }
    }
}

impl PartitionMappingKind {
    /// Map upstream partition keys to downstream partition keys, without a
    /// downstream universe: dimension fan-outs over-approximate to `All`.
    pub fn map_to_downstream(&self, upstream_keys: &PartitionSelection) -> PartitionSelection {
        self.map_to_downstream_in(upstream_keys, None)
    }

    /// Map upstream partition keys to downstream partition keys. With a
    /// downstream universe, a Multi mapping whose dimensions partially fan out
    /// expands precisely against it instead of escalating to `All`.
    pub fn map_to_downstream_in(
        &self,
        upstream_keys: &PartitionSelection,
        downstream_universe: Option<&HashSet<PartitionKey>>,
    ) -> PartitionSelection {
        match self {
            Self::Identity => upstream_keys.clone(),
            Self::AllPartitions => {
                if upstream_keys.is_empty() {
                    PartitionSelection::Empty
                } else {
                    PartitionSelection::All
                }
            }
            Self::Static { mapping } => match upstream_keys {
                PartitionSelection::Empty => PartitionSelection::Empty,
                PartitionSelection::All => PartitionSelection::All,
                PartitionSelection::Keys(keys) => {
                    let mut mapped: HashSet<PartitionKey> = mapping
                        .iter()
                        .filter(|(_, v)| {
                            keys.contains(&PartitionKey::Single {
                                keys: vec![(*v).clone()],
                            })
                        })
                        .map(|(k, _)| PartitionKey::Single {
                            keys: vec![k.clone()],
                        })
                        .collect();
                    for uk in keys {
                        if let PartitionKey::Single { keys: parts } = uk
                            && parts.len() == 1
                            && !mapping.contains_key(&parts[0])
                        {
                            mapped.insert(uk.clone());
                        }
                    }
                    if mapped.is_empty() {
                        PartitionSelection::Empty
                    } else {
                        PartitionSelection::Keys(mapped)
                    }
                }
            },
            Self::TimeWindow { offset, grid } => {
                shift_selection(upstream_keys, -*offset, grid.as_ref())
            }
            Self::SpecificPartitions { .. } => {
                if upstream_keys.is_empty() {
                    PartitionSelection::Empty
                } else {
                    PartitionSelection::All
                }
            }
            Self::Multi { dimension_mappings } => match upstream_keys {
                PartitionSelection::Empty => PartitionSelection::Empty,
                PartitionSelection::All => PartitionSelection::All,
                PartitionSelection::Keys(keys) => {
                    // Per-combo dim constraint; `None` = the per-dim mapping
                    // fanned out to All (expanded against the universe below).
                    type Combo = Vec<(String, Option<Vec<String>>)>;
                    let map_key_downstream = |pk: &PartitionKey| -> Option<Vec<Combo>> {
                        let dims = match pk {
                            PartitionKey::Multi { dims } => dims,
                            _ => return None,
                        };
                        let upstream_dims: HashMap<&str, &[String]> = dims
                            .iter()
                            .map(|(d, v)| (d.as_str(), v.as_slice()))
                            .collect();
                        let mut combos: Vec<Combo> = vec![Vec::new()];
                        for (upstream_dim, (downstream_dim, per_dim_mapping)) in dimension_mappings
                        {
                            let val = upstream_dims.get(upstream_dim.as_str())?;
                            let single_key = PartitionKey::Single { keys: val.to_vec() };
                            let mapped = per_dim_mapping.map_to_downstream(
                                &PartitionSelection::Keys(std::iter::once(single_key).collect()),
                            );
                            match mapped {
                                PartitionSelection::Keys(ks) => {
                                    let mut values: Vec<Vec<String>> = Vec::new();
                                    for k in ks {
                                        if let PartitionKey::Single { keys: kv } = k {
                                            values.push(kv);
                                        } else {
                                            return None;
                                        }
                                    }
                                    if values.is_empty() {
                                        return None;
                                    }
                                    if let [v] = values.as_slice() {
                                        for combo in &mut combos {
                                            combo.push((downstream_dim.clone(), Some(v.clone())));
                                        }
                                    } else {
                                        let mut next =
                                            Vec::with_capacity(combos.len() * values.len());
                                        for combo in &combos {
                                            for v in &values {
                                                let mut c = combo.clone();
                                                c.push((
                                                    downstream_dim.clone(),
                                                    Some(v.clone()),
                                                ));
                                                next.push(c);
                                            }
                                        }
                                        combos = next;
                                    }
                                }
                                PartitionSelection::All => {
                                    for combo in &mut combos {
                                        combo.push((downstream_dim.clone(), None));
                                    }
                                }
                                PartitionSelection::Empty => return None,
                            }
                        }
                        Some(combos)
                    };
                    let mut mapped: HashSet<PartitionKey> = HashSet::new();
                    let mut wildcards: Vec<Combo> = Vec::new();
                    for pk in keys {
                        let Some(combos) = map_key_downstream(pk) else {
                            continue;
                        };
                        for combo in combos {
                            if combo.iter().all(|(_, v)| v.is_some()) {
                                mapped.insert(PartitionKey::Multi {
                                    dims: combo
                                        .into_iter()
                                        .map(|(d, v)| (d, v.expect("checked all Some")))
                                        .collect(),
                                });
                            } else {
                                wildcards.push(combo);
                            }
                        }
                    }
                    if !wildcards.is_empty() {
                        let Some(universe) = downstream_universe else {
                            // No universe to expand against — over-approximate
                            // rather than drop the key.
                            return PartitionSelection::All;
                        };
                        for key in universe {
                            let PartitionKey::Multi { dims } = key else {
                                continue;
                            };
                            let matches = wildcards.iter().any(|combo| {
                                combo.iter().all(|(d, want)| match want {
                                    None => true,
                                    Some(v) => dims.iter().any(|(kd, kv)| kd == d && kv == v),
                                })
                            });
                            if matches {
                                mapped.insert(key.clone());
                            }
                        }
                    }
                    if mapped.is_empty() {
                        PartitionSelection::Empty
                    } else {
                        PartitionSelection::Keys(mapped)
                    }
                }
            },
            Self::MultiToSingle {
                dimension_name,
                inner,
            } => match upstream_keys {
                PartitionSelection::Empty => PartitionSelection::Empty,
                PartitionSelection::All => PartitionSelection::All,
                PartitionSelection::Keys(keys) => {
                    let mut mapped: HashSet<PartitionKey> = HashSet::new();
                    // Multi upstream → Single downstream: the named dimension's
                    // value feeds `inner`, landing in the downstream key space.
                    let dim_vals = keys.iter().filter_map(|k| {
                        if let PartitionKey::Multi { dims } = k {
                            dims.iter()
                                .find(|(d, _)| d == dimension_name)
                                .map(|(_, v)| PartitionKey::Single { keys: v.clone() })
                        } else {
                            None
                        }
                    });
                    for dim_val in dim_vals {
                        let sel = PartitionSelection::Keys(std::iter::once(dim_val).collect());
                        match inner.map_to_downstream(&sel) {
                            PartitionSelection::Keys(ks) => mapped.extend(ks),
                            PartitionSelection::All => return PartitionSelection::All,
                            PartitionSelection::Empty => {}
                        }
                    }
                    // Single upstream → Multi downstream: the upstream key
                    // constrains the named dimension; the remaining dimensions
                    // expand against the downstream universe (over-approximate
                    // without one, mirroring the `Multi` arm's wildcards).
                    let mut wanted: HashSet<Vec<String>> = HashSet::new();
                    for uk in keys {
                        if !matches!(uk, PartitionKey::Single { .. }) {
                            continue;
                        }
                        let sel = PartitionSelection::Keys(std::iter::once(uk.clone()).collect());
                        match inner.map_to_downstream(&sel) {
                            PartitionSelection::Keys(ks) => {
                                for k in ks {
                                    if let PartitionKey::Single { keys: vals } = k {
                                        wanted.insert(vals);
                                    }
                                }
                            }
                            PartitionSelection::All => return PartitionSelection::All,
                            PartitionSelection::Empty => {}
                        }
                    }
                    if !wanted.is_empty() {
                        let Some(universe) = downstream_universe else {
                            return PartitionSelection::All;
                        };
                        for key in universe {
                            let PartitionKey::Multi { dims } = key else {
                                continue;
                            };
                            if dims
                                .iter()
                                .any(|(d, v)| d == dimension_name && wanted.contains(v))
                            {
                                mapped.insert(key.clone());
                            }
                        }
                    }
                    if mapped.is_empty() {
                        PartitionSelection::Empty
                    } else {
                        PartitionSelection::Keys(mapped)
                    }
                }
            },
            Self::ForKeys => {
                if upstream_keys.is_empty() {
                    PartitionSelection::Empty
                } else {
                    PartitionSelection::All
                }
            }
            Self::Subset => upstream_keys.clone(),
        }
    }
}

/// Resolves partition key mappings between connected assets.
pub struct PartitionResolver<'a> {
    /// Per-edge mapping kind. Key = (downstream_asset, upstream_asset).
    mappings: &'a HashMap<(String, String), PartitionMappingKind>,
    /// Upstream asset → its valid partition keys.
    pub(crate) upstream_partition_keys: &'a HashMap<String, HashSet<PartitionKey>>,
}

impl<'a> PartitionResolver<'a> {
    pub fn new(
        mappings: &'a HashMap<(String, String), PartitionMappingKind>,
        upstream_partition_keys: &'a HashMap<String, HashSet<PartitionKey>>,
    ) -> Self {
        Self {
            mappings,
            upstream_partition_keys,
        }
    }

    /// A resolver with no mappings and no upstream keys — identity for every edge.
    pub fn empty() -> PartitionResolver<'static> {
        static EMPTY_MAPPINGS: std::sync::OnceLock<
            HashMap<(String, String), PartitionMappingKind>,
        > = std::sync::OnceLock::new();
        static EMPTY_KEYS: std::sync::OnceLock<HashMap<String, HashSet<PartitionKey>>> =
            std::sync::OnceLock::new();
        PartitionResolver {
            mappings: EMPTY_MAPPINGS.get_or_init(HashMap::new),
            upstream_partition_keys: EMPTY_KEYS.get_or_init(HashMap::new),
        }
    }

    /// Map upstream partition keys to downstream partition keys, expanding
    /// dimension fan-outs against the downstream universe when given.
    pub fn map_downstream(
        &self,
        upstream_asset: &str,
        downstream_asset: &str,
        upstream_keys: &PartitionSelection,
        downstream_universe: Option<&HashSet<PartitionKey>>,
    ) -> PartitionSelection {
        let key = (downstream_asset.to_string(), upstream_asset.to_string());
        let mapping = match self.mappings.get(&key) {
            Some(m) => m,
            None => return upstream_keys.clone(),
        };
        mapping.map_to_downstream_in(upstream_keys, downstream_universe)
    }

    /// The mapping kind for an edge, if one was declared (absent = Identity).
    pub(crate) fn mapping_kind(
        &self,
        upstream_asset: &str,
        downstream_asset: &str,
    ) -> Option<&PartitionMappingKind> {
        self.mappings
            .get(&(downstream_asset.to_string(), upstream_asset.to_string()))
    }
}

/// All partition-level data needed during condition evaluation.
pub struct PartitionEvalContext<'a> {
    /// All valid partition keys for this asset.
    pub all_keys: &'a HashSet<PartitionKey>,
    /// Which partitions have been materialized at least once.
    pub materialized: &'a HashSet<PartitionKey>,
    /// Which partitions are currently being materialized.
    pub in_progress: &'a HashSet<PartitionKey>,
    /// Which partitions failed in latest execution.
    pub failed: &'a HashSet<PartitionKey>,
    /// Per-partition last materialization timestamp.
    pub timestamps: &'a HashMap<PartitionKey, i64>,
    /// Partition mapping resolver for upstream deps.
    pub resolver: PartitionResolver<'a>,
    /// Latest-time-window resolver for `InLatestTimeWindow`; `None` means no
    /// window information is available and the filter selects nothing.
    pub time_windows: Option<&'a crate::condition::pass::TimeWindowResolver<'a>>,
    /// Per-asset partition status for ALL assets in the cache (not just this asset).
    pub all_partition_statuses: &'a HashMap<String, crate::condition::cache::PartitionStatusEntry>,
    /// Staleness floor for `NewlyUpdated` in a dep pivot, keyed by upstream key.
    pub dep_root_floor: Option<&'a HashMap<PartitionKey, Option<i64>>>,
}

/// Serde adapter for a `PartitionKey`-keyed map (persisted as `(key, value)` pairs).
mod partition_key_i64_map {
    use super::PartitionKey;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S: Serializer>(
        map: &HashMap<PartitionKey, i64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<PartitionKey, i64>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MapOrSeq {
            Seq(Vec<(PartitionKey, i64)>),
            #[allow(dead_code)]
            LegacyMap(HashMap<String, serde::de::IgnoredAny>),
        }
        Ok(match MapOrSeq::deserialize(deserializer)? {
            MapOrSeq::Seq(pairs) => pairs.into_iter().collect(),
            MapOrSeq::LegacyMap(_) => HashMap::new(),
        })
    }
}

/// Per-partition condition evaluation state, persisted across ticks.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionState {
    /// Per-node partition selections from previous tick.
    pub previous_selections: HashMap<u32, PartitionSelection>,
    /// Per-partition materialization timestamps from previous tick.
    #[serde(with = "partition_key_i64_map")]
    pub timestamps: HashMap<PartitionKey, i64>,
    /// Partitions that have been handled (materialization triggered) since last reset.
    pub handled: HashSet<PartitionKey>,
    /// Previous tick's selections for stateful operators evaluated inside a dep-aggregate.
    #[serde(default)]
    pub dep_previous_selections: HashMap<String, HashMap<u32, PartitionSelection>>,
}
