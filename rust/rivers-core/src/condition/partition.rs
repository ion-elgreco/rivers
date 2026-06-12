//! Partition-aware condition types.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::storage::PartitionKey;

/// The result of evaluating a condition — which partitions satisfy it.
///
/// For unpartitioned assets, this degrades to a boolean (`All` or `Empty`).
/// For partitioned assets, it carries a concrete set of partition keys.
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
                    Self::All
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

    /// Set difference (self - other) — takes the universe of all valid keys
    /// so `All - Keys` resolves to the concrete complement (mirrors
    /// [`Self::complement`]).
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
    /// `All` returns `total` (the full universe size), `Empty` returns 0,
    /// `Keys` returns the set length.
    pub fn key_count(&self, total: usize) -> usize {
        match self {
            Self::All => total,
            Self::Empty => 0,
            Self::Keys(keys) => keys.len(),
        }
    }

    /// Convert to bool (for unpartitioned assets).
    /// `Keys` with any entries = true, empty = false.
    pub fn to_bool(&self) -> bool {
        !self.is_empty()
    }
}

/// Serializable partition mapping — the subset of PartitionMapping
/// that rivers-core needs (no PyO3 dependency).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionMappingKind {
    Identity,
    AllPartitions,
    Static {
        mapping: HashMap<String, String>,
    },
    TimeWindow {
        offset: i64,
        /// The upstream definition's time grid, captured at conversion so
        /// eval can shift keys the same way the runtime does. `None` for
        /// mappings serialized before the grid existed — degrades to
        /// pass-through.
        #[serde(default)]
        grid: Option<crate::timegrid::TimeGrid>,
    },
    SpecificPartitions {
        keys: Vec<String>,
    },
    /// Multi-dimension mapping: maps each dimension independently.
    /// Key = upstream dimension name, value = (downstream dimension name, per-dimension mapping).
    Multi {
        dimension_mappings: HashMap<String, (String, Box<PartitionMappingKind>)>,
    },
    /// Maps one dimension of a multi-partitioned asset to/from a single-dimension asset.
    /// `dimension_name` identifies which dimension to extract/inject.
    /// `inner` controls how that dimension maps (defaults to Identity).
    MultiToSingle {
        dimension_name: String,
        inner: Box<PartitionMappingKind>,
    },
    /// Maps an unpartitioned upstream to specific downstream partition keys.
    /// Only the matching downstream keys load the upstream; others skip.
    ForKeys,
    /// Subset mapping: upstream has a subset of downstream's partition keys.
    /// Non-matching downstream keys skip the upstream.
    Subset,
}

/// Shift every single-dimension key in a selection by `offset` grid windows.
/// Keys whose shift falls outside the grid's `[start, end)` have no
/// counterpart partition and are dropped. Without a grid (mappings
/// serialized before it existed) the selection passes through unchanged.
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
    /// Map upstream partition keys to downstream partition keys.
    pub fn map_to_downstream(&self, upstream_keys: &PartitionSelection) -> PartitionSelection {
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
                    // Reverse lookup: mapping is downstream → upstream.
                    let mapped: HashSet<PartitionKey> = mapping
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
                    if mapped.is_empty() {
                        PartitionSelection::Empty
                    } else {
                        PartitionSelection::Keys(mapped)
                    }
                }
            },
            // Downstream key K reads upstream K+offset, so the downstream
            // that reads upstream U is U-offset.
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
                    let map_key_downstream = |pk: &PartitionKey| -> Option<PartitionKey> {
                        let dims = match pk {
                            PartitionKey::Multi { dims } => dims,
                            _ => return None,
                        };
                        let upstream_dims: HashMap<&str, &[String]> = dims
                            .iter()
                            .map(|(d, v)| (d.as_str(), v.as_slice()))
                            .collect();
                        let mut downstream_dims = Vec::new();
                        for (upstream_dim, (downstream_dim, per_dim_mapping)) in dimension_mappings
                        {
                            let val = upstream_dims.get(upstream_dim.as_str())?;
                            let single_key = PartitionKey::Single { keys: val.to_vec() };
                            let mapped = per_dim_mapping.map_to_downstream(
                                &PartitionSelection::Keys(std::iter::once(single_key).collect()),
                            );
                            match mapped {
                                PartitionSelection::Keys(ks) => {
                                    let first = ks.into_iter().next()?;
                                    if let PartitionKey::Single { keys: kv } = first {
                                        downstream_dims.push((downstream_dim.clone(), kv));
                                    } else {
                                        return None;
                                    }
                                }
                                _ => return None,
                            }
                        }
                        Some(PartitionKey::Multi {
                            dims: downstream_dims,
                        })
                    };
                    let mapped: HashSet<PartitionKey> =
                        keys.iter().filter_map(map_key_downstream).collect();
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
                    let mapped: HashSet<PartitionKey> = keys
                        .iter()
                        .filter_map(|k| {
                            if let PartitionKey::Multi { dims } = k {
                                dims.iter()
                                    .find(|(d, _)| d == dimension_name)
                                    .map(|(_, v)| PartitionKey::Single { keys: v.clone() })
                            } else {
                                None
                            }
                        })
                        .flat_map(|dim_val| {
                            let sel = PartitionSelection::Keys(std::iter::once(dim_val).collect());
                            match inner.map_to_downstream(&sel) {
                                PartitionSelection::Keys(ks) => ks.into_iter().collect::<Vec<_>>(),
                                PartitionSelection::All => vec![],
                                PartitionSelection::Empty => vec![],
                            }
                        })
                        .collect();
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

/// Resolves partition key mappings between connected assets. Borrows the
/// long-lived maps — constructed per asset per tick on the hot loop.
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

    /// A resolver with no mappings and no upstream keys — identity for
    /// every edge. Used where a context is built without dep traversal.
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

    /// Map upstream partition keys to downstream partition keys.
    pub fn map_downstream(
        &self,
        upstream_asset: &str,
        downstream_asset: &str,
        upstream_keys: &PartitionSelection,
    ) -> PartitionSelection {
        let key = (downstream_asset.to_string(), upstream_asset.to_string());
        let mapping = match self.mappings.get(&key) {
            Some(m) => m,
            None => return upstream_keys.clone(), // no mapping = identity
        };
        mapping.map_to_downstream(upstream_keys)
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
/// Borrows from the cache and condition info — no cloning per tick.
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
    /// Partition keys in the latest time window (for `InLatestTimeWindow` condition).
    /// `None` for non-time-windowed partitions (treated as all keys).
    pub latest_time_window_keys: Option<&'a HashSet<PartitionKey>>,
    /// Per-asset partition status for ALL assets in the cache (not just this asset).
    /// Used by `eval_partitioned_on_dep` to look up upstream deps' actual
    /// materialized/in-progress/failed partition sets instead of heuristics.
    pub all_partition_statuses: &'a HashMap<String, crate::condition::cache::PartitionStatusEntry>,
    /// Staleness floor for `NewlyUpdated` in a dep pivot, keyed by UPSTREAM
    /// key: the root's materialization state of the downstream key(s) that
    /// upstream key maps to. Absent key = no counterpart in the downstream
    /// universe (never counts as updated); `None` = a mapped downstream key
    /// exists but was never materialized (counts as updated). `None` for the
    /// whole field outside dep pivots — the self path keeps baselines.
    pub dep_root_floor: Option<&'a HashMap<PartitionKey, Option<i64>>>,
}

/// Per-partition condition evaluation state, persisted across ticks.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionState {
    /// Per-node partition selections from previous tick.
    pub previous_selections: HashMap<u32, PartitionSelection>,
    /// Per-partition materialization timestamps from previous tick.
    pub timestamps: HashMap<PartitionKey, i64>,
    /// Partitions that have been handled (materialization triggered) since last reset.
    pub handled: HashSet<PartitionKey>,
}
