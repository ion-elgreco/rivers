//! Partition-universe shapes and their per-tick advancement.
use std::collections::{HashMap, HashSet};

use ordermap::OrderSet;

use crate::storage::PartitionKey;
use crate::timegrid::TimeGrid;
use chrono::NaiveDateTime;

/// How an asset's partition universe evolves after extraction.
#[derive(Clone, Debug)]
pub enum PartitionUniverse {
    /// Fixed key set (Static definitions).
    Frozen,
    /// Window starts enter the universe as wall-clock time passes.
    TimeWindow {
        grid: TimeGrid,
        enumerated_to: NaiveDateTime,
    },
    /// Storage-managed: the key set mirrors the `dynamic_partitions` namespace.
    Dynamic { namespace: String },
    /// Cartesian product over dimensions; recomputed when any dimension's key list changes.
    Multi {
        dims: Vec<(String, DimensionUniverse)>,
    },
}

/// One dimension of a Multi universe: its current key list plus how it evolves.
#[derive(Clone, Debug)]
pub struct DimensionUniverse {
    /// Dim values in definition/seed order.
    pub keys: OrderSet<String>,
    pub kind: DimensionKind,
}

#[derive(Clone, Debug)]
pub enum DimensionKind {
    Frozen,
    TimeWindow {
        grid: TimeGrid,
        enumerated_to: NaiveDateTime,
    },
    Dynamic {
        namespace: String,
    },
}

/// Advance one universe, mutating `all_keys` in place. Returns whether the key set changed.
pub(crate) fn refresh_universe(
    universe: &mut PartitionUniverse,
    all_keys: &mut HashSet<PartitionKey>,
    now: NaiveDateTime,
    dynamic_keys: &HashMap<String, HashSet<String>>,
) -> bool {
    match universe {
        PartitionUniverse::Frozen => false,
        PartitionUniverse::TimeWindow {
            grid,
            enumerated_to,
        } => advance_time_window_axis(grid, enumerated_to, now, |k| {
            all_keys.insert(PartitionKey::Single { keys: vec![k] })
        }),
        PartitionUniverse::Dynamic { namespace } => {
            let Some(keys) = dynamic_keys.get(namespace) else {
                return false;
            };
            if !dynamic_axis_changed(keys, all_keys.len(), |k| {
                all_keys.contains(&PartitionKey::Single {
                    keys: vec![k.to_string()],
                })
            }) {
                return false;
            }
            *all_keys = keys
                .iter()
                .map(|k| PartitionKey::Single {
                    keys: vec![k.clone()],
                })
                .collect();
            true
        }
        PartitionUniverse::Multi { dims } => {
            let mut changed = false;
            for (_, du) in dims.iter_mut() {
                let DimensionUniverse {
                    keys: dim_keys,
                    kind,
                } = du;
                match kind {
                    DimensionKind::Frozen => {}
                    DimensionKind::TimeWindow {
                        grid,
                        enumerated_to,
                    } => {
                        changed |= advance_time_window_axis(grid, enumerated_to, now, |k| {
                            dim_keys.insert(k)
                        });
                    }
                    DimensionKind::Dynamic { namespace } => {
                        if let Some(keys) = dynamic_keys.get(namespace)
                            && dynamic_axis_changed(keys, dim_keys.len(), |k| dim_keys.contains(k))
                        {
                            let mut sorted: Vec<String> = keys.iter().cloned().collect();
                            sorted.sort();
                            *dim_keys = sorted.into_iter().collect();
                            changed = true;
                        }
                    }
                }
            }
            if changed {
                *all_keys = cartesian_universe(dims);
            }
            changed
        }
    }
}

/// Advance one time-window axis: enumerate window starts in
/// `(enumerated_to, min(grid.end, now)]` and offer each new key to `insert`.
/// Owned by every axis shape — the top-level universe and each `Multi`
/// dimension — so window-enumeration fixes can't drift between them.
fn advance_time_window_axis(
    grid: &TimeGrid,
    enumerated_to: &mut NaiveDateTime,
    now: NaiveDateTime,
    mut insert: impl FnMut(String) -> bool,
) -> bool {
    let bound = grid.end.map_or(now, |e| e.min(now));
    if bound <= *enumerated_to {
        return false;
    }
    let mut changed = false;
    match grid.window_starts_in(*enumerated_to, bound) {
        Ok(new_keys) => {
            for k in new_keys {
                changed |= insert(k);
            }
            *enumerated_to = bound;
        }
        Err(e) => {
            tracing::warn!(target: "rivers::daemon", error = %e, "time-window axis refresh failed");
        }
    }
    changed
}

/// Whether a dynamic axis' membership differs from the freshly fetched
/// namespace keys (equal length and full containment ⇒ equal sets).
fn dynamic_axis_changed(
    fresh: &HashSet<String>,
    current_len: usize,
    contains: impl Fn(&str) -> bool,
) -> bool {
    fresh.len() != current_len || !fresh.iter().all(|k| contains(k.as_str()))
}

/// Cartesian product of dimension key lists as `Multi` keys (def order).
fn cartesian_universe(dims: &[(String, DimensionUniverse)]) -> HashSet<PartitionKey> {
    if dims.is_empty() || dims.iter().any(|(_, du)| du.keys.is_empty()) {
        return HashSet::new();
    }
    let mut out = HashSet::new();
    let mut idx = vec![0usize; dims.len()];
    loop {
        out.insert(PartitionKey::Multi {
            dims: dims
                .iter()
                .zip(&idx)
                .map(|((name, du), &i)| (name.clone(), vec![du.keys[i].clone()]))
                .collect(),
        });
        let mut d = dims.len();
        loop {
            if d == 0 {
                return out;
            }
            d -= 1;
            idx[d] += 1;
            if idx[d] < dims[d].1.keys.len() {
                break;
            }
            idx[d] = 0;
        }
    }
}

/// Collect the dynamic namespaces a universe depends on.
pub(crate) fn universe_namespaces(universe: &PartitionUniverse, out: &mut HashSet<String>) {
    match universe {
        PartitionUniverse::Dynamic { namespace } => {
            out.insert(namespace.clone());
        }
        PartitionUniverse::Multi { dims } => {
            for (_, du) in dims {
                if let DimensionKind::Dynamic { namespace } = &du.kind {
                    out.insert(namespace.clone());
                }
            }
        }
        _ => {}
    }
}
