//! Latest-time-window resolution (grid-derived fast path + full scan).
use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;
use crate::util::parse_key_datetime;
use chrono::NaiveDateTime;

/// How an asset's latest time window is resolved: its key format, plus the
/// grid when the universe is grid-enumerated (enabling O(window) derivation
/// instead of parsing and sorting every key).
pub struct TimeWindowSource {
    pub fmt: String,
    pub grid: Option<crate::timegrid::TimeGrid>,
}

/// Lazily computes per-(asset, lookback) latest-time-window key sets during
/// one evaluation tick. `sources` spans every time-window-partitioned asset
/// the pass knows about — conditioned assets AND upstream deps — so dep
/// pivots resolve against the dep's own window instead of selecting
/// everything.
pub struct TimeWindowResolver<'a> {
    sources: &'a HashMap<String, TimeWindowSource>,
    now_local: NaiveDateTime,
    #[allow(clippy::type_complexity)]
    memo: std::cell::RefCell<HashMap<(String, u64), std::sync::Arc<HashSet<PartitionKey>>>>,
}

impl<'a> TimeWindowResolver<'a> {
    pub fn new(sources: &'a HashMap<String, TimeWindowSource>, now_local: NaiveDateTime) -> Self {
        Self {
            sources,
            now_local,
            memo: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Keys of `asset`'s latest time window (widened by `lookback_delta`);
    /// `None` when the asset is not time-window partitioned.
    pub fn keys_for(
        &self,
        asset: &str,
        all_keys: &HashSet<PartitionKey>,
        lookback_delta: Option<f64>,
    ) -> Option<std::sync::Arc<HashSet<PartitionKey>>> {
        let source = self.sources.get(asset)?;
        let memo_key = (
            asset.to_string(),
            lookback_delta.map(f64::to_bits).unwrap_or(u64::MAX),
        );
        if let Some(hit) = self.memo.borrow().get(&memo_key) {
            return Some(std::sync::Arc::clone(hit));
        }
        let keys = source
            .grid
            .as_ref()
            .and_then(|grid| {
                derive_window_keys_from_grid(grid, all_keys, self.now_local, lookback_delta)
            })
            .unwrap_or_else(|| {
                compute_latest_time_window_keys(
                    all_keys,
                    &source.fmt,
                    self.now_local,
                    lookback_delta,
                )
            });
        let keys = std::sync::Arc::new(keys);
        self.memo
            .borrow_mut()
            .insert(memo_key, std::sync::Arc::clone(&keys));
        Some(keys)
    }
}

/// Derive the latest-window key set in O(window) from the grid instead of
/// parsing and sorting the whole universe. `None` falls back to the scan.
pub(crate) fn derive_window_keys_from_grid(
    grid: &crate::timegrid::TimeGrid,
    all_keys: &HashSet<PartitionKey>,
    now_local: NaiveDateTime,
    lookback_delta: Option<f64>,
) -> Option<HashSet<PartitionKey>> {
    let (latest, _) = grid.nearest_keys(now_local);
    let latest = latest?;
    let latest_dt = parse_key_datetime(&latest, &grid.fmt).ok()?;
    let mut out = HashSet::new();
    let mut insert_if_known = |key: String| {
        let pk = PartitionKey::Single { keys: vec![key] };
        if all_keys.contains(&pk) {
            out.insert(pk);
        }
    };
    match lookback_delta {
        None => insert_if_known(latest),
        Some(delta_secs) => {
            let cutoff = latest_dt.checked_sub_signed(chrono::Duration::nanoseconds(
                (delta_secs * 1_000_000_000.0) as i64,
            ))?;
            for key in grid.keys_in_range(cutoff, latest_dt).ok()? {
                // keys_in_range brackets the window straddling `cutoff`; the
                // scan keeps only keys whose window START is at/after it.
                let dt = parse_key_datetime(&key, &grid.fmt).ok()?;
                if dt >= cutoff {
                    insert_if_known(key);
                }
            }
            insert_if_known(latest);
        }
    }
    Some(out)
}

/// Compute partition keys that fall within the latest time window.
pub fn compute_latest_time_window_keys(
    all_keys: &HashSet<PartitionKey>,
    fmt: &str,
    now_local: NaiveDateTime,
    lookback_delta: Option<f64>,
) -> HashSet<PartitionKey> {
    let mut parsed: Vec<(&PartitionKey, NaiveDateTime)> = all_keys
        .iter()
        .filter_map(|pk| {
            let key_str = match pk {
                PartitionKey::Single { keys } if !keys.is_empty() => &keys[0],
                _ => return None,
            };
            let dt = parse_key_datetime(key_str, fmt).ok()?;
            if dt <= now_local {
                Some((pk, dt))
            } else {
                None
            }
        })
        .collect();

    if parsed.is_empty() {
        return HashSet::new();
    }

    parsed.sort_by(|a, b| b.1.cmp(&a.1));

    match lookback_delta {
        Some(delta_secs) => {
            let latest_start = parsed[0].1;
            let lookback_nanos = (delta_secs * 1_000_000_000.0) as i64;
            let cutoff =
                latest_start.checked_sub_signed(chrono::Duration::nanoseconds(lookback_nanos));
            parsed
                .into_iter()
                .filter(|(_, dt)| cutoff.is_none_or(|c| *dt >= c))
                .map(|(pk, _)| pk.clone())
                .collect()
        }
        None => HashSet::from([parsed[0].0.clone()]),
    }
}
