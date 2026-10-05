//! Tests for the condition evaluation engine.

#![allow(clippy::type_complexity)] // test scaffolding LazyLocks mirror cache types verbatim
#![allow(clippy::field_reassign_with_default)] // reads clearly in test record setup

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;
use crate::assets::graph::GraphTopology;
use crate::storage::{
    AssetRecord, DEFAULT_CODE_LOCATION_ID, LaunchedBy, PartitionKey, RunRecord, RunStatus,
    StorageBackend,
};

mod backfill;
mod bench;
mod cache;
mod cache_dispatch;
mod cache_sweep;
mod cron;
mod deps;
mod eval_state;
mod fingerprint;
mod last_run;
mod latches;
mod leaves;
mod operators;
mod parity;
mod partition_mappings;
mod partition_scenarios;
mod partitions;
mod presets;
mod run_tags;
mod since;
mod tree;
mod will_be_requested;

/// A UTC instant, for schedules and tick windows.
fn utc(y: i16, mo: i8, d: i8, h: i8, mi: i8, sec: i8) -> jiff::Timestamp {
    jiff::civil::date(y, mo, d)
        .at(h, mi, sec, 0)
        .to_zoned(jiff::tz::TimeZone::UTC)
        .unwrap()
        .timestamp()
}

fn make_record(key: &str) -> AssetRecord {
    AssetRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: key.to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }
}

fn make_materialized_record(key: &str, ts: i64) -> AssetRecord {
    let mut r = make_record(key);
    r.last_timestamp = Some(ts);
    r.last_data_version = Some(format!("dv_{key}"));
    r.last_run_id = Some(format!("run_{key}"));
    r
}

static EMPTY_SET: std::sync::LazyLock<HashSet<String>> = std::sync::LazyLock::new(HashSet::new);

static EMPTY_BACKFILL: std::sync::LazyLock<crate::condition::cache::BackfillState> =
    std::sync::LazyLock::new(crate::condition::cache::BackfillState::default);

static DEFAULT_STATE: std::sync::LazyLock<AssetConditionState> =
    std::sync::LazyLock::new(AssetConditionState::default);

static EMPTY_ASSET_STATES: std::sync::LazyLock<HashMap<String, AssetConditionState>> =
    std::sync::LazyLock::new(HashMap::new);

static EMPTY_RUN_TAGS: std::sync::LazyLock<crate::condition::cache::SlotMap<RunTags>> =
    std::sync::LazyLock::new(HashMap::new);

static EMPTY_TICK_MAT_TAGS: std::sync::LazyLock<crate::condition::cache::SlotMap<Vec<RunTags>>> =
    std::sync::LazyLock::new(HashMap::new);

static EMPTY_RUN_ASSET_NAMES: std::sync::LazyLock<crate::condition::cache::SlotMap<Arc<[String]>>> =
    std::sync::LazyLock::new(HashMap::new);

fn empty_tag_snapshot() -> RunTagSnapshot<'static> {
    RunTagSnapshot {
        last_run_tags: &EMPTY_RUN_TAGS,
        tick_materialization_tags: &EMPTY_TICK_MAT_TAGS,
        last_run_asset_names: &EMPTY_RUN_ASSET_NAMES,
    }
}

/// Adapt an unpartitioned fixture map to the slotted shape (`None` slot).
fn slotted<V>(m: HashMap<String, V>) -> crate::condition::cache::SlotMap<V> {
    m.into_iter()
        .map(|(asset, v)| (asset, HashMap::from([(None, v)])))
        .collect()
}

/// Adapt a per-partition fixture map to the slotted shape (`Some` slots).
fn slotted_parts<V>(
    m: HashMap<String, HashMap<PartitionKey, V>>,
) -> crate::condition::cache::SlotMap<V> {
    m.into_iter()
        .map(|(asset, vals)| {
            (
                asset,
                vals.into_iter().map(|(pk, v)| (Some(pk), v)).collect(),
            )
        })
        .collect()
}

fn spk(s: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![s.to_string()],
    }
}

fn mpk(dims: &[(&str, &str)]) -> PartitionKey {
    PartitionKey::Multi {
        dims: dims
            .iter()
            .map(|(d, v)| (d.to_string(), vec![v.to_string()]))
            .collect(),
    }
}

static EMPTY_REQUESTED: std::sync::LazyLock<HashMap<String, PartitionSelection>> =
    std::sync::LazyLock::new(HashMap::new);

static EMPTY_FAILED_TS: std::sync::LazyLock<HashMap<String, i64>> =
    std::sync::LazyLock::new(HashMap::new);

fn make_ctx<'a>(
    target_key: &'a str,
    target_record: &'a AssetRecord,
    records: &'a HashMap<String, AssetRecord>,
    upstream_deps: &'a HashMap<String, Vec<String>>,
) -> EvalContext<'a> {
    EvalContext {
        target_key,
        root_key: target_key,
        target_record,
        cache: CacheSnapshot {
            records,
            upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000, // 1000s in nanos
        is_initial: false,
        partitions: None,
        root_partition_floor: None,
    }
}

/// Owned partition data for tests. Build this, then borrow from it to create PartitionEvalContext.
struct OwnedPartitionData {
    all_keys: HashSet<PartitionKey>,
    in_progress: HashSet<PartitionKey>,
    failed: HashSet<PartitionKey>,
    timestamps: HashMap<PartitionKey, i64>,
    all_partition_statuses: HashMap<String, crate::condition::cache::PartitionStatusEntry>,
}

impl OwnedPartitionData {
    fn new(all_keys: &[&str], materialized: &[&str], timestamps: &[(&str, i64)]) -> Self {
        // Materialized == has a timestamp (the cache keeps them in lockstep);
        // keys listed only in `materialized` get a placeholder ts.
        let mut ts: HashMap<PartitionKey, i64> =
            timestamps.iter().map(|(k, v)| (spk(k), *v)).collect();
        for k in materialized {
            ts.entry(spk(k)).or_insert(1);
        }
        Self {
            all_keys: all_keys.iter().map(|s| spk(s)).collect(),
            in_progress: HashSet::new(),
            failed: HashSet::new(),
            timestamps: ts,
            all_partition_statuses: HashMap::new(),
        }
    }

    fn as_eval_ctx(&self) -> PartitionEvalContext<'_> {
        PartitionEvalContext {
            all_keys: &self.all_keys,
            in_progress: &self.in_progress,
            failed: &self.failed,
            timestamps: &self.timestamps,
            resolver: PartitionResolver::empty(),
            time_windows: None,
            all_partition_statuses: &self.all_partition_statuses,
            dep_root_floor: None,
        }
    }
}

fn make_partitioned_ctx<'a>(
    target_key: &'a str,
    target_record: &'a AssetRecord,
    records: &'a HashMap<String, AssetRecord>,
    upstream_deps: &'a HashMap<String, Vec<String>>,
    pctx: &'a PartitionEvalContext<'a>,
) -> EvalContext<'a> {
    EvalContext {
        target_key,
        root_key: target_key,
        target_record,
        cache: CacheSnapshot {
            records,
            upstream_deps,
            in_progress_assets: &EMPTY_SET,
            failed_assets: &EMPTY_SET,
            failed_asset_timestamps: &EMPTY_FAILED_TS,
            backfill: &EMPTY_BACKFILL,
        },
        tags: empty_tag_snapshot(),
        prev_state: &DEFAULT_STATE,
        all_asset_states: &EMPTY_ASSET_STATES,
        requested_this_tick: &EMPTY_REQUESTED,
        now: 1_000_000_000_000,
        is_initial: false,
        partitions: Some(pctx),
        root_partition_floor: None,
    }
}
