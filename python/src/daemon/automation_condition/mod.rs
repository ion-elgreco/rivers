//! Automation condition daemon loop — evaluates condition trees and triggers materializations.
//!
//! Uses `AssetConditionCache` for incremental evaluation state across ticks. Evaluates
//! `ConditionNode` trees per asset (including partition-aware conditions), persists
//! tick/eval records to storage, and emits `RunRequest`s to trigger materializations.
//!
//! The pure-Rust per-tick state machine — refresh / evaluate / apply /
//! classify — lives in [`rivers_core::condition::ConditionPass`]. Python's
//! [`engine::ConditionTickEngine`] holds the PyO3-bound resources (storage,
//! repo cell, channels) and delegates each phase to the pass; the
//! materialization fan-out (after a condition fires) lives in `materialize`.
//! This module's `condition_eval_loop` is the entry point — it owns the
//! engine and drives `engine.tick()` on a periodic interval.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rivers_core::assets::graph::NodeRef;
use rivers_core::condition::{
    AssetConditionCache, AssetConditionInfo, ConditionEvalState, ConditionPass, DimensionKind,
    DimensionUniverse, PartitionInfo, PartitionMappingKind, PartitionUniverse,
};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{PartitionKey as CorePartitionKey, ScopedStorageHandle};
use tokio_util::sync::CancellationToken;

use super::{ConditionEvalWriteMsg, TickWriteMsg};
use crate::executor::ops::now_ts;
use crate::partitions::{PartitionMapping, PartitionsDefinition};
use crate::repository::PyCodeRepository;
use crate::repository::resolved_node::ResolvedNode;

mod engine;
mod materialize;
mod persist;

use engine::ConditionTickEngine;

/// Build a [`PartitionInfo`] from a graph node. Returns `None` if unpartitioned.
/// `node_map` supplies upstream definitions so time-window mappings carry
/// their grid into core eval; `deps` supplies the asset's dependency edges so
/// unmapped partitioned deps get their default Identity mapping.
fn partition_info_from_node(
    asset_name: &str,
    node: &ResolvedNode,
    node_map: &HashMap<String, ResolvedNode>,
    deps: &[NodeRef],
) -> Option<PartitionInfo> {
    let def = node.partitions_def()?;
    // Captured before enumeration: a window starting between this and the
    // enumerate call is re-added by the first refresh (extend is idempotent)
    // rather than skipped.
    let now = chrono::Local::now().naive_local();
    let universe = partition_universe_for(def, now);
    // Cap the automation universe at `now`: future time windows are not yet
    // materializable (refresh_universe grows them in as wall-clock advances), so
    // an explicit future `end` must not seed a backfill of the whole range.
    let all_keys = def
        .get_partition_keys_capped(now)
        .ok()
        .map(|keys| {
            keys.iter()
                .map(CorePartitionKey::from)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let time_window_fmt = match def {
        PartitionsDefinition::TimeWindow { fmt, .. } => Some(fmt.clone()),
        _ => None,
    };
    let mappings = extract_partition_mappings(asset_name, node, node_map, deps);
    Some(PartitionInfo {
        all_keys,
        mappings,
        time_window_fmt,
        universe,
    })
}

/// Watermark the per-tick refresh resumes from. Seeding caps enumeration at
/// `now` (see `get_partition_keys_capped`), so the watermark must also be
/// `min(end, now)` — `refresh_universe` then grows the universe one window at a
/// time as wall-clock reaches each, stopping at the explicit `end`.
fn seeded_watermark(
    def: &PartitionsDefinition,
    now: chrono::NaiveDateTime,
) -> chrono::NaiveDateTime {
    match def {
        PartitionsDefinition::TimeWindow { end: Some(e), .. } => (*e).min(now),
        _ => now,
    }
}

/// How `def`'s key universe evolves after extraction. `now` is the
/// enumeration high-water mark for open time grids.
fn partition_universe_for(
    def: &PartitionsDefinition,
    now: chrono::NaiveDateTime,
) -> PartitionUniverse {
    match def {
        PartitionsDefinition::Static { .. } => PartitionUniverse::Frozen,
        PartitionsDefinition::TimeWindow { .. } => match def.time_grid() {
            Some(grid) => PartitionUniverse::TimeWindow {
                grid,
                enumerated_to: seeded_watermark(def, now),
            },
            None => PartitionUniverse::Frozen,
        },
        PartitionsDefinition::Dynamic { name } => PartitionUniverse::Dynamic {
            namespace: name.clone(),
        },
        PartitionsDefinition::Multi { dimensions } => PartitionUniverse::Multi {
            dims: dimensions
                .iter()
                .map(|(dim_name, dim_def)| {
                    let keys = dim_def
                        .enumerate_single_dim_keys()
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    let kind = match dim_def {
                        PartitionsDefinition::TimeWindow { .. } => match dim_def.time_grid() {
                            Some(grid) => DimensionKind::TimeWindow {
                                grid,
                                enumerated_to: seeded_watermark(dim_def, now),
                            },
                            None => DimensionKind::Frozen,
                        },
                        PartitionsDefinition::Dynamic { name } => DimensionKind::Dynamic {
                            namespace: name.clone(),
                        },
                        _ => DimensionKind::Frozen,
                    };
                    (dim_name.clone(), DimensionUniverse { keys, kind })
                })
                .collect(),
        },
    }
}

impl PyCodeRepository {
    /// Extract assets with automation conditions from the repo. Returns
    /// conditions in topological order (deps before downstreams) so that
    /// `WillBeRequested` can see upstream results during same-tick evaluation.
    pub(in crate::daemon) fn extract_asset_conditions(&self) -> Vec<AssetConditionInfo> {
        let guard = self.state.read().unwrap();
        let Some(state) = guard.as_ref() else {
            return Vec::new();
        };

        // Backfill strategy is converted to the pure-Rust core variant here
        // while we have the GIL — the eval loop runs without it.
        let mut by_key: HashMap<String, AssetConditionInfo> = HashMap::new();
        for (name, node) in &state.node_map {
            if let ResolvedNode::Asset(asset_node) = node
                && let Some(ref cond) = asset_node.automation_condition
            {
                let deps = state
                    .inner_repo
                    .assets()
                    .get(name)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                let partition_info = partition_info_from_node(name, node, &state.node_map, deps);
                let backfill_strategy = node.backfill_strategy().map(|s| s.to_core());
                by_key.insert(
                    name.clone(),
                    AssetConditionInfo {
                        asset_key: name.clone(),
                        condition: cond.node.clone(),
                        partition_info,
                        backfill_strategy,
                    },
                );
            }
        }

        if by_key.is_empty() {
            return Vec::new();
        }

        state.inner_repo.sort_topologically(by_key)
    }

    /// Extract upstream partition keys for all conditioned assets. Reads
    /// the cached `PartitionsDefinition` value off each `ResolvedNode`, so
    /// no GIL is required.
    pub(in crate::daemon) fn extract_upstream_partition_keys(
        &self,
        conditions: &[AssetConditionInfo],
    ) -> HashMap<String, (HashSet<CorePartitionKey>, PartitionUniverse)> {
        let guard = self.state.read().unwrap();
        let Some(state) = guard.as_ref() else {
            return HashMap::new();
        };

        let mut map: HashMap<String, (HashSet<CorePartitionKey>, PartitionUniverse)> =
            HashMap::new();
        for cond in conditions {
            if let Some(ref pi) = cond.partition_info {
                // Conditioned assets re-sync from their refreshed
                // PartitionInfo each tick, so Frozen here is fine.
                map.entry(cond.asset_key.clone())
                    .or_insert_with(|| (pi.all_keys.clone(), PartitionUniverse::Frozen));
                for (_, upstream_key) in pi.mappings.keys() {
                    if !map.contains_key(upstream_key)
                        && let Some(node) = state.node_map.get(upstream_key)
                        && let Some(def) = node.partitions_def()
                    {
                        let now = chrono::Local::now().naive_local();
                        let universe = partition_universe_for(def, now);
                        let core_keys: HashSet<CorePartitionKey> = def
                            .get_partition_keys()
                            .ok()
                            .map(|keys| keys.iter().map(CorePartitionKey::from).collect())
                            .unwrap_or_default();
                        map.insert(upstream_key.clone(), (core_keys, universe));
                    }
                }
            }
        }
        map
    }
}

/// `upstream_def` is the definition of the side the mapping shifts within —
/// it supplies the time grid for `TimeWindow` and per-dimension defs for
/// nested mappings.
fn mapping_to_kind(
    m: &PartitionMapping,
    upstream_def: Option<&PartitionsDefinition>,
) -> PartitionMappingKind {
    match m {
        PartitionMapping::Identity {} => PartitionMappingKind::Identity,
        PartitionMapping::AllPartitions {} => PartitionMappingKind::AllPartitions,
        PartitionMapping::Static { mapping } => PartitionMappingKind::Static {
            mapping: mapping.clone(),
        },
        PartitionMapping::TimeWindow { offset } => PartitionMappingKind::TimeWindow {
            offset: *offset,
            grid: upstream_def.and_then(|d| d.time_grid()),
        },
        PartitionMapping::SpecificPartitions { partition_keys } => {
            PartitionMappingKind::SpecificPartitions {
                keys: partition_keys.clone(),
            }
        }
        PartitionMapping::Multi { dimension_mappings } => PartitionMappingKind::Multi {
            dimension_mappings: dimension_mappings
                .iter()
                .map(|(up_dim, (down_dim, per_dim))| {
                    let dim_def = upstream_def.and_then(|d| match d {
                        PartitionsDefinition::Multi { dimensions } => dimensions
                            .iter()
                            .find(|(n, _)| n == up_dim)
                            .map(|(_, dd)| dd),
                        _ => None,
                    });
                    (
                        up_dim.clone(),
                        (
                            down_dim.clone(),
                            Box::new(mapping_to_kind(per_dim, dim_def)),
                        ),
                    )
                })
                .collect(),
        },
        PartitionMapping::MultiToSingle {
            dimension_name,
            partition_mapping,
        } => {
            // The inner mapping shifts within the named dimension when the
            // upstream is Multi, else within the upstream itself.
            let inner_def = upstream_def.and_then(|d| match d {
                PartitionsDefinition::Multi { dimensions } => dimensions
                    .iter()
                    .find(|(n, _)| n == dimension_name)
                    .map(|(_, dd)| dd),
                _ => upstream_def,
            });
            PartitionMappingKind::MultiToSingle {
                dimension_name: dimension_name.clone(),
                inner: Box::new(mapping_to_kind(&partition_mapping.0, inner_def)),
            }
        }
        PartitionMapping::ForKeys { .. } => PartitionMappingKind::ForKeys,
        PartitionMapping::Subset {} => PartitionMappingKind::Subset,
    }
}

fn extract_partition_mappings(
    asset_name: &str,
    node: &crate::repository::resolved_node::ResolvedNode,
    node_map: &HashMap<String, ResolvedNode>,
    deps: &[NodeRef],
) -> HashMap<(String, String), PartitionMappingKind> {
    let mut result = HashMap::new();
    if let Some(mappings) = node.partition_mapping() {
        for (upstream_name, mapping) in &mappings {
            let upstream_def = node_map.get(upstream_name).and_then(|n| n.partitions_def());
            let kind = mapping_to_kind(mapping, upstream_def);
            result.insert((asset_name.to_string(), upstream_name.clone()), kind);
        }
    }
    // An unmapped partitioned dep defaults to Identity at resolve time; the
    // eval context must carry that default or the dep is treated as
    // unpartitioned and its condition truth broadcasts across the universe.
    for dep in deps {
        let NodeRef::ByName(upstream_name) = dep else {
            continue;
        };
        let key = (asset_name.to_string(), upstream_name.clone());
        if result.contains_key(&key) {
            continue;
        }
        if node_map
            .get(upstream_name)
            .and_then(|n| n.partitions_def())
            .is_some()
        {
            result.insert(key, PartitionMappingKind::Identity);
        }
    }
    result
}

pub(super) struct ConditionEvalLoopConfig {
    pub conditions: Vec<AssetConditionInfo>,
    /// Storage scoped to the owning code location. Filters every per-CL
    /// query in the loop and supplies `code_location_id` for queued
    /// `RunRecord`s so the coordinator only dequeues this CL's runs.
    pub storage: ScopedStorageHandle<SurrealStorage>,
    /// Shared with the schedule/sensor loop — both run and backfill
    /// shapes route through these dispatchers.
    pub run_dispatcher: Arc<crate::daemon::dispatchers::RunDispatcherKind>,
    pub backfill_dispatcher: Arc<crate::daemon::dispatchers::BackfillDispatcherKind>,
    pub cancel: CancellationToken,
    pub interval: std::time::Duration,
    pub tick_tx: tokio::sync::mpsc::UnboundedSender<TickWriteMsg>,
    pub max_ticks_retained: Option<usize>,
    pub eval_tx: tokio::sync::mpsc::UnboundedSender<ConditionEvalWriteMsg>,
    pub max_evals_retained: Option<usize>,
    /// Upstream partition keys (+ how each set evolves), pre-extracted with
    /// GIL at daemon start.
    pub upstream_partition_keys: HashMap<String, (HashSet<CorePartitionKey>, PartitionUniverse)>,
}

/// Background loop that periodically evaluates automation conditions on assets.
/// Spawned as a separate tokio task alongside the schedule/sensor loop.
///
/// Owns one [`ConditionTickEngine`] for the lifetime of the daemon and drives
/// `engine.tick()` on `interval`. All per-tick orchestration — cache refresh,
/// evaluation, state mutation, persistence, materialization fan-out — lives
/// inside the engine.
#[tracing::instrument(skip_all, target = "rivers::daemon", name = "condition_loop", fields(asset_count = config.conditions.len()))]
pub(super) async fn condition_eval_loop(config: ConditionEvalLoopConfig) {
    let ConditionEvalLoopConfig {
        conditions,
        storage,
        run_dispatcher,
        backfill_dispatcher,
        cancel,
        interval,
        tick_tx,
        max_ticks_retained,
        eval_tx,
        max_evals_retained,
        upstream_partition_keys,
    } = config;

    let code_location_id = storage.code_location_id().to_string();
    let mut cache = AssetConditionCache::new(code_location_id.clone());

    let fresh = || ConditionEvalState {
        is_initial: true,
        ..Default::default()
    };
    let mut eval_state: ConditionEvalState = match storage.scoped().get_condition_eval_state().await
    {
        Ok(Some(state)) => state,
        Ok(None) => fresh(),
        // Degrade gracefully but surface it: a swallowed load error silently
        // wipes every latch and treats all assets as initial (sibling of the
        // save-path hardening). Transient-retry belongs in the storage layer.
        Err(e) => {
            tracing::warn!(
                target: "rivers::daemon",
                error = %e,
                "failed to load condition eval state; starting fresh (latches reset)"
            );
            fresh()
        }
    };

    for info in &conditions {
        let current_fp = info.condition.fingerprint_hex();
        let state = eval_state.assets.entry(info.asset_key.clone()).or_default();
        if state.condition_fingerprint == current_fp {
            continue;
        }
        tracing::debug!(
            target: "rivers::daemon",
            asset = %info.asset_key,
            old_fp = %state.condition_fingerprint,
            new_fp = %current_fp,
            "condition tree changed, invalidating evaluation state"
        );
        state.reset_for_new_tree(current_fp);
    }

    // Prune state for assets that no longer have conditions. Keep dep
    // entries with meaningful state (last_materialized_timestamp) — these
    // are used by NewlyUpdated in dep pivots across daemon restarts.
    let active: HashSet<String> = conditions.iter().map(|c| c.asset_key.clone()).collect();
    eval_state
        .assets
        .retain(|k, v| active.contains(k) || v.last_materialized_timestamp.is_some());

    // Register partitioned assets with the cache so refresh() loads their
    // status. Include both conditioned assets AND their upstream deps —
    // needed for accurate partition-level AnyDepsMissing / any_deps_updated.
    let mut partitioned_asset_keys: Vec<String> = conditions
        .iter()
        .filter_map(|c| c.partition_info.as_ref().map(|_| c.asset_key.clone()))
        .collect();
    for cond in &conditions {
        if let Some(ref pi) = cond.partition_info {
            for (_, upstream) in pi.mappings.keys() {
                if !partitioned_asset_keys.contains(upstream) {
                    partitioned_asset_keys.push(upstream.clone());
                }
            }
        }
    }
    cache.set_partitioned_assets(partitioned_asset_keys.clone());
    let cond_nodes: Vec<_> = conditions.iter().map(|c| c.condition.clone()).collect();
    cache.set_needs_tick_tags(&cond_nodes);

    tracing::info!(
        target: "rivers::daemon",
        count = conditions.len(),
        partitioned = partitioned_asset_keys.len(),
        "condition eval loop started"
    );

    let pass = ConditionPass::new(cache, eval_state, conditions, upstream_partition_keys);

    let mut engine = ConditionTickEngine {
        pass,
        code_location_id,
        storage,
        run_dispatcher,
        backfill_dispatcher,
        tick_tx,
        eval_tx,
        max_ticks_retained,
        max_evals_retained,
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Persist state before exiting.
                if let Err(e) = engine
                    .storage
                    .scoped()
                    .set_condition_eval_state(&engine.pass.eval_state)
                    .await
                {
                    tracing::warn!(
                        target: "rivers::daemon",
                        error = %e,
                        "failed to persist condition eval state on shutdown"
                    );
                }
                tracing::info!(target: "rivers::daemon", "condition eval loop stopped");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        let now = now_ts();
        engine.tick(now).await;
    }
}
