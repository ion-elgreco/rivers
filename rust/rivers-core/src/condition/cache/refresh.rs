//! Steady-state refresh: plan-phase fetches and the delta apply.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::storage::{PartitionKey, RunRecord, RunStatus, StorageBackend};

use super::*;

impl AssetConditionCache {
    /// Plan phase: all fallible storage I/O for one steady-state refresh.
    pub(super) async fn fetch_refresh_delta<S: StorageBackend>(
        &self,
        storage: &S,
        now: i64,
    ) -> anyhow::Result<RefreshDelta> {
        let mut delta = RefreshDelta {
            clear_tick_accumulators: true,
            ..Default::default()
        };

        let scoped = storage.for_code_location(&self.ctx);

        let new_runs = scoped
            .get_runs_since(self.last_seen_run_ts, None, crate::storage::SortOrder::Asc)
            .await?;
        let mut invalidated_keys: Vec<String> = new_runs
            .iter()
            .filter(|r| {
                run_status_is_terminal(&r.status) && !self.applied_run_ids.contains_key(&r.run_id)
            })
            .flat_map(|r| r.node_names.iter().cloned())
            .collect();

        // Run ids the tracked-run sweep below observed as terminal this refresh;
        // the new_runs loop must not re-track them from a stale Queued snapshot.
        let mut swept_terminal: HashSet<String> = HashSet::new();

        if !self.in_progress_assets.is_empty() {
            let ip_keys: Vec<String> = self.in_progress_assets.keys().cloned().collect();
            let fresh_records = scoped.get_asset_records_by_keys(&ip_keys).await?;
            let mut completed_keys: Vec<String> = Vec::new();
            let mut completed_run_ids: HashSet<String> = HashSet::new();
            for record in fresh_records {
                let old = self.records.get(&record.asset_key);
                let ts_changed = old
                    .map(|o| o.last_timestamp != record.last_timestamp)
                    .unwrap_or(true);
                tracing::trace!(
                    target: "rivers::dbg::cond",
                    asset = %record.asset_key,
                    old_ts = ?old.and_then(|o| o.last_timestamp),
                    new_ts = record.last_timestamp,
                    ts_changed,
                    "refresh: in_progress completion check"
                );
                if ts_changed {
                    if let Some(rid) = &record.last_run_id {
                        completed_run_ids.insert(rid.clone());
                    }
                    completed_keys.push(record.asset_key.clone());
                    delta.record_updates.push(record);
                } else if let Some(runs) = self.in_progress_assets.get(&record.asset_key) {
                    let run_ids: Vec<String> = runs.keys().cloned().collect();
                    let (completed, succeeded_runs) =
                        storage.step_completion(&record.asset_key, &run_ids).await?;
                    if completed {
                        completed_keys.push(record.asset_key.clone());
                        if !succeeded_runs.is_empty() {
                            delta
                                .materialized_overrides
                                .entry(record.asset_key.clone())
                                .or_default()
                                .extend(succeeded_runs);
                        }
                        completed_run_ids.extend(run_ids);
                    }
                }
            }
            for key in &completed_keys {
                invalidated_keys.push(key.clone());
            }

            let new_runs_terminal: HashSet<&str> = new_runs
                .iter()
                .filter(|r| run_status_is_terminal(&r.status))
                .map(|r| r.run_id.as_str())
                .collect();

            let clearable: Vec<String> = self
                .in_progress_assets
                .values()
                .flat_map(|runs| runs.keys().cloned())
                .collect();
            let mut swept_applied: HashSet<String> = HashSet::new();
            if !clearable.is_empty() {
                let tracked_runs = storage.get_runs_by_ids(&clearable, None).await?;
                for run in &tracked_runs {
                    if !run_status_is_terminal(&run.status) {
                        continue;
                    }
                    swept_terminal.insert(run.run_id.clone());
                    delta
                        .applied_runs
                        .push((run.run_id.clone(), run.start_time));
                    delta.clear_run(run);
                    if !new_runs_terminal.contains(run.run_id.as_str())
                        && !completed_run_ids.contains(&run.run_id)
                        && !self.applied_run_ids.contains_key(&run.run_id)
                    {
                        invalidated_keys.extend(run.node_names.iter().cloned());
                        if self.apply_run_effects_to_delta(run, &mut delta) {
                            swept_applied.insert(run.run_id.clone());
                        }
                    }
                }
            }

            completed_run_ids.retain(|id| {
                !new_runs_terminal.contains(id.as_str()) && !swept_applied.contains(id)
            });
            if !completed_run_ids.is_empty() {
                let ids: Vec<String> = completed_run_ids.into_iter().collect();
                let completed_runs = storage.get_runs_by_ids(&ids, None).await?;
                for run in &completed_runs {
                    if self.applied_run_ids.contains_key(&run.run_id) {
                        continue;
                    }
                    self.apply_run_effects_to_delta(run, &mut delta);
                    invalidated_keys.extend(run.node_names.iter().cloned());
                }
            }
        }

        if !new_runs.is_empty() {
            // The cursor trails the newest start_time, so already-processed
            // runs re-deliver every tick; only genuinely new work may defeat
            // should_skip.
            let known_in_flight = |r: &crate::storage::RunRecord| {
                !r.node_names.is_empty()
                    && r.node_names.iter().all(|a| {
                        self.in_progress_assets
                            .get(a)
                            .is_some_and(|runs| runs.contains_key(&r.run_id))
                    })
            };
            let has_new_work = new_runs.iter().any(|r| {
                if run_status_is_terminal(&r.status) {
                    !self.applied_run_ids.contains_key(&r.run_id)
                        && !swept_terminal.contains(&r.run_id)
                } else {
                    !known_in_flight(r)
                }
            });
            if has_new_work {
                delta.changed = true;
            }

            for run in &new_runs {
                delta.confirmed_pending.push(run.run_id.clone());

                match run.status {
                    RunStatus::Started | RunStatus::NotStarted | RunStatus::Queued => {
                        if !swept_terminal.contains(&run.run_id) {
                            for asset in &run.node_names {
                                delta.in_progress_changes.push(InProgressChange::Push {
                                    asset_key: asset.clone(),
                                    run_id: run.run_id.clone(),
                                    partition_key: run.partition_key.clone(),
                                });
                            }
                        }
                    }
                    RunStatus::Success | RunStatus::Failure => {
                        delta.clear_run(run);
                        if !self.applied_run_ids.contains_key(&run.run_id) {
                            self.apply_run_effects_to_delta(run, &mut delta);
                        }
                    }
                    RunStatus::Canceled => {
                        delta.clear_run(run);
                        delta
                            .applied_runs
                            .push((run.run_id.clone(), run.start_time));
                    }
                }
            }

            // Trail the newest start_time by 1ns: dispatchers stamp one `now`
            // across a batch committed record-by-record, so equal-timestamp
            // runs can land after this refresh. `applied_run_ids` dedups the
            // re-delivered ones.
            if let Some(newest) = new_runs.iter().map(|r| r.start_time).max() {
                delta.new_last_seen_run_ts = Some(newest.saturating_sub(1));
            }
        }

        if !invalidated_keys.is_empty() {
            delta.changed = true;
            let downstream_records = self
                .fetch_records_with_downstream(storage, &invalidated_keys)
                .await?;
            delta.record_updates.extend(downstream_records);

            delta.partition_status = self
                .fetch_partition_status_for_invalidated(storage, &invalidated_keys)
                .await?;
        }

        // BackfillStatus has two live states and load_active_backfills returns
        // every backfill in them, so the fresh query IS the new state — a
        // tracked id that is terminal (or deleted) simply stops appearing.
        let new_backfill = Self::load_active_backfills(storage, &self.ctx).await?;
        if new_backfill != self.backfill {
            delta.changed = true;
            // Assets whose last active backfill ended may still carry the
            // empty pre-dispatch in-flight placeholder (a canceled backfill
            // never produces an observed sub-run to clear it).
            for asset in self.backfill.assets.keys() {
                if !new_backfill.assets.contains_key(asset) {
                    delta.backfill_ended_assets.push(asset.clone());
                }
            }
            delta.backfill = Some(new_backfill);
        }

        self.fetch_refresh_observations_delta(storage, &mut delta)
            .await?;

        let confirmed_set: HashSet<&str> =
            delta.confirmed_pending.iter().map(String::as_str).collect();
        for (run_id, pending) in &self.pending_runs {
            if confirmed_set.contains(run_id.as_str()) {
                continue;
            }
            if (now - pending.first_seen_ts) > self.pending_grace_nanos {
                delta
                    .evicted_pending
                    .push((run_id.clone(), pending.asset_keys.clone()));
            }
        }
        if !delta.evicted_pending.is_empty() {
            delta.changed = true;
        }

        Ok(delta)
    }

    /// Plan-phase helper: observations sub-pass.
    pub(super) async fn fetch_refresh_observations_delta<S: StorageBackend>(
        &self,
        storage: &S,
        delta: &mut RefreshDelta,
    ) -> anyhow::Result<()> {
        let observations = storage
            .get_observations_since(self.ctx.id(), self.last_observation_ts)
            .await?;
        if observations.is_empty() {
            return Ok(());
        }

        let mut observed_keys: Vec<String> = Vec::new();
        let mut max_ts = self.last_observation_ts;
        for event in &observations {
            if let Some(ref key) = event.asset_key
                && !observed_keys.contains(key)
            {
                observed_keys.push(key.clone());
            }
            if event.timestamp > max_ts {
                max_ts = event.timestamp;
            }
        }

        if !observed_keys.is_empty() {
            let records = self
                .fetch_records_with_downstream(storage, &observed_keys)
                .await?;
            // Replayed observations (the cursor trails the newest by 1) must
            // be no-ops: only assets whose record actually moved are cleared
            // and count as change — an AssetClear for an unchanged record
            // would wipe live run tracking seeded at initial_load.
            for record in records {
                let unchanged = self
                    .records
                    .get(&record.asset_key)
                    .is_some_and(|cached| *cached == record);
                if unchanged {
                    continue;
                }
                delta.changed = true;
                if observed_keys.contains(&record.asset_key) {
                    delta
                        .in_progress_changes
                        .push(InProgressChange::AssetClear(record.asset_key.clone()));
                }
                delta.record_updates.push(record);
            }
        }

        // Trail the newest stamp by 1ns, matching the run cursor (and the
        // initial-load observation cursor): observations in one batch share a
        // stamped `now` committed record-by-record, so a co-timestamped write
        // can land after this refresh. The replay no-op guard above dedups the
        // re-delivered ones.
        delta.new_last_observation_ts = Some(max_ts.saturating_sub(1));
        Ok(())
    }

    /// Plan-phase helper: append a completed Success/Failure run's mutations into `delta`.
    pub(super) fn apply_run_effects_to_delta(
        &self,
        run: &RunRecord,
        delta: &mut RefreshDelta,
    ) -> bool {
        if !matches!(run.status, RunStatus::Success | RunStatus::Failure) {
            return false;
        }
        delta
            .applied_runs
            .push((run.run_id.clone(), run.start_time));
        let run_asset_names: Arc<[String]> = Arc::from(run.node_names.as_slice());
        let run_tags: Arc<[(String, String)]> = Arc::from(run.tags.as_slice());
        let is_failure = matches!(run.status, RunStatus::Failure);
        let run_ts = run.end_time.unwrap_or(run.start_time);
        for asset in &run.node_names {
            if run.partition_key.is_none() || !self.is_partitioned(asset) {
                if is_failure {
                    delta
                        .failed_adds
                        .entry(asset.clone())
                        .and_modify(|e| {
                            if run_ts > e.ts {
                                e.run_id = run.run_id.clone();
                                e.ts = run_ts;
                            }
                        })
                        .or_insert_with(|| FailedRun {
                            ts: run_ts,
                            run_id: run.run_id.clone(),
                        });
                } else {
                    delta
                        .failed_removes
                        .entry(asset.clone())
                        .and_modify(|t| *t = (*t).max(run_ts))
                        .or_insert(run_ts);
                }
            }
            // A successful run materialized every asset it covered; record it
            // so multiple runs of the same asset completing in one refresh each
            // pass the materialization gate at apply — the scalar record credits
            // only the newest, which would otherwise drop the others' slots.
            if !is_failure {
                delta
                    .materialized_overrides
                    .entry(asset.clone())
                    .or_default()
                    .insert(run.run_id.clone());
            }
            // Route by the ASSET's partitioning, not the run's key: a joint
            // partition-keyed run still writes an unpartitioned asset's entry
            // into the scalar maps the unpartitioned eval path reads.
            for partition_key in run_partition_slots(self.is_partitioned(asset), run) {
                delta.last_run_updates.push((
                    asset.clone(),
                    partition_key.clone(),
                    run.run_id.clone(),
                    run_ts,
                    Arc::clone(&run_tags),
                    Arc::clone(&run_asset_names),
                ));
                // Push for every run, carrying the failure flag: a Success run
                // materialized all its covered assets, but an overall-Failure
                // joint run counts only for the assets whose step actually
                // materialized here (resolved by the apply gate below).
                if self.needs_tick_tags {
                    delta.tick_tag_updates.push((
                        asset.clone(),
                        partition_key.clone(),
                        run.run_id.clone(),
                        is_failure,
                        Arc::clone(&run_tags),
                    ));
                }
            }
        }
        true
    }

    /// Plan-phase helper: fetch fresh records for `keys` and their transitive downstream dependents.
    pub(super) async fn fetch_records_with_downstream<S: StorageBackend>(
        &self,
        storage: &S,
        keys: &[String],
    ) -> anyhow::Result<Vec<AssetRecord>> {
        let touched: HashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
        let all_keys: Vec<String> = self
            .expand_downstream(&touched)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        if all_keys.is_empty() {
            return Ok(Vec::new());
        }
        storage
            .for_code_location(&self.ctx)
            .get_asset_records_by_keys(&all_keys)
            .await
    }

    /// Plan-phase helper: re-fetch partition status for invalidated assets.
    pub(super) async fn fetch_partition_status_for_invalidated<S: StorageBackend>(
        &self,
        storage: &S,
        invalidated_keys: &[String],
    ) -> anyhow::Result<HashMap<String, PartitionStatusPatch>> {
        let scoped = storage.for_code_location(&self.ctx);
        let mut out: HashMap<String, PartitionStatusPatch> = HashMap::new();
        let unique: HashSet<&String> = invalidated_keys.iter().collect();
        for asset_key in unique {
            let Some(current) = self.partition_status.get(asset_key.as_str()) else {
                continue;
            };
            // Incremental: only rows whose last_timestamp advanced past what
            // the cache already knows — a full asset_partitions scan here was
            // the dominant per-tick cost at large partition counts. The cursor
            // trails the max by 1 (like the run cursor): equal stamps from one
            // batched `now` can land in a later refresh.
            let since = current
                .timestamps
                .values()
                .copied()
                .max()
                .map(|m| m - 1)
                .unwrap_or(-1);
            let fresh_timestamps = scoped
                .get_partition_timestamps_since(asset_key, since)
                .await?;
            let in_progress: HashSet<PartitionKey> = scoped
                .get_in_progress_partitions(asset_key)
                .await?
                .into_iter()
                .collect();
            // Supersession against fresh timestamps is reconciled at apply
            // (timestamps only grow, so recomputing there is sound).
            let failed = scoped
                .get_failed_partitions(asset_key, &current.timestamps)
                .await?;
            out.insert(
                asset_key.clone(),
                PartitionStatusPatch {
                    fresh_timestamps,
                    in_progress,
                    failed,
                },
            );
        }
        Ok(out)
    }

    /// Apply phase: replay the planned delta against the cache. Returns `delta.changed`.
    pub(super) fn apply_refresh_delta(&mut self, delta: RefreshDelta) -> bool {
        let RefreshDelta {
            changed,
            clear_tick_accumulators,
            record_updates,
            in_progress_changes,
            failed_adds,
            failed_removes,
            materialized_overrides,
            last_run_updates,
            tick_tag_updates,
            partition_status,
            backfill,
            new_last_seen_run_ts,
            new_last_observation_ts,
            confirmed_pending,
            evicted_pending,
            applied_runs,
            backfill_ended_assets,
        } = delta;

        if clear_tick_accumulators {
            self.tick_materialization_tags.clear();
        }

        for record in record_updates {
            self.records.insert(record.asset_key.clone(), record);
        }

        for change in in_progress_changes {
            match change {
                InProgressChange::Push {
                    asset_key,
                    run_id,
                    partition_key,
                } => self.track_in_progress_run(asset_key, run_id, partition_key),
                InProgressChange::ClearRun { asset_key, run_id } => {
                    self.untrack_in_progress_run(&asset_key, &run_id)
                }
                InProgressChange::AssetClear(asset_key) => {
                    self.in_progress_assets.remove(asset_key.as_str());
                }
            }
        }

        for (asset, FailedRun { ts, run_id }) in failed_adds {
            let materialized_here = self
                .records
                .get(asset.as_str())
                .and_then(|r| r.last_run_id.as_deref())
                == Some(run_id.as_str())
                || materialized_overrides
                    .get(&asset)
                    .is_some_and(|runs| runs.contains(run_id.as_str()));
            if materialized_here {
                if self
                    .failed_asset_timestamps
                    .get(asset.as_str())
                    .is_none_or(|&f| ts >= f)
                {
                    self.failed_assets.remove(asset.as_str());
                    self.failed_asset_timestamps.remove(asset.as_str());
                }
                continue;
            }
            self.failed_assets.insert(asset.clone());
            self.failed_asset_timestamps
                .entry(asset)
                .and_modify(|t| *t = (*t).max(ts))
                .or_insert(ts);
        }
        for (asset, success_ts) in failed_removes {
            let outranked_by_failure = self
                .failed_asset_timestamps
                .get(asset.as_str())
                .is_some_and(|&fail_ts| success_ts < fail_ts);
            if !outranked_by_failure {
                self.failed_assets.remove(asset.as_str());
                self.failed_asset_timestamps.remove(asset.as_str());
            }
        }

        for (asset, pk, run_id, run_ts, tags, names) in last_run_updates {
            // LastExecutedWithTags/LastRunIncludesTarget reflect the latest run
            // that MATERIALIZED the asset — mirror the failure-floor gate.
            let materialized_here = self
                .records
                .get(asset.as_str())
                .and_then(|r| r.last_run_id.as_deref())
                == Some(run_id.as_str())
                || materialized_overrides
                    .get(&asset)
                    .is_some_and(|runs| runs.contains(&run_id));
            if materialized_here {
                self.update_last_run_maps(&asset, &pk, run_ts, &tags, &names);
            }
        }
        for (asset, pk, run_id, run_failed, tags) in tick_tag_updates {
            // A Success run materialized every asset it covered. An overall-
            // Failure joint run counts only for the assets it actually
            // materialized (record credits this run, or its step succeeded per
            // materialized_overrides) — mirroring the last_run gate.
            let record_it = !run_failed
                || self
                    .records
                    .get(asset.as_str())
                    .and_then(|r| r.last_run_id.as_deref())
                    == Some(run_id.as_str())
                || materialized_overrides
                    .get(&asset)
                    .is_some_and(|runs| runs.contains(&run_id));
            if record_it {
                self.update_tick_materialization_tags(&asset, &pk, &tags);
            }
        }

        for (key, patch) in partition_status {
            let entry = self.partition_status.entry(key).or_default();
            for (pk, ts) in patch.fresh_timestamps {
                entry.timestamps.insert(pk, ts);
            }
            entry.in_progress = patch.in_progress;
            // Drop failures superseded by the freshly merged timestamps (the
            // plan-phase supersession ran against the pre-merge view).
            entry.failed_timestamps = patch
                .failed
                .into_iter()
                .filter(|(pk, fail_ts)| entry.timestamps.get(pk).is_none_or(|mat| fail_ts > mat))
                .collect();
            entry.failed = entry.failed_timestamps.keys().cloned().collect();
        }

        if let Some(bf) = backfill {
            self.backfill = bf;
        }
        for asset in backfill_ended_assets {
            self.clear_predispatch_mark(&asset);
        }

        for (run_id, start_time) in applied_runs {
            self.applied_run_ids.insert(run_id, start_time);
        }
        if let Some(ts) = new_last_seen_run_ts {
            self.last_seen_run_ts = ts;
        }
        // Runs at or below the cursor can never be re-delivered (`start_time > $since`).
        let cursor = self.last_seen_run_ts;
        self.applied_run_ids.retain(|_, st| *st > cursor);
        if let Some(ts) = new_last_observation_ts {
            self.last_observation_ts = ts;
        }

        for run_id in confirmed_pending {
            self.pending_runs.remove(&run_id);
        }

        for (run_id, asset_keys) in evicted_pending {
            self.pending_runs.remove(&run_id);
            for asset_key in &asset_keys {
                self.untrack_in_progress_run(asset_key, &run_id);
            }
            tracing::warn!(
                target: "rivers::daemon",
                run_id = %run_id,
                assets = asset_keys.len(),
                "evicting phantom run_id from cache after grace period"
            );
        }

        changed
    }
}
