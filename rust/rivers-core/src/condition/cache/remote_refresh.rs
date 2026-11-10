//! Per-tick refresh of a resolved remote location: its runs and
//! observations since the last tick.

use std::collections::{HashMap, HashSet};

use crate::assets::remote::LocationRef;
use crate::storage::{RunRecord, RunStatus, ScopedStorage, SortOrder, StorageBackend};

use super::remote::{LiveRemoteRun, RemoteRead, RemoteScope, SlotState};
use super::{AssetConditionCache, run_status_is_terminal};

/// What one tick learned about a resolved location.
#[derive(Default)]
struct ScopeDelta {
    started: Vec<(String, LiveRemoteRun)>,
    /// Tracked runs that ended or vanished.
    ended: HashSet<String>,
    applied: Vec<(String, i64)>,
    /// Owner keys to re-read. Those in `reload` re-read every partition row,
    /// because an action run can delete rows.
    touched: HashSet<String>,
    reload: HashSet<String>,
    /// Owner keys a failed run floors: the newest failure's time and run.
    failed: HashMap<String, (i64, String)>,
    /// Owner keys a success or a deletion clears, with its time.
    cleared: HashMap<String, i64>,
    run_cursor: Option<i64>,
    observation_cursor: Option<i64>,
}

impl ScopeDelta {
    fn touch(&mut self, run: &LiveRemoteRun) {
        self.touched.extend(run.asset_keys.iter().cloned());
        if run.action {
            self.reload.extend(run.asset_keys.iter().cloned());
        }
    }

    /// `run` ended: re-read its keys, and floor or clear them by its outcome.
    fn end(&mut self, run: &RunRecord, live: &LiveRemoteRun, partitioned: &HashSet<&str>) {
        self.touch(live);
        if live.action {
            return;
        }
        let ts = run.end_time.unwrap_or(run.start_time);
        for key in &live.asset_keys {
            if run.partition_key.is_some() && partitioned.contains(key.as_str()) {
                continue;
            }
            match run.status {
                RunStatus::Failure => {
                    let newest = self
                        .failed
                        .entry(key.clone())
                        .or_insert((ts, run.run_id.clone()));
                    if ts > newest.0 {
                        *newest = (ts, run.run_id.clone());
                    }
                }
                RunStatus::Success => self.clear(key.clone(), ts),
                _ => {}
            }
        }
    }

    fn clear(&mut self, key: String, ts: i64) {
        let newest = self.cleared.entry(key).or_insert(ts);
        *newest = (*newest).max(ts);
    }

    /// A failed run did not fail a key it materialized itself, and a
    /// whole-asset deletion clears older floors.
    async fn settle_floors<S: StorageBackend>(
        &mut self,
        storage: &S,
        scoped: &ScopedStorage<'_, S>,
        read: &RemoteRead,
    ) -> anyhow::Result<()> {
        if !self.failed.is_empty() {
            let keys: Vec<String> = self.failed.keys().cloned().collect();
            let mut runs: Vec<String> = self.failed.values().map(|(_, id)| id.clone()).collect();
            runs.sort_unstable();
            runs.dedup();
            let built = storage.materialized_by_runs(&keys, &runs).await?;
            let materialized: Vec<(String, i64)> = self
                .failed
                .iter()
                .filter(|(key, (_, run_id))| {
                    read.record(key).and_then(|r| r.last_run_id.as_deref()) == Some(run_id)
                        || built.get(*key).is_some_and(|runs| runs.contains(run_id))
                })
                .map(|(key, (ts, _))| (key.clone(), *ts))
                .collect();
            for (key, ts) in materialized {
                self.failed.remove(&key);
                self.clear(key, ts);
            }
        }

        if !self.reload.is_empty() {
            let deletion_ts = scoped.get_asset_deletion_timestamps().await?;
            let deleted: Vec<(String, i64)> = self
                .reload
                .iter()
                .filter_map(|key| Some((key.clone(), *deletion_ts.get(key)?)))
                .collect();
            for (key, ts) in deleted {
                self.clear(key, ts);
            }
        }
        Ok(())
    }
}

impl RemoteScope {
    async fn fetch_delta<S: StorageBackend>(
        &self,
        storage: &S,
        bindings: &HashMap<String, Vec<String>>,
        partitioned: &HashSet<&str>,
    ) -> anyhow::Result<ScopeDelta> {
        let mut delta = ScopeDelta::default();

        // Tracked runs first: once the cursor passes a run, this is the only
        // place its end shows. A missing row was a terminal run, deleted.
        if !self.live_runs.is_empty() {
            let ids: Vec<String> = self.live_runs.keys().cloned().collect();
            let runs = storage.get_runs_by_ids(&ids, None).await?;
            let found: HashMap<&str, &RunRecord> =
                runs.iter().map(|run| (run.run_id.as_str(), run)).collect();
            for (run_id, live) in &self.live_runs {
                match found.get(run_id.as_str()) {
                    Some(run) if !run_status_is_terminal(&run.status) => continue,
                    Some(run) => {
                        delta.applied.push((run.run_id.clone(), run.start_time));
                        delta.end(run, live, partitioned);
                    }
                    None => delta.touch(live),
                }
                delta.ended.insert(run_id.clone());
            }
        }

        let new_runs = storage
            .for_code_location(&self.ctx)
            .get_runs_since(self.run_cursor, None, SortOrder::Asc)
            .await?;
        for run in &new_runs {
            if delta.ended.contains(&run.run_id) {
                continue;
            }
            let Some(live) = LiveRemoteRun::of(run, bindings) else {
                continue;
            };
            if !run_status_is_terminal(&run.status) {
                if !self.live_runs.contains_key(&run.run_id) {
                    delta.started.push((run.run_id.clone(), live));
                }
            } else if !self.applied_run_ids.contains_key(&run.run_id) {
                if self.live_runs.contains_key(&run.run_id) {
                    delta.ended.insert(run.run_id.clone());
                }
                delta.applied.push((run.run_id.clone(), run.start_time));
                delta.end(run, &live, partitioned);
            }
        }
        delta.run_cursor = new_runs
            .iter()
            .map(|run| run.start_time)
            .max()
            .map(|newest| newest.saturating_sub(1));

        let observations = storage
            .get_observations_since(self.ctx.id(), self.observation_cursor)
            .await?;
        for key in observations.iter().filter_map(|e| e.asset_key.as_ref()) {
            if bindings.contains_key(key) {
                delta.touched.insert(key.clone());
            }
        }
        delta.observation_cursor = observations
            .iter()
            .map(|e| e.timestamp)
            .max()
            .map(|newest| newest.saturating_sub(1));

        Ok(delta)
    }
}

impl AssetConditionCache {
    /// Follow `location`'s runs and observations since the last tick.
    /// Returns `true` if anything changed. On a failed read nothing is
    /// applied, and the next tick reads the same span again.
    pub(super) async fn refresh_remote_scope<S: StorageBackend>(
        &mut self,
        storage: &S,
        location: &LocationRef,
    ) -> bool {
        let slot = &self.remote.slots[location];
        let Some(scope) = slot.state.scope() else {
            return false;
        };
        let partitioned = self.partitioned_remote_keys(slot);
        let fetched = async {
            let mut delta = scope
                .fetch_delta(storage, &slot.bindings, &partitioned)
                .await?;
            let scoped = storage.for_code_location(&scope.ctx);
            let read = self
                .read_remote_keys(&scoped, slot, &delta.touched, |key| {
                    delta.reload.contains(key)
                })
                .await?;
            delta.settle_floors(storage, &scoped, &read).await?;
            anyhow::Ok((delta, read))
        }
        .await;
        match fetched {
            Ok((delta, read)) => self.apply_scope_delta(location, delta, read),
            Err(error) => {
                tracing::warn!(
                    target: "rivers::conditions",
                    %location,
                    %error,
                    "reading remote runs failed; retrying"
                );
                false
            }
        }
    }

    fn apply_scope_delta(
        &mut self,
        location: &LocationRef,
        delta: ScopeDelta,
        read: RemoteRead,
    ) -> bool {
        let SlotState::Resolved { scope, .. } = &mut self.remote.slot_mut(location).state else {
            return false;
        };

        let mut changed = false;
        for run_id in &delta.ended {
            if let Some(run) = scope.live_runs.remove(run_id) {
                changed |= !run.action;
            }
        }
        let mut keyed_starts: HashSet<String> = HashSet::new();
        for (run_id, run) in delta.started {
            if !run.action {
                changed = true;
                if run.partition_key.is_some() {
                    keyed_starts.extend(run.asset_keys.iter().cloned());
                }
            }
            scope.live_runs.insert(run_id, run);
        }

        // Floors before clears: a clear older than the floor leaves it.
        for (key, (ts, _)) in delta.failed {
            let floor = scope.failed.entry(key).or_insert_with(|| {
                changed = true;
                ts
            });
            *floor = (*floor).max(ts);
        }
        for (key, ts) in delta.cleared {
            if scope.failed.get(&key).is_some_and(|&floor| floor <= ts) {
                scope.failed.remove(&key);
                changed = true;
            }
        }

        scope.applied_run_ids.extend(delta.applied);
        if let Some(cursor) = delta.run_cursor {
            scope.run_cursor = cursor;
        }
        let cursor = scope.run_cursor;
        scope.applied_run_ids.retain(|_, start| *start > cursor);
        if let Some(cursor) = delta.observation_cursor {
            scope.observation_cursor = cursor;
        }

        // Re-read keys get their live partitions in `apply_remote_read`.
        for key in keyed_starts.difference(&delta.touched) {
            changed |= self.add_live_partitions(location, key);
        }
        changed |= self.apply_remote_read(location, read);
        changed
    }
}
