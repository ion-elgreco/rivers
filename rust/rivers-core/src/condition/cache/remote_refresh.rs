//! Per-tick refresh of a resolved remote location: its runs and
//! observations since the last tick.

use std::collections::{HashMap, HashSet};

use crate::assets::remote::LocationRef;
use crate::storage::{RunRecord, SortOrder, StorageBackend};

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
}

impl RemoteScope {
    async fn fetch_delta<S: StorageBackend>(
        &self,
        storage: &S,
        bindings: &HashMap<String, Vec<String>>,
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
                    Some(run) => delta.applied.push((run.run_id.clone(), run.start_time)),
                    None => {}
                }
                delta.ended.insert(run_id.clone());
                delta.touch(live);
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
                delta.touch(&live);
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
        let SlotState::Resolved { scope, .. } = &slot.state else {
            return false;
        };
        let fetched = async {
            let delta = scope.fetch_delta(storage, &slot.bindings).await?;
            let read = self
                .read_remote_keys(
                    &storage.for_code_location(&scope.ctx),
                    slot,
                    &delta.touched,
                    |key| delta.reload.contains(key),
                )
                .await?;
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
