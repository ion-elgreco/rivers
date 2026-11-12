use std::sync::Arc;

use pyo3::prelude::*;

use crate::config::run_config::checked_run_config;
use crate::errors::{AssetNotFoundError, ExecutionError};
use crate::executor::ops::now_ts;
use crate::job::PyJob;
use crate::partitions::{PyBackfillStrategy, PyPartitionKey, PyPartitionKeyRange};
use crate::runtime::{io_rt, rt};
use rivers_core::storage::{
    BackfillFailurePolicy, BackfillRecord, BackfillRunOutcome, BackfillStatus, LaunchedBy,
    PartitionKey, StorageBackend, UnstartedBackfillRun, tag_keys,
};

use super::*;

/// Default run priority for backfill-spawned runs (-10 = lower than regular runs at 0).
pub(crate) const DEFAULT_BACKFILL_PRIORITY: i32 = -10;

/// Check the requested keys of a backfill against `selection`: each value of
/// each dimension of a batched key one time, and dynamic values against storage.
fn validate_backfill_keys(
    state: &ResolvedState,
    selection: &[String],
    keys: &[PyPartitionKey],
    verb: Option<&str>,
) -> PyResult<()> {
    let mut dyn_checks: Vec<DynamicKeyCheck> = Vec::new();
    for key in keys {
        validate_partition_for_verb(state, selection.iter().map(String::as_str), Some(key), verb)?;
        dyn_checks.extend(dynamic_partition_checks(
            state,
            selection.iter().map(String::as_str),
            Some(key),
        ));
    }

    if !dyn_checks.is_empty() {
        rt().block_on(verify_dynamic_partition_keys(
            &state.storage,
            &state.code_location_id,
            &dyn_checks,
        ))?;
    }
    Ok(())
}

/// Plan the runs of a `Requested` backfill and move it to `InProgress`.
/// Returns its requested keys and one key per run, in run order. A backfill
/// whose keys name no partition is failed: no run can end it.
fn start_backfill(
    state: &ResolvedState,
    record: &BackfillRecord,
) -> PyResult<(Vec<PartitionKey>, Vec<PartitionKey>)> {
    let backfill_id = &record.backfill_id;
    let storage_err = |e: anyhow::Error| ExecutionError::new_err(format!("{e:#}"));

    let requested = rt()
        .block_on(state.storage.get_backfill_keys(backfill_id))
        .map_err(storage_err)?;
    let run_keys = rivers_core::execution::backfill::plan_runs(&record.strategy, &requested);

    if run_keys.is_empty() {
        let error = format!("Backfill '{backfill_id}' names no partition");
        io_rt()
            .block_on(state.storage.fail_backfill(backfill_id, &error))
            .map_err(storage_err)?;
        return Err(ExecutionError::new_err(error));
    }

    let started = io_rt()
        .block_on(state.storage.start_backfill(backfill_id, run_keys.len()))
        .map_err(storage_err)?;
    if !started {
        return Err(ExecutionError::new_err(format!(
            "Backfill '{backfill_id}' is no longer Requested"
        )));
    }
    Ok((requested, run_keys))
}

/// Kept out of `#[pymethods]` so they aren't exposed to Python. Daemon
/// dispatchers, subdaemons, and other pymethod bodies that have already
/// detached the GIL call these directly.
impl PyCodeRepository {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn backfill_inner(
        &self,
        target: crate::daemon::RunType,
        partition_keys: Option<Vec<PyPartitionKey>>,
        partition_range: Option<PyPartitionKeyRange>,
        strategy: Option<PyBackfillStrategy>,
        failure_policy: &str,
        max_concurrency: u32,
        tags: Option<Vec<(String, String)>>,
        config: Option<String>,
        block: bool,
        dry_run: bool,
        preminted_id: Option<String>,
        launched_by: rivers_core::storage::LaunchedBy,
        action: Option<String>,
    ) -> PyResult<PyBackfillResult> {
        let resolved = self.ensure_resolved()?;
        let state = &*resolved;
        let config = Python::attach(|py| {
            checked_run_config(py, config.as_deref(), &state.node_map, &state.resources)
        })?;

        // A `Job` target resolves to its own asset selection; everything below
        // (partition resolution, strategy, the record) is identical for both kinds.
        let (selection, job_name): (Vec<String>, Option<String>) = match target {
            crate::daemon::RunType::Materialization(sel) => (sel, None),
            crate::daemon::RunType::Job(name) => {
                let assets = state
                    .jobs_info
                    .get(&name)
                    .map(|j| j.asset_names.clone())
                    .ok_or_else(|| ExecutionError::new_err(format!("Job '{name}' not found")))?;
                (assets, Some(name))
            }
        };
        // A Job target runs its own verb: `action` is the one the caller showed
        // for it, or a rerun's recorded one.
        let action = match &job_name {
            Some(name) => {
                ensure_job_verb(&state.jobs_info, name, action.as_deref())?;
                None
            }
            None => action,
        };
        // An empty Materialization selection means "all assets" — expand it
        // so key/strategy validation sees the effective selection.
        let selection: Vec<String> = if selection.is_empty() {
            match &action {
                // An action backfill over "everything" means every asset
                // that defines the verb, mirroring `run_action`.
                Some(verb) => assets_supporting_action(&state.node_map, verb),
                None => {
                    let mut names: Vec<String> = state
                        .node_map
                        .iter()
                        .filter(|(_, node)| !node.is_remote())
                        .map(|(name, _)| name.clone())
                        .collect();
                    names.sort();
                    names
                }
            }
        } else {
            selection
        };
        ensure_materializable(&state.node_map, &selection)?;

        if let Some(verb) = &action {
            if selection.is_empty() {
                return Err(AssetNotFoundError::new_err(format!(
                    "No assets define action '{verb}'"
                )));
            }
            ensure_assets_support_action(&state.node_map, &selection, verb)?;
        }

        // Keys/ranges against an unpartitioned selection would bypass every
        // def-aware check (and PerDimension grouping would silently collapse
        // all keys into one run).
        if (partition_keys.is_some() || partition_range.is_some())
            && iter_partitioned_assets(&state.node_map, selection.iter().map(String::as_str))
                .is_empty()
        {
            return Err(ExecutionError::new_err(
                "Backfill partition_keys/partition_range require a partitioned selection; \
                 no asset in the selection is partitioned.",
            ));
        }

        // What the children execute: the selection's verb, or the job's own.
        let child_verb = action
            .clone()
            .or_else(|| job_action(&state.jobs_info, job_name.as_deref()));
        let resolved_keys: Vec<PyPartitionKey> = if let Some(keys) = partition_keys {
            if keys.is_empty() {
                return Err(ExecutionError::new_err("partition_keys must not be empty"));
            }
            keys
        } else if let Some(range) = partition_range {
            let selected = selection.as_slice();
            let parts_def = selected
                .iter()
                .filter_map(|name| state.node_map.get(name).and_then(|n| n.partitions_def()))
                .next()
                .or_else(|| {
                    state
                        .node_map
                        .values()
                        .filter_map(|n| n.partitions_def())
                        .next()
                })
                .ok_or_else(|| {
                    ExecutionError::new_err(
                        "partition_range specified but no partitioned assets found in selection",
                    )
                })?;
            // The range resolved against one def — the selection's other
            // partitioned assets must accept its keys too, checked below.
            vec![range.resolve(parts_def)?]
        } else {
            return Err(ExecutionError::new_err(
                "Either partition_keys or partition_range must be provided",
            ));
        };

        // repo.backfill() and gRPC LaunchBackfill are boundaries — reject
        // invalid keys here like `materialize` does, instead of persisting a
        // BackfillRecord that counts them and fails per-run later. The
        // children run the verb, so the keys must satisfy it.
        validate_backfill_keys(state, &selection, &resolved_keys, child_verb.as_deref())?;

        let core_keys: Vec<PartitionKey> = resolved_keys.iter().map(PartitionKey::from).collect();
        let num_partitions: usize = core_keys.iter().map(PartitionKey::member_count).sum();

        // Resolution: explicit > asset default > MultiRun.
        let resolved_strategy = if let Some(s) = strategy {
            s
        } else {
            let selected_names = selection.as_slice();
            let asset_strategies: Vec<PyBackfillStrategy> = selected_names
                .iter()
                .filter_map(|name| state.node_map.get(name)?.backfill_strategy())
                .collect();
            if !asset_strategies.is_empty()
                && asset_strategies.iter().all(|s| s == &asset_strategies[0])
            {
                asset_strategies.into_iter().next().unwrap()
            } else {
                PyBackfillStrategy::MultiRun {}
            }
        };
        validate_backfill_strategy(&resolved_strategy, state, &selection)?;

        let core_strategy = resolved_strategy.to_core();
        let num_runs =
            rivers_core::execution::backfill::plan_runs(&core_strategy, &core_keys).len();

        if dry_run {
            return Ok(PyBackfillResult {
                backfill_id: String::new(),
                num_partitions,
                num_runs,
                status: "dry_run".to_string(),
                completed: 0,
                failed: 0,
                canceled: 0,
                run_ids: Vec::new(),
                is_dry_run: true,
                requested_keys: resolved_keys,
            });
        }

        let backfill_id = preminted_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let fp = match failure_policy {
            "stop_on_failure" => BackfillFailurePolicy::StopOnFailure,
            _ => BackfillFailurePolicy::Continue,
        };
        let backfill_tags = tags.clone().unwrap_or_default();

        let record = BackfillRecord {
            backfill_id: backfill_id.clone(),
            code_location_id: state.code_location_id.clone(),
            status: BackfillStatus::Requested,
            strategy: core_strategy,
            failure_policy: fp.clone(),
            asset_selection: selection.clone(),
            job_name: job_name.clone(),
            num_partitions: num_partitions as u64,
            planned_runs: 0,
            run_ids: Vec::new(),
            completed_partitions: 0,
            failed_partitions: 0,
            canceled_partitions: 0,
            max_concurrency: max_concurrency as i64,
            tags: backfill_tags.clone(),
            create_time: now_ts(),
            end_time: None,
            error: None,
            launched_by,
            // The verb the children run — a Job target's own, not `None`.
            action: child_verb,
            config: config.clone(),
        };

        io_rt()
            .block_on(state.storage.create_backfill(&record, &core_keys))
            .map_err(|e| ExecutionError::new_err(format!("Failed to create backfill: {e:#}")))?;

        if block {
            self.execute_backfill_with(state, &backfill_id)?;

            let final_record = rt()
                .block_on(state.storage.get_backfill(&backfill_id))
                .map_err(|e| ExecutionError::new_err(format!("{e}")))?;

            let status = final_record
                .as_ref()
                .map(|r| format!("{:?}", r.status))
                .unwrap_or_else(|| "Unknown".to_string());
            let completed = final_record
                .as_ref()
                .map(|r| r.completed_partitions as usize)
                .unwrap_or(0);
            let failed = final_record
                .as_ref()
                .map(|r| r.failed_partitions as usize)
                .unwrap_or(0);
            let canceled = final_record
                .as_ref()
                .map(|r| r.canceled_partitions as usize)
                .unwrap_or(0);
            let run_ids = final_record
                .as_ref()
                .map(|r| r.run_ids.clone())
                .unwrap_or_default();

            Ok(PyBackfillResult {
                backfill_id,
                num_partitions,
                num_runs: run_ids.len(),
                status,
                completed,
                failed,
                canceled,
                run_ids,
                is_dry_run: false,
                requested_keys: resolved_keys,
            })
        } else {
            // Non-blocking: return immediately. A daemon picks up
            // Requested backfills via execute_backfill().
            Ok(PyBackfillResult {
                backfill_id,
                num_partitions,
                num_runs,
                status: "Requested".to_string(),
                completed: 0,
                failed: 0,
                canceled: 0,
                run_ids: Vec::new(),
                is_dry_run: false,
                requested_keys: resolved_keys,
            })
        }
    }

    /// Run one partition of a Job-targeted backfill with the job's *own* plan +
    /// executor: mint a Started run attributed to the job + backfill, then drive
    /// it via [`PyJob::execute_run`] — identical to how `execute_job` runs a
    /// single partition. The Materialization counterpart is
    /// [`Self::materialize_with_launcher`].
    #[allow(clippy::too_many_arguments)]
    fn execute_backfill_job_run(
        &self,
        state: &ResolvedState,
        job: &Py<PyJob>,
        job_name: &str,
        py_pk: PyPartitionKey,
        config: Option<String>,
        backfill_id: &str,
        run_id: String,
    ) -> PyResult<PyRunResult> {
        let run_id = io_rt().block_on(create_started_run(
            state,
            job_name,
            Some(&py_pk),
            LaunchedBy::Backfill {
                backfill_id: backfill_id.to_string(),
            },
            Some(run_id),
            config.clone(),
        ))?;
        Python::attach(|py| {
            job.get()
                .execute_stored_run(py, &run_id, Some(py_pk), config, false, false)
        })
    }

    pub(crate) fn execute_backfill_inner(&self, backfill_id: &str) -> PyResult<()> {
        let state = self.ensure_resolved()?;
        self.execute_backfill_with(&state, backfill_id)
    }

    fn execute_backfill_with(&self, state: &ResolvedState, backfill_id: &str) -> PyResult<()> {
        let record = rt()
            .block_on(state.storage.get_backfill(backfill_id))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?
            .ok_or_else(|| {
                ExecutionError::new_err(format!("Backfill '{backfill_id}' not found"))
            })?;

        if record.status != BackfillStatus::Requested {
            return Err(ExecutionError::new_err(format!(
                "Backfill '{backfill_id}' is {:?}, expected Requested",
                record.status
            )));
        }
        fail_backfill_if_job_verb_changed(state, &record)?;
        let (_, run_keys) = start_backfill(state, &record)?;

        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut flags = self.backfill_cancel_flags.lock().unwrap();
            flags.insert(backfill_id.to_string(), cancel.clone());
        }

        let mut stop = false;
        // Planned runs that end here, before they start.
        let mut unstarted: Vec<UnstartedBackfillRun> = Vec::new();

        for (run_number, run_key) in run_keys.iter().enumerate() {
            let member_count = run_key.member_count();
            let unstarted_as = |outcome: BackfillRunOutcome| UnstartedBackfillRun {
                run_number,
                member_count,
                outcome,
            };
            // A failed launch shows in the counts at once. The write at the
            // end repeats it, so an error here is not final.
            let launch_failed = || {
                let failed = unstarted_as(BackfillRunOutcome::Failed);
                let _ = io_rt().block_on(
                    state
                        .storage
                        .settle_unstarted_backfill_runs(backfill_id, &[failed]),
                );
                failed
            };

            if cancel.load(std::sync::atomic::Ordering::Relaxed) || stop {
                unstarted.push(unstarted_as(BackfillRunOutcome::Canceled));
                continue;
            }

            // Link the run id before the run exists so a cancel cascade can
            // always reach it; a rejected link means the backfill was
            // canceled under us — fold into the flag path.
            let run_id = uuid::Uuid::new_v4().to_string();
            match io_rt().block_on(state.storage.link_backfill_run(
                backfill_id,
                &run_id,
                run_number,
                member_count,
            )) {
                Ok(true) => {}
                Ok(false) => {
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    unstarted.push(unstarted_as(BackfillRunOutcome::Canceled));
                    continue;
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::repo",
                        backfill_id = %backfill_id,
                        error = %e,
                        "backfill run link failed"
                    );
                    unstarted.push(launch_failed());
                    continue;
                }
            }

            // Inherit backfill tags and default priority to -10 (lower than
            // scheduled runs) unless user explicitly set it. The backfill
            // origin is tracked via `LaunchedBy::Backfill`, not a tag.
            let mut run_tags: Vec<(String, String)> = record.tags.clone();
            if !run_tags.iter().any(|(k, _)| k == tag_keys::PRIORITY) {
                run_tags.push((
                    tag_keys::PRIORITY.to_string(),
                    DEFAULT_BACKFILL_PRIORITY.to_string(),
                ));
            }

            let batch_pk = PyPartitionKey::from(run_key);
            let run_config = record.config.clone();
            let result = match &record.job_name {
                Some(job_name) => match state.jobs.get(job_name) {
                    Some(job) => self.execute_backfill_job_run(
                        state,
                        job,
                        job_name,
                        batch_pk,
                        run_config,
                        backfill_id,
                        run_id.clone(),
                    ),
                    None => Err(ExecutionError::new_err(format!(
                        "Job '{job_name}' not found"
                    ))),
                },
                None => match &record.action {
                    // Child runs inherit the backfill's verb.
                    Some(verb) => self.run_action_with_launcher(
                        verb.clone(),
                        Some(record.asset_selection.clone()),
                        Some(batch_pk),
                        Some(run_tags.clone()),
                        false,
                        run_config,
                        Some(run_id.clone()),
                        false,
                        LaunchedBy::Backfill {
                            backfill_id: backfill_id.to_string(),
                        },
                    ),
                    None => self.materialize_with_launcher(
                        Some(record.asset_selection.clone()),
                        Some(batch_pk),
                        Some(run_tags.clone()),
                        false,
                        run_config,
                        Some(run_id.clone()),
                        false,
                        false,
                        None,
                        LaunchedBy::Backfill {
                            backfill_id: backfill_id.to_string(),
                        },
                    ),
                },
            };

            // A run that ended recorded its own outcome, and keeps it. One
            // that failed to launch has none.
            let succeeded = match result {
                Ok(run_result) => run_result.success,
                Err(_) => {
                    unstarted.push(launch_failed());
                    false
                }
            };

            if !succeeded && matches!(record.failure_policy, BackfillFailurePolicy::StopOnFailure) {
                stop = true;
            }
        }

        // An external cancel that stopped a run from starting wins over the
        // status that the ended runs give. It is written before those runs
        // get their outcome: after that, the backfill is complete.
        let canceled_a_run = unstarted
            .iter()
            .any(|run| run.outcome == BackfillRunOutcome::Canceled);
        if cancel.load(std::sync::atomic::Ordering::Relaxed)
            && canceled_a_run
            && let Err(e) = io_rt().block_on(state.storage.cancel_backfill(backfill_id))
        {
            tracing::error!(target: "rivers::repo", backfill_id = %backfill_id, error = %e, "backfill cancel failed");
        }

        // Transient errors retry inside the storage layer; a remaining error
        // is surfaced rather than swallowed (it would otherwise leave the
        // backfill InProgress).
        let finalize = io_rt().block_on(async {
            state
                .storage
                .settle_unstarted_backfill_runs(backfill_id, &unstarted)
                .await?;
            state.storage.try_complete_backfill(backfill_id).await
        });
        if let Err(e) = finalize {
            tracing::error!(target: "rivers::repo", backfill_id = %backfill_id, error = %e, "backfill finalize failed");
        }

        {
            let mut flags = self.backfill_cancel_flags.lock().unwrap();
            flags.remove(backfill_id);
        }

        Ok(())
    }

    pub(crate) fn execute_backfill_queued_inner(&self, backfill_id: &str) -> PyResult<()> {
        let resolved = self.ensure_resolved()?;
        let state = &*resolved;

        let record = rt()
            .block_on(state.storage.get_backfill(backfill_id))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?
            .ok_or_else(|| {
                ExecutionError::new_err(format!("Backfill '{backfill_id}' not found"))
            })?;

        if record.status != BackfillStatus::Requested {
            return Err(ExecutionError::new_err(format!(
                "Backfill '{backfill_id}' is {:?}, expected Requested",
                record.status
            )));
        }
        fail_backfill_if_job_verb_changed(state, &record)?;
        let (requested, run_keys) = start_backfill(state, &record)?;

        let submit = || -> PyResult<Vec<String>> {
            // The definitions can have changed since the backfill was created.
            let requested: Vec<PyPartitionKey> =
                requested.iter().map(PyPartitionKey::from).collect();
            validate_backfill_keys(
                state,
                &record.asset_selection,
                &requested,
                record.action.as_deref(),
            )?;

            let mut run_tags: Vec<(String, String)> = record.tags.clone();
            if !run_tags.iter().any(|(k, _)| k == tag_keys::PRIORITY) {
                run_tags.push((
                    tag_keys::PRIORITY.to_string(),
                    DEFAULT_BACKFILL_PRIORITY.to_string(),
                ));
            }

            let runs: Vec<RunSubmission> = run_keys
                .iter()
                .map(|run_key| RunSubmission {
                    selection: Some(record.asset_selection.clone()),
                    partition_key: Some(PyPartitionKey::from(run_key)),
                    tags: Some(run_tags.clone()),
                    job_name: record.job_name.clone(),
                    action: record.action.clone(),
                    config: record.config.clone(),
                })
                .collect();

            // The run_ids link is written by enqueue_backfill_runs in the
            // same transaction as the runs themselves.
            io_rt().block_on(submit_runs(
                state,
                runs,
                LaunchedBy::Backfill {
                    backfill_id: backfill_id.to_string(),
                },
            ))
        };

        match submit() {
            Ok(_) => Ok(()),
            Err(e) => {
                // Post-InProgress failure would otherwise strand the backfill
                // with no runs (storage-layer retries already absorbed
                // transients). If this write also fails, the monitor's
                // zero-run sweep re-queues the backfill instead.
                tracing::error!(
                    target: "rivers::repo",
                    backfill_id = %backfill_id,
                    error = %e,
                    "backfill run submission failed; marking backfill failed"
                );
                if let Err(mark_err) =
                    io_rt().block_on(state.storage.fail_backfill(backfill_id, &format!("{e}")))
                {
                    tracing::error!(
                        target: "rivers::repo",
                        backfill_id = %backfill_id,
                        error = %mark_err,
                        "failed to mark backfill failed"
                    );
                }
                Err(e)
            }
        }
    }
}
