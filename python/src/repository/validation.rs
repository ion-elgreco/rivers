use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;

use crate::automation::schedule::PyScheduleDefinition;
use crate::automation::sensor::PySensorDefinition;
use crate::errors::{
    AssetNotFoundError, ConfigurationError, ExecutionError, GraphValidationError,
    PartitionValidationError,
};
use crate::executor::ops::{enumerate_params, get_annotations, is_context_annotation};
use crate::partitions::{PartitionsDefinition, PyBackfillStrategy, PyPartitionKey};
use crate::runtime::io_rt;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{BackfillRecord, PartitionKey, StorageBackend};

use super::handle::{JobSummary, ResolvedState};
use super::resolved_node::ResolvedNode;

/// Walk a selection of asset names and yield `(name, partitions_def)`
/// for each asset that has one. Single source of truth for the iteration
/// pattern shared by `validate_partition_for_selection` and
/// `validate_job_partition_compatibility`. GIL-free — reads the cached
/// `PartitionsDefinition` value off the resolved node.
pub(super) fn iter_partitioned_assets<'a, I>(
    node_map: &'a HashMap<String, ResolvedNode>,
    asset_names: I,
) -> Vec<(&'a str, &'a PartitionsDefinition)>
where
    I: IntoIterator<Item = &'a str>,
{
    asset_names
        .into_iter()
        .filter_map(|name| {
            node_map
                .get(name)
                .and_then(|n| n.partitions_def())
                .map(|pd| (name, pd))
        })
        .collect()
}

/// Reject running `job_name` as `expected` (`None` is materialize) when the
/// job now runs another verb. A job runs its own verb, so a page that showed
/// an older one, or a record that stores it, must not start the new one. An
/// unknown job is left to the lookup that reports it.
pub(super) fn ensure_job_verb(
    jobs_info: &HashMap<String, JobSummary>,
    job_name: &str,
    expected: Option<&str>,
) -> PyResult<()> {
    let Some(job) = jobs_info.get(job_name) else {
        return Ok(());
    };
    let current = job.action.as_deref().unwrap_or("materialize");
    let expected = expected.unwrap_or("materialize");
    if current == expected {
        return Ok(());
    }
    Err(ExecutionError::new_err(format!(
        "job '{job_name}' now runs '{current}', not '{expected}'"
    )))
}

/// A picked-up job backfill runs the verb it recorded. If its job now runs
/// another one, fail the backfill (still `Requested`) instead of running it.
pub(super) fn fail_backfill_if_job_verb_changed(
    state: &ResolvedState,
    record: &BackfillRecord,
) -> PyResult<()> {
    let Some(job_name) = &record.job_name else {
        return Ok(());
    };
    let Err(e) = ensure_job_verb(&state.jobs_info, job_name, record.action.as_deref()) else {
        return Ok(());
    };
    if let Err(mark_err) = io_rt().block_on(
        state
            .storage
            .fail_backfill(&record.backfill_id, &format!("{e}")),
    ) {
        tracing::error!(
            target: "rivers::repo",
            backfill_id = %record.backfill_id,
            error = %mark_err,
            "failed to mark backfill failed"
        );
    }
    Err(e)
}

/// Assets defining `action`, sorted. Single source of truth for what
/// `selection=None` means for an action — a queued or Kubernetes-backed run
/// carries its asset list on the record, so "everything" has to be spelled out.
/// `node_map` is a HashMap, so the sort is what makes step order reproducible.
pub(super) fn assets_supporting_action(
    node_map: &HashMap<String, ResolvedNode>,
    action: &str,
) -> Vec<String> {
    let mut names: Vec<String> = node_map
        .iter()
        .filter(|(_, node)| node.supports_action(action))
        .map(|(name, _)| name.clone())
        .collect();
    names.sort();
    names
}

/// Reject submissions that either (a) omit a partition key when the
/// selection contains partitioned assets, or (b) supply a key that doesn't
/// match the asset's partition definition (wrong shape, out-of-range time
/// window, unknown static key, etc.).
///
/// Single source of truth, called from `submit_run`, `submit_runs`, and
/// `materialize_with_launcher` so the run-queue path fails synchronously
/// (otherwise a queued run goes nowhere when dequeued) and the direct path
/// gets the same message before any storage write.
pub(super) fn validate_partition_for_selection<'a>(
    state: &'a ResolvedState,
    asset_names: impl IntoIterator<Item = &'a str>,
    partition_key: Option<&PyPartitionKey>,
) -> PyResult<()> {
    validate_partition_for_verb(state, asset_names, partition_key, None)
}

/// `verb` names the action in the error message; `None` reads as materialize.
pub(super) fn validate_partition_for_verb<'a>(
    state: &'a ResolvedState,
    asset_names: impl IntoIterator<Item = &'a str>,
    partition_key: Option<&PyPartitionKey>,
    verb: Option<&str>,
) -> PyResult<()> {
    validate_partition_in_map(&state.node_map, asset_names, partition_key, verb)
}

/// Partitioning declared for `verb` on `name`'s resolved action.
/// Materialize (`verb` None) and unknown verbs read as `Required` — the
/// verb-support check rejects unknown verbs elsewhere.
fn action_partitioning(
    node_map: &HashMap<String, ResolvedNode>,
    name: &str,
    verb: Option<&str>,
) -> crate::assets::action::PyActionPartitioning {
    verb.and_then(|v| node_map.get(name)?.find_action(v))
        .map(|a| a.partitioning)
        .unwrap_or_default()
}

/// Resolve every selected name, rejecting unknown ones with the canonical
/// message. Callers that only validate existence discard the nodes.
pub(super) fn resolve_selection<'a>(
    node_map: &'a HashMap<String, ResolvedNode>,
    names: impl IntoIterator<Item = &'a String>,
) -> PyResult<Vec<&'a ResolvedNode>> {
    names
        .into_iter()
        .map(|name| {
            node_map.get(name).ok_or_else(|| {
                AssetNotFoundError::new_err(format!("Selection contains unknown asset: '{name}'"))
            })
        })
        .collect()
}

/// Every selected asset must exist and define `verb` — the shared boundary
/// check (gRPC RunAction, backfill verbs). A free function over the node map
/// so callers already holding the state don't read it again.
pub(super) fn ensure_assets_support_action(
    node_map: &HashMap<String, ResolvedNode>,
    selection: &[String],
    verb: &str,
) -> PyResult<()> {
    let nodes = resolve_selection(node_map, selection)?;
    for (name, node) in selection.iter().zip(nodes) {
        if !node.supports_action(verb) {
            return Err(GraphValidationError::new_err(format!(
                "Asset '{name}' does not define action '{verb}'"
            )));
        }
    }
    Ok(())
}

/// `validate_partition_for_verb` over a borrowed node map, for callers that
/// hold the map but not the whole `ResolvedState` (the job execution path).
pub(crate) fn validate_partition_in_map<'a>(
    node_map: &'a HashMap<String, ResolvedNode>,
    asset_names: impl IntoIterator<Item = &'a str>,
    partition_key: Option<&PyPartitionKey>,
    verb: Option<&str>,
) -> PyResult<()> {
    use crate::assets::action::PyActionPartitioning;
    let partitioned = iter_partitioned_assets(node_map, asset_names);

    let Some(pk) = partition_key else {
        // Keyless is only an error where the verb actually requires a key:
        // a `Keyless` verb (vacuum, observe) is whole-asset by declaration,
        // an `Optional` one (delete, optimize) covers the whole asset when
        // unkeyed.
        let requiring: Vec<&str> = partitioned
            .iter()
            .filter(|(n, _)| {
                action_partitioning(node_map, n, verb) == PyActionPartitioning::Required
            })
            .map(|(n, _)| *n)
            .collect();
        if requiring.is_empty() {
            return Ok(());
        }
        return Err(ExecutionError::new_err(format!(
            "Cannot run '{}' without partition_key: assets {:?} have partition \
             definitions. Provide a partition_key or exclude them from selection.",
            verb.unwrap_or("materialize"),
            requiring
        )));
    };

    for (name, pd) in &partitioned {
        if action_partitioning(node_map, name, verb) == PyActionPartitioning::Keyless {
            return Err(ExecutionError::new_err(format!(
                "Action '{}' is whole-asset on '{}': run it without a partition_key.",
                verb.unwrap_or("materialize"),
                name
            )));
        }
        if !pd.validate_partition_key(pk)? {
            return Err(ExecutionError::new_err(format!(
                "Invalid partition_key '{}' for asset '{}': not a member of its \
                 partition definition.",
                PartitionKey::from(pk).to_display(),
                name
            )));
        }
    }
    Ok(())
}

/// Reject a keyless run of `verb` that acts on every partition of an asset —
/// one where the verb's key is `Optional` — for callers that must choose the
/// whole asset explicitly. The gRPC boundary is one: a UI that could not build
/// a key sends none either. The built-in observe changes no data.
pub(super) fn ensure_whole_asset_chosen<'a>(
    node_map: &'a HashMap<String, ResolvedNode>,
    asset_names: impl IntoIterator<Item = &'a str>,
    verb: &str,
) -> PyResult<()> {
    use crate::assets::action::{ActionOutcome, PyActionPartitioning};
    let whole: Vec<&str> = iter_partitioned_assets(node_map, asset_names)
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| {
            node_map[*name].find_action(verb).is_some_and(|a| {
                a.partitioning == PyActionPartitioning::Optional
                    && a.outcome != ActionOutcome::Observe
            })
        })
        .collect();
    if whole.is_empty() {
        return Ok(());
    }
    Err(ExecutionError::new_err(format!(
        "Action '{verb}' without partition_key runs on every partition of assets \
         {whole:?}. Pick a partition, or choose the whole asset (whole_asset)."
    )))
}

/// Storage-side companion to [`validate_partition_for_selection`]: a Dynamic
/// def can't validate membership statically (its keys live in storage), so
/// collect the `(asset, namespace, keys)` triples to verify against
/// `dynamic_partitions` in storage.
pub(super) type DynamicKeyCheck = (String, String, Vec<String>);

pub(super) fn dynamic_partition_checks<'a, I>(
    state: &'a ResolvedState,
    asset_names: I,
    partition_key: Option<&PyPartitionKey>,
) -> Vec<DynamicKeyCheck>
where
    I: IntoIterator<Item = &'a str>,
{
    let Some(pk) = partition_key else {
        return Vec::new();
    };
    iter_partitioned_assets(&state.node_map, asset_names)
        .into_iter()
        .flat_map(|(name, pd)| {
            collect_dynamic_namespaces(pd, pk)
                .into_iter()
                .map(|(ns, keys)| (name.to_string(), ns, keys))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `(namespace, key values)` pairs a key must have registered in storage to
/// be valid for `pd` — the Dynamic def itself and any Dynamic dimension of a
/// Multi def. Shape mismatches are already rejected by
/// `validate_partition_key`, so unmatched shapes yield nothing here.
fn collect_dynamic_namespaces(
    pd: &PartitionsDefinition,
    pk: &PyPartitionKey,
) -> Vec<(String, Vec<String>)> {
    match (pd, pk) {
        (PartitionsDefinition::Dynamic { name }, PyPartitionKey::Single { key }) => {
            vec![(name.clone(), key.clone())]
        }
        (PartitionsDefinition::Multi { dimensions }, PyPartitionKey::Multi { keys }) => dimensions
            .iter()
            .filter_map(|(dim, dim_def)| match dim_def {
                PartitionsDefinition::Dynamic { name } => {
                    keys.get(dim).map(|vals| (name.clone(), vals.clone()))
                }
                _ => None,
            })
            .collect(),
        (_, PyPartitionKey::Set { keys }) => keys
            .iter()
            .flat_map(|k| collect_dynamic_namespaces(pd, k))
            .collect(),
        _ => Vec::new(),
    }
}

/// PerDimension only groups what it can see: an unknown dimension name (or a
/// non-Multi def, whose keys carry no dimensions at all) matches nothing, so
/// every key would get the same empty group key and the whole backfill would
/// silently collapse into ONE run. Reject the strategy against the
/// selection's partition defs instead.
pub(super) fn validate_backfill_strategy(
    strategy: &PyBackfillStrategy,
    state: &ResolvedState,
    selection: &[String],
) -> PyResult<()> {
    let PyBackfillStrategy::PerDimension {
        multi_run_dims,
        single_run_dims,
    } = strategy
    else {
        return Ok(());
    };
    // Same invariants as BackfillStrategy.per_dimension() — strategies built
    // from the proto path skip the constructor, and an empty multi_run list
    // would collapse the whole backfill into a single run.
    if multi_run_dims.is_empty() {
        return Err(ExecutionError::new_err(
            "multi_run must contain at least one dimension",
        ));
    }
    if single_run_dims.is_empty() {
        return Err(ExecutionError::new_err(
            "single_run must contain at least one dimension",
        ));
    }
    for dim in multi_run_dims {
        if single_run_dims.contains(dim) {
            return Err(ExecutionError::new_err(format!(
                "dimension '{dim}' cannot be in both multi_run and single_run"
            )));
        }
    }
    let partitioned =
        iter_partitioned_assets(&state.node_map, selection.iter().map(String::as_str));
    for (asset, pd) in &partitioned {
        let PartitionsDefinition::Multi { dimensions } = pd else {
            return Err(ExecutionError::new_err(format!(
                "BackfillStrategy.per_dimension requires Multi-partitioned assets; \
                 asset '{asset}' is not Multi-partitioned"
            )));
        };
        for dim in multi_run_dims.iter().chain(single_run_dims.iter()) {
            if !dimensions.iter().any(|(name, _)| name == dim) {
                return Err(ExecutionError::new_err(format!(
                    "BackfillStrategy.per_dimension references dimension '{dim}', \
                     which is not a dimension of asset '{asset}'"
                )));
            }
        }
    }
    Ok(())
}

/// Storage-backed membership check: returns the `(asset, ns, key)` triples
/// that are NOT registered. Storage failures propagate — an unreachable
/// store says nothing about whether a key is retired.
pub(super) async fn unregistered_dynamic_keys(
    storage: &SurrealStorage,
    code_location_id: &str,
    checks: &[DynamicKeyCheck],
) -> PyResult<Vec<(String, String, String)>> {
    let ctx = rivers_core::storage::CodeLocationContext::new(code_location_id);
    let scoped = storage.for_code_location(&ctx);
    let mut missing = Vec::new();
    for (asset, ns, keys) in checks {
        for key in keys {
            let known = scoped.has_dynamic_partition(ns, key).await.map_err(|e| {
                ExecutionError::new_err(format!("Failed to check dynamic partition '{key}': {e}"))
            })?;
            if !known {
                missing.push((asset.clone(), ns.clone(), key.clone()));
            }
        }
    }
    Ok(missing)
}

pub(super) async fn verify_dynamic_partition_keys(
    storage: &SurrealStorage,
    code_location_id: &str,
    checks: &[DynamicKeyCheck],
) -> PyResult<()> {
    let missing = unregistered_dynamic_keys(storage, code_location_id, checks).await?;
    if let Some((asset, ns, key)) = missing.first() {
        return Err(ExecutionError::new_err(format!(
            "Invalid partition_key '{key}' for asset '{asset}': not a registered \
             dynamic partition key of namespace '{ns}'. Register it with \
             add_dynamic_partitions(\"{ns}\", [...]) first."
        )));
    }
    Ok(())
}

/// Reject a user-defined job whose partitioned assets can never share a
/// single partition_key. Folds `PartitionsDefinition::intersect` across
/// every partitioned asset; on failure the assets and the per-kind reason
/// (cadence mismatch, disjoint keys, etc.) get surfaced at `repo.resolve()`
/// instead of at every execute click.
///
/// Only enforced on user-defined `Job`s. `repo.materialize(selection=...)`
/// builds an ephemeral plan per call and isn't subject to this check —
/// callers can pick any compatible asset subset.
pub(super) fn validate_job_partition_compatibility(
    job_name: &str,
    asset_names: &[String],
    node_map: &HashMap<String, ResolvedNode>,
    verb: Option<&str>,
) -> PyResult<()> {
    use crate::assets::action::PyActionPartitioning;
    let mut partitioned = iter_partitioned_assets(node_map, asset_names.iter().map(String::as_str));

    // A run has one key. A verb that is whole-asset on one target and needs a
    // key on another can never run as one job.
    let rule = |name: &str| action_partitioning(node_map, name, verb);
    let keyless: Vec<&str> = partitioned
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| rule(n) == PyActionPartitioning::Keyless)
        .collect();
    let required: Vec<&str> = partitioned
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| rule(n) == PyActionPartitioning::Required)
        .collect();
    if let (Some(verb), false, false) = (verb, keyless.is_empty(), required.is_empty()) {
        return Err(PartitionValidationError::new_err(format!(
            "Job '{job_name}' runs '{verb}' whole-asset on {keyless:?} but needs a key on \
             {required:?}: one run can't do both — split the job."
        )));
    }
    // Only a key-taking run needs one key valid for every target; a job whose
    // verb never requires a key can always run keyless.
    if required.is_empty() {
        return Ok(());
    }
    partitioned.retain(|(n, _)| rule(n) != PyActionPartitioning::Keyless);

    if partitioned.len() < 2 {
        return Ok(());
    }

    let (first_name, first_pd) = &partitioned[0];
    let mut acc: PartitionsDefinition = (*first_pd).clone();
    let mut acc_assets: Vec<&str> = vec![*first_name];

    for (name, pd) in &partitioned[1..] {
        match acc.intersect(pd) {
            Ok(intersected) => {
                acc = intersected;
                acc_assets.push(*name);
            }
            Err(reason) => {
                return Err(PartitionValidationError::new_err(format!(
                    "Job '{}' has incompatible partition definitions: assets \
                     {:?} intersect, but adding '{}' fails: {}.",
                    job_name, acc_assets, name, reason
                )));
            }
        }
    }
    Ok(())
}

/// Reject any non-context, non-upstream parameter on a node that doesn't
/// reference a known resource key.
pub(super) fn validate_resource_references(
    py: Python,
    node_map: &HashMap<String, ResolvedNode>,
    resource_keys: &HashSet<&String>,
    composition_task_names: &HashSet<String>,
) -> PyResult<()> {
    let node_names: HashSet<&str> = node_map.keys().map(|s| s.as_str()).collect();

    for (node_name, node) in node_map {
        // Tasks whose deps come from composition bindings have positional
        // params that don't match asset/resource names — skip them.
        if composition_task_names.contains(node_name) {
            continue;
        }

        // BashTask / ExternalAsset without observe_fn have no callable to
        // introspect — nothing to validate.
        let func = match node {
            ResolvedNode::BashTask(_) => continue,
            _ => match node.annotations(py)? {
                Some(_) => node.callable(py)?,
                None => continue,
            },
        };

        let mut is_first_param = true;
        for (param_name, annotation) in enumerate_params(py, &func)? {
            if param_name == "return" || param_name == "self" {
                continue;
            }

            let is_ctx = annotation
                .as_ref()
                .is_some_and(|a| is_context_annotation(py, a));

            if is_first_param {
                is_first_param = false;
                if is_ctx || (param_name == "context" && !node_names.contains(param_name.as_str()))
                {
                    continue;
                }
            } else if is_ctx {
                continue; // context in wrong position — execute_step will catch this
            }

            if node_names.contains(param_name.as_str()) {
                continue;
            }

            if resource_keys.contains(&param_name) {
                continue;
            }

            let available: Vec<&str> = resource_keys.iter().map(|s| s.as_str()).collect();
            return Err(ConfigurationError::new_err(format!(
                "Node '{}': parameter '{}' does not match any upstream asset or resource. \
                 Available resources: {:?}",
                node_name, param_name, available
            )));
        }
    }

    Ok(())
}

pub(super) fn validate_schedule_sensor_resource_references(
    py: Python,
    schedules: &HashMap<String, Py<PyScheduleDefinition>>,
    sensors: &HashMap<String, Py<PySensorDefinition>>,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    for (name, schedule) in schedules {
        let schedule_ref = schedule.borrow(py);
        if let Some(ref eval_fn) = schedule_ref.evaluation_fn {
            validate_eval_fn_resources(py, eval_fn, name, "Schedule", resource_keys)?;
        }
    }

    for (name, sensor) in sensors {
        let sensor_ref = sensor.borrow(py);
        if let Some(ref eval_fn) = sensor_ref.evaluation_fn {
            validate_eval_fn_resources(py, eval_fn, name, "Sensor", resource_keys)?;
        }
    }

    Ok(())
}

/// Sensors must declare *some* run target — either `job_name` or
/// `asset_selection`. Without one, a tick has nothing to dispatch against
/// and the daemon would silently fail every fired RunRequest.
///
/// Also rejects targets that don't exist in the resolved repo: a typo'd
/// `job_name` or `asset_selection` entry would otherwise resolve fine
/// and produce stuck queued runs (Queued mode) or logged-not-surfaced
/// errors (Direct mode) at every tick. The check fires once at
/// resolve time so the failure mode is "your repo doesn't define X"
/// instead of silent dispatch failure forever.
pub(super) fn validate_sensor_run_targets(
    py: Python,
    sensors: &HashMap<String, Py<PySensorDefinition>>,
    asset_names: &HashSet<&str>,
    job_names: &HashSet<&str>,
) -> PyResult<()> {
    for (name, sensor) in sensors {
        let s = sensor.borrow(py);
        // A run-status sensor launches nothing; only its watched jobs must exist.
        if s.monitored_status.is_some() {
            for job_name in s.monitored_jobs.iter().flatten() {
                if !job_names.contains(job_name.as_str()) {
                    return Err(crate::errors::SensorDefinitionError::new_err(format!(
                        "Sensor '{}' monitors unknown job '{}'. Define the job and \
                         add it to `CodeRepository(jobs=...)`.",
                        name, job_name
                    )));
                }
            }
            continue;
        }
        let has_job = s.job_name.as_ref().is_some_and(|j| !j.is_empty());
        let has_selection = s.asset_selection.as_ref().is_some_and(|a| !a.is_empty());
        if !has_job && !has_selection {
            return Err(crate::errors::SensorDefinitionError::new_err(format!(
                "Sensor '{}' must declare either `job_name` or `asset_selection` — \
                 the daemon has no target to dispatch against.",
                name
            )));
        }

        if let Some(job_name) = s.job_name.as_ref().filter(|j| !j.is_empty())
            && !job_names.contains(job_name.as_str())
        {
            return Err(crate::errors::SensorDefinitionError::new_err(format!(
                "Sensor '{}' references unknown job '{}'. Define the job and \
                 add it to `CodeRepository(jobs=...)`.",
                name, job_name
            )));
        }

        if let Some(selection) = s.asset_selection.as_ref() {
            for asset in selection {
                if !asset_names.contains(asset.as_str()) {
                    return Err(crate::errors::SensorDefinitionError::new_err(format!(
                        "Sensor '{}' references unknown asset '{}' in \
                         `asset_selection`.",
                        name, asset
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Schedules carry a mandatory `job_name`; reject schedules whose
/// target isn't a user-defined job in this repo. Same motivation as
/// [`validate_sensor_run_targets`]'s existence check — a typo'd
/// `job_name` would otherwise produce stuck/failed runs at every
/// cron tick.
pub(super) fn validate_schedule_run_targets(
    py: Python,
    schedules: &HashMap<String, Py<PyScheduleDefinition>>,
    job_names: &HashSet<&str>,
) -> PyResult<()> {
    for (name, schedule) in schedules {
        let s = schedule.borrow(py);
        if !job_names.contains(s.job_name.as_str()) {
            return Err(crate::errors::ScheduleDefinitionError::new_err(format!(
                "Schedule '{}' references unknown job '{}'. Define the job \
                 and add it to `CodeRepository(jobs=...)`.",
                name, s.job_name
            )));
        }
    }
    Ok(())
}

fn validate_eval_fn_resources(
    py: Python,
    eval_fn: &Py<PyAny>,
    name: &str,
    kind: &str,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    let annotations = get_annotations(py, eval_fn)?;

    let mut is_first = true;
    for (k, v) in annotations.iter() {
        let param_name: String = k.extract()?;
        if param_name == "return" {
            continue;
        }

        if is_first {
            is_first = false;
            if is_context_annotation(py, &v) || param_name == "context" {
                continue;
            }
        }

        if resource_keys.contains(&param_name) {
            continue;
        }

        let available: Vec<&str> = resource_keys.iter().map(|s| s.as_str()).collect();
        return Err(ConfigurationError::new_err(format!(
            "{} '{}': parameter '{}' does not match any known resource. \
             Available resources: {:?}",
            kind, name, param_name, available
        )));
    }

    Ok(())
}
