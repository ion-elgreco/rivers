use std::collections::HashSet;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::config::ResourceVariant;
use crate::config::run_config::checked_run_config;
use crate::errors::{AssetNotFoundError, ExecutionError};
use crate::executor::Executor;
use crate::job::PyJob;
use crate::partitions::PyPartitionKey;
use crate::runtime::{io_rt, rt};
use rivers_core::storage::{LaunchedBy, StorageBackend};

use super::*;

impl PyCodeRepository {
    /// The repo-wide retry default with any registry name resolved; errors on
    /// an unknown name.
    pub(super) fn resolved_default_retry(
        &self,
    ) -> PyResult<Option<rivers_core::execution::retry::RetryPolicy>> {
        use rivers_core::execution::retry::RetryRef;
        match &self.raw_default_retry {
            None => Ok(None),
            Some(RetryRef::Inline(p)) => Ok(Some(p.clone())),
            Some(RetryRef::Named(key)) => match self.raw_retries.get(key) {
                Some(p) => Ok(Some(p.clone())),
                None => Err(crate::errors::ConfigurationError::new_err(format!(
                    "unknown retry policy '{key}' in default_retry_policy; registered: {:?}",
                    self.raw_retries.keys().collect::<Vec<_>>()
                ))),
            },
        }
    }

    pub(super) fn effective_executor(&self) -> Executor {
        if crate::executor::in_step_pod() {
            return Executor::InProcess {};
        }
        self.default_executor.clone().unwrap_or(Executor::Parallel {
            max_workers: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            max_async_concurrent: None,
        })
    }

    /// `job_name=None` records the run as ad-hoc (asset-selection only — no
    /// user-defined `Job`).
    pub(crate) async fn submit_run(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<&PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        launched_by: LaunchedBy,
        job_name: Option<String>,
        config: Option<String>,
    ) -> PyResult<PyRunHandle> {
        self.handle()
            .submit_run(
                selection,
                partition_key,
                tags,
                launched_by,
                job_name,
                config,
            )
            .await
    }

    /// Build a non-py [`RepoHandle`] sharing this repo's resolved state.
    /// Cheap (one Arc clone + bool copy); call once at dispatcher startup
    /// while holding the GIL, then use the handle from any thread.
    pub(crate) fn handle(&self) -> RepoHandle {
        RepoHandle {
            state: self.state.clone(),
            backfill_cancel_flags: Arc::clone(&self.backfill_cancel_flags),
        }
    }

    pub(crate) fn has_run_queue(&self) -> bool {
        self.run_queue_config.is_some()
    }

    /// Shared tail of the two launcher paths: reuse the caller's run id (a
    /// queue- or dispatcher-created record) or mint one, write the record when
    /// it's new, then execute the prepared plan. The job carries the verb.
    #[allow(clippy::too_many_arguments)]
    fn launch_prepared_job(
        &self,
        state: &ResolvedState,
        synthetic_job: PyJob,
        run_id_override: Option<String>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        config: Option<String>,
        launched_by: LaunchedBy,
        resume: bool,
        raise_on_error: bool,
    ) -> PyResult<PyRunResult> {
        // Only an override can name an existing record — a minted UUID can't,
        // so skip the storage read (and do it on the io runtime).
        let (run_id, existing) = match run_id_override {
            Some(id) => {
                let existing = io_rt().block_on(state.storage.get_run(&id)).unwrap_or(None);
                (id, existing)
            }
            None => (uuid::Uuid::new_v4().to_string(), None),
        };
        let tags_vec = tags.unwrap_or_default();
        if existing.is_none() {
            let core_pk = partition_key.as_ref().map(|pk| pk.into());
            let asset_selection: Vec<String> = synthetic_job.asset_names();
            io_rt().block_on(create_materialization_run(
                state,
                asset_selection,
                core_pk,
                tags_vec.clone(),
                launched_by,
                run_id.clone(),
                synthetic_job.action.clone(),
                config.clone(),
            ))?;
        }

        Python::attach(|py| {
            synthetic_job.run_inner(
                py,
                run_id,
                crate::executor::run_lifecycle::RunInit::Existing,
                partition_key,
                tags_vec,
                config,
                resume,
                raise_on_error,
            )
        })
    }

    /// Internal counterpart of the pymethod `materialize`, accepting an
    /// explicit `LaunchedBy` so internal callers (backfill executor, condition
    /// daemon) can stamp the run origin. The pymethod version delegates here
    /// with `LaunchedBy::Manual`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn materialize_with_launcher(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<String>,
        run_id_override: Option<String>,
        include_upstream: bool,
        resume: bool,
        retry: Option<rivers_core::execution::retry::RetryRef>,
        launched_by: LaunchedBy,
    ) -> PyResult<PyRunResult> {
        let resolved = self.ensure_resolved()?;
        let state = &*resolved;
        let config = Python::attach(|py| {
            checked_run_config(py, config.as_deref(), &state.node_map, &state.resources)
        })?;
        let graph = state
            .inner_repo
            .graph
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Graph not resolved"))?;

        // Job::validate_and_build_plan auto-includes graph task names and
        // collect-step virtual nodes, so we only need to compute the
        // externally-visible selection here.
        let mut selected_names: HashSet<String> = if let Some(ref sel) = selection {
            for name in sel {
                match state.node_map.get(name) {
                    None => {
                        return Err(AssetNotFoundError::new_err(format!(
                            "Selection contains unknown asset: '{name}'"
                        )));
                    }
                    Some(node) if node.is_external() && !node.is_observable_external() => {
                        return Err(ExecutionError::new_err(format!(
                            "Cannot materialize external asset without observe function: '{name}'"
                        )));
                    }
                    _ => {}
                }
            }
            sel.iter().cloned().collect()
        } else {
            state
                .node_map
                .iter()
                .filter(|(_, node)| !node.is_external() || node.is_observable_external())
                .map(|(name, _)| name.clone())
                .collect()
        };

        if include_upstream && selection.is_some() {
            let expanded = rivers_core::assets::graph::upstream_closure(graph, &selected_names);
            selected_names = expanded
                .into_iter()
                .filter(|name| {
                    state
                        .node_map
                        .get(name)
                        .map(|n| !n.is_external() || n.is_observable_external())
                        .unwrap_or(false)
                })
                .collect();
        };

        validate_partition_for_selection(
            state,
            selected_names.iter().map(String::as_str),
            partition_key.as_ref(),
        )?;
        let dyn_checks = dynamic_partition_checks(
            state,
            selected_names.iter().map(String::as_str),
            partition_key.as_ref(),
        );
        if !dyn_checks.is_empty() {
            rt().block_on(verify_dynamic_partition_keys(
                &state.storage,
                &state.code_location_id,
                &dyn_checks,
            ))?;
        }

        // allow_incomplete_deps keeps the permissive "load missing upstream
        // from io_handler" semantics materialize has always offered (Job's
        // strict completeness check is too strict for ad-hoc selections).
        let synthetic_job = PyJob::new_synthetic(
            selected_names.into_iter().collect(),
            self.effective_executor(),
            true,
            retry,
        );
        self.launch_synthetic_job(
            state,
            graph,
            synthetic_job,
            true,
            run_id_override,
            partition_key,
            tags,
            config,
            launched_by,
            resume,
            raise_on_error,
        )
    }

    /// Shared tail of the two launchers: configure the synthetic job for this
    /// repo, build its plan, and launch. Materialize resolves asset-level
    /// retry defaults after the build; an action declares its own retry, so
    /// its synthetic job deliberately skips that pass.
    #[allow(clippy::too_many_arguments)]
    fn launch_synthetic_job(
        &self,
        state: &ResolvedState,
        graph: &rivers_core::assets::graph::AssetGraph,
        mut synthetic_job: PyJob,
        resolve_retry_defaults: bool,
        run_id_override: Option<String>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        config: Option<String>,
        launched_by: LaunchedBy,
        resume: bool,
        raise_on_error: bool,
    ) -> PyResult<PyRunResult> {
        Python::attach(|py| {
            synthetic_job.configure_for_repo(
                py,
                &state.storage,
                &state.code_location_id,
                &state.resources,
                &state.io_handler_registry,
                &self.raw_retries,
            );
        });
        synthetic_job.validate_and_build_plan(
            graph,
            &state.node_map,
            &state.step_kinds,
            &state.multi_asset_groups,
            &state.composition_order,
            &self.raw_retries,
        )?;
        if resolve_retry_defaults {
            synthetic_job.resolve_retry_ref(&self.raw_retries)?;
            synthetic_job.fill_retry_defaults(self.resolved_default_retry()?.as_ref());
        }

        self.launch_prepared_job(
            state,
            synthetic_job,
            run_id_override,
            partition_key,
            tags,
            config,
            launched_by,
            resume,
            raise_on_error,
        )
    }

    /// Shared body of `run_action` — selection resolution + validation, an
    /// action-plan synthetic job, and a run record carrying the verb.
    /// `run_id_override` follows the same seam contract as
    /// `materialize_with_launcher`: a pre-linked id (backfill children) is
    /// reused so cancellation observed before start skips execution.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run_action_with_launcher(
        &self,
        action: String,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<String>,
        run_id_override: Option<String>,
        resume: bool,
        launched_by: LaunchedBy,
    ) -> PyResult<PyRunResult> {
        let resolved = self.ensure_resolved()?;
        let state = &*resolved;
        let config = Python::attach(|py| {
            checked_run_config(py, config.as_deref(), &state.node_map, &state.resources)
        })?;
        let graph = state
            .inner_repo
            .graph
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Graph not resolved"))?;

        let selected_names: Vec<String> = match selection {
            Some(sel) => {
                resolve_selection(&state.node_map, &sel)?;
                sel
            }
            None => assets_supporting_action(&state.node_map, &action),
        };
        if selected_names.is_empty() {
            return Err(AssetNotFoundError::new_err(format!(
                "No assets define action '{action}'"
            )));
        }

        validate_partition_for_verb(
            state,
            selected_names.iter().map(String::as_str),
            partition_key.as_ref(),
            Some(&action),
        )?;
        let dyn_checks = dynamic_partition_checks(
            state,
            selected_names.iter().map(String::as_str),
            partition_key.as_ref(),
        );
        if !dyn_checks.is_empty() {
            rt().block_on(verify_dynamic_partition_keys(
                &state.storage,
                &state.code_location_id,
                &dyn_checks,
            ))?;
        }

        let mut synthetic_job =
            PyJob::new_synthetic(selected_names, self.effective_executor(), true, None);
        // The job is the single carrier of the verb: the run record reads it
        // in launch_prepared_job, so record and plan can never disagree.
        synthetic_job.action = Some(action);
        self.launch_synthetic_job(
            state,
            graph,
            synthetic_job,
            false,
            run_id_override,
            partition_key,
            tags,
            config,
            launched_by,
            resume,
            raise_on_error,
        )
    }

    /// Logs errors instead of propagating them.
    pub(super) fn teardown_resources(&self, py: Python) {
        if let Some(state) = self.state.get_attached(py) {
            for (key, resource) in &state.resources {
                if let ResourceVariant::Resource(inner) = resource {
                    let obj = inner.bind(py);
                    if obj.hasattr("teardown").unwrap_or(false)
                        && let Err(e) = inner.call_method0(py, "teardown")
                    {
                        tracing::warn!(target: "rivers::resources", resource = %key, error = %e, "resource teardown failed");
                    }
                }
            }
        }
    }
}
