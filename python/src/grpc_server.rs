//! gRPC CodeLocation server — exposes repository operations to the UI via tonic.
//!
//! Each RPC handler runs Python work on a dedicated OS thread (`Python::try_attach`),
//! returning the result through a oneshot channel. `run_on_python` consolidates this
//! plumbing so handlers only contain their actual logic.
use pyo3::prelude::*;
use rivers_api::rivers::code_location_service_server::{
    CodeLocationService, CodeLocationServiceServer,
};
use rivers_api::rivers::*;
use tonic::{Request, Response, Status};

use std::collections::HashMap;
use std::sync::Arc;

use crate::automation::schedule::TickRequest;
use crate::automation::{PyScheduleStatus, PySensorStatus};
use crate::daemon::{BackfillDispatcherKind, RunDispatcherKind};
use crate::executor::Executor;
use crate::gil_threads::GilThreads;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::key_range::{
    DimensionSelection, PartitionKeyRangeInner, PyPartitionKeyRange,
};
use crate::partitions::{PartitionsDefinition, PyPartitionKey};
use crate::repository::{PyCodeRepository, RepoHandle, ResolvedState};

const PY_UNAVAILABLE: &str = "Python interpreter not available";

pub struct CodeLocationImpl {
    repo: Py<PyCodeRepository>,
    /// Non-py twin of `repo`. Used for GIL-free reads (e.g. expanding
    /// empty asset selection in `materialize`) and as the handle the
    /// dispatchers were constructed with.
    handle: RepoHandle,
    /// Routes `ExecuteJob` requests through the same Direct/Queued seam
    /// the daemon uses for schedule/sensor ticks.
    run_dispatcher: Arc<RunDispatcherKind>,
    /// Routes `LaunchBackfill` through the same dispatcher the daemon's
    /// schedule/sensor and condition loops use.
    backfill_dispatcher: Arc<BackfillDispatcherKind>,
    /// `run_on_python` workers and dispatched runs for this server, drained when
    /// it stops. See [`crate::gil_threads`].
    gil_threads: GilThreads,
}

impl CodeLocationImpl {
    #[allow(clippy::result_large_err)]
    fn clone_repo(&self) -> Result<Py<PyCodeRepository>, Status> {
        Python::try_attach(|py| self.repo.clone_ref(py))
            .ok_or_else(|| Status::internal(PY_UNAVAILABLE))
    }

    /// Run `f` on a dedicated OS thread with the GIL held, awaiting its
    /// result. See [`crate::runtime`] for why we use `std::thread::spawn`
    /// rather than `tokio::task::spawn_blocking` here.
    async fn run_on_python<F, R>(&self, f: F) -> Result<R, Status>
    where
        F: FnOnce(Python<'_>, Py<PyCodeRepository>) -> Result<R, String> + Send + 'static,
        R: Send + 'static,
    {
        let repo = self.clone_repo()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.gil_threads.spawn(move || {
            let result = Python::try_attach(|py| f(py, repo));
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| Status::internal("thread panicked"))?
            .ok_or_else(|| Status::internal(PY_UNAVAILABLE))?
            .map_err(Status::internal)
    }
}

#[tonic::async_trait]
impl CodeLocationService for CodeLocationImpl {
    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.ping")]
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            status: "ok".to_string(),
            location: String::new(),
        }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_info")]
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        let asset_names = self
            .handle
            .asset_names()
            .map_err(|e| Status::internal(e.to_string()))?;
        let job_names = self.handle.job_names();
        Ok(Response::new(GetInfoResponse {
            name: self.handle.code_location_id().unwrap_or_default(),
            asset_names,
            job_names,
        }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.materialize")]
    async fn materialize(
        &self,
        request: Request<MaterializeRequest>,
    ) -> Result<Response<MaterializeResponse>, Status> {
        let req = request.into_inner();
        let pk = req.partition_key.and_then(proto_partition_key_to_py);
        let pk_core = pk.as_ref().map(rivers_core::storage::PartitionKey::from);
        let tags = proto_tags_to_pairs(req.tags).unwrap_or_default();

        // Empty selection means "materialize everything" — expand at the
        // gRPC boundary so the dispatcher always receives an explicit
        // asset list and can stamp it on the `RunRecord`.
        let asset_selection = if req.selection.is_empty() {
            self.handle
                .asset_names()
                .map_err(|e| Status::internal(e.to_string()))?
        } else {
            let sel = req.selection;
            self.handle
                .validate_assets_exist(&sel)
                .map_err(|e| Status::internal(e.to_string()))?;
            sel
        };

        // The dispatcher trusts its caller to validate — for gRPC this
        // is the system boundary, so reject bad selection / partition
        // here. Otherwise dispatch_materialization's fire-and-forget
        // thread (Direct) or queued record (Queued) would swallow the
        // error and leave the caller with a run_id pointing at a stuck
        // or never-started run.
        self.handle
            .validate_partition_for_selection(&asset_selection, pk.as_ref())
            .map_err(|e| Status::internal(e.to_string()))?;

        let mat_request = crate::daemon::MaterializationRequestData {
            run_id: uuid::Uuid::new_v4().to_string(),
            asset_selection,
            partition_key: pk_core,
            tags,
            launched_by: rivers_core::storage::LaunchedBy::Manual,
        };
        let run_id = mat_request.run_id.clone();
        let status = self.run_dispatcher.mode_label().to_string();

        let mut outcome = self
            .run_dispatcher
            .dispatch_materialization(&[mat_request])
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(err) = outcome.errors.pop() {
            return Err(Status::internal(err.to_string()));
        }

        Ok(Response::new(MaterializeResponse { run_id, status }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.execute_job")]
    async fn execute_job(
        &self,
        request: Request<ExecuteJobRequest>,
    ) -> Result<Response<ExecuteJobResponse>, Status> {
        let req = request.into_inner();
        let run_request = crate::daemon::RunRequestData {
            run_key: None,
            tags: None,
            partition_key: req.partition_key.and_then(proto_partition_key_to_py),
            job_name: Some(req.job_name),
        };

        let mut outcome = self
            .run_dispatcher
            .dispatch_jobs(&[run_request], rivers_core::storage::LaunchedBy::Manual)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(err) = outcome.errors.pop() {
            return Err(Status::internal(err.to_string()));
        }
        let run_id = outcome
            .ids
            .pop()
            .expect("dispatch_jobs returned no id for a single non-empty input");
        Ok(Response::new(ExecuteJobResponse {
            success: true,
            run_id,
        }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_partition_keys")]
    async fn get_partition_keys(
        &self,
        request: Request<GetPartitionKeysRequest>,
    ) -> Result<Response<GetPartitionKeysResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .run_on_python(move |_py, repo| {
                let repo_ref = repo.get();
                let guard = repo_ref.state.read().unwrap();
                let state = guard.as_ref().ok_or("repository not resolved")?;
                let target = resolve_partition_target(state, &req.asset_key, &req.dimension)?;
                // Empty query = browse (cheap total + window); non-empty = filter
                // (page the matches; `total` is the match count).
                let (keys, total) = if req.query.is_empty() {
                    let total = target.partition_count() as u64;
                    let keys = target
                        .get_partition_keys_window(req.offset as usize, req.limit as usize)
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .map(py_partition_key_display)
                        .collect();
                    (keys, total)
                } else {
                    let (keys, total) = target
                        .get_partition_keys_filtered(
                            &req.query,
                            req.offset as usize,
                            req.limit as usize,
                        )
                        .map_err(|e| e.to_string())?;
                    (keys, total as u64)
                };
                Ok(GetPartitionKeysResponse { keys, total })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(
        skip_all,
        target = "rivers::grpc",
        name = "grpc.get_partition_key_index"
    )]
    async fn get_partition_key_index(
        &self,
        request: Request<GetPartitionKeyIndexRequest>,
    ) -> Result<Response<GetPartitionKeyIndexResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .run_on_python(move |_py, repo| {
                let repo_ref = repo.get();
                let guard = repo_ref.state.read().unwrap();
                let state = guard.as_ref().ok_or("repository not resolved")?;
                let target = resolve_partition_target(state, &req.asset_key, &req.dimension)?;
                let index = target
                    .single_dim_key_index(&req.key)
                    .map_err(|e| e.to_string())?
                    .map(|i| i as i64)
                    .unwrap_or(-1);
                Ok(GetPartitionKeyIndexResponse { index })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_assets_info")]
    async fn get_assets_info(
        &self,
        _request: Request<GetAssetsInfoRequest>,
    ) -> Result<Response<GetAssetsInfoResponse>, Status> {
        // TODO(ion): benchmark if having a prefetched summary is worth it for assets as well
        let resp = self
            .run_on_python(|py, repo| {
                let repo_ref = repo.get();
                let guard = repo_ref.state.read().unwrap();
                let state = guard.as_ref().ok_or("repository not resolved")?;

                let mut assets = Vec::new();
                for (name, node) in &state.node_map {
                    let partition_def = node.partitions_def().map(node_partition_def_info);

                    let mut hooks = Vec::new();
                    for h in node.success_hooks() {
                        hooks.push(HookInfo {
                            hook_type: "success".to_string(),
                            function_name: h.borrow(py).resolve_name().to_string(),
                        });
                    }
                    for h in node.failure_hooks() {
                        hooks.push(HookInfo {
                            hook_type: "failure".to_string(),
                            function_name: h.borrow(py).resolve_name().to_string(),
                        });
                    }

                    let automation_condition =
                        if let crate::repository::resolved_node::ResolvedNode::Asset(asset_node) =
                            node
                        {
                            asset_node.automation_condition.as_ref().map(|c| {
                                c.label.clone().unwrap_or_else(|| {
                                    crate::automation::condition::description(&c.node)
                                })
                            })
                        } else {
                            None
                        };

                    assets.push(AssetDefinitionInfo {
                        asset_key: name.clone(),
                        description: None,
                        partition_def,
                        hooks,
                        io_handler: node.has_io_handler(py).then(|| "custom".to_string()),
                        has_self_dependency: false,
                        is_external: node.is_external(),
                        automation_condition,
                        tags: node.tags().unwrap_or_default(),
                        kinds: node.kinds(),
                        group: node.group(),
                        code_version: node.code_version(),
                        asset_type: node.asset_type().to_string(),
                    });
                }

                Ok(GetAssetsInfoResponse { assets })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_schedules")]
    async fn get_schedules(
        &self,
        _request: Request<GetSchedulesRequest>,
    ) -> Result<Response<GetSchedulesResponse>, Status> {
        let schedules = self
            .handle
            .list_schedules()
            .into_iter()
            .map(|s| {
                let status = match s.default_status {
                    PyScheduleStatus::Running => "RUNNING",
                    PyScheduleStatus::Stopped => "STOPPED",
                };
                ScheduleInfo {
                    name: s.name,
                    cron_schedule: s.cron_schedule,
                    job_name: s.job_name,
                    status: status.to_string(),
                    timezone: s.timezone,
                    description: s.description,
                    tags: tags_to_proto(s.tags.as_ref()),
                }
            })
            .collect();
        Ok(Response::new(GetSchedulesResponse { schedules }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_sensors")]
    async fn get_sensors(
        &self,
        _request: Request<GetSensorsRequest>,
    ) -> Result<Response<GetSensorsResponse>, Status> {
        let sensors = self
            .handle
            .list_sensors()
            .into_iter()
            .map(|s| {
                let status = match s.default_status {
                    PySensorStatus::Running => "RUNNING",
                    PySensorStatus::Stopped => "STOPPED",
                };
                SensorInfo {
                    name: s.name,
                    job_name: s.job_name,
                    status: status.to_string(),
                    minimum_interval: s.minimum_interval,
                    description: s.description,
                    asset_selection: s.asset_selection.unwrap_or_default(),
                    tags: tags_to_proto(s.tags.as_ref()),
                }
            })
            .collect();
        Ok(Response::new(GetSensorsResponse { sensors }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_jobs")]
    async fn get_jobs(
        &self,
        _request: Request<GetJobsRequest>,
    ) -> Result<Response<GetJobsResponse>, Status> {
        // TODO(ion): check if prefetched is worth it or not
        let jobs = self
            .handle
            .list_jobs()
            .into_iter()
            .map(|j| {
                let executor_type = j
                    .executor
                    .as_ref()
                    .map(|e| match e {
                        Executor::InProcess {} => "InProcess".to_string(),
                        Executor::Parallel { max_workers, .. } => {
                            format!("Parallel({})", max_workers)
                        }
                        Executor::Kubernetes { worker_image, .. } => {
                            format!("Kubernetes({})", worker_image.as_deref().unwrap_or("auto"))
                        }
                    })
                    .unwrap_or_default();
                JobInfo {
                    name: j.name,
                    asset_selection: j.node_names,
                    executor_type,
                }
            })
            .collect();
        Ok(Response::new(GetJobsResponse { jobs }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.evaluate_schedule")]
    async fn evaluate_schedule(
        &self,
        request: Request<EvaluateScheduleRequest>,
    ) -> Result<Response<EvaluateScheduleResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .run_on_python(move |py, repo| {
                let repo_ref = repo.get();
                let tick_result = repo_ref
                    .evaluate_schedule(py, &req.schedule_name, None)
                    .map_err(|e| e.to_string())?;
                Ok(EvaluateScheduleResponse {
                    run_ids: collect_run_ids(py, &tick_result.run_requests),
                    skip_reason: tick_result
                        .skip_reason
                        .map(|sr| sr.borrow(py).message.clone()),
                })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.evaluate_sensor")]
    async fn evaluate_sensor(
        &self,
        request: Request<EvaluateSensorRequest>,
    ) -> Result<Response<EvaluateSensorResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .run_on_python(move |py, repo| {
                let repo_ref = repo.get();
                let tick_result = repo_ref
                    .evaluate_sensor(py, &req.sensor_name, None, None)
                    .map_err(|e| e.to_string())?;
                Ok(EvaluateSensorResponse {
                    run_ids: collect_run_ids(py, &tick_result.run_requests),
                    skip_reason: tick_result
                        .skip_reason
                        .map(|sr| sr.borrow(py).message.clone()),
                    cursor: tick_result.cursor,
                })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.observe_asset")]
    async fn observe_asset(
        &self,
        request: Request<ObserveAssetRequest>,
    ) -> Result<Response<ObserveAssetResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .run_on_python(move |py, repo| {
                repo.get()
                    .observe(py, Some(vec![req.asset_key]))
                    .map_err(|e| e.to_string())?;
                Ok(ObserveAssetResponse {
                    success: true,
                    error: None,
                })
            })
            .await?;
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.launch_backfill")]
    async fn launch_backfill(
        &self,
        request: Request<LaunchBackfillRequest>,
    ) -> Result<Response<LaunchBackfillResponse>, Status> {
        let req = request.into_inner();
        let partition_keys = empty_to_none(req.partition_keys).map(|ks| {
            ks.into_iter()
                .filter_map(proto_partition_key_to_py)
                .collect()
        });
        let partition_range = req.partition_range.and_then(proto_partition_range_to_py);
        let strategy = req.strategy.and_then(proto_strategy_to_py);
        let failure_policy = if req.failure_policy.is_empty() {
            None
        } else {
            Some(req.failure_policy)
        };
        let tags = proto_tags_to_pairs(req.tags)
            .map(|pairs| pairs.into_iter().collect::<HashMap<String, String>>());

        let backfill_request = crate::daemon::BackfillRequestData {
            selection: req.selection,
            partition_keys,
            partition_range,
            strategy,
            failure_policy,
            max_concurrency: req.max_concurrency,
            tags,
            dry_run: req.dry_run,
        };

        let mut outcome = self
            .backfill_dispatcher
            .dispatch(&[backfill_request])
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(err) = outcome.errors.pop() {
            return Err(Status::internal(err.to_string()));
        }
        let result = outcome
            .results
            .pop()
            .expect("dispatch returned no result for a single non-empty input");

        Ok(Response::new(LaunchBackfillResponse {
            backfill_id: result.backfill_id,
            num_partitions: result.num_partitions as u32,
            num_runs: result.num_runs as u32,
            is_dry_run: result.is_dry_run,
            status: result.status,
        }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.rerun_backfill")]
    async fn rerun_backfill(
        &self,
        request: Request<RerunBackfillRequest>,
    ) -> Result<Response<RerunBackfillResponse>, Status> {
        let req = request.into_inner();
        let backfill_request = self
            .handle
            .build_rerun_request(&req.backfill_id, req.dry_run)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut outcome = self
            .backfill_dispatcher
            .dispatch(&[backfill_request])
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(err) = outcome.errors.pop() {
            return Err(Status::internal(err.to_string()));
        }
        let result = outcome
            .results
            .pop()
            .expect("dispatch returned no result for a single non-empty input");

        Ok(Response::new(RerunBackfillResponse {
            backfill_id: result.backfill_id,
            num_partitions: result.num_partitions as u32,
            num_runs: result.num_runs as u32,
            is_dry_run: result.is_dry_run,
            status: result.status,
        }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.get_backfill_status")]
    async fn get_backfill_status(
        &self,
        request: Request<GetBackfillStatusRequest>,
    ) -> Result<Response<GetBackfillStatusResponse>, Status> {
        let req = request.into_inner();
        let status = self
            .handle
            .get_backfill(&req.backfill_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        match status {
            Some(s) => Ok(Response::new(GetBackfillStatusResponse {
                backfill_id: s.backfill_id,
                status: s.status,
                total_partitions: s.total_partitions as u32,
                completed_partitions: s.completed_partitions as u32,
                failed_partitions: s.failed_partitions as u32,
                canceled_partitions: s.canceled_partitions as u32,
                run_ids: s.run_ids,
                error: s.error,
            })),
            None => Err(Status::not_found(format!(
                "Backfill '{}' not found",
                req.backfill_id
            ))),
        }
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.cancel_backfill")]
    async fn cancel_backfill(
        &self,
        request: Request<CancelBackfillRequest>,
    ) -> Result<Response<CancelBackfillResponse>, Status> {
        let req = request.into_inner();
        let success = self
            .handle
            .cancel_backfill(req.backfill_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CancelBackfillResponse { success }))
    }

    #[tracing::instrument(skip_all, target = "rivers::grpc", name = "grpc.cancel_run")]
    async fn cancel_run(
        &self,
        request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        let req = request.into_inner();
        let success = self
            .handle
            .cancel_run(&req.run_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CancelRunResponse { success }))
    }
}

// --- Helpers ---

fn empty_to_none<T>(v: Vec<T>) -> Option<Vec<T>> {
    if v.is_empty() { None } else { Some(v) }
}

fn proto_tags_to_pairs(tags: Vec<Tag>) -> Option<Vec<(String, String)>> {
    if tags.is_empty() {
        None
    } else {
        Some(tags.into_iter().map(|t| (t.key, t.value)).collect())
    }
}

fn tags_to_proto(tags: Option<&HashMap<String, String>>) -> Vec<Tag> {
    tags.map(|t| {
        t.iter()
            .map(|(k, v)| Tag {
                key: k.clone(),
                value: v.clone(),
            })
            .collect()
    })
    .unwrap_or_default()
}

fn collect_run_ids(py: Python<'_>, run_requests: &[TickRequest]) -> Vec<String> {
    run_requests
        .iter()
        .filter_map(|tr| {
            tr.as_run().map(|r| {
                r.borrow(py)
                    .run_key
                    .clone()
                    .unwrap_or_else(|| "pending".to_string())
            })
        })
        .collect()
}

/// Resolve the `PartitionsDefinition` a partition-keys request targets: the
/// asset's own def, or — when `dimension` is non-empty — that dimension's
/// sub-definition of a Multi. Shared by `get_partition_keys` /
/// `get_partition_key_index` so the lookup + error strings live in one place.
fn resolve_partition_target<'a>(
    state: &'a ResolvedState,
    asset_key: &str,
    dimension: &str,
) -> Result<&'a PartitionsDefinition, String> {
    let node = state
        .node_map
        .get(asset_key)
        .ok_or_else(|| format!("asset '{asset_key}' not found"))?;
    let pd = node
        .partitions_def()
        .ok_or_else(|| format!("asset '{asset_key}' is not partitioned"))?;
    if dimension.is_empty() {
        Ok(pd)
    } else {
        pd.dimension_def(dimension)
            .ok_or_else(|| format!("asset '{asset_key}' has no partition dimension '{dimension}'"))
    }
}

/// For Multi, surface per-dimension keys so the UI can render one selector per
/// dimension instead of the cartesian-product enumeration. Static and TimeWindow
/// populate the flat `keys` list. Dynamic returns Err from `get_partition_keys`
/// — both lists stay empty and the UI hides the picker.
fn node_partition_def_info(pd: &PartitionsDefinition) -> PartitionDefInfo {
    // Max keys shipped inline. Beyond this the UI pages via the windowed API,
    // so the payload stays bounded no matter how many partitions exist.
    const KEYS_WINDOW: usize = 1000;
    let window = |d: &PartitionsDefinition| {
        d.get_partition_keys_window(0, KEYS_WINDOW)
            .ok()
            .map(|pks| {
                pks.into_iter()
                    .map(py_partition_key_display)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let total = pd.partition_count() as u64;
    let (kind, keys, dimensions, keys_truncated) = match pd {
        // Static and TimeWindow are both single-dim: a windowed `keys` list,
        // truncated when shorter than the total.
        PartitionsDefinition::Static { .. } | PartitionsDefinition::TimeWindow { .. } => {
            let k = window(pd);
            let trunc = (k.len() as u64) < total;
            (pd.variant_name(), k, vec![], trunc)
        }
        PartitionsDefinition::Multi { dimensions } => {
            // Each dimension's keys are windowed independently; flag truncation
            // if any dimension was capped (the flat `keys` list is empty here).
            let mut trunc = false;
            let dims = dimensions
                .iter()
                .map(|(name, dim_def)| {
                    let k = window(dim_def);
                    let dim_total = dim_def.partition_count();
                    let dim_trunc = k.len() < dim_total;
                    if dim_trunc {
                        trunc = true;
                    }
                    PartitionDimensionInfo {
                        name: name.clone(),
                        keys: k,
                        total_count: dim_total as u64,
                        keys_truncated: dim_trunc,
                    }
                })
                .collect();
            ("Multi", vec![], dims, trunc)
        }
        PartitionsDefinition::Dynamic { .. } => ("Dynamic", vec![], vec![], false),
    };
    // Dynamic keys are storage-managed; ship the namespace so the UI can source
    // the real count + keys from storage (def-level `total_count` is 0 here).
    let dynamic_name = match pd {
        PartitionsDefinition::Dynamic { name } => name.clone(),
        _ => String::new(),
    };
    PartitionDefInfo {
        kind: kind.to_string(),
        keys,
        dimensions,
        total_count: total,
        keys_truncated,
        dynamic_name,
    }
}

/// Render a `PyPartitionKey` for display (and round-trip) in the UI.
/// `Single` keys collapse to their string; `Multi` keys are encoded as
/// `dim=val|dim=val` so the UI can split them deterministically.
fn py_partition_key_display(pk: PyPartitionKey) -> String {
    match pk {
        PyPartitionKey::Single { key } => key.first().cloned().unwrap_or_default(),
        PyPartitionKey::Multi { keys } => {
            let mut entries: Vec<(String, Vec<String>)> = keys.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries
                .into_iter()
                .map(|(d, vs)| format!("{}={}", d, vs.join(",")))
                .collect::<Vec<_>>()
                .join("|")
        }
    }
}

fn proto_partition_key_to_py(pk: ProtoPartitionKey) -> Option<PyPartitionKey> {
    match pk.kind? {
        proto_partition_key::Kind::Single(s) => Some(PyPartitionKey::Single { key: s.keys }),
        proto_partition_key::Kind::Multi(m) => {
            let keys: HashMap<String, Vec<String>> =
                m.dimensions.into_iter().map(|d| (d.name, d.keys)).collect();
            Some(PyPartitionKey::Multi { keys })
        }
    }
}

fn proto_partition_range_to_py(r: PartitionRange) -> Option<PyPartitionKeyRange> {
    match r.kind? {
        partition_range::Kind::Single(s) => Some(PyPartitionKeyRange {
            inner: PartitionKeyRangeInner::Single {
                from_key: s.from_key,
                to_key: s.to_key,
            },
        }),
        partition_range::Kind::Multi(m) => {
            let mut dims = HashMap::new();
            for d in m.dimensions {
                let sel = match d.selection? {
                    dimension_range::Selection::Range(r) => DimensionSelection::Range {
                        from_key: r.from_key,
                        to_key: r.to_key,
                    },
                    dimension_range::Selection::Keys(k) => {
                        DimensionSelection::Keys(k.keys.into_iter().collect())
                    }
                };
                dims.insert(d.name, sel);
            }
            Some(PyPartitionKeyRange {
                inner: PartitionKeyRangeInner::Multi { dimensions: dims },
            })
        }
    }
}

fn proto_strategy_to_py(s: BackfillStrategyProto) -> Option<PyBackfillStrategy> {
    match s.kind? {
        backfill_strategy_proto::Kind::Shorthand(s) => match s.as_str() {
            "single_run" => Some(PyBackfillStrategy::SingleRun {}),
            _ => Some(PyBackfillStrategy::MultiRun {}),
        },
        backfill_strategy_proto::Kind::PerDimension(pd) => Some(PyBackfillStrategy::PerDimension {
            multi_run_dims: pd.multi_run_dimensions,
            single_run_dims: pd.single_run_dimensions,
        }),
    }
}

pub(crate) async fn start_grpc_server(
    repo: Py<PyCodeRepository>,
    handle: RepoHandle,
    run_dispatcher: Arc<RunDispatcherKind>,
    backfill_dispatcher: Arc<BackfillDispatcherKind>,
    gil_threads: GilThreads,
    host: String,
    port: u16,
    port_tx: std::sync::mpsc::Sender<u16>,
    server_cancel: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let service = CodeLocationImpl {
        repo,
        handle,
        run_dispatcher,
        backfill_dispatcher,
        gil_threads,
    };

    tracing::trace!(target: "rivers::dbg::grpc", host = %host, port, "start_grpc_server: ENTER, finding port");
    let listener = crate::net::find_available_port(&host, port, port + 99).await?;
    let actual_addr = listener.local_addr()?;
    tracing::trace!(target: "rivers::dbg::grpc", %actual_addr, "start_grpc_server: bound, sending port");
    let _ = port_tx.send(actual_addr.port());

    // tonic-health: mark SERVING on startup
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<CodeLocationServiceServer<CodeLocationImpl>>()
        .await;

    // Phase 1 (drain): mark NOT_SERVING so K8s readiness probe fails and LB removes pod
    let drain = crate::shutdown::drain_token().clone();
    tokio::spawn(async move {
        drain.cancelled().await;
        health_reporter
            .set_not_serving::<CodeLocationServiceServer<CodeLocationImpl>>()
            .await;
        tracing::info!(target: "rivers::grpc", "gRPC health set to NOT_SERVING");
    });

    tracing::trace!(target: "rivers::dbg::grpc", %actual_addr, "start_grpc_server: handing off to tonic Server::builder().serve_with_incoming_shutdown");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    // `server_cancel` is a child of `shutdown_token()`, so it fires both
    // on graceful SIGTERM and when a fresh `_start_grpc_server` call
    // supersedes us (common across tests).
    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(CodeLocationServiceServer::new(service))
        .serve_with_incoming_shutdown(incoming, async move {
            server_cancel.cancelled().await;
        })
        .await?;
    tracing::trace!(target: "rivers::dbg::grpc", %actual_addr, "start_grpc_server: EXIT (server stopped)");

    Ok(())
}
