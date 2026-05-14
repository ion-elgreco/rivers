use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube_client::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube_client::{Api, ResourceExt};
use kube_runtime::controller::Action;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{RunOutcome, RunStatus, StorageBackend};
use rivers_k8s::crd::code_location::CodeLocation;
use rivers_k8s::crd::run::{CONDITION_EXECUTOR_READY, Run, RunCondition, RunCrdStatus, RunPhase};

use super::cancel;
use super::cleanup;
use super::pod_builder::build_executor_pod;
use super::progress;
use super::restart;
use super::timeout;
use crate::codelocation::DirectoryState;

const FINALIZER: &str = "rivers.io/run-cleanup";
const SHORT_REQUEUE: Duration = Duration::from_secs(1);
const MEDIUM_REQUEUE: Duration = Duration::from_secs(5);
const PROGRESS_POLL: Duration = Duration::from_secs(10);
const MAX_CONDITIONS: usize = 20;

pub struct Context {
    pub client: kube_client::Client,
    pub namespace: String,
    pub storage: Arc<SurrealStorage>,
    /// Shared CodeLocation cache fed by `codelocation::run_watcher`. The
    /// run reconciler reads `spec.env` from here on every Pending → Running
    /// transition; on cache miss (startup window before the watcher syncs)
    /// `fetch_cl_env` falls back to a live API GET.
    pub directory: Arc<DirectoryState>,
    /// SurrealDB connection bundle stamped onto every Run pod the operator
    /// creates.
    pub surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube_client::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Storage error: {0}")]
    Storage(#[from] anyhow::Error),
}

/// Dispatches on `status.phase` and adds a finalizer up front so the
/// deletion-timestamp branch can run cleanup before the CR vanishes.
pub async fn reconcile(run: Arc<Run>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = run.name_any();
    let ns = run.namespace().unwrap_or(ctx.namespace.clone());

    let runs_api: Api<Run> = Api::namespaced(ctx.client.clone(), &ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);

    if run.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&runs_api, &pods_api, &run, &name, &ctx).await;
    }

    if !run.finalizers().iter().any(|f| f == FINALIZER) {
        add_finalizer(&runs_api, &name).await?;
        return Ok(Action::requeue(SHORT_REQUEUE));
    }

    let phase = run.status.as_ref().and_then(|s| s.phase.as_ref());

    match phase {
        None => reconcile_init(&runs_api, &run, &name).await,
        Some(RunPhase::Pending) => reconcile_pending(&runs_api, &pods_api, &ctx, &run, &name).await,
        Some(RunPhase::Running) => reconcile_running(&runs_api, &pods_api, &run, &name, &ctx).await,
        Some(RunPhase::Cancelling) => {
            cancel::reconcile_cancelling(&runs_api, &pods_api, &run, &name, &ctx).await
        }
        Some(phase) if phase.is_terminal() => Ok(Action::await_change()),
        Some(_) => Ok(Action::await_change()),
    }
}

/// Look up a Run's owning CodeLocation and return its `spec.env`. Reads from
/// the operator's shared cache (populated by `codelocation::run_watcher`)
/// and falls back to a live API GET on miss — covers the brief startup
/// window before the watcher's initial sync. Errors (CR missing, no RBAC,
/// transient API failure) bubble up so the reconciler requeues; we never
/// want to launch a run pod with a partial env list and silently lose
/// user-declared `secretKeyRef`s.
pub(crate) async fn fetch_cl_env(
    ctx: &Context,
    run: &Run,
) -> Result<Vec<k8s_openapi::api::core::v1::EnvVar>, Error> {
    let namespace = run.namespace().unwrap_or_else(|| ctx.namespace.clone());
    let cl_name = &run.spec.code_location_ref.name;
    if let Some(spec) = ctx.directory.lookup_spec(&namespace, cl_name).await {
        return Ok(spec.env.clone());
    }
    let cls_api: Api<CodeLocation> = Api::namespaced(ctx.client.clone(), &namespace);
    let cl = cls_api.get(cl_name).await?;
    Ok(cl.spec.env)
}

/// Phase-specific retry tuning lives inside `reconcile()`; this is the
/// catch-all 30s requeue for the kube controller-runtime.
pub fn error_policy(run: Arc<Run>, error: &Error, _ctx: Arc<Context>) -> Action {
    tracing::error!(run = %run.name_any(), %error, "reconcile error");
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_init(runs_api: &Api<Run>, run: &Run, name: &str) -> Result<Action, Error> {
    let run_id = run
        .spec
        .run_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let status = RunCrdStatus {
        phase: Some(RunPhase::Pending),
        run_id: Some(run_id.clone()),
        started_at: Some(chrono::Utc::now().to_rfc3339()),
        ..Default::default()
    };
    patch_status(runs_api, name, &status).await?;

    tracing::info!(run = %name, %run_id, "assigned run_id, transitioning to Pending");
    Ok(Action::requeue(SHORT_REQUEUE))
}

async fn reconcile_pending(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    ctx: &Context,
    run: &Run,
    name: &str,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();
    let pod_name = executor_pod_name(name);

    let cl_env = fetch_cl_env(ctx, run).await?;
    let pod = build_executor_pod(run, &pod_name, run_id, false, &cl_env, &ctx.surreal_pod_cfg);
    match pods_api.create(&PostParams::default(), &pod).await {
        Ok(_) => {
            tracing::info!(run = %name, pod = %pod_name, "created executor pod");
        }
        Err(kube_client::Error::Api(ref e)) if e.code == 409 => {
            tracing::info!(run = %name, pod = %pod_name, "executor pod already exists");
        }
        Err(e) => return Err(e.into()),
    }

    let mut new_status = status.clone();
    new_status.phase = Some(RunPhase::Running);
    new_status.executor_pod = Some(pod_name);
    push_condition(
        &mut new_status,
        make_condition(CONDITION_EXECUTOR_READY, "PodCreated"),
    );
    patch_status(runs_api, name, &new_status).await?;

    Ok(Action::requeue(MEDIUM_REQUEUE))
}

async fn reconcile_running(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    run: &Run,
    name: &str,
    ctx: &Context,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();

    if cancel::is_cancel_requested(run) {
        return cancel::start_cancelling(runs_api, run, name, ctx).await;
    }

    if timeout::is_timed_out(run) {
        return timeout::transition_to_timed_out(runs_api, pods_api, run, name, ctx).await;
    }

    let fallback_name = executor_pod_name(name);
    let pod_name = status.executor_pod.as_deref().unwrap_or(&fallback_name);

    match pods_api.get_opt(pod_name).await? {
        Some(pod) => match pod_phase(&pod) {
            PodState::Succeeded => {
                let mut new_status = status.clone();
                new_status.completed_at = Some(chrono::Utc::now().to_rfc3339());

                let executor_wrote_status = match ctx.storage.get_run_outcome(run_id).await {
                    Ok(Some(outcome)) => {
                        apply_outcome_to_status(&mut new_status, outcome);
                        true
                    }
                    Ok(None) => {
                        new_status.phase = Some(RunPhase::Succeeded);
                        new_status.message =
                            Some("Pod succeeded (no outcome in storage)".to_string());
                        false
                    }
                    Err(e) => {
                        tracing::warn!(run = %name, %e, "failed to read run outcome from storage");
                        new_status.phase = Some(RunPhase::Succeeded);
                        new_status.message =
                            Some("Pod succeeded (storage read failed)".to_string());
                        false
                    }
                };

                patch_status(runs_api, name, &new_status).await?;
                if !executor_wrote_status && let Some(ref phase) = new_status.phase {
                    sync_run_status_to_storage(&ctx.storage, run_id, phase).await;
                }
                tracing::info!(run = %name, phase = ?new_status.phase, "run completed");
                Ok(Action::await_change())
            }
            PodState::Failed => {
                restart::handle_executor_failure(runs_api, pods_api, ctx, run, name).await
            }
            PodState::Running => {
                let _ = progress::update_progress(runs_api, &ctx.storage, run, name).await;
                Ok(Action::requeue(PROGRESS_POLL))
            }
            PodState::Pending => Ok(Action::requeue(PROGRESS_POLL)),
            PodState::Unknown => {
                tracing::warn!(run = %name, "executor pod in unknown state");
                Ok(Action::requeue(PROGRESS_POLL))
            }
        },
        None => restart::handle_executor_failure(runs_api, pods_api, ctx, run, name).await,
    }
}

fn executor_pod_name(run_name: &str) -> String {
    format!("{run_name}-executor")
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PodState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

pub(crate) fn pod_phase(pod: &Pod) -> PodState {
    match pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        Some("Succeeded") => PodState::Succeeded,
        Some("Failed") => PodState::Failed,
        Some("Running") => PodState::Running,
        Some("Pending") => PodState::Pending,
        _ => PodState::Unknown,
    }
}

pub(crate) fn apply_outcome_to_status(status: &mut RunCrdStatus, outcome: RunOutcome) {
    match outcome {
        RunOutcome::Success {
            completed_steps,
            total_steps,
        } => {
            status.phase = Some(RunPhase::Succeeded);
            status.completed_steps = Some(completed_steps);
            status.total_steps = Some(total_steps);
            status.message = Some(format!("{completed_steps}/{total_steps} steps completed"));
        }
        RunOutcome::Failure {
            message,
            completed_steps,
            total_steps,
        } => {
            status.phase = Some(RunPhase::Failed);
            status.completed_steps = Some(completed_steps);
            status.total_steps = Some(total_steps);
            status.message = Some(message);
        }
        RunOutcome::Cancelled {
            completed_steps,
            total_steps,
        } => {
            status.phase = Some(RunPhase::Cancelled);
            status.completed_steps = Some(completed_steps);
            status.total_steps = Some(total_steps);
            status.message = Some("Run was cancelled".to_string());
        }
    }
}

pub(crate) async fn delete_pod_if_exists(
    pods_api: &Api<Pod>,
    pod_name: &str,
) -> Result<bool, Error> {
    match pods_api.delete(pod_name, &DeleteParams::default()).await {
        Ok(_) => Ok(true),
        Err(kube_client::Error::Api(ref e)) if e.code == 404 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn make_condition(type_: &str, reason: &str) -> RunCondition {
    RunCondition {
        r#type: type_.to_string(),
        status: "True".to_string(),
        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
        reason: Some(reason.to_string()),
        message: None,
    }
}

pub(crate) fn push_condition(status: &mut RunCrdStatus, condition: RunCondition) {
    status.conditions.push(condition);
    if status.conditions.len() > MAX_CONDITIONS {
        let drain = status.conditions.len() - MAX_CONDITIONS;
        status.conditions.drain(..drain);
    }
}

async fn add_finalizer(runs_api: &Api<Run>, name: &str) -> Result<(), Error> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [FINALIZER]
        }
    });
    runs_api
        .patch(
            name,
            &PatchParams::apply("rivers-operator"),
            &Patch::Merge(&patch),
        )
        .await?;
    Ok(())
}

async fn handle_deletion(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    run: &Run,
    name: &str,
    ctx: &Context,
) -> Result<Action, Error> {
    if !run.finalizers().iter().any(|f| f == FINALIZER) {
        return Ok(Action::await_change());
    }

    let run_id = run.status.as_ref().and_then(|s| s.run_id.as_deref());

    let is_terminal = run
        .status
        .as_ref()
        .and_then(|s| s.phase.as_ref())
        .is_some_and(|p| p.is_terminal());

    if let Some(run_id) = run_id
        && !is_terminal
    {
        if let Err(e) = ctx.storage.request_cancellation(run_id).await {
            tracing::warn!(run = %name, %e, "failed to signal cancellation during deletion");
        }
        sync_run_status_to_storage(&ctx.storage, run_id, &RunPhase::Cancelled).await;
    }

    if let Some(pod_name) = run.status.as_ref().and_then(|s| s.executor_pod.as_deref())
        && delete_pod_if_exists(pods_api, pod_name).await?
    {
        tracing::info!(run = %name, pod = %pod_name, "deleted executor pod");
    }

    if let Some(run_id) = run_id
        && let Err(e) = cleanup::cleanup_step_jobs(ctx, run_id).await
    {
        tracing::warn!(run = %name, %e, "failed to clean up step jobs during deletion");
    }

    let remaining: Vec<&String> = run
        .finalizers()
        .iter()
        .filter(|f| f.as_str() != FINALIZER)
        .collect();
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": remaining
        }
    });
    runs_api
        .patch(
            name,
            &PatchParams::apply("rivers-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

    tracing::info!(run = %name, "removed finalizer, allowing deletion");
    Ok(Action::await_change())
}

/// Called when the operator transitions a run to a terminal phase and the
/// executor didn't get a chance to write the status itself.
pub(crate) async fn sync_run_status_to_storage(
    storage: &SurrealStorage,
    run_id: &str,
    phase: &RunPhase,
) {
    let status = match phase {
        RunPhase::Succeeded => RunStatus::Success,
        RunPhase::Failed | RunPhase::TimedOut => RunStatus::Failure,
        RunPhase::Cancelled => RunStatus::Canceled,
        _ => return,
    };
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    if let Err(e) = storage.update_run_status(run_id, status, Some(now)).await {
        tracing::warn!(
            run_id = %run_id,
            error = %e,
            "failed to sync terminal run status to storage"
        );
    }
}

/// Single authority for status writes — every reconciler branch routes
/// through here so the field manager (`rivers-operator`) is consistent.
pub async fn patch_status(
    runs_api: &Api<Run>,
    name: &str,
    status: &RunCrdStatus,
) -> Result<(), Error> {
    let patch = serde_json::json!({ "status": status });
    runs_api
        .patch_status(
            name,
            &PatchParams::apply("rivers-operator"),
            &Patch::Merge(&patch),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::PodStatus;
    use rivers_core::storage::RunOutcome;

    fn make_pod(phase: Option<&str>) -> Pod {
        Pod {
            status: phase.map(|p| PodStatus {
                phase: Some(p.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pod_phase_succeeded() {
        assert_eq!(pod_phase(&make_pod(Some("Succeeded"))), PodState::Succeeded);
    }

    #[test]
    fn pod_phase_failed() {
        assert_eq!(pod_phase(&make_pod(Some("Failed"))), PodState::Failed);
    }

    #[test]
    fn pod_phase_running() {
        assert_eq!(pod_phase(&make_pod(Some("Running"))), PodState::Running);
    }

    #[test]
    fn pod_phase_pending() {
        assert_eq!(pod_phase(&make_pod(Some("Pending"))), PodState::Pending);
    }

    #[test]
    fn pod_phase_unknown_string() {
        assert_eq!(
            pod_phase(&make_pod(Some("SomethingElse"))),
            PodState::Unknown
        );
    }

    #[test]
    fn pod_phase_no_status() {
        assert_eq!(pod_phase(&make_pod(None)), PodState::Unknown);
    }

    #[test]
    fn apply_outcome_success() {
        let mut status = RunCrdStatus::default();
        apply_outcome_to_status(
            &mut status,
            RunOutcome::Success {
                completed_steps: 5,
                total_steps: 5,
            },
        );
        assert_eq!(status.phase, Some(RunPhase::Succeeded));
        assert_eq!(status.completed_steps, Some(5));
        assert_eq!(status.total_steps, Some(5));
        assert_eq!(status.message.as_deref(), Some("5/5 steps completed"));
    }

    #[test]
    fn apply_outcome_failure() {
        let mut status = RunCrdStatus::default();
        apply_outcome_to_status(
            &mut status,
            RunOutcome::Failure {
                message: "step X failed".to_string(),
                completed_steps: 3,
                total_steps: 5,
            },
        );
        assert_eq!(status.phase, Some(RunPhase::Failed));
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(5));
        assert_eq!(status.message.as_deref(), Some("step X failed"));
    }

    #[test]
    fn apply_outcome_cancelled() {
        let mut status = RunCrdStatus::default();
        apply_outcome_to_status(
            &mut status,
            RunOutcome::Cancelled {
                completed_steps: 2,
                total_steps: 10,
            },
        );
        assert_eq!(status.phase, Some(RunPhase::Cancelled));
        assert_eq!(status.completed_steps, Some(2));
        assert_eq!(status.total_steps, Some(10));
        assert_eq!(status.message.as_deref(), Some("Run was cancelled"));
    }

    #[test]
    fn push_condition_basic() {
        let mut status = RunCrdStatus::default();
        push_condition(&mut status, make_condition("ExecutorReady", "PodCreated"));
        assert_eq!(status.conditions.len(), 1);
        assert_eq!(status.conditions[0].r#type, "ExecutorReady");
        assert_eq!(status.conditions[0].reason.as_deref(), Some("PodCreated"));
        assert_eq!(status.conditions[0].status, "True");
        assert!(status.conditions[0].last_transition_time.is_some());
    }

    #[test]
    fn push_condition_rotates_at_max() {
        let mut status = RunCrdStatus::default();
        for i in 0..MAX_CONDITIONS + 5 {
            push_condition(
                &mut status,
                make_condition(&format!("Cond{i}"), &format!("Reason{i}")),
            );
        }
        assert_eq!(status.conditions.len(), MAX_CONDITIONS);
        assert_eq!(status.conditions[0].r#type, format!("Cond{}", 5));
        assert_eq!(
            status.conditions.last().unwrap().r#type,
            format!("Cond{}", MAX_CONDITIONS + 4)
        );
    }

    #[test]
    fn make_condition_fields() {
        let c = make_condition("TestType", "TestReason");
        assert_eq!(c.r#type, "TestType");
        assert_eq!(c.status, "True");
        assert_eq!(c.reason.as_deref(), Some("TestReason"));
        assert!(c.last_transition_time.is_some());
        assert!(c.message.is_none());
    }
}
