//! CodeLocation reconciler.
//!
//! Responsibilities:
//!   1. Resolve `spec.image:spec.tag` → `status.resolvedImage` (a digest
//!      reference), unless `spec.digest` is set (which short-circuits the
//!      registry call).
//!   2. Maintain the owned `Deployment` + `Service`, keyed by the CR name.
//!   3. Keep `status` in sync: phase, conditions, `resolvedImage`,
//!      `grpcEndpoint`, `observedGeneration`, `readyReplicas`.
//!
//! Registry polling is gated on the leader lease (see [`crate::leader`]): only
//! the leader issues registry HEADs. Followers still reconcile owned resources
//! against whatever digest is already in `status.resolvedImage`, so the
//! backing Deployment doesn't drift while a leader election is in flight.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube_client::ResourceExt;
use kube_client::api::{Api, DeleteParams, ListParams, Patch, PatchParams, Preconditions};
use kube_runtime::controller::Action;
use kube_runtime::reflector::Store;
use rivers_k8s::crd::code_location::{
    CONDITION_DEPLOYMENT_AVAILABLE, CONDITION_IMAGE_RESOLVED, CONDITION_SOURCE_RESOLVED,
    CONDITION_WORKSPACE_KEPT, CodeLocation, CodeLocationCondition, CodeLocationPhase,
    CodeLocationSpec, CodeLocationStatus, GitRef, IMMUTABLE_TAG_ANNOTATION, REASON_APPLY_FAILED,
    REASON_AUTH_FAILED, REASON_AWAITING_LEADER, REASON_COMMIT_PINNED, REASON_COMMIT_RESOLVED,
    REASON_DIGEST_PINNED, REASON_DIGEST_RESOLVED, REASON_GIT_AUTH_FAILED,
    REASON_GIT_HOST_KEY_REJECTED, REASON_GIT_MALFORMED_RESPONSE, REASON_GIT_RATE_LIMITED,
    REASON_GIT_UNREACHABLE, REASON_INVALID_REF, REASON_INVALID_URL, REASON_MIN_REPLICAS,
    REASON_NO_DEPLOYMENT_STATUS, REASON_PROGRESS_DEADLINE, REASON_RATE_LIMITED,
    REASON_REF_NOT_FOUND, REASON_REGISTRY_ERROR, REASON_ROLLING_OUT, REASON_RUNS_NOT_LISTED,
    REASON_RUNS_USE_WORKSPACE, REASON_TAG_NOT_FOUND,
};

use super::git::{self, GitCredentials, GitResolveRequest};

use super::image_auth::resolve_auth;
use super::registry::{
    ImageRef, RegistryAuth, RegistryClient, RegistryError, Resolution, ResolveRequest,
};
use super::resources::{
    MAIN_CONTAINER, build_deployment, build_git_deployment, build_keep_config_map, build_service,
    build_workspace_pvc, deployment_name, grpc_endpoint, keep_config_map_name, labels,
    service_name, workspace_pvc_name,
};
use crate::leader::LeaderGate;
use crate::metrics;
use k8s_openapi::api::core::v1::{
    ConfigMap, ContainerStateTerminated, PersistentVolumeClaim, Pod, PodSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use rivers_k8s::crd::run::{Run, RunSource};
use rivers_k8s::workspace::{self, WorkspaceSpec, WorkspaceVolume};

/// Default registry-refresh interval when the CR does not override it.
const DEFAULT_REFRESH: Duration = Duration::from_secs(300);
/// Minimum acceptable refresh interval. Anything tighter could blow through
/// registry rate limits quickly.
const MIN_REFRESH: Duration = Duration::from_secs(60);
/// Requeue delay for followers waiting on the leader to resolve a digest.
const FOLLOWER_WAIT: Duration = Duration::from_secs(30);
/// Requeue delay after a terminal-looking registry error (auth, 404). The
/// reconciler still checks back occasionally in case the user fixes the
/// underlying issue without bumping the generation.
const TERMINAL_RETRY: Duration = Duration::from_secs(300);
/// Short follow-up requeue when the Deployment is rolling out.
const DEPLOYMENT_ROLLOUT_POLL: Duration = Duration::from_secs(10);
/// How often image mode looks again at a workspace PVC that it keeps for an
/// earlier git source: no event wakes it when a run is deleted.
const WORKSPACE_RECHECK: Duration = Duration::from_secs(60);
/// Field manager used for all server-side applies.
const FIELD_MANAGER: &str = "rivers-operator-code-location";

pub struct Context {
    pub client: kube_client::Client,
    pub namespace: String,
    pub registry: Arc<RegistryClient>,
    /// Ref→commit resolver for git-sourced CodeLocations (RFC-044). Only
    /// the leader calls it, as with `registry`.
    pub git: Arc<super::git::GitResolver>,
    /// Default runtime image for git CLs that don't set `spec.image`.
    /// Chart-configured via `RIVERS_RUNTIME_IMAGE`.
    pub runtime_image: ImageRef,
    pub leader: Arc<LeaderGate>,
    pub code_location_service_account: String,
    /// Chart-level workspace settings for git-sourced CLs.
    pub workspace: WorkspaceConfig,
    /// The run controller's store: the operator's one watch on Runs.
    pub runs: Store<Run>,
    /// SurrealDB endpoint, scope (`use_ns` / `use_db`) and auth-secret
    /// coordinates stamped onto every code-location pod the operator creates.
    /// Read once from the operator's own env at startup. When `auth_secret`
    /// is set, `RIVERS_SURREAL_USERNAME` / `_PASSWORD` are emitted via
    /// `valueFrom.secretKeyRef`; otherwise pods connect unauthenticated.
    pub surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig,
    pub otel_pod_cfg: rivers_k8s::env::OtelPodConfig,
}

/// `codeLocation.workspace.*` from the chart, via operator env.
#[derive(Clone, Debug)]
pub struct WorkspaceConfig {
    /// Shared RWX PVC per CL (true) vs per-pod emptyDir (false).
    pub shared_enabled: bool,
    pub storage_class: Option<String>,
    pub shared_size: Quantity,
    pub empty_dir_limit: Quantity,
    pub keep_revisions: u32,
    pub min_tree_age: Duration,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            shared_enabled: false,
            storage_class: None,
            shared_size: Quantity("20Gi".to_string()),
            empty_dir_limit: Quantity("2Gi".to_string()),
            keep_revisions: 3,
            min_tree_age: Duration::from_secs(3600),
        }
    }
}

impl WorkspaceConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let mut cfg = Self::default();
        let env = |name: &str| get(name).filter(|v| !v.is_empty());
        if let Some(v) = env("RIVERS_WORKSPACE_SHARED_ENABLED") {
            cfg.shared_enabled = matches!(v.as_str(), "true" | "1");
        }
        cfg.storage_class = env("RIVERS_WORKSPACE_STORAGE_CLASS");
        let size = |var: &str, value: String| {
            if rivers_k8s::quantity::is_positive(&value) {
                Ok(Quantity(value))
            } else {
                Err(anyhow::anyhow!(
                    "{var}: expected a Kubernetes quantity more than zero, like 10Gi or 500M, \
                     got {value:?}"
                ))
            }
        };
        if let Some(v) = env("RIVERS_WORKSPACE_SHARED_SIZE") {
            cfg.shared_size = size(
                "RIVERS_WORKSPACE_SHARED_SIZE (codeLocation.workspace.shared.size)",
                v,
            )?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_EMPTYDIR_LIMIT") {
            cfg.empty_dir_limit = size(
                "RIVERS_WORKSPACE_EMPTYDIR_LIMIT (codeLocation.workspace.sizeLimit)",
                v,
            )?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_KEEP_REVISIONS") {
            cfg.keep_revisions = v.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "RIVERS_WORKSPACE_KEEP_REVISIONS (codeLocation.workspace.keepRevisions): \
                     expected a whole number, got {v:?}"
                )
            })?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_MIN_AGE") {
            cfg.min_tree_age = humantime_lite::parse(&v).ok_or_else(|| {
                anyhow::anyhow!(
                    "RIVERS_WORKSPACE_MIN_AGE (codeLocation.workspace.minTreeAge): expected a \
                     whole number with s, m or h, like 90m or 24h, got {v:?}"
                )
            })?;
        }
        Ok(cfg)
    }

    /// Workspace volume for the pods of CodeLocation `cl_name` and of its
    /// runs: the CL's shared PVC, or an `emptyDir` capped at
    /// `spec.git.workspaceSize`, else at the chart's `sizeLimit`.
    pub fn volume(&self, cl_name: &str, cl_spec: &CodeLocationSpec) -> WorkspaceVolume {
        if self.shared_enabled {
            return WorkspaceVolume::SharedPvc {
                claim_name: workspace_pvc_name(cl_name),
            };
        }
        let size_limit = cl_spec
            .git
            .as_ref()
            .and_then(|git| git.workspace_size.clone())
            .unwrap_or_else(|| self.empty_dir_limit.clone());
        WorkspaceVolume::EmptyDir {
            size_limit: Some(size_limit),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes api error: {0}")]
    Kube(#[from] kube_client::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Reconcile a single `CodeLocation` CR: resolve `image:tag` to a digest,
/// apply the backing Deployment + Service via server-side apply, then patch
/// status with the new phase and conditions. Returns an [`Action`] telling
/// the controller runtime when to requeue.
pub async fn reconcile(cl: Arc<CodeLocation>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = cl.name_any();
    let namespace = cl.namespace().unwrap_or(ctx.namespace.clone());
    let generation = cl.metadata.generation;

    let code_locations_api: Api<CodeLocation> = Api::namespaced(ctx.client.clone(), &namespace);
    let deployments_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let services_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);

    if cl.spec.is_git() {
        return reconcile_git(
            &cl,
            &ctx,
            &namespace,
            &code_locations_api,
            &secrets_api,
            &deployments_api,
            &services_api,
        )
        .await;
    }

    let leader = ctx.leader.is_leader();
    if !leader && cl.status.as_ref().is_some_and(is_git_status) {
        // Its resolvedImage is the git runtime image, and the leader's
        // first image pass decides about the git workspace.
        return Ok(Action::requeue(FOLLOWER_WAIT));
    }

    let timer = Instant::now();
    let (resolved_image, image_reason, refresh_after, immutable) =
        match resolve_image(&cl, &ctx, &secrets_api).await {
            ImageOutcome::Resolved {
                resolved_image,
                reason,
                refresh_after,
                immutable,
            } => {
                metrics::observe_resolution_seconds(timer.elapsed().as_secs_f64());
                (resolved_image, reason, refresh_after, immutable)
            }
            ImageOutcome::AwaitingLeader => {
                patch_waiting_status(&code_locations_api, &name, generation, cl.as_ref()).await?;
                return Ok(Action::requeue(FOLLOWER_WAIT));
            }
            ImageOutcome::Error(err) => {
                let retry = err.retry_after();
                patch_error_status(&code_locations_api, &name, generation, cl.as_ref(), &err)
                    .await?;
                return Ok(Action::requeue(retry));
            }
        };

    let deployment = build_deployment(
        &cl,
        &resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
        &ctx.otel_pod_cfg,
    );
    let service = build_service(&cl);

    tokio::try_join!(
        apply_deployment(&deployments_api, &deployment),
        apply_service(&services_api, &service),
    )?;

    let dep_status = deployments_api
        .get_status(&deployment_name(&name))
        .await
        .ok();
    let (phase, ready_replicas, deployment_reason, deployment_ok) =
        evaluate_deployment_phase(&cl, dep_status.as_ref());
    let workspace_kept = if leader {
        remove_git_workspace(&cl, &ctx, &namespace, dep_status.as_ref()).await?
    } else {
        None
    };

    let endpoint = grpc_endpoint(&name, &namespace, cl.spec.grpc_port);
    let now_rfc3339 = jiff::Timestamp::now().to_string();

    let prior_status = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(phase.clone()),
        observed_generation: generation,
        resolved_image: Some(resolved_image.clone()),
        grpc_endpoint: Some(endpoint),
        last_reconciled: Some(now_rfc3339.clone()),
        ready_replicas,
        message: None,
        conditions: Vec::new(),
        // Image mode: the pinned digest ref doubles as the Source column.
        source: Some(resolved_image.clone()),
        resolved_commit: None,
        resolved_ref: None,
        last_fetched_at: None,
        run_source: None,
    };
    push_condition(
        &mut status,
        prior_status,
        CONDITION_IMAGE_RESOLVED,
        "True",
        image_reason,
        Some(resolved_image.clone()),
        &now_rfc3339,
    );
    push_condition(
        &mut status,
        prior_status,
        CONDITION_DEPLOYMENT_AVAILABLE,
        if deployment_ok { "True" } else { "False" },
        deployment_reason,
        None,
        &now_rfc3339,
    );
    if leader {
        if let Some((reason, message)) = &workspace_kept {
            push_condition(
                &mut status,
                prior_status,
                CONDITION_WORKSPACE_KEPT,
                "True",
                reason,
                Some(message.clone()),
                &now_rfc3339,
            );
        }
    } else {
        status
            .conditions
            .extend(kept_conditions(prior_status, &[CONDITION_WORKSPACE_KEPT]));
    }

    if !status_substantively_equal(prior_status, &status) {
        patch_status(&code_locations_api, &name, &status).await?;
    }

    let requeue = if !matches!(phase, CodeLocationPhase::Ready) {
        Action::requeue(DEPLOYMENT_ROLLOUT_POLL)
    } else if workspace_kept.is_some() {
        Action::requeue(WORKSPACE_RECHECK)
    } else if immutable {
        // Immutable + Ready → rely on CR/Deployment change events.
        Action::await_change()
    } else {
        Action::requeue(jitter(refresh_after))
    };
    Ok(requeue)
}

/// Image mode: delete the workspace PVC and keep ConfigMap that the code
/// location owns from an earlier git source, once no pod can mount the PVC.
/// Kubernetes deletes a PVC only when no pod mounts it, and the pods of a
/// run stay until the run is deleted, so the PVC stays for as long as
/// [`git_workspace_kept`] says. The keep ConfigMap stays with it: a return
/// to git before the run store syncs prunes the PVC by that keep-set.
/// Looks only while the status can still name such a workspace (a git
/// status, or `WorkspaceKept`); image mode keeps both until it is gone.
/// Returns why they stay, or `None` once nothing is left to delete.
async fn remove_git_workspace(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    deployment: Option<&Deployment>,
) -> Result<Option<(&'static str, String)>, kube_client::Error> {
    let Some(owner) = cl.metadata.uid.as_deref() else {
        return Ok(None);
    };
    let may_be_left = cl.status.as_ref().is_some_and(|status| {
        is_git_status(status)
            || status
                .conditions
                .iter()
                .any(|c| c.r#type == CONDITION_WORKSPACE_KEPT)
    });
    if !may_be_left {
        return Ok(None);
    }
    let name = cl.name_any();
    let (claim, keep_name) = (workspace_pvc_name(&name), keep_config_map_name(&name));
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), namespace);
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let (pvc, keep) = tokio::try_join!(pvc_api.get_opt(&claim), cm_api.get_opt(&keep_name))?;
    let pvc = pvc.filter(|pvc| owned_by(pvc, owner) && pvc.metadata.deletion_timestamp.is_none());
    if pvc.is_some()
        && let Some(kept) = git_workspace_kept(cl, ctx, namespace, deployment, &claim)
    {
        return Ok(Some(kept));
    }
    if let Some(keep) = keep.filter(|keep| owned_by(keep, owner)) {
        delete_exactly(&cm_api, &keep).await?;
    }
    if let Some(pvc) = pvc {
        delete_exactly(&pvc_api, &pvc).await?;
    }
    Ok(None)
}

/// Why the git workspace PVC `claim` of image-mode `cl` must stay, if it
/// must: the status still gives runs the git source, a code-location pod of
/// the git source is left, the run store has not synced, or runs of `cl`
/// with a git source exist — finished ones too, as their pods mount it.
fn git_workspace_kept(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    deployment: Option<&Deployment>,
    claim: &str,
) -> Option<(&'static str, String)> {
    let stays = format!("PVC '{claim}' of the git source stays");
    let git_serves = cl.status.as_ref().is_some_and(|s| s.run_source.is_some());
    if git_serves || !deployment.is_some_and(no_old_pods) {
        return Some((
            REASON_ROLLING_OUT,
            format!("{stays} until every code-location pod runs the image"),
        ));
    }
    if !matches!(ctx.runs.wait_until_ready().now_or_never(), Some(Ok(()))) {
        return Some((
            REASON_RUNS_NOT_LISTED,
            format!("{stays} until the operator has listed the runs"),
        ));
    }
    let cl_name = cl.name_any();
    let mut runs: Vec<String> = ctx
        .runs
        .state()
        .iter()
        .filter(|run| is_run_of(run, namespace, &cl_name) && run.spec.source.is_some())
        .map(|run| run.name_any())
        .collect();
    if runs.is_empty() {
        return None;
    }
    runs.sort();
    Some((
        REASON_RUNS_USE_WORKSPACE,
        format!(
            "{stays} while the pods of these runs mount it: {}",
            brief_list(&runs)
        ),
    ))
}

/// A status of the git source, not yet replaced by the leader's first image
/// pass after a switch to image mode.
fn is_git_status(status: &CodeLocationStatus) -> bool {
    status.run_source.is_some()
        || status.resolved_commit.is_some()
        || status.resolved_ref.is_some()
        || status.last_fetched_at.is_some()
        || status
            .conditions
            .iter()
            .any(|c| c.r#type == CONDITION_SOURCE_RESOLVED)
}

/// The first five of `names`, then how many more.
fn brief_list(names: &[String]) -> String {
    const SHOWN: usize = 5;
    let shown = names[..names.len().min(SHOWN)].join(", ");
    match names.len().saturating_sub(SHOWN) {
        0 => shown,
        more => format!("{shown} and {more} more"),
    }
}

/// The git path makes the code location with `uid` the owner of its
/// workspace PVC and keep ConfigMap.
fn owned_by(object: &impl kube_client::Resource, uid: &str) -> bool {
    object
        .owner_references()
        .iter()
        .any(|owner| owner.uid == uid)
}

/// Delete `object`, but not an object that has taken its name since it was
/// read.
async fn delete_exactly<K>(api: &Api<K>, object: &K) -> Result<(), kube_client::Error>
where
    K: kube_client::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: object.uid(),
            resource_version: None,
        }),
        ..DeleteParams::default()
    };
    match api.delete(&object.name_any(), &params).await {
        Ok(_) => Ok(()),
        Err(kube_client::Error::Api(status)) if status.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}

/// Git-mode reconcile (RFC-044 phase 1): pin the runtime image digest and
/// the commit, publish status. **No Deployment yet** — the workspace
/// materializer is phase 2, so a git CL deliberately never reaches `Ready`
/// here and the Run admission webhook keeps rejecting runs against it.
#[allow(clippy::too_many_arguments)]
async fn reconcile_git(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    code_locations_api: &Api<CodeLocation>,
    secrets_api: &Api<Secret>,
    deployments_api: &Api<Deployment>,
    services_api: &Api<Service>,
) -> Result<Action, Error> {
    let name = cl.name_any();
    let generation = cl.metadata.generation;
    let git_spec = cl
        .spec
        .git
        .as_ref()
        .expect("reconcile_git dispatched for a non-git CodeLocation");
    let endpoint = grpc_endpoint(&name, namespace, cl.spec.grpc_port);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);

    // Followers neither resolve nor apply: they refresh what the Deployment
    // shows and keep the leader's resolution conditions, so both replicas
    // publish the same status. A Failed status shows nothing of the
    // Deployment, and a refused object only the leader's apply saw: only the
    // leader's next pass clears either.
    if !ctx.leader.is_leader() {
        let prior = cl.status.as_ref();
        let leaders_alone = prior.is_some_and(|s| {
            s.phase == Some(CodeLocationPhase::Failed)
                || s.conditions.iter().any(|c| {
                    c.r#type == CONDITION_DEPLOYMENT_AVAILABLE
                        && c.reason.as_deref() == Some(REASON_APPLY_FAILED)
                })
        });
        if leaders_alone {
            return Ok(Action::requeue(FOLLOWER_WAIT));
        }
        let published = prior.is_some_and(|s| {
            s.run_source.is_some()
                || s.conditions
                    .iter()
                    .any(|c| c.r#type == CONDITION_SOURCE_RESOLVED)
        });
        if published {
            let rollout = observe_git_deployment(&name, deployments_api, &pods_api).await;
            let update = GitStatusUpdate {
                resolution: None,
                fetched_at: None,
                rollout,
                apply_failure: None,
                endpoint,
            };
            patch_git_status(code_locations_api, &name, cl, update).await?;
        } else {
            patch_waiting_status(code_locations_api, &name, generation, cl).await?;
        }
        return Ok(Action::requeue(FOLLOWER_WAIT));
    }

    let serving = cl.status.as_ref().is_some_and(|s| s.run_source.is_some());

    // Runtime image first, through the same registry pipeline as image mode.
    let timer = Instant::now();
    let (resolved_image, image_reason, refresh_after, immutable) =
        match resolve_image(cl, ctx, secrets_api).await {
            ImageOutcome::Resolved {
                resolved_image,
                reason,
                refresh_after,
                immutable,
            } => {
                metrics::observe_resolution_seconds(timer.elapsed().as_secs_f64());
                (resolved_image, reason, refresh_after, immutable)
            }
            ImageOutcome::AwaitingLeader => {
                patch_waiting_status(code_locations_api, &name, generation, cl).await?;
                return Ok(Action::requeue(FOLLOWER_WAIT));
            }
            ImageOutcome::Error(err) => {
                let retry = err.retry_after();
                if err.is_transient() && serving {
                    // The Deployment keeps the tree it runs until the
                    // registry answers again.
                    let update = GitStatusUpdate {
                        resolution: Some(GitResolution::ImageFailed(err)),
                        fetched_at: None,
                        rollout: observe_git_deployment(&name, deployments_api, &pods_api).await,
                        apply_failure: None,
                        endpoint,
                    };
                    patch_git_status(code_locations_api, &name, cl, update).await?;
                } else {
                    patch_error_status(code_locations_api, &name, generation, cl, &err).await?;
                }
                return Ok(Action::requeue(retry));
            }
        };

    let poll = parse_refresh_interval(git_spec.poll_interval.as_deref());
    let resolution = match git_credentials(git_spec, secrets_api).await {
        Ok(credentials) => {
            let request = GitResolveRequest {
                url: git_spec.url.clone(),
                r#ref: git_spec.r#ref.clone(),
                credentials,
                cache_ttl: poll,
            };
            ctx.git.resolve(&request).await
        }
        Err(err) => Err(err.into()),
    };
    let resolved = match resolution {
        Ok(resolved) => resolved,
        Err(failure) => {
            let retry = failure
                .retry_after
                .unwrap_or_else(|| git_error_reason_retry(&failure.error).1);
            if failure.error.is_transient() && serving {
                // The Deployment keeps the tree it runs until a commit
                // resolves again.
                let update = GitStatusUpdate {
                    resolution: Some(GitResolution::ImageResolved {
                        image: resolved_image,
                        image_reason,
                        source: Err(failure),
                    }),
                    fetched_at: None,
                    rollout: observe_git_deployment(&name, deployments_api, &pods_api).await,
                    apply_failure: None,
                    endpoint,
                };
                patch_git_status(code_locations_api, &name, cl, update).await?;
            } else {
                patch_git_error(code_locations_api, &name, generation, cl, &failure).await?;
            }
            return Ok(Action::requeue(retry));
        }
    };

    let applied = apply_git_workspace(
        cl,
        ctx,
        namespace,
        deployments_api,
        services_api,
        &resolved_image,
        &resolved,
    )
    .await?;
    let refusal = match applied {
        Applied::All => None,
        Applied::WaitingForPvc => {
            tracing::info!(
                code_location = %name,
                "the workspace PVC is being deleted; the rollout waits until it is gone"
            );
            return Ok(Action::requeue(DEPLOYMENT_ROLLOUT_POLL));
        }
        Applied::Refused(failure) => Some(failure),
    };
    let rollout = observe_git_deployment(&name, deployments_api, &pods_api).await;
    let rolling_out = !rollout.complete;
    let update = GitStatusUpdate {
        resolution: Some(GitResolution::ImageResolved {
            image: resolved_image,
            image_reason,
            source: Ok(resolved),
        }),
        fetched_at: Some(jiff::Timestamp::now().to_string()),
        rollout,
        apply_failure: refusal.as_ref().map(|failure| failure.message.clone()),
        endpoint,
    };
    let phase = patch_git_status(code_locations_api, &name, cl, update).await?;

    let requeue = if let Some(failure) = refusal {
        Action::requeue(failure.retry)
    } else if rolling_out || !matches!(phase, CodeLocationPhase::Ready) {
        Action::requeue(DEPLOYMENT_ROLLOUT_POLL)
    } else {
        git_requeue(&git_spec.r#ref, poll, refresh_after, immutable)
    };
    Ok(requeue)
}

/// When a Ready git code location whose rollout is complete is reconciled
/// again: after the sooner of the ref's poll and the runtime image's
/// refresh. A pinned commit has no poll; an `image_immutable` image (a
/// digest, or an immutable tag once resolved) no refresh, as in image mode.
fn git_requeue(
    git_ref: &GitRef,
    poll: Duration,
    image_refresh: Duration,
    image_immutable: bool,
) -> Action {
    let ref_poll = if git_ref.commit.as_deref().is_some_and(|c| !c.is_empty()) {
        None
    } else if git_ref
        .tag
        .as_deref()
        .is_some_and(super::registry::looks_immutable)
    {
        Some(Duration::from_secs(3600))
    } else {
        Some(poll)
    };
    let image_refresh = (!image_immutable).then_some(image_refresh);
    match ref_poll.into_iter().chain(image_refresh).min() {
        Some(after) => Action::requeue(jitter(after)),
        None => Action::await_change(),
    }
}

/// What [`apply_git_workspace`] did.
enum Applied {
    All,
    /// Nothing: the workspace PVC is being deleted.
    WaitingForPvc,
    /// The API server refused an object; the objects after it are not
    /// applied.
    Refused(ApplyFailure),
}

/// An object of a git code location that the API server refused.
struct ApplyFailure {
    /// `applying Deployment 'x' failed: <the API server's message>`.
    message: String,
    /// When the leader tries again.
    retry: Duration,
}

/// An API error of `action` is the API server's refusal, which the code
/// location shows; any other error is the reconcile's. A refused object
/// stays refused until the CR or the cluster's rules change; a server
/// error, a conflict or throttling may pass.
fn refused(action: &str, error: kube_client::Error) -> Result<Applied, Error> {
    let status = match error {
        kube_client::Error::Api(status) => status,
        other => return Err(other.into()),
    };
    let answer = if status.message.is_empty() {
        format!("{} {}", status.code, status.reason)
    } else {
        status.message
    };
    let retry = if status.code >= 500 || matches!(status.code, 409 | 429) {
        Duration::from_secs(60)
    } else {
        TERMINAL_RETRY
    };
    Ok(Applied::Refused(ApplyFailure {
        message: format!("{action} failed: {answer}"),
        retry,
    }))
}

/// Apply the git CL's owned resources: (shared mode) the workspace PVC and
/// keep ConfigMap, then the Deployment + Service running out of the
/// materialized tree.
async fn apply_git_workspace(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    deployments_api: &Api<Deployment>,
    services_api: &Api<Service>,
    resolved_image: &str,
    resolved: &git::ResolvedRef,
) -> Result<Applied, Error> {
    let name = cl.name_any();
    let git_spec = cl.spec.git.as_ref().expect("git CL");
    let wspec = WorkspaceSpec {
        volume: ctx.workspace.volume(&name, &cl.spec),
        runtime_image: resolved_image.to_string(),
        git_url: git_spec.url.clone(),
        commit: resolved.commit.clone(),
        git_ref: resolved.ref_name.clone(),
        path: git_spec.path.clone(),
        secret_name: git_spec.secret_ref.as_ref().map(|s| s.name.clone()),
        deps: git_spec.dependencies.clone(),
        keep_config_map: ctx
            .workspace
            .shared_enabled
            .then(|| keep_config_map_name(&name)),
        keep_revisions: Some(ctx.workspace.keep_revisions),
        min_tree_age: Some(ctx.workspace.min_tree_age),
        extra_env: cl.spec.env.clone(),
    };
    let pieces = workspace::builder_pod_pieces(&wspec);

    if ctx.workspace.shared_enabled {
        let pvc_size = git_spec
            .workspace_size
            .clone()
            .unwrap_or_else(|| ctx.workspace.shared_size.clone());
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), namespace);
        let pvc = build_workspace_pvc(cl, &pvc_size, ctx.workspace.storage_class.as_deref());
        match ensure_workspace_pvc(&pvc_api, pvc).await {
            Ok(true) => {}
            Ok(false) => return Ok(Applied::WaitingForPvc),
            Err(e) => {
                let claim = workspace_pvc_name(&name);
                return refused(&format!("creating PersistentVolumeClaim '{claim}'"), e);
            }
        }
        // Keep-set: the tree being rolled out, the tree runs get until that
        // rollout finishes, and every non-terminal run's tree, from the run
        // controller's store. A store that lags the API server is covered by
        // keepRevisions + minTreeAge (see the RFC's prune soundness
        // argument). Until the store has synced the keep-set in force stays:
        // one built from a partial store would drop the trees of the runs it
        // misses. The ConfigMap indirection means this refresh never rolls
        // the Deployment.
        if let Some(Ok(())) = ctx.runs.wait_until_ready().now_or_never() {
            let serving_key = cl
                .status
                .as_ref()
                .and_then(|s| s.run_source.as_ref())
                .map(RunSource::workspace_key);
            let runs = ctx.runs.state();
            let keep = workspace_keep_csv(
                &wspec.key(),
                serving_key.as_deref(),
                namespace,
                &name,
                runs.iter().map(Arc::as_ref),
            );
            let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
            if let Err(e) = apply_config_map(&cm_api, &build_keep_config_map(cl, &keep)).await {
                let keep_name = keep_config_map_name(&name);
                return refused(&format!("applying ConfigMap '{keep_name}'"), e);
            }
        } else {
            tracing::info!("the Run store has not synced yet; the keep-set in force stays");
        }
    }

    let deployment = build_git_deployment(
        cl,
        resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
        &ctx.otel_pod_cfg,
        &pieces,
    );
    let service = build_service(cl);
    let (deployed, served) = tokio::join!(
        apply_deployment(deployments_api, &deployment),
        apply_service(services_api, &service),
    );
    if let Err(e) = deployed {
        return refused(
            &format!("applying Deployment '{}'", deployment_name(&name)),
            e,
        );
    }
    if let Err(e) = served {
        return refused(&format!("applying Service '{}'", service_name(&name)), e);
    }
    Ok(Applied::All)
}

/// Read the git Deployment and, until its rollout is complete, its pods,
/// without mutating anything — shared by the leader (post-apply) and
/// follower (status refresh) paths, so both publish the same status.
async fn observe_git_deployment(
    name: &str,
    deployments_api: &Api<Deployment>,
    pods_api: &Api<Pod>,
) -> GitRollout {
    let deployment = deployments_api
        .get_status(&deployment_name(name))
        .await
        .ok();
    let mut rollout = git_rollout(deployment.as_ref());
    let Some(template) = rollout.template.as_ref().filter(|_| !rollout.complete) else {
        return rollout;
    };
    let selector = labels(name)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    match pods_api
        .list(&ListParams::default().labels(&selector))
        .await
    {
        Ok(pods) => rollout.build_failure = newest_build_failure(&pods.items, template),
        Err(error) => tracing::warn!(
            code_location = name,
            %error,
            "could not list the code-location pods; their build errors are not shown"
        ),
    }
    rollout
}

/// The tree `deployment`'s pod template runs.
fn template_source(deployment: &Deployment) -> Option<RunSource> {
    pod_source(deployment.spec.as_ref()?.template.spec.as_ref()?)
}

/// The tree a code-location pod runs: its main container's
/// `RIVERS_RUN_SOURCE`.
fn pod_source(spec: &PodSpec) -> Option<RunSource> {
    let source = spec
        .containers
        .iter()
        .find(|c| c.name == MAIN_CONTAINER)?
        .env
        .as_ref()?
        .iter()
        .find(|e| e.name == rivers_k8s::env::ENV_RUN_SOURCE)?
        .value
        .as_deref()?;
    serde_json::from_str(source).ok()
}

/// The newest failed run of the `workspace` init container among the pods
/// that run `template`. A container whose last run succeeded has none, even
/// if a run before it failed.
fn newest_build_failure(pods: &[Pod], template: &RunSource) -> Option<ContainerStateTerminated> {
    pods.iter()
        .filter(|pod| pod.spec.as_ref().and_then(pod_source).as_ref() == Some(template))
        .filter_map(|pod| {
            let sync = pod
                .status
                .as_ref()?
                .init_container_statuses
                .as_ref()?
                .iter()
                .find(|c| c.name == workspace::SYNC_CONTAINER)?;
            let last_run = match sync.state.as_ref().and_then(|s| s.terminated.as_ref()) {
                Some(ended) => ended,
                None => sync.last_state.as_ref()?.terminated.as_ref()?,
            };
            (last_run.exit_code != 0).then_some((last_run, pod.metadata.name.as_deref()))
        })
        .max_by_key(|(last_run, pod_name)| (last_run.finished_at.clone(), *pod_name))
        .map(|(last_run, _)| last_run.clone())
}

/// What the git Deployment shows: the tree its pod template runs, and how
/// far that template has rolled out.
#[derive(Debug, Default)]
struct GitRollout {
    template: Option<RunSource>,
    /// Every pod runs the current template and is ready.
    complete: bool,
    /// `None` when the Deployment or its status could not be read.
    ready_replicas: Option<i32>,
    deadline_exceeded: bool,
    /// While the rollout is not complete: the newest failed build of the
    /// template's tree in its pods.
    build_failure: Option<ContainerStateTerminated>,
}

fn git_rollout(deployment: Option<&Deployment>) -> GitRollout {
    let Some((deployment, status)) = deployment.and_then(|d| Some((d, d.status.as_ref()?))) else {
        return GitRollout::default();
    };
    GitRollout {
        template: template_source(deployment),
        complete: rollout_complete(deployment),
        ready_replicas: Some(status.ready_replicas.unwrap_or(0)),
        deadline_exceeded: generation_observed(deployment)
            && has_progress_deadline_exceeded(status),
        build_failure: None,
    }
}

/// Every pod runs the current template and is ready, and no pod of an older
/// template is left. Ready counts alone include the old ReplicaSet's pods.
fn rollout_complete(deployment: &Deployment) -> bool {
    let (Some(spec), Some(status)) = (&deployment.spec, &deployment.status) else {
        return false;
    };
    let desired = spec.replicas.unwrap_or(1);
    let all = |count: Option<i32>| count.unwrap_or(0) >= desired;
    desired > 0
        && no_old_pods(deployment)
        && all(status.updated_replicas)
        && all(status.ready_replicas)
        && all(status.available_replicas)
}

/// The Deployment controller has seen the current template, and every pod
/// it counts runs it.
fn no_old_pods(deployment: &Deployment) -> bool {
    deployment.status.as_ref().is_some_and(|status| {
        generation_observed(deployment)
            && status.replicas.unwrap_or(0) == status.updated_replicas.unwrap_or(0)
    })
}

/// The Deployment controller has seen the current template; until then the
/// status describes the previous one.
fn generation_observed(deployment: &Deployment) -> bool {
    let observed = deployment
        .status
        .as_ref()
        .and_then(|s| s.observed_generation);
    matches!(
        (observed, deployment.metadata.generation),
        (Some(seen), Some(current)) if seen >= current
    )
}

/// Keep-set for the prune step: the tree being rolled out (`target_key`),
/// the tree runs get until that rollout finishes (`serving_key`), plus the
/// tree of every non-terminal Run of the CL (`namespace`/`cl_name`) — queued
/// (`Pending`, or no status yet) and `Cancelling` included, not just
/// `Running`; a run waiting on a concurrency pool is precisely the one most
/// likely to sit through several commits. Keys, not commits: each run's tree
/// is keyed on its source's runtime image, whatever image its pods run.
/// Sorted + deduped so the ConfigMap value is deterministic and refreshes
/// don't churn.
fn workspace_keep_csv<'a>(
    target_key: &str,
    serving_key: Option<&str>,
    namespace: &str,
    cl_name: &str,
    runs: impl IntoIterator<Item = &'a Run>,
) -> String {
    let mut keys = std::collections::BTreeSet::new();
    keys.insert(target_key.to_string());
    keys.extend(serving_key.map(str::to_string));
    for run in runs {
        if !is_run_of(run, namespace, cl_name) {
            continue;
        }
        let terminal = run
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .is_some_and(|p| p.is_terminal());
        if terminal {
            continue;
        }
        if let Some(source) = &run.spec.source {
            keys.insert(source.workspace_key());
        }
    }
    keys.into_iter().collect::<Vec<_>>().join(",")
}

fn is_run_of(run: &Run, namespace: &str, cl_name: &str) -> bool {
    run.metadata.namespace.as_deref() == Some(namespace)
        && run.spec.code_location_ref.name == cl_name
}

/// PVC specs are largely immutable — create when absent, otherwise leave
/// the existing claim alone (a size change would need manual expansion).
/// `false` while a claim of that name is being deleted: no new pod can
/// mount it, so the caller waits until it is gone and then creates one.
async fn ensure_workspace_pvc(
    api: &Api<PersistentVolumeClaim>,
    pvc: PersistentVolumeClaim,
) -> Result<bool, kube_client::Error> {
    let name = pvc.metadata.name.clone().unwrap_or_default();
    match api.get_opt(&name).await? {
        Some(existing) => Ok(existing.metadata.deletion_timestamp.is_none()),
        None => api
            .create(&kube_client::api::PostParams::default(), &pvc)
            .await
            .map(|_| true),
    }
}

async fn apply_config_map(
    api: &Api<ConfigMap>,
    desired: &ConfigMap,
) -> Result<(), kube_client::Error> {
    let name = desired.metadata.name.as_deref().unwrap_or_default();
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
}

/// Build [`GitCredentials`] from the CR's Secret. The url's scheme picks the
/// keys, like the pod's sync script: `ssh://` needs `identity` + `known_hosts`;
/// `https://` / `http://` use `username` + `password`, or none for an
/// anonymous fetch. These two lose their trailing newlines, as the script's
/// `$(cat …)` drops them. The other scheme's keys are ignored, so ssh and
/// https code locations can share one Secret. A Secret that does not exist
/// or that the operator may not read is an auth failure, not an outage.
async fn git_credentials(
    git_spec: &rivers_k8s::crd::code_location::GitSource,
    secrets_api: &Api<Secret>,
) -> Result<GitCredentials, git::GitError> {
    let transport = git::Transport::of(&git_spec.url)?;
    let Some(secret_ref) = &git_spec.secret_ref else {
        return match transport {
            git::Transport::Http => Ok(GitCredentials::Anonymous),
            git::Transport::Ssh => Err(git::GitError::AuthFailed(
                "ssh:// urls need a git Secret (spec.git.secretRef) with `identity` and \
                 `known_hosts`"
                    .to_string(),
            )),
        };
    };
    let name = &secret_ref.name;
    let secret = secrets_api.get(name).await.map_err(|e| match e {
        kube_client::Error::Api(status) if status.code == 404 => {
            git::GitError::AuthFailed(format!("git Secret '{name}' does not exist"))
        }
        kube_client::Error::Api(status) if status.code == 403 => {
            git::GitError::AuthFailed(format!(
                "the operator may not read git Secret '{name}': {}",
                status.message
            ))
        }
        e => git::GitError::Unreachable(format!("reading git Secret '{name}': {e}")),
    })?;
    let data = secret.data.unwrap_or_default();
    let entry = |key: &str| {
        data.get(key)
            .map(|v| String::from_utf8_lossy(&v.0).into_owned())
    };
    let cat = |key: &str| entry(key).map(|v| v.trim_end_matches('\n').to_owned());

    match transport {
        git::Transport::Ssh => match (entry("identity"), entry("known_hosts")) {
            (Some(private_key_openssh), Some(known_hosts)) => Ok(GitCredentials::Ssh {
                private_key_openssh,
                known_hosts,
            }),
            (Some(_), None) => Err(git::GitError::KnownHostsUnavailable(format!(
                "git Secret '{name}' has `identity` but no `known_hosts` — refusing SSH \
                 without host-key pinning"
            ))),
            (None, known_hosts) => {
                let missing = match known_hosts {
                    Some(_) => "`identity`",
                    None => "`identity` or `known_hosts`",
                };
                Err(git::GitError::AuthFailed(format!(
                    "git Secret '{name}' has no {missing} — ssh:// urls need `identity` and \
                     `known_hosts`"
                )))
            }
        },
        git::Transport::Http => match (cat("username"), cat("password")) {
            (Some(username), Some(password)) => Ok(GitCredentials::Basic { username, password }),
            (None, None) => Ok(GitCredentials::Anonymous),
            (Some(_), None) => Err(git::GitError::AuthFailed(format!(
                "git Secret '{name}' has `username` but no `password`"
            ))),
            (None, Some(_)) => Err(git::GitError::AuthFailed(format!(
                "git Secret '{name}' has `password` but no `username`"
            ))),
        },
    }
}

/// Condition reason + requeue delay for a git resolution failure. Mirrors
/// `ImageError::retry_after`'s philosophy: auth/ref problems are terminal
/// until the CR or Secret changes (slow recheck), transport is transient.
/// A transient fetch failure requeues by the resolver's backoff instead.
fn git_error_reason_retry(err: &git::GitError) -> (&'static str, Duration) {
    match err {
        git::GitError::RefNotFound(_) => (REASON_REF_NOT_FOUND, TERMINAL_RETRY),
        git::GitError::InvalidRef(_) => (REASON_INVALID_REF, TERMINAL_RETRY),
        git::GitError::InvalidUrl(_) => (REASON_INVALID_URL, TERMINAL_RETRY),
        git::GitError::AuthFailed(_) => (REASON_GIT_AUTH_FAILED, TERMINAL_RETRY),
        git::GitError::HostKeyRejected(_) | git::GitError::KnownHostsUnavailable(_) => {
            (REASON_GIT_HOST_KEY_REJECTED, TERMINAL_RETRY)
        }
        git::GitError::Unreachable(_) => (REASON_GIT_UNREACHABLE, Duration::from_secs(60)),
        git::GitError::RateLimited { .. } => (REASON_GIT_RATE_LIMITED, Duration::from_secs(60)),
        git::GitError::Malformed(_) => (REASON_GIT_MALFORMED_RESPONSE, TERMINAL_RETRY),
    }
}

/// `SourceResolved` reason and message for a resolve that produced no
/// commit; a rate limit says when the operator asks the host again.
fn source_failure(failure: &git::GitFailure, now: jiff::Timestamp) -> (&'static str, String) {
    let (reason, _) = git_error_reason_retry(&failure.error);
    let message = match (&failure.error, failure.retry_after) {
        (git::GitError::RateLimited { .. }, Some(wait)) => {
            let at = now
                .checked_add(wait)
                .and_then(|at| at.round(jiff::Unit::Second))
                .unwrap_or(now);
            format!("{}; the operator asks again at {at}", failure.error)
        }
        _ => failure.error.to_string(),
    };
    (reason, message)
}

/// `main@9f3c1ab` for a branch or tag, `9f3c1ab (pinned)` for a pinned commit.
fn source_display(source: &RunSource) -> String {
    let commit = &source.git.commit;
    let short = commit.get(..7).unwrap_or(commit);
    let name = source.git.r#ref.as_deref().map(|r| {
        r.strip_prefix("refs/heads/")
            .or_else(|| r.strip_prefix("refs/tags/"))
            .unwrap_or(r)
    });
    match name {
        Some(name) => format!("{name}@{short}"),
        None => format!("{short} (pinned)"),
    }
}

/// Names the tree being rolled out and the tree runs keep meanwhile; `None`
/// when the rollout does not change the tree.
fn rollout_message(
    target: Option<&RunSource>,
    serving: Option<&RunSource>,
    stuck: bool,
) -> Option<String> {
    let target = target.filter(|t| Some(*t) != serving)?;
    let with_image = serving.is_some_and(|s| s.runtime_image != target.runtime_image);
    let describe = |tree: &RunSource| {
        let source = source_display(tree);
        if with_image {
            format!("{source} on {}", tree.runtime_image)
        } else {
            source
        }
    };
    let rollout = if stuck {
        format!(
            "rollout of {} exceeded its progress deadline",
            describe(target)
        )
    } else {
        format!("rolling out {}", describe(target))
    };
    Some(match serving {
        Some(serving) => format!("{rollout}; runs use {}", describe(serving)),
        None => rollout,
    })
}

/// `workspace build of main@9f3c1ab failed (Error, exit code 1): <its
/// termination message>`. No times or restart counts: every pass over the
/// same failure says the same, so it writes the status once.
fn build_failure_message(target: &RunSource, failed: &ContainerStateTerminated) -> String {
    let cause = match &failed.reason {
        Some(reason) => format!("{reason}, exit code {}", failed.exit_code),
        None => format!("exit code {}", failed.exit_code),
    };
    let summary = format!(
        "workspace build of {} failed ({cause})",
        source_display(target)
    );
    match failed.message.as_deref().map(str::trim) {
        Some(said) if !said.is_empty() => format!("{summary}: {said}"),
        _ => summary,
    }
}

struct GitStatusUpdate {
    /// `None` on followers: resolution is the leader's.
    resolution: Option<GitResolution>,
    fetched_at: Option<String>,
    rollout: GitRollout,
    /// The leader's apply of this pass: the object the API server refused,
    /// and its answer ([`ApplyFailure::message`]).
    apply_failure: Option<String>,
    endpoint: String,
}

/// The leader's runtime-image and ref resolution of this pass.
enum GitResolution {
    ImageResolved {
        image: String,
        image_reason: &'static str,
        /// The ref's commit, or the transient failure that left the serving
        /// tree in place.
        source: Result<git::ResolvedRef, git::GitFailure>,
    },
    /// A transient registry error left the serving tree in place before
    /// the ref was resolved.
    ImageFailed(ImageError),
}

/// Publish a git CL's status; returns the phase it published.
async fn patch_git_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    cl: &CodeLocation,
    update: GitStatusUpdate,
) -> Result<CodeLocationPhase, kube_client::Error> {
    let status = git_status(cl, update, jiff::Timestamp::now());
    let phase = status.phase.clone().unwrap_or(CodeLocationPhase::Deploying);
    if !status_substantively_equal(cl.status.as_ref(), &status) {
        patch_status(code_locations_api, name, &status).await?;
    }
    Ok(phase)
}

/// Git-mode status. Runs get the serving tree: the pod template's once every
/// pod runs it and is ready, until then the prior status's — a commit that
/// is still building, or fails to build, never reaches a run. A rollout in
/// progress or stuck shows on `DeploymentAvailable`, not in the phase, with
/// the error of a failed build of its tree, which `status.message` also
/// shows unless a resolution error is there. An object the API server
/// refused shows the same way: the cluster still runs what it ran before,
/// so the serving tree serves on; with no serving tree the phase is Failed.
fn git_status(
    cl: &CodeLocation,
    update: GitStatusUpdate,
    at: jiff::Timestamp,
) -> CodeLocationStatus {
    let now = &at.to_string();
    let prior = cl.status.as_ref();
    let rollout = &update.rollout;
    let serving = match &rollout.template {
        Some(template) if rollout.complete => Some(template.clone()),
        _ => prior.and_then(|s| s.run_source.clone()),
    };
    let desired = cl.spec.replicas;
    let ready =
        serving.is_some() && desired > 0 && rollout.ready_replicas.is_some_and(|r| r >= desired);
    let failed = update.apply_failure.is_some() && serving.is_none();
    let build_failure = rollout
        .template
        .as_ref()
        .zip(rollout.build_failure.as_ref())
        .map(|(target, failed)| build_failure_message(target, failed));
    let (deployment_reason, deployment_message) = if let Some(failure) = &update.apply_failure {
        let message = match &serving {
            Some(serving) => format!("{failure}; runs use {}", source_display(serving)),
            None => failure.clone(),
        };
        (REASON_APPLY_FAILED, Some(message))
    } else if rollout.ready_replicas.is_none() {
        (REASON_NO_DEPLOYMENT_STATUS, None)
    } else if ready && rollout.complete {
        (REASON_MIN_REPLICAS, None)
    } else {
        let reason = if rollout.deadline_exceeded {
            REASON_PROGRESS_DEADLINE
        } else {
            REASON_ROLLING_OUT
        };
        let message = match (
            rollout_message(
                rollout.template.as_ref(),
                serving.as_ref(),
                rollout.deadline_exceeded,
            ),
            build_failure.clone(),
        ) {
            (Some(rollout), Some(failure)) => Some(format!("{rollout}; {failure}")),
            (rollout, failure) => rollout.or(failure),
        };
        (reason, message)
    };
    let mut status = CodeLocationStatus {
        phase: Some(if ready {
            CodeLocationPhase::Ready
        } else if failed {
            CodeLocationPhase::Failed
        } else {
            CodeLocationPhase::Deploying
        }),
        observed_generation: cl.metadata.generation,
        resolved_image: serving.as_ref().map(|t| t.runtime_image.clone()),
        grpc_endpoint: (!failed).then_some(update.endpoint),
        last_reconciled: Some(now.to_string()),
        ready_replicas: rollout.ready_replicas.filter(|_| !failed),
        message: None,
        conditions: Vec::new(),
        source: serving.as_ref().map(source_display),
        resolved_commit: serving.as_ref().map(|t| t.git.commit.clone()),
        resolved_ref: serving.as_ref().and_then(|t| t.git.r#ref.clone()),
        last_fetched_at: update
            .fetched_at
            .or_else(|| prior.and_then(|s| s.last_fetched_at.clone())),
        run_source: serving,
    };
    match update.resolution {
        Some(GitResolution::ImageResolved {
            image,
            image_reason,
            source,
        }) => {
            let (source_status, source_reason, source_message) = match source {
                Ok(resolved) if resolved.ref_name.is_none() => {
                    ("True", REASON_COMMIT_PINNED, resolved.commit)
                }
                Ok(resolved) => ("True", REASON_COMMIT_RESOLVED, resolved.commit),
                Err(failure) => {
                    let (reason, message) = source_failure(&failure, at);
                    ("False", reason, message)
                }
            };
            push_condition(
                &mut status,
                prior,
                CONDITION_IMAGE_RESOLVED,
                "True",
                image_reason,
                Some(image),
                now,
            );
            push_condition(
                &mut status,
                prior,
                CONDITION_SOURCE_RESOLVED,
                source_status,
                source_reason,
                Some(source_message),
                now,
            );
        }
        Some(GitResolution::ImageFailed(err)) => {
            push_condition(
                &mut status,
                prior,
                CONDITION_IMAGE_RESOLVED,
                "False",
                err.reason(),
                Some(err.message()),
                now,
            );
            status
                .conditions
                .extend(kept_conditions(prior, &[CONDITION_SOURCE_RESOLVED]));
        }
        None => status.conditions.extend(kept_conditions(
            prior,
            &[CONDITION_IMAGE_RESOLVED, CONDITION_SOURCE_RESOLVED],
        )),
    }
    push_condition(
        &mut status,
        prior,
        CONDITION_DEPLOYMENT_AVAILABLE,
        if ready { "True" } else { "False" },
        deployment_reason,
        deployment_message,
        now,
    );
    // A resolution error first: until it clears, no commit that fixes the
    // build reaches the pods. From the conditions, so followers that copy
    // them publish the same.
    status.message = status
        .conditions
        .iter()
        .find(|c| {
            c.status == "False"
                && (c.r#type == CONDITION_IMAGE_RESOLVED || c.r#type == CONDITION_SOURCE_RESOLVED)
        })
        .and_then(|c| c.message.clone())
        .or(update.apply_failure)
        .or(build_failure);
    status
}

/// `prior`'s conditions of the `kinds` this pass did not resolve again.
fn kept_conditions<'a>(
    prior: Option<&'a CodeLocationStatus>,
    kinds: &'a [&'a str],
) -> impl Iterator<Item = CodeLocationCondition> + 'a {
    prior
        .into_iter()
        .flat_map(|s| &s.conditions)
        .filter(move |c| kinds.contains(&c.r#type.as_str()))
        .cloned()
}

async fn patch_git_error(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    generation: Option<i64>,
    cl: &CodeLocation,
    failure: &git::GitFailure,
) -> Result<(), kube_client::Error> {
    let at = jiff::Timestamp::now();
    let (reason, message) = source_failure(failure, at);
    let now = at.to_string();
    let prior = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(CodeLocationPhase::Failed),
        observed_generation: generation,
        resolved_image: prior.and_then(|s| s.resolved_image.clone()),
        grpc_endpoint: None,
        last_reconciled: Some(now.clone()),
        ready_replicas: None,
        message: Some(message.clone()),
        conditions: Vec::new(),
        source: prior.and_then(|s| s.source.clone()),
        resolved_commit: prior.and_then(|s| s.resolved_commit.clone()),
        resolved_ref: prior.and_then(|s| s.resolved_ref.clone()),
        last_fetched_at: prior.and_then(|s| s.last_fetched_at.clone()),
        run_source: prior.and_then(|s| s.run_source.clone()),
    };
    push_condition(
        &mut status,
        prior,
        CONDITION_SOURCE_RESOLVED,
        "False",
        reason,
        Some(message),
        &now,
    );
    if !status_substantively_equal(prior, &status) {
        patch_status(code_locations_api, name, &status).await?;
    }
    Ok(())
}

/// Controller-runtime error policy: log the failure and requeue after 30s.
/// Per-error retry tuning happens inside `reconcile()` itself; this is the
/// catch-all for unhandled errors that escape that path.
pub fn error_policy(cl: Arc<CodeLocation>, error: &Error, _ctx: Arc<Context>) -> Action {
    tracing::error!(
        code_location = %cl.name_any(),
        %error,
        "CodeLocation reconcile error"
    );
    Action::requeue(Duration::from_secs(30))
}

enum ImageOutcome {
    Resolved {
        resolved_image: String,
        reason: &'static str,
        refresh_after: Duration,
        immutable: bool,
    },
    /// Follower waiting for the leader to seed `status.resolvedImage`.
    AwaitingLeader,
    Error(ImageError),
}

#[derive(Debug)]
enum ImageError {
    NotFound,
    AuthFailed,
    RateLimited(Duration),
    Transient(String),
}

impl ImageError {
    fn reason(&self) -> &'static str {
        match self {
            ImageError::NotFound => REASON_TAG_NOT_FOUND,
            ImageError::AuthFailed => REASON_AUTH_FAILED,
            ImageError::RateLimited(_) => REASON_RATE_LIMITED,
            ImageError::Transient(_) => REASON_REGISTRY_ERROR,
        }
    }

    fn message(&self) -> String {
        match self {
            ImageError::NotFound => "tag not found in registry".into(),
            ImageError::AuthFailed => "registry authentication failed".into(),
            ImageError::RateLimited(d) => {
                format!("registry rate-limited; retrying in {}s", d.as_secs())
            }
            ImageError::Transient(msg) => msg.clone(),
        }
    }

    fn retry_after(&self) -> Duration {
        match self {
            ImageError::RateLimited(d) => *d,
            ImageError::NotFound | ImageError::AuthFailed => TERMINAL_RETRY,
            ImageError::Transient(_) => Duration::from_secs(60),
        }
    }

    /// The registry was down or rate-limited: a later try may succeed.
    fn is_transient(&self) -> bool {
        matches!(self, ImageError::Transient(_) | ImageError::RateLimited(_))
    }
}

async fn resolve_image(
    cl: &CodeLocation,
    ctx: &Context,
    secrets_api: &Api<Secret>,
) -> ImageOutcome {
    if cl.spec.image.is_none() && !cl.spec.is_git() {
        return ImageOutcome::Error(ImageError::Transient(
            "spec.image is unset on an image-mode CodeLocation".into(),
        ));
    }
    let image = wanted_image(&cl.spec, &ctx.runtime_image);
    resolve_image_ref(cl, &image, ctx, secrets_api).await
}

/// The image a CL's pods run, before digest resolution: `spec.image` with
/// the CL's own tag / digest, else (git mode) the chart's runtime default,
/// whose tag and digest give way to `spec.tag` / `spec.digest` when either
/// is set.
pub(crate) fn wanted_image(spec: &CodeLocationSpec, runtime_default: &ImageRef) -> ImageRef {
    let digest = spec.digest.clone().filter(|d| !d.is_empty());
    let repository = match &spec.image {
        Some(image) => image.clone(),
        None if digest.is_none() && spec.tag.is_none() => return runtime_default.clone(),
        None => runtime_default.repository.clone(),
    };
    ImageRef {
        repository,
        tag: spec.tag.clone(),
        digest,
    }
}

/// Resolve `image` to a pinned digest reference: its digest pins it without
/// a registry call, else its tag (`latest` when it has none) is looked up.
/// One path for image mode and both git cases (see [`wanted_image`]).
async fn resolve_image_ref(
    cl: &CodeLocation,
    image: &ImageRef,
    ctx: &Context,
    secrets_api: &Api<Secret>,
) -> ImageOutcome {
    // Explicit digest wins immediately — no leader gating, no HTTP.
    if let Some(digest) = &image.digest {
        let resolved = format!("{}@{digest}", image.repository);
        return ImageOutcome::Resolved {
            resolved_image: resolved,
            reason: REASON_DIGEST_PINNED,
            refresh_after: Duration::from_secs(3600), // effectively immutable
            immutable: true,
        };
    }

    let refresh = parse_refresh_interval(cl.spec.digest_refresh_interval.as_deref());
    let immutable_hint = cl
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(IMMUTABLE_TAG_ANNOTATION))
        .is_some_and(|v| v == "true");

    // Followers can serve status as long as the leader has already resolved
    // the digest (we reuse whatever is currently in status.resolvedImage).
    if !ctx.leader.is_leader() {
        if let Some(prev) = cl.status.as_ref().and_then(|s| s.resolved_image.clone()) {
            return ImageOutcome::Resolved {
                resolved_image: prev,
                reason: REASON_DIGEST_RESOLVED,
                refresh_after: FOLLOWER_WAIT,
                immutable: false,
            };
        }
        return ImageOutcome::AwaitingLeader;
    }

    let secret_names: Vec<String> = cl
        .spec
        .image_pull_secrets
        .iter()
        .map(|r| r.name.clone())
        .collect();

    let registry_host = first_component(&image.repository).unwrap_or_default();
    let auth: RegistryAuth = if secret_names.is_empty() {
        RegistryAuth::Anonymous
    } else {
        resolve_auth(secrets_api, &secret_names, &registry_host).await
    };

    let req = ResolveRequest {
        image: image.repository.clone(),
        tag: image.tag.clone().unwrap_or_else(|| "latest".to_string()),
        auth,
        immutable_hint,
        cache_ttl: refresh,
    };

    match ctx.registry.resolve(&req).await {
        Ok(res) => {
            let resolved = format!("{}@{}", image.repository, res.digest());
            let immutable = matches!(res, Resolution::Immutable { .. });
            ImageOutcome::Resolved {
                resolved_image: resolved,
                reason: REASON_DIGEST_RESOLVED,
                refresh_after: refresh,
                immutable,
            }
        }
        Err(e) => ImageOutcome::Error(e.into()),
    }
}

impl From<RegistryError> for ImageError {
    fn from(err: RegistryError) -> Self {
        match err {
            RegistryError::NotFound => ImageError::NotFound,
            RegistryError::AuthFailed => ImageError::AuthFailed,
            RegistryError::RateLimited { retry_after } => ImageError::RateLimited(retry_after),
            RegistryError::Transient(msg) | RegistryError::Malformed(msg) => {
                ImageError::Transient(msg)
            }
        }
    }
}

fn parse_refresh_interval(spec_value: Option<&str>) -> Duration {
    let Some(raw) = spec_value else {
        return DEFAULT_REFRESH;
    };
    let trimmed = raw.trim();
    if trimmed == "0" {
        return Duration::from_secs(3600 * 24);
    }
    match humantime_lite::parse(trimmed) {
        Some(d) if d >= MIN_REFRESH => d,
        Some(_) => MIN_REFRESH,
        None => DEFAULT_REFRESH,
    }
}

/// Minimal duration parser accepting `30s`, `5m`, `1h`; a bare number is
/// seconds.
mod humantime_lite {
    use std::time::Duration;

    pub fn parse(s: &str) -> Option<Duration> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('s') {
            return num.parse::<u64>().ok().map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('m') {
            return num
                .parse::<u64>()
                .ok()
                .and_then(|m| m.checked_mul(60))
                .map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('h') {
            return num
                .parse::<u64>()
                .ok()
                .and_then(|h| h.checked_mul(3600))
                .map(Duration::from_secs);
        }
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

fn first_component(image: &str) -> Option<String> {
    image.split('/').next().map(str::to_string)
}

fn evaluate_deployment_phase(
    cl: &CodeLocation,
    status: Option<&Deployment>,
) -> (CodeLocationPhase, Option<i32>, &'static str, bool) {
    let desired = cl.spec.replicas;
    let Some(d) = status.and_then(|d| d.status.clone()) else {
        return (
            CodeLocationPhase::Deploying,
            None,
            REASON_NO_DEPLOYMENT_STATUS,
            false,
        );
    };
    let ready = d.ready_replicas.unwrap_or(0);
    if ready >= desired && desired > 0 {
        (
            CodeLocationPhase::Ready,
            Some(ready),
            REASON_MIN_REPLICAS,
            true,
        )
    } else {
        let reason = if has_progress_deadline_exceeded(&d) {
            REASON_PROGRESS_DEADLINE
        } else {
            REASON_ROLLING_OUT
        };
        (CodeLocationPhase::Deploying, Some(ready), reason, false)
    }
}

fn has_progress_deadline_exceeded(d: &k8s_openapi::api::apps::v1::DeploymentStatus) -> bool {
    d.conditions
        .as_ref()
        .map(|cs| {
            cs.iter().any(|c| {
                c.type_ == "Progressing"
                    && c.status == "False"
                    && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
            })
        })
        .unwrap_or(false)
}

async fn apply_deployment(
    api: &Api<Deployment>,
    desired: &Deployment,
) -> Result<(), kube_client::Error> {
    let name = desired.metadata.name.as_deref().unwrap_or_default();
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
}

async fn apply_service(api: &Api<Service>, desired: &Service) -> Result<(), kube_client::Error> {
    let name = desired.metadata.name.as_deref().unwrap_or_default();
    // Services have immutable fields (ClusterIP) that Apply tolerates as long
    // as the desired value matches what's already there — we let the server
    // reconcile.
    let _ = service_name(name); // use the import; keeps the helper exercised.
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
}

/// Whether the computed status matches the prior status on everything that
/// actually carries meaning for API consumers. `last_reconciled` changes on
/// every pass and is deliberately excluded — if nothing *else* moved, we
/// skip the patch entirely so followers/watchers aren't woken up for no reason.
fn status_substantively_equal(
    prior: Option<&CodeLocationStatus>,
    new: &CodeLocationStatus,
) -> bool {
    let Some(prior) = prior else { return false };
    prior.phase == new.phase
        && prior.observed_generation == new.observed_generation
        && prior.resolved_image == new.resolved_image
        && prior.grpc_endpoint == new.grpc_endpoint
        && prior.ready_replicas == new.ready_replicas
        && prior.message == new.message
        && prior.conditions == new.conditions
        && prior.source == new.source
        && prior.resolved_commit == new.resolved_commit
        && prior.resolved_ref == new.resolved_ref
        && prior.run_source == new.run_source
}

/// Append a condition, preserving `last_transition_time` from the prior
/// status when the condition's `status` field hasn't flipped. Per K8s API
/// convention, `lastTransitionTime` only changes on actual transitions.
fn push_condition(
    status: &mut CodeLocationStatus,
    prior: Option<&CodeLocationStatus>,
    cond_type: &str,
    cond_status: &str,
    reason: &'static str,
    message: Option<String>,
    now_rfc3339: &str,
) {
    let prior_transition = prior
        .and_then(|s| s.conditions.iter().find(|c| c.r#type == cond_type))
        .filter(|c| c.status == cond_status)
        .and_then(|c| c.last_transition_time.clone());
    status.conditions.push(CodeLocationCondition {
        r#type: cond_type.to_string(),
        status: cond_status.to_string(),
        last_transition_time: prior_transition.or_else(|| Some(now_rfc3339.to_string())),
        reason: Some(reason.to_string()),
        message,
    });
}

async fn patch_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    status: &CodeLocationStatus,
) -> Result<(), kube_client::Error> {
    let body = serde_json::json!({
        "apiVersion": "rivers.io/v1alpha1",
        "kind": "CodeLocation",
        "status": status,
    });
    code_locations_api
        .patch_status(
            name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&body),
        )
        .await?;
    Ok(())
}

async fn patch_waiting_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    generation: Option<i64>,
    cl: &CodeLocation,
) -> Result<(), kube_client::Error> {
    let now = jiff::Timestamp::now().to_string();
    let prior = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(CodeLocationPhase::Pending),
        observed_generation: generation,
        resolved_image: prior.and_then(|s| s.resolved_image.clone()),
        grpc_endpoint: None,
        last_reconciled: Some(now.clone()),
        ready_replicas: None,
        message: Some("awaiting leader replica".into()),
        conditions: Vec::new(),
        source: prior.and_then(|s| s.source.clone()),
        resolved_commit: prior.and_then(|s| s.resolved_commit.clone()),
        resolved_ref: prior.and_then(|s| s.resolved_ref.clone()),
        last_fetched_at: prior.and_then(|s| s.last_fetched_at.clone()),
        run_source: prior.and_then(|s| s.run_source.clone()),
    };
    push_condition(
        &mut status,
        prior,
        CONDITION_IMAGE_RESOLVED,
        "Unknown",
        REASON_AWAITING_LEADER,
        Some("follower replica — leader will resolve digest".into()),
        &now,
    );
    if status_substantively_equal(prior, &status) {
        return Ok(());
    }
    patch_status(code_locations_api, name, &status).await
}

async fn patch_error_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    generation: Option<i64>,
    cl: &CodeLocation,
    err: &ImageError,
) -> Result<(), kube_client::Error> {
    let now = jiff::Timestamp::now().to_string();
    let prior = cl.status.as_ref();
    // Only the leader's first image pass replaces a git status (git fields
    // and runtime image); a kept git workspace stays named until it is gone.
    let mut status = CodeLocationStatus {
        phase: Some(CodeLocationPhase::Failed),
        observed_generation: generation,
        resolved_image: prior.and_then(|s| s.resolved_image.clone()),
        grpc_endpoint: None,
        last_reconciled: Some(now.clone()),
        ready_replicas: None,
        message: Some(err.message()),
        conditions: Vec::new(),
        source: prior.and_then(|s| s.source.clone()),
        resolved_commit: prior.and_then(|s| s.resolved_commit.clone()),
        resolved_ref: prior.and_then(|s| s.resolved_ref.clone()),
        last_fetched_at: prior.and_then(|s| s.last_fetched_at.clone()),
        run_source: prior.and_then(|s| s.run_source.clone()),
    };
    push_condition(
        &mut status,
        prior,
        CONDITION_IMAGE_RESOLVED,
        "False",
        err.reason(),
        Some(err.message()),
        &now,
    );
    if !cl.spec.is_git() {
        status
            .conditions
            .extend(kept_conditions(prior, &[CONDITION_WORKSPACE_KEPT]));
    }
    if status_substantively_equal(prior, &status) {
        return Ok(());
    }
    patch_status(code_locations_api, name, &status).await
}

/// Apply jittered poll cadence: `interval + rand(0, interval/4)`.
fn jitter(interval: Duration) -> Duration {
    let quarter = interval.as_millis() / 4;
    if quarter == 0 {
        return interval;
    }
    let extra_ms = fastrand::u64(0..=quarter as u64);
    interval + Duration::from_millis(extra_ms)
}

// Fallback RNG: we intentionally avoid adding a full dep for a micro-jitter.
// This is not a security-sensitive choice — the goal is just to spread
// requeue timing across CRs after operator startup.
mod fastrand {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEED: AtomicU64 = AtomicU64::new(0);

    fn seed() -> u64 {
        let existing = SEED.load(Ordering::Relaxed);
        if existing != 0 {
            return existing;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64 | 1)
            .unwrap_or(0x9E3779B97F4A7C15);
        SEED.store(now, Ordering::Relaxed);
        now
    }

    pub fn u64(range: std::ops::RangeInclusive<u64>) -> u64 {
        let mut x = SEED.load(Ordering::Relaxed);
        if x == 0 {
            x = seed();
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        SEED.store(x, Ordering::Relaxed);
        let span = range.end().saturating_sub(*range.start()).saturating_add(1);
        range.start() + (x % span.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube_client::api::ObjectMeta;
    use rivers_k8s::crd::run::{Run, RunCrdStatus, RunPhase, RunSpec};

    fn make_cl(spec: serde_json::Value) -> CodeLocation {
        let parsed: CodeLocationSpec = serde_json::from_value(spec).unwrap();
        CodeLocation {
            metadata: ObjectMeta {
                name: Some("x".into()),
                namespace: Some("y".into()),
                uid: Some("u".into()),
                generation: Some(1),
                ..Default::default()
            },
            spec: parsed,
            status: None,
        }
    }

    #[test]
    fn refresh_default() {
        assert_eq!(parse_refresh_interval(None), DEFAULT_REFRESH);
    }

    #[test]
    fn refresh_honors_minimum() {
        assert_eq!(parse_refresh_interval(Some("10s")), MIN_REFRESH);
        assert_eq!(parse_refresh_interval(Some("1m")), MIN_REFRESH);
    }

    #[test]
    fn refresh_parses_standard_units() {
        assert_eq!(parse_refresh_interval(Some("2m")), Duration::from_secs(120));
        assert_eq!(
            parse_refresh_interval(Some("1h")),
            Duration::from_secs(3600)
        );
        assert_eq!(
            parse_refresh_interval(Some("300s")),
            Duration::from_secs(300)
        );
    }

    fn workspace_config(vars: &[(&str, &str)]) -> anyhow::Result<WorkspaceConfig> {
        WorkspaceConfig::from_lookup(&|k| {
            vars.iter()
                .find(|(name, _)| *name == k)
                .map(|(_, v)| v.to_string())
        })
    }

    #[test]
    fn workspace_config_refuses_floors_the_prune_cannot_read() {
        for (var, value) in [
            ("RIVERS_WORKSPACE_MIN_AGE", "1d"),
            ("RIVERS_WORKSPACE_MIN_AGE", "7d"),
            ("RIVERS_WORKSPACE_MIN_AGE", "2H"),
            ("RIVERS_WORKSPACE_MIN_AGE", "1h30m"),
            ("RIVERS_WORKSPACE_MIN_AGE", "1.5h"),
            ("RIVERS_WORKSPACE_MIN_AGE", "abc"),
            ("RIVERS_WORKSPACE_MIN_AGE", "-1h"),
            // Past u64::MAX seconds.
            ("RIVERS_WORKSPACE_MIN_AGE", "5124095576030432h"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "three"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "2.5"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "-1"),
        ] {
            let expected = if var == "RIVERS_WORKSPACE_MIN_AGE" {
                format!(
                    "RIVERS_WORKSPACE_MIN_AGE (codeLocation.workspace.minTreeAge): expected a \
                     whole number with s, m or h, like 90m or 24h, got {value:?}"
                )
            } else {
                format!(
                    "RIVERS_WORKSPACE_KEEP_REVISIONS (codeLocation.workspace.keepRevisions): \
                     expected a whole number, got {value:?}"
                )
            };
            match workspace_config(&[(var, value)]) {
                Ok(cfg) => panic!("{var}={value} accepted: {cfg:?}"),
                Err(e) => assert_eq!(e.to_string(), expected),
            }
        }
    }

    #[test]
    fn workspace_config_reads_the_chart_sizes() {
        let sizes = |vars: &[(&str, &str)]| {
            let cfg = workspace_config(vars).unwrap();
            (cfg.shared_size.0, cfg.empty_dir_limit.0)
        };
        assert_eq!(sizes(&[]), ("20Gi".into(), "2Gi".into()));
        assert_eq!(
            sizes(&[
                ("RIVERS_WORKSPACE_SHARED_SIZE", "50Gi"),
                ("RIVERS_WORKSPACE_EMPTYDIR_LIMIT", "1e9"),
            ]),
            ("50Gi".into(), "1e9".into())
        );
    }

    #[test]
    fn workspace_config_refuses_sizes_the_api_server_does_not_take() {
        for (var, setting) in [
            (
                "RIVERS_WORKSPACE_SHARED_SIZE",
                "codeLocation.workspace.shared.size",
            ),
            (
                "RIVERS_WORKSPACE_EMPTYDIR_LIMIT",
                "codeLocation.workspace.sizeLimit",
            ),
        ] {
            for value in ["5GB", "abc", "-1Gi", "0", "2 Gi", "1e"] {
                match workspace_config(&[(var, value)]) {
                    Ok(cfg) => panic!("{var}={value} accepted: {cfg:?}"),
                    Err(e) => assert_eq!(
                        e.to_string(),
                        format!(
                            "{var} ({setting}): expected a Kubernetes quantity more than zero, \
                             like 10Gi or 500M, got {value:?}"
                        )
                    ),
                }
            }
        }
    }

    #[test]
    fn jitter_within_bounds() {
        let base = Duration::from_secs(60);
        for _ in 0..100 {
            let j = jitter(base);
            assert!(j >= base);
            assert!(j <= base + Duration::from_secs(15));
        }
    }

    /// `action`'s requeue delay, which `Action` shows only in its `Debug`.
    fn requeue_after(action: &Action) -> Option<Duration> {
        if *action == Action::await_change() {
            return None;
        }
        let debug = format!("{action:?}");
        let secs = debug
            .strip_prefix("Action { requeue_after: Some(")
            .and_then(|rest| rest.strip_suffix("s) }"))
            .unwrap_or_else(|| panic!("no requeue delay in {debug}"));
        Some(Duration::from_secs_f64(secs.parse().unwrap()))
    }

    /// The delays [`jitter`] makes of `interval`.
    fn jittered(interval: Duration) -> std::ops::RangeInclusive<Duration> {
        interval..=interval + interval / 4
    }

    #[test]
    fn ready_git_code_location_requeues_at_the_sooner_of_ref_poll_and_image_refresh() {
        let minutes = |m: u64| Duration::from_secs(60 * m);
        let (two, five, ten, hour) = (minutes(2), minutes(5), minutes(10), minutes(60));
        let git_ref = |field: &str, value: &str| -> GitRef {
            serde_json::from_value(serde_json::json!({ field: value })).unwrap()
        };
        let pinned = git_ref("commit", &"a".repeat(40));
        let semver_tag = git_ref("tag", "v1.2.3");
        let other_tag = git_ref("tag", "nightly");
        let branch = git_ref("branch", "main");
        // The runtime image as resolve_image_ref answers it, (refresh_after,
        // immutable), with digestRefreshInterval 5m.
        let images = [
            ("digest", (hour, true)),
            ("immutable tag", (five, true)),
            ("mutable tag", (five, false)),
        ];
        // The requeue with each of `images`, by ref and pollInterval.
        let table = [
            (&pinned, two, [None, None, Some(five)]),
            (&semver_tag, two, [Some(hour), Some(hour), Some(five)]),
            (&branch, two, [Some(two), Some(two), Some(two)]),
            (&branch, ten, [Some(ten), Some(ten), Some(five)]),
            (&other_tag, ten, [Some(ten), Some(ten), Some(five)]),
        ];

        let mut wrong = Vec::new();
        for (git_ref, poll, wants) in table {
            for ((image, (refresh_after, immutable)), want) in images.into_iter().zip(wants) {
                let got = requeue_after(&git_requeue(git_ref, poll, refresh_after, immutable));
                let right = match want {
                    None => got.is_none(),
                    Some(want) => got.is_some_and(|got| jittered(want).contains(&got)),
                };
                if !right {
                    let git_ref = serde_json::to_string(git_ref).unwrap();
                    wrong.push(format!(
                        "{git_ref} polled every {poll:?}, {image}: requeue after {got:?}, \
                         want {want:?} + jitter"
                    ));
                }
            }
        }

        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    fn evaluate_deployment_ready() {
        use k8s_openapi::api::apps::v1::DeploymentStatus;
        let cl = make_cl(serde_json::json!({"image": "img", "tag": "v1", "replicas": 2}));
        let d = Deployment {
            status: Some(DeploymentStatus {
                ready_replicas: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (phase, ready, reason, ok) = evaluate_deployment_phase(&cl, Some(&d));
        assert_eq!(phase, CodeLocationPhase::Ready);
        assert_eq!(ready, Some(2));
        assert_eq!(reason, REASON_MIN_REPLICAS);
        assert!(ok);
    }

    #[test]
    fn evaluate_deployment_rolling() {
        use k8s_openapi::api::apps::v1::DeploymentStatus;
        let cl = make_cl(serde_json::json!({"image": "img", "tag": "v1", "replicas": 2}));
        let d = Deployment {
            status: Some(DeploymentStatus {
                ready_replicas: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (phase, ready, _reason, ok) = evaluate_deployment_phase(&cl, Some(&d));
        assert_eq!(phase, CodeLocationPhase::Deploying);
        assert_eq!(ready, Some(1));
        assert!(!ok);
    }

    #[test]
    fn first_component_extracts_host() {
        assert_eq!(
            first_component("ghcr.io/acme/pipeline"),
            Some("ghcr.io".into())
        );
        assert_eq!(
            first_component("localhost:5000/x"),
            Some("localhost:5000".into())
        );
    }

    const KEEP_SET_RUNTIME: &str =
        "ghcr.io/rt@sha256:1a2b3c4d00000000000000000000000000000000000000000000000000000000";

    fn source_at(commit_char: char) -> RunSource {
        serde_json::from_value(serde_json::json!({
            "git": {
                "url": "https://forge.example/r.git",
                "commit": commit_char.to_string().repeat(40),
            },
            "runtimeImage": KEEP_SET_RUNTIME,
        }))
        .unwrap()
    }

    fn run_for(cl: &str, commit_char: char, phase: Option<RunPhase>) -> Run {
        let spec: RunSpec = serde_json::from_value(serde_json::json!({
            "codeLocationRef": { "name": cl },
            "image": KEEP_SET_RUNTIME,
            "target": "*",
            "source": source_at(commit_char),
        }))
        .unwrap();
        let mut run = Run::new(&format!("run-{commit_char}"), spec);
        run.metadata.namespace = Some("y".into());
        run.status = phase.map(|p| RunCrdStatus {
            phase: Some(p),
            ..Default::default()
        });
        run
    }

    #[test]
    fn keep_set_holds_current_and_non_terminal_runs() {
        let mut other_namespace = run_for("analytics", 'h', Some(RunPhase::Running));
        other_namespace.metadata.namespace = Some("z".into());
        let runs = vec![
            run_for("analytics", 'a', Some(RunPhase::Running)),
            run_for("analytics", 'b', Some(RunPhase::Pending)),
            run_for("analytics", 'c', Some(RunPhase::Cancelling)),
            run_for("analytics", 'd', Some(RunPhase::Succeeded)), // released
            run_for("analytics", 'e', Some(RunPhase::Failed)),    // released
            run_for("analytics", 'g', Some(RunPhase::TimedOut)),  // released
            run_for("analytics", 'a', None),                      // no status yet == not terminal
            run_for("other", 'f', Some(RunPhase::Running)),       // different CL
            other_namespace,                                      // same name, other namespace
        ];

        let keep = workspace_keep_csv("current-key", None, "y", "analytics", &runs);

        // a (Running + statusless dedup), b (Pending — queued runs hold
        // their tree), c (Cancelling). Terminal runs release; other CLs'
        // runs are not ours.
        assert_eq!(
            keep,
            format!(
                "{},{},{},current-key",
                source_at('a').workspace_key(),
                source_at('b').workspace_key(),
                source_at('c').workspace_key(),
            )
        );
    }

    #[test]
    fn keep_set_holds_a_run_tree_until_the_run_is_terminal() {
        let table = [
            (None, true),
            (Some(RunPhase::Pending), true),
            (Some(RunPhase::Running), true),
            (Some(RunPhase::Cancelling), true),
            (Some(RunPhase::Succeeded), false),
            (Some(RunPhase::Failed), false),
            (Some(RunPhase::Cancelled), false),
            (Some(RunPhase::TimedOut), false),
        ];
        let run_key = source_at('a').workspace_key();

        for (phase, kept) in table {
            assert_eq!(
                kept,
                !phase.as_ref().is_some_and(RunPhase::is_terminal),
                "{phase:?}"
            );
            let run = run_for("analytics", 'a', phase.clone());

            let keep = workspace_keep_csv("current-key", None, "y", "analytics", &[run]);

            let expected = if kept {
                format!("{run_key},current-key")
            } else {
                "current-key".to_string()
            };
            assert_eq!(keep, expected, "{phase:?}");
        }
    }

    #[test]
    fn keep_set_keys_each_run_on_the_runtime_image_of_its_source() {
        // RunBackendConfig.kubernetes(image=...): the run's pods run another
        // image on the tree the code location's runtime image built.
        let runtime = format!(
            "ghcr.io/acme/rivers-runtime@sha256:{}",
            "1a2b3c4d".repeat(8)
        );
        let mut run = rivers_k8s::crd::run::Run::new(
            "run-a",
            serde_json::from_value(serde_json::json!({
                "codeLocationRef": { "name": "analytics" },
                "image": format!("ghcr.io/acme/gpu-worker@sha256:{}", "ffee0011".repeat(8)),
                "target": "*",
                "source": {
                    "git": { "url": "https://forge.example/r.git", "commit": "a".repeat(40) },
                    "runtimeImage": runtime,
                },
            }))
            .unwrap(),
        );
        run.metadata.namespace = Some("y".into());

        let run_key = run.spec.source.as_ref().unwrap().workspace_key();

        let keep = workspace_keep_csv("current-key", None, "y", "analytics", &[run]);

        assert!(run_key.contains("-1a2b3c4d-"), "{run_key}");
        assert_eq!(keep, format!("{run_key},current-key"));
    }

    #[test]
    fn git_error_reasons_and_retries() {
        use super::super::git::GitError;
        let cases = [
            (
                GitError::RefNotFound("x".into()),
                REASON_REF_NOT_FOUND,
                TERMINAL_RETRY,
            ),
            (
                GitError::InvalidRef("x".into()),
                REASON_INVALID_REF,
                TERMINAL_RETRY,
            ),
            (
                GitError::InvalidUrl("x".into()),
                REASON_INVALID_URL,
                TERMINAL_RETRY,
            ),
            (
                GitError::AuthFailed("x".into()),
                REASON_GIT_AUTH_FAILED,
                TERMINAL_RETRY,
            ),
            (
                GitError::HostKeyRejected("x".into()),
                REASON_GIT_HOST_KEY_REJECTED,
                TERMINAL_RETRY,
            ),
            (
                GitError::KnownHostsUnavailable("x".into()),
                REASON_GIT_HOST_KEY_REJECTED,
                TERMINAL_RETRY,
            ),
            (
                GitError::Unreachable("x".into()),
                REASON_GIT_UNREACHABLE,
                Duration::from_secs(60),
            ),
            (
                GitError::RateLimited {
                    message: "x".into(),
                    retry_after: None,
                },
                REASON_GIT_RATE_LIMITED,
                Duration::from_secs(60),
            ),
            (
                GitError::Malformed("x".into()),
                REASON_GIT_MALFORMED_RESPONSE,
                TERMINAL_RETRY,
            ),
        ];
        for (err, reason, retry) in cases {
            assert_eq!(git_error_reason_retry(&err), (reason, retry), "{err}");
        }
    }

    #[test]
    fn source_display_shapes() {
        let at = |r#ref: Option<&str>| RunSource {
            git: rivers_k8s::crd::run::GitCoordinates {
                url: "https://forge.example/r.git".into(),
                commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".into(),
                r#ref: r#ref.map(str::to_string),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(source_display(&at(Some("refs/heads/main"))), "main@9f3c1ab");
        assert_eq!(
            source_display(&at(Some("refs/tags/v1.2.3"))),
            "v1.2.3@9f3c1ab"
        );
        assert_eq!(source_display(&at(None)), "9f3c1ab (pinned)");
    }

    mod image_resolution {
        use super::*;
        use crate::run::test_helpers::{MockApiState, mock_client};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn digest(c: char) -> String {
            format!("sha256:{}", c.to_string().repeat(64))
        }

        /// A leader whose registry client talks plain HTTP (wiremock).
        fn context(runtime_image: &str) -> Context {
            Context {
                client: mock_client(Arc::new(std::sync::Mutex::new(MockApiState::default()))),
                namespace: "y".into(),
                registry: Arc::new(RegistryClient::with_insecure(true)),
                git: Arc::new(git::GitResolver::new(Duration::from_secs(5), false)),
                runtime_image: runtime_image.parse().unwrap(),
                leader: Arc::new(LeaderGate::leading()),
                code_location_service_account: "rivers-code-location".into(),
                workspace: WorkspaceConfig::default(),
                runs: kube_runtime::reflector::store().0,
                surreal_pod_cfg: Default::default(),
                otel_pod_cfg: Default::default(),
            }
        }

        fn host(server: &MockServer) -> String {
            server.uri().strip_prefix("http://").unwrap().to_string()
        }

        /// Serves `digest` for exactly one HEAD of `manifest_path`.
        async fn registry_serving(manifest_path: &str, digest: &str) -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("HEAD"))
                .and(path(manifest_path))
                .respond_with(
                    ResponseTemplate::new(200).insert_header("docker-content-digest", digest),
                )
                .expect(1)
                .mount(&server)
                .await;
            server
        }

        async fn registry_never_called() -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("HEAD"))
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .mount(&server)
                .await;
            server
        }

        fn git_cl(fields: serde_json::Value) -> CodeLocation {
            let mut spec = serde_json::json!({
                "git": { "url": "https://forge.example/r.git", "ref": { "branch": "main" } },
            });
            spec.as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            make_cl(spec)
        }

        async fn resolve(cl: &CodeLocation, ctx: &Context) -> (String, &'static str) {
            let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), "y");
            match resolve_image(cl, ctx, &secrets).await {
                ImageOutcome::Resolved {
                    resolved_image,
                    reason,
                    ..
                } => (resolved_image, reason),
                ImageOutcome::AwaitingLeader => panic!("expected an image, got AwaitingLeader"),
                ImageOutcome::Error(err) => panic!("expected an image, got {err:?}"),
            }
        }

        #[test]
        fn wanted_image_picks_tag_and_digest() {
            let repository = "ghcr.io/ion-elgreco/rivers-runtime";
            let default_digest = digest('c');
            let spec_digest = digest('d');
            let tagged: ImageRef = format!("{repository}:0.5.0-py3.12").parse().unwrap();
            let pinned: ImageRef = format!("{repository}@{default_digest}").parse().unwrap();
            let want = |repository: &str, tag: Option<&str>, digest: Option<&String>| ImageRef {
                repository: repository.to_string(),
                tag: tag.map(str::to_string),
                digest: digest.cloned(),
            };

            let cases = [
                // git without spec.image: spec.tag / spec.digest replace both
                // of the default's parts.
                (
                    git_cl(serde_json::json!({})),
                    &pinned,
                    want(repository, None, Some(&default_digest)),
                ),
                (
                    git_cl(serde_json::json!({ "tag": "0.6.0-py3.13" })),
                    &pinned,
                    want(repository, Some("0.6.0-py3.13"), None),
                ),
                (
                    git_cl(serde_json::json!({ "digest": spec_digest })),
                    &tagged,
                    want(repository, None, Some(&spec_digest)),
                ),
                // An empty digest counts as unset.
                (
                    git_cl(serde_json::json!({ "digest": "" })),
                    &tagged,
                    tagged.clone(),
                ),
                (
                    make_cl(serde_json::json!({ "image": "ghcr.io/acme/pipeline", "digest": "" })),
                    &tagged,
                    want("ghcr.io/acme/pipeline", None, None),
                ),
            ];
            for (cl, default, expected) in cases {
                let spec = serde_json::to_string(&cl.spec).unwrap();
                assert_eq!(wanted_image(&cl.spec, default), expected, "{spec}");
            }
        }

        #[tokio::test]
        async fn git_default_resolves_its_own_tag() {
            let digest = digest('a');
            let server = registry_serving(
                "/v2/ion-elgreco/rivers-runtime/manifests/0.5.0-py3.12",
                &digest,
            )
            .await;
            let host = host(&server);
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

            let (image, reason) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

            assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
            assert_eq!(reason, REASON_DIGEST_RESOLVED);
        }

        #[tokio::test]
        async fn git_spec_tag_replaces_the_default_tag() {
            let digest = digest('b');
            let server = registry_serving(
                "/v2/ion-elgreco/rivers-runtime/manifests/0.6.0-py3.13",
                &digest,
            )
            .await;
            let host = host(&server);
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

            let cl = git_cl(serde_json::json!({ "tag": "0.6.0-py3.13" }));
            let (image, _) = resolve(&cl, &ctx).await;

            assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
        }

        #[tokio::test]
        async fn git_default_digest_is_pinned_without_a_registry_call() {
            let server = registry_never_called().await;
            let host = host(&server);
            let digest = digest('c');
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime@{digest}"));

            let (image, reason) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

            assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
            assert_eq!(reason, REASON_DIGEST_PINNED);
        }

        #[tokio::test]
        async fn git_default_without_tag_or_digest_resolves_latest() {
            let digest = digest('d');
            let server = registry_serving("/v2/rivers-runtime/manifests/latest", &digest).await;
            let host = host(&server);
            let ctx = context(&format!("{host}/rivers-runtime"));

            let (image, _) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

            assert_eq!(image, format!("{host}/rivers-runtime@{digest}"));
        }

        #[tokio::test]
        async fn git_spec_image_ignores_the_default_tag() {
            let digest = digest('e');
            let server = registry_serving("/v2/acme/runtime/manifests/latest", &digest).await;
            let host = host(&server);
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

            let cl = git_cl(serde_json::json!({ "image": format!("{host}/acme/runtime") }));
            let (image, _) = resolve(&cl, &ctx).await;

            assert_eq!(image, format!("{host}/acme/runtime@{digest}"));
        }

        #[tokio::test]
        async fn image_mode_resolves_spec_tag() {
            let digest = digest('f');
            let server = registry_serving("/v2/acme/pipeline/manifests/v1.2.3", &digest).await;
            let host = host(&server);
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

            let cl = make_cl(serde_json::json!({
                "image": format!("{host}/acme/pipeline"),
                "tag": "v1.2.3",
            }));
            let (image, reason) = resolve(&cl, &ctx).await;

            assert_eq!(image, format!("{host}/acme/pipeline@{digest}"));
            assert_eq!(reason, REASON_DIGEST_RESOLVED);
        }

        #[tokio::test]
        async fn image_mode_spec_digest_is_pinned_without_a_registry_call() {
            let server = registry_never_called().await;
            let host = host(&server);
            let digest = digest('9');
            let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

            let cl = make_cl(serde_json::json!({
                "image": format!("{host}/acme/pipeline"),
                "tag": "v1.2.3",
                "digest": digest,
            }));
            let (image, reason) = resolve(&cl, &ctx).await;

            assert_eq!(image, format!("{host}/acme/pipeline@{digest}"));
            assert_eq!(reason, REASON_DIGEST_PINNED);
        }
    }

    mod git_secret {
        use super::*;
        use crate::run::test_helpers::{MockApiState, mock_client};
        use k8s_openapi::ByteString;

        const SECRET: &str = "git-creds";
        const HTTPS: &str = "https://forge.example/acme/pipelines.git";
        const SSH: &str = "ssh://git@forge.example/acme/pipelines.git";
        pub(super) const IDENTITY: (&str, &str) = ("identity", "PRIVATE KEY");
        pub(super) const KNOWN_HOSTS: (&str, &str) =
            ("known_hosts", "forge.example ssh-ed25519 AAAA");
        pub(super) const USERNAME: (&str, &str) = ("username", "ci-bot");
        pub(super) const PASSWORD: (&str, &str) = ("password", "forge-token");

        pub(super) fn secret(keys: &[(&str, &str)]) -> Secret {
            let data = keys
                .iter()
                .map(|(key, value)| (key.to_string(), ByteString(value.as_bytes().to_vec())))
                .collect();
            Secret {
                data: Some(data),
                ..Default::default()
            }
        }

        /// The credentials of a code location on `url`, whose secretRef (if
        /// `secret_ref`) names [`SECRET`]. A GET of that Secret answers
        /// `stored`; `None` is a Secret that does not exist.
        async fn credentials(
            url: &str,
            secret_ref: bool,
            stored: Option<Result<Secret, kube_core::Status>>,
        ) -> Result<GitCredentials, git::GitError> {
            let mut state = MockApiState::default();
            state
                .secrets
                .extend(stored.map(|answer| (SECRET.to_string(), answer)));
            let api = Api::namespaced(mock_client(Arc::new(std::sync::Mutex::new(state))), "y");
            let source = serde_json::from_value(serde_json::json!({
                "url": url,
                "ref": { "branch": "main" },
                "secretRef": secret_ref.then(|| serde_json::json!({ "name": SECRET })),
            }))
            .unwrap();
            git_credentials(&source, &api).await
        }

        #[tokio::test]
        async fn the_url_scheme_picks_the_secret_keys() {
            let basic = GitCredentials::Basic {
                username: USERNAME.1.into(),
                password: PASSWORD.1.into(),
            };
            let ssh = GitCredentials::Ssh {
                private_key_openssh: IDENTITY.1.into(),
                known_hosts: KNOWN_HOSTS.1.into(),
            };
            let shared = [IDENTITY, KNOWN_HOSTS, USERNAME, PASSWORD];
            let refused = |message: &str| Err(format!("git authentication failed: {message}"));
            let cases: [(&str, &[(&str, &str)], Result<GitCredentials, String>); 10] = [
                (HTTPS, &shared, Ok(basic)),
                (SSH, &shared, Ok(ssh)),
                (
                    HTTPS,
                    &[IDENTITY, KNOWN_HOSTS],
                    Ok(GitCredentials::Anonymous),
                ),
                (HTTPS, &[IDENTITY], Ok(GitCredentials::Anonymous)),
                (HTTPS, &[], Ok(GitCredentials::Anonymous)),
                (
                    HTTPS,
                    &[IDENTITY, KNOWN_HOSTS, USERNAME],
                    refused("git Secret 'git-creds' has `username` but no `password`"),
                ),
                (
                    HTTPS,
                    &[PASSWORD],
                    refused("git Secret 'git-creds' has `password` but no `username`"),
                ),
                (
                    SSH,
                    &[USERNAME, PASSWORD],
                    refused(
                        "git Secret 'git-creds' has no `identity` or `known_hosts` — ssh:// urls \
                         need `identity` and `known_hosts`",
                    ),
                ),
                (
                    SSH,
                    &[KNOWN_HOSTS, USERNAME, PASSWORD],
                    refused(
                        "git Secret 'git-creds' has no `identity` — ssh:// urls need `identity` \
                         and `known_hosts`",
                    ),
                ),
                (
                    SSH,
                    &[IDENTITY, USERNAME, PASSWORD],
                    Err(
                        "known_hosts unavailable: git Secret 'git-creds' has `identity` but no \
                         `known_hosts` — refusing SSH without host-key pinning"
                            .into(),
                    ),
                ),
            ];
            for (url, keys, want) in cases {
                let got = credentials(url, true, Some(Ok(secret(keys)))).await;
                let keys: Vec<_> = keys.iter().map(|(key, _)| *key).collect();
                assert_eq!(got.map_err(|e| e.to_string()), want, "{url} with {keys:?}");
            }
        }

        #[tokio::test]
        async fn without_a_secret_https_is_anonymous_and_ssh_is_refused() {
            assert_eq!(
                credentials(HTTPS, false, None)
                    .await
                    .map_err(|e| e.to_string()),
                Ok(GitCredentials::Anonymous)
            );
            assert_eq!(
                credentials(SSH, false, None)
                    .await
                    .map_err(|e| e.to_string()),
                Err("git authentication failed: ssh:// urls need a git Secret \
                     (spec.git.secretRef) with `identity` and `known_hosts`"
                    .to_string())
            );
        }

        #[tokio::test]
        async fn a_git_secret_that_is_missing_or_forbidden_is_terminal() {
            let forbidden = "secrets \"git-creds\" is forbidden: User \
                             \"system:serviceaccount:rivers:rivers-operator\" cannot get \
                             resource \"secrets\" in API group \"\" in the namespace \"y\"";
            let cases = [
                (
                    None,
                    "git authentication failed: git Secret 'git-creds' does not exist".to_string(),
                ),
                (
                    Some(Err(
                        kube_core::Status::failure(forbidden, "Forbidden").with_code(403)
                    )),
                    format!(
                        "git authentication failed: the operator may not read git Secret \
                         'git-creds': {forbidden}"
                    ),
                ),
            ];
            for (stored, message) in cases {
                let error = credentials(HTTPS, true, stored).await.unwrap_err();
                assert!(matches!(error, git::GitError::AuthFailed(_)), "{error:?}");
                assert!(!error.is_transient(), "{error}");
                assert_eq!(error.to_string(), message);
            }
        }

        #[tokio::test]
        async fn an_api_server_error_reading_the_git_secret_is_transient() {
            let status =
                kube_core::Status::failure("etcdserver: request timed out", "InternalError")
                    .with_code(500);

            let error = credentials(HTTPS, true, Some(Err(status)))
                .await
                .unwrap_err();

            assert!(matches!(error, git::GitError::Unreachable(_)), "{error:?}");
            assert!(error.is_transient());
            assert!(
                error
                    .to_string()
                    .starts_with("git host unreachable: reading git Secret 'git-creds': "),
                "{error}"
            );
        }

        /// `[username, password]` of Basic credentials (their `Debug` is
        /// redacted).
        fn basic(credentials: &GitCredentials) -> [&str; 2] {
            match credentials {
                GitCredentials::Basic { username, password } => [username, password],
                other => panic!("not Basic: {other:?}"),
            }
        }

        #[tokio::test]
        async fn username_and_password_drop_trailing_newlines_like_the_pod() {
            let stored = secret(&[
                ("username", "bot\n"),
                ("password", "ghp_TOKEN\n\n"),
                ("identity", "PRIVATE KEY\n"),
                ("known_hosts", "forge.example ssh-ed25519 AAAA\n"),
            ]);

            let https = credentials(HTTPS, true, Some(Ok(stored.clone())))
                .await
                .unwrap();
            assert_eq!(basic(&https), ["bot", "ghp_TOKEN"]);

            let ssh = credentials(SSH, true, Some(Ok(stored))).await.unwrap();
            let GitCredentials::Ssh {
                private_key_openssh,
                known_hosts,
            } = &ssh
            else {
                panic!("not Ssh: {ssh:?}");
            };
            assert_eq!(
                [private_key_openssh.as_str(), known_hosts.as_str()],
                ["PRIVATE KEY\n", "forge.example ssh-ed25519 AAAA\n"]
            );
        }

        #[tokio::test]
        async fn username_and_password_keep_what_the_pods_cat_keeps() {
            // (stored value, what the sync script's `$(cat …)` reads)
            let cases = [
                ("ghp\nTOKEN\n", "ghp\nTOKEN"),
                ("ghp_TOKEN\r\n", "ghp_TOKEN\r"),
            ];
            for (stored, read) in cases {
                let got = credentials(
                    HTTPS,
                    true,
                    Some(Ok(secret(&[("username", stored), ("password", stored)]))),
                )
                .await
                .unwrap();

                assert_eq!(basic(&got), [read, read], "stored {stored:?}");
            }
        }
    }

    mod rollout {
        use super::*;
        use crate::run::test_helpers::{ApiRequest, MockApiState, mock_client};
        use k8s_openapi::api::apps::v1::DeploymentStatus;
        use kube_runtime::reflector::store::Writer;
        use kube_runtime::watcher;
        use serde_json::json;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const RUNTIME: &str = "ghcr.io/ion-elgreco/rivers-runtime";
        const URL: &str = "https://forge.example/r.git";

        fn commit(c: char) -> String {
            c.to_string().repeat(40)
        }

        fn digest(c: char) -> String {
            format!("sha256:{}", c.to_string().repeat(64))
        }

        fn runtime(c: char) -> String {
            format!("{RUNTIME}@{}", digest(c))
        }

        fn run_source_json(c: char) -> serde_json::Value {
            json!({
                "git": { "url": URL, "commit": commit(c) },
                "dependencies": { "mode": "auto" },
                "runtimeImage": runtime(c),
            })
        }

        /// Status of a git CL whose every pod runs commit `c` on runtime `c`.
        fn serving_status(c: char) -> serde_json::Value {
            json!({
                "phase": "Ready",
                "observedGeneration": 1,
                "resolvedImage": runtime(c),
                "resolvedCommit": commit(c),
                "runSource": run_source_json(c),
                "source": format!("{} (pinned)", &commit(c)[..7]),
                "readyReplicas": 1,
            })
        }

        /// Git CL pinned to commit `target` on runtime digest `target` — no
        /// registry or git traffic — with `prior` as its status.
        fn git_cl_at(target: char, prior: serde_json::Value) -> CodeLocation {
            let mut cl = make_cl(json!({
                "git": { "url": URL, "ref": { "commit": commit(target) } },
                "digest": digest(target),
            }));
            cl.status = Some(serde_json::from_value(prior).unwrap());
            cl
        }

        fn deployment(generation: i64, status: DeploymentStatus) -> Deployment {
            Deployment {
                metadata: ObjectMeta {
                    name: Some("x".into()),
                    generation: Some(generation),
                    ..Default::default()
                },
                spec: None,
                status: Some(status),
            }
        }

        /// One replica: the old pod is ready, the surge pod of the new
        /// template is not.
        fn mid_rollout() -> Deployment {
            deployment(
                2,
                DeploymentStatus {
                    observed_generation: Some(2),
                    replicas: Some(2),
                    updated_replicas: Some(1),
                    ready_replicas: Some(1),
                    available_replicas: Some(1),
                    ..Default::default()
                },
            )
        }

        fn rolled_out() -> Deployment {
            deployment(
                2,
                DeploymentStatus {
                    observed_generation: Some(2),
                    replicas: Some(1),
                    updated_replicas: Some(1),
                    ready_replicas: Some(1),
                    available_replicas: Some(1),
                    ..Default::default()
                },
            )
        }

        fn condition<'a>(status: &'a serde_json::Value, kind: &str) -> &'a serde_json::Value {
            status["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["type"] == kind)
                .unwrap_or_else(|| panic!("no {kind} condition: {status}"))
        }

        type ApiState = Arc<std::sync::Mutex<MockApiState>>;

        /// Mock API server holding `cl`, whose Deployment applies keep
        /// `observed`'s generation and status.
        fn api_with(cl: CodeLocation, observed: Deployment) -> ApiState {
            let state = ApiState::default();
            {
                let mut s = state.lock().unwrap();
                s.code_locations.insert("x".into(), cl);
                s.deployments.insert("x".into(), observed);
            }
            state
        }

        /// A resolver that may fetch the `http://` urls of the test git hosts
        /// (`operator.git.allowInsecure`).
        fn resolver() -> Arc<git::GitResolver> {
            Arc::new(git::GitResolver::new(Duration::from_secs(5), true))
        }

        /// One reconcile pass over the stored CL, resolving refs with `git`;
        /// returns its requeue and the API requests it made.
        async fn reconcile_with(
            state: &ApiState,
            leader: LeaderGate,
            git: &Arc<git::GitResolver>,
        ) -> (Action, Vec<ApiRequest>) {
            reconcile_in(
                state,
                leader,
                git,
                WorkspaceConfig::default(),
                run_store(Vec::new()),
            )
            .await
        }

        /// [`reconcile_with`] under the chart's `workspace` settings, with
        /// `runs` as the run controller's store.
        async fn reconcile_in(
            state: &ApiState,
            leader: LeaderGate,
            git: &Arc<git::GitResolver>,
            workspace: WorkspaceConfig,
            runs: Store<Run>,
        ) -> (Action, Vec<ApiRequest>) {
            let (cl, seen) = {
                let s = state.lock().unwrap();
                (s.code_locations["x"].clone(), s.requests.len())
            };
            let ctx = Context {
                client: mock_client(state.clone()),
                namespace: "y".into(),
                registry: Arc::new(RegistryClient::with_insecure(true)),
                git: git.clone(),
                runtime_image: format!("{RUNTIME}:0.5.0-py3.12").parse().unwrap(),
                leader: Arc::new(leader),
                code_location_service_account: "rivers-code-location".into(),
                workspace,
                runs,
                surreal_pod_cfg: Default::default(),
                otel_pod_cfg: Default::default(),
            };
            let requeue = reconcile(Arc::new(cl), Arc::new(ctx)).await.unwrap();
            (requeue, state.lock().unwrap().requests[seen..].to_vec())
        }

        /// The run controller's store while its first LIST comes in: `runs`
        /// have arrived, more may follow. It stays so while the writer lives.
        fn listing_run_store(runs: Vec<Run>) -> (Store<Run>, Writer<Run>) {
            let (store, mut writer) = kube_runtime::reflector::store();
            writer.apply_watcher_event(&watcher::Event::Init);
            for run in runs {
                writer.apply_watcher_event(&watcher::Event::InitApply(run));
            }
            (store, writer)
        }

        /// The run controller's store once its first LIST, `runs`, is in.
        fn run_store(runs: Vec<Run>) -> Store<Run> {
            let (store, mut writer) = listing_run_store(runs);
            writer.apply_watcher_event(&watcher::Event::InitDone);
            store
        }

        /// The status `requests` patched, if any.
        fn patched_status(requests: &[ApiRequest]) -> Option<serde_json::Value> {
            requests
                .iter()
                .rev()
                .find(|r| r.method == "PATCH" && r.path.ends_with("/codelocations/x/status"))
                .map(|r| r.body.as_ref().unwrap()["status"].clone())
        }

        /// One reconcile pass over the stored CL with a fresh resolver;
        /// returns the status it patched, if any.
        async fn pass(state: &ApiState, leader: LeaderGate) -> Option<serde_json::Value> {
            patched_status(&reconcile_with(state, leader, &resolver()).await.1)
        }

        async fn leader_pass(cl: CodeLocation, observed: Deployment) -> serde_json::Value {
            pass(&api_with(cl, observed), LeaderGate::leading())
                .await
                .expect("no status patch")
        }

        /// Commit `c` built with runtime digest `c`.
        fn tree(c: char) -> RunSource {
            serde_json::from_value(run_source_json(c)).unwrap()
        }

        fn rollout(template: char, complete: bool, ready: i32) -> GitRollout {
            GitRollout {
                template: Some(tree(template)),
                complete,
                ready_replicas: Some(ready),
                deadline_exceeded: false,
                build_failure: None,
            }
        }

        /// The status a leader that resolved `target` publishes for `rollout`.
        fn leader_status(
            cl: &CodeLocation,
            target: char,
            rollout: GitRollout,
        ) -> CodeLocationStatus {
            let update = GitStatusUpdate {
                resolution: Some(GitResolution::ImageResolved {
                    image: runtime(target),
                    image_reason: REASON_DIGEST_PINNED,
                    source: Ok(git::ResolvedRef {
                        commit: commit(target),
                        ref_name: None,
                    }),
                }),
                fetched_at: None,
                rollout,
                apply_failure: None,
                endpoint: grpc_endpoint("x", "y", 3001),
            };
            git_status(cl, update, "2026-10-03T00:00:00Z".parse().unwrap())
        }

        fn available(status: &CodeLocationStatus) -> (&str, Option<&str>, Option<&str>) {
            let c = status
                .conditions
                .iter()
                .find(|c| c.r#type == CONDITION_DEPLOYMENT_AVAILABLE)
                .expect("DeploymentAvailable condition");
            (&c.status, c.reason.as_deref(), c.message.as_deref())
        }

        #[test]
        fn mid_rollout_keeps_runs_on_the_rolled_out_tree() {
            // Both pods still run a; the first pod of b is not ready.
            let mut cl = git_cl_at('b', serving_status('a'));
            cl.spec.replicas = 2;

            let status = leader_status(&cl, 'b', rollout('b', false, 2));

            assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
            assert_eq!(status.resolved_commit, Some(commit('a')));
            assert_eq!(status.resolved_ref, None);
            assert_eq!(status.resolved_image, Some(runtime('a')));
            assert_eq!(status.run_source, Some(tree('a')));
            assert_eq!(status.source.as_deref(), Some("aaaaaaa (pinned)"));
            let message = format!(
                "rolling out bbbbbbb (pinned) on {}; runs use aaaaaaa (pinned) on {}",
                runtime('b'),
                runtime('a')
            );
            assert_eq!(
                available(&status),
                ("True", Some(REASON_ROLLING_OUT), Some(message.as_str()))
            );
        }

        #[test]
        fn completed_rollout_moves_every_serving_field_to_the_template() {
            let mut cl = git_cl_at('b', serving_status('a'));
            cl.spec.replicas = 2;

            let status = leader_status(&cl, 'b', rollout('b', true, 2));

            assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
            assert_eq!(status.resolved_commit, Some(commit('b')));
            assert_eq!(status.resolved_ref, None);
            assert_eq!(status.resolved_image, Some(runtime('b')));
            assert_eq!(status.run_source, Some(tree('b')));
            assert_eq!(status.source.as_deref(), Some("bbbbbbb (pinned)"));
            assert_eq!(
                available(&status),
                ("True", Some(REASON_MIN_REPLICAS), None)
            );
        }

        #[test]
        fn first_rollout_stays_deploying_until_it_completes() {
            // The one pod is ready, but the controller has not confirmed the
            // rollout and no tree has rolled out before.
            let mut cl = git_cl_at('b', json!({}));
            cl.status = None;

            let status = leader_status(&cl, 'b', rollout('b', false, 1));

            assert_eq!(status.phase, Some(CodeLocationPhase::Deploying));
            assert_eq!(status.resolved_commit, None);
            assert_eq!(status.resolved_image, None);
            assert_eq!(status.run_source, None);
            assert_eq!(status.source, None);
            assert_eq!(
                available(&status),
                (
                    "False",
                    Some(REASON_ROLLING_OUT),
                    Some("rolling out bbbbbbb (pinned)")
                )
            );
        }

        #[test]
        fn failed_build_keeps_runs_on_the_rolled_out_tree_and_says_so() {
            // b fails to build: its pod never gets ready, the old pod serves.
            let cl = git_cl_at('b', serving_status('a'));
            let rollout = GitRollout {
                template: Some(RunSource {
                    runtime_image: runtime('a'),
                    ..tree('b')
                }),
                deadline_exceeded: true,
                ..rollout('b', false, 1)
            };

            let status = leader_status(&cl, 'b', rollout);

            assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
            assert_eq!(status.resolved_commit, Some(commit('a')));
            assert_eq!(status.run_source, Some(tree('a')));
            assert_eq!(
                available(&status),
                (
                    "True",
                    Some(REASON_PROGRESS_DEADLINE),
                    Some(
                        "rollout of bbbbbbb (pinned) exceeded its progress deadline; \
                         runs use aaaaaaa (pinned)"
                    )
                )
            );
        }

        #[test]
        fn rollout_completes_only_when_every_pod_runs_the_current_template() {
            let deployment =
                |replicas, generation, [observed, total, updated, ready, available]: [i32; 5]| {
                    Deployment {
                        metadata: ObjectMeta {
                            generation: Some(generation),
                            ..Default::default()
                        },
                        spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                            replicas: Some(replicas),
                            ..Default::default()
                        }),
                        status: Some(DeploymentStatus {
                            observed_generation: Some(observed.into()),
                            replicas: Some(total),
                            updated_replicas: Some(updated),
                            ready_replicas: Some(ready),
                            available_replicas: Some(available),
                            ..Default::default()
                        }),
                    }
                };
            let cases = [
                (
                    "every pod updated and ready",
                    deployment(2, 3, [3, 2, 2, 2, 2]),
                    true,
                ),
                (
                    "template not yet seen by the controller",
                    deployment(2, 3, [2, 2, 2, 2, 2]),
                    false,
                ),
                ("old pod left", deployment(2, 3, [3, 3, 2, 2, 2]), false),
                (
                    "new pod not created",
                    deployment(2, 3, [3, 2, 1, 2, 2]),
                    false,
                ),
                (
                    "new pod not ready",
                    deployment(2, 3, [3, 2, 2, 1, 1]),
                    false,
                ),
                (
                    "new pod not available",
                    deployment(2, 3, [3, 2, 2, 2, 1]),
                    false,
                ),
                ("scaled to zero", deployment(0, 3, [3, 0, 0, 0, 0]), false),
            ];
            for (case, d, complete) in cases {
                assert_eq!(rollout_complete(&d), complete, "{case}");
            }
        }

        #[test]
        fn rollout_reads_the_tree_from_the_pod_template() {
            let cl = git_cl_at('b', serving_status('a'));
            let pieces = workspace::builder_pod_pieces(&WorkspaceSpec {
                volume: WorkspaceVolume::EmptyDir { size_limit: None },
                runtime_image: runtime('b'),
                git_url: URL.into(),
                commit: commit('b'),
                git_ref: None,
                path: None,
                secret_name: None,
                deps: Default::default(),
                keep_config_map: None,
                keep_revisions: None,
                min_tree_age: None,
                extra_env: Vec::new(),
            });
            let mut d = build_git_deployment(
                &cl,
                &runtime('b'),
                "rivers-code-location",
                &Default::default(),
                &Default::default(),
                &pieces,
            );
            d.metadata.generation = Some(2);
            d.status = Some(DeploymentStatus {
                observed_generation: Some(2),
                ready_replicas: Some(1),
                conditions: Some(vec![k8s_openapi::api::apps::v1::DeploymentCondition {
                    type_: "Progressing".into(),
                    status: "False".into(),
                    reason: Some("ProgressDeadlineExceeded".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            });

            let rollout = git_rollout(Some(&d));
            assert_eq!(rollout.template, Some(tree('b')));
            assert!(rollout.deadline_exceeded);

            // Until the controller sees this template, the deadline is the
            // previous rollout's.
            d.status.as_mut().unwrap().observed_generation = Some(1);
            assert!(!git_rollout(Some(&d)).deadline_exceeded);
        }

        #[tokio::test]
        async fn code_location_pods_hand_their_runtime_image_with_their_source() {
            let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());

            pass(&state, LeaderGate::leading()).await;

            let template = state.lock().unwrap().deployments["x"]
                .spec
                .clone()
                .and_then(|d| d.template.spec)
                .unwrap();
            let main = template
                .containers
                .iter()
                .find(|c| c.name == MAIN_CONTAINER)
                .unwrap();
            let source: serde_json::Value = main
                .env
                .iter()
                .flatten()
                .find(|e| e.name == rivers_k8s::env::ENV_RUN_SOURCE)
                .and_then(|e| e.value.as_deref())
                .map(|json| serde_json::from_str(json).unwrap())
                .unwrap();
            assert_eq!(main.image, Some(runtime('b')));
            assert_eq!(source["runtimeImage"], runtime('b'), "{source}");
        }

        #[test]
        fn keep_set_holds_the_serving_tree_beside_the_target() {
            let keep = workspace_keep_csv(
                &tree('b').workspace_key(),
                Some(&tree('a').workspace_key()),
                "y",
                "x",
                &[],
            );

            assert_eq!(
                keep,
                format!(
                    "{},{}",
                    tree('a').workspace_key(),
                    tree('b').workspace_key()
                )
            );
        }

        #[tokio::test]
        async fn new_dependencies_at_the_same_commit_get_and_keep_their_own_tree() {
            // Tree a with the gpu extra serves; the user adds a group, with
            // no new commit or runtime image.
            let mut serving = serving_status('a');
            serving["runSource"]["dependencies"]["extras"] = json!(["gpu"]);
            let mut cl = git_cl_at('a', serving.clone());
            let deps = &mut cl.spec.git.as_mut().unwrap().dependencies;
            deps.extras = vec!["gpu".into()];
            deps.groups = vec!["test".into()];
            let state = api_with(cl, mid_rollout());
            // Admitted before the gpu extra: it still runs on plain tree a.
            let mut run = Run::new(
                "r",
                serde_json::from_value(json!({
                    "codeLocationRef": { "name": "x" },
                    "image": runtime('a'),
                    "target": "job",
                    "source": run_source_json('a'),
                }))
                .unwrap(),
            );
            run.metadata.namespace = Some("y".into());

            reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(vec![run]),
            )
            .await;

            let s = state.lock().unwrap();
            let target = template_source(&s.deployments["x"]).unwrap();
            assert_eq!(target.dependencies.groups, ["test"]);
            let pod = s.deployments["x"]
                .spec
                .clone()
                .unwrap()
                .template
                .spec
                .unwrap();
            let sub_paths: Vec<_> = pod
                .init_containers
                .iter()
                .flatten()
                .chain(&pod.containers)
                .flat_map(|c| c.volume_mounts.iter().flatten())
                .filter(|m| m.mount_path == workspace::WORKSPACE_MOUNT)
                .map(|m| m.sub_path.clone())
                .collect();
            let target_key = target.workspace_key();
            assert_eq!(
                sub_paths,
                [Some(target_key.clone()), Some(target_key.clone())]
            );
            let serving_tree: RunSource =
                serde_json::from_value(serving["runSource"].clone()).unwrap();
            let mut trees = [
                tree('a').workspace_key(),
                serving_tree.workspace_key(),
                target_key,
            ];
            trees.sort();
            assert_eq!(
                s.config_maps[&keep_config_map_name("x")]
                    .data
                    .as_ref()
                    .unwrap()["keep"],
                trees.join(","),
                "one tree per dependency setting"
            );
        }

        fn shared() -> WorkspaceConfig {
            WorkspaceConfig {
                shared_enabled: true,
                ..Default::default()
            }
        }

        /// The keep ConfigMap's value for `keys`.
        fn keep_value(mut keys: Vec<String>) -> String {
            keys.sort();
            keys.join(",")
        }

        fn keep_in(state: &ApiState) -> String {
            state.lock().unwrap().config_maps[&keep_config_map_name("x")]
                .data
                .as_ref()
                .unwrap()["keep"]
                .clone()
        }

        /// Commit b rolls out while tree a serves and [`long_run`] still uses
        /// tree c, outside the floors. The keep-set in force is from a pass
        /// before commit b.
        fn rolling_out_beside_a_long_run() -> ApiState {
            let cl = git_cl_at('b', serving_status('a'));
            let in_force = keep_value(vec![
                tree('a').workspace_key(),
                source_at('c').workspace_key(),
            ]);
            let state = api_with(cl.clone(), mid_rollout());
            state.lock().unwrap().config_maps.insert(
                keep_config_map_name("x"),
                build_keep_config_map(&cl, &in_force),
            );
            state
        }

        fn long_run() -> Run {
            run_for("x", 'c', Some(RunPhase::Running))
        }

        /// The requests of `requests` that LIST Runs.
        fn runs_lists(requests: &[ApiRequest]) -> Vec<&ApiRequest> {
            requests
                .iter()
                .filter(|r| r.method == "GET" && r.path.ends_with("/runs"))
                .collect()
        }

        #[tokio::test]
        async fn keep_set_takes_the_runs_of_this_code_location_from_the_run_store() {
            let state = rolling_out_beside_a_long_run();
            let mut other_namespace = run_for("x", 'f', Some(RunPhase::Running));
            other_namespace.metadata.namespace = Some("z".into());
            let runs = run_store(vec![
                long_run(),
                run_for("x", 'd', Some(RunPhase::Succeeded)),
                run_for("other", 'e', Some(RunPhase::Running)),
                other_namespace,
            ]);

            let (_, requests) =
                reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

            let lists = runs_lists(&requests);
            assert!(lists.is_empty(), "{lists:?}");
            assert_eq!(
                keep_in(&state),
                keep_value(vec![
                    tree('a').workspace_key(),
                    tree('b').workspace_key(),
                    source_at('c').workspace_key(),
                ])
            );
        }

        #[tokio::test]
        async fn keep_set_in_force_stays_until_the_run_store_has_synced() {
            let state = rolling_out_beside_a_long_run();
            let in_force = keep_in(&state);
            // The first LIST has not reached the long run yet.
            let (runs, _listing) = listing_run_store(Vec::new());

            let (_, requests) =
                reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

            let lists = runs_lists(&requests);
            assert!(lists.is_empty(), "{lists:?}");
            let keep_requests: Vec<_> = requests
                .iter()
                .filter(|r| r.path.contains("/configmaps/"))
                .collect();
            assert!(keep_requests.is_empty(), "{keep_requests:?}");
            assert_eq!(keep_in(&state), in_force);
            // The rest of the pass goes on: b rolls out, runs stay on a.
            assert_eq!(
                template_source(&state.lock().unwrap().deployments["x"]),
                Some(tree('b'))
            );
            assert_eq!(
                patched_status(&requests).expect("status patch")["runSource"],
                run_source_json('a')
            );
        }

        #[tokio::test]
        async fn sync_container_gets_the_chart_floors_as_whole_numbers() {
            for (chart, keep, age) in [
                (vec![], "3", "3600"),
                (
                    vec![
                        ("RIVERS_WORKSPACE_KEEP_REVISIONS", "5"),
                        ("RIVERS_WORKSPACE_MIN_AGE", "90m"),
                    ],
                    "5",
                    "5400",
                ),
                (
                    vec![
                        ("RIVERS_WORKSPACE_KEEP_REVISIONS", "0"),
                        ("RIVERS_WORKSPACE_MIN_AGE", "0s"),
                    ],
                    "0",
                    "0",
                ),
                (vec![("RIVERS_WORKSPACE_MIN_AGE", "24h")], "3", "86400"),
                (vec![("RIVERS_WORKSPACE_MIN_AGE", "45s")], "3", "45"),
                (vec![("RIVERS_WORKSPACE_MIN_AGE", "600")], "3", "600"),
            ] {
                let mut vars = vec![("RIVERS_WORKSPACE_SHARED_ENABLED", "true")];
                vars.extend(chart.iter().copied());
                let workspace = workspace_config(&vars).unwrap();
                let state = api_with(git_cl_at('a', serving_status('a')), rolled_out());

                reconcile_in(
                    &state,
                    LeaderGate::leading(),
                    &resolver(),
                    workspace,
                    run_store(Vec::new()),
                )
                .await;

                let pod = state.lock().unwrap().deployments["x"]
                    .spec
                    .clone()
                    .unwrap()
                    .template
                    .spec
                    .unwrap();
                let floors: Vec<(&str, &str)> = pod.init_containers.as_ref().unwrap()[0]
                    .env
                    .iter()
                    .flatten()
                    .filter(|e| {
                        matches!(
                            e.name.as_str(),
                            "RIVERS_WORKSPACE_KEEP_REVISIONS" | "RIVERS_WORKSPACE_MIN_AGE_SECONDS"
                        )
                    })
                    .filter_map(|e| Some((e.name.as_str(), e.value.as_deref()?)))
                    .collect();
                assert_eq!(
                    floors,
                    [
                        ("RIVERS_WORKSPACE_KEEP_REVISIONS", keep),
                        ("RIVERS_WORKSPACE_MIN_AGE_SECONDS", age),
                    ],
                    "{chart:?}"
                );
            }
        }

        #[tokio::test]
        async fn leader_keeps_runs_on_the_rolled_out_tree_while_a_commit_rolls_out() {
            let status = leader_pass(git_cl_at('b', serving_status('a')), mid_rollout()).await;

            assert_eq!(status["phase"], "Ready");
            assert_eq!(status["resolvedCommit"], commit('a'));
            assert_eq!(status["resolvedImage"], runtime('a'));
            assert_eq!(status["runSource"], run_source_json('a'));
            let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
            assert_eq!(available["reason"], REASON_ROLLING_OUT, "{available}");
            assert!(
                available["message"]
                    .as_str()
                    .is_some_and(|m| m.contains(&commit('b')[..7])),
                "{available}"
            );
        }

        #[tokio::test]
        async fn leader_moves_runs_to_the_new_tree_once_every_pod_runs_it() {
            let status = leader_pass(git_cl_at('b', serving_status('a')), rolled_out()).await;

            assert_eq!(status["phase"], "Ready");
            assert_eq!(status["resolvedCommit"], commit('b'));
            assert_eq!(status["resolvedImage"], runtime('b'));
            assert_eq!(status["runSource"], run_source_json('b'));
            assert_eq!(status["source"], format!("{} (pinned)", &commit('b')[..7]));
            let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
            assert_eq!(available["reason"], REASON_MIN_REPLICAS, "{available}");
        }

        #[tokio::test]
        async fn follower_publishes_nothing_new_after_the_leader_mid_rollout() {
            let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());

            let leader = pass(&state, LeaderGate::leading()).await;
            assert_eq!(leader.unwrap()["resolvedCommit"], commit('a'));
            // Same Deployment, the leader's status: a follower that disagreed
            // would patch here, and the two would overwrite each other.
            assert_eq!(pass(&state, LeaderGate::new()).await, None);
        }

        const STALE_LOCK: &str = "error: The lockfile at `uv.lock` needs to be updated, but \
                                  `--locked` was provided. To update the lockfile, run `uv lock`.";

        /// A pod of code location x that runs `tree`; `sync` is the status of
        /// its `workspace` init container.
        fn code_location_pod(name: &str, tree: &RunSource, sync: serde_json::Value) -> Pod {
            let mut sync_status = json!({ "name": "workspace" });
            sync_status
                .as_object_mut()
                .unwrap()
                .extend(sync.as_object().unwrap().clone());
            serde_json::from_value(json!({
                "metadata": {
                    "name": name,
                    "namespace": "y",
                    "labels": crate::codelocation::resources::labels("x"),
                },
                "spec": {
                    "containers": [{
                        "name": MAIN_CONTAINER,
                        "env": [{
                            "name": rivers_k8s::env::ENV_RUN_SOURCE,
                            "value": serde_json::to_string(tree).unwrap(),
                        }],
                    }],
                },
                "status": { "initContainerStatuses": [sync_status] },
            }))
            .unwrap()
        }

        /// The sync failed with `message` at `finished_at`; kubelet waits
        /// before it starts it again.
        fn failed_sync(message: &str, finished_at: &str) -> serde_json::Value {
            json!({
                "state": { "waiting": {
                    "reason": "CrashLoopBackOff",
                    "message": "back-off 40s restarting failed container=workspace",
                } },
                "lastState": { "terminated": {
                    "exitCode": 1,
                    "reason": "Error",
                    "message": message,
                    "finishedAt": finished_at,
                } },
                "restartCount": 3,
            })
        }

        fn completed_sync() -> serde_json::Value {
            json!({ "state": { "terminated": {
                "exitCode": 0,
                "reason": "Completed",
                "finishedAt": "2026-10-02T00:00:00Z",
            } } })
        }

        fn add_pods(state: &ApiState, pods: impl IntoIterator<Item = Pod>) {
            let mut s = state.lock().unwrap();
            for pod in pods {
                s.pods.insert(pod.metadata.name.clone().unwrap(), pod);
            }
        }

        #[tokio::test]
        async fn a_failed_build_of_the_tree_rolling_out_shows_in_status() {
            let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());
            // The old pod serves a; the new pod cannot build b.
            add_pods(
                &state,
                [
                    code_location_pod("x-a", &tree('a'), completed_sync()),
                    code_location_pod(
                        "x-b",
                        &tree('b'),
                        failed_sync(&format!("{STALE_LOCK}\n"), "2026-10-03T00:00:00Z"),
                    ),
                ],
            );

            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            let failure = format!(
                "workspace build of bbbbbbb (pinned) failed (Error, exit code 1): {STALE_LOCK}"
            );
            assert_eq!(status["message"], failure, "{status}");
            let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
            assert_eq!(available["reason"], REASON_ROLLING_OUT, "{available}");
            assert_eq!(
                available["message"],
                format!(
                    "rolling out bbbbbbb (pinned) on {}; runs use aaaaaaa (pinned) on {}; {failure}",
                    runtime('b'),
                    runtime('a')
                )
            );
            assert_eq!(status["phase"], "Ready");
            assert_eq!(status["runSource"], run_source_json('a'));
            // The same failure in the next passes: nothing new to write, from
            // either replica.
            assert_eq!(pass(&state, LeaderGate::leading()).await, None);
            assert_eq!(pass(&state, LeaderGate::new()).await, None);
        }

        /// What the API server answers, with `code`, for a Deployment whose
        /// `emptyDir` size it cannot read.
        fn unreadable_size(code: u16) -> kube_core::Status {
            kube_core::Status::failure(
                "Deployment.apps \"x\" is invalid: spec.template.spec.volumes[0].emptyDir.\
                 sizeLimit: Invalid value: \"5GB\": quantities must match the regular \
                 expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
                "Invalid",
            )
            .with_code(code)
        }

        /// `cl` with `workspaceSize: 5GB`, as admitted before the webhook
        /// checked it.
        fn sized_5gb(mut cl: CodeLocation) -> CodeLocation {
            cl.spec.git.as_mut().unwrap().workspace_size = Some(Quantity("5GB".into()));
            cl
        }

        fn refuse(state: &ApiState, object: &str, answer: &kube_core::Status) {
            let mut s = state.lock().unwrap();
            s.refused.insert(object.to_string(), answer.clone());
        }

        #[tokio::test]
        async fn a_refused_deployment_shows_in_status_while_runs_keep_the_serving_tree() {
            let state = api_with(sized_5gb(git_cl_at('b', serving_status('a'))), rolled_out());
            let answer = unreadable_size(422);
            refuse(&state, "deployments/x", &answer);

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            let failure = format!("applying Deployment 'x' failed: {}", answer.message);
            let available = format!("{failure}; runs use aaaaaaa (pinned)");
            assert_eq!(status["message"], failure, "{status}");
            assert_eq!(
                says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
                ("True", REASON_APPLY_FAILED, available.as_str())
            );
            assert_eq!(status["phase"], "Ready");
            assert_eq!(status["runSource"], run_source_json('a'));
            assert_eq!(status["resolvedCommit"], commit('a'));
            assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));
            // The same refusal in the next passes: nothing new to write, from
            // either replica.
            assert_eq!(pass(&state, LeaderGate::leading()).await, None);
            assert_eq!(pass(&state, LeaderGate::new()).await, None);

            // Once the API server takes the Deployment, the next pass says so.
            state.lock().unwrap().refused.clear();
            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");
            assert_eq!(status.get("message"), None, "{status}");
            assert_eq!(
                says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
                ("True", REASON_MIN_REPLICAS, "")
            );
            assert_eq!(status["runSource"], run_source_json('b'));
        }

        #[tokio::test]
        async fn a_refused_first_deployment_fails_the_code_location() {
            let mut cl = sized_5gb(git_cl_at('a', json!({})));
            cl.status = None;
            let state = ApiState::default();
            state.lock().unwrap().code_locations.insert("x".into(), cl);
            let answer = unreadable_size(500);
            refuse(&state, "deployments/x", &answer);

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            let failure = format!("applying Deployment 'x' failed: {}", answer.message);
            assert_eq!(status["phase"], "Failed", "{status}");
            assert_eq!(status["message"], failure);
            assert_eq!(
                says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
                ("False", REASON_APPLY_FAILED, failure.as_str())
            );
            for field in [
                "grpcEndpoint",
                "readyReplicas",
                "runSource",
                "resolvedCommit",
            ] {
                assert_eq!(status.get(field), None, "{field}: {status}");
            }
            // A server error may pass: the leader tries again sooner.
            assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
            assert_eq!(pass(&state, LeaderGate::leading()).await, None);
            assert_eq!(pass(&state, LeaderGate::new()).await, None);
        }

        #[tokio::test]
        async fn each_refused_object_of_a_git_code_location_shows_in_status() {
            for (workspace, object, action) in [
                (
                    shared(),
                    "persistentvolumeclaims/x-workspace",
                    "creating PersistentVolumeClaim 'x-workspace'",
                ),
                (
                    shared(),
                    "configmaps/x-workspace-keep",
                    "applying ConfigMap 'x-workspace-keep'",
                ),
                (
                    WorkspaceConfig::default(),
                    "deployments/x",
                    "applying Deployment 'x'",
                ),
                (
                    WorkspaceConfig::default(),
                    "services/x-grpc",
                    "applying Service 'x-grpc'",
                ),
            ] {
                let state = api_with(git_cl_at('b', serving_status('a')), rolled_out());
                let answer = kube_core::Status::failure(
                    &format!("{object} is forbidden: denied by the cluster's policy"),
                    "Forbidden",
                )
                .with_code(403);
                refuse(&state, object, &answer);

                let (requeue, requests) = reconcile_in(
                    &state,
                    LeaderGate::leading(),
                    &resolver(),
                    workspace,
                    run_store(Vec::new()),
                )
                .await;

                let status = patched_status(&requests).expect("status patch");
                assert_eq!(
                    status["message"],
                    format!("{action} failed: {}", answer.message),
                    "{object}"
                );
                assert_eq!(
                    says(&status, CONDITION_DEPLOYMENT_AVAILABLE).1,
                    REASON_APPLY_FAILED,
                    "{object}"
                );
                assert_eq!(requeue, Action::requeue(TERMINAL_RETRY), "{object}");
            }
        }

        #[test]
        fn build_failure_is_the_newest_failed_sync_of_a_pod_that_runs_the_template() {
            let failed_now = json!({ "state": { "terminated": {
                "exitCode": 1,
                "reason": "Error",
                "message": "second",
                "finishedAt": "2026-10-03T00:02:00Z",
            } } });
            let recovered = json!({
                "state": { "terminated": {
                    "exitCode": 0,
                    "reason": "Completed",
                    "finishedAt": "2026-10-03T00:09:00Z",
                } },
                "lastState": { "terminated": {
                    "exitCode": 1,
                    "reason": "Error",
                    "message": "index down",
                    "finishedAt": "2026-10-03T00:08:00Z",
                } },
            });
            let pods = [
                code_location_pod(
                    "x-a",
                    &tree('a'),
                    failed_sync("other tree", "2026-10-03T00:10:00Z"),
                ),
                code_location_pod(
                    "x-b-1",
                    &tree('b'),
                    failed_sync("first", "2026-10-03T00:01:00Z"),
                ),
                code_location_pod("x-b-2", &tree('b'), failed_now),
                code_location_pod("x-b-3", &tree('b'), recovered),
            ];

            let newest = newest_build_failure(&pods, &tree('b')).expect("a failed build");
            assert_eq!(newest.message.as_deref(), Some("second"));
            assert_eq!(newest_build_failure(&pods[..1], &tree('b')), None);
            assert_eq!(newest_build_failure(&pods[3..], &tree('b')), None);
        }

        #[test]
        fn build_failure_message_shapes() {
            let failed =
                |reason: Option<&str>, exit_code, message: Option<&str>| ContainerStateTerminated {
                    exit_code,
                    reason: reason.map(str::to_string),
                    message: message.map(str::to_string),
                    ..Default::default()
                };
            for (failed, expected) in [
                (
                    failed(Some("Error"), 1, Some("error: no lock\n")),
                    "workspace build of bbbbbbb (pinned) failed (Error, exit code 1): \
                     error: no lock",
                ),
                (
                    failed(Some("OOMKilled"), 137, None),
                    "workspace build of bbbbbbb (pinned) failed (OOMKilled, exit code 137)",
                ),
                (
                    failed(None, 2, Some(" \n")),
                    "workspace build of bbbbbbb (pinned) failed (exit code 2)",
                ),
            ] {
                assert_eq!(build_failure_message(&tree('b'), &failed), expected);
            }
        }

        #[tokio::test]
        async fn a_complete_rollout_reads_no_pods() {
            let state = api_with(git_cl_at('b', serving_status('a')), rolled_out());

            let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let pod_reads: Vec<_> = requests
                .iter()
                .filter(|r| r.path.contains("/pods"))
                .collect();
            assert!(pod_reads.is_empty(), "{pod_reads:?}");
        }

        #[tokio::test]
        async fn run_pods_size_their_workspace_like_the_code_location_pods() {
            use crate::run::test_helpers::emptydir_limits;
            use k8s_openapi::api::core::v1::Pod;
            use rivers_k8s::crd::run::Run;

            for workspace_size in [Some("10Gi"), None] {
                let mut cl = git_cl_at('a', serving_status('a'));
                cl.spec.git.as_mut().unwrap().workspace_size =
                    workspace_size.map(|s| Quantity(s.to_string()));
                let state = api_with(cl.clone(), rolled_out());
                pass(&state, LeaderGate::leading()).await;
                let cl_pod = Pod {
                    spec: state.lock().unwrap().deployments["x"]
                        .spec
                        .clone()
                        .and_then(|d| d.template.spec),
                    ..Default::default()
                };

                let run = Run::new(
                    "r",
                    serde_json::from_value(json!({
                        "codeLocationRef": { "name": "x" },
                        "image": runtime('a'),
                        "target": "job",
                        "source": run_source_json('a'),
                    }))
                    .unwrap(),
                );
                let run_pod = crate::run::pod_builder::build_executor_pod(
                    &run,
                    "r-executor",
                    "run-1",
                    false,
                    &cl.spec,
                    &Default::default(),
                    &Default::default(),
                    &WorkspaceConfig::default(),
                );

                let limit = workspace_size.unwrap_or("2Gi").to_string();
                assert_eq!(
                    emptydir_limits(&cl_pod),
                    (Some(limit.clone()), Some(limit)),
                    "workspaceSize {workspace_size:?}"
                );
                assert_eq!(
                    emptydir_limits(&run_pod),
                    emptydir_limits(&cl_pod),
                    "workspaceSize {workspace_size:?}"
                );
            }
        }

        #[tokio::test]
        async fn image_mode_publishes_the_new_digest_mid_rollout_as_before() {
            let image = |c: char| format!("ghcr.io/acme/pipeline@{}", digest(c));
            let mut cl =
                make_cl(json!({ "image": "ghcr.io/acme/pipeline", "digest": digest('b') }));
            cl.status = Some(
                serde_json::from_value(json!({ "phase": "Ready", "resolvedImage": image('a') }))
                    .unwrap(),
            );

            let status = leader_pass(cl, mid_rollout()).await;

            assert_eq!(status["phase"], "Ready");
            assert_eq!(status["resolvedImage"], image('b'));
            assert_eq!(status["source"], image('b'));
            assert_eq!(status.get("runSource"), None, "{status}");
            assert_eq!(
                condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["reason"],
                REASON_MIN_REPLICAS
            );
        }

        const PIPELINE: &str = "ghcr.io/acme/pipeline";

        /// What code location x published while main (commit a) served it
        /// from git.
        fn git_era_status() -> serde_json::Value {
            let mut status = serving_status('a');
            status["runSource"]["git"]["ref"] = json!("refs/heads/main");
            status["resolvedRef"] = json!("refs/heads/main");
            status["lastFetchedAt"] = json!("2026-10-02T00:00:00Z");
            status["source"] = json!(format!("main@{}", &commit('a')[..7]));
            status
        }

        /// What image mode publishes for [`image_cl`] once its pods run the
        /// image.
        fn image_status() -> serde_json::Value {
            let image = format!("{PIPELINE}@{}", digest('i'));
            let since = "2026-10-02T00:00:00Z";
            json!({
                "phase": "Ready",
                "observedGeneration": 1,
                "resolvedImage": image,
                "grpcEndpoint": grpc_endpoint("x", "y", 3001),
                "readyReplicas": 1,
                "source": image,
                "conditions": [
                    {
                        "type": CONDITION_IMAGE_RESOLVED,
                        "status": "True",
                        "lastTransitionTime": since,
                        "reason": REASON_DIGEST_PINNED,
                        "message": image,
                    },
                    {
                        "type": CONDITION_DEPLOYMENT_AVAILABLE,
                        "status": "True",
                        "lastTransitionTime": since,
                        "reason": REASON_MIN_REPLICAS,
                    },
                ],
            })
        }

        /// Code location x in image mode, on digest i of [`PIPELINE`], with
        /// `prior` as its status.
        fn image_cl(prior: serde_json::Value) -> CodeLocation {
            let mut cl = make_cl(json!({ "image": PIPELINE, "digest": digest('i') }));
            cl.status = Some(serde_json::from_value(prior).unwrap());
            cl
        }

        const GIT_FIELDS: [&str; 4] = [
            "resolvedCommit",
            "resolvedRef",
            "lastFetchedAt",
            "runSource",
        ];

        #[tokio::test]
        async fn image_mode_publishes_no_git_fields_after_a_switch_from_git() {
            let state = api_with(image_cl(git_era_status()), rolled_out());

            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            assert_eq!(status["phase"], "Ready", "{status}");
            assert_eq!(
                status["resolvedImage"],
                format!("{PIPELINE}@{}", digest('i'))
            );
            for field in GIT_FIELDS {
                assert_eq!(status.get(field), None, "{field} in {status}");
            }
        }

        /// Code location x in image mode on `acme/runtime:latest` of
        /// `registry`, with `prior` as its status.
        fn image_cl_on(registry: &MockServer, prior: serde_json::Value) -> CodeLocation {
            let mut cl = make_cl(json!({
                "image": format!("{}/acme/runtime", registry.address()),
                "tag": "latest",
            }));
            cl.status = Some(serde_json::from_value(prior).unwrap());
            cl
        }

        /// The requests of `requests` that write.
        fn writes(requests: &[ApiRequest]) -> Vec<(&str, &str)> {
            requests
                .iter()
                .filter(|r| r.method != "GET")
                .map(|r| (r.method.as_str(), r.path.as_str()))
                .collect()
        }

        fn main_image(state: &ApiState) -> Option<String> {
            state.lock().unwrap().deployments["x"]
                .spec
                .clone()
                .and_then(|d| d.template.spec)
                .map(|pod| pod.containers[0].image.clone())?
        }

        #[tokio::test]
        async fn a_follower_leaves_the_switch_from_git_to_the_leader() {
            let registry = registry(vec![manifest('i')]).await;
            // The git status's resolvedImage is the runtime image, on a tag
            // or with spec.digest pinned.
            for cl in [
                image_cl_on(&registry, git_era_status()),
                image_cl(git_era_status()),
            ] {
                let spec = serde_json::to_string(&cl.spec).unwrap();
                let state = api_with(cl, rolled_out());

                let (requeue, requests) =
                    reconcile_with(&state, LeaderGate::new(), &resolver()).await;

                assert_eq!(writes(&requests), [], "{spec}");
                assert_eq!(main_image(&state), None, "{spec}");
                assert_eq!(requeue, Action::requeue(FOLLOWER_WAIT), "{spec}");
            }

            let state = api_with(image_cl_on(&registry, git_era_status()), rolled_out());
            let image = format!("{}/acme/runtime@{}", registry.address(), digest('i'));
            let leader = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");
            assert_eq!(leader["resolvedImage"], image);

            // Then the follower refreshes as usual: it applies the leader's
            // image and has nothing new to publish.
            let (_, requests) = reconcile_with(&state, LeaderGate::new(), &resolver()).await;

            assert!(
                writes(&requests).contains(&("PATCH", "/apis/apps/v1/namespaces/y/deployments/x")),
                "{requests:?}"
            );
            assert_eq!(main_image(&state), Some(image));
            assert_eq!(patched_status(&requests), None);
        }

        #[tokio::test]
        async fn a_switch_to_image_mode_that_cannot_resolve_its_image_keeps_the_git_status() {
            let registry = registry(vec![ResponseTemplate::new(404)]).await;
            let state = api_with(image_cl_on(&registry, git_era_status()), rolled_out());

            let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let failed = patched_status(&requests).expect("status patch");
            assert_eq!(failed["phase"], "Failed", "{failed}");
            assert_eq!(
                says(&failed, CONDITION_IMAGE_RESOLVED).1,
                REASON_TAG_NOT_FOUND
            );
            // The pods still run the git source: its status stays until an
            // image pass replaces it.
            let git = git_era_status();
            for field in GIT_FIELDS.into_iter().chain(["resolvedImage"]) {
                assert_eq!(failed[field], git[field], "{field}");
            }
            assert_eq!(
                writes(&requests),
                [(
                    "PATCH",
                    "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
                )]
            );
            assert_eq!(pass(&state, LeaderGate::new()).await, None);
        }

        /// [`image_status`] after a pass that kept x's git workspace.
        fn image_status_keeping_the_workspace() -> serde_json::Value {
            let mut status = image_status();
            status["conditions"].as_array_mut().unwrap().push(json!({
                "type": CONDITION_WORKSPACE_KEPT,
                "status": "True",
                "lastTransitionTime": "2026-10-02T00:00:00Z",
                "reason": REASON_ROLLING_OUT,
                "message": "PVC 'x-workspace' of the git source stays until every code-location \
                            pod runs the image",
            }));
            status
        }

        /// The status `state` holds for x.
        fn stored_status(state: &ApiState) -> serde_json::Value {
            serde_json::to_value(&state.lock().unwrap().code_locations["x"]).unwrap()["status"]
                .clone()
        }

        #[tokio::test]
        async fn a_registry_error_keeps_the_kept_workspace_in_status() {
            let registry = registry(vec![ResponseTemplate::new(503), manifest('i')]).await;
            let cl = image_cl_on(&registry, image_status_keeping_the_workspace());
            let state = api_with(cl.clone(), rolled_out());
            add_git_workspace(&state, &cl);

            let failed = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            assert_eq!(failed["phase"], "Failed", "{failed}");
            assert_eq!(
                workspace_kept(&failed),
                workspace_kept(&image_status_keeping_the_workspace())
            );
            assert_eq!(git_workspace_in(&state), (true, true));

            // The registry answers again: the cleanup goes on.
            let (_, requests) = reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(Vec::new()),
            )
            .await;

            assert_eq!(
                deleted(&requests),
                [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
            );
            assert_eq!(workspace_kept(&stored_status(&state)), None);
        }

        #[tokio::test]
        async fn the_leader_looks_for_a_git_workspace_only_after_a_git_source() {
            let looks = vec![("GET", KEEP_PATH), ("GET", PVC_PATH)];
            let cases = [
                ("never reconciled", None, vec![]),
                ("an image status", Some(image_status()), vec![]),
                ("a git status", Some(git_era_status()), looks.clone()),
                // Also after an operator restart: the condition stays until
                // the workspace is gone.
                (
                    "a kept workspace",
                    Some(image_status_keeping_the_workspace()),
                    looks,
                ),
            ];
            for (case, prior, expected) in cases {
                let mut cl = make_cl(json!({ "image": PIPELINE, "digest": digest('i') }));
                cl.status = prior.map(|prior| serde_json::from_value(prior).unwrap());
                let state = api_with(cl, rolled_out());

                let (_, requests) =
                    reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

                let mut seen: Vec<_> = requests
                    .iter()
                    .filter(|r| {
                        r.path.contains("/persistentvolumeclaims") || r.path.contains("/configmaps")
                    })
                    .map(|r| (r.method.as_str(), r.path.as_str()))
                    .collect();
                seen.sort();
                assert_eq!(seen, expected, "{case}");
                // Nothing of the git workspace is left: nothing to keep.
                assert_eq!(workspace_kept(&stored_status(&state)), None, "{case}");
            }
        }

        const PVC_UID: &str = "pvc-uid";
        const KEEP_UID: &str = "keep-uid";

        /// The workspace PVC and keep ConfigMap that the git path made for
        /// `owner`.
        fn git_workspace_of(owner: &CodeLocation) -> (PersistentVolumeClaim, ConfigMap) {
            let mut pvc = build_workspace_pvc(owner, &Quantity("20Gi".into()), None);
            pvc.metadata.uid = Some(PVC_UID.into());
            let mut keep = build_keep_config_map(owner, &tree('a').workspace_key());
            keep.metadata.uid = Some(KEEP_UID.into());
            (pvc, keep)
        }

        fn add_git_workspace(state: &ApiState, owner: &CodeLocation) {
            let (pvc, keep) = git_workspace_of(owner);
            let mut s = state.lock().unwrap();
            s.pvcs.insert(workspace_pvc_name("x"), pvc);
            s.config_maps.insert(keep_config_map_name("x"), keep);
        }

        /// The objects `requests` deleted, with the uid each DELETE required.
        fn deleted(requests: &[ApiRequest]) -> Vec<(&str, Option<&str>)> {
            let mut deleted: Vec<_> = requests
                .iter()
                .filter(|r| r.method == "DELETE")
                .map(|r| {
                    let uid = r
                        .body
                        .as_ref()
                        .and_then(|b| b["preconditions"]["uid"].as_str());
                    (r.path.as_str(), uid)
                })
                .collect();
            deleted.sort();
            deleted
        }

        const PVC_PATH: &str = "/api/v1/namespaces/y/persistentvolumeclaims/x-workspace";
        const KEEP_PATH: &str = "/api/v1/namespaces/y/configmaps/x-workspace-keep";

        /// Whether `state` still holds x's workspace PVC and keep ConfigMap.
        fn git_workspace_in(state: &ApiState) -> (bool, bool) {
            let s = state.lock().unwrap();
            (
                s.pvcs.contains_key(&workspace_pvc_name("x")),
                s.config_maps.contains_key(&keep_config_map_name("x")),
            )
        }

        /// The status, reason and message of `status`'s `WorkspaceKept`
        /// condition, if it has one.
        fn workspace_kept(status: &serde_json::Value) -> Option<(&str, &str, &str)> {
            status["conditions"]
                .as_array()?
                .iter()
                .any(|c| c["type"] == CONDITION_WORKSPACE_KEPT)
                .then(|| says(status, CONDITION_WORKSPACE_KEPT))
        }

        /// A run of x that runs `image` without a git source.
        fn image_run(name: &str, phase: RunPhase) -> Run {
            let mut run = Run::new(
                name,
                serde_json::from_value(json!({
                    "codeLocationRef": { "name": "x" },
                    "image": format!("{PIPELINE}@{}", digest('i')),
                    "target": "*",
                }))
                .unwrap(),
            );
            run.metadata.namespace = Some("y".into());
            run.status = Some(rivers_k8s::crd::run::RunCrdStatus {
                phase: Some(phase),
                ..Default::default()
            });
            run
        }

        #[tokio::test]
        async fn leaving_git_deletes_the_workspace_once_no_pod_can_mount_it() {
            let cl = image_cl(git_era_status());
            let state = api_with(cl.clone(), rolled_out());
            add_git_workspace(&state, &cl);
            let mut other_namespace = run_for("x", 'f', Some(RunPhase::Running));
            other_namespace.metadata.namespace = Some("z".into());
            // None of these runs mounts x's workspace.
            let runs = || {
                run_store(vec![
                    image_run("image-run", RunPhase::Running),
                    run_for("other", 'e', Some(RunPhase::Running)),
                    other_namespace.clone(),
                ])
            };

            // The pass that leaves git: until this status is out, the
            // webhook still gives new runs the git source.
            let (_, leaving) =
                reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs()).await;
            assert_eq!(deleted(&leaving), []);
            let status = patched_status(&leaving).expect("status patch");
            assert_eq!(
                workspace_kept(&status),
                Some((
                    "True",
                    REASON_ROLLING_OUT,
                    "PVC 'x-workspace' of the git source stays until every code-location \
                     pod runs the image"
                ))
            );

            let (requeue, requests) =
                reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs()).await;

            assert_eq!(
                deleted(&requests),
                [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
            );
            assert_eq!(git_workspace_in(&state), (false, false));
            let status = patched_status(&requests).expect("status patch");
            assert_eq!(workspace_kept(&status), None, "{status}");
            assert_eq!(status["phase"], "Ready");
            // Ready on a pinned digest, with nothing left to delete.
            assert_eq!(requeue, Action::await_change());
        }

        #[tokio::test]
        async fn the_workspace_stays_while_a_pod_may_mount_it_and_the_status_says_why() {
            let git_run = |c: char, phase: Option<RunPhase>| run_for("x", c, phase);
            let runs_use_it = |names: &str| {
                format!(
                    "PVC 'x-workspace' of the git source stays while the pods of these runs \
                     mount it: {names}"
                )
            };
            let (listing, _writer) = listing_run_store(Vec::new());
            let cases = [
                (
                    "a pod of the git source runs",
                    mid_rollout(),
                    run_store(Vec::new()),
                    REASON_ROLLING_OUT,
                    "PVC 'x-workspace' of the git source stays until every code-location pod \
                     runs the image"
                        .to_string(),
                ),
                (
                    "the run store has not synced",
                    rolled_out(),
                    listing,
                    REASON_RUNS_NOT_LISTED,
                    "PVC 'x-workspace' of the git source stays until the operator has listed \
                     the runs"
                        .to_string(),
                ),
                (
                    "a queued run",
                    rolled_out(),
                    run_store(vec![git_run('b', None)]),
                    REASON_RUNS_USE_WORKSPACE,
                    runs_use_it("run-b"),
                ),
                (
                    "a pending and a running run",
                    rolled_out(),
                    run_store(vec![
                        git_run('c', Some(RunPhase::Running)),
                        git_run('b', Some(RunPhase::Pending)),
                        image_run("image-run", RunPhase::Running),
                    ]),
                    REASON_RUNS_USE_WORKSPACE,
                    runs_use_it("run-b, run-c"),
                ),
                (
                    "a finished run, whose pod stays until the run is deleted",
                    rolled_out(),
                    run_store(vec![git_run('d', Some(RunPhase::Succeeded))]),
                    REASON_RUNS_USE_WORKSPACE,
                    runs_use_it("run-d"),
                ),
                (
                    "many runs",
                    rolled_out(),
                    run_store(
                        "abcdefg"
                            .chars()
                            .map(|c| git_run(c, Some(RunPhase::Failed)))
                            .collect(),
                    ),
                    REASON_RUNS_USE_WORKSPACE,
                    runs_use_it("run-a, run-b, run-c, run-d, run-e and 2 more"),
                ),
            ];

            for (case, observed, runs, reason, message) in cases {
                let cl = image_cl(image_status_keeping_the_workspace());
                let state = api_with(cl.clone(), observed);
                add_git_workspace(&state, &cl);

                let (requeue, requests) =
                    reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

                assert_eq!(deleted(&requests), [], "{case}");
                assert_eq!(git_workspace_in(&state), (true, true), "{case}");
                assert_eq!(
                    workspace_kept(&stored_status(&state)),
                    Some(("True", reason, message.as_str())),
                    "{case}"
                );
                assert_eq!(requeue, Action::requeue(WORKSPACE_RECHECK), "{case}");
                // The follower shows what the leader found.
                assert_eq!(pass(&state, LeaderGate::new()).await, None, "{case}");
            }
        }

        #[tokio::test]
        async fn the_workspace_goes_once_the_last_run_of_the_git_source_is_deleted() {
            let cl = image_cl(image_status_keeping_the_workspace());
            let state = api_with(cl.clone(), rolled_out());
            add_git_workspace(&state, &cl);
            let finished = run_for("x", 'd', Some(RunPhase::Succeeded));
            reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(vec![finished]),
            )
            .await;
            assert_eq!(git_workspace_in(&state), (true, true));

            let (requeue, requests) = reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(Vec::new()),
            )
            .await;

            assert_eq!(
                deleted(&requests),
                [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
            );
            assert_eq!(git_workspace_in(&state), (false, false));
            let status = patched_status(&requests).expect("status patch");
            assert_eq!(workspace_kept(&status), None, "{status}");
            assert_eq!(requeue, Action::await_change());
        }

        #[tokio::test]
        async fn only_the_code_locations_own_workspace_is_deleted() {
            // (owner uid of the PVC and ConfigMap named like x's, deleted)
            let cases = [
                (Some("u"), true),
                (Some("uid-of-an-earlier-x"), false),
                (None, false),
            ];
            for (owner_uid, deletes) in cases {
                let cl = image_cl(image_status_keeping_the_workspace());
                assert_eq!(cl.metadata.uid.as_deref(), Some("u"));
                let state = api_with(cl.clone(), rolled_out());
                let mut owner = cl.clone();
                owner.metadata.uid = owner_uid.map(str::to_string);
                let (mut pvc, mut keep) = git_workspace_of(&owner);
                if owner_uid.is_none() {
                    pvc.metadata.owner_references = None;
                    keep.metadata.owner_references = None;
                }
                {
                    let mut s = state.lock().unwrap();
                    s.pvcs.insert(workspace_pvc_name("x"), pvc.clone());
                    s.config_maps
                        .insert(keep_config_map_name("x"), keep.clone());
                }

                let (requeue, requests) = reconcile_in(
                    &state,
                    LeaderGate::leading(),
                    &resolver(),
                    shared(),
                    run_store(Vec::new()),
                )
                .await;

                let case = format!("owner {owner_uid:?}");
                // Nothing of the code location's own is left to keep.
                assert_eq!(workspace_kept(&stored_status(&state)), None, "{case}");
                let s = state.lock().unwrap();
                if deletes {
                    assert_eq!(
                        deleted(&requests),
                        [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))],
                        "{case}"
                    );
                    assert!(s.pvcs.is_empty(), "{case}");
                    assert!(s.config_maps.is_empty(), "{case}");
                } else {
                    assert_eq!(deleted(&requests), [], "{case}");
                    assert_eq!(s.pvcs.get(&workspace_pvc_name("x")), Some(&pvc), "{case}");
                    assert_eq!(
                        s.config_maps.get(&keep_config_map_name("x")),
                        Some(&keep),
                        "{case}"
                    );
                }
                assert_eq!(requeue, Action::await_change(), "{case}");
            }
        }

        #[tokio::test]
        async fn a_git_rollout_waits_until_the_old_workspace_pvc_is_gone() {
            // Back to git while the PVC of the earlier git source is being
            // deleted: a pod cannot mount it.
            let cl = git_cl_at('a', image_status());
            let state = api_with(cl.clone(), rolled_out());
            let (mut pvc, _) = git_workspace_of(&cl);
            pvc.metadata.deletion_timestamp =
                Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                    "2026-10-03T00:00:00Z".parse().unwrap(),
                ));
            pvc.metadata.finalizers = Some(vec!["kubernetes.io/pvc-protection".into()]);
            state
                .lock()
                .unwrap()
                .pvcs
                .insert(workspace_pvc_name("x"), pvc);

            let (requeue, requests) = reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(Vec::new()),
            )
            .await;

            let writes: Vec<_> = requests
                .iter()
                .filter(|r| r.method != "GET")
                .map(|r| (r.method.as_str(), r.path.as_str()))
                .collect();
            assert_eq!(writes, []);
            assert_eq!(state.lock().unwrap().deployments["x"].spec, None);
            assert_eq!(requeue, Action::requeue(DEPLOYMENT_ROLLOUT_POLL));

            // Kubernetes deletes it once no pod mounts it.
            state.lock().unwrap().pvcs.clear();
            let (_, requests) = reconcile_in(
                &state,
                LeaderGate::leading(),
                &resolver(),
                shared(),
                run_store(Vec::new()),
            )
            .await;

            let s = state.lock().unwrap();
            let created = requests
                .iter()
                .find(|r| r.method == "POST" && r.path.ends_with("/persistentvolumeclaims"))
                .and_then(|r| r.body.clone())
                .expect("a new workspace PVC");
            assert_eq!(created["metadata"]["name"], "x-workspace");
            assert_eq!(created["metadata"].get("deletionTimestamp"), None);
            let claims: Vec<_> = s.deployments["x"]
                .spec
                .clone()
                .and_then(|d| d.template.spec)
                .unwrap()
                .volumes
                .into_iter()
                .flatten()
                .filter_map(|v| v.persistent_volume_claim.map(|c| c.claim_name))
                .collect();
            assert_eq!(claims, ["x-workspace"]);
        }

        const REPO: &str = "/acme/pipelines.git";

        /// A server answering `verb` requests for `at` with `responses` in
        /// turn, the last one from then on.
        async fn answering(verb: &str, at: &str, responses: Vec<ResponseTemplate>) -> MockServer {
            let server = MockServer::start().await;
            let last = responses.len() - 1;
            for (i, response) in responses.into_iter().enumerate() {
                let mock = Mock::given(method(verb))
                    .and(path(at))
                    .respond_with(response);
                let mock = if i < last {
                    mock.up_to_n_times(1)
                } else {
                    mock
                };
                mock.mount(&server).await;
            }
            server
        }

        /// A git host answering ref polls with `responses` in turn, the last
        /// one from then on.
        async fn forge(responses: Vec<ResponseTemplate>) -> MockServer {
            answering("GET", &format!("{REPO}/info/refs"), responses).await
        }

        /// The ref advertisement: `refs/heads/main` is commit `a`.
        fn advertisement() -> ResponseTemplate {
            ResponseTemplate::new(200)
                .insert_header(
                    "content-type",
                    "application/x-git-upload-pack-advertisement",
                )
                .set_body_bytes(git::fixtures::http_adv())
        }

        fn unreachable_error(forge: &MockServer, status: &str) -> String {
            format!(
                "git host unreachable: HTTP {status} from {}{REPO}/info/refs?service=git-upload-pack",
                forge.uri()
            )
        }

        /// Git CL tracking `main` on `forge`, on runtime digest `a`.
        fn branch_cl(forge: &MockServer) -> CodeLocation {
            make_cl(json!({
                "git": {
                    "url": format!("{}{REPO}", forge.uri()),
                    "ref": { "branch": "main" },
                },
                "digest": digest('a'),
            }))
        }

        /// The leader's first pass after the git host started answering
        /// with `responses`: before that, `main` (commit a) resolved and
        /// rolled out, and its cached commit has expired since.
        struct Outage {
            forge: MockServer,
            state: ApiState,
            git: Arc<git::GitResolver>,
            /// The status before the outage.
            serving: serde_json::Value,
            requeue: Action,
            requests: Vec<ApiRequest>,
        }

        async fn outage(responses: Vec<ResponseTemplate>) -> Outage {
            let mut polls = vec![advertisement()];
            polls.extend(responses);
            let forge = forge(polls).await;
            let state = api_with(branch_cl(&forge), rolled_out());
            let serving = pass(&state, LeaderGate::leading())
                .await
                .expect("rollout of main");
            assert_eq!(serving["phase"], "Ready", "{serving}");
            assert_eq!(serving["resolvedCommit"], commit('a'), "{serving}");
            assert_eq!(serving["resolvedRef"], "refs/heads/main", "{serving}");

            let git = resolver();
            let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &git).await;
            Outage {
                forge,
                state,
                git,
                serving,
                requeue,
                requests,
            }
        }

        /// The status, reason and message of condition `kind`.
        fn says<'a>(status: &'a serde_json::Value, kind: &str) -> (&'a str, &'a str, &'a str) {
            let c = condition(status, kind);
            let field = |name: &str| c[name].as_str().unwrap_or_default();
            (field("status"), field("reason"), field("message"))
        }

        fn source_resolved(status: &serde_json::Value) -> (&str, &str, &str) {
            says(status, CONDITION_SOURCE_RESOLVED)
        }

        #[tokio::test]
        async fn unreachable_git_host_keeps_the_serving_tree_ready() {
            let outage = outage(vec![ResponseTemplate::new(503)]).await;
            let status = patched_status(&outage.requests).expect("status patch");

            assert_eq!(status["phase"], "Ready", "{status}");
            assert_eq!(status["grpcEndpoint"], outage.serving["grpcEndpoint"]);
            assert_eq!(status["readyReplicas"], 1);
            for field in [
                "runSource",
                "resolvedCommit",
                "resolvedRef",
                "resolvedImage",
                "source",
            ] {
                assert_eq!(status[field], outage.serving[field], "{field}");
            }
            let error = unreachable_error(&outage.forge, "503 Service Unavailable");
            assert_eq!(
                source_resolved(&status),
                ("False", REASON_GIT_UNREACHABLE, error.as_str())
            );
            assert_eq!(status["message"], error);
            assert_eq!(
                condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["reason"],
                REASON_MIN_REPLICAS
            );
            // The Deployment keeps the tree it runs.
            let applies: Vec<_> = outage
                .requests
                .iter()
                .filter(|r| r.method == "PATCH" && r.path.contains("/deployments/"))
                .collect();
            assert!(applies.is_empty(), "{applies:?}");
            assert_eq!(outage.requeue, Action::requeue(Duration::from_secs(60)));
        }

        #[tokio::test]
        async fn a_git_resolution_error_keeps_status_message_over_a_failed_build() {
            let forge = forge(vec![advertisement(), ResponseTemplate::new(503)]).await;
            let state = api_with(branch_cl(&forge), rolled_out());
            pass(&state, LeaderGate::leading())
                .await
                .expect("rollout of main");
            // The pod restarts and cannot fetch main either: the git host is
            // down for the pods too.
            let fetch_error = format!(
                "fatal: unable to access '{}{REPO}/': The requested URL returned error: 503",
                forge.uri()
            );
            {
                let mut s = state.lock().unwrap();
                let deployment = s.deployments.get_mut("x").unwrap();
                let rollout = deployment.status.as_mut().unwrap();
                rollout.ready_replicas = Some(0);
                rollout.available_replicas = Some(0);
                let main = template_source(deployment).unwrap();
                let pod = code_location_pod(
                    "x-1",
                    &main,
                    failed_sync(&fetch_error, "2026-10-03T00:00:00Z"),
                );
                s.pods.insert("x-1".into(), pod);
            }

            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            let error = unreachable_error(&forge, "503 Service Unavailable");
            assert_eq!(
                source_resolved(&status),
                ("False", REASON_GIT_UNREACHABLE, error.as_str())
            );
            assert_eq!(status["message"], error);
            assert_eq!(
                condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["message"],
                format!(
                    "workspace build of main@aaaaaaa failed (Error, exit code 1): {fetch_error}"
                )
            );
        }

        #[tokio::test]
        async fn rate_limited_git_host_keeps_the_serving_tree_ready_and_says_when_it_asks_again() {
            let before = jiff::Timestamp::now();
            let outage = outage(vec![
                ResponseTemplate::new(429).insert_header("retry-after", "900"),
            ])
            .await;
            let after = jiff::Timestamp::now();
            let status = patched_status(&outage.requests).expect("status patch");

            assert_eq!(status["phase"], "Ready", "{status}");
            for field in [
                "grpcEndpoint",
                "runSource",
                "resolvedCommit",
                "resolvedRef",
                "resolvedImage",
                "source",
            ] {
                assert_eq!(status[field], outage.serving[field], "{field}");
            }
            let (condition_status, reason, message) = source_resolved(&status);
            assert_eq!(
                (condition_status, reason),
                ("False", REASON_GIT_RATE_LIMITED),
                "{status}"
            );
            assert_eq!(status["message"], message);
            let (error, asks_again_at) = message
                .split_once("; the operator asks again at ")
                .unwrap_or_else(|| panic!("no retry time: {message}"));
            assert_eq!(
                error,
                format!(
                    "rate-limited by the git host: HTTP 429 Too Many Requests from {}",
                    outage.forge.address()
                )
            );
            let asks_again_at: jiff::Timestamp = asks_again_at.parse().unwrap();
            let wait = jiff::SignedDuration::from_secs(900);
            let second = jiff::SignedDuration::from_secs(1);
            assert!(
                before + wait - second <= asks_again_at && asks_again_at <= after + wait + second,
                "{asks_again_at} is not 900s after {before}..{after}"
            );
            assert_eq!(outage.requeue, Action::requeue(Duration::from_secs(900)));
        }

        #[tokio::test]
        async fn status_stays_unresolved_while_the_ref_backs_off() {
            // The host answers again right after the failed poll; the leader
            // must not ask it before the backoff ends.
            let outage = outage(vec![ResponseTemplate::new(503), advertisement()]).await;
            let stale = patched_status(&outage.requests).expect("status patch");

            let (_, requests) =
                reconcile_with(&outage.state, LeaderGate::leading(), &outage.git).await;

            assert_eq!(patched_status(&requests), None);
            assert_eq!(outage.forge.received_requests().await.unwrap().len(), 2);
            let stored = serde_json::to_value(&outage.state.lock().unwrap().code_locations["x"])
                .unwrap()["status"]
                .clone();
            assert_eq!(source_resolved(&stored), source_resolved(&stale));
            assert_eq!(source_resolved(&stored).0, "False");
        }

        #[tokio::test]
        async fn follower_publishes_nothing_new_while_the_git_host_is_unreachable() {
            let outage = outage(vec![ResponseTemplate::new(503)]).await;
            assert!(patched_status(&outage.requests).is_some());

            assert_eq!(pass(&outage.state, LeaderGate::new()).await, None);
        }

        #[tokio::test]
        async fn follower_publishes_nothing_new_after_a_terminal_git_error() {
            for (code, reason) in [(401, REASON_GIT_AUTH_FAILED), (404, REASON_REF_NOT_FOUND)] {
                let outage = outage(vec![ResponseTemplate::new(code)]).await;
                let failed = patched_status(&outage.requests).expect("status patch");
                assert_eq!(failed["phase"], "Failed", "{failed}");
                assert_eq!(failed.get("grpcEndpoint"), None, "{failed}");
                assert_eq!(source_resolved(&failed).1, reason, "{failed}");

                assert_eq!(
                    pass(&outage.state, LeaderGate::new()).await,
                    None,
                    "HTTP {code}"
                );
            }
        }

        #[tokio::test]
        async fn unreachable_git_host_fails_a_code_location_with_nothing_to_serve() {
            let forge = forge(vec![ResponseTemplate::new(503)]).await;
            let state = ApiState::default();
            state
                .lock()
                .unwrap()
                .code_locations
                .insert("x".into(), branch_cl(&forge));

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            let error = unreachable_error(&forge, "503 Service Unavailable");
            assert_eq!(status["phase"], "Failed", "{status}");
            assert_eq!(status["message"], error);
            for field in [
                "grpcEndpoint",
                "readyReplicas",
                "runSource",
                "resolvedCommit",
            ] {
                assert_eq!(status.get(field), None, "{field}: {status}");
            }
            assert_eq!(
                source_resolved(&status),
                ("False", REASON_GIT_UNREACHABLE, error.as_str())
            );
            assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
            // The follower leaves the leader's verdict alone.
            assert_eq!(pass(&state, LeaderGate::new()).await, None);
        }

        #[tokio::test]
        async fn a_pinned_uppercase_commit_fails_before_any_deployment() {
            // Created before the webhook and the CRD schema refused one.
            let upper = commit('a').to_ascii_uppercase();
            let state = ApiState::default();
            state.lock().unwrap().code_locations.insert(
                "x".into(),
                make_cl(json!({
                    "git": { "url": URL, "ref": { "commit": upper } },
                    "digest": digest('a'),
                })),
            );

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            let error = format!(
                "invalid git ref: git.ref.commit '{upper}' has uppercase letters — use the \
                 lowercase SHA '{}'",
                commit('a')
            );
            assert_eq!(status["phase"], "Failed", "{status}");
            assert_eq!(status["message"], error);
            assert_eq!(
                source_resolved(&status),
                ("False", REASON_INVALID_REF, error.as_str())
            );
            let writes: Vec<_> = requests
                .iter()
                .filter(|r| r.method != "GET")
                .map(|r| (r.method.as_str(), r.path.as_str()))
                .collect();
            assert_eq!(
                writes,
                [(
                    "PATCH",
                    "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
                )]
            );
            assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));
        }

        #[tokio::test]
        async fn an_http_code_location_fails_without_a_request_unless_the_operator_allows_it() {
            // Created before the webhook refused http://, or with no webhook.
            let forge = forge(vec![advertisement()]).await;
            let state = api_with(branch_cl(&forge), rolled_out());
            let refusing = Arc::new(git::GitResolver::new(Duration::from_secs(5), false));

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &refusing).await;

            let status = patched_status(&requests).expect("status patch");
            let error = "invalid git url: http:// sends the code and the git Secret's \
                         credentials unencrypted — use https://, or set \
                         operator.git.allowInsecure to true for a git host on a trusted network";
            assert_eq!(status["phase"], "Failed", "{status}");
            assert_eq!(status["message"], error);
            assert_eq!(
                source_resolved(&status),
                ("False", REASON_INVALID_URL, error)
            );
            assert_eq!(forge.received_requests().await.unwrap().len(), 0);
            let writes: Vec<_> = requests
                .iter()
                .filter(|r| r.method != "GET")
                .map(|r| (r.method.as_str(), r.path.as_str()))
                .collect();
            assert_eq!(
                writes,
                [(
                    "PATCH",
                    "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
                )]
            );
            assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));

            // With operator.git.allowInsecure, the same code location rolls out.
            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");
            assert_eq!(status["resolvedCommit"], commit('a'), "{status}");
            assert_eq!(forge.received_requests().await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn https_code_location_fetches_with_the_password_of_a_secret_shared_with_ssh() {
            use super::git_secret::{IDENTITY, KNOWN_HOSTS, PASSWORD, USERNAME, secret};
            use base64::Engine as _;
            use wiremock::matchers::header;

            let forge = MockServer::start().await;
            let basic = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", USERNAME.1, PASSWORD.1));
            Mock::given(method("GET"))
                .and(path(format!("{REPO}/info/refs")))
                .and(header("authorization", format!("Basic {basic}")))
                .respond_with(advertisement())
                .expect(1)
                .mount(&forge)
                .await;
            let cl = make_cl(json!({
                "git": {
                    "url": format!("{}{REPO}", forge.uri()),
                    "ref": { "branch": "main" },
                    "secretRef": { "name": "git-creds" },
                },
                "digest": digest('a'),
            }));
            let state = api_with(cl, rolled_out());
            state.lock().unwrap().secrets.insert(
                "git-creds".into(),
                Ok(secret(&[IDENTITY, KNOWN_HOSTS, USERNAME, PASSWORD])),
            );

            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            assert_eq!(status["phase"], "Ready", "{status}");
            assert_eq!(status["resolvedCommit"], commit('a'));
            assert_eq!(status["resolvedRef"], "refs/heads/main");
            assert_eq!(
                source_resolved(&status),
                ("True", REASON_COMMIT_RESOLVED, commit('a').as_str())
            );
            // The tree of the applied Deployment's pod template.
            assert_eq!(status["runSource"]["git"]["commit"], commit('a'));
            assert_eq!(status["runSource"]["git"]["secretName"], "git-creds");
        }

        #[tokio::test]
        async fn https_code_location_fetches_with_a_password_file_that_ends_in_a_newline() {
            use super::git_secret::secret;
            use base64::Engine as _;
            use wiremock::matchers::header;

            let forge = MockServer::start().await;
            let basic = base64::engine::general_purpose::STANDARD.encode("bot:ghp_TOKEN");
            Mock::given(method("GET"))
                .and(path(format!("{REPO}/info/refs")))
                .and(header("authorization", format!("Basic {basic}")))
                .respond_with(advertisement())
                .expect(1)
                .mount(&forge)
                .await;
            Mock::given(method("GET"))
                .and(path(format!("{REPO}/info/refs")))
                .respond_with(ResponseTemplate::new(401))
                .mount(&forge)
                .await;
            let cl = make_cl(json!({
                "git": {
                    "url": format!("{}{REPO}", forge.uri()),
                    "ref": { "branch": "main" },
                    "secretRef": { "name": "git-creds" },
                },
                "digest": digest('a'),
            }));
            let state = api_with(cl, rolled_out());
            // echo ghp_TOKEN > token.txt; kubectl create secret generic git-creds \
            //   --from-literal=username=bot --from-file=password=token.txt
            state.lock().unwrap().secrets.insert(
                "git-creds".into(),
                Ok(secret(&[("username", "bot"), ("password", "ghp_TOKEN\n")])),
            );

            let status = pass(&state, LeaderGate::leading())
                .await
                .expect("status patch");

            assert_eq!(status["phase"], "Ready", "{status}");
            assert_eq!(status["resolvedCommit"], commit('a'));
            assert_eq!(
                source_resolved(&status),
                ("True", REASON_COMMIT_RESOLVED, commit('a').as_str())
            );
            let deployment = state.lock().unwrap().deployments["x"].clone();
            assert_eq!(
                template_source(&deployment).map(|source| source.git.commit),
                Some(commit('a'))
            );
        }

        /// A registry answering HEADs of `acme/runtime:latest` with
        /// `responses` in turn, the last one from then on.
        async fn registry(responses: Vec<ResponseTemplate>) -> MockServer {
            answering("HEAD", "/v2/acme/runtime/manifests/latest", responses).await
        }

        /// The registry's answer: `latest` is digest `c`.
        fn manifest(c: char) -> ResponseTemplate {
            ResponseTemplate::new(200).insert_header("docker-content-digest", digest(c))
        }

        /// Git CL pinned to commit a on `acme/runtime:latest` of `registry`,
        /// refreshed every 5m.
        fn latest_runtime_cl(registry: &MockServer) -> CodeLocation {
            make_cl(json!({
                "git": { "url": URL, "ref": { "commit": commit('a') } },
                "image": format!("{}/acme/runtime", registry.address()),
                "tag": "latest",
                "digestRefreshInterval": "5m",
            }))
        }

        #[tokio::test]
        async fn pinned_commit_on_a_mutable_runtime_tag_requeues_after_digest_refresh_interval() {
            let registry = registry(vec![manifest('b')]).await;
            let state = api_with(latest_runtime_cl(&registry), rolled_out());

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            assert_eq!(status["phase"], "Ready", "{status}");
            assert_eq!(status["resolvedCommit"], commit('a'));
            assert_eq!(
                status["resolvedImage"],
                format!("{}/acme/runtime@{}", registry.address(), digest('b'))
            );
            assert_eq!(registry.received_requests().await.unwrap().len(), 1);
            let refresh = Duration::from_secs(300);
            let after = requeue_after(&requeue);
            assert!(
                after.is_some_and(|after| jittered(refresh).contains(&after)),
                "requeue after {after:?}, want {refresh:?} plus up to a quarter"
            );
        }

        /// The leader's first pass after the registry started answering
        /// with `responses`: before that, digest b of `latest` rolled out
        /// with commit a.
        struct RegistryOutage {
            state: ApiState,
            /// The status before the outage.
            serving: serde_json::Value,
            requeue: Action,
            requests: Vec<ApiRequest>,
        }

        async fn registry_outage(responses: Vec<ResponseTemplate>) -> RegistryOutage {
            let mut answers = vec![manifest('b')];
            answers.extend(responses);
            let registry = registry(answers).await;
            let state = api_with(latest_runtime_cl(&registry), rolled_out());
            let serving = pass(&state, LeaderGate::leading())
                .await
                .expect("rollout of digest b");
            assert_eq!(serving["phase"], "Ready", "{serving}");
            assert_eq!(says(&serving, CONDITION_IMAGE_RESOLVED).0, "True");

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;
            RegistryOutage {
                state,
                serving,
                requeue,
                requests,
            }
        }

        #[tokio::test]
        async fn transient_registry_error_keeps_the_serving_tree_ready() {
            let cases = [
                (
                    ResponseTemplate::new(503),
                    REASON_REGISTRY_ERROR,
                    "status 503 Service Unavailable",
                    Duration::from_secs(60),
                ),
                (
                    ResponseTemplate::new(429).insert_header("retry-after", "900"),
                    REASON_RATE_LIMITED,
                    "registry rate-limited; retrying in 900s",
                    Duration::from_secs(900),
                ),
            ];
            for (answer, reason, error, retry) in cases {
                let outage = registry_outage(vec![answer]).await;
                let status = patched_status(&outage.requests).expect("status patch");

                assert_eq!(status["phase"], "Ready", "{status}");
                for field in [
                    "grpcEndpoint",
                    "readyReplicas",
                    "runSource",
                    "resolvedCommit",
                    "resolvedImage",
                    "source",
                ] {
                    assert_eq!(status[field], outage.serving[field], "{reason}: {field}");
                }
                assert_eq!(
                    says(&status, CONDITION_IMAGE_RESOLVED),
                    ("False", reason, error)
                );
                assert_eq!(status["message"], error);
                assert_eq!(
                    condition(&status, CONDITION_SOURCE_RESOLVED),
                    condition(&outage.serving, CONDITION_SOURCE_RESOLVED)
                );
                assert_eq!(
                    condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["reason"],
                    REASON_MIN_REPLICAS
                );
                // The Deployment keeps the tree it runs.
                let applies: Vec<_> = outage
                    .requests
                    .iter()
                    .filter(|r| r.method == "PATCH" && r.path.contains("/deployments/"))
                    .collect();
                assert!(applies.is_empty(), "{reason}: {applies:?}");
                assert_eq!(outage.requeue, Action::requeue(retry), "{reason}");
                assert_eq!(
                    pass(&outage.state, LeaderGate::new()).await,
                    None,
                    "{reason}: the follower disagrees"
                );
            }
        }

        #[tokio::test]
        async fn missing_tag_or_failed_registry_login_still_fails_a_serving_code_location() {
            for (code, reason) in [(404, REASON_TAG_NOT_FOUND), (401, REASON_AUTH_FAILED)] {
                let outage = registry_outage(vec![ResponseTemplate::new(code)]).await;
                let failed = patched_status(&outage.requests).expect("status patch");

                assert_eq!(failed["phase"], "Failed", "{failed}");
                assert_eq!(failed.get("grpcEndpoint"), None, "{failed}");
                assert_eq!(
                    says(&failed, CONDITION_IMAGE_RESOLVED).1,
                    reason,
                    "{failed}"
                );
                assert_eq!(
                    outage.requeue,
                    Action::requeue(TERMINAL_RETRY),
                    "HTTP {code}"
                );
                assert_eq!(
                    pass(&outage.state, LeaderGate::new()).await,
                    None,
                    "HTTP {code}"
                );
            }
        }

        #[tokio::test]
        async fn registry_outage_fails_a_git_code_location_with_nothing_to_serve() {
            let registry = registry(vec![ResponseTemplate::new(503)]).await;
            let state = ApiState::default();
            state
                .lock()
                .unwrap()
                .code_locations
                .insert("x".into(), latest_runtime_cl(&registry));

            let (requeue, requests) =
                reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

            let status = patched_status(&requests).expect("status patch");
            assert_eq!(status["phase"], "Failed", "{status}");
            assert_eq!(
                says(&status, CONDITION_IMAGE_RESOLVED),
                (
                    "False",
                    REASON_REGISTRY_ERROR,
                    "status 503 Service Unavailable"
                )
            );
            assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
        }
    }
}
