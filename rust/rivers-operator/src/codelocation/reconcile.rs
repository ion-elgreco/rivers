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

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube_client::ResourceExt;
use kube_client::api::{Api, Patch, PatchParams};
use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::{
    CONDITION_DEPLOYMENT_AVAILABLE, CONDITION_IMAGE_RESOLVED, CONDITION_SOURCE_RESOLVED,
    CodeLocation, CodeLocationCondition, CodeLocationPhase, CodeLocationSpec, CodeLocationStatus,
    IMMUTABLE_TAG_ANNOTATION, REASON_AUTH_FAILED, REASON_AWAITING_LEADER, REASON_COMMIT_PINNED,
    REASON_COMMIT_RESOLVED, REASON_DIGEST_PINNED, REASON_DIGEST_RESOLVED, REASON_GIT_AUTH_FAILED,
    REASON_GIT_HOST_KEY_REJECTED, REASON_GIT_MALFORMED_RESPONSE, REASON_GIT_RATE_LIMITED,
    REASON_GIT_UNREACHABLE, REASON_MIN_REPLICAS, REASON_NO_DEPLOYMENT_STATUS,
    REASON_PROGRESS_DEADLINE, REASON_RATE_LIMITED, REASON_REF_NOT_FOUND, REASON_REGISTRY_ERROR,
    REASON_ROLLING_OUT, REASON_TAG_NOT_FOUND,
};

use super::git::{self, GitCredentials, GitResolveRequest};

use super::image_auth::resolve_auth;
use super::registry::{
    ImageRef, RegistryAuth, RegistryClient, RegistryError, Resolution, ResolveRequest,
};
use super::resources::{
    MAIN_CONTAINER, build_deployment, build_git_deployment, build_keep_config_map, build_service,
    build_workspace_pvc, deployment_name, git_working_dir, grpc_endpoint, keep_config_map_name,
    service_name, workspace_pvc_name,
};
use crate::leader::LeaderGate;
use crate::metrics;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use rivers_k8s::crd::run::RunSource;
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
    pub shared_size: String,
    pub empty_dir_limit: String,
    pub keep_revisions: u32,
    pub min_tree_age: String,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            shared_enabled: false,
            storage_class: None,
            shared_size: "20Gi".to_string(),
            empty_dir_limit: "2Gi".to_string(),
            keep_revisions: 3,
            min_tree_age: "1h".to_string(),
        }
    }
}

impl WorkspaceConfig {
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
            .unwrap_or_else(|| Quantity(self.empty_dir_limit.clone()));
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
        // Git-mode display (`main@9f3c1ab`) arrives with resolve_source().
        source: Some(resolved_image.clone()),
        resolved_commit: prior_status.and_then(|s| s.resolved_commit.clone()),
        resolved_ref: prior_status.and_then(|s| s.resolved_ref.clone()),
        last_fetched_at: prior_status.and_then(|s| s.last_fetched_at.clone()),
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

    if !status_substantively_equal(prior_status, &status) {
        patch_status(&code_locations_api, &name, &status).await?;
    }

    let requeue = if matches!(phase, CodeLocationPhase::Ready) && immutable {
        // Immutable + Ready → rely on CR/Deployment change events.
        Action::await_change()
    } else if !matches!(phase, CodeLocationPhase::Ready) {
        Action::requeue(DEPLOYMENT_ROLLOUT_POLL)
    } else {
        Action::requeue(jitter(refresh_after))
    };
    Ok(requeue)
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

    // Followers neither resolve nor apply: they refresh what the Deployment
    // shows and keep the leader's resolution conditions, so both replicas
    // publish the same status. A Failed status shows nothing of the
    // Deployment, and only the leader's next resolution clears it.
    if !ctx.leader.is_leader() {
        let prior = cl.status.as_ref();
        if prior.is_some_and(|s| s.phase == Some(CodeLocationPhase::Failed)) {
            return Ok(Action::requeue(FOLLOWER_WAIT));
        }
        let published = prior.is_some_and(|s| {
            s.run_source.is_some()
                || s.conditions
                    .iter()
                    .any(|c| c.r#type == CONDITION_SOURCE_RESOLVED)
        });
        if published {
            let rollout = observe_git_deployment(&name, deployments_api).await;
            let update = GitStatusUpdate {
                resolution: None,
                fetched_at: None,
                rollout,
                endpoint,
            };
            patch_git_status(code_locations_api, &name, cl, update).await?;
        } else {
            patch_waiting_status(code_locations_api, &name, generation, cl).await?;
        }
        return Ok(Action::requeue(FOLLOWER_WAIT));
    }

    // Runtime image first, through the same registry pipeline as image mode.
    let timer = Instant::now();
    let (resolved_image, image_reason) = match resolve_image(cl, ctx, secrets_api).await {
        ImageOutcome::Resolved {
            resolved_image,
            reason,
            ..
        } => {
            metrics::observe_resolution_seconds(timer.elapsed().as_secs_f64());
            (resolved_image, reason)
        }
        ImageOutcome::AwaitingLeader => {
            patch_waiting_status(code_locations_api, &name, generation, cl).await?;
            return Ok(Action::requeue(FOLLOWER_WAIT));
        }
        ImageOutcome::Error(err) => {
            let retry = err.retry_after();
            patch_error_status(code_locations_api, &name, generation, cl, &err).await?;
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
            let serving = cl.status.as_ref().is_some_and(|s| s.run_source.is_some());
            if failure.error.is_transient() && serving {
                // The Deployment keeps the tree it runs until a commit
                // resolves again.
                let update = GitStatusUpdate {
                    resolution: Some(GitResolution {
                        image: resolved_image,
                        image_reason,
                        source: Err(failure),
                    }),
                    fetched_at: None,
                    rollout: observe_git_deployment(&name, deployments_api).await,
                    endpoint,
                };
                patch_git_status(code_locations_api, &name, cl, update).await?;
            } else {
                patch_git_error(code_locations_api, &name, generation, cl, &failure).await?;
            }
            return Ok(Action::requeue(retry));
        }
    };

    apply_git_workspace(
        cl,
        ctx,
        namespace,
        deployments_api,
        services_api,
        &resolved_image,
        &resolved,
    )
    .await?;
    let rollout = observe_git_deployment(&name, deployments_api).await;
    let rolling_out = !rollout.complete;
    let update = GitStatusUpdate {
        resolution: Some(GitResolution {
            image: resolved_image,
            image_reason,
            source: Ok(resolved),
        }),
        fetched_at: Some(jiff::Timestamp::now().to_string()),
        rollout,
        endpoint,
    };
    let phase = patch_git_status(code_locations_api, &name, cl, update).await?;

    let pinned = git_spec
        .r#ref
        .commit
        .as_deref()
        .is_some_and(|c| !c.is_empty());
    let immutable_tag = git_spec
        .r#ref
        .tag
        .as_deref()
        .is_some_and(super::registry::looks_immutable);
    let requeue = if rolling_out || !matches!(phase, CodeLocationPhase::Ready) {
        Action::requeue(DEPLOYMENT_ROLLOUT_POLL)
    } else if pinned {
        Action::await_change()
    } else if immutable_tag {
        Action::requeue(jitter(Duration::from_secs(3600)))
    } else {
        Action::requeue(jitter(poll))
    };
    Ok(requeue)
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
) -> Result<(), Error> {
    let name = cl.name_any();
    let git_spec = cl.spec.git.as_ref().expect("git CL");
    let key = workspace::workspace_key(&resolved.commit, resolved_image);

    let wspec = WorkspaceSpec {
        key: key.clone(),
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
        min_tree_age: Some(ctx.workspace.min_tree_age.clone()),
        extra_env: cl.spec.env.clone(),
    };
    let pieces = workspace::builder_pod_pieces(&wspec);

    if ctx.workspace.shared_enabled {
        let pvc_size = git_spec
            .workspace_size
            .as_ref()
            .map(|q| q.0.clone())
            .unwrap_or_else(|| ctx.workspace.shared_size.clone());
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), namespace);
        ensure_workspace_pvc(
            &pvc_api,
            build_workspace_pvc(cl, &pvc_size, ctx.workspace.storage_class.as_deref()),
        )
        .await?;
        // Keep-set: the tree being rolled out, the tree runs get until that
        // rollout finishes, and every non-terminal run's tree. One LIST per
        // reconcile is fine at reconcile cadence; a stale snapshot is covered
        // by keepRevisions + minTreeAge (see the RFC's prune soundness
        // argument). The ConfigMap indirection means this refresh never
        // rolls the Deployment.
        let runs_api: Api<rivers_k8s::crd::run::Run> =
            Api::namespaced(ctx.client.clone(), namespace);
        let runs = runs_api
            .list(&Default::default())
            .await
            .map(|l| l.items)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "listing Runs for the keep-set; keeping the CL's own trees only");
                Vec::new()
            });
        let serving_key = cl
            .status
            .as_ref()
            .and_then(|s| s.run_source.as_ref())
            .map(RunSource::workspace_key);
        let keep = workspace_keep_csv(&key, serving_key.as_deref(), &name, &runs);
        let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
        apply_config_map(&cm_api, &build_keep_config_map(cl, &keep)).await?;
    }

    let deployment = build_git_deployment(
        cl,
        resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
        &ctx.otel_pod_cfg,
        &pieces,
        git_working_dir(git_spec.path.as_deref()),
    );
    let service = build_service(cl);
    tokio::try_join!(
        apply_deployment(deployments_api, &deployment),
        apply_service(services_api, &service),
    )?;
    Ok(())
}

/// Read the git Deployment without mutating anything — shared by the leader
/// (post-apply) and follower (status refresh) paths.
async fn observe_git_deployment(name: &str, deployments_api: &Api<Deployment>) -> GitRollout {
    let deployment = deployments_api
        .get_status(&deployment_name(name))
        .await
        .ok();
    git_rollout(deployment.as_ref())
}

/// The tree `deployment`'s pod template runs: its main container's
/// `RIVERS_RUN_SOURCE`.
fn template_source(deployment: &Deployment) -> Option<RunSource> {
    let source = deployment
        .spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
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
        && generation_observed(deployment)
        && all(status.updated_replicas)
        && all(status.ready_replicas)
        && all(status.available_replicas)
        && status.replicas.unwrap_or(0) == status.updated_replicas.unwrap_or(0)
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
/// tree of every non-terminal Run referencing the CL — queued (`Pending`, or
/// no status yet) and `Cancelling` included, not just `Running`; a run
/// waiting on a concurrency pool is precisely the one most likely to sit
/// through several commits. Keys, not commits: each run's tree is keyed on
/// its source's runtime image, whatever image its pods run. Sorted + deduped
/// so the ConfigMap value is deterministic and refreshes don't churn.
fn workspace_keep_csv(
    target_key: &str,
    serving_key: Option<&str>,
    cl_name: &str,
    runs: &[rivers_k8s::crd::run::Run],
) -> String {
    use rivers_k8s::crd::run::RunPhase;
    let mut keys = std::collections::BTreeSet::new();
    keys.insert(target_key.to_string());
    keys.extend(serving_key.map(str::to_string));
    for run in runs {
        if run.spec.code_location_ref.name != cl_name {
            continue;
        }
        let terminal = matches!(
            run.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(RunPhase::Succeeded) | Some(RunPhase::Failed) | Some(RunPhase::Cancelled)
        );
        if terminal {
            continue;
        }
        if let Some(source) = &run.spec.source {
            keys.insert(source.workspace_key());
        }
    }
    keys.into_iter().collect::<Vec<_>>().join(",")
}

/// PVC specs are largely immutable — create when absent, otherwise leave
/// the existing claim alone (a size change would need manual expansion).
async fn ensure_workspace_pvc(
    api: &Api<PersistentVolumeClaim>,
    pvc: PersistentVolumeClaim,
) -> Result<(), kube_client::Error> {
    let name = pvc.metadata.name.clone().unwrap_or_default();
    match api.get(&name).await {
        Ok(_) => Ok(()),
        Err(kube_client::Error::Api(ae)) if ae.code == 404 => api
            .create(&kube_client::api::PostParams::default(), &pvc)
            .await
            .map(|_| ()),
        Err(e) => Err(e),
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

/// Build [`GitCredentials`] from the CR's Secret. `identity` (+ mandatory
/// `known_hosts`) selects SSH; `username`+`password` selects basic auth; an
/// absent secretRef or empty Secret is anonymous.
async fn git_credentials(
    git_spec: &rivers_k8s::crd::code_location::GitSource,
    secrets_api: &Api<Secret>,
) -> Result<GitCredentials, git::GitError> {
    let Some(secret_ref) = &git_spec.secret_ref else {
        return Ok(GitCredentials::Anonymous);
    };
    let secret = secrets_api.get(&secret_ref.name).await.map_err(|e| {
        git::GitError::Unreachable(format!("reading git Secret '{}': {e}", secret_ref.name))
    })?;
    let data = secret.data.unwrap_or_default();
    let entry = |key: &str| {
        data.get(key)
            .map(|v| String::from_utf8_lossy(&v.0).into_owned())
    };

    if let Some(identity) = entry("identity") {
        let known_hosts = entry("known_hosts").ok_or_else(|| {
            git::GitError::KnownHostsUnavailable(format!(
                "git Secret '{}' has `identity` but no `known_hosts` — refusing SSH without \
                 host-key pinning",
                secret_ref.name
            ))
        })?;
        return Ok(GitCredentials::Ssh {
            private_key_openssh: identity,
            known_hosts,
        });
    }
    match (entry("username"), entry("password")) {
        (Some(username), Some(password)) => Ok(GitCredentials::Basic { username, password }),
        (None, None) => Ok(GitCredentials::Anonymous),
        _ => Err(git::GitError::AuthFailed(format!(
            "git Secret '{}' needs both `username` and `password` (or `identity` + `known_hosts`)",
            secret_ref.name
        ))),
    }
}

/// Condition reason + requeue delay for a git resolution failure. Mirrors
/// `ImageError::retry_after`'s philosophy: auth/ref problems are terminal
/// until the CR or Secret changes (slow recheck), transport is transient.
/// A transient fetch failure requeues by the resolver's backoff instead.
fn git_error_reason_retry(err: &git::GitError) -> (&'static str, Duration) {
    match err {
        git::GitError::RefNotFound(_) => (REASON_REF_NOT_FOUND, TERMINAL_RETRY),
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

struct GitStatusUpdate {
    /// `None` on followers: resolution is the leader's.
    resolution: Option<GitResolution>,
    fetched_at: Option<String>,
    rollout: GitRollout,
    endpoint: String,
}

/// The leader's runtime-image and ref resolution of this pass.
struct GitResolution {
    image: String,
    image_reason: &'static str,
    /// The ref's commit, or the transient failure that left the serving
    /// tree in place.
    source: Result<git::ResolvedRef, git::GitFailure>,
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
/// progress or stuck shows on `DeploymentAvailable`, not in the phase.
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
    let (deployment_reason, deployment_message) = if rollout.ready_replicas.is_none() {
        (REASON_NO_DEPLOYMENT_STATUS, None)
    } else if ready && rollout.complete {
        (REASON_MIN_REPLICAS, None)
    } else {
        let reason = if rollout.deadline_exceeded {
            REASON_PROGRESS_DEADLINE
        } else {
            REASON_ROLLING_OUT
        };
        let message = rollout_message(
            rollout.template.as_ref(),
            serving.as_ref(),
            rollout.deadline_exceeded,
        );
        (reason, message)
    };
    let mut status = CodeLocationStatus {
        phase: Some(if ready {
            CodeLocationPhase::Ready
        } else {
            CodeLocationPhase::Deploying
        }),
        observed_generation: cl.metadata.generation,
        resolved_image: serving.as_ref().map(|t| t.runtime_image.clone()),
        grpc_endpoint: Some(update.endpoint),
        last_reconciled: Some(now.to_string()),
        ready_replicas: rollout.ready_replicas,
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
        Some(resolution) => {
            let (source_status, source_reason, source_message) = match resolution.source {
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
                resolution.image_reason,
                Some(resolution.image),
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
        None => status.conditions.extend(
            prior
                .iter()
                .flat_map(|s| &s.conditions)
                .filter(|c| {
                    c.r#type == CONDITION_IMAGE_RESOLVED || c.r#type == CONDITION_SOURCE_RESOLVED
                })
                .cloned(),
        ),
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
    // From the condition, so followers that copy it publish the same.
    status.message = status
        .conditions
        .iter()
        .find(|c| c.r#type == CONDITION_SOURCE_RESOLVED && c.status == "False")
        .and_then(|c| c.message.clone());
    status
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

/// Minimal duration parser accepting `30s`, `5m`, `1h`. We avoid pulling in
/// the full `humantime` crate for this one use.
mod humantime_lite {
    use std::time::Duration;

    pub fn parse(s: &str) -> Option<Duration> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('s') {
            return num.parse::<u64>().ok().map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('m') {
            return num.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60));
        }
        if let Some(num) = s.strip_suffix('h') {
            return num
                .parse::<u64>()
                .ok()
                .map(|h| Duration::from_secs(h * 3600));
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

    #[test]
    fn jitter_within_bounds() {
        let base = Duration::from_secs(60);
        for _ in 0..100 {
            let j = jitter(base);
            assert!(j >= base);
            assert!(j <= base + Duration::from_secs(15));
        }
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

    #[test]
    fn keep_set_holds_current_and_non_terminal_runs() {
        use rivers_k8s::crd::run::{Run, RunCrdStatus, RunPhase, RunSpec};

        const RUNTIME: &str =
            "ghcr.io/rt@sha256:1a2b3c4d00000000000000000000000000000000000000000000000000000000";

        fn run_for(cl: &str, commit_char: char, phase: Option<RunPhase>) -> Run {
            let spec: RunSpec = serde_json::from_value(serde_json::json!({
                "codeLocationRef": { "name": cl },
                "image": RUNTIME,
                "target": "*",
                "source": {
                    "git": {
                        "url": "https://forge.example/r.git",
                        "commit": commit_char.to_string().repeat(40),
                    },
                    "runtimeImage": RUNTIME,
                },
            }))
            .unwrap();
            let mut run = Run::new(&format!("run-{commit_char}"), spec);
            run.status = phase.map(|p| RunCrdStatus {
                phase: Some(p),
                ..Default::default()
            });
            run
        }

        let runs = vec![
            run_for("analytics", 'a', Some(RunPhase::Running)),
            run_for("analytics", 'b', Some(RunPhase::Pending)),
            run_for("analytics", 'c', Some(RunPhase::Cancelling)),
            run_for("analytics", 'd', Some(RunPhase::Succeeded)), // released
            run_for("analytics", 'e', Some(RunPhase::Failed)),    // released
            run_for("analytics", 'a', None),                      // no status yet == not terminal
            run_for("other", 'f', Some(RunPhase::Running)),       // different CL
        ];

        let keep = workspace_keep_csv("current-key", None, "analytics", &runs);
        let entries: Vec<&str> = keep.split(',').collect();
        assert!(entries.contains(&"current-key"));
        // a (Running + statusless dedup), b (Pending — queued runs hold
        // their tree), c (Cancelling).
        let a_key = workspace::workspace_key(&"a".repeat(40), RUNTIME);
        let b_key = workspace::workspace_key(&"b".repeat(40), RUNTIME);
        let c_key = workspace::workspace_key(&"c".repeat(40), RUNTIME);
        assert!(entries.contains(&a_key.as_str()), "{keep}");
        assert!(entries.contains(&b_key.as_str()), "{keep}");
        assert!(entries.contains(&c_key.as_str()), "{keep}");
        // Terminal runs release; other CLs' runs are not ours.
        assert_eq!(entries.len(), 4, "dedup + releases: {keep}");
        let d_key = workspace::workspace_key(&"d".repeat(40), RUNTIME);
        assert!(!entries.contains(&d_key.as_str()));
    }

    #[test]
    fn keep_set_keys_each_run_on_the_runtime_image_of_its_source() {
        // RunBackendConfig.kubernetes(image=...): the run's pods run another
        // image on the tree the code location's runtime image built.
        let runtime = format!(
            "ghcr.io/acme/rivers-runtime@sha256:{}",
            "1a2b3c4d".repeat(8)
        );
        let run = rivers_k8s::crd::run::Run::new(
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

        let keep = workspace_keep_csv("current-key", None, "analytics", &[run]);

        assert_eq!(
            keep,
            format!(
                "{},current-key",
                workspace::workspace_key(&"a".repeat(40), &runtime)
            )
        );
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
                git: Arc::new(git::GitResolver::new(Duration::from_secs(5))),
                runtime_image: runtime_image.parse().unwrap(),
                leader: Arc::new(LeaderGate::leading()),
                code_location_service_account: "rivers-code-location".into(),
                workspace: WorkspaceConfig::default(),
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

    mod rollout {
        use super::*;
        use crate::run::test_helpers::{ApiRequest, MockApiState, mock_client};
        use k8s_openapi::api::apps::v1::DeploymentStatus;
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

        fn resolver() -> Arc<git::GitResolver> {
            Arc::new(git::GitResolver::new(Duration::from_secs(5)))
        }

        /// One reconcile pass over the stored CL, resolving refs with `git`;
        /// returns its requeue and the API requests it made.
        async fn reconcile_with(
            state: &ApiState,
            leader: LeaderGate,
            git: &Arc<git::GitResolver>,
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
                workspace: WorkspaceConfig::default(),
                surreal_pod_cfg: Default::default(),
                otel_pod_cfg: Default::default(),
            };
            let requeue = reconcile(Arc::new(cl), Arc::new(ctx)).await.unwrap();
            (requeue, state.lock().unwrap().requests[seen..].to_vec())
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
            }
        }

        /// The status a leader that resolved `target` publishes for `rollout`.
        fn leader_status(
            cl: &CodeLocation,
            target: char,
            rollout: GitRollout,
        ) -> CodeLocationStatus {
            let update = GitStatusUpdate {
                resolution: Some(GitResolution {
                    image: runtime(target),
                    image_reason: REASON_DIGEST_PINNED,
                    source: Ok(git::ResolvedRef {
                        commit: commit(target),
                        ref_name: None,
                    }),
                }),
                fetched_at: None,
                rollout,
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
                key: tree('b').workspace_key(),
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
                git_working_dir(None),
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

        const REPO: &str = "/acme/pipelines.git";

        /// A git host answering ref polls with `responses` in turn, the last
        /// one from then on.
        async fn forge(responses: Vec<ResponseTemplate>) -> MockServer {
            let server = MockServer::start().await;
            let last = responses.len() - 1;
            for (i, response) in responses.into_iter().enumerate() {
                let mock = Mock::given(method("GET"))
                    .and(path(format!("{REPO}/info/refs")))
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

        fn source_resolved(status: &serde_json::Value) -> (&str, &str, &str) {
            let c = condition(status, CONDITION_SOURCE_RESOLVED);
            let field = |name: &str| c[name].as_str().unwrap_or_default();
            (field("status"), field("reason"), field("message"))
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
    }
}
