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
//! the leader issues registry HEADs. In image mode, followers still reconcile
//! owned resources against whatever digest is already in
//! `status.resolvedImage`, so the backing Deployment doesn't drift while a
//! leader election is in flight.

use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube_client::ResourceExt;
use kube_client::api::{Api, Patch, PatchParams};
use kube_runtime::controller::Action;
use kube_runtime::reflector::Store;
use rivers_k8s::crd::code_location::{
    CONDITION_DEPLOYMENT_AVAILABLE, CONDITION_IMAGE_RESOLVED, CONDITION_SOURCE_RESOLVED,
    CONDITION_WORKSPACE_KEPT, CodeLocation, CodeLocationPhase, CodeLocationStatus, GitRef,
    REASON_APPLY_FAILED,
};
use rivers_k8s::crd::run::Run;

use super::git::GitResolveRequest;
use super::registry::{ImageRef, RegistryClient};
use super::resources::{build_deployment, build_service, deployment_name, grpc_endpoint};
use crate::leader::LeaderGate;
use crate::metrics;

mod config;
mod credentials;
mod image;
mod rollout;
mod status;
#[cfg(test)]
mod tests;
mod workspace;

pub use config::WorkspaceConfig;
pub(crate) use image::wanted_image;

use config::parse_refresh_interval;
use credentials::git_credentials;
use image::{ImageOutcome, resolve_image};
use rollout::evaluate_deployment_phase;
use status::{
    GitResolution, Unresolved, git_error_reason_retry, is_git_status, kept_conditions,
    patch_git_status, patch_status, patch_unresolved, push_condition,
};
use workspace::{Applied, apply_git_workspace, remove_git_workspace};

/// Requeue delay for followers waiting on the leader to resolve a digest.
pub(super) const FOLLOWER_WAIT: Duration = Duration::from_secs(30);
/// Requeue delay after a terminal-looking registry error (auth, 404). The
/// reconciler still checks back occasionally in case the user fixes the
/// underlying issue without bumping the generation.
pub(super) const TERMINAL_RETRY: Duration = Duration::from_secs(300);
/// Short follow-up requeue when the Deployment is rolling out.
pub(super) const DEPLOYMENT_ROLLOUT_POLL: Duration = Duration::from_secs(10);
/// How often image mode looks again at a workspace PVC that it keeps for an
/// earlier git source: no event wakes it when a run is deleted.
pub(super) const WORKSPACE_RECHECK: Duration = Duration::from_secs(60);
/// Field manager used for all server-side applies.
pub(super) const FIELD_MANAGER: &str = "rivers-operator-code-location";

pub struct Context {
    pub client: kube_client::Client,
    pub namespace: String,
    pub registry: Arc<RegistryClient>,
    /// Ref→commit resolver for git-sourced CodeLocations. Only
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
                patch_unresolved(&code_locations_api, &cl, Unresolved::AwaitingLeader).await?;
                return Ok(Action::requeue(FOLLOWER_WAIT));
            }
            ImageOutcome::Error(err) => {
                patch_unresolved(&code_locations_api, &cl, Unresolved::Image(&err)).await?;
                return Ok(Action::requeue(err.retry_after()));
            }
        };

    let deployment = build_deployment(
        &cl,
        &resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
        &ctx.otel_pod_cfg,
        None,
    );
    let service = build_service(&cl);

    tokio::try_join!(
        apply(&deployments_api, &deployment),
        apply(&services_api, &service),
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
    // No git fields: the pods run the image.
    let mut status = CodeLocationStatus {
        phase: Some(phase.clone()),
        observed_generation: generation,
        resolved_image: Some(resolved_image.clone()),
        grpc_endpoint: Some(endpoint),
        last_reconciled: Some(now_rfc3339.clone()),
        ready_replicas,
        // Image mode: the pinned digest ref doubles as the Source column.
        source: Some(resolved_image.clone()),
        ..Default::default()
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
    patch_status(&code_locations_api, &cl, &status).await?;

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

/// Git-mode reconcile: pin the runtime image digest and the ref's commit,
/// apply the objects that build and serve that tree, and publish status.
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
    let git_spec = cl
        .spec
        .git
        .as_ref()
        .expect("reconcile_git dispatched for a non-git CodeLocation");

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
            patch_git_status(cl, ctx, namespace, None, None).await?;
        } else {
            patch_unresolved(code_locations_api, cl, Unresolved::AwaitingLeader).await?;
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
                patch_unresolved(code_locations_api, cl, Unresolved::AwaitingLeader).await?;
                return Ok(Action::requeue(FOLLOWER_WAIT));
            }
            ImageOutcome::Error(err) => {
                let retry = err.retry_after();
                if err.is_transient() && serving {
                    // The Deployment keeps the tree it runs until the
                    // registry answers again.
                    let resolution = GitResolution::ImageFailed(err);
                    patch_git_status(cl, ctx, namespace, Some(resolution), None).await?;
                } else {
                    patch_unresolved(code_locations_api, cl, Unresolved::Image(&err)).await?;
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
                let resolution = GitResolution::ImageResolved {
                    image: resolved_image,
                    image_reason,
                    source: Err(failure),
                };
                patch_git_status(cl, ctx, namespace, Some(resolution), None).await?;
            } else {
                patch_unresolved(code_locations_api, cl, Unresolved::Source(&failure)).await?;
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
    let resolution = GitResolution::ImageResolved {
        image: resolved_image,
        image_reason,
        source: Ok(resolved),
    };
    let apply_failure = refusal.as_ref().map(|failure| failure.message.clone());
    let settled = patch_git_status(cl, ctx, namespace, Some(resolution), apply_failure).await?;

    let requeue = if let Some(failure) = refusal {
        Action::requeue(failure.retry)
    } else if !settled {
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

/// Server-side apply `desired`, taking over the fields it sets from any other
/// field manager.
pub(super) async fn apply<K>(api: &Api<K>, desired: &K) -> Result<(), kube_client::Error>
where
    K: kube_client::Resource
        + Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug,
{
    api.patch(
        &desired.name_any(),
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
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
