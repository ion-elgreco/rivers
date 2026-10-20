//! The CodeLocation status a pass publishes, and the conditions in it.

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube_client::ResourceExt;
use kube_client::api::{Api, Patch, PatchParams};
use rivers_k8s::crd::code_location::{
    CONDITION_DEPLOYMENT_AVAILABLE, CONDITION_IMAGE_RESOLVED, CONDITION_SOURCE_RESOLVED,
    CONDITION_WORKSPACE_KEPT, CodeLocation, CodeLocationCondition, CodeLocationPhase,
    CodeLocationStatus, REASON_APPLY_FAILED, REASON_AWAITING_LEADER, REASON_COMMIT_PINNED,
    REASON_COMMIT_RESOLVED, REASON_GIT_AUTH_FAILED, REASON_GIT_HOST_KEY_REJECTED,
    REASON_GIT_MALFORMED_RESPONSE, REASON_GIT_RATE_LIMITED, REASON_GIT_UNREACHABLE,
    REASON_INVALID_REF, REASON_INVALID_URL, REASON_MIN_REPLICAS, REASON_NO_DEPLOYMENT_STATUS,
    REASON_PROGRESS_DEADLINE, REASON_REF_NOT_FOUND, REASON_ROLLING_OUT,
};
use rivers_k8s::crd::run::RunSource;

use super::image::ImageError;
use super::rollout::{
    GitRollout, Rollout, Template, build_failure_message, load_failure_message, observe_deployment,
    rollout_message,
};
use super::{Context, FIELD_MANAGER, TERMINAL_RETRY};
use crate::codelocation::git;
use crate::codelocation::resources::{deployment_name, grpc_endpoint};

/// A status of the git source, not yet replaced by the leader's first image
/// pass after a switch to image mode.
pub(super) fn is_git_status(status: &CodeLocationStatus) -> bool {
    status.run_source.is_some()
        || status.resolved_commit.is_some()
        || status.resolved_ref.is_some()
        || status.last_fetched_at.is_some()
        || status
            .conditions
            .iter()
            .any(|c| c.r#type == CONDITION_SOURCE_RESOLVED)
}

/// Condition reason + requeue delay for a git resolution failure. Mirrors
/// `ImageError::retry_after`'s philosophy: auth/ref problems are terminal
/// until the CR or Secret changes (slow recheck), transport is transient.
/// A transient fetch failure requeues by the resolver's backoff instead.
pub(super) fn git_error_reason_retry(err: &git::GitError) -> (&'static str, Duration) {
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
pub(super) fn source_display(source: &RunSource) -> String {
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

pub(super) struct GitStatusUpdate {
    /// `None` on followers: resolution is the leader's.
    pub(super) resolution: Option<GitResolution>,
    pub(super) rollout: GitRollout,
    /// The leader's apply of this pass: the object the API server refused,
    /// and its answer ([`ApplyFailure::message`]).
    pub(super) apply_failure: Option<String>,
    pub(super) endpoint: String,
}

/// The leader's runtime-image and ref resolution of this pass.
pub(super) enum GitResolution {
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

/// Publish git CL `cl`'s status for what its Deployment shows now and for
/// the leader's `resolution` and `apply_failure` of this pass. Returns
/// whether `cl` is Ready with every pod on the current template.
pub(super) async fn patch_git_status(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    resolution: Option<GitResolution>,
    apply_failure: Option<String>,
) -> Result<bool, kube_client::Error> {
    let name = cl.name_any();
    let deployments_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), namespace);
    let deployment = deployments_api
        .get_status(&deployment_name(&name))
        .await
        .ok();
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let rollout = observe_deployment(&name, deployment.as_ref(), &pods_api).await;
    let complete = rollout.complete;
    let update = GitStatusUpdate {
        resolution,
        rollout,
        apply_failure,
        endpoint: grpc_endpoint(&name, namespace, cl.spec.grpc_port),
    };
    let status = git_status(cl, update, jiff::Timestamp::now());
    let code_locations_api: Api<CodeLocation> = Api::namespaced(ctx.client.clone(), namespace);
    patch_status(&code_locations_api, cl, &status).await?;
    Ok(complete && status.phase == Some(CodeLocationPhase::Ready))
}

/// What a pass publishes of a rollout, in either mode. Runs get the serving
/// template: the pod template's once every pod runs it and is ready, until
/// then `prior_serving` — a template that is still building, or fails to
/// build or load, never reaches a run. A rollout in progress or stuck shows
/// on `DeploymentAvailable`, not in the phase, with the error of a failed
/// build or load of its template. An object the API server refused shows
/// the same way: the cluster still runs what it ran before, so the serving
/// template serves on; with no serving template the phase is Failed.
struct RolloutStatus<T> {
    serving: Option<T>,
    ready: bool,
    failed: bool,
    phase: CodeLocationPhase,
    reason: &'static str,
    message: Option<String>,
    /// A failed build or load of the template rolling out.
    pod_failure: Option<String>,
}

fn rollout_status<T: Template>(
    cl: &CodeLocation,
    rollout: &Rollout<T>,
    prior_serving: Option<T>,
    apply_failure: Option<&str>,
) -> RolloutStatus<T> {
    let serving = match &rollout.template {
        Some(template) if rollout.complete => Some(template.clone()),
        _ => prior_serving,
    };
    let desired = cl.spec.replicas;
    let ready =
        serving.is_some() && desired > 0 && rollout.ready_replicas.is_some_and(|r| r >= desired);
    let failed = apply_failure.is_some() && serving.is_none();
    let pod_failure = rollout.template.as_ref().and_then(|target| {
        let build = rollout
            .build_failure
            .as_ref()
            .map(|f| build_failure_message(target, f));
        build.or_else(|| {
            rollout
                .load_failure
                .as_ref()
                .map(|f| load_failure_message(target, f))
        })
    });
    let (reason, message) = if let Some(failure) = apply_failure {
        let message = match &serving {
            Some(serving) => format!("{failure}; runs use {}", serving.describe(None)),
            None => failure.to_string(),
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
            pod_failure.clone(),
        ) {
            (Some(rollout), Some(failure)) => Some(format!("{rollout}; {failure}")),
            (rollout, failure) => rollout.or(failure),
        };
        (reason, message)
    };
    let phase = if ready {
        CodeLocationPhase::Ready
    } else if failed {
        CodeLocationPhase::Failed
    } else {
        CodeLocationPhase::Deploying
    };
    RolloutStatus {
        serving,
        ready,
        failed,
        phase,
        reason,
        message,
        pod_failure,
    }
}

/// Git-mode status: the serving tree of [`rollout_status`], and
/// `status.message` shows a resolution error before the Deployment's.
pub(super) fn git_status(
    cl: &CodeLocation,
    update: GitStatusUpdate,
    at: jiff::Timestamp,
) -> CodeLocationStatus {
    let now = &at.to_string();
    let prior = cl.status.as_ref();
    let rollout = rollout_status(
        cl,
        &update.rollout,
        prior.and_then(|s| s.run_source.clone()),
        update.apply_failure.as_deref(),
    );
    let serving = rollout.serving;
    let mut status = CodeLocationStatus {
        resolved_image: serving.as_ref().map(|t| t.runtime_image.clone()),
        grpc_endpoint: (!rollout.failed).then_some(update.endpoint),
        ready_replicas: update.rollout.ready_replicas.filter(|_| !rollout.failed),
        source: serving.as_ref().map(source_display),
        resolved_commit: serving.as_ref().map(|t| t.git.commit.clone()),
        resolved_ref: serving.as_ref().and_then(|t| t.git.r#ref.clone()),
        run_source: serving,
        ..next_status(cl, rollout.phase, now)
    };
    match update.resolution {
        Some(GitResolution::ImageResolved {
            image,
            image_reason,
            source,
        }) => {
            let (source_status, source_reason, source_message) = match source {
                Ok(resolved) => {
                    status.last_fetched_at = resolved.fetched_at.map(|at| later_fetch(prior, at));
                    let reason = match resolved.ref_name {
                        None => REASON_COMMIT_PINNED,
                        Some(_) => REASON_COMMIT_RESOLVED,
                    };
                    ("True", reason, resolved.commit)
                }
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
        if rollout.ready { "True" } else { "False" },
        rollout.reason,
        rollout.message,
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
        .or(rollout.pod_failure);
    status
}

/// The leader's pass over an image-mode CL.
pub(super) struct ImagePass {
    /// The digest reference the Deployment now rolls out.
    pub(super) image: String,
    pub(super) image_reason: &'static str,
    /// Why the git workspace of an earlier git source stays, while it does.
    pub(super) workspace_kept: Option<(&'static str, String)>,
}

pub(super) struct ImageStatusUpdate {
    /// `None` on followers: resolution and the git workspace are the leader's.
    pub(super) leader: Option<ImagePass>,
    pub(super) rollout: Rollout<String>,
    pub(super) endpoint: String,
}

/// Publish image-mode CL `cl`'s status for what `deployment` shows now and
/// for the leader's pass, if this is one. Returns whether `cl` is Ready with
/// every pod on the current template.
pub(super) async fn patch_image_status(
    cl: &CodeLocation,
    ctx: &Context,
    namespace: &str,
    deployment: Option<&Deployment>,
    leader: Option<ImagePass>,
) -> Result<bool, kube_client::Error> {
    let name = cl.name_any();
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let rollout = observe_deployment(&name, deployment, &pods_api).await;
    let complete = rollout.complete;
    let update = ImageStatusUpdate {
        leader,
        rollout,
        endpoint: grpc_endpoint(&name, namespace, cl.spec.grpc_port),
    };
    let status = image_status(cl, update, jiff::Timestamp::now());
    let code_locations_api: Api<CodeLocation> = Api::namespaced(ctx.client.clone(), namespace);
    patch_status(&code_locations_api, cl, &status).await?;
    Ok(complete && status.phase == Some(CodeLocationPhase::Ready))
}

/// Image-mode status: `resolvedImage` is the serving image of
/// [`rollout_status`], the one runs get; `ImageResolved` names the one
/// rolling out. A git status names the runtime image, which serves no run
/// of the image, so after a switch from git nothing serves until the image
/// has rolled out.
pub(super) fn image_status(
    cl: &CodeLocation,
    update: ImageStatusUpdate,
    at: jiff::Timestamp,
) -> CodeLocationStatus {
    let now = &at.to_string();
    let prior = cl.status.as_ref();
    let prior_serving = prior
        .filter(|s| !is_git_status(s))
        .and_then(|s| s.resolved_image.clone());
    let rollout = rollout_status(cl, &update.rollout, prior_serving, None);
    let mut status = CodeLocationStatus {
        phase: Some(rollout.phase),
        observed_generation: cl.metadata.generation,
        grpc_endpoint: Some(update.endpoint),
        last_reconciled: Some(now.clone()),
        ready_replicas: update.rollout.ready_replicas,
        message: rollout.pod_failure,
        // The pinned digest ref doubles as the Source column.
        source: rollout.serving.clone(),
        resolved_image: rollout.serving,
        ..Default::default()
    };
    match &update.leader {
        Some(pass) => push_condition(
            &mut status,
            prior,
            CONDITION_IMAGE_RESOLVED,
            "True",
            pass.image_reason,
            Some(pass.image.clone()),
            now,
        ),
        None => status
            .conditions
            .extend(kept_conditions(prior, &[CONDITION_IMAGE_RESOLVED])),
    }
    push_condition(
        &mut status,
        prior,
        CONDITION_DEPLOYMENT_AVAILABLE,
        if rollout.ready { "True" } else { "False" },
        rollout.reason,
        rollout.message,
        now,
    );
    match update.leader {
        Some(ImagePass {
            workspace_kept: Some((reason, message)),
            ..
        }) => push_condition(
            &mut status,
            prior,
            CONDITION_WORKSPACE_KEPT,
            "True",
            reason,
            Some(message),
            now,
        ),
        Some(_) => {}
        None => status
            .conditions
            .extend(kept_conditions(prior, &[CONDITION_WORKSPACE_KEPT])),
    }
    status
}

/// `lastFetchedAt` for a fetch at `at`: never before `prior`'s. A replica that
/// leads after another can answer from an older fetch in its cache, and two
/// replicas that both act as leader would else undo each other's write on
/// every pass.
fn later_fetch(prior: Option<&CodeLocationStatus>, at: jiff::Timestamp) -> String {
    prior
        .and_then(|s| s.last_fetched_at.clone())
        .filter(|known| {
            known
                .parse::<jiff::Timestamp>()
                .is_ok_and(|known| known >= at)
        })
        .unwrap_or_else(|| at.to_string())
}

/// `prior`'s conditions of the `kinds` this pass did not resolve again.
pub(super) fn kept_conditions<'a>(
    prior: Option<&'a CodeLocationStatus>,
    kinds: &'a [&'a str],
) -> impl Iterator<Item = CodeLocationCondition> + 'a {
    prior
        .into_iter()
        .flat_map(|s| &s.conditions)
        .filter(move |c| kinds.contains(&c.r#type.as_str()))
        .cloned()
}

/// Append a condition, preserving `last_transition_time` from the prior
/// status when the condition's `status` field hasn't flipped. Per K8s API
/// convention, `lastTransitionTime` only changes on actual transitions.
pub(super) fn push_condition(
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

/// Write `status` as `cl`'s status, unless every field but `lastReconciled`
/// is as the prior status has it: such a write would wake watchers for
/// nothing. A write starts another pass at once, so no field may change in
/// a pass that finds nothing new, else the passes never stop:
/// `lastFetchedAt` is the time of the fetch that the resolver's cache
/// answers the next pass with.
pub(super) async fn patch_status(
    code_locations_api: &Api<CodeLocation>,
    cl: &CodeLocation,
    status: &CodeLocationStatus,
) -> Result<(), kube_client::Error> {
    let unchanged = cl.status.as_ref().is_some_and(|prior| {
        *status
            == CodeLocationStatus {
                last_reconciled: status.last_reconciled.clone(),
                ..prior.clone()
            }
    });
    if unchanged {
        return Ok(());
    }
    let body = serde_json::json!({
        "apiVersion": "rivers.io/v1alpha1",
        "kind": "CodeLocation",
        "status": status,
    });
    code_locations_api
        .patch_status(
            &cl.name_any(),
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&body),
        )
        .await?;
    Ok(())
}

/// `cl`'s status for a pass at `at` in `phase`. It keeps what the prior
/// status says the pods run (`resolvedImage`, `source` and the git fields)
/// until the pass sets these; the pass sets the other fields anew.
fn next_status(cl: &CodeLocation, phase: CodeLocationPhase, at: &str) -> CodeLocationStatus {
    CodeLocationStatus {
        phase: Some(phase),
        observed_generation: cl.metadata.generation,
        last_reconciled: Some(at.to_string()),
        grpc_endpoint: None,
        ready_replicas: None,
        message: None,
        conditions: Vec::new(),
        ..cl.status.clone().unwrap_or_default()
    }
}

/// Why a pass resolved nothing for the pods to run.
pub(super) enum Unresolved<'a> {
    /// A follower, before the leader has resolved the image.
    AwaitingLeader,
    Image(&'a ImageError),
    Source(&'a git::GitFailure),
}

/// The status of a pass that resolved nothing for the pods to run. They
/// still run what the prior status says, so that stays: also a git status
/// after a switch to image mode, which only the leader's first image pass
/// replaces. In image mode, a kept git workspace stays named until it is
/// gone.
fn unresolved_status(
    cl: &CodeLocation,
    why: Unresolved<'_>,
    at: jiff::Timestamp,
) -> CodeLocationStatus {
    let (phase, message, condition) = match why {
        Unresolved::AwaitingLeader => (
            CodeLocationPhase::Pending,
            "awaiting leader replica".to_string(),
            (
                CONDITION_IMAGE_RESOLVED,
                "Unknown",
                REASON_AWAITING_LEADER,
                "follower replica — leader will resolve digest".to_string(),
            ),
        ),
        Unresolved::Image(err) => (
            CodeLocationPhase::Failed,
            err.message(),
            (
                CONDITION_IMAGE_RESOLVED,
                "False",
                err.reason(),
                err.message(),
            ),
        ),
        Unresolved::Source(failure) => {
            let (reason, message) = source_failure(failure, at);
            (
                CodeLocationPhase::Failed,
                message.clone(),
                (CONDITION_SOURCE_RESOLVED, "False", reason, message),
            )
        }
    };
    let (kind, condition_status, reason, condition_message) = condition;
    let now = at.to_string();
    let prior = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        message: Some(message),
        ..next_status(cl, phase, &now)
    };
    push_condition(
        &mut status,
        prior,
        kind,
        condition_status,
        reason,
        Some(condition_message),
        &now,
    );
    if !cl.spec.is_git() {
        status
            .conditions
            .extend(kept_conditions(prior, &[CONDITION_WORKSPACE_KEPT]));
    }
    status
}

pub(super) async fn patch_unresolved(
    code_locations_api: &Api<CodeLocation>,
    cl: &CodeLocation,
    why: Unresolved<'_>,
) -> Result<(), kube_client::Error> {
    let status = unresolved_status(cl, why, jiff::Timestamp::now());
    patch_status(code_locations_api, cl, &status).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_error_reasons_and_retries() {
        use crate::codelocation::git::GitError;
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
}
