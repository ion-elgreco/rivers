//! What the git Deployment and its pods show: the tree the template runs,
//! how far it has rolled out, and a failed build of it.

use k8s_openapi::api::apps::v1::{Deployment, DeploymentStatus};
use k8s_openapi::api::core::v1::{ContainerStateTerminated, Pod, PodSpec};
use kube_client::api::{Api, ListParams};
use rivers_k8s::crd::code_location::{
    CodeLocation, CodeLocationPhase, REASON_MIN_REPLICAS, REASON_NO_DEPLOYMENT_STATUS,
    REASON_PROGRESS_DEADLINE, REASON_ROLLING_OUT,
};
use rivers_k8s::crd::run::RunSource;
use rivers_k8s::workspace;

use super::status::source_display;
use crate::codelocation::resources::{MAIN_CONTAINER, deployment_name, labels};

/// Read the git Deployment and, until its rollout is complete, its pods,
/// without mutating anything — shared by the leader (post-apply) and
/// follower (status refresh) paths, so both publish the same status.
pub(super) async fn observe_git_deployment(
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
pub(super) fn template_source(deployment: &Deployment) -> Option<RunSource> {
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
pub(super) fn newest_build_failure(
    pods: &[Pod],
    template: &RunSource,
) -> Option<ContainerStateTerminated> {
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
pub(super) struct GitRollout {
    pub(super) template: Option<RunSource>,
    /// Every pod runs the current template and is ready.
    pub(super) complete: bool,
    /// `None` when the Deployment or its status could not be read.
    pub(super) ready_replicas: Option<i32>,
    pub(super) deadline_exceeded: bool,
    /// While the rollout is not complete: the newest failed build of the
    /// template's tree in its pods.
    pub(super) build_failure: Option<ContainerStateTerminated>,
}

pub(super) fn git_rollout(deployment: Option<&Deployment>) -> GitRollout {
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
pub(super) fn rollout_complete(deployment: &Deployment) -> bool {
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
pub(super) fn no_old_pods(deployment: &Deployment) -> bool {
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

/// Names the tree being rolled out and the tree runs keep meanwhile; `None`
/// when the rollout does not change the tree.
pub(super) fn rollout_message(
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
pub(super) fn build_failure_message(
    target: &RunSource,
    failed: &ContainerStateTerminated,
) -> String {
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

pub(super) fn evaluate_deployment_phase(
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

fn has_progress_deadline_exceeded(d: &DeploymentStatus) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codelocation::reconcile::tests::support::make_cl;

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
}
