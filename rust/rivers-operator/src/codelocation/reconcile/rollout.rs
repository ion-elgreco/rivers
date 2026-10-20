//! What the Deployment and its pods show: what the pod template runs, how
//! far it has rolled out, and a failed build or load of it.

use k8s_openapi::api::apps::v1::{Deployment, DeploymentStatus};
use k8s_openapi::api::core::v1::{
    Container, ContainerStateTerminated, ContainerStatus, Pod, PodSpec, PodStatus,
};
use kube_client::api::{Api, ListParams};
use rivers_k8s::crd::run::RunSource;
use rivers_k8s::workspace;

use super::status::source_display;
use crate::codelocation::resources::{MAIN_CONTAINER, labels};

/// What a code-location pod runs, by which a rollout tells the pods of the
/// current template from the old ones: the git tree, or in image mode the
/// image.
pub(super) trait Template: Clone + PartialEq + Default {
    fn of_pod(spec: &PodSpec) -> Option<Self>;

    /// How status messages name it, beside `other` in the same message.
    fn describe(&self, other: Option<&Self>) -> String;
}

/// The tree of a git code-location pod: its main container's
/// `RIVERS_RUN_SOURCE`.
impl Template for RunSource {
    fn of_pod(spec: &PodSpec) -> Option<Self> {
        let source = main_container(spec)?
            .env
            .as_ref()?
            .iter()
            .find(|e| e.name == rivers_k8s::env::ENV_RUN_SOURCE)?
            .value
            .as_deref()?;
        serde_json::from_str(source).ok()
    }

    fn describe(&self, other: Option<&Self>) -> String {
        let source = source_display(self);
        if other.is_some_and(|other| other.runtime_image != self.runtime_image) {
            format!("{source} on {}", self.runtime_image)
        } else {
            source
        }
    }
}

/// The image of an image-mode code-location pod's main container.
impl Template for String {
    fn of_pod(spec: &PodSpec) -> Option<Self> {
        main_container(spec)?.image.clone()
    }

    fn describe(&self, _other: Option<&Self>) -> String {
        self.clone()
    }
}

fn main_container(spec: &PodSpec) -> Option<&Container> {
    spec.containers.iter().find(|c| c.name == MAIN_CONTAINER)
}

/// Read code location `name`'s pods while `deployment`'s rollout is not
/// complete, without mutating anything — shared by the leader (post-apply)
/// and follower (status refresh) paths, so both publish the same status.
pub(super) async fn observe_deployment<T: Template>(
    name: &str,
    deployment: Option<&Deployment>,
    pods_api: &Api<Pod>,
) -> Rollout<T> {
    let mut rollout = deployment_rollout(deployment);
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
        Ok(pods) => {
            rollout.build_failure = newest_build_failure(&pods.items, template);
            rollout.load_failure = newest_load_failure(&pods.items, template);
        }
        Err(error) => tracing::warn!(
            code_location = name,
            %error,
            "could not list the code-location pods; their failures are not shown"
        ),
    }
    rollout
}

/// What `deployment`'s pod template runs.
pub(super) fn template_of<T: Template>(deployment: &Deployment) -> Option<T> {
    T::of_pod(deployment.spec.as_ref()?.template.spec.as_ref()?)
}

/// The newest failed run of the `workspace` init container among the pods
/// that run `template`. A container whose last run succeeded has none, even
/// if a run before it failed.
pub(super) fn newest_build_failure<T: Template>(
    pods: &[Pod],
    template: &T,
) -> Option<ContainerStateTerminated> {
    newest_failure(pods, template, |status| {
        status
            .init_container_statuses
            .as_deref()?
            .iter()
            .find(|c| c.name == workspace::SYNC_CONTAINER)
    })
}

/// The newest failed run of the main container among the pods that run
/// `template`: `rivers serve` exits when the module does not load.
pub(super) fn newest_load_failure<T: Template>(
    pods: &[Pod],
    template: &T,
) -> Option<ContainerStateTerminated> {
    newest_failure(pods, template, |status| {
        status
            .container_statuses
            .as_deref()?
            .iter()
            .find(|c| c.name == MAIN_CONTAINER)
    })
}

fn newest_failure<T: Template>(
    pods: &[Pod],
    template: &T,
    container: impl Fn(&PodStatus) -> Option<&ContainerStatus>,
) -> Option<ContainerStateTerminated> {
    pods.iter()
        .filter(|pod| pod.spec.as_ref().and_then(T::of_pod).as_ref() == Some(template))
        .filter_map(|pod| {
            let container = container(pod.status.as_ref()?)?;
            let last_run = match container.state.as_ref().and_then(|s| s.terminated.as_ref()) {
                Some(ended) => ended,
                None => container.last_state.as_ref()?.terminated.as_ref()?,
            };
            (last_run.exit_code != 0).then_some((last_run, pod.metadata.name.as_deref()))
        })
        .max_by_key(|(last_run, pod_name)| (last_run.finished_at.clone(), *pod_name))
        .map(|(last_run, _)| last_run.clone())
}

/// What the Deployment shows: what its pod template runs, and how far that
/// template has rolled out.
#[derive(Debug, Default)]
pub(super) struct Rollout<T> {
    pub(super) template: Option<T>,
    /// Every pod runs the current template and is ready.
    pub(super) complete: bool,
    /// `None` when the Deployment or its status could not be read.
    pub(super) ready_replicas: Option<i32>,
    pub(super) deadline_exceeded: bool,
    /// While the rollout is not complete: the newest failed build of the
    /// template's tree in its pods.
    pub(super) build_failure: Option<ContainerStateTerminated>,
    /// While the rollout is not complete: the newest exit of a pod of the
    /// template that could not load its code location.
    pub(super) load_failure: Option<ContainerStateTerminated>,
}

pub(super) type GitRollout = Rollout<RunSource>;

pub(super) fn deployment_rollout<T: Template>(deployment: Option<&Deployment>) -> Rollout<T> {
    let Some((deployment, status)) = deployment.and_then(|d| Some((d, d.status.as_ref()?))) else {
        return Rollout::default();
    };
    Rollout {
        template: template_of(deployment),
        complete: rollout_complete(deployment),
        ready_replicas: Some(status.ready_replicas.unwrap_or(0)),
        deadline_exceeded: generation_observed(deployment)
            && has_progress_deadline_exceeded(status),
        build_failure: None,
        load_failure: None,
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

/// Names what is being rolled out and what runs keep meanwhile; `None`
/// when the rollout does not change what runs get.
pub(super) fn rollout_message<T: Template>(
    target: Option<&T>,
    serving: Option<&T>,
    stuck: bool,
) -> Option<String> {
    let target = target.filter(|t| Some(*t) != serving)?;
    let rollout = if stuck {
        format!(
            "rollout of {} exceeded its progress deadline",
            target.describe(serving)
        )
    } else {
        format!("rolling out {}", target.describe(serving))
    };
    Some(match serving {
        Some(serving) => format!("{rollout}; runs use {}", serving.describe(Some(target))),
        None => rollout,
    })
}

/// `workspace build of main@9f3c1ab failed (Error, exit code 1): <its
/// termination message>`. No times or restart counts: every pass over the
/// same failure says the same, so it writes the status once.
pub(super) fn build_failure_message<T: Template>(
    target: &T,
    failed: &ContainerStateTerminated,
) -> String {
    failure_message(
        format!("workspace build of {} failed", target.describe(None)),
        failed,
    )
}

/// `code location main@9f3c1ab failed to load (Error, exit code 1): <the
/// end of its log>`, worded once per failure like [`build_failure_message`].
pub(super) fn load_failure_message<T: Template>(
    target: &T,
    failed: &ContainerStateTerminated,
) -> String {
    failure_message(
        format!("code location {} failed to load", target.describe(None)),
        failed,
    )
}

fn failure_message(what_failed: String, failed: &ContainerStateTerminated) -> String {
    let cause = match &failed.reason {
        Some(reason) => format!("{reason}, exit code {}", failed.exit_code),
        None => format!("exit code {}", failed.exit_code),
    };
    let summary = format!("{what_failed} ({cause})");
    match failed.message.as_deref().map(str::trim) {
        Some(said) if !said.is_empty() => format!("{summary}: {said}"),
        _ => summary,
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
