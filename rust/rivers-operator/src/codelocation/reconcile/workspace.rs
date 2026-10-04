//! The git code location's workspace: the PVC, the keep-set and the objects
//! that build and serve a tree; their removal after a switch to image mode.

use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Service};
use kube_client::ResourceExt;
use kube_client::api::{Api, DeleteParams, Preconditions};
use rivers_k8s::crd::code_location::{
    CONDITION_WORKSPACE_KEPT, CodeLocation, REASON_ROLLING_OUT, REASON_RUNS_NOT_LISTED,
    REASON_RUNS_USE_WORKSPACE,
};
use rivers_k8s::crd::run::{GitCoordinates, Run, RunSource};
use rivers_k8s::workspace::{self, WorkspaceSpec};

use super::rollout::no_old_pods;
use super::status::is_git_status;
use super::{Context, Error, TERMINAL_RETRY, apply};
use crate::codelocation::git;
use crate::codelocation::resources::{
    build_deployment, build_keep_config_map, build_service, build_workspace_pvc, deployment_name,
    keep_config_map_name, service_name, workspace_pvc_name,
};

/// Image mode: delete the workspace PVC and keep ConfigMap that the code
/// location owns from an earlier git source, once no pod can mount the PVC.
/// Kubernetes deletes a PVC only when no pod mounts it, and the pods of a
/// run stay until the run is deleted, so the PVC stays for as long as
/// [`git_workspace_kept`] says. The keep ConfigMap stays with it: a return
/// to git before the run store syncs prunes the PVC by that keep-set.
/// Looks only while the status can still name such a workspace (a git
/// status, or `WorkspaceKept`); image mode keeps both until it is gone.
/// Returns why they stay, or `None` once nothing is left to delete.
pub(super) async fn remove_git_workspace(
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

/// What [`apply_git_workspace`] did.
pub(super) enum Applied {
    All,
    /// Nothing: the workspace PVC is being deleted.
    WaitingForPvc,
    /// The API server refused an object; the objects after it are not
    /// applied.
    Refused(ApplyFailure),
}

/// An object of a git code location that the API server refused.
pub(super) struct ApplyFailure {
    /// `applying Deployment 'x' failed: <the API server's message>`.
    pub(super) message: String,
    /// When the leader tries again.
    pub(super) retry: Duration,
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
pub(super) async fn apply_git_workspace(
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
        source: RunSource {
            git: GitCoordinates {
                url: git_spec.url.clone(),
                commit: resolved.commit.clone(),
                r#ref: resolved.ref_name.clone(),
                path: git_spec.path.clone(),
                secret_name: git_spec.secret_ref.as_ref().map(|s| s.name.clone()),
            },
            dependencies: git_spec.dependencies.clone(),
            runtime_image: resolved_image.to_string(),
        },
        volume: ctx.workspace.volume(&name, &cl.spec),
        extra_env: cl.spec.env.clone(),
    };
    let pieces = workspace::builder_pod_pieces(&wspec, &ctx.workspace.prune(&name));

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
                &wspec.source.workspace_key(),
                serving_key.as_deref(),
                namespace,
                &name,
                runs.iter().map(Arc::as_ref),
            );
            let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
            if let Err(e) = apply(&cm_api, &build_keep_config_map(cl, &keep)).await {
                let keep_name = keep_config_map_name(&name);
                return refused(&format!("applying ConfigMap '{keep_name}'"), e);
            }
        } else {
            tracing::info!("the Run store has not synced yet; the keep-set in force stays");
        }
    }

    let deployment = build_deployment(
        cl,
        resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
        &ctx.otel_pod_cfg,
        Some(&pieces),
    );
    let service = build_service(cl);
    let (deployed, served) = tokio::join!(
        apply(deployments_api, &deployment),
        apply(services_api, &service),
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

/// Keep-set for the prune step: the tree being rolled out (`target_key`),
/// the tree runs get until that rollout finishes (`serving_key`), plus the
/// tree of every non-terminal Run of the CL (`namespace`/`cl_name`) — queued
/// (`Pending`, or no status yet) and `Cancelling` included, not just
/// `Running`; a run waiting on a concurrency pool is precisely the one most
/// likely to sit through several commits. Keys, not commits: each run's tree
/// is keyed on its source's runtime image, whatever image its pods run.
/// Sorted + deduped so the ConfigMap value is deterministic and refreshes
/// don't churn.
pub(super) fn workspace_keep_csv<'a>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codelocation::reconcile::tests::support::{run_for, source_at};
    use rivers_k8s::crd::run::RunPhase;

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
}
