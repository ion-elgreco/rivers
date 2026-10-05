//! Fixtures of the reconcile tests: a mock API server with a code location,
//! runs, pods and a Deployment; git hosts and registries on wiremock.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentStatus};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Secret};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_client::api::ObjectMeta;
use kube_runtime::controller::Action;
use kube_runtime::reflector::Store;
use kube_runtime::reflector::store::Writer;
use kube_runtime::watcher;
use rivers_k8s::crd::code_location::*;
use rivers_k8s::crd::run::{Run, RunCrdStatus, RunPhase, RunSource, RunSpec};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::codelocation::git;
use crate::codelocation::reconcile::rollout::GitRollout;
use crate::codelocation::reconcile::status::{GitResolution, GitStatusUpdate, git_status};
use crate::codelocation::reconcile::{Context, WorkspaceConfig, reconcile};
use crate::codelocation::registry::RegistryClient;
use crate::codelocation::resources::{
    MAIN_CONTAINER, build_keep_config_map, build_workspace_pvc, grpc_endpoint,
    keep_config_map_name, workspace_pvc_name,
};
use crate::leader::LeaderGate;
use crate::run::test_helpers::{ApiRequest, MockApiState, mock_client};

pub(crate) fn make_cl(spec: serde_json::Value) -> CodeLocation {
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

pub(crate) fn workspace_config(vars: &[(&str, &str)]) -> anyhow::Result<WorkspaceConfig> {
    WorkspaceConfig::from_lookup(&|k| {
        vars.iter()
            .find(|(name, _)| *name == k)
            .map(|(_, v)| v.to_string())
    })
}

/// `action`'s requeue delay, which `Action` shows only in its `Debug`.
pub(crate) fn requeue_after(action: &Action) -> Option<Duration> {
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
pub(crate) fn jittered(interval: Duration) -> std::ops::RangeInclusive<Duration> {
    interval..=interval + interval / 4
}

pub(crate) const KEEP_SET_RUNTIME: &str =
    "ghcr.io/rt@sha256:1a2b3c4d00000000000000000000000000000000000000000000000000000000";

pub(crate) fn source_at(commit_char: char) -> RunSource {
    serde_json::from_value(serde_json::json!({
        "git": {
            "url": "https://forge.example/r.git",
            "commit": commit_char.to_string().repeat(40),
        },
        "runtimeImage": KEEP_SET_RUNTIME,
    }))
    .unwrap()
}

pub(crate) fn run_for(cl: &str, commit_char: char, phase: Option<RunPhase>) -> Run {
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

pub(crate) const IDENTITY: (&str, &str) = ("identity", "PRIVATE KEY");
pub(crate) const KNOWN_HOSTS: (&str, &str) = ("known_hosts", "forge.example ssh-ed25519 AAAA");
pub(crate) const USERNAME: (&str, &str) = ("username", "ci-bot");
pub(crate) const PASSWORD: (&str, &str) = ("password", "forge-token");

pub(crate) fn secret(keys: &[(&str, &str)]) -> Secret {
    let data = keys
        .iter()
        .map(|(key, value)| (key.to_string(), ByteString(value.as_bytes().to_vec())))
        .collect();
    Secret {
        data: Some(data),
        ..Default::default()
    }
}

pub(crate) const RUNTIME: &str = "ghcr.io/ion-elgreco/rivers-runtime";

pub(crate) const URL: &str = "https://forge.example/r.git";

pub(crate) fn commit(c: char) -> String {
    c.to_string().repeat(40)
}

pub(crate) fn digest(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

pub(crate) fn runtime(c: char) -> String {
    format!("{RUNTIME}@{}", digest(c))
}

pub(crate) fn run_source_json(c: char) -> serde_json::Value {
    json!({
        "git": { "url": URL, "commit": commit(c) },
        "dependencies": { "mode": "auto" },
        "runtimeImage": runtime(c),
    })
}

/// Status of a git CL whose every pod runs commit `c` on runtime `c`.
pub(crate) fn serving_status(c: char) -> serde_json::Value {
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
pub(crate) fn git_cl_at(target: char, prior: serde_json::Value) -> CodeLocation {
    let mut cl = make_cl(json!({
        "git": { "url": URL, "ref": { "commit": commit(target) } },
        "digest": digest(target),
    }));
    cl.status = Some(serde_json::from_value(prior).unwrap());
    cl
}

pub(crate) fn deployment(generation: i64, status: DeploymentStatus) -> Deployment {
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
pub(crate) fn mid_rollout() -> Deployment {
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

pub(crate) fn rolled_out() -> Deployment {
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

pub(crate) fn condition<'a>(status: &'a serde_json::Value, kind: &str) -> &'a serde_json::Value {
    status["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == kind)
        .unwrap_or_else(|| panic!("no {kind} condition: {status}"))
}

pub(crate) type ApiState = Arc<std::sync::Mutex<MockApiState>>;

/// Mock API server holding `cl`, whose Deployment applies keep
/// `observed`'s generation and status.
pub(crate) fn api_with(cl: CodeLocation, observed: Deployment) -> ApiState {
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
pub(crate) fn resolver() -> Arc<git::GitResolver> {
    Arc::new(git::GitResolver::new(Duration::from_secs(5), true))
}

/// One reconcile pass over the stored CL, resolving refs with `git`;
/// returns its requeue and the API requests it made.
pub(crate) async fn reconcile_with(
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
pub(crate) async fn reconcile_in(
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
pub(crate) fn listing_run_store(runs: Vec<Run>) -> (Store<Run>, Writer<Run>) {
    let (store, mut writer) = kube_runtime::reflector::store();
    writer.apply_watcher_event(&watcher::Event::Init);
    for run in runs {
        writer.apply_watcher_event(&watcher::Event::InitApply(run));
    }
    (store, writer)
}

/// The run controller's store once its first LIST, `runs`, is in.
pub(crate) fn run_store(runs: Vec<Run>) -> Store<Run> {
    let (store, mut writer) = listing_run_store(runs);
    writer.apply_watcher_event(&watcher::Event::InitDone);
    store
}

/// The status `requests` patched, if any.
pub(crate) fn patched_status(requests: &[ApiRequest]) -> Option<serde_json::Value> {
    requests
        .iter()
        .rev()
        .find(|r| r.method == "PATCH" && r.path.ends_with("/codelocations/x/status"))
        .map(|r| r.body.as_ref().unwrap()["status"].clone())
}

/// One reconcile pass over the stored CL with a fresh resolver;
/// returns the status it patched, if any.
pub(crate) async fn pass(state: &ApiState, leader: LeaderGate) -> Option<serde_json::Value> {
    patched_status(&reconcile_with(state, leader, &resolver()).await.1)
}

pub(crate) async fn leader_pass(cl: CodeLocation, observed: Deployment) -> serde_json::Value {
    pass(&api_with(cl, observed), LeaderGate::leading())
        .await
        .expect("no status patch")
}

/// Commit `c` built with runtime digest `c`.
pub(crate) fn tree(c: char) -> RunSource {
    serde_json::from_value(run_source_json(c)).unwrap()
}

pub(crate) fn rollout(template: char, complete: bool, ready: i32) -> GitRollout {
    GitRollout {
        template: Some(tree(template)),
        complete,
        ready_replicas: Some(ready),
        deadline_exceeded: false,
        build_failure: None,
    }
}

/// The status a leader that resolved `target` publishes for `rollout`.
pub(crate) fn leader_status(
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
                fetched_at: None,
            }),
        }),
        rollout,
        apply_failure: None,
        endpoint: grpc_endpoint("x", "y", 3001),
    };
    git_status(cl, update, "2026-10-03T00:00:00Z".parse().unwrap())
}

pub(crate) fn available(status: &CodeLocationStatus) -> (&str, Option<&str>, Option<&str>) {
    let c = status
        .conditions
        .iter()
        .find(|c| c.r#type == CONDITION_DEPLOYMENT_AVAILABLE)
        .expect("DeploymentAvailable condition");
    (&c.status, c.reason.as_deref(), c.message.as_deref())
}

pub(crate) fn shared() -> WorkspaceConfig {
    WorkspaceConfig {
        shared_enabled: true,
        ..Default::default()
    }
}

/// The keep ConfigMap's value for `keys`.
pub(crate) fn keep_value(mut keys: Vec<String>) -> String {
    keys.sort();
    keys.join(",")
}

pub(crate) fn keep_in(state: &ApiState) -> String {
    state.lock().unwrap().config_maps[&keep_config_map_name("x")]
        .data
        .as_ref()
        .unwrap()["keep"]
        .clone()
}

/// Commit b rolls out while tree a serves and [`long_run`] still uses
/// tree c, outside the floors. The keep-set in force is from a pass
/// before commit b.
pub(crate) fn rolling_out_beside_a_long_run() -> ApiState {
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

pub(crate) fn long_run() -> Run {
    run_for("x", 'c', Some(RunPhase::Running))
}

/// The requests of `requests` that LIST Runs.
pub(crate) fn runs_lists(requests: &[ApiRequest]) -> Vec<&ApiRequest> {
    requests
        .iter()
        .filter(|r| r.method == "GET" && r.path.ends_with("/runs"))
        .collect()
}

pub(crate) const STALE_LOCK: &str = "error: The lockfile at `uv.lock` needs to be updated, but \
                          `--locked` was provided. To update the lockfile, run `uv lock`.";

/// A pod of code location x that runs `tree`; `sync` is the status of
/// its `workspace` init container.
pub(crate) fn code_location_pod(name: &str, tree: &RunSource, sync: serde_json::Value) -> Pod {
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
pub(crate) fn failed_sync(message: &str, finished_at: &str) -> serde_json::Value {
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

pub(crate) fn completed_sync() -> serde_json::Value {
    json!({ "state": { "terminated": {
        "exitCode": 0,
        "reason": "Completed",
        "finishedAt": "2026-10-02T00:00:00Z",
    } } })
}

pub(crate) fn add_pods(state: &ApiState, pods: impl IntoIterator<Item = Pod>) {
    let mut s = state.lock().unwrap();
    for pod in pods {
        s.pods.insert(pod.metadata.name.clone().unwrap(), pod);
    }
}

/// What the API server answers, with `code`, for a Deployment whose
/// `emptyDir` size it cannot read.
pub(crate) fn unreadable_size(code: u16) -> kube_core::Status {
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
pub(crate) fn sized_5gb(mut cl: CodeLocation) -> CodeLocation {
    cl.spec.git.as_mut().unwrap().workspace_size = Some(Quantity("5GB".into()));
    cl
}

pub(crate) fn refuse(state: &ApiState, object: &str, answer: &kube_core::Status) {
    let mut s = state.lock().unwrap();
    s.refused.insert(object.to_string(), answer.clone());
}

pub(crate) const PIPELINE: &str = "ghcr.io/acme/pipeline";

/// What code location x published while main (commit a) served it
/// from git.
pub(crate) fn git_era_status() -> serde_json::Value {
    let mut status = serving_status('a');
    status["runSource"]["git"]["ref"] = json!("refs/heads/main");
    status["resolvedRef"] = json!("refs/heads/main");
    status["lastFetchedAt"] = json!("2026-10-02T00:00:00Z");
    status["source"] = json!(format!("main@{}", &commit('a')[..7]));
    status
}

/// What image mode publishes for [`image_cl`] once its pods run the
/// image.
pub(crate) fn image_status() -> serde_json::Value {
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
pub(crate) fn image_cl(prior: serde_json::Value) -> CodeLocation {
    let mut cl = make_cl(json!({ "image": PIPELINE, "digest": digest('i') }));
    cl.status = Some(serde_json::from_value(prior).unwrap());
    cl
}

pub(crate) const GIT_FIELDS: [&str; 4] = [
    "resolvedCommit",
    "resolvedRef",
    "lastFetchedAt",
    "runSource",
];

/// Code location x in image mode on `acme/runtime:latest` of
/// `registry`, with `prior` as its status.
pub(crate) fn image_cl_on(registry: &MockServer, prior: serde_json::Value) -> CodeLocation {
    let mut cl = make_cl(json!({
        "image": format!("{}/acme/runtime", registry.address()),
        "tag": "latest",
    }));
    cl.status = Some(serde_json::from_value(prior).unwrap());
    cl
}

/// The requests of `requests` that write.
pub(crate) fn writes(requests: &[ApiRequest]) -> Vec<(&str, &str)> {
    requests
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| (r.method.as_str(), r.path.as_str()))
        .collect()
}

pub(crate) fn main_image(state: &ApiState) -> Option<String> {
    state.lock().unwrap().deployments["x"]
        .spec
        .clone()
        .and_then(|d| d.template.spec)
        .map(|pod| pod.containers[0].image.clone())?
}

/// [`image_status`] after a pass that kept x's git workspace.
pub(crate) fn image_status_keeping_the_workspace() -> serde_json::Value {
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
pub(crate) fn stored_status(state: &ApiState) -> serde_json::Value {
    serde_json::to_value(&state.lock().unwrap().code_locations["x"]).unwrap()["status"].clone()
}

pub(crate) const PVC_UID: &str = "pvc-uid";

pub(crate) const KEEP_UID: &str = "keep-uid";

/// The workspace PVC and keep ConfigMap that the git path made for
/// `owner`.
pub(crate) fn git_workspace_of(owner: &CodeLocation) -> (PersistentVolumeClaim, ConfigMap) {
    let mut pvc = build_workspace_pvc(owner, &Quantity("20Gi".into()), None);
    pvc.metadata.uid = Some(PVC_UID.into());
    let mut keep = build_keep_config_map(owner, &tree('a').workspace_key());
    keep.metadata.uid = Some(KEEP_UID.into());
    (pvc, keep)
}

pub(crate) fn add_git_workspace(state: &ApiState, owner: &CodeLocation) {
    let (pvc, keep) = git_workspace_of(owner);
    let mut s = state.lock().unwrap();
    s.pvcs.insert(workspace_pvc_name("x"), pvc);
    s.config_maps.insert(keep_config_map_name("x"), keep);
}

/// The objects `requests` deleted, with the uid each DELETE required.
pub(crate) fn deleted(requests: &[ApiRequest]) -> Vec<(&str, Option<&str>)> {
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

pub(crate) const PVC_PATH: &str = "/api/v1/namespaces/y/persistentvolumeclaims/x-workspace";

pub(crate) const KEEP_PATH: &str = "/api/v1/namespaces/y/configmaps/x-workspace-keep";

/// Whether `state` still holds x's workspace PVC and keep ConfigMap.
pub(crate) fn git_workspace_in(state: &ApiState) -> (bool, bool) {
    let s = state.lock().unwrap();
    (
        s.pvcs.contains_key(&workspace_pvc_name("x")),
        s.config_maps.contains_key(&keep_config_map_name("x")),
    )
}

/// The status, reason and message of `status`'s `WorkspaceKept`
/// condition, if it has one.
pub(crate) fn workspace_kept(status: &serde_json::Value) -> Option<(&str, &str, &str)> {
    status["conditions"]
        .as_array()?
        .iter()
        .any(|c| c["type"] == CONDITION_WORKSPACE_KEPT)
        .then(|| says(status, CONDITION_WORKSPACE_KEPT))
}

/// A run of x that runs `image` without a git source.
pub(crate) fn image_run(name: &str, phase: RunPhase) -> Run {
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

pub(crate) const REPO: &str = "/acme/pipelines.git";

/// A server answering `verb` requests for `at` with `responses` in
/// turn, the last one from then on.
pub(crate) async fn answering(
    verb: &str,
    at: &str,
    responses: Vec<ResponseTemplate>,
) -> MockServer {
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
pub(crate) async fn forge(responses: Vec<ResponseTemplate>) -> MockServer {
    answering("GET", &format!("{REPO}/info/refs"), responses).await
}

/// The ref advertisement: `refs/heads/main` is commit `a`.
pub(crate) fn advertisement() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header(
            "content-type",
            "application/x-git-upload-pack-advertisement",
        )
        .set_body_bytes(git::fixtures::http_adv())
}

pub(crate) fn unreachable_error(forge: &MockServer, status: &str) -> String {
    format!(
        "git host unreachable: HTTP {status} from {}{REPO}/info/refs?service=git-upload-pack",
        forge.uri()
    )
}

/// Git CL tracking `main` on `forge`, on runtime digest `a`.
pub(crate) fn branch_cl(forge: &MockServer) -> CodeLocation {
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
pub(crate) struct Outage {
    pub(crate) forge: MockServer,
    pub(crate) state: ApiState,
    pub(crate) git: Arc<git::GitResolver>,
    /// The status before the outage.
    pub(crate) serving: serde_json::Value,
    pub(crate) requeue: Action,
    pub(crate) requests: Vec<ApiRequest>,
}

pub(crate) async fn outage(responses: Vec<ResponseTemplate>) -> Outage {
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
pub(crate) fn says<'a>(status: &'a serde_json::Value, kind: &str) -> (&'a str, &'a str, &'a str) {
    let c = condition(status, kind);
    let field = |name: &str| c[name].as_str().unwrap_or_default();
    (field("status"), field("reason"), field("message"))
}

pub(crate) fn source_resolved(status: &serde_json::Value) -> (&str, &str, &str) {
    says(status, CONDITION_SOURCE_RESOLVED)
}

/// The `lastFetchedAt` of `status`.
pub(crate) fn fetched_at(status: &serde_json::Value) -> jiff::Timestamp {
    status["lastFetchedAt"]
        .as_str()
        .unwrap_or_else(|| panic!("no lastFetchedAt: {status}"))
        .parse()
        .unwrap()
}

/// A registry answering HEADs of `acme/runtime:latest` with
/// `responses` in turn, the last one from then on.
pub(crate) async fn registry(responses: Vec<ResponseTemplate>) -> MockServer {
    answering("HEAD", "/v2/acme/runtime/manifests/latest", responses).await
}

/// The registry's answer: `latest` is digest `c`.
pub(crate) fn manifest(c: char) -> ResponseTemplate {
    ResponseTemplate::new(200).insert_header("docker-content-digest", digest(c))
}

/// Git CL pinned to commit a on `acme/runtime:latest` of `registry`,
/// refreshed every 5m.
pub(crate) fn latest_runtime_cl(registry: &MockServer) -> CodeLocation {
    make_cl(json!({
        "git": { "url": URL, "ref": { "commit": commit('a') } },
        "image": format!("{}/acme/runtime", registry.address()),
        "tag": "latest",
        "digestRefreshInterval": "5m",
    }))
}

/// The leader's first pass after the registry started answering
/// with `responses`: before that, digest b of `latest` rolled out
/// with commit a.
pub(crate) struct RegistryOutage {
    pub(crate) state: ApiState,
    /// The status before the outage.
    pub(crate) serving: serde_json::Value,
    pub(crate) requeue: Action,
    pub(crate) requests: Vec<ApiRequest>,
}

pub(crate) async fn registry_outage(responses: Vec<ResponseTemplate>) -> RegistryOutage {
    let mut answers = vec![manifest('b')];
    answers.extend(responses);
    let registry = registry(answers).await;
    let state = api_with(latest_runtime_cl(&registry), rolled_out());
    let serving = pass(&state, LeaderGate::leading())
        .await
        .expect("rollout of digest b");
    assert_eq!(serving["phase"], "Ready", "{serving}");
    assert_eq!(says(&serving, CONDITION_IMAGE_RESOLVED).0, "True");

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;
    RegistryOutage {
        state,
        serving,
        requeue,
        requests,
    }
}
