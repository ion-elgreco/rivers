use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Pod, PodStatus, Secret};
use rivers_core::storage::StorageBackend;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::code_location::{CodeLocation, CodeLocationSpec};
use rivers_k8s::crd::run::{Run, RunCrdStatus, RunSource, RunSpec};

use super::reconcile::Context;
use crate::codelocation::DirectoryState;

pub async fn memory_storage() -> Arc<SurrealStorage> {
    Arc::new(SurrealStorage::new_memory().await.unwrap())
}

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub method: String,
    pub path: String,
    pub body: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct MockApiState {
    pub pods: BTreeMap<String, Pod>,
    pub code_locations: BTreeMap<String, CodeLocation>,
    /// An apply replaces only `spec`: the seeded `metadata.generation` and
    /// `status` stand for what the Deployment controller reports.
    pub deployments: BTreeMap<String, Deployment>,
    /// What a GET of each Secret answers: the Secret, or an API error. A
    /// Secret not listed is not found.
    pub secrets: BTreeMap<String, Result<Secret, kube_core::Status>>,
    /// What a LIST of Runs answers: the Runs, or an API error.
    pub runs: Result<Vec<Run>, kube_core::Status>,
    /// The ConfigMaps applied, by name.
    pub config_maps: BTreeMap<String, ConfigMap>,
    pub requests: Vec<ApiRequest>,
}

impl Default for MockApiState {
    /// Seeds a `demo` CodeLocation matching `test_run_spec`'s
    /// `code_location_ref.name`, so reconciler paths that fetch the CR
    /// succeed without per-test boilerplate.
    fn default() -> Self {
        let mut code_locations = BTreeMap::new();
        code_locations.insert(
            "demo".to_string(),
            CodeLocation::new("demo", CodeLocationSpec::default()),
        );
        Self {
            pods: BTreeMap::new(),
            code_locations,
            deployments: BTreeMap::new(),
            secrets: BTreeMap::new(),
            runs: Ok(Vec::new()),
            config_maps: BTreeMap::new(),
            requests: Vec::new(),
        }
    }
}

pub fn mock_client(state: Arc<Mutex<MockApiState>>) -> kube_client::Client {
    let service = tower::service_fn(move |req: Request<kube_client::client::Body>| {
        let state = state.clone();
        async move {
            let method = req.method().to_string();
            let path = req.uri().path().to_string();

            let body_bytes = req.into_body().collect().await.ok().map(|b| b.to_bytes());
            let body_json: Option<serde_json::Value> = body_bytes
                .as_ref()
                .and_then(|b| serde_json::from_slice(b).ok());

            let mut s = state.lock().unwrap();
            s.requests.push(ApiRequest {
                method: method.clone(),
                path: path.clone(),
                body: body_json.clone(),
            });

            let response = match method.as_str() {
                "GET" => {
                    if path.contains("/pods/") {
                        let pod_name = path.rsplit('/').next().unwrap_or("");
                        if let Some(pod) = s.pods.get(pod_name) {
                            json_response(200, &serde_json::to_value(pod).unwrap())
                        } else {
                            json_response(404, &not_found_status())
                        }
                    } else if path.contains("/codelocations/") {
                        let cl_name = path.rsplit('/').next().unwrap_or("");
                        if let Some(cl) = s.code_locations.get(cl_name) {
                            json_response(200, &serde_json::to_value(cl).unwrap())
                        } else {
                            json_response(404, &not_found_status())
                        }
                    } else if let Some(name) = object_name(&path, "deployments") {
                        match s.deployments.get(name) {
                            Some(d) => json_response(200, &serde_json::to_value(d).unwrap()),
                            None => json_response(404, &not_found_status()),
                        }
                    } else if let Some(name) = object_name(&path, "secrets") {
                        match s.secrets.get(name) {
                            Some(Ok(secret)) => {
                                json_response(200, &serde_json::to_value(secret).unwrap())
                            }
                            Some(Err(status)) => {
                                json_response(status.code, &serde_json::to_value(status).unwrap())
                            }
                            None => json_response(404, &not_found_status()),
                        }
                    } else if path.ends_with("/runs") {
                        match &s.runs {
                            Ok(runs) => json_response(
                                200,
                                &serde_json::json!({
                                    "apiVersion": "rivers.io/v1alpha1",
                                    "kind": "RunList",
                                    "metadata": {},
                                    "items": runs,
                                }),
                            ),
                            Err(status) => {
                                json_response(status.code, &serde_json::to_value(status).unwrap())
                            }
                        }
                    } else {
                        json_response(200, &serde_json::json!({}))
                    }
                }
                "POST" => {
                    let body = body_bytes.map(|b| b.to_vec()).unwrap_or_default();
                    Response::builder()
                        .status(201)
                        .header("content-type", "application/json")
                        .body(http_body_util::Full::new(Bytes::from(body)))
                        .unwrap()
                }
                "DELETE" => {
                    if path.contains("/pods/") {
                        let pod_name = path.rsplit('/').next().unwrap_or("");
                        s.pods.remove(pod_name);
                    }
                    json_response(
                        200,
                        &serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "metadata": {},
                            "status": "Success",
                            "code": 200
                        }),
                    )
                }
                "PATCH" if object_name(&path, "deployments").is_some() => {
                    let name = object_name(&path, "deployments").unwrap();
                    let applied: Option<Deployment> = body_json
                        .as_ref()
                        .and_then(|b| serde_json::from_value(b.clone()).ok());
                    let stored = s.deployments.entry(name.to_string()).or_default();
                    if let Some(applied) = applied {
                        stored.spec = applied.spec;
                    }
                    json_response(200, &serde_json::to_value(&*stored).unwrap())
                }
                "PATCH" if object_name(&path, "configmaps").is_some() => {
                    let name = object_name(&path, "configmaps").unwrap();
                    let applied: ConfigMap = body_json
                        .as_ref()
                        .and_then(|b| serde_json::from_value(b.clone()).ok())
                        .unwrap_or_default();
                    let response = json_response(200, &serde_json::to_value(&applied).unwrap());
                    s.config_maps.insert(name.to_string(), applied);
                    response
                }
                "PATCH" if path.contains("/services/") => {
                    json_response(200, body_json.as_ref().unwrap_or(&serde_json::json!({})))
                }
                "PATCH" if object_name(&path, "codelocations").is_some() => {
                    let name = object_name(&path, "codelocations").unwrap();
                    let status = body_json.as_ref().and_then(|b| b.get("status")).cloned();
                    match s.code_locations.get_mut(name) {
                        Some(cl) => {
                            if let Some(status) = status {
                                cl.status = serde_json::from_value(status).ok();
                            }
                            json_response(200, &serde_json::to_value(&*cl).unwrap())
                        }
                        None => json_response(404, &not_found_status()),
                    }
                }
                "PATCH" => {
                    let run_json = serde_json::json!({
                        "apiVersion": "rivers.io/v1alpha1",
                        "kind": "Run",
                        "metadata": {
                            "name": "test-run",
                            "namespace": "default",
                            "uid": "test-uid",
                            "resourceVersion": "1"
                        },
                        "spec": {"image": "img:v1", "target": "job"},
                        "status": {}
                    });
                    json_response(200, &run_json)
                }
                _ => json_response(200, &serde_json::json!({})),
            };

            Ok::<_, std::convert::Infallible>(response)
        }
    });

    kube_client::Client::new(service, "default")
}

/// `x` in `…/<plural>/x` or `…/<plural>/x/status`.
fn object_name<'a>(path: &'a str, plural: &str) -> Option<&'a str> {
    path.split(&format!("/{plural}/")).nth(1)?.split('/').next()
}

fn json_response(status: u16, body: &serde_json::Value) -> Response<http_body_util::Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(body).unwrap(),
        )))
        .unwrap()
}

fn not_found_status() -> serde_json::Value {
    serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": "not found",
        "reason": "NotFound",
        "code": 404
    })
}

const GIT_URL: &str = "https://forge.example/acme/pipelines.git";

/// `demo` as a git CodeLocation; `workspace_size` sets `spec.git.workspaceSize`.
pub fn git_code_location(workspace_size: Option<&str>) -> CodeLocation {
    let spec = serde_json::from_value(serde_json::json!({
        "git": {
            "url": GIT_URL,
            "ref": { "branch": "main" },
            "workspaceSize": workspace_size,
        },
    }))
    .unwrap();
    CodeLocation::new("demo", spec)
}

/// The source the admission webhook stamps on runs of [`git_code_location`].
pub fn git_run_source() -> RunSource {
    serde_json::from_value(serde_json::json!({
        "git": {
            "url": GIT_URL,
            "commit": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
            "ref": "refs/heads/main",
        },
        "dependencies": { "mode": "auto" },
        "runtimeImage": format!("ghcr.io/acme/rivers-runtime@sha256:{}", "1a2b3c4d".repeat(8)),
    }))
    .unwrap()
}

/// The body of the last pod create request.
pub fn created_pod(state: &MockApiState) -> Pod {
    let create = state
        .requests
        .iter()
        .rev()
        .find(|r| r.method == "POST" && r.path.ends_with("/pods"))
        .expect("no pod create request");
    serde_json::from_value(create.body.clone().expect("pod create has no body")).unwrap()
}

/// A git-mode pod's `emptyDir` cap: on its `workspace` volume, and in the
/// `RIVERS_WORKSPACE_EMPTYDIR_LIMIT` it hands to the step Jobs it launches.
pub fn emptydir_limits(pod: &Pod) -> (Option<String>, Option<String>) {
    let spec = pod.spec.as_ref().expect("pod has a spec");
    let volume = spec
        .volumes
        .iter()
        .flatten()
        .find(|v| v.name == rivers_k8s::workspace::WORKSPACE_VOLUME)
        .and_then(|v| v.empty_dir.as_ref()?.size_limit.as_ref())
        .map(|q| q.0.clone());
    let env = spec.containers[0]
        .env
        .iter()
        .flatten()
        .find(|e| e.name == rivers_k8s::env::ENV_WORKSPACE_EMPTYDIR_LIMIT)
        .and_then(|e| e.value.clone());
    (volume, env)
}

pub fn test_run_spec() -> RunSpec {
    serde_json::from_value(serde_json::json!({
        "codeLocationRef": { "name": "demo" },
        "image": "img:v1",
        "target": "job"
    }))
    .unwrap()
}

pub fn test_run_running(run_id: &str, completed_steps: Option<u32>) -> Run {
    let mut run = Run::new("test-run", test_run_spec());
    run.metadata.uid = Some("test-uid".to_string());
    run.metadata.namespace = Some("default".to_string());
    run.status = Some(RunCrdStatus {
        phase: Some(rivers_k8s::crd::run::RunPhase::Running),
        run_id: Some(run_id.to_string()),
        executor_pod: Some("test-run-executor".to_string()),
        started_at: Some(jiff::Timestamp::now().to_string()),
        completed_steps,
        ..Default::default()
    });
    run
}

pub fn test_run_cancelling(run_id: &str, cancelling_since: &str) -> Run {
    let mut run = Run::new("test-run", test_run_spec());
    run.metadata.uid = Some("test-uid".to_string());
    run.metadata.namespace = Some("default".to_string());
    run.status = Some(RunCrdStatus {
        phase: Some(rivers_k8s::crd::run::RunPhase::Cancelling),
        run_id: Some(run_id.to_string()),
        executor_pod: Some("test-run-executor".to_string()),
        started_at: Some(jiff::Timestamp::now().to_string()),
        conditions: vec![rivers_k8s::crd::run::RunCondition {
            r#type: rivers_k8s::crd::run::CONDITION_CANCELLING.to_string(),
            status: "True".to_string(),
            last_transition_time: Some(cancelling_since.to_string()),
            reason: Some("CancelRequested".to_string()),
            message: None,
        }],
        ..Default::default()
    });
    run
}

pub fn test_pod(name: &str, phase: &str) -> Pod {
    Pod {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn make_context(client: kube_client::Client, storage: Arc<SurrealStorage>) -> Context {
    Context {
        client,
        namespace: "default".to_string(),
        storage,
        directory: Arc::new(DirectoryState::new()),
        workspace: crate::codelocation::WorkspaceConfig::default(),
        surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig::default(),
        otel_pod_cfg: rivers_k8s::env::OtelPodConfig::default(),
    }
}

/// Create a run record in storage so update_run_status has something to update.
pub async fn seed_run_record(storage: &SurrealStorage, run_id: &str) {
    use rivers_core::storage::{DEFAULT_CODE_LOCATION_ID, LaunchedBy, RunRecord, RunStatus};
    storage
        .create_run(&RunRecord {
            run_id: run_id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test-job".to_string()),
            status: RunStatus::Started,
            start_time: jiff::Timestamp::now().as_nanosecond() as i64,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
}

pub fn last_status_patch(state: &MockApiState) -> RunCrdStatus {
    let patch = state
        .requests
        .iter()
        .filter(|r| r.method == "PATCH")
        .next_back()
        .expect("no PATCH request found");
    let body = patch.body.as_ref().expect("PATCH has no body");
    serde_json::from_value(body["status"].clone()).expect("failed to deserialize RunCrdStatus")
}

pub fn patch_count(state: &MockApiState) -> usize {
    state
        .requests
        .iter()
        .filter(|r| r.method == "PATCH")
        .count()
}
