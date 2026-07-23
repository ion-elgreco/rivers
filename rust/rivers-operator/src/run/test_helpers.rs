use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use rivers_core::storage::StorageBackend;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::code_location::{CodeLocation, CodeLocationSpec};
use rivers_k8s::crd::run::{Run, RunCrdStatus, RunSpec};

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
                body: body_json,
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
        started_at: Some(chrono::Utc::now().to_rfc3339()),
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
        started_at: Some(chrono::Utc::now().to_rfc3339()),
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
        surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig::default(),
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
            start_time: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
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
