use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::EnvVar;
use pyo3::prelude::*;
use rivers_core::storage::{EventType, StorageBackend};
use rivers_k8s::crd::code_location::CodeLocation;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::errors::ExecutionError;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::rt;

use super::dispatch::{BatchContext, ExecutorBackend, StepInstance};

const POLL_INTERVAL: Duration = Duration::from_secs(2);

const K8S_IN_MEMORY_IO_ERROR: &str = "uses InMemoryIOHandler which cannot work with Kubernetes \
     execution. InMemoryIOHandler stores data in process-local memory that is not \
     accessible from step pods. Use PickleIOHandler or DeltaIOHandler instead.";

fn validate_not_in_memory_io(py: Python, step_name: &str, node: &ResolvedNode) -> PyResult<()> {
    match node.io_handler(py) {
        None => Err(ExecutionError::new_err(format!(
            "Asset '{step_name}' {K8S_IN_MEMORY_IO_ERROR}"
        ))),
        Some(handler) => {
            let memory_cls = py
                .import("rivers.io_handlers.memory")?
                .getattr("InMemoryIOHandler")?;
            if handler.bind(py).is_instance(&memory_cls)? {
                return Err(ExecutionError::new_err(format!(
                    "Asset '{step_name}' {K8S_IN_MEMORY_IO_ERROR}"
                )));
            }
            Ok(())
        }
    }
}

pub(crate) struct KubernetesBackend {
    pub worker_image: Option<String>,
    pub max_concurrent_steps: Option<usize>,
    pub namespace: String,
    pub service_account: String,
    pub worker_cpu: String,
    pub worker_memory: String,
    pub module: String,
    /// SurrealDB endpoint, scope, and auth-secret coordinates stamped on
    /// every step pod. Read once from the run pod's own env (set by the
    /// operator) so step pods get the same `secretKeyRef` without
    /// round-tripping the password value through this process.
    pub surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig,
    pub run_cr_name: String,
    pub run_cr_uid: String,
    /// Resolved once via [`rivers_k8s::env::current_code_location_id`]
    /// (panics in cloud mode if the operator-injected env is missing).
    pub code_location_id: String,
    /// Structured envvars sourced from `CodeLocation.spec.env` via a one-shot
    /// kube API GET on first construction. Forwarded verbatim to step pods so
    /// `valueFrom.secretKeyRef` / `configMapKeyRef` / `fieldRef` semantics
    /// survive the orchestrator → step-pod hop.
    pub extra_env: Vec<EnvVar>,
    /// In-cluster kube client. `kube_client::Client` is internally `Arc`-y so
    /// cloning is cheap; one client per backend instance, reused across both
    /// Job creation and the CodeLocation lookup in [`fetch_code_location_env`].
    pub client: kube_client::Client,
}

/// Process-scoped kube client. `kube_client::Client` is internally `Arc`-y, so
/// every `KubernetesBackend` holds a cheap clone of the same underlying
/// connection pool — one `try_default()` per run pod, not per level batch.
fn shared_kube_client() -> kube_client::Client {
    static CLIENT: OnceLock<kube_client::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            rt().block_on(kube_client::Client::try_default())
                .expect("failed to construct in-cluster kube client")
        })
        .clone()
}

/// Look up `CodeLocation.spec.env` via kube API once per process and cache
/// the result. The run pod handles a single CodeLocation throughout its
/// lifetime, so a process-scoped cache is correct — multiple
/// `KubernetesBackend` instances (one per execution level) share one fetch.
///
/// Panics if the CR or its env can't be retrieved. `KubernetesBackend` only
/// ever runs inside a pod the operator created, where the CodeLocation env
/// vars are always set; a failure here is a config bug worth surfacing
/// immediately rather than running the workflow with silently-empty step env.
fn fetch_code_location_env(client: &kube_client::Client) -> Vec<EnvVar> {
    static CACHE: OnceLock<Vec<EnvVar>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let name = rivers_k8s::env::detect_code_location_name().expect(
                "RIVERS_CODE_LOCATION_NAME is required for KubernetesBackend; \
                 the operator stamps it on the run pod from the owning CodeLocation CR",
            );
            let namespace = rivers_k8s::env::detect_namespace();
            let api: kube_client::Api<CodeLocation> =
                kube_client::Api::namespaced(client.clone(), &namespace);
            rt().block_on(api.get(&name))
                .unwrap_or_else(|e| {
                    panic!("failed to fetch CodeLocation '{name}' in namespace '{namespace}': {e}")
                })
                .spec
                .env
        })
        .clone()
}

impl KubernetesBackend {
    pub fn new(
        worker_image: Option<String>,
        max_concurrent_steps: Option<usize>,
        namespace: String,
        service_account: String,
        worker_cpu: String,
        worker_memory: String,
    ) -> Self {
        let module = std::env::var("RIVERS_MODULE").unwrap_or_default();
        let surreal_pod_cfg = rivers_k8s::env::SurrealPodConfig::from_env();
        let run_cr_name = std::env::var("RIVERS_RUN_CR_NAME").unwrap_or_default();
        let run_cr_uid = std::env::var("RIVERS_RUN_CR_UID").unwrap_or_default();
        let code_location_id = rivers_k8s::env::current_code_location_id();
        let client = shared_kube_client();
        let extra_env = fetch_code_location_env(&client);

        Self {
            worker_image,
            max_concurrent_steps,
            namespace,
            service_account,
            worker_cpu,
            worker_memory,
            module,
            surreal_pod_cfg,
            run_cr_name,
            run_cr_uid,
            code_location_id,
            extra_env,
            client,
        }
    }

    fn build_config(
        &self,
        run_id: &str,
        partition_key: Option<String>,
    ) -> Result<rivers_k8s::executor::K8sStepExecutorConfig, PyErr> {
        let worker_image = self.worker_image.clone().ok_or_else(|| {
            crate::errors::ConfigurationError::new_err(
                "Kubernetes executor requires a worker image. Set RIVERS_CODE_LOCATION_IMAGE \
                 env var or pass worker_image to Executor.kubernetes()",
            )
        })?;

        Ok(rivers_k8s::executor::K8sStepExecutorConfig {
            worker_image,
            namespace: self.namespace.clone(),
            service_account: self.service_account.clone(),
            worker_cpu: self.worker_cpu.clone(),
            worker_memory: self.worker_memory.clone(),
            module: self.module.clone(),
            surreal_pod_cfg: self.surreal_pod_cfg.clone(),
            run_id: run_id.to_string(),
            run_cr_name: self.run_cr_name.clone(),
            run_cr_uid: self.run_cr_uid.clone(),
            code_location_id: self.code_location_id.clone(),
            extra_env: self.extra_env.clone(),
            partition_key,
        })
    }
}

async fn create_or_adopt_k8s_job(
    client: &kube_client::Client,
    namespace: &str,
    job: &Job,
) -> anyhow::Result<()> {
    let jobs_api: kube_client::Api<Job> = kube_client::Api::namespaced(client.clone(), namespace);
    let job_name = job.metadata.name.as_deref().unwrap_or("<unnamed>");
    match jobs_api
        .create(&kube_client::api::PostParams::default(), job)
        .await
    {
        Ok(_) => {
            tracing::info!(
                target: "rivers::k8s",
                job = %job_name,
                namespace = %namespace,
                "created step Job"
            );
        }
        Err(kube_client::Error::Api(ref e)) if e.code == 409 => {
            tracing::info!(
                target: "rivers::k8s",
                job = %job_name,
                "step Job already exists — adopting (resume)"
            );
        }
        Err(e) => {
            return Err(e).with_context(|| format!("failed to create step Job {job_name}"));
        }
    }
    Ok(())
}

async fn poll_step_completion(
    storage: &Arc<rivers_core::storage::surrealdb_backend::SurrealStorage>,
    run_id: &str,
    step_key: &str,
) -> bool {
    loop {
        match storage.get_events_for_step(run_id, step_key).await {
            Ok(events) => {
                for ev in &events {
                    if matches!(
                        ev.event_type,
                        EventType::StepSuccess | EventType::StepFailure
                    ) {
                        return matches!(ev.event_type, EventType::StepSuccess);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "rivers::k8s",
                    step = %step_key,
                    error = %e,
                    "failed to poll step events"
                );
            }
        }
        if storage.is_cancelled(run_id).await.unwrap_or(false) {
            tracing::info!(
                target: "rivers::k8s",
                step = %step_key,
                "run cancellation detected, aborting step poll"
            );
            return false;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Create the given step Jobs (adopting any that already exist) and poll each
/// one to completion. Returns per-job `(poll_key, success)` once all monitor
/// tasks have finished. Errors only on Job creation failure; per-step failures
/// surface via the bool flag.
async fn run_jobs_to_completion(
    client: kube_client::Client,
    namespace: &str,
    storage: Arc<rivers_core::storage::surrealdb_backend::SurrealStorage>,
    run_id: String,
    max_concurrent: Option<usize>,
    jobs: Vec<(String, Job)>,
) -> Result<Vec<(String, bool)>, String> {
    let semaphore = max_concurrent.map(|n| Arc::new(Semaphore::new(n)));
    let mut join_set = JoinSet::new();

    for (poll_key, job) in jobs {
        create_or_adopt_k8s_job(&client, namespace, &job)
            .await
            .map_err(|e| format!("failed to create step Job '{poll_key}': {e}"))?;

        let storage = storage.clone();
        let rid = run_id.clone();
        let permit = match semaphore {
            Some(ref sem) => Some(
                sem.clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore not closed"),
            ),
            None => None,
        };

        join_set.spawn(async move {
            let success = poll_step_completion(&storage, &rid, &poll_key).await;
            drop(permit);
            (poll_key, success)
        });
    }

    let mut results = Vec::new();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(r) => results.push(r),
            Err(e) => {
                tracing::error!(
                    target: "rivers::k8s",
                    error = %e,
                    "step monitor task panicked"
                );
            }
        }
    }
    Ok(results)
}

impl ExecutorBackend for KubernetesBackend {
    fn run_instances(
        &self,
        py: Python,
        ctx: &mut BatchContext,
        instances: Vec<StepInstance>,
        _max_concurrency: Option<usize>,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        for inst in &instances {
            let step = &ctx.scope.plan.steps[inst.idx];
            if let Some(node) = ctx.repo.node_map.get(&step.name)
                && let Err(e) = validate_not_in_memory_io(py, &step.name, node)
            {
                ctx.record_failure_no_hooks(&step.name, e, failures);
                return;
            }
        }

        let pk_string = ctx.scope.partition_key.as_ref().map(|pk| pk.to_json());
        let config = match self.build_config(ctx.scope.run_id, pk_string) {
            Ok(c) => c,
            Err(e) => {
                for inst in &instances {
                    ctx.record_failure_no_hooks(&inst.instance_name, e.clone_ref(py), failures);
                }
                return;
            }
        };

        let jobs: Vec<(String, Job)> = instances
            .iter()
            .map(|inst| {
                let job = if let Some(key) = &inst.mapping_key {
                    let step_name = &ctx.scope.plan.steps[inst.idx].name;
                    rivers_k8s::executor::build_mapped_step_job(&config, step_name, key)
                } else {
                    rivers_k8s::executor::build_step_job(&config, &inst.instance_name)
                };
                (inst.instance_name.clone(), job)
            })
            .collect();
        let instance_names_for_fallback: Vec<String> =
            instances.iter().map(|i| i.instance_name.clone()).collect();

        let namespace = self.namespace.clone();
        let max_concurrent = self.max_concurrent_steps;
        let storage = Arc::clone(ctx.sink.storage.backend());
        let run_id = ctx.scope.run_id.to_string();
        let client = self.client.clone();

        let results: Vec<(String, bool)> = py.detach(move || {
            rt().block_on(run_jobs_to_completion(
                client,
                &namespace,
                storage,
                run_id,
                max_concurrent,
                jobs,
            ))
            .unwrap_or_else(|e| {
                tracing::error!(target: "rivers::k8s", error = %e, "k8s execution init failed");
                instance_names_for_fallback
                    .iter()
                    .map(|n| (n.clone(), false))
                    .collect()
            })
        });

        // Step pods emit their own StepStart/StepSuccess/StepFailure events to
        // SurrealDB. The coordinator only needs to track failures for execution
        // plan progression — no event emission needed here.
        for (instance_name, success) in &results {
            if !success {
                ctx.state.mark_failed(instance_name.clone());
                failures.push((
                    instance_name.clone(),
                    ExecutionError::new_err(format!("K8s step pod failed for '{instance_name}'")),
                ));
            }
        }
    }
}
