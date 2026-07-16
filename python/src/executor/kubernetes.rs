use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{EnvVar, Pod};
use pyo3::prelude::*;
use rivers_core::execution::compute::Compute;
use rivers_core::execution::retry::{
    FailureReason, RetryPolicy, compute_delay, meta as retry_meta, should_retry,
};
use rivers_core::storage::{EventType, StorageBackend};
use rivers_k8s::crd::code_location::CodeLocation;
use rivers_k8s::executor::StepJobOverrides;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::errors::ExecutionError;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::rt;

use super::dispatch::failure::{backoff_sleep_cancellable, rng01};
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

enum StepPollOutcome {
    Success,
    Failed {
        reason: FailureReason,
        /// MRO class names from the recorded StepFailure event; empty when
        /// the pod died without writing one (e.g. OOM kill).
        exc_types: Vec<String>,
    },
    Cancelled,
}

/// Terminal reason for a failed step pod. An OOM-killed pod dies before the
/// `execute-step` CLI can write any event, so the pod's container status is
/// the only signal (`OOMKilled` / exit 137).
async fn classify_failed_pod(
    client: &kube_client::Client,
    namespace: &str,
    job_name: &str,
) -> FailureReason {
    let pods: kube_client::Api<Pod> = kube_client::Api::namespaced(client.clone(), namespace);
    let lp = kube_client::api::ListParams::default().labels(&format!("job-name={job_name}"));
    match pods.list(&lp).await {
        Ok(list) => {
            for pod in &list.items {
                let statuses = pod
                    .status
                    .iter()
                    .flat_map(|s| s.container_statuses.iter().flatten());
                for cs in statuses {
                    let terminated = cs
                        .state
                        .as_ref()
                        .and_then(|s| s.terminated.as_ref())
                        .or_else(|| cs.last_state.as_ref().and_then(|s| s.terminated.as_ref()));
                    if let Some(t) = terminated {
                        if t.reason.as_deref() == Some("OOMKilled") || t.exit_code == 137 {
                            return FailureReason::OutOfMemory;
                        }
                        if t.reason.as_deref() == Some("DeadlineExceeded") {
                            return FailureReason::Timeout;
                        }
                        if t.exit_code != 0 {
                            return FailureReason::Error;
                        }
                    }
                }
            }
            FailureReason::Infrastructure
        }
        Err(e) => {
            tracing::warn!(
                target: "rivers::k8s",
                job = %job_name,
                error = %e,
                "failed to list pods for failure classification"
            );
            FailureReason::Infrastructure
        }
    }
}

/// Wait for the current attempt of a step to reach a terminal state.
///
/// Primary signal: the step pod's own events — any `StepSuccess` settles the
/// ladder (a step succeeds at most once), and a `StepFailure` beyond
/// `baseline_failures` (the count when this attempt started) is this attempt's
/// failure. Counting new-since-baseline rather than indexing by attempt number
/// matters because an OOM-killed pod writes *no* event. Fallback: the Job's
/// status — when the Job reports failed and no new event lands within a grace
/// window, classify from the pod instead of hanging forever.
async fn poll_step_attempt(
    client: &kube_client::Client,
    namespace: &str,
    storage: &Arc<rivers_core::storage::surrealdb_backend::SurrealStorage>,
    run_id: &str,
    poll_key: &str,
    job_name: &str,
    baseline_failures: usize,
) -> StepPollOutcome {
    const EVENT_GRACE_CYCLES: u32 = 3;
    let jobs_api: kube_client::Api<Job> = kube_client::Api::namespaced(client.clone(), namespace);
    let mut job_failed_cycles: u32 = 0;
    loop {
        match storage.get_events_for_step(run_id, poll_key).await {
            Ok(events) => {
                if events
                    .iter()
                    .any(|ev| matches!(ev.event_type, EventType::StepSuccess))
                {
                    return StepPollOutcome::Success;
                }
                // Step-level failures only — per-partition failure marks
                // (partition_key set) can land mid-attempt on a step that
                // still succeeds.
                let failures: Vec<_> = events
                    .iter()
                    .filter(|ev| {
                        matches!(ev.event_type, EventType::StepFailure)
                            && ev.partition_key.is_none()
                    })
                    .collect();
                if failures.len() > baseline_failures {
                    // The pod records a reason when it can; absent = ordinary
                    // user error.
                    let last = failures.last();
                    let reason = last
                        .and_then(|ev| {
                            ev.metadata
                                .iter()
                                .find(|(k, _)| k == retry_meta::REASON)
                                .and_then(|(_, v)| FailureReason::parse(v))
                        })
                        .unwrap_or(FailureReason::Error);
                    let exc_types = last
                        .and_then(|ev| {
                            ev.metadata
                                .iter()
                                .find(|(k, _)| k == retry_meta::EXC_TYPE)
                                .map(|(_, v)| retry_meta::decode_exc_types(v))
                        })
                        .unwrap_or_default();
                    return StepPollOutcome::Failed { reason, exc_types };
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "rivers::k8s",
                    step = %poll_key,
                    error = %e,
                    "failed to poll step events"
                );
            }
        }
        if storage.is_cancelled(run_id).await.unwrap_or(false) {
            tracing::info!(
                target: "rivers::k8s",
                step = %poll_key,
                "run cancellation detected, aborting step poll"
            );
            return StepPollOutcome::Cancelled;
        }
        match jobs_api.get_opt(job_name).await {
            Ok(Some(job)) => {
                let failed = job
                    .status
                    .as_ref()
                    .and_then(|s| s.failed)
                    .unwrap_or_default()
                    > 0;
                if failed {
                    job_failed_cycles += 1;
                    if job_failed_cycles >= EVENT_GRACE_CYCLES {
                        let reason = classify_failed_pod(client, namespace, job_name).await;
                        tracing::warn!(
                            target: "rivers::k8s",
                            step = %poll_key,
                            job = %job_name,
                            reason = reason.as_str(),
                            "step Job failed without a terminal event; classified from pod status"
                        );
                        return StepPollOutcome::Failed {
                            reason,
                            exc_types: vec![],
                        };
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    target: "rivers::k8s",
                    job = %job_name,
                    error = %e,
                    "failed to poll step Job status"
                );
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

struct StepJobSpec {
    instance_name: String,
    step_name: String,
    mapping_key: Option<String>,
    policy: Option<RetryPolicy>,
    /// Per-asset compute; axes left unset fall back to the executor's
    /// worker_cpu / worker_memory.
    compute: Option<Compute>,
}

/// Run one step instance to completion, re-creating its Job per retry attempt
/// (a finished K8s Job can't re-run, so attempt N gets a `-rN` name). On an
/// escalating reason the next attempt's pod gets grown resources. An
/// orchestrator restart resumes the ladder from the recorded `StepRetry`
/// events: their reasons replay the escalation chain and set the next attempt
/// number, so only the attempt actually in flight at the crash is re-polled
/// (409-adopting its Job).
async fn run_step_with_retries(
    client: kube_client::Client,
    namespace: String,
    storage: Arc<rivers_core::storage::surrealdb_backend::SurrealStorage>,
    run_id: String,
    code_location_id: String,
    config: Arc<rivers_k8s::executor::K8sStepExecutorConfig>,
    semaphore: Option<Arc<Semaphore>>,
    spec: StepJobSpec,
) -> (String, bool) {
    let executor_base = Compute {
        cpu: Some(config.worker_cpu.clone()),
        memory: Some(config.worker_memory.clone()),
        gpu: None,
    };
    let mut compute = spec
        .compute
        .as_ref()
        .map(|c| c.or_default(&executor_base))
        .unwrap_or(executor_base);
    let initial_events = storage
        .get_events_for_step(&run_id, &spec.instance_name)
        .await
        .unwrap_or_default();
    let recorded = retry_meta::recorded_ladder(
        initial_events
            .iter()
            .filter(|e| matches!(e.event_type, EventType::StepRetry))
            .map(|e| e.metadata.as_slice()),
    );
    for (_, reason) in &recorded {
        if let Some(esc) = spec.policy.as_ref().and_then(|p| p.escalate.as_ref()) {
            compute = rivers_k8s::compute::escalate_compute(&compute, esc, *reason);
        }
    }
    let mut attempt: u32 = recorded.len() as u32 + 1;
    let mut cached_events = Some(initial_events);
    loop {
        // One concurrency permit per attempt — released for the backoff sleep
        // so a waiting step can't starve its siblings.
        let permit = match &semaphore {
            Some(sem) => Some(
                sem.clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore not closed"),
            ),
            None => None,
        };
        // Failure events recorded before this attempt started; only a failure
        // beyond this count belongs to this attempt (an OOM-killed attempt
        // records none at all — the Job-status fallback covers it). Capped at
        // attempt-1 so a failure this attempt already recorded before an
        // orchestrator restart resolves the re-poll instantly.
        let events = match cached_events.take() {
            Some(evs) => evs,
            None => storage
                .get_events_for_step(&run_id, &spec.instance_name)
                .await
                .unwrap_or_default(),
        };
        let baseline_failures = events
            .iter()
            .filter(|e| {
                matches!(e.event_type, EventType::StepFailure) && e.partition_key.is_none()
            })
            .count()
            .min((attempt - 1) as usize);
        let overrides = StepJobOverrides {
            attempt,
            compute: Some(&compute),
        };
        let job = match &spec.mapping_key {
            Some(key) => rivers_k8s::executor::build_mapped_step_job_with(
                &config,
                &spec.step_name,
                key,
                &overrides,
            ),
            None => {
                rivers_k8s::executor::build_step_job_with(&config, &spec.instance_name, &overrides)
            }
        };
        let job_name = job.metadata.name.clone().unwrap_or_default();
        if let Err(e) = create_or_adopt_k8s_job(&client, &namespace, &job).await {
            tracing::error!(
                target: "rivers::k8s",
                step = %spec.instance_name,
                error = %e,
                "failed to create step Job"
            );
            return (spec.instance_name, false);
        }
        let outcome = poll_step_attempt(
            &client,
            &namespace,
            &storage,
            &run_id,
            &spec.instance_name,
            &job_name,
            baseline_failures,
        )
        .await;
        drop(permit);
        let (reason, exc_types) = match outcome {
            StepPollOutcome::Success => return (spec.instance_name, true),
            StepPollOutcome::Cancelled => return (spec.instance_name, false),
            StepPollOutcome::Failed { reason, exc_types } => (reason, exc_types),
        };
        let Some(policy) = &spec.policy else {
            return (spec.instance_name, false);
        };
        if !should_retry(policy, reason, &exc_types, attempt) {
            return (spec.instance_name, false);
        }
        if let Some(esc) = &policy.escalate {
            compute = rivers_k8s::compute::escalate_compute(&compute, esc, reason);
        }
        let delay = compute_delay(policy, attempt, rng01());
        let mut metadata = vec![
            (retry_meta::ATTEMPT.to_string(), attempt.to_string()),
            (retry_meta::REASON.to_string(), reason.as_str().to_string()),
            (
                retry_meta::NEXT_DELAY_MS.to_string(),
                delay.as_millis().to_string(),
            ),
        ];
        if let Ok(compute_json) = serde_json::to_string(&compute) {
            metadata.push((retry_meta::NEXT_COMPUTE.to_string(), compute_json));
        }
        let _ = storage
            .store_event(&rivers_core::storage::EventRecord {
                code_location_id: code_location_id.clone(),
                event_type: EventType::StepRetry,
                asset_key: Some(spec.instance_name.clone()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: rivers_core::util::now_ts(),
                metadata,
                input_data_versions: vec![],
            })
            .await;
        tracing::info!(
            target: "rivers::k8s",
            step = %spec.instance_name,
            attempt,
            reason = reason.as_str(),
            delay_ms = delay.as_millis() as u64,
            "step failed, retrying"
        );
        if backoff_sleep_cancellable(&storage, &run_id, delay).await {
            return (spec.instance_name, false);
        }
        attempt += 1;
    }
}

/// Run every step instance to completion, one monitor task each. Each retry
/// attempt holds one concurrency permit; backoff sleeps release it.
async fn run_jobs_to_completion(
    client: kube_client::Client,
    namespace: String,
    storage: Arc<rivers_core::storage::surrealdb_backend::SurrealStorage>,
    run_id: String,
    code_location_id: String,
    config: Arc<rivers_k8s::executor::K8sStepExecutorConfig>,
    max_concurrent: Option<usize>,
    specs: Vec<StepJobSpec>,
) -> Vec<(String, bool)> {
    let semaphore = max_concurrent.map(|n| Arc::new(Semaphore::new(n)));
    let mut join_set = JoinSet::new();

    for spec in specs {
        let client = client.clone();
        let namespace = namespace.clone();
        let storage = storage.clone();
        let run_id = run_id.clone();
        let code_location_id = code_location_id.clone();
        let config = Arc::clone(&config);
        let semaphore = semaphore.clone();

        join_set.spawn(async move {
            run_step_with_retries(
                client,
                namespace,
                storage,
                run_id,
                code_location_id,
                config,
                semaphore,
                spec,
            )
            .await
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
    results
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

        let specs: Vec<StepJobSpec> = instances
            .iter()
            .map(|inst| {
                let step = &ctx.scope.plan.steps[inst.idx];
                StepJobSpec {
                    instance_name: inst.instance_name.clone(),
                    policy: ctx.retry_policy_for(step),
                    compute: ctx.compute_for(step),
                    step_name: step.name.clone(),
                    mapping_key: inst.mapping_key.clone(),
                }
            })
            .collect();

        let namespace = self.namespace.clone();
        let max_concurrent = self.max_concurrent_steps;
        let storage = Arc::clone(ctx.sink.storage.backend());
        let run_id = ctx.scope.run_id.to_string();
        let code_location_id = self.code_location_id.clone();
        let client = self.client.clone();
        let config = Arc::new(config);

        let results: Vec<(String, bool)> = py.detach(move || {
            rt().block_on(run_jobs_to_completion(
                client,
                namespace,
                storage,
                run_id,
                code_location_id,
                config,
                max_concurrent,
                specs,
            ))
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
