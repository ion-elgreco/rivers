use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube_client::Api;
use kube_client::api::{Patch, PatchParams, PostParams};
use rivers_core::run_backend::{RunBackend, RunHealthStatus};
use rivers_core::storage::CoordinatorRunInfo;
use serde_json::json;

use crate::crd::run::{
    CANCEL_ANNOTATION, CodeLocationRef, Executor, ResourceSpec, Run, RunPhase, RunSpec,
};

/// Backoff schedule for transient `Run` CR creation failures.
/// Covers the window where the operator's admission webhook Service may be
/// momentarily unreachable: pod restart, EndpointSlice/kube-proxy propagation,
/// brief network glitch. Total worst-case wait ~2.6s before surfacing the error.
const CREATE_RETRY_BACKOFFS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
];

/// Captures the image, env, and resource defaults stamped onto every `Run`
/// CR this daemon launches. Constructed once at daemon startup.
pub struct K8sRunBackendConfig {
    pub image: String,
    pub namespace: String,
    pub service_account: String,
    pub module: String,
    pub surreal_endpoint: String,
    pub default_run_cpu: String,
    pub default_run_memory: String,
    pub default_worker_cpu: String,
    pub default_worker_memory: String,
    pub labels: BTreeMap<String, String>,
    /// Name of the `CodeLocation` CR this daemon represents. Stamped into
    /// every `Run` CR it creates so the operator-hosted admission webhook
    /// can resolve image + module by digest. Empty string means "unknown" —
    /// production webhooks reject Runs missing the ref; only useful for
    /// local tests that bypass admission.
    pub code_location_name: String,
    /// Stable identity (UUID v4) of the code location, sourced from
    /// `RIVERS_CODE_LOCATION_ID` (operator-injected from
    /// `CodeLocation.spec.identity`). Falls back to `code_location_name`
    /// when the env var is unset (e.g. local single-CL dev). Stamped into
    /// both the `Run` CR's `codeLocationRef.identity` and every
    /// `RunRecord.code_location_id` row in storage.
    pub code_location_id: String,
}

/// Materializes runs as `Run` custom resources in Kubernetes. The operator's
/// Run controller reconciles those CRs into executor pods; this type just
/// creates / cancels / health-checks them.
pub struct K8sRunBackend {
    client: kube_client::Client,
    config: K8sRunBackendConfig,
}

impl K8sRunBackendConfig {
    fn cr_name(run_id: &str) -> String {
        format!("rivers-run-{run_id}")
    }

    fn build_run_cr(&self, run_info: &CoordinatorRunInfo) -> Run {
        let target = if run_info.node_names.is_empty() {
            "*".to_string()
        } else {
            run_info.node_names.join(",")
        };

        let mut labels = self.labels.clone();
        labels.insert("rivers.io/run-id".to_string(), run_info.run_id.clone());

        let partition_key = run_info.partition_key.as_ref().map(|pk| pk.to_json());

        Run {
            metadata: ObjectMeta {
                name: Some(Self::cr_name(&run_info.run_id)),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: RunSpec {
                code_location_ref: CodeLocationRef {
                    name: self.code_location_name.clone(),
                    identity: self.code_location_id.clone(),
                },
                image: self.image.clone(),
                module: self.module.clone(),
                target,
                surreal_endpoint: self.surreal_endpoint.clone(),
                executor: Executor::Kubernetes,
                parameters: None,
                partition_key,
                run_id: Some(run_info.run_id.clone()),
                timeout_seconds: None,
                max_restarts: crate::defaults::MAX_RESTARTS,
                cancel_grace_period_seconds: crate::defaults::CANCEL_GRACE_PERIOD,
                run_resources: ResourceSpec {
                    cpu: self.default_run_cpu.clone(),
                    memory: self.default_run_memory.clone(),
                },
                worker_resources: ResourceSpec {
                    cpu: self.default_worker_cpu.clone(),
                    memory: self.default_worker_memory.clone(),
                },
                max_concurrent_steps: crate::defaults::MAX_CONCURRENT_STEPS,
                service_account_name: self.service_account.clone(),
            },
            status: None,
        }
    }
}

impl K8sRunBackend {
    pub fn new(client: kube_client::Client, config: K8sRunBackendConfig) -> Self {
        Self { client, config }
    }
}

/// Only network/transport faults and 5xx responses qualify; admission
/// rejections (4xx) and decoding failures must surface immediately so the
/// daemon doesn't mask schema bugs.
fn is_transient_error(err: &kube_client::Error) -> bool {
    match err {
        kube_client::Error::Api(resp) => (500..=599).contains(&resp.code),
        kube_client::Error::HyperError(_) | kube_client::Error::Service(_) => true,
        _ => false,
    }
}

async fn retry_transient<F, Fut, T>(run_id: &str, mut op: F) -> Result<T, kube_client::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, kube_client::Error>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if is_transient_error(&e) && attempt < CREATE_RETRY_BACKOFFS.len() => {
                let delay = CREATE_RETRY_BACKOFFS[attempt];
                tracing::warn!(
                    target: "rivers::k8s",
                    run_id = %run_id,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "transient failure creating Run CR, retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

impl RunBackend for K8sRunBackend {
    async fn launch(
        &self,
        run_info: &CoordinatorRunInfo,
        _ctx: &(dyn std::any::Any + Send + Sync),
    ) -> Result<()> {
        let run_cr = self.config.build_run_cr(run_info);

        let runs_api: Api<Run> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let pp = PostParams::default();

        retry_transient(&run_info.run_id, || runs_api.create(&pp, &run_cr))
            .await
            .with_context(|| format!("failed to create Run CR for run {}", run_info.run_id))?;

        tracing::info!(
            target: "rivers::k8s",
            run_id = %run_info.run_id,
            cr_name = %K8sRunBackendConfig::cr_name(&run_info.run_id),
            namespace = %self.config.namespace,
            "created Run CR"
        );

        Ok(())
    }

    async fn terminate_run(&self, run_id: &str) -> Result<bool> {
        let runs_api: Api<Run> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let cr_name = K8sRunBackendConfig::cr_name(run_id);

        let patch = json!({
            "metadata": {
                "annotations": {
                    CANCEL_ANNOTATION: "true"
                }
            }
        });

        match runs_api
            .patch(
                &cr_name,
                &PatchParams::apply("rivers-daemon"),
                &Patch::Merge(&patch),
            )
            .await
        {
            Ok(_) => {
                tracing::info!(
                    target: "rivers::k8s",
                    run_id = %run_id,
                    cr_name = %cr_name,
                    "requested cancellation of Run CR"
                );
                Ok(true)
            }
            Err(kube_client::Error::Api(e)) if e.code == 404 => Ok(false),
            Err(e) => Err(e).context(format!("failed to cancel Run CR {cr_name}")),
        }
    }

    async fn check_run_health(&self, run_id: &str) -> Result<RunHealthStatus> {
        let runs_api: Api<Run> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let cr_name = K8sRunBackendConfig::cr_name(run_id);

        match runs_api.get_opt(&cr_name).await? {
            Some(run_cr) => {
                let phase = run_cr.status.as_ref().and_then(|s| s.phase.as_ref());

                match phase {
                    Some(p) if p.is_terminal() => Ok(RunHealthStatus::Exited),
                    Some(RunPhase::Running | RunPhase::Pending | RunPhase::Cancelling) => {
                        Ok(RunHealthStatus::Healthy)
                    }
                    Some(_) => Ok(RunHealthStatus::Healthy),
                    None => Ok(RunHealthStatus::Unknown("no status on CR".into())),
                }
            }
            None => Ok(RunHealthStatus::Missing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_core::storage::CoordinatorRunInfo;

    fn test_config() -> K8sRunBackendConfig {
        K8sRunBackendConfig {
            image: "myrepo/myimage:latest".to_string(),
            namespace: "rivers-test".to_string(),
            service_account: "rivers-executor".to_string(),
            module: "my_project.repo".to_string(),
            surreal_endpoint: "ws://surrealdb:8000".to_string(),
            default_run_cpu: "500m".to_string(),
            default_run_memory: "512Mi".to_string(),
            default_worker_cpu: "1".to_string(),
            default_worker_memory: "1Gi".to_string(),
            labels: BTreeMap::from([("app".to_string(), "rivers".to_string())]),
            code_location_name: "demo".to_string(),
            code_location_id: "demo".to_string(),
        }
    }

    fn test_run_info() -> CoordinatorRunInfo {
        CoordinatorRunInfo {
            run_id: "abc-123".to_string(),
            code_location_id: "demo".to_string(),
            tags: vec![("team".to_string(), "data".to_string())],
            node_names: vec!["asset_a".to_string(), "asset_b".to_string()],
            priority: 5,
            partition_key: None,
            start_time: 1000,
        }
    }

    #[test]
    fn build_cr_basic() {
        let run = test_config().build_run_cr(&test_run_info());

        assert_eq!(run.metadata.name, Some("rivers-run-abc-123".to_string()));
        assert_eq!(run.metadata.namespace, Some("rivers-test".to_string()));
        assert_eq!(
            run.metadata.labels,
            Some(BTreeMap::from([
                ("app".to_string(), "rivers".to_string()),
                ("rivers.io/run-id".to_string(), "abc-123".to_string()),
            ]))
        );
        assert_eq!(
            run.spec,
            RunSpec {
                code_location_ref: CodeLocationRef {
                    name: "demo".to_string(),
                    identity: "demo".to_string(),
                },
                image: "myrepo/myimage:latest".to_string(),
                module: "my_project.repo".to_string(),
                target: "asset_a,asset_b".to_string(),
                surreal_endpoint: "ws://surrealdb:8000".to_string(),
                executor: Executor::Kubernetes,
                parameters: None,
                partition_key: None,
                run_id: Some("abc-123".to_string()),
                timeout_seconds: None,
                max_restarts: crate::defaults::MAX_RESTARTS,
                cancel_grace_period_seconds: crate::defaults::CANCEL_GRACE_PERIOD,
                run_resources: ResourceSpec {
                    cpu: "500m".to_string(),
                    memory: "512Mi".to_string()
                },
                worker_resources: ResourceSpec {
                    cpu: "1".to_string(),
                    memory: "1Gi".to_string()
                },
                max_concurrent_steps: crate::defaults::MAX_CONCURRENT_STEPS,
                service_account_name: "rivers-executor".to_string(),
            }
        );
        assert!(run.status.is_none());
    }

    #[test]
    fn build_cr_with_single_partition_key() {
        use rivers_core::storage::PartitionKey;

        let mut info = test_run_info();
        info.partition_key = Some(PartitionKey::Single {
            keys: vec!["2024-01-15".to_string()],
        });

        let run = test_config().build_run_cr(&info);
        assert_eq!(
            run.spec.partition_key,
            Some(r#"{"single":["2024-01-15"]}"#.to_string())
        );
    }

    #[test]
    fn build_cr_with_multi_partition_key() {
        use rivers_core::storage::PartitionKey;

        let mut info = test_run_info();
        info.partition_key = Some(PartitionKey::Multi {
            dims: vec![
                ("region".to_string(), vec!["us".to_string()]),
                ("date".to_string(), vec!["2025-03".to_string()]),
            ],
        });

        let run = test_config().build_run_cr(&info);
        let pk = run.spec.partition_key.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&pk).unwrap();
        assert_eq!(parsed["multi"]["date"], serde_json::json!(["2025-03"]));
        assert_eq!(parsed["multi"]["region"], serde_json::json!(["us"]));
    }

    #[test]
    fn build_cr_wildcard_target_when_empty() {
        let mut info = test_run_info();
        info.node_names = vec![];

        let run = test_config().build_run_cr(&info);
        assert_eq!(run.spec.target, "*");
    }

    use kube_core::response::{Status, StatusSummary};
    use std::cell::Cell;

    fn api_err(code: u16) -> kube_client::Error {
        kube_client::Error::Api(Box::new(Status {
            status: Some(StatusSummary::Failure),
            code,
            message: format!("simulated {code}"),
            reason: "Simulated".to_string(),
            ..Default::default()
        }))
    }

    #[test]
    fn is_transient_error_classifies_5xx_as_transient() {
        assert!(is_transient_error(&api_err(500)));
        assert!(is_transient_error(&api_err(503)));
        assert!(is_transient_error(&api_err(599)));
    }

    #[test]
    fn is_transient_error_rejects_4xx() {
        // Admission rejections, validation failures, and not-found must fail fast.
        assert!(!is_transient_error(&api_err(400)));
        assert!(!is_transient_error(&api_err(404)));
        assert!(!is_transient_error(&api_err(409)));
        assert!(!is_transient_error(&api_err(422)));
    }

    #[test]
    fn is_transient_error_accepts_service_errors() {
        let svc: Box<dyn std::error::Error + Send + Sync> =
            Box::new(std::io::Error::other("connection refused"));
        assert!(is_transient_error(&kube_client::Error::Service(svc)));
    }

    #[test]
    fn is_transient_error_rejects_decoding_failures() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        assert!(!is_transient_error(&kube_client::Error::SerdeError(
            serde_err
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_succeeds_after_transient_errors() {
        let attempts = Cell::new(0u32);
        let result: Result<&str, kube_client::Error> = retry_transient("run-1", || {
            attempts.set(attempts.get() + 1);
            let n = attempts.get();
            async move {
                if n < 3 {
                    Err(api_err(503))
                } else {
                    Ok("created")
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), "created");
        assert_eq!(attempts.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_fails_fast_on_non_transient() {
        let attempts = Cell::new(0u32);
        let result: Result<(), kube_client::Error> = retry_transient("run-1", || {
            attempts.set(attempts.get() + 1);
            async { Err(api_err(422)) }
        })
        .await;
        assert!(matches!(result, Err(kube_client::Error::Api(ref r)) if r.code == 422));
        assert_eq!(attempts.get(), 1, "non-transient errors must not retry");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_after_max_attempts() {
        let attempts = Cell::new(0u32);
        let result: Result<(), kube_client::Error> = retry_transient("run-1", || {
            attempts.set(attempts.get() + 1);
            async { Err(api_err(503)) }
        })
        .await;
        assert!(result.is_err());
        // Initial attempt + one retry per backoff entry.
        assert_eq!(attempts.get() as usize, CREATE_RETRY_BACKOFFS.len() + 1);
    }
}
