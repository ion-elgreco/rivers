use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

/// Per-step pod template inputs used by [`build_step_job`]. The resulting
/// `Job` runs `rivers execute-step` in a container that inherits resources
/// and env from the parent CodeLocation.
pub struct K8sStepExecutorConfig {
    pub worker_image: String,
    pub namespace: String,
    pub service_account: String,
    pub worker_cpu: String,
    pub worker_memory: String,
    pub module: String,
    /// SurrealDB endpoint, scope and auth-secret coordinates stamped on the
    /// step pod. `auth_secret` is read by coordinate (not by value) so the
    /// step Job's `valueFrom.secretKeyRef` references the same Secret the
    /// rest of the rivers pods use — the run pod never holds the password
    /// in memory.
    pub surreal_pod_cfg: crate::env::SurrealPodConfig,
    pub run_id: String,
    pub run_cr_name: String,
    pub run_cr_uid: String,
    /// Owning CodeLocation identity (UUID), stamped on every step pod as
    /// `RIVERS_CODE_LOCATION_ID` so per-step writes share the executor's scope.
    pub code_location_id: String,
    /// Structured envvars sourced from `CodeLocation.spec.env`. Forwarded
    /// verbatim onto every step pod's container so `valueFrom.secretKeyRef` /
    /// `configMapKeyRef` / `fieldRef` semantics are preserved end-to-end.
    pub extra_env: Vec<EnvVar>,
    pub partition_key: Option<String>,
}

/// Sanitize a string into a valid K8s label value:
/// - lowercased
/// - max 63 characters
/// - only alphanumerics, `-`, `_`, `.` (other chars replaced with `-`)
/// - must start and end with an alphanumeric character
fn sanitize_k8s_label(s: &str) -> String {
    let sanitized: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .to_lowercase();
    let trimmed = sanitized.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    if trimmed.len() > 63 {
        trimmed[..63]
            .trim_end_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_string()
    } else {
        trimmed.to_string()
    }
}

fn short_hash(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

fn step_job_name(run_id: &str, step_name: &str) -> String {
    format!("rivers-step-{run_id}-{}", short_hash(step_name))
}

fn mapped_step_job_name(run_id: &str, step_name: &str, mapping_key: &str) -> String {
    let key = format!("{step_name}/{mapping_key}");
    format!("rivers-step-{run_id}-{}", short_hash(&key))
}

/// Per-attempt knobs for a step Job: retry attempts get a distinct Job name
/// (a finished K8s Job can't be re-run) and possibly escalated resources.
pub struct StepJobOverrides<'a> {
    /// 1-indexed attempt; attempts > 1 append `-r<n>` to the Job name.
    pub attempt: u32,
    /// Replaces the config's worker cpu/memory per set axis; `gpu` adds
    /// `nvidia.com/gpu` to requests and limits.
    pub compute: Option<&'a rivers_core::execution::compute::Compute>,
}

impl Default for StepJobOverrides<'_> {
    fn default() -> Self {
        Self {
            attempt: 1,
            compute: None,
        }
    }
}

/// Job name is `rivers-step-<run_id>-<8-char-hash(step_name)>` to stay
/// within K8s' 63-char resource-name limit. Owned by the parent Run pod via
/// OwnerReference so kube garbage-collects on Run delete.
pub fn build_step_job(config: &K8sStepExecutorConfig, step_name: &str) -> Job {
    let keys = [step_name.to_string()];
    build_step_job_with(config, step_name, &keys, &StepJobOverrides::default())
}

/// `step_keys` become the pod's `--step-key` args — a multi-asset step passes
/// every output so the pod materializes the whole unit, not one selection.
pub fn build_step_job_with(
    config: &K8sStepExecutorConfig,
    step_name: &str,
    step_keys: &[String],
    overrides: &StepJobOverrides,
) -> Job {
    let job_name = attempt_name(step_job_name(&config.run_id, step_name), overrides.attempt);
    build_job_inner(config, &job_name, step_name, step_keys, None, overrides)
}

/// One mapping shard of a fan-out step. Job name hashes
/// `<step_name>/<mapping_key>` so concurrent shards stay distinct while
/// still fitting K8s' name limit.
pub fn build_mapped_step_job(
    config: &K8sStepExecutorConfig,
    step_name: &str,
    mapping_key: &str,
) -> Job {
    build_mapped_step_job_with(config, step_name, mapping_key, &StepJobOverrides::default())
}

pub fn build_mapped_step_job_with(
    config: &K8sStepExecutorConfig,
    step_name: &str,
    mapping_key: &str,
    overrides: &StepJobOverrides,
) -> Job {
    let job_name = attempt_name(
        mapped_step_job_name(&config.run_id, step_name, mapping_key),
        overrides.attempt,
    );
    let keys = [step_name.to_string()];
    build_job_inner(
        config,
        &job_name,
        step_name,
        &keys,
        Some(mapping_key),
        overrides,
    )
}

fn attempt_name(base: String, attempt: u32) -> String {
    if attempt > 1 {
        format!("{base}-r{attempt}")
    } else {
        base
    }
}

fn build_job_inner(
    config: &K8sStepExecutorConfig,
    job_name: &str,
    step_name: &str,
    step_keys: &[String],
    mapping_key: Option<&str>,
    overrides: &StepJobOverrides,
) -> Job {
    let mut labels = BTreeMap::from([
        ("rivers.io/run-id".to_string(), config.run_id.clone()),
        ("rivers.io/step".to_string(), sanitize_k8s_label(step_name)),
        ("rivers.io/component".to_string(), "step-worker".to_string()),
    ]);
    if let Some(key) = mapping_key {
        labels.insert("rivers.io/mapping-key".to_string(), sanitize_k8s_label(key));
    }

    let mut args = vec![
        "execute-step".to_string(),
        config.module.clone(),
        "--run-id".to_string(),
        config.run_id.clone(),
    ];
    for key in step_keys {
        args.extend(["--step-key".to_string(), key.clone()]);
    }
    if let Some(ref pk) = config.partition_key {
        args.extend(["--partition-key".to_string(), pk.clone()]);
    }
    if let Some(key) = mapping_key {
        args.extend(["--mapping-key".to_string(), key.to_string()]);
    }

    Job {
        metadata: ObjectMeta {
            name: Some(job_name.to_string()),
            namespace: Some(config.namespace.clone()),
            labels: Some(labels),
            owner_references: Some(vec![OwnerReference {
                api_version: "rivers.io/v1alpha1".to_string(),
                kind: "Run".to_string(),
                name: config.run_cr_name.clone(),
                uid: config.run_cr_uid.clone(),
                controller: Some(false),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(0),
            ttl_seconds_after_finished: Some(600),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([
                        ("rivers.io/run-id".to_string(), config.run_id.clone()),
                        ("rivers.io/step".to_string(), sanitize_k8s_label(step_name)),
                        ("rivers.io/component".to_string(), "step-worker".to_string()),
                    ])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(config.service_account.clone()),
                    restart_policy: Some("Never".to_string()),
                    containers: vec![Container {
                        name: "step".to_string(),
                        image: Some(config.worker_image.clone()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        command: Some(vec!["rivers".to_string()]),
                        args: Some(args),
                        resources: Some({
                            let cpu = overrides
                                .compute
                                .and_then(|c| c.cpu.clone())
                                .unwrap_or_else(|| config.worker_cpu.clone());
                            let memory = overrides
                                .compute
                                .and_then(|c| c.memory.clone())
                                .unwrap_or_else(|| config.worker_memory.clone());
                            let mut quantities = BTreeMap::from([
                                ("cpu".to_string(), Quantity(cpu)),
                                ("memory".to_string(), Quantity(memory)),
                            ]);
                            if let Some(gpu) = overrides.compute.and_then(|c| c.gpu.clone()) {
                                quantities.insert("nvidia.com/gpu".to_string(), Quantity(gpu));
                            }
                            ResourceRequirements {
                                requests: Some(quantities.clone()),
                                limits: Some(quantities),
                                ..Default::default()
                            }
                        }),
                        env: Some({
                            let mut env = vec![
                                EnvVar {
                                    name: "RIVERS_STEP_POD".to_string(),
                                    value: Some("1".to_string()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_STEP_ATTEMPT".to_string(),
                                    value: Some(overrides.attempt.to_string()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_RUN_ID".to_string(),
                                    value: Some(config.run_id.clone()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_CODE_LOCATION_IMAGE".to_string(),
                                    value: Some(config.worker_image.clone()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_CODE_LOCATION_ID".to_string(),
                                    value: Some(config.code_location_id.clone()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_NAMESPACE".to_string(),
                                    value: Some(config.namespace.clone()),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "RIVERS_MODULE".to_string(),
                                    value: Some(config.module.clone()),
                                    ..Default::default()
                                },
                            ];
                            env.extend(crate::env::build_surreal_pod_env(&config.surreal_pod_cfg));
                            env.extend(config.extra_env.iter().cloned());
                            env
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> K8sStepExecutorConfig {
        K8sStepExecutorConfig {
            worker_image: "registry.example.com/my-repo:latest".to_string(),
            namespace: "rivers-test".to_string(),
            service_account: "rivers-step-worker".to_string(),
            worker_cpu: "1".to_string(),
            worker_memory: "2Gi".to_string(),
            module: "my_project.definitions".to_string(),
            surreal_pod_cfg: crate::env::SurrealPodConfig::default()
                .with_endpoint("ws://surrealdb:8000"),
            run_id: "abc-123".to_string(),
            run_cr_name: "rivers-run-abc-123".to_string(),
            run_cr_uid: "uid-456".to_string(),
            code_location_id: "demo-id".to_string(),
            extra_env: vec![],
            partition_key: None,
        }
    }

    #[test]
    fn retry_attempt_gets_suffixed_name_and_escalated_resources() {
        let config = test_config();
        let base = build_step_job(&config, "big_join");
        let base_name = base.metadata.name.as_deref().unwrap().to_string();

        let compute = rivers_core::execution::compute::Compute {
            cpu: None,
            memory: Some("16Gi".to_string()),
            gpu: None,
        };
        let retry = build_step_job_with(
            &config,
            "big_join",
            &["big_join".to_string()],
            &StepJobOverrides {
                attempt: 3,
                compute: Some(&compute),
            },
        );
        assert_eq!(
            retry.metadata.name.as_deref().unwrap(),
            format!("{base_name}-r3")
        );
        assert_eq!(
            env_val(&get_env(&retry), "RIVERS_STEP_ATTEMPT"),
            Some("3".to_string())
        );

        let container = &retry
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        let resources = container.resources.as_ref().unwrap();
        let requests = resources.requests.as_ref().unwrap();
        let limits = resources.limits.as_ref().unwrap();
        // memory overridden, cpu inherited from config
        assert_eq!(requests["memory"].0, "16Gi");
        assert_eq!(requests["cpu"].0, "1");
        assert_eq!(limits["memory"].0, "16Gi");
        assert!(!requests.contains_key("nvidia.com/gpu"));
    }

    #[test]
    fn multi_output_step_passes_every_step_key() {
        let keys = ["mr_x".to_string(), "mr_y".to_string()];
        let job = build_step_job_with(&test_config(), "mr_x", &keys, &StepJobOverrides::default());
        let args = job
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();
        let step_keys: Vec<_> = args
            .windows(2)
            .filter(|w| w[0] == "--step-key")
            .map(|w| w[1].clone())
            .collect();
        assert_eq!(step_keys, ["mr_x", "mr_y"]);
    }

    #[test]
    fn gpu_axis_adds_extended_resource() {
        let compute = rivers_core::execution::compute::Compute {
            cpu: None,
            memory: None,
            gpu: Some("1".to_string()),
        };
        let job = build_step_job_with(
            &test_config(),
            "gpu_step",
            &["gpu_step".to_string()],
            &StepJobOverrides {
                attempt: 1,
                compute: Some(&compute),
            },
        );
        let container = &job
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        let requests = container
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(requests["nvidia.com/gpu"].0, "1");
        // unset axes fall back to config
        assert_eq!(requests["memory"].0, "2Gi");
    }

    #[test]
    fn build_step_job_basic() {
        let job = build_step_job(&test_config(), "parse_document");

        let meta = &job.metadata;
        let name = meta.name.as_deref().unwrap();
        assert!(name.starts_with("rivers-step-abc-123-"));
        assert!(name.len() <= 63);
        assert_eq!(meta.namespace.as_deref(), Some("rivers-test"));

        let labels = meta.labels.as_ref().unwrap();
        assert_eq!(labels["rivers.io/run-id"], "abc-123");
        assert_eq!(labels["rivers.io/step"], "parse_document");
        assert_eq!(labels["rivers.io/component"], "step-worker");
        assert!(!labels.contains_key("rivers.io/mapping-key"));

        let owner_refs = meta.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "Run");
        assert_eq!(owner_refs[0].name, "rivers-run-abc-123");
        assert_eq!(owner_refs[0].uid, "uid-456");
        assert!(!owner_refs[0].controller.unwrap());

        let spec = job.spec.as_ref().unwrap();
        assert_eq!(spec.backoff_limit, Some(0));
        assert_eq!(spec.ttl_seconds_after_finished, Some(600));

        let pod_spec = spec.template.spec.as_ref().unwrap();
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
        assert_eq!(
            pod_spec.service_account_name.as_deref(),
            Some("rivers-step-worker")
        );

        let container = &pod_spec.containers[0];
        assert_eq!(container.name, "step");
        assert_eq!(
            container.image.as_deref(),
            Some("registry.example.com/my-repo:latest")
        );

        let args = container.args.as_ref().unwrap();
        assert!(args.contains(&"execute-step".to_string()));
        assert!(args.contains(&"my_project.definitions".to_string()));
        assert!(args.contains(&"--run-id".to_string()));
        assert!(args.contains(&"abc-123".to_string()));
        assert!(args.contains(&"--step-key".to_string()));
        assert!(args.contains(&"parse_document".to_string()));
        assert!(!args.contains(&"--surreal-endpoint".to_string()));
        assert!(!args.contains(&"--partition-key".to_string()));

        let resources = container.resources.as_ref().unwrap();
        let requests = resources.requests.as_ref().unwrap();
        assert_eq!(requests["cpu"].0, "1");
        assert_eq!(requests["memory"].0, "2Gi");
    }

    #[test]
    fn build_mapped_step_job_includes_mapping_key() {
        let job = build_mapped_step_job(&test_config(), "process_chunk", "doc_3");

        let meta = &job.metadata;
        let name = meta.name.as_deref().unwrap();
        assert!(name.starts_with("rivers-step-abc-123-"));
        assert!(name.len() <= 63);

        let labels = meta.labels.as_ref().unwrap();
        assert_eq!(labels["rivers.io/mapping-key"], "doc_3");

        let args = job
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .args
            .as_ref()
            .unwrap();
        assert!(args.contains(&"--mapping-key".to_string()));
        assert!(args.contains(&"doc_3".to_string()));
    }

    #[test]
    fn job_names_are_always_within_k8s_limit() {
        let name = step_job_name("run-id", "a_very_long_step_name_that_exceeds_the_k8s_limit");
        assert!(name.len() <= 63);
    }

    #[test]
    fn different_steps_produce_different_job_names() {
        let a = step_job_name("run-id", "step_a");
        let b = step_job_name("run-id", "step_b");
        assert_ne!(a, b);
    }

    #[test]
    fn different_mapping_keys_produce_different_job_names() {
        let a = mapped_step_job_name("run-id", "step", "key_1");
        let b = mapped_step_job_name("run-id", "step", "key_2");
        assert_ne!(a, b);
    }

    #[test]
    fn sanitize_replaces_invalid_chars() {
        assert_eq!(sanitize_k8s_label("my_step/task"), "my_step-task");
        assert_eq!(sanitize_k8s_label("Step.Name"), "step.name");
    }

    #[test]
    fn sanitize_trims_non_alphanumeric_edges() {
        assert_eq!(sanitize_k8s_label("-leading"), "leading");
        assert_eq!(sanitize_k8s_label("trailing-"), "trailing");
        assert_eq!(sanitize_k8s_label("__wrapped__"), "wrapped");
    }

    #[test]
    fn sanitize_truncates_to_63_chars() {
        let long = "a".repeat(100);
        let result = sanitize_k8s_label(&long);
        assert!(result.len() <= 63);
        assert!(result.chars().last().unwrap().is_ascii_alphanumeric());
    }

    #[test]
    fn owner_reference_not_controller() {
        let job = build_step_job(&test_config(), "step_a");
        let owner_ref = &job.metadata.owner_references.as_ref().unwrap()[0];
        assert!(!owner_ref.controller.unwrap());
        assert!(owner_ref.block_owner_deletion.unwrap());
    }

    fn get_env(job: &Job) -> Vec<(String, String)> {
        job.spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| (e.name.clone(), e.value.clone().unwrap_or_default()))
            .collect()
    }

    fn env_val(env: &[(String, String)], key: &str) -> Option<String> {
        env.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    #[test]
    fn step_pod_env_vars() {
        let job = build_step_job(&test_config(), "my_step");
        let env = get_env(&job);

        assert_eq!(env_val(&env, "RIVERS_STEP_POD"), Some("1".to_string()));
        assert_eq!(env_val(&env, "RIVERS_STEP_ATTEMPT"), Some("1".to_string()));
        assert_eq!(env_val(&env, "RIVERS_RUN_ID"), Some("abc-123".to_string()));
        assert_eq!(
            env_val(&env, "RIVERS_SURREAL_ENDPOINT"),
            Some("ws://surrealdb:8000".to_string())
        );
        assert_eq!(
            env_val(&env, "RIVERS_CODE_LOCATION_IMAGE"),
            Some("registry.example.com/my-repo:latest".to_string())
        );
        assert_eq!(
            env_val(&env, "RIVERS_CODE_LOCATION_ID"),
            Some("demo-id".to_string())
        );
        assert_eq!(
            env_val(&env, "RIVERS_NAMESPACE"),
            Some("rivers-test".to_string())
        );
        assert_eq!(
            env_val(&env, "RIVERS_MODULE"),
            Some("my_project.definitions".to_string())
        );
    }

    #[test]
    fn extra_env_forwarded_to_step_pod() {
        let mut config = test_config();
        config.extra_env = vec![
            EnvVar {
                name: "AWS_ACCESS_KEY_ID".to_string(),
                value: Some("mykey".to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVERS_S3_BUCKET".to_string(),
                value: Some("mybucket".to_string()),
                ..Default::default()
            },
        ];
        let job = build_step_job(&config, "step");
        let env = get_env(&job);

        assert_eq!(
            env_val(&env, "AWS_ACCESS_KEY_ID"),
            Some("mykey".to_string())
        );
        assert_eq!(
            env_val(&env, "RIVERS_S3_BUCKET"),
            Some("mybucket".to_string())
        );
    }

    #[test]
    fn extra_env_preserves_secret_key_ref() {
        use k8s_openapi::api::core::v1::{EnvVarSource, SecretKeySelector};

        let mut config = test_config();
        config.extra_env = vec![EnvVar {
            name: "AWS_ACCESS_KEY_ID".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: "aws-creds".to_string(),
                    key: "access-key".to_string(),
                    optional: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }];

        let job = build_step_job(&config, "step");
        let envs = job.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        let secret_var = envs.iter().find(|e| e.name == "AWS_ACCESS_KEY_ID").unwrap();
        let secret_ref = secret_var
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(secret_ref.name, "aws-creds");
        assert_eq!(secret_ref.key, "access-key");
        assert!(secret_var.value.is_none());
    }

    #[test]
    fn pod_template_labels_match_job_labels() {
        let job = build_step_job(&test_config(), "my_step");
        let job_labels = job.metadata.labels.as_ref().unwrap();
        let pod_labels = job
            .spec
            .as_ref()
            .unwrap()
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .as_ref()
            .unwrap();

        assert_eq!(
            pod_labels["rivers.io/run-id"],
            job_labels["rivers.io/run-id"]
        );
        assert_eq!(pod_labels["rivers.io/step"], job_labels["rivers.io/step"]);
        assert_eq!(
            pod_labels["rivers.io/component"],
            job_labels["rivers.io/component"]
        );
    }

    #[test]
    fn short_hash_is_deterministic() {
        assert_eq!(short_hash("hello"), short_hash("hello"));
        assert_ne!(short_hash("hello"), short_hash("world"));
    }

    #[test]
    fn sanitize_empty_string() {
        assert_eq!(sanitize_k8s_label(""), "");
    }

    #[test]
    fn step_job_includes_partition_key() {
        let mut config = test_config();
        config.partition_key = Some(r#"{"single":["2025-01-16"]}"#.to_string());
        let job = build_step_job(&config, "daily_events");
        let args = job.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();
        let pk_idx = args.iter().position(|a| a == "--partition-key").unwrap();
        assert_eq!(args[pk_idx + 1], r#"{"single":["2025-01-16"]}"#);
    }

    #[test]
    fn step_job_omits_partition_key_when_none() {
        let config = test_config();
        let job = build_step_job(&config, "raw_users");
        let args = job.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();
        assert!(!args.contains(&"--partition-key".to_string()));
    }
}
