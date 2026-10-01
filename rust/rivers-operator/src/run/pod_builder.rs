use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube_client::ResourceExt;
use rivers_k8s::crd::run::Run;
use rivers_k8s::workspace::{self, WorkspaceSpec, WorkspaceVolume};

use crate::codelocation::WorkspaceConfig;
use crate::codelocation::resources::workspace_pvc_name;

/// Consumer-side workspace for a git-mode run, or `None` for image mode.
/// Derived from the Run's stamped `spec.source` + chart config; the same
/// spec shape the step-Job builder rebuilds in-pod from `RIVERS_RUN_SOURCE`.
fn run_workspace_spec(
    run: &Run,
    workspace_cfg: &WorkspaceConfig,
    cl_env: &[EnvVar],
) -> Option<WorkspaceSpec> {
    run.spec.source.as_ref().map(|source| {
        let volume = if workspace_cfg.shared_enabled {
            WorkspaceVolume::SharedPvc {
                claim_name: workspace_pvc_name(&run.spec.code_location_ref.name),
            }
        } else {
            WorkspaceVolume::EmptyDir {
                size_limit: Some(Quantity(workspace_cfg.empty_dir_limit.clone())),
            }
        };
        workspace::consumer_spec_from_run_source(source, &run.spec.image, volume, cl_env.to_vec())
    })
}

/// `resume = true` flips the `rivers execute` invocation to resume mode
/// (skip already-completed steps); `cl_env` is the parent
/// `CodeLocation.spec.env` forwarded onto the pod so `secretKeyRef` /
/// `configMapKeyRef` / `fieldRef` semantics are preserved.
/// `surreal_pod_cfg` carries the SurrealDB scope + auth-secret coordinates
/// stamped on every rivers pod — sourced from the operator's own env, so the
/// auth-secret coordinates the run pod re-emits onto step pods stay
/// consistent with the operator's view. The pod is owned by the Run CR so
/// kube garbage-collects it on Run delete.
pub fn build_executor_pod(
    run: &Run,
    pod_name: &str,
    run_id: &str,
    resume: bool,
    cl_env: &[EnvVar],
    surreal_pod_cfg: &rivers_k8s::env::SurrealPodConfig,
    otel_pod_cfg: &rivers_k8s::env::OtelPodConfig,
    workspace_cfg: &WorkspaceConfig,
) -> Pod {
    let spec = &run.spec;
    let run_uid = run.metadata.uid.as_deref().unwrap_or_default();
    let run_name = run.name_any();
    let workspace_spec = run_workspace_spec(run, workspace_cfg, cl_env);
    let pieces = workspace_spec.as_ref().map(workspace::consumer_pod_pieces);
    let command = if workspace_spec.is_some() {
        workspace::VENV_RIVERS_BIN.to_string()
    } else {
        "rivers".to_string()
    };

    Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: run.namespace(),
            labels: Some(BTreeMap::from([
                ("rivers.io/run-id".to_string(), run_id.to_string()),
                ("rivers.io/component".to_string(), "executor".to_string()),
                (
                    "app.kubernetes.io/managed-by".to_string(),
                    "rivers-operator".to_string(),
                ),
            ])),
            owner_references: Some(vec![OwnerReference {
                api_version: "rivers.io/v1alpha1".to_string(),
                kind: "Run".to_string(),
                name: run_name.clone(),
                uid: run_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            service_account_name: Some(spec.service_account_name.clone()),
            restart_policy: Some("Never".to_string()),
            init_containers: pieces
                .as_ref()
                .filter(|p| !p.init_containers.is_empty())
                .map(|p| p.init_containers.clone()),
            volumes: pieces
                .as_ref()
                .filter(|p| !p.volumes.is_empty())
                .map(|p| p.volumes.clone()),
            containers: vec![Container {
                name: "executor".to_string(),
                image: Some(spec.image.clone()),
                image_pull_policy: Some("IfNotPresent".to_string()),
                command: Some(vec![command]),
                working_dir: workspace_spec.as_ref().map(|w| {
                    match w
                        .path
                        .as_deref()
                        .map(|p| p.trim_matches('/'))
                        .filter(|p| !p.is_empty())
                    {
                        Some(p) => format!("{}/src/{p}", workspace::WORKSPACE_MOUNT),
                        None => format!("{}/src", workspace::WORKSPACE_MOUNT),
                    }
                }),
                volume_mounts: pieces
                    .as_ref()
                    .filter(|p| !p.main_mounts.is_empty())
                    .map(|p| p.main_mounts.clone()),
                args: Some(build_execute_args(spec, run_id, resume)),
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([
                        ("cpu".to_string(), Quantity(spec.run_resources.cpu.clone())),
                        (
                            "memory".to_string(),
                            Quantity(spec.run_resources.memory.clone()),
                        ),
                    ])),
                    limits: Some(BTreeMap::from([
                        ("cpu".to_string(), Quantity(spec.run_resources.cpu.clone())),
                        (
                            "memory".to_string(),
                            Quantity(spec.run_resources.memory.clone()),
                        ),
                    ])),
                    ..Default::default()
                }),
                env: Some({
                    let mut env = vec![
                        EnvVar {
                            name: "RIVERS_CODE_LOCATION_IMAGE".to_string(),
                            value: Some(spec.image.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_CODE_LOCATION_ID".to_string(),
                            value: Some(spec.code_location_ref.identity.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: rivers_k8s::env::ENV_CODE_LOCATION_NAME.to_string(),
                            value: Some(spec.code_location_ref.name.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_NAMESPACE".to_string(),
                            value: Some(run.namespace().unwrap_or_default()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_RUN_ID".to_string(),
                            value: Some(run_id.to_string()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_MODULE".to_string(),
                            value: Some(spec.module.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_RUN_CR_NAME".to_string(),
                            value: Some(run_name.clone()),
                            ..Default::default()
                        },
                        EnvVar {
                            name: "RIVERS_RUN_CR_UID".to_string(),
                            value: Some(run_uid.to_string()),
                            ..Default::default()
                        },
                    ];
                    // Endpoint comes from RunSpec; scope + auth coords from operator state.
                    env.extend(rivers_k8s::env::build_surreal_pod_env(
                        &surreal_pod_cfg
                            .clone()
                            .with_endpoint(spec.surreal_endpoint.clone()),
                    ));
                    if let (Some(pieces), Some(wspec)) = (&pieces, &workspace_spec) {
                        env.extend(pieces.main_env.iter().cloned());
                        // The hop to step Jobs: the in-pod builder rebuilds
                        // the same WorkspaceSpec from these (RFC-044).
                        env.push(EnvVar {
                            name: rivers_k8s::env::ENV_RUN_SOURCE.to_string(),
                            value: Some(
                                serde_json::to_string(
                                    spec.source.as_ref().expect("workspace implies source"),
                                )
                                .expect("RunSource serializes"),
                            ),
                            ..Default::default()
                        });
                        match &wspec.volume {
                            rivers_k8s::workspace::WorkspaceVolume::SharedPvc { claim_name } => {
                                env.push(EnvVar {
                                    name: rivers_k8s::env::ENV_WORKSPACE_PVC.to_string(),
                                    value: Some(claim_name.clone()),
                                    ..Default::default()
                                });
                            }
                            rivers_k8s::workspace::WorkspaceVolume::EmptyDir { size_limit } => {
                                if let Some(limit) = size_limit {
                                    env.push(EnvVar {
                                        name: rivers_k8s::env::ENV_WORKSPACE_EMPTYDIR_LIMIT
                                            .to_string(),
                                        value: Some(limit.0.clone()),
                                        ..Default::default()
                                    });
                                }
                            }
                        }
                    }
                    env.extend(rivers_k8s::env::build_otel_pod_env(otel_pod_cfg));
                    rivers_k8s::env::merge_env(env, cl_env.iter().cloned())
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn build_execute_args(
    spec: &rivers_k8s::crd::run::RunSpec,
    run_id: &str,
    resume: bool,
) -> Vec<String> {
    let mut args = vec![
        "execute".to_string(),
        spec.module.clone(),
        "--run-id".to_string(),
        run_id.to_string(),
        "--surreal-endpoint".to_string(),
        spec.surreal_endpoint.clone(),
    ];

    args.extend(["--target".to_string(), spec.target.clone()]);

    if let Some(ref job) = spec.job_name {
        args.extend(["--job".to_string(), job.clone()]);
    }

    if let Some(ref pk) = spec.partition_key {
        args.extend(["--partition-key".to_string(), pk.clone()]);
    }

    if resume {
        args.push("--resume".to_string());
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_k8s::crd::run::{CodeLocationRef, Executor, ResourceSpec, RunSpec};

    fn test_run() -> Run {
        let spec = RunSpec {
            job_name: None,
            action: None,
            code_location_ref: CodeLocationRef {
                name: "demo".to_string(),
                identity: "demo-id".to_string(),
            },
            image: "registry.example.com/my-repo:latest".to_string(),
            module: "my_project.definitions".to_string(),
            target: "my_job".to_string(),
            surreal_endpoint: "ws://surrealdb.rivers.svc:8000".to_string(),
            executor: Executor::Kubernetes,
            parameters: None,
            partition_key: None,
            run_id: Some("test-run-id".to_string()),
            timeout_seconds: None,
            max_restarts: 3,
            cancel_grace_period_seconds: 300,
            source: None,
            run_resources: ResourceSpec {
                cpu: "500m".to_string(),
                memory: "512Mi".to_string(),
            },
            worker_resources: ResourceSpec {
                cpu: "1".to_string(),
                memory: "2Gi".to_string(),
            },
            max_concurrent_steps: 10,
            service_account_name: "rivers-executor".to_string(),
        };

        Run::new("test-run", spec)
    }

    fn build(run: &Run, pod_name: &str, run_id: &str, resume: bool, cl_env: &[EnvVar]) -> Pod {
        build_executor_pod(
            run,
            pod_name,
            run_id,
            resume,
            cl_env,
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            &WorkspaceConfig::default(),
        )
    }

    fn git_run() -> Run {
        let mut run = test_run();
        run.spec.source = Some(
            serde_json::from_value(serde_json::json!({
                "git": {
                    "url": "https://forge.example/acme/pipelines.git",
                    "commit": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
                    "ref": "refs/heads/main",
                    "path": "analytics",
                },
                "dependencies": { "mode": "uvSync" },
            }))
            .unwrap(),
        );
        run
    }

    #[test]
    fn git_run_executor_pod_mounts_shared_tree_and_stamps_the_hop() {
        let run = git_run();
        let shared = WorkspaceConfig {
            shared_enabled: true,
            ..Default::default()
        };
        let pod = build_executor_pod(
            &run,
            "p",
            "run-123",
            false,
            &[],
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            &shared,
        );
        let pod_spec = pod.spec.unwrap();
        assert!(pod_spec.init_containers.is_none(), "shared consumer");
        let c = &pod_spec.containers[0];
        assert_eq!(
            c.command.as_ref().unwrap(),
            &vec!["/workspace/venv/bin/rivers".to_string()]
        );
        assert_eq!(c.working_dir.as_deref(), Some("/workspace/src/analytics"));

        let env: std::collections::HashMap<&str, &str> = c
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap_or("")))
            .collect();
        // The hop: the in-pod step-job builder rebuilds the workspace from
        // these two.
        assert_eq!(env["RIVERS_WORKSPACE_PVC"], "demo-workspace");
        let hop: rivers_k8s::crd::run::RunSource =
            serde_json::from_str(env["RIVERS_RUN_SOURCE"]).unwrap();
        assert_eq!(hop, run.spec.source.clone().unwrap());
        assert_eq!(env["VIRTUAL_ENV"], "/workspace/venv");

        let mounts = serde_json::to_value(c.volume_mounts.as_ref().unwrap()).unwrap();
        assert!(mounts.to_string().contains("\"readOnly\":true"), "{mounts}");
    }

    #[test]
    fn git_run_fallback_carries_init_container_and_emptydir_limit() {
        let run = git_run();
        let pod = build_executor_pod(
            &run,
            "p",
            "run-123",
            false,
            &[],
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            &WorkspaceConfig::default(), // shared_enabled: false
        );
        let pod_spec = pod.spec.unwrap();
        assert_eq!(pod_spec.init_containers.as_ref().unwrap().len(), 1);
        let env: std::collections::HashMap<&str, &str> = pod_spec.containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap_or("")))
            .collect();
        assert!(!env.contains_key("RIVERS_WORKSPACE_PVC"));
        assert_eq!(env["RIVERS_WORKSPACE_EMPTYDIR_LIMIT"], "2Gi");
    }

    #[test]
    fn image_mode_executor_pod_untouched_by_workspace_wiring() {
        let run = test_run();
        let pod = build(&run, "p", "run-123", false, &[]);
        let pod_spec = pod.spec.unwrap();
        assert!(pod_spec.init_containers.is_none());
        assert!(pod_spec.volumes.is_none());
        let c = &pod_spec.containers[0];
        assert_eq!(c.command.as_ref().unwrap(), &vec!["rivers".to_string()]);
        assert!(c.working_dir.is_none());
        assert!(
            !c.env
                .as_ref()
                .unwrap()
                .iter()
                .any(|e| e.name == "RIVERS_RUN_SOURCE")
        );
    }

    fn env_secret_ref(env: &[EnvVar], name: &str) -> Option<(String, String)> {
        env.iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value_from.as_ref())
            .and_then(|src| src.secret_key_ref.as_ref())
            .map(|sk| (sk.name.clone(), sk.key.clone()))
    }

    #[test]
    fn otel_settings_are_stamped_on_the_run_pod() {
        let run = test_run();
        let pod = build_executor_pod(
            &run,
            "test-pod",
            "run-123",
            false,
            &[],
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig {
                endpoint: "https://otlp.example.com:4317".to_string(),
                headers_secret_name: "otel-headers".to_string(),
                headers_secret_key: "headers".to_string(),
            },
            &WorkspaceConfig::default(),
        );
        let envs = pod.spec.unwrap().containers[0].env.clone().unwrap();

        let endpoint = envs
            .iter()
            .find(|e| e.name == "OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap();
        assert_eq!(
            endpoint.value.as_deref(),
            Some("https://otlp.example.com:4317")
        );

        assert_eq!(
            env_secret_ref(&envs, "OTEL_EXPORTER_OTLP_HEADERS"),
            Some(("otel-headers".to_string(), "headers".to_string()))
        );
        let headers = envs
            .iter()
            .find(|e| e.name == "OTEL_EXPORTER_OTLP_HEADERS")
            .unwrap();
        assert!(headers.value.is_none());

        // Coordinates the run pod re-emits on step pods.
        let coord = envs
            .iter()
            .find(|e| e.name == "RIVERS_OTEL_HEADERS_SECRET_NAME")
            .unwrap();
        assert_eq!(coord.value.as_deref(), Some("otel-headers"));
        let endpoint_coord = envs
            .iter()
            .find(|e| e.name == "RIVERS_OTEL_ENDPOINT")
            .unwrap();
        assert_eq!(
            endpoint_coord.value.as_deref(),
            Some("https://otlp.example.com:4317")
        );
    }

    #[test]
    fn test_build_executor_pod_basic() {
        let run = test_run();
        let pod = build(&run, "test-run-executor", "test-run-id", false, &[]);

        assert_eq!(pod.metadata.name.as_deref(), Some("test-run-executor"));

        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("rivers.io/run-id").unwrap(), "test-run-id");
        assert_eq!(labels.get("rivers.io/component").unwrap(), "executor");

        let owner_refs = pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "Run");
        assert_eq!(owner_refs[0].name, "test-run");
        assert!(owner_refs[0].controller.unwrap());

        let pod_spec = pod.spec.as_ref().unwrap();
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
        assert_eq!(
            pod_spec.service_account_name.as_deref(),
            Some("rivers-executor")
        );

        let container = &pod_spec.containers[0];
        assert_eq!(container.name, "executor");
        assert_eq!(
            container.image.as_deref(),
            Some("registry.example.com/my-repo:latest")
        );
        assert_eq!(
            container.command.as_ref().unwrap(),
            &vec!["rivers".to_string()]
        );

        let args = container.args.as_ref().unwrap();
        assert!(args.contains(&"execute".to_string()));
        assert!(args.contains(&"my_project.definitions".to_string()));
        assert!(args.contains(&"--run-id".to_string()));
        assert!(args.contains(&"test-run-id".to_string()));
        assert!(args.contains(&"--surreal-endpoint".to_string()));
        assert!(args.contains(&"--target".to_string()));
        assert!(args.contains(&"my_job".to_string()));

        let env = container.env.as_ref().unwrap();
        let env_map: std::collections::HashMap<&str, &str> = env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap_or("")))
            .collect();
        assert_eq!(env_map["RIVERS_RUN_ID"], "test-run-id");
        assert_eq!(
            env_map["RIVERS_SURREAL_ENDPOINT"],
            "ws://surrealdb.rivers.svc:8000"
        );
        assert_eq!(env_map["RIVERS_MODULE"], "my_project.definitions");
        assert_eq!(env_map["RIVERS_RUN_CR_NAME"], "test-run");
    }

    #[test]
    fn test_build_executor_pod_with_partition_key() {
        let mut run = test_run();
        run.spec.partition_key = Some("2026-04-14".to_string());

        let pod = build(&run, "test-pod", "run-123", false, &[]);
        let args = pod.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();

        assert!(args.contains(&"--partition-key".to_string()));
        assert!(args.contains(&"2026-04-14".to_string()));
    }

    #[test]
    fn test_build_executor_pod_resources() {
        let run = test_run();
        let pod = build(&run, "test-pod", "run-123", false, &[]);

        let resources = pod.spec.unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap()
            .clone();
        let requests = resources.requests.unwrap();
        assert_eq!(requests.get("cpu").unwrap().0, "500m");
        assert_eq!(requests.get("memory").unwrap().0, "512Mi");
    }

    #[test]
    fn test_build_executor_pod_env_vars() {
        let run = test_run();
        let pod = build(&run, "test-pod", "run-123", false, &[]);
        let env = pod.spec.unwrap().containers[0]
            .env
            .as_ref()
            .unwrap()
            .clone();
        let env_map: std::collections::HashMap<&str, &str> = env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap_or("")))
            .collect();

        assert_eq!(
            env_map["RIVERS_CODE_LOCATION_IMAGE"],
            "registry.example.com/my-repo:latest"
        );
        assert_eq!(env_map["RIVERS_CODE_LOCATION_ID"], "demo-id");
        assert_eq!(env_map["RIVERS_NAMESPACE"], "");
        assert_eq!(env_map["RIVERS_RUN_ID"], "run-123");
        assert_eq!(
            env_map["RIVERS_SURREAL_ENDPOINT"],
            "ws://surrealdb.rivers.svc:8000"
        );
        assert_eq!(env_map["RIVERS_MODULE"], "my_project.definitions");
        assert_eq!(env_map["RIVERS_RUN_CR_NAME"], "test-run");
        assert_eq!(env_map["RIVERS_RUN_CR_UID"], "");
    }

    #[test]
    fn test_build_executor_pod_image_pull_policy() {
        let run = test_run();
        let pod = build(&run, "test-pod", "run-123", false, &[]);
        let container = &pod.spec.unwrap().containers[0];
        assert_eq!(container.image_pull_policy.as_deref(), Some("IfNotPresent"));
    }

    #[test]
    fn test_build_executor_pod_restart_policy() {
        let run = test_run();
        let pod = build(&run, "test-pod", "run-123", false, &[]);
        assert_eq!(pod.spec.unwrap().restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn test_build_executor_pod_resume_flag() {
        let run = test_run();
        let pod = build(&run, "test-pod", "run-123", true, &[]);
        let args = pod.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();
        assert!(args.contains(&"--resume".to_string()));

        let pod_no_resume = build(&run, "test-pod", "run-123", false, &[]);
        let args_no = pod_no_resume.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .clone();
        assert!(!args_no.contains(&"--resume".to_string()));
    }

    #[test]
    fn cl_env_replaces_same_named_otel_env() {
        let run = test_run();
        let cl_env = vec![EnvVar {
            name: "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
            value: Some("http://team-collector:4317".to_string()),
            ..Default::default()
        }];
        let pod = build_executor_pod(
            &run,
            "test-pod",
            "run-123",
            false,
            &cl_env,
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig {
                endpoint: "https://otlp.example.com:4317".to_string(),
                ..Default::default()
            },
            &WorkspaceConfig::default(),
        );
        let envs = pod.spec.unwrap().containers[0].env.clone().unwrap();

        let endpoints: Vec<_> = envs
            .iter()
            .filter(|e| e.name == "OTEL_EXPORTER_OTLP_ENDPOINT")
            .map(|e| e.value.as_deref())
            .collect();
        assert_eq!(endpoints, vec![Some("http://team-collector:4317")]);
    }

    #[test]
    fn test_build_executor_pod_propagates_cl_env() {
        use k8s_openapi::api::core::v1::{EnvVarSource, SecretKeySelector};

        let run = test_run();
        let cl_env = vec![
            EnvVar {
                name: "RIVERS_S3_BUCKET".to_string(),
                value: Some("my-bucket".to_string()),
                ..Default::default()
            },
            EnvVar {
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
            },
        ];

        let pod = build(&run, "test-pod", "run-123", false, &cl_env);
        let envs = pod.spec.unwrap().containers[0].env.clone().unwrap();

        let bucket = envs.iter().find(|e| e.name == "RIVERS_S3_BUCKET").unwrap();
        assert_eq!(bucket.value.as_deref(), Some("my-bucket"));

        let key = envs.iter().find(|e| e.name == "AWS_ACCESS_KEY_ID").unwrap();
        assert_eq!(
            env_secret_ref(&envs, "AWS_ACCESS_KEY_ID"),
            Some(("aws-creds".to_string(), "access-key".to_string()))
        );
        assert!(key.value.is_none());

        let cl_name = envs
            .iter()
            .find(|e| e.name == "RIVERS_CODE_LOCATION_NAME")
            .unwrap();
        assert_eq!(cl_name.value.as_deref(), Some("demo"));
    }
}
