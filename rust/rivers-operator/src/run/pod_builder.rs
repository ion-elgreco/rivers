use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube_client::ResourceExt;
use rivers_k8s::crd::run::Run;

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
) -> Pod {
    let spec = &run.spec;
    let run_uid = run.metadata.uid.as_deref().unwrap_or_default();
    let run_name = run.name_any();

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
            containers: vec![Container {
                name: "executor".to_string(),
                image: Some(spec.image.clone()),
                image_pull_policy: Some("IfNotPresent".to_string()),
                command: Some(vec!["rivers".to_string()]),
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
                    env.extend(cl_env.iter().cloned());
                    env
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
        )
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
        let secret_ref = key
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(secret_ref.name, "aws-creds");
        assert_eq!(secret_ref.key, "access-key");
        assert!(key.value.is_none());

        let cl_name = envs
            .iter()
            .find(|e| e.name == "RIVERS_CODE_LOCATION_NAME")
            .unwrap();
        assert_eq!(cl_name.value.as_deref(), Some("demo"));
    }
}
