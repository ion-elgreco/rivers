//! Builders for the `Deployment` and `Service` owned by a `CodeLocation` CR.
//!
//! These functions are deliberately pure — they take the CR + resolved image
//! and emit the desired object. The reconciler handles apply semantics.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, PodSpec, PodTemplateSpec, Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube_client::ResourceExt;
use rivers_k8s::crd::code_location::CodeLocation;

pub const COMPONENT_LABEL: &str = "code-location";
pub const MANAGED_BY: &str = "rivers-operator";
pub const MAIN_CONTAINER: &str = "code-location";

/// Deterministic name for the owned Service — the registry surfaces this, so
/// it must be predictable from the CR name alone.
pub fn service_name(cr_name: &str) -> String {
    format!("{cr_name}-grpc")
}

pub fn deployment_name(cr_name: &str) -> String {
    cr_name.to_string()
}

/// In-cluster DNS endpoint the UI dials for per-location gRPC calls.
pub fn grpc_endpoint(cr_name: &str, namespace: &str, port: i32) -> String {
    format!(
        "{}.{}.svc.cluster.local:{}",
        service_name(cr_name),
        namespace,
        port
    )
}

pub fn labels(cr_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("rivers.io/code-location".to_string(), cr_name.to_string()),
        (
            "rivers.io/component".to_string(),
            COMPONENT_LABEL.to_string(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            MANAGED_BY.to_string(),
        ),
        (
            "app.kubernetes.io/name".to_string(),
            format!("rivers-code-location-{cr_name}"),
        ),
    ])
}

pub fn owner_reference(cl: &CodeLocation) -> OwnerReference {
    OwnerReference {
        api_version: "rivers.io/v1alpha1".to_string(),
        kind: "CodeLocation".to_string(),
        name: cl.name_any(),
        uid: cl.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Build the desired `Deployment`.
///
/// `resolved_image` is the fully-qualified digest reference (`repo@sha256:...`)
/// of the code location's image, or in git mode of the runtime image. The
/// container image is pinned to this value so rolling the tag upstream has no
/// effect on running pods.
///
/// `workspace` (git mode) grafts the workspace pieces on: the container runs
/// out of the materialized tree.
pub fn build_deployment(
    cl: &CodeLocation,
    resolved_image: &str,
    code_location_service_account: &str,
    surreal_pod_cfg: &rivers_k8s::env::SurrealPodConfig,
    otel_pod_cfg: &rivers_k8s::env::OtelPodConfig,
    workspace: Option<&rivers_k8s::workspace::WorkspacePodPieces>,
) -> Deployment {
    let name = deployment_name(&cl.name_any());
    let ns = cl.namespace();
    let spec = &cl.spec;
    let lbls = labels(&cl.name_any());

    let pull_secrets = if spec.image_pull_secrets.is_empty() {
        None
    } else {
        Some(spec.image_pull_secrets.clone())
    };

    let mut env = build_base_env(cl, resolved_image, surreal_pod_cfg, otel_pod_cfg);
    if let Some(pieces) = workspace {
        env.extend(pieces.main_env.iter().cloned());
    }
    let env = rivers_k8s::env::merge_env(env, spec.env.iter().cloned());

    Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns.clone(),
            labels: Some(lbls.clone()),
            owner_references: Some(vec![owner_reference(cl)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(lbls.clone()),
                ..Default::default()
            },
            // Old pods keep serving the rolled-out tree until their
            // replacements are ready: the ready count never drops below
            // spec.replicas, even when a new commit never builds.
            strategy: workspace.map(|_| DeploymentStrategy {
                type_: Some("RollingUpdate".to_string()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_unavailable: Some(IntOrString::Int(0)),
                    max_surge: None,
                }),
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(lbls.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(
                        spec.service_account_name
                            .clone()
                            .unwrap_or_else(|| code_location_service_account.to_string()),
                    ),
                    image_pull_secrets: pull_secrets,
                    security_context: workspace.map(|p| p.pod_security_context.clone()),
                    init_containers: workspace.map(|p| p.init_containers.clone()),
                    volumes: workspace.map(|p| p.volumes.clone()),
                    containers: vec![Container {
                        name: MAIN_CONTAINER.to_string(),
                        image: Some(resolved_image.to_string()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        command: Some(workspace.map_or_else(
                            || vec!["rivers".to_string()],
                            |p| p.main_command.clone(),
                        )),
                        // `module` is a positional argument to `rivers serve`;
                        // `--grpc-port` is the flag name (not `--port`).
                        // `--surreal-endpoint` is read from the env we inject
                        // below via Typer's `envvar="RIVERS_SURREAL_ENDPOINT"`.
                        args: Some(vec![
                            "serve".to_string(),
                            spec.module.clone(),
                            "--grpc-port".to_string(),
                            spec.grpc_port.to_string(),
                        ]),
                        working_dir: workspace.map(|p| p.main_working_dir.clone()),
                        ports: Some(vec![ContainerPort {
                            name: Some("grpc".to_string()),
                            container_port: spec.grpc_port,
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        resources: Some(spec.resources.clone()),
                        env: Some(env),
                        volume_mounts: workspace.map(|p| p.main_mounts.clone()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// Deterministic name of the CL-owned workspace PVC (shared mode).
pub fn workspace_pvc_name(cr_name: &str) -> String {
    format!("{cr_name}-workspace")
}

/// Deterministic name of the CL-owned keep-set ConfigMap.
pub fn keep_config_map_name(cr_name: &str) -> String {
    format!("{cr_name}-workspace-keep")
}

/// RWX PVC backing a git CL's shared workspace. Owned by the CR so kube
/// garbage-collects it (and every tree on it) on CL delete.
pub fn build_workspace_pvc(
    cl: &CodeLocation,
    size: &Quantity,
    storage_class: Option<&str>,
) -> k8s_openapi::api::core::v1::PersistentVolumeClaim {
    use k8s_openapi::api::core::v1::{PersistentVolumeClaim, PersistentVolumeClaimSpec};
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(workspace_pvc_name(&cl.name_any())),
            namespace: cl.namespace(),
            labels: Some(labels(&cl.name_any())),
            owner_references: Some(vec![owner_reference(cl)]),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteMany".to_string()]),
            storage_class_name: storage_class.map(str::to_string),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(BTreeMap::from([("storage".to_string(), size.clone())])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Keep-set ConfigMap: the prune's survival list, delivered to the init
/// container via `configMapKeyRef` so refreshing it never touches the pod
/// template (an inline env value would roll the Deployment on run
/// lifecycle).
pub fn build_keep_config_map(
    cl: &CodeLocation,
    keep_csv: &str,
) -> k8s_openapi::api::core::v1::ConfigMap {
    k8s_openapi::api::core::v1::ConfigMap {
        metadata: ObjectMeta {
            name: Some(keep_config_map_name(&cl.name_any())),
            namespace: cl.namespace(),
            labels: Some(labels(&cl.name_any())),
            owner_references: Some(vec![owner_reference(cl)]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            rivers_k8s::workspace::KEEP_CONFIG_MAP_KEY.to_string(),
            keep_csv.to_string(),
        )])),
        ..Default::default()
    }
}

/// Operator-injected env common to both source modes, WITHOUT the user's
/// `spec.env` (merged last, after any workspace env, so it overrides by name).
fn build_base_env(
    cl: &CodeLocation,
    resolved_image: &str,
    surreal_pod_cfg: &rivers_k8s::env::SurrealPodConfig,
    otel_pod_cfg: &rivers_k8s::env::OtelPodConfig,
) -> Vec<EnvVar> {
    let mut env = vec![
        EnvVar {
            name: "RIVERS_CODE_LOCATION_NAME".to_string(),
            value: Some(cl.name_any()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVERS_CODE_LOCATION_ID".to_string(),
            value: Some(cl.spec.identity.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVERS_CODE_LOCATION_IMAGE".to_string(),
            value: Some(resolved_image.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVERS_MODULE".to_string(),
            value: Some(cl.spec.module.clone()),
            ..Default::default()
        },
    ];
    env.extend(rivers_k8s::env::build_surreal_pod_env(surreal_pod_cfg));
    env.extend(rivers_k8s::env::build_otel_pod_env(otel_pod_cfg));
    env
}

pub fn build_service(cl: &CodeLocation) -> Service {
    let name = service_name(&cl.name_any());
    let lbls = labels(&cl.name_any());
    let port = cl.spec.grpc_port;

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cl.namespace(),
            labels: Some(lbls.clone()),
            owner_references: Some(vec![owner_reference(cl)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            selector: Some(lbls),
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port,
                target_port: Some(IntOrString::Int(port)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    //! Golden-object tests.
    //!
    //! Each test asserts the *full* serialized shape of the builder output
    //! against a JSON literal. This catches accidental field additions,
    //! renames, or ordering changes that cherry-picked `.field` assertions
    //! would miss — the tests are noisier but the contract is exact.

    use super::*;
    use rivers_k8s::crd::code_location::CodeLocationSpec;
    use serde_json::json;

    fn make_cl(spec_json: serde_json::Value, name: &str, ns: &str, uid: &str) -> CodeLocation {
        let spec: CodeLocationSpec = serde_json::from_value(spec_json).unwrap();
        CodeLocation {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(ns.to_string()),
                uid: Some(uid.to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    fn expected_labels(cr_name: &str) -> serde_json::Value {
        json!({
            "app.kubernetes.io/managed-by": "rivers-operator",
            "app.kubernetes.io/name": format!("rivers-code-location-{cr_name}"),
            "rivers.io/code-location": cr_name,
            "rivers.io/component": "code-location",
        })
    }

    fn expected_owner_ref(cr_name: &str, uid: &str) -> serde_json::Value {
        json!({
            "apiVersion": "rivers.io/v1alpha1",
            "blockOwnerDeletion": true,
            "controller": true,
            "kind": "CodeLocation",
            "name": cr_name,
            "uid": uid,
        })
    }

    #[test]
    fn service_name_is_deterministic() {
        assert_eq!(service_name("analytics"), "analytics-grpc");
    }

    #[test]
    fn grpc_endpoint_dns_format() {
        assert_eq!(
            grpc_endpoint("analytics", "team-data", 3001),
            "analytics-grpc.team-data.svc.cluster.local:3001"
        );
    }

    #[test]
    fn deployment_matches_golden_for_minimal_cr() {
        let cl = make_cl(
            json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v1.0.0",
                "module": "acme.pipeline",
            }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let d = build_deployment(
            &cl,
            "ghcr.io/acme/pipeline@sha256:abc",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            None,
        );

        let expected = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "analytics",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": expected_labels("analytics") },
                "template": {
                    "metadata": { "labels": expected_labels("analytics") },
                    "spec": {
                        "serviceAccountName": "rivers-code-location",
                        "containers": [{
                            "name": "code-location",
                            "image": "ghcr.io/acme/pipeline@sha256:abc",
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["rivers"],
                            "args": [
                                "serve",
                                "acme.pipeline",
                                "--grpc-port", "3001",
                            ],
                            "ports": [{
                                "name": "grpc",
                                "containerPort": 3001,
                                "protocol": "TCP",
                            }],
                            "resources": {},
                            "env": [
                                { "name": "RIVERS_CODE_LOCATION_NAME", "value": "analytics" },
                                { "name": "RIVERS_CODE_LOCATION_ID", "value": "" },
                                { "name": "RIVERS_CODE_LOCATION_IMAGE", "value": "ghcr.io/acme/pipeline@sha256:abc" },
                                { "name": "RIVERS_MODULE", "value": "acme.pipeline" },
                                { "name": "RIVERS_SURREAL_ENDPOINT", "value": "ws://surrealdb.rivers.svc:8000" },
                                { "name": "RIVERS_SURREAL_NAMESPACE", "value": "rivers" },
                                { "name": "RIVERS_SURREAL_DATABASE", "value": "main" },
                            ],
                        }],
                    },
                },
            },
        });

        assert_eq!(serde_json::to_value(&d).unwrap(), expected);
    }

    #[test]
    fn deployment_spec_env_replaces_chart_otel_endpoint() {
        let cl = make_cl(
            json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v1.0.0",
                "module": "acme.pipeline",
                "env": [{ "name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://team-collector:4317" }],
            }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let d = build_deployment(
            &cl,
            "ghcr.io/acme/pipeline@sha256:abc",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig {
                endpoint: "https://otlp.example.com:4317".to_string(),
                headers_secret_name: "otel-headers".to_string(),
                headers_secret_key: "headers".to_string(),
            },
            None,
        );

        let env = serde_json::to_value(&d.spec.unwrap().template.spec.unwrap().containers[0].env)
            .unwrap();
        let env = env.as_array().unwrap();
        let mut names: Vec<&str> = env.iter().map(|e| e["name"].as_str().unwrap()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "duplicate env names: {env:?}");
        assert_eq!(
            serde_json::Value::Array(env[7..].to_vec()),
            json!([
                { "name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://team-collector:4317" },
                { "name": "RIVERS_OTEL_ENDPOINT", "value": "https://otlp.example.com:4317" },
                { "name": "OTEL_EXPORTER_OTLP_HEADERS", "valueFrom": { "secretKeyRef": { "name": "otel-headers", "key": "headers" } } },
                { "name": "RIVERS_OTEL_HEADERS_SECRET_NAME", "value": "otel-headers" },
                { "name": "RIVERS_OTEL_HEADERS_SECRET_KEY", "value": "headers" },
            ])
        );
    }

    #[test]
    fn deployment_matches_golden_for_full_featured_cr() {
        // Exercises every optional field we care about:
        //   * replicas overridden
        //   * custom serviceAccountName
        //   * imagePullSecrets
        //   * requests + limits (both sides, both cpu and memory)
        //   * env with plain value, secretKeyRef, and fieldRef
        //     (the last one was *not* supported before the upstream-type
        //      migration and is the proof-of-work for "full k8s compat")
        //   * custom grpcPort
        let cl = make_cl(
            json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v2.5.0",
                "module": "acme.pipeline",
                "identity": "550e8400-e29b-41d4-a716-446655440000",
                "replicas": 3,
                "grpcPort": 50051,
                "serviceAccountName": "analytics-sa",
                "imagePullSecrets": [
                    {"name": "ghcr-creds"},
                    {"name": "dockerhub-creds"},
                ],
                "resources": {
                    "requests": { "cpu": "250m", "memory": "512Mi" },
                    "limits":   { "cpu": "1",    "memory": "1Gi" },
                },
                "env": [
                    {"name": "AWS_REGION", "value": "us-east-1"},
                    {
                        "name": "DB_PASSWORD",
                        "valueFrom": {"secretKeyRef": {"name": "db-creds", "key": "password"}},
                    },
                    {
                        "name": "POD_IP",
                        "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
                    },
                ],
            }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let d = build_deployment(
            &cl,
            "ghcr.io/acme/pipeline@sha256:deadbeef",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            None,
        );

        let expected = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "analytics",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "replicas": 3,
                "selector": { "matchLabels": expected_labels("analytics") },
                "template": {
                    "metadata": { "labels": expected_labels("analytics") },
                    "spec": {
                        "serviceAccountName": "analytics-sa",
                        "imagePullSecrets": [
                            {"name": "ghcr-creds"},
                            {"name": "dockerhub-creds"},
                        ],
                        "containers": [{
                            "name": "code-location",
                            "image": "ghcr.io/acme/pipeline@sha256:deadbeef",
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["rivers"],
                            "args": [
                                "serve",
                                "acme.pipeline",
                                "--grpc-port", "50051",
                            ],
                            "ports": [{
                                "name": "grpc",
                                "containerPort": 50051,
                                "protocol": "TCP",
                            }],
                            "resources": {
                                "requests": { "cpu": "250m", "memory": "512Mi" },
                                "limits":   { "cpu": "1",    "memory": "1Gi" },
                            },
                            "env": [
                                { "name": "RIVERS_CODE_LOCATION_NAME", "value": "analytics" },
                                { "name": "RIVERS_CODE_LOCATION_ID", "value": "550e8400-e29b-41d4-a716-446655440000" },
                                { "name": "RIVERS_CODE_LOCATION_IMAGE", "value": "ghcr.io/acme/pipeline@sha256:deadbeef" },
                                { "name": "RIVERS_MODULE", "value": "acme.pipeline" },
                                { "name": "RIVERS_SURREAL_ENDPOINT", "value": "ws://surrealdb.rivers.svc:8000" },
                                { "name": "RIVERS_SURREAL_NAMESPACE", "value": "rivers" },
                                { "name": "RIVERS_SURREAL_DATABASE", "value": "main" },
                                { "name": "AWS_REGION", "value": "us-east-1" },
                                {
                                    "name": "DB_PASSWORD",
                                    "valueFrom": {"secretKeyRef": {"name": "db-creds", "key": "password"}},
                                },
                                {
                                    "name": "POD_IP",
                                    "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
                                },
                            ],
                        }],
                    },
                },
            },
        });

        assert_eq!(serde_json::to_value(&d).unwrap(), expected);
    }

    #[test]
    fn deployment_matches_golden_for_partial_resources() {
        // `limits.memory` only — no requests, no cpu limit.
        let cl = make_cl(
            json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v1.0.0",
                "module": "acme.pipeline",
                "resources": { "limits": { "memory": "2Gi" } },
            }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let d = build_deployment(
            &cl,
            "img@sha256:a",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            None,
        );

        let expected_resources = json!({
            "limits": { "memory": "2Gi" },
        });

        let container_resources = serde_json::to_value(
            &d.spec
                .as_ref()
                .unwrap()
                .template
                .spec
                .as_ref()
                .unwrap()
                .containers[0]
                .resources,
        )
        .unwrap();
        assert_eq!(container_resources, expected_resources);
    }

    fn git_spec_json() -> serde_json::Value {
        json!({
            "git": {
                "url": "https://forge.example/acme/pipelines.git",
                "ref": { "branch": "main" },
                "path": "analytics",
                "secretRef": { "name": "git-creds" },
                "dependencies": { "mode": "uvSync", "extras": ["ml"] },
            },
            "module": "analytics.pipeline",
        })
    }

    /// Every envelope field set, beside the git source.
    fn full_git_spec_json() -> serde_json::Value {
        json!({
            "git": git_spec_json()["git"],
            "module": "analytics.pipeline",
            "identity": "550e8400-e29b-41d4-a716-446655440000",
            "replicas": 2,
            "grpcPort": 50051,
            "serviceAccountName": "analytics-sa",
            "imagePullSecrets": [{ "name": "ghcr-creds" }],
            "resources": {
                "requests": { "cpu": "250m", "memory": "512Mi" },
                "limits": { "memory": "1Gi" },
            },
            "env": [
                { "name": "UV_INDEX_URL", "value": "https://pypi.internal/simple" },
                {
                    "name": "DB_PASSWORD",
                    "valueFrom": { "secretKeyRef": { "name": "db-creds", "key": "password" } },
                },
            ],
        })
    }

    fn shared_volume() -> rivers_k8s::workspace::WorkspaceVolume {
        rivers_k8s::workspace::WorkspaceVolume::SharedPvc {
            claim_name: workspace_pvc_name("analytics"),
        }
    }

    fn fallback_volume() -> rivers_k8s::workspace::WorkspaceVolume {
        rivers_k8s::workspace::WorkspaceVolume::EmptyDir {
            size_limit: Some(Quantity("5Gi".to_string())),
        }
    }

    fn otel_endpoint() -> rivers_k8s::env::OtelPodConfig {
        rivers_k8s::env::OtelPodConfig {
            endpoint: "https://otlp.example.com:4317".to_string(),
            ..Default::default()
        }
    }

    /// The pieces the reconciler builds under the chart's default floors.
    fn git_pieces(
        cl: &CodeLocation,
        volume: rivers_k8s::workspace::WorkspaceVolume,
    ) -> rivers_k8s::workspace::WorkspacePodPieces {
        let git = cl.spec.git.as_ref().unwrap();
        rivers_k8s::workspace::builder_pod_pieces(
            &rivers_k8s::workspace::WorkspaceSpec {
                source: rivers_k8s::crd::run::RunSource {
                    git: rivers_k8s::crd::run::GitCoordinates {
                        url: git.url.clone(),
                        commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string(),
                        r#ref: Some("refs/heads/main".to_string()),
                        path: git.path.clone(),
                        secret_name: Some("git-creds".to_string()),
                    },
                    dependencies: git.dependencies.clone(),
                    runtime_image: "ghcr.io/rt@sha256:1a2b3c4dff".to_string(),
                },
                volume,
                extra_env: cl.spec.env.clone(),
            },
            &crate::codelocation::WorkspaceConfig::default().prune("analytics"),
        )
    }

    /// Subpath and `RIVERS_RUN_SOURCE` of the tree [`git_pieces`] builds.
    const TREE_KEY: &str = "9f3c1ab8d2e4-1a2b3c4d-edfe9516";
    const TREE_SOURCE: &str = r#"{"git":{"url":"https://forge.example/acme/pipelines.git","commit":"9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8","ref":"refs/heads/main","path":"analytics","secretName":"git-creds"},"dependencies":{"mode":"uvSync","extras":["ml"]},"runtimeImage":"ghcr.io/rt@sha256:1a2b3c4dff"}"#;

    #[test]
    fn git_deployment_matches_golden_for_shared_workspace() {
        let cl = make_cl(full_git_spec_json(), "analytics", "team-data", "uid-1234");
        let d = build_deployment(
            &cl,
            "ghcr.io/rt@sha256:1a2b3c4dff",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &otel_endpoint(),
            Some(&git_pieces(&cl, shared_volume())),
        );

        let sync = json!({
            "name": "workspace",
            "image": "ghcr.io/rt@sha256:1a2b3c4dff",
            "imagePullPolicy": "IfNotPresent",
            "command": ["rivers-workspace-sync"],
            "terminationMessagePolicy": "FallbackToLogsOnError",
            "securityContext": {
                "allowPrivilegeEscalation": false,
                "runAsNonRoot": true,
                "capabilities": { "drop": ["ALL"] },
            },
            "env": [
                { "name": "RIVERS_GIT_URL", "value": "https://forge.example/acme/pipelines.git" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                { "name": "RIVERS_GIT_REF", "value": "refs/heads/main" },
                { "name": "RIVERS_GIT_PATH", "value": "analytics" },
                { "name": "RIVERS_DEPS_MODE", "value": "uvSync" },
                { "name": "RIVERS_DEPS_EXTRAS", "value": "ml" },
                { "name": "UV_PROJECT_ENVIRONMENT", "value": "/workspace/venv" },
                { "name": "UV_CACHE_DIR", "value": "/uv-cache" },
                { "name": "UV_LINK_MODE", "value": "copy" },
                { "name": "UV_COMPILE_BYTECODE", "value": "1" },
                { "name": "RIVERS_WORKSPACE_KEY", "value": TREE_KEY },
                {
                    "name": "RIVERS_WORKSPACE_KEEP",
                    "valueFrom": { "configMapKeyRef": {
                        "name": "analytics-workspace-keep", "key": "keep", "optional": true,
                    } },
                },
                { "name": "RIVERS_WORKSPACE_KEEP_REVISIONS", "value": "3" },
                { "name": "RIVERS_WORKSPACE_MIN_AGE_SECONDS", "value": "3600" },
                { "name": "UV_INDEX_URL", "value": "https://pypi.internal/simple" },
                {
                    "name": "DB_PASSWORD",
                    "valueFrom": { "secretKeyRef": { "name": "db-creds", "key": "password" } },
                },
            ],
            "volumeMounts": [
                { "name": "workspace", "mountPath": "/workspace", "subPath": TREE_KEY },
                { "name": "workspace", "mountPath": "/uv-cache", "subPath": "cache" },
                { "name": "workspace", "mountPath": "/workspaces" },
                { "name": "git-credentials", "mountPath": "/etc/rivers/git", "readOnly": true },
            ],
        });
        let main = json!({
            "name": "code-location",
            "image": "ghcr.io/rt@sha256:1a2b3c4dff",
            "imagePullPolicy": "IfNotPresent",
            "command": ["/workspace/venv/bin/rivers"],
            "args": ["serve", "analytics.pipeline", "--grpc-port", "50051"],
            "workingDir": "/workspace/src/analytics",
            "ports": [{ "name": "grpc", "containerPort": 50051, "protocol": "TCP" }],
            "resources": {
                "requests": { "cpu": "250m", "memory": "512Mi" },
                "limits": { "memory": "1Gi" },
            },
            "env": [
                { "name": "RIVERS_CODE_LOCATION_NAME", "value": "analytics" },
                { "name": "RIVERS_CODE_LOCATION_ID", "value": "550e8400-e29b-41d4-a716-446655440000" },
                { "name": "RIVERS_CODE_LOCATION_IMAGE", "value": "ghcr.io/rt@sha256:1a2b3c4dff" },
                { "name": "RIVERS_MODULE", "value": "analytics.pipeline" },
                { "name": "RIVERS_SURREAL_ENDPOINT", "value": "ws://surrealdb.rivers.svc:8000" },
                { "name": "RIVERS_SURREAL_NAMESPACE", "value": "rivers" },
                { "name": "RIVERS_SURREAL_DATABASE", "value": "main" },
                { "name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "https://otlp.example.com:4317" },
                { "name": "RIVERS_OTEL_ENDPOINT", "value": "https://otlp.example.com:4317" },
                { "name": "PYTHONPATH", "value": "/workspace/src/analytics" },
                { "name": "VIRTUAL_ENV", "value": "/workspace/venv" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                { "name": "RIVERS_RUN_SOURCE", "value": TREE_SOURCE },
                { "name": "RIVERS_WORKSPACE_PVC", "value": "analytics-workspace" },
                { "name": "UV_INDEX_URL", "value": "https://pypi.internal/simple" },
                {
                    "name": "DB_PASSWORD",
                    "valueFrom": { "secretKeyRef": { "name": "db-creds", "key": "password" } },
                },
            ],
            "volumeMounts": [{ "name": "workspace", "mountPath": "/workspace", "subPath": TREE_KEY }],
        });
        let expected = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "analytics",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "replicas": 2,
                "selector": { "matchLabels": expected_labels("analytics") },
                "strategy": { "type": "RollingUpdate", "rollingUpdate": { "maxUnavailable": 0 } },
                "template": {
                    "metadata": { "labels": expected_labels("analytics") },
                    "spec": {
                        "serviceAccountName": "analytics-sa",
                        "imagePullSecrets": [{ "name": "ghcr-creds" }],
                        "securityContext": { "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch" },
                        "initContainers": [sync],
                        "volumes": [
                            { "name": "workspace", "persistentVolumeClaim": { "claimName": "analytics-workspace" } },
                            { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 0o440 } },
                        ],
                        "containers": [main],
                    },
                },
            },
        });

        assert_eq!(serde_json::to_value(&d).unwrap(), expected);
    }

    #[test]
    fn git_deployment_matches_golden_for_fallback_workspace() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let d = build_deployment(
            &cl,
            "ghcr.io/rt@sha256:1a2b3c4dff",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            Some(&git_pieces(&cl, fallback_volume())),
        );

        let sync = json!({
            "name": "workspace",
            "image": "ghcr.io/rt@sha256:1a2b3c4dff",
            "imagePullPolicy": "IfNotPresent",
            "command": ["rivers-workspace-sync"],
            "terminationMessagePolicy": "FallbackToLogsOnError",
            "securityContext": {
                "allowPrivilegeEscalation": false,
                "runAsNonRoot": true,
                "capabilities": { "drop": ["ALL"] },
            },
            "env": [
                { "name": "RIVERS_GIT_URL", "value": "https://forge.example/acme/pipelines.git" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                { "name": "RIVERS_GIT_REF", "value": "refs/heads/main" },
                { "name": "RIVERS_GIT_PATH", "value": "analytics" },
                { "name": "RIVERS_DEPS_MODE", "value": "uvSync" },
                { "name": "RIVERS_DEPS_EXTRAS", "value": "ml" },
                { "name": "UV_PROJECT_ENVIRONMENT", "value": "/workspace/venv" },
                { "name": "UV_NO_CACHE", "value": "1" },
                { "name": "UV_LINK_MODE", "value": "copy" },
                { "name": "UV_COMPILE_BYTECODE", "value": "1" },
            ],
            "volumeMounts": [
                { "name": "workspace", "mountPath": "/workspace", "subPath": TREE_KEY },
                { "name": "git-credentials", "mountPath": "/etc/rivers/git", "readOnly": true },
            ],
        });
        let main = json!({
            "name": "code-location",
            "image": "ghcr.io/rt@sha256:1a2b3c4dff",
            "imagePullPolicy": "IfNotPresent",
            "command": ["/workspace/venv/bin/rivers"],
            "args": ["serve", "analytics.pipeline", "--grpc-port", "3001"],
            "workingDir": "/workspace/src/analytics",
            "ports": [{ "name": "grpc", "containerPort": 3001, "protocol": "TCP" }],
            "resources": {},
            "env": [
                { "name": "RIVERS_CODE_LOCATION_NAME", "value": "analytics" },
                { "name": "RIVERS_CODE_LOCATION_ID", "value": "" },
                { "name": "RIVERS_CODE_LOCATION_IMAGE", "value": "ghcr.io/rt@sha256:1a2b3c4dff" },
                { "name": "RIVERS_MODULE", "value": "analytics.pipeline" },
                { "name": "RIVERS_SURREAL_ENDPOINT", "value": "ws://surrealdb.rivers.svc:8000" },
                { "name": "RIVERS_SURREAL_NAMESPACE", "value": "rivers" },
                { "name": "RIVERS_SURREAL_DATABASE", "value": "main" },
                { "name": "PYTHONPATH", "value": "/workspace/src/analytics" },
                { "name": "VIRTUAL_ENV", "value": "/workspace/venv" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                { "name": "RIVERS_RUN_SOURCE", "value": TREE_SOURCE },
                { "name": "RIVERS_WORKSPACE_EMPTYDIR_LIMIT", "value": "5Gi" },
            ],
            "volumeMounts": [{ "name": "workspace", "mountPath": "/workspace", "subPath": TREE_KEY }],
        });
        let expected = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "analytics",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": expected_labels("analytics") },
                "strategy": { "type": "RollingUpdate", "rollingUpdate": { "maxUnavailable": 0 } },
                "template": {
                    "metadata": { "labels": expected_labels("analytics") },
                    "spec": {
                        "serviceAccountName": "rivers-code-location",
                        "securityContext": { "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch" },
                        "initContainers": [sync],
                        "volumes": [
                            { "name": "workspace", "emptyDir": { "sizeLimit": "5Gi" } },
                            { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 0o440 } },
                        ],
                        "containers": [main],
                    },
                },
            },
        });

        assert_eq!(serde_json::to_value(&d).unwrap(), expected);
    }

    /// Removes the field at JSON `pointer` and returns it.
    fn take(doc: &mut serde_json::Value, pointer: &str) -> Option<serde_json::Value> {
        let (parent, field) = pointer.rsplit_once('/')?;
        doc.pointer_mut(parent)?.as_object_mut()?.remove(field)
    }

    #[test]
    fn git_deployment_is_the_image_deployment_plus_the_workspace() {
        let cl = make_cl(full_git_spec_json(), "analytics", "team-data", "uid-1234");
        let build = |workspace: Option<&rivers_k8s::workspace::WorkspacePodPieces>| {
            serde_json::to_value(build_deployment(
                &cl,
                "ghcr.io/rt@sha256:1a2b3c4dff",
                "rivers-code-location",
                &rivers_k8s::env::SurrealPodConfig::default(),
                &otel_endpoint(),
                workspace,
            ))
            .unwrap()
        };
        let main = "/spec/template/spec/containers/0";
        for (shape, volume) in [("shared", shared_volume()), ("fallback", fallback_volume())] {
            let pieces = git_pieces(&cl, volume);
            let mut image = build(None);
            let mut git = build(Some(&pieces));

            // Git-only fields: image mode must leave them unset, or git mode
            // would silently drop the image-mode value.
            let added = [
                (
                    "/spec/strategy".to_string(),
                    json!({ "type": "RollingUpdate", "rollingUpdate": { "maxUnavailable": 0 } }),
                ),
                (
                    "/spec/template/spec/securityContext".to_string(),
                    json!(pieces.pod_security_context),
                ),
                (
                    "/spec/template/spec/initContainers".to_string(),
                    json!(pieces.init_containers),
                ),
                (
                    "/spec/template/spec/volumes".to_string(),
                    json!(pieces.volumes),
                ),
                (format!("{main}/workingDir"), json!(pieces.main_working_dir)),
                (format!("{main}/volumeMounts"), json!(pieces.main_mounts)),
            ];
            for (pointer, value) in added {
                assert_eq!(take(&mut git, &pointer), Some(value), "{shape}: {pointer}");
                assert_eq!(
                    take(&mut image, &pointer),
                    None,
                    "{shape}: image mode sets {pointer}"
                );
            }

            let command = format!("{main}/command");
            assert_eq!(
                take(&mut git, &command),
                Some(json!(pieces.main_command)),
                "{shape}"
            );
            assert_eq!(
                take(&mut image, &command),
                Some(json!(["rivers"])),
                "{shape}"
            );

            // The workspace env sits between the operator's env and the user's.
            let env = format!("{main}/env");
            let image_env = take(&mut image, &env).unwrap();
            let image_env = image_env.as_array().unwrap();
            let (operator_env, user_env) = image_env.split_at(image_env.len() - cl.spec.env.len());
            assert_eq!(json!(user_env), json!(cl.spec.env), "{shape}");
            let workspace_env = json!(pieces.main_env);
            let expected_env = [operator_env, workspace_env.as_array().unwrap(), user_env].concat();
            assert_eq!(take(&mut git, &env), Some(json!(expected_env)), "{shape}");

            assert_eq!(git, image, "{shape}");
        }
    }

    #[test]
    fn workspace_pvc_matches_golden() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let pvc = build_workspace_pvc(&cl, &Quantity("20Gi".into()), Some("nfs-rwx"));
        let expected = json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": "analytics-workspace",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "storageClassName": "nfs-rwx",
                "resources": { "requests": { "storage": "20Gi" } },
            },
        });
        assert_eq!(serde_json::to_value(&pvc).unwrap(), expected);
    }

    #[test]
    fn keep_config_map_matches_golden() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let cm = build_keep_config_map(&cl, "9f3c1ab8d2e4-1a2b3c4d,7e21bb04c19f-1a2b3c4d");
        let expected = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "analytics-workspace-keep",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "data": { "keep": "9f3c1ab8d2e4-1a2b3c4d,7e21bb04c19f-1a2b3c4d" },
        });
        assert_eq!(serde_json::to_value(&cm).unwrap(), expected);
    }

    #[test]
    fn git_deployment_takes_an_old_pod_down_only_after_its_replacement_is_ready() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let d = build_deployment(
            &cl,
            "ghcr.io/rt@sha256:1a2b3c4dff",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            Some(&git_pieces(&cl, shared_volume())),
        );

        // maxSurge stays the Kubernetes default (25%, at least one pod).
        assert_eq!(
            serde_json::to_value(&d.spec.unwrap().strategy).unwrap(),
            json!({ "type": "RollingUpdate", "rollingUpdate": { "maxUnavailable": 0 } })
        );
    }

    #[test]
    fn image_deployment_keeps_the_default_strategy() {
        let cl = make_cl(
            json!({ "image": "ghcr.io/acme/pipeline", "tag": "v1.0.0" }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let d = build_deployment(
            &cl,
            "ghcr.io/acme/pipeline@sha256:abc",
            "rivers-code-location",
            &rivers_k8s::env::SurrealPodConfig::default(),
            &rivers_k8s::env::OtelPodConfig::default(),
            None,
        );

        assert_eq!(d.spec.unwrap().strategy, None);
    }

    #[test]
    fn git_deployment_sets_the_runtime_fs_group() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let fallback = rivers_k8s::workspace::WorkspaceVolume::EmptyDir { size_limit: None };
        for (shape, volume) in [("shared", shared_volume()), ("fallback", fallback)] {
            let d = build_deployment(
                &cl,
                "ghcr.io/rt@sha256:1a2b3c4dff",
                "rivers-code-location",
                &rivers_k8s::env::SurrealPodConfig::default(),
                &rivers_k8s::env::OtelPodConfig::default(),
                Some(&git_pieces(&cl, volume)),
            );
            let pod = d.spec.unwrap().template.spec.unwrap();

            assert_eq!(
                serde_json::to_value(&pod.security_context).unwrap(),
                json!({ "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch" }),
                "{shape}"
            );
            let modes: Vec<_> = pod
                .volumes
                .unwrap()
                .into_iter()
                .filter(|v| v.name == "git-credentials")
                .map(|v| v.secret.unwrap().default_mode)
                .collect();
            assert_eq!(modes, vec![Some(0o440)], "{shape}");
        }
    }

    #[test]
    fn git_deployment_hands_its_tree_to_the_runs_it_launches() {
        let cl = make_cl(git_spec_json(), "analytics", "team-data", "uid-1234");
        let expected_source: rivers_k8s::crd::run::RunSource = serde_json::from_value(json!({
            "git": {
                "url": "https://forge.example/acme/pipelines.git",
                "commit": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
                "ref": "refs/heads/main",
                "path": "analytics",
                "secretName": "git-creds",
            },
            "dependencies": { "mode": "uvSync", "extras": ["ml"] },
            "runtimeImage": "ghcr.io/rt@sha256:1a2b3c4dff",
        }))
        .unwrap();
        let fallback = rivers_k8s::workspace::WorkspaceVolume::EmptyDir {
            size_limit: Some(Quantity("5Gi".to_string())),
        };

        for (volume, pvc, emptydir_limit) in [
            (shared_volume(), Some("analytics-workspace"), None),
            (fallback, None, Some("5Gi")),
        ] {
            let d = build_deployment(
                &cl,
                "ghcr.io/rt@sha256:1a2b3c4dff",
                "rivers-code-location",
                &rivers_k8s::env::SurrealPodConfig::default(),
                &rivers_k8s::env::OtelPodConfig::default(),
                Some(&git_pieces(&cl, volume)),
            );
            let pod = d.spec.unwrap().template.spec.unwrap();
            let env: std::collections::HashMap<String, Option<String>> = pod.containers[0]
                .env
                .clone()
                .unwrap()
                .into_iter()
                .map(|e| (e.name, e.value))
                .collect();
            let value = |name: &str| env.get(name).cloned().flatten();

            let source: Option<rivers_k8s::crd::run::RunSource> =
                value("RIVERS_RUN_SOURCE").map(|json| serde_json::from_str(&json).unwrap());
            assert_eq!(source.as_ref(), Some(&expected_source), "{pvc:?}");
            assert_eq!(value("RIVERS_WORKSPACE_PVC").as_deref(), pvc);
            assert_eq!(
                value("RIVERS_WORKSPACE_EMPTYDIR_LIMIT").as_deref(),
                emptydir_limit
            );
        }
    }

    #[test]
    fn service_matches_golden() {
        let cl = make_cl(
            json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v1.0.0",
                "module": "acme.pipeline",
                "grpcPort": 50051,
            }),
            "analytics",
            "team-data",
            "uid-1234",
        );
        let s = build_service(&cl);

        let expected = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "analytics-grpc",
                "namespace": "team-data",
                "labels": expected_labels("analytics"),
                "ownerReferences": [expected_owner_ref("analytics", "uid-1234")],
            },
            "spec": {
                "type": "ClusterIP",
                "selector": expected_labels("analytics"),
                "ports": [{
                    "name": "grpc",
                    "port": 50051,
                    "targetPort": 50051,
                    "protocol": "TCP",
                }],
            },
        });

        assert_eq!(serde_json::to_value(&s).unwrap(), expected);
    }
}
