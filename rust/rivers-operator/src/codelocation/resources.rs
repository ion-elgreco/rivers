//! Builders for the `Deployment` and `Service` owned by a `CodeLocation` CR.
//!
//! These functions are deliberately pure — they take the CR + resolved image
//! and emit the desired object. The reconciler handles apply semantics.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, PodSpec, PodTemplateSpec, Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube_client::ResourceExt;
use rivers_k8s::crd::code_location::CodeLocation;

pub const COMPONENT_LABEL: &str = "code-location";
pub const MANAGED_BY: &str = "rivers-operator";

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
/// coming from `status.resolvedImage`. The container image is pinned to this
/// value so rolling the tag upstream has no effect on running pods.
pub fn build_deployment(
    cl: &CodeLocation,
    resolved_image: &str,
    code_location_service_account: &str,
    surreal_pod_cfg: &rivers_k8s::env::SurrealPodConfig,
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
                    containers: vec![Container {
                        name: "code-location".to_string(),
                        image: Some(resolved_image.to_string()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        command: Some(vec!["rivers".to_string()]),
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
                        ports: Some(vec![ContainerPort {
                            name: Some("grpc".to_string()),
                            container_port: spec.grpc_port,
                            protocol: Some("TCP".to_string()),
                            ..Default::default()
                        }]),
                        resources: Some(spec.resources.clone()),
                        env: Some(build_env(cl, resolved_image, surreal_pod_cfg)),
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

fn build_env(
    cl: &CodeLocation,
    resolved_image: &str,
    surreal_pod_cfg: &rivers_k8s::env::SurrealPodConfig,
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
    env.extend(cl.spec.env.iter().cloned());
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
