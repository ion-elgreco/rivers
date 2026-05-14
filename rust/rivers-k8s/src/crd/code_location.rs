//! `CodeLocation` CRD.
//!
//! Container-shaped fields (`resources`, `env`, `imagePullSecrets`) are
//! **upstream `k8s_openapi` types**, not local redefinitions — users get the
//! full, stable Kubernetes surface. See:
//!   * <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.29/#resourcerequirements-v1-core>
//!   * <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.29/#envvar-v1-core>
//!   * <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.29/#localobjectreference-v1-core>
//!
//! The CRD YAML is regenerated from these types via `cargo run --bin
//! rivers-gen-crd` (see `src/bin/gen_crd.rs`) rather than hand-maintained.

use k8s_openapi::api::core::v1::{EnvVar, LocalObjectReference, ResourceRequirements};
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const IMMUTABLE_TAG_ANNOTATION: &str = "rivers.io/tag-immutable";

pub const CONDITION_IMAGE_RESOLVED: &str = "ImageResolved";
pub const CONDITION_DEPLOYMENT_AVAILABLE: &str = "DeploymentAvailable";

pub const REASON_DIGEST_RESOLVED: &str = "DigestResolved";
pub const REASON_DIGEST_PINNED: &str = "DigestPinned";
pub const REASON_TAG_NOT_FOUND: &str = "TagNotFound";
pub const REASON_AUTH_FAILED: &str = "AuthenticationFailed";
pub const REASON_RATE_LIMITED: &str = "RateLimited";
pub const REASON_REGISTRY_ERROR: &str = "RegistryError";
pub const REASON_MIN_REPLICAS: &str = "MinimumReplicasAvailable";
pub const REASON_PROGRESS_DEADLINE: &str = "ProgressDeadlineExceeded";
pub const REASON_ROLLING_OUT: &str = "RollingOut";
pub const REASON_NO_DEPLOYMENT_STATUS: &str = "NoDeploymentStatus";
pub const REASON_AWAITING_LEADER: &str = "AwaitingLeader";

#[derive(CustomResource, Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "rivers.io",
    version = "v1alpha1",
    kind = "CodeLocation",
    plural = "codelocations",
    singular = "codelocation",
    shortname = "rcl",
    category = "rivers",
    namespaced,
    status = "CodeLocationStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Image","type":"string","jsonPath":".status.resolvedImage"}"#,
    printcolumn = r#"{"name":"Replicas","type":"string","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#,
    crates(kube_core = "::kube_core")
)]
#[serde(rename_all = "camelCase")]
pub struct CodeLocationSpec {
    /// Stable opaque identity of this CodeLocation (UUID v4) — used as the
    /// storage key for every per-CL row in the shared SurrealDB.
    /// Distinct from `metadata.uid` (which is regenerated on every recreate)
    /// so the same logical CodeLocation can survive a namespace move or a
    /// `kubectl delete && kubectl apply` round-trip without orphaning its
    /// stored data.
    ///
    /// The mutating admission webhook auto-stamps a fresh UUID on CREATE if
    /// this is empty; the validating admission webhook rejects any change to
    /// it on UPDATE. Stripping the field after creation orphans all stored
    /// data — preserve it when migrating between namespaces:
    ///
    /// `kubectl get codelocation foo -n old -o yaml | yq '.metadata.namespace = "new"' | kubectl apply -f -`
    /// then `kubectl delete codelocation foo -n old`.
    #[serde(default)]
    pub identity: String,

    /// OCI image repository without tag or digest (e.g. `ghcr.io/acme/pipeline`).
    pub image: String,

    /// Tag to resolve to a digest. Ignored when `digest` is set. Defaults to
    /// `latest` if both are omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,

    /// Authoritative digest reference (`sha256:...`). When set, the operator
    /// skips registry lookup entirely and uses this value as
    /// `status.resolvedImage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,

    /// Python module path exporting a `CodeRepository`.
    #[serde(default = "default_module")]
    pub module: String,

    /// Number of code-location pod replicas.
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// How often the operator re-polls the registry. Accepts duration
    /// suffixes `s`, `m`, `h`. Minimum 60s. Immutable-looking tags (semver)
    /// are polled once and cached regardless of this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest_refresh_interval: Option<String>,

    /// Resource requests/limits for the code-location container. Pass-through
    /// to the Pod's container spec; see upstream Kubernetes API reference.
    #[serde(default, skip_serializing_if = "resource_requirements_is_empty")]
    pub resources: ResourceRequirements,

    /// ServiceAccount the code-location pod runs under. Falls back to the
    /// operator's default when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,

    /// Upstream `LocalObjectReference` list, wired directly to
    /// `PodSpec.imagePullSecrets`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,

    /// Upstream `EnvVar` list, wired directly to the container's `env`.
    /// Supports the full k8s envvar surface (`value`, `valueFrom.secretKeyRef`,
    /// `valueFrom.configMapKeyRef`, `valueFrom.fieldRef`, etc.).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,

    /// gRPC port the `rivers serve` subcommand binds.
    #[serde(default = "default_grpc_port")]
    pub grpc_port: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CodeLocationStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<CodeLocationPhase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Fully-qualified digest reference (`repo@sha256:...`) that the
    /// reconciler has pinned. For multi-arch images this is the index digest,
    /// so each node can still descend into per-platform manifests at pull time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_image: Option<String>,

    /// In-cluster DNS + port of the backing Service,
    /// e.g. `analytics.team-data.svc:50051`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconciled: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<CodeLocationCondition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum CodeLocationPhase {
    Pending,
    Deploying,
    Ready,
    Failed,
}

impl CodeLocationPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            CodeLocationPhase::Pending => "Pending",
            CodeLocationPhase::Deploying => "Deploying",
            CodeLocationPhase::Ready => "Ready",
            CodeLocationPhase::Failed => "Failed",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodeLocationCondition {
    pub r#type: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl CodeLocationSpec {
    pub fn has_pinned_digest(&self) -> bool {
        self.digest.as_deref().is_some_and(|d| !d.is_empty())
    }

    /// Defaults to "latest" when neither tag nor digest is supplied.
    pub fn effective_tag(&self) -> &str {
        self.tag.as_deref().unwrap_or("latest")
    }
}

fn resource_requirements_is_empty(r: &ResourceRequirements) -> bool {
    r.requests.as_ref().is_none_or(|m| m.is_empty())
        && r.limits.as_ref().is_none_or(|m| m.is_empty())
        && r.claims.as_ref().is_none_or(|v| v.is_empty())
}

fn default_module() -> String {
    crate::defaults::MODULE.to_string()
}

fn default_replicas() -> i32 {
    1
}

fn default_grpc_port() -> i32 {
    3001
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{EnvVarSource, SecretKeySelector};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::collections::BTreeMap;

    #[test]
    fn minimal_spec_parses_without_resources() {
        // No resources in the CR → empty ResourceRequirements (no auto-fill).
        // Full k8s compat means no magic operator defaults for resources.
        let json = serde_json::json!({
            "image": "ghcr.io/acme/pipeline",
            "tag": "v1.0.0"
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();

        assert_eq!(spec.image, "ghcr.io/acme/pipeline");
        assert_eq!(spec.tag.as_deref(), Some("v1.0.0"));
        assert_eq!(spec.digest, None);
        assert_eq!(spec.module, crate::defaults::MODULE);
        assert_eq!(spec.replicas, 1);
        assert_eq!(spec.grpc_port, 3001);
        assert!(resource_requirements_is_empty(&spec.resources));
        assert!(spec.service_account_name.is_none());
        assert!(spec.image_pull_secrets.is_empty());
        assert!(spec.env.is_empty());
    }

    #[test]
    fn has_pinned_digest() {
        let tag_only: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "img", "tag": "v1"
        }))
        .unwrap();
        assert!(!tag_only.has_pinned_digest());

        let digest: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "img", "digest": "sha256:abc"
        }))
        .unwrap();
        assert!(digest.has_pinned_digest());

        let empty_digest: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "img", "digest": ""
        }))
        .unwrap();
        assert!(!empty_digest.has_pinned_digest());
    }

    #[test]
    fn effective_tag_fallback() {
        let no_tag: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "img"
        }))
        .unwrap();
        assert_eq!(no_tag.effective_tag(), "latest");

        let tagged: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "img", "tag": "v2.0.0"
        }))
        .unwrap();
        assert_eq!(tagged.effective_tag(), "v2.0.0");
    }

    #[test]
    fn camel_case_serialization() {
        let json = serde_json::json!({
            "image": "img",
            "tag": "v1",
            "serviceAccountName": "custom-sa",
            "imagePullSecrets": [{"name": "ghcr-creds"}],
            "digestRefreshInterval": "10m",
            "grpcPort": 5000
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();

        assert_eq!(spec.service_account_name.as_deref(), Some("custom-sa"));
        assert_eq!(spec.image_pull_secrets.len(), 1);
        assert_eq!(spec.image_pull_secrets[0].name, "ghcr-creds");
        assert_eq!(spec.digest_refresh_interval.as_deref(), Some("10m"));
        assert_eq!(spec.grpc_port, 5000);

        let re_json = serde_json::to_value(&spec).unwrap();
        assert!(re_json.get("serviceAccountName").is_some());
        assert!(re_json.get("imagePullSecrets").is_some());
        assert!(re_json.get("service_account_name").is_none());
    }

    #[test]
    fn resources_requests_and_limits_parse_independently() {
        let json = serde_json::json!({
            "image": "img",
            "resources": {
                "requests": { "cpu": "100m", "memory": "256Mi" },
                "limits":   { "cpu": "500m", "memory": "1Gi" }
            }
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();

        let req: &BTreeMap<String, Quantity> = spec.resources.requests.as_ref().unwrap();
        let lim: &BTreeMap<String, Quantity> = spec.resources.limits.as_ref().unwrap();
        assert_eq!(req.get("cpu").map(|q| q.0.as_str()), Some("100m"));
        assert_eq!(req.get("memory").map(|q| q.0.as_str()), Some("256Mi"));
        assert_eq!(lim.get("cpu").map(|q| q.0.as_str()), Some("500m"));
        assert_eq!(lim.get("memory").map(|q| q.0.as_str()), Some("1Gi"));
    }

    #[test]
    fn resources_can_omit_one_side() {
        let json = serde_json::json!({
            "image": "img",
            "resources": { "requests": { "cpu": "250m" } }
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();
        let req = spec.resources.requests.as_ref().unwrap();
        assert_eq!(req.get("cpu").map(|q| q.0.as_str()), Some("250m"));
        assert!(req.get("memory").is_none());
        assert!(
            spec.resources.limits.is_none() || spec.resources.limits.as_ref().unwrap().is_empty()
        );
    }

    #[test]
    fn env_accepts_full_upstream_shape() {
        // Exercise the parts of EnvVar we previously had to hand-roll.
        let json = serde_json::json!({
            "image": "img",
            "env": [
                {"name": "AWS_REGION", "value": "us-east-1"},
                {
                    "name": "TOKEN",
                    "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}
                },
                {
                    "name": "POD_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}
                }
            ]
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec.env.len(), 3);
        assert_eq!(spec.env[0].value.as_deref(), Some("us-east-1"));
        let secret: &EnvVarSource = spec.env[1].value_from.as_ref().unwrap();
        let s: &SecretKeySelector = secret.secret_key_ref.as_ref().unwrap();
        assert_eq!(s.name, "s");
        assert_eq!(s.key, "k");
        // fieldRef is part of the upstream shape now — we didn't support it
        // before.
        let field = spec.env[2].value_from.as_ref().unwrap();
        let fr = field.field_ref.as_ref().unwrap();
        assert_eq!(fr.field_path, "metadata.name");
    }

    #[test]
    fn optional_fields_omitted_when_none() {
        let json = serde_json::json!({
            "image": "img",
            "tag": "v1"
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();
        let serialized = serde_json::to_value(&spec).unwrap();

        assert!(serialized.get("digest").is_none());
        assert!(serialized.get("serviceAccountName").is_none());
        assert!(serialized.get("digestRefreshInterval").is_none());
        assert!(serialized.get("imagePullSecrets").is_none());
        assert!(serialized.get("env").is_none());
        // Empty resources round-trip to omission.
        assert!(serialized.get("resources").is_none());
    }

    #[test]
    fn phase_is_enum_camel_case() {
        let v = serde_json::to_value(CodeLocationPhase::Ready).unwrap();
        assert_eq!(v, serde_json::Value::String("Ready".to_string()));
    }
}
