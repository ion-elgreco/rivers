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
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const IMMUTABLE_TAG_ANNOTATION: &str = "rivers.io/tag-immutable";

pub const CONDITION_IMAGE_RESOLVED: &str = "ImageResolved";
pub const CONDITION_SOURCE_RESOLVED: &str = "SourceResolved";
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

// git source (RFC-044)
pub const REASON_COMMIT_RESOLVED: &str = "CommitResolved";
pub const REASON_COMMIT_PINNED: &str = "CommitPinned";
pub const REASON_REF_NOT_FOUND: &str = "RefNotFound";
pub const REASON_GIT_AUTH_FAILED: &str = "GitAuthFailed";
pub const REASON_GIT_UNREACHABLE: &str = "GitUnreachable";
pub const REASON_GIT_RATE_LIMITED: &str = "GitRateLimited";
pub const REASON_GIT_HOST_KEY_REJECTED: &str = "GitHostKeyRejected";
pub const REASON_GIT_MALFORMED_RESPONSE: &str = "GitMalformedResponse";

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
    printcolumn = r#"{"name":"Source","type":"string","jsonPath":".status.source"}"#,
    printcolumn = r#"{"name":"Replicas","type":"string","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#,
    crates(kube_core = "::kube_core")
)]
#[serde(rename_all = "camelCase")]
// At least one of `image` / `git` must be set. Not an xor: `image` + `git`
// together means "run this runtime image, take the code from git". Enforced
// here via CEL so a bad CR fails at `kubectl apply` even when the admission
// webhook is down; the webhook re-checks it for older API servers.
#[schemars(extend("x-kubernetes-validations" = [
    {"rule": "has(self.image) || has(self.git)",
     "message": "spec.image is required unless spec.git is set"},
]))]
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
    ///
    /// This is always "the container image the pods run". Without `git` it
    /// also carries the code (today's behaviour). With `git` set it is the
    /// *runtime* image and the code comes from the repository; when omitted
    /// in git mode, the operator falls back to the chart-configured default
    /// runtime image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Git source for the pipeline code (RFC-044). Mutually composable with
    /// `image` (see there); at least one of the two must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitSource>,

    /// Tag to resolve to a digest. Ignored when `digest` is set. Defaults to
    /// `latest` if both are omitted, except in git mode without `image`: the
    /// chart's default runtime image then keeps its own tag or digest.
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

/// Git source declaration. Field names follow Flux's `GitRepository` so the
/// shape is familiar to platform engineers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitSource {
    /// `https://host/org/repo.git` or `ssh://git@host/org/repo.git`.
    pub url: String,

    /// Which revision to pin. Exactly one field set.
    pub r#ref: GitRef,

    /// Subdirectory of the repo holding the project. Becomes the container's
    /// `workingDir`, so `spec.module` is imported relative to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Credentials Secret in the same namespace. Key names follow Flux:
    /// `username`/`password` for HTTPS, `identity` + `known_hosts` for SSH.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,

    /// How often the operator re-resolves the ref. Same parser and 60s floor
    /// as `digestRefreshInterval`. Ignored when `ref.commit` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<String>,

    #[serde(default)]
    pub dependencies: Dependencies,

    /// Per-CL override for the workspace volume size: the PVC request in
    /// shared mode, the `emptyDir` cap in fallback mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_size: Option<Quantity>,
}

/// One-of revision selector. The CEL rule enforces exactly-one at admission;
/// [`GitRef::validate`] re-checks it server-side for the webhook path.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-kubernetes-validations" = [
    {"rule": "(has(self.branch) ? 1 : 0) + (has(self.tag) ? 1 : 0) + (has(self.commit) ? 1 : 0) == 1",
     "message": "exactly one of branch, tag or commit must be set"},
]))]
pub struct GitRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,

    /// Full 40-hex commit. Pinned: no polling, no network at reconcile time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

impl GitRef {
    /// Exactly one non-empty field, and `commit` must be a full 40-hex SHA.
    pub fn validate(&self) -> Result<(), String> {
        let set = [
            self.branch.as_deref(),
            self.tag.as_deref(),
            self.commit.as_deref(),
        ]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .count();
        if set != 1 {
            return Err(format!(
                "exactly one of git.ref.branch / tag / commit must be set (got {set})"
            ));
        }
        if let Some(commit) = self.commit.as_deref().filter(|s| !s.is_empty()) {
            if commit.len() != 40 || !commit.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(format!(
                    "git.ref.commit '{commit}' is not a full 40-hex commit SHA"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Dependencies {
    /// `auto` reads `path`: `uvSync` when `uv.lock` exists, else
    /// `requirements` when `requirements.txt` exists, else `uvSync` when
    /// `pyproject.toml` exists and the nearest `uv.lock` above it, up to the
    /// repository root, is next to a `pyproject.toml` with
    /// `[tool.uv.workspace]` (a uv workspace member), else installs nothing.
    /// Detection happens in the workspace pod — the operator never reads repo
    /// contents.
    #[serde(default)]
    pub mode: DependencyMode,

    /// `requirements` mode: files relative to `path`. Default `requirements.txt`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,

    /// `uvSync` mode: `--extra` passthrough.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extras: Vec<String>,

    /// `uvSync` mode: `--group` passthrough.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,

    /// Wall-clock budget for fetch + install. Default 600s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DependencyMode {
    #[default]
    Auto,
    UvSync,
    Requirements,
    /// Fetch the code, install nothing — the runtime image carries the deps.
    /// Unlike `auto` with no lockfile, this ignores a lockfile that *is*
    /// present; escape hatch for platform-curated runtimes only.
    None,
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
    /// In git mode, `runSource.runtimeImage`.
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

    /// Short display of the pinned code version, for the print column.
    /// `ghcr.io/acme/p@sha256:abc1234` (image) or `main@9f3c1ab` (git:
    /// `runSource`'s commit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    /// git mode: full 40-hex commit of `runSource`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_commit: Option<String>,

    /// git mode: the ref `runSource`'s commit came from, e.g.
    /// `refs/heads/main`; absent for a pinned commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_ref: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fetched_at: Option<String>,

    /// git mode: the source runs get — that of the last tree every
    /// code-location pod ran and was ready on. A newer commit takes over
    /// only when its rollout finishes; until then, and for good if it fails
    /// to build, runs keep this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_source: Option<crate::crd::run::RunSource>,
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
    /// True when the pipeline code comes from git rather than being baked
    /// into `image`.
    pub fn is_git(&self) -> bool {
        self.git.is_some()
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
    use kube_client::CustomResourceExt;
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

        assert_eq!(spec.image.as_deref(), Some("ghcr.io/acme/pipeline"));
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

    // ---- git source (RFC-044) ----

    #[test]
    fn git_source_parses_camel_case() {
        let json = serde_json::json!({
            "git": {
                "url": "https://github.com/acme/pipelines.git",
                "ref": { "branch": "main" },
                "path": "analytics",
                "secretRef": { "name": "git-creds" },
                "pollInterval": "2m",
                "dependencies": { "mode": "uvSync", "extras": ["dev"], "groups": ["test"] },
                "workspaceSize": "5Gi"
            },
            "module": "analytics.pipeline"
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();

        assert!(spec.image.is_none());
        assert!(spec.is_git());
        let git = spec.git.as_ref().unwrap();
        assert_eq!(git.url, "https://github.com/acme/pipelines.git");
        assert_eq!(git.r#ref.branch.as_deref(), Some("main"));
        assert_eq!(git.r#ref.tag, None);
        assert_eq!(git.r#ref.commit, None);
        assert_eq!(git.path.as_deref(), Some("analytics"));
        assert_eq!(git.secret_ref.as_ref().unwrap().name, "git-creds");
        assert_eq!(git.poll_interval.as_deref(), Some("2m"));
        assert_eq!(git.dependencies.mode, DependencyMode::UvSync);
        assert_eq!(git.dependencies.extras, vec!["dev".to_string()]);
        assert_eq!(git.dependencies.groups, vec!["test".to_string()]);
        assert_eq!(git.workspace_size.as_ref().unwrap().0, "5Gi");
    }

    #[test]
    fn git_dependencies_default_to_auto() {
        let json = serde_json::json!({
            "git": { "url": "https://x/y.git", "ref": { "tag": "v1.2.3" } }
        });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();
        let deps = &spec.git.as_ref().unwrap().dependencies;
        assert_eq!(deps.mode, DependencyMode::Auto);
        assert!(deps.files.is_empty());
        assert!(deps.extras.is_empty());
        assert!(deps.groups.is_empty());
        assert_eq!(deps.timeout_seconds, None);
    }

    #[test]
    fn dependency_mode_serde_names() {
        for (variant, name) in [
            (DependencyMode::Auto, "auto"),
            (DependencyMode::UvSync, "uvSync"),
            (DependencyMode::Requirements, "requirements"),
            (DependencyMode::None, "none"),
        ] {
            assert_eq!(
                serde_json::to_value(variant).unwrap(),
                serde_json::Value::String(name.to_string()),
            );
        }
    }

    #[test]
    fn image_mode_still_parses_and_is_not_git() {
        let json = serde_json::json!({ "image": "ghcr.io/acme/pipeline", "tag": "v1" });
        let spec: CodeLocationSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec.image.as_deref(), Some("ghcr.io/acme/pipeline"));
        assert!(spec.git.is_none());
        assert!(!spec.is_git());
    }

    #[test]
    fn absent_image_is_omitted_from_serialization() {
        let git_only: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "git": { "url": "https://x/y.git", "ref": { "branch": "main" } }
        }))
        .unwrap();
        let ser = serde_json::to_value(&git_only).unwrap();
        assert!(
            ser.get("image").is_none(),
            "None image must serialize absent so CEL has() works"
        );
        assert!(ser.get("git").is_some());
    }

    #[test]
    fn git_ref_one_of_validation() {
        fn r(json: serde_json::Value) -> GitRef {
            serde_json::from_value(json).unwrap()
        }
        assert!(
            r(serde_json::json!({ "branch": "main" }))
                .validate()
                .is_ok()
        );
        assert!(r(serde_json::json!({ "tag": "v1.0.0" })).validate().is_ok());
        assert!(
            r(serde_json::json!({ "commit": "a".repeat(40) }))
                .validate()
                .is_ok()
        );

        assert!(r(serde_json::json!({})).validate().is_err());
        assert!(
            r(serde_json::json!({ "branch": "main", "tag": "v1" }))
                .validate()
                .is_err()
        );
        assert!(
            r(serde_json::json!({ "branch": "b", "tag": "t", "commit": "c".repeat(40) }))
                .validate()
                .is_err()
        );
        // Empty strings count as unset, not as a chosen field.
        assert!(r(serde_json::json!({ "branch": "" })).validate().is_err());
        // Commit must be a full 40-hex SHA.
        assert!(
            r(serde_json::json!({ "commit": "abc123" }))
                .validate()
                .is_err()
        );
        assert!(
            r(serde_json::json!({ "commit": "g".repeat(40) }))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn crd_schema_carries_source_cel_rules() {
        let crd = serde_json::to_value(CodeLocation::crd()).unwrap();
        let spec_schema =
            &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];

        let rules = spec_schema["x-kubernetes-validations"]
            .as_array()
            .expect("spec-level x-kubernetes-validations present");
        assert!(
            rules
                .iter()
                .any(|r| r["rule"] == "has(self.image) || has(self.git)"),
            "at-least-one source rule missing: {rules:?}"
        );

        let ref_rules =
            spec_schema["properties"]["git"]["properties"]["ref"]["x-kubernetes-validations"]
                .as_array()
                .expect("ref-level x-kubernetes-validations present");
        assert!(
            ref_rules.iter().any(|r| r["rule"]
                .as_str()
                .is_some_and(|s| s.contains("has(self.branch)"))),
            "ref one-of rule missing: {ref_rules:?}"
        );
    }

    #[test]
    fn crd_print_columns_use_source_not_image() {
        let crd = serde_json::to_value(CodeLocation::crd()).unwrap();
        let cols = crd["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_array()
            .unwrap();
        assert!(
            cols.iter()
                .any(|c| c["name"] == "Source" && c["jsonPath"] == ".status.source"),
            "Source column missing: {cols:?}"
        );
        assert!(
            !cols.iter().any(|c| c["name"] == "Image"),
            "Image column should be replaced by Source"
        );
    }

    #[test]
    fn status_git_fields_round_trip_camel_case() {
        let run_source: crate::crd::run::RunSource = serde_json::from_value(serde_json::json!({
            "git": {
                "url": "https://forge.example/acme/pipelines.git",
                "commit": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
                "ref": "refs/heads/main",
                "secretName": "git-creds",
            },
            "runtimeImage": "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff",
        }))
        .unwrap();
        let status = CodeLocationStatus {
            source: Some("main@9f3c1ab".to_string()),
            resolved_commit: Some("9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string()),
            resolved_ref: Some("refs/heads/main".to_string()),
            last_fetched_at: Some("2026-07-26T00:00:00Z".to_string()),
            run_source: Some(run_source.clone()),
            ..Default::default()
        };
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(v["source"], "main@9f3c1ab");
        assert_eq!(
            v["resolvedCommit"],
            "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8"
        );
        assert_eq!(v["resolvedRef"], "refs/heads/main");
        assert_eq!(v["lastFetchedAt"], "2026-07-26T00:00:00Z");
        assert_eq!(v["runSource"]["git"]["secretName"], "git-creds");
        let back: CodeLocationStatus = serde_json::from_value(v).unwrap();
        assert_eq!(back.run_source, Some(run_source));

        let empty = serde_json::to_value(CodeLocationStatus::default()).unwrap();
        assert!(empty.get("source").is_none());
        assert!(empty.get("resolvedCommit").is_none());
        assert!(empty.get("resolvedRef").is_none());
        assert!(empty.get("lastFetchedAt").is_none());
        assert!(empty.get("runSource").is_none());
    }
}
