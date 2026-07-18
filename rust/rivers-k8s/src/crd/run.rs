use kube_derive::CustomResource;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

pub const CANCEL_ANNOTATION: &str = "rivers.io/cancel-requested";

pub const CONDITION_EXECUTOR_READY: &str = "ExecutorReady";
pub const CONDITION_EXECUTOR_RESTARTED: &str = "ExecutorRestarted";
pub const CONDITION_CANCELLING: &str = "Cancelling";

#[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "rivers.io",
    version = "v1alpha1",
    kind = "Run",
    plural = "runs",
    singular = "run",
    shortname = "rr",
    category = "rivers",
    namespaced,
    status = "RunCrdStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.target"}"#,
    printcolumn = r#"{"name":"Steps","type":"string","jsonPath":".status.completedSteps"}"#,
    printcolumn = r#"{"name":"Restarts","type":"string","jsonPath":".status.restartsWithoutProgress"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#,
    crates(kube_core = "::kube_core")
)]
#[serde(rename_all = "camelCase")]
// `extend("required" = [...])` REPLACES the schema's `required:` list, so
// we list every field that should be required on apply: `target` (which
// schemars infers from the absence of a default) plus `codeLocationRef`
// (which we want admission to reject when missing, even though its
// `serde(default)` would otherwise mark it optional — the default is
// needed so internal Run snapshots and operator status updates that
// never carry the spec round-trip without failing).
#[schemars(extend("required" = ["codeLocationRef", "target"]))]
pub struct RunSpec {
    /// Reference to the CodeLocation this run belongs to. The admission
    /// webhook stamps `image` and `module` from the referenced CodeLocation's
    /// `status.resolvedImage` and `spec.module` on CREATE; power-users can
    /// set `image` directly with a digest as an escape hatch.
    #[serde(default)]
    pub code_location_ref: CodeLocationRef,

    /// Fully-qualified pinned digest reference (`registry/repo@sha256:...`)
    /// populated by the admission webhook from
    /// `CodeLocation.status.resolvedImage`. Immutable after creation. Empty
    /// string is acceptable on apply — webhook fills it.
    #[serde(default)]
    pub image: String,

    /// Python module path. Populated by the admission webhook from
    /// `CodeLocation.spec.module`. Immutable after creation.
    #[serde(default = "default_module")]
    pub module: String,

    /// Asset or job to materialize/execute.
    pub target: String,

    /// Job this run executes, when it was dispatched as one. The run pod then
    /// goes through the job path so job-level config (retry, executor)
    /// applies; absent for plain asset-selection runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,

    #[serde(default = "default_surreal_endpoint")]
    pub surreal_endpoint: String,

    /// Which executor backend runs the steps. `kubernetes` (default) launches
    /// per-step pods; `parallel` runs in worker subprocesses inside the
    /// executor pod; `in_process` runs them in-thread (mostly for tests).
    #[serde(default)]
    pub executor: Executor,

    /// Free-form parameters object passed through to the user pipeline.
    /// `x-kubernetes-preserve-unknown-fields: true` so admission accepts
    /// arbitrary nested structure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "preserve_unknown_object_schema")]
    pub parameters: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,

    #[serde(default = "default_cancel_grace_period")]
    pub cancel_grace_period_seconds: u64,

    #[serde(default)]
    pub run_resources: ResourceSpec,

    #[serde(default)]
    pub worker_resources: ResourceSpec,

    #[serde(default = "default_max_concurrent_steps")]
    pub max_concurrent_steps: u32,

    #[serde(default = "default_service_account")]
    pub service_account_name: String,
}

/// Reference to a `CodeLocation` CR. Same-namespace only: every real caller
/// (the operator-deployed daemon, the namespaced UI routes) creates Runs in
/// the CodeLocation's own namespace, and the operator's reflector is itself
/// namespace-scoped — so cross-namespace refs would always cache-miss. If a
/// cluster-wide operator mode is ever introduced, add `namespace: Option`
/// here at the same time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CodeLocationRef {
    pub name: String,

    /// Stable identity (UUID v4) of the referenced CodeLocation, copied from
    /// `CodeLocation.spec.identity` by the Run admission webhook on CREATE.
    /// Observability-only metadata — the executor pod scopes its storage
    /// from the operator-injected `RIVERS_CODE_LOCATION_ID` env, not from
    /// this field. Skipped on the digest escape-hatch path. Immutable after
    /// the webhook stamps it; the Run UPDATE check rejects any change.
    #[serde(default)]
    pub identity: String,
}

/// True if `reference` is a fully-pinned OCI digest reference of the form
/// `registry/repo@sha256:<64 lowercase hex>`. Daemons that bypass the
/// CodeLocation lookup must hand-pin a digest, not a tag.
pub fn is_digest_reference(reference: &str) -> bool {
    let Some(idx) = reference.find("@sha256:") else {
        return false;
    };
    let digest = &reference[idx + "@sha256:".len()..];
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpec {
    #[serde(default = "default_run_cpu")]
    pub cpu: String,
    #[serde(default = "default_run_memory")]
    pub memory: String,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            cpu: crate::defaults::RUN_CPU.to_string(),
            memory: crate::defaults::RUN_MEMORY.to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunCrdStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RunPhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_pod: Option<String>,
    #[serde(default)]
    pub restarts_without_progress: u32,
    /// Lifetime restart count — never reset, so every restart pod gets a
    /// fresh `-executor-<n>` name (`restartsWithoutProgress` resets on
    /// progress and would repeat names, 409-adopting an old completed pod).
    #[serde(default)]
    pub total_restarts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_steps: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_steps: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<RunCondition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum RunPhase {
    Pending,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl RunPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunCondition {
    pub r#type: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Executor backend selection for a Run. Generated CRD emits the
/// `enum: [in_process, parallel, kubernetes]` admission constraint
/// automatically from this type.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Executor {
    InProcess,
    Parallel,
    #[default]
    Kubernetes,
}

/// Custom schema for `RunSpec.parameters`: `type: object` with
/// `x-kubernetes-preserve-unknown-fields: true` so admission accepts
/// any nested structure inside the user-supplied parameters blob.
fn preserve_unknown_object_schema(_gen: &mut SchemaGenerator) -> Schema {
    serde_json::from_value(serde_json::json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
    }))
    .expect("preserve-unknown-fields schema is valid JSON Schema")
}

fn default_module() -> String {
    crate::defaults::MODULE.to_string()
}
fn default_surreal_endpoint() -> String {
    crate::defaults::SURREAL_ENDPOINT.to_string()
}
fn default_max_restarts() -> u32 {
    crate::defaults::MAX_RESTARTS
}
fn default_cancel_grace_period() -> u64 {
    crate::defaults::CANCEL_GRACE_PERIOD
}
fn default_max_concurrent_steps() -> u32 {
    crate::defaults::MAX_CONCURRENT_STEPS
}
fn default_service_account() -> String {
    crate::defaults::SERVICE_ACCOUNT.to_string()
}
fn default_run_cpu() -> String {
    crate::defaults::RUN_CPU.to_string()
}
fn default_run_memory() -> String {
    crate::defaults::RUN_MEMORY.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defaults;

    #[test]
    fn minimal_spec_gets_all_defaults() {
        let json = serde_json::json!({
            "codeLocationRef": { "name": "demo" },
            "image": "my-image:latest",
            "target": "asset_a,asset_b"
        });
        let spec: RunSpec = serde_json::from_value(json).unwrap();

        assert_eq!(spec.code_location_ref.name, "demo");
        assert_eq!(spec.image, "my-image:latest");
        assert_eq!(spec.target, "asset_a,asset_b");
        assert_eq!(spec.module, defaults::MODULE);
        assert_eq!(spec.surreal_endpoint, defaults::SURREAL_ENDPOINT);
        assert_eq!(spec.executor, Executor::Kubernetes);
        assert_eq!(spec.max_restarts, defaults::MAX_RESTARTS);
        assert_eq!(
            spec.cancel_grace_period_seconds,
            defaults::CANCEL_GRACE_PERIOD
        );
        assert_eq!(spec.max_concurrent_steps, defaults::MAX_CONCURRENT_STEPS);
        assert_eq!(spec.service_account_name, defaults::SERVICE_ACCOUNT);
        assert_eq!(spec.run_resources.cpu, defaults::RUN_CPU);
        assert_eq!(spec.run_resources.memory, defaults::RUN_MEMORY);
        assert_eq!(spec.worker_resources.cpu, defaults::RUN_CPU);
        assert_eq!(spec.worker_resources.memory, defaults::RUN_MEMORY);
        assert!(spec.partition_key.is_none());
        assert!(spec.run_id.is_none());
        assert!(spec.timeout_seconds.is_none());
        assert!(spec.parameters.is_none());
    }

    #[test]
    fn spec_serializes_as_camel_case() {
        let json = serde_json::json!({
            "codeLocationRef": { "name": "x" },
            "image": "img:v1",
            "target": "*",
            "surrealEndpoint": "ws://custom:8000",
            "maxRestarts": 5,
            "cancelGracePeriodSeconds": 60,
            "maxConcurrentSteps": 20,
            "serviceAccountName": "custom-sa",
            "runResources": { "cpu": "1", "memory": "1Gi" },
            "workerResources": { "cpu": "2", "memory": "4Gi" }
        });
        let spec: RunSpec = serde_json::from_value(json).unwrap();

        assert_eq!(spec.surreal_endpoint, "ws://custom:8000");
        assert_eq!(spec.max_restarts, 5);
        assert_eq!(spec.cancel_grace_period_seconds, 60);
        assert_eq!(spec.max_concurrent_steps, 20);
        assert_eq!(spec.service_account_name, "custom-sa");
        assert_eq!(spec.run_resources.cpu, "1");
        assert_eq!(spec.worker_resources.memory, "4Gi");
        assert_eq!(spec.code_location_ref.name, "x");

        let re_json = serde_json::to_value(&spec).unwrap();
        assert!(re_json.get("surrealEndpoint").is_some());
        assert!(re_json.get("maxRestarts").is_some());
        assert!(re_json.get("surreal_endpoint").is_none());
        assert!(re_json.get("codeLocationRef").is_some());
    }

    #[test]
    fn spec_round_trip() {
        let json = serde_json::json!({
            "codeLocationRef": { "name": "analytics" },
            "image": "img:v1",
            "target": "a,b,c",
            "module": "my_mod",
            "partitionKey": "2026-01-01",
            "runId": "abc-123",
            "timeoutSeconds": 600
        });
        let spec: RunSpec = serde_json::from_value(json).unwrap();
        let serialized = serde_json::to_value(&spec).unwrap();
        let spec2: RunSpec = serde_json::from_value(serialized).unwrap();

        assert_eq!(spec.image, spec2.image);
        assert_eq!(spec.target, spec2.target);
        assert_eq!(spec.module, spec2.module);
        assert_eq!(spec.partition_key, spec2.partition_key);
        assert_eq!(spec.run_id, spec2.run_id);
        assert_eq!(spec.timeout_seconds, spec2.timeout_seconds);
        assert_eq!(spec.max_restarts, spec2.max_restarts);
        assert_eq!(spec.code_location_ref.name, spec2.code_location_ref.name);
    }

    #[test]
    fn run_phase_terminal_states() {
        assert!(RunPhase::Succeeded.is_terminal());
        assert!(RunPhase::Failed.is_terminal());
        assert!(RunPhase::Cancelled.is_terminal());
        assert!(RunPhase::TimedOut.is_terminal());
        assert!(!RunPhase::Pending.is_terminal());
        assert!(!RunPhase::Running.is_terminal());
        assert!(!RunPhase::Cancelling.is_terminal());
    }

    #[test]
    fn optional_fields_omitted_when_none() {
        let json = serde_json::json!({
            "codeLocationRef": { "name": "demo" },
            "image": "img:v1",
            "target": "*"
        });
        let spec: RunSpec = serde_json::from_value(json).unwrap();
        let serialized = serde_json::to_value(&spec).unwrap();

        assert!(serialized.get("partitionKey").is_none());
        assert!(serialized.get("runId").is_none());
        assert!(serialized.get("timeoutSeconds").is_none());
        assert!(serialized.get("parameters").is_none());
    }

    #[test]
    fn is_digest_reference_accepts_canonical_sha256_form() {
        let digest = format!("ghcr.io/x@sha256:{}", "a".repeat(64));
        assert!(is_digest_reference(&digest));
    }

    #[test]
    fn is_digest_reference_rejects_tag_and_malformed() {
        assert!(!is_digest_reference("ghcr.io/x:latest"));
        assert!(!is_digest_reference("ghcr.io/x@sha256:short"));
        assert!(!is_digest_reference(&format!(
            "ghcr.io/x@sha256:{}",
            "Z".repeat(64)
        )));
        assert!(!is_digest_reference(""));
    }

    #[test]
    fn webhook_apply_path_omits_image() {
        // Daemon-launched and kubectl-applied CRs leave `image` empty —
        // the webhook fills it from CodeLocation.status.resolvedImage.
        let json = serde_json::json!({
            "codeLocationRef": { "name": "analytics" },
            "target": "*"
        });
        let spec: RunSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec.image, "");
        assert_eq!(spec.code_location_ref.name, "analytics");
    }
}
