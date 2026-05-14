//! CodeLocation reconciler.
//!
//! Responsibilities:
//!   1. Resolve `spec.image:spec.tag` → `status.resolvedImage` (a digest
//!      reference), unless `spec.digest` is set (which short-circuits the
//!      registry call).
//!   2. Maintain the owned `Deployment` + `Service`, keyed by the CR name.
//!   3. Keep `status` in sync: phase, conditions, `resolvedImage`,
//!      `grpcEndpoint`, `observedGeneration`, `readyReplicas`.
//!
//! Registry polling is gated on the leader lease (see [`crate::leader`]): only
//! the leader issues registry HEADs. Followers still reconcile owned resources
//! against whatever digest is already in `status.resolvedImage`, so the
//! backing Deployment doesn't drift while a leader election is in flight.

use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube_client::ResourceExt;
use kube_client::api::{Api, Patch, PatchParams};
use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::{
    CONDITION_DEPLOYMENT_AVAILABLE, CONDITION_IMAGE_RESOLVED, CodeLocation, CodeLocationCondition,
    CodeLocationPhase, CodeLocationStatus, IMMUTABLE_TAG_ANNOTATION, REASON_AUTH_FAILED,
    REASON_AWAITING_LEADER, REASON_DIGEST_PINNED, REASON_DIGEST_RESOLVED, REASON_MIN_REPLICAS,
    REASON_NO_DEPLOYMENT_STATUS, REASON_PROGRESS_DEADLINE, REASON_RATE_LIMITED,
    REASON_REGISTRY_ERROR, REASON_ROLLING_OUT, REASON_TAG_NOT_FOUND,
};

use super::image_auth::resolve_auth;
use super::registry::{RegistryAuth, RegistryClient, RegistryError, Resolution, ResolveRequest};
use super::resources::{
    build_deployment, build_service, deployment_name, grpc_endpoint, service_name,
};
use crate::leader::LeaderGate;
use crate::metrics;

/// Default registry-refresh interval when the CR does not override it.
const DEFAULT_REFRESH: Duration = Duration::from_secs(300);
/// Minimum acceptable refresh interval. Anything tighter could blow through
/// registry rate limits quickly.
const MIN_REFRESH: Duration = Duration::from_secs(60);
/// Requeue delay for followers waiting on the leader to resolve a digest.
const FOLLOWER_WAIT: Duration = Duration::from_secs(30);
/// Requeue delay after a terminal-looking registry error (auth, 404). The
/// reconciler still checks back occasionally in case the user fixes the
/// underlying issue without bumping the generation.
const TERMINAL_RETRY: Duration = Duration::from_secs(300);
/// Short follow-up requeue when the Deployment is rolling out.
const DEPLOYMENT_ROLLOUT_POLL: Duration = Duration::from_secs(10);
/// Field manager used for all server-side applies.
const FIELD_MANAGER: &str = "rivers-operator-code-location";

pub struct Context {
    pub client: kube_client::Client,
    pub namespace: String,
    pub registry: Arc<RegistryClient>,
    pub leader: Arc<LeaderGate>,
    pub code_location_service_account: String,
    /// SurrealDB endpoint, scope (`use_ns` / `use_db`) and auth-secret
    /// coordinates stamped onto every code-location pod the operator creates.
    /// Read once from the operator's own env at startup. When `auth_secret`
    /// is set, `RIVERS_SURREAL_USERNAME` / `_PASSWORD` are emitted via
    /// `valueFrom.secretKeyRef`; otherwise pods connect unauthenticated.
    pub surreal_pod_cfg: rivers_k8s::env::SurrealPodConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes api error: {0}")]
    Kube(#[from] kube_client::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Reconcile a single `CodeLocation` CR: resolve `image:tag` to a digest,
/// apply the backing Deployment + Service via server-side apply, then patch
/// status with the new phase and conditions. Returns an [`Action`] telling
/// the controller runtime when to requeue.
pub async fn reconcile(cl: Arc<CodeLocation>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = cl.name_any();
    let namespace = cl.namespace().unwrap_or(ctx.namespace.clone());
    let generation = cl.metadata.generation;

    let code_locations_api: Api<CodeLocation> = Api::namespaced(ctx.client.clone(), &namespace);
    let deployments_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let services_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);

    let timer = Instant::now();
    let (resolved_image, image_reason, refresh_after, immutable) =
        match resolve_image(&cl, &ctx, &secrets_api).await {
            ImageOutcome::Resolved {
                resolved_image,
                reason,
                refresh_after,
                immutable,
            } => {
                metrics::observe_resolution_seconds(timer.elapsed().as_secs_f64());
                (resolved_image, reason, refresh_after, immutable)
            }
            ImageOutcome::AwaitingLeader => {
                patch_waiting_status(&code_locations_api, &name, generation, cl.as_ref()).await?;
                return Ok(Action::requeue(FOLLOWER_WAIT));
            }
            ImageOutcome::Error(err) => {
                let retry = err.retry_after();
                patch_error_status(&code_locations_api, &name, generation, cl.as_ref(), &err)
                    .await?;
                return Ok(Action::requeue(retry));
            }
        };

    let deployment = build_deployment(
        &cl,
        &resolved_image,
        &ctx.code_location_service_account,
        &ctx.surreal_pod_cfg,
    );
    let service = build_service(&cl);

    tokio::try_join!(
        apply_deployment(&deployments_api, &deployment),
        apply_service(&services_api, &service),
    )?;

    let dep_status = deployments_api
        .get_status(&deployment_name(&name))
        .await
        .ok();
    let (phase, ready_replicas, deployment_reason, deployment_ok) =
        evaluate_deployment_phase(&cl, dep_status.as_ref());

    let endpoint = grpc_endpoint(&name, &namespace, cl.spec.grpc_port);
    let now_rfc3339 = chrono::Utc::now().to_rfc3339();

    let prior_status = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(phase.clone()),
        observed_generation: generation,
        resolved_image: Some(resolved_image.clone()),
        grpc_endpoint: Some(endpoint),
        last_reconciled: Some(now_rfc3339.clone()),
        ready_replicas,
        message: None,
        conditions: Vec::new(),
    };
    push_condition(
        &mut status,
        prior_status,
        CONDITION_IMAGE_RESOLVED,
        "True",
        image_reason,
        Some(resolved_image.clone()),
        &now_rfc3339,
    );
    push_condition(
        &mut status,
        prior_status,
        CONDITION_DEPLOYMENT_AVAILABLE,
        if deployment_ok { "True" } else { "False" },
        deployment_reason,
        None,
        &now_rfc3339,
    );

    if !status_substantively_equal(prior_status, &status) {
        patch_status(&code_locations_api, &name, &status).await?;
    }

    let requeue = if matches!(phase, CodeLocationPhase::Ready) && immutable {
        // Immutable + Ready → rely on CR/Deployment change events.
        Action::await_change()
    } else if !matches!(phase, CodeLocationPhase::Ready) {
        Action::requeue(DEPLOYMENT_ROLLOUT_POLL)
    } else {
        Action::requeue(jitter(refresh_after))
    };
    Ok(requeue)
}

/// Controller-runtime error policy: log the failure and requeue after 30s.
/// Per-error retry tuning happens inside `reconcile()` itself; this is the
/// catch-all for unhandled errors that escape that path.
pub fn error_policy(cl: Arc<CodeLocation>, error: &Error, _ctx: Arc<Context>) -> Action {
    tracing::error!(
        code_location = %cl.name_any(),
        %error,
        "CodeLocation reconcile error"
    );
    Action::requeue(Duration::from_secs(30))
}

enum ImageOutcome {
    Resolved {
        resolved_image: String,
        reason: &'static str,
        refresh_after: Duration,
        immutable: bool,
    },
    /// Follower waiting for the leader to seed `status.resolvedImage`.
    AwaitingLeader,
    Error(ImageError),
}

#[derive(Debug)]
enum ImageError {
    NotFound,
    AuthFailed,
    RateLimited(Duration),
    Transient(String),
}

impl ImageError {
    fn reason(&self) -> &'static str {
        match self {
            ImageError::NotFound => REASON_TAG_NOT_FOUND,
            ImageError::AuthFailed => REASON_AUTH_FAILED,
            ImageError::RateLimited(_) => REASON_RATE_LIMITED,
            ImageError::Transient(_) => REASON_REGISTRY_ERROR,
        }
    }

    fn message(&self) -> String {
        match self {
            ImageError::NotFound => "tag not found in registry".into(),
            ImageError::AuthFailed => "registry authentication failed".into(),
            ImageError::RateLimited(d) => {
                format!("registry rate-limited; retrying in {}s", d.as_secs())
            }
            ImageError::Transient(msg) => msg.clone(),
        }
    }

    fn retry_after(&self) -> Duration {
        match self {
            ImageError::RateLimited(d) => *d,
            ImageError::NotFound | ImageError::AuthFailed => TERMINAL_RETRY,
            ImageError::Transient(_) => Duration::from_secs(60),
        }
    }
}

async fn resolve_image(
    cl: &CodeLocation,
    ctx: &Context,
    secrets_api: &Api<Secret>,
) -> ImageOutcome {
    // Explicit digest wins immediately — no leader gating, no HTTP.
    if cl.spec.has_pinned_digest() {
        let digest = cl.spec.digest.as_deref().unwrap_or_default();
        let resolved = format!("{}@{}", cl.spec.image, digest);
        return ImageOutcome::Resolved {
            resolved_image: resolved,
            reason: REASON_DIGEST_PINNED,
            refresh_after: Duration::from_secs(3600), // effectively immutable
            immutable: true,
        };
    }

    let refresh = parse_refresh_interval(cl.spec.digest_refresh_interval.as_deref());
    let immutable_hint = cl
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(IMMUTABLE_TAG_ANNOTATION))
        .is_some_and(|v| v == "true");

    // Followers can serve status as long as the leader has already resolved
    // the digest (we reuse whatever is currently in status.resolvedImage).
    if !ctx.leader.is_leader() {
        if let Some(prev) = cl.status.as_ref().and_then(|s| s.resolved_image.clone()) {
            return ImageOutcome::Resolved {
                resolved_image: prev,
                reason: REASON_DIGEST_RESOLVED,
                refresh_after: FOLLOWER_WAIT,
                immutable: false,
            };
        }
        return ImageOutcome::AwaitingLeader;
    }

    let secret_names: Vec<String> = cl
        .spec
        .image_pull_secrets
        .iter()
        .map(|r| r.name.clone())
        .collect();

    let registry_host = first_component(&cl.spec.image).unwrap_or_default();
    let auth: RegistryAuth = if secret_names.is_empty() {
        RegistryAuth::Anonymous
    } else {
        resolve_auth(secrets_api, &secret_names, &registry_host).await
    };

    let req = ResolveRequest {
        image: cl.spec.image.clone(),
        tag: cl.spec.effective_tag().to_string(),
        auth,
        immutable_hint,
        cache_ttl: refresh,
    };

    match ctx.registry.resolve(&req).await {
        Ok(res) => {
            let resolved = format!("{}@{}", cl.spec.image, res.digest());
            let immutable = matches!(res, Resolution::Immutable { .. });
            ImageOutcome::Resolved {
                resolved_image: resolved,
                reason: REASON_DIGEST_RESOLVED,
                refresh_after: refresh,
                immutable,
            }
        }
        Err(e) => ImageOutcome::Error(e.into()),
    }
}

impl From<RegistryError> for ImageError {
    fn from(err: RegistryError) -> Self {
        match err {
            RegistryError::NotFound => ImageError::NotFound,
            RegistryError::AuthFailed => ImageError::AuthFailed,
            RegistryError::RateLimited { retry_after } => ImageError::RateLimited(retry_after),
            RegistryError::Transient(msg) | RegistryError::Malformed(msg) => {
                ImageError::Transient(msg)
            }
        }
    }
}

fn parse_refresh_interval(spec_value: Option<&str>) -> Duration {
    let Some(raw) = spec_value else {
        return DEFAULT_REFRESH;
    };
    let trimmed = raw.trim();
    if trimmed == "0" {
        return Duration::from_secs(3600 * 24);
    }
    match humantime_lite::parse(trimmed) {
        Some(d) if d >= MIN_REFRESH => d,
        Some(_) => MIN_REFRESH,
        None => DEFAULT_REFRESH,
    }
}

/// Minimal duration parser accepting `30s`, `5m`, `1h`. We avoid pulling in
/// the full `humantime` crate for this one use.
mod humantime_lite {
    use std::time::Duration;

    pub fn parse(s: &str) -> Option<Duration> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('s') {
            return num.parse::<u64>().ok().map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('m') {
            return num.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60));
        }
        if let Some(num) = s.strip_suffix('h') {
            return num
                .parse::<u64>()
                .ok()
                .map(|h| Duration::from_secs(h * 3600));
        }
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

fn first_component(image: &str) -> Option<String> {
    image.split('/').next().map(str::to_string)
}

fn evaluate_deployment_phase(
    cl: &CodeLocation,
    status: Option<&Deployment>,
) -> (CodeLocationPhase, Option<i32>, &'static str, bool) {
    let desired = cl.spec.replicas;
    let Some(d) = status.and_then(|d| d.status.clone()) else {
        return (
            CodeLocationPhase::Deploying,
            None,
            REASON_NO_DEPLOYMENT_STATUS,
            false,
        );
    };
    let ready = d.ready_replicas.unwrap_or(0);
    if ready >= desired && desired > 0 {
        (
            CodeLocationPhase::Ready,
            Some(ready),
            REASON_MIN_REPLICAS,
            true,
        )
    } else {
        let reason = if has_progress_deadline_exceeded(&d) {
            REASON_PROGRESS_DEADLINE
        } else {
            REASON_ROLLING_OUT
        };
        (CodeLocationPhase::Deploying, Some(ready), reason, false)
    }
}

fn has_progress_deadline_exceeded(d: &k8s_openapi::api::apps::v1::DeploymentStatus) -> bool {
    d.conditions
        .as_ref()
        .map(|cs| {
            cs.iter().any(|c| {
                c.type_ == "Progressing"
                    && c.status == "False"
                    && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
            })
        })
        .unwrap_or(false)
}

async fn apply_deployment(
    api: &Api<Deployment>,
    desired: &Deployment,
) -> Result<(), kube_client::Error> {
    let name = desired.metadata.name.as_deref().unwrap_or_default();
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
}

async fn apply_service(api: &Api<Service>, desired: &Service) -> Result<(), kube_client::Error> {
    let name = desired.metadata.name.as_deref().unwrap_or_default();
    // Services have immutable fields (ClusterIP) that Apply tolerates as long
    // as the desired value matches what's already there — we let the server
    // reconcile.
    let _ = service_name(name); // use the import; keeps the helper exercised.
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(desired),
    )
    .await?;
    Ok(())
}

/// Whether the computed status matches the prior status on everything that
/// actually carries meaning for API consumers. `last_reconciled` changes on
/// every pass and is deliberately excluded — if nothing *else* moved, we
/// skip the patch entirely so followers/watchers aren't woken up for no reason.
fn status_substantively_equal(
    prior: Option<&CodeLocationStatus>,
    new: &CodeLocationStatus,
) -> bool {
    let Some(prior) = prior else { return false };
    prior.phase == new.phase
        && prior.observed_generation == new.observed_generation
        && prior.resolved_image == new.resolved_image
        && prior.grpc_endpoint == new.grpc_endpoint
        && prior.ready_replicas == new.ready_replicas
        && prior.message == new.message
        && prior.conditions == new.conditions
}

/// Append a condition, preserving `last_transition_time` from the prior
/// status when the condition's `status` field hasn't flipped. Per K8s API
/// convention, `lastTransitionTime` only changes on actual transitions.
fn push_condition(
    status: &mut CodeLocationStatus,
    prior: Option<&CodeLocationStatus>,
    cond_type: &str,
    cond_status: &str,
    reason: &'static str,
    message: Option<String>,
    now_rfc3339: &str,
) {
    let prior_transition = prior
        .and_then(|s| s.conditions.iter().find(|c| c.r#type == cond_type))
        .filter(|c| c.status == cond_status)
        .and_then(|c| c.last_transition_time.clone());
    status.conditions.push(CodeLocationCondition {
        r#type: cond_type.to_string(),
        status: cond_status.to_string(),
        last_transition_time: prior_transition.or_else(|| Some(now_rfc3339.to_string())),
        reason: Some(reason.to_string()),
        message,
    });
}

async fn patch_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    status: &CodeLocationStatus,
) -> Result<(), kube_client::Error> {
    let body = serde_json::json!({
        "apiVersion": "rivers.io/v1alpha1",
        "kind": "CodeLocation",
        "status": status,
    });
    code_locations_api
        .patch_status(
            name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&body),
        )
        .await?;
    Ok(())
}

async fn patch_waiting_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    generation: Option<i64>,
    cl: &CodeLocation,
) -> Result<(), kube_client::Error> {
    let now = chrono::Utc::now().to_rfc3339();
    let prior = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(CodeLocationPhase::Pending),
        observed_generation: generation,
        resolved_image: prior.and_then(|s| s.resolved_image.clone()),
        grpc_endpoint: None,
        last_reconciled: Some(now.clone()),
        ready_replicas: None,
        message: Some("awaiting leader replica".into()),
        conditions: Vec::new(),
    };
    push_condition(
        &mut status,
        prior,
        CONDITION_IMAGE_RESOLVED,
        "Unknown",
        REASON_AWAITING_LEADER,
        Some("follower replica — leader will resolve digest".into()),
        &now,
    );
    if status_substantively_equal(prior, &status) {
        return Ok(());
    }
    patch_status(code_locations_api, name, &status).await
}

async fn patch_error_status(
    code_locations_api: &Api<CodeLocation>,
    name: &str,
    generation: Option<i64>,
    cl: &CodeLocation,
    err: &ImageError,
) -> Result<(), kube_client::Error> {
    let now = chrono::Utc::now().to_rfc3339();
    let prior = cl.status.as_ref();
    let mut status = CodeLocationStatus {
        phase: Some(CodeLocationPhase::Failed),
        observed_generation: generation,
        resolved_image: prior.and_then(|s| s.resolved_image.clone()),
        grpc_endpoint: None,
        last_reconciled: Some(now.clone()),
        ready_replicas: None,
        message: Some(err.message()),
        conditions: Vec::new(),
    };
    push_condition(
        &mut status,
        prior,
        CONDITION_IMAGE_RESOLVED,
        "False",
        err.reason(),
        Some(err.message()),
        &now,
    );
    if status_substantively_equal(prior, &status) {
        return Ok(());
    }
    patch_status(code_locations_api, name, &status).await
}

/// Apply jittered poll cadence: `interval + rand(0, interval/4)`.
fn jitter(interval: Duration) -> Duration {
    let quarter = interval.as_millis() / 4;
    if quarter == 0 {
        return interval;
    }
    let extra_ms = fastrand::u64(0..=quarter as u64);
    interval + Duration::from_millis(extra_ms)
}

// Fallback RNG: we intentionally avoid adding a full dep for a micro-jitter.
// This is not a security-sensitive choice — the goal is just to spread
// requeue timing across CRs after operator startup.
mod fastrand {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEED: AtomicU64 = AtomicU64::new(0);

    fn seed() -> u64 {
        let existing = SEED.load(Ordering::Relaxed);
        if existing != 0 {
            return existing;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64 | 1)
            .unwrap_or(0x9E3779B97F4A7C15);
        SEED.store(now, Ordering::Relaxed);
        now
    }

    pub fn u64(range: std::ops::RangeInclusive<u64>) -> u64 {
        let mut x = SEED.load(Ordering::Relaxed);
        if x == 0 {
            x = seed();
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        SEED.store(x, Ordering::Relaxed);
        let span = range.end().saturating_sub(*range.start()).saturating_add(1);
        range.start() + (x % span.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube_client::api::ObjectMeta;
    use rivers_k8s::crd::code_location::CodeLocationSpec;

    fn make_cl(spec: serde_json::Value) -> CodeLocation {
        let parsed: CodeLocationSpec = serde_json::from_value(spec).unwrap();
        CodeLocation {
            metadata: ObjectMeta {
                name: Some("x".into()),
                namespace: Some("y".into()),
                uid: Some("u".into()),
                generation: Some(1),
                ..Default::default()
            },
            spec: parsed,
            status: None,
        }
    }

    #[test]
    fn refresh_default() {
        assert_eq!(parse_refresh_interval(None), DEFAULT_REFRESH);
    }

    #[test]
    fn refresh_honors_minimum() {
        assert_eq!(parse_refresh_interval(Some("10s")), MIN_REFRESH);
        assert_eq!(parse_refresh_interval(Some("1m")), MIN_REFRESH);
    }

    #[test]
    fn refresh_parses_standard_units() {
        assert_eq!(parse_refresh_interval(Some("2m")), Duration::from_secs(120));
        assert_eq!(
            parse_refresh_interval(Some("1h")),
            Duration::from_secs(3600)
        );
        assert_eq!(
            parse_refresh_interval(Some("300s")),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn jitter_within_bounds() {
        let base = Duration::from_secs(60);
        for _ in 0..100 {
            let j = jitter(base);
            assert!(j >= base);
            assert!(j <= base + Duration::from_secs(15));
        }
    }

    #[test]
    fn evaluate_deployment_ready() {
        use k8s_openapi::api::apps::v1::DeploymentStatus;
        let cl = make_cl(serde_json::json!({"image": "img", "tag": "v1", "replicas": 2}));
        let d = Deployment {
            status: Some(DeploymentStatus {
                ready_replicas: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (phase, ready, reason, ok) = evaluate_deployment_phase(&cl, Some(&d));
        assert_eq!(phase, CodeLocationPhase::Ready);
        assert_eq!(ready, Some(2));
        assert_eq!(reason, REASON_MIN_REPLICAS);
        assert!(ok);
    }

    #[test]
    fn evaluate_deployment_rolling() {
        use k8s_openapi::api::apps::v1::DeploymentStatus;
        let cl = make_cl(serde_json::json!({"image": "img", "tag": "v1", "replicas": 2}));
        let d = Deployment {
            status: Some(DeploymentStatus {
                ready_replicas: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (phase, ready, _reason, ok) = evaluate_deployment_phase(&cl, Some(&d));
        assert_eq!(phase, CodeLocationPhase::Deploying);
        assert_eq!(ready, Some(1));
        assert!(!ok);
    }

    #[test]
    fn first_component_extracts_host() {
        assert_eq!(
            first_component("ghcr.io/acme/pipeline"),
            Some("ghcr.io".into())
        );
        assert_eq!(
            first_component("localhost:5000/x"),
            Some("localhost:5000".into())
        );
    }

    #[test]
    fn image_outcome_pinned_digest_bypass() {
        // We can't easily await resolve_image without a Context, so just
        // sanity-check the spec helper that drives the short-circuit.
        let cl = make_cl(serde_json::json!({
            "image": "ghcr.io/acme/pipeline",
            "digest": "sha256:abc"
        }));
        assert!(cl.spec.has_pinned_digest());
    }
}
