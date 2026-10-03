//! AdmissionReview request/response handling for `Run` + `CodeLocation` CR
//! create + update events.
//!
//! Builds on the upstream `kube_core::admission` types (gated behind the
//! `admission` feature on `kube-core`), so the wire shape is whatever the K8s
//! API server expects without us hand-rolling a copy. Internally each
//! handler produces an outcome enum so the decision logic stays easy to
//! assert on in tests, and a thin translator maps that to the upstream
//! `AdmissionResponse` at the boundary.

use std::sync::Arc;

use json_patch::{AddOperation, Patch, PatchOperation, ReplaceOperation};
use jsonptr::PointerBuf;
use kube_client::Api;
use kube_core::DynamicObject;
use kube_core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview, Operation};
use rivers_api::CodeLocationEntry;
use rivers_k8s::crd::code_location::{CodeLocation, CodeLocationPhase, CodeLocationSpec};
use rivers_k8s::crd::run::{GitCoordinates, Run, RunSource, is_digest_reference};
use uuid::Uuid;

use crate::codelocation::git::ssh::with_git_user;
use crate::codelocation::reconcile::wanted_image;
use crate::codelocation::{DirectoryState, ImageRef};

/// Bundle of the kube/cache handles the admission handler reads from. The
/// `code_locations` API is `Option` so tests can run cache-only; production
/// wiring always passes `Some`.
#[derive(Clone)]
pub(super) struct AdmissionDeps {
    pub directory: Arc<DirectoryState>,
    pub code_locations: Option<Api<CodeLocation>>,
    /// Default runtime image for git CLs that don't set `spec.image` — the
    /// operator's `RIVERS_RUNTIME_IMAGE`.
    pub runtime_image: ImageRef,
}

impl AdmissionDeps {
    /// No live `GET`; the chart's default runtime image.
    #[cfg(test)]
    pub fn cache_only(directory: Arc<DirectoryState>) -> Self {
        Self {
            directory,
            code_locations: None,
            runtime_image: rivers_k8s::defaults::RUNTIME_IMAGE.parse().unwrap(),
        }
    }
}

/// Outcome of processing one Run admission request, separate from the
/// wire-level `AdmissionResponse` so callers (and tests) can inspect what
/// happened.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum RunOutcome {
    /// CREATE that was allowed and produced a JSON Patch stamping
    /// `image`, `module`, `codeLocationRef.identity`, and — for
    /// git-sourced CodeLocations — `spec.source`.
    Mutated {
        image: String,
        module: String,
        identity: String,
        source: Option<RunSource>,
    },
    /// CREATE accepted without modification — escape-hatch path: caller
    /// pre-set `image` to a digest. Identity isn't stamped from the CL
    /// here; the Run's `codeLocationRef.identity` is observability-only,
    /// and the executor pod scopes its storage from the operator-injected
    /// `RIVERS_CODE_LOCATION_ID` env regardless of what's on the Run CR.
    AcceptedAsIs,
    /// UPDATE that was allowed (immutable fields unchanged).
    UpdateAllowed,
    Rejected(String),
}

/// Top-level entrypoint for the Run mutating webhook. Wraps the [`mutate_run`]
/// pure logic in the `AdmissionReview` envelope.
pub(super) async fn handle_run_admission(
    review: AdmissionReview<Run>,
    deps: &AdmissionDeps,
) -> AdmissionReview<DynamicObject> {
    let request: AdmissionRequest<Run> = match review.try_into() {
        Ok(r) => r,
        Err(_) => {
            return AdmissionResponse::invalid("AdmissionReview missing request field")
                .into_review();
        }
    };
    let outcome = mutate_run(&request, deps).await;
    run_response_for(&request, outcome).into_review()
}

/// Pure decision logic for Run admission: examine an admission request and
/// decide what to do with it. Side-effect-free except for the optional
/// one-shot `GET` against the API server when the cache misses.
pub(super) async fn mutate_run(req: &AdmissionRequest<Run>, deps: &AdmissionDeps) -> RunOutcome {
    match req.operation {
        Operation::Create => mutate_run_on_create(req, deps).await,
        Operation::Update => check_run_update(req),
        ref op => RunOutcome::Rejected(format!(
            "operator webhook received unexpected operation '{op:?}'; \
             webhook is configured for CREATE/UPDATE only"
        )),
    }
}

async fn mutate_run_on_create(req: &AdmissionRequest<Run>, deps: &AdmissionDeps) -> RunOutcome {
    let Some(run) = req.object.as_ref() else {
        return RunOutcome::Rejected("CREATE admission request missing object".into());
    };

    let ref_name = run.spec.code_location_ref.name.trim();
    if ref_name.is_empty() {
        return RunOutcome::Rejected("spec.codeLocationRef.name is required".into());
    }
    // CodeLocation lives in the same namespace as the Run — see CodeLocationRef
    // doc-comment for why cross-namespace isn't supported. A namespaced-CRD
    // admission request without a namespace is malformed; reject explicitly
    // rather than producing a `'/foo'` lookup error downstream.
    let Some(ref_ns) = req.namespace.as_deref() else {
        return RunOutcome::Rejected("Run admission request missing metadata.namespace".into());
    };

    // Escape hatch: user pre-set image to a digest. Validate format and
    // accept without resolving from the CodeLocation — the user has explicitly
    // bypassed CL resolution. Identity stamping is skipped on this path:
    // `Run.codeLocationRef.identity` is observability-only metadata, and
    // the executor pod scopes its storage from the operator-injected
    // `RIVERS_CODE_LOCATION_ID` env, not from the Run CR.
    if !run.spec.image.is_empty() {
        if !is_digest_reference(&run.spec.image) {
            return RunOutcome::Rejected(format!(
                "spec.image '{}' is not a digest reference \
                 (must be of the form 'registry/repo@sha256:...'); \
                 omit it to let the webhook resolve from the CodeLocation",
                run.spec.image
            ));
        }
        // The code-location pod (`K8sRunBackend`) dispatches runs with a
        // pre-set digest AND the source of its own tree.
        if let Some(source) = &run.spec.source
            && let Err(message) =
                check_self_stamped_source(source, ref_ns, ref_name, deps, &git_allowed_hosts())
                    .await
        {
            return RunOutcome::Rejected(message);
        }
        return RunOutcome::AcceptedAsIs;
    }

    // Normal path: the webhook is the sole author of spec.source.
    if run.spec.source.is_some() {
        return RunOutcome::Rejected(
            "spec.source is stamped by the webhook from the CodeLocation — remove it \
             (pre-set spec.image to a digest for the self-stamped escape hatch)"
                .into(),
        );
    }

    let entry = match lookup_with_fallback(deps, ref_ns, ref_name).await {
        Ok(Some(e)) => e,
        Ok(None) => {
            return RunOutcome::Rejected(format!(
                "codeLocation '{ref_name}' not found in namespace '{ref_ns}'"
            ));
        }
        Err(e) => {
            return RunOutcome::Rejected(format!(
                "live lookup of codeLocation '{ref_ns}/{ref_name}' failed: {e}"
            ));
        }
    };

    if entry.phase != CodeLocationPhase::Ready.as_str() {
        return RunOutcome::Rejected(format!(
            "codeLocation '{ref_ns}/{ref_name}' is not Ready (phase={})",
            entry.phase
        ));
    }
    if entry.image.is_empty() {
        return RunOutcome::Rejected(format!(
            "codeLocation '{ref_ns}/{ref_name}' has no resolved image yet; \
             status.resolvedImage is empty"
        ));
    }

    // Git-sourced CL: stamp provenance from spec (coordinates) + entry
    // (pinned commit and runtime image). The spec cache is populated by the
    // same watcher that feeds entries, status-independent — a missing spec
    // here means an image-mode CL, not a race.
    let source = match deps.directory.lookup_spec(ref_ns, ref_name).await {
        Some(spec) if spec.git.is_some() => {
            let git = spec.git.as_ref().expect("checked");
            if entry.resolved_commit.is_empty() {
                return RunOutcome::Rejected(format!(
                    "codeLocation '{ref_ns}/{ref_name}' has no resolved commit yet; \
                     status.resolvedCommit is empty"
                ));
            }
            Some(RunSource {
                git: GitCoordinates {
                    url: git.url.clone(),
                    commit: entry.resolved_commit.clone(),
                    r#ref: Some(entry.resolved_ref.clone()).filter(|r| !r.is_empty()),
                    path: git.path.clone(),
                    secret_name: git.secret_ref.as_ref().map(|s| s.name.clone()),
                },
                dependencies: git.dependencies.clone(),
                runtime_image: entry.image.clone(),
            })
        }
        _ => None,
    };

    RunOutcome::Mutated {
        image: entry.image,
        module: entry.module,
        identity: entry.identity,
        source,
    }
}

/// Escape-hatch provenance: commit, ref, path and dependencies are the
/// caller's own (e.g. a pod still serving the previous commit), but the url
/// and Secret must be the CodeLocation's — a fallback-mode run pod mounts
/// that Secret and sends its credentials to that url — and so must the
/// runtime image's repository, since that image runs the init container
/// that mounts the Secret: the serving tree's (`status.resolvedImage`) or
/// the one the CL's pods move to (`spec.image`, else the chart default).
/// Any digest is accepted, as during a rollout.
async fn check_self_stamped_source(
    source: &RunSource,
    namespace: &str,
    name: &str,
    deps: &AdmissionDeps,
    allowed_hosts: &[String],
) -> Result<(), String> {
    let git = &source.git;
    if git.commit.len() != 40 || !git.commit.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "spec.source.git.commit '{}' is not a full 40-hex commit SHA",
            git.commit
        ));
    }
    if git.url.is_empty() {
        return Err("spec.source.git.url is empty".into());
    }
    validate_git_url("spec.source.git.url", &git.url, allowed_hosts)?;
    let runtime = source
        .runtime_image
        .parse::<ImageRef>()
        .ok()
        .filter(|image| image.digest.is_some())
        .ok_or_else(|| {
            format!(
                "spec.source.runtimeImage '{}' is not a digest reference \
                 (must be of the form 'registry/repo@sha256:...')",
                source.runtime_image
            )
        })?;

    let (spec, entry) = match lookup_code_location_with_fallback(deps, namespace, name).await {
        Ok(Some(found)) => found,
        Ok(None) => {
            return Err(format!(
                "codeLocation '{name}' not found in namespace '{namespace}'"
            ));
        }
        Err(e) => {
            return Err(format!(
                "live lookup of codeLocation '{namespace}/{name}' failed: {e}"
            ));
        }
    };
    let Some(cl_git) = &spec.git else {
        return Err(format!(
            "codeLocation '{namespace}/{name}' is not git-sourced; remove spec.source"
        ));
    };
    if git.url != cl_git.url {
        return Err(format!(
            "spec.source.git.url does not match spec.git.url of codeLocation '{namespace}/{name}'"
        ));
    }
    let cl_secret = cl_git.secret_ref.as_ref().map(|s| s.name.as_str());
    if git.secret_name.as_deref() != cl_secret {
        let show = |s: Option<&str>| s.map_or_else(|| "none".to_string(), |s| format!("'{s}'"));
        return Err(format!(
            "spec.source.git.secretName {} does not match spec.git.secretRef {} \
             of codeLocation '{namespace}/{name}'",
            show(git.secret_name.as_deref()),
            show(cl_secret),
        ));
    }
    let mut repositories: Vec<String> = entry
        .and_then(|e| e.image.parse::<ImageRef>().ok())
        .map(|image| image.repository)
        .into_iter()
        .collect();
    let wanted = wanted_image(&spec, &deps.runtime_image).repository;
    if !repositories.contains(&wanted) {
        repositories.push(wanted);
    }
    if !repositories.contains(&runtime.repository) {
        let quoted: Vec<String> = repositories.iter().map(|r| format!("'{r}'")).collect();
        return Err(format!(
            "spec.source.runtimeImage '{}' is not in the runtime image repository {} \
             of codeLocation '{namespace}/{name}'",
            source.runtime_image,
            quoted.join(" or "),
        ));
    }
    Ok(())
}

fn check_run_update(req: &AdmissionRequest<Run>) -> RunOutcome {
    let (Some(new), Some(old)) = (req.object.as_ref(), req.old_object.as_ref()) else {
        return RunOutcome::Rejected("UPDATE admission request missing object or oldObject".into());
    };

    if new.spec.image != old.spec.image {
        return RunOutcome::Rejected("spec.image is immutable after creation".into());
    }
    if new.spec.module != old.spec.module {
        return RunOutcome::Rejected("spec.module is immutable after creation".into());
    }
    if new.spec.code_location_ref != old.spec.code_location_ref {
        return RunOutcome::Rejected("spec.codeLocationRef is immutable after creation".into());
    }
    if new.spec.source != old.spec.source {
        return RunOutcome::Rejected("spec.source is immutable after creation".into());
    }
    RunOutcome::UpdateAllowed
}

fn run_response_for(req: &AdmissionRequest<Run>, outcome: RunOutcome) -> AdmissionResponse {
    let base = AdmissionResponse::from(req);
    match outcome {
        RunOutcome::Mutated {
            image,
            module,
            identity,
            source,
        } => {
            // `replace` for fields the admission request always carries
            // (the existing Phase-4 wiring); `add` for fields that may be
            // absent from a user's raw apply payload — `add` both creates
            // and overwrites, while `replace` would fail on a missing path.
            let mut ops = vec![
                PatchOperation::Replace(ReplaceOperation {
                    path: PointerBuf::from_tokens(["spec", "image"]),
                    value: serde_json::Value::String(image),
                }),
                PatchOperation::Replace(ReplaceOperation {
                    path: PointerBuf::from_tokens(["spec", "module"]),
                    value: serde_json::Value::String(module),
                }),
                PatchOperation::Add(AddOperation {
                    path: PointerBuf::from_tokens(["spec", "codeLocationRef", "identity"]),
                    value: serde_json::Value::String(identity),
                }),
            ];
            if let Some(source) = source {
                ops.push(PatchOperation::Add(AddOperation {
                    path: PointerBuf::from_tokens(["spec", "source"]),
                    value: serde_json::to_value(&source).expect("RunSource serializes to JSON"),
                }));
            }
            base.with_patch(Patch(ops))
                .expect("RFC 6902 Patch serializes to JSON")
        }
        RunOutcome::AcceptedAsIs | RunOutcome::UpdateAllowed => base,
        RunOutcome::Rejected(message) => base.deny(message),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CodeLocationOutcome {
    /// CREATE with empty `spec.identity` — the webhook stamps a fresh
    /// UUID v4. The string is the UUID that was generated.
    IdentityStamped(String),
    /// CREATE with a user-supplied valid UUID — accepted as-is.
    AcceptedAsIs,
    /// UPDATE that was allowed (identity unchanged).
    UpdateAllowed,
    Rejected(String),
}

pub(super) async fn handle_codelocation_admission(
    review: AdmissionReview<CodeLocation>,
) -> AdmissionReview<DynamicObject> {
    let request: AdmissionRequest<CodeLocation> = match review.try_into() {
        Ok(r) => r,
        Err(_) => {
            return AdmissionResponse::invalid("AdmissionReview missing request field")
                .into_review();
        }
    };
    let outcome = mutate_codelocation(&request);
    codelocation_response_for(&request, outcome).into_review()
}

/// Pure decision logic for CodeLocation admission. No external lookups —
/// everything we need is in the request payload.
pub(super) fn mutate_codelocation(req: &AdmissionRequest<CodeLocation>) -> CodeLocationOutcome {
    match req.operation {
        Operation::Create => mutate_codelocation_on_create(req),
        Operation::Update => check_codelocation_update(req),
        ref op => CodeLocationOutcome::Rejected(format!(
            "operator webhook received unexpected operation '{op:?}'; \
             webhook is configured for CREATE/UPDATE only"
        )),
    }
}

fn mutate_codelocation_on_create(req: &AdmissionRequest<CodeLocation>) -> CodeLocationOutcome {
    let Some(cl) = req.object.as_ref() else {
        return CodeLocationOutcome::Rejected("CREATE admission request missing object".into());
    };

    // Source validation MUST precede identity stamping — the stamp path
    // returns early, and stamping an invalid CR would accept it.
    if let Err(message) = validate_git_source(cl, &git_allowed_hosts()) {
        return CodeLocationOutcome::Rejected(message);
    }

    let identity = cl.spec.identity.trim();
    if identity.is_empty() {
        // Mutating path: stamp a fresh UUID v4. The validating step on
        // UPDATE locks it in place from there on.
        return CodeLocationOutcome::IdentityStamped(Uuid::new_v4().to_string());
    }
    if !is_uuid_v4(identity) {
        return CodeLocationOutcome::Rejected(format!(
            "spec.identity '{identity}' is not a valid UUID v4 \
             (expected `xxxxxxxx-xxxx-4xxx-{{8|9|a|b}}xxx-xxxxxxxxxxxx`); \
             omit the field to let the webhook stamp one"
        ));
    }
    CodeLocationOutcome::AcceptedAsIs
}

/// The source rules apply only when `spec.git` or `spec.image` changes, and
/// not while the object is being deleted: a CodeLocation that predates a
/// stricter rule still takes other updates, such as labels or the garbage
/// collector's finalizer removal.
fn check_codelocation_update(req: &AdmissionRequest<CodeLocation>) -> CodeLocationOutcome {
    let (Some(new), Some(old)) = (req.object.as_ref(), req.old_object.as_ref()) else {
        return CodeLocationOutcome::Rejected(
            "UPDATE admission request missing object or oldObject".into(),
        );
    };
    if new.spec.identity != old.spec.identity {
        return CodeLocationOutcome::Rejected(format!(
            "spec.identity is immutable after creation (was '{}', now '{}')",
            old.spec.identity, new.spec.identity
        ));
    }
    let source_changed = new.spec.git != old.spec.git || new.spec.image != old.spec.image;
    let deleting = new.metadata.deletion_timestamp.is_some();
    if source_changed
        && !deleting
        && let Err(message) = validate_git_source(new, &git_allowed_hosts())
    {
        return CodeLocationOutcome::Rejected(message);
    }
    CodeLocationOutcome::UpdateAllowed
}

/// Validate the source shape (RFC-044): at least one of `image`/`git`
/// (re-checked here for API servers whose CEL support predates the CRD
/// rule), a well-formed one-of `git.ref`, a supported URL scheme, a user in
/// an `ssh://` url, and the operator's host allowlist.
fn validate_git_source(cl: &CodeLocation, allowed_hosts: &[String]) -> Result<(), String> {
    if cl.spec.image.is_none() && cl.spec.git.is_none() {
        return Err("one of spec.image or spec.git must be set".to_string());
    }
    let Some(git) = &cl.spec.git else {
        return Ok(());
    };
    git.r#ref.validate()?;
    validate_git_url("spec.git.url", &git.url, allowed_hosts)
}

/// Supported scheme, a user in `ssh://` urls, and the operator's host
/// allowlist. `field` names the url in messages.
fn validate_git_url(field: &str, raw: &str, allowed_hosts: &[String]) -> Result<(), String> {
    let url =
        url::Url::parse(raw).map_err(|e| format!("{field} '{raw}' is not a valid url: {e}"))?;
    match url.scheme() {
        "https" | "http" | "ssh" => {}
        other => {
            return Err(format!(
                "{field} scheme '{other}' is unsupported (use https:// or ssh://)"
            ));
        }
    }
    let Some(host) = url.host_str() else {
        return Err(format!("{field} '{raw}' has no host"));
    };
    // Without a user, the pods' ssh logs in as their own account.
    if url.scheme() == "ssh" && url.username().is_empty() {
        return Err(format!(
            "{field} must name the ssh user — use {}, or the user your git host expects",
            with_git_user(&url)
        ));
    }
    if !host_allowed(host, allowed_hosts) {
        return Err(format!(
            "git host '{host}' is not in the operator's allowed list — \
             adjust operator.git.allowedHosts or use an approved host"
        ));
    }
    Ok(())
}

fn host_allowed(host: &str, allowed: &[String]) -> bool {
    allowed.is_empty() || allowed.iter().any(|a| a.eq_ignore_ascii_case(host))
}

/// `RIVERS_GIT_ALLOWED_HOSTS`, comma-separated; empty admits any host.
/// Read once — the operator restarts on config change (it's pod env).
fn git_allowed_hosts() -> Vec<String> {
    static HOSTS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    HOSTS
        .get_or_init(|| {
            std::env::var("RIVERS_GIT_ALLOWED_HOSTS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .clone()
}

fn codelocation_response_for(
    req: &AdmissionRequest<CodeLocation>,
    outcome: CodeLocationOutcome,
) -> AdmissionResponse {
    let base = AdmissionResponse::from(req);
    match outcome {
        CodeLocationOutcome::IdentityStamped(uuid) => {
            // `add` rather than `replace` — user's apply payload may omit
            // the field entirely, in which case `replace` would fail.
            let patch = Patch(vec![PatchOperation::Add(AddOperation {
                path: PointerBuf::from_tokens(["spec", "identity"]),
                value: serde_json::Value::String(uuid),
            })]);
            base.with_patch(patch)
                .expect("RFC 6902 Patch serializes to JSON")
        }
        CodeLocationOutcome::AcceptedAsIs | CodeLocationOutcome::UpdateAllowed => base,
        CodeLocationOutcome::Rejected(message) => base.deny(message),
    }
}

/// Strict UUID v4 check (version nibble = 4, variant nibble in `{8,9,a,b}`).
/// `uuid::Uuid::parse_str` accepts any version; we add the v4 constraint so
/// the field has the same shape the mutating webhook stamps and a future
/// CRD-side `pattern` validation can mirror.
fn is_uuid_v4(s: &str) -> bool {
    let Ok(uuid) = Uuid::parse_str(s) else {
        return false;
    };
    uuid.get_version_num() == 4
}

/// Cache lookup with one-shot live `GET` fallback ([`live_get`]).
async fn lookup_with_fallback(
    deps: &AdmissionDeps,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<CodeLocationEntry>> {
    if let Some(entry) = deps.directory.lookup(namespace, name).await {
        return Ok(Some(entry));
    }
    Ok(live_get(deps, name)
        .await?
        .as_ref()
        .and_then(crate::codelocation::directory::project_entry))
}

/// The CodeLocation's spec and, once it has a status, its directory entry:
/// both from the cache, else both from one live `GET` ([`live_get`]).
async fn lookup_code_location_with_fallback(
    deps: &AdmissionDeps,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<(Arc<CodeLocationSpec>, Option<CodeLocationEntry>)>> {
    if let (Some(spec), Some(entry)) = (
        deps.directory.lookup_spec(namespace, name).await,
        deps.directory.lookup(namespace, name).await,
    ) {
        return Ok(Some((spec, Some(entry))));
    }
    Ok(live_get(deps, name).await?.map(|cl| {
        let entry = crate::codelocation::directory::project_entry(&cl);
        (Arc::new(cl.spec), entry)
    }))
}

/// The live `GET` MUST be a quorum read against etcd — kube's default
/// `GetParams` (no resource_version set) does exactly that, bypassing the API
/// server's watch cache. A stale watch cache could otherwise mirror the same
/// not-found the local reflector saw, defeating the fallback.
async fn live_get(deps: &AdmissionDeps, name: &str) -> anyhow::Result<Option<CodeLocation>> {
    let Some(api) = deps.code_locations.as_ref() else {
        return Ok(None);
    };
    api.get_opt(name)
        .await
        .map_err(|e| anyhow::Error::new(e).context("live GET on CodeLocation (quorum read)"))
}

#[cfg(test)]
pub(crate) fn admission_request_create(
    uid: &str,
    namespace: &str,
    run: &Run,
) -> AdmissionRequest<Run> {
    use kube_core::admission::AdmissionRequest as Req;
    use kube_core::gvk::{GroupVersionKind, GroupVersionResource};
    use kube_core::metadata::TypeMeta;
    Req {
        types: TypeMeta {
            kind: "AdmissionReview".to_string(),
            api_version: "admission.k8s.io/v1".to_string(),
        },
        uid: uid.to_string(),
        kind: GroupVersionKind::gvk("rivers.io", "v1alpha1", "Run"),
        resource: GroupVersionResource::gvr("rivers.io", "v1alpha1", "runs"),
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: run.metadata.name.clone().unwrap_or_default(),
        namespace: Some(namespace.to_string()),
        operation: Operation::Create,
        user_info: Default::default(),
        object: Some(run.clone()),
        old_object: None,
        dry_run: false,
        options: None,
    }
}

#[cfg(test)]
pub(crate) fn admission_request_update(
    uid: &str,
    namespace: &str,
    new: &Run,
    old: &Run,
) -> AdmissionRequest<Run> {
    let mut req = admission_request_create(uid, namespace, new);
    req.operation = Operation::Update;
    req.old_object = Some(old.clone());
    req
}

#[cfg(test)]
pub(crate) fn cl_admission_request_create(
    uid: &str,
    namespace: &str,
    cl: &CodeLocation,
) -> AdmissionRequest<CodeLocation> {
    use kube_core::admission::AdmissionRequest as Req;
    use kube_core::gvk::{GroupVersionKind, GroupVersionResource};
    use kube_core::metadata::TypeMeta;
    Req {
        types: TypeMeta {
            kind: "AdmissionReview".to_string(),
            api_version: "admission.k8s.io/v1".to_string(),
        },
        uid: uid.to_string(),
        kind: GroupVersionKind::gvk("rivers.io", "v1alpha1", "CodeLocation"),
        resource: GroupVersionResource::gvr("rivers.io", "v1alpha1", "codelocations"),
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: cl.metadata.name.clone().unwrap_or_default(),
        namespace: Some(namespace.to_string()),
        operation: Operation::Create,
        user_info: Default::default(),
        object: Some(cl.clone()),
        old_object: None,
        dry_run: false,
        options: None,
    }
}

#[cfg(test)]
pub(crate) fn cl_admission_request_update(
    uid: &str,
    namespace: &str,
    new: &CodeLocation,
    old: &CodeLocation,
) -> AdmissionRequest<CodeLocation> {
    let mut req = cl_admission_request_create(uid, namespace, new);
    req.operation = Operation::Update;
    req.old_object = Some(old.clone());
    req
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_k8s::crd::run::RunSpec;

    fn build_run(name: &str, image: &str) -> Run {
        let spec: RunSpec = serde_json::from_value(serde_json::json!({
            "codeLocationRef": { "name": name },
            "image": image,
            "target": "*",
        }))
        .unwrap();
        Run::new("test-run", spec)
    }

    fn ready_entry(
        ns: &str,
        name: &str,
        image: &str,
        module: &str,
        identity: &str,
    ) -> CodeLocationEntry {
        CodeLocationEntry {
            namespace: ns.to_string(),
            name: name.to_string(),
            grpc_endpoint: "grpc://example:50051".to_string(),
            image: image.to_string(),
            module: module.to_string(),
            phase: CodeLocationPhase::Ready.as_str().to_string(),
            observed_generation: 1,
            identity: identity.to_string(),
            resolved_commit: String::new(),
            resolved_ref: String::new(),
        }
    }

    const COMMIT: &str = "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8";
    /// Repository of the chart's default runtime image, which a git CL
    /// without `spec.image` runs ([`AdmissionDeps::cache_only`]).
    fn runtime_repository() -> String {
        rivers_k8s::defaults::RUNTIME_IMAGE
            .parse::<ImageRef>()
            .unwrap()
            .repository
    }

    /// Runtime image digest `c` — `runtime('a')` is the CL's resolved one.
    fn runtime(c: char) -> String {
        format!(
            "{}@sha256:{}",
            runtime_repository(),
            c.to_string().repeat(64)
        )
    }

    /// Unrelated to any CL's runtime image.
    fn gpu_worker() -> String {
        format!("ghcr.io/acme/gpu-worker@sha256:{}", "f".repeat(64))
    }

    fn with_runtime_image(image: &str) -> RunSource {
        RunSource {
            runtime_image: image.to_string(),
            ..expected_run_source()
        }
    }

    async fn expect_accepted(run: &Run, deps: &AdmissionDeps) {
        let req = admission_request_create("uid-hatch", "team-data", run);
        assert_eq!(mutate_run(&req, deps).await, RunOutcome::AcceptedAsIs);
    }

    /// Directory state for a git-sourced CL: entry with a pinned commit AND
    /// the cached spec (the webhook stamps coordinates from both halves).
    async fn git_directory(ns: &str, name: &str, commit: &str) -> Arc<DirectoryState> {
        let state = Arc::new(DirectoryState::new());
        let mut entry = ready_entry(
            ns,
            name,
            &runtime('a'),
            "analytics.pipeline",
            "11111111-1111-4111-8111-111111111111",
        );
        entry.resolved_commit = commit.to_string();
        entry.resolved_ref = "refs/heads/main".to_string();
        state.upsert(entry).await;
        state.upsert_spec(ns, name, Arc::new(git_spec())).await;
        state
    }

    fn git_spec() -> CodeLocationSpec {
        serde_json::from_value(serde_json::json!({
            "git": {
                "url": "https://forge.example/acme/pipelines.git",
                "ref": { "branch": "main" },
                "path": "analytics",
                "secretRef": { "name": "git-creds" },
                "dependencies": { "mode": "uvSync" },
            },
            "module": "analytics.pipeline",
        }))
        .unwrap()
    }

    /// `team-data/analytics` with `spec`, Ready on runtime image `runtime('a')`.
    async fn spec_directory(spec: serde_json::Value) -> Arc<DirectoryState> {
        let state = Arc::new(DirectoryState::new());
        let spec: CodeLocationSpec = serde_json::from_value(spec).unwrap();
        state
            .upsert_spec("team-data", "analytics", Arc::new(spec))
            .await;
        state
            .upsert(ready_entry(
                "team-data",
                "analytics",
                &runtime('a'),
                "analytics.pipeline",
                "11111111-1111-4111-8111-111111111111",
            ))
            .await;
        state
    }

    /// Digest-pinned Run carrying its own `spec.source` (escape hatch).
    fn self_stamped_run(source: RunSource) -> Run {
        let mut run = build_run("analytics", &format!("ghcr.io/x@sha256:{}", "a".repeat(64)));
        run.spec.source = Some(source);
        run
    }

    async fn expect_rejected(run: &Run, deps: &AdmissionDeps) -> String {
        let req = admission_request_create("uid-hatch", "team-data", run);
        match mutate_run(&req, deps).await {
            RunOutcome::Rejected(msg) => msg,
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    fn expected_run_source() -> RunSource {
        RunSource {
            git: rivers_k8s::crd::run::GitCoordinates {
                url: "https://forge.example/acme/pipelines.git".to_string(),
                commit: COMMIT.to_string(),
                r#ref: Some("refs/heads/main".to_string()),
                path: Some("analytics".to_string()),
                secret_name: Some("git-creds".to_string()),
            },
            dependencies: serde_json::from_value(serde_json::json!({ "mode": "uvSync" })).unwrap(),
            runtime_image: runtime('a'),
        }
    }

    #[tokio::test]
    async fn create_stamps_source_for_git_cl() {
        let state = git_directory("team-data", "analytics", COMMIT).await;
        let run = build_run("analytics", "");
        let req = admission_request_create("uid-git-1", "team-data", &run);
        match mutate_run(&req, &AdmissionDeps::cache_only(state)).await {
            RunOutcome::Mutated { image, source, .. } => {
                assert_eq!(image, runtime('a'));
                // The tree is the commit built with the CL's runtime image.
                assert_eq!(
                    serde_json::to_value(&source).unwrap()["runtimeImage"],
                    runtime('a')
                );
                assert_eq!(source, Some(expected_run_source()));
            }
            other => panic!("expected Mutated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_rejects_git_cl_whose_commit_is_unresolved() {
        // Entry Ready but no pinned commit — must not admit a run that
        // would have no tree to mount.
        let state = git_directory("team-data", "analytics", "").await;
        let run = build_run("analytics", "");
        let req = admission_request_create("uid-git-2", "team-data", &run);
        match mutate_run(&req, &AdmissionDeps::cache_only(state)).await {
            RunOutcome::Rejected(msg) => assert!(msg.contains("commit"), "{msg}"),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_rejects_hand_set_source_on_the_normal_path() {
        let state = git_directory("team-data", "analytics", COMMIT).await;
        let mut run = build_run("analytics", "");
        run.spec.source = Some(expected_run_source());
        let req = admission_request_create("uid-git-3", "team-data", &run);
        match mutate_run(&req, &AdmissionDeps::cache_only(state)).await {
            RunOutcome::Rejected(msg) => assert!(msg.contains("webhook"), "{msg}"),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn escape_hatch_accepts_daemon_stamped_source() {
        // Daemon-dispatched runs pre-set a digest image AND their own
        // provenance — the daemon pins the commit its own code runs.
        let digest_image = format!("ghcr.io/x@sha256:{}", "a".repeat(64));
        let state = git_directory("team-data", "analytics", COMMIT).await;
        let mut run = build_run("analytics", &digest_image);
        run.spec.source = Some(expected_run_source());
        let req = admission_request_create("uid-git-4", "team-data", &run);
        assert_eq!(
            mutate_run(&req, &AdmissionDeps::cache_only(state.clone())).await,
            RunOutcome::AcceptedAsIs
        );

        // …but a malformed pin is still rejected.
        let mut bad = build_run("analytics", &digest_image);
        let mut src = expected_run_source();
        src.git.commit = "not-a-sha".to_string();
        bad.spec.source = Some(src);
        let req = admission_request_create("uid-git-5", "team-data", &bad);
        assert!(matches!(
            mutate_run(&req, &AdmissionDeps::cache_only(state)).await,
            RunOutcome::Rejected(_)
        ));
    }

    #[tokio::test]
    async fn escape_hatch_rejects_other_repo_url() {
        // The source url is where a fallback run pod sends the CL's
        // credentials.
        let deps = AdmissionDeps::cache_only(git_directory("team-data", "analytics", COMMIT).await);
        let mut source = expected_run_source();
        source.git.url = "https://attacker.example/acme/pipelines.git".to_string();
        let msg = expect_rejected(&self_stamped_run(source), &deps).await;
        assert!(
            msg.contains("spec.source.git.url does not match spec.git.url"),
            "{msg}"
        );
        assert!(msg.contains("'team-data/analytics'"), "{msg}");
    }

    #[tokio::test]
    async fn escape_hatch_rejects_other_secret_name() {
        // The source secretName is the Secret a fallback run pod mounts.
        let deps = AdmissionDeps::cache_only(git_directory("team-data", "analytics", COMMIT).await);
        let mut source = expected_run_source();
        source.git.secret_name = Some("db-credentials".to_string());
        let msg = expect_rejected(&self_stamped_run(source.clone()), &deps).await;
        assert!(
            msg.contains(
                "spec.source.git.secretName 'db-credentials' does not match \
                 spec.git.secretRef 'git-creds'"
            ),
            "{msg}"
        );

        source.git.secret_name = None;
        let msg = expect_rejected(&self_stamped_run(source), &deps).await;
        assert!(
            msg.contains(
                "spec.source.git.secretName none does not match spec.git.secretRef 'git-creds'"
            ),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn escape_hatch_rejects_secret_when_codelocation_has_none() {
        let deps = AdmissionDeps::cache_only(
            spec_directory(serde_json::json!({
                "git": {
                    "url": "https://forge.example/acme/pipelines.git",
                    "ref": { "branch": "main" },
                },
            }))
            .await,
        );
        let msg = expect_rejected(&self_stamped_run(expected_run_source()), &deps).await;
        assert!(
            msg.contains(
                "spec.source.git.secretName 'git-creds' does not match spec.git.secretRef none"
            ),
            "{msg}"
        );

        // Neither side naming a Secret is a match.
        let mut source = expected_run_source();
        source.git.secret_name = None;
        let req =
            admission_request_create("uid-hatch-nosecret", "team-data", &self_stamped_run(source));
        assert_eq!(mutate_run(&req, &deps).await, RunOutcome::AcceptedAsIs);
    }

    #[tokio::test]
    async fn escape_hatch_rejects_source_host_outside_allowed_hosts() {
        let deps = AdmissionDeps::cache_only(git_directory("team-data", "analytics", COMMIT).await);
        let source = expected_run_source();
        check_self_stamped_source(
            &source,
            "team-data",
            "analytics",
            &deps,
            &["Forge.Example".to_string()],
        )
        .await
        .unwrap();
        let err = check_self_stamped_source(
            &source,
            "team-data",
            "analytics",
            &deps,
            &["github.com".to_string()],
        )
        .await
        .unwrap_err();
        assert!(err.contains("git host 'forge.example'"), "{err}");
        assert!(err.contains("allowedHosts"), "{err}");
    }

    #[tokio::test]
    async fn escape_hatch_rejects_ssh_source_url_without_a_user() {
        /// A self-stamped run on `url`, which is also the code location's
        /// own url (as admitted before ssh urls had to name the user).
        async fn on_url(url: &str) -> RunOutcome {
            let deps = AdmissionDeps::cache_only(
                spec_directory(serde_json::json!({
                    "git": {
                        "url": url,
                        "ref": { "branch": "main" },
                        "secretRef": { "name": "git-creds" },
                    },
                }))
                .await,
            );
            let mut source = expected_run_source();
            source.git.url = url.to_string();
            let req =
                admission_request_create("uid-hatch-ssh", "team-data", &self_stamped_run(source));
            mutate_run(&req, &deps).await
        }

        assert_eq!(
            on_url("ssh://github.com/acme/r.git").await,
            RunOutcome::Rejected(
                "spec.source.git.url must name the ssh user — use \
                 ssh://git@github.com/acme/r.git, or the user your git host expects"
                    .to_string()
            )
        );
        for url in [
            "ssh://git@github.com/acme/r.git",
            "https://github.com/acme/r.git",
        ] {
            assert_eq!(on_url(url).await, RunOutcome::AcceptedAsIs, "{url}");
        }
    }

    #[tokio::test]
    async fn escape_hatch_rejects_source_for_image_mode_codelocation() {
        let deps = AdmissionDeps::cache_only(
            spec_directory(serde_json::json!({
                "image": "ghcr.io/acme/pipeline",
                "tag": "v1",
            }))
            .await,
        );
        let msg = expect_rejected(&self_stamped_run(expected_run_source()), &deps).await;
        assert!(
            msg.contains("codeLocation 'team-data/analytics' is not git-sourced"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn escape_hatch_rejects_source_when_codelocation_missing() {
        let deps = AdmissionDeps::cache_only(Arc::new(DirectoryState::new()));
        let msg = expect_rejected(&self_stamped_run(expected_run_source()), &deps).await;
        assert_eq!(
            msg,
            "codeLocation 'analytics' not found in namespace 'team-data'"
        );
    }

    #[tokio::test]
    async fn escape_hatch_accepts_own_repo_at_another_commit_and_path() {
        // A pod still serving the previous commit during a rollout stamps
        // that commit; ref, path and dependencies follow the commit.
        let deps = AdmissionDeps::cache_only(git_directory("team-data", "analytics", COMMIT).await);
        let mut source = expected_run_source();
        source.git.commit = "b".repeat(40);
        source.git.r#ref = Some("refs/heads/release".to_string());
        source.git.path = Some("legacy/analytics".to_string());
        source.dependencies =
            serde_json::from_value(serde_json::json!({ "mode": "none" })).unwrap();
        let req =
            admission_request_create("uid-hatch-other", "team-data", &self_stamped_run(source));
        assert_eq!(mutate_run(&req, &deps).await, RunOutcome::AcceptedAsIs);
    }

    #[tokio::test]
    async fn escape_hatch_builds_trees_only_with_the_code_locations_runtime_repository() {
        // The source's runtime image runs the sync init container, which
        // mounts the CodeLocation's git Secret in fallback mode.
        let deps = AdmissionDeps::cache_only(git_directory("team-data", "analytics", COMMIT).await);

        // Another digest of the same repository: a runtime image rollout.
        expect_accepted(&self_stamped_run(with_runtime_image(&runtime('b'))), &deps).await;

        let other = gpu_worker();
        assert_eq!(
            expect_rejected(&self_stamped_run(with_runtime_image(&other)), &deps).await,
            format!(
                "spec.source.runtimeImage '{other}' is not in the runtime image repository \
                 '{}' of codeLocation 'team-data/analytics'",
                runtime_repository()
            )
        );
        let tagged = format!("{}:0.5.0-py3.12", runtime_repository());
        assert_eq!(
            expect_rejected(&self_stamped_run(with_runtime_image(&tagged)), &deps).await,
            format!(
                "spec.source.runtimeImage '{tagged}' is not a digest reference \
                 (must be of the form 'registry/repo@sha256:...')"
            )
        );
    }

    /// A git CL whose pods are up before its first rollout completed: a
    /// status without `resolvedImage`.
    async fn before_the_first_rollout() -> AdmissionDeps {
        let state = git_directory("team-data", "analytics", COMMIT).await;
        let mut entry = state.lookup("team-data", "analytics").await.unwrap();
        entry.image = String::new();
        entry.phase = CodeLocationPhase::Deploying.as_str().to_string();
        state.upsert(entry).await;
        AdmissionDeps::cache_only(state)
    }

    const MIRROR: &str = "harbor.internal/rivers/rivers-runtime";

    /// A git CL whose `spec.image` moved to [`MIRROR`] while runs still get
    /// the tree built with the chart's default runtime image.
    async fn while_spec_image_moves_to_the_mirror() -> AdmissionDeps {
        let mut spec = serde_json::to_value(git_spec()).unwrap();
        spec["image"] = serde_json::json!(MIRROR);
        AdmissionDeps::cache_only(spec_directory(spec).await)
    }

    #[tokio::test]
    async fn escape_hatch_accepts_the_wanted_runtime_repository_before_the_first_rollout() {
        let deps = before_the_first_rollout().await;

        // No spec.image: the chart's default runtime image.
        expect_accepted(&self_stamped_run(with_runtime_image(&runtime('b'))), &deps).await;
    }

    #[tokio::test]
    async fn escape_hatch_accepts_both_runtime_repositories_while_spec_image_changes() {
        let deps = while_spec_image_moves_to_the_mirror().await;

        // Pods still on the previous repository, and those of the new one.
        expect_accepted(&self_stamped_run(with_runtime_image(&runtime('b'))), &deps).await;
        let new = format!("{MIRROR}@sha256:{}", "c".repeat(64));
        expect_accepted(&self_stamped_run(with_runtime_image(&new)), &deps).await;
    }

    #[tokio::test]
    async fn escape_hatch_rejects_runtime_images_outside_the_runtime_repositories() {
        let run = self_stamped_run(with_runtime_image(&gpu_worker()));
        for (deps, repositories) in [
            (
                before_the_first_rollout().await,
                format!("'{}'", runtime_repository()),
            ),
            (
                while_spec_image_moves_to_the_mirror().await,
                format!("'{}' or '{MIRROR}'", runtime_repository()),
            ),
        ] {
            assert_eq!(
                expect_rejected(&run, &deps).await,
                format!(
                    "spec.source.runtimeImage '{}' is not in the runtime image repository \
                     {repositories} of codeLocation 'team-data/analytics'",
                    gpu_worker()
                )
            );
        }
    }

    #[tokio::test]
    async fn escape_hatch_reads_codelocation_with_live_get_on_cache_miss() {
        use crate::run::test_helpers::{MockApiState, mock_client};
        let api_state = Arc::new(std::sync::Mutex::new(MockApiState::default()));
        let mut cl = CodeLocation::new("analytics", git_spec());
        cl.metadata.namespace = Some("team-data".to_string());
        cl.status = Some(
            serde_json::from_value(serde_json::json!({
                "phase": "Ready",
                "resolvedImage": runtime('a'),
                "resolvedCommit": COMMIT,
            }))
            .unwrap(),
        );
        api_state
            .lock()
            .unwrap()
            .code_locations
            .insert("analytics".to_string(), cl);
        let deps = AdmissionDeps {
            code_locations: Some(Api::namespaced(mock_client(api_state.clone()), "team-data")),
            ..AdmissionDeps::cache_only(Arc::new(DirectoryState::new()))
        };

        let mut source = expected_run_source();
        source.git.secret_name = Some("db-credentials".to_string());
        let msg = expect_rejected(&self_stamped_run(source), &deps).await;
        assert!(msg.contains("'db-credentials'"), "{msg}");

        let req = admission_request_create(
            "uid-hatch-live",
            "team-data",
            &self_stamped_run(expected_run_source()),
        );
        assert_eq!(mutate_run(&req, &deps).await, RunOutcome::AcceptedAsIs);

        let gets: Vec<String> = api_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "GET")
            .map(|r| r.path.clone())
            .collect();
        assert_eq!(
            gets,
            vec!["/apis/rivers.io/v1alpha1/namespaces/team-data/codelocations/analytics"; 2]
        );
    }

    #[tokio::test]
    async fn update_rejects_source_change() {
        let mut old = build_run("analytics", "ghcr.io/x@sha256:aaaa");
        old.spec.source = Some(expected_run_source());
        let mut new = old.clone();
        new.spec.source.as_mut().unwrap().git.commit =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let req = admission_request_update("uid-git-6", "team-data", &new, &old);
        match check_run_update(&req) {
            RunOutcome::Rejected(msg) => assert!(msg.contains("source"), "{msg}"),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    fn build_cl(name: &str, identity: &str) -> CodeLocation {
        let spec: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "ghcr.io/acme/pipeline",
            "tag": "v1",
            "identity": identity,
        }))
        .unwrap();
        CodeLocation {
            metadata: kube_client::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("team-data".to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    #[tokio::test]
    async fn create_mutates_when_codelocation_ready_and_image_unset() {
        let state = Arc::new(DirectoryState::new());
        state
            .upsert(ready_entry(
                "team-data",
                "analytics",
                "ghcr.io/x@sha256:aaaa",
                "my.module",
                "11111111-1111-4111-8111-111111111111",
            ))
            .await;

        let run = build_run("analytics", "");
        let req = admission_request_create("uid-1", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        assert_eq!(
            outcome,
            RunOutcome::Mutated {
                image: "ghcr.io/x@sha256:aaaa".to_string(),
                module: "my.module".to_string(),
                identity: "11111111-1111-4111-8111-111111111111".to_string(),
                source: None,
            }
        );
    }

    #[tokio::test]
    async fn create_rejects_when_codelocation_missing() {
        let state = Arc::new(DirectoryState::new());
        let run = build_run("analytics", "");
        let req = admission_request_create("uid-3", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        match outcome {
            RunOutcome::Rejected(msg) => {
                assert!(msg.contains("not found"));
                assert!(msg.contains("analytics"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_rejects_when_codelocation_not_ready() {
        let state = Arc::new(DirectoryState::new());
        let mut entry = ready_entry(
            "team-data",
            "analytics",
            "ghcr.io/x@sha256:cc",
            "m",
            "22222222-2222-4222-8222-222222222222",
        );
        entry.phase = CodeLocationPhase::Failed.as_str().to_string();
        state.upsert(entry).await;

        let run = build_run("analytics", "");
        let req = admission_request_create("uid-4", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        match outcome {
            RunOutcome::Rejected(msg) => {
                assert!(msg.contains("not Ready"));
                assert!(msg.contains("Failed"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_escape_hatch_accepts_pinned_digest_without_cl_lookup() {
        // Pinned digest bypasses CodeLocation resolution entirely — no
        // identity stamping, no cache lookup. Executor pod gets identity
        // from the operator-injected RIVERS_CODE_LOCATION_ID env at boot.
        let state = Arc::new(DirectoryState::new());
        let digest = format!("ghcr.io/x@sha256:{}", "a".repeat(64));
        let run = build_run("analytics", &digest);
        let req = admission_request_create("uid-5", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        assert_eq!(outcome, RunOutcome::AcceptedAsIs);
    }

    #[tokio::test]
    async fn create_escape_hatch_rejects_tag_reference() {
        let state = Arc::new(DirectoryState::new());
        let run = build_run("analytics", "ghcr.io/x:latest");
        let req = admission_request_create("uid-6", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        match outcome {
            RunOutcome::Rejected(msg) => {
                assert!(msg.contains("digest reference"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_rejects_empty_codelocation_name() {
        let state = Arc::new(DirectoryState::new());
        let run = build_run("", "");
        let req = admission_request_create("uid-7", "team-data", &run);

        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        match outcome {
            RunOutcome::Rejected(msg) => {
                assert!(msg.contains("codeLocationRef.name"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_rejects_image_change() {
        let state = Arc::new(DirectoryState::new());
        let mut old = build_run("analytics", "");
        old.spec.image = "ghcr.io/x@sha256:old".to_string();
        let mut new = old.clone();
        new.spec.image = "ghcr.io/x@sha256:new".to_string();

        let req = admission_request_update("uid-8", "team-data", &new, &old);
        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        match outcome {
            RunOutcome::Rejected(msg) => {
                assert!(msg.contains("spec.image is immutable"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_rejects_module_change() {
        let state = Arc::new(DirectoryState::new());
        let mut old = build_run("analytics", "");
        old.spec.module = "old.mod".to_string();
        let mut new = old.clone();
        new.spec.module = "new.mod".to_string();

        let req = admission_request_update("uid-9", "team-data", &new, &old);
        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        assert!(matches!(outcome, RunOutcome::Rejected(m) if m.contains("module")));
    }

    #[tokio::test]
    async fn update_rejects_codelocationref_change() {
        let state = Arc::new(DirectoryState::new());
        let old = build_run("analytics", "");
        let new = build_run("different", "");

        let req = admission_request_update("uid-10", "team-data", &new, &old);
        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        assert!(matches!(outcome, RunOutcome::Rejected(m) if m.contains("codeLocationRef")));
    }

    #[tokio::test]
    async fn update_allows_unrelated_field_change() {
        let state = Arc::new(DirectoryState::new());
        let old = build_run("analytics", "");
        let mut new = old.clone();
        new.spec.target = "different,target".to_string();

        let req = admission_request_update("uid-11", "team-data", &new, &old);
        let outcome = mutate_run(&req, &AdmissionDeps::cache_only(state)).await;
        assert_eq!(outcome, RunOutcome::UpdateAllowed);
    }

    #[tokio::test]
    async fn handle_admission_round_trip_for_rejected_create() {
        let state = Arc::new(DirectoryState::new());
        let run = build_run("missing-cl", "");
        let req = admission_request_create("uid-rt", "team-data", &run);
        let review = AdmissionReview {
            types: kube_core::metadata::TypeMeta {
                kind: "AdmissionReview".to_string(),
                api_version: "admission.k8s.io/v1".to_string(),
            },
            request: Some(req),
            response: None,
        };

        let resp = handle_run_admission(review, &AdmissionDeps::cache_only(state)).await;
        let r = resp.response.unwrap();
        assert_eq!(r.uid, "uid-rt");
        assert!(!r.allowed);
        assert!(r.result.message.contains("not found"));
    }

    #[tokio::test]
    async fn handle_admission_emits_jsonpatch_for_mutated_create() {
        let state = Arc::new(DirectoryState::new());
        state
            .upsert(ready_entry(
                "team-data",
                "analytics",
                "ghcr.io/x@sha256:dd",
                "m",
                "44444444-4444-4444-8444-444444444444",
            ))
            .await;
        let run = build_run("analytics", "");
        let req = admission_request_create("uid-pat", "team-data", &run);
        let review = AdmissionReview {
            types: kube_core::metadata::TypeMeta {
                kind: "AdmissionReview".to_string(),
                api_version: "admission.k8s.io/v1".to_string(),
            },
            request: Some(req),
            response: None,
        };

        let resp = handle_run_admission(review, &AdmissionDeps::cache_only(state)).await;
        let r = resp.response.unwrap();
        assert!(r.allowed);
        let patch_bytes = r.patch.expect("mutation produced a JSON Patch");
        let ops: Vec<serde_json::Value> = serde_json::from_slice(&patch_bytes).unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0]["op"], "replace");
        assert_eq!(ops[0]["path"], "/spec/image");
        assert_eq!(ops[0]["value"], "ghcr.io/x@sha256:dd");
        assert_eq!(ops[1]["op"], "replace");
        assert_eq!(ops[1]["path"], "/spec/module");
        assert_eq!(ops[1]["value"], "m");
        assert_eq!(ops[2]["op"], "add");
        assert_eq!(ops[2]["path"], "/spec/codeLocationRef/identity");
        assert_eq!(ops[2]["value"], "44444444-4444-4444-8444-444444444444");
    }

    #[test]
    fn cl_create_stamps_uuid_when_identity_empty() {
        let cl = build_cl("analytics", "");
        let req = cl_admission_request_create("uid-cl-1", "team-data", &cl);
        match mutate_codelocation(&req) {
            CodeLocationOutcome::IdentityStamped(uuid) => {
                assert!(is_uuid_v4(&uuid), "stamped uuid {uuid} is not v4");
            }
            other => panic!("expected IdentityStamped, got {other:?}"),
        }
    }

    #[test]
    fn cl_create_accepts_valid_user_supplied_uuid() {
        let cl = build_cl("analytics", "550e8400-e29b-41d4-a716-446655440000");
        let req = cl_admission_request_create("uid-cl-2", "team-data", &cl);
        assert_eq!(mutate_codelocation(&req), CodeLocationOutcome::AcceptedAsIs);
    }

    #[test]
    fn cl_create_rejects_non_uuid_identity() {
        let cl = build_cl("analytics", "garbage");
        let req = cl_admission_request_create("uid-cl-3", "team-data", &cl);
        match mutate_codelocation(&req) {
            CodeLocationOutcome::Rejected(msg) => {
                assert!(msg.contains("not a valid UUID v4"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn cl_create_rejects_non_v4_uuid() {
        // UUID v1 (time-based) — well-formed but wrong version.
        let cl = build_cl("analytics", "550e8400-e29b-11d4-a716-446655440000");
        let req = cl_admission_request_create("uid-cl-3b", "team-data", &cl);
        match mutate_codelocation(&req) {
            CodeLocationOutcome::Rejected(msg) => {
                assert!(msg.contains("UUID v4"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn cl_update_rejects_identity_change() {
        let old = build_cl("analytics", "550e8400-e29b-41d4-a716-446655440000");
        let new = build_cl("analytics", "11111111-1111-4111-8111-111111111111");
        let req = cl_admission_request_update("uid-cl-4", "team-data", &new, &old);
        match mutate_codelocation(&req) {
            CodeLocationOutcome::Rejected(msg) => {
                assert!(msg.contains("immutable"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn cl_update_allows_unrelated_change() {
        let old = build_cl("analytics", "550e8400-e29b-41d4-a716-446655440000");
        let mut new = old.clone();
        new.spec.tag = Some("v2".to_string());
        let req = cl_admission_request_update("uid-cl-5", "team-data", &new, &old);
        assert_eq!(
            mutate_codelocation(&req),
            CodeLocationOutcome::UpdateAllowed
        );
    }

    fn build_git_cl(git_spec: serde_json::Value) -> CodeLocation {
        let spec = serde_json::from_value(serde_json::json!({ "git": git_spec })).unwrap();
        CodeLocation::new("analytics", spec)
    }

    #[test]
    fn cl_source_validation_requires_image_or_git() {
        let spec = serde_json::from_value(serde_json::json!({ "module": "m" })).unwrap();
        let cl = CodeLocation::new("bare", spec);
        let err = validate_git_source(&cl, &[]).unwrap_err();
        assert!(err.contains("spec.image or spec.git"));
    }

    #[test]
    fn cl_git_ref_one_of_enforced_at_admission() {
        let cl = build_git_cl(serde_json::json!({
            "url": "https://forge.example/r.git",
            "ref": { "branch": "main", "tag": "v1" }
        }));
        let err = validate_git_source(&cl, &[]).unwrap_err();
        assert!(err.contains("exactly one"), "{err}");
    }

    #[test]
    fn cl_git_url_schemes_validated() {
        let ok = build_git_cl(serde_json::json!({
            "url": "ssh://git@forge.example/r.git",
            "ref": { "branch": "main" }
        }));
        validate_git_source(&ok, &[]).unwrap();

        let scp_style = build_git_cl(serde_json::json!({
            "url": "git@forge.example:acme/r.git",
            "ref": { "branch": "main" }
        }));
        assert!(validate_git_source(&scp_style, &[]).is_err());

        let ftp = build_git_cl(serde_json::json!({
            "url": "ftp://forge.example/r.git",
            "ref": { "branch": "main" }
        }));
        let err = validate_git_source(&ftp, &[]).unwrap_err();
        assert!(err.contains("unsupported"), "{err}");
    }

    const USERLESS_SSH_REJECTION: &str = "spec.git.url must name the ssh user — use \
        ssh://git@github.com/acme/r.git, or the user your git host expects";

    #[test]
    fn cl_create_rejects_ssh_url_without_a_user() {
        let cl = |url: &str| {
            build_git_cl(serde_json::json!({ "url": url, "ref": { "branch": "main" } }))
        };
        let create = |url: &str| {
            mutate_codelocation(&cl_admission_request_create(
                "uid-cl-ssh",
                "team-data",
                &cl(url),
            ))
        };

        assert_eq!(
            create("ssh://github.com/acme/r.git"),
            CodeLocationOutcome::Rejected(USERLESS_SSH_REJECTION.to_string())
        );
        for url in [
            "ssh://git@github.com/acme/r.git",
            "https://github.com/acme/r.git",
        ] {
            assert!(
                matches!(create(url), CodeLocationOutcome::IdentityStamped(_)),
                "{url}"
            );
        }
    }

    #[test]
    fn cl_update_rejects_ssh_url_without_a_user() {
        let old = build_git_cl(serde_json::json!({
            "url": "ssh://git@github.com/acme/r.git",
            "ref": { "branch": "main" }
        }));
        let mut new = old.clone();
        new.spec.git.as_mut().unwrap().url = "ssh://github.com/acme/r.git".to_string();
        let req = cl_admission_request_update("uid-cl-ssh-update", "team-data", &new, &old);
        assert_eq!(
            mutate_codelocation(&req),
            CodeLocationOutcome::Rejected(USERLESS_SSH_REJECTION.to_string())
        );

        let req = cl_admission_request_update("uid-cl-ssh-update", "team-data", &old, &old);
        assert_eq!(
            mutate_codelocation(&req),
            CodeLocationOutcome::UpdateAllowed
        );
    }

    /// A CodeLocation admitted before ssh urls had to name the user.
    fn userless_ssh_cl() -> CodeLocation {
        build_git_cl(serde_json::json!({
            "url": "ssh://github.com/acme/r.git",
            "ref": { "branch": "main" }
        }))
    }

    fn cl_update(new: &CodeLocation, old: &CodeLocation) -> CodeLocationOutcome {
        mutate_codelocation(&cl_admission_request_update(
            "uid-cl-source-update",
            "team-data",
            new,
            old,
        ))
    }

    #[test]
    fn cl_update_that_keeps_the_source_skips_source_validation() {
        let old = userless_ssh_cl();
        let mut new = old.clone();
        new.metadata.labels = Some([("team".to_string(), "data".to_string())].into());
        assert_eq!(cl_update(&new, &old), CodeLocationOutcome::UpdateAllowed);
    }

    #[test]
    fn cl_update_of_a_deleting_code_location_skips_source_validation() {
        let mut old = userless_ssh_cl();
        old.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                "2026-10-03T12:00:00Z".parse().unwrap(),
            ));
        old.metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        // The garbage collector removes its finalizer.
        let mut new = old.clone();
        new.metadata.finalizers = None;
        assert_eq!(cl_update(&new, &old), CodeLocationOutcome::UpdateAllowed);

        // Also with a changed source.
        new.spec.git.as_mut().unwrap().url = "ssh://github.com/acme/other.git".to_string();
        assert_eq!(cl_update(&new, &old), CodeLocationOutcome::UpdateAllowed);
    }

    #[test]
    fn cl_update_that_changes_the_source_validates_it() {
        let old = userless_ssh_cl();
        let mut new = old.clone();
        new.spec.git.as_mut().unwrap().url = "ssh://github.com/acme/other.git".to_string();
        assert_eq!(
            cl_update(&new, &old),
            CodeLocationOutcome::Rejected(
                "spec.git.url must name the ssh user — use ssh://git@github.com/acme/other.git, \
                 or the user your git host expects"
                    .to_string()
            )
        );

        new.spec.git.as_mut().unwrap().url = "ssh://git@github.com/acme/r.git".to_string();
        assert_eq!(cl_update(&new, &old), CodeLocationOutcome::UpdateAllowed);

        // A change of spec.image alone is a source change too.
        let old = build_cl("analytics", "");
        let mut new = old.clone();
        new.spec.image = None;
        assert_eq!(
            cl_update(&new, &old),
            CodeLocationOutcome::Rejected("one of spec.image or spec.git must be set".to_string())
        );
    }

    #[test]
    fn cl_git_host_allowlist() {
        let cl = build_git_cl(serde_json::json!({
            "url": "https://forge.example/r.git",
            "ref": { "branch": "main" }
        }));
        // Empty list admits any host.
        validate_git_source(&cl, &[]).unwrap();
        // Listed host, case-insensitively.
        validate_git_source(&cl, &["Forge.Example".to_string()]).unwrap();
        // Unlisted host rejected with the knob named.
        let err = validate_git_source(&cl, &["github.com".to_string()]).unwrap_err();
        assert!(err.contains("allowedHosts"), "{err}");
    }

    #[test]
    fn cl_create_validates_git_before_stamping_identity() {
        // Invalid git + empty identity: must reject, not stamp-and-accept.
        let cl = build_git_cl(serde_json::json!({
            "url": "https://forge.example/r.git",
            "ref": {}
        }));
        let req = cl_admission_request_create("uid-cl-git-1", "team-data", &cl);
        match mutate_codelocation(&req) {
            CodeLocationOutcome::Rejected(msg) => assert!(msg.contains("exactly one"), "{msg}"),
            other => panic!("expected Rejected, got {other:?}"),
        }

        // Valid git + empty identity still stamps.
        let cl = build_git_cl(serde_json::json!({
            "url": "https://forge.example/r.git",
            "ref": { "branch": "main" }
        }));
        let req = cl_admission_request_create("uid-cl-git-2", "team-data", &cl);
        assert!(matches!(
            mutate_codelocation(&req),
            CodeLocationOutcome::IdentityStamped(_)
        ));
    }

    #[test]
    fn cl_update_rejects_git_going_invalid() {
        let old = build_git_cl(serde_json::json!({
            "url": "https://forge.example/r.git",
            "ref": { "branch": "main" }
        }));
        let mut new = old.clone();
        new.spec.git.as_mut().unwrap().r#ref.tag = Some("v1".to_string());
        let req = cl_admission_request_update("uid-cl-git-3", "team-data", &new, &old);
        assert!(matches!(
            mutate_codelocation(&req),
            CodeLocationOutcome::Rejected(_)
        ));
    }

    #[tokio::test]
    async fn handle_codelocation_admission_emits_patch_for_create() {
        let cl = build_cl("analytics", "");
        let req = cl_admission_request_create("uid-cl-rt", "team-data", &cl);
        let review = AdmissionReview {
            types: kube_core::metadata::TypeMeta {
                kind: "AdmissionReview".to_string(),
                api_version: "admission.k8s.io/v1".to_string(),
            },
            request: Some(req),
            response: None,
        };
        let resp = handle_codelocation_admission(review).await;
        let r = resp.response.unwrap();
        assert!(r.allowed);
        let patch_bytes = r.patch.expect("CREATE produced a JSON Patch");
        let ops: Vec<serde_json::Value> = serde_json::from_slice(&patch_bytes).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0]["op"], "add");
        assert_eq!(ops[0]["path"], "/spec/identity");
        assert!(is_uuid_v4(ops[0]["value"].as_str().unwrap()));
    }

    #[test]
    fn is_uuid_v4_accepts_v4() {
        assert!(is_uuid_v4("550e8400-e29b-41d4-a716-446655440000"));
        assert!(is_uuid_v4(&Uuid::new_v4().to_string()));
    }

    #[test]
    fn is_uuid_v4_rejects_v1_and_garbage() {
        assert!(!is_uuid_v4("550e8400-e29b-11d4-a716-446655440000"));
        assert!(!is_uuid_v4("not-a-uuid"));
        assert!(!is_uuid_v4(""));
    }
}
