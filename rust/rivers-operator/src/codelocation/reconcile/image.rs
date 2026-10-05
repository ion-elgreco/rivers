//! The runtime / code-location image: `spec.image:tag` → a digest.

use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube_client::api::Api;
use rivers_k8s::crd::code_location::{
    CodeLocation, CodeLocationSpec, IMMUTABLE_TAG_ANNOTATION, REASON_AUTH_FAILED,
    REASON_DIGEST_PINNED, REASON_DIGEST_RESOLVED, REASON_RATE_LIMITED, REASON_REGISTRY_ERROR,
    REASON_TAG_NOT_FOUND,
};

use super::config::parse_refresh_interval;
use super::{Context, FOLLOWER_WAIT, TERMINAL_RETRY};
use crate::codelocation::image_auth::resolve_auth;
use crate::codelocation::registry::{
    ImageRef, RegistryAuth, RegistryError, Resolution, ResolveRequest,
};

pub(super) enum ImageOutcome {
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
pub(super) enum ImageError {
    NotFound,
    AuthFailed,
    RateLimited(Duration),
    Transient(String),
}

impl ImageError {
    pub(super) fn reason(&self) -> &'static str {
        match self {
            ImageError::NotFound => REASON_TAG_NOT_FOUND,
            ImageError::AuthFailed => REASON_AUTH_FAILED,
            ImageError::RateLimited(_) => REASON_RATE_LIMITED,
            ImageError::Transient(_) => REASON_REGISTRY_ERROR,
        }
    }

    pub(super) fn message(&self) -> String {
        match self {
            ImageError::NotFound => "tag not found in registry".into(),
            ImageError::AuthFailed => "registry authentication failed".into(),
            ImageError::RateLimited(d) => {
                format!("registry rate-limited; retrying in {}s", d.as_secs())
            }
            ImageError::Transient(msg) => msg.clone(),
        }
    }

    pub(super) fn retry_after(&self) -> Duration {
        match self {
            ImageError::RateLimited(d) => *d,
            ImageError::NotFound | ImageError::AuthFailed => TERMINAL_RETRY,
            ImageError::Transient(_) => Duration::from_secs(60),
        }
    }

    /// The registry was down or rate-limited: a later try may succeed.
    pub(super) fn is_transient(&self) -> bool {
        matches!(self, ImageError::Transient(_) | ImageError::RateLimited(_))
    }
}

pub(super) async fn resolve_image(
    cl: &CodeLocation,
    ctx: &Context,
    secrets_api: &Api<Secret>,
) -> ImageOutcome {
    if cl.spec.image.is_none() && !cl.spec.is_git() {
        return ImageOutcome::Error(ImageError::Transient(
            "spec.image is unset on an image-mode CodeLocation".into(),
        ));
    }
    let image = wanted_image(&cl.spec, &ctx.runtime_image);
    resolve_image_ref(cl, &image, ctx, secrets_api).await
}

/// The image a CL's pods run, before digest resolution: `spec.image` with
/// the CL's own tag / digest, else (git mode) the chart's runtime default,
/// whose tag and digest give way to `spec.tag` / `spec.digest` when either
/// is set.
pub(crate) fn wanted_image(spec: &CodeLocationSpec, runtime_default: &ImageRef) -> ImageRef {
    let digest = spec.digest.clone().filter(|d| !d.is_empty());
    let repository = match &spec.image {
        Some(image) => image.clone(),
        None if digest.is_none() && spec.tag.is_none() => return runtime_default.clone(),
        None => runtime_default.repository.clone(),
    };
    ImageRef {
        repository,
        tag: spec.tag.clone(),
        digest,
    }
}

/// Resolve `image` to a pinned digest reference: its digest pins it without
/// a registry call, else its tag (`latest` when it has none) is looked up.
/// One path for image mode and both git cases (see [`wanted_image`]).
async fn resolve_image_ref(
    cl: &CodeLocation,
    image: &ImageRef,
    ctx: &Context,
    secrets_api: &Api<Secret>,
) -> ImageOutcome {
    // Explicit digest wins immediately — no leader gating, no HTTP.
    if let Some(digest) = &image.digest {
        let resolved = format!("{}@{digest}", image.repository);
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

    let registry_host = first_component(&image.repository).unwrap_or_default();
    let auth: RegistryAuth = if secret_names.is_empty() {
        RegistryAuth::Anonymous
    } else {
        resolve_auth(secrets_api, &secret_names, &registry_host).await
    };

    let req = ResolveRequest {
        image: image.repository.clone(),
        tag: image.tag.clone().unwrap_or_else(|| "latest".to_string()),
        auth,
        immutable_hint,
        cache_ttl: refresh,
    };

    match ctx.registry.resolve(&req).await {
        Ok(res) => {
            let resolved = format!("{}@{}", image.repository, res.digest());
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

fn first_component(image: &str) -> Option<String> {
    image.split('/').next().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codelocation::git;
    use crate::codelocation::reconcile::WorkspaceConfig;
    use crate::codelocation::reconcile::tests::support::make_cl;
    use crate::codelocation::registry::RegistryClient;
    use crate::leader::LeaderGate;
    use crate::run::test_helpers::{MockApiState, mock_client};
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    fn digest(c: char) -> String {
        format!("sha256:{}", c.to_string().repeat(64))
    }

    /// A leader whose registry client talks plain HTTP (wiremock).
    fn context(runtime_image: &str) -> Context {
        Context {
            client: mock_client(Arc::new(std::sync::Mutex::new(MockApiState::default()))),
            namespace: "y".into(),
            registry: Arc::new(RegistryClient::with_insecure(true)),
            git: Arc::new(git::GitResolver::new(Duration::from_secs(5), false)),
            runtime_image: runtime_image.parse().unwrap(),
            leader: Arc::new(LeaderGate::leading()),
            code_location_service_account: "rivers-code-location".into(),
            workspace: WorkspaceConfig::default(),
            runs: kube_runtime::reflector::store().0,
            surreal_pod_cfg: Default::default(),
            otel_pod_cfg: Default::default(),
        }
    }

    fn host(server: &MockServer) -> String {
        server.uri().strip_prefix("http://").unwrap().to_string()
    }

    /// Serves `digest` for exactly one HEAD of `manifest_path`.
    async fn registry_serving(manifest_path: &str, digest: &str) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path(manifest_path))
            .respond_with(ResponseTemplate::new(200).insert_header("docker-content-digest", digest))
            .expect(1)
            .mount(&server)
            .await;
        server
    }

    async fn registry_never_called() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        server
    }

    fn git_cl(fields: serde_json::Value) -> CodeLocation {
        let mut spec = serde_json::json!({
            "git": { "url": "https://forge.example/r.git", "ref": { "branch": "main" } },
        });
        spec.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        make_cl(spec)
    }

    async fn resolve(cl: &CodeLocation, ctx: &Context) -> (String, &'static str) {
        let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), "y");
        match resolve_image(cl, ctx, &secrets).await {
            ImageOutcome::Resolved {
                resolved_image,
                reason,
                ..
            } => (resolved_image, reason),
            ImageOutcome::AwaitingLeader => panic!("expected an image, got AwaitingLeader"),
            ImageOutcome::Error(err) => panic!("expected an image, got {err:?}"),
        }
    }

    #[test]
    fn wanted_image_picks_tag_and_digest() {
        let repository = "ghcr.io/ion-elgreco/rivers-runtime";
        let default_digest = digest('c');
        let spec_digest = digest('d');
        let tagged: ImageRef = format!("{repository}:0.5.0-py3.12").parse().unwrap();
        let pinned: ImageRef = format!("{repository}@{default_digest}").parse().unwrap();
        let want = |repository: &str, tag: Option<&str>, digest: Option<&String>| ImageRef {
            repository: repository.to_string(),
            tag: tag.map(str::to_string),
            digest: digest.cloned(),
        };

        let cases = [
            // git without spec.image: spec.tag / spec.digest replace both
            // of the default's parts.
            (
                git_cl(serde_json::json!({})),
                &pinned,
                want(repository, None, Some(&default_digest)),
            ),
            (
                git_cl(serde_json::json!({ "tag": "0.6.0-py3.13" })),
                &pinned,
                want(repository, Some("0.6.0-py3.13"), None),
            ),
            (
                git_cl(serde_json::json!({ "digest": spec_digest })),
                &tagged,
                want(repository, None, Some(&spec_digest)),
            ),
            // An empty digest counts as unset.
            (
                git_cl(serde_json::json!({ "digest": "" })),
                &tagged,
                tagged.clone(),
            ),
            (
                make_cl(serde_json::json!({ "image": "ghcr.io/acme/pipeline", "digest": "" })),
                &tagged,
                want("ghcr.io/acme/pipeline", None, None),
            ),
        ];
        for (cl, default, expected) in cases {
            let spec = serde_json::to_string(&cl.spec).unwrap();
            assert_eq!(wanted_image(&cl.spec, default), expected, "{spec}");
        }
    }

    #[tokio::test]
    async fn git_default_resolves_its_own_tag() {
        let digest = digest('a');
        let server = registry_serving(
            "/v2/ion-elgreco/rivers-runtime/manifests/0.5.0-py3.12",
            &digest,
        )
        .await;
        let host = host(&server);
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

        let (image, reason) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

        assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
        assert_eq!(reason, REASON_DIGEST_RESOLVED);
    }

    #[tokio::test]
    async fn git_spec_tag_replaces_the_default_tag() {
        let digest = digest('b');
        let server = registry_serving(
            "/v2/ion-elgreco/rivers-runtime/manifests/0.6.0-py3.13",
            &digest,
        )
        .await;
        let host = host(&server);
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

        let cl = git_cl(serde_json::json!({ "tag": "0.6.0-py3.13" }));
        let (image, _) = resolve(&cl, &ctx).await;

        assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
    }

    #[tokio::test]
    async fn git_default_digest_is_pinned_without_a_registry_call() {
        let server = registry_never_called().await;
        let host = host(&server);
        let digest = digest('c');
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime@{digest}"));

        let (image, reason) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

        assert_eq!(image, format!("{host}/ion-elgreco/rivers-runtime@{digest}"));
        assert_eq!(reason, REASON_DIGEST_PINNED);
    }

    #[tokio::test]
    async fn git_default_without_tag_or_digest_resolves_latest() {
        let digest = digest('d');
        let server = registry_serving("/v2/rivers-runtime/manifests/latest", &digest).await;
        let host = host(&server);
        let ctx = context(&format!("{host}/rivers-runtime"));

        let (image, _) = resolve(&git_cl(serde_json::json!({})), &ctx).await;

        assert_eq!(image, format!("{host}/rivers-runtime@{digest}"));
    }

    #[tokio::test]
    async fn git_spec_image_ignores_the_default_tag() {
        let digest = digest('e');
        let server = registry_serving("/v2/acme/runtime/manifests/latest", &digest).await;
        let host = host(&server);
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

        let cl = git_cl(serde_json::json!({ "image": format!("{host}/acme/runtime") }));
        let (image, _) = resolve(&cl, &ctx).await;

        assert_eq!(image, format!("{host}/acme/runtime@{digest}"));
    }

    #[tokio::test]
    async fn image_mode_resolves_spec_tag() {
        let digest = digest('f');
        let server = registry_serving("/v2/acme/pipeline/manifests/v1.2.3", &digest).await;
        let host = host(&server);
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

        let cl = make_cl(serde_json::json!({
            "image": format!("{host}/acme/pipeline"),
            "tag": "v1.2.3",
        }));
        let (image, reason) = resolve(&cl, &ctx).await;

        assert_eq!(image, format!("{host}/acme/pipeline@{digest}"));
        assert_eq!(reason, REASON_DIGEST_RESOLVED);
    }

    #[tokio::test]
    async fn image_mode_spec_digest_is_pinned_without_a_registry_call() {
        let server = registry_never_called().await;
        let host = host(&server);
        let digest = digest('9');
        let ctx = context(&format!("{host}/ion-elgreco/rivers-runtime:0.5.0-py3.12"));

        let cl = make_cl(serde_json::json!({
            "image": format!("{host}/acme/pipeline"),
            "tag": "v1.2.3",
            "digest": digest,
        }));
        let (image, reason) = resolve(&cl, &ctx).await;

        assert_eq!(image, format!("{host}/acme/pipeline@{digest}"));
        assert_eq!(reason, REASON_DIGEST_PINNED);
    }
}
