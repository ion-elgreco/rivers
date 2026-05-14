//! Image registry client used to resolve `image:tag` to a pinned digest.
//!
//! Responsibilities:
//! * Issue `HEAD /v2/<repo>/manifests/<tag>` and read `Docker-Content-Digest`.
//! * Return the digest the registry served as-is (index digest for multi-arch
//!   images, single-manifest digest otherwise). No per-platform descent.
//! * Maintain a cross-CR cache keyed on `(registry, repo, tag)` so multiple CRs
//!   pointing at the same upstream tag share a single resolution per poll.
//! * Pause all traffic to a registry hostname when it returns `429 Retry-After`.
//! * Detect semver-ish tags (or explicit `rivers.io/tag-immutable=true`) and
//!   stop re-polling them after the first successful resolution.
//! * Gracefully handle 401 (with registry bearer-token dance) and surface
//!   terminal errors (404, persistent auth failure) as dedicated variants.
//!
//! Rate limiting, leader gating, and jittered scheduling live in the reconciler
//! — this module is concerned only with HTTP and in-memory state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::header::{
    ACCEPT, AUTHORIZATION, ETAG, HeaderMap, HeaderValue, IF_NONE_MATCH, WWW_AUTHENTICATE,
};
use reqwest::{Method, StatusCode};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::metrics;

static IMMUTABLE_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^v?\d+\.\d+\.\d+([-+.][0-9A-Za-z.-]+)?$").unwrap());

/// Tags that look like semver (`v1.2.3`, `1.2.3-rc.1`) are treated as immutable.
///
/// Callers may also opt in explicitly via the `rivers.io/tag-immutable`
/// annotation; the reconciler passes that bit in via [`ResolveRequest::immutable_hint`].
pub fn looks_immutable(tag: &str) -> bool {
    IMMUTABLE_TAG_RE.is_match(tag)
}

#[derive(Clone, Debug, Default)]
pub enum RegistryAuth {
    #[default]
    Anonymous,
    Basic {
        username: String,
        password: String,
    },
    Bearer {
        token: String,
    },
}

/// Errors a single resolve call can produce. `NotFound` and `AuthFailed`
/// are terminal; `RateLimited` and `Transient` should be retried after
/// the indicated backoff; `Malformed` indicates an unexpected wire shape.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("tag not found")]
    NotFound,
    #[error("authentication failed")]
    AuthFailed,
    #[error("rate limited (retry after {retry_after:?})")]
    RateLimited { retry_after: Duration },
    #[error("registry error (transient): {0}")]
    Transient(String),
    #[error("malformed response: {0}")]
    Malformed(String),
}

#[derive(Clone, Debug)]
pub enum Resolution {
    /// Authoritative digest from the cache (no HTTP performed).
    Cached { digest: String },
    /// Fresh resolve from the registry.
    Resolved { digest: String },
    /// `If-None-Match` matched — cached digest is still authoritative.
    Unchanged { digest: String },
    /// Polling is skipped: the tag is known-immutable and has already been
    /// resolved. Caller should use the returned digest without attempting
    /// another call until the spec changes.
    Immutable { digest: String },
}

impl Resolution {
    pub fn digest(&self) -> &str {
        match self {
            Resolution::Cached { digest }
            | Resolution::Resolved { digest }
            | Resolution::Unchanged { digest }
            | Resolution::Immutable { digest } => digest,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolveRequest {
    pub image: String,
    pub tag: String,
    pub auth: RegistryAuth,
    /// Callers who know their tag is immutable (e.g. via annotation) can hint
    /// here. Semver detection happens inside the client regardless.
    pub immutable_hint: bool,
    /// How long a cached digest remains authoritative before the next poll.
    pub cache_ttl: Duration,
}

#[derive(Clone)]
struct CacheEntry {
    digest: String,
    etag: Option<String>,
    resolved_at: Instant,
    immutable: bool,
    /// Set after a 429 or transient failure — the next poll is suppressed
    /// until this deadline even if the TTL has elapsed.
    next_attempt_after: Option<Instant>,
    /// Consecutive transient failures; drives exponential backoff for 5xx/network.
    consecutive_errors: u32,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    /// Paused-until deadline, keyed by registry hostname. Consulted before
    /// every outbound call and updated on each 429.
    rate_limited: HashMap<String, Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    registry: String,
    repository: String,
    tag: String,
}

#[derive(Clone)]
pub struct RegistryClient {
    http: reqwest::Client,
    state: Arc<Mutex<CacheState>>,
    /// When true, all registry probes use plain HTTP instead of HTTPS.
    /// Production default is false; the dev-cluster path (k3d's HTTP-only
    /// local registry) flips this on via `RIVERS_ALLOW_INSECURE_REGISTRY`
    /// so CodeLocations don't need their `spec.digest` pre-pinned.
    allow_insecure: bool,
    /// Max backoff on transient errors. Capped to 1h.
    max_backoff: Duration,
}

impl RegistryClient {
    pub fn new() -> Self {
        Self::with_insecure(false)
    }

    /// Construct with the insecure flag set explicitly. Tests use this with
    /// `true` to talk to a wiremock-backed HTTP server; production main
    /// passes the value parsed from `RIVERS_ALLOW_INSECURE_REGISTRY`.
    pub fn with_insecure(allow_insecure: bool) -> Self {
        let http = reqwest::Client::builder()
            .user_agent(concat!("rivers-operator/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client builds with default TLS");
        Self {
            http,
            state: Arc::new(Mutex::new(CacheState::default())),
            allow_insecure,
            max_backoff: Duration::from_secs(3600),
        }
    }

    fn scheme(&self) -> &'static str {
        if self.allow_insecure { "http" } else { "https" }
    }

    /// Resolve `image:tag` to a digest, consulting the cache and respecting
    /// backoff state. Returns a [`Resolution`] describing whether the result
    /// came from cache, the wire, or immutable-tag memoization.
    pub async fn resolve(&self, req: &ResolveRequest) -> Result<Resolution, RegistryError> {
        let (registry, repository) = parse_image_ref(&req.image)?;
        let key = CacheKey {
            registry: registry.clone(),
            repository: repository.clone(),
            tag: req.tag.clone(),
        };

        if let Some(hit) = self.cache_lookup(&key, req).await {
            return Ok(hit);
        }

        if let Some(pause) = self.registry_pause(&registry).await {
            metrics::registry_request(&registry, "rate_limited_cached");
            return Err(RegistryError::RateLimited { retry_after: pause });
        }

        let previous_etag = self.previous_etag(&key).await;
        let outcome = self
            .head_manifest(
                &registry,
                &repository,
                &req.tag,
                &req.auth,
                previous_etag.as_deref(),
            )
            .await;

        self.record_outcome(&key, req, outcome).await
    }

    async fn cache_lookup(&self, key: &CacheKey, req: &ResolveRequest) -> Option<Resolution> {
        let state = self.state.lock().await;
        let entry = state.entries.get(key)?;

        if entry.immutable {
            metrics::cache_hit();
            return Some(Resolution::Immutable {
                digest: entry.digest.clone(),
            });
        }

        let now = Instant::now();
        if let Some(deadline) = entry.next_attempt_after
            && now < deadline
        {
            metrics::cache_hit();
            return Some(Resolution::Cached {
                digest: entry.digest.clone(),
            });
        }

        if now.duration_since(entry.resolved_at) < req.cache_ttl {
            metrics::cache_hit();
            return Some(Resolution::Cached {
                digest: entry.digest.clone(),
            });
        }

        None
    }

    async fn registry_pause(&self, registry: &str) -> Option<Duration> {
        let state = self.state.lock().await;
        let deadline = state.rate_limited.get(registry)?;
        let now = Instant::now();
        if *deadline > now {
            Some(*deadline - now)
        } else {
            None
        }
    }

    async fn previous_etag(&self, key: &CacheKey) -> Option<String> {
        let state = self.state.lock().await;
        state.entries.get(key).and_then(|e| e.etag.clone())
    }

    async fn record_outcome(
        &self,
        key: &CacheKey,
        req: &ResolveRequest,
        outcome: Result<HeadOutcome, RegistryError>,
    ) -> Result<Resolution, RegistryError> {
        let mut state = self.state.lock().await;
        match outcome {
            Ok(HeadOutcome::Fresh { digest, etag }) => {
                let immutable = req.immutable_hint || looks_immutable(&req.tag);
                state.entries.insert(
                    key.clone(),
                    CacheEntry {
                        digest: digest.clone(),
                        etag,
                        resolved_at: Instant::now(),
                        immutable,
                        next_attempt_after: None,
                        consecutive_errors: 0,
                    },
                );
                metrics::registry_request(&key.registry, "resolved");
                Ok(Resolution::Resolved { digest })
            }
            Ok(HeadOutcome::NotModified) => {
                let Some(entry) = state.entries.get_mut(key) else {
                    // NotModified without a prior entry shouldn't happen (we
                    // only send If-None-Match when we have one), but treat as
                    // transient.
                    metrics::registry_request(&key.registry, "unexpected_304");
                    return Err(RegistryError::Malformed(
                        "304 Not Modified without cached digest".into(),
                    ));
                };
                entry.resolved_at = Instant::now();
                entry.consecutive_errors = 0;
                entry.next_attempt_after = None;
                metrics::registry_request(&key.registry, "unchanged");
                Ok(Resolution::Unchanged {
                    digest: entry.digest.clone(),
                })
            }
            Err(RegistryError::RateLimited { retry_after }) => {
                let deadline = Instant::now() + retry_after;
                state.rate_limited.insert(key.registry.clone(), deadline);
                if let Some(entry) = state.entries.get_mut(key) {
                    entry.next_attempt_after = Some(deadline);
                }
                metrics::registry_request(&key.registry, "rate_limited");
                Err(RegistryError::RateLimited { retry_after })
            }
            Err(e @ RegistryError::NotFound) => {
                // Terminal for this tag — drop any cached entry.
                state.entries.remove(key);
                metrics::registry_request(&key.registry, "not_found");
                Err(e)
            }
            Err(e @ RegistryError::AuthFailed) => {
                state.entries.remove(key);
                metrics::registry_request(&key.registry, "auth_failed");
                Err(e)
            }
            Err(e @ (RegistryError::Transient(_) | RegistryError::Malformed(_))) => {
                if let Some(entry) = state.entries.get_mut(key) {
                    entry.consecutive_errors = entry.consecutive_errors.saturating_add(1);
                    let backoff = exponential_backoff(entry.consecutive_errors, self.max_backoff);
                    entry.next_attempt_after = Some(Instant::now() + backoff);
                }
                metrics::registry_request(&key.registry, "transient");
                Err(e)
            }
        }
    }

    async fn head_manifest(
        &self,
        registry: &str,
        repository: &str,
        tag: &str,
        auth: &RegistryAuth,
        previous_etag: Option<&str>,
    ) -> Result<HeadOutcome, RegistryError> {
        let url = format!(
            "{scheme}://{registry}/v2/{repository}/manifests/{tag}",
            scheme = self.scheme(),
        );

        let mut headers = base_accept_headers();
        apply_auth(&mut headers, auth);
        if let Some(etag) = previous_etag
            && let Ok(v) = HeaderValue::from_str(etag)
        {
            headers.insert(IF_NONE_MATCH, v);
        }

        let response = self.send(Method::HEAD, &url, headers.clone()).await?;
        let status = response.status();

        if status == StatusCode::UNAUTHORIZED {
            // Either no auth provided, or the Basic/Bearer didn't satisfy the
            // challenge. If the registry offered a Bearer realm we can do a
            // token exchange and retry once; otherwise surface AuthFailed.
            let Some(challenge) = response
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|h| h.to_str().ok())
                .and_then(BearerChallenge::parse)
            else {
                return Err(RegistryError::AuthFailed);
            };

            let token = self.fetch_bearer_token(&challenge, auth).await?;
            let mut retry_headers = headers.clone();
            retry_headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|e| RegistryError::Malformed(format!("bearer header: {e}")))?,
            );
            let retried = self.send(Method::HEAD, &url, retry_headers).await?;
            return interpret_response(retried);
        }

        interpret_response(response)
    }

    async fn fetch_bearer_token(
        &self,
        challenge: &BearerChallenge,
        auth: &RegistryAuth,
    ) -> Result<String, RegistryError> {
        let mut request = self.http.get(&challenge.realm);
        if let Some(service) = &challenge.service {
            request = request.query(&[("service", service.as_str())]);
        }
        if let Some(scope) = &challenge.scope {
            request = request.query(&[("scope", scope.as_str())]);
        }
        if let RegistryAuth::Basic { username, password } = auth {
            request = request.basic_auth(username, Some(password));
        }

        let response = request
            .send()
            .await
            .map_err(|e| RegistryError::Transient(format!("token exchange: {e}")))?;

        if response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN
        {
            return Err(RegistryError::AuthFailed);
        }
        if !response.status().is_success() {
            return Err(RegistryError::Transient(format!(
                "token exchange returned {}",
                response.status()
            )));
        }

        #[derive(serde::Deserialize)]
        struct TokenBody {
            token: Option<String>,
            access_token: Option<String>,
        }

        let body: TokenBody = response
            .json()
            .await
            .map_err(|e| RegistryError::Malformed(format!("token body: {e}")))?;
        body.token
            .or(body.access_token)
            .ok_or_else(|| RegistryError::Malformed("token body missing token field".into()))
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        headers: HeaderMap,
    ) -> Result<reqwest::Response, RegistryError> {
        self.http
            .request(method, url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| RegistryError::Transient(format!("http: {e}")))
    }
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self::new()
    }
}

enum HeadOutcome {
    Fresh {
        digest: String,
        etag: Option<String>,
    },
    NotModified,
}

fn interpret_response(response: reqwest::Response) -> Result<HeadOutcome, RegistryError> {
    let status = response.status();
    if status == StatusCode::NOT_MODIFIED {
        return Ok(HeadOutcome::NotModified);
    }
    if status == StatusCode::NOT_FOUND {
        return Err(RegistryError::NotFound);
    }
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(RegistryError::AuthFailed);
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after =
            parse_retry_after(response.headers()).unwrap_or_else(|| Duration::from_secs(60));
        return Err(RegistryError::RateLimited { retry_after });
    }
    if !status.is_success() {
        return Err(RegistryError::Transient(format!("status {status}")));
    }

    let digest = response
        .headers()
        .get("docker-content-digest")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| RegistryError::Malformed("missing Docker-Content-Digest header".into()))?;

    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    Ok(HeadOutcome::Fresh { digest, etag })
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get("retry-after")?.to_str().ok()?;
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date format — we don't bother parsing it; fall back to the default.
    None
}

fn base_accept_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    // Single-manifest + index, both OCI and the older Docker media types. The
    // server picks whichever it serves for this tag and sets the digest header
    // to match what it actually returned.
    let accepts = [
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    ];
    for a in accepts {
        headers.append(ACCEPT, HeaderValue::from_static(a));
    }
    headers
}

fn apply_auth(headers: &mut HeaderMap, auth: &RegistryAuth) {
    match auth {
        RegistryAuth::Anonymous => {}
        RegistryAuth::Basic { username, password } => {
            use base64::Engine;
            let credential = format!("{username}:{password}");
            let encoded = base64::engine::general_purpose::STANDARD.encode(credential);
            if let Ok(v) = HeaderValue::from_str(&format!("Basic {encoded}")) {
                headers.insert(AUTHORIZATION, v);
            }
        }
        RegistryAuth::Bearer { token } => {
            if let Ok(v) = HeaderValue::from_str(&format!("Bearer {token}")) {
                headers.insert(AUTHORIZATION, v);
            }
        }
    }
}

fn exponential_backoff(consecutive_errors: u32, cap: Duration) -> Duration {
    let base = Duration::from_secs(60);
    // 1, 2, 4, 8, 16 ... minutes, capped.
    let shift = consecutive_errors.saturating_sub(1).min(12);
    let scaled = base.saturating_mul(1u32 << shift);
    std::cmp::min(scaled, cap)
}

/// Parse `image` into `(registry, repository)`. Docker-style shorthand
/// (`alpine`, `library/alpine`) is normalized to `registry-1.docker.io`; all
/// other forms are split on the first `/`.
fn parse_image_ref(image: &str) -> Result<(String, String), RegistryError> {
    if image.is_empty() {
        return Err(RegistryError::Malformed("empty image".into()));
    }

    let (registry, repository) = match image.split_once('/') {
        Some((head, rest)) if head.contains('.') || head.contains(':') || head == "localhost" => {
            (head.to_string(), rest.to_string())
        }
        Some(_) | None => {
            // No explicit registry: Docker Hub.
            let repo = if image.contains('/') {
                image.to_string()
            } else {
                format!("library/{image}")
            };
            ("registry-1.docker.io".to_string(), repo)
        }
    };

    Ok((registry, repository))
}

struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

impl BearerChallenge {
    fn parse(header: &str) -> Option<Self> {
        // Example: Bearer realm="https://...",service="registry.docker.io",scope="repository:a/b:pull"
        let rest = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))?;
        let mut realm = None;
        let mut service = None;
        let mut scope = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("realm=") {
                realm = Some(trim_quotes(v).to_string());
            } else if let Some(v) = part.strip_prefix("service=") {
                service = Some(trim_quotes(v).to_string());
            } else if let Some(v) = part.strip_prefix("scope=") {
                scope = Some(trim_quotes(v).to_string());
            }
        }
        Some(Self {
            realm: realm?,
            service,
            scope,
        })
    }
}

fn trim_quotes(s: &str) -> &str {
    let s = s.trim();
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semver_tags_are_immutable() {
        assert!(looks_immutable("v1.2.3"));
        assert!(looks_immutable("1.2.3"));
        assert!(looks_immutable("v1.2.3-rc.1"));
        assert!(looks_immutable("v1.2.3+build.5"));
    }

    #[test]
    fn non_semver_tags_are_mutable() {
        assert!(!looks_immutable("latest"));
        assert!(!looks_immutable("main"));
        assert!(!looks_immutable("v1.2"));
        assert!(!looks_immutable(""));
    }

    #[test]
    fn parses_docker_shorthand() {
        let (reg, repo) = parse_image_ref("alpine").unwrap();
        assert_eq!(reg, "registry-1.docker.io");
        assert_eq!(repo, "library/alpine");

        let (reg, repo) = parse_image_ref("library/alpine").unwrap();
        assert_eq!(reg, "registry-1.docker.io");
        assert_eq!(repo, "library/alpine");
    }

    #[test]
    fn parses_explicit_registry() {
        let (reg, repo) = parse_image_ref("ghcr.io/acme/pipeline").unwrap();
        assert_eq!(reg, "ghcr.io");
        assert_eq!(repo, "acme/pipeline");

        let (reg, repo) = parse_image_ref("localhost:5000/my/img").unwrap();
        assert_eq!(reg, "localhost:5000");
        assert_eq!(repo, "my/img");
    }

    #[test]
    fn bearer_challenge_parses() {
        let header = r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull""#;
        let c = BearerChallenge::parse(header).unwrap();
        assert_eq!(c.realm, "https://auth.docker.io/token");
        assert_eq!(c.service.as_deref(), Some("registry.docker.io"));
        assert_eq!(c.scope.as_deref(), Some("repository:library/alpine:pull"));
    }

    #[test]
    fn exponential_backoff_capped() {
        let cap = Duration::from_secs(3600);
        assert_eq!(exponential_backoff(1, cap), Duration::from_secs(60));
        assert_eq!(exponential_backoff(2, cap), Duration::from_secs(120));
        assert_eq!(exponential_backoff(3, cap), Duration::from_secs(240));
        // Ramps up to 1h and then stays there.
        assert_eq!(exponential_backoff(20, cap), cap);
    }

    #[test]
    fn resolution_digest_accessor() {
        let r = Resolution::Cached {
            digest: "sha256:deadbeef".into(),
        };
        assert_eq!(r.digest(), "sha256:deadbeef");
    }
}

#[cfg(test)]
mod integration_tests {
    //! Wiremock-backed tests covering the core registry-client contract:
    //!   * cross-CR cache (second resolve after success returns Cached)
    //!   * immutable semver short-circuit (second resolve returns Immutable)
    //!   * 429 Retry-After pauses subsequent resolves on the same registry
    //!   * 404 surfaces as NotFound
    //!   * 401 token dance succeeds when the challenge is answerable
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn request(image: String, tag: &str) -> ResolveRequest {
        ResolveRequest {
            image,
            tag: tag.to_string(),
            auth: RegistryAuth::Anonymous,
            immutable_hint: false,
            cache_ttl: Duration::from_secs(300),
        }
    }

    fn client_for(server: &MockServer) -> (RegistryClient, String) {
        // wiremock gives us `http://127.0.0.1:NNNN` — strip scheme so the
        // URL builder can glue it back. `with_insecure(true)` makes the
        // client talk HTTP, matching the wiremock server.
        let client = RegistryClient::with_insecure(true);
        let host = server
            .uri()
            .strip_prefix("http://")
            .expect("wiremock URI has http scheme")
            .to_string();
        (client, host)
    }

    #[tokio::test]
    async fn resolves_digest_and_caches_second_call() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        Mock::given(method("HEAD"))
            .and(path("/v2/acme/pipeline/manifests/v9.9.9"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("docker-content-digest", "sha256:abc"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/pipeline");
        let req = request(image.clone(), "v9.9.9");

        let first = client.resolve(&req).await.unwrap();
        // Semver, so this becomes Immutable; either way the digest matches.
        assert_eq!(first.digest(), "sha256:abc");

        // Second call must not hit the server (expect(1) fails the test otherwise).
        let second = client.resolve(&req).await.unwrap();
        assert_eq!(second.digest(), "sha256:abc");
    }

    #[tokio::test]
    async fn mutable_tag_respects_cache_ttl() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        Mock::given(method("HEAD"))
            .and(path("/v2/acme/pipeline/manifests/latest"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("docker-content-digest", "sha256:ttl-1"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/pipeline");
        let req = ResolveRequest {
            image: image.clone(),
            tag: "latest".into(),
            auth: RegistryAuth::Anonymous,
            immutable_hint: false,
            cache_ttl: Duration::from_secs(60),
        };

        let r1 = client.resolve(&req).await.unwrap();
        assert_eq!(r1.digest(), "sha256:ttl-1");
        assert!(matches!(r1, Resolution::Resolved { .. }));

        // Still within TTL — should not hit server.
        let r2 = client.resolve(&req).await.unwrap();
        assert_eq!(r2.digest(), "sha256:ttl-1");
        assert!(matches!(r2, Resolution::Cached { .. }));
    }

    #[tokio::test]
    async fn immutable_semver_does_not_repoll_even_when_ttl_expires() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        Mock::given(method("HEAD"))
            .and(path("/v2/acme/pipeline/manifests/v1.2.3"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("docker-content-digest", "sha256:immutable"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/pipeline");
        // 0 TTL — mutable tags would re-poll immediately, but semver is
        // detected as immutable and should not.
        let req = ResolveRequest {
            image,
            tag: "v1.2.3".into(),
            auth: RegistryAuth::Anonymous,
            immutable_hint: false,
            cache_ttl: Duration::from_secs(0),
        };

        let r1 = client.resolve(&req).await.unwrap();
        assert!(matches!(r1, Resolution::Resolved { .. }));

        let r2 = client.resolve(&req).await.unwrap();
        assert!(matches!(r2, Resolution::Immutable { .. }));
        assert_eq!(r2.digest(), "sha256:immutable");
    }

    #[tokio::test]
    async fn rate_limit_pauses_subsequent_calls() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        Mock::given(method("HEAD"))
            .and(path("/v2/acme/throttled/manifests/latest"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "42"))
            // Only ever called once — the second resolve must short-circuit
            // from the rate-limit state instead of hitting the server.
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/throttled");
        let req = ResolveRequest {
            image,
            tag: "latest".into(),
            auth: RegistryAuth::Anonymous,
            immutable_hint: false,
            cache_ttl: Duration::from_secs(60),
        };

        let first = client.resolve(&req).await.unwrap_err();
        match first {
            RegistryError::RateLimited { retry_after } => {
                assert_eq!(retry_after, Duration::from_secs(42));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }

        let second = client.resolve(&req).await.unwrap_err();
        assert!(matches!(second, RegistryError::RateLimited { .. }));
    }

    #[tokio::test]
    async fn not_found_surfaces_and_drops_cache() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        Mock::given(method("HEAD"))
            .and(path("/v2/acme/missing/manifests/v1.0.0"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/missing");
        let req = request(image, "v1.0.0");
        let err = client.resolve(&req).await.unwrap_err();
        assert!(matches!(err, RegistryError::NotFound));
    }

    #[tokio::test]
    async fn token_challenge_retries_with_bearer() {
        let server = MockServer::start().await;
        let (client, host) = client_for(&server);

        let realm = format!("{}/token", server.uri());

        // Token exchange endpoint.
        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "test-token-123"
            })))
            .expect(1)
            .mount(&server)
            .await;

        // First HEAD → 401 with bearer challenge.
        Mock::given(method("HEAD"))
            .and(path("/v2/acme/authed/manifests/v1.0.0"))
            .and(wiremock::matchers::header_exists("accept"))
            .respond_with(ResponseTemplate::new(401).insert_header(
                "www-authenticate",
                format!("Bearer realm=\"{realm}\",service=\"test\",scope=\"repository:acme/authed:pull\""),
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Second HEAD (after token) → 200 with digest.
        Mock::given(method("HEAD"))
            .and(path("/v2/acme/authed/manifests/v1.0.0"))
            .and(header("authorization", "Bearer test-token-123"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("docker-content-digest", "sha256:authed-ok"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let image = format!("{host}/acme/authed");
        let req = request(image, "v1.0.0");
        let res = client.resolve(&req).await.unwrap();
        assert_eq!(res.digest(), "sha256:authed-ok");
    }
}
