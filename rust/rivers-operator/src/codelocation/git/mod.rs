//! Git ref → commit resolution (RFC-044).
//!
//! The operator resolves `spec.git.ref` to a pinned commit the way
//! `registry.rs` resolves `tag` → digest. Both transports yield the same
//! protocol-v0 ref advertisement; the single asymmetry is the smart-HTTP
//! `# service=git-upload-pack` preamble + flush, which the parser strips
//! when present:
//!
//! ```text
//! HTTPS:  GET <url>/info/refs?service=git-upload-pack
//!         → "# service=git-upload-pack" pkt + flush, then the advertisement
//! SSH:    exec `git-upload-pack '<path>'`
//!         → the advertisement directly
//! both:   "<40-hex oid> <refname>[\0capabilities]" per pkt-line, flush-terminated
//! ```
//!
//! Annotated tags advertise both the tag object (`refs/tags/x`) and the
//! peeled commit (`refs/tags/x^{}`); resolution prefers the peel so the
//! pinned value is always a commit, matching `git ls-remote`'s behaviour.

pub mod http;
pub mod known_hosts;
pub mod ssh;

use gix_packetline::PacketLineRef;
use gix_packetline::blocking_io::StreamingPeekableIter;
use rivers_k8s::crd::code_location::GitRef;

const ZERO_OID: &str = "0000000000000000000000000000000000000000";
const HTTP_SERVICE_PREAMBLE: &[u8] = b"# service=";
const UPLOAD_PACK_SERVICE: &[u8] = b"git-upload-pack";

#[derive(Debug, Clone, thiserror::Error)]
pub enum GitError {
    /// The requested ref is not in the advertisement (or, over HTTP, the
    /// repository itself was not found). Terminal until the CR or the
    /// remote changes.
    #[error("ref not found: {0}")]
    RefNotFound(String),
    /// Credentials rejected (or unusable — e.g. an unreadable private key).
    /// Terminal until the Secret changes.
    #[error("git authentication failed: {0}")]
    AuthFailed(String),
    /// SSH host key not trusted: unknown host, or a changed key. Never
    /// auto-accepted. Terminal until the Secret's `known_hosts` changes.
    #[error("host key rejected: {0}")]
    HostKeyRejected(String),
    /// The `known_hosts` material itself is missing or unusable — distinct
    /// from "unknown host" so a mis-mounted Secret is diagnosable.
    #[error("known_hosts unavailable: {0}")]
    KnownHostsUnavailable(String),
    /// Transport-level failure (connect, timeout, 5xx). Transient.
    #[error("git host unreachable: {0}")]
    Unreachable(String),
    /// The host asked the operator to wait: HTTP 429, or 503 with a
    /// `Retry-After`. Transient; no ref on that host is fetched until the
    /// wait ends.
    #[error("rate-limited by the git host: {message}")]
    RateLimited {
        message: String,
        /// The host's `Retry-After`, when it sent a usable one.
        retry_after: Option<std::time::Duration>,
    },
    /// The response was not a protocol-v0 ref advertisement.
    #[error("malformed advertisement: {0}")]
    Malformed(String),
}

impl GitError {
    /// A failure that passes on its own; every other one stands until the
    /// CR, the Secret or the remote changes.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            GitError::Unreachable(_) | GitError::RateLimited { .. }
        )
    }
}

/// A resolve that produced no commit.
#[derive(Debug)]
pub struct GitFailure {
    pub error: GitError,
    /// Transient errors: how long until the resolver fetches the ref again.
    pub retry_after: Option<std::time::Duration>,
}

impl From<GitError> for GitFailure {
    fn from(error: GitError) -> Self {
        Self {
            error,
            retry_after: None,
        }
    }
}

/// Transport/protocol failures out of russh that we didn't classify
/// ourselves land as transient — host-key and auth outcomes are produced
/// explicitly before this conversion can occur.
impl From<russh::Error> for GitError {
    fn from(e: russh::Error) -> Self {
        GitError::Unreachable(e.to_string())
    }
}

/// One advertised `<oid> <refname>` pair, peel entries (`name^{}`) included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedRef {
    pub oid: String,
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Advertisement {
    pub refs: Vec<AdvertisedRef>,
}

impl Advertisement {
    fn oid_for(&self, name: &str) -> Option<String> {
        self.refs
            .iter()
            .find(|r| r.name == name)
            .map(|r| r.oid.clone())
    }
}

/// A pinned resolution. `ref_name` is the matched ref (`refs/heads/main`),
/// or `None` when the CR pinned an explicit commit and no lookup happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRef {
    pub commit: String,
    pub ref_name: Option<String>,
}

/// Parse a protocol-v0 ref advertisement from a fully-buffered response.
///
/// Accepts both transport shapes: the bare SSH form, and the smart-HTTP form
/// whose `# service=git-upload-pack` pkt + flush preamble is stripped here so
/// callers never see the difference. Requires the terminating flush-pkt —
/// a stream that just ends is reported as truncated, not treated as complete.
pub fn parse_advertisement(bytes: &[u8]) -> Result<Advertisement, GitError> {
    let mut iter = StreamingPeekableIter::new(bytes, &[PacketLineRef::Flush], false);
    let mut refs = Vec::new();
    let mut first_line = true;
    let mut in_preamble = false;

    loop {
        match iter.read_line() {
            Some(Ok(Ok(PacketLineRef::Data(raw)))) => {
                let line = raw.strip_suffix(b"\n").unwrap_or(raw);
                if first_line {
                    first_line = false;
                    if let Some(service) = line.strip_prefix(HTTP_SERVICE_PREAMBLE) {
                        if service != UPLOAD_PACK_SERVICE {
                            return Err(GitError::Malformed(format!(
                                "unexpected service '{}' (wanted git-upload-pack)",
                                String::from_utf8_lossy(service)
                            )));
                        }
                        // Preamble consumed; its own flush follows, then the
                        // advertisement restarts the first-line rules.
                        in_preamble = true;
                        first_line = true;
                        continue;
                    }
                    if line.starts_with(b"version ") {
                        return Err(GitError::Malformed(format!(
                            "got '{}' where a v0 advertisement was expected",
                            String::from_utf8_lossy(line)
                        )));
                    }
                }
                if let Some(r) = parse_ref_line(line)? {
                    refs.push(r);
                }
            }
            Some(Ok(Ok(other))) => {
                return Err(GitError::Malformed(format!(
                    "unexpected packet in advertisement: {other:?}"
                )));
            }
            Some(Ok(Err(decode_err))) => {
                return Err(GitError::Malformed(decode_err.to_string()));
            }
            Some(Err(io_err)) => {
                return Err(GitError::Malformed(io_err.to_string()));
            }
            None => {
                let stopped_at_flush = iter.stopped_at() == Some(PacketLineRef::Flush);
                if stopped_at_flush && in_preamble {
                    in_preamble = false;
                    iter.reset();
                    continue;
                }
                if !stopped_at_flush {
                    return Err(GitError::Malformed(
                        "advertisement not terminated by a flush-pkt (truncated response?)"
                            .to_string(),
                    ));
                }
                return Ok(Advertisement { refs });
            }
        }
    }
}

/// Parse one `<oid> <refname>[\0capabilities]` line. Returns `None` for the
/// empty-repo placeholder (`<zero-oid> capabilities^{}`).
fn parse_ref_line(line: &[u8]) -> Result<Option<AdvertisedRef>, GitError> {
    // Capabilities ride after a NUL on the first advertised ref only; the
    // symref/agent data in there is not needed for ref→commit resolution.
    let line = match line.iter().position(|&b| b == 0) {
        Some(nul) => &line[..nul],
        None => line,
    };
    let text = std::str::from_utf8(line)
        .map_err(|_| GitError::Malformed("non-UTF-8 advertisement line".to_string()))?;
    let (oid, name) = text
        .split_once(' ')
        .ok_or_else(|| GitError::Malformed(format!("expected '<oid> <ref>', got '{text}'")))?;
    if oid.len() != 40 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(GitError::Malformed(format!("'{oid}' is not a 40-hex oid")));
    }
    if name.is_empty() {
        return Err(GitError::Malformed("empty ref name".to_string()));
    }
    if oid == ZERO_OID && name == "capabilities^{}" {
        return Ok(None); // empty repository
    }
    Ok(Some(AdvertisedRef {
        oid: oid.to_string(),
        name: name.to_string(),
    }))
}

/// Resolve the CR's ref selector against an advertisement.
///
/// * `commit` — passthrough, no lookup (the caller avoids fetching at all).
/// * `branch` — exact `refs/heads/<name>`.
/// * `tag` — prefers the peeled `refs/tags/<name>^{}` (annotated tags point
///   at a tag object; the peel is the commit), falls back to the tag oid
///   itself (lightweight tags).
pub fn commit_for_ref(adv: &Advertisement, wanted: &GitRef) -> Result<ResolvedRef, GitError> {
    fn non_empty(s: &Option<String>) -> Option<&str> {
        s.as_deref().filter(|v| !v.is_empty())
    }

    if let Some(commit) = non_empty(&wanted.commit) {
        return Ok(ResolvedRef {
            commit: commit.to_string(),
            ref_name: None,
        });
    }
    if let Some(branch) = non_empty(&wanted.branch) {
        let name = format!("refs/heads/{branch}");
        return match adv.oid_for(&name) {
            Some(oid) => Ok(ResolvedRef {
                commit: oid,
                ref_name: Some(name),
            }),
            None => Err(GitError::RefNotFound(format!("{name} is not advertised"))),
        };
    }
    if let Some(tag) = non_empty(&wanted.tag) {
        let name = format!("refs/tags/{tag}");
        let peeled = format!("{name}^{{}}");
        return match adv.oid_for(&peeled).or_else(|| adv.oid_for(&name)) {
            Some(oid) => Ok(ResolvedRef {
                commit: oid,
                ref_name: Some(name),
            }),
            None => Err(GitError::RefNotFound(format!("{name} is not advertised"))),
        };
    }
    Err(GitError::Malformed(
        "git ref selects nothing — admission validation should have rejected this".to_string(),
    ))
}

/// Credential material for a resolve, straight from the CR's Secret.
/// SSH material arrives as *contents* (the operator reads the Secret over
/// the API, it doesn't mount it); the resolver writes it to short-lived
/// 0600 tempfiles because russh's key/known_hosts APIs are path-based.
#[derive(Clone, Debug, Default, Hash)]
pub enum GitCredentials {
    #[default]
    Anonymous,
    Basic {
        username: String,
        password: String,
    },
    Ssh {
        private_key_openssh: String,
        known_hosts: String,
    },
}

#[derive(Clone, Debug)]
pub struct GitResolveRequest {
    /// `https://…` / `http://…` / `ssh://…` repository URL.
    pub url: String,
    pub r#ref: GitRef,
    pub credentials: GitCredentials,
    /// How old a cached branch resolution may be and still answer this
    /// request. Semver-like tags are cached indefinitely
    /// (`registry::looks_immutable`), pinned commits never hit the network
    /// at all.
    pub cache_ttl: std::time::Duration,
}

/// The longest wait between fetches of a failing ref: the default poll
/// interval, so a host that stays down is asked no more often than a
/// default code location polls it, and its recovery shows within one poll.
const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(300);

/// The longest `Retry-After` honoured, so a far-off or garbled value does
/// not stop a host's code locations for days.
const MAX_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(3600);

/// A cached ref no request used for this long is dropped at the next insert:
/// a day, what `pollInterval: "0"` polls at. Past it, only an immutable tag
/// or a slower poll fetches again.
const MAX_IDLE: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

#[derive(Default)]
struct Cache {
    refs: std::collections::HashMap<RefKey, CachedRef>,
    /// Hosts that asked the operator to wait, by [`host_of`].
    paused_hosts: std::collections::HashMap<String, HostPause>,
}

impl Cache {
    /// Store `entry` for `key`, and drop the refs no request used for
    /// [`MAX_IDLE`] and the host pauses that ended.
    fn insert(&mut self, key: RefKey, entry: CacheEntry, now: std::time::Instant) {
        self.refs
            .retain(|_, cached| now.saturating_duration_since(cached.last_used) < MAX_IDLE);
        self.paused_hosts.retain(|_, pause| now < pause.until);
        self.refs.insert(
            key,
            CachedRef {
                entry,
                last_used: now,
            },
        );
    }
}

struct CachedRef {
    entry: CacheEntry,
    /// The last cache hit or insert.
    last_used: std::time::Instant,
}

/// A ref as one set of credentials sees it: a code location only gets a
/// cached commit or failure that its own credentials produced.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RefKey {
    url: String,
    r#ref: String,
    credentials: CredentialFingerprint,
}

/// A hash of the credentials' kind and contents, so the cache never holds
/// the secret.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CredentialFingerprint(u64);

struct HostPause {
    until: std::time::Instant,
    error: GitError,
}

enum CacheEntry {
    Resolved {
        resolved: ResolvedRef,
        resolved_at: std::time::Instant,
        immutable: bool,
    },
    /// Transient failures since the last success; no fetch before
    /// `next_attempt_after`.
    Failing {
        error: GitError,
        consecutive_errors: u32,
        next_attempt_after: std::time::Instant,
    },
}

/// Ref→commit resolver: cross-CR cache keyed on url, ref and credentials
/// (each request sets how old a cached commit it takes; refs unused for a
/// day are dropped), semver-like tags resolved once, exponential backoff
/// after transient errors, and a host that asks the operator to wait gets
/// no fetch, for any ref, until its `Retry-After` ends. Leader gating is
/// left to the reconciler. Unlike
/// [`super::registry`]'s digest cache, a ref that backs off does not serve
/// its last result: it fails until a fetch succeeds.
pub struct GitResolver {
    http: reqwest::Client,
    timeout: std::time::Duration,
    fingerprints: std::hash::RandomState,
    cache: tokio::sync::Mutex<Cache>,
}

impl GitResolver {
    pub fn new(timeout: std::time::Duration) -> Self {
        let http = reqwest::Client::builder()
            .user_agent(super::USER_AGENT)
            .build()
            .expect("reqwest client builds with default TLS");
        Self {
            http,
            timeout,
            fingerprints: std::hash::RandomState::new(),
            cache: Default::default(),
        }
    }

    pub async fn resolve(&self, req: &GitResolveRequest) -> Result<ResolvedRef, GitFailure> {
        self.resolve_at(req, std::time::Instant::now()).await
    }

    /// [`Self::resolve`] at `now` (tests step the clock).
    async fn resolve_at(
        &self,
        req: &GitResolveRequest,
        now: std::time::Instant,
    ) -> Result<ResolvedRef, GitFailure> {
        // Pinned commit: no lookup, no cache, no network.
        if let Some(commit) = req.r#ref.commit.as_deref().filter(|c| !c.is_empty()) {
            return Ok(ResolvedRef {
                commit: commit.to_string(),
                ref_name: None,
            });
        }

        let cache_key = self.ref_key(req)?;
        let host = host_of(&req.url);
        {
            let mut cache = self.cache.lock().await;
            if let Some(cached) = cache.refs.get_mut(&cache_key) {
                match &cached.entry {
                    CacheEntry::Resolved {
                        resolved,
                        resolved_at,
                        immutable,
                    } if *immutable
                        || now.saturating_duration_since(*resolved_at) < req.cache_ttl =>
                    {
                        cached.last_used = now;
                        return Ok(resolved.clone());
                    }
                    CacheEntry::Failing {
                        error,
                        next_attempt_after,
                        ..
                    } if now < *next_attempt_after => {
                        cached.last_used = now;
                        return Err(GitFailure {
                            error: error.clone(),
                            retry_after: Some(*next_attempt_after - now),
                        });
                    }
                    _ => {}
                }
            }
            if let Some(pause) = host.as_ref().and_then(|h| cache.paused_hosts.get(h))
                && now < pause.until
            {
                return Err(GitFailure {
                    error: pause.error.clone(),
                    retry_after: Some(pause.until - now),
                });
            }
        }

        let outcome = self.fetch_commit(req).await;
        let mut cache = self.cache.lock().await;
        match outcome {
            Ok(resolved) => {
                let immutable = req
                    .r#ref
                    .tag
                    .as_deref()
                    .is_some_and(super::registry::looks_immutable);
                let entry = CacheEntry::Resolved {
                    resolved: resolved.clone(),
                    resolved_at: now,
                    immutable,
                };
                cache.insert(cache_key, entry, now);
                Ok(resolved)
            }
            Err(error) if error.is_transient() => {
                let consecutive_errors = match cache.refs.get(&cache_key).map(|c| &c.entry) {
                    // A concurrent resolve of this ref failed first and
                    // counted this attempt.
                    Some(CacheEntry::Failing {
                        error: counted,
                        next_attempt_after,
                        ..
                    }) if *next_attempt_after > now => {
                        return Err(GitFailure {
                            error: counted.clone(),
                            retry_after: Some(*next_attempt_after - now),
                        });
                    }
                    Some(CacheEntry::Failing {
                        consecutive_errors, ..
                    }) => consecutive_errors.saturating_add(1),
                    _ => 1,
                };
                let mut wait =
                    super::registry::exponential_backoff(consecutive_errors, MAX_BACKOFF);
                if let GitError::RateLimited { retry_after, .. } = &error {
                    wait = wait.max(retry_after.unwrap_or_default().min(MAX_RETRY_AFTER));
                    if let Some(host) = host
                        && cache
                            .paused_hosts
                            .get(&host)
                            .is_none_or(|pause| pause.until < now + wait)
                    {
                        let pause = HostPause {
                            until: now + wait,
                            error: error.clone(),
                        };
                        cache.paused_hosts.insert(host, pause);
                    }
                }
                let entry = CacheEntry::Failing {
                    error: error.clone(),
                    consecutive_errors,
                    next_attempt_after: now + wait,
                };
                cache.insert(cache_key, entry, now);
                Err(GitFailure {
                    error,
                    retry_after: Some(wait),
                })
            }
            Err(error) => {
                cache.refs.remove(&cache_key);
                Err(error.into())
            }
        }
    }

    fn ref_key(&self, req: &GitResolveRequest) -> Result<RefKey, GitError> {
        use std::hash::BuildHasher as _;
        Ok(RefKey {
            url: req.url.clone(),
            r#ref: ref_cache_key(&req.r#ref)?,
            credentials: CredentialFingerprint(self.fingerprints.hash_one(&req.credentials)),
        })
    }

    async fn fetch_commit(&self, req: &GitResolveRequest) -> Result<ResolvedRef, GitError> {
        let bytes = self.fetch(req).await?;
        commit_for_ref(&parse_advertisement(&bytes)?, &req.r#ref)
    }

    async fn fetch(&self, req: &GitResolveRequest) -> Result<Vec<u8>, GitError> {
        let scheme = url::Url::parse(&req.url)
            .map_err(|e| GitError::Malformed(format!("invalid git url '{}': {e}", req.url)))?
            .scheme()
            .to_string();
        match scheme.as_str() {
            "http" | "https" => {
                let auth = match &req.credentials {
                    GitCredentials::Anonymous => http::GitAuth::Anonymous,
                    GitCredentials::Basic { username, password } => http::GitAuth::Basic {
                        username: username.clone(),
                        password: password.clone(),
                    },
                    GitCredentials::Ssh { .. } => {
                        return Err(GitError::AuthFailed(
                            "the Secret carries SSH material (identity/known_hosts) but the \
                             url is http(s) — provide username/password instead"
                                .to_string(),
                        ));
                    }
                };
                http::fetch_advertisement(&self.http, &req.url, &auth, self.timeout).await
            }
            "ssh" => {
                let target = ssh::parse_ssh_url(&req.url)?;
                let GitCredentials::Ssh {
                    private_key_openssh,
                    known_hosts,
                } = &req.credentials
                else {
                    return Err(GitError::AuthFailed(
                        "ssh:// urls need `identity` and `known_hosts` entries in the git Secret"
                            .to_string(),
                    ));
                };
                let material = SshTempMaterial::write(private_key_openssh, known_hosts)?;
                ssh::fetch_advertisement(&target, &material.auth, self.timeout).await
            }
            other => Err(GitError::Malformed(format!(
                "unsupported git url scheme '{other}' (https:// or ssh://)"
            ))),
        }
    }
}

/// Secret contents written to 0600 tempfiles for the duration of one fetch.
/// Backed by the operator's Memory-medium `/tmp` emptyDir (the root fs is
/// read-only), so key material stays off disk and is unlinked on drop.
struct SshTempMaterial {
    _key: tempfile::NamedTempFile,
    _known_hosts: tempfile::NamedTempFile,
    auth: ssh::SshAuth,
}

impl SshTempMaterial {
    fn write(private_key: &str, known_hosts: &str) -> Result<Self, GitError> {
        use std::io::Write as _;
        let scratch = |what: &str, contents: &str| -> Result<tempfile::NamedTempFile, GitError> {
            let mut f = tempfile::NamedTempFile::new()
                .map_err(|e| GitError::Unreachable(format!("cannot stage {what} in /tmp: {e}")))?;
            f.write_all(contents.as_bytes())
                .and_then(|()| f.flush())
                .map_err(|e| GitError::Unreachable(format!("cannot stage {what}: {e}")))?;
            Ok(f)
        };
        let key = scratch("ssh identity", private_key)?;
        let kh = scratch("known_hosts", known_hosts)?;
        let auth = ssh::SshAuth {
            private_key_path: key.path().to_path_buf(),
            known_hosts_path: kh.path().to_path_buf(),
        };
        Ok(Self {
            _key: key,
            _known_hosts: kh,
            auth,
        })
    }
}

/// `host[:port]` of a git url: what a rate limit pauses.
fn host_of(url: &str) -> Option<String> {
    let url = url::Url::parse(url).ok()?;
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// Stable cache-key fragment for a ref selector. Pinned commits never reach
/// the cache, so only branch/tag shapes occur here.
fn ref_cache_key(r#ref: &GitRef) -> Result<String, GitError> {
    if let Some(b) = r#ref.branch.as_deref().filter(|s| !s.is_empty()) {
        return Ok(format!("branch:{b}"));
    }
    if let Some(t) = r#ref.tag.as_deref().filter(|s| !s.is_empty()) {
        return Ok(format!("tag:{t}"));
    }
    Err(GitError::Malformed(
        "git ref selects nothing — admission validation should have rejected this".to_string(),
    ))
}

#[cfg(test)]
mod resolver_tests {
    use super::ssh::test_server::{self, ServerBehaviour};
    use super::*;
    use std::time::{Duration, Instant};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TIMEOUT: Duration = Duration::from_secs(5);
    const TTL: Duration = Duration::from_secs(300);

    fn adv_response() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header(
                "content-type",
                "application/x-git-upload-pack-advertisement",
            )
            .set_body_bytes(fixtures::http_adv())
    }

    async fn mock_git_server(hits: u64) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/acme/pipelines.git/info/refs"))
            .respond_with(adv_response())
            .expect(hits)
            .mount(&server)
            .await;
        server
    }

    fn request(url: String, r#ref: rivers_k8s::crd::code_location::GitRef) -> GitResolveRequest {
        GitResolveRequest {
            url,
            r#ref,
            credentials: GitCredentials::Anonymous,
            cache_ttl: TTL,
        }
    }

    #[tokio::test]
    async fn caches_branch_resolution_within_ttl() {
        let server = mock_git_server(1).await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = request(
            format!("{}/acme/pipelines.git", server.uri()),
            fixtures::branch("main"),
        );

        let first = resolver.resolve(&req).await.unwrap();
        let second = resolver.resolve(&req).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.commit, fixtures::oid('a'));
        // MockServer verifies expect(1) on drop — a second HTTP hit fails.
    }

    #[tokio::test]
    async fn expired_ttl_refetches() {
        let server = mock_git_server(2).await;
        let resolver = GitResolver::new(TIMEOUT);
        let mut req = request(
            format!("{}/acme/pipelines.git", server.uri()),
            fixtures::branch("main"),
        );
        req.cache_ttl = Duration::ZERO;

        resolver.resolve(&req).await.unwrap();
        resolver.resolve(&req).await.unwrap();
    }

    #[tokio::test]
    async fn immutable_tag_is_cached_past_ttl() {
        let server = mock_git_server(1).await;
        let resolver = GitResolver::new(TIMEOUT);
        let mut req = request(
            format!("{}/acme/pipelines.git", server.uri()),
            fixtures::tag("v1.0.0"),
        );
        req.cache_ttl = Duration::ZERO; // would expire instantly if mutable

        let first = resolver.resolve(&req).await.unwrap();
        let second = resolver.resolve(&req).await.unwrap();
        assert_eq!(first.commit, fixtures::oid('d')); // peeled commit
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn pinned_commit_never_touches_the_network() {
        let server = mock_git_server(0).await;
        let resolver = GitResolver::new(TIMEOUT);
        let pinned = rivers_k8s::crd::code_location::GitRef {
            commit: Some(fixtures::oid('9')),
            ..Default::default()
        };
        let req = request(format!("{}/acme/pipelines.git", server.uri()), pinned);

        let resolved = resolver.resolve(&req).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('9'));
        assert_eq!(resolved.ref_name, None);
    }

    #[tokio::test]
    async fn ssh_scheme_dispatches_through_russh() {
        let port = test_server::spawn_server(ServerBehaviour::default()).await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = GitResolveRequest {
            url: format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git"),
            r#ref: fixtures::branch("main"),
            credentials: GitCredentials::Ssh {
                private_key_openssh: test_server::CLIENT_KEY.to_string(),
                known_hosts: format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB),
            },
            cache_ttl: TTL,
        };

        let resolved = resolver.resolve(&req).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/heads/main"));
    }

    #[tokio::test]
    async fn ssh_url_with_non_ssh_credentials_is_auth_failed() {
        let resolver = GitResolver::new(TIMEOUT);
        let req = GitResolveRequest {
            url: "ssh://git@forge.example/r.git".to_string(),
            r#ref: fixtures::branch("main"),
            credentials: GitCredentials::Anonymous,
            cache_ttl: TTL,
        };
        let err = resolver.resolve(&req).await.unwrap_err().error;
        assert!(
            matches!(err, GitError::AuthFailed(ref m) if m.contains("identity")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn unsupported_scheme_is_malformed() {
        let resolver = GitResolver::new(TIMEOUT);
        let req = request(
            "ftp://forge.example/r.git".to_string(),
            fixtures::branch("m"),
        );
        let err = resolver.resolve(&req).await.unwrap_err().error;
        assert!(matches!(err, GitError::Malformed(_)), "{err}");
    }

    /// A git server answering ref polls with each response its number of
    /// times in turn; `None` answers every later poll.
    async fn git_server(responses: Vec<(ResponseTemplate, Option<u64>)>) -> MockServer {
        let server = MockServer::start().await;
        for (response, times) in responses {
            let mock = Mock::given(method("GET"))
                .and(path("/acme/pipelines.git/info/refs"))
                .respond_with(response);
            match times {
                Some(n) => mock.up_to_n_times(n),
                None => mock,
            }
            .mount(&server)
            .await;
        }
        server
    }

    fn main_of(server: &MockServer) -> GitResolveRequest {
        request(
            format!("{}/acme/pipelines.git", server.uri()),
            fixtures::branch("main"),
        )
    }

    async fn polls(server: &MockServer) -> usize {
        server.received_requests().await.unwrap().len()
    }

    #[tokio::test]
    async fn transient_failures_back_off_exponentially_and_a_success_resets_it() {
        let server = git_server(vec![
            (ResponseTemplate::new(503), Some(5)),
            (adv_response(), Some(1)),
            (ResponseTemplate::new(503), None),
        ])
        .await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = main_of(&server);

        let mut now = Instant::now();
        let mut backoffs = Vec::new();
        for _ in 0..5 {
            let failure = resolver.resolve_at(&req, now).await.unwrap_err();
            assert!(
                matches!(failure.error, GitError::Unreachable(_)),
                "{}",
                failure.error
            );
            let backoff = failure.retry_after.expect("a transient error backs off");
            // A second before the backoff ends, the host is not asked.
            let waiting = resolver
                .resolve_at(&req, now + backoff - Duration::from_secs(1))
                .await
                .unwrap_err();
            assert_eq!(waiting.retry_after, Some(Duration::from_secs(1)));
            backoffs.push(backoff.as_secs());
            now += backoff;
        }
        assert_eq!(backoffs, [60, 120, 240, 300, 300]);

        let resolved = resolver.resolve_at(&req, now).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
        let failure = resolver.resolve_at(&req, now + TTL).await.unwrap_err();
        assert_eq!(failure.retry_after, Some(Duration::from_secs(60)));
        assert_eq!(polls(&server).await, 7);
    }

    #[tokio::test]
    async fn a_ref_backing_off_stays_unresolved_until_a_fetch_succeeds() {
        let server = git_server(vec![
            (adv_response(), Some(1)),
            (ResponseTemplate::new(503), Some(1)),
            (adv_response(), None),
        ])
        .await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = main_of(&server);
        let start = Instant::now();
        resolver.resolve_at(&req, start).await.unwrap();
        let failed_at = start + TTL;
        let failure = resolver.resolve_at(&req, failed_at).await.unwrap_err();

        // The host answers again, but until the backoff ends the last error
        // stands: the commit resolved before it is not current.
        let waiting = resolver
            .resolve_at(&req, failed_at + Duration::from_secs(30))
            .await
            .unwrap_err();
        assert_eq!(waiting.error.to_string(), failure.error.to_string());
        assert_eq!(waiting.retry_after, Some(Duration::from_secs(30)));
        assert_eq!(polls(&server).await, 2);

        let resolved = resolver
            .resolve_at(&req, failed_at + Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
    }

    #[tokio::test]
    async fn concurrent_failures_of_a_ref_count_once() {
        let slow_503 = ResponseTemplate::new(503).set_delay(Duration::from_millis(200));
        let server = git_server(vec![(slow_503, None)]).await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = main_of(&server);
        let now = Instant::now();

        let (a, b) = tokio::join!(
            resolver.resolve_at(&req, now),
            resolver.resolve_at(&req, now)
        );

        assert_eq!(polls(&server).await, 2);
        assert_eq!(a.unwrap_err().retry_after, Some(Duration::from_secs(60)));
        assert_eq!(b.unwrap_err().retry_after, Some(Duration::from_secs(60)));
        let next = resolver
            .resolve_at(&req, now + Duration::from_secs(60))
            .await
            .unwrap_err();
        assert_eq!(next.retry_after, Some(Duration::from_secs(120)));
    }

    #[tokio::test]
    async fn an_answer_from_the_host_ends_the_backoff() {
        let server = git_server(vec![
            (ResponseTemplate::new(503), Some(2)),
            (ResponseTemplate::new(401), Some(1)),
            (ResponseTemplate::new(503), None),
        ])
        .await;
        let resolver = GitResolver::new(TIMEOUT);
        let req = main_of(&server);
        let start = Instant::now();
        resolver.resolve_at(&req, start).await.unwrap_err();
        let second = resolver
            .resolve_at(&req, start + Duration::from_secs(60))
            .await
            .unwrap_err();
        assert_eq!(second.retry_after, Some(Duration::from_secs(120)));

        let answered_at = start + Duration::from_secs(180);
        let auth = resolver.resolve_at(&req, answered_at).await.unwrap_err();
        assert!(
            matches!(auth.error, GitError::AuthFailed(_)),
            "{}",
            auth.error
        );
        assert_eq!(auth.retry_after, None);
        let next = resolver.resolve_at(&req, answered_at).await.unwrap_err();
        assert_eq!(next.retry_after, Some(Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn requests_carry_the_operator_user_agent() {
        let server = mock_git_server(1).await;
        let resolver = GitResolver::new(TIMEOUT);

        resolver.resolve(&main_of(&server)).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let agent = requests[0]
            .headers
            .get("user-agent")
            .map(|v| v.to_str().unwrap());
        assert_eq!(
            agent,
            Some(concat!("rivers-operator/", env!("CARGO_PKG_VERSION")))
        );
    }

    fn rate_limited(server: &MockServer, status: &str) -> String {
        format!(
            "rate-limited by the git host: HTTP {status} from {}",
            server.address()
        )
    }

    #[tokio::test]
    async fn retry_after_pauses_every_ref_on_the_host() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "900"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(adv_response())
            .mount(&server)
            .await;
        let resolver = GitResolver::new(TIMEOUT);
        let main = main_of(&server);
        let other = request(
            format!("{}/acme/other.git", server.uri()),
            fixtures::branch("dev"),
        );
        let main_with_token = GitResolveRequest {
            credentials: basic(USER, TOKEN),
            ..main_of(&server)
        };
        let start = Instant::now();

        let throttled = resolver.resolve_at(&main, start).await.unwrap_err();
        assert_eq!(throttled.retry_after, Some(Duration::from_secs(900)));
        assert_eq!(
            throttled.error.to_string(),
            rate_limited(&server, "429 Too Many Requests")
        );

        // No ref on the host, with any credentials, is fetched before the
        // 900s are up.
        let almost = start + Duration::from_secs(899);
        for req in [&other, &main_with_token, &main] {
            let waiting = resolver.resolve_at(req, almost).await.unwrap_err();
            assert_eq!(
                waiting.retry_after,
                Some(Duration::from_secs(1)),
                "{}",
                req.url
            );
            assert_eq!(waiting.error.to_string(), throttled.error.to_string());
        }
        assert_eq!(polls(&server).await, 1);

        let after = start + Duration::from_secs(900);
        let resolved = resolver.resolve_at(&other, after).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('b'));
        let resolved = resolver.resolve_at(&main, after).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
    }

    #[tokio::test]
    async fn rate_limit_without_retry_after_backs_the_host_off_exponentially() {
        let server = git_server(vec![
            (ResponseTemplate::new(429), Some(2)),
            (adv_response(), None),
        ])
        .await;
        let resolver = GitResolver::new(TIMEOUT);
        let main = main_of(&server);
        let dev = request(main.url.clone(), fixtures::branch("dev"));
        let start = Instant::now();

        let first = resolver.resolve_at(&main, start).await.unwrap_err();
        assert_eq!(first.retry_after, Some(Duration::from_secs(60)));
        // The other ref on the host waits out the same backoff.
        let waiting = resolver
            .resolve_at(&dev, start + Duration::from_secs(59))
            .await
            .unwrap_err();
        assert_eq!(waiting.retry_after, Some(Duration::from_secs(1)));
        assert_eq!(
            waiting.error.to_string(),
            rate_limited(&server, "429 Too Many Requests")
        );
        let second = resolver
            .resolve_at(&main, start + Duration::from_secs(60))
            .await
            .unwrap_err();
        assert_eq!(second.retry_after, Some(Duration::from_secs(120)));
        assert_eq!(polls(&server).await, 2);

        let resolved = resolver
            .resolve_at(&dev, start + Duration::from_secs(180))
            .await
            .unwrap();
        assert_eq!(resolved.commit, fixtures::oid('b'));
    }

    #[tokio::test]
    async fn retry_after_sets_how_long_the_host_waits() {
        let in_15_minutes = jiff::fmt::rfc2822::DateTimePrinter::new()
            .timestamp_to_rfc9110_string(&(jiff::Timestamp::now() + Duration::from_secs(900)))
            .unwrap();
        let secs = Duration::from_secs;
        let cases = [
            // (status, Retry-After, shortest wait, longest wait)
            (429, in_15_minutes.as_str(), secs(890), secs(900)),
            (503, "900", secs(900), secs(900)),
            // An hour at most.
            (429, "86400", secs(3600), secs(3600)),
            // Never sooner than the backoff without one.
            (429, "5", secs(60), secs(60)),
            (429, "soon", secs(60), secs(60)),
        ];
        for (status, retry_after, shortest, longest) in cases {
            let response = ResponseTemplate::new(status).insert_header("retry-after", retry_after);
            let server = git_server(vec![(response, None)]).await;
            let failure = GitResolver::new(TIMEOUT)
                .resolve_at(&main_of(&server), Instant::now())
                .await
                .unwrap_err();

            let case = format!("HTTP {status}, Retry-After {retry_after}");
            let wait = failure.retry_after.expect(&case);
            assert!((shortest..=longest).contains(&wait), "{case}: {wait:?}");
            let reason = reqwest::StatusCode::from_u16(status).unwrap().to_string();
            assert_eq!(
                failure.error.to_string(),
                rate_limited(&server, &reason),
                "{case}"
            );
        }
    }

    /// The advertisement after a push moved `main` to commit 9.
    fn pushed_response() -> ResponseTemplate {
        let mut body = fixtures::pkt("# service=git-upload-pack\n");
        body.extend(fixtures::flush());
        body.extend(fixtures::body(&[format!(
            "{} refs/heads/main\n",
            fixtures::oid('9')
        )]));
        adv_response().set_body_bytes(body)
    }

    #[tokio::test]
    async fn each_request_sets_how_old_a_cached_commit_may_be() {
        let server = git_server(vec![(adv_response(), Some(1)), (pushed_response(), None)]).await;
        let resolver = GitResolver::new(TIMEOUT);
        let daily = GitResolveRequest {
            cache_ttl: Duration::from_secs(24 * 3600),
            ..main_of(&server)
        };
        let every_minute = GitResolveRequest {
            cache_ttl: Duration::from_secs(60),
            ..main_of(&server)
        };
        let start = Instant::now();
        let first = resolver.resolve_at(&daily, start).await.unwrap();
        assert_eq!(first.commit, fixtures::oid('a'));

        let later = start + Duration::from_secs(120);
        let cached = resolver.resolve_at(&daily, later).await.unwrap();
        assert_eq!(cached.commit, fixtures::oid('a'));
        assert_eq!(polls(&server).await, 1);
        let fetched = resolver.resolve_at(&every_minute, later).await.unwrap();
        assert_eq!(fetched.commit, fixtures::oid('9'));
        assert_eq!(polls(&server).await, 2);
        let cached = resolver.resolve_at(&daily, later).await.unwrap();
        assert_eq!(cached.commit, fixtures::oid('9'));
        assert_eq!(polls(&server).await, 2);
    }

    const USER: &str = "ci-bot";
    const TOKEN: &str = "glpat-valid-8f1c2d";

    fn basic(username: &str, password: &str) -> GitCredentials {
        GitCredentials::Basic {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    fn ssh_credentials() -> GitCredentials {
        GitCredentials::Ssh {
            private_key_openssh: test_server::CLIENT_KEY.to_string(),
            known_hosts: format!("forge.example {}", test_server::HOST_PUB),
        }
    }

    /// A private repository: it advertises its refs to `USER`/`TOKEN` and
    /// answers everyone else with 401.
    async fn private_git_server() -> MockServer {
        use base64::Engine as _;
        let server = MockServer::start().await;
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{TOKEN}"));
        Mock::given(method("GET"))
            .and(path("/acme/pipelines.git/info/refs"))
            .and(header("authorization", format!("Basic {token}").as_str()))
            .respond_with(adv_response())
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn a_resolution_is_shared_only_with_the_same_credentials() {
        let server = private_git_server().await;
        let resolver = GitResolver::new(TIMEOUT);
        let url = format!("{}/acme/pipelines.git", server.uri());
        let release = |credentials| GitResolveRequest {
            credentials,
            ..request(url.clone(), fixtures::tag("v1.0.0"))
        };
        let now = Instant::now();
        let resolved = resolver
            .resolve_at(&release(basic(USER, TOKEN)), now)
            .await
            .unwrap();
        assert_eq!(resolved.commit, fixtures::oid('d'));

        let others = [
            ("no credentials", GitCredentials::Anonymous),
            ("a revoked token", basic(USER, "glpat-revoked-0000")),
            ("another user", basic("someone-else", TOKEN)),
            ("SSH credentials", ssh_credentials()),
        ];
        for (case, credentials) in others {
            let outcome = resolver.resolve_at(&release(credentials), now).await;
            assert!(
                matches!(
                    &outcome,
                    Err(GitFailure {
                        error: GitError::AuthFailed(_),
                        retry_after: None
                    })
                ),
                "{case}: {outcome:?}"
            );
        }
        // Each username/password asked the host; SSH material never fits an
        // https url.
        assert_eq!(polls(&server).await, 4);

        // The same credentials from another code location share the commit.
        let shared = resolver
            .resolve_at(&release(basic(USER, TOKEN)), now)
            .await
            .unwrap();
        assert_eq!(shared, resolved);
        assert_eq!(polls(&server).await, 4);
    }

    #[tokio::test]
    async fn an_auth_failure_drops_the_commit_those_credentials_resolved() {
        let server = git_server(vec![
            (adv_response(), Some(1)),
            (ResponseTemplate::new(401), None),
        ])
        .await;
        let resolver = GitResolver::new(TIMEOUT);
        let every_minute = GitResolveRequest {
            credentials: basic(USER, TOKEN),
            cache_ttl: Duration::from_secs(60),
            ..main_of(&server)
        };
        let daily = GitResolveRequest {
            cache_ttl: Duration::from_secs(24 * 3600),
            ..every_minute.clone()
        };
        let start = Instant::now();
        resolver.resolve_at(&every_minute, start).await.unwrap();

        // The token is revoked: once a fetch is refused, the daily request
        // no longer gets the commit, young enough as it is.
        let later = start + Duration::from_secs(120);
        for req in [&every_minute, &daily] {
            let outcome = resolver.resolve_at(req, later).await;
            assert!(
                matches!(
                    &outcome,
                    Err(GitFailure {
                        error: GitError::AuthFailed(_),
                        retry_after: None
                    })
                ),
                "{outcome:?}"
            );
        }
        assert_eq!(polls(&server).await, 3);
    }

    #[test]
    fn a_cache_key_holds_no_secret() {
        let resolver = GitResolver::new(TIMEOUT);
        let key = |credentials| {
            let req = GitResolveRequest {
                credentials,
                ..request(
                    "https://forge.example/acme/pipelines.git".to_string(),
                    fixtures::branch("main"),
                )
            };
            resolver.ref_key(&req).unwrap()
        };

        for credentials in [basic(USER, TOKEN), ssh_credentials()] {
            let shown = format!("{:?}", key(credentials));
            for secret in [USER, TOKEN]
                .into_iter()
                .chain(test_server::CLIENT_KEY.lines())
            {
                assert!(!shown.contains(secret), "{shown}");
            }
        }
        let no_contents = [
            key(GitCredentials::Anonymous),
            key(basic("", "")),
            key(GitCredentials::Ssh {
                private_key_openssh: String::new(),
                known_hosts: String::new(),
            }),
        ];
        assert_ne!(no_contents[0], no_contents[1]);
        assert_ne!(no_contents[0], no_contents[2]);
        assert_ne!(no_contents[1], no_contents[2]);
    }

    /// Which of `reqs` the resolver holds a commit or failure for.
    async fn held(resolver: &GitResolver, reqs: &[&GitResolveRequest]) -> Vec<bool> {
        let cache = resolver.cache.lock().await;
        reqs.iter()
            .map(|req| cache.refs.contains_key(&resolver.ref_key(req).unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn an_insert_drops_the_refs_no_request_used_for_a_day() {
        let server = git_server(vec![(adv_response(), None)]).await;
        let resolver = GitResolver::new(TIMEOUT);
        let hour = Duration::from_secs(3600);
        let daily = |git_ref| GitResolveRequest {
            cache_ttl: 24 * hour,
            ..request(format!("{}/acme/pipelines.git", server.uri()), git_ref)
        };
        let release = |token: &str| GitResolveRequest {
            credentials: basic(USER, token),
            ..daily(fixtures::tag("v1.0.0"))
        };
        let old_token = release("glpat-rotated-away");
        let new_token = release(TOKEN);
        let main = daily(fixtures::branch("main"));
        let dev = daily(fixtures::branch("dev"));
        let start = Instant::now();
        for req in [&old_token, &main, &dev] {
            resolver.resolve_at(req, start).await.unwrap();
        }
        // The Secret rotates to the new token; main is asked for again.
        resolver.resolve_at(&new_token, start + hour).await.unwrap();
        resolver.resolve_at(&main, start + 12 * hour).await.unwrap();
        assert_eq!(polls(&server).await, 4);

        let light = daily(fixtures::tag("light"));
        resolver
            .resolve_at(&light, start + 24 * hour)
            .await
            .unwrap();
        assert_eq!(
            held(&resolver, &[&old_token, &new_token, &main, &dev, &light]).await,
            [false, true, true, false, true]
        );
    }

    async fn paused_hosts(resolver: &GitResolver) -> Vec<String> {
        let cache = resolver.cache.lock().await;
        let mut hosts: Vec<String> = cache.paused_hosts.keys().cloned().collect();
        hosts.sort();
        hosts
    }

    #[tokio::test]
    async fn an_insert_drops_the_host_pauses_that_ended() {
        let mut servers = Vec::new();
        for retry_after in ["900", "3600"] {
            let throttled = ResponseTemplate::new(429).insert_header("retry-after", retry_after);
            servers.push(git_server(vec![(throttled, Some(1)), (adv_response(), None)]).await);
        }
        let host = |server: &MockServer| server.address().to_string();
        let resolver = GitResolver::new(TIMEOUT);
        let start = Instant::now();
        for server in &servers {
            resolver
                .resolve_at(&main_of(server), start)
                .await
                .unwrap_err();
        }
        let mut both = vec![host(&servers[0]), host(&servers[1])];
        both.sort();
        assert_eq!(paused_hosts(&resolver).await, both);

        let ended = start + Duration::from_secs(900);
        resolver
            .resolve_at(&main_of(&servers[0]), ended)
            .await
            .unwrap();
        assert_eq!(paused_hosts(&resolver).await, [host(&servers[1])]);
    }
}

/// Byte-stream fixtures shared by the parser tests and the transport tests —
/// the same table drives both, which is what proves the parser is shared.
#[cfg(test)]
pub(crate) mod fixtures {
    use rivers_k8s::crd::code_location::GitRef;

    pub fn oid(c: char) -> String {
        c.to_string().repeat(40)
    }

    pub fn pkt(payload: &str) -> Vec<u8> {
        let mut v = format!("{:04x}", payload.len() + 4).into_bytes();
        v.extend_from_slice(payload.as_bytes());
        v
    }

    pub fn flush() -> Vec<u8> {
        b"0000".to_vec()
    }

    /// pkt-encode `lines` and terminate with a flush-pkt.
    pub fn body(lines: &[String]) -> Vec<u8> {
        let mut v: Vec<u8> = lines.iter().flat_map(|l| pkt(l)).collect();
        v.extend(flush());
        v
    }

    /// The advertisement exactly as `git-upload-pack` emits it over SSH.
    pub fn ssh_adv() -> Vec<u8> {
        body(&[
            format!(
                "{} HEAD\0multi_ack thin-pack symref=HEAD:refs/heads/main agent=git/2.43.0\n",
                oid('a')
            ),
            format!("{} refs/heads/dev\n", oid('b')),
            format!("{} refs/heads/main\n", oid('a')),
            format!("{} refs/tags/light\n", oid('e')),
            format!("{} refs/tags/main\n", oid('f')), // name collision with the branch
            format!("{} refs/tags/v1.0.0\n", oid('c')), // annotated tag object
            format!("{} refs/tags/v1.0.0^{{}}\n", oid('d')), // its peeled commit
        ])
    }

    /// The same advertisement behind the smart-HTTP service preamble.
    pub fn http_adv() -> Vec<u8> {
        let mut v = pkt("# service=git-upload-pack\n");
        v.extend(flush());
        v.extend(ssh_adv());
        v
    }

    pub fn branch(name: &str) -> GitRef {
        GitRef {
            branch: Some(name.to_string()),
            ..Default::default()
        }
    }

    pub fn tag(name: &str) -> GitRef {
        GitRef {
            tag: Some(name.to_string()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn parses_ssh_form_and_matches_branch() {
        let adv = parse_advertisement(&ssh_adv()).unwrap();
        let resolved = commit_for_ref(&adv, &branch("main")).unwrap();
        assert_eq!(resolved.commit, oid('a'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/heads/main"));
    }

    #[test]
    fn http_preamble_is_stripped_and_yields_identical_refs() {
        let ssh = parse_advertisement(&ssh_adv()).unwrap();
        let http = parse_advertisement(&http_adv()).unwrap();
        assert_eq!(ssh.refs, http.refs, "one parser, two transports");
        assert!(!ssh.refs.is_empty());
    }

    #[test]
    fn annotated_tag_prefers_peeled_commit() {
        let adv = parse_advertisement(&ssh_adv()).unwrap();
        let resolved = commit_for_ref(&adv, &tag("v1.0.0")).unwrap();
        // The tag *object* is oid('c'); the commit it points at is oid('d').
        assert_eq!(resolved.commit, oid('d'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/tags/v1.0.0"));
    }

    #[test]
    fn lightweight_tag_resolves_to_its_own_oid() {
        let adv = parse_advertisement(&ssh_adv()).unwrap();
        let resolved = commit_for_ref(&adv, &tag("light")).unwrap();
        assert_eq!(resolved.commit, oid('e'));
    }

    #[test]
    fn branch_and_tag_sharing_a_name_do_not_collide() {
        let adv = parse_advertisement(&ssh_adv()).unwrap();
        assert_eq!(
            commit_for_ref(&adv, &branch("main")).unwrap().commit,
            oid('a')
        );
        assert_eq!(commit_for_ref(&adv, &tag("main")).unwrap().commit, oid('f'));
    }

    #[test]
    fn pinned_commit_short_circuits_without_advertisement() {
        let pinned = GitRef {
            commit: Some(oid('9')),
            ..Default::default()
        };
        let adv = Advertisement { refs: Vec::new() };
        let resolved = commit_for_ref(&adv, &pinned).unwrap();
        assert_eq!(resolved.commit, oid('9'));
        assert_eq!(resolved.ref_name, None);
    }

    #[test]
    fn absent_ref_is_ref_not_found() {
        let adv = parse_advertisement(&ssh_adv()).unwrap();
        let err = commit_for_ref(&adv, &branch("gone")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(ref m) if m.contains("refs/heads/gone")));
        let err = commit_for_ref(&adv, &tag("gone")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(ref m) if m.contains("refs/tags/gone")));
    }

    #[test]
    fn empty_repo_advertises_no_refs() {
        let adv_bytes = body(&[format!(
            "{} capabilities^{{}}\0multi_ack agent=git/2.43.0\n",
            "0".repeat(40)
        )]);
        let adv = parse_advertisement(&adv_bytes).unwrap();
        assert!(adv.refs.is_empty());
        let err = commit_for_ref(&adv, &branch("main")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(_)));
    }

    #[test]
    fn wrong_http_service_is_malformed() {
        let mut v = pkt("# service=git-receive-pack\n");
        v.extend(flush());
        v.extend(ssh_adv());
        let err = parse_advertisement(&v).unwrap_err();
        assert!(matches!(err, GitError::Malformed(ref m) if m.contains("git-receive-pack")));
    }

    #[test]
    fn protocol_v2_response_is_rejected() {
        // We never request v2, so a "version 2" banner in v0 position means
        // something is off — fail loudly instead of misparsing.
        let v = body(&["version 2\n".to_string(), "agent=git/2.43.0\n".to_string()]);
        let err = parse_advertisement(&v).unwrap_err();
        assert!(matches!(err, GitError::Malformed(ref m) if m.contains("version 2")));
    }

    #[test]
    fn truncated_stream_is_malformed() {
        let mut v = ssh_adv();
        v.truncate(v.len() - 10); // chop mid-pkt, losing the trailing flush
        assert!(matches!(
            parse_advertisement(&v),
            Err(GitError::Malformed(_))
        ));
    }

    #[test]
    fn missing_terminating_flush_is_malformed() {
        let v: Vec<u8> = pkt(&format!("{} refs/heads/main\n", oid('a')));
        assert!(matches!(
            parse_advertisement(&v),
            Err(GitError::Malformed(_))
        ));
    }

    #[test]
    fn garbage_line_is_malformed() {
        let v = body(&["not an advertisement line\n".to_string()]);
        assert!(matches!(
            parse_advertisement(&v),
            Err(GitError::Malformed(_))
        ));
    }
}
