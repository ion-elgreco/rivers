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

#[derive(Debug, thiserror::Error)]
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
    /// The response was not a protocol-v0 ref advertisement.
    #[error("malformed advertisement: {0}")]
    Malformed(String),
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
#[derive(Clone, Debug, Default)]
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
    /// How long a branch resolution stays cached. Semver-like tags are
    /// cached indefinitely (`registry::looks_immutable`), pinned commits
    /// never hit the network at all.
    pub cache_ttl: std::time::Duration,
}

struct CachedResolution {
    resolved: ResolvedRef,
    expires_at: std::time::Instant,
    immutable: bool,
}

/// Ref→commit resolver with the same posture as [`super::registry`]'s
/// digest resolver: cross-CR cache keyed on `(url, ref)`, immutable-tag
/// short-circuit, leader gating left to the reconciler.
pub struct GitResolver {
    http: reqwest::Client,
    timeout: std::time::Duration,
    cache: tokio::sync::Mutex<std::collections::HashMap<(String, String), CachedResolution>>,
}

impl GitResolver {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            http: reqwest::Client::new(),
            timeout,
            cache: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub async fn resolve(&self, req: &GitResolveRequest) -> Result<ResolvedRef, GitError> {
        // Pinned commit: no lookup, no cache, no network.
        if let Some(commit) = req.r#ref.commit.as_deref().filter(|c| !c.is_empty()) {
            return Ok(ResolvedRef {
                commit: commit.to_string(),
                ref_name: None,
            });
        }

        let cache_key = (req.url.clone(), ref_cache_key(&req.r#ref)?);
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&cache_key) {
                if entry.immutable || entry.expires_at > std::time::Instant::now() {
                    return Ok(entry.resolved.clone());
                }
            }
        }

        let bytes = self.fetch(req).await?;
        let advertisement = parse_advertisement(&bytes)?;
        let resolved = commit_for_ref(&advertisement, &req.r#ref)?;

        let immutable = req
            .r#ref
            .tag
            .as_deref()
            .is_some_and(super::registry::looks_immutable);
        self.cache.lock().await.insert(
            cache_key,
            CachedResolution {
                resolved: resolved.clone(),
                expires_at: std::time::Instant::now() + req.cache_ttl,
                immutable,
            },
        );
        Ok(resolved)
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
    use std::time::Duration;
    use wiremock::matchers::{method, path};
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
        let err = resolver.resolve(&req).await.unwrap_err();
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
        let err = resolver.resolve(&req).await.unwrap_err();
        assert!(matches!(err, GitError::Malformed(_)), "{err}");
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
