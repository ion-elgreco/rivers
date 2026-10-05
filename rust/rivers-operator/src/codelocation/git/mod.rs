//! Git ref → commit resolution.
//!
//! The operator resolves `spec.git.ref` to a pinned commit the way
//! `registry.rs` resolves `tag` → digest. Both transports yield the same
//! protocol-v0 ref advertisement, which [`RefMatcher`] reads as it arrives;
//! the single asymmetry is the smart-HTTP `# service=git-upload-pack`
//! preamble + flush, which the matcher skips when present:
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
use gix_packetline::decode::{Stream, streaming};
use rivers_k8s::crd::code_location::{GitRef, is_commit_sha};

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
    /// `spec.git.ref` breaks the CRD's rules, e.g. a pinned commit that is
    /// not a full lowercase SHA. The admission webhook and the CRD schema
    /// refuse such a ref, so only a CodeLocation they did not check gets
    /// here. Terminal until the CR changes.
    #[error("invalid git ref: {0}")]
    InvalidRef(String),
    /// `spec.git.url` breaks the operator's url rules ([`check_url`]). The
    /// admission webhook refuses such a url, so only a CodeLocation it did
    /// not check gets here. Terminal until the CR or the operator's
    /// settings change.
    #[error("invalid git url: {0}")]
    InvalidUrl(String),
    /// Credentials rejected (or unusable — e.g. an unreadable private key,
    /// or an ssh url without a user). Terminal until the Secret or the CR
    /// changes.
    #[error("git authentication failed: {0}")]
    AuthFailed(String),
    /// SSH host key not trusted: unknown host, or a changed key. Never
    /// auto-accepted. Terminal until the Secret's `known_hosts` changes.
    #[error("host key rejected: {0}")]
    HostKeyRejected(String),
    /// The `known_hosts` material itself is missing or unusable — distinct
    /// from "unknown host" so a broken Secret is diagnosable.
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
    /// The response was not a protocol-v0 ref advertisement, or was longer
    /// than the resolver reads.
    #[error("malformed advertisement: {0}")]
    Malformed(String),
}

impl GitError {
    /// A failure that passes on its own; every other one stands until the
    /// CR, the Secret, the remote or the operator's settings change.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            GitError::Unreachable(_) | GitError::RateLimited { .. }
        )
    }
}

/// A resolve that produced no commit.
#[derive(Debug, Clone)]
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

/// Transport/protocol failures out of russh land as transient, except no
/// host-key algorithm in common: our list is the key types `known_hosts`
/// records for the host (`known_hosts::HostKeys::algorithms`), so that is a
/// host-key rejection. Other host-key and auth outcomes are produced
/// explicitly before this conversion can occur.
impl From<russh::Error> for GitError {
    fn from(e: russh::Error) -> Self {
        match e {
            russh::Error::NoCommonAlgo {
                kind: russh::AlgorithmKind::Key,
                ours,
                theirs,
            } => GitError::HostKeyRejected(format!(
                "the host offers {} and the git Secret's `known_hosts` allows only {} for it — \
                 add one of the host's offered keys to `known_hosts`",
                theirs.join(", "),
                ours.join(", ")
            )),
            e => GitError::Unreachable(e.to_string()),
        }
    }
}

/// One advertised `<oid> <refname>` pair, peel entries (`name^{}`) included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedRef {
    pub oid: String,
    pub name: String,
}

/// The advertised refs that can answer one request: what a [`RefMatcher`]
/// keeps of an advertisement.
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
    /// When the resolver fetched this answer from the git host; a cached
    /// answer keeps the time of its fetch. `None` for a pinned commit, which
    /// is never fetched, and from [`commit_for_ref`] alone.
    pub fetched_at: Option<jiff::Timestamp>,
}

/// Reads a protocol-v0 ref advertisement as it arrives and keeps only the
/// lines that can answer one ref selector. The repository's other refs (on
/// a forge, often tens of thousands of `refs/pull/*`) are checked and
/// dropped, so between reads the matcher holds at most one pkt-line.
///
/// Accepts both transport shapes: the bare SSH form, and the smart-HTTP form
/// whose `# service=git-upload-pack` pkt + flush preamble is skipped here so
/// callers never see the difference. Requires the terminating flush-pkt —
/// a stream that just ends is reported as truncated, not treated as complete.
pub struct RefMatcher {
    /// The advertised names that answer the request.
    wanted: Vec<String>,
    /// The first line that advertised each of `wanted`.
    found: Vec<AdvertisedRef>,
    /// The start of a pkt-line whose end has not arrived.
    partial: Vec<u8>,
    first_line: bool,
    in_preamble: bool,
    /// The advertisement's terminating flush-pkt arrived.
    complete: bool,
}

impl RefMatcher {
    pub fn new(wanted: &GitRef) -> Self {
        Self {
            wanted: advertised_names(wanted).map_or_else(Vec::new, |(_, names)| names),
            found: Vec::new(),
            partial: Vec::new(),
            first_line: true,
            in_preamble: false,
            complete: false,
        }
    }

    /// Reads the next `bytes` of the advertisement. True once its
    /// terminating flush-pkt has arrived; later bytes are ignored.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<bool, GitError> {
        if self.complete {
            return Ok(true);
        }
        let mut pending = std::mem::take(&mut self.partial);
        pending.extend_from_slice(bytes);
        let mut read = 0;
        while !self.complete {
            match streaming(&pending[read..]).map_err(|e| GitError::Malformed(e.to_string()))? {
                Stream::Complete {
                    line,
                    bytes_consumed,
                } => {
                    read += bytes_consumed;
                    self.read_line(line)?;
                }
                Stream::Incomplete { .. } => break,
            }
        }
        if !self.complete {
            pending.drain(..read);
            self.partial = pending;
        }
        Ok(self.complete)
    }

    /// The refs that answer the request, once the whole advertisement has
    /// arrived.
    pub fn finish(self) -> Result<Advertisement, GitError> {
        if !self.complete {
            return Err(GitError::Malformed(
                "advertisement not terminated by a flush-pkt (truncated response?)".to_string(),
            ));
        }
        Ok(Advertisement { refs: self.found })
    }

    fn read_line(&mut self, line: PacketLineRef<'_>) -> Result<(), GitError> {
        let raw = match line {
            PacketLineRef::Data(raw) => raw,
            PacketLineRef::Flush if self.in_preamble => {
                self.in_preamble = false;
                return Ok(());
            }
            PacketLineRef::Flush => {
                self.complete = true;
                return Ok(());
            }
            other => {
                return Err(GitError::Malformed(format!(
                    "unexpected packet in advertisement: {other:?}"
                )));
            }
        };
        let line = raw.strip_suffix(b"\n").unwrap_or(raw);
        if self.first_line {
            self.first_line = false;
            if let Some(service) = line.strip_prefix(HTTP_SERVICE_PREAMBLE) {
                if service != UPLOAD_PACK_SERVICE {
                    return Err(GitError::Malformed(format!(
                        "unexpected service '{}' (wanted git-upload-pack)",
                        String::from_utf8_lossy(service)
                    )));
                }
                // Preamble consumed; its own flush follows, then the
                // advertisement restarts the first-line rules.
                self.in_preamble = true;
                self.first_line = true;
                return Ok(());
            }
            if line.starts_with(b"version ") {
                return Err(GitError::Malformed(format!(
                    "got '{}' where a v0 advertisement was expected",
                    String::from_utf8_lossy(line)
                )));
            }
        }
        if let Some((oid, name)) = parse_ref_line(line)?
            && self.wanted.iter().any(|wanted| wanted == name)
            && !self.found.iter().any(|found| found.name == name)
        {
            self.found.push(AdvertisedRef {
                oid: oid.to_string(),
                name: name.to_string(),
            });
        }
        Ok(())
    }
}

/// Parse one `<oid> <refname>[\0capabilities]` line. Returns `None` for the
/// empty-repo placeholder (`<zero-oid> capabilities^{}`).
fn parse_ref_line(line: &[u8]) -> Result<Option<(&str, &str)>, GitError> {
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
    if !is_commit_sha(oid) {
        return Err(GitError::Malformed(format!(
            "'{oid}' is not a lowercase 40-hex oid"
        )));
    }
    if name.is_empty() {
        return Err(GitError::Malformed("empty ref name".to_string()));
    }
    if oid == ZERO_OID && name == "capabilities^{}" {
        return Ok(None); // empty repository
    }
    Ok(Some((oid, name)))
}

fn non_empty(s: &Option<String>) -> Option<&str> {
    s.as_deref().filter(|v| !v.is_empty())
}

/// The ref a branch or tag selector names, and the advertised names that
/// answer it, best first (see [`commit_for_ref`]). `None` for a pinned
/// commit, which needs no lookup, and for a selector that names nothing.
fn advertised_names(wanted: &GitRef) -> Option<(String, Vec<String>)> {
    if non_empty(&wanted.commit).is_some() {
        return None;
    }
    if let Some(branch) = non_empty(&wanted.branch) {
        let name = format!("refs/heads/{branch}");
        return Some((name.clone(), vec![name]));
    }
    let name = format!("refs/tags/{}", non_empty(&wanted.tag)?);
    Some((name.clone(), vec![format!("{name}^{{}}"), name]))
}

/// Resolve the CR's ref selector against an advertisement.
///
/// * `commit` — passthrough, no lookup (the caller avoids fetching at all).
/// * `branch` — exact `refs/heads/<name>`.
/// * `tag` — prefers the peeled `refs/tags/<name>^{}` (annotated tags point
///   at a tag object; the peel is the commit), falls back to the tag oid
///   itself (lightweight tags).
pub fn commit_for_ref(adv: &Advertisement, wanted: &GitRef) -> Result<ResolvedRef, GitError> {
    if let Some(commit) = non_empty(&wanted.commit) {
        return Ok(ResolvedRef {
            commit: commit.to_string(),
            ref_name: None,
            fetched_at: None,
        });
    }
    let Some((name, answers)) = advertised_names(wanted) else {
        return Err(GitError::Malformed(
            "git ref selects nothing — admission validation should have rejected this".to_string(),
        ));
    };
    match answers.iter().find_map(|answer| adv.oid_for(answer)) {
        Some(commit) => Ok(ResolvedRef {
            commit,
            ref_name: Some(name),
            fetched_at: None,
        }),
        None => Err(GitError::RefNotFound(format!("{name} is not advertised"))),
    }
}

/// How a git url is fetched, by its scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// `https://` or `http://`.
    Http,
    /// `ssh://`.
    Ssh,
}

impl Transport {
    pub fn of(url: &str) -> Result<Self, GitError> {
        Self::of_url(&parse_url(url)?)
    }

    fn of_url(url: &url::Url) -> Result<Self, GitError> {
        match url.scheme() {
            "http" | "https" => Ok(Transport::Http),
            "ssh" => Ok(Transport::Ssh),
            other => Err(GitError::InvalidUrl(format!(
                "unsupported scheme '{other}' — use https:// or ssh://"
            ))),
        }
    }
}

/// The error leaves the url out: it may hold a password.
fn parse_url(raw: &str) -> Result<url::Url, GitError> {
    url::Url::parse(raw).map_err(|e| GitError::InvalidUrl(e.to_string()))
}

/// The rules for a git url; the admission webhook applies them too. The url
/// is copied into pod env, Runs and the tree's `.git/config`, so a password
/// belongs in the git Secret, not in it. `http://` sends the code and the
/// Secret's credentials unencrypted, so it needs
/// `operator.git.allowInsecure`. And git must connect to the host that the
/// operator and the webhook read: curl reads the host after a `\`, where
/// `url` ends it, and an ssh url must pass [`ssh::parse_ssh_url`].
pub fn check_url(raw: &str, allow_insecure: bool) -> Result<Transport, GitError> {
    let invalid = |problem: &str| -> Result<Transport, GitError> {
        Err(GitError::InvalidUrl(problem.to_string()))
    };
    let url = parse_url(raw)?;
    let transport = Transport::of_url(&url)?;
    if url.password().is_some() {
        return invalid(
            "the url has a password — put the credentials in the git Secret (spec.git.secretRef)",
        );
    }
    match transport {
        Transport::Http if raw.contains('\\') => {
            invalid("the url has a '\\', which git and the operator read differently — use '/'")
        }
        Transport::Http if url.scheme() == "http" && !allow_insecure => invalid(
            "http:// sends the code and the git Secret's credentials unencrypted — use \
             https://, or set operator.git.allowInsecure to true for a git host on a trusted \
             network",
        ),
        Transport::Http => Ok(transport),
        Transport::Ssh => ssh::parse_ssh_url(raw).map(|_| transport),
    }
}

const ALLOW_INSECURE_ENV: &str = "RIVERS_GIT_ALLOW_INSECURE";
const ALLOWED_HOSTS_ENV: &str = "RIVERS_GIT_ALLOWED_HOSTS";
const TIMEOUT_SECONDS_ENV: &str = "RIVERS_GIT_TIMEOUT_SECONDS";

/// The chart's `operator.git` settings, via operator env.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitConfig {
    /// `operator.git.allowInsecure`: `http://` urls are admitted and fetched.
    pub allow_insecure: bool,
    /// `operator.git.allowedHosts`: the hosts the admission webhook admits;
    /// empty admits any host.
    pub allowed_hosts: Vec<String>,
    /// `operator.git.timeoutSeconds`: how long one ref resolution may take.
    pub timeout: std::time::Duration,
}

impl Default for GitConfig {
    fn default() -> Self {
        Self {
            allow_insecure: false,
            allowed_hosts: Vec::new(),
            timeout: std::time::Duration::from_secs(30),
        }
    }
}

impl GitConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let allow_insecure = match get(ALLOW_INSECURE_ENV).as_deref().map(str::trim) {
            None | Some("" | "false" | "0") => false,
            Some("true" | "1") => true,
            Some(other) => anyhow::bail!(
                "{ALLOW_INSECURE_ENV} (operator.git.allowInsecure): expected true or false, \
                 got {other:?}"
            ),
        };
        let allowed_hosts = get(ALLOWED_HOSTS_ENV)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(String::from)
            .collect();
        let timeout = match get(TIMEOUT_SECONDS_ENV).as_deref().map(str::trim) {
            None | Some("") => Self::default().timeout,
            Some(seconds) => match seconds.parse::<u64>() {
                Ok(seconds) if seconds > 0 => std::time::Duration::from_secs(seconds),
                _ => anyhow::bail!(
                    "{TIMEOUT_SECONDS_ENV} (operator.git.timeoutSeconds): expected a whole \
                     number of seconds more than zero, got {seconds:?}"
                ),
            },
        };
        Ok(Self {
            allow_insecure,
            allowed_hosts,
            timeout,
        })
    }
}

/// Credential material for a resolve, straight from the CR's Secret. The
/// SSH transport uses it as contents; nothing is written to disk.
#[derive(Clone, Default, Hash, PartialEq, Eq)]
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

/// The kind only: the Secret's contents stay out of logs and panics.
impl std::fmt::Debug for GitCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitCredentials::Anonymous => f.write_str("Anonymous"),
            GitCredentials::Basic { .. } => f.debug_struct("Basic").finish_non_exhaustive(),
            GitCredentials::Ssh { .. } => f.debug_struct("Ssh").finish_non_exhaustive(),
        }
    }
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

/// The longest backoff between fetches of a failing ref: the default poll
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
    /// The refs being fetched: a resolve of one of them waits for that
    /// fetch's outcome instead of fetching the ref again.
    fetching: std::collections::HashMap<RefKey, Fetching>,
}

/// The outcome of a fetch under way, once it ends.
type Fetching = tokio::sync::watch::Receiver<Option<Result<ResolvedRef, GitFailure>>>;

impl Cache {
    /// The answer to a request for `key` that needs no fetch: a commit no
    /// older than `ttl` (any age for an immutable tag), a failure whose
    /// backoff has not ended, or the pause of `host`.
    fn answer(
        &mut self,
        key: &RefKey,
        host: Option<&str>,
        ttl: std::time::Duration,
        now: std::time::Instant,
    ) -> Option<Result<ResolvedRef, GitFailure>> {
        if let Some(cached) = self.refs.get_mut(key) {
            match &cached.entry {
                CacheEntry::Resolved {
                    resolved,
                    resolved_at,
                    immutable,
                } if *immutable || now.saturating_duration_since(*resolved_at) < ttl => {
                    cached.last_used = now;
                    return Some(Ok(resolved.clone()));
                }
                CacheEntry::Failing {
                    error,
                    next_attempt_after,
                    ..
                } if now < *next_attempt_after => {
                    cached.last_used = now;
                    return Some(Err(GitFailure {
                        error: error.clone(),
                        retry_after: Some(*next_attempt_after - now),
                    }));
                }
                _ => {}
            }
        }
        let pause = self.paused_hosts.get(host?)?;
        (now < pause.until).then(|| {
            Err(GitFailure {
                error: pause.error.clone(),
                retry_after: Some(pause.until - now),
            })
        })
    }

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
/// cached commit or failure that its own credentials produced (a host pause
/// holds for all).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RefKey {
    url: String,
    r#ref: String,
    credentials: CredentialFingerprint,
}

/// A hash of the credentials' kind and contents, so the cache never holds
/// the secret.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
/// day are dropped), one fetch at a time per key (resolves that ask while it
/// runs get its outcome), semver-like tags resolved once, exponential
/// backoff after transient errors, and a host that asks the operator to wait
/// gets no fetch, for any ref, until its `Retry-After` (at most an hour)
/// ends. Leader gating is left to the reconciler. Unlike
/// [`super::registry`]'s digest cache, a ref that backs off does not serve
/// its last result: it fails until a fetch succeeds.
pub struct GitResolver {
    http: reqwest::Client,
    timeout: std::time::Duration,
    /// `operator.git.allowInsecure`: `http://` urls may be fetched.
    allow_insecure: bool,
    fingerprints: std::hash::RandomState,
    cache: tokio::sync::Mutex<Cache>,
}

impl GitResolver {
    pub fn new(timeout: std::time::Duration, allow_insecure: bool) -> Self {
        let http = reqwest::Client::builder()
            .user_agent(super::USER_AGENT)
            .build()
            .expect("reqwest client builds with default TLS");
        Self {
            http,
            timeout,
            allow_insecure,
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
        // Also for a pinned commit: the pods fetch it with this url.
        check_url(&req.url, self.allow_insecure)?;
        // The pods take a pinned commit as written, and the webhook may not
        // have checked this CR.
        req.r#ref.validate().map_err(GitError::InvalidRef)?;
        // Pinned commit: no lookup, no cache, no network.
        if let Some(commit) = req.r#ref.commit.as_deref().filter(|c| !c.is_empty()) {
            return Ok(ResolvedRef {
                commit: commit.to_string(),
                ref_name: None,
                fetched_at: None,
            });
        }

        let cache_key = self.ref_key(req)?;
        let host = host_of(&req.url);
        let fetched = loop {
            let mut cache = self.cache.lock().await;
            if let Some(answer) = cache.answer(&cache_key, host.as_deref(), req.cache_ttl, now) {
                return answer;
            }
            match cache.fetching.get(&cache_key) {
                // A fetch dropped before it ended leaves its channel closed.
                Some(fetching) if fetching.has_changed().is_ok() => {
                    let mut fetching = fetching.clone();
                    drop(cache);
                    if let Ok(outcome) = fetching.wait_for(Option::is_some).await
                        && let Some(outcome) = outcome.as_ref()
                    {
                        return outcome.clone();
                    }
                }
                _ => {
                    let (fetched, fetching) = tokio::sync::watch::channel(None);
                    cache.fetching.insert(cache_key.clone(), fetching);
                    break fetched;
                }
            }
        };

        let fetch = self.fetch_commit(req).await;
        let mut cache = self.cache.lock().await;
        cache.fetching.remove(&cache_key);
        let outcome = match fetch {
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
        };
        drop(cache);
        fetched.send_replace(Some(outcome.clone()));
        outcome
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
        let resolved = commit_for_ref(&self.fetch(req).await?, &req.r#ref)?;
        Ok(ResolvedRef {
            fetched_at: Some(jiff::Timestamp::now()),
            ..resolved
        })
    }

    /// The refs of `req`'s repository that answer its ref.
    async fn fetch(&self, req: &GitResolveRequest) -> Result<Advertisement, GitError> {
        match Transport::of(&req.url)? {
            Transport::Http => {
                let auth = match &req.credentials {
                    GitCredentials::Anonymous => http::GitAuth::Anonymous,
                    GitCredentials::Basic { username, password } => http::GitAuth::Basic {
                        username: username.clone(),
                        password: password.clone(),
                    },
                    GitCredentials::Ssh { .. } => {
                        return Err(GitError::AuthFailed(
                            "SSH credentials cannot fetch an http(s) url".to_string(),
                        ));
                    }
                };
                http::fetch_advertisement(&self.http, &req.url, &auth, &req.r#ref, self.timeout)
                    .await
            }
            Transport::Ssh => {
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
                let auth = ssh::SshAuth {
                    private_key: private_key_openssh,
                    known_hosts,
                };
                ssh::fetch_advertisement(&target, &auth, &req.r#ref, self.timeout).await
            }
        }
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
    use super::ssh::test_server::{self, AfterAdvertisement, ServerBehaviour, StalledHost};
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

    /// A resolver that may fetch the `http://` urls of the test git servers
    /// (`operator.git.allowInsecure`).
    fn resolver() -> GitResolver {
        GitResolver::new(TIMEOUT, true)
    }

    #[tokio::test]
    async fn caches_branch_resolution_within_ttl() {
        let server = mock_git_server(1).await;
        let resolver = resolver();
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
    async fn an_answer_says_when_the_git_host_gave_it() {
        let server = mock_git_server(2).await;
        let resolver = resolver();
        let main = main_of(&server);
        let now = Instant::now();

        let before = jiff::Timestamp::now();
        let fetched = resolver.resolve_at(&main, now).await.unwrap();
        let fetched_at = fetched.fetched_at.expect("the time of the fetch");
        assert!(before <= fetched_at && fetched_at <= jiff::Timestamp::now());

        // The cache answers with the time of the fetch it holds.
        let cached = resolver
            .resolve_at(&main, now + TTL - Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(cached, fetched);

        let refetched = resolver.resolve_at(&main, now + TTL).await.unwrap();
        assert!(
            refetched.fetched_at.is_some_and(|at| at > fetched_at),
            "{refetched:?}"
        );
        assert_eq!(polls(&server).await, 2);

        let pinned = request(
            main.url.clone(),
            GitRef {
                commit: Some(fixtures::oid('9')),
                ..Default::default()
            },
        );
        assert_eq!(resolver.resolve(&pinned).await.unwrap().fetched_at, None);
    }

    #[tokio::test]
    async fn expired_ttl_refetches() {
        let server = mock_git_server(2).await;
        let resolver = resolver();
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
        let resolver = resolver();
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
        let resolver = resolver();
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
    async fn a_pinned_uppercase_commit_fails_for_good_without_a_fetch() {
        let server = mock_git_server(0).await;
        let commit = fixtures::oid('a').to_ascii_uppercase();
        let pinned = GitRef {
            commit: Some(commit.clone()),
            ..Default::default()
        };
        let req = request(format!("{}/acme/pipelines.git", server.uri()), pinned);

        let failure = resolver().resolve(&req).await.unwrap_err();
        assert_eq!(
            failure.error.to_string(),
            format!(
                "invalid git ref: git.ref.commit '{commit}' has uppercase letters — use the \
                 lowercase SHA '{}'",
                fixtures::oid('a')
            )
        );
        assert!(!failure.error.is_transient());
        assert_eq!(failure.retry_after, None);
    }

    #[tokio::test]
    async fn ssh_scheme_dispatches_through_russh() {
        let port = test_server::spawn_server(ServerBehaviour::default()).await;
        let resolver = resolver();
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
    async fn an_ssh_host_that_never_answers_fails_the_resolve_and_backs_off() {
        let (port, _) = test_server::spawn_stalled_host(StalledHost::Silent).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);
        let resolver = GitResolver::new(Duration::from_secs(1), false);

        let failure = tokio::time::timeout(Duration::from_secs(5), resolver.resolve(&req))
            .await
            .expect("the resolve did not end within 5s")
            .unwrap_err();

        assert_eq!(
            failure.error.to_string(),
            format!(
                "git host unreachable: the SSH handshake with '127.0.0.1:{port}' did not finish \
                 within 1s"
            )
        );
        assert_eq!(failure.retry_after, Some(Duration::from_secs(60)));
    }

    /// Resolve `main` on a test git server that behaves as `server`.
    async fn resolve_over_ssh(server: ServerBehaviour) -> (u16, GitFailure) {
        let port = test_server::spawn_server(server).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);
        let failure = resolver().resolve(&req).await.unwrap_err();
        (port, failure)
    }

    #[tokio::test]
    async fn an_ssh_connection_that_ends_before_the_refs_do_backs_off() {
        let mut half = fixtures::ssh_adv();
        half.truncate(half.len() / 2);
        for advertisement in [half, Vec::new()] {
            // No exit status, no flush-pkt: the host drops the connection.
            let server = ServerBehaviour {
                advertisement,
                drop_when_idle: Some(Duration::from_secs(1)),
                ..Default::default()
            };

            let (port, failure) = resolve_over_ssh(server).await;

            assert_eq!(
                failure.error.to_string(),
                format!(
                    "git host unreachable: the connection to '127.0.0.1:{port}' closed before \
                     all refs arrived"
                )
            );
            assert_eq!(failure.retry_after, Some(Duration::from_secs(60)));
        }
    }

    #[tokio::test]
    async fn an_upload_pack_that_fails_over_ssh_fails_for_good() {
        let server = ServerBehaviour {
            advertisement: Vec::new(),
            after_advertisement: AfterAdvertisement::Exit(128),
            stderr: Some(
                "fatal: '/acme/pipelines.git' does not appear to be a git repository\n".to_string(),
            ),
            ..Default::default()
        };

        let (_, failure) = resolve_over_ssh(server).await;

        assert_eq!(
            failure.error.to_string(),
            "ref not found: git-upload-pack failed (exit 128) for '/acme/pipelines.git': fatal: \
             '/acme/pipelines.git' does not appear to be a git repository"
        );
        assert_eq!(failure.retry_after, None);
    }

    #[tokio::test]
    async fn ssh_url_with_non_ssh_credentials_is_auth_failed() {
        let resolver = resolver();
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

    /// A request to the test git server with the Secret's `identity` and
    /// `known_hosts` contents.
    fn ssh_request(port: u16, identity: &str, known_hosts: &str) -> GitResolveRequest {
        GitResolveRequest {
            url: format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git"),
            r#ref: fixtures::branch("main"),
            credentials: GitCredentials::Ssh {
                private_key_openssh: identity.to_string(),
                known_hosts: known_hosts.to_string(),
            },
            cache_ttl: TTL,
        }
    }

    /// Resolves `req` twice; the status message of a failure must not change
    /// between attempts.
    async fn fails_the_same_way_twice(
        resolver: &GitResolver,
        req: &GitResolveRequest,
        case: &str,
    ) -> GitFailure {
        let first = resolver.resolve(req).await.unwrap_err();
        let second = resolver.resolve(req).await.unwrap_err();
        assert_eq!(first.error.to_string(), second.error.to_string(), "{case}");
        first
    }

    #[tokio::test]
    async fn an_unusable_ssh_key_fails_the_same_way_every_time() {
        let server = ServerBehaviour::default();
        let port = test_server::spawn_server(server.clone()).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let resolver = resolver();
        let cases = [
            ("passphrase-protected", test_server::PASSPHRASE_KEY),
            ("not a key", "not a private key\n"),
            ("truncated", &test_server::CLIENT_KEY[..120]),
        ];

        let mut messages = Vec::new();
        for (case, identity) in cases {
            let req = ssh_request(port, identity, &known_hosts);
            let failure = fails_the_same_way_twice(&resolver, &req, case).await;
            assert!(
                matches!(failure.error, GitError::AuthFailed(_)),
                "{case}: {}",
                failure.error
            );
            assert_eq!(failure.retry_after, None, "{case}");
            messages.push(failure.error.to_string());
        }
        assert_eq!(
            messages[0],
            "git authentication failed: the SSH private key is passphrase-protected — the \
             git Secret's `identity` must be a key without a passphrase"
        );
        assert_eq!(server.connections(), 0);
    }

    #[tokio::test]
    async fn an_unusable_known_hosts_fails_the_same_way_before_connecting() {
        let server = ServerBehaviour::default();
        let port = test_server::spawn_server(server.clone()).await;
        let resolver = resolver();
        let cases = [
            ("empty", String::new()),
            (
                "comments only",
                "# 127.0.0.1 SSH-2.0-OpenSSH_9.6\n\n".to_string(),
            ),
            (
                "not known_hosts",
                "this is not a known_hosts line\n".to_string(),
            ),
            (
                "a broken key",
                format!("[127.0.0.1]:{port} ssh-ed25519 not-base64\n"),
            ),
        ];

        for (case, known_hosts) in cases {
            let req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);
            let failure = fails_the_same_way_twice(&resolver, &req, case).await;
            assert!(
                matches!(failure.error, GitError::KnownHostsUnavailable(_)),
                "{case}: {}",
                failure.error
            );
        }
        assert_eq!(server.connections(), 0);
    }

    #[tokio::test]
    async fn a_host_missing_from_known_hosts_is_rejected_before_connecting() {
        let server = ServerBehaviour::default();
        let port = test_server::spawn_server(server.clone()).await;
        let known_hosts = format!("forge.example {}", test_server::HOST_PUB);
        let req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);

        let failure = resolver().resolve(&req).await.unwrap_err();
        assert_eq!(
            failure.error.to_string(),
            format!(
                "host key rejected: host '127.0.0.1:{port}' is not in known_hosts (exact and \
                 hashed |1| entries only; wildcards and @cert-authority are unsupported) — add \
                 this host's key to the git Secret"
            )
        );
        assert_eq!(server.connections(), 0);
    }

    /// A request for `r#ref` on the test git server through an ssh url
    /// without a user, with credentials the server accepts for user `git`.
    async fn userless_ssh_request(r#ref: GitRef) -> (ServerBehaviour, GitResolveRequest) {
        let server = ServerBehaviour::default();
        let port = test_server::spawn_server(server.clone()).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let mut req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);
        req.url = format!("ssh://127.0.0.1:{port}/acme/pipelines.git");
        req.r#ref = r#ref;
        (server, req)
    }

    /// `failure` is the terminal one for `req`'s ssh url without a user.
    fn assert_userless_failure(failure: &GitFailure, req: &GitResolveRequest) {
        assert!(
            matches!(failure.error, GitError::AuthFailed(_)),
            "{:?}",
            failure.error
        );
        let fixed = req.url.replace("ssh://", "ssh://git@");
        assert_eq!(
            failure.error.to_string(),
            format!(
                "git authentication failed: the ssh url must name the user — use {fixed}, or \
                 the user your git host expects"
            )
        );
        assert_eq!(failure.retry_after, None);
    }

    #[tokio::test]
    async fn an_ssh_url_without_a_user_fails_before_connecting() {
        let (server, req) = userless_ssh_request(fixtures::branch("main")).await;

        let failure = resolver().resolve(&req).await.unwrap_err();
        assert_userless_failure(&failure, &req);
        assert_eq!(server.connections(), 0);
    }

    #[tokio::test]
    async fn a_pinned_commit_on_an_ssh_url_without_a_user_fails() {
        // Never fetched here, but the pods fetch the commit with this url.
        let pinned = GitRef {
            commit: Some(fixtures::oid('9')),
            ..Default::default()
        };
        let (server, req) = userless_ssh_request(pinned).await;

        let failure = resolver().resolve(&req).await.unwrap_err();
        assert_userless_failure(&failure, &req);
        assert_eq!(server.connections(), 0);
    }

    #[tokio::test]
    async fn unsupported_scheme_is_an_invalid_url() {
        let resolver = resolver();
        let req = request(
            "ftp://forge.example/r.git".to_string(),
            fixtures::branch("m"),
        );
        let err = resolver.resolve(&req).await.unwrap_err().error;
        assert!(matches!(err, GitError::InvalidUrl(_)), "{err}");
        assert_eq!(
            err.to_string(),
            "invalid git url: unsupported scheme 'ftp' — use https:// or ssh://"
        );
    }

    /// `failure` is terminal, with `message`.
    #[track_caller]
    fn assert_invalid_url(failure: &GitFailure, message: &str) {
        assert!(
            matches!(failure.error, GitError::InvalidUrl(_)),
            "{:?}",
            failure.error
        );
        assert_eq!(failure.error.to_string(), message);
        assert_eq!(failure.retry_after, None);
    }

    const HTTP_REFUSED: &str = "invalid git url: http:// sends the code and the git Secret's \
        credentials unencrypted — use https://, or set operator.git.allowInsecure to true for a \
        git host on a trusted network";

    #[tokio::test]
    async fn an_http_url_fails_for_good_without_a_fetch_unless_the_operator_allows_it() {
        let server = mock_git_server(1).await;
        let main = main_of(&server);
        let pinned = request(
            main.url.clone(),
            GitRef {
                commit: Some(fixtures::oid('9')),
                ..Default::default()
            },
        );
        let refusing = GitResolver::new(TIMEOUT, false);

        // Also a pinned commit: the pods would fetch it over http.
        for req in [&main, &pinned] {
            let failure = refusing.resolve(req).await.unwrap_err();
            assert_invalid_url(&failure, HTTP_REFUSED);
        }
        assert_eq!(polls(&server).await, 0);

        // With operator.git.allowInsecure, the same url resolves.
        let resolved = resolver().resolve(&main).await.unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
    }

    #[tokio::test]
    async fn a_password_in_the_url_fails_for_good_without_a_fetch() {
        let http = mock_git_server(0).await;
        let ssh = ServerBehaviour::default();
        let port = test_server::spawn_server(ssh.clone()).await;
        let refused = "invalid git url: the url has a password — put the credentials in the git \
                       Secret (spec.git.secretRef)";

        let over_http = request(
            format!(
                "http://{USER}:{TOKEN}@{}/acme/pipelines.git",
                http.address()
            ),
            fixtures::branch("main"),
        );
        let failure = resolver().resolve(&over_http).await.unwrap_err();
        assert_invalid_url(&failure, refused);
        assert_eq!(polls(&http).await, 0);

        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let mut over_ssh = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);
        over_ssh.url = format!("ssh://git:{TOKEN}@127.0.0.1:{port}/acme/pipelines.git");
        let failure = resolver().resolve(&over_ssh).await.unwrap_err();
        assert_invalid_url(&failure, refused);
        assert_eq!(ssh.connections(), 0);
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
        let resolver = resolver();
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
        let resolver = resolver();
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

    /// Long enough for the other resolves of a test to start while the
    /// host answers the first.
    const SLOW: Duration = Duration::from_millis(200);

    /// Resolves each of `reqs` at the same time, at `now`.
    async fn resolve_together(
        resolver: &GitResolver,
        reqs: &[&GitResolveRequest],
        now: Instant,
    ) -> Vec<Result<ResolvedRef, GitFailure>> {
        futures_util::future::join_all(reqs.iter().map(|req| resolver.resolve_at(req, now))).await
    }

    /// `outcome` as data: the commit, or the error and the wait.
    fn summary(
        outcome: Result<ResolvedRef, GitFailure>,
    ) -> Result<String, (String, Option<Duration>)> {
        outcome
            .map(|resolved| resolved.commit)
            .map_err(|failure| (failure.error.to_string(), failure.retry_after))
    }

    /// The url the resolver asks `server` for the refs of `acme/pipelines`.
    fn refs_url(server: &MockServer) -> String {
        format!(
            "{}/acme/pipelines.git/info/refs?service=git-upload-pack",
            server.uri()
        )
    }

    #[tokio::test]
    async fn concurrent_resolves_of_a_ref_share_one_fetch() {
        let server = git_server(vec![(adv_response().set_delay(SLOW), None)]).await;
        let req = main_of(&server);

        let outcomes = resolve_together(&resolver(), &[&req; 5], Instant::now()).await;

        assert_eq!(polls(&server).await, 1);
        let resolved: Vec<ResolvedRef> = outcomes.into_iter().map(Result::unwrap).collect();
        let main = ResolvedRef {
            commit: fixtures::oid('a'),
            ref_name: Some("refs/heads/main".to_string()),
            fetched_at: resolved[0].fetched_at,
        };
        assert!(main.fetched_at.is_some());
        assert_eq!(resolved, vec![main; 5]);
    }

    #[tokio::test]
    async fn concurrent_resolves_of_a_ref_share_a_refusal() {
        let refused = ResponseTemplate::new(401).set_delay(SLOW);
        let server = git_server(vec![(refused, None)]).await;
        let req = main_of(&server);

        let outcomes = resolve_together(&resolver(), &[&req; 5], Instant::now()).await;

        assert_eq!(polls(&server).await, 1);
        let error = format!(
            "git authentication failed: HTTP 401 Unauthorized from {}",
            refs_url(&server)
        );
        for outcome in outcomes {
            assert_eq!(summary(outcome), Err((error.clone(), None)));
        }
    }

    #[tokio::test]
    async fn concurrent_failures_of_a_ref_count_once() {
        let slow_503 = ResponseTemplate::new(503).set_delay(SLOW);
        let server = git_server(vec![(slow_503, None)]).await;
        let resolver = resolver();
        let req = main_of(&server);
        let now = Instant::now();

        let outcomes = resolve_together(&resolver, &[&req; 5], now).await;

        assert_eq!(polls(&server).await, 1);
        let error = format!(
            "git host unreachable: HTTP 503 Service Unavailable from {}",
            refs_url(&server)
        );
        for outcome in outcomes {
            assert_eq!(
                summary(outcome),
                Err((error.clone(), Some(Duration::from_secs(60))))
            );
        }
        let next = resolver
            .resolve_at(&req, now + Duration::from_secs(60))
            .await
            .unwrap_err();
        assert_eq!(next.retry_after, Some(Duration::from_secs(120)));
    }

    #[tokio::test]
    async fn concurrent_resolves_with_other_credentials_fetch_for_themselves() {
        let server = git_server(vec![(adv_response().set_delay(SLOW), None)]).await;
        let anonymous = main_of(&server);
        let with_token = GitResolveRequest {
            credentials: basic(USER, TOKEN),
            ..main_of(&server)
        };

        let outcomes = resolve_together(
            &resolver(),
            &[&anonymous, &with_token, &anonymous, &with_token],
            Instant::now(),
        )
        .await;

        for outcome in outcomes {
            assert_eq!(summary(outcome), Ok(fixtures::oid('a')));
        }
        let mut sent: Vec<Option<String>> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| {
                let authorization = request.headers.get("authorization");
                authorization.map(|v| v.to_str().unwrap().to_string())
            })
            .collect();
        sent.sort();
        assert_eq!(sent, [None, Some(basic_authorization(USER, TOKEN))]);
    }

    #[tokio::test]
    async fn a_fetch_dropped_before_it_ends_holds_up_no_resolve() {
        use futures_util::FutureExt as _;
        let server = git_server(vec![(adv_response().set_delay(SLOW), None)]).await;
        let resolver = std::sync::Arc::new(resolver());
        let req = main_of(&server);
        let now = Instant::now();
        let resolve = || {
            let (resolver, req) = (resolver.clone(), req.clone());
            Box::pin(async move { resolver.resolve_at(&req, now).await })
        };
        let mut fetching = resolve();
        let mut waiting = resolve();
        assert!(fetching.as_mut().now_or_never().is_none());
        assert!(waiting.as_mut().now_or_never().is_none());

        drop(fetching);

        // Spawned: a resolve that spins must fail this test, not hang it.
        let resolved = tokio::time::timeout(Duration::from_secs(5), tokio::spawn(waiting))
            .await
            .expect("the resolve did not end within 5s")
            .unwrap()
            .unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
        let cached = resolver.resolve_at(&req, now).await.unwrap();
        assert_eq!(cached, resolved);
    }

    #[tokio::test]
    async fn a_ref_among_many_pull_requests_resolves() {
        let lines = fixtures::lines_with_pulls(50_000);
        let refs = adv_response().set_body_bytes(fixtures::over_http(fixtures::body(&lines)));
        let server = git_server(vec![(refs, None)]).await;
        let resolver = resolver();
        let main = main_of(&server);
        let release = request(main.url.clone(), fixtures::tag("v1.0.0"));
        let now = Instant::now();

        let resolved = resolver.resolve_at(&main, now).await.unwrap();
        assert_eq!(
            resolved,
            ResolvedRef {
                commit: fixtures::oid('a'),
                ref_name: Some("refs/heads/main".to_string()),
                fetched_at: resolved.fetched_at,
            }
        );
        // The tag and its peel come after the pull requests.
        let resolved = resolver.resolve_at(&release, now).await.unwrap();
        assert_eq!(
            resolved,
            ResolvedRef {
                commit: fixtures::oid('d'),
                ref_name: Some("refs/tags/v1.0.0".to_string()),
                fetched_at: resolved.fetched_at,
            }
        );
    }

    #[tokio::test]
    async fn a_ref_among_many_pull_requests_resolves_over_ssh() {
        let server = ServerBehaviour {
            advertisement: fixtures::body(&fixtures::lines_with_pulls(50_000)),
            ..Default::default()
        };
        let port = test_server::spawn_server(server).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let req = GitResolveRequest {
            r#ref: fixtures::tag("v1.0.0"),
            ..ssh_request(port, test_server::CLIENT_KEY, &known_hosts)
        };

        let resolved = resolver().resolve_at(&req, Instant::now()).await.unwrap();

        assert_eq!(
            resolved,
            ResolvedRef {
                commit: fixtures::oid('d'),
                ref_name: Some("refs/tags/v1.0.0".to_string()),
                fetched_at: resolved.fetched_at,
            }
        );
    }

    #[tokio::test]
    async fn concurrent_ssh_resolves_of_a_ref_share_one_connection() {
        let server = ServerBehaviour::default();
        let port = test_server::spawn_server(server.clone()).await;
        let known_hosts = format!("[127.0.0.1]:{port} {}", test_server::HOST_PUB);
        let req = ssh_request(port, test_server::CLIENT_KEY, &known_hosts);

        let outcomes = resolve_together(&resolver(), &[&req; 3], Instant::now()).await;

        assert_eq!(server.connections(), 1);
        for outcome in outcomes {
            assert_eq!(summary(outcome), Ok(fixtures::oid('a')));
        }
    }

    #[tokio::test]
    async fn an_answer_from_the_host_ends_the_backoff() {
        let server = git_server(vec![
            (ResponseTemplate::new(503), Some(2)),
            (ResponseTemplate::new(401), Some(1)),
            (ResponseTemplate::new(503), None),
        ])
        .await;
        let resolver = resolver();
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
        let resolver = resolver();

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
        let resolver = resolver();
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
        let resolver = resolver();
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
            let failure = resolver()
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
        let resolver = resolver();
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

    /// The `Authorization` header of a request with `username`/`password`.
    fn basic_authorization(username: &str, password: &str) -> String {
        use base64::Engine as _;
        let token =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        format!("Basic {token}")
    }

    /// A private repository: it advertises its refs to `USER`/`TOKEN` and
    /// answers everyone else with 401.
    async fn private_git_server() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/acme/pipelines.git/info/refs"))
            .and(header(
                "authorization",
                basic_authorization(USER, TOKEN).as_str(),
            ))
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
        let resolver = resolver();
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
        let resolver = resolver();
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
        let resolver = resolver();
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

    #[test]
    fn credentials_and_requests_print_no_secret() {
        let secrets: Vec<&str> = [USER, TOKEN, test_server::HOST_PUB]
            .into_iter()
            .chain(test_server::CLIENT_KEY.lines())
            .collect();
        for (credentials, shown_as) in [
            (basic(USER, TOKEN), "Basic { .. }"),
            (ssh_credentials(), "Ssh { .. }"),
        ] {
            assert_eq!(format!("{credentials:?}"), shown_as);
            let req = GitResolveRequest {
                credentials,
                ..request(
                    "https://forge.example/acme/pipelines.git".to_string(),
                    fixtures::branch("main"),
                )
            };
            let shown = format!("{req:?}");
            assert!(
                shown.contains(&format!("credentials: {shown_as}")),
                "{shown}"
            );
            for secret in &secrets {
                assert!(!shown.contains(secret), "{shown}");
            }
        }
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
        let resolver = resolver();
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
        let resolver = resolver();
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

    /// The lines of the advertisement `git-upload-pack` emits over SSH.
    pub fn ssh_lines() -> Vec<String> {
        vec![
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
        ]
    }

    /// The advertisement exactly as `git-upload-pack` emits it over SSH.
    pub fn ssh_adv() -> Vec<u8> {
        body(&ssh_lines())
    }

    /// [`ssh_lines`] and a `refs/pull/<n>/head` for each of `pulls` pull
    /// requests, where a forge advertises them: after the branches, before
    /// the tags.
    pub fn lines_with_pulls(pulls: usize) -> Vec<String> {
        let mut lines = ssh_lines();
        let tags = lines
            .iter()
            .position(|l| l.contains(" refs/tags/"))
            .unwrap();
        let heads = (0..pulls).map(|n| format!("{} refs/pull/{n}/head\n", oid('7')));
        lines.splice(tags..tags, heads);
        lines
    }

    /// `adv` behind the smart-HTTP service preamble.
    pub fn over_http(adv: Vec<u8>) -> Vec<u8> {
        let mut v = pkt("# service=git-upload-pack\n");
        v.extend(flush());
        v.extend(adv);
        v
    }

    /// The same advertisement behind the smart-HTTP service preamble.
    pub fn http_adv() -> Vec<u8> {
        over_http(ssh_adv())
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

    /// `bytes` read in one piece, keeping the refs that answer `wanted`.
    fn parse(bytes: &[u8], wanted: &GitRef) -> Result<Advertisement, GitError> {
        let mut matcher = RefMatcher::new(wanted);
        matcher.feed(bytes)?;
        matcher.finish()
    }

    /// `wanted` resolved against `bytes` read in one piece.
    fn resolve(bytes: &[u8], wanted: &GitRef) -> Result<ResolvedRef, GitError> {
        commit_for_ref(&parse(bytes, wanted)?, wanted)
    }

    fn advertised(name: &str, oid_of: char) -> AdvertisedRef {
        AdvertisedRef {
            oid: oid(oid_of),
            name: name.to_string(),
        }
    }

    const TRUNCATED: &str = "malformed advertisement: advertisement not terminated by a flush-pkt (truncated response?)";

    #[test]
    fn parses_ssh_form_and_matches_branch() {
        let resolved = resolve(&ssh_adv(), &branch("main")).unwrap();
        assert_eq!(resolved.commit, oid('a'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/heads/main"));
    }

    #[test]
    fn http_preamble_is_stripped_and_yields_identical_refs() {
        let wanted = tag("v1.0.0");
        let ssh = parse(&ssh_adv(), &wanted).unwrap();
        let http = parse(&http_adv(), &wanted).unwrap();
        assert_eq!(ssh.refs, http.refs, "one matcher, two transports");
        assert!(!ssh.refs.is_empty());
    }

    #[test]
    fn annotated_tag_prefers_peeled_commit() {
        let resolved = resolve(&ssh_adv(), &tag("v1.0.0")).unwrap();
        // The tag *object* is oid('c'); the commit it points at is oid('d').
        assert_eq!(resolved.commit, oid('d'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/tags/v1.0.0"));
    }

    #[test]
    fn lightweight_tag_resolves_to_its_own_oid() {
        let resolved = resolve(&ssh_adv(), &tag("light")).unwrap();
        assert_eq!(resolved.commit, oid('e'));
    }

    #[test]
    fn branch_and_tag_sharing_a_name_do_not_collide() {
        assert_eq!(
            resolve(&ssh_adv(), &branch("main")).unwrap().commit,
            oid('a')
        );
        assert_eq!(resolve(&ssh_adv(), &tag("main")).unwrap().commit, oid('f'));
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
        let err = resolve(&ssh_adv(), &branch("gone")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(ref m) if m.contains("refs/heads/gone")));
        let err = resolve(&ssh_adv(), &tag("gone")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(ref m) if m.contains("refs/tags/gone")));
    }

    #[test]
    fn empty_repo_advertises_no_refs() {
        let adv_bytes = body(&[format!(
            "{} capabilities^{{}}\0multi_ack agent=git/2.43.0\n",
            "0".repeat(40)
        )]);
        let adv = parse(&adv_bytes, &branch("main")).unwrap();
        assert!(adv.refs.is_empty());
        let err = commit_for_ref(&adv, &branch("main")).unwrap_err();
        assert!(matches!(err, GitError::RefNotFound(_)));
    }

    #[test]
    fn wrong_http_service_is_malformed() {
        let mut v = pkt("# service=git-receive-pack\n");
        v.extend(flush());
        v.extend(ssh_adv());
        let err = parse(&v, &branch("main")).unwrap_err();
        assert!(matches!(err, GitError::Malformed(ref m) if m.contains("git-receive-pack")));
    }

    #[test]
    fn protocol_v2_response_is_rejected() {
        // We never request v2, so a "version 2" banner in v0 position means
        // something is off — fail loudly instead of misparsing.
        let v = body(&["version 2\n".to_string(), "agent=git/2.43.0\n".to_string()]);
        let err = parse(&v, &branch("main")).unwrap_err();
        assert!(matches!(err, GitError::Malformed(ref m) if m.contains("version 2")));
    }

    #[test]
    fn truncated_stream_is_malformed() {
        let mut v = ssh_adv();
        v.truncate(v.len() - 10); // chop mid-pkt, losing the trailing flush
        let err = parse(&v, &branch("main")).unwrap_err();
        assert_eq!(err.to_string(), TRUNCATED);
    }

    #[test]
    fn missing_terminating_flush_is_malformed() {
        let v: Vec<u8> = pkt(&format!("{} refs/heads/main\n", oid('a')));
        let err = parse(&v, &branch("main")).unwrap_err();
        assert_eq!(err.to_string(), TRUNCATED);
    }

    #[test]
    fn an_uppercase_oid_is_malformed() {
        let upper = oid('a').to_ascii_uppercase();
        // Every line is checked, not only those that answer the request.
        for line in [
            format!("{upper} refs/heads/main\n"),
            format!("{upper} refs/pull/1/head\n"),
        ] {
            let v = body(&[line]);
            assert_eq!(
                parse(&v, &branch("main")).unwrap_err().to_string(),
                format!("malformed advertisement: '{upper}' is not a lowercase 40-hex oid")
            );
        }
    }

    #[test]
    fn garbage_line_is_malformed() {
        let v = body(&["not an advertisement line\n".to_string()]);
        assert!(matches!(
            parse(&v, &branch("main")),
            Err(GitError::Malformed(_))
        ));
    }

    #[test]
    fn only_the_lines_that_answer_the_ref_are_kept() {
        let cases = [
            (branch("main"), vec![advertised("refs/heads/main", 'a')]),
            (
                tag("v1.0.0"),
                vec![
                    advertised("refs/tags/v1.0.0", 'c'),
                    advertised("refs/tags/v1.0.0^{}", 'd'),
                ],
            ),
            (tag("main"), vec![advertised("refs/tags/main", 'f')]),
            (branch("gone"), vec![]),
        ];
        for (wanted, kept) in cases {
            for adv in [ssh_adv(), http_adv()] {
                assert_eq!(parse(&adv, &wanted).unwrap().refs, kept, "{wanted:?}");
            }
        }
    }

    #[test]
    fn the_advertisement_ends_at_its_flush_pkt_however_it_is_split() {
        for adv in [ssh_adv(), http_adv()] {
            let mut matcher = RefMatcher::new(&tag("v1.0.0"));
            let mut ends = Vec::new();
            for (at, byte) in adv.iter().enumerate() {
                if matcher.feed(std::slice::from_ref(byte)).unwrap() {
                    ends.push(at + 1);
                }
            }
            assert_eq!(ends, [adv.len()]);
            let resolved = commit_for_ref(&matcher.finish().unwrap(), &tag("v1.0.0")).unwrap();
            assert_eq!(resolved.commit, oid('d'));
        }
    }

    #[test]
    fn a_long_advertisement_is_held_one_pkt_line_at_a_time() {
        let lines = lines_with_pulls(50_000);
        let longest_line = lines.iter().map(|line| pkt(line).len()).max().unwrap();
        let adv = over_http(body(&lines));
        let mut matcher = RefMatcher::new(&tag("v1.0.0"));

        let mut most_held = 0;
        for chunk in adv.chunks(16 * 1024) {
            matcher.feed(chunk).unwrap();
            most_held = most_held.max(matcher.partial.len());
        }

        assert!(
            most_held < longest_line,
            "held {most_held} bytes of a {}-byte advertisement",
            adv.len()
        );
        assert_eq!(
            matcher.finish().unwrap().refs,
            [
                advertised("refs/tags/v1.0.0", 'c'),
                advertised("refs/tags/v1.0.0^{}", 'd'),
            ]
        );
    }

    fn git_config(vars: &[(&str, &str)]) -> anyhow::Result<GitConfig> {
        GitConfig::from_lookup(&|name| {
            vars.iter()
                .find(|(var, _)| *var == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn git_config_reads_the_charts_operator_git_settings() {
        assert_eq!(git_config(&[]).unwrap(), GitConfig::default());
        assert_eq!(
            git_config(&[
                ("RIVERS_GIT_ALLOW_INSECURE", "true"),
                ("RIVERS_GIT_ALLOWED_HOSTS", "gitea.internal, github.com,"),
            ])
            .unwrap(),
            GitConfig {
                allow_insecure: true,
                allowed_hosts: vec!["gitea.internal".to_string(), "github.com".to_string()],
                ..GitConfig::default()
            }
        );
        for (value, seconds) in [("", 30), ("45", 45), (" 600 ", 600)] {
            let config = git_config(&[("RIVERS_GIT_TIMEOUT_SECONDS", value)]).unwrap();
            assert_eq!(
                config.timeout,
                std::time::Duration::from_secs(seconds),
                "{value:?}"
            );
        }
        assert_eq!(
            git_config(&[]).unwrap().timeout,
            std::time::Duration::from_secs(30)
        );
        for (value, allow_insecure) in [("false", false), ("", false), ("1", true), ("0", false)] {
            let config = git_config(&[("RIVERS_GIT_ALLOW_INSECURE", value)]).unwrap();
            assert_eq!(config.allow_insecure, allow_insecure, "{value:?}");
        }
    }

    #[test]
    fn git_config_refuses_a_timeout_it_cannot_read() {
        for value in ["0", "abc", "-1", "1.5", "30s", "00"] {
            match git_config(&[("RIVERS_GIT_TIMEOUT_SECONDS", value)]) {
                Ok(config) => panic!("timeout {value:?} accepted: {config:?}"),
                Err(err) => assert_eq!(
                    err.to_string(),
                    format!(
                        "RIVERS_GIT_TIMEOUT_SECONDS (operator.git.timeoutSeconds): expected a \
                         whole number of seconds more than zero, got {value:?}"
                    )
                ),
            }
        }
    }

    #[test]
    fn git_config_refuses_an_allow_insecure_it_cannot_read() {
        for value in ["yes", "on", "2", "truth"] {
            let err = git_config(&[("RIVERS_GIT_ALLOW_INSECURE", value)]).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "RIVERS_GIT_ALLOW_INSECURE (operator.git.allowInsecure): expected true or \
                     false, got {value:?}"
                )
            );
        }
    }
}
