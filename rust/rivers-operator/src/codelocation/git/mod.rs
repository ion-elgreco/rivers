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
    /// Credentials rejected. Terminal until the Secret changes.
    #[error("git authentication failed")]
    AuthFailed,
    /// Transport-level failure (connect, timeout, 5xx). Transient.
    #[error("git host unreachable: {0}")]
    Unreachable(String),
    /// The response was not a protocol-v0 ref advertisement.
    #[error("malformed advertisement: {0}")]
    Malformed(String),
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
