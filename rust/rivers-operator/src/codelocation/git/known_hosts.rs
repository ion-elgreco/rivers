//! Host-key verification for the SSH poll.
//!
//! The git Secret's `known_hosts` is read from its contents, before any
//! connection to the host. `ssh_key` parses each line; this module owns the
//! rest:
//!
//! * a `known_hosts` with no entry, or with a line that is not an entry, is
//!   a *distinct* error, so a broken Secret is diagnosable and never
//!   reaches the host;
//! * a changed key must be named as such, not folded into "unknown host";
//! * only exact (`host`, `[host]:port`) and hashed (`|1|`) host names match
//!   — no `*` wildcards, no `@cert-authority`/`@revoked` lines — and tests
//!   assert that limitation.
//!
//! Everything here fails closed. There is deliberately no accept-unknown
//! path anywhere in this module's API.

use russh::keys::PublicKey;
use russh::keys::ssh_key::known_hosts::{Entry, HostPatterns};

use super::GitError;

/// The keys the git Secret's `known_hosts` records for one `host:port`.
pub struct HostKeys {
    host: String,
    port: u16,
    /// Each matching entry's line number and key.
    keys: Vec<(usize, PublicKey)>,
}

impl HostKeys {
    /// The entries of `known_hosts` (the Secret's contents) for `host:port`.
    pub fn new(known_hosts: &str, host: &str, port: u16) -> Result<Self, GitError> {
        let name = if port == 22 {
            host.to_string()
        } else {
            format!("[{host}]:{port}")
        };
        let mut entries = 0;
        let mut keys = Vec::new();
        for (index, line) in known_hosts.lines().enumerate() {
            let line = line.split_once('#').map_or(line, |(entry, _comment)| entry);
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.is_empty() {
                continue;
            }
            let entry: Entry = fields.join(" ").parse().map_err(|e| {
                GitError::KnownHostsUnavailable(format!(
                    "line {} of the git Secret's `known_hosts` is not a host key entry: {e}",
                    index + 1
                ))
            })?;
            entries += 1;
            if entry.marker().is_none() && matches_host(entry.host_patterns(), &name) {
                keys.push((index + 1, entry.public_key().clone()));
            }
        }
        if entries == 0 {
            return Err(GitError::KnownHostsUnavailable(
                "the git Secret's `known_hosts` lists no host keys — refusing to trust any host"
                    .to_string(),
            ));
        }
        if keys.is_empty() {
            return Err(not_listed(host, port));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            keys,
        })
    }

    /// Accept `key` only when `known_hosts` records it for the host.
    pub fn verify(&self, key: &PublicKey) -> Result<(), GitError> {
        // `key_data()`, not `==`: `PublicKey`'s `PartialEq` includes the
        // comment, so a hand-copied entry (`… ops@bastion`) would read as a
        // CHANGED key against the comment-less wire key.
        let mut changed_line = None;
        for (line, recorded) in &self.keys {
            if key.key_data() == recorded.key_data() {
                return Ok(());
            }
            if key.algorithm() == recorded.algorithm() {
                changed_line = Some(*line);
            }
        }
        match changed_line {
            Some(line) => Err(GitError::HostKeyRejected(format!(
                "HOST KEY CHANGED for '{}:{}' (known_hosts line {line}) — possible \
                 man-in-the-middle; refusing. Update the Secret only after out-of-band verification",
                self.host, self.port
            ))),
            None => Err(not_listed(&self.host, self.port)),
        }
    }
}

/// Whether an entry's host names are `name` (`host` or `[host]:port`).
fn matches_host(patterns: &HostPatterns, name: &str) -> bool {
    match patterns {
        HostPatterns::Patterns(patterns) => patterns.iter().any(|p| p == name),
        HostPatterns::HashedName { salt, hash } => {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, salt);
            ring::hmac::verify(&key, name.as_bytes(), hash).is_ok()
        }
    }
}

fn not_listed(host: &str, port: u16) -> GitError {
    GitError::HostKeyRejected(format!(
        "host '{host}:{port}' is not in known_hosts (exact and hashed |1| entries only; \
         wildcards and @cert-authority are unsupported) — add this host's key to the git Secret"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed fixtures generated once with ssh-keygen (ed25519). The hashed
    /// line is `ssh-keygen -H` output for host `[gitea.internal]:2222` and
    /// HOST_PUB — regenerating it requires recomputing the HMAC, so treat
    /// these as one immutable set.
    const HOST_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFiXHrGv1EZGzi9tPYZ+mCp6mJ98ybVQvfbdNz3hrFeQ";
    const ROGUE_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDqeWXNsfD9+VE5aWkw91nguvO9RjLWqy87Xon+WA3w0";
    const HASHED_LINE: &str = "|1|Foa2jWkQsC0If/oD8I9WRG1bJyk=|LVW1/NfSV7Wk/ffpVteO0IcULG4= \
         ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFiXHrGv1EZGzi9tPYZ+mCp6mJ98ybVQvfbdNz3hrFeQ";

    fn host_key() -> PublicKey {
        PublicKey::from_openssh(&format!("{HOST_PUB} host")).unwrap()
    }

    /// Check the key `gitea.internal:port` presents against `known_hosts`.
    fn verify(known_hosts: &str, port: u16) -> Result<(), GitError> {
        HostKeys::new(known_hosts, "gitea.internal", port)?.verify(&host_key())
    }

    #[test]
    fn exact_entry_with_port_accepts() {
        verify(&format!("[gitea.internal]:2222 {HOST_PUB}\n"), 2222).unwrap();
    }

    #[test]
    fn exact_entry_default_port_accepts() {
        verify(&format!("gitea.internal {HOST_PUB}\n"), 22).unwrap();
    }

    #[test]
    fn hashed_entry_accepts_only_its_host() {
        verify(&format!("{HASHED_LINE}\n"), 2222).unwrap();
        let err = verify(&format!("{HASHED_LINE}\n"), 22).unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("not in known_hosts")),
            "{err}"
        );
    }

    #[test]
    fn commented_entry_matches_commentless_wire_key() {
        // Hand-copied entries usually keep a trailing comment. The wire key
        // never has one — whole-key equality would misreport this exact
        // shape as a CHANGED key (false MITM), so key-data comparison is
        // load-bearing, not cosmetic.
        verify(
            &format!("[gitea.internal]:2222 {HOST_PUB} ops@bastion\n"),
            2222,
        )
        .unwrap();
    }

    #[test]
    fn our_host_on_a_later_line_accepts() {
        let known_hosts = format!("other.example {ROGUE_PUB}\n[gitea.internal]:2222 {HOST_PUB}\n");
        verify(&known_hosts, 2222).unwrap();
    }

    #[test]
    fn comments_blank_lines_and_extra_whitespace_are_ignored() {
        let known_hosts = format!(
            "# gitea.internal:2222 SSH-2.0-OpenSSH_9.6\n\n  [gitea.internal]:2222\t{HOST_PUB}  # ops\n"
        );
        verify(&known_hosts, 2222).unwrap();
    }

    #[test]
    fn unknown_host_rejects() {
        let err = verify(&format!("other.example {HOST_PUB}\n"), 2222).unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("not in known_hosts")),
            "{err}"
        );
    }

    #[test]
    fn changed_key_rejects_distinctly() {
        // The host is present but pins a different key of the same
        // algorithm — the classic MITM shape. Must not read as "unknown".
        let known_hosts = format!("# pinned\n[gitea.internal]:2222 {ROGUE_PUB}\n");
        let err = verify(&known_hosts, 2222).unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m)
                if m.contains("CHANGED") && m.contains("known_hosts line 2")),
            "{err}"
        );
    }

    #[test]
    fn known_hosts_without_entries_is_a_distinct_error() {
        for known_hosts in ["", " \n\n", "# gitea.internal:22 SSH-2.0-OpenSSH_9.6\n"] {
            let err = verify(known_hosts, 22).unwrap_err();
            assert_eq!(
                err.to_string(),
                "known_hosts unavailable: the git Secret's `known_hosts` lists no host keys — \
                 refusing to trust any host",
                "{known_hosts:?}"
            );
        }
    }

    #[test]
    fn a_line_that_is_not_an_entry_is_a_distinct_error() {
        let known_hosts = format!("gitea.internal {HOST_PUB}\nthis is not a known_hosts line\n");
        let err = verify(&known_hosts, 22).unwrap_err();
        assert!(
            matches!(err, GitError::KnownHostsUnavailable(ref m)
                if m.starts_with("line 2 of the git Secret's `known_hosts` is not a host key entry")),
            "{err}"
        );
    }

    #[test]
    fn wildcard_entry_is_not_matched() {
        // Documented limitation: exact + hashed entries only. Matching
        // wildcards is a deliberate change of this test and the docs.
        let err = verify(&format!("*.internal {HOST_PUB}\n"), 22).unwrap_err();
        assert!(matches!(err, GitError::HostKeyRejected(_)), "{err}");
    }

    #[test]
    fn marked_entries_are_never_trusted() {
        for marker in ["@cert-authority", "@revoked"] {
            let err = verify(&format!("{marker} gitea.internal {HOST_PUB}\n"), 22).unwrap_err();
            assert!(
                matches!(err, GitError::HostKeyRejected(ref m) if m.contains("not in known_hosts")),
                "{marker}: {err}"
            );
        }
    }
}
