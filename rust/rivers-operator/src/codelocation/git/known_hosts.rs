//! Host-key verification for the SSH poll.
//!
//! The parsing and matching (including hashed `|1|` entries) is
//! **delegated to `russh::keys::known_hosts`** — this module owns only the
//! edges russh leaves to the caller:
//!
//! * a missing/unreadable/empty `known_hosts` must be a *distinct* error
//!   (russh alone returns an empty match set there — fail-closed but
//!   silent, which makes a mis-mounted Secret undiagnosable);
//! * a changed key must be named as such, not folded into "unknown host";
//! * the matcher supports exact and hashed entries only — no `*` wildcards,
//!   no `@cert-authority`/`@revoked` — and that limitation is asserted by
//!   test so a russh upgrade that adds support is noticed.
//!
//! Everything here fails closed. There is deliberately no accept-unknown
//! path anywhere in this module's API.

use std::path::Path;

use super::GitError;

/// Verify `key` for `host:port` against the mounted `known_hosts` file.
pub fn verify_host_key(
    known_hosts: &Path,
    host: &str,
    port: u16,
    key: &russh::keys::PublicKey,
) -> Result<(), GitError> {
    match std::fs::metadata(known_hosts) {
        Err(e) => {
            return Err(GitError::KnownHostsUnavailable(format!(
                "known_hosts not readable at {}: {e} — check the git Secret's `known_hosts` entry",
                known_hosts.display()
            )));
        }
        Ok(m) if m.len() == 0 => {
            return Err(GitError::KnownHostsUnavailable(format!(
                "known_hosts at {} is empty — refusing to trust any host",
                known_hosts.display()
            )));
        }
        Ok(_) => {}
    }

    // russh does the parsing and hashed-hostname matching; the *comparison*
    // is ours because `ssh_key::PublicKey`'s derived `PartialEq` includes
    // the comment field — russh's own `check_known_hosts_path` therefore
    // reports a known_hosts entry with a trailing comment (`… user@host`)
    // as a CHANGED key against the comment-less wire key. Comparing
    // `key_data()` only is what OpenSSH semantics require.
    let entries = match russh::keys::known_hosts::known_host_keys_path(host, port, known_hosts) {
        Ok(entries) => entries,
        Err(e) => {
            return Err(GitError::KnownHostsUnavailable(format!(
                "known_hosts at {} could not be used: {e}",
                known_hosts.display()
            )));
        }
    };

    let mut changed_line = None;
    for (line, recorded) in &entries {
        if key.key_data() == recorded.key_data() {
            return Ok(());
        }
        if key.algorithm() == recorded.algorithm() {
            changed_line = Some(*line);
        }
    }
    match changed_line {
        Some(line) => Err(GitError::HostKeyRejected(format!(
            "HOST KEY CHANGED for '{host}:{port}' (known_hosts line {line}) — possible \
             man-in-the-middle; refusing. Update the Secret only after out-of-band verification"
        ))),
        None => Err(GitError::HostKeyRejected(format!(
            "host '{host}:{port}' is not in known_hosts (exact and hashed |1| entries only; \
             wildcards and @cert-authority are unsupported) — add this host's key to the git Secret"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

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

    fn host_key() -> russh::keys::PublicKey {
        russh::keys::PublicKey::from_openssh(&format!("{HOST_PUB} host")).unwrap()
    }

    fn kh_file(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn exact_entry_with_port_accepts() {
        let f = kh_file(&format!("[gitea.internal]:2222 {HOST_PUB}\n"));
        verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap();
    }

    #[test]
    fn exact_entry_default_port_accepts() {
        let f = kh_file(&format!("gitea.internal {HOST_PUB}\n"));
        verify_host_key(f.path(), "gitea.internal", 22, &host_key()).unwrap();
    }

    #[test]
    fn hashed_entry_accepts() {
        let f = kh_file(&format!("{HASHED_LINE}\n"));
        verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap();
    }

    #[test]
    fn commented_entry_matches_commentless_wire_key() {
        // Hand-copied entries usually keep a trailing comment. The wire key
        // never has one — whole-key equality would misreport this exact
        // shape as a CHANGED key (false MITM), so key-data comparison is
        // load-bearing, not cosmetic.
        let f = kh_file(&format!("[gitea.internal]:2222 {HOST_PUB} ops@bastion\n"));
        verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap();
    }

    #[test]
    fn our_host_on_a_later_line_accepts() {
        let f = kh_file(&format!(
            "other.example {ROGUE_PUB}\n[gitea.internal]:2222 {HOST_PUB}\n"
        ));
        verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap();
    }

    #[test]
    fn unknown_host_rejects() {
        let f = kh_file(&format!("other.example {HOST_PUB}\n"));
        let err = verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("not in known_hosts")),
            "{err}"
        );
    }

    #[test]
    fn changed_key_rejects_distinctly() {
        // The host is present but pins a different key of the same
        // algorithm — the classic MITM shape. Must not read as "unknown".
        let f = kh_file(&format!("[gitea.internal]:2222 {ROGUE_PUB}\n"));
        let err = verify_host_key(f.path(), "gitea.internal", 2222, &host_key()).unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("CHANGED")),
            "{err}"
        );
    }

    #[test]
    fn missing_file_is_a_distinct_error() {
        let err = verify_host_key(
            Path::new("/definitely/not/mounted/known_hosts"),
            "gitea.internal",
            22,
            &host_key(),
        )
        .unwrap_err();
        assert!(
            matches!(err, GitError::KnownHostsUnavailable(ref m) if m.contains("known_hosts")),
            "{err}"
        );
    }

    #[test]
    fn empty_file_is_a_distinct_error() {
        let f = kh_file("");
        let err = verify_host_key(f.path(), "gitea.internal", 22, &host_key()).unwrap_err();
        assert!(
            matches!(err, GitError::KnownHostsUnavailable(ref m) if m.contains("empty")),
            "{err}"
        );
    }

    #[test]
    fn wildcard_entry_is_not_matched() {
        // Documented limitation: russh matches exact + hashed entries only.
        // If a russh upgrade starts matching wildcards, this test fails and
        // the docs (and this policy) get revisited deliberately.
        let f = kh_file(&format!("*.internal {HOST_PUB}\n"));
        let err = verify_host_key(f.path(), "gitea.internal", 22, &host_key()).unwrap_err();
        assert!(matches!(err, GitError::HostKeyRejected(_)), "{err}");
    }

    #[test]
    fn garbage_only_file_fails_closed() {
        let f = kh_file("this is not a known_hosts line\n");
        assert!(verify_host_key(f.path(), "gitea.internal", 22, &host_key()).is_err());
    }
}
