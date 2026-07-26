//! SSH transport arm: in-process `git-upload-pack` over russh.
//!
//! ```text
//! connect → publickey auth (key file from the Secret) → open session
//!         → exec "git-upload-pack '<path>'" → buffer stdout → parse
//! ```
//!
//! Host keys are verified against the Secret's `known_hosts` via
//! [`super::known_hosts`] before authentication — russh's `check_server_key`
//! default already rejects everything, and the handler here only ever
//! upgrades that to "accept" on an explicit known_hosts match.
//!
//! The exec command is handed to a shell on the *remote* side, so the repo
//! path is single-quoted here. This transport is structurally immune to the
//! local argv-injection class (RUSTSEC-2024-0335 hit gix's spawning
//! transport) — remote-side quoting is the part that stays ours.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use russh::ChannelMsg;
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};

use super::http::DEFAULT_MAX_ADVERTISEMENT_BYTES;
use super::{GitError, known_hosts};

/// `ssh://[user@]host[:port]/path`, decomposed. Only the `ssh://` URL form
/// is accepted — scp-like `git@host:path` strings are rejected at admission
/// and again here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshTarget {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub repo_path: String,
}

pub fn parse_ssh_url(raw: &str) -> Result<SshTarget, GitError> {
    let url = url::Url::parse(raw)
        .map_err(|e| GitError::Malformed(format!("invalid ssh url '{raw}': {e}")))?;
    if url.scheme() != "ssh" {
        return Err(GitError::Malformed(format!(
            "'{raw}' is not an ssh:// url (scp-style 'git@host:path' is not supported)"
        )));
    }
    let host = url
        .host_str()
        .ok_or_else(|| GitError::Malformed(format!("'{raw}' has no host")))?
        .to_string();
    let repo_path = url.path().to_string();
    if repo_path.is_empty() || repo_path == "/" {
        return Err(GitError::Malformed(format!(
            "'{raw}' has no repository path"
        )));
    }
    Ok(SshTarget {
        user: if url.username().is_empty() {
            "git".to_string()
        } else {
            url.username().to_string()
        },
        host,
        port: url.port().unwrap_or(22),
        repo_path,
    })
}

/// Single-quote `s` for the remote shell: `'…'` with embedded single quotes
/// as `'\''`. Everything else — `$`, backticks, `;`, spaces — is literal
/// inside single quotes.
pub(crate) fn shell_quote_single(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// SSH credential material, as file paths from the mounted Secret.
#[derive(Clone, Debug)]
pub struct SshAuth {
    pub private_key_path: PathBuf,
    pub known_hosts_path: PathBuf,
}

/// Delegates the server-key decision to the `known_hosts` wrapper. The
/// default `check_server_key` rejects all keys; this only flips to accept
/// on an explicit match — errors carry the specific rejection.
struct HostKeyCheck {
    host: String,
    port: u16,
    known_hosts_path: PathBuf,
}

impl client::Handler for HostKeyCheck {
    type Error = GitError;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        known_hosts::verify_host_key(
            &self.known_hosts_path,
            &self.host,
            self.port,
            server_public_key,
        )?;
        Ok(true)
    }
}

/// Fetch the ref advertisement over SSH, fully buffered.
pub async fn fetch_advertisement(
    target: &SshTarget,
    auth: &SshAuth,
    timeout: Duration,
) -> Result<Vec<u8>, GitError> {
    fetch_advertisement_with_cap(target, auth, timeout, DEFAULT_MAX_ADVERTISEMENT_BYTES).await
}

pub(crate) async fn fetch_advertisement_with_cap(
    target: &SshTarget,
    auth: &SshAuth,
    timeout: Duration,
    cap: usize,
) -> Result<Vec<u8>, GitError> {
    let key = load_secret_key(&auth.private_key_path, None).map_err(|e| {
        GitError::AuthFailed(format!(
            "cannot load SSH private key from {}: {e} — check the git Secret's `identity` entry",
            auth.private_key_path.display()
        ))
    })?;

    let config = Arc::new(client::Config {
        inactivity_timeout: Some(timeout),
        ..Default::default()
    });
    let handler = HostKeyCheck {
        host: target.host.clone(),
        port: target.port,
        known_hosts_path: auth.known_hosts_path.clone(),
    };

    // `connect` returns `H::Error`, so a host-key rejection from the handler
    // surfaces as our own GitError; transport failures arrive via
    // `From<russh::Error>` as `Unreachable`.
    let mut handle = client::connect(config, (target.host.as_str(), target.port), handler).await?;

    let rsa_hash = handle.best_supported_rsa_hash().await?.flatten();
    let outcome = handle
        .authenticate_publickey(
            target.user.clone(),
            PrivateKeyWithHashAlg::new(Arc::new(key), rsa_hash),
        )
        .await?;
    if !matches!(outcome, russh::client::AuthResult::Success) {
        return Err(GitError::AuthFailed(format!(
            "server rejected publickey authentication for user '{}'",
            target.user
        )));
    }

    let mut channel = handle.channel_open_session().await?;
    let command = format!("git-upload-pack {}", shell_quote_single(&target.repo_path));
    channel.exec(true, command).await?;

    let mut body = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_status = None;
    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { data } => {
                body.extend_from_slice(&data);
                if body.len() > cap {
                    return Err(GitError::Malformed(format!(
                        "advertisement exceeds {cap} bytes"
                    )));
                }
            }
            // ext 1 == SSH_EXTENDED_DATA_STDERR — upload-pack's error text.
            ChannelMsg::ExtendedData { data, ext: 1 } => stderr.extend_from_slice(&data),
            ChannelMsg::ExitStatus { exit_status: s } => exit_status = Some(s),
            _ => {}
        }
    }
    let _ = handle
        .disconnect(russh::Disconnect::ByApplication, "", "en")
        .await;

    match exit_status {
        Some(0) => Ok(body),
        // Some servers close without sending an exit-status; the parser's
        // flush-required strictness catches a truncated body.
        None if !body.is_empty() => Ok(body),
        other => {
            let stderr = String::from_utf8_lossy(&stderr);
            Err(GitError::RefNotFound(format!(
                "git-upload-pack failed{} for '{}': {}",
                other.map(|s| format!(" (exit {s})")).unwrap_or_default(),
                target.repo_path,
                stderr.trim()
            )))
        }
    }
}

/// In-process russh git server for tests — shared with the resolver tests in
/// `super::mod`, which is why it lives outside the `tests` module.
#[cfg(test)]
pub(crate) mod test_server {
    use super::super::fixtures;
    use russh::keys::decode_secret_key;
    use russh::server::{self, Auth, ChannelOpenHandle, Msg, Server as _, Session};
    use russh::{Channel, ChannelId};
    use std::sync::Arc;

    // Fixed ed25519 fixtures generated once with ssh-keygen; the pubkeys
    // pair with the private keys below.
    pub(crate) const CLIENT_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDUahwZoxoHfJYcEqL7WsxYj1WdYMw8Tafk2Tk5Ful/y";
    pub(crate) const HOST_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFiXHrGv1EZGzi9tPYZ+mCp6mJ98ybVQvfbdNz3hrFeQ";
    pub(crate) const ROGUE_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDqeWXNsfD9+VE5aWkw91nguvO9RjLWqy87Xon+WA3w0";

    pub(crate) const CLIENT_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACA1GocGaMaB3yWHBKi+1rMWI9VnWDMPE2n5Nk5ORbpf8gAAAJD7qj34+6o9
+AAAAAtzc2gtZWQyNTUxOQAAACA1GocGaMaB3yWHBKi+1rMWI9VnWDMPE2n5Nk5ORbpf8g
AAAEBT/YSaZG7+P8QWodvIV/4Agh71GYW57RffMH/5hQKoijUahwZoxoHfJYcEqL7WsxYj
1WdYMw8Tafk2Tk5Ful/yAAAABmNsaWVudAECAwQFBgc=
-----END OPENSSH PRIVATE KEY-----
";
    pub(crate) const HOST_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACBYlx6xr9RGRs4vbT2GfpgqepiffMm1UL323Tc94axXkAAAAIhLOkY7SzpG
OwAAAAtzc2gtZWQyNTUxOQAAACBYlx6xr9RGRs4vbT2GfpgqepiffMm1UL323Tc94axXkA
AAAEAePrUUBrPim4oMVYXt6hNWyccDkZeMfyjXbV/qzxFx6ViXHrGv1EZGzi9tPYZ+mCp6
mJ98ybVQvfbdNz3hrFeQAAAABGhvc3QB
-----END OPENSSH PRIVATE KEY-----
";

    #[derive(Clone)]
    pub(crate) struct ServerBehaviour {
        pub(crate) advertisement: Vec<u8>,
        pub(crate) expected_command: String,
        pub(crate) exit_status: u32,
        pub(crate) stderr: Option<&'static str>,
        pub(crate) authorized_pub: &'static str,
    }

    impl Default for ServerBehaviour {
        fn default() -> Self {
            Self {
                advertisement: fixtures::ssh_adv(),
                expected_command: "git-upload-pack '/acme/pipelines.git'".to_string(),
                exit_status: 0,
                stderr: None,
                authorized_pub: CLIENT_PUB,
            }
        }
    }

    struct TestGitServer {
        behaviour: ServerBehaviour,
    }

    impl server::Server for TestGitServer {
        type Handler = TestSession;
        fn new_client(&mut self, _peer: Option<std::net::SocketAddr>) -> TestSession {
            TestSession {
                behaviour: self.behaviour.clone(),
            }
        }
    }

    struct TestSession {
        behaviour: ServerBehaviour,
    }

    impl server::Handler for TestSession {
        type Error = russh::Error;

        async fn auth_publickey(
            &mut self,
            user: &str,
            key: &russh::keys::PublicKey,
        ) -> Result<Auth, Self::Error> {
            let authorized = russh::keys::PublicKey::from_openssh(&format!(
                "{} t",
                self.behaviour.authorized_pub
            ))
            .expect("fixture pubkey");
            // key_data comparison — PublicKey's derived eq includes the
            // comment, and the wire key never carries one.
            if user == "git" && key.key_data() == authorized.key_data() {
                Ok(Auth::Accept)
            } else {
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                })
            }
        }

        async fn channel_open_session(
            &mut self,
            _channel: Channel<Msg>,
            reply: ChannelOpenHandle,
            _session: &mut Session,
        ) -> Result<(), Self::Error> {
            reply.accept().await;
            Ok(())
        }

        async fn exec_request(
            &mut self,
            channel: ChannelId,
            data: &[u8],
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            // Exact byte match — this is also what proves the client's
            // shell-quoting reached the wire literally.
            if data == self.behaviour.expected_command.as_bytes() {
                session.channel_success(channel)?;
                session.data(
                    channel,
                    bytes::Bytes::from(self.behaviour.advertisement.clone()),
                )?;
                if let Some(err) = self.behaviour.stderr {
                    session.extended_data(channel, 1, bytes::Bytes::from_static(err.as_bytes()))?;
                }
                session.exit_status_request(channel, self.behaviour.exit_status)?;
            } else {
                session.channel_failure(channel)?;
                session.extended_data(
                    channel,
                    1,
                    bytes::Bytes::from(format!(
                        "unexpected command: {}",
                        String::from_utf8_lossy(data)
                    )),
                )?;
                session.exit_status_request(channel, 127)?;
            }
            session.eof(channel)?;
            session.close(channel)?;
            Ok(())
        }
    }

    /// Boot a russh server on an ephemeral port; returns its port.
    pub(crate) async fn spawn_server(behaviour: ServerBehaviour) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = Arc::new(server::Config {
            keys: vec![decode_secret_key(HOST_KEY, None).unwrap()],
            ..Default::default()
        });
        tokio::spawn(async move {
            let mut srv = TestGitServer { behaviour };
            let _ = srv.run_on_socket(config, &listener).await;
        });
        port
    }
}

#[cfg(test)]
mod tests {
    use super::super::{commit_for_ref, fixtures, parse_advertisement};
    use super::test_server::*;
    use super::*;
    use std::io::Write as _;

    struct TestAuth {
        _key: tempfile::NamedTempFile,
        _kh: tempfile::NamedTempFile,
        auth: SshAuth,
    }

    /// Client key + a known_hosts pinning `pub_for_host` for this port.
    fn auth_material(port: u16, pub_for_host: &str) -> TestAuth {
        let mut key = tempfile::NamedTempFile::new().unwrap();
        key.write_all(CLIENT_KEY.as_bytes()).unwrap();
        key.flush().unwrap();
        let mut kh = tempfile::NamedTempFile::new().unwrap();
        writeln!(kh, "[127.0.0.1]:{port} {pub_for_host}").unwrap();
        kh.flush().unwrap();
        let auth = SshAuth {
            private_key_path: key.path().to_path_buf(),
            known_hosts_path: kh.path().to_path_buf(),
        };
        TestAuth {
            _key: key,
            _kh: kh,
            auth,
        }
    }

    const TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn end_to_end_resolves_branch_over_ssh() {
        let port = spawn_server(ServerBehaviour::default()).await;
        let material = auth_material(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let bytes = fetch_advertisement(&target, &material.auth, TIMEOUT)
            .await
            .unwrap();
        let adv = parse_advertisement(&bytes).unwrap();
        let resolved = commit_for_ref(&adv, &fixtures::branch("main")).unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
    }

    #[tokio::test]
    async fn unauthorized_key_is_auth_failed() {
        let behaviour = ServerBehaviour {
            authorized_pub: ROGUE_PUB, // our client key is not this one
            ..Default::default()
        };
        let port = spawn_server(behaviour).await;
        let material = auth_material(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &material.auth, TIMEOUT)
            .await
            .unwrap_err();
        assert!(matches!(err, GitError::AuthFailed(_)), "{err}");
    }

    #[tokio::test]
    async fn changed_host_key_is_rejected_before_auth() {
        let port = spawn_server(ServerBehaviour::default()).await;
        // known_hosts pins the *rogue* key for this host — the server
        // presents HOST_PUB, i.e. the MITM shape.
        let material = auth_material(port, ROGUE_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &material.auth, TIMEOUT)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("CHANGED")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn missing_private_key_is_auth_failed_with_path() {
        let port = spawn_server(ServerBehaviour::default()).await;
        let material = auth_material(port, HOST_PUB);
        let auth = SshAuth {
            private_key_path: PathBuf::from("/not/mounted/identity"),
            known_hosts_path: material.auth.known_hosts_path.clone(),
        };
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &auth, TIMEOUT)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::AuthFailed(ref m) if m.contains("/not/mounted/identity")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn nonzero_exit_surfaces_stderr() {
        let behaviour = ServerBehaviour {
            advertisement: Vec::new(),
            exit_status: 128,
            stderr: Some("fatal: '/acme/pipelines.git' does not appear to be a git repository"),
            ..Default::default()
        };
        let port = spawn_server(behaviour).await;
        let material = auth_material(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &material.auth, TIMEOUT)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::RefNotFound(ref m)
                if m.contains("exit 128") && m.contains("does not appear")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn connection_refused_is_unreachable() {
        let material = auth_material(1, HOST_PUB);
        let target = parse_ssh_url("ssh://git@127.0.0.1:1/r.git").unwrap();
        let err = fetch_advertisement(&target, &material.auth, Duration::from_secs(3))
            .await
            .unwrap_err();
        assert!(matches!(err, GitError::Unreachable(_)), "{err}");
    }

    #[test]
    fn parse_ssh_url_shapes() {
        let t = parse_ssh_url("ssh://git@gitea.internal:2222/acme/repo.git").unwrap();
        assert_eq!(
            t,
            SshTarget {
                user: "git".to_string(),
                host: "gitea.internal".to_string(),
                port: 2222,
                repo_path: "/acme/repo.git".to_string(),
            }
        );
        // Defaults: user git, port 22.
        let t = parse_ssh_url("ssh://forge.example/org/repo.git").unwrap();
        assert_eq!(t.user, "git");
        assert_eq!(t.port, 22);

        assert!(parse_ssh_url("https://forge.example/org/repo.git").is_err());
        assert!(parse_ssh_url("git@forge.example:org/repo.git").is_err());
        assert!(parse_ssh_url("ssh://forge.example").is_err());
    }

    #[test]
    fn shell_quoting_neutralizes_metacharacters() {
        assert_eq!(shell_quote_single("/a/b.git"), "'/a/b.git'");
        assert_eq!(
            shell_quote_single("/a/$(rm -rf ~)/`x`;b.git"),
            "'/a/$(rm -rf ~)/`x`;b.git'"
        );
        assert_eq!(shell_quote_single("a'b"), r"'a'\''b'");
    }
}
