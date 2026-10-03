//! SSH transport arm: in-process `git-upload-pack` over russh.
//!
//! ```text
//! decode key + read known_hosts → connect → publickey auth → open session
//!         → exec "git-upload-pack '<path>'" → buffer stdout → parse
//! ```
//!
//! The Secret's key and `known_hosts` are used as contents and both are
//! checked before connecting. Host keys are verified against the
//! `known_hosts` entries via [`super::known_hosts`] before authentication —
//! russh's `check_server_key` default already rejects everything, and the
//! handler here only ever upgrades that to "accept" on an explicit match.
//!
//! The exec command is handed to a shell on the *remote* side, so the repo
//! path is single-quoted here. This transport is structurally immune to the
//! local argv-injection class (RUSTSEC-2024-0335 hit gix's spawning
//! transport) — remote-side quoting is the part that stays ours.

use std::sync::Arc;
use std::time::Duration;

use russh::ChannelMsg;
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, decode_secret_key};

use super::http::DEFAULT_MAX_ADVERTISEMENT_BYTES;
use super::{GitError, known_hosts};

/// `ssh://user@host[:port]/path`, decomposed. Only the `ssh://` URL form
/// is accepted — scp-like `git@host:path` strings are rejected at admission
/// and again here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshTarget {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub repo_path: String,
}

/// A url without a user is an auth failure, not a default: the pods' ssh
/// would log in as their own account, not as the user resolved with here.
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
    if url.username().is_empty() {
        return Err(GitError::AuthFailed(format!(
            "the ssh url must name the user — use {}, or the user your git host expects",
            with_git_user(&url)
        )));
    }
    Ok(SshTarget {
        user: url.username().to_string(),
        host,
        port: url.port().unwrap_or(22),
        repo_path,
    })
}

/// `url` as `ssh://git@host[:port]/path`: the user most git hosts take.
/// Built from parts, so no password in `url` reaches a message.
pub(crate) fn with_git_user(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    format!("ssh://git@{host}{port}{}", url.path())
}

/// Single-quote `s` for the remote shell: `'…'` with embedded single quotes
/// as `'\''`. Everything else — `$`, backticks, `;`, spaces — is literal
/// inside single quotes.
pub(crate) fn shell_quote_single(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// SSH credential material: the git Secret's `identity` and `known_hosts`
/// contents.
pub struct SshAuth<'a> {
    pub private_key: &'a str,
    pub known_hosts: &'a str,
}

/// Delegates the server-key decision to the `known_hosts` entries. The
/// default `check_server_key` rejects all keys; this only flips to accept
/// on an explicit match — errors carry the specific rejection.
struct HostKeyCheck(known_hosts::HostKeys);

impl client::Handler for HostKeyCheck {
    type Error = GitError;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        self.0.verify(server_public_key)?;
        Ok(true)
    }
}

/// Fetch the ref advertisement over SSH, fully buffered.
pub async fn fetch_advertisement(
    target: &SshTarget,
    auth: &SshAuth<'_>,
    timeout: Duration,
) -> Result<Vec<u8>, GitError> {
    fetch_advertisement_with_cap(target, auth, timeout, DEFAULT_MAX_ADVERTISEMENT_BYTES).await
}

pub(crate) async fn fetch_advertisement_with_cap(
    target: &SshTarget,
    auth: &SshAuth<'_>,
    timeout: Duration,
    cap: usize,
) -> Result<Vec<u8>, GitError> {
    let key = decode_secret_key(auth.private_key, None).map_err(|e| {
        GitError::AuthFailed(match e {
            russh::keys::Error::KeyIsEncrypted => "the SSH private key is passphrase-protected \
                 — the git Secret's `identity` must be a key without a passphrase"
                .to_string(),
            e => format!(
                "cannot read the SSH private key: {e} — check the git Secret's `identity` entry"
            ),
        })
    })?;
    let host_keys = known_hosts::HostKeys::new(auth.known_hosts, &target.host, target.port)?;

    let config = Arc::new(client::Config {
        inactivity_timeout: Some(timeout),
        preferred: russh::Preferred {
            key: host_keys.algorithms()?.into(),
            ..russh::Preferred::DEFAULT
        },
        ..Default::default()
    });
    let handler = HostKeyCheck(host_keys);

    // `connect` returns `H::Error`, so a host-key rejection from the handler
    // surfaces as our own GitError; russh's own errors arrive via
    // `From<russh::Error>`.
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    // ECDSA P-256 and RSA 2048 host keys, generated the same way.
    pub(crate) const ECDSA_HOST_PUB: &str = "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBD3Jl0sLsh30HWs8Oge6DWH1DvM3x5tId44vx5vakRiOGd7UOibKDTURI/tgGISQloA70kzLr1vdlByTS9ku51k=";
    pub(crate) const ECDSA_HOST_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS
1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQQ9yZdLC7Id9B1rPDoHug1h9Q7zN8eb
SHeOL8eb2pEYjhne1Domyg01ESP7YBiEkJaAO9JMy69b3ZQck0vZLudZAAAAmE6AyE1OgM
hNAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBD3Jl0sLsh30HWs8
Oge6DWH1DvM3x5tId44vx5vakRiOGd7UOibKDTURI/tgGISQloA70kzLr1vdlByTS9ku51
kAAAAgHlQws6aGudt2/q03RKUuNjJNKSxI1rmyE9z6Gb9bAC4AAAAA
-----END OPENSSH PRIVATE KEY-----
";
    pub(crate) const RSA_HOST_PUB: &str = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDqyZy6n10ANLsqh7xRyowl9TDgYPJp2QFIB8xhxrFxZx+nM/rixg4GDolm92kN4tT8BV68I7tHt4cx5MtrkMs65Gmx4gA3OB/aDnCpIMk66ecmeXs6aBCkKFxKqDSu3YOqZ/COm5dikIs42PDgkf1u13veDUPtsrTzkd8HI4R5gNbCTq/tZ4DO5+LA8kmCWb/AMziFj/XlEKMNRsjHi0vBN7WlWfYJD1x23xO7JWaUjrJF9hK5gb9Q2ansoCn9DWqttvnvdRGXSbx2w3aP9md40bZG90IsFMaJK1K4sR6UmdYQ52chhZizdn3mFJFmkSYEbCY2GlZX/6kLNOK3rKXv";
    pub(crate) const RSA_HOST_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn
NhAAAAAwEAAQAAAQEA6smcup9dADS7Koe8UcqMJfUw4GDyadkBSAfMYcaxcWcfpzP64sYO
Bg6JZvdpDeLU/AVevCO7R7eHMeTLa5DLOuRpseIANzgf2g5wqSDJOunnJnl7OmgQpChcSq
g0rt2DqmfwjpuXYpCLONjw4JH9btd73g1D7bK085HfByOEeYDWwk6v7WeAzufiwPJJglm/
wDM4hY/15RCjDUbIx4tLwTe1pVn2CQ9cdt8TuyVmlI6yRfYSuYG/UNmp7KAp/Q1qrbb573
URl0m8dsN2j/ZneNG2RvdCLBTGiStSuLEelJnWEOdnIYWYs3Z95hSRZpEmBGwmNhpWV/+p
CzTit6yl7wAAA7j/gvke/4L5HgAAAAdzc2gtcnNhAAABAQDqyZy6n10ANLsqh7xRyowl9T
DgYPJp2QFIB8xhxrFxZx+nM/rixg4GDolm92kN4tT8BV68I7tHt4cx5MtrkMs65Gmx4gA3
OB/aDnCpIMk66ecmeXs6aBCkKFxKqDSu3YOqZ/COm5dikIs42PDgkf1u13veDUPtsrTzkd
8HI4R5gNbCTq/tZ4DO5+LA8kmCWb/AMziFj/XlEKMNRsjHi0vBN7WlWfYJD1x23xO7JWaU
jrJF9hK5gb9Q2ansoCn9DWqttvnvdRGXSbx2w3aP9md40bZG90IsFMaJK1K4sR6UmdYQ52
chhZizdn3mFJFmkSYEbCY2GlZX/6kLNOK3rKXvAAAAAwEAAQAAAQEAlEt3dOCHa0PjG//T
0I1wa+EsV+yj8HsiNg7G5tMK7KfH9GH+ixGxdd3kp2aJsj2dbPkvVwHynl+rQrZSTcSMuM
vgfnxSyo3mgGIygoC02UM3vrNoRAHK0QS1Fmqbq/851H4GEOzxd034wZU0v4M2YbY1BJG4
YABKDrYJ+oZSfDLQstRFYRKyMdA7JU+CvFbNjZFQwBj7KZ6SDzNuZLpm6v8relCEiAKI9e
MSWsOqcaKyOuhN9bo7eTTIwCZrLzIUSR0AHr5Sw3Wj967vVt3X/geNFVNMsbBxf8XnUq+4
CQENNuZFzMEeCqF/AvTrGYr9WNgjgPpQwOeglcq54Ako0QAAAIEAhi7ELdZ6mzbFPKVPDB
t4whwOg+SEow2qCCIABYk4oqAsUT+z9wGT+QxDe3mkfLdxIl36B8DmwetzXFunphWf+u3/
gmqr+PXjk8NIdTHymVcdewbUaP3VTTwA5k6n4Md8nsiBP6g9PsxwpNnXJUlDGxwxSc/+SN
7rAfBzFDzc7HUAAACBAPl3UBNdIkibvVZvS9j13S284k/LnPZShLRxG2/TWB/k8+r/HpuW
cO5RcikfTxiw4UTiDfXF8p+T+ko9LJy/cMGJJZq+AvPO4u+9mSW+GhJzuSbrL77J+Zze31
fhNvNS3AmJS2wIrw5+pmObtBHo5xfaphr18+5sK4yoZuwD7EqTAAAAgQDw7+EC660pCEr2
EFm2/aclGiTMqjSIl4VEZwXr67KPrnZKgqJRzG9/GfWRKFX4EMrUQLnijrOXsfx9k0eyEL
3y4QzbEdci9FI4qKwClmIrzrASmZWnZT2j2CPJUBSQ5noECrX9CFg2AAZt5ttbveVI4SvN
E874t4SYF2IL9avktQAAAAAB
-----END OPENSSH PRIVATE KEY-----
";
    /// An ed25519 key with the passphrase `correct horse battery staple`.
    pub(crate) const PASSPHRASE_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAACmFlczI1Ni1jdHIAAAAGYmNyeXB0AAAAGAAAABBbt00mIv
aBxzUvRsrbX0E6AAAAGAAAAAEAAAAzAAAAC3NzaC1lZDI1NTE5AAAAIENOvlpuGUtF/VWe
3ixG94ybzOsXvz7652J3V/iHsIQWAAAAkOytbcb2sxfjKZUSdf3OIiWMTjmLkdkSyAxMCu
NZZvvWzCq6Y8bzBbmq2D7XidJGZQ8aqVD7Sv+dV4+BZyIZmxki7xCruI5lGJA0DgqWlt9V
+D+ZOfsIs/3JtS68VOqQe7+r70XWLN+hB6+RqYMVjHwnk+xMbvggT4GeAZFNhPolT4lvwJ
Qjm9zYDGIBlKeGwA==
-----END OPENSSH PRIVATE KEY-----
";

    #[derive(Clone)]
    pub(crate) struct ServerBehaviour {
        pub(crate) advertisement: Vec<u8>,
        pub(crate) expected_command: String,
        pub(crate) exit_status: u32,
        pub(crate) stderr: Option<&'static str>,
        pub(crate) authorized_pub: &'static str,
        /// The host keys the server can present.
        pub(crate) host_keys: Vec<&'static str>,
        /// The connections the server accepted.
        pub(crate) connections: Arc<AtomicUsize>,
    }

    impl Default for ServerBehaviour {
        fn default() -> Self {
            Self {
                advertisement: fixtures::ssh_adv(),
                expected_command: "git-upload-pack '/acme/pipelines.git'".to_string(),
                exit_status: 0,
                stderr: None,
                authorized_pub: CLIENT_PUB,
                host_keys: vec![HOST_KEY],
                connections: Arc::default(),
            }
        }
    }

    impl ServerBehaviour {
        pub(crate) fn connections(&self) -> usize {
            self.connections.load(Ordering::SeqCst)
        }
    }

    struct TestGitServer {
        behaviour: ServerBehaviour,
    }

    impl server::Server for TestGitServer {
        type Handler = TestSession;
        fn new_client(&mut self, _peer: Option<std::net::SocketAddr>) -> TestSession {
            self.behaviour.connections.fetch_add(1, Ordering::SeqCst);
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
            keys: behaviour
                .host_keys
                .iter()
                .map(|key| decode_secret_key(key, None).unwrap())
                .collect(),
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

    /// A known_hosts pinning `pub_for_host` for the test server's port.
    fn known_hosts_pinning(port: u16, pub_for_host: &str) -> String {
        format!("[127.0.0.1]:{port} {pub_for_host}\n")
    }

    /// The client key with `known_hosts`.
    fn client_auth(known_hosts: &str) -> SshAuth<'_> {
        SshAuth {
            private_key: CLIENT_KEY,
            known_hosts,
        }
    }

    const TIMEOUT: Duration = Duration::from_secs(10);

    const ROGUE_ECDSA_PUB: &str = "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBBQOOCWBjec6lOoe6rlMytTsM3nU6dGU8eeIALBjDBrQ1yLhpkwqO/2WW2DdlW5sponZMlcIS3ExCBicbnQddkA=";
    /// From Python `cryptography`: OpenSSH 10 no longer makes DSA keys.
    const DSA_PUB: &str = "ssh-dss AAAAB3NzaC1kc3MAAACBAJgs9kIE3deKCau/lBFHIX3ZNYbNjoCnmz7OJW3IHNa1Il7ghlKac5+7Dn2kzcYr+vEWNJEq7VkwzujBxlFFDtlKvI9l7P2f/NRgX/rdwxdmmOCKmeMmMDVvrEmQRj34qc++L0nDd+JDLEUPuSFRld0+R+rYOZAiqPV1jyUztpBLAAAAFQC7w2w7c+NTV90M/aFbivHpWoqwJQAAAIAKn5PsatJgiLf0hHc7tR0w4rvCnoY1ISiXCd+Dpw8MosoI+3/ypMyeQZoA14iTGOKXXP6eYUyUt79dQ1hBh8KkbwxH7jcbQ6gNnPho0oqdgBz1QENcqqeOXmheKFWPmfJPd7oS4Kcq1StcbjFN9cn7tzUvbZaERk4cnD47PQw73QAAAIEAkUWwehPM1TbfdZW6Wkc/Zzu+VzOepGk+9PIOZxdkg1xi7WHUDh4XhSknnmA1mDCcNT7uijXs4rWoxruYr8BVO8MZlMd/EHCnpwxtG7WVZjYYJOlkVjSbSNVpDO/GfJzpwiwuxL8JNbWft6DYFUBVgKNp+8+pBCR4oKTVNxTI6U0=";

    /// A server with an Ed25519, an ECDSA and an RSA host key, like most
    /// forges.
    fn forge() -> ServerBehaviour {
        ServerBehaviour {
            host_keys: vec![HOST_KEY, ECDSA_HOST_KEY, RSA_HOST_KEY],
            ..Default::default()
        }
    }

    /// Spawn `server` and resolve `main` with a `known_hosts` that pins only
    /// `pinned` for it. Returns the server's port and the commit.
    async fn resolve_main_pinning(
        server: &ServerBehaviour,
        pinned: &str,
    ) -> (u16, Result<String, GitError>) {
        let port = spawn_server(server.clone()).await;
        let known_hosts = known_hosts_pinning(port, pinned);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();
        let commit = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
            .await
            .map(|bytes| {
                let adv = parse_advertisement(&bytes).unwrap();
                commit_for_ref(&adv, &fixtures::branch("main"))
                    .unwrap()
                    .commit
            });
        (port, commit)
    }

    #[tokio::test]
    async fn a_forge_pinned_by_its_ecdsa_key_alone_resolves() {
        let server = forge();
        let (_, commit) = resolve_main_pinning(&server, ECDSA_HOST_PUB).await;
        assert_eq!(commit.unwrap(), fixtures::oid('a'));
        assert_eq!(server.connections(), 1);
    }

    #[tokio::test]
    async fn a_forge_pinned_by_its_rsa_key_alone_resolves() {
        let server = forge();
        let (_, commit) = resolve_main_pinning(&server, RSA_HOST_PUB).await;
        assert_eq!(commit.unwrap(), fixtures::oid('a'));
        assert_eq!(server.connections(), 1);
    }

    #[tokio::test]
    async fn a_forge_whose_key_of_the_pinned_type_differs_is_rejected() {
        let server = forge();
        let (port, commit) = resolve_main_pinning(&server, ROGUE_ECDSA_PUB).await;
        assert_eq!(
            commit.unwrap_err().to_string(),
            format!(
                "host key rejected: HOST KEY CHANGED for '127.0.0.1:{port}' (known_hosts line 1) \
                 — possible man-in-the-middle; refusing. Update the Secret only after out-of-band \
                 verification"
            )
        );
        assert_eq!(server.connections(), 1);
    }

    #[tokio::test]
    async fn a_host_without_a_key_of_the_pinned_type_is_rejected() {
        let server = ServerBehaviour::default();
        let (_, commit) = resolve_main_pinning(&server, ECDSA_HOST_PUB).await;
        let err = commit.unwrap_err();
        assert!(matches!(err, GitError::HostKeyRejected(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            "host key rejected: the host offers ssh-ed25519 and the git Secret's `known_hosts` \
             allows only ecdsa-sha2-nistp256 for it — add one of the host's offered keys to \
             `known_hosts`"
        );
        assert_eq!(server.connections(), 1);
    }

    #[tokio::test]
    async fn a_pin_of_a_key_type_the_operator_cannot_check_fails_before_connecting() {
        let server = forge();
        let (port, commit) = resolve_main_pinning(&server, DSA_PUB).await;
        let err = commit.unwrap_err();
        assert!(matches!(err, GitError::KnownHostsUnavailable(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            format!(
                "known_hosts unavailable: the git Secret's `known_hosts` has only ssh-dss keys \
                 for '127.0.0.1:{port}'; the operator accepts ssh-ed25519, ecdsa-sha2-nistp256, \
                 ecdsa-sha2-nistp384, ecdsa-sha2-nistp521, ssh-rsa host keys — add the host's \
                 key of one of these types"
            )
        );
        assert_eq!(server.connections(), 0);
    }

    #[tokio::test]
    async fn end_to_end_resolves_branch_over_ssh() {
        let port = spawn_server(ServerBehaviour::default()).await;
        let known_hosts = known_hosts_pinning(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let bytes = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
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
        let known_hosts = known_hosts_pinning(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
            .await
            .unwrap_err();
        assert!(matches!(err, GitError::AuthFailed(_)), "{err}");
    }

    #[tokio::test]
    async fn changed_host_key_is_rejected_before_auth() {
        let port = spawn_server(ServerBehaviour::default()).await;
        // known_hosts pins the *rogue* key for this host — the server
        // presents HOST_PUB, i.e. the MITM shape.
        let known_hosts = known_hosts_pinning(port, ROGUE_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::HostKeyRejected(ref m) if m.contains("CHANGED")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn undecodable_private_key_is_auth_failed_before_connecting() {
        let server = ServerBehaviour::default();
        let port = spawn_server(server.clone()).await;
        let known_hosts = known_hosts_pinning(port, HOST_PUB);
        let auth = SshAuth {
            private_key: "not a private key\n",
            known_hosts: &known_hosts,
        };
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &auth, TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "git authentication failed: cannot read the SSH private key: Could not read key — \
             check the git Secret's `identity` entry"
        );
        assert_eq!(server.connections(), 0);
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
        let known_hosts = known_hosts_pinning(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();

        let err = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
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
        let known_hosts = known_hosts_pinning(1, HOST_PUB);
        let target = parse_ssh_url("ssh://git@127.0.0.1:1/r.git").unwrap();
        let err = fetch_advertisement(&target, &client_auth(&known_hosts), Duration::from_secs(3))
            .await
            .unwrap_err();
        assert!(matches!(err, GitError::Unreachable(_)), "{err}");
    }

    #[test]
    fn parse_ssh_url_shapes() {
        let t = parse_ssh_url("ssh://deploy@gitea.internal:2222/r.git").unwrap();
        assert_eq!(
            t,
            SshTarget {
                user: "deploy".to_string(),
                host: "gitea.internal".to_string(),
                port: 2222,
                repo_path: "/r.git".to_string(),
            }
        );
        let t = parse_ssh_url("ssh://git@forge.example/org/repo.git").unwrap();
        assert_eq!(t.port, 22);

        assert!(parse_ssh_url("https://forge.example/org/repo.git").is_err());
        assert!(parse_ssh_url("git@forge.example:org/repo.git").is_err());
        assert!(parse_ssh_url("ssh://git@forge.example").is_err());
    }

    #[test]
    fn an_ssh_url_without_a_user_is_an_auth_failure() {
        // The pod's ssh would log in as its own account, not as the user
        // the operator checked the key with.
        for (url, fixed) in [
            (
                "ssh://github.com/acme/r.git",
                "ssh://git@github.com/acme/r.git",
            ),
            (
                "ssh://gitea.internal:2222/r.git",
                "ssh://git@gitea.internal:2222/r.git",
            ),
        ] {
            let err = parse_ssh_url(url).unwrap_err();
            assert!(matches!(err, GitError::AuthFailed(_)), "{err:?}");
            assert_eq!(
                err.to_string(),
                format!(
                    "git authentication failed: the ssh url must name the user — use {fixed}, \
                     or the user your git host expects"
                )
            );
        }
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
