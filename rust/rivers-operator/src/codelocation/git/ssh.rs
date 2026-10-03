//! SSH transport arm: in-process `git-upload-pack` over russh.
//!
//! ```text
//! decode key + read known_hosts → connect → publickey auth → open session
//!         → exec "git-upload-pack '<path>'" → buffer stdout up to the
//!           advertisement's flush-pkt → send a flush-pkt + EOF → parse
//! ```
//!
//! One deadline, the operator's git timeout, covers every step from
//! `connect` to upload-pack's exit.
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

use gix_packetline::PacketLineRef;
use gix_packetline::decode::{Stream, streaming};
use russh::ChannelMsg;
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, decode_secret_key};

use super::http::DEFAULT_MAX_ADVERTISEMENT_BYTES;
use super::{GitError, known_hosts};

/// How long upload-pack gets to exit once it has the client's flush-pkt,
/// if the fetch's timeout leaves that long.
const EXIT_GRACE: Duration = Duration::from_secs(2);

/// How much of upload-pack's stderr a failure keeps: the end, where git
/// prints its `fatal:` line.
const MAX_STDERR_BYTES: usize = 4096;

/// `ssh://user@host[:port]/path`, decomposed. Only the `ssh://` URL form
/// is accepted — scp-like `git@host:path` strings are rejected at admission
/// and again here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshTarget {
    pub user: String,
    /// An IPv6 address is in its short form, without brackets: OpenSSH
    /// names it so in `known_hosts`, however the url writes it.
    pub host: String,
    pub port: u16,
    /// The path as git sends it to `git-upload-pack`; see
    /// [`upload_pack_path`].
    pub repo_path: String,
}

/// `raw` as git's ssh transport reads it (connect.c `parse_connect_url`):
/// git percent-decodes the url, the host part runs to the first `/`, and
/// ssh takes the user from it up to the last `@`. A url that `url` reads
/// with another host is refused, so the host the webhook allows is the one
/// the pods connect to. A url without a user is an auth failure, not a
/// default: the pods' ssh would log in as their own account, not as the
/// user resolved with here.
pub fn parse_ssh_url(raw: &str) -> Result<SshTarget, GitError> {
    let invalid = |problem: &str| GitError::InvalidUrl(format!("the ssh url {problem}"));
    let url = super::parse_url(raw)?;
    if url.scheme() != "ssh" {
        return Err(GitError::InvalidUrl(
            "not an ssh:// url (scp-style 'git@host:path' is not supported)".to_string(),
        ));
    }
    let after_scheme = raw.split_once("://").map_or("", |(_, rest)| rest);
    let (authority, path_text) =
        after_scheme.split_at(after_scheme.find('/').unwrap_or(after_scheme.len()));
    if authority.contains(['?', '#']) {
        return Err(invalid(
            "has '?' or '#' before its path, which git reads as part of the host",
        ));
    }
    if git_url_decode(authority).contains(&b'/') {
        return Err(invalid(
            "has an escaped '/' before its path, where git ends the host",
        ));
    }
    let host = match url.host() {
        Some(url::Host::Ipv6(address)) => address.to_string(),
        Some(host) => host.to_string(),
        None => return Err(invalid("has no host")),
    };
    if host.contains('%') {
        return Err(invalid(
            "has a '%' escape in its host — write the host as is",
        ));
    }
    if url.path().is_empty() || url.path() == "/" {
        return Err(invalid("has no repository path"));
    }
    if url.username().is_empty() {
        return Err(GitError::AuthFailed(format!(
            "the ssh url must name the user — use {}, or the user your git host expects",
            with_git_user(&url)
        )));
    }
    let user = String::from_utf8(git_url_decode(url.username()))
        .map_err(|_| invalid("has a user that is not UTF-8 once percent-decoded"))?;
    Ok(SshTarget {
        user,
        host,
        port: url.port().unwrap_or(22),
        repo_path: upload_pack_path(path_text)?,
    })
}

/// The path git's ssh transport asks `git-upload-pack` for (connect.c
/// `parse_connect_url`): `text`, the url from the first `/` after the host,
/// percent-decoded, without the `/` before a `~` — `~user/…` and `~/…` are
/// relative to a home directory on the host.
fn upload_pack_path(text: &str) -> Result<String, GitError> {
    let path = String::from_utf8(git_url_decode(text)).map_err(|_| {
        GitError::InvalidUrl(format!(
            "the repository path '{text}' is not UTF-8 once percent-decoded"
        ))
    })?;
    Ok(match path.strip_prefix("/~") {
        Some(home) => format!("~{home}"),
        None => path,
    })
}

/// git's `url_decode` (url.c): `%xx` becomes its byte, except `%00` and a
/// `%` without two hex digits, which stay as written.
fn git_url_decode(text: &str) -> Vec<u8> {
    let hex = |b: &u8| char::from(*b).to_digit(16);
    let mut decoded = Vec::with_capacity(text.len());
    let mut rest = text.as_bytes();
    while let [first, tail @ ..] = rest {
        let escape = match rest {
            [b'%', hi, lo, after @ ..] => hex(hi)
                .zip(hex(lo))
                .map(|(hi, lo)| ((hi << 4) | lo) as u8)
                .filter(|&byte| byte != 0)
                .map(|byte| (byte, after)),
            _ => None,
        };
        let (byte, after) = escape.unwrap_or((*first, tail));
        decoded.push(byte);
        rest = after;
    }
    decoded
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

/// Fetch the ref advertisement over SSH, fully buffered. `timeout` bounds
/// the whole fetch: a host that does not finish in time is
/// [`GitError::Unreachable`], with the stage it did not finish.
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

    // No `inactivity_timeout`: a host that keeps sending resets it, and when
    // it fires, the read ends as if upload-pack had closed (a terminal
    // error); the deadline below is the one time limit.
    let config = Arc::new(client::Config {
        preferred: russh::Preferred {
            key: host_keys.algorithms()?.into(),
            ..russh::Preferred::DEFAULT
        },
        ..Default::default()
    });
    let handler = HostKeyCheck(host_keys);

    let deadline = tokio::time::Instant::now() + timeout;
    let mut stage = Stage::Connect;
    let mut connection = None;
    let started = tokio::time::timeout_at(deadline, async {
        let (stream, socket) = connect(target).await?;
        connection = Some(socket);
        stage = Stage::Handshake;
        // `connect_stream` returns `H::Error`, so a host-key rejection from
        // the handler surfaces as our own GitError; russh's own errors
        // arrive via `From<russh::Error>`.
        let mut handle = client::connect_stream(config, stream, handler).await?;

        stage = Stage::Login;
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

        stage = Stage::Exec;
        let mut channel = handle.channel_open_session().await?;
        let command = format!("git-upload-pack {}", shell_quote_single(&target.repo_path));
        channel.exec(true, command).await?;

        stage = Stage::Refs;
        let output = Output::read(&mut channel, cap).await?;
        Ok::<_, GitError>((handle, channel, output))
    })
    .await;
    let Ok(started) = started else {
        // russh's session task can outlive the dropped future: mid-handshake
        // it does not see the dropped handle, and a host that keeps sending
        // keeps it reading. Shutting the connection down ends it.
        if let Some(socket) = connection {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
        return Err(stage.timed_out(target, timeout));
    };
    let (handle, mut channel, output) = started?;

    if output.advertised {
        let grace = deadline.min(tokio::time::Instant::now() + EXIT_GRACE);
        let _ = tokio::time::timeout_at(grace, want_nothing(&mut channel)).await;
    }
    let _ = handle
        .disconnect(russh::Disconnect::ByApplication, "", "en")
        .await;
    output.into_advertisement(target)
}

/// Opens the TCP connection, as two handles on it: russh's session task
/// takes the first, and a fetch that runs out of time shuts the
/// connection down through the second.
async fn connect(
    target: &SshTarget,
) -> Result<(tokio::net::TcpStream, std::net::TcpStream), GitError> {
    let unreachable = |e: std::io::Error| GitError::Unreachable(e.to_string());
    let stream = tokio::net::TcpStream::connect((target.host.as_str(), target.port))
        .await
        .and_then(tokio::net::TcpStream::into_std)
        .map_err(unreachable)?;
    let socket = stream.try_clone().map_err(unreachable)?;
    let stream = tokio::net::TcpStream::from_std(stream).map_err(unreachable)?;
    Ok((stream, socket))
}

/// Where a fetch was when it ran out of time.
#[derive(Clone, Copy)]
enum Stage {
    Connect,
    Handshake,
    Login,
    Exec,
    Refs,
}

impl Stage {
    fn timed_out(self, target: &SshTarget, timeout: Duration) -> GitError {
        let host = format!("'{}'", known_hosts::address(&target.host, target.port));
        let stage = match self {
            Stage::Connect => format!("connecting to {host}"),
            Stage::Handshake => format!("the SSH handshake with {host}"),
            Stage::Login => format!("logging in to {host}"),
            Stage::Exec => format!("starting git-upload-pack on {host}"),
            Stage::Refs => format!("reading the refs from {host}"),
        };
        GitError::Unreachable(format!(
            "{stage} did not finish within {}s",
            timeout.as_secs()
        ))
    }
}

/// What upload-pack wrote until its advertisement's flush-pkt, or until it
/// closed the channel.
struct Output {
    stdout: Vec<u8>,
    /// The last [`MAX_STDERR_BYTES`] of stderr; `stderr_cut` if more came.
    stderr: Vec<u8>,
    stderr_cut: bool,
    exit_status: Option<u32>,
    advertised: bool,
}

impl Output {
    async fn read(channel: &mut russh::Channel<client::Msg>, cap: usize) -> Result<Self, GitError> {
        let mut output = Output {
            stdout: Vec::new(),
            stderr: Vec::new(),
            stderr_cut: false,
            exit_status: None,
            advertised: false,
        };
        let mut scanned = 0;
        while let Some(msg) = channel.wait().await {
            match msg {
                ChannelMsg::Data { data } => {
                    output.stdout.extend_from_slice(&data);
                    if output.stdout.len() > cap {
                        return Err(GitError::Malformed(format!(
                            "advertisement exceeds {cap} bytes"
                        )));
                    }
                    output.advertised = reaches_flush(&output.stdout, &mut scanned)?;
                    if output.advertised {
                        break;
                    }
                }
                // ext 1 == SSH_EXTENDED_DATA_STDERR — upload-pack's error text.
                ChannelMsg::ExtendedData { data, ext: 1 } => output.keep_stderr(&data),
                ChannelMsg::ExitStatus { exit_status } => output.exit_status = Some(exit_status),
                _ => {}
            }
        }
        Ok(output)
    }

    fn keep_stderr(&mut self, data: &[u8]) {
        self.stderr.extend_from_slice(data);
        let excess = self.stderr.len().saturating_sub(MAX_STDERR_BYTES);
        if excess > 0 {
            self.stderr.drain(..excess);
            self.stderr_cut = true;
        }
    }

    fn into_advertisement(self, target: &SshTarget) -> Result<Vec<u8>, GitError> {
        match self.exit_status {
            // As for `git ls-remote`: once the refs are in, how upload-pack
            // exits does not matter.
            _ if self.advertised => Ok(self.stdout),
            Some(0) => Ok(self.stdout),
            // The connection or the channel ended mid-read.
            None => {
                let stderr = self.stderr_text();
                Err(GitError::Unreachable(format!(
                    "the connection to '{}' closed before all refs arrived{}",
                    known_hosts::address(&target.host, target.port),
                    if stderr.is_empty() {
                        String::new()
                    } else {
                        format!(": {stderr}")
                    }
                )))
            }
            Some(status) => Err(GitError::RefNotFound(format!(
                "git-upload-pack failed (exit {status}) for '{}': {}",
                target.repo_path,
                self.stderr_text()
            ))),
        }
    }

    fn stderr_text(&self) -> String {
        let stderr = String::from_utf8_lossy(&self.stderr);
        let stderr = stderr.trim();
        if self.stderr_cut {
            format!("…{stderr}")
        } else {
            stderr.to_string()
        }
    }
}

/// Advances `scanned` past the whole pkt-lines in `stdout`; true once it
/// passes a flush-pkt — over ssh, the end of the advertisement.
fn reaches_flush(stdout: &[u8], scanned: &mut usize) -> Result<bool, GitError> {
    loop {
        match streaming(&stdout[*scanned..]) {
            Ok(Stream::Complete {
                line,
                bytes_consumed,
            }) => {
                *scanned += bytes_consumed;
                if matches!(line, PacketLineRef::Flush) {
                    return Ok(true);
                }
            }
            Ok(Stream::Incomplete { .. }) => return Ok(false),
            Err(e) => return Err(GitError::Malformed(e.to_string())),
        }
    }
}

/// After the advertisement upload-pack waits for the client's wants. A
/// flush-pkt in their place asks for nothing and it exits 0; an EOF alone
/// makes it exit 128. Returns once it has exited.
async fn want_nothing(channel: &mut russh::Channel<client::Msg>) {
    if channel.data_bytes(&b"0000"[..]).await.is_err() || channel.eof().await.is_err() {
        return;
    }
    while let Some(msg) = channel.wait().await {
        if matches!(msg, ChannelMsg::ExitStatus { .. }) {
            return;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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

    /// What the server does once the advertisement is out.
    #[derive(Clone, Copy)]
    pub(crate) enum AfterAdvertisement {
        /// Waits for the client's request, as git-upload-pack does: a
        /// flush-pkt ends it with exit 0, an EOF before one with exit 128.
        AwaitRequest,
        /// Exits with this status and closes the channel at once.
        Exit(u32),
        /// Never answers the request: no exit, no close.
        Stall,
    }

    #[derive(Clone)]
    pub(crate) struct ServerBehaviour {
        pub(crate) advertisement: Vec<u8>,
        pub(crate) expected_command: String,
        pub(crate) after_advertisement: AfterAdvertisement,
        pub(crate) stderr: Option<String>,
        pub(crate) authorized_pub: &'static str,
        /// The host keys the server can present.
        pub(crate) host_keys: Vec<&'static str>,
        /// How often the server sends an SSH keepalive while the client is
        /// silent.
        pub(crate) keepalive: Option<Duration>,
        /// Drops the TCP connection, with no SSH goodbye, once neither side
        /// has sent anything for this long.
        pub(crate) drop_when_idle: Option<Duration>,
        /// The connections the server accepted.
        pub(crate) connections: Arc<AtomicUsize>,
        /// The connections that ended.
        pub(crate) closed: Arc<tokio::sync::watch::Sender<usize>>,
        /// The commands clients asked to exec, in order.
        pub(crate) commands: Arc<Mutex<Vec<String>>>,
        /// What clients wrote to upload-pack's stdin.
        pub(crate) received: Arc<Mutex<Vec<u8>>>,
    }

    impl Default for ServerBehaviour {
        fn default() -> Self {
            Self {
                advertisement: fixtures::ssh_adv(),
                expected_command: "git-upload-pack '/acme/pipelines.git'".to_string(),
                after_advertisement: AfterAdvertisement::AwaitRequest,
                stderr: None,
                authorized_pub: CLIENT_PUB,
                host_keys: vec![HOST_KEY],
                keepalive: None,
                drop_when_idle: None,
                connections: Arc::default(),
                closed: Arc::new(tokio::sync::watch::channel(0).0),
                commands: Arc::default(),
                received: Arc::default(),
            }
        }
    }

    impl ServerBehaviour {
        pub(crate) fn connections(&self) -> usize {
            self.connections.load(Ordering::SeqCst)
        }

        /// Whether every connection the server accepted ends within
        /// `within`.
        pub(crate) async fn all_closed_within(&self, within: Duration) -> bool {
            let accepted = self.connections();
            let mut closed = self.closed.subscribe();
            let all_closed = closed.wait_for(|&closed| closed == accepted);
            matches!(tokio::time::timeout(within, all_closed).await, Ok(Ok(_)))
        }

        pub(crate) fn commands(&self) -> Vec<String> {
            self.commands.lock().unwrap().clone()
        }

        pub(crate) fn received(&self) -> Vec<u8> {
            self.received.lock().unwrap().clone()
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
                request: Vec::new(),
                exited: false,
            }
        }
    }

    struct TestSession {
        behaviour: ServerBehaviour,
        /// What this session's client wrote to upload-pack's stdin.
        request: Vec<u8>,
        exited: bool,
    }

    /// russh drops the handler when the connection ends.
    impl Drop for TestSession {
        fn drop(&mut self) {
            self.behaviour.closed.send_modify(|closed| *closed += 1);
        }
    }

    impl TestSession {
        fn answers_requests(&self) -> bool {
            !self.exited
                && matches!(
                    self.behaviour.after_advertisement,
                    AfterAdvertisement::AwaitRequest
                )
        }

        fn exit(
            &mut self,
            channel: ChannelId,
            status: u32,
            session: &mut Session,
        ) -> Result<(), russh::Error> {
            self.exited = true;
            session.exit_status_request(channel, status)?;
            session.eof(channel)?;
            session.close(channel)
        }
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
            self.behaviour
                .commands
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(data).into_owned());
            // Exact byte match — this is also what proves the client's
            // shell-quoting reached the wire literally.
            if data != self.behaviour.expected_command.as_bytes() {
                session.channel_failure(channel)?;
                session.extended_data(
                    channel,
                    1,
                    bytes::Bytes::from(format!(
                        "unexpected command: {}",
                        String::from_utf8_lossy(data)
                    )),
                )?;
                return self.exit(channel, 127, session);
            }
            session.channel_success(channel)?;
            session.data(
                channel,
                bytes::Bytes::from(self.behaviour.advertisement.clone()),
            )?;
            if let Some(err) = &self.behaviour.stderr {
                session.extended_data(channel, 1, bytes::Bytes::from(err.clone()))?;
            }
            match self.behaviour.after_advertisement {
                AfterAdvertisement::AwaitRequest | AfterAdvertisement::Stall => Ok(()),
                AfterAdvertisement::Exit(status) => self.exit(channel, status, session),
            }
        }

        async fn data(
            &mut self,
            channel: ChannelId,
            data: &[u8],
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            self.behaviour
                .received
                .lock()
                .unwrap()
                .extend_from_slice(data);
            self.request.extend_from_slice(data);
            if self.answers_requests() && self.request.starts_with(b"0000") {
                return self.exit(channel, 0, session);
            }
            Ok(())
        }

        async fn channel_eof(
            &mut self,
            channel: ChannelId,
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            if !self.answers_requests() {
                return Ok(());
            }
            session.extended_data(
                channel,
                1,
                bytes::Bytes::from_static(b"fatal: the remote end hung up unexpectedly\n"),
            )?;
            self.exit(channel, 128, session)
        }
    }

    /// Boot a russh server on an ephemeral port; returns its port.
    pub(crate) async fn spawn_server(behaviour: ServerBehaviour) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        serve(listener, behaviour)
    }

    /// Run a russh server on `listener`; returns its port.
    pub(crate) fn serve(listener: tokio::net::TcpListener, behaviour: ServerBehaviour) -> u16 {
        let port = listener.local_addr().unwrap().port();
        let defaults = server::Config::default();
        let config = Arc::new(server::Config {
            keys: behaviour
                .host_keys
                .iter()
                .map(|key| decode_secret_key(key, None).unwrap())
                .collect(),
            keepalive_interval: behaviour.keepalive,
            inactivity_timeout: behaviour.drop_when_idle.or(defaults.inactivity_timeout),
            ..defaults
        });
        tokio::spawn(async move {
            let mut srv = TestGitServer { behaviour };
            let _ = srv.run_on_socket(config, &listener).await;
        });
        port
    }

    /// A host that takes TCP connections but is no working SSH server.
    #[derive(Clone, Copy)]
    pub(crate) enum StalledHost {
        /// Never writes: an L4 load balancer without a live backend.
        Silent,
        /// Sends an SSH banner, then an SSH_MSG_IGNORE packet every 200ms,
        /// and never starts the key exchange.
        Ignoring,
    }

    /// Serves one connection as `host` on an ephemeral port; returns the
    /// port, and what the client sent until it closed the connection.
    pub(crate) async fn spawn_stalled_host(
        host: StalledHost,
    ) -> (u16, tokio::task::JoinHandle<Vec<u8>>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        // Unencrypted: packet length 12, padding length 6, the payload
        // (message 2 and an empty string), 6 bytes of padding.
        const IGNORE: [u8; 16] = [0, 0, 0, 12, 6, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let sent = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.split();
            let stall = async {
                if let StalledHost::Ignoring = host {
                    writer.write_all(b"SSH-2.0-OpenSSH_9.6\r\n").await?;
                    loop {
                        writer.write_all(&IGNORE).await?;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
                std::future::pending::<std::io::Result<()>>().await
            };
            let mut sent = Vec::new();
            tokio::select! {
                _ = reader.read_to_end(&mut sent) => {}
                _ = stall => {}
            }
            sent
        });
        (port, sent)
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

    /// The operator's default `RIVERS_GIT_TIMEOUT_SECONDS`.
    const OPERATOR_TIMEOUT: Duration = Duration::from_secs(30);

    /// Spawn `server` and resolve `main` with a `known_hosts` that pins only
    /// `pinned` for it, with the operator's default timeout. Returns the
    /// server's port and the commit.
    async fn resolve_main_pinning(
        server: &ServerBehaviour,
        pinned: &str,
    ) -> (u16, Result<String, GitError>) {
        resolve_main(server, pinned, OPERATOR_TIMEOUT).await
    }

    /// [`resolve_main_pinning`] with `timeout`.
    async fn resolve_main(
        server: &ServerBehaviour,
        pinned: &str,
        timeout: Duration,
    ) -> (u16, Result<String, GitError>) {
        let port = spawn_server(server.clone()).await;
        (port, resolve_main_at(port, pinned, timeout).await)
    }

    /// Resolve `main` on the host at `port` with a `known_hosts` that pins
    /// `pinned` for it; the resolve must end within 5s.
    async fn resolve_main_at(
        port: u16,
        pinned: &str,
        timeout: Duration,
    ) -> Result<String, GitError> {
        let known_hosts = known_hosts_pinning(port, pinned);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/acme/pipelines.git")).unwrap();
        let auth = client_auth(&known_hosts);
        let fetch = fetch_advertisement(&target, &auth, timeout);
        tokio::time::timeout(Duration::from_secs(5), fetch)
            .await
            .expect("the resolve did not end within 5s")
            .map(|bytes| {
                let adv = parse_advertisement(&bytes).unwrap();
                commit_for_ref(&adv, &fixtures::branch("main"))
                    .unwrap()
                    .commit
            })
    }

    /// The timeout of the tests that run into it.
    const SHORT_TIMEOUT: Duration = Duration::from_secs(1);

    /// `resolve`'s outcome, and how long it took.
    async fn timed<T>(resolve: impl std::future::Future<Output = T>) -> (T, Duration) {
        let started = tokio::time::Instant::now();
        let outcome = resolve.await;
        (outcome, started.elapsed())
    }

    /// The line the client opens each connection with.
    fn client_banner() -> String {
        let russh::SshId::Standard(id) = client::Config::default().client_id else {
            unreachable!("russh's own id is a standard one")
        };
        format!("{id}\r\n")
    }

    #[tokio::test]
    async fn a_host_that_never_answers_is_unreachable_once_the_timeout_is_up() {
        let (port, sent) = spawn_stalled_host(StalledHost::Silent).await;

        let (outcome, took) = timed(resolve_main_at(port, HOST_PUB, SHORT_TIMEOUT)).await;

        assert_eq!(
            outcome.unwrap_err().to_string(),
            format!(
                "git host unreachable: the SSH handshake with '127.0.0.1:{port}' did not finish \
                 within 1s"
            )
        );
        assert!(took < 2 * SHORT_TIMEOUT, "{took:?}");
        // The host got the client's banner, then the connection closed.
        let sent = tokio::time::timeout(SHORT_TIMEOUT, sent)
            .await
            .expect("the connection is still open")
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&sent), client_banner());
    }

    #[tokio::test]
    async fn a_handshake_kept_busy_ends_with_its_resolve() {
        // russh's session task keeps reading a host that keeps sending.
        let (port, sent) = spawn_stalled_host(StalledHost::Ignoring).await;

        let (outcome, took) = timed(resolve_main_at(port, HOST_PUB, SHORT_TIMEOUT)).await;

        assert_eq!(
            outcome.unwrap_err().to_string(),
            format!(
                "git host unreachable: the SSH handshake with '127.0.0.1:{port}' did not finish \
                 within 1s"
            )
        );
        assert!(took < 2 * SHORT_TIMEOUT, "{took:?}");
        let sent = tokio::time::timeout(SHORT_TIMEOUT, sent)
            .await
            .expect("the connection is still open")
            .unwrap();
        assert!(sent.starts_with(client_banner().as_bytes()), "{sent:?}");
    }

    #[tokio::test]
    async fn keepalives_do_not_stretch_a_resolve_past_its_timeout() {
        // The refs never end, and the server keeps the session busy.
        let mut advertisement = fixtures::ssh_adv();
        advertisement.truncate(advertisement.len() - fixtures::flush().len());
        let server = ServerBehaviour {
            advertisement,
            keepalive: Some(Duration::from_millis(200)),
            ..forge()
        };

        let ((port, outcome), took) = timed(resolve_main(&server, HOST_PUB, SHORT_TIMEOUT)).await;

        assert_eq!(
            outcome.unwrap_err().to_string(),
            format!(
                "git host unreachable: reading the refs from '127.0.0.1:{port}' did not finish \
                 within 1s"
            )
        );
        assert!(took < 2 * SHORT_TIMEOUT, "{took:?}");
        assert!(server.all_closed_within(SHORT_TIMEOUT).await);
        assert_eq!(server.received(), b"");
    }

    #[tokio::test]
    async fn waiting_for_upload_pack_to_exit_stays_within_the_timeout() {
        // upload-pack never exits, and keepalives hold the session open.
        let server = ServerBehaviour {
            after_advertisement: AfterAdvertisement::Stall,
            keepalive: Some(Duration::from_millis(200)),
            ..forge()
        };

        let ((_, commit), took) = timed(resolve_main(&server, HOST_PUB, SHORT_TIMEOUT)).await;

        assert_eq!(commit.unwrap(), fixtures::oid('a'));
        assert_eq!(server.received(), b"0000");
        assert!(took < EXIT_GRACE, "{took:?}");
        assert!(server.all_closed_within(SHORT_TIMEOUT).await);
    }

    #[tokio::test]
    async fn a_host_that_drops_connection_attempts_is_unreachable_once_the_timeout_is_up() {
        // A full listen queue: the kernel drops further SYNs, as a firewall
        // that blackholes the port does.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut queued = Vec::new();
        for _ in 0..16 {
            let connect = tokio::net::TcpStream::connect(("127.0.0.1", port));
            match tokio::time::timeout(Duration::from_millis(500), connect).await {
                Ok(Ok(stream)) => queued.push(stream),
                Ok(Err(e)) => panic!("the listen queue refused a connection: {e}"),
                Err(_) => break,
            }
        }
        assert!(queued.len() < 16, "the listen queue never filled");

        let (outcome, took) = timed(resolve_main_at(port, HOST_PUB, SHORT_TIMEOUT)).await;

        assert_eq!(
            outcome.unwrap_err().to_string(),
            format!(
                "git host unreachable: connecting to '127.0.0.1:{port}' did not finish within 1s"
            )
        );
        assert!(took < 2 * SHORT_TIMEOUT, "{took:?}");
    }

    #[tokio::test]
    async fn a_failure_keeps_the_end_of_a_long_stderr() {
        let noise: String = (0..10_000)
            .map(|i| format!("remote: counting objects {i}\n"))
            .collect();
        let stderr =
            format!("{noise}fatal: '/acme/pipelines.git' does not appear to be a git repository\n");
        let server = ServerBehaviour {
            advertisement: Vec::new(),
            after_advertisement: AfterAdvertisement::Exit(128),
            stderr: Some(stderr.clone()),
            ..forge()
        };

        let (_, outcome) = resolve_main_pinning(&server, HOST_PUB).await;

        let kept = &stderr[stderr.len() - MAX_STDERR_BYTES..];
        assert_eq!(
            outcome.unwrap_err().to_string(),
            format!(
                "ref not found: git-upload-pack failed (exit 128) for '/acme/pipelines.git': …{}",
                kept.trim()
            )
        );
    }

    #[tokio::test]
    async fn the_resolve_asks_upload_pack_for_nothing_and_ends_without_waiting_for_it() {
        let server = forge();
        let (_, commit) = resolve_main_pinning(&server, HOST_PUB).await;
        assert_eq!(commit.unwrap(), fixtures::oid('a'));
        assert_eq!(server.received(), b"0000");
        assert_eq!(server.connections(), 1);
    }

    #[tokio::test]
    async fn server_keepalives_do_not_hold_the_resolve_open() {
        // Each keepalive restarts the 2s timeout.
        let server = ServerBehaviour {
            keepalive: Some(Duration::from_secs(1)),
            ..forge()
        };
        let (_, commit) = resolve_main(&server, HOST_PUB, Duration::from_secs(2)).await;
        assert_eq!(commit.unwrap(), fixtures::oid('a'));
        assert_eq!(server.received(), b"0000");
    }

    #[tokio::test]
    async fn a_server_that_closes_after_the_advertisement_still_resolves() {
        let server = ServerBehaviour {
            after_advertisement: AfterAdvertisement::Exit(0),
            ..forge()
        };
        let (_, commit) = resolve_main_pinning(&server, HOST_PUB).await;
        assert_eq!(commit.unwrap(), fixtures::oid('a'));
    }

    #[test]
    fn the_advertisement_ends_at_its_flush_pkt_however_stdout_is_split() {
        let adv = fixtures::ssh_adv();
        let mut stdout = Vec::new();
        let mut scanned = 0;
        let mut ends = Vec::new();
        for &byte in &adv {
            stdout.push(byte);
            if reaches_flush(&stdout, &mut scanned).unwrap() {
                ends.push(stdout.len());
            }
        }
        assert_eq!(ends, [adv.len()]);
    }

    #[tokio::test]
    async fn text_before_the_advertisement_fails_without_waiting_for_the_server() {
        // A login shell that prints a greeting on stdout.
        let mut advertisement = b"Welcome to the forge!\n".to_vec();
        advertisement.extend(fixtures::ssh_adv());
        let server = ServerBehaviour {
            advertisement,
            ..forge()
        };
        let (_, commit) = resolve_main_pinning(&server, HOST_PUB).await;
        let err = commit.unwrap_err();
        assert!(
            matches!(err, GitError::Malformed(ref m) if m.contains("line length")),
            "{err:?}"
        );
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
    async fn upload_pack_gets_a_home_relative_path_as_git_sends_it() {
        let command = "git-upload-pack '~svc/pipelines.git'";
        let server = ServerBehaviour {
            expected_command: command.to_string(),
            ..Default::default()
        };
        let port = spawn_server(server.clone()).await;
        let known_hosts = known_hosts_pinning(port, HOST_PUB);
        let target =
            parse_ssh_url(&format!("ssh://git@127.0.0.1:{port}/~svc/pipelines.git")).unwrap();

        let bytes = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT).await;

        assert_eq!(server.commands(), [command]);
        let adv = parse_advertisement(&bytes.unwrap()).unwrap();
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
            after_advertisement: AfterAdvertisement::Exit(128),
            stderr: Some(
                "fatal: '/acme/pipelines.git' does not appear to be a git repository".to_string(),
            ),
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
    fn an_ipv6_host_is_its_address_without_brackets() {
        let t = parse_ssh_url("ssh://git@[0:0::1]:2222/r.git").unwrap();
        assert_eq!(
            t,
            SshTarget {
                user: "git".to_string(),
                host: "::1".to_string(),
                port: 2222,
                repo_path: "/r.git".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn an_ipv6_literal_host_resolves() {
        let listener = match tokio::net::TcpListener::bind("[::1]:0").await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("skipped: this host has no IPv6 loopback ({e})");
                return;
            }
        };
        let port = serve(listener, forge());
        // What OpenSSH records for ssh://git@[0:0::1]:<port>/…: the address
        // in its short form, in brackets with the port.
        let known_hosts = format!("[::1]:{port} {HOST_PUB}\n");
        let target =
            parse_ssh_url(&format!("ssh://git@[0:0::1]:{port}/acme/pipelines.git")).unwrap();

        let bytes = fetch_advertisement(&target, &client_auth(&known_hosts), TIMEOUT)
            .await
            .unwrap();

        let adv = parse_advertisement(&bytes).unwrap();
        let resolved = commit_for_ref(&adv, &fixtures::branch("main")).unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
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

    /// Each `(url, path)` case's url parses to `path`. The paths are what
    /// `git ls-remote <url>` (git 2.49) asks git-upload-pack for.
    #[track_caller]
    fn assert_repo_paths(cases: &[(&str, &str)]) {
        let got: Vec<_> = cases
            .iter()
            .map(|(url, _)| (*url, parse_ssh_url(url).unwrap().repo_path))
            .collect();
        let want: Vec<_> = cases
            .iter()
            .map(|(url, path)| (*url, path.to_string()))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn a_home_relative_path_loses_only_the_slash_before_the_tilde() {
        assert_repo_paths(&[
            (
                "ssh://git@forge.internal/~svc/pipelines.git",
                "~svc/pipelines.git",
            ),
            ("ssh://git@host:2222/~/r.git", "~/r.git"),
            ("ssh://git@[::1]:2222/~/r.git", "~/r.git"),
            ("ssh://git@host/srv/git/r.git", "/srv/git/r.git"),
            ("ssh://git@host//~svc/r.git", "//~svc/r.git"),
        ]);
    }

    #[test]
    fn the_path_is_decoded_and_kept_as_git_reads_it() {
        assert_repo_paths(&[
            ("ssh://git@host/%7Esvc/r.git", "~svc/r.git"),
            ("ssh://git@host/my%20repo.git", "/my repo.git"),
            ("ssh://git@host/projets-é/r.git", "/projets-é/r.git"),
            ("ssh://git@host/a%2Fb.git", "/a/b.git"),
            ("ssh://git@host/x%00y%zz%4.git", "/x%00y%zz%4.git"),
            ("ssh://git@host/a/../b.git", "/a/../b.git"),
            ("ssh://git@host/r.git?x=1#frag", "/r.git?x=1#frag"),
        ]);
    }

    #[test]
    fn a_path_that_is_not_utf8_once_decoded_is_an_invalid_url() {
        let err = parse_ssh_url("ssh://git@host/%FF.git").unwrap_err();
        assert!(
            matches!(err, GitError::InvalidUrl(ref m)
                if m == "the repository path '/%FF.git' is not UTF-8 once percent-decoded"),
            "{err:?}"
        );
    }

    #[test]
    fn a_url_git_reads_with_another_host_is_refused() {
        // git percent-decodes the url and its host runs up to the first `/`;
        // ssh takes the user up to the last `@`. So ssh would connect to
        // `evil` (or, last, to `host`) where `url` sees another host.
        let fragment = "the ssh url has '?' or '#' before its path, which git reads as part of \
                        the host";
        let cases = [
            ("ssh://git@host#@evil/r.git", fragment),
            ("ssh://git@host?@evil/r.git", fragment),
            (
                "ssh://evil%2F@host/r.git",
                "the ssh url has an escaped '/' before its path, where git ends the host",
            ),
            (
                "ssh://git@h%6Fst/r.git",
                "the ssh url has a '%' escape in its host — write the host as is",
            ),
        ];
        for (url, problem) in cases {
            let err = parse_ssh_url(url).unwrap_err();
            assert!(
                matches!(err, GitError::InvalidUrl(ref m) if m == problem),
                "{url}: {err:?}"
            );
        }
    }

    #[test]
    fn the_user_is_percent_decoded_as_git_does() {
        // What git 2.49 hands ssh: `deploy@host`, `git@x@host`.
        for (url, user) in [
            ("ssh://d%65ploy@host/r.git", "deploy"),
            ("ssh://git%40x@host/r.git", "git@x"),
            ("ssh://git@host/r.git", "git"),
        ] {
            assert_eq!(parse_ssh_url(url).unwrap().user, user, "{url}");
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
