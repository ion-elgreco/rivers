//! git auth from the mounted Secret, and the fetch of the pinned commit.

use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Command;

use base64::Engine as _;

use super::child::{self, Deadline, Output};
use super::{Config, Failure, Paths, log};

/// What git needs to reach the url: `-c` arguments and env.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Auth {
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

/// The url's scheme picks the Secret's keys, as in the operator, so one
/// Secret can serve ssh:// and https:// code locations.
pub(crate) fn auth(url: &str, paths: &Paths) -> Result<Auth, Failure> {
    let creds_dir = &paths.creds_dir;
    let mut auth = Auth::default();
    if url.starts_with("ssh://") {
        let identity = creds_dir.join("identity");
        let known_hosts = creds_dir.join("known_hosts");
        if !identity.is_file() {
            return Err(Failure::Message(
                "git Secret has no 'identity' — ssh:// urls need 'identity' and 'known_hosts'"
                    .to_string(),
            ));
        }
        if !known_hosts.is_file() {
            return Err(Failure::Message(
                "git Secret has 'identity' but no 'known_hosts' — refusing SSH without host-key pinning"
                    .to_string(),
            ));
        }
        read_key(creds_dir, "identity")?;
        read_key(creds_dir, "known_hosts")?;
        // git hands GIT_SSH_COMMAND to `sh -c` as soon as it holds a space.
        // GIT_SSH takes a program only: a symlink to this binary, which run
        // by that name execs ssh with the options — no shell, no interpreter.
        let helper = paths.workspace.join(GIT_SSH_HELPER);
        link_helper(&helper)?;
        auth.env
            .push(("GIT_SSH".to_string(), helper.display().to_string()));
        // Without it git probes an unknown program with `ssh -G` first.
        auth.env
            .push(("GIT_SSH_VARIANT".to_string(), "ssh".to_string()));
    } else if creds_dir.join("username").is_file() && creds_dir.join("password").is_file() {
        let username = read_key(creds_dir, "username")?;
        let password = read_key(creds_dir, "password")?;
        let basic =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        auth.args.extend([
            "-c".to_string(),
            format!("http.extraheader=Authorization: Basic {basic}"),
        ]);
    }
    Ok(auth)
}

/// Name of the `GIT_SSH` symlink in the tree; run by that name, the binary
/// is the ssh helper.
pub const GIT_SSH_HELPER: &str = ".git-ssh";

fn link_helper(helper: &Path) -> Result<(), Failure> {
    let failed =
        |e: std::io::Error| Failure::Message(format!("cannot link {}: {e}", helper.display()));
    let binary = std::env::current_exe().map_err(failed)?;
    if fs::symlink_metadata(helper).is_ok() {
        fs::remove_file(helper).map_err(failed)?;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&binary, helper).map_err(failed)
    }
    #[cfg(not(unix))]
    {
        let _ = binary;
        Err(failed(std::io::Error::other(
            "the ssh helper needs a Unix filesystem",
        )))
    }
}

/// What git runs as `GIT_SSH`: ssh with the Secret's identity and
/// known_hosts in front of git's arguments (`[-p port] host command`).
pub fn git_ssh(args: &[String]) -> i32 {
    let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let paths = Paths::from_lookup(&env);
    let mut cmd = Command::new("ssh");
    cmd.args(ssh_options(&paths.creds_dir)).args(args);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        let error = cmd.exec();
        log(format_args!("cannot run ssh: {error}"));
        127
    }
    #[cfg(not(unix))]
    {
        match cmd.status() {
            Ok(status) => child::exit_code(&status),
            Err(error) => {
                log(format_args!("cannot run ssh: {error}"));
                127
            }
        }
    }
}

fn ssh_options(creds_dir: &Path) -> [String; 8] {
    [
        "-i".to_string(),
        creds_dir.join("identity").display().to_string(),
        "-o".to_string(),
        format!(
            "UserKnownHostsFile={}",
            creds_dir.join("known_hosts").display()
        ),
        "-o".to_string(),
        "StrictHostKeyChecking=yes".to_string(),
        "-o".to_string(),
        "IdentitiesOnly=yes".to_string(),
    ]
}

/// A Secret key the url needs. An unreadable key must stop the build here:
/// read as empty, it fails the fetch later with a misleading auth or
/// host-key error. Trailing newlines go, as the operator trims them.
fn read_key(creds_dir: &Path, key: &str) -> Result<String, Failure> {
    let path = creds_dir.join(key);
    match fs::read(&path) {
        Ok(bytes) => Ok(String::from_utf8_lossy(&bytes)
            .trim_end_matches('\n')
            .to_string()),
        Err(e) => {
            let reason = match e.kind() {
                ErrorKind::PermissionDenied => "permission denied".to_string(),
                _ => e.to_string(),
            };
            Err(Failure::Message(format!(
                "cannot read the git Secret's '{key}' ({}): {reason}",
                path.display()
            )))
        }
    }
}

/// Fetch by SHA so a force-push cannot change what we get. Some servers
/// refuse SHA-in-want; fall back to the ref, then VERIFY. Pinned commits
/// have no ref to fall back to.
pub(crate) fn fetch(cfg: &Config, auth: &Auth, deadline: &Deadline) -> Result<(), Failure> {
    let src = cfg.paths.src();
    if let Err(e) = fs::remove_dir_all(&src)
        && e.kind() != ErrorKind::NotFound
    {
        return Err(Failure::Message(format!(
            "cannot remove {}: {e}",
            src.display()
        )));
    }
    fs::create_dir_all(&src)
        .map_err(|e| Failure::Message(format!("cannot create {}: {e}", src.display())))?;

    let git = |args: &[&str], output: Output| -> Result<child::Finished, Failure> {
        let mut cmd = Command::new("git");
        cmd.arg("-C")
            .arg(&src)
            .args(args)
            .envs(auth.env.iter().cloned());
        let done = child::run(&mut cmd, deadline, output)?;
        if done.status.success() {
            Ok(done)
        } else {
            Err(Failure::Build {
                exit: child::exit_code(&done.status),
            })
        }
    };
    let fetch = |what: &str| -> Result<Result<(), String>, Failure> {
        let mut cmd = Command::new("git");
        cmd.args(&auth.args)
            .arg("-C")
            .arg(&src)
            .args(["fetch", "-q", "--depth", "1", "origin", what])
            .envs(auth.env.iter().cloned());
        let done = child::run(&mut cmd, deadline, Output::Capture)?;
        Ok(if done.status.success() {
            Ok(())
        } else {
            Err(done.captured)
        })
    };

    git(&["init", "-q"], Output::Inherit)?;
    git(
        &["remote", "add", "origin", &cfg.source.git.url],
        Output::Inherit,
    )?;
    let commit = &cfg.source.git.commit;
    if let Err(output) = fetch(commit)? {
        let error = format!(
            "git fetch of commit {commit} failed: {}",
            last_lines(&output)
        );
        let Some(r#ref) = cfg.source.git.r#ref.as_deref() else {
            return Err(Failure::Message(error));
        };
        log(&error);
        log(format_args!(
            "fetching {ref} instead, then verifying the commit"
        ));
        if let Err(output) = fetch(r#ref)? {
            return Err(Failure::Message(format!(
                "git fetch of {ref} failed: {}",
                last_lines(&output)
            )));
        }
    }
    git(&["checkout", "-q", "FETCH_HEAD"], Output::Inherit)?;
    let actual = git(&["rev-parse", "HEAD"], Output::Stdout)?
        .captured
        .trim()
        .to_string();
    if actual != *commit {
        return Err(Failure::Message(format!(
            "fetched commit {actual} does not match pinned {commit} — ref moved between resolve and fetch; retrying on next pod start"
        )));
    }
    Ok(())
}

/// The last lines of an error output, without blank lines: room for git's
/// error and its ssh helper's, within the 4 KiB kubelet keeps of the
/// termination log.
pub(crate) fn last_lines(output: &str) -> String {
    let lines: Vec<&str> = output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let tail = lines[lines.len().saturating_sub(10)..].join("\n");
    let mut start = tail.len().saturating_sub(2048);
    while !tail.is_char_boundary(start) {
        start += 1;
    }
    tail[start..].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const SSH_URL: &str = "ssh://git@forge.example/acme/pipelines.git";
    const HTTPS_URL: &str = "https://forge.example/acme/pipelines.git";

    fn secret(dir: &Path, keys: &[&str]) {
        for key in keys {
            fs::write(dir.join(key), format!("{key}\n")).unwrap();
        }
    }

    fn paths(dir: &Path) -> Paths {
        let paths = Paths {
            workspace: dir.join("tree"),
            workspaces_root: dir.join("no-root"),
            creds_dir: dir.join("creds"),
            termination_log: dir.join("termination-log"),
        };
        fs::create_dir_all(&paths.workspace).unwrap();
        fs::create_dir_all(&paths.creds_dir).unwrap();
        paths
    }

    #[test]
    fn ssh_url_needs_identity_and_known_hosts() {
        let dir = tempdir().unwrap();
        let paths = paths(dir.path());
        secret(&paths.creds_dir, &["username", "password"]);
        assert_eq!(
            auth(SSH_URL, &paths),
            Err(Failure::Message(
                "git Secret has no 'identity' — ssh:// urls need 'identity' and 'known_hosts'"
                    .to_string()
            ))
        );
        secret(&paths.creds_dir, &["identity"]);
        assert_eq!(
            auth(SSH_URL, &paths),
            Err(Failure::Message(
                "git Secret has 'identity' but no 'known_hosts' — refusing SSH without host-key pinning"
                    .to_string()
            ))
        );
    }

    #[cfg(unix)]
    #[test]
    fn ssh_auth_runs_ssh_through_a_helper_without_a_shell() {
        let dir = tempdir().unwrap();
        let paths = paths(dir.path());
        secret(
            &paths.creds_dir,
            &["identity", "known_hosts", "username", "password"],
        );
        let helper = paths.workspace.join(GIT_SSH_HELPER);
        assert_eq!(
            auth(SSH_URL, &paths),
            Ok(Auth {
                args: vec![],
                env: vec![
                    ("GIT_SSH".to_string(), helper.display().to_string()),
                    ("GIT_SSH_VARIANT".to_string(), "ssh".to_string()),
                ],
            })
        );
        assert_eq!(
            fs::read_link(&helper).unwrap(),
            std::env::current_exe().unwrap()
        );
        let creds = paths.creds_dir.display();
        assert_eq!(
            ssh_options(&paths.creds_dir).to_vec(),
            vec![
                "-i".to_string(),
                format!("{creds}/identity"),
                "-o".to_string(),
                format!("UserKnownHostsFile={creds}/known_hosts"),
                "-o".to_string(),
                "StrictHostKeyChecking=yes".to_string(),
                "-o".to_string(),
                "IdentitiesOnly=yes".to_string(),
            ]
        );
        // Linking again replaces the symlink.
        assert!(auth(SSH_URL, &paths).is_ok());
    }

    #[test]
    fn https_without_a_secret_is_anonymous_and_ignores_ssh_keys() {
        let dir = tempdir().unwrap();
        let paths = paths(dir.path());
        assert_eq!(auth(HTTPS_URL, &paths), Ok(Auth::default()));
        secret(&paths.creds_dir, &["identity", "known_hosts", "username"]);
        assert_eq!(auth(HTTPS_URL, &paths), Ok(Auth::default()));
        assert!(!paths.workspace.join(".git-ssh").exists());
    }

    #[test]
    fn https_basic_header_trims_trailing_newlines() {
        let dir = tempdir().unwrap();
        let paths = paths(dir.path());
        fs::write(paths.creds_dir.join("username"), "deploy\n\n").unwrap();
        fs::write(paths.creds_dir.join("password"), "s3cr3t\n").unwrap();
        assert_eq!(
            auth(HTTPS_URL, &paths),
            Ok(Auth {
                args: vec![
                    "-c".to_string(),
                    "http.extraheader=Authorization: Basic ZGVwbG95OnMzY3IzdA==".to_string(),
                ],
                env: vec![],
            })
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_key_stops_the_build() {
        use std::os::unix::fs::PermissionsExt as _;
        // SAFETY: a plain syscall without preconditions.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipped: root reads files whatever their mode");
            return;
        }
        let dir = tempdir().unwrap();
        let paths = paths(dir.path());
        secret(&paths.creds_dir, &["username", "password"]);
        let password = paths.creds_dir.join("password");
        fs::set_permissions(&password, fs::Permissions::from_mode(0)).unwrap();
        assert_eq!(
            auth(HTTPS_URL, &paths),
            Err(Failure::Message(format!(
                "cannot read the git Secret's 'password' ({}): permission denied",
                password.display()
            )))
        );
    }

    #[test]
    fn last_lines_keeps_ten_nonblank_lines_within_2048_bytes() {
        let text: String = (1..=12).map(|i| format!("line {i}\n\n")).collect();
        let expected: Vec<String> = (3..=12).map(|i| format!("line {i}")).collect();
        assert_eq!(last_lines(&text), expected.join("\n"));
        assert_eq!(last_lines("\n  \n"), "");
        assert_eq!(last_lines(&"x".repeat(3000)).len(), 2048);
        assert_eq!(last_lines(&"é".repeat(1500)).len(), 2048);
    }
}
