//! The `workspace` init container of git-sourced CodeLocation pods,
//! run as `rivers-runtime workspace-sync`: builds the tree at the workspace mount
//! (checkout + venv), then prunes the trees beside it on the shared volume.
//! Driven by env:
//!
//! * `RIVERS_RUN_SOURCE` — the tree's [`RunSource`]: url, commit, ref, path,
//!   dependencies (mode, files, extras, groups, timeout).
//! * Prune env, set only on the shared-mode code-location pod:
//!   `RIVERS_WORKSPACE_KEY` (this pod's own tree, never pruned),
//!   `RIVERS_WORKSPACE_KEEP` (csv of keys to retain),
//!   `RIVERS_WORKSPACE_KEEP_REVISIONS` (recency floor, default 3),
//!   `RIVERS_WORKSPACE_MIN_AGE_SECONDS` (age floor, default 3600).
//! * Path overrides, for tests: `RIVERS_WORKSPACE_DIR`,
//!   `RIVERS_WORKSPACES_ROOT`, `RIVERS_GIT_CREDS_DIR`, `RIVERS_TERMINATION_LOG`.
//!
//! Layout (identical in shared-PVC and emptyDir modes): `/workspace` is this
//! tree (subPath mount) with `src/`, `venv/`, `.ready`, `.lock`, `.git-ssh`; `/workspaces`
//! is the PVC root, mounted only on the code-location pod — only there does
//! the prune have siblings to walk; `/uv-cache` exists in shared mode only
//! (fallback pods install with `UV_NO_CACHE`); `/etc/rivers/git` is the
//! mounted git Secret: `username`/`password` for https://, `identity` +
//! `known_hosts` for ssh://.
//!
//! Ordering invariants:
//! * `.ready` is written LAST — a crash mid-build leaves a tree that gets
//!   retried, never one that looks complete;
//! * the build lock is released as soon as `.ready` is there, before the
//!   prunes — pods waiting for this tree start without waiting for them;
//! * the prune runs on EVERY invocation, including the `.ready`
//!   short-circuit — reclamation triggers on pod starts, not commits;
//! * the uv cache is pruned only after a build, the one step that fills it;
//! * prune failures are non-fatal — a full volume must not block the tree
//!   that was just built;
//! * the prune renames a tree to `/workspaces/.deleting-<key>-…` before it
//!   deletes it — a prune killed mid-delete leaves no partial tree with
//!   `.ready` under the key; each prune first deletes what earlier prunes left;
//! * a floor that is not a whole number skips the prune — a value the sync
//!   cannot read must never let it delete a tree;
//! * the prune never removes this pod's own tree, whatever the keep-set says
//!   — the operator may not have written this tree's key to it yet.
//!
//! Termination message (the operator shows it in the CodeLocation's status):
//! an error the sync finds, a failed git fetch with the last lines of git's
//! error, or a build past its time budget is written to
//! `/dev/termination-log`. Nothing else is: when uv fails, the file stays
//! empty, so kubelet reports the tail of the log, where uv's error is
//! (`terminationMessagePolicy: FallbackToLogsOnError`).
//!
//! Every program runs as a direct exec — git, uv, python3, and ssh through
//! `GIT_SSH` — so the image needs no shell.
//!
//! The log is plain stderr lines prefixed `workspace-sync: `, never
//! `tracing`: kubelet shows this log and the operator lifts its tail into
//! `status.message`.

use std::collections::HashSet;
use std::fmt::Display;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use rivers_crd::crd::run::RunSource;
use rivers_crd::workspace::{
    ENV_RUN_SOURCE, GIT_CREDS_MOUNT, WORKSPACE_MOUNT, WORKSPACES_ROOT_MOUNT,
};

mod child;
mod deps;
mod git;
mod prune;

use child::Deadline;
pub use git::{GIT_SSH_HELPER, git_ssh};

pub const LOG_PREFIX: &str = "workspace-sync: ";
const TERMINATION_LOG: &str = "/dev/termination-log";
const DEFAULT_BUDGET_SECONDS: u64 = 600;

/// Runs the sync against the process env; returns the exit code.
pub fn run() -> i32 {
    let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let paths = Paths::from_lookup(&env);
    match sync(&env, &paths) {
        Ok(()) => 0,
        Err(Failure::Build { exit }) => {
            log(format_args!("ERROR: workspace build failed (exit {exit})"));
            1
        }
        Err(Failure::Message(message)) => {
            die(&paths, &message);
            1
        }
        Err(Failure::TimedOut) => {
            die(
                &paths,
                "workspace build did not finish within its time budget",
            );
            1
        }
    }
}

pub(crate) fn log(message: impl Display) {
    eprintln!("{LOG_PREFIX}{message}");
}

/// Logs the message and makes it the termination message.
fn die(paths: &Paths, message: &str) {
    log(format_args!("ERROR: {message}"));
    // Best effort: a termination log that cannot be written must not hide the exit.
    if let Ok(mut file) = File::create(&paths.termination_log) {
        let _ = writeln!(file, "{message}");
    }
}

/// How the sync ends when it does not succeed.
#[derive(Debug, PartialEq)]
pub(crate) enum Failure {
    /// Logged as `ERROR: …`, and the termination message.
    Message(String),
    /// A child failed after printing its own error: logged as
    /// `ERROR: workspace build failed (exit N)`; the termination log stays
    /// empty, so kubelet reports the log tail.
    Build { exit: i32 },
    /// The build ran past its budget.
    TimedOut,
}

impl From<child::Error> for Failure {
    fn from(error: child::Error) -> Self {
        match error {
            child::Error::TimedOut => Failure::TimedOut,
            child::Error::Spawn { program, source } => {
                Failure::Message(format!("cannot run {program}: {source}"))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Paths {
    pub workspace: PathBuf,
    pub workspaces_root: PathBuf,
    pub creds_dir: PathBuf,
    pub termination_log: PathBuf,
}

impl Paths {
    fn from_lookup(env: &dyn Fn(&str) -> Option<String>) -> Self {
        let path = |name: &str, default: &str| {
            env(name)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(default))
        };
        Self {
            workspace: path("RIVERS_WORKSPACE_DIR", WORKSPACE_MOUNT),
            workspaces_root: path("RIVERS_WORKSPACES_ROOT", WORKSPACES_ROOT_MOUNT),
            creds_dir: path("RIVERS_GIT_CREDS_DIR", GIT_CREDS_MOUNT),
            termination_log: path("RIVERS_TERMINATION_LOG", TERMINATION_LOG),
        }
    }

    pub fn src(&self) -> PathBuf {
        self.workspace.join("src")
    }

    pub fn venv(&self) -> PathBuf {
        self.workspace.join("venv")
    }

    fn ready(&self) -> PathBuf {
        self.workspace.join(".ready")
    }

    fn lock(&self) -> PathBuf {
        self.workspace.join(".lock")
    }
}

pub(crate) struct Config {
    pub source: RunSource,
    pub paths: Paths,
    /// Wall-clock budget of the build: fetch, install, bytecode.
    pub budget: Duration,
    /// This pod's own tree, which the prune never removes.
    pub own_key: Option<String>,
    pub keep: HashSet<String>,
    pub floors: prune::Floors,
}

impl Config {
    fn from_lookup(env: &dyn Fn(&str) -> Option<String>, paths: &Paths) -> Result<Self, Failure> {
        let json = env(ENV_RUN_SOURCE)
            .ok_or_else(|| Failure::Message(format!("{ENV_RUN_SOURCE} is unset")))?;
        let source: RunSource = serde_json::from_str(&json).map_err(|e| {
            Failure::Message(format!("{ENV_RUN_SOURCE} is not valid RunSource JSON: {e}"))
        })?;
        let budget = Duration::from_secs(
            source
                .dependencies
                .timeout_seconds
                .unwrap_or(DEFAULT_BUDGET_SECONDS),
        );
        let keep = env("RIVERS_WORKSPACE_KEEP")
            .map(|csv| {
                csv.split(',')
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            source,
            paths: paths.clone(),
            budget,
            own_key: env("RIVERS_WORKSPACE_KEY"),
            keep,
            floors: prune::Floors {
                keep_revisions: env("RIVERS_WORKSPACE_KEEP_REVISIONS")
                    .unwrap_or_else(|| "3".to_string()),
                min_age_seconds: env("RIVERS_WORKSPACE_MIN_AGE_SECONDS")
                    .unwrap_or_else(|| "3600".to_string()),
            },
        })
    }

    /// The project directory: the checkout plus `path`.
    pub fn project_dir(&self) -> PathBuf {
        match self
            .source
            .git
            .path
            .as_deref()
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty())
        {
            Some(path) => self.paths.src().join(path),
            None => self.paths.src(),
        }
    }

    /// Shared mode: the PVC root is mounted, with the trees beside this one.
    pub fn shared(&self) -> bool {
        self.paths.workspaces_root.is_dir()
    }
}

fn sync(env: &dyn Fn(&str) -> Option<String>, paths: &Paths) -> Result<(), Failure> {
    let cfg = Config::from_lookup(env, paths)?;
    fs::create_dir_all(&cfg.paths.workspace).map_err(|e| {
        Failure::Message(format!(
            "cannot create {}: {e}",
            cfg.paths.workspace.display()
        ))
    })?;
    if cfg.paths.ready().exists() {
        log("tree already ready — skipping build");
        reclaim(&cfg, false);
        return Ok(());
    }
    let lock = BuildLock::acquire(&cfg.paths.lock())?;
    if cfg.paths.ready().exists() {
        lock.release();
        log("tree became ready while waiting — skipping build");
        reclaim(&cfg, false);
        return Ok(());
    }
    let deadline = Deadline::after(cfg.budget);
    if let Err(failure) = build(&cfg, &deadline) {
        return Err(match failure {
            Failure::Message(message) => {
                die(&cfg.paths, &message);
                Failure::Build { exit: 1 }
            }
            Failure::TimedOut => Failure::Message(format!(
                "workspace build did not finish within {}s (spec.git.dependencies.timeoutSeconds)",
                cfg.budget.as_secs()
            )),
            build_failed => build_failed,
        });
    }
    File::create(cfg.paths.ready()).map_err(|e| {
        Failure::Message(format!("cannot write {}: {e}", cfg.paths.ready().display()))
    })?;
    lock.release();
    log("workspace ready");
    reclaim(&cfg, true);
    Ok(())
}

fn build(cfg: &Config, deadline: &Deadline) -> Result<(), Failure> {
    let auth = git::auth(&cfg.source.git.url, &cfg.paths)?;
    git::fetch(cfg, &auth, deadline)?;
    let mode = deps::detect(cfg, &deps::uv_workspace_root(deadline))?;
    deps::install(mode, cfg, deadline)?;
    if cfg.shared() {
        deps::compile_bytecode(cfg, deadline)?;
    }
    Ok(())
}

/// After `.ready`: old trees, then — after a build, in shared mode — the uv
/// cache.
fn reclaim(cfg: &Config, built: bool) {
    if let Err(e) = prune::run(
        &cfg.paths.workspaces_root,
        cfg.own_key.as_deref(),
        &cfg.keep,
        &cfg.floors,
    ) {
        log(format_args!("prune failed (non-fatal): {e}"));
    }
    if built && cfg.shared() {
        // `--ci` keeps just the wheels uv built from source. uv >= 0.8.19 locks
        // its cache, so this waits for other pods' installs instead of removing
        // files under them.
        let pruned = Command::new("uv").args(["cache", "prune", "--ci"]).status();
        if !pruned.map(|status| status.success()).unwrap_or(false) {
            log("uv cache prune failed (non-fatal)");
        }
    }
}

/// `flock` on `/workspace/.lock`: one builder per tree, the others wait.
struct BuildLock(File);

impl BuildLock {
    fn acquire(path: &Path) -> Result<Self, Failure> {
        let file = File::options()
            .create(true)
            .write(true)
            .open(path)
            .map_err(|e| Failure::Message(format!("cannot open {}: {e}", path.display())))?;
        log("waiting for the build lock");
        file.lock()
            .map_err(|e| Failure::Message(format!("cannot lock {}: {e}", path.display())))?;
        Ok(Self(file))
    }

    /// Closing the file ends the flock.
    fn release(self) {
        drop(self.0);
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use rivers_crd::crd::code_location::{Dependencies, DependencyMode};
    use rivers_crd::crd::run::GitCoordinates;

    pub(crate) fn config(workspace: &Path, path: Option<&str>, mode: DependencyMode) -> Config {
        Config {
            source: RunSource {
                git: GitCoordinates {
                    url: "https://forge.example/acme/pipelines.git".to_string(),
                    commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string(),
                    r#ref: None,
                    path: path.map(String::from),
                    secret_name: None,
                },
                dependencies: Dependencies {
                    mode,
                    ..Default::default()
                },
                runtime_image: "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff".to_string(),
            },
            paths: Paths {
                workspace: workspace.to_path_buf(),
                workspaces_root: workspace.join("no-such-root"),
                creds_dir: workspace.join("creds"),
                termination_log: workspace.join("termination-log"),
            },
            budget: Duration::from_secs(600),
            own_key: None,
            keep: HashSet::new(),
            floors: prune::Floors {
                keep_revisions: "3".to_string(),
                min_age_seconds: "3600".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::TryLockError;
    use tempfile::tempdir;

    const SOURCE: &str = r#"{"git":{"url":"https://forge.example/acme/pipelines.git","commit":"9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8","ref":"refs/heads/main","path":"analytics"},"dependencies":{"mode":"auto","extras":["dev"],"timeoutSeconds":300},"runtimeImage":"ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff"}"#;

    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        move |name| vars.get(name).cloned().filter(|value| !value.is_empty())
    }

    #[test]
    fn config_defaults() {
        let env = env_of(&[(ENV_RUN_SOURCE, SOURCE)]);
        let paths = Paths::from_lookup(&env);
        assert_eq!(paths.workspace, Path::new("/workspace"));
        assert_eq!(paths.workspaces_root, Path::new("/workspaces"));
        assert_eq!(paths.creds_dir, Path::new("/etc/rivers/git"));
        assert_eq!(paths.termination_log, Path::new("/dev/termination-log"));
        let cfg = Config::from_lookup(&env, &paths).unwrap();
        assert_eq!(cfg.budget, Duration::from_secs(300));
        assert_eq!(cfg.own_key, None);
        assert!(cfg.keep.is_empty());
        assert_eq!(cfg.floors.keep_revisions, "3");
        assert_eq!(cfg.floors.min_age_seconds, "3600");
        assert_eq!(cfg.project_dir(), Path::new("/workspace/src/analytics"));
        assert_eq!(cfg.source.dependencies.extras, ["dev"]);

        let source = SOURCE
            .replace(r#","timeoutSeconds":300"#, "")
            .replace(r#","path":"analytics""#, "");
        let env = env_of(&[(ENV_RUN_SOURCE, &source)]);
        let cfg = Config::from_lookup(&env, &paths).unwrap();
        assert_eq!(cfg.budget, Duration::from_secs(600));
        assert_eq!(cfg.project_dir(), Path::new("/workspace/src"));
    }

    #[test]
    fn config_overrides() {
        let env = env_of(&[
            (ENV_RUN_SOURCE, SOURCE),
            ("RIVERS_WORKSPACE_DIR", "/v/tree"),
            ("RIVERS_WORKSPACES_ROOT", "/v"),
            ("RIVERS_GIT_CREDS_DIR", "/c"),
            ("RIVERS_TERMINATION_LOG", "/t"),
            ("RIVERS_WORKSPACE_KEY", "tree"),
            ("RIVERS_WORKSPACE_KEEP", " a,,b "),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "0"),
            ("RIVERS_WORKSPACE_MIN_AGE_SECONDS", "0"),
        ]);
        let paths = Paths::from_lookup(&env);
        assert_eq!(paths.workspace, Path::new("/v/tree"));
        assert_eq!(paths.workspaces_root, Path::new("/v"));
        assert_eq!(paths.creds_dir, Path::new("/c"));
        assert_eq!(paths.termination_log, Path::new("/t"));
        let cfg = Config::from_lookup(&env, &paths).unwrap();
        assert_eq!(cfg.own_key.as_deref(), Some("tree"));
        assert_eq!(cfg.keep, HashSet::from(["a".to_string(), "b".to_string()]));
        assert_eq!(cfg.floors.keep_revisions, "0");
        assert_eq!(cfg.floors.min_age_seconds, "0");
        assert_eq!(cfg.project_dir(), Path::new("/v/tree/src/analytics"));
    }

    #[test]
    fn missing_run_source_is_the_termination_message() {
        let env = env_of(&[]);
        let paths = Paths::from_lookup(&env);
        assert_eq!(
            Config::from_lookup(&env, &paths).err(),
            Some(Failure::Message("RIVERS_RUN_SOURCE is unset".to_string()))
        );
    }

    #[test]
    fn invalid_run_source_names_the_json_error() {
        let source = SOURCE.replace(r#""mode":"auto""#, r#""mode":"poetry""#);
        let env = env_of(&[(ENV_RUN_SOURCE, &source)]);
        let paths = Paths::from_lookup(&env);
        let Err(Failure::Message(message)) = Config::from_lookup(&env, &paths) else {
            panic!("a bad mode must fail");
        };
        assert!(
            message.starts_with("RIVERS_RUN_SOURCE is not valid RunSource JSON: "),
            "{message}"
        );
        assert!(message.contains("poetry"), "{message}");
    }

    #[test]
    fn build_lock_blocks_a_second_holder_until_released() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".lock");
        let lock = BuildLock::acquire(&path).unwrap();
        let other = File::options()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert!(matches!(other.try_lock(), Err(TryLockError::WouldBlock)));
        lock.release();
        assert!(other.try_lock().is_ok());
    }
}
