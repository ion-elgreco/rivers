//! Dependencies: which mode `auto` lands on, the install, the bytecode.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use rivers_crd::crd::code_location::DependencyMode;

use super::child::{self, Deadline, Output};
use super::{Config, Failure, log};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    UvSync,
    Requirements,
    None,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::UvSync => "uvSync",
            Mode::Requirements => "requirements",
            Mode::None => "none",
        }
    }
}

/// The mode to install with. `auto` reads the project directory, its own
/// files first — without a `pyproject.toml`, `uv sync --project` would
/// install the enclosing project instead. `root` answers which workspace
/// root uv reports for a project ([`uv_workspace_root`] in production).
pub(crate) fn detect(
    cfg: &Config,
    root: &dyn Fn(&Path) -> Result<PathBuf, Failure>,
) -> Result<Mode, Failure> {
    let project = cfg.project_dir();
    if !project.is_dir() {
        return Err(Failure::Message(format!(
            "spec.git.path '{}' does not exist in the repository",
            cfg.source.git.path.as_deref().unwrap_or_default()
        )));
    }
    Ok(match cfg.source.dependencies.mode {
        DependencyMode::UvSync => Mode::UvSync,
        DependencyMode::Requirements => Mode::Requirements,
        DependencyMode::None => Mode::None,
        DependencyMode::Auto => {
            let mode = auto(cfg, &project, root)?;
            log(format_args!("deps mode auto -> {}", mode.name()));
            mode
        }
    })
}

fn auto(
    cfg: &Config,
    project: &Path,
    root: &dyn Fn(&Path) -> Result<PathBuf, Failure>,
) -> Result<Mode, Failure> {
    if project.join("uv.lock").is_file() {
        return Ok(Mode::UvSync);
    }
    if project.join("requirements.txt").is_file() {
        return Ok(Mode::Requirements);
    }
    if !project.join("pyproject.toml").is_file() {
        return Ok(Mode::None);
    }
    let src = canonical(&cfg.paths.src());
    let project = canonical(project);
    let reported = canonical(&root(&project)?);
    log(format_args!("uv workspace root: {}", reported.display()));
    // uv walks past the checkout: a root above it, like the project itself,
    // does not count.
    if reported != project && project.starts_with(&reported) && reported.starts_with(&src) {
        let lock = reported.join("uv.lock");
        if !lock.is_file() {
            return Ok(Mode::None);
        }
        log(format_args!("uv workspace lock: {}", lock.display()));
        return Ok(Mode::UvSync);
    }
    match nearest_lock_above(&project, &src) {
        Some(dir) => {
            let at = match dir.strip_prefix(&src) {
                Ok(rel) if rel.as_os_str().is_empty() => "the repository root".to_string(),
                Ok(rel) => rel.display().to_string(),
                Err(_) => dir.display().to_string(),
            };
            Err(Failure::Message(format!(
                "'{}' has no uv.lock, and uv does not count it as a member of the workspace at {at}: \
                 add it to [tool.uv.workspace] members, give it its own uv.lock, or set \
                 spec.git.dependencies.mode to none",
                cfg.source.git.path.as_deref().unwrap_or_default()
            )))
        }
        None => Ok(Mode::None),
    }
}

/// The nearest `uv.lock` from the project's parent up to the checkout root.
fn nearest_lock_above(project: &Path, src: &Path) -> Option<PathBuf> {
    project
        .ancestors()
        .skip(1)
        .take_while(|dir| dir.starts_with(src))
        .find(|dir| dir.join("uv.lock").is_file())
        .map(Path::to_path_buf)
}

fn canonical(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The production `root` for [`detect`]: `uv workspace dir --project <dir>`,
/// uv's own view of membership (`members`, `exclude`); stable since uv 0.10.0.
pub(crate) fn uv_workspace_root(
    deadline: &Deadline,
) -> impl Fn(&Path) -> Result<PathBuf, Failure> + '_ {
    move |project| {
        let mut cmd = Command::new("uv");
        cmd.args(["workspace", "dir", "--project"]).arg(project);
        let done = child::run(&mut cmd, deadline, Output::Stdout)?;
        if !done.status.success() {
            return Err(Failure::Build {
                exit: child::exit_code(&done.status),
            });
        }
        Ok(PathBuf::from(done.captured.trim()))
    }
}

pub(crate) fn install(mode: Mode, cfg: &Config, deadline: &Deadline) -> Result<(), Failure> {
    let project = cfg.project_dir();
    let venv = cfg.paths.venv();
    let deps = &cfg.source.dependencies;
    let uv = |cmd: &mut Command| -> Result<(), Failure> {
        let done = child::run(cmd, deadline, Output::Inherit)?;
        if done.status.success() {
            Ok(())
        } else {
            Err(Failure::Build {
                exit: child::exit_code(&done.status),
            })
        }
    };
    match mode {
        Mode::UvSync => {
            let mut cmd = Command::new("uv");
            cmd.args(["sync", "--locked", "--project"]).arg(&project);
            for extra in &deps.extras {
                cmd.args(["--extra", extra]);
            }
            for group in &deps.groups {
                cmd.args(["--group", group]);
            }
            // uv sync creates the env itself at UV_PROJECT_ENVIRONMENT.
            cmd.env("UV_PROJECT_ENVIRONMENT", &venv);
            uv(&mut cmd)
        }
        Mode::Requirements => {
            // uv pip targets VIRTUAL_ENV, not UV_PROJECT_ENVIRONMENT — create
            // the env explicitly first.
            uv(Command::new("uv").args(["venv", "-q"]).arg(&venv))?;
            let mut cmd = Command::new("uv");
            cmd.args(["pip", "install"]);
            let default = ["requirements.txt".to_string()];
            let files = if deps.files.is_empty() {
                &default[..]
            } else {
                &deps.files[..]
            };
            for file in files {
                cmd.arg("-r").arg(project.join(file));
            }
            cmd.env("VIRTUAL_ENV", &venv);
            uv(&mut cmd)
        }
        Mode::None => {
            log("deps mode none — installing nothing; the runtime image carries the deps");
            // The pod command is uniformly /workspace/venv/bin/rivers so the
            // operator never has to guess which mode `auto` landed on —
            // provide the entrypoint as a shim to the image-level install.
            match find_on_path("rivers") {
                Some(rivers) => link_rivers(&rivers, &venv),
                None => {
                    log("rivers not on PATH — skipping the venv shim");
                    Ok(())
                }
            }
        }
    }
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(program))
        .find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::metadata(path)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

fn link_rivers(rivers: &Path, venv: &Path) -> Result<(), Failure> {
    let bin = venv.join("bin");
    let link = bin.join("rivers");
    let failed =
        |e: std::io::Error| Failure::Message(format!("cannot link {}: {e}", link.display()));
    fs::create_dir_all(&bin).map_err(failed)?;
    if fs::symlink_metadata(&link).is_ok() {
        fs::remove_file(&link).map_err(failed)?;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(rivers, &link).map_err(failed)
    }
    #[cfg(not(unix))]
    {
        let _ = rivers;
        Err(Failure::Message(format!(
            "cannot link {}: the venv shim needs a Unix filesystem",
            link.display()
        )))
    }
}

/// Shared mode only: run and step pods mount the tree read-only, so Python
/// cannot write `__pycache__` there. Compiles what they import from the
/// checkout: the project directory, and the directories the venv's `.pth`
/// files add from it (editable installs — uv workspace members). Non-fatal:
/// a syntax error in user code should surface at import time with a real
/// traceback.
pub(crate) fn compile_bytecode(cfg: &Config, deadline: &Deadline) -> Result<(), Failure> {
    let dirs = compile_dirs(&cfg.project_dir(), &cfg.paths.src(), &cfg.paths.venv());
    let mut cmd = Command::new("python3");
    // Not -j 0: that starts one worker per node core, whatever the pod's CPU
    // limit, and on a big node their memory can pass the container's limit.
    cmd.args(["-m", "compileall", "-q", "-j", "4"]).args(&dirs);
    let done = child::run(&mut cmd, deadline, Output::Inherit)?;
    if !done.status.success() {
        log("compileall reported errors (non-fatal)");
    }
    Ok(())
}

/// The project directory, then the checkout directories the venv's `.pth`
/// files name, in file order (uv writes the path without a trailing newline).
fn compile_dirs(project: &Path, src: &Path, venv: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![project.to_path_buf()];
    for site in site_packages(venv) {
        let mut pths: Vec<PathBuf> = fs::read_dir(&site)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "pth"))
                    .collect()
            })
            .unwrap_or_default();
        pths.sort();
        for pth in pths {
            let Ok(text) = fs::read_to_string(&pth) else {
                continue;
            };
            for line in text.lines() {
                let dir = Path::new(line);
                if dir.starts_with(project) {
                    continue;
                }
                if dir.starts_with(src) && dir.is_dir() {
                    dirs.push(dir.to_path_buf());
                }
            }
        }
    }
    dirs
}

/// `venv/lib/python*/site-packages`, in name order.
fn site_packages(venv: &Path) -> Vec<PathBuf> {
    let mut pythons: Vec<PathBuf> = fs::read_dir(venv.join("lib"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("python"))
                })
                .collect()
        })
        .unwrap_or_default();
    pythons.sort();
    pythons
        .into_iter()
        .map(|python| python.join("site-packages"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_sync::test_support::config;
    use std::time::Duration;
    use tempfile::{TempDir, tempdir};

    const MEMBER: &str = "[project]\nname = \"analytics\"\nversion = \"0.1.0\"\n";
    const ROOT: &str = "[project]\nname = \"root\"\nversion = \"0.1.0\"\n";
    const WORKSPACE: &str = "[tool.uv.workspace]\nmembers = [\"analytics\", \"libs/*\"]\n";
    const REMEDIES: &str = "'analytics' has no uv.lock, and uv does not count it as a member of the \
        workspace at the repository root: add it to [tool.uv.workspace] members, give it its own \
        uv.lock, or set spec.git.dependencies.mode to none";

    struct Checkout {
        _dir: TempDir,
        cfg: Config,
        src: PathBuf,
    }

    fn checkout(path: &str, files: &[(&str, &str)]) -> Checkout {
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), Some(path), DependencyMode::Auto);
        let src = cfg.paths.src();
        for (name, content) in files {
            let file = src.join(name);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, content).unwrap();
        }
        Checkout {
            _dir: dir,
            cfg,
            src,
        }
    }

    fn standalone(project: &Path) -> Result<PathBuf, Failure> {
        Ok(project.to_path_buf())
    }

    fn root_at(root: PathBuf) -> impl Fn(&Path) -> Result<PathBuf, Failure> {
        move |_| Ok(root.clone())
    }

    fn never(_: &Path) -> Result<PathBuf, Failure> {
        panic!("uv must not be asked")
    }

    #[test]
    fn explicit_modes_do_not_look_at_the_project() {
        let dir = tempdir().unwrap();
        for (mode, expected) in [
            (DependencyMode::UvSync, Mode::UvSync),
            (DependencyMode::Requirements, Mode::Requirements),
            (DependencyMode::None, Mode::None),
        ] {
            let cfg = config(dir.path(), None, mode);
            fs::create_dir_all(cfg.paths.src()).unwrap();
            assert_eq!(detect(&cfg, &never), Ok(expected));
        }
    }

    #[test]
    fn auto_prefers_the_projects_own_lock() {
        let c = checkout(
            "analytics",
            &[
                ("uv.lock", "version = 1\n"),
                ("pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                ("analytics/pyproject.toml", MEMBER),
                ("analytics/uv.lock", "version = 1\n"),
            ],
        );
        assert_eq!(detect(&c.cfg, &never), Ok(Mode::UvSync));
    }

    #[test]
    fn auto_installs_requirements_txt() {
        let c = checkout("analytics", &[("analytics/requirements.txt", "pandas\n")]);
        assert_eq!(detect(&c.cfg, &never), Ok(Mode::Requirements));
    }

    #[test]
    fn auto_prefers_requirements_txt_to_the_workspace_lock() {
        let c = checkout(
            "analytics",
            &[
                ("uv.lock", "version = 1\n"),
                ("pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                ("analytics/pyproject.toml", MEMBER),
                ("analytics/requirements.txt", "pandas\n"),
            ],
        );
        assert_eq!(detect(&c.cfg, &never), Ok(Mode::Requirements));
    }

    #[test]
    fn auto_uses_the_workspace_root_lock() {
        for member in ["analytics", "libs/analytics"] {
            let c = checkout(
                member,
                &[
                    ("uv.lock", "version = 1\n"),
                    ("pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                    (&format!("{member}/pyproject.toml"), MEMBER),
                ],
            );
            assert_eq!(detect(&c.cfg, &root_at(c.src.clone())), Ok(Mode::UvSync));
        }
    }

    #[test]
    fn a_standalone_project_under_a_lock_fails_with_the_remedies() {
        let c = checkout(
            "analytics",
            &[
                ("uv.lock", "version = 1\n"),
                ("pyproject.toml", ROOT),
                ("analytics/pyproject.toml", MEMBER),
            ],
        );
        assert_eq!(
            detect(&c.cfg, &standalone),
            Err(Failure::Message(REMEDIES.to_string()))
        );

        let c = checkout(
            "libs/analytics",
            &[
                ("libs/uv.lock", "version = 1\n"),
                ("libs/pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                ("libs/analytics/pyproject.toml", MEMBER),
            ],
        );
        assert_eq!(
            detect(&c.cfg, &standalone),
            Err(Failure::Message(
                REMEDIES
                    .replace("'analytics'", "'libs/analytics'")
                    .replace("the repository root", "libs")
            ))
        );
    }

    #[test]
    fn auto_installs_nothing_for_a_standalone_project_without_a_lock_above() {
        let c = checkout(
            "analytics",
            &[
                ("pyproject.toml", ROOT),
                ("analytics/pyproject.toml", MEMBER),
            ],
        );
        assert_eq!(detect(&c.cfg, &standalone), Ok(Mode::None));
    }

    #[test]
    fn auto_installs_nothing_when_the_root_has_no_lock() {
        let c = checkout(
            "analytics",
            &[
                ("pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                ("analytics/pyproject.toml", MEMBER),
            ],
        );
        assert_eq!(detect(&c.cfg, &root_at(c.src.clone())), Ok(Mode::None));
    }

    #[test]
    fn auto_installs_nothing_without_pyproject() {
        let c = checkout(
            "analytics",
            &[
                ("uv.lock", "version = 1\n"),
                ("pyproject.toml", &format!("{ROOT}{WORKSPACE}")),
                ("analytics/pipeline.py", ""),
            ],
        );
        assert_eq!(detect(&c.cfg, &never), Ok(Mode::None));
    }

    #[test]
    fn a_root_above_the_checkout_does_not_count() {
        let c = checkout("analytics", &[("analytics/pyproject.toml", MEMBER)]);
        let above = c.cfg.paths.workspace.clone();
        fs::write(above.join("pyproject.toml"), format!("{ROOT}{WORKSPACE}")).unwrap();
        fs::write(above.join("uv.lock"), "version = 1\n").unwrap();
        assert_eq!(detect(&c.cfg, &root_at(above.clone())), Ok(Mode::None));

        fs::write(c.src.join("uv.lock"), "version = 1\n").unwrap();
        assert_eq!(
            detect(&c.cfg, &root_at(above)),
            Err(Failure::Message(REMEDIES.to_string()))
        );
    }

    #[test]
    fn a_failing_root_lookup_fails_the_build() {
        let c = checkout("analytics", &[("analytics/pyproject.toml", MEMBER)]);
        let broken = |_: &Path| Err(Failure::Build { exit: 2 });
        assert_eq!(detect(&c.cfg, &broken), Err(Failure::Build { exit: 2 }));
    }

    #[test]
    fn missing_path_is_the_termination_message() {
        let c = checkout("analytics", &[("pyproject.toml", ROOT)]);
        assert_eq!(
            detect(&c.cfg, &never),
            Err(Failure::Message(
                "spec.git.path 'analytics' does not exist in the repository".to_string()
            ))
        );
    }

    #[test]
    fn compile_dirs_follow_project_then_pth_entries_in_order() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let project = src.join("analytics");
        let venv = dir.path().join("venv");
        let elsewhere = dir.path().join("elsewhere");
        for d in [
            project.join("src"),
            src.join("libs/shared/src"),
            src.join("libs/unused/src"),
            elsewhere.clone(),
        ] {
            fs::create_dir_all(d).unwrap();
        }
        let site = venv.join("lib/python3.12/site-packages");
        fs::create_dir_all(&site).unwrap();
        fs::write(site.join("_virtualenv.pth"), "import _virtualenv\n").unwrap();
        fs::write(
            site.join("_editable_0.pth"),
            src.join("libs/shared/src").display().to_string(),
        )
        .unwrap();
        fs::write(
            site.join("_editable_1.pth"),
            project.join("src").display().to_string(),
        )
        .unwrap();
        fs::write(
            site.join("_editable_2.pth"),
            elsewhere.display().to_string(),
        )
        .unwrap();
        fs::write(
            site.join("_editable_3.pth"),
            src.join("libs/missing").display().to_string(),
        )
        .unwrap();
        assert_eq!(
            compile_dirs(&project, &src, &venv),
            vec![project.clone(), src.join("libs/shared/src")]
        );
        // The checkout itself is a directory the venv may name (flat layout).
        fs::write(site.join("_editable_4.pth"), src.display().to_string()).unwrap();
        assert_eq!(
            compile_dirs(&project, &src, &venv),
            vec![project, src.join("libs/shared/src"), src]
        );
    }

    #[test]
    fn uv_reports_the_root_of_a_member() {
        assert!(
            find_on_path("uv").is_some(),
            "install uv 0.10.0 or later: `uv workspace dir` decides the workspace root"
        );
        let deadline = Deadline::after(Duration::from_secs(30));
        let root = uv_workspace_root(&deadline);
        let dir = tempdir().unwrap();
        let lookup = |files: &[(&str, &str)], project: &str| -> PathBuf {
            let base = dir.path().join(files[0].0.split('/').next().unwrap());
            let _ = fs::remove_dir_all(&base);
            for (name, content) in files {
                let file = dir.path().join(name);
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, content).unwrap();
            }
            canonical(&root(&dir.path().join(project)).unwrap())
        };
        let table = &[
            (
                "table/pyproject.toml",
                "[project]\nname = \"root\"\nversion = \"0.1.0\"\n\n[tool.uv.workspace]\nmembers = [\"analytics\"]\nexclude = [\"libs/excluded\"]\n",
            ),
            ("table/analytics/pyproject.toml", MEMBER),
            (
                "table/libs/excluded/pyproject.toml",
                "[project]\nname = \"excluded\"\nversion = \"0.1.0\"\n",
            ),
            (
                "table/other/pyproject.toml",
                "[project]\nname = \"other\"\nversion = \"0.1.0\"\n",
            ),
        ];
        let base = canonical(dir.path()).join("table");
        assert_eq!(lookup(table, "table/analytics"), base);
        assert_eq!(
            lookup(table, "table/libs/excluded"),
            base.join("libs/excluded")
        );
        assert_eq!(lookup(table, "table/other"), base.join("other"));
        let inline = &[
            (
                "inline/pyproject.toml",
                "[project]\nname = \"root\"\nversion = \"0.1.0\"\n\n[tool.uv]\nworkspace = { members = [\"analytics\"] }\n",
            ),
            ("inline/analytics/pyproject.toml", MEMBER),
        ];
        assert_eq!(
            lookup(inline, "inline/analytics"),
            canonical(dir.path()).join("inline")
        );
        let alone = &[("alone/pyproject.toml", MEMBER)];
        assert_eq!(lookup(alone, "alone"), canonical(dir.path()).join("alone"));
    }
}
