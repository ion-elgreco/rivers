//! The code-location child of `rivers dev`: one process per generation,
//! replaced on request. A reload retires the running generation (it stops
//! scheduling, frees the gRPC port and finishes its runs in the background)
//! and starts the next one once the port is free, so two daemons never run
//! at the same time.
//!
//! The host spawns `python -c` on [`serve_dev_code_location`] with the
//! child's settings in `RIVERS_*` environment variables; no Python glue is
//! involved.

use std::process::ExitStatus;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{join_all, select_all};
use pyo3::exceptions::{PyModuleNotFoundError, PyRuntimeError, PySystemExit};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep};

use crate::repository::PyCodeRepository;

const CHILD_ENTRY: &str = "import rivers._core as c; c.serve_dev_code_location()";
const ENV_MODULE: &str = "RIVERS_MODULE";
const ENV_REPO_VAR: &str = "RIVERS_DEV_REPO_VAR";
const ENV_HOST: &str = "RIVERS_DEV_HOST";
const ENV_GRPC_PORT: &str = "RIVERS_DEV_GRPC_PORT";
const ENV_ENDPOINT: &str = "RIVERS_SURREAL_ENDPOINT";
const ENV_NO_DAEMON: &str = "RIVERS_DEV_NO_DAEMON";

const READY_TIMEOUT: Duration = Duration::from_secs(120);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const RETIRE_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// How to start a code location.
pub(crate) struct ChildSpec {
    pub python: String,
    pub module: String,
    pub repo_var: String,
    pub host: String,
    pub grpc_port: u16,
    pub endpoint: String,
    pub no_daemon: bool,
}

struct Generation {
    /// Matches the UI's reload counter: the first generation is 0.
    number: u64,
    child: Child,
}

pub(crate) enum Outcome {
    Up,
    Down,
    Interrupted,
}

pub(crate) struct Supervisor {
    spec: ChildSpec,
    active: Option<Generation>,
    retiring: Vec<Generation>,
    generations: u64,
}

impl Supervisor {
    pub(crate) fn new(spec: ChildSpec) -> Self {
        Self {
            spec,
            active: None,
            retiring: Vec::new(),
            generations: 0,
        }
    }

    /// Spawn the first generation and wait until it serves.
    pub(crate) async fn start(&mut self) -> Outcome {
        let mut signals = match Signals::new() {
            Ok(signals) => signals,
            Err(e) => {
                eprintln!("Error: cannot listen for signals: {e}");
                return Outcome::Down;
            }
        };
        self.spawn_and_wait(&mut signals).await
    }

    /// Supervise until a terminate signal. A UI request reloads; retired
    /// generations are reaped as they finish.
    pub(crate) async fn run(&mut self) {
        let mut signals = match Signals::new() {
            Ok(signals) => signals,
            Err(e) => {
                eprintln!("Error: cannot listen for signals: {e}");
                return;
            }
        };
        loop {
            tokio::select! {
                biased;
                _ = signals.terminated() => return,
                _ = rivers_ui::dev_reload::requested() => {
                    if matches!(self.reload(&mut signals).await, Outcome::Interrupted) {
                        return;
                    }
                }
                status = exit_of(&mut self.active) => self.report_exit(status),
                (index, status) = exit_of_any(&mut self.retiring) => self.reap(index, status),
            }
        }
    }

    /// Stop every generation: terminate all, wait once, kill what is left.
    /// A further terminate signal kills them at once.
    pub(crate) async fn stop(&mut self) {
        let mut live: Vec<Generation> = self
            .active
            .take()
            .into_iter()
            .chain(self.retiring.drain(..))
            .collect();
        live.retain_mut(|g| matches!(g.child.try_wait(), Ok(None)));
        for generation in &live {
            terminate(&generation.child);
        }
        let mut signals = Signals::new().ok();
        tokio::select! {
            _ = join_all(live.iter_mut().map(|g| g.child.wait())) => {}
            _ = sleep(STOP_TIMEOUT) => {}
            _ = async {
                match signals.as_mut() {
                    Some(signals) => signals.terminated().await,
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
        for generation in &mut live {
            if matches!(generation.child.try_wait(), Ok(None)) {
                let _ = generation.child.start_kill();
                let _ = generation.child.wait().await;
            }
        }
    }

    /// Retire the active generation and start a fresh one; the UI learns
    /// the outcome.
    async fn reload(&mut self, signals: &mut Signals) -> Outcome {
        println!("Reloading code location...");
        if matches!(self.retire(signals).await, Outcome::Interrupted) {
            return Outcome::Interrupted;
        }
        let outcome = self.spawn_and_wait(signals).await;
        match outcome {
            Outcome::Up => {
                rivers_ui::dev_reload::reloaded();
                rivers_ui::live::kick("code_location");
                println!("Code location reloaded.");
            }
            Outcome::Down => {
                rivers_ui::dev_reload::failed(
                    "the code location did not come back; fix the error and reload".into(),
                );
                eprintln!("Fix the error and reload.");
            }
            Outcome::Interrupted => {}
        }
        outcome
    }

    /// Tell the active generation to stop scheduling, then wait for its port.
    /// It keeps running until its in-flight runs are done.
    async fn retire(&mut self, signals: &mut Signals) -> Outcome {
        let Some(mut generation) = self.active.take() else {
            return Outcome::Up;
        };
        if !matches!(generation.child.try_wait(), Ok(None)) {
            return Outcome::Up;
        }
        terminate(&generation.child);
        let number = generation.number;
        self.retiring.push(generation);
        let (host, port) = (self.spec.host.clone(), self.spec.grpc_port);
        let mut warn_at = Instant::now() + RETIRE_WARN_INTERVAL;
        while !port_free(&host, port).await {
            if Instant::now() >= warn_at {
                println!("Waiting for generation {number} to stop scheduling...");
                warn_at += RETIRE_WARN_INTERVAL;
            }
            tokio::select! {
                biased;
                _ = signals.terminated() => return Outcome::Interrupted,
                _ = sleep(POLL_INTERVAL) => {}
            }
        }
        Outcome::Up
    }

    async fn spawn_and_wait(&mut self, signals: &mut Signals) -> Outcome {
        let (host, port) = (self.spec.host.clone(), self.spec.grpc_port);
        if !port_free(&host, port).await {
            eprintln!("Error: gRPC port {port} is in use");
            return Outcome::Down;
        }
        let mut child = match self.spawn() {
            Ok(child) => child,
            Err(e) => {
                eprintln!("Error: cannot start the code location: {e}");
                return Outcome::Down;
            }
        };
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                eprintln!(
                    "Error: code location exited with code {} before serving",
                    exit_code(&status)
                );
                return Outcome::Down;
            }
            if port_open(&host, port).await {
                self.active = Some(Generation {
                    number: self.generations,
                    child,
                });
                self.generations += 1;
                return Outcome::Up;
            }
            if Instant::now() >= deadline {
                eprintln!(
                    "Error: code location did not serve within {}s",
                    READY_TIMEOUT.as_secs()
                );
                stop_child(&mut child).await;
                return Outcome::Down;
            }
            tokio::select! {
                biased;
                _ = signals.terminated() => {
                    stop_child(&mut child).await;
                    return Outcome::Interrupted;
                }
                _ = sleep(POLL_INTERVAL) => {}
            }
        }
    }

    fn spawn(&self) -> std::io::Result<Child> {
        let spec = &self.spec;
        let mut cmd = Command::new(&spec.python);
        cmd.args(["-c", CHILD_ENTRY])
            .env(ENV_MODULE, &spec.module)
            .env("RIVERS_DEPLOYMENT", "dev")
            .env(ENV_REPO_VAR, &spec.repo_var)
            .env(ENV_HOST, &spec.host)
            .env(ENV_GRPC_PORT, spec.grpc_port.to_string())
            .env(ENV_ENDPOINT, &spec.endpoint)
            .env(ENV_NO_DAEMON, if spec.no_daemon { "1" } else { "0" });
        // Its own session: the terminal's Ctrl-C reaches only this process,
        // which then stops the children once, in order.
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        #[cfg(windows)]
        cmd.creation_flags(windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP);
        cmd.spawn()
    }

    fn report_exit(&mut self, status: std::io::Result<ExitStatus>) {
        let Some(_) = self.active.take() else {
            return;
        };
        let code = status.as_ref().map(exit_code).unwrap_or(-1);
        rivers_ui::dev_reload::failed(format!("the code location exited with code {code}"));
        eprintln!("Code location exited with code {code}; fix the error and reload from the UI.");
    }

    fn reap(&mut self, index: usize, status: std::io::Result<ExitStatus>) {
        let generation = self.retiring.swap_remove(index);
        match status.as_ref().map(exit_code) {
            Ok(0) => println!("Generation {} finished.", generation.number),
            Ok(code) => eprintln!("Generation {} exited with code {code}.", generation.number),
            Err(e) => eprintln!(
                "Generation {} could not be waited for: {e}",
                generation.number
            ),
        }
    }
}

async fn exit_of(active: &mut Option<Generation>) -> std::io::Result<ExitStatus> {
    match active {
        Some(generation) => generation.child.wait().await,
        None => std::future::pending().await,
    }
}

async fn exit_of_any(retiring: &mut [Generation]) -> (usize, std::io::Result<ExitStatus>) {
    if retiring.is_empty() {
        return std::future::pending().await;
    }
    let (status, index, _) = select_all(retiring.iter_mut().map(|g| g.child.wait().boxed())).await;
    (index, status)
}

/// Terminate, wait up to the stop timeout, then kill.
async fn stop_child(child: &mut Child) {
    terminate(child);
    if tokio::time::timeout(STOP_TIMEOUT, child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn terminate(child: &Child) {
    let Some(pid) = child.id() else {
        return;
    };
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
    }
}

/// The exit code, or the negated signal number for a signalled process.
fn exit_code(status: &ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .or_else(|| status.signal().map(|s| -s))
            .unwrap_or(-1)
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(-1)
    }
}

async fn port_open(host: &str, port: u16) -> bool {
    let connect = tokio::net::TcpStream::connect((host, port));
    matches!(
        tokio::time::timeout(Duration::from_millis(500), connect).await,
        Ok(Ok(_))
    )
}

async fn port_free(host: &str, port: u16) -> bool {
    tokio::net::TcpListener::bind((host, port)).await.is_ok()
}

/// The signals that stop the host; a hangup counts, so a closed terminal
/// still stops every generation.
#[cfg(unix)]
struct Signals {
    hangup: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            hangup: signal(SignalKind::hangup())?,
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    async fn terminated(&mut self) {
        tokio::select! {
            _ = self.hangup.recv() => {}
            _ = self.terminate.recv() => {}
            _ = self.interrupt.recv() => {}
        }
    }
}

#[cfg(windows)]
struct Signals {
    ctrl_c: tokio::signal::windows::CtrlC,
    ctrl_break: tokio::signal::windows::CtrlBreak,
    ctrl_close: tokio::signal::windows::CtrlClose,
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

#[cfg(windows)]
impl Signals {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_shutdown};
        Ok(Self {
            ctrl_c: ctrl_c()?,
            ctrl_break: ctrl_break()?,
            ctrl_close: ctrl_close()?,
            ctrl_shutdown: ctrl_shutdown()?,
        })
    }

    async fn terminated(&mut self) {
        tokio::select! {
            _ = self.ctrl_c.recv() => {}
            _ = self.ctrl_break.recv() => {}
            _ = self.ctrl_close.recv() => {}
            _ = self.ctrl_shutdown.recv() => {}
        }
    }
}

/// Entry of the code-location child: import the module, resolve it against
/// the dev host's storage, serve gRPC and run the daemon until told to stop.
/// Its settings arrive in the environment; the messages for a bad module
/// are the ones `rivers serve` prints.
#[pyfunction]
pub fn serve_dev_code_location(py: Python<'_>) -> PyResult<()> {
    let module = env(ENV_MODULE)?;
    let repo_var = env(ENV_REPO_VAR)?;
    let host = env(ENV_HOST)?;
    let grpc_port: u16 = env(ENV_GRPC_PORT)?
        .parse()
        .map_err(|e| PyRuntimeError::new_err(format!("{ENV_GRPC_PORT}: {e}")))?;
    let endpoint = env(ENV_ENDPOINT)?;
    let no_daemon = env(ENV_NO_DAEMON)? == "1";

    // The module lives in the working directory, as for `rivers dev`.
    let cwd = std::env::current_dir()?.to_string_lossy().into_owned();
    py.import("sys")?
        .getattr("path")?
        .call_method1("insert", (0, cwd))?;

    let repo = import_repo(py, &module, &repo_var)?;
    let core = py.import("rivers._core")?;
    let storage = core
        .getattr("storage")?
        .getattr("Storage")?
        .call_method1("connect", (&endpoint,))?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("storage", &storage)?;
    repo.call_method("resolve", (), Some(&kwargs))?;

    let actual_port: u16 = repo
        .call_method1("_start_grpc_server", (&host, grpc_port))?
        .extract()?;
    if actual_port != grpc_port {
        eprintln!("Warning: gRPC port {grpc_port} is in use; serving on {actual_port}");
    }
    // Held until exit: dropping the daemon object stops its loops.
    let _daemon = if no_daemon {
        None
    } else {
        let daemon = core
            .getattr("AutomationDaemon")?
            .call((&repo, &storage), None)?;
        daemon.call_method0("start")?;
        Some(daemon)
    };
    crate::shutdown::py_wait_for_exit(py, true)
}

fn env(name: &str) -> PyResult<String> {
    std::env::var(name).map_err(|_| {
        PyRuntimeError::new_err(format!("{name} is not set; rivers dev starts this entry"))
    })
}

fn import_repo<'py>(py: Python<'py>, module: &str, repo_var: &str) -> PyResult<Bound<'py, PyAny>> {
    let imported = match py.import(module) {
        Ok(imported) => imported,
        Err(e) if e.is_instance_of::<PyModuleNotFoundError>(py) => {
            return Err(fail(&format!("Error: module '{module}' not found")));
        }
        Err(e) => return Err(e),
    };
    let Ok(repo) = imported.getattr(repo_var) else {
        return Err(fail(&format!(
            "Error: '{repo_var}' not found in module '{module}'"
        )));
    };
    if !repo.is_instance_of::<PyCodeRepository>() {
        return Err(fail(&format!(
            "Error: '{repo_var}' is not a CodeRepository"
        )));
    }
    Ok(repo)
}

/// Print `message` and exit 1, as the CLI does.
fn fail(message: &str) -> PyErr {
    eprintln!("{message}");
    PySystemExit::new_err(1)
}
