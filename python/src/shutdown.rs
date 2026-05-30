//! Two-phase graceful shutdown primitives.
//!
//! Phase 1 (drain): stop accepting new work, drain in-flight operations.
//! Phase 2 (shutdown): servers stop, process exits.
//!
//! The signal handler triggers phase 1. Phase 2 is triggered by the shutdown
//! coordinator after confirming all in-flight work has drained.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::runtime::rt;

// ── Two-phase tokens ──

/// Phase 1: "stop accepting new work, start draining"
static DRAIN_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// Phase 2: "all work drained, shut down servers"
static SHUTDOWN_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

pub fn drain_token() -> &'static CancellationToken {
    DRAIN_TOKEN.get_or_init(CancellationToken::new)
}

pub fn shutdown_token() -> &'static CancellationToken {
    SHUTDOWN_TOKEN.get_or_init(CancellationToken::new)
}

// ── Handle registry ──

#[derive(Copy, Clone)]
enum Service {
    Daemon = 0,
    Grpc = 1,
    Ui = 2,
}

const SERVICE_NAMES: [&str; 3] = ["daemon", "grpc", "ui"];

enum ServiceHandle {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

struct Slot {
    handle: ServiceHandle,
    /// Cancels the previous instance when a new registration replaces it,
    /// so we don't leak its runtime / thread / bound sockets while waiting
    /// for the next graceful shutdown.
    cancel: Option<CancellationToken>,
}

static SLOTS: Mutex<[Option<Slot>; 3]> = Mutex::new([const { None }; 3]);

fn replace_handle(svc: Service, handle: ServiceHandle, cancel: Option<CancellationToken>) {
    let prev = SLOTS.lock().unwrap()[svc as usize].replace(Slot { handle, cancel });
    let Some(Slot { handle, cancel }) = prev else {
        return;
    };
    if let Some(token) = cancel {
        // Cooperative shutdown: the task observes the cancel token and
        // drains itself gracefully (subdaemons join, Arc<SurrealStorage>
        // clones drop)
        token.cancel();
        drop(handle);
        return;
    }
    // No cancel token: abort the tokio task. Threads are left to the OS
    // (tonic's graceful shutdown can wait many seconds for stale client
    // connections, and we don't want the new instance to block on that).
    match handle {
        ServiceHandle::Thread(_) => {}
        ServiceHandle::Tokio(j) => j.abort(),
    }
}

pub fn register_daemon_handle(h: tokio::task::JoinHandle<()>, cancel: CancellationToken) {
    replace_handle(Service::Daemon, ServiceHandle::Tokio(h), Some(cancel));
}

pub fn register_grpc_handle(h: std::thread::JoinHandle<()>, cancel: CancellationToken) {
    replace_handle(Service::Grpc, ServiceHandle::Thread(h), Some(cancel));
}

pub fn register_ui_handle(h: tokio::task::JoinHandle<()>) {
    replace_handle(Service::Ui, ServiceHandle::Tokio(h), None);
}

// In-flight Direct-dispatched materialization threads. Drained by
// `drain_materializations` so SIGTERM waits for them before Python
// finalizes — otherwise `Python::try_attach` returns None mid-step and
// the run silently dies. Unbounded per-request so it doesn't fit the
// fixed-slot HANDLES array.
static MATERIALIZATION_HANDLES: Mutex<Vec<std::thread::JoinHandle<()>>> = Mutex::new(Vec::new());

pub fn register_materialization_handle(h: std::thread::JoinHandle<()>) {
    let mut guard = MATERIALIZATION_HANDLES.lock().unwrap();
    guard.retain(|h| !h.is_finished());
    guard.push(h);
}

// In-flight backfill execution threads spawned by the backfill pickup loop
// (`subdaemons::spawn_backfill_pickup_loop`) and the BackfillDispatcher.
// Each thread owns an `Arc<Py<PyCodeRepository>>` clone — and transitively
// `Arc<SurrealStorage>` — so leaving them untracked means storage refs
// outlive the daemon that spawned them.
static BACKFILL_HANDLES: Mutex<Vec<std::thread::JoinHandle<()>>> = Mutex::new(Vec::new());

pub fn register_backfill_handle(h: std::thread::JoinHandle<()>) {
    let mut guard = BACKFILL_HANDLES.lock().unwrap();
    guard.retain(|h| !h.is_finished());
    guard.push(h);
}

// In-flight run-execution threads: Direct-dispatched job launches
// (`daemon::dispatchers::launch_started_run`) and dequeued queued runs
// (`backends::local::LocalRunBackend`). Like the pools above, each attaches
// the GIL to run user asset code.
static RUN_HANDLES: Mutex<Vec<std::thread::JoinHandle<()>>> = Mutex::new(Vec::new());

pub fn register_run_handle(h: std::thread::JoinHandle<()>) {
    let mut guard = RUN_HANDLES.lock().unwrap();
    guard.retain(|h| !h.is_finished());
    guard.push(h);
}

// ── Signal handler ──

static SIGNAL_HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

fn install_signal_handler() -> std::io::Result<()> {
    if SIGNAL_HANDLER_INSTALLED.load(Relaxed) {
        return Ok(());
    }

    // Register synchronously so a failure surfaces to the caller — a detached
    // task would swallow the error and leave the process unable to notice signals.
    let mut signals = {
        let _guard = rt().enter();
        TerminateSignals::try_new()?
    };

    if SIGNAL_HANDLER_INSTALLED.swap(true, Relaxed) {
        // Lost a race with a concurrent install; drop our handlers and let the winner run.
        return Ok(());
    }

    let drain = drain_token().clone();

    rt().spawn(async move {
        let kind = signals.recv().await;
        tracing::info!(target: "rivers::shutdown", signal = kind, "received terminate signal, initiating drain");

        drain.cancel();

        // Second signal → force exit
        signals.recv().await;
        tracing::warn!(target: "rivers::shutdown", "second signal received, force exiting");
        std::process::exit(1);
    });

    Ok(())
}

#[cfg(unix)]
struct TerminateSignals {
    sigterm: tokio::signal::unix::Signal,
    sigint: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminateSignals {
    fn try_new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            sigterm: signal(SignalKind::terminate())?,
            sigint: signal(SignalKind::interrupt())?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sigint.recv() => "SIGINT",
        }
    }
}

#[cfg(windows)]
struct TerminateSignals {
    ctrl_c: tokio::signal::windows::CtrlC,
    ctrl_break: tokio::signal::windows::CtrlBreak,
    ctrl_close: tokio::signal::windows::CtrlClose,
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

#[cfg(windows)]
impl TerminateSignals {
    fn try_new() -> std::io::Result<Self> {
        use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_shutdown};
        Ok(Self {
            ctrl_c: ctrl_c()?,
            ctrl_break: ctrl_break()?,
            ctrl_close: ctrl_close()?,
            ctrl_shutdown: ctrl_shutdown()?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.ctrl_c.recv() => "CTRL_C",
            _ = self.ctrl_break.recv() => "CTRL_BREAK",
            _ = self.ctrl_close.recv() => "CTRL_CLOSE",
            _ = self.ctrl_shutdown.recv() => "CTRL_SHUTDOWN",
        }
    }
}

// ── Shutdown coordinator ──

// Per-service progress flags read by the watchdog to report what's blocking on timeout.
const NOT_STARTED: u8 = 0;
const WAITING: u8 = 1;
const DONE: u8 = 2;
static STATES: [AtomicU8; 3] = [
    AtomicU8::new(NOT_STARTED),
    AtomicU8::new(NOT_STARTED),
    AtomicU8::new(NOT_STARTED),
];

async fn drain_service(svc: Service) {
    let idx = svc as usize;
    let slot = SLOTS.lock().unwrap()[idx].take();
    let Some(Slot { handle, .. }) = slot else {
        return;
    };
    STATES[idx].store(WAITING, Relaxed);
    match handle {
        ServiceHandle::Tokio(j) => {
            let _ = j.await;
        }
        ServiceHandle::Thread(j) => {
            tokio::task::spawn_blocking(move || {
                let _ = j.join();
            })
            .await
            .ok();
        }
    }
    STATES[idx].store(DONE, Relaxed);
    tracing::info!(target: "rivers::shutdown", service = SERVICE_NAMES[idx], "drained");
}

/// Block until every Direct-dispatched materialization thread has exited.
/// Joins on a `spawn_blocking` worker so the shutdown coordinator (which
/// runs on a tokio worker) doesn't park itself.
async fn drain_materializations() {
    drain_thread_pool(&MATERIALIZATION_HANDLES, "materializations").await;
}

async fn drain_backfills() {
    drain_thread_pool(&BACKFILL_HANDLES, "backfills").await;
}

async fn drain_runs() {
    drain_thread_pool(&RUN_HANDLES, "runs").await;
}

async fn drain_thread_pool(pool: &Mutex<Vec<std::thread::JoinHandle<()>>>, label: &'static str) {
    let handles = std::mem::take(&mut *pool.lock().unwrap());
    if handles.is_empty() {
        return;
    }
    let count = handles.len();
    tokio::task::spawn_blocking(move || {
        for h in handles {
            let _ = h.join();
        }
    })
    .await
    .ok();
    tracing::info!(target: "rivers::shutdown", count, kind = label, "in-flight threads drained");
}

async fn run_shutdown() {
    drain_token().cancelled().await;
    tracing::info!(target: "rivers::shutdown", "drain signal received, starting graceful shutdown");

    // Watchdog starts after drain fires — not before, to avoid killing an idle process
    let watchdog = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let blocking: Vec<&str> = SERVICE_NAMES
            .iter()
            .zip(STATES.iter())
            .filter(|(_, s)| s.load(Relaxed) == WAITING)
            .map(|(n, _)| *n)
            .collect();
        let blocking = if blocking.is_empty() {
            "unknown".to_string()
        } else {
            blocking.join(", ")
        };
        tracing::error!(
            target: "rivers::shutdown",
            blocking = %blocking,
            "shutdown timed out after 30s, force exiting",
        );
        std::process::exit(1);
    });

    // Phase 1: drain_token is already cancelled, so:
    //   - gRPC health is NOT_SERVING (LB removes pod)
    //   - daemon sees child token cancelled, stops scheduling, drains evals
    drain_service(Service::Daemon).await;

    // Phase 2: stop servers in parallel and wait for any in-flight
    // Direct-dispatched materializations (the gRPC handler returns the
    // run_id and detaches the actual work).
    shutdown_token().cancel();
    tokio::join!(
        drain_service(Service::Grpc),
        drain_service(Service::Ui),
        drain_materializations(),
        drain_backfills(),
        drain_runs(),
    );

    watchdog.abort();
    tracing::info!(target: "rivers::shutdown", "shutdown complete");
}

// ── PyO3 API ──

#[pyfunction]
#[pyo3(name = "install_signal_handler")]
pub fn py_install_signal_handler() -> PyResult<()> {
    install_signal_handler()?;
    Ok(())
}

#[pyfunction]
#[pyo3(name = "wait_for_exit")]
pub fn py_wait_for_exit(py: Python<'_>) -> PyResult<()> {
    install_signal_handler()?;
    py.detach(|| {
        rt().block_on(run_shutdown());
    });
    Ok(())
}

/// Join in-flight worker threads (materializations, backfills, runs) with the
/// GIL released, before `Py_Finalize` — one still attached at finalization
/// aborts the process. A Python `atexit` hook; no-op after a SIGTERM `run_shutdown`.
#[pyfunction]
#[pyo3(name = "drain_in_flight")]
pub fn py_drain_in_flight(py: Python<'_>) {
    // Nothing dispatched: skip spinning up the tokio runtime to drain empty pools.
    let idle = MATERIALIZATION_HANDLES.lock().unwrap().is_empty()
        && BACKFILL_HANDLES.lock().unwrap().is_empty()
        && RUN_HANDLES.lock().unwrap().is_empty();
    if idle {
        return;
    }
    py.detach(|| {
        rt().block_on(async {
            tokio::join!(drain_materializations(), drain_backfills(), drain_runs());
        });
    });
}
