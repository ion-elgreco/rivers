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
}

const SERVICE_NAMES: [&str; 2] = ["daemon", "grpc"];

enum ServiceHandle {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

struct Slot {
    handle: ServiceHandle,
    /// Cancels the previous instance when a new registration replaces it,
    /// so we don't leak its runtime / thread / bound sockets while waiting
    /// for the next graceful shutdown.
    cancel: CancellationToken,
    /// Daemon only: cancelled once every loop has exited and nothing
    /// dispatches any more, while its in-flight runs still drain.
    scheduling_stopped: Option<CancellationToken>,
}

static SLOTS: Mutex<[Option<Slot>; 2]> = Mutex::new([const { None }; 2]);

fn replace_handle(svc: Service, slot: Slot) {
    if let Some(prev) = SLOTS.lock().unwrap()[svc as usize].replace(slot) {
        // Cooperative shutdown: the task observes the cancel token and
        // drains itself (subdaemons join, Arc<SurrealStorage> clones drop).
        prev.cancel.cancel();
    }
}

pub fn register_daemon_handle(
    h: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
    scheduling_stopped: CancellationToken,
) {
    replace_handle(
        Service::Daemon,
        Slot {
            handle: ServiceHandle::Tokio(h),
            cancel,
            scheduling_stopped: Some(scheduling_stopped),
        },
    );
}

pub fn register_grpc_handle(h: std::thread::JoinHandle<()>, cancel: CancellationToken) {
    replace_handle(
        Service::Grpc,
        Slot {
            handle: ServiceHandle::Thread(h),
            cancel,
            scheduling_stopped: None,
        },
    );
}

/// Stop the running gRPC server (if any): cancel its token and join its thread,
/// which drains its in-flight workers before exiting. MUST run with the GIL
/// released so those workers can finish. SIGTERM takes the same path via
/// `shutdown_token` + `drain_service(Grpc)`.
pub fn stop_grpc() {
    let slot = SLOTS.lock().unwrap()[Service::Grpc as usize].take();
    let Some(Slot { handle, cancel, .. }) = slot else {
        return;
    };
    cancel.cancel();
    if let ServiceHandle::Thread(j) = handle {
        let _ = j.join();
    }
}

// ── Signal handler ──

static SIGNAL_HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// The first terminate signal starts the drain. Every further signal
/// escalates: the one that finds the 30s cap unarmed arms it, the next
/// force-exits.
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

        loop {
            signals.recv().await;
            if arm_watchdog() {
                tracing::warn!(target: "rivers::shutdown", "second signal received, capping the drain at 30s");
            } else {
                tracing::warn!(target: "rivers::shutdown", "repeated signal received, force exiting");
                std::process::exit(1);
            }
        }
    });

    Ok(())
}

#[cfg(unix)]
pub(crate) struct TerminateSignals {
    sigterm: tokio::signal::unix::Signal,
    sigint: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminateSignals {
    pub(crate) fn try_new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            sigterm: signal(SignalKind::terminate())?,
            sigint: signal(SignalKind::interrupt())?,
        })
    }

    pub(crate) async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sigint.recv() => "SIGINT",
        }
    }
}

#[cfg(windows)]
pub(crate) struct TerminateSignals {
    ctrl_c: tokio::signal::windows::CtrlC,
    ctrl_break: tokio::signal::windows::CtrlBreak,
    ctrl_close: tokio::signal::windows::CtrlClose,
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

#[cfg(windows)]
impl TerminateSignals {
    pub(crate) fn try_new() -> std::io::Result<Self> {
        use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_shutdown};
        Ok(Self {
            ctrl_c: ctrl_c()?,
            ctrl_break: ctrl_break()?,
            ctrl_close: ctrl_close()?,
            ctrl_shutdown: ctrl_shutdown()?,
        })
    }

    pub(crate) async fn recv(&mut self) -> &'static str {
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
static STATES: [AtomicU8; 2] = [AtomicU8::new(NOT_STARTED), AtomicU8::new(NOT_STARTED)];

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

static WATCHDOG: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);

/// Force-exit in 30s unless shutdown completes first; `false` when it was
/// already armed. Armed only after the drain fires, never on an idle process.
fn arm_watchdog() -> bool {
    let mut slot = WATCHDOG.lock().unwrap();
    if slot.is_some() {
        return false;
    }
    *slot = Some(rt().spawn(async {
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
    }));
    true
}

/// Resolves once the daemon stopped scheduling; at once when no daemon is
/// registered.
async fn daemon_stopped_scheduling() {
    let scheduling_stopped = SLOTS.lock().unwrap()[Service::Daemon as usize]
        .as_ref()
        .and_then(|slot| slot.scheduling_stopped.clone());
    if let Some(token) = scheduling_stopped {
        STATES[Service::Daemon as usize].store(WAITING, Relaxed);
        token.cancelled().await;
        tracing::info!(target: "rivers::shutdown", "daemon stopped scheduling");
    }
}

/// Once the drain fires, stop the services. In retire mode they stop as soon
/// as the daemon cannot dispatch any more, so the gRPC port is free while this
/// process still drains its runs, and the 30s cap waits for a second signal.
/// Otherwise the daemon drains first, under the cap.
async fn run_shutdown(retire: bool) {
    drain_token().cancelled().await;
    tracing::info!(target: "rivers::shutdown", "drain signal received, starting graceful shutdown");

    // Phase 1: drain_token is already cancelled, so:
    //   - gRPC health is NOT_SERVING (LB removes pod)
    //   - daemon sees child token cancelled, stops scheduling, drains evals
    if retire {
        daemon_stopped_scheduling().await;
    } else {
        arm_watchdog();
        drain_service(Service::Daemon).await;
    }

    // Phase 2: stop servers. Each subsystem self-drains its in-flight runs
    // before its handle resolves (daemon in `daemon_main_loop`, gRPC in its
    // serve thread), so there's no separate pool to drain here.
    shutdown_token().cancel();
    tokio::join!(drain_service(Service::Daemon), drain_service(Service::Grpc));

    if let Some(watchdog) = WATCHDOG.lock().unwrap().take() {
        watchdog.abort();
    }
    tracing::info!(target: "rivers::shutdown", "shutdown complete");
}

// ── PyO3 API ──

#[pyfunction]
#[pyo3(name = "install_signal_handler")]
pub fn py_install_signal_handler() -> PyResult<()> {
    install_signal_handler()?;
    Ok(())
}

/// Block until graceful shutdown completes. With `retire`, the first terminate
/// signal stops scheduling and releases the gRPC port once the daemon loops
/// have exited, while in-flight runs drain without the 30s cap; a second
/// signal arms the cap, a third force-exits.
#[pyfunction]
#[pyo3(name = "wait_for_exit", signature = (retire = false))]
pub fn py_wait_for_exit(py: Python<'_>, retire: bool) -> PyResult<()> {
    install_signal_handler()?;
    py.detach(|| {
        rt().block_on(run_shutdown(retire));
    });
    Ok(())
}
