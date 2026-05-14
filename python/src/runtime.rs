//! Shared Tokio runtime for blocking async operations at the PyO3 boundary,
//! plus the threading-primitive guide for the rest of the crate.
//!
//! Lazy-initialized multi-thread Tokio runtime via `OnceLock`. `rt()` returns
//! a `&Runtime` for `block_on()` calls at the PyO3 sync/async boundary. Panics
//! on fork detection to prevent undefined behavior with Tokio's thread pool.
//!
//! # Spawn primitive — when to use which
//!
//! * **`tokio::spawn(async ...)`** — pure async loops with no blocking work:
//!   event-writer batchers, daemon polling loops, shutdown watchdogs.
//! * **`tokio::task::spawn_blocking(...)`** — *short* GIL work that needs to
//!   be `.await`-ed by an async caller: per-step worker dispatch
//!   (`executor/dispatch/step_lifecycle.rs`), per-tick eval phases
//!   (`daemon/schedule.rs`, `daemon/sensors.rs`, `daemon/eval_dispatcher.rs`),
//!   joining OS threads from async shutdown (`shutdown.rs`). Gated by
//!   [`crate::daemon::GIL_SEMAPHORE`] where applicable to bound concurrent
//!   GIL holders.
//! * **`std::thread::spawn(...)`** — *long-lived* GIL work that should not
//!   squat on the bounded blocking pool or hold a `GIL_SEMAPHORE` permit for
//!   its lifetime: per-run materialize (`backends/local.rs`,
//!   `daemon/dispatchers.rs::launch_started_run` + Direct dispatcher),
//!   per-backfill execution (`daemon/subdaemons.rs`, `LocalBackfillDispatcher`),
//!   the gRPC `run_on_python` offload, and the AsyncBridge's `asyncio`
//!   event loop (`executor/async_exec/bridge.rs`).
//!
//! # When `rt().block_on(...)` actually panics
//!
//! Only when called from a thread currently driving futures for the runtime
//! — i.e., from inside an `async fn` body, a `tokio::spawn`-ed task, or a
//! `#[tonic::async_trait]` handler. `spawn_blocking` workers are NOT
//! runtime workers and can `block_on` safely. Practical implication: any
//! sync method that calls `rt().block_on(...)` and gets exposed to async
//! code (a tonic handler, a `tokio::spawn`-ed loop) must be converted to
//! `async fn` instead, and synchronous (pymethod) callers wrap it with
//! `rt().block_on(method().await)`.
use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;

#[inline]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();
    let pid = std::process::id();
    let runtime_pid = *PID.get_or_init(|| pid);
    if pid != runtime_pid {
        panic!(
            "Forked process detected - current PID is {pid} but the tokio runtime was created by \
             {runtime_pid}. The tokio runtime does not support forked processes \
             https://github.com/tokio-rs/tokio/issues/4301. If you are seeing this message while \
             using Python multithreading make sure to use the `spawn` or `forkserver` mode.",
        );
    }
    TOKIO_RT.get_or_init(|| {
        let rt = Runtime::new().expect("Failed to create a tokio runtime.");
        tracing::info!(
            target: "rivers::runtime",
            runtime = "main",
            workers = rt.metrics().num_workers(),
            "tokio runtime initialised"
        );
        rt
    })
}

/// Dedicated runtime for SurrealDB / storage work.
pub fn io_rt() -> &'static Runtime {
    static IO_RT: OnceLock<Runtime> = OnceLock::new();
    IO_RT.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8)
            .max(8);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("rivers-io")
            .worker_threads(workers)
            .build()
            .expect("Failed to create a tokio io runtime.");
        tracing::info!(
            target: "rivers::runtime",
            runtime = "io",
            workers = rt.metrics().num_workers(),
            "tokio runtime initialised"
        );
        rt
    })
}

/// Returns `{"main_workers": N, "io_workers": M}` for the two tokio runtimes.
///
/// Calling this eagerly initialises both runtimes (they're `OnceLock`-lazy
/// otherwise), so the `"tokio runtime initialised"` info logs we emit at
/// init also fire here. Used by the pytest conftest to print the worker
/// counts at session start — so on CI we always see them, not just when a
/// test happens to fail after the runtimes were first touched.
#[pyfunction]
pub fn runtime_info(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let main = rt().metrics().num_workers();
    let io = io_rt().metrics().num_workers();
    let dict = PyDict::new(py);
    dict.set_item("main_workers", main)?;
    dict.set_item("io_workers", io)?;
    Ok(dict.unbind())
}
