//! Shared async bridge: manages a Python asyncio event loop on a background thread
//! and provides `TaskLocals` for `into_future_with_locals()`.
//!
//! Used by both in-process and async executors to convert Python coroutines
//! into Rust futures via `pyo3-async-runtimes`.

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

use crate::runtime::rt;

pub(crate) struct AsyncBridge {
    pub(crate) task_locals: TaskLocals,
    loop_obj: Py<PyAny>,
}

impl AsyncBridge {
    pub(crate) fn new(py: Python) -> PyResult<Self> {
        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("new_event_loop")?;
        let task_locals = TaskLocals::new(loop_obj.clone());

        let loop_ref = loop_obj.clone().unbind();
        std::thread::spawn(move || {
            Python::try_attach(|py| {
                let _ = loop_ref.call_method0(py, "run_forever");
            });
        });

        Ok(Self {
            task_locals,
            loop_obj: loop_obj.unbind(),
        })
    }

    /// Await a Python coroutine synchronously: convert to Rust future, release GIL, block on it.
    /// Used for single-step sequential execution (e.g. in execute_step, async generator __anext__).
    pub(crate) fn run_coroutine(&self, py: Python, coroutine: Bound<PyAny>) -> PyResult<Py<PyAny>> {
        let future = pyo3_async_runtimes::into_future_with_locals(&self.task_locals, coroutine)?;
        py.detach(|| rt().block_on(future))
    }

    pub(crate) fn shutdown(&self, py: Python) {
        if let Ok(stop) = self.loop_obj.getattr(py, "stop") {
            let _ = self
                .loop_obj
                .call_method1(py, "call_soon_threadsafe", (stop,));
        }
    }
}

impl Drop for AsyncBridge {
    fn drop(&mut self) {
        // Best-effort shutdown: try to stop the event loop if Python is available.
        // If Python is shutting down, try_attach returns None and the thread
        // will exit when the event loop is garbage collected.
        Python::try_attach(|py| {
            self.shutdown(py);
        });
    }
}
