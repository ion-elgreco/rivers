//! Shared async bridge: manages a Python asyncio event loop on a background thread
//! and provides `TaskLocals` for `into_future_with_locals()`.
//!
//! Used by both in-process and async executors to convert Python coroutines
//! into Rust futures via `pyo3-async-runtimes`.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

use crate::runtime::rt;

pub(crate) struct AsyncBridge {
    pub(crate) task_locals: TaskLocals,
    loop_obj: Py<PyAny>,
    // The event-loop thread holds a python guard for the
    // entirety of `loop.run_forever()`. If we let it outlive the bridge, the
    // attached tstate can survive into `Py_Finalize` and trip
    // `PyGILState_Release: thread state must be current when releasing`
    // (SIGABRT). `shutdown` joins it the thread first; `Drop` is the safety net.
    loop_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl AsyncBridge {
    pub(crate) fn new(py: Python) -> PyResult<Self> {
        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("new_event_loop")?;
        let task_locals = TaskLocals::new(loop_obj.clone());

        let loop_ref = loop_obj.clone().unbind();
        let loop_thread = std::thread::spawn(move || {
            Python::try_attach(|py| {
                let _ = loop_ref.call_method0(py, "run_forever");
            });
        });

        Ok(Self {
            task_locals,
            loop_obj: loop_obj.unbind(),
            loop_thread: Mutex::new(Some(loop_thread)),
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
        let handle = self.loop_thread.lock().unwrap().take();
        if let Some(h) = handle {
            py.detach(|| {
                let _ = h.join();
            });
        }
    }
}

impl Drop for AsyncBridge {
    fn drop(&mut self) {
        let Some(handle) = self.loop_thread.lock().unwrap().take() else {
            return;
        };
        // Try to schedule loop.stop while the interpreter is still alive.
        let scheduled = Python::try_attach(|py| {
            let Ok(stop) = self.loop_obj.getattr(py, "stop") else {
                return false;
            };
            self.loop_obj
                .call_method1(py, "call_soon_threadsafe", (stop,))
                .is_ok()
        });
        if scheduled != Some(true) {
            // Python is finalizing or unreachable: the loop thread is wedged
            // inside `run_forever` and joining would deadlock. Leave it to
            // process teardown.
            return;
        }
        // Join with the GIL released so the loop thread can grab it to run
        // the scheduled stop callback. `try_attach` returns the current
        // attach when one is already held (e.g. unwinding past `Python::attach`).
        Python::try_attach(|py| {
            py.detach(|| {
                let _ = handle.join();
            });
        });
    }
}
