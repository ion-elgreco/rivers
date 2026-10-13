//! Shared async bridge: manages a Python asyncio event loop on a background thread
//! and provides `TaskLocals` for `into_future_with_locals()`.
//!
//! Used by both in-process and async executors to convert Python coroutines
//! into Rust futures via `pyo3-async-runtimes`.

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use pyo3::types::PyCFunction;
use pyo3_async_runtimes::TaskLocals;

use crate::runtime::rt;

const SIGNAL_CHECK: Duration = Duration::from_millis(250);

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
                let _ = loop_ref.call_method0(py, "close");
            });
        });

        Ok(Self {
            task_locals,
            loop_obj: loop_obj.unbind(),
            loop_thread: Mutex::new(Some(loop_thread)),
        })
    }

    /// Run `future` to its end with the GIL released. Python raises a Ctrl-C
    /// only when this thread runs Python code, which it does not while it
    /// waits; so check for one every [`SIGNAL_CHECK`], and cancel the loop's
    /// tasks, as Ctrl-C interrupts a sync step.
    pub(crate) fn block_on<F>(&self, py: Python, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        let mut future = std::pin::pin!(future);
        loop {
            let waited = py.detach(|| {
                rt().block_on(async { tokio::time::timeout(SIGNAL_CHECK, &mut future).await })
            });
            if let Ok(output) = waited {
                return output;
            }
            if py.check_signals().is_err() {
                tracing::info!("interrupted: cancelling the running async steps");
                if let Err(e) = self.cancel_tasks(py) {
                    tracing::warn!(error = %e, "could not cancel the async steps");
                }
            }
        }
    }

    /// Cancel every task on the loop: a running async step gets
    /// `CancelledError` at its next `await`.
    fn cancel_tasks(&self, py: Python) -> PyResult<()> {
        let event_loop = self.loop_obj.clone_ref(py);
        // `all_tasks` and `Task.cancel` must run on the loop's thread.
        let cancel = PyCFunction::new_closure(py, None, None, move |args, _| -> PyResult<()> {
            let py = args.py();
            let tasks = py
                .import("asyncio")?
                .call_method1("all_tasks", (&event_loop,))?;
            for task in tasks.try_iter()? {
                task?.call_method0("cancel")?;
            }
            Ok(())
        })?;
        self.loop_obj
            .call_method1(py, "call_soon_threadsafe", (cancel,))?;
        Ok(())
    }

    pub(crate) fn shutdown(&self, py: Python) {
        // A pending Ctrl-C would raise inside `call_soon_threadsafe`, so
        // `stop` would never run and the join would hang. Take it first, and
        // raise it again once the loop has stopped.
        let interrupted = py
            .check_signals()
            .is_err_and(|e| e.is_instance_of::<PyKeyboardInterrupt>(py));
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
        if interrupted {
            // SAFETY: async-signal-safe, and needs no attached thread state.
            unsafe { pyo3::ffi::PyErr_SetInterrupt() };
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
