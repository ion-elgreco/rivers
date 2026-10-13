//! Per-step capture of log output via a tee writer on the fmt layer.
//!
//! The fmt layer's writer tees to both stderr and a thread-local buffer.
//! When capture is active (`start()` called), formatted output is copied
//! into the buffer. `take()` returns the buffered output and stops capture.
//!
//! An async step's body runs on the event loop's thread, beside the other
//! async steps. Its Python log records find the step's buffer through the
//! step's context, as its stdout does (`rivers._capture`).

use std::cell::RefCell;
use std::io;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyTuple};
use tracing_subscriber::fmt::MakeWriter;

/// A step's formatted log output; `None` once the step took it.
type Buffer = Arc<Mutex<Option<Vec<u8>>>>;

thread_local! {
    static BUFFER: RefCell<Option<Buffer>> = const { RefCell::new(None) };
}

/// The buffer of a step's capture, carried by the step's context.
#[pyclass(frozen, module = "rivers._core")]
pub struct LogBuffer(Buffer);

/// Start capturing formatted log output on the current thread.
pub fn start() -> LogBuffer {
    let buffer = Arc::new(Mutex::new(Some(Vec::new())));
    BUFFER.with(|b| {
        *b.borrow_mut() = Some(Arc::clone(&buffer));
    });
    LogBuffer(buffer)
}

/// Stop capturing and return all captured output as a string. Later writes
/// from the step's context are not kept.
pub fn take() -> String {
    BUFFER
        .with(|b| b.borrow_mut().take())
        .and_then(|buffer| buffer.lock().ok()?.take())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_default()
}

/// Wrap pyo3-pylogger's `logging.host_log`, which hands each Python log
/// record to Rust: on a thread that does not capture, a record logged in a
/// step's context goes to that step's buffer.
pub fn route_python_records(py: Python<'_>) -> PyResult<()> {
    let logging = py.import("logging")?;
    let host_log = logging.getattr("host_log")?.unbind();
    let current_capture = py.import("rivers._capture")?.getattr("_get")?.unbind();

    let routed = PyCFunction::new_closure(
        py,
        Some(c"host_log"),
        None,
        move |args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>| {
            let py = args.py();
            let step = if BUFFER.with(|b| b.borrow().is_some()) {
                None
            } else {
                step_buffer(current_capture.bind(py).call0()?)?
            };
            let Some(step) = step else {
                return host_log.call(py, args, kwargs);
            };

            BUFFER.with(|b| *b.borrow_mut() = Some(step));
            let result = host_log.call(py, args, kwargs);
            BUFFER.with(|b| *b.borrow_mut() = None);
            result
        },
    )?;
    logging.setattr("host_log", routed)
}

/// The log buffer among a step's capture buffers, or `None` outside a step.
fn step_buffer(bufs: Bound<'_, PyAny>) -> PyResult<Option<Buffer>> {
    if bufs.is_none() {
        return Ok(None);
    }
    Ok(bufs
        .get_item(2)?
        .cast::<LogBuffer>()
        .ok()
        .map(|logs| Arc::clone(&logs.get().0)))
}

pub struct TeeWriter;

impl<'a> MakeWriter<'a> for TeeWriter {
    type Writer = TeeIo;

    fn make_writer(&'a self) -> Self::Writer {
        TeeIo
    }
}

pub struct TeeIo;

impl io::Write for TeeIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        io::stderr().write_all(buf)?;
        BUFFER.with(|b| {
            if let Some(buffer) = b.borrow().as_ref()
                && let Ok(mut held) = buffer.lock()
                && let Some(bytes) = held.as_mut()
            {
                bytes.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }
}
