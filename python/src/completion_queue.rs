//! CompletionQueue — event-driven notification for streaming collect.
//!
//! Wraps a channel that receives mapping keys as map instances complete.
//! Supports sync iteration (`for key in queue`).

use std::sync::{Arc, Mutex};

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;

pub struct CompletionReceiver {
    rx: std::sync::mpsc::Receiver<Option<String>>,
}

impl CompletionReceiver {
    /// Block until the next mapping key arrives.
    /// Returns `None` when the sentinel is received (all done).
    pub fn next(&self) -> Option<String> {
        match self.rx.recv() {
            Ok(Some(key)) => Some(key),
            Ok(None) | Err(_) => None,
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct CompletionSender {
    tx: std::sync::mpsc::Sender<Option<String>>,
}

#[allow(dead_code)]
impl CompletionSender {
    /// Signal that a map instance with `mapping_key` has completed.
    pub fn put(&self, mapping_key: String) {
        let _ = self.tx.send(Some(mapping_key));
    }

    /// Signal that all map instances are done.
    pub fn done(&self) {
        let _ = self.tx.send(None);
    }
}

#[allow(dead_code)]
pub fn completion_channel() -> (CompletionSender, CompletionReceiver) {
    let (tx, rx) = std::sync::mpsc::channel();
    (CompletionSender { tx }, CompletionReceiver { rx })
}

/// Python-facing CompletionQueue. Supports sync iteration.
/// The GIL is released while blocking on the channel, so map instances
/// running in other threads can complete.
#[pyclass(name = "CompletionQueue", module = "rivers._core")]
pub struct PyCompletionQueue {
    inner: Arc<Mutex<CompletionReceiver>>,
}

impl PyCompletionQueue {
    pub fn new(receiver: CompletionReceiver) -> Self {
        Self {
            inner: Arc::new(Mutex::new(receiver)),
        }
    }
}

#[pymethods]
impl PyCompletionQueue {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<String> {
        let inner = Arc::clone(&self.inner);
        py.detach(|| {
            let receiver = inner.lock().expect("CompletionQueue lock poisoned");
            receiver.next().ok_or_else(|| PyStopIteration::new_err(()))
        })
    }

    fn __repr__(&self) -> String {
        "CompletionQueue(...)".to_string()
    }
}
