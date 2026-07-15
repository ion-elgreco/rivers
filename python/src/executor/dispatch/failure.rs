//! Failure classification for the step retry loop.

use pyo3::exceptions::{PyKeyboardInterrupt, PyMemoryError, PyTimeoutError};
use pyo3::prelude::*;
use rivers_core::execution::retry::FailureReason;

/// Classify a raised exception and collect its MRO as fully-qualified class
/// names (`module.qualname`, derived-first) for the `retry_on` allow-list.
pub(crate) fn classify_pyerr(py: Python, err: &PyErr) -> (FailureReason, Vec<String>) {
    let ty = err.get_type(py);
    let mut mro_names = Vec::new();
    for item in ty.mro().iter() {
        let module = item
            .getattr("__module__")
            .and_then(|m| m.extract::<String>());
        let qualname = item
            .getattr("__qualname__")
            .and_then(|q| q.extract::<String>());
        if let (Ok(module), Ok(qualname)) = (module, qualname) {
            mro_names.push(format!("{module}.{qualname}"));
        }
    }

    let reason = if err.is_instance_of::<PyMemoryError>(py) {
        FailureReason::OutOfMemory
    } else if err.is_instance_of::<PyTimeoutError>(py) {
        FailureReason::Timeout
    } else if err.is_instance_of::<PyKeyboardInterrupt>(py)
        || mro_names
            .iter()
            .any(|n| n == "asyncio.exceptions.CancelledError")
    {
        FailureReason::Cancelled
    } else {
        FailureReason::Error
    };
    (reason, mro_names)
}

/// Uniform-ish sample in [0, 1) for backoff jitter, from thread ID + wall
/// clock — same dependency-free approach as `pool_claim::rand_jitter`.
pub(crate) fn rng01() -> f64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos()
        .hash(&mut hasher);
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}
