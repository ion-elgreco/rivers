//! Per-code-location registry of in-flight GIL-attaching worker threads.
//!
//! Runs, materializations, and backfills execute on detached `std::thread`s
//! that attach the GIL to drive user asset code. One such thread still attached
//! when `Py_Finalize` runs aborts the process:
//! `Fatal Python error: PyGILState_Release: thread state ... must be current`.
//!
//! Every spawner registers its handle here instead of leaking it. The owning
//! code location (`PyCodeRepository`) holds one `GilThreads`, shared by the daemon
//! and the gRPC server, and each subsystem [`GilThreads::drain`]s it at shutdown —
//! after its own spawners are quiesced — so no worker outlives the subsystem
//! that launched it and nothing is left attached at finalize.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A shared bag of worker-thread join handles. Cheap to clone (`Arc`).
#[derive(Clone, Default)]
pub(crate) struct GilThreads {
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl GilThreads {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Spawn a GIL-attaching worker and track its handle. Already-finished
    /// handles are reaped first (dropping a finished handle detaches it
    /// instantly) so a long-lived location doesn't accumulate them.
    pub(crate) fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = std::thread::spawn(f);
        let mut handles = self.handles.lock().unwrap();
        handles.retain(|h| !h.is_finished());
        handles.push(handle);
    }

    /// Join every tracked worker. Idempotent. MUST be called with the GIL
    /// released — the workers attach the GIL, so draining while holding it
    /// would deadlock. Returns the number of threads joined.
    pub(crate) fn drain(&self) -> usize {
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        let count = handles.len();
        for h in handles {
            let _ = h.join();
        }
        count
    }
}
