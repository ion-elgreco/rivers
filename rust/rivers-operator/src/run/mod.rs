//! Run controller (`runs.rivers.io`): schedules the executor pod, polls run
//! progress from storage, handles cancel/timeout transitions, restarts the
//! executor on transient failure, and cleans up step jobs on finalization.

pub mod cancel;
pub mod cleanup;
pub mod pod_builder;
pub mod progress;
pub mod reconcile;
pub mod restart;
#[cfg(test)]
pub mod test_helpers;
pub mod timeout;

pub use reconcile::{Context, error_policy, reconcile};
