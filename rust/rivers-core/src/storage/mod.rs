//! Storage trait and types for persisting orchestration state.

pub mod retry;
pub mod surrealdb_backend;

mod asset;
mod backfill;
mod event;
mod partition_key;
mod pool;
mod run;
mod scoped;
mod tick;
mod traits;

pub use asset::*;
pub use backfill::*;
pub use event::*;
pub use partition_key::*;
pub use pool::*;
pub use run::*;
pub use scoped::*;
pub use tick::*;
pub use traits::*;

/// Well-known rivers tag keys used on `RunRecord.tags` and run/event metadata.
pub mod tag_keys {
    /// Run priority. Higher = dequeued first.
    pub const PRIORITY: &str = "rivers/priority";
    /// Set on a backfill's tags when created via `rerun_backfill`.
    pub const RERUN_OF: &str = "rivers/rerun_of";
}
