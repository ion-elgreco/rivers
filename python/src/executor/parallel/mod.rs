//! Parallel executor — runs independent steps concurrently via loky process pool.
mod collect_specs;
mod execute;
mod loky;
pub(crate) mod refs;
pub(crate) mod worker;
pub(crate) mod worker_args;

pub(crate) use execute::ParallelBackend;
