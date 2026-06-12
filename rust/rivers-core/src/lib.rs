//! Core library for rivers — a Rust-powered asset orchestration engine.

pub mod assets;
pub mod composition;
pub mod concurrency;
pub mod condition;
pub mod execution;
pub mod repo;
pub mod run_backend;
pub mod staleness;
pub mod storage;
pub mod task;
pub mod timegrid;
pub mod util;

// Storage record ids are `surrealdb::types::RecordId`; re-export the crate so
// dependents can construct records without pinning the git dependency themselves.
pub use surrealdb;

pub const GRAPH_TOPOLOGY_KEY_PREFIX: &str = "graph_topology:";

/// KV key for serialized graph topology — per-CL so the UI doesn't render
/// whichever CL last called `resolve()`.
pub fn graph_topology_key(code_location_id: &str) -> String {
    format!("{GRAPH_TOPOLOGY_KEY_PREFIX}{code_location_id}")
}

pub const CONDITION_EVAL_STATE_KEY_PREFIX: &str = "condition_eval_state:";

/// KV key for the condition daemon's persisted eval state — per-CL so two
/// daemons sharing a SurrealDB don't clobber each other's snapshot.
pub fn condition_eval_state_key(code_location_id: &str) -> String {
    format!("{CONDITION_EVAL_STATE_KEY_PREFIX}{code_location_id}")
}

pub const DYNAMIC_KEYS_KEY_PREFIX: &str = "dynamic_keys:";

/// KV key for a fan-out source's mapping keys. Keyed by `data_version` so each
/// successful materialization writes to its own slot — no overwrites, no race
/// when concurrent runs materialize the same asset+partition. Downstream
/// readers resolve the upstream's current `data_version` and look up keys for
/// that specific version.
pub fn dynamic_keys_key(
    code_location_id: &str,
    asset_key: &str,
    partition: Option<&str>,
    data_version: &str,
) -> String {
    let p = partition.unwrap_or("_");
    format!("{DYNAMIC_KEYS_KEY_PREFIX}{code_location_id}:{asset_key}:{p}:{data_version}")
}
