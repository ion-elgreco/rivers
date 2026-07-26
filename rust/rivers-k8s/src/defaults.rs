pub const MODULE: &str = "definitions";
pub const SURREAL_ENDPOINT: &str = "ws://surrealdb.rivers.svc:8000";
pub const NAMESPACE: &str = "rivers";
pub const SERVICE_ACCOUNT: &str = "rivers-executor";
pub const RUN_CPU: &str = "500m";
pub const RUN_MEMORY: &str = "512Mi";
pub const WORKER_CPU: &str = RUN_CPU;
pub const WORKER_MEMORY: &str = RUN_MEMORY;
pub const MAX_RESTARTS: u32 = 3;
pub const CANCEL_GRACE_PERIOD: u64 = 300;
pub const MAX_CONCURRENT_STEPS: u32 = 10;
/// Fallback runtime image for git-sourced CodeLocations. The chart overrides
/// this via `RIVERS_RUNTIME_IMAGE` with a version-pinned per-Python tag.
pub const RUNTIME_IMAGE: &str = "ghcr.io/ion-elgreco/rivers-runtime:latest";
