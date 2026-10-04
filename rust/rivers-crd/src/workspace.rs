//! The pod contract of git-sourced CodeLocations: what the operator's pod
//! builder (rivers-k8s) emits and what `rivers-runtime` reads in the pod.

/// This tree (a subPath mount).
pub const WORKSPACE_MOUNT: &str = "/workspace";
/// The PVC root, mounted only on the code-location pod in shared mode.
pub const WORKSPACES_ROOT_MOUNT: &str = "/workspaces";
/// The shared uv cache, shared mode only.
pub const UV_CACHE_MOUNT: &str = "/uv-cache";
/// The mounted git Secret.
pub const GIT_CREDS_MOUNT: &str = "/etc/rivers/git";
pub const VENV_PATH: &str = "/workspace/venv";
/// The init container's command.
pub const SYNC_COMMAND: [&str; 2] = ["rivers-runtime", "workspace-sync"];
/// The init container that runs [`SYNC_COMMAND`].
pub const SYNC_CONTAINER: &str = "workspace";
/// The tree's `RunSource` as JSON, on every pod that runs from a tree and
/// on the init container that builds it.
pub const ENV_RUN_SOURCE: &str = "RIVERS_RUN_SOURCE";
