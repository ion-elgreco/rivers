//! CodeLocation controller: registry digest resolution, ownership of the
//! backing `Deployment` + `Service`, and image-pull-secret handling.

pub mod directory;
pub mod git;
pub mod image_auth;
pub mod reconcile;
pub mod registry;
pub mod resources;

pub use directory::{DirectoryState, run_watcher as run_directory_watcher};
pub use reconcile::{Context, WorkspaceConfig, error_policy, reconcile};
pub use registry::{ImageRef, RegistryClient};

/// `User-Agent` of the operator's registry and git requests.
pub const USER_AGENT: &str = concat!("rivers-operator/", env!("CARGO_PKG_VERSION"));
