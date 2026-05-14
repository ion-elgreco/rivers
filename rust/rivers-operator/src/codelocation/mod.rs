//! CodeLocation controller: registry digest resolution, ownership of the
//! backing `Deployment` + `Service`, and image-pull-secret handling.

pub mod directory;
pub mod image_auth;
pub mod reconcile;
pub mod registry;
pub mod resources;

pub use directory::{DirectoryState, run_watcher as run_directory_watcher};
pub use reconcile::{Context, error_policy, reconcile};
pub use registry::RegistryClient;
