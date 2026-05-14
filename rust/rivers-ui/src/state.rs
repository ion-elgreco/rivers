//! Client-side reactive state and SSR-side global context.
//!
//! On the server, provides access to the SurrealDB storage and graph topology
//! via Leptos context. On the client, holds reactive signals for UI state.

#[cfg(feature = "ssr")]
use rivers_api::rivers::code_location_service_client::CodeLocationServiceClient;
#[cfg(feature = "ssr")]
use rivers_core::assets::graph::GraphTopology;
#[cfg(feature = "ssr")]
use rivers_core::storage::surrealdb_backend::SurrealStorage;
#[cfg(feature = "ssr")]
use std::sync::Arc;
#[cfg(feature = "ssr")]
use tonic::transport::Channel;

#[cfg(feature = "ssr")]
use crate::code_location_registry::{Registry, connect_code_location};
#[cfg(feature = "ssr")]
use crate::types::CodeLocationEntry;

/// SSR-only application context: the storage handle, the code-location
/// registry, and an optional synthetic graph override. Cloned freely
/// (cheap — all fields are Arc-shared) and provided via Leptos context so
/// every server fn can reach storage and the registry.
#[cfg(feature = "ssr")]
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<SurrealStorage>,
    /// Synthetic-only override. When `Some`, every per-CL graph server
    /// function returns this fixed topology instead of reading per-CL keys
    /// from storage — used by `rivers dev --synthetic` and benchmark fixtures.
    /// `None` in production: server fns resolve the active CL identity from
    /// the registry and read `kv["graph_topology:<id>"]`.
    pub graph: Option<Arc<GraphTopology>>,
    pub registry: Registry,
}

#[cfg(feature = "ssr")]
impl AppState {
    /// Resolve a specific code location by `(namespace, name)` and dial its
    /// gRPC endpoint. Errors when the entry isn't in the registry (deleted,
    /// never existed, watcher hasn't synced yet) or when its phase isn't
    /// `Ready`.
    pub async fn connect_to(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(CodeLocationEntry, CodeLocationServiceClient<Channel>), anyhow::Error> {
        let entry = self.registry.lookup(namespace, name).await.ok_or_else(|| {
            anyhow::anyhow!("code location {namespace}/{name} not found in registry")
        })?;
        if !entry.is_ready() {
            anyhow::bail!(
                "code location {namespace}/{name} is not Ready (phase={})",
                entry.phase
            );
        }
        let client = connect_code_location(&entry.grpc_endpoint).await?;
        Ok((entry, client))
    }
}
