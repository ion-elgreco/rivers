use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::StorageBackend;
use crate::assets::remote::LocationRef;

/// A code location's entry in the directory. Stored as JSON under
/// [`crate::code_location_directory_key`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationDirectoryEntry {
    /// The storage identity the location's rows are scoped by.
    pub identity: String,
    /// Where the location serves gRPC, if it says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_endpoint: Option<String>,
    /// The Python module the location loads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// When the location last showed it is alive, in nanoseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<i64>,
    /// The remote assets the location consumes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remotes: Vec<RemoteConsumer>,
}

impl CodeLocationDirectoryEntry {
    pub fn new(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
            ..Self::default()
        }
    }
}

/// A remote asset, as the location that consumes it declares it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteConsumer {
    /// The name in the consuming location.
    pub local: String,
    /// The owning location.
    pub code_location: LocationRef,
    /// The asset's key in the owning location.
    pub asset_key: String,
    /// The consuming location's assets that depend on it.
    #[serde(default)]
    pub downstream: Vec<String>,
}

/// Storage identity of each location; `None` for a location that never
/// registered.
pub async fn resolve_code_locations<'a, S: StorageBackend + ?Sized>(
    storage: &S,
    locations: impl IntoIterator<Item = &'a LocationRef>,
) -> anyhow::Result<HashMap<LocationRef, Option<String>>> {
    let mut out = HashMap::new();
    for location in locations {
        if !out.contains_key(location) {
            let identity = storage.get_code_location_identity(location).await?;
            out.insert(location.clone(), identity);
        }
    }
    Ok(out)
}
