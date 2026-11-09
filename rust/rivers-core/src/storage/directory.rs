use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::StorageBackend;
use crate::assets::remote::LocationRef;

/// A code location's entry in the directory: the storage identity its rows
/// are scoped by. Stored as JSON under
/// [`crate::code_location_directory_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationDirectoryEntry {
    pub identity: String,
}

/// Storage identity of each location; `None` for a location that never
/// registered.
pub async fn resolve_code_locations<'a, S: StorageBackend>(
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
