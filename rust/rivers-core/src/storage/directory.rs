use serde::{Deserialize, Serialize};

/// A code location's entry in the directory: the storage identity its rows
/// are scoped by. Stored as JSON under
/// [`crate::code_location_directory_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationDirectoryEntry {
    pub identity: String,
}
