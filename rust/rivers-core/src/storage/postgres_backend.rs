//! PostgreSQL storage backend.
//!
//! Remote-only: there is no embedded PostgreSQL. Local development uses the
//! embedded SurrealDB backend instead.

mod migration;

pub use crate::storage::migration::{Capability, SchemaMigrationNeeded};
