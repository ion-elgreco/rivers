//! Partition types: keys, definitions, mappings, partition context, and backfill strategy.
pub mod backfill_strategy;
mod context;
pub mod definition;
mod key;
pub mod key_range;
pub mod mapping;

pub use backfill_strategy::PyBackfillStrategy;
pub use context::PartitionContext;
pub use definition::PartitionsDefinition;
pub use key::PyPartitionKey;
pub use key_range::{DimensionSelection, PyPartitionKeyRange};
pub use mapping::{PartitionKeySelector, PartitionMapping};

use pyo3::prelude::*;

pub fn register_partition_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "partitions", [
        PyPartitionKey as "PartitionKey",
        PyPartitionKeyRange as "PartitionKeyRange",
        PyBackfillStrategy as "BackfillStrategy",
        PartitionsDefinition as "PartitionsDefinition",
        PartitionContext as "PartitionContext",
        PartitionMapping as "PartitionMapping",
    ])
}
