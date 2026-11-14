//! The partitions of an asset whose latest result is a failure.

use super::PartitionKey;

/// A partition whose latest result is a failure: no materialization and no
/// deletion is at or after the failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFailure {
    pub key: PartitionKey,
    pub failed_at: i64,
    /// The run that failed.
    pub run_id: String,
    /// The partition has a row: a materialization older than the failure.
    pub has_row: bool,
}

/// How many partitions failed, and how many of them have a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FailureCount {
    pub failed: u64,
    pub with_row: u64,
}
