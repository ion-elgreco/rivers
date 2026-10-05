use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDefinitionInfo {
    pub kind: String,
    /// Single-dim enumerable keys (Static, TimeWindow). Empty for Multi
    /// (the per-dim keys live in `dimensions`) and for Dynamic.
    pub keys: Vec<String>,
    /// Per-dimension keys for Multi partitions. Empty for non-Multi
    /// kinds. The dialog renders one selector per dimension.
    pub dimensions: Vec<PartitionDimensionInfo>,
    /// Total partition count. `keys` may be a bounded window (see `keys_truncated`).
    /// For Dynamic this is sourced from storage (the def-level count is 0).
    pub total_count: u64,
    /// True when `keys` is a truncated window of the full single-dim key set.
    pub keys_truncated: bool,
    /// For Dynamic partitions, the namespace to query storage with; else empty.
    #[serde(default)]
    pub dynamic_name: String,
}

impl PartitionDefinitionInfo {
    /// The storage namespace for a Dynamic (storage-managed) partition set, or
    /// `None` for definition-derived kinds (Static/TimeWindow/Multi). Single
    /// source of truth for "is this storage-managed, and under what name".
    pub fn dynamic_namespace(&self) -> Option<&str> {
        (self.kind == "Dynamic" && !self.dynamic_name.is_empty())
            .then_some(self.dynamic_name.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDimensionInfo {
    pub name: String,
    /// A bounded window of this dimension's keys (see `keys_truncated`).
    pub keys: Vec<String>,
    /// True size of this dimension; `keys` may be a window of it.
    #[serde(default)]
    pub total_count: u64,
    /// True when `keys` is a truncated window — the picker pages the dimension.
    #[serde(default)]
    pub keys_truncated: bool,
}

/// Structured partition key for UI → gRPC submissions. Mirrors the
/// shape of `rivers_api::ProtoPartitionKey` so the wire format carries
/// `Single` vs `Multi` explicitly instead of relying on string parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitPartitionKey {
    Single(String),
    /// `(dim_name, value)` pairs, one per dimension. The cartesian-
    /// product expansion happens at the dialog layer, so each
    /// `SubmitPartitionKey::Multi` already represents one concrete
    /// partition.
    Multi(Vec<(String, String)>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub asset_key: String,
    pub total_partitions: usize,
    pub materialized: usize,
    pub failed: usize,
    pub missing: usize,
    pub partition_details: Vec<PartitionDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub key: String,
    pub status: String,
    pub last_timestamp: Option<i64>,
}
