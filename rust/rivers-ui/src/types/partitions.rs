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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubmitPartitionKey {
    Single(String),
    /// `(dim_name, value)` pairs, one per dimension. The cartesian-
    /// product expansion happens at the dialog layer, so each
    /// `SubmitPartitionKey::Multi` already represents one concrete
    /// partition.
    Multi(Vec<(String, String)>),
    /// Every single-dim key at index `start..=end` of `source`'s order. Expanded
    /// into `Single` keys on the server, so a range never ships its keys to the
    /// browser.
    Span {
        source: SpanSource,
        start: u64,
        end: u64,
    },
}

/// Where a [`SubmitPartitionKey::Span`]'s keys come from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SpanSource {
    Asset(String),
    Dynamic(String),
}

/// Above this many partitions, a submit launches one backfill instead of a run each.
pub const BACKFILL_THRESHOLD: u64 = 2;

/// Whether `keys` submit as one backfill: past [`BACKFILL_THRESHOLD`], or
/// holding a span, which only the backfill path expands.
pub fn launches_backfill(keys: &[SubmitPartitionKey]) -> bool {
    submit_partition_count(keys) > BACKFILL_THRESHOLD
        || keys
            .iter()
            .any(|k| matches!(k, SubmitPartitionKey::Span { .. }))
}

/// How many partitions `keys` submit.
pub fn submit_partition_count(keys: &[SubmitPartitionKey]) -> u64 {
    keys.iter()
        .map(|k| match k {
            SubmitPartitionKey::Span { start, end, .. } => end - start + 1,
            _ => 1,
        })
        .sum()
}

/// The rows of the partition outline that are read, or shown, in one step.
pub const OUTLINE_PAGE: u64 = 100;

/// The partitions of an asset, or of one row of its partition outline, by
/// state. A partition that is not materialized and not failed is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionCounts {
    pub total: u64,
    pub materialized: u64,
    pub failed: u64,
}

impl PartitionCounts {
    /// The counts of `total` partitions. The store can hold rows of keys
    /// that the asset no longer has: no state counts more than `total`.
    pub fn of(total: u64, materialized: u64, failed: u64) -> Self {
        let materialized = materialized.min(total);
        Self {
            total,
            materialized,
            failed: failed.min(total - materialized),
        }
    }

    pub fn missing(&self) -> u64 {
        self.total - self.materialized - self.failed
    }
}

/// One dimension of the partition keys of an asset.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OutlineDimension {
    /// Empty for an asset with one dimension.
    pub name: String,
    /// The number of keys.
    pub keys: u64,
    /// The dynamic set that holds the keys. Empty: the definition gives them.
    #[serde(default)]
    pub dynamic_name: String,
}

/// A run of keys of one dimension. The rows below a row of the partition
/// outline are read with the place of that row.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OutlinePlace {
    /// The value of each dimension above.
    #[serde(default)]
    pub fixed: Vec<(String, String)>,
    /// The dimension of the keys, then the dimensions below it. Empty when
    /// the code location did not give the keys: the rows are then the
    /// materialized partitions.
    #[serde(default)]
    pub dimensions: Vec<OutlineDimension>,
    /// The keys from `first_key` to `last_key`. Both empty: every key.
    #[serde(default)]
    pub first_key: String,
    #[serde(default)]
    pub last_key: String,
}

/// A row of the partition outline that opens to more rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutlineRow {
    pub label: String,
    /// What the row holds: `720 hours`, `265 × zone`.
    pub holds: String,
    pub counts: PartitionCounts,
    /// The rows below.
    pub place: OutlinePlace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionRowState {
    Materialized,
    Failed,
    Missing,
}

impl PartitionRowState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Failed => "failed",
            Self::Missing => "missing",
        }
    }
}

/// One partition as a row of the partition outline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRow {
    /// The key of the last dimension.
    pub key: String,
    pub state: PartitionRowState,
    /// The time of the last materialization. A failed partition can have one.
    pub materialized_at: Option<i64>,
    /// The run of the last materialization.
    pub run_id: Option<String>,
    /// The time of the failure of a failed partition.
    pub failed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutlineRows {
    Groups(Vec<OutlineRow>),
    Partitions(Vec<PartitionRow>),
}

impl OutlineRows {
    pub fn len(&self) -> usize {
        match self {
            Self::Groups(rows) => rows.len(),
            Self::Partitions(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The rows below one place of the partition outline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutlineLevel {
    /// The head of the first column: the dimension of the rows.
    pub heading: String,
    pub rows: OutlineRows,
    /// The keys after the rows that the level did not read.
    pub more: u64,
}

/// One bar of the strip above the partition outline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripBar {
    pub label: String,
    pub counts: PartitionCounts,
}

/// The partitions of an asset along the first dimension of its keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStrip {
    /// The dimension of the bars.
    pub heading: String,
    /// What one bar is: `one bar per day`.
    pub note: String,
    pub bars: Vec<StripBar>,
    /// The marks of the axis: the place of a bar and the text.
    pub marks: Vec<(u32, String)>,
}

/// What the partitions of an asset are: the line below their number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionShape {
    /// The dimensions of the keys, each with the number of its keys.
    Dimensions(Vec<(String, u64)>),
    /// Windows of time, one dimension, from `first` to `last`.
    Windows {
        /// The unit of one window (`day`). Empty when not known.
        unit: String,
        first: String,
        last: String,
    },
    /// Keys of one dimension.
    Keys,
}

/// The Partitions tab of an asset: the counts, the strip and the first rows
/// of the outline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOverview {
    pub counts: PartitionCounts,
    pub shape: PartitionShape,
    pub strip: Option<PartitionStrip>,
    pub top: OutlineLevel,
    /// The place of the top rows: a search reads its keys.
    pub root: OutlinePlace,
    /// The code location gave the keys. False: the rows are the materialized
    /// partitions only.
    pub from_definition: bool,
}
