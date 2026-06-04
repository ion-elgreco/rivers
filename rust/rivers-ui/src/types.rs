//! DTO types for the UI — mirrors rivers-core types without surrealdb dependencies.
//! Used in server function signatures so they work on both SSR and WASM hydration targets.

use serde::{Deserialize, Serialize};

#[cfg(feature = "ssr")]
fn partition_key_to_display(pk: rivers_core::storage::PartitionKey) -> String {
    match pk {
        rivers_core::storage::PartitionKey::Single { keys } => {
            keys.first().cloned().unwrap_or_default()
        }
        rivers_core::storage::PartitionKey::Multi { dims } => {
            let mut sorted = dims;
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            sorted
                .iter()
                .map(|(dim, vals)| format!("{}={}", dim, vals.join(",")))
                .collect::<Vec<_>>()
                .join("|")
        }
    }
}

/// Mirrors `rivers_core::storage::StaleStatus`. Computed on demand via
/// `staleness::compute_staleness` over the records + topology — never persisted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StaleStatus {
    UpToDate,
    Stale,
    #[default]
    Missing,
}

/// Asset registration + last-materialization snapshot. Mirrors
/// `rivers_core::storage::AssetRecord` (sans the per-CL identity field —
/// scoping happens server-side).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetRecord {
    pub asset_key: String,
    pub tags: Vec<String>,
    pub kinds: Vec<String>,
    pub asset_group: Option<String>,
    pub code_version: Option<String>,
    pub last_event_id: Option<String>,
    pub last_run_id: Option<String>,
    pub last_timestamp: Option<i64>,
    pub last_data_version: Option<String>,
    pub pool: Vec<(String, u32)>,
    #[serde(default)]
    pub stale_status: StaleStatus,
}

/// One code location discovered via the operator's `CodeLocationRegistry`.
/// Mirrors the proto `CodeLocationEntry` shape so it serializes cleanly to
/// both SSR responses and WASM-side hydration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationEntry {
    pub namespace: String,
    pub name: String,
    /// In-cluster DNS + port of the backing Service, e.g. `analytics.team-data.svc:3001`.
    /// No URL scheme — callers prepend `http://` when dialing.
    pub grpc_endpoint: String,
    pub image: String,
    pub module: String,
    /// "Pending" | "Deploying" | "Ready" | "Failed". Only `Ready` entries are
    /// safe to dial.
    pub phase: String,
    pub observed_generation: i64,
    /// Stable identity (UUID) from `CodeLocation.spec.identity`.
    pub identity: String,
}

impl CodeLocationEntry {
    /// True when the operator-reported `phase` is exactly `"Ready"` — the
    /// only state where the entry is safe to dial. Other phases (`Pending`,
    /// `Deploying`, `Failed`) mean the backing pod isn't serving yet.
    pub fn is_ready(&self) -> bool {
        self.phase == "Ready"
    }
}

/// Lifecycle states a run progresses through. `Queued` → `NotStarted` →
/// `Started` → terminal (`Success` / `Failure` / `Canceled`). Mirrors
/// `rivers_core::storage::RunStatus`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RunStatus {
    Queued,
    NotStarted,
    Started,
    Success,
    Failure,
    Canceled,
}

/// Origin of a run — mirrors `rivers_core::storage::LaunchedBy`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchedBy {
    #[default]
    Manual,
    Schedule {
        name: String,
    },
    Sensor {
        name: String,
    },
    Backfill {
        backfill_id: String,
    },
    Condition,
}

/// One row in the runs table — produced by every `materialize` /
/// `execute_job` / queued backfill submission. Mirrors
/// `rivers_core::storage::RunRecord`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    /// `None` for ad-hoc runs (e.g. `repo.materialize()`, asset-selection
    /// sensors). `Some` when the run targets a user-defined `Job`.
    #[serde(default)]
    pub job_name: Option<String>,
    pub status: RunStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub tags: Vec<(String, String)>,
    pub node_names: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub partition_key: Option<String>,
    #[serde(default)]
    pub block_reason: Option<String>,
    #[serde(default)]
    pub launched_by: LaunchedBy,
    #[serde(default)]
    pub code_location_id: String,
}

/// Filter passed to the paginated runs server fn. Empty/`None` means no
/// restriction on that dimension. Mirrors `rivers_core::storage::RunFilter`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFilter {
    #[serde(default)]
    pub status: Option<RunStatus>,
    /// Exact `job_name` match. Used by the job-detail page.
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_substring: Option<String>,
    #[serde(default)]
    pub asset_substring: Option<String>,
    #[serde(default)]
    pub partition_substring: Option<String>,
}

/// One page of runs plus the total number of rows matching the filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunsPage {
    pub rows: Vec<RunRecord>,
    pub total: u64,
}

/// Aggregate run counts for the runs-list page header.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub queued: u64,
    pub failure: u64,
    pub success: u64,
    pub last_24h: u64,
}

/// Discriminator for [`StoredEvent`]. Mirrors
/// `rivers_core::storage::EventType` (without payload variants — the UI
/// reads payload fields as separate columns).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    Materialization,
    Observation,
    StepStart,
    StepSuccess,
    StepFailure,
    LogOutput,
    RunQueued,
    RunDequeued,
    StepSlotClaimed,
    StepSlotWaiting,
    StepSlotRenewed,
    StepSlotReleased,
}

/// One row from the events table — drives the run-detail / asset-detail
/// timelines. Mirrors `rivers_core::storage::EventRecord`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub id: String,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<String>,
    pub timestamp: i64,
    pub metadata: Vec<(String, MetadataDisplay)>,
    pub data_version: Option<String>,
}

/// Asset DAG topology read from the per-CL storage blob. `edges` are
/// `(from, to)` where `from` depends on `to`. Mirrors
/// `rivers_core::assets::graph::GraphTopology`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphTopology {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<(String, String)>,
}

impl GraphTopology {
    /// Compute (ancestors, descendants) of `node_name` from the edge list.
    /// Ancestors = transitive dependencies; descendants = transitive dependents.
    pub fn lineage(&self, node_name: &str) -> (Vec<String>, Vec<String>) {
        use std::collections::{HashMap, HashSet};

        let mut deps_of: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut dependents_of: HashMap<&str, Vec<&str>> = HashMap::new();
        for (from, to) in &self.edges {
            deps_of.entry(from.as_str()).or_default().push(to.as_str());
            dependents_of
                .entry(to.as_str())
                .or_default()
                .push(from.as_str());
        }

        let mut ancestors = HashSet::new();
        let mut stack: Vec<&str> = deps_of.get(node_name).cloned().unwrap_or_default();
        while let Some(n) = stack.pop() {
            if ancestors.insert(n.to_string())
                && let Some(deps) = deps_of.get(n)
            {
                stack.extend(deps.iter());
            }
        }

        let mut descendants = HashSet::new();
        let mut stack: Vec<&str> = dependents_of.get(node_name).cloned().unwrap_or_default();
        while let Some(n) = stack.pop() {
            if descendants.insert(n.to_string())
                && let Some(deps) = dependents_of.get(n)
            {
                stack.extend(deps.iter());
            }
        }

        let mut anc: Vec<String> = ancestors.into_iter().collect();
        let mut desc: Vec<String> = descendants.into_iter().collect();
        anc.sort();
        desc.sort();
        (anc, desc)
    }

    /// Direct upstream dependencies (one hop) of `node_name`.
    ///
    /// Edges are `(from, to)` where `from` depends on `to`, so the upstream of
    /// `node_name` are the `to` of edges where it is the `from`.
    pub fn direct_upstream(&self, node_name: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|(from, _)| from == node_name)
            .map(|(_, to)| to.clone())
            .collect()
    }

    /// Direct downstream dependents (one hop) of `node_name`.
    ///
    /// The downstream of `node_name` are the `from` of edges where it is the
    /// `to` (i.e. the assets that depend on it).
    pub fn direct_downstream(&self, node_name: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|(_, to)| to == node_name)
            .map(|(from, _)| from.clone())
            .collect()
    }

    /// Return a new topology with collapsed graph assets.
    /// Nodes whose `parent_graph` points to a graph asset NOT in `expanded` are removed.
    /// Edges to/from removed nodes are rewired to their parent graph asset node.
    pub fn collapsed(&self, expanded: &std::collections::HashSet<String>) -> GraphTopology {
        use std::collections::{HashMap, HashSet};

        let mut remap: HashMap<&str, &str> = HashMap::new();
        for node in &self.nodes {
            if let Some(ref parent) = node.parent_graph
                && !expanded.contains(parent)
            {
                remap.insert(&node.name, parent.as_str());
            }
        }

        let hidden: HashSet<&str> = remap.keys().copied().collect();
        let nodes: Vec<TopologyNode> = self
            .nodes
            .iter()
            .filter(|n| !hidden.contains(n.name.as_str()))
            .cloned()
            .collect();

        let visible: HashSet<&str> = nodes.iter().map(|n| n.name.as_str()).collect();

        let mut edges: Vec<(String, String)> = Vec::new();
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        for (from, to) in &self.edges {
            let new_from = remap.get(from.as_str()).copied().unwrap_or(from.as_str());
            let new_to = remap.get(to.as_str()).copied().unwrap_or(to.as_str());
            // Self-edges arise from internal edges within a collapsed graph.
            if new_from == new_to {
                continue;
            }
            if !visible.contains(new_from) || !visible.contains(new_to) {
                continue;
            }
            let edge = (new_from.to_string(), new_to.to_string());
            if seen_edges.insert(edge.clone()) {
                edges.push(edge);
            }
        }
        edges.sort();

        GraphTopology { nodes, edges }
    }
}

/// One node in [`GraphTopology`]. `kind` is one of `"asset"`, `"task"`,
/// `"graph_asset"`. `parent_graph` is `Some(name)` when the node lives
/// inside a graph asset (its name appears in that asset's
/// `inner_invocations`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub parent_graph: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub name: String,
    pub cron_schedule: String,
    pub cron_description: Option<String>,
    pub job_name: String,
    pub status: String,
    pub timezone: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorRecord {
    pub name: String,
    pub job_name: Option<String>,
    pub status: String,
    pub minimum_interval: Option<String>,
    pub description: Option<String>,
    pub asset_selection: Vec<String>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub name: String,
    pub asset_selection: Vec<String>,
    pub executor_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetDefinitionInfo {
    pub asset_key: String,
    pub description: Option<String>,
    pub partition_def: Option<PartitionDefinitionInfo>,
    pub hooks: Vec<HookInfo>,
    pub io_handler: Option<String>,
    pub has_self_dependency: bool,
    pub is_external: bool,
    pub automation_condition: Option<String>,
    pub tags: Vec<String>,
    pub kinds: Vec<String>,
    pub group: Option<String>,
    pub code_version: Option<String>,
    #[serde(default)]
    pub asset_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct HookInfo {
    pub hook_type: String,
    pub function_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickRecord {
    pub id: String,
    pub automation_name: String,
    pub automation_type: String,
    pub status: String,
    pub timestamp: i64,
    pub run_ids: Vec<String>,
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    pub skip_reason: Option<String>,
    pub error: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    True,
    False,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalNodeResult {
    pub node_idx: u32,
    pub label: String,
    pub node_type: String,
    pub status: NodeStatus,
    pub children: Vec<EvalNodeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_partitions: Option<usize>,
}

/// Detail of an expanded condition tick — per-asset evaluations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConditionTickDetail {
    pub evals: Vec<ConditionEvalRecord>,
}

/// A global condition evaluation tick — summary of one daemon evaluation cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionTickRecord {
    pub id: String,
    pub timestamp: i64,
    pub total_evaluated: u32,
    pub total_fired: u32,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    /// Populated at read-time by joining `BackfillRecord.create_time` within a
    /// short window after `timestamp`. Empty in storage.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
}

/// A per-asset condition evaluation record linked to a global tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionEvalRecord {
    pub id: String,
    pub asset_key: String,
    pub tick_id: String,
    pub timestamp: i64,
    pub fired: bool,
    pub eval_duration_us: u64,
    /// Populated at read-time from the join `RunRecord.node_names → run_id`.
    /// Stored eval records always have this empty.
    #[serde(default)]
    pub run_ids: Vec<String>,
    /// Populated at read-time from the join `BackfillRecord.asset_selection → backfill_id`.
    /// Only backfill-driven materializations (multi-partition) have these.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    pub tree: EvalNodeResult,
    /// Which partitions were selected (None for unpartitioned assets).
    pub selected_partitions: Option<Vec<String>>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunStats {
    pub total: usize,
    pub success: usize,
    pub failure: usize,
    pub started: usize,
    pub not_started: usize,
    pub queued: usize,
    pub canceled: usize,
}

/// Filter passed to the paginated backfills server fn. Empty/`None` means no
/// restriction. Mirrors `rivers_core::storage::BackfillFilter`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackfillFilter {
    #[serde(default)]
    pub status: Option<String>,
}

/// One page of backfills plus the total row count matching the filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillsPage {
    pub rows: Vec<BackfillInfo>,
    pub total: u64,
}

/// Aggregate backfill counts for the list-page status pills.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub completed_success: u64,
    pub completed_failed: u64,
    pub canceled: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillInfo {
    pub backfill_id: String,
    pub status: String,
    pub strategy: String,
    pub asset_selection: Vec<String>,
    pub total_partitions: u32,
    pub completed_partitions: u32,
    pub failed_partitions: u32,
    pub canceled_partitions: u32,
    pub max_concurrency: u32,
    pub run_ids: Vec<String>,
    pub tags: Vec<(String, String)>,
    pub create_time: i64,
    pub end_time: Option<i64>,
    pub error: Option<String>,
    #[serde(default)]
    pub code_location_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    pub claimed_count: u32,
    pub pending_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDetail {
    pub info: PoolInfo,
    pub holders: Vec<SlotHolder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataDisplay {
    Text(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Url {
        text: String,
        url: String,
    },
    Path(String),
    Json(String),
    Markdown(String),
    CodeBlock {
        code: String,
        language: Option<String>,
    },
    Sql(String),
    Image(String),
    Timestamp(i64),
    Duration(f64),
    DateRange {
        start: i64,
        end: i64,
    },
    Bytes(u64),
    Percentage(f64),
    Schema(Vec<(String, String)>),
    DataVersion(String),
    Null,
}

impl MetadataDisplay {
    /// Deserialize a JSON-encoded MetadataValue from storage.
    /// Falls back to `Text` for internal events (errors, log output) stored as plain strings.
    pub fn from_stored(raw: &str) -> Self {
        serde_json::from_str::<StoredMetadataValue>(raw)
            .map(Into::into)
            .unwrap_or_else(|_| Self::Text(raw.to_string()))
    }

    /// Extract a plain text representation (for log output, error messages, etc.).
    pub fn as_text(&self) -> String {
        match self {
            Self::Text(s)
            | Self::Path(s)
            | Self::Json(s)
            | Self::Markdown(s)
            | Self::Sql(s)
            | Self::Image(s)
            | Self::DataVersion(s) => s.clone(),
            Self::Url { text, .. } => text.clone(),
            Self::CodeBlock { code, .. } => code.clone(),
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format!("{n}"),
            Self::Bool(b) => b.to_string(),
            Self::Timestamp(t) => t.to_string(),
            Self::Duration(d) => format!("{d}s"),
            Self::DateRange { start, end } => format!("{start} - {end}"),
            Self::Bytes(b) => b.to_string(),
            Self::Percentage(p) => format!("{:.1}%", p * 100.0),
            Self::Schema(cols) => cols
                .iter()
                .map(|(n, t)| format!("{n}: {t}"))
                .collect::<Vec<_>>()
                .join(", "),
            Self::Null => String::new(),
        }
    }
}

/// Mirror of Python's `MetadataValue` serde layout for deserialization.
#[derive(Deserialize)]
enum StoredMetadataValue {
    Text {
        value: String,
    },
    Int {
        value: i64,
    },
    Float {
        value: f64,
    },
    Bool {
        value: bool,
    },
    Url {
        value: String,
    },
    Path {
        value: String,
    },
    Json {
        value: String,
    },
    Markdown {
        value: String,
    },
    Timestamp {
        value: f64,
    },
    Null {},
    Bytes {
        value: u64,
    },
    Duration {
        value: f64,
    },
    Sql {
        query: String,
        #[allow(dead_code)]
        dialect: Option<String>,
    },
    CodeBlock {
        code: String,
        language: Option<String>,
    },
    Image {
        value: String,
    },
    Percentage {
        value: f64,
    },
    List {
        values: Vec<StoredMetadataValue>,
    },
    DateRange {
        start: String,
        end: String,
    },
    Schema {
        #[allow(dead_code)]
        ipc_bytes: Vec<u8>,
    },
    DataVersion {
        value: String,
    },
}

impl From<StoredMetadataValue> for MetadataDisplay {
    fn from(v: StoredMetadataValue) -> Self {
        match v {
            StoredMetadataValue::Text { value } => Self::Text(value),
            StoredMetadataValue::Int { value } => Self::Int(value),
            StoredMetadataValue::Float { value } => Self::Float(value),
            StoredMetadataValue::Bool { value } => Self::Bool(value),
            StoredMetadataValue::Url { value } => Self::Url {
                text: value.clone(),
                url: value,
            },
            StoredMetadataValue::Path { value } => Self::Path(value),
            StoredMetadataValue::Json { value } => Self::Json(value),
            StoredMetadataValue::Markdown { value } => Self::Markdown(value),
            StoredMetadataValue::Timestamp { value } => Self::Timestamp((value * 1e9) as i64),
            StoredMetadataValue::Null {} => Self::Null,
            StoredMetadataValue::Bytes { value } => Self::Bytes(value),
            StoredMetadataValue::Duration { value } => Self::Duration(value),
            StoredMetadataValue::Sql { query, .. } => Self::Sql(query),
            StoredMetadataValue::CodeBlock { code, language } => Self::CodeBlock { code, language },
            StoredMetadataValue::Image { value } => Self::Image(value),
            StoredMetadataValue::Percentage { value } => Self::Percentage(value),
            StoredMetadataValue::List { values } => {
                let text = values
                    .into_iter()
                    .map(|v| MetadataDisplay::from(v).as_text())
                    .collect::<Vec<_>>()
                    .join(", ");
                Self::Text(format!("[{text}]"))
            }
            StoredMetadataValue::DateRange { start, end } => {
                let s = chrono::NaiveDateTime::parse_from_str(&start, "%Y-%m-%dT%H:%M:%S")
                    .map(|d| d.and_utc().timestamp_nanos_opt().unwrap_or(0))
                    .unwrap_or(0);
                let e = chrono::NaiveDateTime::parse_from_str(&end, "%Y-%m-%dT%H:%M:%S")
                    .map(|d| d.and_utc().timestamp_nanos_opt().unwrap_or(0))
                    .unwrap_or(0);
                Self::DateRange { start: s, end: e }
            }
            StoredMetadataValue::Schema { ipc_bytes: _ } => {
                // Arrow IPC decoding requires arrow-ipc, which is SSR-only.
                Self::Text("[Arrow Schema]".to_string())
            }
            StoredMetadataValue::DataVersion { value } => Self::DataVersion(value),
        }
    }
}

#[cfg(feature = "ssr")]
mod conversions {
    use super::*;

    impl AssetRecord {
        /// Build a UI DTO from a core record plus a staleness value computed
        /// for it via `staleness::compute_staleness`. Staleness is not on the
        /// core record itself — it depends on the rest of the graph and is
        /// computed once per request, then merged into each row.
        pub fn from_core_with_staleness(
            r: rivers_core::storage::AssetRecord,
            stale_status: StaleStatus,
        ) -> Self {
            Self {
                asset_key: r.asset_key,
                tags: r.tags,
                kinds: r.kinds,
                asset_group: r.asset_group,
                code_version: r.code_version,
                last_event_id: r.last_event_id,
                last_run_id: r.last_run_id,
                last_timestamp: r.last_timestamp,
                last_data_version: r.last_data_version,
                pool: r.pool,
                stale_status,
            }
        }
    }

    impl From<rivers_core::storage::StaleStatus> for StaleStatus {
        fn from(s: rivers_core::storage::StaleStatus) -> Self {
            match s {
                rivers_core::storage::StaleStatus::UpToDate => Self::UpToDate,
                rivers_core::storage::StaleStatus::Stale => Self::Stale,
                rivers_core::storage::StaleStatus::Missing => Self::Missing,
            }
        }
    }

    impl From<rivers_core::storage::RunStatus> for RunStatus {
        fn from(s: rivers_core::storage::RunStatus) -> Self {
            match s {
                rivers_core::storage::RunStatus::Queued => Self::Queued,
                rivers_core::storage::RunStatus::NotStarted => Self::NotStarted,
                rivers_core::storage::RunStatus::Started => Self::Started,
                rivers_core::storage::RunStatus::Success => Self::Success,
                rivers_core::storage::RunStatus::Failure => Self::Failure,
                rivers_core::storage::RunStatus::Canceled => Self::Canceled,
            }
        }
    }

    impl From<rivers_core::storage::LaunchedBy> for LaunchedBy {
        fn from(l: rivers_core::storage::LaunchedBy) -> Self {
            match l {
                rivers_core::storage::LaunchedBy::Manual => Self::Manual,
                rivers_core::storage::LaunchedBy::Schedule { name } => Self::Schedule { name },
                rivers_core::storage::LaunchedBy::Sensor { name } => Self::Sensor { name },
                rivers_core::storage::LaunchedBy::Backfill { backfill_id } => {
                    Self::Backfill { backfill_id }
                }
                rivers_core::storage::LaunchedBy::Condition => Self::Condition,
            }
        }
    }

    impl From<rivers_core::storage::RunRecord> for RunRecord {
        fn from(r: rivers_core::storage::RunRecord) -> Self {
            let partition_key = r.partition_key.as_ref().map(|pk| match pk {
                rivers_core::storage::PartitionKey::Single { keys } => keys.join("|"),
                rivers_core::storage::PartitionKey::Multi { dims } => dims
                    .iter()
                    .map(|(d, ks)| format!("{d}={}", ks.join("|")))
                    .collect::<Vec<_>>()
                    .join(", "),
            });
            Self {
                run_id: r.run_id,
                job_name: r.job_name,
                status: r.status.into(),
                start_time: r.start_time,
                end_time: r.end_time,
                tags: r.tags,
                node_names: r.node_names,
                priority: r.priority,
                partition_key,
                block_reason: r.block_reason,
                launched_by: r.launched_by.into(),
                code_location_id: r.code_location_id,
            }
        }
    }

    impl From<rivers_core::storage::RunsSummary> for RunsSummary {
        fn from(s: rivers_core::storage::RunsSummary) -> Self {
            Self {
                total: s.total,
                in_progress: s.in_progress,
                queued: s.queued,
                failure: s.failure,
                success: s.success,
                last_24h: s.last_24h,
            }
        }
    }

    impl From<rivers_core::storage::RunsPage> for RunsPage {
        fn from(p: rivers_core::storage::RunsPage) -> Self {
            Self {
                rows: p.rows.into_iter().map(Into::into).collect(),
                total: p.total,
            }
        }
    }

    impl From<RunStatus> for rivers_core::storage::RunStatus {
        fn from(s: RunStatus) -> Self {
            match s {
                RunStatus::Queued => Self::Queued,
                RunStatus::NotStarted => Self::NotStarted,
                RunStatus::Started => Self::Started,
                RunStatus::Success => Self::Success,
                RunStatus::Failure => Self::Failure,
                RunStatus::Canceled => Self::Canceled,
            }
        }
    }

    impl From<RunFilter> for rivers_core::storage::RunFilter {
        fn from(f: RunFilter) -> Self {
            Self {
                status: f.status.map(Into::into),
                job_name: f.job_name.filter(|s| !s.is_empty()),
                job_substring: f.job_substring.filter(|s| !s.is_empty()),
                asset_substring: f.asset_substring.filter(|s| !s.is_empty()),
                partition_substring: f.partition_substring.filter(|s| !s.is_empty()),
            }
        }
    }

    impl From<rivers_core::storage::EventType> for EventType {
        fn from(e: rivers_core::storage::EventType) -> Self {
            match e {
                rivers_core::storage::EventType::Materialization { .. } => Self::Materialization,
                rivers_core::storage::EventType::Observation { .. } => Self::Observation,
                rivers_core::storage::EventType::StepStart => Self::StepStart,
                rivers_core::storage::EventType::StepSuccess => Self::StepSuccess,
                rivers_core::storage::EventType::StepFailure => Self::StepFailure,
                rivers_core::storage::EventType::LogOutput => Self::LogOutput,
                rivers_core::storage::EventType::RunQueued => Self::RunQueued,
                rivers_core::storage::EventType::RunDequeued => Self::RunDequeued,
                rivers_core::storage::EventType::StepSlotClaimed => Self::StepSlotClaimed,
                rivers_core::storage::EventType::StepSlotWaiting => Self::StepSlotWaiting,
                rivers_core::storage::EventType::StepSlotRenewed => Self::StepSlotRenewed,
                rivers_core::storage::EventType::StepSlotReleased => Self::StepSlotReleased,
            }
        }
    }

    impl From<rivers_core::storage::StoredEvent> for StoredEvent {
        fn from(e: rivers_core::storage::StoredEvent) -> Self {
            let data_version = e.event_type.data_version().map(|s| s.to_string());
            Self {
                id: format!("{:?}", e.id),
                event_type: e.event_type.into(),
                asset_key: e.asset_key,
                run_id: e.run_id,
                partition_key: e.partition_key.map(|pk| format!("{:?}", pk)),
                timestamp: e.timestamp,
                metadata: e
                    .metadata
                    .into_iter()
                    .map(|(k, v)| (k, MetadataDisplay::from_stored(&v)))
                    .collect(),
                data_version,
            }
        }
    }

    impl From<rivers_core::assets::graph::GraphTopology> for GraphTopology {
        fn from(g: rivers_core::assets::graph::GraphTopology) -> Self {
            Self {
                nodes: g
                    .nodes
                    .into_iter()
                    .map(|n| TopologyNode {
                        name: n.name,
                        kind: n.kind.as_str().to_string(),
                        group: n.group,
                        parent_graph: n.parent_graph,
                    })
                    .collect(),
                edges: g.edges,
            }
        }
    }

    impl From<rivers_core::condition::NodeStatus> for NodeStatus {
        fn from(s: rivers_core::condition::NodeStatus) -> Self {
            match s {
                rivers_core::condition::NodeStatus::True => Self::True,
                rivers_core::condition::NodeStatus::False => Self::False,
                rivers_core::condition::NodeStatus::Skipped => Self::Skipped,
            }
        }
    }

    impl From<rivers_core::condition::EvalNodeResult> for EvalNodeResult {
        fn from(t: rivers_core::condition::EvalNodeResult) -> Self {
            Self {
                node_idx: t.node_idx,
                label: t.label,
                node_type: t.node_type,
                status: t.status.into(),
                children: t.children.into_iter().map(Into::into).collect(),
                num_partitions: t.num_partitions,
            }
        }
    }

    impl ConditionTickRecord {
        pub fn from_stored(t: rivers_core::storage::StoredConditionTick) -> Self {
            Self {
                // Must match the format used by record_id_str() in the daemon,
                // since that's what's stored in condition_evals.tick_id.
                id: format!("{}:{:?}", t.id.table.as_str(), t.id.key),
                timestamp: t.timestamp,
                total_evaluated: t.total_evaluated,
                total_fired: t.total_fired,
                eval_duration_us: t.eval_duration_us,
                run_ids: t.run_ids,
                backfill_ids: t.backfill_ids,
            }
        }
    }

    impl ConditionEvalRecord {
        pub fn from_stored(e: rivers_core::storage::StoredConditionEval) -> Self {
            let tree: rivers_core::condition::EvalNodeResult = serde_json::from_slice(&e.tree_json)
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        target: "rivers::ui",
                        asset_key = %e.asset_key,
                        error = %err,
                        "failed to deserialize condition eval tree"
                    );
                    rivers_core::condition::EvalNodeResult {
                        node_idx: 0,
                        label: "parse error".into(),
                        node_type: "Leaf".into(),
                        status: rivers_core::condition::NodeStatus::False,
                        children: vec![],
                        num_partitions: None,
                    }
                });
            Self {
                id: format!("{:?}", e.id),
                asset_key: e.asset_key,
                tick_id: e.tick_id,
                timestamp: e.timestamp,
                fired: e.fired,
                eval_duration_us: e.eval_duration_us,
                run_ids: e.run_ids,
                backfill_ids: Vec::new(),
                tree: tree.into(),
                selected_partitions: e.selection_json.and_then(|json| {
                    serde_json::from_slice::<rivers_core::condition::PartitionSelection>(&json)
                        .ok()
                        .and_then(|sel| match sel {
                            rivers_core::condition::PartitionSelection::Keys(keys) => Some(
                                keys.into_iter()
                                    .map(|pk| partition_key_to_display(pk))
                                    .collect(),
                            ),
                            rivers_core::condition::PartitionSelection::All => None,
                            rivers_core::condition::PartitionSelection::Empty => Some(vec![]),
                        })
                }),
            }
        }
    }

    impl From<rivers_core::storage::PoolInfo> for PoolInfo {
        fn from(p: rivers_core::storage::PoolInfo) -> Self {
            Self {
                pool_key: p.pool_key,
                slot_limit: p.slot_limit,
                lease_duration_secs: p.lease_duration_secs,
                claimed_count: p.claimed_count,
                pending_count: p.pending_count,
            }
        }
    }

    impl From<rivers_core::storage::SlotHolder> for SlotHolder {
        fn from(h: rivers_core::storage::SlotHolder) -> Self {
            Self {
                run_id: h.run_id,
                step_key: h.step_key,
                slots_consumed: h.slots_consumed,
                claimed_at: h.claimed_at,
                lease_expires_at: h.lease_expires_at,
            }
        }
    }

    impl From<rivers_core::storage::BackfillRecord> for BackfillInfo {
        fn from(b: rivers_core::storage::BackfillRecord) -> Self {
            let strategy = match &b.strategy {
                rivers_core::storage::BackfillStrategy::MultiRun => "MultiRun".to_string(),
                rivers_core::storage::BackfillStrategy::SingleRun => "SingleRun".to_string(),
                rivers_core::storage::BackfillStrategy::PerDimension {
                    multi_run,
                    single_run,
                } => format!("PerDimension(multi_run={multi_run:?}, single_run={single_run:?})"),
            };
            Self {
                backfill_id: b.backfill_id,
                status: format!("{:?}", b.status),
                strategy,
                asset_selection: b.asset_selection,
                total_partitions: b.partition_keys.len() as u32,
                completed_partitions: b.completed_partitions.len() as u32,
                failed_partitions: b.failed_partitions.len() as u32,
                canceled_partitions: b.canceled_partitions.len() as u32,
                max_concurrency: b.max_concurrency as u32,
                run_ids: b.run_ids,
                tags: b.tags,
                create_time: b.create_time,
                end_time: b.end_time,
                error: b.error,
                code_location_id: b.code_location_id,
            }
        }
    }

    impl From<rivers_core::storage::BackfillsPage> for BackfillsPage {
        fn from(p: rivers_core::storage::BackfillsPage) -> Self {
            Self {
                rows: p.rows.into_iter().map(Into::into).collect(),
                total: p.total,
            }
        }
    }

    impl From<rivers_api::rivers::CodeLocationEntry> for CodeLocationEntry {
        fn from(e: rivers_api::rivers::CodeLocationEntry) -> Self {
            Self {
                namespace: e.namespace,
                name: e.name,
                grpc_endpoint: e.grpc_endpoint,
                image: e.image,
                module: e.module,
                phase: e.phase,
                observed_generation: e.observed_generation,
                identity: e.identity,
            }
        }
    }

    impl From<rivers_core::storage::BackfillsSummary> for BackfillsSummary {
        fn from(s: rivers_core::storage::BackfillsSummary) -> Self {
            Self {
                total: s.total,
                in_progress: s.in_progress,
                completed_success: s.completed_success,
                completed_failed: s.completed_failed,
                canceled: s.canceled,
            }
        }
    }

    impl From<BackfillFilter> for rivers_core::storage::BackfillFilter {
        fn from(f: BackfillFilter) -> Self {
            use rivers_core::storage::BackfillStatus;
            Self {
                status: f.status.and_then(|s| match s.as_str() {
                    "Requested" => Some(BackfillStatus::Requested),
                    "InProgress" => Some(BackfillStatus::InProgress),
                    "CompletedSuccess" => Some(BackfillStatus::CompletedSuccess),
                    "CompletedFailed" => Some(BackfillStatus::CompletedFailed),
                    "Canceled" => Some(BackfillStatus::Canceled),
                    _ => None,
                }),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The UI's filter inputs send `Some("")` when a search box is empty.
        /// The storage layer expects `None` for "no filter" — if the empty
        /// string leaked through, `string::contains(x, "")` is always true
        /// in SurrealQL so nothing would be filtered out (wrong), or worse,
        /// some backends would ERROR on an empty substring. The `From`
        /// impl is where that coercion happens — regression-proof it.
        #[test]
        fn run_filter_coerces_empty_strings_to_none() {
            let ui = RunFilter {
                status: None,
                job_name: Some(String::new()),
                job_substring: Some(String::new()),
                asset_substring: Some(String::new()),
                partition_substring: Some(String::new()),
            };
            let core: rivers_core::storage::RunFilter = ui.into();
            assert!(core.status.is_none());
            assert!(core.job_name.is_none());
            assert!(core.job_substring.is_none());
            assert!(core.asset_substring.is_none());
            assert!(core.partition_substring.is_none());
        }

        /// Non-empty filter values round-trip unchanged.
        #[test]
        fn run_filter_preserves_non_empty_strings() {
            let ui = RunFilter {
                status: Some(RunStatus::Failure),
                job_name: Some("daily_ingest".into()),
                job_substring: Some("daily".into()),
                asset_substring: Some("orders".into()),
                partition_substring: Some("2024-01".into()),
            };
            let core: rivers_core::storage::RunFilter = ui.into();
            assert_eq!(core.status, Some(rivers_core::storage::RunStatus::Failure));
            assert_eq!(core.job_name.as_deref(), Some("daily_ingest"));
            assert_eq!(core.job_substring.as_deref(), Some("daily"));
            assert_eq!(core.asset_substring.as_deref(), Some("orders"));
            assert_eq!(core.partition_substring.as_deref(), Some("2024-01"));
        }

        /// Backfill filter: status string → enum mapping round-trips the
        /// five valid variants and drops anything unknown (instead of
        /// passing garbage through to the DB).
        #[test]
        fn backfill_filter_status_mapping() {
            use rivers_core::storage::BackfillStatus;
            let cases = [
                ("Requested", Some(BackfillStatus::Requested)),
                ("InProgress", Some(BackfillStatus::InProgress)),
                ("CompletedSuccess", Some(BackfillStatus::CompletedSuccess)),
                ("CompletedFailed", Some(BackfillStatus::CompletedFailed)),
                ("Canceled", Some(BackfillStatus::Canceled)),
                ("Garbage", None),
                ("", None),
            ];
            for (input, expected) in cases {
                let ui = BackfillFilter {
                    status: Some(input.to_string()),
                };
                let core: rivers_core::storage::BackfillFilter = ui.into();
                assert_eq!(core.status, expected, "for input {input:?}");
            }
            // `None` stays `None`.
            let ui = BackfillFilter { status: None };
            let core: rivers_core::storage::BackfillFilter = ui.into();
            assert!(core.status.is_none());
        }
    }
}

#[cfg(test)]
mod topology_tests {
    use super::{GraphTopology, TopologyNode};

    fn node(name: &str) -> TopologyNode {
        TopologyNode {
            name: name.into(),
            kind: "asset".into(),
            group: None,
            parent_graph: None,
        }
    }

    /// `summary` depends on `raw_data` → edge `(summary, raw_data)`. Selecting
    /// `raw_data`, `summary` must be DOWNSTREAM (it consumes raw_data), not
    /// upstream. Regression for the swapped lineage labels (issue #57).
    #[test]
    fn direct_upstream_downstream_directions() {
        let topo = GraphTopology {
            nodes: vec![node("raw_data"), node("summary")],
            edges: vec![("summary".into(), "raw_data".into())],
        };

        // raw_data is a source: no upstream, summary downstream.
        assert!(topo.direct_upstream("raw_data").is_empty());
        assert_eq!(topo.direct_downstream("raw_data"), vec!["summary"]);

        // summary consumes raw_data: raw_data upstream, no downstream.
        assert_eq!(topo.direct_upstream("summary"), vec!["raw_data"]);
        assert!(topo.direct_downstream("summary").is_empty());
    }
}
