use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::PartitionDefinitionInfo;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Named actions this asset supports beyond materialize.
    #[serde(default)]
    pub actions: Vec<AssetActionInfo>,
    /// JSON schema of the asset's config class; `None` when it takes no config.
    #[serde(default)]
    pub config_schema: Option<String>,
    /// The asset's metadata as defined; a launch may add or replace keys.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Mirror of the gRPC `ActionInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetActionInfo {
    pub name: String,
    /// "unchanged" | "may_materialize" | "unmaterialize" | "observe"
    pub outcome: String,
    pub exclusive: bool,
    /// "required" | "keyless" | "optional" — partition-key rule for the verb.
    #[serde(default)]
    pub partitioning: String,
    pub description: Option<String>,
    /// JSON schema of the action's own config class; `None` when it takes none.
    #[serde(default)]
    pub config_schema: Option<String>,
}

impl AssetActionInfo {
    /// The verb clears materialization state — every surface offering it must
    /// derive its warning styling from here, not a local string compare.
    pub fn is_destructive(&self) -> bool {
        self.outcome == "unmaterialize"
    }

    /// Whole-asset verb: never takes a partition key (vacuum).
    pub fn is_keyless(&self) -> bool {
        self.partitioning == "keyless"
    }

    /// A key is accepted but not required (delete, optimize, observe): keyless runs
    /// cover the whole asset.
    pub fn key_optional(&self) -> bool {
        self.partitioning == "optional"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookInfo {
    pub hook_type: String,
    pub function_name: String,
}
