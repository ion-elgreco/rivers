//! SurrealDB storage backend implementation.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::{self, Any};
use surrealdb::types::{Bytes, RecordId, SurrealValue};

#[cfg(test)]
use super::DEFAULT_CODE_LOCATION_ID;
use super::{
    AssetRecord, BackfillFilter, BackfillRecord, BackfillStatus, BackfillsPage, BackfillsSummary,
    BlockReason, ConcurrencyClaimStatus, ConditionEvalRecord, ConditionTickRecord,
    CoordinatorRunInfo, EventRecord, EventType, PartitionKey, PerCodeLocationStorage,
    PoolBlockDetail, PoolInfo, PoolLimit, RunFilter, RunOutcome, RunProgress, RunRecord, RunStatus,
    RunsPage, RunsSummary, SlotHolder, StorageBackend, StoredConditionEval, StoredConditionTick,
    StoredEvent, StoredTick, TickRecord,
};
#[cfg(test)]
use crate::assets::graph::GraphTopology;

#[derive(Debug, SurrealValue)]
struct DbKv {
    key: String,
    value: Bytes,
}

#[derive(Debug, SurrealValue)]
struct DbDynamicPartition {
    code_location_id: String,
    partitions_def_name: String,
    partition_key: String,
    create_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbEventWrite {
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    code_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
}

/// One `asset_partitions` row, written in bulk via `upsert_asset_partitions`.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbAssetPartitionWrite {
    code_location_id: String,
    asset_key: String,
    partition_key: PartitionKey,
    last_event_id: String,
    last_run_id: String,
    last_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredEvent {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    #[serde(default)]
    code_version: Option<String>,
    #[serde(default)]
    input_data_versions: Vec<(String, String)>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbTickWrite {
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredTick {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbConditionTickWrite {
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredConditionTick {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbConditionEvalWrite {
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    selection_json: Option<Bytes>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
struct DbStoredConditionEval {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    #[serde(default)]
    selection_json: Option<Bytes>,
}

impl From<&ConditionTickRecord> for DbConditionTickWrite {
    fn from(t: &ConditionTickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            timestamp: t.timestamp,
            total_evaluated: t.total_evaluated as i64,
            total_fired: t.total_fired as i64,
            eval_duration_us: t.eval_duration_us as i64,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
        }
    }
}

impl DbStoredConditionTick {
    fn into_stored(self) -> StoredConditionTick {
        StoredConditionTick {
            id: self.id,
            code_location_id: self.code_location_id,
            timestamp: self.timestamp,
            total_evaluated: self.total_evaluated as u32,
            total_fired: self.total_fired as u32,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
        }
    }
}

impl From<&ConditionEvalRecord> for DbConditionEvalWrite {
    fn from(e: &ConditionEvalRecord) -> Self {
        Self {
            code_location_id: e.code_location_id.clone(),
            asset_key: e.asset_key.clone(),
            tick_id: e.tick_id.clone(),
            timestamp: e.timestamp,
            fired: e.fired,
            eval_duration_us: e.eval_duration_us as i64,
            run_ids: e.run_ids.clone(),
            tree_json: Bytes::from(e.tree_json.clone()),
            selection_json: e.selection_json.as_ref().map(|b| Bytes::from(b.clone())),
        }
    }
}

impl DbStoredConditionEval {
    fn into_stored(self) -> StoredConditionEval {
        StoredConditionEval {
            id: self.id,
            code_location_id: self.code_location_id,
            asset_key: self.asset_key,
            tick_id: self.tick_id,
            timestamp: self.timestamp,
            fired: self.fired,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            tree_json: self.tree_json.to_vec(),
            selection_json: self.selection_json.map(|b| b.to_vec()),
        }
    }
}

impl From<&TickRecord> for DbTickWrite {
    fn from(t: &TickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            automation_name: t.automation_name.clone(),
            automation_type: t.automation_type.clone(),
            status: t.status.clone(),
            timestamp: t.timestamp,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
            skip_reason: t.skip_reason.clone(),
            error: t.error.clone(),
            cursor: t.cursor.clone(),
        }
    }
}

impl DbStoredTick {
    fn into_stored_tick(self) -> StoredTick {
        StoredTick {
            id: self.id,
            code_location_id: self.code_location_id,
            automation_name: self.automation_name,
            automation_type: self.automation_type,
            status: self.status,
            timestamp: self.timestamp,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
            skip_reason: self.skip_reason,
            error: self.error,
            cursor: self.cursor,
        }
    }
}

impl From<&EventRecord> for DbEventWrite {
    fn from(e: &EventRecord) -> Self {
        Self {
            code_location_id: e.code_location_id.clone(),
            event_type: e.event_type.type_name().to_string(),
            asset_key: e.asset_key.clone(),
            run_id: e.run_id.clone(),
            partition_key: e.partition_key.clone(),
            timestamp: e.timestamp,
            sort_order: e.event_type.sort_order(),
            metadata: e.metadata.clone(),
            data_version: e.event_type.data_version().map(|s| s.to_string()),
            // Version tracking fields — populated by store_event for materializations.
            code_version: None,
            input_data_versions: Vec::new(),
        }
    }
}

impl DbStoredEvent {
    fn into_stored_event(self) -> StoredEvent {
        let event_type = EventType::from_type_name(&self.event_type, self.data_version).unwrap_or(
            EventType::StepFailure, // fallback for unknown types
        );
        StoredEvent {
            id: self.id,
            event_type,
            asset_key: self.asset_key,
            run_id: self.run_id,
            partition_key: self.partition_key,
            timestamp: self.timestamp,
            metadata: self.metadata,
            code_version: self.code_version,
            input_data_versions: self.input_data_versions,
        }
    }
}

/// Current storage schema version. Bump when adding a migration step to
/// [`run_migrations`].
const SCHEMA_VERSION: u32 = 3;
/// `kv` key holding the persisted schema version. Absent means v1 — a
/// database from before versioning existed.
const SCHEMA_VERSION_KEY: &str = "schema_version";

async fn stored_schema_version(db: &Surreal<Any>) -> anyhow::Result<u32> {
    let mut result = db
        .query("SELECT * FROM kv WHERE key = $key LIMIT 1")
        .bind(("key", SCHEMA_VERSION_KEY.to_string()))
        .await?;
    let rows: Vec<DbKv> = result.take(0)?;
    Ok(rows
        .into_iter()
        .next()
        .and_then(|kv| String::from_utf8(kv.value.to_vec()).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1))
}

async fn set_schema_version(db: &Surreal<Any>, version: u32) -> anyhow::Result<()> {
    // Single upsert on the unique key: concurrent first boots both converge
    // instead of the loser aborting on idx_kv_key, and a crash can't leave
    // the stamp deleted-but-unwritten.
    db.query(
        "INSERT INTO kv { key: $key, value: $value } \
         ON DUPLICATE KEY UPDATE value = $value",
    )
    .bind(("key", SCHEMA_VERSION_KEY.to_string()))
    .bind(("value", Bytes::from(version.to_string().into_bytes())))
    .await?
    .check()?;
    Ok(())
}

/// `kv` key listing dynamic partition keys that contain display-reserved
/// characters (registered before the reserved-char guard existed). Operator
/// remediation: delete each and re-register it under a clean name.
const RESERVED_DYNAMIC_KEYS_KEY: &str = "reserved_dynamic_keys";

/// v2 → v3 scan: stored reserved-char dynamic keys still classify (matching
/// is structural) but cannot round-trip any display-string path (UI, gRPC).
/// Renaming them silently would change user-visible keys, so warn and record
/// them in `kv` instead.
async fn scan_reserved_char_dynamic_keys(db: &Surreal<Any>) -> anyhow::Result<()> {
    let mut result = db
        .query(
            "SELECT code_location_id, partitions_def_name, partition_key, create_timestamp \
             FROM dynamic_partitions",
        )
        .await?;
    let rows: Vec<DbDynamicPartition> = result.take(0)?;
    let offenders: Vec<String> = rows
        .iter()
        .filter(|r| PartitionKey::reserved_display_char(&r.partition_key).is_some())
        .map(|r| {
            format!(
                "{}/{}: '{}'",
                r.code_location_id, r.partitions_def_name, r.partition_key
            )
        })
        .collect();
    if offenders.is_empty() {
        return Ok(());
    }
    for o in offenders.iter().take(20) {
        tracing::warn!(
            key = %o,
            "dynamic partition key contains display-reserved characters; \
             delete it and re-register under a clean name"
        );
    }
    let body = serde_json::to_vec(&offenders[..offenders.len().min(100)])?;
    db.query(
        "INSERT INTO kv { key: $key, value: $value } \
         ON DUPLICATE KEY UPDATE value = $value",
    )
    .bind(("key", RESERVED_DYNAMIC_KEYS_KEY.to_string()))
    .bind(("value", Bytes::from(body)))
    .await?
    .check()?;
    Ok(())
}

/// Versioned in-place migrations, run once at every constructor. Steps
/// between the persisted version and [`SCHEMA_VERSION`] run in order; the
/// stamp is written only after they all succeed (each step is idempotent and
/// crash-safe, so a partial run simply resumes on the next start). A
/// database already at the current version skips the table scans entirely.
async fn run_migrations(db: &Surreal<Any>) -> anyhow::Result<()> {
    let from = stored_schema_version(db).await?;
    if from >= SCHEMA_VERSION {
        return Ok(());
    }
    if from < 2 {
        migrate_multi_partition_key_order(db).await?;
    }
    if from < 3 {
        if from >= 2 {
            // Released 0.1.x wheels predate canonical-on-write Multi dims;
            // rows they wrote into a v2-stamped database were never healed
            // once the every-boot scan became version-gated.
            migrate_multi_partition_key_order(db).await?;
        }
        scan_reserved_char_dynamic_keys(db).await?;
    }
    set_schema_version(db, SCHEMA_VERSION).await?;
    tracing::info!(from, to = SCHEMA_VERSION, "storage schema migrated");
    Ok(())
}

/// Upsert the heal's canonical row. The migration computes its values from a
/// snapshot taken before the write loop, so a live materialization can land a
/// newer canonical row in between.
async fn upsert_canonical_partition_row(
    db: &Surreal<Any>,
    write: DbAssetPartitionWrite,
) -> anyhow::Result<()> {
    // Pointer fields only move forward: a stale snapshot must never roll
    // back a row a live materialization just advanced. last_timestamp is
    // compared first and assigned last so the guards all see the old value.
    db.query(
        "INSERT INTO asset_partitions $row ON DUPLICATE KEY UPDATE \
         last_event_id = IF $input.last_timestamp > last_timestamp THEN $input.last_event_id ELSE last_event_id END, \
         last_run_id = IF $input.last_timestamp > last_timestamp THEN $input.last_run_id ELSE last_run_id END, \
         last_timestamp = IF $input.last_timestamp > last_timestamp THEN $input.last_timestamp ELSE last_timestamp END",
    )
    .bind(("row", write))
    .await?
    .check()?;
    Ok(())
}

/// v1 → v2: rewrite persisted `Multi` partition keys whose dims aren't in
/// canonical (sorted) order. Earlier builds serialized HashMap iteration
/// order, which defeats the `asset_partitions` UNIQUE index (one logical
/// partition could occupy several rows) and `partition_key =` lookups on
/// `events`. Idempotent; sorted rows are left untouched.
async fn migrate_multi_partition_key_order(db: &Surreal<Any>) -> anyhow::Result<()> {
    fn is_sorted(dims: &[(String, Vec<String>)]) -> bool {
        dims.windows(2).all(|w| w[0].0 <= w[1].0)
    }

    #[derive(Debug, SurrealValue)]
    struct PartRow {
        id: RecordId,
        code_location_id: String,
        asset_key: String,
        partition_key: PartitionKey,
        last_event_id: Option<String>,
        last_run_id: Option<String>,
        last_timestamp: Option<i64>,
    }
    let mut result = db
        .query("SELECT id, code_location_id, asset_key, partition_key, last_event_id, last_run_id, last_timestamp FROM asset_partitions WHERE partition_key.variant = 'Multi'")
        .await?;
    let rows: Vec<PartRow> = result.take(0)?;
    // Group by logical key (PartitionKey's Eq/Hash are order-insensitive):
    // unsorted duplicates collapse onto the newest row.
    let mut groups: HashMap<(String, String, PartitionKey), Vec<PartRow>> = HashMap::new();
    for row in rows {
        groups
            .entry((
                row.code_location_id.clone(),
                row.asset_key.clone(),
                row.partition_key.clone(),
            ))
            .or_default()
            .push(row);
    }
    let mut rewritten = 0usize;
    for ((_, _, key), group) in groups {
        let needs_rewrite = group.len() > 1
            || group.iter().any(
                |r| matches!(&r.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)),
            );
        if !needs_rewrite {
            continue;
        }
        let latest = group
            .iter()
            .max_by_key(|r| r.last_timestamp.unwrap_or(i64::MIN))
            .expect("group is non-empty");
        let write = DbAssetPartitionWrite {
            code_location_id: latest.code_location_id.clone(),
            asset_key: latest.asset_key.clone(),
            partition_key: key,
            last_event_id: latest.last_event_id.clone().unwrap_or_default(),
            last_run_id: latest.last_run_id.clone().unwrap_or_default(),
            last_timestamp: latest.last_timestamp.unwrap_or_default(),
        };
        // Write the canonical row before touching the legacy ones: a crash
        // between the two steps leaves a recoverable duplicate for the next
        // start to clean up, never a lost partition. A pre-existing sorted
        // row is updated in place via the UNIQUE index.
        upsert_canonical_partition_row(db, write).await?;
        for row in &group {
            if matches!(&row.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)) {
                db.query("DELETE $rec")
                    .bind(("rec", row.id.clone()))
                    .await?
                    .check()?;
            }
        }
        rewritten += 1;
    }

    #[derive(Debug, SurrealValue)]
    struct EventRow {
        id: RecordId,
        partition_key: PartitionKey,
    }
    let mut result = db
        .query("SELECT id, partition_key FROM events WHERE partition_key.variant = 'Multi'")
        .await?;
    let events: Vec<EventRow> = result.take(0)?;
    let to_rewrite: Vec<EventRow> = events
        .into_iter()
        .filter(|ev| matches!(&ev.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)))
        .collect();
    let event_rewrites = to_rewrite.len();
    // Chunked multi-statement updates: one round-trip per 200 rows instead
    // of per row. Re-binding the same key canonicalizes (into_value sorts).
    for chunk in to_rewrite.chunks(200) {
        let stmts: String = (0..chunk.len())
            .map(|i| format!("UPDATE $r{i} SET partition_key = $p{i};"))
            .collect();
        let mut query = db.query(stmts);
        for (i, ev) in chunk.iter().enumerate() {
            query = query
                .bind((format!("r{i}"), ev.id.clone()))
                .bind((format!("p{i}"), ev.partition_key.clone()));
        }
        query.await?.check()?;
    }
    if rewritten > 0 || event_rewrites > 0 {
        tracing::info!(
            partitions = rewritten,
            events = event_rewrites,
            "migrated Multi partition keys to canonical dim order"
        );
    }
    Ok(())
}

const SCHEMA: &str = "
DEFINE TABLE IF NOT EXISTS events SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON events TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS event_type ON events TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON events TYPE option<string>;
DEFINE FIELD IF NOT EXISTS run_id ON events TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON events TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS timestamp ON events TYPE int;
DEFINE FIELD IF NOT EXISTS sort_order ON events TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS metadata ON events TYPE array;
DEFINE FIELD IF NOT EXISTS data_version ON events TYPE option<string>;
DEFINE FIELD IF NOT EXISTS code_version ON events TYPE option<string>;
DEFINE FIELD IF NOT EXISTS input_data_versions ON events TYPE array DEFAULT [];
DEFINE INDEX IF NOT EXISTS idx_events_run ON events FIELDS run_id;
DEFINE INDEX IF NOT EXISTS idx_events_type ON events FIELDS event_type;
DEFINE INDEX IF NOT EXISTS idx_events_run_type ON events FIELDS run_id, event_type;
DEFINE INDEX IF NOT EXISTS idx_events_run_ts ON events FIELDS run_id, timestamp, sort_order;
-- Per-CL by-asset filters; events for the same asset_key under different
-- code locations don't bleed into each other.
DEFINE INDEX IF NOT EXISTS idx_events_loc_asset ON events FIELDS code_location_id, asset_key;
DEFINE INDEX IF NOT EXISTS idx_events_loc_asset_part ON events FIELDS code_location_id, asset_key, partition_key;
-- Lets get_failed_partitions skip to an asset's failures by event_type.
DEFINE INDEX IF NOT EXISTS idx_events_loc_asset_type ON events FIELDS code_location_id, asset_key, event_type;
-- Timestamp-ordered per-asset event pagination (asset-detail events tab) — scan
-- in order rather than sorting every matching event, mirroring idx_events_run_ts.
DEFINE INDEX IF NOT EXISTS idx_events_loc_asset_ts ON events FIELDS code_location_id, asset_key, timestamp, sort_order;

DEFINE TABLE IF NOT EXISTS assets SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON assets TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS asset_key ON assets TYPE string;
DEFINE FIELD IF NOT EXISTS tags ON assets TYPE array<string>;
DEFINE FIELD IF NOT EXISTS kinds ON assets TYPE array<string>;
DEFINE FIELD IF NOT EXISTS asset_group ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS code_version ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_event_id ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_run_id ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_timestamp ON assets TYPE option<int>;
DEFINE FIELD IF NOT EXISTS last_data_version ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_materialization_code_version ON assets TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_input_data_versions ON assets TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS pool ON assets TYPE array DEFAULT [];
-- Composite unique index — two CLs may register the same `asset_key`
-- independently; uniqueness is per-CL, not global.
DEFINE INDEX IF NOT EXISTS idx_assets_loc_key ON assets FIELDS code_location_id, asset_key UNIQUE;
DEFINE INDEX IF NOT EXISTS idx_assets_loc ON assets FIELDS code_location_id;
DEFINE INDEX IF NOT EXISTS idx_assets_loc_group ON assets FIELDS code_location_id, asset_group;

DEFINE TABLE IF NOT EXISTS asset_partitions SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON asset_partitions TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS asset_key ON asset_partitions TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON asset_partitions TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS last_event_id ON asset_partitions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_run_id ON asset_partitions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_timestamp ON asset_partitions TYPE option<int>;
-- Composite unique index — partition keys are per-(CL, asset).
DEFINE INDEX IF NOT EXISTS idx_asset_part ON asset_partitions FIELDS code_location_id, asset_key, partition_key UNIQUE;

DEFINE TABLE IF NOT EXISTS runs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS run_id ON runs TYPE string;
DEFINE FIELD IF NOT EXISTS code_location_id ON runs TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS job_name ON runs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS status ON runs TYPE string;
DEFINE FIELD IF NOT EXISTS start_time ON runs TYPE int;
DEFINE FIELD IF NOT EXISTS end_time ON runs TYPE option<int>;
DEFINE FIELD IF NOT EXISTS tags ON runs TYPE array;
DEFINE FIELD IF NOT EXISTS node_names ON runs TYPE array<string>;
DEFINE FIELD IF NOT EXISTS priority ON runs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS partition_key ON runs TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS block_reason ON runs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS launched_by ON runs TYPE object FLEXIBLE DEFAULT { kind: 'manual' };
DEFINE INDEX IF NOT EXISTS idx_runs_status ON runs FIELDS status;
DEFINE INDEX IF NOT EXISTS idx_runs_job ON runs FIELDS job_name;
DEFINE INDEX IF NOT EXISTS idx_runs_id ON runs FIELDS run_id UNIQUE;
DEFINE INDEX IF NOT EXISTS idx_runs_start_time ON runs FIELDS start_time;
DEFINE INDEX IF NOT EXISTS idx_runs_priority ON runs FIELDS priority;
-- Composite index for `get_all_last_run_per_job`: each statement does
-- `WHERE job_name = $x ORDER BY start_time DESC LIMIT 1`. Without this,
-- the planner can use either `idx_runs_job` (seek by job, then sort) or
-- `idx_runs_start_time` (walk time-ordered, filter by job) — both scale
-- badly for jobs with many runs or for stale jobs. The composite lets
-- the planner seek directly to the job's time-ordered partition.
DEFINE INDEX IF NOT EXISTS idx_runs_job_time ON runs FIELDS job_name, start_time;
-- Queue isolation between code locations sharing one SurrealDB. The
-- coordinator's tick query filters by code_location_id + status.
DEFINE INDEX IF NOT EXISTS idx_runs_loc_status ON runs FIELDS code_location_id, status;
-- Scoped get_runs / get_runs_since walk runs for one CL in start_time order.
-- Without this composite, the planner falls back to idx_runs_loc_status + sort,
-- or idx_runs_start_time + filter — both scale poorly as the per-CL run table grows.
DEFINE INDEX IF NOT EXISTS idx_runs_loc_time ON runs FIELDS code_location_id, start_time;

DEFINE TABLE IF NOT EXISTS kv SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS key ON kv TYPE string;
DEFINE FIELD IF NOT EXISTS value ON kv TYPE bytes;
DEFINE INDEX IF NOT EXISTS idx_kv_key ON kv FIELDS key UNIQUE;

DEFINE TABLE IF NOT EXISTS dynamic_partitions SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON dynamic_partitions TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS partitions_def_name ON dynamic_partitions TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON dynamic_partitions TYPE string;
DEFINE FIELD IF NOT EXISTS create_timestamp ON dynamic_partitions TYPE int;
-- Composite unique — dynamic partition defs are per-CL.
DEFINE INDEX IF NOT EXISTS idx_dyn_part ON dynamic_partitions FIELDS code_location_id, partitions_def_name;
DEFINE INDEX IF NOT EXISTS idx_dyn_part_unique ON dynamic_partitions FIELDS code_location_id, partitions_def_name, partition_key UNIQUE;

DEFINE TABLE IF NOT EXISTS ticks SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON ticks TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS automation_name ON ticks TYPE string;
DEFINE FIELD IF NOT EXISTS automation_type ON ticks TYPE string;
DEFINE FIELD IF NOT EXISTS status ON ticks TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON ticks TYPE int;
DEFINE FIELD IF NOT EXISTS run_ids ON ticks TYPE array<string>;
DEFINE FIELD IF NOT EXISTS backfill_ids ON ticks TYPE array<string> DEFAULT [];
DEFINE FIELD IF NOT EXISTS skip_reason ON ticks TYPE option<string>;
DEFINE FIELD IF NOT EXISTS error ON ticks TYPE option<string>;
DEFINE FIELD IF NOT EXISTS cursor ON ticks TYPE option<string>;
-- Composite — schedules/sensors are per-CL by automation_name.
DEFINE INDEX IF NOT EXISTS idx_ticks_loc_name ON ticks FIELDS code_location_id, automation_name;
DEFINE INDEX IF NOT EXISTS idx_ticks_loc_name_ts ON ticks FIELDS code_location_id, automation_name, timestamp;

DEFINE TABLE IF NOT EXISTS condition_ticks SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON condition_ticks TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS timestamp ON condition_ticks TYPE int;
DEFINE FIELD IF NOT EXISTS total_evaluated ON condition_ticks TYPE int;
DEFINE FIELD IF NOT EXISTS total_fired ON condition_ticks TYPE int;
DEFINE FIELD IF NOT EXISTS eval_duration_us ON condition_ticks TYPE int;
DEFINE FIELD IF NOT EXISTS run_ids ON condition_ticks TYPE array<string>;
DEFINE FIELD IF NOT EXISTS backfill_ids ON condition_ticks TYPE array<string> DEFAULT [];
-- Per-CL condition cycle counter.
DEFINE INDEX IF NOT EXISTS idx_cond_ticks_loc_ts ON condition_ticks FIELDS code_location_id, timestamp;

DEFINE TABLE IF NOT EXISTS condition_evals SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON condition_evals TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS asset_key ON condition_evals TYPE string;
DEFINE FIELD IF NOT EXISTS tick_id ON condition_evals TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON condition_evals TYPE int;
DEFINE FIELD IF NOT EXISTS fired ON condition_evals TYPE bool;
DEFINE FIELD IF NOT EXISTS eval_duration_us ON condition_evals TYPE int;
DEFINE FIELD IF NOT EXISTS run_ids ON condition_evals TYPE array<string>;
DEFINE FIELD IF NOT EXISTS tree_json ON condition_evals TYPE bytes;
DEFINE FIELD IF NOT EXISTS selection_json ON condition_evals TYPE option<bytes>;
-- Per-CL condition eval rows.
DEFINE INDEX IF NOT EXISTS idx_cond_evals_loc_key ON condition_evals FIELDS code_location_id, asset_key;
DEFINE INDEX IF NOT EXISTS idx_cond_evals_loc_key_ts ON condition_evals FIELDS code_location_id, asset_key, timestamp;
-- tick_id is a globally-unique surreal record id, no CL scope needed.
DEFINE INDEX IF NOT EXISTS idx_cond_evals_tick ON condition_evals FIELDS tick_id;

DEFINE TABLE IF NOT EXISTS backfills SCHEMALESS;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfills TYPE string;
DEFINE FIELD IF NOT EXISTS code_location_id ON backfills TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS status ON backfills TYPE string;
-- FLEXIBLE is SCHEMAFULL-only; on this SCHEMALESS table a plain object type
-- validates the shape without stripping nested content.
DEFINE FIELD IF NOT EXISTS strategy ON backfills TYPE object;
DEFINE FIELD IF NOT EXISTS failure_policy ON backfills TYPE string;
DEFINE FIELD IF NOT EXISTS asset_selection ON backfills TYPE array<string>;
DEFINE FIELD IF NOT EXISTS partition_keys ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS run_ids ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS completed_partitions ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS failed_partitions ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS canceled_partitions ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS max_concurrency ON backfills TYPE int;
DEFINE FIELD IF NOT EXISTS tags ON backfills TYPE array;
DEFINE FIELD IF NOT EXISTS create_time ON backfills TYPE int;
DEFINE FIELD IF NOT EXISTS end_time ON backfills TYPE option<int>;
DEFINE FIELD IF NOT EXISTS error ON backfills TYPE option<string>;
-- backfill_id is globally unique (UUID).
DEFINE INDEX IF NOT EXISTS idx_backfills_id ON backfills FIELDS backfill_id UNIQUE;
-- Composite for the per-CL pickup loop and listing.
DEFINE INDEX IF NOT EXISTS idx_backfills_loc_status ON backfills FIELDS code_location_id, status;

DEFINE TABLE IF NOT EXISTS concurrency_pools SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON concurrency_pools TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS pool_key ON concurrency_pools TYPE string;
DEFINE FIELD IF NOT EXISTS slot_limit ON concurrency_pools TYPE int;
DEFINE FIELD IF NOT EXISTS lease_duration_secs ON concurrency_pools TYPE int DEFAULT 300;
DEFINE FIELD IF NOT EXISTS claim_version ON concurrency_pools TYPE int DEFAULT 0;
-- Pools are per-CL. CL-A's `default` pool is independent of CL-B's.
DEFINE INDEX IF NOT EXISTS idx_pool_key ON concurrency_pools FIELDS code_location_id, pool_key UNIQUE;

DEFINE TABLE IF NOT EXISTS concurrency_slots SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON concurrency_slots TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS pool_key ON concurrency_slots TYPE string;
DEFINE FIELD IF NOT EXISTS run_id ON concurrency_slots TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON concurrency_slots TYPE string;
DEFINE FIELD IF NOT EXISTS slots_consumed ON concurrency_slots TYPE int;
DEFINE FIELD IF NOT EXISTS claimed_at ON concurrency_slots TYPE int;
DEFINE FIELD IF NOT EXISTS lease_expires_at ON concurrency_slots TYPE int;
DEFINE FIELD IF NOT EXISTS last_heartbeat ON concurrency_slots TYPE int;
-- Slots scope by CL via the owning pool.
DEFINE INDEX IF NOT EXISTS idx_slot_pool ON concurrency_slots FIELDS code_location_id, pool_key;
DEFINE INDEX IF NOT EXISTS idx_slot_run ON concurrency_slots FIELDS run_id;
DEFINE INDEX IF NOT EXISTS idx_slot_unique ON concurrency_slots FIELDS code_location_id, pool_key, run_id, step_key UNIQUE;
DEFINE INDEX IF NOT EXISTS idx_slot_lease ON concurrency_slots FIELDS lease_expires_at;

DEFINE TABLE IF NOT EXISTS pending_steps SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON pending_steps TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS pool_key ON pending_steps TYPE string;
DEFINE FIELD IF NOT EXISTS run_id ON pending_steps TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON pending_steps TYPE string;
DEFINE FIELD IF NOT EXISTS priority ON pending_steps TYPE int;
DEFINE FIELD IF NOT EXISTS enqueued_at ON pending_steps TYPE int;
DEFINE FIELD IF NOT EXISTS block_reason ON pending_steps TYPE string;
-- Pending steps scope by CL via the owning pool.
DEFINE INDEX IF NOT EXISTS idx_pending_pool ON pending_steps FIELDS code_location_id, pool_key;
DEFINE INDEX IF NOT EXISTS idx_pending_step ON pending_steps FIELDS run_id, step_key UNIQUE;
";

/// Identifies which underlying SurrealDB transport a [`SurrealStorage`] was
/// constructed with.
#[derive(Debug, Clone)]
pub enum SurrealBackendKind {
    Embedded { path: String },
    Memory,
    Remote { endpoint: String },
}

impl SurrealBackendKind {
    pub fn label(&self) -> String {
        match self {
            SurrealBackendKind::Embedded { path } => {
                format!("SurrealDB (Embedded RocksDB at {path})")
            }
            SurrealBackendKind::Memory => "SurrealDB (In-Memory)".to_string(),
            SurrealBackendKind::Remote { endpoint } => {
                format!("SurrealDB (Remote: {endpoint})")
            }
        }
    }
}

/// Default SurrealDB namespace used by every rivers connection.
pub const DEFAULT_NAMESPACE: &str = "rivers";

/// Default SurrealDB database used by every rivers connection.
pub const DEFAULT_DATABASE: &str = "main";

/// Connection parameters for [`SurrealStorage::connect`].
#[derive(Debug, Clone)]
pub struct SurrealConnectConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub credentials: Option<SurrealCredentials>,
}

impl SurrealConnectConfig {
    /// Config with default `rivers / main` scope and no credentials.
    pub fn unauthenticated(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            credentials: None,
        }
    }

    /// Attach database-scoped credentials. The scope (`namespace`/`database`)
    /// is taken from this config — the user must be defined with
    /// `DEFINE USER ... ON DATABASE` matching those fields, otherwise signin
    /// fails.
    pub fn with_credentials(mut self, username: String, password: String) -> Self {
        self.credentials = Some(SurrealCredentials::Database { username, password });
        self
    }
}

/// Credentials used during [`SurrealStorage::connect`].
#[derive(Debug, Clone)]
pub enum SurrealCredentials {
    /// Sign in as a `DEFINE USER ... ON DATABASE` user. The namespace/database
    /// values come from the enclosing [`SurrealConnectConfig`].
    Database { username: String, password: String },
}

pub struct SurrealStorage {
    db: Surreal<Any>,
    retry_config: super::retry::StorageRetryConfig,
    backend_kind: SurrealBackendKind,
    /// Tokio runtime that hosts the router task when the storage was
    /// constructed via one of the `*_blocking` constructors. `None` when an
    /// async constructor was used (caller's runtime hosts the router).
    /// Declared LAST so it drops after `db` in default field-drop order —
    /// see [`Drop`] for the synchronous-shutdown handoff.
    runtime: Option<tokio::runtime::Runtime>,
}

impl Drop for SurrealStorage {
    fn drop(&mut self) {
        // For per-storage owned runtimes: force `db` to drop first (closes
        // the route channel, signals the router task to wind down), then
        // drain the runtime so its tasks aren't force-cancelled mid-cleanup
        // (RocksDB lock release, in-memory state teardown).
        if let Some(runtime) = self.runtime.take() {
            let _ = std::mem::replace(&mut self.db, Surreal::init());
            if tokio::runtime::Handle::try_current().is_ok() {
                runtime.shutdown_background();
            } else {
                runtime.shutdown_timeout(std::time::Duration::from_secs(5));
            }
        }
    }
}

/// Build a dedicated multi-thread runtime for one [`SurrealStorage`]
fn build_storage_runtime() -> tokio::runtime::Runtime {
    static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name(format!("rivers-stg-{id}"))
        .worker_threads(2)
        .build()
        .expect("Failed to create per-storage tokio runtime.")
}

impl SurrealStorage {
    pub fn backend_kind(&self) -> &SurrealBackendKind {
        &self.backend_kind
    }

    /// Embedded storage backed by RocksDB at the given path.
    pub async fn new_embedded(path: &str) -> Result<Self> {
        Self::new_embedded_with_retry(path, super::retry::StorageRetryConfig::default()).await
    }

    /// Variant of [`Self::new_embedded`] with a custom retry policy.
    pub async fn new_embedded_with_retry(
        path: &str,
        retry_config: super::retry::StorageRetryConfig,
    ) -> Result<Self> {
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "embedded",
            path = %path,
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!(path = %path, "connecting rocksdb engine");
            let db = any::connect(format!("rocksdb://{path}"))
                .await
                .context("failed to open RocksDB")?;
            tracing::debug!(
                ns = DEFAULT_NAMESPACE,
                db = DEFAULT_DATABASE,
                "selecting namespace/database"
            );
            db.use_ns(DEFAULT_NAMESPACE)
                .use_db(DEFAULT_DATABASE)
                .await?;
            tracing::debug!("applying schema");
            db.query(SCHEMA).await?.check()?;
            run_migrations(&db).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "embedded",
            path = %path,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Embedded {
                path: path.to_string(),
            },
            runtime: None,
        })
    }

    /// In-memory storage (useful for tests).
    pub async fn new_memory() -> Result<Self> {
        Self::new_memory_with_retry(super::retry::StorageRetryConfig::default()).await
    }

    /// Variant of [`Self::new_memory`] with a custom retry policy.
    pub async fn new_memory_with_retry(
        retry_config: super::retry::StorageRetryConfig,
    ) -> Result<Self> {
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "memory",
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!("connecting in-memory engine");
            let db = any::connect("mem://")
                .await
                .context("failed to create in-memory DB")?;
            tracing::debug!(
                ns = DEFAULT_NAMESPACE,
                db = DEFAULT_DATABASE,
                "selecting namespace/database"
            );
            db.use_ns(DEFAULT_NAMESPACE)
                .use_db(DEFAULT_DATABASE)
                .await?;
            tracing::debug!("applying schema");
            db.query(SCHEMA).await?.check()?;
            run_migrations(&db).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "memory",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Memory,
            runtime: None,
        })
    }

    /// Connect to a remote SurrealDB server.
    ///
    /// Authenticates with the database-scoped credentials in `config.credentials`
    /// when set; an absent credentials field skips `signin` and is appropriate
    /// for SurrealDB instances started with `--unauthenticated`.
    pub async fn connect(config: SurrealConnectConfig) -> Result<Self> {
        Self::connect_with_retry(config, super::retry::StorageRetryConfig::default()).await
    }

    /// Variant of [`Self::connect`] with a custom retry policy.
    pub async fn connect_with_retry(
        config: SurrealConnectConfig,
        retry_config: super::retry::StorageRetryConfig,
    ) -> Result<Self> {
        let SurrealConnectConfig {
            endpoint,
            namespace,
            database,
            credentials,
        } = config;
        let started = std::time::Instant::now();
        tracing::info!(
            backend = "remote",
            endpoint = %endpoint,
            ns = %namespace,
            db = %database,
            authenticated = credentials.is_some(),
            max_retries = retry_config.max_retries,
            max_backoff_ms = retry_config.max_backoff.as_millis() as u64,
            "opening surreal storage"
        );
        let db = super::retry::with_retry(&retry_config, || async {
            tracing::debug!(endpoint = %endpoint, "connecting remote surrealdb");
            let db = any::connect(&endpoint)
                .await
                .context("failed to connect to remote SurrealDB")?;
            if let Some(creds) = credentials.clone() {
                match creds {
                    SurrealCredentials::Database { username, password } => {
                        tracing::debug!(
                            ns = %namespace,
                            db = %database,
                            username = %username,
                            "authenticating (database scope)"
                        );
                        db.signin(surrealdb::opt::auth::Database {
                            namespace: namespace.clone(),
                            database: database.clone(),
                            username,
                            password,
                        })
                        .await
                        .context("SurrealDB signin failed")?;
                    }
                }
            }
            tracing::debug!(
                ns = %namespace,
                db = %database,
                "selecting namespace/database"
            );
            db.use_ns(&namespace).use_db(&database).await?;
            tracing::debug!("applying schema");
            db.query(SCHEMA).await?.check()?;
            run_migrations(&db).await?;
            Ok(db)
        })
        .await?;
        tracing::info!(
            backend = "remote",
            endpoint = %endpoint,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "surreal storage opened"
        );
        Ok(Self {
            db,
            retry_config,
            backend_kind: SurrealBackendKind::Remote {
                endpoint: endpoint.to_string(),
            },
            runtime: None,
        })
    }

    /// [`SurrealStorage`] on a dedicated runtime owned for its lifetime — test fixtures only.
    pub fn new_embedded_blocking(path: &str) -> Result<Self> {
        let runtime = build_storage_runtime();
        let mut storage = runtime.block_on(Self::new_embedded(path))?;
        storage.runtime = Some(runtime);
        Ok(storage)
    }

    /// See [`Self::new_embedded_blocking`]. In-memory variant.
    pub fn new_memory_blocking() -> Result<Self> {
        let runtime = build_storage_runtime();
        let mut storage = runtime.block_on(Self::new_memory())?;
        storage.runtime = Some(runtime);
        Ok(storage)
    }

    /// Paginated, filtered slice of runs plus total matching row count.
    /// Ordered by `start_time` DESC. Substring filters fall back to a full
    /// scan over the status-filtered subset.
    pub async fn get_all_runs_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_runs_page`] for the
    /// `/locations/<ns>/<name>/runs` view.
    pub async fn get_runs_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        self.runs_page_impl(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn runs_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &RunFilter,
    ) -> Result<RunsPage> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            if filter.status.is_some() {
                wheres.push("status = $status");
            }
            if filter.job_name.is_some() {
                wheres.push("job_name = $job_exact");
            }
            if filter.job_substring.is_some() {
                wheres.push(
                    "job_name IS NOT NONE AND \
                     string::contains(string::lowercase(job_name), $job_pat)",
                );
            }
            if filter.asset_substring.is_some() {
                wheres.push(
                    "array::any(node_names, |$a| string::contains(string::lowercase($a), $asset_pat))",
                );
            }
            if filter.partition_substring.is_some() {
                wheres.push(
                    "array::any(tags, |$t| ($t[0] = 'partition' OR $t[0] = 'partition_key') \
                     AND string::contains(string::lowercase($t[1]), $partition_pat))",
                );
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM runs {where_clause} ORDER BY start_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM runs {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }
            if let Some(name) = &filter.job_name {
                q = q.bind(("job_exact", name.clone()));
            }
            if let Some(pat) = &filter.job_substring {
                q = q.bind(("job_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.asset_substring {
                q = q.bind(("asset_pat", pat.to_lowercase()));
            }
            if let Some(pat) = &filter.partition_substring {
                q = q.bind(("partition_pat", pat.to_lowercase()));
            }

            let mut result = q.await?;
            let rows: Vec<RunRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(RunsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// A page of an asset's events (newest first) restricted to `event_types`,
    /// plus the total count matching that filter — backs the asset-detail events
    /// pagination. Mirrors `get_runs_page`.
    pub async fn get_events_for_asset_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        event_types: &[String],
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types \
                     ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types GROUP ALL;",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("types", event_types.to_vec()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A run's step events (`StepStart`/`Success`/`Failure`) — backs the timeline/DAG.
    pub async fn get_run_step_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE run_id = $id \
                     AND event_type IN ['StepStart', 'StepSuccess', 'StepFailure'] \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    /// A run's `LogOutput` events (stdout/stderr/logs) — small, kept out of the
    /// materialization stream so the log tabs don't pull it.
    pub async fn get_run_log_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE run_id = $id AND event_type = 'LogOutput' \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    /// A page of a run's structured (non-`LogOutput`) events, optionally scoped
    /// to one asset, plus the total — backs the run-detail events table.
    pub async fn get_run_structured_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let asset_clause = if asset_key.is_some() {
                " AND asset_key = $ak"
            } else {
                ""
            };
            let sql = format!(
                "SELECT * FROM events WHERE run_id = $id AND event_type != 'LogOutput'{asset_clause} \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM events WHERE run_id = $id AND event_type != 'LogOutput'{asset_clause} GROUP ALL;"
            );
            let mut q = self
                .db
                .query(sql)
                .bind(("id", run_id.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(ak) = asset_key {
                q = q.bind(("ak", ak.to_string()));
            }
            let mut result = q.await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// A page of one asset's events of a single type within a run (e.g.
    /// `Materialization`) + total — so the drawer pages a 15k-partition asset's
    /// materializations instead of rendering all of them.
    pub async fn get_run_asset_events_page(
        &self,
        run_id: &str,
        asset_key: &str,
        event_type: &str,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type \
                     ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT $limit START $offset; \
                     SELECT count() AS total FROM events \
                     WHERE run_id = $id AND asset_key = $ak AND event_type = $type GROUP ALL;",
                )
                .bind(("id", run_id.to_string()))
                .bind(("ak", asset_key.to_string()))
                .bind(("type", event_type.to_string()))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok((
                events.into_iter().map(|e| e.into_stored_event()).collect(),
                total.unwrap_or(0),
            ))
        })
        .await
    }

    /// Aggregate run counts for the runs-list page header. Unfiltered so the
    /// pill badges stay stable as substring filters change.
    pub async fn get_all_runs_summary(&self, cutoff_24h_ns: i64) -> Result<RunsSummary> {
        self.runs_summary_impl(None, cutoff_24h_ns).await
    }

    /// Per-CL variant of [`Self::get_all_runs_summary`].
    pub async fn get_runs_summary(
        &self,
        code_location_id: &str,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        self.runs_summary_impl(Some(code_location_id), cutoff_24h_ns)
            .await
    }

    async fn runs_summary_impl(
        &self,
        code_location_id: Option<&str>,
        cutoff_24h_ns: i64,
    ) -> Result<RunsSummary> {
        super::retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM runs {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Started' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status IN ['Queued', 'NotStarted'] GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Failure' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}status = 'Success' GROUP ALL; \
                 SELECT count() AS total FROM runs WHERE {cl_filter}start_time > $cutoff GROUP ALL;",
            );
            let mut q = self.db.query(sql).bind(("cutoff", cutoff_24h_ns));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(RunsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                queued: take(&mut result, 2)?,
                failure: take(&mut result, 3)?,
                success: take(&mut result, 4)?,
                last_24h: take(&mut result, 5)?,
            })
        })
        .await
    }

    /// For each requested job name, return the most recent run if any. Ignores
    /// missing jobs. One multi-statement query (one round-trip) using the
    /// `idx_runs_job_time` composite, so cost is proportional to job count not row count.
    pub async fn get_all_last_run_per_job(
        &self,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(None, job_names).await
    }

    /// Per-CL variant of [`Self::get_all_last_run_per_job`].
    pub async fn get_last_run_per_job(
        &self,
        code_location_id: &str,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        self.last_run_per_job_impl(Some(code_location_id), job_names)
            .await
    }

    async fn last_run_per_job_impl(
        &self,
        code_location_id: Option<&str>,
        job_names: &[String],
    ) -> Result<Vec<(String, RunRecord)>> {
        super::retry::with_retry(&self.retry_config, || async {
            use std::fmt::Write;

            if job_names.is_empty() {
                return Ok(Vec::new());
            }

            let cl_filter = if code_location_id.is_some() {
                " AND code_location_id = $cl"
            } else {
                ""
            };
            let mut sql = String::with_capacity(job_names.len() * 128);
            for i in 0..job_names.len() {
                let _ = writeln!(
                    sql,
                    "SELECT * FROM runs WHERE job_name = $job_{i}{cl_filter} \
                     ORDER BY start_time DESC LIMIT 1;"
                );
            }
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            for (i, name) in job_names.iter().enumerate() {
                q = q.bind((format!("job_{i}"), name.clone()));
            }
            let mut result = q.await?;

            let mut out = Vec::with_capacity(job_names.len());
            for (i, name) in job_names.iter().enumerate() {
                let rows: Vec<RunRecord> = result.take(i)?;
                if let Some(run) = rows.into_iter().next() {
                    out.push((name.clone(), run));
                }
            }
            Ok(out)
        })
        .await
    }

    /// Paginated + filtered backfills list. Mirrors `get_all_runs_page`.
    pub async fn get_all_backfills_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(None, offset, limit, filter).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_page`].
    pub async fn get_backfills_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page_impl(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn backfills_page_impl(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut wheres: Vec<&'static str> = Vec::new();
            if code_location_id.is_some() {
                wheres.push("code_location_id = $cl");
            }
            if filter.status.is_some() {
                wheres.push("status = $status");
            }
            let where_clause = if wheres.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wheres.join(" AND "))
            };

            let sql = format!(
                "SELECT * FROM backfills {where_clause} ORDER BY create_time DESC LIMIT $limit START $offset; \
                 SELECT count() AS total FROM backfills {where_clause} GROUP ALL;"
            );

            let mut q = self
                .db
                .query(sql)
                .bind(("limit", limit))
                .bind(("offset", offset));
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            if let Some(s) = &filter.status {
                q = q.bind(("status", format!("{:?}", s)));
            }

            let mut result = q.await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            let total: Option<u64> = result.take((1, "total"))?;
            Ok(BackfillsPage {
                rows,
                total: total.unwrap_or(0),
            })
        })
        .await
    }

    /// Aggregate backfill counts for the list-page status pills. Unfiltered.
    pub async fn get_all_backfills_summary(&self) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(None).await
    }

    /// Per-CL variant of [`Self::get_all_backfills_summary`].
    pub async fn get_backfills_summary(&self, code_location_id: &str) -> Result<BackfillsSummary> {
        self.backfills_summary_impl(Some(code_location_id)).await
    }

    async fn backfills_summary_impl(
        &self,
        code_location_id: Option<&str>,
    ) -> Result<BackfillsSummary> {
        super::retry::with_retry(&self.retry_config, || async {
            let cl_filter = if code_location_id.is_some() {
                "code_location_id = $cl AND "
            } else {
                ""
            };
            let cl_where = if code_location_id.is_some() {
                "WHERE code_location_id = $cl"
            } else {
                ""
            };
            let sql = format!(
                "SELECT count() AS total FROM backfills {cl_where} GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'InProgress' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedSuccess' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'CompletedFailed' GROUP ALL; \
                 SELECT count() AS total FROM backfills WHERE {cl_filter}status = 'Canceled' GROUP ALL;",
            );
            let mut q = self.db.query(sql);
            if let Some(cl) = code_location_id {
                q = q.bind(("cl", cl.to_string()));
            }
            let mut result = q.await?;

            let take = |r: &mut surrealdb::IndexedResults, idx: usize| -> Result<u64> {
                let n: Option<u64> = r.take((idx, "total"))?;
                Ok(n.unwrap_or(0))
            };
            Ok(BackfillsSummary {
                total: take(&mut result, 0)?,
                in_progress: take(&mut result, 1)?,
                completed_success: take(&mut result, 2)?,
                completed_failed: take(&mut result, 3)?,
                canceled: take(&mut result, 4)?,
            })
        })
        .await
    }

    /// Subscribe to change notifications on an arbitrary table via a SurrealDB
    /// LIVE query. Each yielded `()` corresponds to a create/update/delete;
    /// payload is discarded so callers refetch.
    pub async fn subscribe_table(
        &self,
        table: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, ()>> {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use surrealdb::types::{Action, Object};
        let stream = self
            .db
            .select::<Vec<Object>>(table.to_string())
            .live()
            .await?;
        // `Arc<str>` instead of `String` so the per-notification clone in
        // `filter_map` below is a refcount bump rather than a heap
        // allocation. The name is only consumed by the error-path
        // `tracing::warn!`, which is a cold path on a healthy system —
        // paying an allocation per hot-path event is pure waste.
        let table_owned: Arc<str> = Arc::from(table);
        Ok(stream
            .filter_map(move |result| {
                let table = Arc::clone(&table_owned);
                async move {
                    let notif = match result {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::warn!(
                                target: "rivers::storage",
                                table = %table,
                                error = %e,
                                "live query yielded error"
                            );
                            return None;
                        }
                    };
                    match notif.action {
                        Action::Create | Action::Update | Action::Delete => Some(()),
                        // Killed: the live query itself was terminated; nothing
                        // to forward to subscribers.
                        _ => None,
                    }
                }
            })
            .boxed())
    }
}

fn record_id_str(id: &RecordId) -> String {
    format!("{}:{:?}", id.table.as_str(), id.key)
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

#[derive(Debug, SurrealValue, serde::Deserialize)]
struct OptStringField {
    value: Option<String>,
}

/// Build the `RunQueued` event row that pairs with a freshly-written
/// queued `RunRecord`.
fn run_queued_event(record: &RunRecord) -> EventRecord {
    EventRecord {
        code_location_id: record.code_location_id.clone(),
        event_type: EventType::RunQueued,
        asset_key: None,
        run_id: record.run_id.clone(),
        partition_key: record.partition_key.clone(),
        timestamp: record.start_time,
        metadata: vec![(
            super::tag_keys::PRIORITY.to_string(),
            record.priority.to_string(),
        )],
        input_data_versions: vec![],
    }
}

impl SurrealStorage {
    /// Persist a queued `RunRecord` and emit its `RunQueued` event in one
    /// step. Single source of truth for the queued-run write — every
    /// queued-runs writer routes through this so the `RunQueued` event always
    /// carries `tag_keys::PRIORITY` metadata in the same shape.
    pub async fn enqueue_run(&self, record: &RunRecord) -> Result<()> {
        // Atomic: a poller observing the run record is guaranteed to also
        // see its `RunQueued` event. Splitting the two writes leaves a
        // race window that breaks the per-run-has-events invariant tests /
        // UI / metrics depend on.
        let event = DbEventWrite::from(&run_queued_event(record));
        let result = super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     CREATE runs CONTENT $run;\n\
                     CREATE events CONTENT $event;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("run", record.clone()))
                .bind(("event", event.clone()))
                .await
                .context("failed to enqueue run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_run", &record.run_id)
    }

    /// Batch counterpart to [`Self::enqueue_run`].
    pub async fn enqueue_runs(&self, records: &[RunRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Atomic batch — same rationale as `enqueue_run`: every run record
        // must materialize together with its `RunQueued` event.
        let events: Vec<DbEventWrite> = records
            .iter()
            .map(|r| DbEventWrite::from(&run_queued_event(r)))
            .collect();
        let result = super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .await
                .context("failed to enqueue runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_runs", &format!("batch[{}]", records.len()))
    }

    async fn get_code_version(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<String>> {
        let mut result = self
            .db
            .query("SELECT code_version AS value FROM assets WHERE code_location_id = $cl AND asset_key = $key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .await?;
        let rows: Vec<OptStringField> = result.take(0)?;
        Ok(rows.first().and_then(|r| r.value.clone()))
    }

    /// Bulk upsert `asset_partitions` rows, matched on the table's UNIQUE
    /// (code_location_id, asset_key, partition_key) index — one statement that
    /// updates existing rows in place and inserts new ones. Shared by the
    /// batched `store_events` flush and the single-event `store_event` path.
    async fn upsert_asset_partitions(&self, rows: Vec<DbAssetPartitionWrite>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.db
            .query(
                "INSERT INTO asset_partitions $rows ON DUPLICATE KEY UPDATE \
                 last_event_id = $input.last_event_id, \
                 last_run_id = $input.last_run_id, \
                 last_timestamp = $input.last_timestamp",
            )
            .bind(("rows", rows))
            .await?
            .check()?;
        Ok(())
    }

    async fn query_pool_usage(
        &self,
        code_location_id: &str,
        pool_key: &str,
        now_ns: i64,
    ) -> Result<(PoolLimit, u32)> {
        let mut result = self
            .db
            .query(
                "SELECT * FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $pool_key LIMIT 1; \
                 SELECT math::sum(slots_consumed) AS total FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now GROUP ALL",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns))
            .await?;

        let pools: Vec<PoolLimit> = result.take(0)?;
        let pool = pools
            .into_iter()
            .next()
            .with_context(|| format!("pool '{}' not configured", pool_key))?;
        let claimed: Option<u32> = result.take((1, "total"))?;
        Ok((pool, claimed.unwrap_or(0)))
    }

    /// Build a SurrealQL transaction that atomically checks capacity and claims
    /// slots. Uses a sentinel `claim_version` bump on each pool to force
    /// write-write conflicts under RocksDB's optimistic snapshot isolation,
    /// preventing two concurrent claims from both succeeding.
    ///
    /// All reads/writes are scoped to the caller's `code_location_id` (bound
    /// as `$cl`); pools / slots / pending_steps in other CLs are invisible.
    fn build_claim_transaction(pools: &[(String, u32)]) -> String {
        let mut q = String::from("BEGIN TRANSACTION;\n");

        for (i, (_, _slots)) in pools.iter().enumerate() {
            q += &format!(
                "LET $lim_{i} = (SELECT VALUE slot_limit \
                     FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $p{i})[0] ?? 0;\n\
                 LET $used_{i} = (SELECT VALUE math::sum(slots_consumed) \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $p{i} \
                     AND lease_expires_at > $now \
                     GROUP ALL)[0] ?? 0;\n"
            );
        }

        let conditions: Vec<String> = pools
            .iter()
            .enumerate()
            .map(|(i, (_, slots))| format!("($used_{i} + {slots}) <= $lim_{i}"))
            .collect();
        q += &format!("IF {} {{\n", conditions.join(" AND "));

        // Sentinel: bump claim_version on each pool to force write-write conflicts.
        for i in 0..pools.len() {
            q += &format!(
                "  UPDATE concurrency_pools \
                     SET claim_version = claim_version + 1 \
                     WHERE code_location_id = $cl AND pool_key = $p{i};\n"
            );
        }

        for (i, (_, slots)) in pools.iter().enumerate() {
            q += &format!(
                "  CREATE concurrency_slots SET \
                     code_location_id = $cl, \
                     pool_key = $p{i}, run_id = $run_id, step_key = $step_key, \
                     slots_consumed = {slots}, claimed_at = $now, \
                     lease_expires_at = $lease_exp, last_heartbeat = $now;\n"
            );
        }

        q += "  DELETE FROM pending_steps \
                  WHERE run_id = $run_id AND step_key = $step_key;\n";
        q += "};\n";
        q += "COMMIT TRANSACTION;\n";
        // run_id+step_key are globally unique on slots, so no CL filter needed.
        q += "SELECT count() AS total FROM concurrency_slots \
                  WHERE run_id = $run_id AND step_key = $step_key GROUP ALL;\n";
        q
    }

    /// Statement index of the post-COMMIT SELECT in the claim transaction query.
    /// Layout: BEGIN(0) + 2*N LETs + IF(2N+1) + COMMIT(2N+2) + SELECT(2N+3).
    fn claim_check_statement_index(num_pools: usize) -> usize {
        2 * num_pools + 3
    }

    async fn kv_get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.kv_get(key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn kv_set_json<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.kv_set(key, &bytes).await
    }
}

/// Sentinel error type used by `claim_concurrency_slots` to encode the
/// "snapshot saw the pool as full" race condition as a retryable failure.
/// This is a *logical* retry, not a SurrealDB transient one — it fires when
/// the claim transaction's post-CREATE count check returns 0, meaning a
/// concurrent claim won the race.
#[derive(Debug)]
struct PoolContended;

impl std::fmt::Display for PoolContended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pool snapshot saw full capacity, retry needed")
    }
}

impl std::error::Error for PoolContended {}

/// Convert a unique-index violation on a client-supplied-ID `CREATE` into
/// `Ok(())`, logging a warning. Used by `create_run`, `create_runs`, and
/// `create_backfill` after a `with_retry` loop: the only realistic way to see
/// "already contains" on these methods is a phantom commit on a previous
/// retry attempt, since UUID collision between independent processes is
/// statistically negligible. Treating it as success is therefore correct,
/// and the warning surfaces it to operators in case it ever happens for a
/// reason other than retry (e.g. a real id collision in a test).
fn swallow_phantom_commit(result: Result<()>, op: &'static str, id: &str) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if super::retry::is_unique_index_violation(&e) => {
            tracing::warn!(
                op = op,
                id = %id,
                error = %e,
                "CREATE returned unique-index violation after retry; treating \
                 as success — a previous retry attempt likely committed before \
                 the client saw a transient error (UUID collision across \
                 processes is statistically negligible)"
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

impl StorageBackend for SurrealStorage {
    #[tracing::instrument(skip_all, target = "rivers::storage", fields(cl = %event.code_location_id, asset_key = event.asset_key))]
    async fn store_event(&self, event: &EventRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
        let cl = event.code_location_id.as_str();
        let mut db_event = DbEventWrite::from(event);

        let mut materialization_code_version: Option<String> = None;
        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let cv = self.get_code_version(cl, asset_key).await?;
                db_event.code_version = cv.clone();
                db_event.input_data_versions = event.input_data_versions.clone();
                materialization_code_version = cv;
            }

        let result: Option<DbStoredEvent> = self
            .db
            .create("events")
            .content(db_event)
            .await
            .context("failed to store event")?;
        let stored = result.context("no event returned from create")?;
        let event_id = record_id_str(&stored.id);

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let code_version = materialization_code_version;
                let input_data_versions = event.input_data_versions.clone();

                let data_version = event.event_type.data_version().map(|s| s.to_string());
                self.db
                    .query("UPDATE assets SET last_event_id = $event_id, last_run_id = $run_id, last_timestamp = $timestamp, last_data_version = $data_version, last_materialization_code_version = $mcv, last_input_data_versions = $idv WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.clone()))
                    .bind(("event_id", event_id.clone()))
                    .bind(("run_id", event.run_id.clone()))
                    .bind(("timestamp", event.timestamp))
                    .bind(("data_version", data_version))
                    .bind(("mcv", code_version))
                    .bind(("idv", input_data_versions))
                    .await?;

                if let Some(partition_key) = &event.partition_key {
                    self.upsert_asset_partitions(vec![DbAssetPartitionWrite {
                        code_location_id: cl.to_string(),
                        asset_key: asset_key.clone(),
                        partition_key: partition_key.clone(),
                        last_event_id: event_id.clone(),
                        last_run_id: event.run_id.clone(),
                        last_timestamp: event.timestamp,
                    }])
                    .await?;
                }
            }

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_observation() {
                let data_version = event.event_type.data_version().map(|s| s.to_string());
                self.db
                    .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.clone()))
                    .bind(("event_id", event_id.clone()))
                    .bind(("timestamp", event.timestamp))
                    .bind(("data_version", data_version))
                    .await?;
            }

        Ok(event_id)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = events.len()))]
    async fn store_events(&self, events: &[EventRecord]) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
        if events.is_empty() {
            return Ok(vec![]);
        }

        // Bulk insert all events (code_version + input_data_versions are empty
        // on the DB rows; the asset record gets them in the post-insert loop)
        let db_events: Vec<DbEventWrite> = events.iter().map(DbEventWrite::from).collect();
        let results: Vec<DbStoredEvent> = self
            .db
            .insert("events")
            .content(db_events)
            .await
            .context("failed to batch store events")?;

        let event_ids: Vec<String> = results.iter().map(|e| record_id_str(&e.id)).collect();

        // Group materializations by asset (latest wins), then one bulk upsert.
        let mut latest_mat: std::collections::HashMap<(&str, &str), usize> =
            std::collections::HashMap::new();
        // Dedup partition rows within the batch (last write wins), keyed by the
        // unique (code_location_id, asset_key, partition_key) index.
        let mut part_rows: std::collections::HashMap<
            (&str, &str, &PartitionKey),
            DbAssetPartitionWrite,
        > = std::collections::HashMap::new();

        for (idx, (event, event_id)) in events.iter().zip(event_ids.iter()).enumerate() {
            let Some(asset_key) = &event.asset_key else {
                continue;
            };
            if !event.event_type.is_materialization() {
                continue;
            }
            let cl = event.code_location_id.as_str();
            latest_mat.insert((cl, asset_key.as_str()), idx);
            if let Some(partition_key) = &event.partition_key {
                part_rows.insert(
                    (cl, asset_key.as_str(), partition_key),
                    DbAssetPartitionWrite {
                        code_location_id: cl.to_string(),
                        asset_key: asset_key.clone(),
                        partition_key: partition_key.clone(),
                        last_event_id: event_id.clone(),
                        last_run_id: event.run_id.clone(),
                        last_timestamp: event.timestamp,
                    },
                );
            }
        }

        // One `assets` row update per materialized asset (latest event wins).
        for (&(cl, asset_key), &idx) in &latest_mat {
            let event = &events[idx];
            let event_id = &event_ids[idx];
            let code_version = self.get_code_version(cl, asset_key).await?;
            let data_version = event.event_type.data_version().map(|s| s.to_string());
            self.db
                .query("UPDATE assets SET last_event_id = $event_id, last_run_id = $run_id, last_timestamp = $timestamp, last_data_version = $data_version, last_materialization_code_version = $mcv, last_input_data_versions = $idv WHERE code_location_id = $cl AND asset_key = $asset_key")
                .bind(("cl", cl.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("event_id", event_id.clone()))
                .bind(("run_id", event.run_id.clone()))
                .bind(("timestamp", event.timestamp))
                .bind(("data_version", data_version))
                .bind(("mcv", code_version))
                .bind(("idv", event.input_data_versions.clone()))
                .await?;
        }

        // Upsert the affected partition rows in one bulk statement.
        self.upsert_asset_partitions(part_rows.into_values().collect())
            .await?;

        for (event, event_id) in events.iter().zip(event_ids.iter()) {
            let cl = event.code_location_id.as_str();
            if let Some(asset_key) = &event.asset_key
                && event.event_type.is_observation() {
                    let data_version = event.event_type.data_version().map(|s| s.to_string());
                    self.db
                        .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                        .bind(("cl", cl.to_string()))
                        .bind(("asset_key", asset_key.clone()))
                        .bind(("event_id", event_id.clone()))
                        .bind(("timestamp", event.timestamp))
                        .bind(("data_version", data_version))
                        .await?;
                }
        }

        Ok(event_ids)
        })
        .await
    }

    async fn get_events_for_run(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE run_id = $run_id ORDER BY timestamp ASC, sort_order ASC, id ASC")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn has_step_completed(&self, asset_key: &str, run_ids: &[String]) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            for run_id in run_ids {
                let events = self.get_events_for_run(run_id).await?;
                let found = events.iter().any(|e| {
                    e.asset_key.as_deref() == Some(asset_key)
                        && matches!(
                            e.event_type,
                            EventType::StepSuccess | EventType::StepFailure
                        )
                });
                if found {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(run_id = %run.run_id))]
    async fn create_run(&self, run: &RunRecord) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            let _: Option<RunRecord> = self
                .db
                .create("runs")
                .content(run.clone())
                .await
                .context("failed to create run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_run", &run.run_id)
    }

    async fn create_runs(&self, runs: &[RunRecord]) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            if runs.is_empty() {
                return Ok(());
            }
            let mut q = String::new();
            for (i, _run) in runs.iter().enumerate() {
                q += &format!("CREATE runs CONTENT $r{i};\n");
            }
            let mut query = self.db.query(&q);
            for (i, run) in runs.iter().enumerate() {
                query = query.bind((format!("r{i}"), run.clone()));
            }
            query.await.context("failed to create runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_runs", &format!("batch[{}]", runs.len()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%run_id, ?status))]
    async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query(
                        "UPDATE runs SET status = $status, end_time = $end_time WHERE run_id = $run_id",
                    )
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE runs SET status = $status WHERE run_id = $run_id")
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_run_block_reason(&self, run_id: &str, reason: Option<&str>) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query("UPDATE runs SET block_reason = $reason WHERE run_id = $run_id")
                .bind(("run_id", run_id.to_string()))
                .bind(("reason", reason.map(|s| s.to_string())))
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE run_id = $run_id LIMIT 1")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.into_iter().next())
        })
        .await
    }

    async fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            if run_ids.is_empty() {
                return Ok(Vec::new());
            }
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids AND status = $status")
                    .bind(("ids", run_ids.to_vec()))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids")
                    .bind(("ids", run_ids.to_vec()))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE status = $status ORDER BY start_time DESC LIMIT $limit")
                    .bind(("status", status_str))
                    .bind(("limit", limit))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs ORDER BY start_time DESC LIMIT $limit")
                    .bind(("limit", limit))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since AND status = $status ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_queued_runs(&self) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status = 'Queued'")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn count_in_progress_runs(&self) -> Result<usize> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.len())
        })
        .await
    }

    async fn get_in_progress_runs(&self) -> Result<Vec<RunRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_observations_since(&self, since_timestamp: i64) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE event_type = $etype AND timestamp > $since ORDER BY timestamp DESC")
                .bind(("etype", "Observation".to_string()))
                .bind(("since", since_timestamp))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM kv WHERE key = $key LIMIT 1")
                .bind(("key", key.to_string()))
                .await?;
            let kvs: Vec<DbKv> = result.take(0)?;
            Ok(kvs.into_iter().next().map(|kv| kv.value.to_vec()))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query("DELETE FROM kv WHERE key = $key")
                .bind(("key", key.to_string()))
                .await?;
            let _: Option<DbKv> = self
                .db
                .create("kv")
                .content(DbKv {
                    key: key.to_string(),
                    value: Bytes::from(value.to_vec()),
                })
                .await?;
            Ok(())
        })
        .await
    }

    async fn store_tick(&self, tick: &TickRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
            let db_tick = DbTickWrite::from(tick);
            let result: Option<DbStoredTick> = self
                .db
                .create("ticks")
                .content(db_tick)
                .await
                .context("failed to store tick")?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = ticks.len()))]
    async fn store_ticks_batch(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            if ticks.is_empty() {
                return Ok(vec![]);
            }
            let db_ticks: Vec<DbTickWrite> = ticks.iter().map(DbTickWrite::from).collect();
            let results: Vec<DbStoredTick> = self
                .db
                .insert("ticks")
                .content(db_ticks)
                .await
                .context("failed to batch store ticks")?;
            Ok(results.iter().map(|t| record_id_str(&t.id)).collect())
        })
        .await
    }

    async fn store_condition_tick(&self, tick: &ConditionTickRecord) -> Result<String> {
        super::retry::with_retry(&self.retry_config, || async {
            let db_tick = DbConditionTickWrite::from(tick);
            let result: Option<DbStoredConditionTick> =
                self.db.create("condition_ticks").content(db_tick).await?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = evals.len()))]
    async fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> Result<Vec<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            if evals.is_empty() {
                return Ok(vec![]);
            }
            let db_evals: Vec<DbConditionEvalWrite> =
                evals.iter().map(DbConditionEvalWrite::from).collect();
            let results: Vec<DbStoredConditionEval> =
                self.db.insert("condition_evals").content(db_evals).await?;
            Ok(results.iter().map(|e| record_id_str(&e.id)).collect())
        })
        .await
    }

    // ── Backfills ──

    async fn create_backfill(&self, backfill: &BackfillRecord) -> Result<()> {
        let result = super::retry::with_retry(&self.retry_config, || async {
            let _: Option<BackfillRecord> = self
                .db
                .create("backfills")
                .content(backfill.clone())
                .await
                .context("failed to create backfill")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_backfill", &backfill.backfill_id)
    }

    async fn update_backfill_status(
        &self,
        backfill_id: &str,
        status: BackfillStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query("UPDATE backfills SET status = $status, end_time = $end_time WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE backfills SET status = $status WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_backfill_progress(
        &self,
        backfill_id: &str,
        run_ids: &[String],
        completed: &[PartitionKey],
        failed: &[PartitionKey],
        canceled: &[PartitionKey],
    ) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     run_ids = array::union(run_ids, $run_ids), \
                     completed_partitions = array::union(completed_partitions, $completed), \
                     failed_partitions = array::union(failed_partitions, $failed), \
                     canceled_partitions = array::union(canceled_partitions, $canceled) \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("run_ids", run_ids.to_vec()))
                .bind(("completed", completed.to_vec()))
                .bind(("failed", failed.to_vec()))
                .bind(("canceled", canceled.to_vec()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM backfills WHERE backfill_id = $id LIMIT 1")
                .bind(("id", backfill_id.to_string()))
                .await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            Ok(rows.into_iter().next())
        })
        .await
    }

    async fn try_complete_backfill(
        &self,
        backfill_id: &str,
        extra_canceled: &[PartitionKey],
    ) -> Result<Option<BackfillStatus>> {
        let backfill = self
            .get_backfill(backfill_id)
            .await?
            .context("backfill not found")?;

        if backfill.status != BackfillStatus::InProgress {
            return Ok(None);
        }

        let runs = if backfill.run_ids.is_empty() {
            Vec::new()
        } else {
            self.get_runs_by_ids(&backfill.run_ids, None).await?
        };
        let all_terminal = runs.iter().all(|r| {
            matches!(
                r.status,
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
            )
        });
        if !all_terminal {
            return Ok(None);
        }
        // Nothing to finalize: no terminal runs and no externally-canceled keys.
        if runs.is_empty() && extra_canceled.is_empty() {
            return Ok(None);
        }

        let mut completed_pks: Vec<PartitionKey> = Vec::new();
        let mut failed_pks: Vec<PartitionKey> = Vec::new();
        let mut canceled_pks: Vec<PartitionKey> = Vec::new();
        let mut any_failed = false;
        let mut any_canceled = false;

        // One query for every Success run's per-partition StepFailures, keyed by
        // run_id. Safe because EventWriter::flush makes events durable before a
        // run is marked terminal (TODO.md: durable RunRecord field alternative).
        #[derive(SurrealValue)]
        struct FailRow {
            run_id: String,
            partition_key: PartitionKey,
        }
        let success_run_ids: Vec<String> = runs
            .iter()
            .filter(|r| matches!(r.status, RunStatus::Success))
            .map(|r| r.run_id.clone())
            .collect();
        let mut failed_by_run: std::collections::HashMap<
            String,
            std::collections::HashSet<PartitionKey>,
        > = std::collections::HashMap::new();
        if !success_run_ids.is_empty() {
            let rows: Vec<FailRow> = super::retry::with_retry(&self.retry_config, || async {
                let mut res = self
                    .db
                    .query(
                        // IS NOT NONE: exclude None-keyed step-level failures (see get_failed_partitions).
                        "SELECT run_id, partition_key FROM events \
                         WHERE run_id IN $rids AND event_type = 'StepFailure' \
                         AND partition_key IS NOT NONE GROUP BY run_id, partition_key",
                    )
                    .bind(("rids", success_run_ids.clone()))
                    .await?;
                Ok(res.take(0)?)
            })
            .await?;
            for row in rows {
                failed_by_run
                    .entry(row.run_id)
                    .or_default()
                    .insert(row.partition_key);
            }
        }

        for run in &runs {
            let Some(ref pk) = run.partition_key else {
                match run.status {
                    RunStatus::Failure => any_failed = true,
                    RunStatus::Canceled => any_canceled = true,
                    _ => {}
                }
                continue;
            };
            match run.status {
                RunStatus::Success => {
                    let failed_members = failed_by_run.get(&run.run_id);
                    for member in pk.members() {
                        if failed_members.is_some_and(|f| f.contains(&member)) {
                            failed_pks.push(member);
                            any_failed = true;
                        } else {
                            completed_pks.push(member);
                        }
                    }
                }
                RunStatus::Failure => {
                    failed_pks.extend(pk.members());
                    any_failed = true;
                }
                RunStatus::Canceled => {
                    canceled_pks.extend(pk.members());
                    any_canceled = true;
                }
                _ => {}
            }
        }

        // Never-launched partitions (stop-on-failure / cancel) have no run record.
        if !extra_canceled.is_empty() {
            canceled_pks.extend(extra_canceled.iter().cloned());
            any_canceled = true;
        }

        // Set partition tracking from the authoritative run statuses.
        // Uses direct SET (not array::union) so this is idempotent regardless
        // of whether the local execute_backfill path already tracked partitions.
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     completed_partitions = $completed, \
                     failed_partitions = $failed, \
                     canceled_partitions = $canceled \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("completed", completed_pks.clone()))
                .bind(("failed", failed_pks.clone()))
                .bind(("canceled", canceled_pks.clone()))
                .await?;
            Ok(())
        })
        .await?;

        let new_status = if any_failed {
            BackfillStatus::CompletedFailed
        } else if any_canceled {
            BackfillStatus::Canceled
        } else {
            BackfillStatus::CompletedSuccess
        };

        let now = now_nanos();
        self.update_backfill_status(backfill_id, new_status.clone(), Some(now))
            .await?;
        Ok(Some(new_status))
    }

    // Concurrency pools

    async fn free_concurrency_slots(&self, run_id: &str, step_key: &str) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     DELETE FROM pending_steps \
                     WHERE run_id = $run_id AND step_key = $step_key",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn free_concurrency_slots_for_run(&self, run_id: &str) -> Result<()> {
        super::retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots WHERE run_id = $run_id; \
                     DELETE FROM pending_steps WHERE run_id = $run_id",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> Result<u32> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

            // UPDATE doesn't return a usable row count via the Rust SDK,
            // so we use a follow-up SELECT to count renewed rows.
            let mut result = self
                .db
                .query(
                    "UPDATE concurrency_slots \
                         SET lease_expires_at = $lease_exp, last_heartbeat = $now \
                         WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM concurrency_slots \
                         WHERE run_id = $run_id AND step_key = $step_key GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("now", now_ns))
                .bind(("lease_exp", lease_exp))
                .await?;
            let renewed: Option<u32> = result.take((1, "total"))?;
            Ok(renewed.unwrap_or(0))
        })
        .await
    }

    async fn free_expired_leases(&self) -> Result<u32> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "SELECT count() AS total FROM concurrency_slots \
                         WHERE lease_expires_at <= $now GROUP ALL; \
                     DELETE FROM concurrency_slots WHERE lease_expires_at <= $now",
                )
                .bind(("now", now_ns))
                .await?;
            let expired: Option<u32> = result.take((0, "total"))?;
            Ok(expired.unwrap_or(0))
        })
        .await
    }

    async fn cancel_queued_run(&self, run_id: &str) -> Result<bool> {
        super::retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "UPDATE runs SET status = $new_status, end_time = $now \
                         WHERE run_id = $run_id AND status = $queued_status; \
                     DELETE FROM pending_steps WHERE run_id = $run_id; \
                     SELECT count() AS total FROM runs \
                         WHERE run_id = $run_id AND status = $new_status GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("new_status", RunStatus::Canceled))
                .bind(("queued_status", RunStatus::Queued))
                .bind(("now", now_ns))
                .await?;
            let count: Option<u32> = result.take((2, "total"))?;
            Ok(count.unwrap_or(0) > 0)
        })
        .await
    }

    async fn get_run_progress(&self, run_id: &str) -> Result<RunProgress> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT \
                         math::sum(IF event_type = 'StepStart' THEN 1 ELSE 0 END) AS total, \
                         math::sum(IF event_type = 'StepSuccess' OR (event_type = 'StepFailure' AND partition_key IS NONE) THEN 1 ELSE 0 END) AS completed \
                     FROM events WHERE run_id = $run_id GROUP ALL; \
                     SELECT asset_key, timestamp FROM events \
                         WHERE run_id = $run_id \
                         AND (event_type = 'StepSuccess' OR (event_type = 'StepFailure' AND partition_key IS NONE)) \
                         ORDER BY timestamp DESC LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            let total: Option<u32> = result.take((0, "total"))?;
            let completed: Option<u32> = result.take((0, "completed"))?;

            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct LastStep {
                asset_key: Option<String>,
                timestamp: i64,
            }
            let last_steps: Vec<LastStep> = result.take(1)?;
            let last = last_steps.into_iter().next();

            Ok(RunProgress {
                completed_steps: completed.unwrap_or(0),
                total_steps: total.unwrap_or(0),
                last_step_completed_at: last.as_ref().map(|s| s.timestamp),
                last_completed_step: last.and_then(|s| s.asset_key),
            })
        })
        .await
    }

    async fn get_run_outcome(&self, run_id: &str) -> Result<Option<RunOutcome>> {
        let key = format!("run_outcome:{run_id}");
        let data = self.kv_get(&key).await?;
        match data {
            Some(bytes) => {
                let outcome: RunOutcome = serde_json::from_slice(&bytes)?;
                Ok(Some(outcome))
            }
            None => Ok(None),
        }
    }

    async fn set_run_outcome(&self, run_id: &str, outcome: &RunOutcome) -> Result<()> {
        let key = format!("run_outcome:{run_id}");
        let bytes = serde_json::to_vec(outcome)?;
        self.kv_set(&key, &bytes).await
    }

    async fn request_cancellation(&self, run_id: &str) -> Result<()> {
        let key = format!("cancel:{run_id}");
        self.kv_set(&key, b"1").await
    }

    async fn is_cancelled(&self, run_id: &str) -> Result<bool> {
        let key = format!("cancel:{run_id}");
        Ok(self.kv_get(&key).await?.is_some())
    }

    async fn get_events_for_step(&self, run_id: &str, step_key: &str) -> Result<Vec<StoredEvent>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                         WHERE run_id = $run_id AND asset_key = $step_key \
                         ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_completed_step_keys(&self, run_id: &str) -> Result<HashSet<String>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepSuccess'",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows.into_iter().filter_map(|r| r.asset_key).collect())
        })
        .await
    }

    async fn get_step_data_versions(&self, run_id: &str) -> Result<HashMap<String, String>> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key, data_version FROM events \
                         WHERE run_id = $run_id AND event_type = 'Materialization' \
                           AND data_version IS NOT NULL",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
                data_version: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows
                .into_iter()
                .filter_map(|r| Some((r.asset_key?, r.data_version?)))
                .collect())
        })
        .await
    }
}

impl PerCodeLocationStorage for SurrealStorage {
    async fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let mut result = self
            .db
            .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let event_type_str = "Materialization".to_string();
        let mut result = if let Some(partition_key) = partition {
            // The display string may denote a Single or a Multi key. Both
            // readings can have rows when the asset's def changed shape
            // (legacy Single events vs current Multi ones) — the structured
            // reading wins, so try candidates most-structured first instead
            // of letting timestamps decide.
            for cand in PartitionKey::display_candidates(partition_key)
                .into_iter()
                .rev()
            {
                let mut result = self.db
                    .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                    .bind(("cl", code_location_id.to_string()))
                    .bind(("asset_key", asset_key.to_string()))
                    .bind(("pk", cand))
                    .bind(("event_type", event_type_str.clone()))
                    .await?;
                let events: Vec<DbStoredEvent> = result.take(0)?;
                if let Some(e) = events.into_iter().next() {
                    return Ok(Some(e.into_stored_event()));
                }
            }
            return Ok(None);
        } else {
            self.db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("event_type", event_type_str))
                .await?
        };
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().next().map(|e| e.into_stored_event()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets.into_iter().next())
    }

    async fn get_asset_records(&self, code_location_id: &str) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> Result<Vec<AssetRecord>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let keys_vec: Vec<String> = keys.to_vec();
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key IN $keys")
            .bind(("cl", code_location_id.to_string()))
            .bind(("keys", keys_vec))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, count = records.len()))]
    async fn register_assets(&self, code_location_id: &str, records: &[AssetRecord]) -> Result<()> {
        for record in records {
            let mut existing = self
                .db
                .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", record.asset_key.clone()))
                .await?;
            let found: Vec<AssetRecord> = existing.take(0)?;

            if found.is_empty() {
                // Create new asset record. Force CL field on the record to
                // match the scope, so a stale value on the input record
                // can't write into a different CL.
                let mut to_insert = record.clone();
                to_insert.code_location_id = code_location_id.to_string();
                let _: Option<AssetRecord> = self.db.create("assets").content(to_insert).await?;
            } else {
                self.db
                    .query("UPDATE assets SET tags = $tags, kinds = $kinds, asset_group = $asset_group, code_version = $code_version, pool = $pool WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", code_location_id.to_string()))
                    .bind(("asset_key", record.asset_key.clone()))
                    .bind(("tags", record.tags.clone()))
                    .bind(("kinds", record.kinds.clone()))
                    .bind(("asset_group", record.asset_group.clone()))
                    .bind(("code_version", record.code_version.clone()))
                    .bind(("pool", record.pool.clone()))
                    .await?;
            }
        }

        Ok(())
    }

    async fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND tags CONTAINS $tag")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tag", tag.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND kinds CONTAINS $kind")
            .bind(("cl", code_location_id.to_string()))
            .bind(("kind", kind.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_group = $group")
            .bind(("cl", code_location_id.to_string()))
            .bind(("group", group.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.db
            .query(
                "UPDATE runs SET block_reason = $reason \
                 WHERE status = $status AND code_location_id = $cl",
            )
            .bind(("status", format!("{:?}", status)))
            .bind(("reason", reason.map(|s| s.to_string())))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        Ok(())
    }

    async fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM concurrency_slots \
                     WHERE lease_expires_at <= $now GROUP ALL; \
                 DELETE FROM concurrency_slots WHERE lease_expires_at <= $now; \
                 SELECT run_id, code_location_id, tags, node_names, priority, partition_key, start_time \
                     FROM runs WHERE status IN ['NotStarted', 'Started'] AND code_location_id = $cl; \
                 SELECT run_id, code_location_id, tags, node_names, priority, partition_key, start_time \
                     FROM runs WHERE status = 'Queued' AND code_location_id = $cl",
            )
            .bind(("now", now_ns))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let expired: Option<u32> = result.take((0, "total"))?;
        // Statement 1 is the DELETE (no result needed)
        let in_progress: Vec<CoordinatorRunInfo> = result.take(2)?;
        let queued: Vec<CoordinatorRunInfo> = result.take(3)?;
        Ok((expired.unwrap_or(0), in_progress, queued))
    }

    async fn add_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> Result<()> {
        for key in partition_keys {
            if key.is_empty() {
                anyhow::bail!("dynamic partition keys must not be empty");
            }
            if let Some(ch) = PartitionKey::reserved_display_char(key) {
                anyhow::bail!(
                    "partition key '{key}' contains reserved character '{ch}' \
                     (used by the canonical display form)"
                );
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for key in partition_keys {
            let mut result = self
                .db
                .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .bind(("key", key.clone()))
                .await?;
            let existing: Vec<DbDynamicPartition> = result.take(0)?;
            if existing.is_empty() {
                let _: Option<DbDynamicPartition> = self
                    .db
                    .create("dynamic_partitions")
                    .content(DbDynamicPartition {
                        code_location_id: code_location_id.to_string(),
                        partitions_def_name: partitions_def_name.to_string(),
                        partition_key: key.clone(),
                        create_timestamp: now,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn delete_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.db
            .query("DELETE FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        Ok(())
    }

    async fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<Vec<String>> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name ORDER BY partition_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(!rows.is_empty())
    }

    async fn get_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        limit: usize,
    ) -> Result<Vec<StoredTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored_tick()).collect())
    }

    async fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM ticks WHERE code_location_id = $cl AND automation_name = $name AND id NOT IN $keep.id RETURN BEFORE;"
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let deleted: Vec<DbStoredTick> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_condition_ticks(
        &self,
        code_location_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredConditionTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored()).collect())
    }

    async fn prune_condition_ticks(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM condition_ticks WHERE code_location_id = $cl AND id NOT IN $keep.id RETURN BEFORE;",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let deleted: Vec<DbStoredConditionTick> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND tick_id = $tick_id ORDER BY asset_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tick_id", tick_id.to_string()))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn prune_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        max_evals: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key AND id NOT IN $keep.id RETURN BEFORE;"
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .bind(("max", max_evals))
            .await?;
        let deleted: Vec<DbStoredConditionEval> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        // Most-structured reading first; see get_latest_materialization.
        for cand in PartitionKey::display_candidates(partition_key)
            .into_iter()
            .rev()
        {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk ORDER BY timestamp DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("pk", cand))
                .bind(("limit", limit))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            if !events.is_empty() {
                return Ok(events.into_iter().map(|e| e.into_stored_event()).collect());
            }
        }
        Ok(Vec::new())
    }

    async fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query("SELECT partition_key FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<u64> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<u64> {
        super::retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let mut result = self
            .db
            .query("SELECT partition_key, last_timestamp FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key AND last_timestamp IS NOT NULL")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
        }

        let rows: Vec<PartTsRow> = result.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp))
            .collect())
    }

    async fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query(
                // IS NOT NONE: exclude None-keyed step-level events (see get_failed_partitions).
                "SELECT partition_key FROM events WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepStart' AND partition_key IS NOT NONE \
                 AND run_id IN (SELECT VALUE run_id FROM runs WHERE code_location_id = $cl AND status = 'Started' \
                 AND $asset_key IN node_names) \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &std::collections::HashMap<PartitionKey, i64>,
    ) -> Result<std::collections::HashMap<PartitionKey, i64>> {
        // No run-status filter: mark_partition_failed lands in a Success run (review #1).
        let mut result = self
            .db
            .query(
                // IS NOT NONE, not NULL — `NONE IS NOT NULL` holds in SurrealDB.
                "SELECT partition_key, math::max(timestamp) AS ts FROM events \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepFailure' AND partition_key IS NOT NONE \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct FailRow {
            partition_key: PartitionKey,
            ts: i64,
        }

        let failed_rows: Vec<FailRow> = result.take(0)?;
        // Expand each failure key to members (a raised batch records one Set), latest ts each.
        let mut latest_failure: std::collections::HashMap<PartitionKey, i64> =
            std::collections::HashMap::new();
        for row in failed_rows {
            for member in row.partition_key.members() {
                latest_failure
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(row.ts))
                    .or_insert(row.ts);
            }
        }
        Ok(latest_failure
            .into_iter()
            .filter(|(pk, ts)| materialized.get(pk).is_none_or(|&mat_ts| mat_ts < *ts))
            .collect())
    }

    async fn get_backfills(
        &self,
        code_location_id: &str,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        let mut query = "SELECT * FROM backfills WHERE code_location_id = $cl".to_string();
        if status.is_some() {
            query.push_str(" AND status = $status");
        }
        query.push_str(" ORDER BY create_time DESC");
        if limit.is_some() {
            query.push_str(" LIMIT $limit");
        }
        let mut q = self
            .db
            .query(&query)
            .bind(("cl", code_location_id.to_string()));
        if let Some(s) = status {
            q = q.bind(("status", format!("{:?}", s)));
        }
        if let Some(lim) = limit {
            q = q.bind(("limit", lim));
        }
        let mut result = q.await?;
        let rows: Vec<BackfillRecord> = result.take(0)?;
        Ok(rows)
    }

    async fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        self.db
            .query(
                "UPSERT concurrency_pools SET \
                     code_location_id = $cl, \
                     pool_key = $pool_key, \
                     slot_limit = $slot_limit, \
                     lease_duration_secs = $lease_duration_secs \
                 WHERE code_location_id = $cl AND pool_key = $pool_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("slot_limit", limit))
            .bind(("lease_duration_secs", lease_duration_secs))
            .await?;
        Ok(())
    }

    async fn get_pool_limits(&self, code_location_id: &str) -> Result<Vec<PoolLimit>> {
        let mut result = self
            .db
            .query("SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let pools: Vec<PoolLimit> = result.take(0)?;
        Ok(pools)
    }

    async fn get_pool_info(&self, code_location_id: &str, pool_key: &str) -> Result<PoolInfo> {
        let now_ns = now_nanos();
        let (pool, claimed_count) = self
            .query_pool_usage(code_location_id, pool_key, now_ns)
            .await?;

        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM pending_steps \
                     WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .await?;
        let pending_count: Option<u32> = result.take((0, "total"))?;

        Ok(PoolInfo {
            pool_key: pool.pool_key,
            slot_limit: pool.slot_limit,
            lease_duration_secs: pool.lease_duration_secs,
            claimed_count,
            pending_count: pending_count.unwrap_or(0),
        })
    }

    async fn get_all_pool_infos(&self, code_location_id: &str) -> Result<Vec<PoolInfo>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC; \
                 SELECT pool_key, math::sum(slots_consumed) AS claimed \
                     FROM concurrency_slots WHERE code_location_id = $cl AND lease_expires_at > $now \
                     GROUP BY pool_key; \
                 SELECT pool_key, count() AS pending \
                     FROM pending_steps WHERE code_location_id = $cl GROUP BY pool_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("now", now_ns))
            .await?;

        let pools: Vec<PoolLimit> = result.take(0)?;

        #[derive(SurrealValue, serde::Deserialize)]
        struct ClaimedRow {
            pool_key: String,
            claimed: u32,
        }
        let claimed_rows: Vec<ClaimedRow> = result.take(1)?;
        let claimed_map: std::collections::HashMap<String, u32> = claimed_rows
            .into_iter()
            .map(|r| (r.pool_key, r.claimed))
            .collect();

        #[derive(SurrealValue, serde::Deserialize)]
        struct PendingRow {
            pool_key: String,
            pending: u32,
        }
        let pending_rows: Vec<PendingRow> = result.take(2)?;
        let pending_map: std::collections::HashMap<String, u32> = pending_rows
            .into_iter()
            .map(|r| (r.pool_key, r.pending))
            .collect();

        Ok(pools
            .into_iter()
            .map(|p| PoolInfo {
                claimed_count: claimed_map.get(&p.pool_key).copied().unwrap_or(0),
                pending_count: pending_map.get(&p.pool_key).copied().unwrap_or(0),
                pool_key: p.pool_key,
                slot_limit: p.slot_limit,
                lease_duration_secs: p.lease_duration_secs,
            })
            .collect())
    }

    async fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        anyhow::ensure!(!pools.is_empty(), "pools must not be empty");

        // Retry on either a SurrealDB transient failure (handled by the
        // default classifier — RocksDB conflicts, query timeouts) OR on a
        // `PoolContended` sentinel emitted when the claim transaction's
        // snapshot saw the pool as full from a concurrent winner. The latter
        // is a *logical* retry (the operation succeeded but a race lost), so
        // it has to be re-injected through the predicate.
        let predicate =
            |e: &anyhow::Error| super::retry::default_should_retry(e) || e.is::<PoolContended>();

        let result = super::retry::with_retry_if(&self.retry_config, predicate, || async {
            self.try_claim_concurrency_slots_once(
                code_location_id,
                pools,
                run_id,
                step_key,
                priority,
                lease_duration_secs,
            )
            .await
        })
        .await;

        // Replace the exhausted-retries error with the user-facing message
        // when the final cause was a pool contention sentinel.
        match result {
            Ok(status) => Ok(status),
            Err(e) if e.is::<PoolContended>() => {
                anyhow::bail!("failed to claim concurrency slots — extreme contention on pool")
            }
            Err(e) => Err(e),
        }
    }

    async fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<Vec<SlotHolder>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT run_id, step_key, slots_consumed, claimed_at, lease_expires_at \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now \
                     ORDER BY claimed_at ASC",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns))
            .await?;
        let holders: Vec<SlotHolder> = result.take(0)?;
        Ok(holders)
    }

    async fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = $status ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("status", status_str))
                .bind(("limit", limit))
                .await?
        } else {
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("limit", limit))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_queued_runs(&self, code_location_id: &str) -> Result<Vec<RunRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = 'Queued'")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: super::SortOrder,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since AND status = $status ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .bind(("status", status_str))
                .await?
        } else {
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        self.kv_get_json(&crate::condition_eval_state_key(code_location_id))
            .await
    }

    async fn set_condition_eval_state(
        &self,
        code_location_id: &str,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        self.kv_set_json(&crate::condition_eval_state_key(code_location_id), state)
            .await
    }

    async fn get_graph_topology(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::assets::graph::GraphTopology>> {
        self.kv_get_json(&crate::graph_topology_key(code_location_id))
            .await
    }

    async fn set_graph_topology(
        &self,
        code_location_id: &str,
        topology: &crate::assets::graph::GraphTopology,
    ) -> Result<()> {
        self.kv_set_json(&crate::graph_topology_key(code_location_id), topology)
            .await
    }
}

impl SurrealStorage {
    /// One attempt of the [`PerCodeLocationStorage::claim_concurrency_slots`]
    /// flow. Returns `Err(PoolContended)` when the snapshot lost a race —
    /// the caller's predicate re-runs the whole attempt.
    async fn try_claim_concurrency_slots_once(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        let now_ns = now_nanos();
        let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

        // Read capacity (outside transaction, for BlockReason data).
        // Pools with slot_limit < 0 are unlimited — skip them entirely.
        let mut blocked = Vec::new();
        let mut limited_pools: Vec<(String, u32)> = Vec::new();
        for (pool_key, slots_needed) in pools {
            let (pool, current_used) = self
                .query_pool_usage(code_location_id, pool_key, now_ns)
                .await?;
            if pool.slot_limit < 0 {
                continue; // unlimited — no claim needed
            }
            limited_pools.push((pool_key.clone(), *slots_needed));
            if current_used + *slots_needed > pool.slot_limit as u32 {
                blocked.push(PoolBlockDetail {
                    pool_key: pool_key.clone(),
                    claimed: current_used,
                    limit: pool.slot_limit,
                });
            }
        }

        if limited_pools.is_empty() {
            return Ok(ConcurrencyClaimStatus::Claimed);
        }

        if !blocked.is_empty() {
            let first_pool = blocked[0].pool_key.clone();
            let reason = if blocked.len() == 1 {
                let b = &blocked[0];
                BlockReason::PoolFull {
                    pool_key: b.pool_key.clone(),
                    claimed: b.claimed,
                    limit: b.limit,
                }
            } else {
                BlockReason::PoolsFull { pools: blocked }
            };
            let reason_str = reason.to_string();

            let mut result = self
                .db
                .query(
                    "UPSERT pending_steps SET \
                         code_location_id = $cl, \
                         pool_key = $pool_key, \
                         run_id = $run_id, \
                         step_key = $step_key, \
                         priority = $priority, \
                         enqueued_at = $now, \
                         block_reason = $reason \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM pending_steps \
                         WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("pool_key", first_pool))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("priority", priority))
                .bind(("now", now_ns))
                .bind(("reason", reason_str))
                .await?;
            let position: Option<u32> = result.take((1, "total"))?;

            return Ok(ConcurrencyClaimStatus::Pending {
                position: position.unwrap_or(1),
                reason,
            });
        }

        // Atomic claim transaction with sentinel. Re-verifies capacity from
        // its snapshot and bumps claim_version to force write-write conflicts
        // with concurrent claims.
        let txn_query = Self::build_claim_transaction(&limited_pools);
        let mut q = self.db.query(&txn_query);
        for (i, (pool_key, _)) in limited_pools.iter().enumerate() {
            q = q.bind((format!("p{i}"), pool_key.clone()));
        }
        q = q
            .bind(("cl", code_location_id.to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("step_key", step_key.to_string()))
            .bind(("now", now_ns))
            .bind(("lease_exp", lease_exp));

        // Transient SurrealDB errors propagate via `?` — the caller's
        // predicate decides whether to retry.
        let mut response = q.await?.check()?;

        let check_idx = Self::claim_check_statement_index(pools.len());
        let count: Option<u32> = response.take((check_idx, "total"))?;

        if count.unwrap_or(0) > 0 {
            Ok(ConcurrencyClaimStatus::Claimed)
        } else {
            // Transaction's snapshot saw the pool as full — surface as a
            // retryable sentinel so the outer `with_retry_if` retries with
            // exponential backoff via the shared retry config.
            Err(anyhow::Error::new(PoolContended))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BackfillFailurePolicy, BackfillStrategy, LaunchedBy, StaleStatus};
    use super::*;

    #[test]
    fn surreal_connect_config_unauthenticated_uses_default_scope() {
        let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000");
        assert_eq!(cfg.endpoint, "ws://surrealdb:8000");
        assert_eq!(cfg.namespace, DEFAULT_NAMESPACE);
        assert_eq!(cfg.database, DEFAULT_DATABASE);
        assert!(cfg.credentials.is_none());
    }

    #[test]
    fn surreal_connect_config_with_credentials_attaches_database_creds() {
        let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000")
            .with_credentials("rivers".into(), "topsecret".into());
        match cfg.credentials {
            Some(SurrealCredentials::Database { username, password }) => {
                assert_eq!(username, "rivers");
                assert_eq!(password, "topsecret");
            }
            None => panic!("credentials should be set"),
        }
    }

    async fn make_storage() -> SurrealStorage {
        SurrealStorage::new_memory()
            .await
            .expect("failed to create in-memory storage")
    }

    /// The run-events page must scan `idx_events_run_ts` (timestamp order), not sort.
    /// RocksDB backend — kv-mem plans differently.
    #[tokio::test]
    async fn run_events_page_uses_ordering_index() {
        let temp = test_temp_dir::test_temp_dir!();
        let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        let rows: Vec<DbEventWrite> = (0..2000i64)
            .map(|i| DbEventWrite {
                code_location_id: "default".into(),
                event_type: if i % 50 == 0 {
                    "LogOutput"
                } else {
                    "Materialization"
                }
                .into(),
                asset_key: Some("a".into()),
                run_id: "r".into(),
                partition_key: None,
                timestamp: i,
                sort_order: 0,
                metadata: vec![],
                data_version: None,
                code_version: None,
                input_data_versions: vec![],
            })
            .collect();
        s.db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", rows))
            .await
            .unwrap()
            .check()
            .unwrap();

        let plan: Vec<serde_json::Value> =
            s.db.query(
                "SELECT * FROM events WHERE run_id = 'r' AND event_type != 'LogOutput' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT 50 START 0 EXPLAIN",
            )
            .await
            .unwrap()
            .take(0)
            .unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("idx_events_run_ts"),
            "page should scan idx_events_run_ts: {plan}"
        );
        assert!(
            !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
            "page should not sort — the ordering index covers it: {plan}"
        );
    }

    /// The asset-events page (`ORDER BY timestamp ... LIMIT`) must scan
    /// `idx_events_loc_asset_ts`, not sort every matching event. RocksDB backend.
    #[tokio::test]
    async fn asset_events_page_uses_ordering_index() {
        let temp = test_temp_dir::test_temp_dir!();
        let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        let rows: Vec<DbEventWrite> = (0..2000i64)
            .map(|i| DbEventWrite {
                code_location_id: "default".into(),
                event_type: if i % 3 == 0 {
                    "Observation"
                } else {
                    "Materialization"
                }
                .into(),
                asset_key: Some("a".into()),
                run_id: "r".into(),
                partition_key: None,
                timestamp: i,
                sort_order: 0,
                metadata: vec![],
                data_version: None,
                code_version: None,
                input_data_versions: vec![],
            })
            .collect();
        s.db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", rows))
            .await
            .unwrap()
            .check()
            .unwrap();

        let plan: Vec<serde_json::Value> =
            s.db.query(
                "SELECT * FROM events WHERE code_location_id = 'default' AND asset_key = 'a' \
                 AND event_type IN ['Materialization', 'Observation'] \
                 ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT 50 START 0 EXPLAIN",
            )
            .await
            .unwrap()
            .take(0)
            .unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("idx_events_loc_asset_ts"),
            "asset page should scan idx_events_loc_asset_ts: {plan}"
        );
        assert!(
            !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
            "asset page should not sort: {plan}"
        );
    }

    /// The UNIQUE index compares the SERIALIZED partition_key, so the same
    /// logical Multi key written with dims in a different order must
    /// canonicalize to one row — Python builds dims from a HashMap whose
    /// iteration order varies per instance.
    #[tokio::test]
    async fn test_multi_partition_key_dims_order_canonicalized() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let mk = |dims: Vec<(&str, &str)>| PartitionKey::Multi {
            dims: dims
                .into_iter()
                .map(|(d, v)| (d.to_string(), vec![v.to_string()]))
                .collect(),
        };
        let row = |pk: PartitionKey, event_id: &str, ts: i64| super::DbAssetPartitionWrite {
            code_location_id: cl.to_string(),
            asset_key: "inventory".to_string(),
            partition_key: pk,
            last_event_id: event_id.to_string(),
            last_run_id: "r".to_string(),
            last_timestamp: ts,
        };

        let date_first = mk(vec![("date", "2024-01-01"), ("region", "eu")]);
        let region_first = mk(vec![("region", "eu"), ("date", "2024-01-01")]);
        storage
            .upsert_asset_partitions(vec![row(date_first.clone(), "ev1", 1)])
            .await
            .unwrap();
        storage
            .upsert_asset_partitions(vec![row(region_first, "ev2", 2)])
            .await
            .unwrap();

        let parts = storage
            .get_materialized_partitions(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            parts,
            vec![date_first],
            "dims order must canonicalize to one row"
        );
        assert_eq!(
            storage
                .count_materialized_partitions(cl, "inventory")
                .await
                .unwrap(),
            1,
            "the count must agree with the deduped key set"
        );
    }

    /// Rows persisted by earlier builds carry HashMap-ordered dims; the
    /// startup migration must collapse duplicates onto the canonical form
    /// and rewrite unsorted event keys.
    #[tokio::test]
    async fn test_migration_canonicalizes_legacy_multi_rows() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        // Legacy row with unsorted dims, written raw to bypass the
        // canonicalizing serializer.
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();
        // The same logical partition written by a current build (sorted).
        let sorted = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        storage
            .upsert_asset_partitions(vec![DbAssetPartitionWrite {
                code_location_id: cl.to_string(),
                asset_key: "inv".to_string(),
                partition_key: sorted.clone(),
                last_event_id: "e2".to_string(),
                last_run_id: "r2".to_string(),
                last_timestamp: 2,
            }])
            .await
            .unwrap();
        // Legacy event with unsorted dims.
        storage
            .db
            .query(
                "CREATE events CONTENT { code_location_id: 'default', \
                 event_type: 'Materialization', asset_key: 'inv', run_id: 'r1', \
                 partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 timestamp: 1, metadata: [] }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        migrate_multi_partition_key_order(&storage.db)
            .await
            .unwrap();

        let parts = storage
            .get_materialized_partitions(cl, "inv")
            .await
            .unwrap();
        assert_eq!(parts, vec![sorted.clone()], "one canonical row survives");
        let ts = storage.get_partition_timestamps(cl, "inv").await.unwrap();
        assert_eq!(ts, vec![(sorted.clone(), 2)], "newest row's values win");
        // The event is reachable through an equality lookup with the
        // canonical (sorted) bind.
        let mut result = storage
            .db
            .query("SELECT count() AS total FROM events WHERE partition_key = $pk GROUP ALL")
            .bind(("pk", sorted))
            .await
            .unwrap();
        let total: Option<u64> = result.take((0, "total")).unwrap();
        assert_eq!(
            total,
            Some(1),
            "event key must be rewritten to canonical order"
        );
    }

    /// Released 0.1.x wheels predate canonical-on-write Multi dims; rows they
    /// write into a v2-stamped database were never healed once the every-boot
    /// scan became version-gated. v3 re-runs the heal once.
    #[tokio::test]
    async fn test_v3_reheals_rows_written_into_v2_database() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        set_schema_version(&storage.db, 2).await.unwrap();
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        run_migrations(&storage.db).await.unwrap();

        assert_eq!(stored_schema_version(&storage.db).await.unwrap(), 3);
        #[derive(Debug, SurrealValue)]
        struct PkRow {
            partition_key: PartitionKey,
        }
        let mut result = storage
            .db
            .query("SELECT partition_key FROM asset_partitions")
            .await
            .unwrap();
        let rows: Vec<PkRow> = result.take(0).unwrap();
        assert_eq!(rows.len(), 1);
        let PartitionKey::Multi { dims } = &rows[0].partition_key else {
            unreachable!()
        };
        assert_eq!(dims[0].0, "date", "v3 must re-heal post-v2 legacy writes");
    }

    /// Pre-guard databases can hold dynamic keys with display-reserved
    /// characters. Renaming them silently would change user-visible keys, so
    /// v3 records them for operator remediation instead.
    #[tokio::test]
    async fn test_v3_records_reserved_char_dynamic_keys() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        set_schema_version(&storage.db, 2).await.unwrap();
        storage
            .db
            .query(
                "CREATE dynamic_partitions CONTENT { code_location_id: 'default', \
                 partitions_def_name: 'colors', partition_key: 'us|eu', \
                 create_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        run_migrations(&storage.db).await.unwrap();

        let mut res = storage
            .db
            .query("SELECT * FROM kv WHERE key = $key")
            .bind(("key", RESERVED_DYNAMIC_KEYS_KEY.to_string()))
            .await
            .unwrap();
        let rows: Vec<DbKv> = res.take(0).unwrap();
        assert_eq!(rows.len(), 1, "offending keys must be recorded in kv");
        let body = String::from_utf8(rows[0].value.to_vec()).unwrap();
        assert!(body.contains("colors") && body.contains("us|eu"), "{body}");
    }

    /// The heal's values come from a snapshot taken before its write loop, so
    /// a live materialization can land newer pointers on the canonical row in
    /// between — the upsert must not roll them back to snapshot values.
    #[tokio::test]
    async fn test_migration_upsert_never_rolls_back_newer_pointers() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let key = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let row = |event: &str, ts: i64| DbAssetPartitionWrite {
            code_location_id: "default".to_string(),
            asset_key: "a".to_string(),
            partition_key: key.clone(),
            last_event_id: event.to_string(),
            last_run_id: format!("run-{event}"),
            last_timestamp: ts,
        };
        // The live writer's row lands first with newer pointers...
        upsert_canonical_partition_row(&storage.db, row("ev-new", 200))
            .await
            .unwrap();
        // ...then the migration applies its stale snapshot values.
        upsert_canonical_partition_row(&storage.db, row("ev-old", 100))
            .await
            .unwrap();
        #[derive(Debug, SurrealValue)]
        struct Ptr {
            last_event_id: String,
            last_timestamp: i64,
        }
        let mut res = storage
            .db
            .query(
                "SELECT last_event_id, last_timestamp FROM asset_partitions WHERE asset_key = 'a'",
            )
            .await
            .unwrap();
        let rows: Vec<Ptr> = res.take(0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_timestamp, 200, "newer pointers must survive");
        assert_eq!(rows[0].last_event_id, "ev-new");
    }

    /// Two processes opening the same fresh/v1 remote database race the
    /// stamp; a DELETE-then-CREATE pair aborts the loser on the kv unique
    /// index, so the stamp must be a single upsert that both survive.
    #[tokio::test]
    async fn test_concurrent_schema_stamps_both_succeed() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let db_a = storage.db.clone();
        let db_b = storage.db.clone();
        let a = tokio::spawn(async move {
            for _ in 0..25 {
                set_schema_version(&db_a, 2).await?;
            }
            anyhow::Ok(())
        });
        let b = tokio::spawn(async move {
            for _ in 0..25 {
                set_schema_version(&db_b, 3).await?;
            }
            anyhow::Ok(())
        });
        a.await.unwrap().expect("stamp task A failed");
        b.await.unwrap().expect("stamp task B failed");
        let v = stored_schema_version(&storage.db).await.unwrap();
        assert!(v == 2 || v == 3, "one of the stamps must win, got {v}");
    }

    /// A fresh database is stamped with the current schema version at
    /// construction, so it never pays the legacy table scans again.
    #[tokio::test]
    async fn test_fresh_database_stamped_with_current_schema_version() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        assert_eq!(
            stored_schema_version(&storage.db).await.unwrap(),
            SCHEMA_VERSION
        );
    }

    /// A database already at the current version must not re-run migrations:
    /// a planted unsorted row stays untouched because the v1→v2 scan is
    /// skipped entirely.
    #[tokio::test]
    async fn test_migrations_skip_when_version_is_current() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        // Simulate the next startup's migration pass.
        run_migrations(&storage.db).await.unwrap();

        #[derive(Debug, SurrealValue)]
        struct PkRow {
            partition_key: PartitionKey,
        }
        let mut result = storage
            .db
            .query("SELECT partition_key FROM asset_partitions")
            .await
            .unwrap();
        let rows: Vec<PkRow> = result.take(0).unwrap();
        assert_eq!(rows.len(), 1);
        let PartitionKey::Multi { dims } = &rows[0].partition_key else {
            unreachable!()
        };
        assert_eq!(
            dims[0].0, "region",
            "a database at the current version must not be rescanned"
        );
    }

    /// A pre-versioning database (no schema_version key) is treated as v1:
    /// the Multi-key migration runs once, then the version is stamped.
    #[tokio::test]
    async fn test_legacy_database_migrates_and_stamps_version() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        // Erase the stamp to simulate a database from before versioning.
        storage
            .db
            .query("DELETE FROM kv WHERE key = $key")
            .bind(("key", SCHEMA_VERSION_KEY.to_string()))
            .await
            .unwrap()
            .check()
            .unwrap();
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        run_migrations(&storage.db).await.unwrap();

        assert_eq!(
            stored_schema_version(&storage.db).await.unwrap(),
            SCHEMA_VERSION
        );
        let sorted = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let parts = storage
            .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "inv")
            .await
            .unwrap();
        assert_eq!(parts, vec![sorted], "legacy row canonicalized exactly once");
    }

    /// A crash mid-migration must never lose a partition: the canonical row
    /// is written before any legacy row is deleted, so a pre-existing sorted
    /// row is updated in place — its record id survives — and the stale
    /// duplicate is only removed afterwards.
    #[tokio::test]
    async fn test_migration_never_deletes_the_canonical_row() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let sorted = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        storage
            .upsert_asset_partitions(vec![DbAssetPartitionWrite {
                code_location_id: cl.to_string(),
                asset_key: "inv".to_string(),
                partition_key: sorted.clone(),
                last_event_id: "e1".to_string(),
                last_run_id: "r1".to_string(),
                last_timestamp: 1,
            }])
            .await
            .unwrap();
        #[derive(Debug, SurrealValue)]
        struct IdRow {
            id: RecordId,
            last_event_id: String,
            last_timestamp: i64,
        }
        let mut result = storage
            .db
            .query("SELECT id, last_event_id, last_timestamp FROM asset_partitions")
            .await
            .unwrap();
        let before: Vec<IdRow> = result.take(0).unwrap();
        assert_eq!(before.len(), 1);
        let canonical_id = before[0].id.clone();
        // Legacy duplicate with unsorted dims and newer values (raw CREATE
        // bypasses the canonicalizing serializer).
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e9', last_run_id: 'r9', last_timestamp: 5 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        migrate_multi_partition_key_order(&storage.db)
            .await
            .unwrap();

        let mut result = storage
            .db
            .query("SELECT id, last_event_id, last_timestamp FROM asset_partitions")
            .await
            .unwrap();
        let after: Vec<IdRow> = result.take(0).unwrap();
        assert_eq!(after.len(), 1, "one canonical row survives");
        assert_eq!(
            after[0].id, canonical_id,
            "the canonical row must be updated in place, never deleted"
        );
        assert_eq!(after[0].last_event_id, "e9", "newest row's values win");
        assert_eq!(after[0].last_timestamp, 5);
    }

    /// `store_events`/`store_event` upsert `asset_partitions` via `INSERT ... ON
    /// DUPLICATE KEY UPDATE`, which must fire on the table's UNIQUE
    /// (code_location_id, asset_key, partition_key) index — not the record id —
    /// to replace rather than duplicate. Guard that on the RocksDB backend the
    /// demo uses (kv-mem enforces indexes differently).
    #[tokio::test]
    async fn test_upsert_asset_partitions_replaces_on_unique_index() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let pk = PartitionKey::Single {
            keys: vec!["p1".to_string()],
        };
        let row = |event_id: &str, ts: i64| super::DbAssetPartitionWrite {
            code_location_id: cl.to_string(),
            asset_key: "inventory".to_string(),
            partition_key: pk.clone(),
            last_event_id: event_id.to_string(),
            last_run_id: "r".to_string(),
            last_timestamp: ts,
        };

        storage
            .upsert_asset_partitions(vec![row("ev1", 1)])
            .await
            .unwrap();
        storage
            .upsert_asset_partitions(vec![row("ev2", 2)])
            .await
            .unwrap();

        // Upsert on the unique index updates in place: one row, latest values.
        let parts = storage
            .get_materialized_partitions(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            parts,
            vec![pk.clone()],
            "must not duplicate the partition row"
        );
        let ts = storage
            .get_partition_timestamps(cl, "inventory")
            .await
            .unwrap();
        assert_eq!(
            ts,
            vec![(pk.clone(), 2)],
            "must update the existing row in place"
        );
    }

    /// Per-partition lookups receive the display string; for Multi-partitioned
    /// assets the persisted key is a Multi object — the lookup must still match.
    #[tokio::test]
    async fn test_partition_string_lookup_matches_multi_keys() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let pk = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let mut event = make_event("inv", "r1", 100);
        event.partition_key = Some(pk.clone());
        storage.store_event(&event).await.unwrap();

        let display = pk.to_display();
        assert_eq!(display, "date=2024-01-01|region=eu");
        let latest = storage
            .get_latest_materialization(cl, "inv", Some(&display))
            .await
            .unwrap();
        assert!(
            latest.is_some(),
            "display-form lookup must match the Multi event"
        );
        let events = storage
            .get_partition_events(cl, "inv", &display, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        // Single-dim lookups keep working.
        let mut single = make_event("inv", "r2", 200);
        single.partition_key = Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        });
        storage.store_event(&single).await.unwrap();
        assert!(
            storage
                .get_latest_materialization(cl, "inv", Some("p1"))
                .await
                .unwrap()
                .is_some()
        );
    }

    /// After a def-shape change, legacy Single events whose key strings look
    /// like Multi displays can coexist with real Multi events for the same
    /// asset. Display lookups must prefer the structured (Multi) reading —
    /// not whichever row happens to be newest.
    #[tokio::test]
    async fn test_partition_string_lookup_prefers_structured_multi() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let multi = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let display = multi.to_display();

        // Legacy event from the asset's static-keyed era — NEWER timestamp.
        let mut old_single = make_event("inv", "r1", 200);
        old_single.partition_key = Some(PartitionKey::Single {
            keys: vec![display.clone()],
        });
        storage.store_event(&old_single).await.unwrap();

        let mut multi_event = make_event("inv", "r2", 100);
        multi_event.partition_key = Some(multi.clone());
        storage.store_event(&multi_event).await.unwrap();

        let latest = storage
            .get_latest_materialization(cl, "inv", Some(&display))
            .await
            .unwrap()
            .expect("lookup must match");
        assert_eq!(
            latest.run_id, "r2",
            "the structured Multi reading wins over a newer legacy Single row"
        );
        let events = storage
            .get_partition_events(cl, "inv", &display, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "only the Multi partition's events return");
        assert_eq!(events[0].run_id, "r2");

        // The Single reading still works when no Multi rows exist.
        let mut plain = make_event("inv2", "r3", 100);
        plain.partition_key = Some(PartitionKey::Single {
            keys: vec![display.clone()],
        });
        storage.store_event(&plain).await.unwrap();
        assert!(
            storage
                .get_latest_materialization(cl, "inv2", Some(&display))
                .await
                .unwrap()
                .is_some(),
            "falls back to the Single reading"
        );
    }

    fn make_event(asset_key: &str, run_id: &str, ts: i64) -> EventRecord {
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some(asset_key.to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        }
    }

    fn make_asset_record(key: &str) -> AssetRecord {
        AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    /// Register assets before store_event (which now updates rather than creates).
    async fn register(storage: &SurrealStorage, keys: &[&str]) {
        let records: Vec<AssetRecord> = keys.iter().map(|k| make_asset_record(k)).collect();
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_and_retrieve_event() {
        let storage = make_storage().await;
        register(&storage, &["my_asset"]).await;
        let event = make_event("my_asset", "run_1", 1000);
        let event_id = storage.store_event(&event).await.unwrap();
        assert!(!event_id.is_empty());

        let events = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let actual = &events[0];
        let expected = StoredEvent {
            id: actual.id.clone(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("my_asset".to_string()),
            run_id: "run_1".to_string(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        assert_eq!(*actual, expected);
    }

    #[tokio::test]
    async fn test_events_ordered_by_timestamp_desc() {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;
        storage
            .store_event(&make_event("a", "r1", 100))
            .await
            .unwrap();
        storage
            .store_event(&make_event("a", "r2", 300))
            .await
            .unwrap();
        storage
            .store_event(&make_event("a", "r3", 200))
            .await
            .unwrap();

        let events = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].timestamp, 300);
        assert_eq!(events[1].timestamp, 200);
        assert_eq!(events[2].timestamp, 100);
    }

    #[tokio::test]
    async fn test_events_for_run() {
        let storage = make_storage().await;
        register(&storage, &["a", "b", "c"]).await;
        storage
            .store_event(&make_event("a", "run_x", 100))
            .await
            .unwrap();
        storage
            .store_event(&make_event("b", "run_x", 200))
            .await
            .unwrap();
        storage
            .store_event(&make_event("c", "run_y", 300))
            .await
            .unwrap();

        let events = storage.get_events_for_run("run_x").await.unwrap();
        assert_eq!(events.len(), 2);
        // Ordered ASC by timestamp
        let expected_0 = StoredEvent {
            id: events[0].id.clone(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("a".to_string()),
            run_id: "run_x".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        let expected_1 = StoredEvent {
            id: events[1].id.clone(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("b".to_string()),
            run_id: "run_x".to_string(),
            partition_key: None,
            timestamp: 200,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        assert_eq!(events[0], expected_0);
        assert_eq!(events[1], expected_1);
    }

    #[tokio::test]
    async fn test_asset_record_upsert_on_materialization() {
        let storage = make_storage().await;
        register(&storage, &["my_asset"]).await;
        let event_id_1 = storage
            .store_event(&make_event("my_asset", "r1", 100))
            .await
            .unwrap();

        let record = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
            .await
            .unwrap()
            .unwrap();
        let expected = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: Some(event_id_1),
            last_run_id: Some("r1".to_string()),
            last_timestamp: Some(100),
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        assert_eq!(record, expected);

        // Store another materialization — should update
        let event_id_2 = storage
            .store_event(&make_event("my_asset", "r2", 200))
            .await
            .unwrap();
        let record = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
            .await
            .unwrap()
            .unwrap();
        let expected = AssetRecord {
            last_event_id: Some(event_id_2),
            last_run_id: Some("r2".to_string()),
            last_timestamp: Some(200),
            ..expected
        };
        assert_eq!(record, expected);
    }

    #[tokio::test]
    async fn test_get_asset_records() {
        let storage = make_storage().await;
        register(&storage, &["a", "b"]).await;
        let eid_a = storage
            .store_event(&make_event("a", "r1", 100))
            .await
            .unwrap();
        let eid_b = storage
            .store_event(&make_event("b", "r1", 200))
            .await
            .unwrap();

        let mut records = storage
            .get_asset_records(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        records.sort_by(|a, b| a.asset_key.cmp(&b.asset_key));
        assert_eq!(records.len(), 2);

        let expected_a = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "a".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: Some(eid_a),
            last_run_id: Some("r1".to_string()),
            last_timestamp: Some(100),
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        let expected_b = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "b".to_string(),
            last_event_id: Some(eid_b),
            last_run_id: Some("r1".to_string()),
            last_timestamp: Some(200),
            ..expected_a.clone()
        };
        assert_eq!(records[0], expected_a);
        assert_eq!(records[1], expected_b);
    }

    #[tokio::test]
    async fn test_latest_materialization() {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;
        storage
            .store_event(&make_event("a", "r1", 100))
            .await
            .unwrap();
        storage
            .store_event(&make_event("a", "r2", 200))
            .await
            .unwrap();

        let latest = storage
            .get_latest_materialization(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", None)
            .await
            .unwrap()
            .unwrap();
        let expected = StoredEvent {
            id: latest.id.clone(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("a".to_string()),
            run_id: "r2".to_string(),
            partition_key: None,
            timestamp: 200,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        assert_eq!(latest, expected);
    }

    #[tokio::test]
    async fn test_count_materialized_partitions() {
        let storage = make_storage().await;
        register(&storage, &["a", "b"]).await;

        // Empty → 0. The `count() ... GROUP ALL` aggregate returns no row when
        // nothing matches, so the impl must map None → 0 rather than error.
        let n = storage
            .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
            .await
            .unwrap();
        assert_eq!(n, 0);

        let materialize = |asset: &str, run: &str, key: &str, ts: i64| EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some(asset.to_string()),
            run_id: run.to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec![key.to_string()],
            }),
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        };

        // Three distinct partitions of "a" + a re-materialization of one — the
        // UNIQUE index upserts, so the count must not double-count p1.
        for ev in [
            materialize("a", "r1", "p1", 100),
            materialize("a", "r2", "p2", 200),
            materialize("a", "r3", "p3", 300),
            materialize("a", "r4", "p1", 400),
            // A different asset's partition must not leak into "a"'s count.
            materialize("b", "r5", "p9", 500),
        ] {
            storage.store_event(&ev).await.unwrap();
        }

        let n = storage
            .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
            .await
            .unwrap();
        assert_eq!(
            n, 3,
            "3 distinct partitions despite the p1 re-materialization"
        );

        // The aggregate agrees with the full row enumeration.
        let rows = storage
            .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);

        // Scoped per asset.
        let nb = storage
            .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "b")
            .await
            .unwrap();
        assert_eq!(nb, 1);
    }

    #[tokio::test]
    async fn test_count_dynamic_partitions() {
        let storage = make_storage().await;
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;

        // Empty namespace → 0 (GROUP ALL returns no row → None → 0).
        assert_eq!(
            storage
                .count_dynamic_partitions(cl, "customers")
                .await
                .unwrap(),
            0
        );

        // add_dynamic_partitions dedupes, so a repeated key must not double-count.
        storage
            .add_dynamic_partitions(
                cl,
                "customers",
                &["acme".into(), "globex".into(), "initech".into()],
            )
            .await
            .unwrap();
        storage
            .add_dynamic_partitions(cl, "customers", &["acme".into()])
            .await
            .unwrap();
        assert_eq!(
            storage
                .count_dynamic_partitions(cl, "customers")
                .await
                .unwrap(),
            3,
            "3 distinct customers despite the duplicate add"
        );

        // Scoped per namespace.
        storage
            .add_dynamic_partitions(cl, "regions", &["us".into(), "eu".into()])
            .await
            .unwrap();
        assert_eq!(
            storage
                .count_dynamic_partitions(cl, "regions")
                .await
                .unwrap(),
            2
        );

        // Agrees with the full key enumeration.
        assert_eq!(
            storage
                .get_dynamic_partitions(cl, "customers")
                .await
                .unwrap()
                .len(),
            3
        );
    }

    /// Dynamic keys feed the canonical display form (`dim=v|dim=v`, values
    /// joined with ','); the storage write path is the choke point for every
    /// transport, so reserved separator characters and empty keys must be
    /// rejected here.
    #[tokio::test]
    async fn test_add_dynamic_partitions_rejects_reserved_and_empty_keys() {
        let storage = make_storage().await;
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        for bad in ["us|eu", "a,b", ""] {
            let result = storage
                .add_dynamic_partitions(cl, "users", &[bad.to_string()])
                .await;
            assert!(result.is_err(), "key {bad:?} must be rejected");
        }
        assert!(
            storage
                .get_dynamic_partitions(cl, "users")
                .await
                .unwrap()
                .is_empty(),
            "rejected keys must not be persisted"
        );
    }

    #[tokio::test]
    async fn test_latest_materialization_with_partition() {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;

        let event1 = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("a".to_string()),
            run_id: "r1".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["2024-01".to_string()],
            }),
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        };
        let event2 = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("a".to_string()),
            run_id: "r2".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["2024-02".to_string()],
            }),
            timestamp: 200,
            metadata: vec![],
            input_data_versions: vec![],
        };
        storage.store_event(&event1).await.unwrap();
        storage.store_event(&event2).await.unwrap();

        let latest = storage
            .get_latest_materialization(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "a",
                Some("2024-01"),
            )
            .await
            .unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().timestamp, 100);

        let latest = storage
            .get_latest_materialization(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "a",
                Some("2024-02"),
            )
            .await
            .unwrap();
        assert_eq!(latest.unwrap().timestamp, 200);
    }

    #[tokio::test]
    async fn test_observation_does_not_upsert_asset() {
        let storage = make_storage().await;

        let event = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation { data_version: None },
            asset_key: Some("obs_asset".to_string()),
            run_id: "r1".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        };
        storage.store_event(&event).await.unwrap();

        // Observation should not create an asset record
        let record = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "obs_asset")
            .await
            .unwrap();
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_observation_updates_registered_asset() {
        let storage = make_storage().await;

        // Register the asset first
        let record = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "ext_feed".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
            .await
            .unwrap();

        // Store an observation event
        let event = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("obs_v1".to_string()),
            },
            asset_key: Some("ext_feed".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        };
        storage.store_event(&event).await.unwrap();

        // Verify observation updated last_timestamp and last_data_version
        let updated = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.last_timestamp, Some(5000));
        assert_eq!(updated.last_data_version.as_deref(), Some("obs_v1"));
        assert!(updated.last_event_id.is_some());
        // Observation should NOT set materialization-specific fields
        assert!(updated.last_run_id.is_none());
        assert!(updated.last_materialization_code_version.is_none());
        assert!(updated.last_input_data_versions.is_empty());
    }

    #[tokio::test]
    async fn test_observation_does_not_overwrite_materialization_fields() {
        let storage = make_storage().await;

        let record = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "ext_feed".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: Some("v1".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
            .await
            .unwrap();

        // First: materialize (sets run_id, materialization_code_version)
        let run = RunRecord {
            run_id: "run_1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(2000),
            tags: vec![],
            node_names: vec!["ext_feed".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();
        let mat_event = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("mat_v1".to_string()),
            },
            asset_key: Some("ext_feed".to_string()),
            run_id: "run_1".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        };
        storage.store_event(&mat_event).await.unwrap();

        let after_mat = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_mat.last_run_id.as_deref(), Some("run_1"));
        assert_eq!(
            after_mat.last_materialization_code_version.as_deref(),
            Some("v1")
        );

        // Then: observe (should update timestamp/data_version but NOT run_id/mcv)
        let obs_event = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("obs_v2".to_string()),
            },
            asset_key: Some("ext_feed".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 6000,
            metadata: vec![],
            input_data_versions: vec![],
        };
        storage.store_event(&obs_event).await.unwrap();

        let after_obs = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_obs.last_timestamp, Some(6000));
        assert_eq!(after_obs.last_data_version.as_deref(), Some("obs_v2"));
        // Materialization fields preserved
        assert_eq!(after_obs.last_run_id.as_deref(), Some("run_1"));
        assert_eq!(
            after_obs.last_materialization_code_version.as_deref(),
            Some("v1")
        );
    }

    #[tokio::test]
    async fn test_run_lifecycle() {
        let storage = make_storage().await;
        let run = RunRecord {
            run_id: "run_1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("my_job".to_string()),
            status: RunStatus::NotStarted,
            start_time: 1000,
            end_time: None,
            tags: vec![("env".to_string(), "prod".to_string())],
            node_names: vec!["a".to_string(), "b".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        let fetched = storage.get_run("run_1").await.unwrap().unwrap();
        assert_eq!(fetched, run);

        // Update to started
        storage
            .update_run_status("run_1", RunStatus::Started, None)
            .await
            .unwrap();
        let fetched = storage.get_run("run_1").await.unwrap().unwrap();
        assert_eq!(
            fetched,
            RunRecord {
                status: RunStatus::Started,
                ..run.clone()
            }
        );

        // Update to success with end_time
        storage
            .update_run_status("run_1", RunStatus::Success, Some(2000))
            .await
            .unwrap();
        let fetched = storage.get_run("run_1").await.unwrap().unwrap();
        assert_eq!(
            fetched,
            RunRecord {
                status: RunStatus::Success,
                end_time: Some(2000),
                ..run
            }
        );
    }

    #[tokio::test]
    async fn test_create_run_swallows_duplicate_id_after_retry() {
        // Documents the phantom-commit handling for client-supplied-ID CREATEs.
        //
        // Surrealdb v3 surfaces a unique-index violation as
        // `ErrorDetails::Internal` with a message like "Database index ...
        // already contains ...", NOT as `AlreadyExists`. Our retry classifier
        // checks `Internal` against the `"onflict"`/`"Busy"`/`"Try again"`
        // substrings — `"already contains"` does not match — so the violation
        // falls through as permanent and the retry loop exits.
        //
        // `create_run` then post-processes that permanent error via
        // `swallow_phantom_commit`: it converts the unique-index violation into
        // `Ok(())` with a warning, on the assumption that the only realistic
        // way to land here is a phantom commit on a previous retry attempt
        // (UUID collision between processes being statistically negligible).
        //
        // This test verifies both halves of that contract:
        //   1. The raw SurrealDB error has the expected shape.
        //   2. `create_run` swallows it and returns Ok.
        use super::super::retry;

        let storage = make_storage().await;
        let run = RunRecord {
            run_id: "duplicate_run".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::NotStarted,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };

        // First CREATE succeeds.
        storage.create_run(&run).await.unwrap();

        // Second CREATE with the same run_id is swallowed (treated as success).
        storage
            .create_run(&run)
            .await
            .expect("duplicate create_run should be swallowed as phantom-commit success");

        // The row is still the original (no overwrite, no extra row).
        let fetched = storage.get_run("duplicate_run").await.unwrap().unwrap();
        assert_eq!(fetched, run);

        // Sanity: the raw SurrealDB error this swallow logic catches has the
        // expected shape — Internal kind + "already contains" message — and
        // the classifier rejects it as permanent (which is why it surfaces
        // here in the first place rather than looping inside `with_retry`).
        let raw_err = storage
            .db
            .create::<Option<RunRecord>>("runs")
            .content(run.clone())
            .await
            .expect_err("direct CREATE bypassing swallow must still error");
        assert!(raw_err.is_internal());
        assert!(raw_err.message().contains("already contains"));
        let anyhow_err = anyhow::Error::from(raw_err);
        assert!(retry::is_unique_index_violation(&anyhow_err));
        assert!(!retry::default_should_retry(&anyhow_err));
    }

    #[tokio::test]
    async fn test_get_runs_with_limit() {
        let storage = make_storage().await;
        let mut all_runs = Vec::new();
        for i in 0..5i64 {
            let run = RunRecord {
                run_id: format!("run_{}", i),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job".to_string()),
                status: RunStatus::Success,
                start_time: i * 100,
                end_time: Some(i * 100 + 50),
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage.create_run(&run).await.unwrap();
            all_runs.push(run);
        }

        // get_runs returns DESC by start_time, limit 3 → newest 3
        let runs = storage.get_all_runs(3, None).await.unwrap();
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0], all_runs[4]); // run_4, start_time=400
        assert_eq!(runs[1], all_runs[3]); // run_3, start_time=300
        assert_eq!(runs[2], all_runs[2]); // run_2, start_time=200
    }

    #[tokio::test]
    async fn test_get_runs_filtered_by_status() {
        let storage = make_storage().await;

        let run_ok = RunRecord {
            run_id: "ok".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Success,
            start_time: 100,
            end_time: Some(200),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        let run_fail = RunRecord {
            run_id: "fail".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Failure,
            start_time: 300,
            end_time: Some(400),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run_ok).await.unwrap();
        storage.create_run(&run_fail).await.unwrap();

        let failures = storage
            .get_all_runs(10, Some(RunStatus::Failure))
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0], run_fail);
    }

    #[tokio::test]
    async fn test_get_all_runs_page_pagination_and_total() {
        let storage = make_storage().await;
        for i in 0..7i64 {
            let run = RunRecord {
                run_id: format!("page_{}", i),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Success,
                start_time: i * 100,
                end_time: Some(i * 100 + 10),
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage.create_run(&run).await.unwrap();
        }

        let page = storage
            .get_all_runs_page(0, 3, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page.total, 7);
        assert_eq!(page.rows.len(), 3);
        // DESC by start_time → newest first.
        assert_eq!(page.rows[0].run_id, "page_6");
        assert_eq!(page.rows[1].run_id, "page_5");
        assert_eq!(page.rows[2].run_id, "page_4");

        let page = storage
            .get_all_runs_page(3, 3, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page.total, 7);
        assert_eq!(page.rows.len(), 3);
        assert_eq!(page.rows[0].run_id, "page_3");
        assert_eq!(page.rows[2].run_id, "page_1");

        let page = storage
            .get_all_runs_page(6, 3, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page.total, 7);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].run_id, "page_0");
    }

    #[tokio::test]
    async fn test_get_all_runs_page_filters() {
        let storage = make_storage().await;
        for (i, (job, status, assets, tags)) in [
            (
                "daily_ingest",
                RunStatus::Success,
                vec!["orders"],
                vec![("partition", "2024-01-01")],
            ),
            (
                "daily_export",
                RunStatus::Failure,
                vec!["orders", "revenue"],
                vec![("partition_key", "2024-01-02")],
            ),
            (
                "weekly_report",
                RunStatus::Success,
                vec!["users"],
                vec![("other", "x")],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let run = RunRecord {
                run_id: format!("r{}", i),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some(job.to_string()),
                status,
                start_time: i as i64 * 100,
                end_time: None,
                tags: tags
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                node_names: assets.iter().map(|s| s.to_string()).collect(),
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage.create_run(&run).await.unwrap();
        }

        // Status filter
        let filter = RunFilter {
            status: Some(RunStatus::Success),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 2);
        assert!(page.rows.iter().all(|r| r.status == RunStatus::Success));

        // Job substring (case-insensitive)
        let filter = RunFilter {
            job_substring: Some("DAILY".into()),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 2);
        assert!(
            page.rows
                .iter()
                .all(|r| r.job_name.as_deref().is_some_and(|n| n.contains("daily")))
        );

        // Asset substring
        let filter = RunFilter {
            asset_substring: Some("ORDER".into()),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 2);

        // Partition tag substring — matches both "partition" and "partition_key" keys
        let filter = RunFilter {
            partition_substring: Some("2024-01".into()),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 2);

        // Combined: status + job substring
        let filter = RunFilter {
            status: Some(RunStatus::Success),
            job_substring: Some("daily".into()),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.rows[0].job_name.as_deref(), Some("daily_ingest"));

        // Exact job_name filter disambiguates `daily_ingest` from
        // `daily_export` (substring `daily` would match both).
        let filter = RunFilter {
            job_name: Some("daily_ingest".into()),
            ..Default::default()
        };
        let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.rows[0].job_name.as_deref(), Some("daily_ingest"));
    }

    #[tokio::test]
    async fn test_get_all_last_run_per_job() {
        let storage = make_storage().await;
        let specs = [
            ("job_a", "r1", 100i64),
            ("job_a", "r2", 300),
            ("job_a", "r3", 200),
            ("job_b", "r4", 500),
            ("job_c_no_runs_expected", "", -1),
        ];
        for (job, rid, start) in &specs[..4] {
            storage
                .create_run(&RunRecord {
                    run_id: rid.to_string(),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some(job.to_string()),
                    status: RunStatus::Success,
                    start_time: *start,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }

        let out = storage
            .get_all_last_run_per_job(&[
                "job_a".into(),
                "job_b".into(),
                "job_c_no_runs_expected".into(),
            ])
            .await
            .unwrap();
        // job_c has no runs → excluded.
        assert_eq!(out.len(), 2);
        let a = out.iter().find(|(n, _)| n == "job_a").unwrap();
        assert_eq!(a.1.run_id, "r2"); // highest start_time
        let b = out.iter().find(|(n, _)| n == "job_b").unwrap();
        assert_eq!(b.1.run_id, "r4");
    }

    /// Empty input must round-trip without a query (no statements to send)
    /// and no DB call overhead.
    #[tokio::test]
    async fn test_get_all_last_run_per_job_empty_input() {
        let storage = make_storage().await;
        let out = storage.get_all_last_run_per_job(&[]).await.unwrap();
        assert!(out.is_empty());
    }

    /// Regression guard for the multi-statement batching: many distinct
    /// job names must all resolve in a single `.query()` call. If the
    /// statement/bind indexing ever gets off-by-one, this test returns
    /// the wrong `run_id` per job.
    #[tokio::test]
    async fn test_get_all_last_run_per_job_batches_many_jobs_correctly() {
        let storage = make_storage().await;
        // 50 jobs × 3 runs each = 150 rows. Pick an arbitrary non-trivial
        // count so the batch covers at least ~5× the default pagination.
        const N_JOBS: usize = 50;
        for i in 0..N_JOBS {
            for k in 0..3 {
                let start = (i * 1_000 + k * 10) as i64;
                storage
                    .create_run(&RunRecord {
                        run_id: format!("j{i}_r{k}"),
                        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                        job_name: Some(format!("batched_job_{i}")),
                        status: RunStatus::Success,
                        start_time: start,
                        end_time: None,
                        tags: vec![],
                        node_names: vec![],
                        priority: 0,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual,
                    })
                    .await
                    .unwrap();
            }
        }

        // Request in a non-sorted order to prove the query indexing doesn't
        // rely on input ordering.
        let mut names: Vec<String> = (0..N_JOBS).map(|i| format!("batched_job_{i}")).collect();
        names.rotate_left(17);
        let out = storage.get_all_last_run_per_job(&names).await.unwrap();

        assert_eq!(out.len(), N_JOBS, "every job should resolve");
        for (job_name, run) in &out {
            // `run_id` must be the latest of the three runs we inserted
            // for that job (k=2 has the largest start_time).
            let expected_suffix = "_r2";
            assert!(
                run.run_id.ends_with(expected_suffix),
                "job {job_name} resolved to wrong run_id {}",
                run.run_id
            );
            assert_eq!(
                run.job_name.as_deref(),
                Some(job_name.as_str()),
                "bind key / statement index mismatch: request {job_name} got {:?}",
                run.job_name
            );
        }
    }

    #[tokio::test]
    async fn test_get_all_runs_summary_counts() {
        let storage = make_storage().await;
        let now_ns = 10_000_000_000i64;
        let cutoff = now_ns - 3_000_000_000;
        // 2 Success (one inside 24h cutoff, one outside)
        // 1 Failure inside
        // 1 Started inside
        // 1 Queued inside, 1 NotStarted inside
        let spec = [
            (RunStatus::Success, now_ns - 1_000_000_000),
            (RunStatus::Success, now_ns - 10_000_000_000),
            (RunStatus::Failure, now_ns - 500_000_000),
            (RunStatus::Started, now_ns - 200_000_000),
            (RunStatus::Queued, now_ns - 100_000_000),
            (RunStatus::NotStarted, now_ns - 50_000_000),
        ];
        for (i, (status, start)) in spec.into_iter().enumerate() {
            let run = RunRecord {
                run_id: format!("s{i}"),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status,
                start_time: start,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage.create_run(&run).await.unwrap();
        }

        let summary = storage.get_all_runs_summary(cutoff).await.unwrap();
        assert_eq!(summary.total, 6);
        assert_eq!(summary.success, 2);
        assert_eq!(summary.failure, 1);
        assert_eq!(summary.in_progress, 1);
        assert_eq!(summary.queued, 2);
        // 5 of 6 runs are inside the 24h window (Success outside is excluded).
        assert_eq!(summary.last_24h, 5);
    }

    #[tokio::test]
    async fn test_subscribe_table_yields_on_change() {
        use futures_util::StreamExt;
        use std::time::Duration;
        let storage = std::sync::Arc::new(make_storage().await);

        let mut stream = storage.subscribe_table("runs").await.unwrap();

        // Create a run from another task; then poll the stream with a timeout.
        let storage_w = storage.clone();
        tokio::spawn(async move {
            // Small delay so the live query is registered before we write.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let run = RunRecord {
                run_id: "live_r".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: 1,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage_w.create_run(&run).await.unwrap();
        });

        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for notification");
        assert!(first.is_some(), "expected a notification, got None");
    }

    /// Each table fed by an `rivers-ui` LIVE channel must wake its
    /// `subscribe_table` stream on a write. Covers every table in
    /// `rivers_ui::live::LIVE_CHANNELS` (kept in lockstep here by table
    /// name — adding a new channel-fed table must add a scenario, else
    /// that table would silently stop emitting to the UI).
    ///
    /// Uses high-level APIs where they exist; falls back to raw inserts via
    /// `self.db` for tables normally written by internal transactions
    /// (`asset_partitions` via the store_event partition-upsert path,
    /// `concurrency_slots` / `pending_steps` via the claim transaction).
    /// For the LIVE-query primitive, bypassing the transaction is equivalent —
    /// all that matters is that the table saw a `CREATE` and the stream woke.
    #[tokio::test]
    async fn test_subscribe_table_wakes_for_every_live_channel_table() {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::time::timeout;

        // Helper: subscribe → brief delay to let the LIVE query register →
        // run the write → assert the stream yields within the deadline.
        async fn expect_yields(
            storage: &Arc<SurrealStorage>,
            table: &'static str,
            write: impl std::future::Future<Output = ()>,
        ) {
            let mut stream = storage
                .subscribe_table(table)
                .await
                .unwrap_or_else(|e| panic!("subscribe_table({table}) failed: {e}"));
            tokio::time::sleep(Duration::from_millis(100)).await;
            write.await;
            let first = timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for notification on `{table}`"));
            assert!(
                first.is_some(),
                "live-query stream for `{table}` ended before any notification"
            );
        }

        let storage = Arc::new(make_storage().await);

        // `runs` table.
        expect_yields(&storage, "runs", async {
            let run = RunRecord {
                run_id: "live_runs".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: 1,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            };
            storage.create_run(&run).await.unwrap();
        })
        .await;

        // `assets` table.
        expect_yields(&storage, "assets", async {
            storage
                .register_assets(
                    crate::storage::DEFAULT_CODE_LOCATION_ID,
                    &[make_asset_record("live_asset")],
                )
                .await
                .unwrap();
        })
        .await;

        // `asset_partitions` table — no clean public API; raw insert
        // matches the `CREATE asset_partitions SET …` shape used by
        // `store_event`'s partition-upsert path.
        expect_yields(&storage, "asset_partitions", async {
            storage
                .db
                .query(
                    "CREATE asset_partitions SET \
                     asset_key = 'live_asset', \
                     partition_key = {kind: 'Single', keys: ['2024-01-01']}, \
                     last_timestamp = 1",
                )
                .await
                .unwrap();
        })
        .await;

        // `events` table.
        expect_yields(&storage, "events", async {
            let ev = make_event("live_asset", "live_runs", 1);
            storage.store_event(&ev).await.unwrap();
        })
        .await;

        // `backfills` table.
        expect_yields(&storage, "backfills", async {
            let bf = BackfillRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                backfill_id: "live_bf".into(),
                status: BackfillStatus::Requested,
                strategy: BackfillStrategy::MultiRun,
                failure_policy: BackfillFailurePolicy::Continue,
                asset_selection: vec!["live_asset".into()],
                job_name: None,
                partition_keys: vec![],
                run_ids: vec![],
                completed_partitions: vec![],
                failed_partitions: vec![],
                canceled_partitions: vec![],
                max_concurrency: 1,
                tags: vec![],
                create_time: 1,
                end_time: None,
                error: None,
            };
            storage.create_backfill(&bf).await.unwrap();
        })
        .await;

        // `ticks` table.
        expect_yields(&storage, "ticks", async {
            let tick = TickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "live_sched".into(),
                automation_type: "Schedule".into(),
                status: "Success".into(),
                timestamp: 1,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: None,
            };
            storage.store_tick(&tick).await.unwrap();
        })
        .await;

        // `condition_ticks` table.
        expect_yields(&storage, "condition_ticks", async {
            let ct = ConditionTickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                timestamp: 1,
                total_evaluated: 0,
                total_fired: 0,
                eval_duration_us: 0,
                run_ids: vec![],
                backfill_ids: vec![],
            };
            storage.store_condition_tick(&ct).await.unwrap();
        })
        .await;

        // `condition_evals` table.
        expect_yields(&storage, "condition_evals", async {
            let ev = ConditionEvalRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "live_asset".into(),
                tick_id: "live_ct".into(),
                timestamp: 1,
                fired: false,
                eval_duration_us: 0,
                run_ids: vec![],
                tree_json: b"{}".to_vec(),
                selection_json: None,
            };
            storage.store_condition_evals_batch(&[ev]).await.unwrap();
        })
        .await;

        // `concurrency_pools` table.
        expect_yields(&storage, "concurrency_pools", async {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "live_pool", 2, 60)
                .await
                .unwrap();
        })
        .await;

        // `concurrency_slots` table — raw insert (full claim transaction
        // is covered by the concurrency tests; here we only need a CREATE
        // on the table to wake the LIVE query).
        expect_yields(&storage, "concurrency_slots", async {
            storage
                .db
                .query(
                    "CREATE concurrency_slots SET \
                     pool_key = 'live_pool', run_id = 'live_runs', step_key = 's1', \
                     slots_consumed = 1, claimed_at = 1, \
                     lease_expires_at = 9999999999, last_heartbeat = 1",
                )
                .await
                .unwrap();
        })
        .await;

        // `pending_steps` table — same argument as `concurrency_slots`.
        expect_yields(&storage, "pending_steps", async {
            storage
                .db
                .query(
                    "CREATE pending_steps SET \
                     pool_key = 'live_pool', run_id = 'live_pending', step_key = 's2', \
                     priority = 0, enqueued_at = 1, block_reason = 'PoolFull'",
                )
                .await
                .unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn test_create_runs_batch() {
        let storage = make_storage().await;
        let runs: Vec<RunRecord> = (0..5)
            .map(|i| RunRecord {
                run_id: format!("batch_{i}"),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job".to_string()),
                status: RunStatus::Queued,
                start_time: i * 100,
                end_time: None,
                tags: vec![],
                node_names: vec![format!("asset_{i}")],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .collect();

        storage.create_runs(&runs).await.unwrap();

        let stored = storage.get_all_runs(10, None).await.unwrap();
        assert_eq!(stored.len(), 5);
        for run in &runs {
            assert!(stored.iter().any(|r| r.run_id == run.run_id));
        }
    }

    #[tokio::test]
    async fn test_create_runs_batch_empty() {
        let storage = make_storage().await;
        storage.create_runs(&[]).await.unwrap();
        let stored = storage.get_all_runs(10, None).await.unwrap();
        assert_eq!(stored.len(), 0);
    }

    #[tokio::test]
    async fn test_create_runs_batch_preserves_fields() {
        let storage = make_storage().await;
        let runs = vec![
            RunRecord {
                run_id: "queued_1".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("__default__".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![("env".to_string(), "prod".to_string())],
                node_names: vec!["a".to_string(), "b".to_string()],
                priority: 5,
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["2025-01-01".to_string()],
                }),
                block_reason: Some("global run limit".to_string()),
                launched_by: LaunchedBy::Manual,
            },
            RunRecord {
                run_id: "queued_2".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("my_job".to_string()),
                status: RunStatus::Queued,
                start_time: 2000,
                end_time: None,
                tags: vec![],
                node_names: vec!["c".to_string()],
                priority: -10,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            },
        ];

        storage.create_runs(&runs).await.unwrap();

        let r1 = storage.get_run("queued_1").await.unwrap().unwrap();
        assert_eq!(r1.status, RunStatus::Queued);
        assert_eq!(r1.priority, 5);
        assert_eq!(r1.tags, vec![("env".to_string(), "prod".to_string())]);
        assert_eq!(r1.node_names, vec!["a".to_string(), "b".to_string()]);
        assert!(r1.partition_key.is_some());
        assert_eq!(r1.block_reason.as_deref(), Some("global run limit"));

        let r2 = storage.get_run("queued_2").await.unwrap().unwrap();
        assert_eq!(r2.job_name.as_deref(), Some("my_job"));
        assert_eq!(r2.priority, -10);
        assert!(r2.block_reason.is_none());
    }

    #[tokio::test]
    async fn test_kv_get_set() {
        let storage = make_storage().await;

        assert!(storage.kv_get("key1").await.unwrap().is_none());

        storage.kv_set("key1", b"hello").await.unwrap();
        let val = storage.kv_get("key1").await.unwrap().unwrap();
        assert_eq!(val, b"hello");

        // Overwrite
        storage.kv_set("key1", b"world").await.unwrap();
        let val = storage.kv_get("key1").await.unwrap().unwrap();
        assert_eq!(val, b"world");
    }

    #[tokio::test]
    async fn test_kv_independent_keys() {
        let storage = make_storage().await;
        storage.kv_set("a", b"1").await.unwrap();
        storage.kv_set("b", b"2").await.unwrap();

        assert_eq!(storage.kv_get("a").await.unwrap().unwrap(), b"1");
        assert_eq!(storage.kv_get("b").await.unwrap().unwrap(), b"2");
    }

    #[tokio::test]
    async fn test_dynamic_keys_round_trip() {
        let storage = make_storage().await;
        let ctx = crate::storage::CodeLocationContext::default_for_tests();
        let scoped = storage.for_code_location(&ctx);

        // Absent → None.
        assert!(
            scoped
                .get_dynamic_keys("src", None, "dv-1")
                .await
                .unwrap()
                .is_none()
        );

        // Persist + read back.
        scoped
            .set_dynamic_keys("src", None, "dv-1", &["a".into(), "b".into()])
            .await
            .unwrap();
        assert_eq!(
            scoped.get_dynamic_keys("src", None, "dv-1").await.unwrap(),
            Some(vec!["a".into(), "b".into()])
        );

        // Different data_version → independent slot, prior is invisible.
        assert!(
            scoped
                .get_dynamic_keys("src", None, "dv-2")
                .await
                .unwrap()
                .is_none()
        );

        // Partition-scoped: same asset+dv but different partition is its own slot.
        let part = PartitionKey::Single {
            keys: vec!["2024-01-01".into()],
        };
        assert!(
            scoped
                .get_dynamic_keys("src", Some(&part), "dv-1")
                .await
                .unwrap()
                .is_none()
        );
        scoped
            .set_dynamic_keys("src", Some(&part), "dv-1", &["x".into()])
            .await
            .unwrap();
        assert_eq!(
            scoped
                .get_dynamic_keys("src", Some(&part), "dv-1")
                .await
                .unwrap(),
            Some(vec!["x".into()])
        );
        // Unpartitioned slot still intact.
        assert_eq!(
            scoped.get_dynamic_keys("src", None, "dv-1").await.unwrap(),
            Some(vec!["a".into(), "b".into()])
        );
    }

    #[tokio::test]
    async fn test_dynamic_keys_isolated_per_code_location() {
        let storage = make_storage().await;
        let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
        let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

        storage
            .for_code_location(&ctx_a)
            .set_dynamic_keys("src", None, "dv", &["from-a".into()])
            .await
            .unwrap();

        // CL-B sees nothing under the same asset/dv.
        assert!(
            storage
                .for_code_location(&ctx_b)
                .get_dynamic_keys("src", None, "dv")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_event_with_metadata() {
        let storage = make_storage().await;
        register(&storage, &["m"]).await;
        let event = EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("m".to_string()),
            run_id: "r".to_string(),
            partition_key: None,
            timestamp: 500,
            metadata: vec![
                ("path".to_string(), "/data/out.parquet".to_string()),
                ("rows".to_string(), "1000".to_string()),
            ],
            input_data_versions: vec![],
        };
        storage.store_event(&event).await.unwrap();

        let events = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "m", 1)
            .await
            .unwrap();
        assert_eq!(events[0].metadata.len(), 2);
        assert_eq!(
            events[0].metadata[0],
            ("path".to_string(), "/data/out.parquet".to_string())
        );
        assert_eq!(
            events[0].metadata[1],
            ("rows".to_string(), "1000".to_string())
        );
    }

    #[tokio::test]
    async fn test_events_limit() {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;
        for i in 0..10 {
            storage
                .store_event(&make_event("a", &format!("r{}", i), i))
                .await
                .unwrap();
        }

        let events = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 3)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn test_nonexistent_asset_returns_none() {
        let storage = make_storage().await;
        assert!(
            storage
                .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "nope")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_latest_materialization(crate::storage::DEFAULT_CODE_LOCATION_ID, "nope", None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_nonexistent_run_returns_none() {
        let storage = make_storage().await;
        assert!(storage.get_run("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_register_assets_with_catalog_fields() {
        let storage = make_storage().await;
        let records = vec![
            AssetRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "x".to_string(),
                tags: vec!["etl".to_string(), "daily".to_string()],
                kinds: vec!["table".to_string()],
                asset_group: Some("analytics".to_string()),
                code_version: Some("v1".to_string()),
                last_event_id: None,
                last_run_id: None,
                last_timestamp: None,
                last_data_version: None,
                last_materialization_code_version: None,
                last_input_data_versions: vec![],
                pool: vec![],
            },
            AssetRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "y".to_string(),
                tags: vec!["ml".to_string()],
                kinds: vec!["model".to_string()],
                asset_group: None,
                code_version: None,
                last_event_id: None,
                last_run_id: None,
                last_timestamp: None,
                last_data_version: None,
                last_materialization_code_version: None,
                last_input_data_versions: vec![],
                pool: vec![],
            },
        ];
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
            .await
            .unwrap();

        let x = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "x")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x, records[0]);

        let y = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "y")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(y, records[1]);
    }

    #[tokio::test]
    async fn test_register_preserves_materialization_fields() {
        let storage = make_storage().await;
        let records = vec![AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "a".to_string(),
            tags: vec!["etl".to_string()],
            kinds: vec!["table".to_string()],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }];
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
            .await
            .unwrap();

        // Materialize
        storage
            .store_event(&make_event("a", "r1", 100))
            .await
            .unwrap();

        // Re-register (e.g. next run) — should preserve last_run_id etc.
        let records = vec![AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "a".to_string(),
            tags: vec!["etl".to_string(), "new_tag".to_string()],
            kinds: vec!["table".to_string()],
            asset_group: Some("warehouse".to_string()),
            code_version: Some("v2".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }];
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
            .await
            .unwrap();

        let record = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.tags, vec!["etl", "new_tag"]);
        assert_eq!(record.asset_group.as_deref(), Some("warehouse"));
        assert_eq!(record.code_version.as_deref(), Some("v2"));
        // Materialization fields preserved
        assert_eq!(record.last_run_id.as_deref(), Some("r1"));
        assert_eq!(record.last_timestamp, Some(100));
    }

    #[tokio::test]
    async fn test_materialization_preserves_catalog_fields() {
        let storage = make_storage().await;
        let records = vec![AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "a".to_string(),
            tags: vec!["etl".to_string()],
            kinds: vec!["table".to_string()],
            asset_group: Some("analytics".to_string()),
            code_version: Some("v1".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }];
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
            .await
            .unwrap();

        // Materialize — should update last_* but preserve tags/kind/group
        storage
            .store_event(&make_event("a", "r1", 500))
            .await
            .unwrap();

        let record = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.tags, vec!["etl"]);
        assert_eq!(record.kinds, vec!["table"]);
        assert_eq!(record.asset_group.as_deref(), Some("analytics"));
        assert_eq!(record.code_version.as_deref(), Some("v1"));
        assert_eq!(record.last_run_id.as_deref(), Some("r1"));
        assert_eq!(record.last_timestamp, Some(500));
    }

    #[tokio::test]
    async fn test_get_assets_by_tag() {
        let storage = make_storage().await;
        storage
            .register_assets(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "a".to_string(),
                        tags: vec!["etl".to_string(), "daily".to_string()],
                        kinds: vec![],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "b".to_string(),
                        tags: vec!["ml".to_string()],
                        kinds: vec![],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "c".to_string(),
                        tags: vec!["etl".to_string()],
                        kinds: vec![],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                ],
            )
            .await
            .unwrap();

        let etl = storage
            .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "etl")
            .await
            .unwrap();
        assert_eq!(etl.len(), 2);

        let ml = storage
            .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "ml")
            .await
            .unwrap();
        assert_eq!(ml.len(), 1);
        assert_eq!(ml[0].asset_key, "b");

        let none = storage
            .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn test_get_assets_by_kind() {
        let storage = make_storage().await;
        storage
            .register_assets(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "a".to_string(),
                        tags: vec![],
                        kinds: vec!["table".to_string()],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "b".to_string(),
                        tags: vec![],
                        kinds: vec!["model".to_string()],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                ],
            )
            .await
            .unwrap();

        let tables = storage
            .get_assets_by_kind(crate::storage::DEFAULT_CODE_LOCATION_ID, "table")
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].asset_key, "a");
    }

    #[tokio::test]
    async fn test_dynamic_partitions_add_and_get() {
        let storage = make_storage().await;
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u1".into(), "u2".into(), "u3".into()],
            )
            .await
            .unwrap();

        let keys = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
            .await
            .unwrap();
        assert_eq!(keys, vec!["u1", "u2", "u3"]);
    }

    #[tokio::test]
    async fn test_dynamic_partitions_idempotent_add() {
        let storage = make_storage().await;
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u1".into(), "u2".into()],
            )
            .await
            .unwrap();
        // Add again with overlap — should not duplicate
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u2".into(), "u3".into()],
            )
            .await
            .unwrap();

        let keys = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
            .await
            .unwrap();
        assert_eq!(keys, vec!["u1", "u2", "u3"]);
    }

    #[tokio::test]
    async fn test_dynamic_partitions_delete() {
        let storage = make_storage().await;
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u1".into(), "u2".into(), "u3".into()],
            )
            .await
            .unwrap();

        storage
            .delete_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u2")
            .await
            .unwrap();

        let keys = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
            .await
            .unwrap();
        assert_eq!(keys, vec!["u1", "u3"]);
    }

    #[tokio::test]
    async fn test_dynamic_partitions_delete_nonexistent() {
        let storage = make_storage().await;
        // Should not error when deleting non-existent partition
        storage
            .delete_dynamic_partition(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                "nonexistent",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_dynamic_partitions_has() {
        let storage = make_storage().await;
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u1".into()],
            )
            .await
            .unwrap();

        assert!(
            storage
                .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u1")
                .await
                .unwrap()
        );
        assert!(
            !storage
                .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u2")
                .await
                .unwrap()
        );
        assert!(
            !storage
                .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "other", "u1")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_dynamic_partitions_isolated_by_name() {
        let storage = make_storage().await;
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "users",
                &["u1".into()],
            )
            .await
            .unwrap();
        storage
            .add_dynamic_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "products",
                &["p1".into(), "p2".into()],
            )
            .await
            .unwrap();

        let users = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
            .await
            .unwrap();
        assert_eq!(users, vec!["u1"]);

        let products = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "products")
            .await
            .unwrap();
        assert_eq!(products, vec!["p1", "p2"]);
    }

    #[tokio::test]
    async fn test_dynamic_partitions_empty() {
        let storage = make_storage().await;
        let keys = storage
            .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
            .await
            .unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_get_assets_by_group() {
        let storage = make_storage().await;
        storage
            .register_assets(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "a".to_string(),
                        tags: vec![],
                        kinds: vec![],
                        asset_group: Some("analytics".to_string()),
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "b".to_string(),
                        tags: vec![],
                        kinds: vec![],
                        asset_group: Some("analytics".to_string()),
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                    AssetRecord {
                        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                        asset_key: "c".to_string(),
                        tags: vec![],
                        kinds: vec![],
                        asset_group: Some("ml".to_string()),
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    },
                ],
            )
            .await
            .unwrap();

        let analytics = storage
            .get_assets_by_group(crate::storage::DEFAULT_CODE_LOCATION_ID, "analytics")
            .await
            .unwrap();
        assert_eq!(analytics.len(), 2);

        let ml = storage
            .get_assets_by_group(crate::storage::DEFAULT_CODE_LOCATION_ID, "ml")
            .await
            .unwrap();
        assert_eq!(ml.len(), 1);
    }

    // ── UI integration regression tests ──────────────────────────────────

    #[tokio::test]
    async fn test_events_same_timestamp_deterministic_order() {
        // Regression: events with identical timestamps must have deterministic
        // ordering via secondary id sort (not random each query).
        let storage = make_storage().await;
        register(&storage, &["a"]).await;

        for etype in [
            EventType::StepStart,
            EventType::Materialization { data_version: None },
            EventType::StepSuccess,
        ] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: etype,
                    asset_key: Some("a".to_string()),
                    run_id: "r1".to_string(),
                    partition_key: None,
                    timestamp: 1000,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        // Query twice — order must be identical (deterministic via id sort)
        let run_a = storage.get_events_for_run("r1").await.unwrap();
        let run_b = storage.get_events_for_run("r1").await.unwrap();
        assert_eq!(run_a.len(), 3);
        for (a, b) in run_a.iter().zip(run_b.iter()) {
            assert_eq!(a.event_type, b.event_type);
        }

        let asset_a = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
            .await
            .unwrap();
        let asset_b = storage
            .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
            .await
            .unwrap();
        assert_eq!(asset_a.len(), 3);
        for (a, b) in asset_a.iter().zip(asset_b.iter()) {
            assert_eq!(a.event_type, b.event_type);
        }
    }

    #[tokio::test]
    async fn test_runs_filtered_by_status() {
        // Regression: UI runs page status tabs must correctly filter.
        let storage = make_storage().await;

        for (id, status) in [
            ("r1", RunStatus::Success),
            ("r2", RunStatus::Success),
            ("r3", RunStatus::Failure),
            ("r4", RunStatus::Started),
            ("r5", RunStatus::NotStarted),
        ] {
            storage
                .create_run(&RunRecord {
                    run_id: id.to_string(),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some("job1".to_string()),
                    status,
                    start_time: 1000,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }

        let all = storage.get_all_runs(100, None).await.unwrap();
        assert_eq!(all.len(), 5);

        let success = storage
            .get_all_runs(100, Some(RunStatus::Success))
            .await
            .unwrap();
        assert_eq!(success.len(), 2);
        assert!(success.iter().all(|r| r.status == RunStatus::Success));

        let failure = storage
            .get_all_runs(100, Some(RunStatus::Failure))
            .await
            .unwrap();
        assert_eq!(failure.len(), 1);
        assert_eq!(failure[0].run_id, "r3");

        let started = storage
            .get_all_runs(100, Some(RunStatus::Started))
            .await
            .unwrap();
        assert_eq!(started.len(), 1);

        let not_started = storage
            .get_all_runs(100, Some(RunStatus::NotStarted))
            .await
            .unwrap();
        assert_eq!(not_started.len(), 1);
    }

    #[tokio::test]
    async fn test_runs_for_asset_filtering() {
        // Regression: asset detail page shows runs for the specific asset only.
        let storage = make_storage().await;
        register(&storage, &["asset_a", "asset_b"]).await;

        storage
            .create_run(&RunRecord {
                run_id: "r1".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job1".to_string()),
                status: RunStatus::Success,
                start_time: 1000,
                end_time: Some(1010),
                tags: vec![],
                node_names: vec!["asset_a".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();
        storage
            .create_run(&RunRecord {
                run_id: "r2".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job1".to_string()),
                status: RunStatus::Success,
                start_time: 2000,
                end_time: Some(2010),
                tags: vec![],
                node_names: vec!["asset_a".to_string(), "asset_b".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();
        storage
            .create_run(&RunRecord {
                run_id: "r3".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job2".to_string()),
                status: RunStatus::Failure,
                start_time: 3000,
                end_time: Some(3005),
                tags: vec![],
                node_names: vec!["asset_b".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Filter by asset_a — should get r1 and r2
        let all = storage.get_all_runs(1000, None).await.unwrap();
        let for_a: Vec<_> = all
            .iter()
            .filter(|r| r.node_names.contains(&"asset_a".to_string()))
            .collect();
        assert_eq!(for_a.len(), 2);

        // Filter by asset_b — should get r2 and r3
        let for_b: Vec<_> = all
            .iter()
            .filter(|r| r.node_names.contains(&"asset_b".to_string()))
            .collect();
        assert_eq!(for_b.len(), 2);
    }

    #[tokio::test]
    async fn test_ticks_ordered_desc_and_counted() {
        // Regression: deployment page shows tick count; sensor detail shows tick history.
        let storage = make_storage().await;

        let ticks: Vec<TickRecord> = (0..5)
            .map(|i| TickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "my_sensor".to_string(),
                automation_type: "Sensor".to_string(),
                status: "Success".to_string(),
                timestamp: 1000 + i * 60,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: Some(format!("cursor_{i}")),
            })
            .collect();
        storage.store_ticks_batch(&ticks).await.unwrap();

        let stored = storage
            .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 100)
            .await
            .unwrap();
        assert_eq!(stored.len(), 5);
        // Should be ordered DESC by timestamp — full struct equality
        for (idx, actual) in stored.iter().enumerate() {
            let i = 4 - idx as i64; // maps to original index (DESC)
            let expected = StoredTick {
                id: actual.id.clone(),
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "my_sensor".to_string(),
                automation_type: "Sensor".to_string(),
                status: "Success".to_string(),
                timestamp: 1000 + i * 60,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: Some(format!("cursor_{i}")),
            };
            assert_eq!(*actual, expected);
        }

        // Limit works
        let limited = storage
            .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 2)
            .await
            .unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].timestamp, 1240);

        // Different automation name returns empty
        let other = storage
            .get_ticks(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "other_sensor",
                100,
            )
            .await
            .unwrap();
        assert!(other.is_empty());
    }

    #[tokio::test]
    async fn test_runs_with_tags_and_node_names() {
        // Regression: runs list page filters by partition tag and asset name.
        let storage = make_storage().await;

        storage
            .create_run(&RunRecord {
                run_id: "r1".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("daily_job".to_string()),
                status: RunStatus::Success,
                start_time: 1000,
                end_time: Some(1060),
                tags: vec![("partition".to_string(), "2024-01-01".to_string())],
                node_names: vec!["orders".to_string(), "revenue".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();
        storage
            .create_run(&RunRecord {
                run_id: "r2".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("weekly_job".to_string()),
                status: RunStatus::Failure,
                start_time: 2000,
                end_time: Some(2120),
                tags: vec![("partition".to_string(), "2024-01-07".to_string())],
                node_names: vec!["summary".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let all = storage.get_all_runs(100, None).await.unwrap();
        assert_eq!(all.len(), 2);

        // Verify tags are preserved
        let r1 = all.iter().find(|r| r.run_id == "r1").unwrap();
        assert_eq!(r1.tags.len(), 1);
        assert_eq!(
            r1.tags[0],
            ("partition".to_string(), "2024-01-01".to_string())
        );
        assert_eq!(r1.node_names, vec!["orders", "revenue"]);

        // Verify node_names on r2
        let r2 = all.iter().find(|r| r.run_id == "r2").unwrap();
        assert_eq!(r2.node_names, vec!["summary"]);
    }

    #[tokio::test]
    async fn test_events_batch_same_timestamp_deterministic() {
        // Regression: batch-inserted events with same timestamp must have
        // deterministic ordering via secondary id sort.
        let storage = make_storage().await;
        register(&storage, &["x"]).await;

        let events = vec![
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("x".to_string()),
                run_id: "batch_run".to_string(),
                partition_key: None,
                timestamp: 5000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v1".to_string()),
                },
                asset_key: Some("x".to_string()),
                run_id: "batch_run".to_string(),
                partition_key: None,
                timestamp: 5000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("x".to_string()),
                run_id: "batch_run".to_string(),
                partition_key: None,
                timestamp: 5000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ];
        let ids = storage.store_events(&events).await.unwrap();
        assert_eq!(ids.len(), 3);

        // Query twice — order must be deterministic and full struct equality
        let run_a = storage.get_events_for_run("batch_run").await.unwrap();
        let run_b = storage.get_events_for_run("batch_run").await.unwrap();
        assert_eq!(run_a.len(), 3);
        for (a, b) in run_a.iter().zip(run_b.iter()) {
            assert_eq!(a, b);
        }
    }

    #[tokio::test]
    async fn test_input_versions_from_event_not_storage() {
        // Regression: input_data_versions must come from the EventRecord (captured
        // at read time by the executor), not looked up from storage after the fact.
        // This prevents a race where a concurrent run updates an upstream's
        // last_data_version between the time downstream reads it and the time
        // the materialization event is stored.
        let storage = make_storage().await;
        register(&storage, &["upstream", "downstream"]).await;

        // Store graph topology so staleness computation knows the dependency
        use crate::assets::graph::TopologyNode;
        let topology = GraphTopology {
            nodes: vec![
                TopologyNode {
                    name: "upstream".to_string(),
                    kind: crate::assets::graph::NodeKind::Asset,
                    group: None,
                    parent_graph: None,
                },
                TopologyNode {
                    name: "downstream".to_string(),
                    kind: crate::assets::graph::NodeKind::Asset,
                    group: None,
                    parent_graph: None,
                },
            ],
            edges: vec![("downstream".to_string(), "upstream".to_string())],
        };
        let topo_json = serde_json::to_vec(&topology).unwrap();
        storage
            .kv_set(
                &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
                &topo_json,
            )
            .await
            .unwrap();

        // 1. Materialize upstream with data_version "v1"
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v1".to_string()),
                },
                asset_key: Some("upstream".to_string()),
                run_id: "r1".to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // 2. Simulate the race: upstream gets re-materialized to "v2"
        //    BEFORE downstream's event is stored
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v2".to_string()),
                },
                asset_key: Some("upstream".to_string()),
                run_id: "r2".to_string(),
                partition_key: None,
                timestamp: 200,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // At this point upstream's last_data_version in storage is "v2".
        // But downstream actually read "v1" during its execution.

        // 3. Store downstream's materialization with the version it actually
        //    consumed ("v1"), provided by the executor in input_data_versions.
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv_down".to_string()),
                },
                asset_key: Some("downstream".to_string()),
                run_id: "r1".to_string(),
                partition_key: None,
                timestamp: 300,
                metadata: vec![],
                // The executor captured this at read time — "v1", not "v2"
                input_data_versions: vec![("upstream".to_string(), "v1".to_string())],
            })
            .await
            .unwrap();

        // 4. Verify: downstream recorded that it consumed "v1"
        let down = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "downstream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            down.last_input_data_versions,
            vec![("upstream".to_string(), "v1".to_string())],
            "Should record the version the executor actually read, not the current storage value"
        );

        // 5. Verify staleness: upstream is now at "v2" but downstream consumed "v1"
        //    → downstream should be Stale. Compute staleness on demand
        //    (no longer persisted on the record).
        let staleness = crate::staleness::compute_staleness(
            &[
                storage
                    .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "upstream")
                    .await
                    .unwrap()
                    .unwrap(),
                down.clone(),
            ],
            &[("downstream".to_string(), "upstream".to_string())],
        );
        let (status, causes) = staleness.get("downstream").unwrap();
        assert_eq!(status, &StaleStatus::Stale);
        assert!(!causes.is_empty());

        // If the old code had looked up storage at write time, it would have
        // recorded "v2" and downstream would incorrectly appear UpToDate.
    }

    // ── get_observations_since tests ──

    #[tokio::test]
    async fn test_get_observations_since() {
        let storage = make_storage().await;
        register(&storage, &["ext_a", "ext_b"]).await;

        // Store an observation at ts=1000
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Observation {
                    data_version: Some("v1".to_string()),
                },
                asset_key: Some("ext_a".to_string()),
                run_id: String::new(),
                partition_key: None,
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Store an observation at ts=2000
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Observation {
                    data_version: Some("v2".to_string()),
                },
                asset_key: Some("ext_b".to_string()),
                run_id: String::new(),
                partition_key: None,
                timestamp: 2000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Store a materialization at ts=1500 (should NOT be returned)
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("m1".to_string()),
                },
                asset_key: Some("ext_a".to_string()),
                run_id: "r1".to_string(),
                partition_key: None,
                timestamp: 1500,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Query observations since ts=0 — should return both observations, not the materialization
        // Ordered DESC by timestamp
        let all = storage.get_observations_since(0).await.unwrap();
        assert_eq!(all.len(), 2);
        let expected_0 = StoredEvent {
            id: all[0].id.clone(),
            event_type: EventType::Observation {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("ext_b".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 2000,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        let expected_1 = StoredEvent {
            id: all[1].id.clone(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("ext_a".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        assert_eq!(all[0], expected_0);
        assert_eq!(all[1], expected_1);

        // Query observations since ts=1000 — should return only the one at ts=2000
        let recent = storage.get_observations_since(1000).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0], expected_0);

        // Query observations since ts=2000 — should return nothing
        let none = storage.get_observations_since(2000).await.unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn test_condition_evals_store_and_retrieve() {
        use super::ConditionEvalRecord;

        let storage = make_storage().await;

        let tree_json = serde_json::to_vec(&serde_json::json!({
            "node_idx": 0, "label": "All of", "node_type": "And",
            "status": "True", "children": [
                {"node_idx": 1, "label": "missing", "node_type": "Leaf", "status": "True", "children": []}
            ]
        }))
        .unwrap();

        let evals: Vec<ConditionEvalRecord> = (0..5)
            .map(|i| ConditionEvalRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "my_asset".to_string(),
                tick_id: format!("tick_{i}"),
                timestamp: 1000 + i * 30,
                fired: i % 2 == 0,
                eval_duration_us: 50 + i as u64 * 10,
                run_ids: vec![],
                tree_json: tree_json.clone(),
                selection_json: None,
            })
            .collect();
        let ids = storage.store_condition_evals_batch(&evals).await.unwrap();
        assert_eq!(ids.len(), 5);

        // Retrieve — should be ordered DESC by timestamp, full struct equality
        let stored = storage
            .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 100)
            .await
            .unwrap();
        assert_eq!(stored.len(), 5);
        for (idx, actual) in stored.iter().enumerate() {
            let i = 4 - idx as i64; // maps to original index (DESC)
            let expected = StoredConditionEval {
                id: actual.id.clone(),
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "my_asset".to_string(),
                tick_id: format!("tick_{i}"),
                timestamp: 1000 + i * 30,
                fired: i % 2 == 0,
                eval_duration_us: 50 + i as u64 * 10,
                run_ids: vec![],
                tree_json: tree_json.clone(),
                selection_json: None,
            };
            assert_eq!(*actual, expected);
        }

        // Limit works
        let limited = storage
            .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 2)
            .await
            .unwrap();
        assert_eq!(limited.len(), 2);

        // Different asset key returns empty
        let other = storage
            .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "other_asset", 100)
            .await
            .unwrap();
        assert!(other.is_empty());

        // Prune keeps newest 2
        let pruned = storage
            .prune_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 2)
            .await
            .unwrap();
        assert_eq!(pruned, 3);
        let after_prune = storage
            .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 100)
            .await
            .unwrap();
        assert_eq!(after_prune.len(), 2);
        assert_eq!(after_prune[0].timestamp, 1120);
    }

    // ── Zero-coverage function tests ──

    #[tokio::test]
    async fn test_has_step_completed() {
        let storage = make_storage().await;
        register(&storage, &["asset_a", "asset_b"]).await;

        // run_1: only StepStart for asset_a
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("asset_a".to_string()),
                run_id: "run_1".to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // run_2: StepSuccess for asset_a
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("asset_a".to_string()),
                run_id: "run_2".to_string(),
                partition_key: None,
                timestamp: 200,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // run_3: StepFailure for asset_b
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset_b".to_string()),
                run_id: "run_3".to_string(),
                partition_key: None,
                timestamp: 300,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Only StepStart — not completed
        assert!(
            !storage
                .has_step_completed("asset_a", &["run_1".to_string()])
                .await
                .unwrap()
        );
        // StepSuccess — completed
        assert!(
            storage
                .has_step_completed("asset_a", &["run_2".to_string()])
                .await
                .unwrap()
        );
        // StepFailure — completed
        assert!(
            storage
                .has_step_completed("asset_b", &["run_3".to_string()])
                .await
                .unwrap()
        );
        // Unknown run
        assert!(
            !storage
                .has_step_completed("asset_a", &["run_99".to_string()])
                .await
                .unwrap()
        );
        // Empty slice
        assert!(!storage.has_step_completed("asset_a", &[]).await.unwrap());
        // Multiple runs — finds it in run_2
        assert!(
            storage
                .has_step_completed("asset_a", &["run_1".to_string(), "run_2".to_string()])
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_asset_records_by_keys() {
        let storage = make_storage().await;
        register(&storage, &["a", "b", "c"]).await;

        let records = storage
            .get_asset_records_by_keys(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &["a".to_string(), "c".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        let mut keys: Vec<&str> = records.iter().map(|r| r.asset_key.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["a", "c"]);

        // Struct equality
        for r in &records {
            let expected = make_asset_record(&r.asset_key);
            assert_eq!(*r, expected);
        }

        // Unknown keys
        let empty = storage
            .get_asset_records_by_keys(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &["unknown".to_string()],
            )
            .await
            .unwrap();
        assert!(empty.is_empty());

        // Empty slice
        let empty = storage
            .get_asset_records_by_keys(crate::storage::DEFAULT_CODE_LOCATION_ID, &[])
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_runs_by_ids() {
        let storage = make_storage().await;

        let run1 = RunRecord {
            run_id: "run_1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j1".to_string()),
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        let run2 = RunRecord {
            run_id: "run_2".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j1".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        let run3 = RunRecord {
            run_id: "run_3".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j2".to_string()),
            status: RunStatus::Failure,
            start_time: 3000,
            end_time: Some(3500),
            tags: vec![("env".to_string(), "prod".to_string())],
            node_names: vec!["b".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run1).await.unwrap();
        storage.create_run(&run2).await.unwrap();
        storage.create_run(&run3).await.unwrap();

        // Query subset
        let mut results = storage
            .get_runs_by_ids(&["run_1".to_string(), "run_3".to_string()], None)
            .await
            .unwrap();
        results.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], run1);
        assert_eq!(results[1], run3);

        // With status filter
        let results = storage
            .get_runs_by_ids(
                &["run_1".to_string(), "run_2".to_string()],
                Some(RunStatus::Started),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], run2);

        // Empty IDs
        let results = storage.get_runs_by_ids(&[], None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_get_runs_since() {
        let storage = make_storage().await;

        let run1 = RunRecord {
            run_id: "run_1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        let run2 = RunRecord {
            run_id: "run_2".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        let run3 = RunRecord {
            run_id: "run_3".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Success,
            start_time: 3000,
            end_time: Some(3500),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run1).await.unwrap();
        storage.create_run(&run2).await.unwrap();
        storage.create_run(&run3).await.unwrap();

        // Since 1500 — run2 (2000) and run3 (3000), DESC order
        let results = storage.get_all_runs_since(1500, None).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], run3);
        assert_eq!(results[1], run2);

        // Since 1500 with status filter
        let results = storage
            .get_all_runs_since(1500, Some(RunStatus::Success))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], run3);

        // Since 5000 — empty
        let results = storage.get_all_runs_since(5000, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_store_tick_single() {
        let storage = make_storage().await;

        let tick = TickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "my_sensor".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Skipped".to_string(),
            timestamp: 5000,
            run_ids: vec!["r1".to_string()],
            backfill_ids: vec![],
            skip_reason: Some("No new data".to_string()),
            error: None,
            cursor: Some("cursor_42".to_string()),
        };
        let id = storage.store_tick(&tick).await.unwrap();
        assert!(!id.is_empty());

        let stored = storage
            .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        let actual = &stored[0];
        let expected = StoredTick {
            id: actual.id.clone(),
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "my_sensor".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Skipped".to_string(),
            timestamp: 5000,
            run_ids: vec!["r1".to_string()],
            backfill_ids: vec![],
            skip_reason: Some("No new data".to_string()),
            error: None,
            cursor: Some("cursor_42".to_string()),
        };
        assert_eq!(*actual, expected);
    }

    #[tokio::test]
    async fn test_prune_ticks() {
        let storage = make_storage().await;

        // Store 10 ticks for sensor_a
        let ticks_a: Vec<TickRecord> = (0..10)
            .map(|i| TickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "sensor_a".to_string(),
                automation_type: "Sensor".to_string(),
                status: "Success".to_string(),
                timestamp: 1000 + i * 100,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: None,
            })
            .collect();
        storage.store_ticks_batch(&ticks_a).await.unwrap();

        // Store 3 ticks for sensor_b
        let ticks_b: Vec<TickRecord> = (0..3)
            .map(|i| TickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                automation_name: "sensor_b".to_string(),
                automation_type: "Sensor".to_string(),
                status: "Success".to_string(),
                timestamp: 2000 + i * 100,
                run_ids: vec![],
                backfill_ids: vec![],
                skip_reason: None,
                error: None,
                cursor: None,
            })
            .collect();
        storage.store_ticks_batch(&ticks_b).await.unwrap();

        // Prune sensor_a to 3
        let deleted = storage
            .prune_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_a", 3)
            .await
            .unwrap();
        assert_eq!(deleted, 7);

        // Only 3 newest remain
        let remaining = storage
            .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_a", 100)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].timestamp, 1900); // 1000 + 9*100
        assert_eq!(remaining[1].timestamp, 1800);
        assert_eq!(remaining[2].timestamp, 1700);

        // sensor_b unaffected
        let b_remaining = storage
            .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_b", 100)
            .await
            .unwrap();
        assert_eq!(b_remaining.len(), 3);
    }

    #[tokio::test]
    async fn test_store_condition_tick() {
        let storage = make_storage().await;

        let tick = ConditionTickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            timestamp: 1000,
            total_evaluated: 5,
            total_fired: 2,
            eval_duration_us: 500,
            run_ids: vec!["r1".to_string()],
            backfill_ids: vec![],
        };
        let id = storage.store_condition_tick(&tick).await.unwrap();
        assert!(!id.is_empty());

        let stored = storage
            .get_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        let actual = &stored[0];
        let expected = StoredConditionTick {
            id: actual.id.clone(),
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            timestamp: 1000,
            total_evaluated: 5,
            total_fired: 2,
            eval_duration_us: 500,
            run_ids: vec!["r1".to_string()],
            backfill_ids: vec![],
        };
        assert_eq!(*actual, expected);
    }

    #[tokio::test]
    async fn test_get_condition_ticks_ordering_and_limit() {
        let storage = make_storage().await;

        for i in 0..5 {
            storage
                .store_condition_tick(&ConditionTickRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    timestamp: 100 + i * 100,
                    total_evaluated: i as u32,
                    total_fired: 0,
                    eval_duration_us: 50,
                    run_ids: vec![],
                    backfill_ids: vec![],
                })
                .await
                .unwrap();
        }

        // Limit 3 — ordered DESC
        let stored = storage
            .get_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 3)
            .await
            .unwrap();
        assert_eq!(stored.len(), 3);
        assert_eq!(stored[0].timestamp, 500); // 100 + 4*100
        assert_eq!(stored[1].timestamp, 400);
        assert_eq!(stored[2].timestamp, 300);

        // Struct equality on each
        for (idx, actual) in stored.iter().enumerate() {
            let i = 4 - idx; // maps to original i
            let expected = StoredConditionTick {
                id: actual.id.clone(),
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                timestamp: 100 + (i as i64) * 100,
                total_evaluated: i as u32,
                total_fired: 0,
                eval_duration_us: 50,
                run_ids: vec![],
                backfill_ids: vec![],
            };
            assert_eq!(*actual, expected);
        }
    }

    #[tokio::test]
    async fn test_prune_condition_ticks() {
        let storage = make_storage().await;

        for i in 0..10 {
            storage
                .store_condition_tick(&ConditionTickRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    timestamp: 1000 + i * 100,
                    total_evaluated: 1,
                    total_fired: 0,
                    eval_duration_us: 10,
                    run_ids: vec![],
                    backfill_ids: vec![],
                })
                .await
                .unwrap();
        }

        let deleted = storage
            .prune_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 3)
            .await
            .unwrap();
        assert_eq!(deleted, 7);

        let remaining = storage
            .get_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 100)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].timestamp, 1900);
        assert_eq!(remaining[1].timestamp, 1800);
        assert_eq!(remaining[2].timestamp, 1700);
    }

    #[tokio::test]
    async fn test_get_condition_evals_for_tick() {
        let storage = make_storage().await;

        let tree_json = b"{}".to_vec();

        // 3 evals for tick t1 with different asset keys
        let evals_t1: Vec<ConditionEvalRecord> = ["asset_a", "asset_c", "asset_b"]
            .iter()
            .enumerate()
            .map(|(i, key)| ConditionEvalRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: key.to_string(),
                tick_id: "t1".to_string(),
                timestamp: 1000 + i as i64 * 10,
                fired: i == 0,
                eval_duration_us: 50,
                run_ids: vec![],
                tree_json: tree_json.clone(),
                selection_json: None,
            })
            .collect();
        storage
            .store_condition_evals_batch(&evals_t1)
            .await
            .unwrap();

        // 2 evals for tick t2
        let evals_t2: Vec<ConditionEvalRecord> = ["asset_x", "asset_y"]
            .iter()
            .map(|key| ConditionEvalRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: key.to_string(),
                tick_id: "t2".to_string(),
                timestamp: 2000,
                fired: false,
                eval_duration_us: 30,
                run_ids: vec![],
                tree_json: tree_json.clone(),
                selection_json: None,
            })
            .collect();
        storage
            .store_condition_evals_batch(&evals_t2)
            .await
            .unwrap();

        // Query for tick t1 — ordered ASC by asset_key
        let t1_results = storage
            .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t1")
            .await
            .unwrap();
        assert_eq!(t1_results.len(), 3);
        assert_eq!(t1_results[0].asset_key, "asset_a");
        assert_eq!(t1_results[1].asset_key, "asset_b");
        assert_eq!(t1_results[2].asset_key, "asset_c");
        // All belong to t1
        for e in &t1_results {
            assert_eq!(e.tick_id, "t1");
        }

        // Query for tick t2
        let t2_results = storage
            .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t2")
            .await
            .unwrap();
        assert_eq!(t2_results.len(), 2);

        // Unknown tick
        let empty = storage
            .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t99")
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_partition_events() {
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        // Two events for partition p1 at different times
        for (ts, run) in [(100, "r1"), (200, "r2")] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::Materialization { data_version: None },
                    asset_key: Some("asset".to_string()),
                    run_id: run.to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec!["p1".to_string()],
                    }),
                    timestamp: ts,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        // One event for partition p2
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: "r3".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["p2".to_string()],
                }),
                timestamp: 300,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Query p1 — 2 events, DESC by timestamp
        let p1_events = storage
            .get_partition_events(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset", "p1", 10)
            .await
            .unwrap();
        assert_eq!(p1_events.len(), 2);
        assert_eq!(p1_events[0].timestamp, 200);
        assert_eq!(p1_events[1].timestamp, 100);

        // Query p2 — 1 event
        let p2_events = storage
            .get_partition_events(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset", "p2", 10)
            .await
            .unwrap();
        assert_eq!(p2_events.len(), 1);
        assert_eq!(p2_events[0].run_id, "r3");
    }

    #[tokio::test]
    async fn test_get_materialized_partitions() {
        let storage = make_storage().await;
        register(&storage, &["asset", "empty_asset"]).await;

        // Store materialization events with partitions
        for pk in ["p1", "p2", "p3"] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::Materialization { data_version: None },
                    asset_key: Some("asset".to_string()),
                    run_id: "r1".to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec![pk.to_string()],
                    }),
                    timestamp: 1000,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let partitions = storage
            .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);
        let mut pk_strs: Vec<String> = partitions
            .iter()
            .map(|pk| match pk {
                PartitionKey::Single { keys } => keys[0].clone(),
                _ => panic!("expected Single partition key"),
            })
            .collect();
        pk_strs.sort();
        assert_eq!(pk_strs, vec!["p1", "p2", "p3"]);

        // Empty asset
        let empty = storage
            .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "empty_asset")
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_partition_timestamps() {
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        // p1 at ts=1000, then p1 again at ts=2000 (should keep latest)
        for ts in [1000, 2000] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::Materialization { data_version: None },
                    asset_key: Some("asset".to_string()),
                    run_id: "r1".to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec!["p1".to_string()],
                    }),
                    timestamp: ts,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        // p2 at ts=1500
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: "r2".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["p2".to_string()],
                }),
                timestamp: 1500,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let mut timestamps = storage
            .get_partition_timestamps(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
            .await
            .unwrap();
        timestamps.sort_by_key(|(pk, _)| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            _ => String::new(),
        });
        assert_eq!(timestamps.len(), 2);
        // p1 → latest is 2000
        assert_eq!(
            timestamps[0].0,
            PartitionKey::Single {
                keys: vec!["p1".to_string()]
            }
        );
        assert_eq!(timestamps[0].1, 2000);
        // p2 → 1500
        assert_eq!(
            timestamps[1].0,
            PartitionKey::Single {
                keys: vec!["p2".to_string()]
            }
        );
        assert_eq!(timestamps[1].1, 1500);
    }

    #[tokio::test]
    async fn test_get_in_progress_partitions() {
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        // Create a run in Started status
        let run = RunRecord {
            run_id: "run_ip".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Started,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        // Store StepStart events with partition keys
        for pk in ["p1", "p2"] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepStart,
                    asset_key: Some("asset".to_string()),
                    run_id: "run_ip".to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec![pk.to_string()],
                    }),
                    timestamp: 1000,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let in_progress = storage
            .get_in_progress_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
            .await
            .unwrap();
        assert_eq!(in_progress.len(), 2);
        let mut pk_strs: Vec<String> = in_progress
            .iter()
            .map(|pk| match pk {
                PartitionKey::Single { keys } => keys[0].clone(),
                _ => panic!("expected Single partition key"),
            })
            .collect();
        pk_strs.sort();
        assert_eq!(pk_strs, vec!["p1", "p2"]);

        // Mark run as Success → no longer in progress
        storage
            .update_run_status("run_ip", RunStatus::Success, Some(2000))
            .await
            .unwrap();
        let in_progress = storage
            .get_in_progress_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
            .await
            .unwrap();
        assert!(in_progress.is_empty());
    }

    #[tokio::test]
    async fn test_get_failed_partitions() {
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        // Create a run in Failure status
        let run = RunRecord {
            run_id: "run_fail".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Failure,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        // Store StepFailure events with partition keys
        for pk in ["p1", "p2"] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepFailure,
                    asset_key: Some("asset".to_string()),
                    run_id: "run_fail".to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec![pk.to_string()],
                    }),
                    timestamp: 1000,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let failed = storage
            .get_failed_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "asset",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(failed.len(), 2);
        let mut pk_strs: Vec<String> = failed
            .keys()
            .map(|pk| match pk {
                PartitionKey::Single { keys } => keys[0].clone(),
                _ => panic!("expected Single partition key"),
            })
            .collect();
        pk_strs.sort();
        assert_eq!(pk_strs, vec!["p1", "p2"]);

        // Asset with no failures
        register(&storage, &["clean_asset"]).await;
        let empty = storage
            .get_failed_partitions(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "clean_asset",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_failed_partitions_includes_marked_in_success_run() {
        // review #1: a mark_partition_failed key lands in a Success run but must
        // still report failed, else automation re-materializes it.
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        let run = RunRecord {
            run_id: "run_ok".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset".to_string()),
                run_id: "run_ok".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["b".to_string()],
                }),
                timestamp: 1000,
                metadata: vec![("error".to_string(), "boom".to_string())],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let failed = storage
            .get_failed_partitions(
                DEFAULT_CODE_LOCATION_ID,
                "asset",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(failed.len(), 1);
        assert!(failed.contains_key(&PartitionKey::Single {
            keys: vec!["b".to_string()]
        }));
    }

    #[tokio::test]
    async fn test_get_failed_partitions_uses_latest_event_per_partition() {
        // Latest event wins: failed-then-materialized clears; materialized-then-failed stays failed.
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        let run = RunRecord {
            run_id: "run_ok".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(2000),
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        let single = |k: &str| PartitionKey::Single {
            keys: vec![k.to_string()],
        };
        let event = |event_type: EventType, pk: &str, ts: i64| EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type,
            asset_key: Some("asset".to_string()),
            run_id: "run_ok".to_string(),
            partition_key: Some(single(pk)),
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        };
        let mat = || EventType::Materialization {
            data_version: Some("v".to_string()),
        };

        // b: fail@1000 then materialize@1500 → cleared.
        storage
            .store_event(&event(EventType::StepFailure, "b", 1000))
            .await
            .unwrap();
        storage.store_event(&event(mat(), "b", 1500)).await.unwrap();
        // c: materialize@1000 then fail@1500 → still failed.
        storage.store_event(&event(mat(), "c", 1000)).await.unwrap();
        storage
            .store_event(&event(EventType::StepFailure, "c", 1500))
            .await
            .unwrap();

        // Supersede uses the caller's materialization map (as the condition cache does).
        let materialized: std::collections::HashMap<_, _> = storage
            .get_partition_timestamps(DEFAULT_CODE_LOCATION_ID, "asset")
            .await
            .unwrap()
            .into_iter()
            .collect();
        let failed = storage
            .get_failed_partitions(DEFAULT_CODE_LOCATION_ID, "asset", &materialized)
            .await
            .unwrap();
        assert_eq!(failed.len(), 1, "only c (latest event = failure)");
        assert!(failed.contains_key(&single("c")));
    }

    #[tokio::test]
    async fn test_get_failed_partitions_ignores_step_level_failures() {
        // A whole-step raise emits a None-keyed StepFailure; it must be excluded
        // (not reported, and not breaking FailRow deserialization).
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        let run = RunRecord {
            run_id: "run_fail".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Failure,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        // step-level failure (a raise) — partition_key None
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset".to_string()),
                run_id: "run_fail".to_string(),
                partition_key: None,
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
        // per-partition failure (a mark) — partition_key Some(b)
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset".to_string()),
                run_id: "run_fail".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["b".to_string()],
                }),
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let failed = storage
            .get_failed_partitions(
                DEFAULT_CODE_LOCATION_ID,
                "asset",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            failed.len(),
            1,
            "only the per-partition failure; None-keyed step-level failure must be excluded"
        );
        assert!(failed.contains_key(&PartitionKey::Single {
            keys: vec!["b".to_string()]
        }));
    }

    #[tokio::test]
    async fn test_get_failed_partitions_expands_set_failure() {
        // A raised batch records one Set-keyed StepFailure; expanded back to members here.
        let storage = make_storage().await;
        register(&storage, &["asset"]).await;

        let run = RunRecord {
            run_id: "run_fail".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Failure,
            start_time: 1000,
            end_time: Some(1500),
            tags: vec![],
            node_names: vec!["asset".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        storage.create_run(&run).await.unwrap();

        let single = |k: &str| PartitionKey::Single {
            keys: vec![k.to_string()],
        };
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset".to_string()),
                run_id: "run_fail".to_string(),
                partition_key: Some(PartitionKey::Set {
                    keys: vec![single("a"), single("b"), single("c")],
                }),
                timestamp: 1000,
                metadata: vec![("error".to_string(), "boom".to_string())],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let failed = storage
            .get_failed_partitions(
                DEFAULT_CODE_LOCATION_ID,
                "asset",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        let mut keys: Vec<String> = failed
            .keys()
            .map(|pk| match pk {
                PartitionKey::Single { keys } => keys[0].clone(),
                _ => panic!("expected Single after expansion"),
            })
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["a", "b", "c"],
            "Set failure expands to its members"
        );
    }

    #[tokio::test]
    async fn test_new_memory_schema_tables() {
        let storage = make_storage().await;

        // Verify all 8 tables exist by querying INFO FOR DB
        let mut result = storage.db.query("INFO FOR DB").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let info = info.unwrap();
        let tables = info["tables"].as_object().unwrap();

        let expected_tables = [
            "events",
            "assets",
            "asset_partitions",
            "runs",
            "kv",
            "dynamic_partitions",
            "ticks",
            "condition_ticks",
            "condition_evals",
        ];
        for table in expected_tables {
            assert!(tables.contains_key(table), "missing table: {table}");
        }
    }

    #[tokio::test]
    async fn test_new_memory_schema_indexes() {
        let storage = make_storage().await;

        // events: per-CL composite indexes — by-asset filters are scoped via
        // `(code_location_id, asset_key[, partition_key])`.
        let mut result = storage.db.query("INFO FOR TABLE events").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_events_run",
            "idx_events_type",
            "idx_events_run_type",
            "idx_events_run_ts",
            "idx_events_loc_asset",
            "idx_events_loc_asset_part",
            "idx_events_loc_asset_type",
            "idx_events_loc_asset_ts",
        ] {
            assert!(indexes.contains_key(idx), "events missing index: {idx}");
        }

        // assets: 3 indexes — composite (loc, key) UNIQUE + composite (loc, group) + (loc).
        let mut result = storage.db.query("INFO FOR TABLE assets").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_assets_loc_key",
            "idx_assets_loc_group",
            "idx_assets_loc",
        ] {
            assert!(indexes.contains_key(idx), "assets missing index: {idx}");
        }

        // runs: 6 indexes
        let mut result = storage.db.query("INFO FOR TABLE runs").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_runs_status",
            "idx_runs_job",
            "idx_runs_id",
            "idx_runs_start_time",
            "idx_runs_priority",
            "idx_runs_job_time",
        ] {
            assert!(indexes.contains_key(idx), "runs missing index: {idx}");
        }

        // kv: 1 index
        let mut result = storage.db.query("INFO FOR TABLE kv").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_kv_key"),
            "kv missing index: idx_kv_key"
        );

        // dynamic_partitions: 2 indexes
        let mut result = storage
            .db
            .query("INFO FOR TABLE dynamic_partitions")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in ["idx_dyn_part", "idx_dyn_part_unique"] {
            assert!(
                indexes.contains_key(idx),
                "dynamic_partitions missing index: {idx}"
            );
        }

        // ticks: 2 composite indexes (keyed per CL).
        let mut result = storage.db.query("INFO FOR TABLE ticks").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in ["idx_ticks_loc_name", "idx_ticks_loc_name_ts"] {
            assert!(indexes.contains_key(idx), "ticks missing index: {idx}");
        }

        // condition_ticks: 1 composite index.
        let mut result = storage
            .db
            .query("INFO FOR TABLE condition_ticks")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_cond_ticks_loc_ts"),
            "condition_ticks missing index: idx_cond_ticks_loc_ts"
        );

        // condition_evals: 3 indexes (2 composite + tick_id).
        let mut result = storage
            .db
            .query("INFO FOR TABLE condition_evals")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        for idx in [
            "idx_cond_evals_loc_key",
            "idx_cond_evals_loc_key_ts",
            "idx_cond_evals_tick",
        ] {
            assert!(
                indexes.contains_key(idx),
                "condition_evals missing index: {idx}"
            );
        }

        // asset_partitions: 1 index
        let mut result = storage
            .db
            .query("INFO FOR TABLE asset_partitions")
            .await
            .unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
        assert!(
            indexes.contains_key("idx_asset_part"),
            "asset_partitions missing index: idx_asset_part"
        );
    }

    #[tokio::test]
    async fn test_new_embedded_schema() {
        let dir = std::env::temp_dir().join(format!("rivers_test_{}", std::process::id()));
        // Clean up from any previous failed run
        let _ = std::fs::remove_dir_all(&dir);

        let storage = SurrealStorage::new_embedded(dir.to_str().unwrap())
            .await
            .unwrap();

        // Verify tables exist by running a simple query on each
        let mut result = storage.db.query("INFO FOR DB").await.unwrap();
        let info: Option<serde_json::Value> = result.take(0).unwrap();
        let tables = info.unwrap()["tables"].as_object().unwrap().clone();

        let expected_tables = [
            "events",
            "assets",
            "asset_partitions",
            "runs",
            "kv",
            "dynamic_partitions",
            "ticks",
            "condition_ticks",
            "condition_evals",
        ];
        for table in expected_tables {
            assert!(tables.contains_key(table), "missing table: {table}");
        }

        // Verify it's functional — write and read back
        storage.kv_set("test_key", b"hello").await.unwrap();
        let val = storage.kv_get("test_key").await.unwrap().unwrap();
        assert_eq!(val, b"hello");

        // Clean up
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_backfill_crud() {
        let storage = make_storage().await;
        let record = BackfillRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "bf-001".to_string(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["my_asset".to_string()],
            job_name: None,
            partition_keys: vec![PartitionKey::Single {
                keys: vec!["2024-01-15".to_string()],
            }],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 4,
            tags: vec![("team".to_string(), "data".to_string())],
            create_time: 1000,
            end_time: None,
            error: None,
        };
        storage.create_backfill(&record).await.unwrap();

        let retrieved = storage.get_backfill("bf-001").await.unwrap();
        assert!(retrieved.is_some());
        let r = retrieved.unwrap();
        assert_eq!(r.backfill_id, "bf-001");
        assert_eq!(r.status, BackfillStatus::Requested);
        assert_eq!(r.partition_keys.len(), 1);
    }

    fn make_backfill(id: &str, status: BackfillStatus, create_time: i64) -> BackfillRecord {
        BackfillRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: id.to_string(),
            status,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["a".to_string()],
            job_name: None,
            partition_keys: vec![],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time,
            end_time: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn test_get_all_backfills_page_pagination_and_filter() {
        let storage = make_storage().await;
        let specs = [
            ("bf0", BackfillStatus::InProgress, 100i64),
            ("bf1", BackfillStatus::CompletedSuccess, 200),
            ("bf2", BackfillStatus::CompletedFailed, 300),
            ("bf3", BackfillStatus::InProgress, 400),
            ("bf4", BackfillStatus::Canceled, 500),
        ];
        for (id, status, ct) in specs {
            storage
                .create_backfill(&make_backfill(id, status, ct))
                .await
                .unwrap();
        }

        // No filter — ordered DESC by create_time.
        let page = storage
            .get_all_backfills_page(0, 3, &BackfillFilter::default())
            .await
            .unwrap();
        assert_eq!(page.total, 5);
        assert_eq!(page.rows.len(), 3);
        assert_eq!(page.rows[0].backfill_id, "bf4");
        assert_eq!(page.rows[2].backfill_id, "bf2");

        // Offset.
        let page = storage
            .get_all_backfills_page(3, 3, &BackfillFilter::default())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0].backfill_id, "bf1");

        // Status filter.
        let filter = BackfillFilter {
            status: Some(BackfillStatus::InProgress),
        };
        let page = storage
            .get_all_backfills_page(0, 10, &filter)
            .await
            .unwrap();
        assert_eq!(page.total, 2);
        assert!(
            page.rows
                .iter()
                .all(|r| r.status == BackfillStatus::InProgress)
        );
    }

    #[tokio::test]
    async fn test_get_all_backfills_summary_counts() {
        let storage = make_storage().await;
        let specs = [
            ("a", BackfillStatus::InProgress),
            ("b", BackfillStatus::InProgress),
            ("c", BackfillStatus::CompletedSuccess),
            ("d", BackfillStatus::CompletedFailed),
            ("e", BackfillStatus::Canceled),
            ("f", BackfillStatus::Requested),
        ];
        for (id, status) in specs {
            storage
                .create_backfill(&make_backfill(id, status, 0))
                .await
                .unwrap();
        }

        let s = storage.get_all_backfills_summary().await.unwrap();
        assert_eq!(s.total, 6);
        assert_eq!(s.in_progress, 2);
        assert_eq!(s.completed_success, 1);
        assert_eq!(s.completed_failed, 1);
        assert_eq!(s.canceled, 1);
    }

    // ── Run queue tests ──

    #[tokio::test]
    async fn test_queued_run_round_trip() {
        let storage = make_storage().await;
        storage
            .create_run(&RunRecord {
                run_id: "q1".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec!["a".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Should appear in get_queued_runs
        let queued = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].run_id, "q1");
        assert_eq!(queued[0].status, RunStatus::Queued);

        // Should NOT count as in-progress
        let in_progress = storage.count_in_progress_runs().await.unwrap();
        assert_eq!(in_progress, 0);
    }

    #[tokio::test]
    async fn test_get_queued_runs_priority_ordering() {
        let storage = make_storage().await;

        // Run A: priority 10, start_time 3000
        storage
            .create_run(&RunRecord {
                run_id: "a".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 3000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 10,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Run B: priority 0, start_time 1000
        storage
            .create_run(&RunRecord {
                run_id: "b".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Run C: priority 10, start_time 1000 (same priority as A, earlier time)
        storage
            .create_run(&RunRecord {
                run_id: "c".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 10,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let mut queued = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(queued.len(), 3);
        // get_queued_runs is unordered — sort by priority DESC, start_time ASC (like coordinator)
        queued.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.start_time.cmp(&b.start_time))
        });
        // Order: C (priority 10, earliest), A (priority 10, later), B (priority 0)
        assert_eq!(queued[0].run_id, "c");
        assert_eq!(queued[1].run_id, "a");
        assert_eq!(queued[2].run_id, "b");
    }

    #[tokio::test]
    async fn test_get_queued_runs_returns_all() {
        let storage = make_storage().await;

        for i in 0..5 {
            storage
                .create_run(&RunRecord {
                    run_id: format!("q{i}"),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some("j".to_string()),
                    status: RunStatus::Queued,
                    start_time: i * 100,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }

        let all = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn test_enqueue_runs_bulk_round_trip() {
        let storage = make_storage().await;

        let records: Vec<RunRecord> = (0..4)
            .map(|i| RunRecord {
                run_id: format!("bulk{i}"),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("jb".to_string()),
                status: RunStatus::Queued,
                start_time: 1000 + i,
                end_time: None,
                tags: vec![("k".to_string(), format!("v{i}"))],
                node_names: vec![format!("asset{i}")],
                priority: i as i32,
                partition_key: Some(PartitionKey::Single {
                    keys: vec![format!("p{i}")],
                }),
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .collect();

        storage.enqueue_runs(&records).await.unwrap();

        let mut all = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(all.len(), 4);
        all.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        let r = &all[2];
        assert_eq!(r.run_id, "bulk2");
        assert_eq!(r.priority, 2);
        assert_eq!(r.job_name.as_deref(), Some("jb"));
        assert_eq!(r.node_names, vec!["asset2".to_string()]);
        assert_eq!(r.tags, vec![("k".to_string(), "v2".to_string())]);
        assert_eq!(
            r.partition_key,
            Some(PartitionKey::Single {
                keys: vec!["p2".to_string()]
            })
        );
    }

    #[tokio::test]
    async fn test_count_in_progress_runs() {
        let storage = make_storage().await;

        let statuses = [
            ("r1", RunStatus::Queued),
            ("r2", RunStatus::NotStarted),
            ("r3", RunStatus::Started),
            ("r4", RunStatus::Success),
            ("r5", RunStatus::Failure),
            ("r6", RunStatus::Canceled),
        ];
        for (id, status) in &statuses {
            storage
                .create_run(&RunRecord {
                    run_id: id.to_string(),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some("j".to_string()),
                    status: status.clone(),
                    start_time: 1000,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }

        // Only NotStarted + Started = 2
        let count = storage.count_in_progress_runs().await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_queued_to_not_started_transition() {
        let storage = make_storage().await;
        storage
            .create_run(&RunRecord {
                run_id: "q1".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 5,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Transition to NotStarted (coordinator dequeued it)
        storage
            .update_run_status("q1", RunStatus::NotStarted, None)
            .await
            .unwrap();

        // No longer in queued runs
        let queued = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(queued.len(), 0);

        // Now counts as in-progress
        let count = storage.count_in_progress_runs().await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_negative_priority_backfill() {
        let storage = make_storage().await;

        // Backfill run with negative priority
        storage
            .create_run(&RunRecord {
                run_id: "backfill".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: -10,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Normal run with default priority
        storage
            .create_run(&RunRecord {
                run_id: "normal".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: 2000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let mut queued = storage.get_all_queued_runs().await.unwrap();
        assert_eq!(queued.len(), 2);
        // get_queued_runs is unordered — sort by priority DESC (like coordinator)
        queued.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.start_time.cmp(&b.start_time))
        });
        // Normal (priority 0) comes before backfill (priority -10)
        assert_eq!(queued[0].run_id, "normal");
        assert_eq!(queued[1].run_id, "backfill");
    }

    // ── Concurrency pool tests ──

    #[tokio::test]
    async fn test_set_and_get_pool_limits() {
        let storage = make_storage().await;

        // Initially empty
        let pools = storage
            .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert!(pools.is_empty());

        // Set a pool
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                10,
                300,
            )
            .await
            .unwrap();
        let pools = storage
            .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].pool_key, "database");
        assert_eq!(pools[0].slot_limit, 10);
        assert_eq!(pools[0].lease_duration_secs, 300);

        // Set another pool
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "api_quota",
                5,
                600,
            )
            .await
            .unwrap();
        let pools = storage
            .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert_eq!(pools.len(), 2);
        assert_eq!(pools[0].pool_key, "api_quota"); // alphabetical order
        assert_eq!(pools[1].pool_key, "database");
    }

    #[tokio::test]
    async fn test_set_pool_limit_upsert() {
        let storage = make_storage().await;

        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                10,
                300,
            )
            .await
            .unwrap();
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                20,
                600,
            )
            .await
            .unwrap();

        let pools = storage
            .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].slot_limit, 20);
        assert_eq!(pools[0].lease_duration_secs, 600);
    }

    #[tokio::test]
    async fn test_get_pool_info_empty() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                10,
                300,
            )
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "database")
            .await
            .unwrap();
        assert_eq!(info.pool_key, "database");
        assert_eq!(info.slot_limit, 10);
        assert_eq!(info.lease_duration_secs, 300);
        assert_eq!(info.claimed_count, 0);
        assert_eq!(info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_get_pool_info_with_slots_and_pending() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "database",
                10,
                300,
            )
            .await
            .unwrap();

        let now_ns = now_nanos();
        let future_ns = now_ns + 600_000_000_000; // 10 min from now
        let past_ns = now_ns - 60_000_000_000; // 1 min ago (expired)

        // Insert active slots via raw queries (bypasses claim_concurrency_slots).
        // Use check_errors() to fully consume each response and avoid stale connection state.
        for (run_id, step_key, slots, expires) in [
            ("run1", "step_a", 2i64, future_ns),
            ("run2", "step_b", 3, future_ns),
            ("run3", "step_c", 1, past_ns), // expired
        ] {
            let resp = storage
                .db
                .query(
                    "INSERT INTO concurrency_slots { \
                         pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                         slots_consumed: $slots, claimed_at: $now, \
                         lease_expires_at: $expires, last_heartbeat: $now \
                     }",
                )
                .bind(("pool_key", "database".to_string()))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("slots", slots))
                .bind(("now", now_ns))
                .bind(("expires", expires))
                .await
                .unwrap();
            resp.check().unwrap();
        }

        // Insert pending step
        let resp = storage
            .db
            .query(
                "INSERT INTO pending_steps { \
                     pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                     priority: 0, enqueued_at: $now, block_reason: 'PoolFull' \
                 }",
            )
            .bind(("pool_key", "database".to_string()))
            .bind(("run_id", "run4".to_string()))
            .bind(("step_key", "step_d".to_string()))
            .bind(("now", now_ns))
            .await
            .unwrap();
        resp.check().unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "database")
            .await
            .unwrap();
        assert_eq!(info.slot_limit, 10);
        // 2 + 3 = 5 (expired slot with slots_consumed=1 should NOT be counted)
        assert_eq!(info.claimed_count, 5);
        assert_eq!(info.pending_count, 1);
    }

    #[tokio::test]
    async fn test_get_pool_info_not_found() {
        let storage = make_storage().await;
        let result = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_asset_record_with_pool() {
        let storage = make_storage().await;

        let record = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tags: vec![],
            kinds: vec!["table".to_string()],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![("database".to_string(), 1), ("api_quota".to_string(), 2)],
        };
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
            .await
            .unwrap();

        let stored = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.pool.len(), 2);
        assert_eq!(stored.pool[0], ("database".to_string(), 1));
        assert_eq!(stored.pool[1], ("api_quota".to_string(), 2));
    }

    #[tokio::test]
    async fn test_asset_pool_preserved_on_re_register() {
        let storage = make_storage().await;

        // Register with pool
        let record = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: Some("v1".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![("database".to_string(), 3)],
        };
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
            .await
            .unwrap();

        // Re-register with different pool config
        let record2 = AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: Some("v2".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![("api".to_string(), 1)],
        };
        storage
            .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record2])
            .await
            .unwrap();

        let stored = storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
            .await
            .unwrap()
            .unwrap();
        // Pool should be updated to new config
        assert_eq!(stored.pool, vec![("api".to_string(), 1)]);
        assert_eq!(stored.code_version, Some("v2".to_string()));
    }

    // ── Claim/Release protocol tests ──

    // This test runs against the RocksDB backend rather than `make_storage()`
    // (kv-mem). The kv-mem implementation (surrealmx) has a known race in its
    // commit-queue conflict check that causes occasional lost updates under
    // concurrent writers — production uses RocksDB, so the test exercises the
    // path that actually has to hold up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sentinel_write_write_conflict() {
        // Prove that concurrent transactions writing the same key are detected:
        // 100 tasks all increment claim_version inside BEGIN/COMMIT.
        // With conflict detection, some will fail. Without retry, the final
        // counter value equals the number of successful commits.
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = std::sync::Arc::new(
            SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
                .await
                .expect("failed to create rocksdb storage"),
        );
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "conflict_test",
                10,
                300,
            )
            .await
            .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(100));
        let conflicts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let successes = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..100 {
            let storage = storage.clone();
            let barrier = barrier.clone();
            let conflicts = conflicts.clone();
            let successes = successes.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = storage
                    .db
                    .query(
                        "BEGIN TRANSACTION; \
                         UPDATE concurrency_pools \
                             SET claim_version = claim_version + 1 \
                             WHERE pool_key = 'conflict_test'; \
                         COMMIT TRANSACTION;",
                    )
                    .await;
                match result {
                    Ok(resp) => match resp.check() {
                        Ok(_) => {
                            successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(_) => {
                            conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let total_conflicts = conflicts.load(std::sync::atomic::Ordering::Relaxed);
        let total_successes = successes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(total_conflicts + total_successes, 100);

        // Final claim_version must equal successful commits (no lost updates).
        let mut result = storage
            .db
            .query(
                "SELECT VALUE claim_version FROM concurrency_pools \
                 WHERE pool_key = 'conflict_test' LIMIT 1",
            )
            .await
            .unwrap();
        let versions: Vec<u32> = result.take(0).unwrap();
        let final_version = versions[0];

        assert_eq!(
            final_version, total_successes,
            "claim_version ({final_version}) must equal successful commits ({total_successes}), \
             conflicts={total_conflicts}"
        );

        // Log the outcome for visibility.
        eprintln!(
            "sentinel test: successes={total_successes}, conflicts={total_conflicts}, \
             claim_version={final_version}"
        );
    }

    #[tokio::test]
    async fn test_claim_check_statement_index() {
        let storage = make_storage().await;
        let pool_names = ["a", "b", "c", "d", "e"];
        for name in &pool_names {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, name, 10, 300)
                .await
                .unwrap();
        }

        for n in 1..=5 {
            let pools: Vec<(String, u32)> =
                pool_names[..n].iter().map(|k| (k.to_string(), 1)).collect();
            let query = SurrealStorage::build_claim_transaction(&pools);
            let now_ns = now_nanos();
            let lease_exp = now_ns + 300_000_000_000i64;

            let mut q = storage.db.query(&query);
            for (i, (pk, _)) in pools.iter().enumerate() {
                q = q.bind((format!("p{i}"), pk.clone()));
            }
            q = q
                .bind(("cl", crate::storage::DEFAULT_CODE_LOCATION_ID.to_string()))
                .bind(("run_id", format!("run_{n}")))
                .bind(("step_key", format!("step_{n}")))
                .bind(("now", now_ns))
                .bind(("lease_exp", lease_exp));

            let mut response = q.await.unwrap().check().unwrap();
            let idx = SurrealStorage::claim_check_statement_index(n);
            let count: Option<u32> = response.take((idx, "total")).unwrap();
            assert_eq!(
                count,
                Some(n as u32),
                "pools={n}: expected {n} slots at statement index {idx}"
            );
        }
    }

    #[tokio::test]
    async fn test_claim_single_pool_success() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 3, 300)
            .await
            .unwrap();

        let status = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();

        assert_eq!(status, ConcurrencyClaimStatus::Claimed);

        // Verify slot was created
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
        assert_eq!(info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_claim_single_pool_full() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Claim the only slot
        let s1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

        // Second claim should be pending
        let s2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        match &s2 {
            ConcurrencyClaimStatus::Pending { reason, .. } => {
                assert!(
                    matches!(reason, BlockReason::PoolFull { pool_key, claimed: 1, limit: 1 } if pool_key == "db")
                );
            }
            ConcurrencyClaimStatus::Claimed => panic!("expected Pending, got Claimed"),
        }

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
        assert_eq!(info.pending_count, 1);
    }

    #[tokio::test]
    async fn test_claim_and_release_then_reclaim() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Claim
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();

        // Release
        storage
            .free_concurrency_slots("run1", "step_a")
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);

        // Reclaim should succeed
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s, ConcurrencyClaimStatus::Claimed);
    }

    #[tokio::test]
    async fn test_claim_weighted_slots() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "gpu", 4, 300)
            .await
            .unwrap();

        // Claim 3 of 4 slots
        let s1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("gpu".to_string(), 3)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

        // Claim 2 more → exceeds (3 + 2 = 5 > 4)
        let s2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("gpu".to_string(), 2)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

        // Claim 1 more → fits (3 + 1 = 4 <= 4)
        let s3 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("gpu".to_string(), 1)],
                "run3",
                "step_c",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s3, ConcurrencyClaimStatus::Claimed);

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "gpu")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 4);
        assert_eq!(info.pending_count, 1);
    }

    #[tokio::test]
    async fn test_claim_multi_pool_all_or_none() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
            .await
            .unwrap();

        // Claim 1 slot in each — should succeed
        let s1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1), ("api".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

        // api is now full (1/1). Claim db+api again — api blocks, so NEITHER should be claimed.
        let s2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1), ("api".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

        // db should still have only 1 claimed (all-or-none: step_b got nothing)
        let db_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(db_info.claimed_count, 1);
        let api_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
            .await
            .unwrap();
        assert_eq!(api_info.claimed_count, 1);
    }

    #[tokio::test]
    async fn test_claim_multi_pool_block_reason_pools_full() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
            .await
            .unwrap();

        // Fill both pools
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "s1",
                0,
                300,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("api".to_string(), 1)],
                "run2",
                "s2",
                0,
                300,
            )
            .await
            .unwrap();

        // Multi-pool claim against both full pools
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1), ("api".to_string(), 1)],
                "run3",
                "s3",
                0,
                300,
            )
            .await
            .unwrap();

        match s {
            ConcurrencyClaimStatus::Pending { reason, .. } => match reason {
                BlockReason::PoolsFull { pools } => {
                    assert_eq!(pools.len(), 2);
                    assert!(pools.iter().any(|p| p.pool_key == "db"));
                    assert!(pools.iter().any(|p| p.pool_key == "api"));
                }
                other => panic!("expected PoolsFull, got {:?}", other),
            },
            ConcurrencyClaimStatus::Claimed => panic!("expected Pending"),
        }
    }

    #[tokio::test]
    async fn test_free_concurrency_slots_for_run() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
            .await
            .unwrap();

        // Claim multiple steps for the same run
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 2)],
                "run1",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 3);

        // Free all for the run
        storage
            .free_concurrency_slots_for_run("run1")
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);
    }

    #[tokio::test]
    async fn test_free_concurrency_slots_for_run_clears_pending() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Fill pool then enqueue a step
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.pending_count, 1);

        // Free all for run1
        storage
            .free_concurrency_slots_for_run("run1")
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);
        assert_eq!(info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_claim_removes_pending_on_success() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Fill pool, making step_b pending
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s, ConcurrencyClaimStatus::Pending { .. }));

        // Free step_a
        storage
            .free_concurrency_slots("run1", "step_a")
            .await
            .unwrap();

        // Now step_b retries and should succeed, removing its pending entry
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s, ConcurrencyClaimStatus::Claimed);

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
        assert_eq!(info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_claim_unconfigured_pool_errors() {
        let storage = make_storage().await;
        let result = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("nonexistent".to_string(), 1)],
                "run1",
                "s1",
                0,
                300,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn test_claim_empty_pools_errors() {
        let storage = make_storage().await;
        let result = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[],
                "run1",
                "s1",
                0,
                300,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_claims_limit_one() {
        // Stress test: 50 tasks claim same pool with limit=1.
        // Exactly 1 should get Claimed, 49 should get Pending.
        let storage = std::sync::Arc::new(make_storage().await);
        storage
            .set_pool_limit(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                "exclusive",
                1,
                300,
            )
            .await
            .unwrap();

        let mut handles = Vec::new();
        for i in 0..50 {
            let storage = storage.clone();
            handles.push(tokio::spawn(async move {
                storage
                    .claim_concurrency_slots(
                        crate::storage::DEFAULT_CODE_LOCATION_ID,
                        &[("exclusive".to_string(), 1)],
                        &format!("run_{i}"),
                        &format!("step_{i}"),
                        0,
                        300,
                    )
                    .await
                    .unwrap()
            }));
        }

        let mut claimed = 0;
        let mut pending = 0;
        for h in handles {
            match h.await.unwrap() {
                ConcurrencyClaimStatus::Claimed => claimed += 1,
                ConcurrencyClaimStatus::Pending { .. } => pending += 1,
            }
        }

        assert_eq!(claimed, 1, "exactly 1 should be claimed");
        assert_eq!(pending, 49, "exactly 49 should be pending");

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "exclusive")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
        assert_eq!(info.pending_count, 49);
    }

    #[tokio::test]
    async fn test_free_step_only_affects_that_step() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
            .await
            .unwrap();

        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 2)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 3)],
                "run1",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();

        // Free only step_a
        storage
            .free_concurrency_slots("run1", "step_a")
            .await
            .unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 3); // only step_b's 3 remain
    }

    #[tokio::test]
    async fn test_claim_multi_pool_weighted() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 3, 300)
            .await
            .unwrap();

        // Claim 2 db slots + 2 api slots
        let s1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 2), ("api".to_string(), 2)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

        // Claim 2 db + 2 api again: api would be 4 > 3
        let s2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 2), ("api".to_string(), 2)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

        // Verify all-or-none: db still at 2, not 4
        let db_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(db_info.claimed_count, 2);
    }

    // ── Lease renewal and expiry ──

    #[tokio::test]
    async fn test_expired_slots_not_counted() {
        // Claim with a very short lease (1 second), wait for expiry,
        // verify the slot is no longer counted as claimed.
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();

        // Claim with 1-second lease
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();
        assert_eq!(s, ConcurrencyClaimStatus::Claimed);

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);

        // Wait for the lease to expire
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // The expired slot should no longer be counted
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0, "expired slot should not be counted");
    }

    #[tokio::test]
    async fn test_expired_slot_frees_capacity() {
        // With limit=1 and an expired lease, a new claim should succeed.
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Fill the pool with a 1-second lease
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();

        // Immediately, a second claim should be Pending
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s, ConcurrencyClaimStatus::Pending { .. }));

        // Wait for the first lease to expire
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Now step_b should be able to claim
        let s = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s, ConcurrencyClaimStatus::Claimed);
    }

    #[tokio::test]
    async fn test_renew_slot_lease() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();

        // Claim with 1-second lease
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();

        // Renew with a long lease before it expires
        let renewed = storage
            .renew_slot_lease("run1", "step_a", 300)
            .await
            .unwrap();
        assert_eq!(renewed, 1, "should renew 1 slot row");

        // Wait past original 1-second lease
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Slot should still be active because we renewed
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(
            info.claimed_count, 1,
            "renewed slot should still be counted"
        );
    }

    #[tokio::test]
    async fn test_renew_multi_pool_lease() {
        // Claim across two pools, renew, verify both renewed.
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 5, 300)
            .await
            .unwrap();

        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1), ("api".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();

        // Renew both
        let renewed = storage
            .renew_slot_lease("run1", "step_a", 300)
            .await
            .unwrap();
        assert_eq!(renewed, 2, "should renew 2 slot rows (one per pool)");

        // Wait past original lease
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let db_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        let api_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
            .await
            .unwrap();
        assert_eq!(db_info.claimed_count, 1);
        assert_eq!(api_info.claimed_count, 1);
    }

    #[tokio::test]
    async fn test_renew_nonexistent_step_returns_zero() {
        let storage = make_storage().await;
        let renewed = storage
            .renew_slot_lease("no_run", "no_step", 300)
            .await
            .unwrap();
        assert_eq!(renewed, 0);
    }

    #[tokio::test]
    async fn test_free_expired_leases() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
            .await
            .unwrap();

        // Claim 3 slots: 2 with 1-second lease, 1 with long lease
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run2",
                "step_b",
                0,
                1,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run3",
                "step_c",
                0,
                300,
            )
            .await
            .unwrap();

        // Wait for the short leases to expire
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // GC expired leases
        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 2, "should free 2 expired slot rows");

        // Only step_c's slot remains
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
    }

    #[tokio::test]
    async fn test_free_expired_leases_none_expired() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
            .await
            .unwrap();

        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();

        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 0, "no expired leases to free");

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
    }

    #[tokio::test]
    async fn test_free_expired_leases_empty() {
        let storage = make_storage().await;
        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 0);
    }

    #[tokio::test]
    async fn test_renewal_prevents_expiry_gc() {
        // Claim with short lease, renew it, run GC — slot should survive.
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();

        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".to_string(), 1)],
                "run1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();

        // Renew with long lease
        storage
            .renew_slot_lease("run1", "step_a", 300)
            .await
            .unwrap();

        // Wait past original lease
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // GC should find nothing expired
        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 0, "renewed slot should not be freed by GC");

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);
    }

    // -----------------------------------------------------------------------
    // Executor integration pattern tests
    // -----------------------------------------------------------------------

    /// Simulates InProcess executor: sequential claim → execute → release cycle.
    /// Three steps sharing a pool with limit=1 should execute one at a time.
    #[tokio::test]
    async fn test_executor_sequential_claim_release() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
            .await
            .unwrap();

        let steps = ["step_a", "step_b", "step_c"];
        for step in &steps {
            // Claim
            let status = storage
                .claim_concurrency_slots(
                    crate::storage::DEFAULT_CODE_LOCATION_ID,
                    &[("api".into(), 1)],
                    "run1",
                    step,
                    0,
                    300,
                )
                .await
                .unwrap();
            assert_eq!(status, ConcurrencyClaimStatus::Claimed);

            // Verify pool is at capacity
            let info = storage
                .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
                .await
                .unwrap();
            assert_eq!(info.claimed_count, 1);

            // Release (simulates step completion)
            storage.free_concurrency_slots("run1", step).await.unwrap();

            let info = storage
                .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
                .await
                .unwrap();
            assert_eq!(info.claimed_count, 0);
        }
    }

    /// Simulates Async executor: concurrent claims with pool limit controlling throughput.
    /// Pool limit=2, 5 concurrent tasks. Exactly 2 should claim, 3 should pend.
    #[tokio::test]
    async fn test_executor_concurrent_claim_limit() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();

        // Claim 2 slots (should succeed)
        let s1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_1",
                0,
                300,
            )
            .await
            .unwrap();
        let s2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_2",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s1, ConcurrencyClaimStatus::Claimed);
        assert_eq!(s2, ConcurrencyClaimStatus::Claimed);

        // 3rd claim should pend
        let s3 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_3",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(s3, ConcurrencyClaimStatus::Pending { .. }));

        // Release step_1, retry step_3 — should now succeed
        storage
            .free_concurrency_slots("run1", "step_1")
            .await
            .unwrap();
        let s3_retry = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_3",
                0,
                300,
            )
            .await
            .unwrap();
        assert_eq!(s3_retry, ConcurrencyClaimStatus::Claimed);
    }

    /// Simulates run-level cleanup: free_concurrency_slots_for_run removes all
    /// slots held by a run (defense-in-depth after execute_plan completes).
    #[tokio::test]
    async fn test_executor_run_level_cleanup() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 5, 300)
            .await
            .unwrap();

        // Claim slots for multiple steps across multiple pools
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1), ("api".into(), 1)],
                "run1",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        // Also enqueue a pending step
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny", 0, 300)
            .await
            .unwrap();
        // Can't claim with limit=0, let's set limit=1 and fill it first
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny", 1, 300)
            .await
            .unwrap();
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("tiny".into(), 1)],
                "run2",
                "other",
                0,
                300,
            )
            .await
            .unwrap();
        let pending = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("tiny".into(), 1)],
                "run1",
                "step_c",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(pending, ConcurrencyClaimStatus::Pending { .. }));

        // Run-level cleanup: free everything for run1
        storage
            .free_concurrency_slots_for_run("run1")
            .await
            .unwrap();

        // All run1 slots should be gone
        let db_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(db_info.claimed_count, 0, "run1 db slots should be freed");
        let api_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
            .await
            .unwrap();
        assert_eq!(api_info.claimed_count, 0, "run1 api slots should be freed");

        // run2's slot should be unaffected
        let tiny_info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny")
            .await
            .unwrap();
        assert_eq!(tiny_info.claimed_count, 1, "run2 tiny slot should remain");
    }

    /// Simulates lease renewal pattern: claim, renew multiple times, verify lease stays alive.
    #[tokio::test]
    async fn test_executor_lease_renewal_pattern() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 10)
            .await
            .unwrap(); // 10s lease

        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "run1",
                "step_a",
                0,
                10,
            )
            .await
            .unwrap();

        // Renew 3 times (simulates renewal interval of lease/3 ≈ 3.3s)
        for _ in 0..3 {
            let renewed = storage
                .renew_slot_lease("run1", "step_a", 10)
                .await
                .unwrap();
            assert_eq!(renewed, 1);
        }

        // Slot should still be active
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);

        // Release
        storage
            .free_concurrency_slots("run1", "step_a")
            .await
            .unwrap();
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);
    }

    /// Coordinator GC pattern: expired leases freed by free_expired_leases during tick.
    #[tokio::test]
    async fn test_coordinator_gc_frees_crashed_slots() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
            .await
            .unwrap();

        // Claim with very short lease (1 nanosecond — already expired)
        let now_ns = now_nanos();
        let expired_lease = now_ns - 1_000_000_000; // 1 second in the past
        storage
            .db
            .query(
                "CREATE concurrency_slots SET \
                 pool_key = 'db', run_id = 'crashed_run', step_key = 'step_x', \
                 slots_consumed = 1, claimed_at = $now, \
                 lease_expires_at = $exp, last_heartbeat = $now",
            )
            .bind(("now", now_ns))
            .bind(("exp", expired_lease))
            .await
            .unwrap();

        // Pool shows 0 claimed (expired excluded from capacity check)
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);

        // GC sweep removes the physical row
        let freed = storage.free_expired_leases().await.unwrap();
        assert_eq!(freed, 1);
    }

    // -----------------------------------------------------------------------
    // Coordinator tick overhead stress test
    // -----------------------------------------------------------------------

    /// Simulates a full coordinator tick cycle at various queue/run sizes and
    /// measures wall-clock time. Run with `cargo test --release coordinator_tick_stress -- --nocapture`
    #[tokio::test]
    async fn coordinator_tick_stress() {
        use std::time::Instant;

        let storage = make_storage().await;

        // Setup: 5 pools with active slots + pending steps
        for pool in ["db", "api", "gpu", "cpu", "net"] {
            storage
                .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, pool, 10, 300)
                .await
                .unwrap();
        }

        // Scenario matrix: (in_progress_runs, queued_runs, active_pool_slots)
        let scenarios: &[(usize, usize, usize)] = &[
            (0, 0, 0),          // idle system
            (5, 10, 20),        // moderate queue
            (10, 50, 50),       // busy system
            (10, 200, 100),     // large queue
            (10, 500, 200),     // 500 queued
            (10, 1_000, 500),   // 1k queued
            (10, 2_000, 500),   // 2k queued
            (10, 5_000, 1000),  // 5k queued
            (10, 10_000, 1000), // 10k queued
        ];

        for &(n_in_progress, n_queued, n_slots) in scenarios {
            // Clean slate per scenario
            storage
                .db
                .query("DELETE FROM runs; DELETE FROM concurrency_slots; DELETE FROM pending_steps")
                .await
                .unwrap();

            let now = now_nanos();
            let lease_exp = now + 300_000_000_000i64; // 5 min from now

            // Create in-progress runs
            for i in 0..n_in_progress {
                storage
                    .create_run(&RunRecord {
                        run_id: format!("ip-{i}"),
                        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                        job_name: Some("bench".into()),
                        status: RunStatus::Started,
                        start_time: now,
                        end_time: None,
                        tags: vec![
                            ("env".into(), "prod".into()),
                            ("team".into(), format!("team-{}", i % 5)),
                        ],
                        node_names: vec![format!("asset_{i}")],
                        priority: 0,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual,
                    })
                    .await
                    .unwrap();
            }

            // Create queued runs
            for i in 0..n_queued {
                storage
                    .create_run(&RunRecord {
                        run_id: format!("q-{i}"),
                        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                        job_name: Some("bench".into()),
                        status: RunStatus::Queued,
                        start_time: now,
                        end_time: None,
                        tags: vec![("env".into(), "staging".into())],
                        node_names: vec![format!("asset_{i}")],
                        priority: i as i32 % 10,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual,
                    })
                    .await
                    .unwrap();
            }

            // Create active pool slots
            for i in 0..n_slots {
                let pool = ["db", "api", "gpu", "cpu", "net"][i % 5];
                storage
                    .db
                    .query(
                        "CREATE concurrency_slots SET \
                     pool_key = $pool, run_id = $rid, step_key = $sk, \
                     slots_consumed = 1, claimed_at = $now, \
                     lease_expires_at = $exp, last_heartbeat = $now",
                    )
                    .bind(("pool", pool))
                    .bind(("rid", format!("ip-{}", i % n_in_progress.max(1))))
                    .bind(("sk", format!("step_{i}")))
                    .bind(("now", now))
                    .bind(("exp", lease_exp))
                    .await
                    .unwrap();
            }

            // Warm up
            let _ = storage
                .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
                .await;

            let n_ticks: usize = if n_queued >= 2000 { 10 } else { 50 };
            let start = Instant::now();
            for _ in 0..n_ticks {
                let _ = storage
                    .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
                    .await
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let per_tick = elapsed / n_ticks as u32;

            eprintln!(
                "  in_progress={n_in_progress:>3}, queued={n_queued:>5}, slots={n_slots:>4} → \
                 {per_tick:>8.3?}/tick ({n_ticks} ticks in {elapsed:.3?})"
            );

            // Assert: each tick must complete well within 250ms
            // No assertion — this test is for observing scaling behavior.
        }
    }

    // ── Observability event tests ──

    #[tokio::test]
    async fn test_concurrency_event_types_roundtrip() {
        let storage = make_storage().await;

        let run_id = "run-events-test";
        storage
            .create_run(&RunRecord {
                run_id: run_id.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("test".into()),
                status: RunStatus::Queued,
                start_time: now_nanos(),
                end_time: None,
                tags: vec![],
                node_names: vec!["asset_a".into()],
                priority: 5,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Store one of each new event type
        let event_types = vec![
            (EventType::RunQueued, vec![("priority".into(), "5".into())]),
            (
                EventType::RunDequeued,
                vec![("priority".into(), "5".into())],
            ),
            (
                EventType::StepSlotClaimed,
                vec![("pools".into(), "db,api".into())],
            ),
            (
                EventType::StepSlotWaiting,
                vec![("reason".into(), "pool 'db' full (5/5)".into())],
            ),
            (EventType::StepSlotRenewed, vec![]),
            (EventType::StepSlotReleased, vec![]),
        ];

        for (evt_type, metadata) in &event_types {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: evt_type.clone(),
                    asset_key: Some("asset_a".into()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: now_nanos(),
                    metadata: metadata.clone(),
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        // Retrieve and verify
        let events = storage.get_events_for_run(run_id).await.unwrap();
        assert_eq!(events.len(), 6, "expected 6 concurrency events");

        let type_names: Vec<&str> = events.iter().map(|e| e.event_type.type_name()).collect();
        assert!(type_names.contains(&"RunQueued"));
        assert!(type_names.contains(&"RunDequeued"));
        assert!(type_names.contains(&"StepSlotClaimed"));
        assert!(type_names.contains(&"StepSlotWaiting"));
        assert!(type_names.contains(&"StepSlotRenewed"));
        assert!(type_names.contains(&"StepSlotReleased"));

        // Verify metadata survives roundtrip
        let claimed = events
            .iter()
            .find(|e| e.event_type.type_name() == "StepSlotClaimed")
            .unwrap();
        assert_eq!(
            claimed
                .metadata
                .iter()
                .find(|(k, _)| k == "pools")
                .map(|(_, v)| v.as_str()),
            Some("db,api")
        );

        let waiting = events
            .iter()
            .find(|e| e.event_type.type_name() == "StepSlotWaiting")
            .unwrap();
        assert_eq!(
            waiting
                .metadata
                .iter()
                .find(|(k, _)| k == "reason")
                .map(|(_, v)| v.as_str()),
            Some("pool 'db' full (5/5)")
        );
    }

    #[tokio::test]
    async fn test_run_block_reason_persistence() {
        let storage = make_storage().await;
        let run_id = "run-block-reason";

        storage
            .create_run(&RunRecord {
                run_id: run_id.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("test".into()),
                status: RunStatus::Queued,
                start_time: now_nanos(),
                end_time: None,
                tags: vec![("env".into(), "prod".into())],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Initially no block reason
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert!(run.block_reason.is_none());

        // Set block reason
        storage
            .update_run_block_reason(run_id, Some("tag limit: env=prod (2/2)"))
            .await
            .unwrap();
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(
            run.block_reason.as_deref(),
            Some("tag limit: env=prod (2/2)")
        );

        // Clear block reason (on dequeue)
        storage.update_run_block_reason(run_id, None).await.unwrap();
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert!(run.block_reason.is_none());
    }

    #[tokio::test]
    async fn test_event_type_from_type_name_roundtrip() {
        // Verify all new event types can roundtrip through type_name / from_type_name
        let types = vec![
            EventType::RunQueued,
            EventType::RunDequeued,
            EventType::StepSlotClaimed,
            EventType::StepSlotWaiting,
            EventType::StepSlotRenewed,
            EventType::StepSlotReleased,
        ];

        for evt in types {
            let name = evt.type_name();
            let reconstructed = EventType::from_type_name(name, None).unwrap();
            assert_eq!(evt, reconstructed, "roundtrip failed for {name}");
        }
    }

    // ── get_pool_slot_holders ──

    #[tokio::test]
    async fn test_get_pool_slot_holders_returns_active_slots() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
            .await
            .unwrap();

        // Claim 2 slots in the "db" pool from different steps
        let status1 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "r1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(status1, ConcurrencyClaimStatus::Claimed));

        let status2 = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 2)],
                "r2",
                "step_b",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(status2, ConcurrencyClaimStatus::Claimed));

        let holders = storage
            .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(holders.len(), 2);

        let h1 = holders.iter().find(|h| h.run_id == "r1").unwrap();
        assert_eq!(h1.step_key, "step_a");
        assert_eq!(h1.slots_consumed, 1);
        assert!(h1.lease_expires_at > h1.claimed_at);

        let h2 = holders.iter().find(|h| h.run_id == "r2").unwrap();
        assert_eq!(h2.step_key, "step_b");
        assert_eq!(h2.slots_consumed, 2);
    }

    #[tokio::test]
    async fn test_get_pool_slot_holders_excludes_expired() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 1)
            .await
            .unwrap();

        // Claim with 1-second lease
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "r1",
                "step_a",
                0,
                1,
            )
            .await
            .unwrap();

        // Wait for expiry
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let holders = storage
            .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert!(holders.is_empty(), "expired slots should not be returned");
    }

    #[tokio::test]
    async fn test_get_pool_slot_holders_empty_pool() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();

        let holders = storage
            .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert!(holders.is_empty());
    }

    // ── get_all_pool_infos ──

    #[tokio::test]
    async fn test_get_all_pool_infos_batched() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
            .await
            .unwrap();
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 10, 300)
            .await
            .unwrap();

        // Claim 2 slots in "db"
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 2)],
                "r1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();

        let infos = storage
            .get_all_pool_infos(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert_eq!(infos.len(), 2);

        let db_info = infos.iter().find(|i| i.pool_key == "api").unwrap();
        assert_eq!(db_info.slot_limit, 10);
        assert_eq!(db_info.claimed_count, 0);
        assert_eq!(db_info.pending_count, 0);

        let api_info = infos.iter().find(|i| i.pool_key == "db").unwrap();
        assert_eq!(api_info.slot_limit, 5);
        assert_eq!(api_info.claimed_count, 2);
        assert_eq!(api_info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_get_all_pool_infos_empty() {
        let storage = make_storage().await;
        let infos = storage
            .get_all_pool_infos(crate::storage::DEFAULT_CODE_LOCATION_ID)
            .await
            .unwrap();
        assert!(infos.is_empty());
    }

    // ── cancel_queued_run ──

    #[tokio::test]
    async fn test_cancel_queued_run_cleans_pending_steps() {
        let storage = make_storage().await;
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
            .await
            .unwrap();

        // Create a queued run and put a step in pending_steps
        storage
            .create_run(&RunRecord {
                run_id: "q1".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        // Fill the pool so the claim goes to pending
        storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "other",
                "blocker",
                0,
                300,
            )
            .await
            .unwrap();
        let status = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("db".into(), 1)],
                "q1",
                "step_a",
                0,
                300,
            )
            .await
            .unwrap();
        assert!(matches!(status, ConcurrencyClaimStatus::Pending { .. }));

        // Verify pending count before cancel
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.pending_count, 1);

        // Cancel the queued run
        let canceled = storage.cancel_queued_run("q1").await.unwrap();
        assert!(canceled);

        // Pending steps should be cleaned up
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
            .await
            .unwrap();
        assert_eq!(info.pending_count, 0);
    }

    #[tokio::test]
    async fn test_cancel_queued_run_transitions_to_canceled() {
        let storage = make_storage().await;
        storage
            .create_run(&RunRecord {
                run_id: "q1".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: Some("global limit".into()),
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let canceled = storage.cancel_queued_run("q1").await.unwrap();
        assert!(canceled);

        let run = storage.get_run("q1").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Canceled);
        assert!(run.end_time.is_some());
    }

    #[tokio::test]
    async fn test_cancel_queued_run_noop_for_non_queued() {
        let storage = make_storage().await;
        storage
            .create_run(&RunRecord {
                run_id: "r1".into(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Started,
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let canceled = storage.cancel_queued_run("r1").await.unwrap();
        assert!(!canceled);

        let run = storage.get_run("r1").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Started);
    }

    #[tokio::test]
    async fn test_cancel_queued_run_not_found() {
        let storage = make_storage().await;
        let canceled = storage.cancel_queued_run("nonexistent").await.unwrap();
        assert!(!canceled);
    }

    // ── Run progress, outcome, cancellation, step events ──

    #[tokio::test]
    async fn test_get_run_progress_empty() {
        let storage = make_storage().await;
        let progress = storage.get_run_progress("no-such-run").await.unwrap();
        assert_eq!(progress.completed_steps, 0);
        assert_eq!(progress.total_steps, 0);
        assert!(progress.last_step_completed_at.is_none());
        assert!(progress.last_completed_step.is_none());
    }

    #[tokio::test]
    async fn test_get_run_progress_counts_steps() {
        let storage = make_storage().await;
        let run_id = "run-progress-1";

        // 3 StepStart events
        for (asset, ts) in [("a", 100), ("b", 200), ("c", 300)] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepStart,
                    asset_key: Some(asset.to_string()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: ts,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        // 2 completed (1 success, 1 failure)
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 150,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("b".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 250,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let progress = storage.get_run_progress(run_id).await.unwrap();
        assert_eq!(progress.total_steps, 3);
        assert_eq!(progress.completed_steps, 2);
        assert_eq!(progress.last_step_completed_at, Some(250));
        assert_eq!(progress.last_completed_step.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn test_get_run_progress_excludes_per_partition_failures() {
        let storage = make_storage().await;
        let run_id = "run-progress-partial";

        // One step: StepStart + StepSuccess.
        for (event_type, ts) in [(EventType::StepStart, 100), (EventType::StepSuccess, 200)] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type,
                    asset_key: Some("a".to_string()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: ts,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        // Per-partition StepFailure events (mark_partition_failed) must NOT count
        // toward step progress, or completed would exceed total.
        for (key, ts) in [("p1", 150), ("p2", 160)] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepFailure,
                    asset_key: Some("a".to_string()),
                    run_id: run_id.to_string(),
                    partition_key: Some(PartitionKey::Single {
                        keys: vec![key.to_string()],
                    }),
                    timestamp: ts,
                    metadata: vec![("error".to_string(), "boom".to_string())],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let progress = storage.get_run_progress(run_id).await.unwrap();
        assert_eq!(progress.total_steps, 1);
        assert_eq!(progress.completed_steps, 1);
        assert_eq!(progress.last_step_completed_at, Some(200));
        assert_eq!(progress.last_completed_step.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn test_run_outcome_roundtrip() {
        let storage = make_storage().await;
        let run_id = "run-outcome-1";

        assert!(storage.get_run_outcome(run_id).await.unwrap().is_none());

        let outcome = RunOutcome::Success {
            completed_steps: 5,
            total_steps: 5,
        };
        storage.set_run_outcome(run_id, &outcome).await.unwrap();

        let retrieved = storage.get_run_outcome(run_id).await.unwrap().unwrap();
        assert_eq!(retrieved, outcome);
    }

    #[tokio::test]
    async fn test_run_outcome_failure() {
        let storage = make_storage().await;
        let outcome = RunOutcome::Failure {
            message: "step X exploded".to_string(),
            completed_steps: 2,
            total_steps: 5,
        };
        storage.set_run_outcome("r1", &outcome).await.unwrap();
        assert_eq!(
            storage.get_run_outcome("r1").await.unwrap().unwrap(),
            outcome
        );
    }

    #[tokio::test]
    async fn test_run_outcome_cancelled() {
        let storage = make_storage().await;
        let outcome = RunOutcome::Cancelled {
            completed_steps: 1,
            total_steps: 3,
        };
        storage.set_run_outcome("r1", &outcome).await.unwrap();
        assert_eq!(
            storage.get_run_outcome("r1").await.unwrap().unwrap(),
            outcome
        );
    }

    #[tokio::test]
    async fn test_run_outcome_overwrite() {
        let storage = make_storage().await;
        let first = RunOutcome::Success {
            completed_steps: 3,
            total_steps: 3,
        };
        storage.set_run_outcome("r1", &first).await.unwrap();

        let second = RunOutcome::Failure {
            message: "oops".to_string(),
            completed_steps: 2,
            total_steps: 3,
        };
        storage.set_run_outcome("r1", &second).await.unwrap();
        assert_eq!(
            storage.get_run_outcome("r1").await.unwrap().unwrap(),
            second
        );
    }

    #[tokio::test]
    async fn test_cancellation_flag() {
        let storage = make_storage().await;
        let run_id = "cancel-test-1";

        assert!(!storage.is_cancelled(run_id).await.unwrap());

        storage.request_cancellation(run_id).await.unwrap();
        assert!(storage.is_cancelled(run_id).await.unwrap());

        // Other runs are unaffected
        assert!(!storage.is_cancelled("other-run").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_events_for_step() {
        let storage = make_storage().await;
        let run_id = "step-events-1";

        // Events for step "asset_a"
        for (etype, ts) in [
            (EventType::StepStart, 100),
            (
                EventType::Materialization {
                    data_version: Some("v1".to_string()),
                },
                150,
            ),
            (EventType::StepSuccess, 200),
        ] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: etype,
                    asset_key: Some("asset_a".to_string()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: ts,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        // Event for different step "asset_b"
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("asset_b".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 300,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let step_a_events = storage
            .get_events_for_step(run_id, "asset_a")
            .await
            .unwrap();
        assert_eq!(step_a_events.len(), 3);
        assert!(matches!(step_a_events[0].event_type, EventType::StepStart));
        assert!(matches!(
            step_a_events[1].event_type,
            EventType::Materialization { .. }
        ));
        assert!(matches!(
            step_a_events[2].event_type,
            EventType::StepSuccess
        ));

        let step_b_events = storage
            .get_events_for_step(run_id, "asset_b")
            .await
            .unwrap();
        assert_eq!(step_b_events.len(), 1);

        let step_c_events = storage
            .get_events_for_step(run_id, "nonexistent")
            .await
            .unwrap();
        assert!(step_c_events.is_empty());
    }

    #[tokio::test]
    async fn test_get_events_for_step_different_runs() {
        let storage = make_storage().await;

        // Same asset in two different runs
        for run_id in ["run-1", "run-2"] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepStart,
                    asset_key: Some("shared_asset".to_string()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: 100,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let run1_events = storage
            .get_events_for_step("run-1", "shared_asset")
            .await
            .unwrap();
        assert_eq!(run1_events.len(), 1);
        assert_eq!(run1_events[0].run_id, "run-1");

        let run2_events = storage
            .get_events_for_step("run-2", "shared_asset")
            .await
            .unwrap();
        assert_eq!(run2_events.len(), 1);
        assert_eq!(run2_events[0].run_id, "run-2");
    }

    #[tokio::test]
    async fn test_get_completed_step_keys_empty() {
        let storage = make_storage().await;
        let keys = storage
            .get_completed_step_keys("no-such-run")
            .await
            .unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_get_completed_step_keys() {
        let storage = make_storage().await;
        let run_id = "run-resume-1";

        for (asset, event_type) in [
            ("a", EventType::StepSuccess),
            ("b", EventType::StepFailure),
            ("c", EventType::StepSuccess),
            ("d", EventType::StepStart),
        ] {
            storage
                .store_event(&EventRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type,
                    asset_key: Some(asset.to_string()),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: 100,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }

        let keys = storage.get_completed_step_keys(run_id).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("a"));
        assert!(keys.contains("c"));
        assert!(!keys.contains("b"));
        assert!(!keys.contains("d"));
    }

    #[tokio::test]
    async fn test_get_step_data_versions_empty() {
        let storage = make_storage().await;
        let dvs = storage.get_step_data_versions("no-such-run").await.unwrap();
        assert!(dvs.is_empty());
    }

    #[tokio::test]
    async fn test_get_step_data_versions() {
        let storage = make_storage().await;
        let run_id = "run-resume-dv-1";

        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v1".to_string()),
                },
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("v2".to_string()),
                },
                asset_key: Some("b".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 200,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Materialization without data_version should be excluded
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("c".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 300,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        // Different run should not appear
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("other".to_string()),
                },
                asset_key: Some("a".to_string()),
                run_id: "other-run".to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let dvs = storage.get_step_data_versions(run_id).await.unwrap();
        assert_eq!(dvs.len(), 2);
        assert_eq!(dvs.get("a").unwrap(), "v1");
        assert_eq!(dvs.get("b").unwrap(), "v2");
    }

    #[tokio::test]
    async fn test_completed_step_keys_ignores_other_runs() {
        let storage = make_storage().await;

        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("x".to_string()),
                run_id: "run-A".to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("y".to_string()),
                run_id: "run-B".to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let keys_a = storage.get_completed_step_keys("run-A").await.unwrap();
        assert_eq!(keys_a.len(), 1);
        assert!(keys_a.contains("x"));

        let keys_b = storage.get_completed_step_keys("run-B").await.unwrap();
        assert_eq!(keys_b.len(), 1);
        assert!(keys_b.contains("y"));
    }

    /// Regression: two CodeLocations writing topology under distinct
    /// identities must not overwrite each other, and the typed scoped
    /// reader returns only the calling CL's blob.
    #[tokio::test]
    async fn graph_topology_isolated_per_code_location() {
        use crate::assets::graph::{NodeKind, TopologyNode};

        let storage = make_storage().await;
        let ctx_a =
            crate::storage::CodeLocationContext::new("11111111-1111-4111-8111-111111111111");
        let ctx_b =
            crate::storage::CodeLocationContext::new("22222222-2222-4222-8222-222222222222");

        let topo_a = GraphTopology {
            nodes: vec![TopologyNode {
                name: "a_only".to_string(),
                kind: NodeKind::Asset,
                group: None,
                parent_graph: None,
            }],
            edges: vec![("a_only".to_string(), "shared".to_string())],
        };
        let topo_b = GraphTopology {
            nodes: vec![TopologyNode {
                name: "b_only".to_string(),
                kind: NodeKind::Asset,
                group: None,
                parent_graph: None,
            }],
            edges: vec![("b_only".to_string(), "shared".to_string())],
        };

        storage
            .for_code_location(&ctx_a)
            .set_graph_topology(&topo_a)
            .await
            .unwrap();
        storage
            .for_code_location(&ctx_b)
            .set_graph_topology(&topo_b)
            .await
            .unwrap();

        let read_a = storage
            .for_code_location(&ctx_a)
            .get_graph_topology()
            .await
            .unwrap()
            .expect("CL-A topology");
        assert_eq!(read_a.nodes.len(), 1);
        assert_eq!(read_a.nodes[0].name, "a_only");

        let read_b = storage
            .for_code_location(&ctx_b)
            .get_graph_topology()
            .await
            .unwrap()
            .expect("CL-B topology");
        assert_eq!(read_b.nodes.len(), 1);
        assert_eq!(read_b.nodes[0].name, "b_only");
    }

    /// `condition_eval_state` is keyed per CL — two daemons sharing a
    /// SurrealDB must round-trip their own state without clobbering each
    /// other's snapshot.
    #[tokio::test]
    async fn condition_eval_state_isolated_per_code_location() {
        use crate::condition::ConditionEvalState;
        let storage = make_storage().await;
        let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
        let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

        let state_a = ConditionEvalState {
            is_initial: false,
            ..Default::default()
        };
        let state_b = ConditionEvalState {
            is_initial: true,
            ..Default::default()
        };

        storage
            .for_code_location(&ctx_a)
            .set_condition_eval_state(&state_a)
            .await
            .unwrap();
        storage
            .for_code_location(&ctx_b)
            .set_condition_eval_state(&state_b)
            .await
            .unwrap();

        let read_a = storage
            .for_code_location(&ctx_a)
            .get_condition_eval_state()
            .await
            .unwrap()
            .expect("CL-A state");
        let read_b = storage
            .for_code_location(&ctx_b)
            .get_condition_eval_state()
            .await
            .unwrap()
            .expect("CL-B state");
        assert!(!read_a.is_initial, "CL-A keeps its own snapshot");
        assert!(read_b.is_initial, "CL-B keeps its own snapshot");
    }

    /// Scoped `get_runs` / `get_queued_runs` / `get_runs_since` only return
    /// runs owned by the calling CL. The unscoped `get_all_*` variants
    /// remain global (UI / CLI views).
    #[tokio::test]
    async fn scoped_run_queries_isolated_per_code_location() {
        let storage = make_storage().await;
        let now = now_nanos();

        // CL-A: 1 queued + 1 success
        for (id, status, ts) in [
            ("a-queued", RunStatus::Queued, now),
            ("a-success", RunStatus::Success, now - 1_000_000),
        ] {
            storage
                .create_run(&RunRecord {
                    run_id: id.to_string(),
                    code_location_id: "cl-a".to_string(),
                    job_name: Some("j".into()),
                    status,
                    start_time: ts,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }
        // CL-B: 1 queued
        storage
            .create_run(&RunRecord {
                run_id: "b-queued".to_string(),
                code_location_id: "cl-b".to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Queued,
                start_time: now,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual,
            })
            .await
            .unwrap();

        let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
        let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

        // get_runs (scoped)
        let runs_a: Vec<String> = storage
            .for_code_location(&ctx_a)
            .get_runs(100, None)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(runs_a.len(), 2);
        assert!(runs_a.iter().all(|id| id.starts_with("a-")));

        // get_queued_runs (scoped)
        let queued_a: Vec<String> = storage
            .for_code_location(&ctx_a)
            .get_queued_runs()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(queued_a, vec!["a-queued".to_string()]);
        let queued_b: Vec<String> = storage
            .for_code_location(&ctx_b)
            .get_queued_runs()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(queued_b, vec!["b-queued".to_string()]);

        // get_runs_since (scoped) with a status filter
        let started_a = storage
            .for_code_location(&ctx_a)
            .get_runs_since(0, Some(RunStatus::Success), crate::storage::SortOrder::Desc)
            .await
            .unwrap();
        assert_eq!(started_a.len(), 1);
        assert_eq!(started_a[0].run_id, "a-success");

        // Unscoped get_all_queued_runs returns both CLs' queued runs.
        let all_queued: Vec<String> = storage
            .get_all_queued_runs()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(all_queued.len(), 2);
    }

    /// The per-CL `get_runs_page` only returns rows for the calling CL,
    /// while the global `get_all_runs_page` returns both.
    #[tokio::test]
    async fn runs_page_isolated_per_code_location() {
        let storage = make_storage().await;
        let now = now_nanos();
        for (id, cl, ts) in [
            ("a-1", "cl-a", now),
            ("a-2", "cl-a", now - 1_000_000),
            ("b-1", "cl-b", now - 500_000),
        ] {
            storage
                .create_run(&RunRecord {
                    run_id: id.to_string(),
                    code_location_id: cl.to_string(),
                    job_name: Some("j".into()),
                    status: RunStatus::Success,
                    start_time: ts,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }

        let page_a = storage
            .get_runs_page("cl-a", 0, 10, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page_a.total, 2);
        assert!(page_a.rows.iter().all(|r| r.run_id.starts_with("a-")));

        let page_b = storage
            .get_runs_page("cl-b", 0, 10, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page_b.total, 1);
        assert_eq!(page_b.rows[0].run_id, "b-1");

        let all = storage
            .get_all_runs_page(0, 10, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(all.total, 3);
    }

    #[tokio::test]
    async fn runs_summary_isolated_per_code_location() {
        let storage = make_storage().await;
        let now = now_nanos();
        let mk = |id: &str, cl: &str, status: RunStatus| RunRecord {
            run_id: id.to_string(),
            code_location_id: cl.to_string(),
            job_name: Some("j".into()),
            status,
            start_time: now,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual,
        };
        // CL-A: 2 success, 1 failure
        for r in [
            mk("a-1", "cl-a", RunStatus::Success),
            mk("a-2", "cl-a", RunStatus::Success),
            mk("a-3", "cl-a", RunStatus::Failure),
        ] {
            storage.create_run(&r).await.unwrap();
        }
        // CL-B: 1 queued
        storage
            .create_run(&mk("b-1", "cl-b", RunStatus::Queued))
            .await
            .unwrap();

        let cutoff = now - 86_400_000_000_000;

        let sum_a = storage.get_runs_summary("cl-a", cutoff).await.unwrap();
        assert_eq!(sum_a.total, 3);
        assert_eq!(sum_a.success, 2);
        assert_eq!(sum_a.failure, 1);
        assert_eq!(sum_a.queued, 0);

        let sum_b = storage.get_runs_summary("cl-b", cutoff).await.unwrap();
        assert_eq!(sum_b.total, 1);
        assert_eq!(sum_b.queued, 1);
        assert_eq!(sum_b.success, 0);

        let all = storage.get_all_runs_summary(cutoff).await.unwrap();
        assert_eq!(all.total, 4);
    }

    #[tokio::test]
    async fn last_run_per_job_isolated_per_code_location() {
        let storage = make_storage().await;
        let now = now_nanos();
        // Same job_name across CLs — the bug we want to catch is one CL's last
        // run leaking into another CL's "latest" view.
        for (id, cl, ts) in [
            ("a-old", "cl-a", now - 10_000_000),
            ("a-new", "cl-a", now),
            ("b-mid", "cl-b", now - 5_000_000),
        ] {
            storage
                .create_run(&RunRecord {
                    run_id: id.to_string(),
                    code_location_id: cl.to_string(),
                    job_name: Some("shared".into()),
                    status: RunStatus::Success,
                    start_time: ts,
                    end_time: None,
                    tags: vec![],
                    node_names: vec![],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual,
                })
                .await
                .unwrap();
        }
        let names = vec!["shared".to_string()];

        let a = storage.get_last_run_per_job("cl-a", &names).await.unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].1.run_id, "a-new");

        let b = storage.get_last_run_per_job("cl-b", &names).await.unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].1.run_id, "b-mid");

        // Global: returns whichever has the most recent start_time.
        let all = storage.get_all_last_run_per_job(&names).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.run_id, "a-new");
    }

    fn mk_isolation_backfill(
        id: &str,
        cl: &str,
        status: BackfillStatus,
        create_time: i64,
    ) -> BackfillRecord {
        BackfillRecord {
            backfill_id: id.to_string(),
            code_location_id: cl.to_string(),
            status,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["a".to_string()],
            job_name: None,
            partition_keys: vec![],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time,
            end_time: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn backfills_page_isolated_per_code_location() {
        let storage = make_storage().await;
        for r in [
            mk_isolation_backfill("a-1", "cl-a", BackfillStatus::Requested, 100),
            mk_isolation_backfill("a-2", "cl-a", BackfillStatus::Requested, 200),
            mk_isolation_backfill("b-1", "cl-b", BackfillStatus::Requested, 300),
        ] {
            storage.create_backfill(&r).await.unwrap();
        }

        let page_a = storage
            .get_backfills_page("cl-a", 0, 10, &BackfillFilter::default())
            .await
            .unwrap();
        assert_eq!(page_a.total, 2);
        assert!(page_a.rows.iter().all(|r| r.backfill_id.starts_with("a-")));

        let page_b = storage
            .get_backfills_page("cl-b", 0, 10, &BackfillFilter::default())
            .await
            .unwrap();
        assert_eq!(page_b.total, 1);

        let all = storage
            .get_all_backfills_page(0, 10, &BackfillFilter::default())
            .await
            .unwrap();
        assert_eq!(all.total, 3);
    }

    #[tokio::test]
    async fn backfills_summary_isolated_per_code_location() {
        let storage = make_storage().await;
        for r in [
            mk_isolation_backfill("a-prog", "cl-a", BackfillStatus::InProgress, 100),
            mk_isolation_backfill("a-done", "cl-a", BackfillStatus::CompletedSuccess, 200),
            mk_isolation_backfill("b-fail", "cl-b", BackfillStatus::CompletedFailed, 300),
        ] {
            storage.create_backfill(&r).await.unwrap();
        }

        let sum_a = storage.get_backfills_summary("cl-a").await.unwrap();
        assert_eq!(sum_a.total, 2);
        assert_eq!(sum_a.in_progress, 1);
        assert_eq!(sum_a.completed_success, 1);
        assert_eq!(sum_a.completed_failed, 0);

        let sum_b = storage.get_backfills_summary("cl-b").await.unwrap();
        assert_eq!(sum_b.total, 1);
        assert_eq!(sum_b.completed_failed, 1);

        let all = storage.get_all_backfills_summary().await.unwrap();
        assert_eq!(all.total, 3);
    }
}
