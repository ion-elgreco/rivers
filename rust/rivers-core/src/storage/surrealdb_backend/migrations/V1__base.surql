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

-- General-purpose key/value store (graph topology, etc.)
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

-- ── migration metadata ──
-- One row per applied migration: its compatibility class and the floors it set.
-- The open guard folds min_reader/min_writer over these rows; refinery's own
-- refinery_schema_history holds version/name/applied_on/checksum.
DEFINE TABLE IF NOT EXISTS migration_meta SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS version ON migration_meta TYPE int;
DEFINE FIELD IF NOT EXISTS class ON migration_meta TYPE string;
DEFINE FIELD IF NOT EXISTS min_reader ON migration_meta TYPE int;
DEFINE FIELD IF NOT EXISTS min_writer ON migration_meta TYPE int;

-- v1 is the additive baseline: any build may read and write.
UPSERT migration_meta:1 CONTENT { version: 1, class: 'additive', min_reader: 1, min_writer: 1 };
