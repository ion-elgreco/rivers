-- PostgreSQL mirror of the SurrealDB V1 baseline: same tables, same indexes,
-- same semantics. Both backends must stay at the same schema version, or a
-- database migrated under one and opened under the other would disagree.
--
-- Nanosecond timestamps are bigint, not int. Untyped SurrealQL arrays and
-- FLEXIBLE objects become jsonb; arrays declared `array<string>` become text[].
-- SurrealDB record ids become explicit text primary keys, because the storage
-- API hands ids back to callers as strings.

CREATE TABLE IF NOT EXISTS events (
    id                  text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id    text NOT NULL DEFAULT 'default',
    event_type          text NOT NULL,
    asset_key           text,
    run_id              text NOT NULL,
    partition_key       jsonb,
    timestamp           bigint NOT NULL,
    sort_order          bigint NOT NULL DEFAULT 0,
    metadata            jsonb NOT NULL DEFAULT '[]'::jsonb,
    data_version        text,
    code_version        text,
    input_data_versions jsonb NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_events_run ON events (run_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_run_type ON events (run_id, event_type);
CREATE INDEX IF NOT EXISTS idx_events_run_ts ON events (run_id, timestamp, sort_order);
-- Per-CL by-asset filters; events for the same asset_key under different
-- code locations don't bleed into each other.
CREATE INDEX IF NOT EXISTS idx_events_loc_asset ON events (code_location_id, asset_key);
CREATE INDEX IF NOT EXISTS idx_events_loc_asset_part
    ON events (code_location_id, asset_key, partition_key);
-- Lets get_failed_partitions skip to an asset's failures by event_type.
CREATE INDEX IF NOT EXISTS idx_events_loc_asset_type
    ON events (code_location_id, asset_key, event_type);
-- Timestamp-ordered per-asset event pagination (asset-detail events tab) — scan
-- in order rather than sorting every matching event, mirroring idx_events_run_ts.
CREATE INDEX IF NOT EXISTS idx_events_loc_asset_ts
    ON events (code_location_id, asset_key, timestamp, sort_order);
-- get_latest_materialization filters asset + event_type then takes the newest.
-- The SurrealDB schema lacks this composite, so its planner scans the events
-- table instead; this index is the reason the two schemas are not identical.
CREATE INDEX IF NOT EXISTS idx_events_loc_asset_type_ts
    ON events (code_location_id, asset_key, event_type, timestamp DESC);

CREATE TABLE IF NOT EXISTS assets (
    code_location_id                   text NOT NULL DEFAULT 'default',
    asset_key                          text NOT NULL,
    tags                               text[] NOT NULL DEFAULT '{}',
    kinds                              text[] NOT NULL DEFAULT '{}',
    asset_group                        text,
    code_version                       text,
    last_event_id                      text,
    last_run_id                        text,
    last_timestamp                     bigint,
    last_data_version                  text,
    last_materialization_code_version  text,
    last_input_data_versions           jsonb NOT NULL DEFAULT '[]'::jsonb,
    pool                               jsonb NOT NULL DEFAULT '[]'::jsonb,
    -- Two CLs may register the same `asset_key` independently; uniqueness is
    -- per-CL, not global.
    PRIMARY KEY (code_location_id, asset_key)
);
CREATE INDEX IF NOT EXISTS idx_assets_loc ON assets (code_location_id);
CREATE INDEX IF NOT EXISTS idx_assets_loc_group ON assets (code_location_id, asset_group);

CREATE TABLE IF NOT EXISTS asset_partitions (
    code_location_id  text NOT NULL DEFAULT 'default',
    asset_key         text NOT NULL,
    partition_key     jsonb NOT NULL,
    last_event_id     text,
    last_run_id       text,
    last_timestamp    bigint,
    -- jsonb normalises key order, so a reordered-dims Multi key collapses to
    -- one row without the explicit canonicalisation the SurrealDB backend needs.
    PRIMARY KEY (code_location_id, asset_key, partition_key)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id            text PRIMARY KEY,
    code_location_id  text NOT NULL DEFAULT 'default',
    job_name          text,
    status            text NOT NULL,
    start_time        bigint NOT NULL,
    end_time          bigint,
    tags              jsonb NOT NULL DEFAULT '[]'::jsonb,
    node_names        text[] NOT NULL DEFAULT '{}',
    priority          bigint NOT NULL DEFAULT 0,
    partition_key     jsonb,
    block_reason      text,
    launched_by       jsonb NOT NULL DEFAULT '{"kind": "manual"}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs (status);
CREATE INDEX IF NOT EXISTS idx_runs_job ON runs (job_name);
CREATE INDEX IF NOT EXISTS idx_runs_start_time ON runs (start_time);
CREATE INDEX IF NOT EXISTS idx_runs_priority ON runs (priority);
-- get_all_last_run_per_job does `WHERE job_name = $x ORDER BY start_time DESC
-- LIMIT 1` per job; the composite seeks straight to that job's time-ordered
-- partition instead of sorting or filtering a whole index.
CREATE INDEX IF NOT EXISTS idx_runs_job_time ON runs (job_name, start_time);
-- Queue isolation between code locations sharing one database. The
-- coordinator's tick query filters by code_location_id + status.
CREATE INDEX IF NOT EXISTS idx_runs_loc_status ON runs (code_location_id, status);
-- Scoped get_runs / get_runs_since walk one CL's runs in start_time order.
CREATE INDEX IF NOT EXISTS idx_runs_loc_time ON runs (code_location_id, start_time);

-- General-purpose key/value store (graph topology, etc.)
CREATE TABLE IF NOT EXISTS kv (
    key    text PRIMARY KEY,
    value  bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS dynamic_partitions (
    code_location_id     text NOT NULL DEFAULT 'default',
    partitions_def_name  text NOT NULL,
    partition_key        text NOT NULL,
    create_timestamp     bigint NOT NULL,
    PRIMARY KEY (code_location_id, partitions_def_name, partition_key)
);
CREATE INDEX IF NOT EXISTS idx_dyn_part
    ON dynamic_partitions (code_location_id, partitions_def_name);

CREATE TABLE IF NOT EXISTS ticks (
    id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id  text NOT NULL DEFAULT 'default',
    automation_name   text NOT NULL,
    automation_type   text NOT NULL,
    status            text NOT NULL,
    timestamp         bigint NOT NULL,
    run_ids           text[] NOT NULL DEFAULT '{}',
    backfill_ids      text[] NOT NULL DEFAULT '{}',
    skip_reason       text,
    error             text,
    cursor            text
);
CREATE INDEX IF NOT EXISTS idx_ticks_loc_name ON ticks (code_location_id, automation_name);
CREATE INDEX IF NOT EXISTS idx_ticks_loc_name_ts
    ON ticks (code_location_id, automation_name, timestamp);

CREATE TABLE IF NOT EXISTS condition_ticks (
    id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id  text NOT NULL DEFAULT 'default',
    timestamp         bigint NOT NULL,
    total_evaluated   bigint NOT NULL,
    total_fired       bigint NOT NULL,
    eval_duration_us  bigint NOT NULL,
    run_ids           text[] NOT NULL DEFAULT '{}',
    backfill_ids      text[] NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_cond_ticks_loc_ts ON condition_ticks (code_location_id, timestamp);

CREATE TABLE IF NOT EXISTS condition_evals (
    id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id  text NOT NULL DEFAULT 'default',
    asset_key         text NOT NULL,
    tick_id           text NOT NULL,
    timestamp         bigint NOT NULL,
    fired             boolean NOT NULL,
    eval_duration_us  bigint NOT NULL,
    run_ids           text[] NOT NULL DEFAULT '{}',
    tree_json         bytea NOT NULL,
    selection_json    bytea
);
CREATE INDEX IF NOT EXISTS idx_cond_evals_loc_key ON condition_evals (code_location_id, asset_key);
CREATE INDEX IF NOT EXISTS idx_cond_evals_loc_key_ts
    ON condition_evals (code_location_id, asset_key, timestamp);
CREATE INDEX IF NOT EXISTS idx_cond_evals_tick ON condition_evals (tick_id);

CREATE TABLE IF NOT EXISTS backfills (
    backfill_id           text PRIMARY KEY,
    code_location_id      text NOT NULL DEFAULT 'default',
    status                text NOT NULL,
    strategy              jsonb NOT NULL,
    failure_policy        text NOT NULL,
    asset_selection       text[] NOT NULL DEFAULT '{}',
    partition_keys        jsonb NOT NULL DEFAULT '[]'::jsonb,
    run_ids               jsonb NOT NULL DEFAULT '[]'::jsonb,
    completed_partitions  jsonb NOT NULL DEFAULT '[]'::jsonb,
    failed_partitions     jsonb NOT NULL DEFAULT '[]'::jsonb,
    canceled_partitions   jsonb NOT NULL DEFAULT '[]'::jsonb,
    max_concurrency       bigint NOT NULL,
    tags                  jsonb NOT NULL DEFAULT '[]'::jsonb,
    create_time           bigint NOT NULL,
    end_time              bigint,
    error                 text
);
CREATE INDEX IF NOT EXISTS idx_backfills_loc_status ON backfills (code_location_id, status);

CREATE TABLE IF NOT EXISTS concurrency_pools (
    code_location_id     text NOT NULL DEFAULT 'default',
    pool_key             text NOT NULL,
    slot_limit           bigint NOT NULL,
    lease_duration_secs  bigint NOT NULL DEFAULT 300,
    claim_version        bigint NOT NULL DEFAULT 0,
    -- Pools are per-CL. CL-A's `default` pool is independent of CL-B's.
    PRIMARY KEY (code_location_id, pool_key)
);

CREATE TABLE IF NOT EXISTS concurrency_slots (
    code_location_id  text NOT NULL DEFAULT 'default',
    pool_key          text NOT NULL,
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    slots_consumed    bigint NOT NULL,
    claimed_at        bigint NOT NULL,
    lease_expires_at  bigint NOT NULL,
    last_heartbeat    bigint NOT NULL,
    PRIMARY KEY (code_location_id, pool_key, run_id, step_key)
);
CREATE INDEX IF NOT EXISTS idx_slot_pool ON concurrency_slots (code_location_id, pool_key);
CREATE INDEX IF NOT EXISTS idx_slot_run ON concurrency_slots (run_id);
CREATE INDEX IF NOT EXISTS idx_slot_lease ON concurrency_slots (lease_expires_at);

CREATE TABLE IF NOT EXISTS pending_steps (
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    code_location_id  text NOT NULL DEFAULT 'default',
    pool_key          text NOT NULL,
    priority          bigint NOT NULL,
    enqueued_at       bigint NOT NULL,
    block_reason      text NOT NULL,
    PRIMARY KEY (run_id, step_key)
);
CREATE INDEX IF NOT EXISTS idx_pending_pool ON pending_steps (code_location_id, pool_key);

-- ── migration metadata ──
-- One row per applied migration: its compatibility class and the floors it set.
-- The open guard folds min_reader/min_writer over these rows; refinery's own
-- refinery_schema_history holds version/name/applied_on/checksum.
CREATE TABLE IF NOT EXISTS migration_meta (
    version     bigint PRIMARY KEY,
    class       text NOT NULL,
    min_reader  bigint NOT NULL,
    min_writer  bigint NOT NULL
);

-- v1 is the additive baseline: any build may read and write.
INSERT INTO migration_meta (version, class, min_reader, min_writer)
VALUES (1, 'additive', 1, 1)
ON CONFLICT (version) DO UPDATE
SET class = EXCLUDED.class,
    min_reader = EXCLUDED.min_reader,
    min_writer = EXCLUDED.min_writer;
