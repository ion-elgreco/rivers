CREATE TABLE backfills (
    backfill_id       text PRIMARY KEY,
    code_location_id  text NOT NULL DEFAULT 'default',
    status            text NOT NULL,
    strategy          jsonb NOT NULL,
    failure_policy    text NOT NULL,
    asset_selection   text[] NOT NULL DEFAULT '{}',
    job_name          text,
    num_partitions    bigint NOT NULL DEFAULT 0,
    planned_runs      bigint NOT NULL DEFAULT 0,
    max_concurrency   bigint NOT NULL,
    tags              jsonb NOT NULL DEFAULT '[]'::jsonb,
    create_time       bigint NOT NULL,
    end_time          bigint,
    error             text,
    launched_by       jsonb NOT NULL DEFAULT '{"kind": "manual"}'::jsonb,
    action            text,
    config            text
);
CREATE INDEX idx_backfills_loc_status ON backfills (code_location_id, status);
CREATE INDEX idx_backfills_time ON backfills (create_time, backfill_id COLLATE "C");
CREATE INDEX idx_backfills_loc_time ON backfills (code_location_id, create_time, backfill_id COLLATE "C");

-- The partition keys of a backfill, in pages.
CREATE TABLE backfill_key_pages (
    backfill_id     text NOT NULL,
    page            integer NOT NULL,
    partition_keys  jsonb NOT NULL,
    PRIMARY KEY (backfill_id, page)
);

-- A run number has one run: two drivers of one backfill cannot both start
-- runs for the same partitions.
CREATE TABLE backfill_runs (
    backfill_id   text NOT NULL,
    run_id        text NOT NULL,
    linked_at     bigint NOT NULL,
    run_number    bigint NOT NULL,
    member_count  bigint NOT NULL,
    PRIMARY KEY (backfill_id, run_id),
    UNIQUE (backfill_id, run_number)
);

CREATE TABLE backfill_run_outcomes (
    backfill_id     text NOT NULL,
    run_number      bigint NOT NULL,
    outcome         text NOT NULL,
    member_count    bigint NOT NULL,
    failed_members  bigint NOT NULL,
    PRIMARY KEY (backfill_id, run_number)
);
