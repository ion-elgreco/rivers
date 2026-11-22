CREATE TABLE runs (
    run_id                text PRIMARY KEY,
    code_location_id      text NOT NULL DEFAULT 'default',
    job_name              text,
    status                text NOT NULL,
    start_time            bigint NOT NULL,
    end_time              bigint,
    execution_start_time  bigint,
    tags                  jsonb NOT NULL DEFAULT '[]'::jsonb,
    node_names            text[] NOT NULL DEFAULT '{}',
    priority              integer NOT NULL DEFAULT 0,
    partition_key         jsonb,
    block_reason          text,
    launched_by           jsonb NOT NULL DEFAULT '{"kind": "manual"}'::jsonb,
    action                text,
    config                text
);
-- A list of runs orders by a time and then by `run_id COLLATE "C"`: with
-- the id as the last column, a page is an index scan that stops at its limit.
CREATE INDEX idx_runs_status ON runs (status, start_time, run_id COLLATE "C");
CREATE INDEX idx_runs_job_time ON runs (job_name, start_time, run_id COLLATE "C");
CREATE INDEX idx_runs_start_time ON runs (start_time, run_id COLLATE "C");
CREATE INDEX idx_runs_loc_status ON runs (code_location_id, status, start_time, run_id COLLATE "C");
CREATE INDEX idx_runs_loc_time ON runs (code_location_id, start_time, run_id COLLATE "C");
CREATE INDEX idx_runs_loc_status_end ON runs (code_location_id, status, end_time, run_id COLLATE "C");

-- The final outcome of a run (a `RunOutcome`), written by its executor
-- before it exits.
CREATE TABLE run_outcomes (
    run_id   text PRIMARY KEY,
    outcome  jsonb NOT NULL
);

-- The runs that were asked to stop.
CREATE TABLE run_cancellations (
    run_id  text PRIMARY KEY
);
