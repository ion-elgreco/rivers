-- Ids are the keys of the record ids that the storage API returns. Lists
-- of ids are jsonb, so a batch store can bind one jsonb[] per column.
CREATE TABLE ticks (
    id                text PRIMARY KEY,
    code_location_id  text NOT NULL DEFAULT 'default',
    automation_name   text NOT NULL,
    automation_type   text NOT NULL,
    status            text NOT NULL,
    timestamp         bigint NOT NULL,
    run_ids           jsonb NOT NULL,
    backfill_ids      jsonb NOT NULL,
    skip_reason       text,
    error             text,
    cursor            text
);
CREATE INDEX idx_ticks_loc_name_ts ON ticks (code_location_id, automation_name, timestamp);

CREATE TABLE condition_ticks (
    id                text PRIMARY KEY,
    code_location_id  text NOT NULL DEFAULT 'default',
    timestamp         bigint NOT NULL,
    total_evaluated   bigint NOT NULL,
    total_fired       bigint NOT NULL,
    eval_duration_us  bigint NOT NULL,
    run_ids           jsonb NOT NULL,
    backfill_ids      jsonb NOT NULL
);
CREATE INDEX idx_cond_ticks_loc_ts ON condition_ticks (code_location_id, timestamp);

-- `tick_id` is the id of the tick as text (`condition_ticks:String("…")`).
CREATE TABLE condition_evals (
    id                text PRIMARY KEY,
    code_location_id  text NOT NULL DEFAULT 'default',
    asset_key         text NOT NULL,
    tick_id           text NOT NULL,
    timestamp         bigint NOT NULL,
    fired             boolean NOT NULL,
    eval_duration_us  bigint NOT NULL,
    run_ids           jsonb NOT NULL,
    tree_json         bytea NOT NULL,
    selection_json    bytea
);
CREATE INDEX idx_cond_evals_loc_key_ts ON condition_evals (code_location_id, asset_key, timestamp);
CREATE INDEX idx_cond_evals_tick ON condition_evals (tick_id);
