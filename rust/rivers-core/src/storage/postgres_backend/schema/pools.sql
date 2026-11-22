-- A claim locks the rows of its pools, so claims of one pool take turns.
CREATE TABLE concurrency_pools (
    code_location_id     text NOT NULL DEFAULT 'default',
    pool_key             text NOT NULL,
    slot_limit           integer NOT NULL,
    lease_duration_secs  bigint NOT NULL DEFAULT 300,
    PRIMARY KEY (code_location_id, pool_key)
);

-- `partitions` NULL: the whole asset. Only an asset's implicit pool sets it.
CREATE TABLE concurrency_slots (
    code_location_id  text NOT NULL DEFAULT 'default',
    pool_key          text NOT NULL,
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    slots_consumed    bigint NOT NULL,
    claimed_at        bigint NOT NULL,
    lease_expires_at  bigint NOT NULL,
    last_heartbeat    bigint NOT NULL,
    partitions        text[],
    exclusive         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (code_location_id, pool_key, run_id, step_key)
);
CREATE INDEX idx_slot_run_step ON concurrency_slots (run_id, step_key);
CREATE INDEX idx_slot_lease ON concurrency_slots (lease_expires_at);

CREATE TABLE pending_steps (
    code_location_id  text NOT NULL DEFAULT 'default',
    pool_key          text NOT NULL,
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    priority          integer NOT NULL,
    enqueued_at       bigint NOT NULL,
    block_reason      text NOT NULL,
    PRIMARY KEY (run_id, step_key)
);
CREATE INDEX idx_pending_pool ON pending_steps (code_location_id, pool_key);
