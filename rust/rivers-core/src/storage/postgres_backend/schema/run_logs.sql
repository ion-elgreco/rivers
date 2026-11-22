-- One row per execution of a step, and one per traceback of a failed attempt.
CREATE TABLE run_logs (
    id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id  text NOT NULL DEFAULT 'default',
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    timestamp         bigint NOT NULL,
    stdout            text,
    stderr            text,
    logs              text,
    traceback         text
);
CREATE INDEX idx_run_logs_run ON run_logs (run_id);

