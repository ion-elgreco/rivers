-- Step logs move out of `events` into a dedicated `run_logs` table: one row
-- per step execution, streams as optional columns. Keeps log payloads off the
-- events table's indexes and out of every structured-events scan.
CREATE TABLE IF NOT EXISTS run_logs (
    id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    code_location_id  text NOT NULL DEFAULT 'default',
    run_id            text NOT NULL,
    step_key          text NOT NULL,
    timestamp         bigint NOT NULL,
    stdout            text,
    stderr            text,
    logs              text
);
CREATE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs (run_id);

-- Move existing LogOutput events (metadata pairs keyed stdout/stderr/logs)
-- into run_logs, then drop them from events. `metadata` is a jsonb array of
-- [key, value] pairs, so each stream is the second element of its pair.
INSERT INTO run_logs (code_location_id, run_id, step_key, timestamp, stdout, stderr, logs)
SELECT
    e.code_location_id,
    e.run_id,
    coalesce(e.asset_key, ''),
    e.timestamp,
    (SELECT p ->> 1 FROM jsonb_array_elements(e.metadata) p WHERE p ->> 0 = 'stdout' LIMIT 1),
    (SELECT p ->> 1 FROM jsonb_array_elements(e.metadata) p WHERE p ->> 0 = 'stderr' LIMIT 1),
    (SELECT p ->> 1 FROM jsonb_array_elements(e.metadata) p WHERE p ->> 0 = 'logs' LIMIT 1)
FROM events e
WHERE e.event_type = 'LogOutput';

DELETE FROM events WHERE event_type = 'LogOutput';

-- The change-notification trigger follows its table: `rivers_notify()` is
-- defined in v1, but nothing can trigger on `run_logs` until it exists.
CREATE OR REPLACE TRIGGER run_logs_notify AFTER INSERT OR UPDATE OR DELETE ON run_logs
    FOR EACH STATEMENT EXECUTE FUNCTION rivers_notify();

-- v2 restructures where logs live: v1 builds would read logs from events
-- (silently empty) and write logs nothing reads — both floors rise.
INSERT INTO migration_meta (version, class, min_reader, min_writer)
VALUES (2, 'breaking', 2, 2)
ON CONFLICT (version) DO UPDATE
SET class = EXCLUDED.class,
    min_reader = EXCLUDED.min_reader,
    min_writer = EXCLUDED.min_writer;
