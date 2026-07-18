-- Step logs move out of `events` into a dedicated `run_logs` table: one row
-- per step execution, streams as optional columns. Keeps log payloads off the
-- events table's indexes and out of every structured-events scan.
DEFINE TABLE IF NOT EXISTS run_logs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON run_logs TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS run_id ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON run_logs TYPE int;
DEFINE FIELD IF NOT EXISTS stdout ON run_logs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS stderr ON run_logs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS logs ON run_logs TYPE option<string>;
DEFINE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs FIELDS run_id;

-- Move existing LogOutput events (metadata pairs keyed stdout/stderr/logs)
-- into run_logs, then drop them from events. The `?? []` guard matters: an
-- empty SELECT subquery evaluates to NONE here, and `FOR ... IN NONE` aborts
-- the transaction on fresh stores.
FOR $e IN ((SELECT code_location_id, asset_key, run_id, timestamp, metadata FROM events WHERE event_type = 'LogOutput') ?? []) {
    CREATE run_logs CONTENT {
        code_location_id: $e.code_location_id,
        run_id: $e.run_id,
        step_key: $e.asset_key ?? '',
        timestamp: $e.timestamp,
        stdout: array::filter($e.metadata, |$m: array| $m[0] == 'stdout')[0][1],
        stderr: array::filter($e.metadata, |$m: array| $m[0] == 'stderr')[0][1],
        logs: array::filter($e.metadata, |$m: array| $m[0] == 'logs')[0][1]
    };
};
DELETE events WHERE event_type = 'LogOutput';

-- v2 restructures where logs live: v1 builds would read logs from events
-- (silently empty) and write logs nothing reads — both floors rise.
UPSERT migration_meta:2 CONTENT { version: 2, class: 'breaking', min_reader: 2, min_writer: 2 };
