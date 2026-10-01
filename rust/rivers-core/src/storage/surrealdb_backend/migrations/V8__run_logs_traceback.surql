-- A failed attempt's structured Python traceback (JSON) lives in its own
-- `run_logs` row: tracebacks are large, and `events` stays free of large
-- payloads (see V2).
DEFINE FIELD IF NOT EXISTS traceback ON run_logs TYPE option<string>;

-- Additive: older readers ignore the extra field, and older writers only
-- leave it out (no traceback shows for their failures). The V7 floors carry
-- forward unchanged.
UPSERT migration_meta:8 CONTENT { version: 8, class: 'additive', min_reader: 2, min_writer: 7 };
