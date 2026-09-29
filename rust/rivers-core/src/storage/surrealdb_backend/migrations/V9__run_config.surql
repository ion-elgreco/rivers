-- Runs gain the per-asset config overrides they were launched with: a JSON
-- object keyed by asset name (`{"asset": {"field": value}}`). `option<string>`:
-- absent on pre-V9 rows and on runs launched with the definitions' defaults —
-- both read back as None. The launchers that start from a stored run (run
-- queue, `rivers execute`, backfill children, reruns) read it here, so a
-- queued or Kubernetes run applies the same overrides an in-process one does.
-- `backfills.config` needs no DDL (backfills is SCHEMALESS).
DEFINE FIELD IF NOT EXISTS config ON runs TYPE option<string>;

-- Additive: older readers ignore the extra field, and older writers only
-- leave it out (their runs use the defaults, which None already means). The
-- V8 floors carry forward unchanged.
UPSERT migration_meta:9 CONTENT { version: 9, class: 'additive', min_reader: 2, min_writer: 7 };
