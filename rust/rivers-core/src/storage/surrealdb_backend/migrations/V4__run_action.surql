-- Runs gain the verb they execute. `option<string>`:
-- absent on pre-V4 rows and on materialize runs — both read back as None, and
-- option-typed fields stay writable on old rows without a stored-value
-- backfill. `backfills.action` needs no DDL (backfills is SCHEMALESS).
DEFINE FIELD IF NOT EXISTS action ON runs TYPE option<string>;

-- Additive: older readers ignore the extra field; runs from older writers
-- simply never carry a verb (materialize runs, which None already means) —
-- the V2 floors carry forward unchanged.
UPSERT migration_meta:4 CONTENT { version: 4, class: 'additive', min_reader: 2, min_writer: 2 };
