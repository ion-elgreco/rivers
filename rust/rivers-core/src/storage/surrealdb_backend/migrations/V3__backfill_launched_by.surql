-- Backfills gain provenance (`launched_by`, incl. the acting user for
-- manual ones). runs.launched_by is already `object FLEXIBLE`, so the new
-- `user` sub-key on manual runs needs no schema change.
-- Plain `object` (no FLEXIBLE — backfills is SCHEMALESS, where nested keys
-- are already unconstrained; FLEXIBLE is SCHEMAFULL-only).
DEFINE FIELD IF NOT EXISTS launched_by ON backfills TYPE object DEFAULT { kind: 'manual' };

-- DEFAULT only fills the field at create time: pre-V3 rows still store NONE
-- and any UPDATE on them (finalize, cancel) would fail field coercion.
-- Backfill the stored value so old rows stay writable.
UPDATE backfills SET launched_by = { kind: 'manual' } WHERE launched_by IS NONE;

-- Additive: v2 readers ignore the extra field; rows from v2 writers get the
-- DB default at create time.
UPSERT migration_meta:3 CONTENT { version: 3, class: 'additive', min_reader: 2, min_writer: 2 };
