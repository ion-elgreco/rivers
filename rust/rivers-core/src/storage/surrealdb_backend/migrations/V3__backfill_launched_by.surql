-- Backfills gain provenance (`launched_by`, incl. the acting user for
-- manual ones). runs.launched_by is already `object FLEXIBLE`, so the new
-- `user` sub-key on manual runs needs no schema change.
-- Plain `object` (no FLEXIBLE — backfills is SCHEMALESS, where nested keys
-- are already unconstrained; FLEXIBLE is SCHEMAFULL-only).
DEFINE FIELD IF NOT EXISTS launched_by ON backfills TYPE object DEFAULT { kind: 'manual' };

-- Additive: v2 readers ignore the extra field; rows from v2 writers get the
-- DB default at create time and pre-V3 rows fall back to the struct default.
UPSERT migration_meta:3 CONTENT { version: 3, class: 'additive', min_reader: 2, min_writer: 2 };
