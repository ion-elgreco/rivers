-- Backfills gain provenance (`launched_by`, incl. the acting user for manual
-- ones). runs.launched_by is already jsonb, so the new `user` sub-key on
-- manual runs needs no schema change.
ALTER TABLE backfills
    ADD COLUMN IF NOT EXISTS launched_by jsonb NOT NULL DEFAULT '{"kind": "manual"}'::jsonb;

-- Unlike SurrealDB's DEFAULT, ADD COLUMN ... DEFAULT backfills existing rows,
-- so no separate UPDATE is needed here.

-- Additive: v2 readers ignore the extra field; rows from v2 writers get the
-- column default at insert time.
INSERT INTO migration_meta (version, class, min_reader, min_writer)
VALUES (3, 'additive', 2, 2)
ON CONFLICT (version) DO UPDATE
SET class = EXCLUDED.class,
    min_reader = EXCLUDED.min_reader,
    min_writer = EXCLUDED.min_writer;
