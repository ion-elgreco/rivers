-- `config` on runs and backfills becomes the launch document:
-- `{"assets": {"<asset>": {"config": {...}, "metadata": {...}}}}` (see V9 for
-- the field). No DDL changes; the rows written before this version are
-- rewritten from the old `{"<asset>": {...}}` shape by the migrator itself,
-- in Rust, before this stamp lands (SurrealQL cannot decode JSON text).

-- Breaking for writers: a V9 code location would read `assets` as an asset
-- name and apply nothing. A reader only shows the text, so `min_reader`
-- carries forward.
UPSERT migration_meta:10 CONTENT { version: 10, class: 'breaking', min_reader: 2, min_writer: 10 };
