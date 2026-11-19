-- One row per asset, with the id [code_location_id, asset_key]. A deletion
-- and a write of failure rows of the asset change the row. A materialization
-- of partitions reads it FOR UPDATE: it then fails to commit when the row
-- changed after it began, and materializations do not stop each other. An
-- asset without such a change has no row.
DEFINE TABLE IF NOT EXISTS asset_holds SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS changes ON asset_holds TYPE int;

-- Breaking for writers: an older writer neither reads nor changes the row.
UPSERT migration_meta:17 CONTENT { version: 17, class: 'breaking', min_reader: 14, min_writer: 17 };
