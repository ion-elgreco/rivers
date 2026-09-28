-- A deletion becomes a durable fact on storage rows, like a materialization
-- already is. Failure supersession used to read Deletion events only, so
-- deleting the run that emitted them resurrected every failure the delete
-- had cleared, while the asset's cleared state stayed.
--
-- Whole asset: the newest deletion time rides the asset row.
DEFINE FIELD IF NOT EXISTS last_deletion_timestamp ON assets TYPE option<int>;

-- Partitions: a deleted partition has no `asset_partitions` row left to carry
-- it, so its newest deletion time lives in a tombstone row of its own.
DEFINE TABLE IF NOT EXISTS asset_partition_deletions SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON asset_partition_deletions TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS asset_key ON asset_partition_deletions TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON asset_partition_deletions TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS timestamp ON asset_partition_deletions TYPE int;
DEFINE INDEX IF NOT EXISTS idx_asset_part_del ON asset_partition_deletions FIELDS code_location_id, asset_key, partition_key UNIQUE;

-- Carry the deletions already recorded as events over to the new rows.
-- `[0]` of no rows is NONE; `math::max` of no rows is -Infinity, not an int.
UPDATE assets SET last_deletion_timestamp = (
    SELECT VALUE timestamp FROM events
    WHERE code_location_id = $parent.code_location_id AND asset_key = $parent.asset_key
    AND event_type = 'Deletion' AND partition_key IS NONE
    ORDER BY timestamp DESC LIMIT 1
)[0];
INSERT INTO asset_partition_deletions (
    SELECT code_location_id, asset_key, partition_key, math::max(timestamp) AS timestamp
    FROM events
    WHERE event_type = 'Deletion' AND partition_key IS NOT NONE AND asset_key IS NOT NONE
    GROUP BY code_location_id, asset_key, partition_key
) ON DUPLICATE KEY UPDATE timestamp = $input.timestamp;

-- NOT additive on the write side: a pre-V6 writer records a deletion without
-- the rows, and V6 readers no longer look at events for it. Old readers stay
-- safe (Deletion events are still written), so min_reader holds at 2.
UPSERT migration_meta:6 CONTENT { version: 6, class: 'breaking', min_reader: 2, min_writer: 6 };
