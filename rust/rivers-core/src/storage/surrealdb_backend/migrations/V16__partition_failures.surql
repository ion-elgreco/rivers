-- One row per partition whose latest result is a failure: no materialization
-- and no deletion is at or after `failed_at`. The writers keep the rows: a
-- failure adds one, a materialization or a deletion at or after it removes
-- it. `built_at` is the time of the partition's row in `asset_partitions`,
-- older than the failure; `has_row` is true when there is such a row. Before
-- this version the failures were derived from the events on each read.
DEFINE TABLE IF NOT EXISTS asset_partition_failures SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON asset_partition_failures TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON asset_partition_failures TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON asset_partition_failures TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS failed_at ON asset_partition_failures TYPE int;
DEFINE FIELD IF NOT EXISTS run_id ON asset_partition_failures TYPE string;
DEFINE FIELD IF NOT EXISTS built_at ON asset_partition_failures TYPE option<int>;
DEFINE FIELD IF NOT EXISTS has_row ON asset_partition_failures TYPE bool;
DEFINE INDEX IF NOT EXISTS idx_apf ON asset_partition_failures FIELDS code_location_id, asset_key, partition_key UNIQUE;
-- Counts of the failed partitions with a row in a range of keys.
DEFINE INDEX IF NOT EXISTS idx_apf_row ON asset_partition_failures FIELDS code_location_id, asset_key, has_row, partition_key;
-- The rows of a run, when the run is deleted or does not end in failure.
DEFINE INDEX IF NOT EXISTS idx_apf_run ON asset_partition_failures FIELDS run_id;

-- Breaking for writers: an older writer does not keep the rows. An older
-- reader derives the failures from the events and still works.
UPSERT migration_meta:16 CONTENT { version: 16, class: 'breaking', min_reader: 14, min_writer: 16 };
