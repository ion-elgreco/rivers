-- One row per asset that declares a version of its stored data, with the id
-- [code_location_id, asset_key]: the version of the data, and the state of a
-- migration to the version that the code declares. `revision` goes up by one
-- with each write: a writer that read the row writes it back only if the
-- revision did not change in between.
DEFINE TABLE IF NOT EXISTS asset_versions SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON asset_versions TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON asset_versions TYPE string;
DEFINE FIELD IF NOT EXISTS applied_version ON asset_versions TYPE int;
DEFINE FIELD IF NOT EXISTS partitions ON asset_versions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS state ON asset_versions TYPE string;
DEFINE FIELD IF NOT EXISTS target_version ON asset_versions TYPE option<int>;
DEFINE FIELD IF NOT EXISTS reason ON asset_versions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS started_at ON asset_versions TYPE option<int>;
DEFINE FIELD IF NOT EXISTS run_id ON asset_versions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS backfill_id ON asset_versions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS owner ON asset_versions TYPE option<string>;
DEFINE FIELD IF NOT EXISTS lease_expires_at ON asset_versions TYPE option<int>;
DEFINE FIELD IF NOT EXISTS revision ON asset_versions TYPE int;
DEFINE FIELD IF NOT EXISTS updated_at ON asset_versions TYPE int;
DEFINE INDEX IF NOT EXISTS idx_asset_versions_key ON asset_versions FIELDS code_location_id, asset_key UNIQUE;

-- One row per version that the stored data of an asset reached, with the id
-- [code_location_id, asset_key, version].
DEFINE TABLE IF NOT EXISTS asset_version_history SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON asset_version_history TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON asset_version_history TYPE string;
DEFINE FIELD IF NOT EXISTS version ON asset_version_history TYPE int;
DEFINE FIELD IF NOT EXISTS kind ON asset_version_history TYPE string;
DEFINE FIELD IF NOT EXISTS breaking ON asset_version_history TYPE bool;
DEFINE FIELD IF NOT EXISTS run_id ON asset_version_history TYPE option<string>;
DEFINE FIELD IF NOT EXISTS backfill_id ON asset_version_history TYPE option<string>;
DEFINE FIELD IF NOT EXISTS filled ON asset_version_history TYPE option<int>;
DEFINE FIELD IF NOT EXISTS failed ON asset_version_history TYPE option<int>;
DEFINE FIELD IF NOT EXISTS finished_at ON asset_version_history TYPE int;
DEFINE INDEX IF NOT EXISTS idx_asset_version_history_key ON asset_version_history FIELDS code_location_id, asset_key, version UNIQUE;

-- Additive: no older build reads or writes the tables. The V17 floors carry
-- forward.
UPSERT migration_meta:18 CONTENT { version: 18, class: 'additive', min_reader: 14, min_writer: 17 };
