-- A backfill's progress moves out of its `backfills` row. That row held every
-- requested key, run id and settled key in arrays, so each per-partition
-- progress write rewrote a row that grew with the backfill: O(m) per write,
-- O(m^2) per backfill. Now the row holds only the header; the requested keys
-- are one `backfill_keys` row written at create time, and each linked run and
-- each settled key is its own row.
DEFINE TABLE IF NOT EXISTS backfill_keys SCHEMALESS;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfill_keys TYPE string;
DEFINE FIELD IF NOT EXISTS partition_keys ON backfill_keys TYPE array;
DEFINE INDEX IF NOT EXISTS idx_backfill_keys_id ON backfill_keys FIELDS backfill_id UNIQUE;

-- `linked_at` orders `run_ids` as they were linked.
DEFINE TABLE IF NOT EXISTS backfill_runs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfill_runs TYPE string;
DEFINE FIELD IF NOT EXISTS run_id ON backfill_runs TYPE string;
DEFINE FIELD IF NOT EXISTS linked_at ON backfill_runs TYPE int;
DEFINE INDEX IF NOT EXISTS idx_backfill_runs_key ON backfill_runs FIELDS backfill_id, run_id UNIQUE;

-- One row per settled key: `outcome` is 'Completed', 'Failed' or 'Canceled'.
DEFINE TABLE IF NOT EXISTS backfill_outcomes SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfill_outcomes TYPE string;
DEFINE FIELD IF NOT EXISTS partition_key ON backfill_outcomes TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS outcome ON backfill_outcomes TYPE string;
DEFINE INDEX IF NOT EXISTS idx_backfill_outcomes_key ON backfill_outcomes FIELDS backfill_id, partition_key UNIQUE;

-- Move the arrays of existing rows. A key in more than one outcome array keeps
-- the last one written here (Failed over Canceled over Completed). The `?? []`
-- guards: an empty SELECT is NONE here, and `FOR ... IN NONE` aborts.
FOR $b IN ((SELECT backfill_id, partition_keys, run_ids, completed_partitions, failed_partitions, canceled_partitions FROM backfills) ?? []) {
    CREATE backfill_keys CONTENT { backfill_id: $b.backfill_id, partition_keys: $b.partition_keys ?? [] };
    INSERT INTO backfill_runs (array::map(array::distinct($b.run_ids ?? []), |$r: string, $i: int| {
        backfill_id: $b.backfill_id, run_id: $r, linked_at: $i
    }));
    FOR $set IN [
        ['Completed', $b.completed_partitions ?? []],
        ['Canceled', $b.canceled_partitions ?? []],
        ['Failed', $b.failed_partitions ?? []]
    ] {
        INSERT INTO backfill_outcomes (array::map($set[1], |$k: object| {
            backfill_id: $b.backfill_id, partition_key: $k, outcome: $set[0]
        })) ON DUPLICATE KEY UPDATE outcome = $input.outcome;
    };
};

REMOVE FIELD IF EXISTS partition_keys ON backfills;
REMOVE FIELD IF EXISTS run_ids ON backfills;
REMOVE FIELD IF EXISTS completed_partitions ON backfills;
REMOVE FIELD IF EXISTS failed_partitions ON backfills;
REMOVE FIELD IF EXISTS canceled_partitions ON backfills;
UPDATE backfills UNSET partition_keys, run_ids, completed_partitions, failed_partitions, canceled_partitions;

-- Breaking both ways: an older reader finds no progress on the row, and an
-- older writer would write progress to arrays no reader looks at.
UPSERT migration_meta:12 CONTENT { version: 12, class: 'breaking', min_reader: 12, min_writer: 12 };
