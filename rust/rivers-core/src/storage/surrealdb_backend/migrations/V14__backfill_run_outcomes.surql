-- A backfill can cover a million partitions. Its requested keys are stored as
-- given (a key can name many partitions), in pages. What happened is one row
-- per planned run, not one row per partition.

-- `num_partitions`: partitions the requested keys name. `planned_runs`: runs
-- in the plan, set when the backfill starts.
DEFINE FIELD IF NOT EXISTS num_partitions ON backfills TYPE option<int>;
DEFINE FIELD IF NOT EXISTS planned_runs ON backfills TYPE option<int>;

-- The requested keys in pages: a row holds some of the keys, in order.
-- `backfill_keys` held them in one row per backfill. It stays, empty: the
-- engine cannot remove a table in the transaction that defined it, and a new
-- database runs every migration in one transaction.
DEFINE TABLE IF NOT EXISTS backfill_key_pages SCHEMALESS;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfill_key_pages TYPE string;
DEFINE FIELD IF NOT EXISTS page ON backfill_key_pages TYPE int;
DEFINE FIELD IF NOT EXISTS partition_keys ON backfill_key_pages TYPE array;
DEFINE INDEX IF NOT EXISTS idx_backfill_key_pages_key ON backfill_key_pages FIELDS backfill_id, page UNIQUE;

-- A linked run knows its place in the plan and the number of partitions it holds.
DEFINE FIELD IF NOT EXISTS run_number ON backfill_runs TYPE option<int>;
DEFINE FIELD IF NOT EXISTS member_count ON backfill_runs TYPE option<int>;

-- One row per planned run that ended: `outcome` is 'Completed', 'Failed' or
-- 'Canceled'. `failed_members` counts the partitions that failed alone inside
-- a run that succeeded. A planned run that never started has a row too.
-- `backfill_outcomes` keeps the rows of backfills that ended before this
-- version: one row per partition.
DEFINE TABLE IF NOT EXISTS backfill_run_outcomes SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS backfill_id ON backfill_run_outcomes TYPE string;
DEFINE FIELD IF NOT EXISTS run_number ON backfill_run_outcomes TYPE int;
DEFINE FIELD IF NOT EXISTS outcome ON backfill_run_outcomes TYPE string;
DEFINE FIELD IF NOT EXISTS member_count ON backfill_run_outcomes TYPE int;
DEFINE FIELD IF NOT EXISTS failed_members ON backfill_run_outcomes TYPE int;
DEFINE INDEX IF NOT EXISTS idx_backfill_run_outcomes_key ON backfill_run_outcomes FIELDS backfill_id, run_number UNIQUE;

-- The number of partitions that a stored key names.
LET $members = |$key: any| IF $key.variant = 'Multi' {
    math::product(array::map($key.dims ?? [], |$dim: array| array::len($dim[1])))
} ELSE {
    array::len($key.keys ?? [$key])
};

-- The `?? []` guards: an empty SELECT is NONE here, and `FOR ... IN NONE` aborts.
FOR $row IN ((SELECT backfill_id, partition_keys FROM backfill_keys) ?? []) {
    LET $keys = $row.partition_keys ?? [];
    UPDATE backfills SET num_partitions = math::sum(array::map($keys, $members))
        WHERE backfill_id = $row.backfill_id;

    -- An older backfill holds one key per partition. There is always a page 0.
    LET $pages = IF $keys = [] { [[]] } ELSE { array::clump($keys, 4096) };
    INSERT INTO backfill_key_pages array::map($pages, |$page_keys: array, $page: int| {
        backfill_id: $row.backfill_id, page: $page, partition_keys: $page_keys
    });
};
DELETE backfill_keys;

-- A backfill in progress goes on under the new rows. Its runs take their
-- numbers from the order of their links, the runs that ended get their row,
-- and its rows per partition are removed.
FOR $backfill_id IN ((SELECT VALUE backfill_id FROM backfills WHERE status = 'InProgress') ?? []) {
    LET $links = array::map(
        (SELECT VALUE run_id FROM backfill_runs WHERE backfill_id = $backfill_id ORDER BY linked_at) ?? [],
        |$run_id: string, $run_number: int| { run_id: $run_id, run_number: $run_number }
    );

    FOR $link IN $links {
        LET $run = (SELECT status, partition_key FROM runs WHERE run_id = $link.run_id)[0];
        LET $member_count = IF $run.partition_key = NONE { 0 } ELSE { $members($run.partition_key) };

        UPDATE backfill_runs SET run_number = $link.run_number, member_count = $member_count
            WHERE backfill_id = $backfill_id AND run_id = $link.run_id;

        IF $run.status IN ['Success', 'Failure', 'Canceled'] {
            LET $failed_members = IF $run.status = 'Success' {
                math::min([$member_count, array::len(array::distinct(SELECT VALUE partition_key FROM events
                    WHERE run_id = $link.run_id AND event_type = 'StepFailure'
                    AND partition_key IS NOT NONE))])
            } ELSE { 0 };
            CREATE backfill_run_outcomes CONTENT {
                backfill_id: $backfill_id,
                run_number: $link.run_number,
                outcome: IF $run.status = 'Success' { 'Completed' }
                    ELSE IF $run.status = 'Failure' { 'Failed' } ELSE { 'Canceled' },
                member_count: $member_count,
                failed_members: $failed_members
            };
        };
    };

    UPDATE backfills SET planned_runs = array::len($links) WHERE backfill_id = $backfill_id;
    DELETE backfill_outcomes WHERE backfill_id = $backfill_id;
};

-- Breaking both ways: an older reader finds no requested keys past page 0 and
-- no outcome of a run, and an older writer would write rows per partition
-- that no reader counts.
UPSERT migration_meta:14 CONTENT { version: 14, class: 'breaking', min_reader: 14, min_writer: 14 };
