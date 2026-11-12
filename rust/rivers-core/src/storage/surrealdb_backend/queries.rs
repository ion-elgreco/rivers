/// A whole-asset deletion's clearing, one transaction in `consolidate_deletion`.
/// The data and the provenance each clear by their own time.
pub(super) const CLEAR_DELETED_ASSET: &str = "\
     UPDATE assets SET last_event_id = $event_id, last_run_id = NONE, \
     last_timestamp = NONE, last_data_version = NONE \
     WHERE code_location_id = $cl AND asset_key = $asset_key \
     AND last_timestamp <= $ts AND last_deletion_timestamp <= $ts; \
     UPDATE assets SET last_materialization_code_version = NONE, \
     last_input_data_versions = [], last_provenance_timestamp = NONE \
     WHERE code_location_id = $cl AND asset_key = $asset_key \
     AND last_provenance_timestamp <= $ts; \
     UPDATE assets SET last_deletion_timestamp = IF last_deletion_timestamp > $ts \
     THEN last_deletion_timestamp ELSE $ts END \
     WHERE code_location_id = $cl AND asset_key = $asset_key; \
     DELETE FROM asset_partitions WHERE code_location_id = $cl \
     AND asset_key = $asset_key AND last_timestamp <= $ts;";

/// Roll an `assets` row's data forward to a materialization event.
pub(super) const MATERIALIZE_ASSET_DATA: &str = "UPDATE assets SET last_event_id = $event_id, last_run_id = $run_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key";

/// Roll the row's provenance, the code version and inputs its data was built
/// from, forward to the materialization that recorded them. An action run's
/// `materialized()` writes none: it executed the verb body, not the asset's
/// materialize function, so the pending code-version comparison
/// (`code_version_changed()`, the Stale(Code) badge) must survive it.
pub(super) const MATERIALIZE_ASSET_PROVENANCE: &str = "UPDATE assets SET last_materialization_code_version = $mcv, last_input_data_versions = $idv, last_provenance_timestamp = $provenance_timestamp WHERE code_location_id = $cl AND asset_key = $asset_key";

/// Same, keeping the inputs. An event that consumed no inputs — an action's
/// `ActionResult.materialized()`, a mapped fan-out instance — reports an empty
/// list because it never read upstream, not because upstream is gone. Writing
/// it through would erase the real provenance and leave the asset permanently
/// Stale against every dependency.
pub(super) const MATERIALIZE_ASSET_PROVENANCE_KEEP_IDV: &str = "UPDATE assets SET last_materialization_code_version = $mcv, last_provenance_timestamp = $provenance_timestamp WHERE code_location_id = $cl AND asset_key = $asset_key";

/// Added to the data update: the `assets` row takes the materialization only
/// when it holds no newer materialization and no newer whole-asset deletion.
/// NONE sorts below every number. An `OR` in a WHERE drops the index, so the
/// unset case is not spelled out.
pub(super) const MATERIALIZATION_IS_NEWER: &str =
    "AND last_timestamp <= $timestamp AND last_deletion_timestamp < $timestamp";

/// Added to each provenance update: the same rule by the provenance's own
/// time. A newer action's `materialized()` moves the data only, so an older
/// real materialization landing after it still records its provenance.
pub(super) const PROVENANCE_IS_NEWER: &str = "AND last_provenance_timestamp <= $provenance_timestamp AND last_deletion_timestamp < $provenance_timestamp";

/// Starts `delete_partitions`' transaction: write the `assets` row and put it
/// back. A materialization that adds partition rows writes that row too.
/// Without it, the two write different rows (the tombstone, the partition
/// row), so both commit from snapshots that miss each other, and the older
/// partition row outlives the newer deletion. Now one fails and retries. An
/// UPDATE that changes nothing writes nothing, hence the placeholder.
pub(super) const BEGIN_PARTITION_DELETIONS: &str = "\
     BEGIN TRANSACTION; \
     LET $held = (SELECT VALUE last_event_id FROM assets \
         WHERE code_location_id = $cl AND asset_key = $asset_key)[0]; \
     UPDATE assets SET last_event_id = 'partition deletion' \
         WHERE code_location_id = $cl AND asset_key = $asset_key; \
     UPDATE assets SET last_event_id = $held \
         WHERE code_location_id = $cl AND asset_key = $asset_key; ";

/// Ends `delete_partitions`' transaction: record the tombstones at each
/// key's newest deletion time. The tombstone outlives the Deletion event:
/// failure supersession reads it, and deleting the run must not undo the
/// deletion. A later materialization of the key reads it too.
pub(super) const RECORD_PARTITION_TOMBSTONES: &str = "\
     INSERT INTO asset_partition_deletions $rows ON DUPLICATE KEY UPDATE timestamp = \
     IF $input.timestamp > timestamp THEN $input.timestamp ELSE timestamp END; \
     COMMIT TRANSACTION;";

/// Upsert `asset_partitions` rows on the UNIQUE index — one row per
/// partition, newest event wins. A key deleted at or after its event, alone or
/// with its whole asset, keeps no row. Always rides one transaction with the
/// asset-row update above so the two never disagree under a concurrent
/// whole-asset deletion. `$oldest` is the oldest event time in `$rows`.
///
/// The `assets` row is read before its update: its `last_timestamp` bounds
/// every partition row's, so a batch no older than it takes the plain upsert.
/// Only an older batch pays for the per-row time check. Its row update may
/// write nothing, so it writes the `assets` row and puts it back. A concurrent
/// deletion, of the whole asset or of partitions, writes that row too: one of
/// the two then fails to commit and retries, and the deletion cannot miss this
/// batch's new rows.
///
/// The keys' tombstones are read with an UPDATE that sets nothing, not a
/// SELECT: only UPDATE and DELETE look up the object-valued `partition_key` on
/// the UNIQUE index. A SELECT reads every tombstone of the asset. A DELETE
/// gets that lookup only from a plain param, hence `$deleted_key`.
pub(super) const UPSERT_ASSET_PARTITIONS: &str = "\
     LET $asset = (SELECT last_timestamp, last_deletion_timestamp, last_event_id FROM assets \
         WHERE code_location_id = $cl AND asset_key = $asset_key)[0]; \
     LET $live = IF $asset.last_deletion_timestamp >= $oldest { \
         $rows[WHERE last_timestamp > $asset.last_deletion_timestamp] \
     } ELSE { $rows }; \
     IF $asset.last_timestamp > $oldest { \
         UPDATE assets SET last_event_id = $event_id \
             WHERE code_location_id = $cl AND asset_key = $asset_key; \
         UPDATE assets SET last_event_id = $asset.last_event_id \
             WHERE code_location_id = $cl AND asset_key = $asset_key; \
         INSERT INTO asset_partitions $live ON DUPLICATE KEY UPDATE \
         last_event_id = IF last_timestamp > $input.last_timestamp \
             THEN last_event_id ELSE $input.last_event_id END, \
         last_run_id = IF last_timestamp > $input.last_timestamp \
             THEN last_run_id ELSE $input.last_run_id END, \
         last_timestamp = IF last_timestamp > $input.last_timestamp \
             THEN last_timestamp ELSE $input.last_timestamp END; \
     } ELSE { \
         INSERT INTO asset_partitions $live ON DUPLICATE KEY UPDATE \
         last_event_id = $input.last_event_id, \
         last_run_id = $input.last_run_id, \
         last_timestamp = $input.last_timestamp; \
     }; \
     IF (SELECT VALUE timestamp FROM asset_partition_deletions \
         WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1) { \
         FOR $deletion IN (UPDATE asset_partition_deletions \
             WHERE code_location_id = $cl AND asset_key = $asset_key \
             AND partition_key IN $keys AND timestamp >= $oldest \
             RETURN partition_key, timestamp) { \
             LET $deleted_key = $deletion.partition_key; \
             DELETE FROM asset_partitions WHERE code_location_id = $cl \
             AND asset_key = $asset_key AND partition_key = $deleted_key \
             AND last_timestamp <= $deletion.timestamp; \
         }; \
     }";

/// One page of an asset's events of the types `$types`, newest first. The
/// events are read in time order off `idx_events_loc_asset_ts` until the page
/// is full: few reads when most events of the asset have one of the types.
pub(super) const ASSET_EVENTS_PAGE: &str = "\
     SELECT * FROM events \
     WHERE code_location_id = $cl AND asset_key = $ak AND event_type IN $types \
     ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit START $offset";

/// The same page for one type that few events of the asset have. In time
/// order the engine reads every event of the asset to find them. This reads
/// the events of the type only, then sorts them.
pub(super) const ASSET_EVENTS_PAGE_OF_RARE_TYPE: &str = "\
     SELECT * FROM events WITH INDEX idx_events_loc_asset_type \
     WHERE code_location_id = $cl AND asset_key = $ak AND event_type = $type \
     ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit START $offset";

/// How many events of the type `$type` an asset has, counted on
/// `idx_events_loc_asset_type`. A count over a list of types reads every
/// event of the asset.
pub(super) const COUNT_ASSET_EVENTS_OF_TYPE: &str = "\
     SELECT count() AS total FROM events \
     WHERE code_location_id = $cl AND asset_key = $ak AND event_type = $type GROUP ALL";

/// The materialization time and last run of the partition `$partition` of an
/// asset. Read by key through
/// [`SurrealStorage::select_by_partitions`](super::SurrealStorage).
pub(super) const PARTITION_TIMESTAMPS_OF_KEY: &str = "\
     SELECT partition_key, last_timestamp, last_run_id FROM asset_partitions \
     WHERE code_location_id = $cl AND asset_key = $asset_key \
     AND partition_key = $partition AND last_timestamp IS NOT NONE";

/// The linked runs and the counts per outcome of the backfills `$ids`. `IN` on
/// the leading index column is one index scan per id. The last statement
/// counts the rows per partition of a backfill that ended before V14.
pub(super) const LOAD_BACKFILL_PROGRESS: &str = "\
     SELECT backfill_id, run_id, linked_at FROM backfill_runs \
         WHERE backfill_id IN $ids ORDER BY linked_at; \
     SELECT backfill_id, outcome, math::sum(member_count) AS members, \
         math::sum(failed_members) AS failed_members FROM backfill_run_outcomes \
         WHERE backfill_id IN $ids GROUP BY backfill_id, outcome; \
     SELECT backfill_id, outcome, count() AS members FROM backfill_outcomes \
         WHERE backfill_id IN $ids GROUP BY backfill_id, outcome;";

/// The `backfills` columns kept in their own tables (see [`LOAD_BACKFILL_PROGRESS`]).
pub(super) const BACKFILL_PROGRESS_COLUMNS: &str =
    "['run_ids', 'completed_partitions', 'failed_partitions', 'canceled_partitions']";

/// Link `$links` (`BackfillRunRow`s) to their backfill; a link that exists keeps its place.
pub(super) const LINK_BACKFILL_RUNS: &str =
    "INSERT INTO backfill_runs $links ON DUPLICATE KEY UPDATE linked_at = linked_at";

/// Record `$outcomes` (`BackfillRunOutcomeRow`s) of planned runs that ended
/// before they started; a planned run that has an outcome keeps it.
pub(super) const RECORD_UNSTARTED_BACKFILL_RUNS: &str =
    "INSERT INTO backfill_run_outcomes $outcomes ON DUPLICATE KEY UPDATE outcome = outcome";

/// Inside the transaction that writes the final status of one run: record
/// the outcome of the run when a backfill planned it. `$final` holds the
/// updated row (`run_id`, `status`, `launched_by`), or no row. One
/// transaction, so the count of outcome rows tells when a backfill is
/// complete. A run settled again takes the new outcome. A successful run
/// counts the partitions it failed one by one: the distinct keys of its keyed
/// `StepFailure` events.
///
/// No `FOR` loop: the engine runs the statements of a loop several times slower.
pub(super) const RECORD_BACKFILL_RUN_OUTCOME: &str = "\
     LET $run = $final[0]; \
     LET $link = IF $run.launched_by.kind = 'backfill' \
             AND $run.status IN ['Success', 'Failure', 'Canceled'] { \
         (SELECT run_number, member_count FROM backfill_runs \
             WHERE backfill_id = $run.launched_by.backfill_id AND run_id = $run.run_id)[0] \
     } ELSE { NONE }; \
     LET $members = $link.member_count ?? 0; \
     LET $failed_members = IF $link.run_number != NONE AND $run.status = 'Success' { \
         math::min([$members, array::len(array::distinct( \
             SELECT VALUE partition_key FROM events WHERE run_id = $run.run_id \
             AND event_type = 'StepFailure' AND partition_key IS NOT NONE))]) \
     } ELSE { 0 }; \
     INSERT INTO backfill_run_outcomes (IF $link.run_number != NONE { [{ \
         backfill_id: $run.launched_by.backfill_id, \
         run_number: $link.run_number, \
         outcome: IF $run.status = 'Success' { 'Completed' } \
             ELSE IF $run.status = 'Failure' { 'Failed' } ELSE { 'Canceled' }, \
         member_count: $members, \
         failed_members: $failed_members \
     }] } ELSE { [] }) ON DUPLICATE KEY UPDATE outcome = $input.outcome, \
         member_count = $input.member_count, failed_members = $input.failed_members;";

/// One transaction: set the final status of the `InProgress` backfill `$id`
/// when each of its planned runs has an outcome. The last statement returns
/// the new status, or NONE.
pub(super) const COMPLETE_BACKFILL: &str = "\
     BEGIN TRANSACTION; \
     LET $backfill = (SELECT status, planned_runs FROM backfills WHERE backfill_id = $id)[0]; \
     LET $outcomes = (SELECT outcome, count() AS runs, math::sum(failed_members) AS failed_members \
         FROM backfill_run_outcomes WHERE backfill_id = $id GROUP BY outcome); \
     LET $status = IF $outcomes[WHERE outcome = 'Failed' OR failed_members > 0] != [] { 'CompletedFailed' } \
         ELSE IF $outcomes[WHERE outcome = 'Canceled'] != [] { 'Canceled' } \
         ELSE { 'CompletedSuccess' }; \
     LET $completed = IF $backfill.status = 'InProgress' AND $backfill.planned_runs > 0 \
             AND math::sum($outcomes.runs) >= $backfill.planned_runs { \
         UPDATE backfills SET status = $status, end_time = $now \
             WHERE backfill_id = $id AND status = 'InProgress' RETURN status \
     } ELSE { [] }; \
     COMMIT TRANSACTION; \
     RETURN $completed[0].status;";
