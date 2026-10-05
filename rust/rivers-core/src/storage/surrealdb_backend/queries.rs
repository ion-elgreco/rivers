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
/// whole-asset deletion writes that row too: one of the two then fails to
/// commit and retries, and the deletion cannot miss this batch's new rows.
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
