-- Run-status sensors read the runs of one code location with one status that
-- ended after a time, in end_time order.
DEFINE INDEX IF NOT EXISTS idx_runs_loc_status_end ON runs FIELDS code_location_id, status, end_time;

-- An index only: older readers and writers are unaffected, floors carry forward.
UPSERT migration_meta:11 CONTENT { version: 11, class: 'additive', min_reader: 2, min_writer: 10 };
