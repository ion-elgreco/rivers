-- An asset row's provenance (the code version and input versions its data was
-- built from) gets a time of its own. It used to go by the row's data time, so
-- after an action's `materialized()`, which moves the data but writes no
-- provenance, an older real materialization landing late lost its provenance.
DEFINE FIELD IF NOT EXISTS last_provenance_timestamp ON assets TYPE option<int>;

-- A row that already holds provenance got it with the materialization it holds.
UPDATE assets SET last_provenance_timestamp = last_timestamp
WHERE last_provenance_timestamp IS NONE
AND (last_materialization_code_version IS NOT NONE OR last_input_data_versions != []);

-- NOT additive on the write side: a pre-V7 writer records provenance without
-- its time, and a V7 writer would then let an older materialization overwrite
-- it. Old readers stay safe (the fields they read keep their meaning), so
-- min_reader holds at 2.
UPSERT migration_meta:7 CONTENT { version: 7, class: 'breaking', min_reader: 2, min_writer: 7 };
