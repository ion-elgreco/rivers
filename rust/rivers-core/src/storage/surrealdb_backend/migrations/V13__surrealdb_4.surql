-- Storage moves to SurrealDB 4: an embedded store is SurrealKV, converted in
-- place from a 3.x RocksDB store, and its records are in the 4.0 format. The
-- indexes are rebuilt before this stamp. A 3.x build can no longer open the
-- store, so both floors rise.
UPSERT migration_meta:13 CONTENT { version: 13, class: 'breaking', min_reader: 13, min_writer: 13 };
