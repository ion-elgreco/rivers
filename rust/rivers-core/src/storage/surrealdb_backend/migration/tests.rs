use super::super::*;
use super::*;
use crate::storage::{
    BackfillRecord, BackfillRunOutcome, BackfillStatus, PartitionKey, RunStatus,
    UnstartedBackfillRun,
};

async fn count_rows(db: &Surreal<Any>, table: &str) -> u64 {
    let mut r = db
        .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
        .await
        .unwrap();
    let n: Option<u64> = r.take((0, "n")).unwrap();
    n.unwrap_or(0)
}

/// The floors migration `version` recorded, whatever was applied after it.
async fn stamps_at(db: &Surreal<Any>, version: u32) -> SchemaStamps {
    #[derive(SurrealValue)]
    struct Row {
        min_reader: i64,
        min_writer: i64,
    }
    let rows: Vec<Row> = db
        .query(format!(
            "SELECT min_reader, min_writer FROM {MIGRATION_META_TABLE} WHERE version = $v"
        ))
        .bind(("v", version))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    let row = rows.into_iter().next().expect("migration_meta row");
    SchemaStamps {
        version,
        min_reader: row.min_reader as u32,
        min_writer: row.min_writer as u32,
    }
}

/// Plant a `migration_meta` row so the open guard reads the given stamps. The
/// fold takes the max, so values dominating any existing row set the result.
async fn plant_stamps(db: &Surreal<Any>, version: u32, min_reader: u32, min_writer: u32) {
    db.query(format!(
            "UPSERT migration_meta:{version} CONTENT \
             {{ version: {version}, class: 'planted', min_reader: {min_reader}, min_writer: {min_writer} }}"
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
}

/// `SCHEMA_VERSION` must equal the highest embedded migration — the guard
/// compares the DB's applied version against it.
#[test]
fn test_schema_version_matches_embedded() {
    let max = embedded_migrations()
        .iter()
        .map(|m| m.version() as u64)
        .max()
        .expect("at least one migration");
    assert_eq!(
        max, SCHEMA_VERSION as u64,
        "SCHEMA_VERSION must track the highest embedded migration"
    );
}

/// The refinery backend applies a migration end-to-end through refinery's
/// real `migrate()` loop: schema lands, refinery records it, the
/// `migration_meta` floor row is written, and a re-run is idempotent.
#[tokio::test]
async fn test_refinery_backend_applies_and_is_idempotent() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();

    const V1: &str = "\
            DEFINE TABLE probe_assets SCHEMAFULL; \
            DEFINE FIELD name ON probe_assets TYPE string; \
            DEFINE TABLE migration_meta SCHEMAFULL; \
            DEFINE FIELD version ON migration_meta TYPE int; \
            DEFINE FIELD class ON migration_meta TYPE string; \
            DEFINE FIELD min_reader ON migration_meta TYPE int; \
            DEFINE FIELD min_writer ON migration_meta TYPE int; \
            UPSERT migration_meta:1 CONTENT { version: 1, class: 'additive', min_reader: 1, min_writer: 1 };";

    let migrations = vec![Migration::unapplied("V1__base", V1).unwrap()];
    let mut backend = SurrealMigrate { db: db.clone() };

    backend
        .migrate(
            &migrations,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("first migrate");

    assert_eq!(
        count_rows(&db, REFINERY_HISTORY_TABLE).await,
        1,
        "one history row"
    );
    assert_eq!(
        count_rows(&db, "probe_assets").await,
        0,
        "schema defined (empty)"
    );
    assert_eq!(count_rows(&db, "migration_meta").await, 1, "one floor row");

    backend
        .migrate(
            &migrations,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("second migrate");
    assert_eq!(
        count_rows(&db, REFINERY_HISTORY_TABLE).await,
        1,
        "idempotent: still one history row"
    );
}

/// V2 moves V1-shape `LogOutput` events (metadata pairs) into `run_logs`
/// rows and deletes them from `events`; structured events stay put.
#[tokio::test]
async fn test_v2_moves_logoutput_events_to_run_logs() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();

    let v1_only: Vec<Migration> = embedded_migrations().into_iter().take(1).collect();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &v1_only,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply V1");

    db.query(
            "INSERT INTO events [
                { code_location_id: 'default', event_type: 'LogOutput', asset_key: 'asset_a', \
                  run_id: 'run-1', timestamp: 10, sort_order: 1, \
                  metadata: [['stdout', 'hello out'], ['logs', 'rust line']], input_data_versions: [] },
                { code_location_id: 'default', event_type: 'LogOutput', asset_key: 'asset_b', \
                  run_id: 'run-1', timestamp: 11, sort_order: 1, \
                  metadata: [['stderr', 'oh no']], input_data_versions: [] },
                { code_location_id: 'default', event_type: 'StepSuccess', asset_key: 'asset_a', \
                  run_id: 'run-1', timestamp: 12, sort_order: 4, metadata: [], input_data_versions: [] }
            ] RETURN NONE",
        )
        .await
        .unwrap()
        .check()
        .unwrap();

    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply V2");

    #[derive(SurrealValue)]
    struct LogRow {
        step_key: String,
        run_id: String,
        timestamp: i64,
        stdout: Option<String>,
        stderr: Option<String>,
        logs: Option<String>,
    }
    let rows: Vec<LogRow> = db
        .query("SELECT * FROM run_logs ORDER BY timestamp ASC")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(rows.len(), 2, "one run_logs row per LogOutput event");
    assert_eq!(rows[0].step_key, "asset_a");
    assert_eq!(rows[0].run_id, "run-1");
    assert_eq!(rows[0].timestamp, 10);
    assert_eq!(rows[0].stdout.as_deref(), Some("hello out"));
    assert_eq!(rows[0].stderr, None);
    assert_eq!(rows[0].logs.as_deref(), Some("rust line"));
    assert_eq!(rows[1].step_key, "asset_b");
    assert_eq!(rows[1].stdout, None);
    assert_eq!(rows[1].stderr.as_deref(), Some("oh no"));

    assert_eq!(
        count_rows(&db, "events").await,
        1,
        "only the structured event remains"
    );
    let stamps = stamps_at(&db, 11).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (11, 2, 10),
        "v2 raised the read floor to 2; v7 raised the write floor to 7 (v10 to 10), because a \
             pre-v7 writer records provenance without the time a v7 writer compares"
    );
}

/// A backfill written under V2 (before `launched_by` was defined) has no
/// such key in storage. After migrating to V3 it must read back (struct
/// default) AND stay writable: DEFAULT only fills the field at create
/// time, so V3 also backfills the stored NONE — without that, any UPDATE
/// on a legacy row trips field coercion.
#[tokio::test]
async fn test_v3_backfill_launched_by_defaults_for_legacy_rows() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();

    // Apply V1+V2 only — `launched_by` on backfills doesn't exist yet.
    let pre_v3: Vec<Migration> = embedded_migrations().into_iter().take(2).collect();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &pre_v3,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply V1+V2");

    // A legacy backfill row: no launched_by key.
    db.query(
        "INSERT INTO backfills { backfill_id: 'bf-legacy', code_location_id: 'default', \
             status: 'Requested', strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', \
             asset_selection: [], partition_keys: [], run_ids: [], completed_partitions: [], \
             failed_partitions: [], canceled_partitions: [], max_concurrency: 1, tags: [], \
             create_time: 1, end_time: NONE, error: NONE } RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    // Now migrate to V3 (adds the launched_by field) and read the old row.
    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply V3");
    let rows: Vec<crate::storage::BackfillRecord> = db
        .query("SELECT * FROM backfills WHERE backfill_id = 'bf-legacy'")
        .await
        .unwrap()
        .take(0)
        .expect("legacy backfill row must deserialize after V3");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].launched_by,
        crate::storage::LaunchedBy::Manual { user: None },
        "a legacy row's missing launched_by falls back to the struct default"
    );

    db.query("UPDATE backfills SET status = 'Canceled' WHERE backfill_id = 'bf-legacy'")
        .await
        .unwrap()
        .check()
        .expect("legacy backfill row must be writable after V3");
}

/// V6 moves deletion times onto storage rows. Deletions recorded before it
/// exist only as events and must carry over, or they stop superseding
/// the failures they cleared.
#[tokio::test]
async fn test_v6_backfills_deletion_tombstones_from_events() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let pre_v6: Vec<Migration> = embedded_migrations().into_iter().take(5).collect();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &pre_v6,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply V1..V5");

    db.query(
            "INSERT INTO assets { code_location_id: 'default', asset_key: 'report', tags: [], kinds: [] }; \
             INSERT INTO assets { code_location_id: 'default', asset_key: 'kept', tags: [], kinds: [] }; \
             INSERT INTO events [ \
               { code_location_id: 'default', event_type: 'Deletion', asset_key: 'report', \
                 run_id: 'r1', timestamp: 2000, metadata: [] }, \
               { code_location_id: 'default', event_type: 'Deletion', asset_key: 'report', \
                 run_id: 'r2', timestamp: 3000, metadata: [] }, \
               { code_location_id: 'default', event_type: 'Deletion', asset_key: 'events', \
                 run_id: 'r3', partition_key: { Single: { keys: ['p1'] } }, timestamp: 4000, metadata: [] } \
             ];",
        )
        .await
        .unwrap()
        .check()
        .unwrap();

    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply V6");

    #[derive(Debug, SurrealValue)]
    struct AssetRow {
        asset_key: String,
        last_deletion_timestamp: Option<i64>,
    }
    #[derive(Debug, SurrealValue)]
    struct TombRow {
        asset_key: String,
        timestamp: i64,
    }
    let mut result = db
        .query(
            "SELECT asset_key, last_deletion_timestamp FROM assets ORDER BY asset_key; \
                 SELECT asset_key, timestamp FROM asset_partition_deletions;",
        )
        .await
        .unwrap();
    let assets: Vec<AssetRow> = result.take(0).unwrap();
    let tombs: Vec<TombRow> = result.take(1).unwrap();
    let by_key: std::collections::HashMap<String, Option<i64>> = assets
        .into_iter()
        .map(|r| (r.asset_key, r.last_deletion_timestamp))
        .collect();
    assert_eq!(
        by_key["report"],
        Some(3000),
        "the newest whole-asset deletion"
    );
    assert_eq!(
        by_key["kept"], None,
        "an asset never deleted has no tombstone"
    );
    assert_eq!(tombs.len(), 1);
    assert_eq!(
        (tombs[0].asset_key.as_str(), tombs[0].timestamp),
        ("events", 4000)
    );
}

/// V7 gives an asset row's code version and inputs a time of their own.
/// A row that already holds them got them with the materialization it
/// holds, so their time starts at that one's.
#[tokio::test]
async fn test_v7_backfills_provenance_timestamp() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let pre_v7: Vec<Migration> = embedded_migrations().into_iter().take(6).collect();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &pre_v7,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply V1..V6");

    db.query(
            "INSERT INTO assets [ \
               { code_location_id: 'default', asset_key: 'built', tags: [], kinds: [], \
                 last_timestamp: 300, last_data_version: 'dv', \
                 last_materialization_code_version: 'v1', last_input_data_versions: [['u', 'dv_u']] }, \
               { code_location_id: 'default', asset_key: 'root', tags: [], kinds: [], \
                 last_timestamp: 200, last_data_version: 'dv', \
                 last_materialization_code_version: 'v1' }, \
               { code_location_id: 'default', asset_key: 'unversioned', tags: [], kinds: [], \
                 last_timestamp: 250, last_data_version: 'dv', \
                 last_input_data_versions: [['u', 'dv_u']] }, \
               { code_location_id: 'default', asset_key: 'by_action', tags: [], kinds: [], \
                 last_timestamp: 100, last_data_version: 'dv' }, \
               { code_location_id: 'default', asset_key: 'never', tags: [], kinds: [] } \
             ] RETURN NONE",
        )
        .await
        .unwrap()
        .check()
        .unwrap();

    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply V7");

    let provenance_times = async || -> Vec<(String, Option<i64>)> {
        db.query(
            "SELECT VALUE [asset_key, last_provenance_timestamp] FROM assets ORDER BY asset_key",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap()
    };
    let expected = vec![
        ("built".to_string(), Some(300)),
        ("by_action".to_string(), None),
        ("never".to_string(), None),
        ("root".to_string(), Some(200)),
        ("unversioned".to_string(), Some(250)),
    ];
    assert_eq!(provenance_times().await, expected);

    let stamps = stamps_at(&db, 7).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (7, 2, 7),
        "a pre-v7 writer records a code version without its time, which a \
             v7 writer then lets an older materialization overwrite"
    );
    assert!(check_compatibility(stamps, Capability::Read, 6).is_ok());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 6).is_err());

    // A re-run applies nothing, and the backfill leaves times already set.
    db.query("UPDATE assets SET last_provenance_timestamp = 150 WHERE asset_key = 'built'")
        .await
        .unwrap()
        .check()
        .unwrap();
    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("re-run");
    let v7 = embedded_migrations()
        .into_iter()
        .nth(6)
        .expect("V7 is embedded");
    db.query(v7.sql().expect("V7 has a body"))
        .await
        .unwrap()
        .check()
        .expect("the V7 body runs again");
    let mut expected = expected;
    expected[0].1 = Some(150);
    assert_eq!(provenance_times().await, expected);
    assert_eq!(
        count_rows(&db, REFINERY_HISTORY_TABLE).await,
        u64::from(SCHEMA_VERSION)
    );
}

/// V8 adds `traceback` to `run_logs`. It is additive: rows from before it
/// read back without one, and a V7 build can still read and write.
#[tokio::test]
async fn test_v8_adds_run_logs_traceback() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let mut backend = SurrealMigrate { db: db.clone() };
    let migrate_to = async |backend: &mut SurrealMigrate, version: usize| {
        let migrations: Vec<Migration> = embedded_migrations().into_iter().take(version).collect();
        backend
            .migrate(
                &migrations,
                true,
                false,
                false,
                Target::Latest,
                REFINERY_HISTORY_TABLE,
            )
            .await
            .expect("apply migrations")
    };
    migrate_to(&mut backend, 7).await;
    db.query(
        "INSERT INTO run_logs [{ code_location_id: 'default', run_id: 'r1', \
             step_key: 'a', timestamp: 1, stdout: 'out' }] RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    migrate_to(&mut backend, 8).await;
    db.query(
        "INSERT INTO run_logs [{ code_location_id: 'default', run_id: 'r1', \
             step_key: 'a', timestamp: 2, traceback: '{\"exceptions\":[]}' }] RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    let rows: Vec<(i64, Option<String>)> = db
        .query("SELECT VALUE [timestamp, traceback] FROM run_logs ORDER BY timestamp")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, None), (2, Some(r#"{"exceptions":[]}"#.to_string()))]
    );
    let stamps = read_schema_stamps(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (8, 2, 7)
    );
    assert!(check_compatibility(stamps, Capability::Read, 7).is_ok());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 7).is_ok());
}

/// V9 adds `config` to `runs`. It is additive: rows from before it read
/// back without one, and a V8 build can still read and write.
#[tokio::test]
async fn test_v9_adds_run_config() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let mut backend = SurrealMigrate { db: db.clone() };
    let migrate_to = async |backend: &mut SurrealMigrate, version: usize| {
        let migrations: Vec<Migration> = embedded_migrations().into_iter().take(version).collect();
        backend
            .migrate(
                &migrations,
                true,
                false,
                false,
                Target::Latest,
                REFINERY_HISTORY_TABLE,
            )
            .await
            .expect("apply migrations")
    };
    migrate_to(&mut backend, 8).await;
    db.query(
        "INSERT INTO runs [{ run_id: 'r1', status: 'Queued', start_time: 1, \
             tags: [], node_names: ['a'] }] RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    migrate_to(&mut backend, 9).await;
    db.query(
        "INSERT INTO runs [{ run_id: 'r2', status: 'Queued', start_time: 2, \
             tags: [], node_names: ['a'], config: '{\"a\":{\"x\":1}}' }] RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    let rows: Vec<(i64, Option<String>)> = db
        .query("SELECT VALUE [start_time, config] FROM runs ORDER BY start_time")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, None), (2, Some(r#"{"a":{"x":1}}"#.to_string()))]
    );
    let stamps = read_schema_stamps(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (9, 2, 7)
    );
    assert!(check_compatibility(stamps, Capability::Read, 8).is_ok());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 8).is_ok());
}

/// V10 turns `config` into the launch document. Rows from before it are
/// rewritten as part of the upgrade, on runs and backfills; a row already
/// in the new shape is left alone. A V9 writer is refused (it would read
/// `assets` as an asset name); a V9 reader still shows the text.
#[tokio::test]
async fn test_v10_rewrites_run_config_into_the_launch_document() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let mut backend = SurrealMigrate { db: db.clone() };
    let migrations: Vec<Migration> = embedded_migrations().into_iter().take(9).collect();
    backend
        .migrate(
            &migrations,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply migrations");
    db.query(
            "INSERT INTO runs [\
                { run_id: 'r1', status: 'Queued', start_time: 1, tags: [], node_names: ['a'], \
                  config: '{\"a\":{\"x\":1},\"b\":{}}' }, \
                { run_id: 'r2', status: 'Queued', start_time: 2, tags: [], node_names: ['a'] }, \
                { run_id: 'r3', status: 'Queued', start_time: 3, tags: [], node_names: ['a'], \
                  config: '{\"assets\":{\"a\":{\"config\":{\"x\":2}}}}' } \
             ] RETURN NONE; \
             INSERT INTO backfills { backfill_id: 'b1', code_location_id: 'default', \
             status: 'Requested', strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', \
             asset_selection: [], partition_keys: [], run_ids: [], completed_partitions: [], \
             failed_partitions: [], canceled_partitions: [], max_concurrency: 1, tags: [], \
             create_time: 1, end_time: NONE, error: NONE, config: '{\"a\":{\"y\":true}}' } RETURN NONE",
        )
        .await
        .unwrap()
        .check()
        .unwrap();

    apply_migrations(&db, &embedded_migrations()).await.unwrap();

    let rows: Vec<(i64, Option<String>)> = db
        .query("SELECT VALUE [start_time, config] FROM runs ORDER BY start_time")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                1,
                Some(r#"{"assets":{"a":{"config":{"x":1}},"b":{"config":{}}}}"#.to_string())
            ),
            (2, None),
            (
                3,
                Some(r#"{"assets":{"a":{"config":{"x":2}}}}"#.to_string())
            ),
        ]
    );
    let backfill: Option<String> = db
        .query("SELECT VALUE config FROM backfills WHERE backfill_id = 'b1'")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        backfill.as_deref(),
        Some(r#"{"assets":{"a":{"config":{"y":true}}}}"#)
    );
    let stamps = stamps_at(&db, 10).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (10, 2, 10)
    );
    assert!(check_compatibility(stamps, Capability::Read, 9).is_ok());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 9).is_err());
    // Nothing to rewrite the second time.
    assert_eq!(launch_document_from_v9(r#"{"assets":{"a":{}}}"#), None);
    assert_eq!(launch_document_from_v9("[1]"), None);
}

/// V12 moves a backfill's keys, run links and settled keys out of its row.
/// Rows from before it read back the same, and neither a V11 reader nor a
/// V11 writer may open the store.
#[tokio::test]
async fn test_v12_moves_backfill_progress_out_of_the_row() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let mut backend = SurrealMigrate { db: db.clone() };
    let migrations: Vec<Migration> = embedded_migrations().into_iter().take(11).collect();
    backend
        .migrate(
            &migrations,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply migrations");
    db.query(
        "INSERT INTO backfills [\
            { backfill_id: 'b1', code_location_id: 'default', status: 'InProgress', \
              strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', asset_selection: ['a'], \
              partition_keys: [{ variant: 'Single', keys: ['p1'] }, { variant: 'Single', keys: ['p2'] }, \
                  { variant: 'Single', keys: ['p3'] }, { variant: 'Single', keys: ['p4'] }], \
              run_ids: ['r2', 'r1', 'r3'], \
              completed_partitions: [{ variant: 'Single', keys: ['p1'] }, { variant: 'Single', keys: ['p3'] }], \
              failed_partitions: [{ variant: 'Single', keys: ['p2'] }, { variant: 'Single', keys: ['p3'] }], \
              canceled_partitions: [{ variant: 'Single', keys: ['p4'] }], \
              max_concurrency: 1, tags: [], create_time: 1, end_time: NONE, error: NONE }, \
            { backfill_id: 'b2', code_location_id: 'default', status: 'Requested', \
              strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', asset_selection: ['a'], \
              partition_keys: [], run_ids: [], completed_partitions: [], failed_partitions: [], \
              canceled_partitions: [], max_concurrency: 1, tags: [], create_time: 2, \
              end_time: NONE, error: NONE } \
         ] RETURN NONE",
    )
    .await
    .unwrap()
    .check()
    .unwrap();

    let through_v12: Vec<Migration> = embedded_migrations().into_iter().take(12).collect();
    apply_migrations(&db, &through_v12).await.unwrap();

    let rows = async |query: &str| -> Vec<serde_json::Value> {
        db.query(query.to_string()).await.unwrap().take(0).unwrap()
    };
    assert_eq!(
        rows("SELECT VALUE partition_keys.keys FROM backfill_keys WHERE backfill_id = 'b1'").await,
        vec![serde_json::json!([["p1"], ["p2"], ["p3"], ["p4"]])]
    );
    assert_eq!(
        rows(
            "SELECT VALUE run_id FROM \
             (SELECT run_id, linked_at FROM backfill_runs WHERE backfill_id = 'b1' ORDER BY linked_at)"
        )
        .await,
        vec!["r2", "r1", "r3"]
    );
    assert_eq!(
        rows(
            "SELECT VALUE [partition_key.keys[0], outcome] FROM backfill_outcomes \
             WHERE backfill_id = 'b1' ORDER BY partition_key.keys[0]"
        )
        .await,
        vec![
            serde_json::json!(["p1", "Completed"]),
            serde_json::json!(["p2", "Failed"]),
            serde_json::json!(["p3", "Failed"]),
            serde_json::json!(["p4", "Canceled"]),
        ]
    );
    assert_eq!(
        rows("SELECT VALUE partition_keys FROM backfill_keys WHERE backfill_id = 'b2'").await,
        vec![serde_json::json!([])]
    );
    for table in ["backfill_runs", "backfill_outcomes"] {
        assert!(
            rows(&format!("SELECT * FROM {table} WHERE backfill_id = 'b2'"))
                .await
                .is_empty()
        );
    }

    let left_on_row: Vec<serde_json::Value> = db
        .query(
            "SELECT VALUE [partition_keys, run_ids, completed_partitions, failed_partitions, \
             canceled_partitions] FROM backfills",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert!(
        left_on_row
            .iter()
            .flat_map(|v| v.as_array().unwrap())
            .all(serde_json::Value::is_null),
        "no progress is left on the backfill rows: {left_on_row:?}"
    );
    db.query("UPDATE backfills SET status = 'Canceled' WHERE backfill_id = 'b1'")
        .await
        .unwrap()
        .check()
        .expect("a migrated backfill row stays writable");

    let stamps = stamps_at(&db, 12).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (12, 12, 12)
    );
    assert!(check_compatibility(stamps, Capability::Read, 11).is_err());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 11).is_err());
}

/// V13 moves storage to SurrealDB 4: neither a V12 reader nor a V12 writer
/// may open the store.
#[tokio::test]
async fn test_v13_raises_both_floors() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    let stamps = stamps_at(&storage.db, 13).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (13, 13, 13)
    );
    assert!(check_compatibility(stamps, Capability::Read, 12).is_err());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 12).is_err());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 13).is_ok());
}

/// V14 counts a backfill per planned run. An older backfill that ended keeps
/// its rows per partition; one in progress goes on under the new rows.
#[tokio::test]
async fn test_v14_moves_a_backfill_in_progress_to_one_outcome_per_run() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let through_v13: Vec<Migration> = embedded_migrations().into_iter().take(13).collect();
    apply_migrations(&db, &through_v13).await.unwrap();

    let backfill = |id: &str, status: &str| {
        format!(
            "{{ backfill_id: '{id}', code_location_id: 'default', status: '{status}', \
               strategy: {{ variant: 'PerDimension', multi_run: ['zone'], single_run: ['hour'] }}, \
               failure_policy: 'Continue', asset_selection: ['a'], max_concurrency: 1, tags: [], \
               create_time: 1, end_time: NONE, error: NONE, launched_by: {{ kind: 'manual' }} }}"
        )
    };
    let cell = |hours: &str, zones: &str| {
        format!("{{ variant: 'Multi', dims: [['hour', [{hours}]], ['zone', [{zones}]]] }}")
    };
    let run = |id: &str, status: &str, zone: &str| {
        format!(
            "{{ run_id: '{id}', status: '{status}', start_time: 1, tags: [], node_names: ['a'], \
               partition_key: {}, launched_by: {{ kind: 'backfill', backfill_id: 'active' }} }}",
            cell("'h1', 'h2'", zone)
        )
    };
    let failure = |step: &str| {
        format!(
            "{{ run_id: 'run-z1', event_type: 'StepFailure', asset_key: '{step}', timestamp: 2, \
               metadata: [], partition_key: {} }}",
            cell("'h2'", "'z1'")
        )
    };
    db.query(format!(
        "INSERT INTO backfills [{}, {}, {}] RETURN NONE; \
         INSERT INTO backfill_keys [\
            {{ backfill_id: 'old', partition_keys: [\
                {{ variant: 'Single', keys: ['p1'] }}, {{ variant: 'Single', keys: ['p2', 'p3'] }}, \
                {}, \
                {{ variant: 'Set', keys: [{{ variant: 'Single', keys: ['s1'] }}, {{ variant: 'Single', keys: ['s2'] }}] }}] }}, \
            {{ backfill_id: 'active', partition_keys: [{}] }}, \
            {{ backfill_id: 'requested', partition_keys: [{{ variant: 'Single', keys: ['p1', 'p2'] }}] }} \
         ] RETURN NONE; \
         INSERT INTO backfill_runs [\
            {{ backfill_id: 'old', run_id: 'old-r1', linked_at: 1 }}, \
            {{ backfill_id: 'active', run_id: 'run-z2', linked_at: 20 }}, \
            {{ backfill_id: 'active', run_id: 'run-z1', linked_at: 10 }}, \
            {{ backfill_id: 'active', run_id: 'run-z3', linked_at: 30 }}, \
            {{ backfill_id: 'active', run_id: 'run-z4', linked_at: 40 }} \
         ] RETURN NONE; \
         INSERT INTO backfill_outcomes [\
            {{ backfill_id: 'old', partition_key: {{ variant: 'Single', keys: ['p1'] }}, outcome: 'Completed' }}, \
            {{ backfill_id: 'old', partition_key: {{ variant: 'Single', keys: ['p2'] }}, outcome: 'Failed' }}, \
            {{ backfill_id: 'old', partition_key: {{ variant: 'Single', keys: ['p3'] }}, outcome: 'Canceled' }}, \
            {{ backfill_id: 'active', partition_key: {}, outcome: 'Completed' }}, \
            {{ backfill_id: 'active', partition_key: {}, outcome: 'Failed' }} \
         ] RETURN NONE; \
         INSERT INTO runs [{}, {}, {}] RETURN NONE; \
         INSERT INTO events [{}, {}] RETURN NONE;",
        backfill("old", "CompletedFailed"),
        backfill("active", "InProgress"),
        backfill("requested", "Requested"),
        cell("'h1', 'h2', 'h3'", "'z1', 'z2'"),
        cell("'h1', 'h2'", "'z1', 'z2', 'z3', 'z4'"),
        cell("'h1'", "'z1'"),
        cell("'h2'", "'z1'"),
        run("run-z1", "Success", "'z1'"),
        run("run-z2", "Failure", "'z2'"),
        run("run-z3", "Started", "'z3'"),
        failure("first"),
        failure("second"),
    ))
    .await
    .unwrap()
    .check()
    .unwrap();
    db.query(format!(
        "INSERT INTO backfills [{}] RETURN NONE; \
         CREATE backfill_keys CONTENT {{ backfill_id: 'wide', partition_keys: \
            array::map(array::range(0, 5000), |$i: int| {{ variant: 'Single', keys: [<string> $i] }}) }} \
            RETURN NONE;",
        backfill("wide", "CompletedSuccess"),
    ))
    .await
    .unwrap()
    .check()
    .unwrap();

    apply_migrations(&db, &embedded_migrations()).await.unwrap();

    let storage = SurrealStorage {
        db: db.clone(),
        retry_config: Default::default(),
        backend_kind: SurrealBackendKind::Memory,
        runtime: None,
    };
    let read = async |id: &str| storage.get_backfill(id).await.unwrap().expect("backfill");
    let counts = |b: &BackfillRecord| {
        (
            b.completed_partitions,
            b.failed_partitions,
            b.canceled_partitions,
        )
    };

    // An older backfill keeps its rows: each counts one partition.
    let old = read("old").await;
    assert_eq!(old.num_partitions, 11, "1 + 2 + 3 x 2 + 2");
    assert_eq!((old.planned_runs, counts(&old)), (0, (1, 1, 1)));
    assert_eq!(old.run_ids, vec!["old-r1"]);
    assert_eq!(storage.get_backfill_keys("old").await.unwrap().len(), 4);

    // Its keys are in pages, in order, and the one row per backfill is gone.
    let wide = storage.get_backfill_keys("wide").await.unwrap();
    assert_eq!(read("wide").await.num_partitions, 5000);
    assert_eq!(wide.len(), 5000);
    assert!(wide.iter().enumerate().all(|(i, key)| *key
        == PartitionKey::Single {
            keys: vec![i.to_string()]
        }));
    let pages: Vec<i64> = db
        .query("SELECT VALUE page FROM backfill_key_pages WHERE backfill_id = 'wide' ORDER BY page")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(pages, vec![0, 1]);
    assert_eq!(count_rows(&db, "backfill_keys").await, 0);

    // The backfill in progress: run numbers in link order, a row per run that
    // ended, no rows per partition.
    let active = read("active").await;
    assert_eq!((active.num_partitions, active.planned_runs), (8, 4));
    assert_eq!(active.run_ids, vec!["run-z1", "run-z2", "run-z3", "run-z4"]);
    assert_eq!(
        counts(&active),
        (1, 3, 0),
        "run-z1 succeeded with one partition failed alone; run-z2 failed"
    );
    let links: Vec<serde_json::Value> = db
        .query(
            "SELECT VALUE [run_id, run_number, member_count] FROM backfill_runs \
             WHERE backfill_id = 'active' ORDER BY run_number",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        links,
        vec![
            serde_json::json!(["run-z1", 0, 2]),
            serde_json::json!(["run-z2", 1, 2]),
            serde_json::json!(["run-z3", 2, 2]),
            serde_json::json!(["run-z4", 3, 0]),
        ],
        "run-z4 is linked but has no run yet"
    );
    let per_partition: Vec<String> = db
        .query("SELECT VALUE backfill_id FROM backfill_outcomes")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(per_partition, vec!["old"; 3]);

    // It completes under the new rows.
    assert_eq!(storage.try_complete_backfill("active").await.unwrap(), None);
    storage
        .update_run_status("run-z3", RunStatus::Success, Some(5))
        .await
        .unwrap();
    storage
        .settle_unstarted_backfill_runs(
            "active",
            &[UnstartedBackfillRun {
                run_number: 3,
                member_count: 2,
                outcome: BackfillRunOutcome::Canceled,
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        storage.try_complete_backfill("active").await.unwrap(),
        Some(BackfillStatus::CompletedFailed)
    );
    assert_eq!(counts(&read("active").await), (3, 3, 2));

    // A requested backfill starts as a new one does.
    let requested = read("requested").await;
    assert_eq!((requested.num_partitions, requested.planned_runs), (2, 0));
    assert!(storage.start_backfill("requested", 2).await.unwrap());

    let stamps = stamps_at(&db, 14).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (14, 14, 14)
    );
    assert!(check_compatibility(stamps, Capability::Read, 13).is_err());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 13).is_err());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 14).is_ok());
}

/// V15 adds the time a run began to execute. An ended run from before it gets
/// the best time that the store has, and only if the store shows that it
/// began. It is additive: a V14 build can still read and write.
#[tokio::test]
async fn test_v15_gives_older_runs_the_time_they_began() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let through_v14: Vec<Migration> = embedded_migrations().into_iter().take(14).collect();
    apply_migrations(&db, &through_v14).await.unwrap();

    // (run, status, end time, events, the time it began)
    type Event = (&'static str, i64);
    let cases: Vec<(&str, &str, Option<i64>, Vec<Event>, Option<i64>)> = vec![
        (
            "direct",
            "Success",
            Some(500),
            vec![("StepStart", 110)],
            Some(100),
        ),
        (
            "direct-in-progress",
            "Started",
            None,
            vec![("StepStart", 110)],
            None,
        ),
        (
            "direct-failed-in-a-step",
            "Failure",
            Some(500),
            vec![("StepStart", 110), ("StepFailure", 500)],
            Some(100),
        ),
        (
            "direct-never-launched",
            "Failure",
            Some(500),
            vec![("RunLaunchFailed", 500)],
            None,
        ),
        (
            "waited",
            "Success",
            Some(500),
            vec![("RunQueued", 100), ("RunDequeued", 400)],
            Some(400),
        ),
        (
            "waited-failed-in-a-step",
            "Failure",
            Some(500),
            vec![("RunQueued", 100), ("RunDequeued", 400), ("StepStart", 420)],
            Some(400),
        ),
        ("waiting", "Queued", None, vec![("RunQueued", 100)], None),
        (
            "left-the-queue",
            "NotStarted",
            None,
            vec![("RunQueued", 100), ("RunDequeued", 400)],
            None,
        ),
        (
            "canceled-in-the-queue",
            "Canceled",
            Some(300),
            vec![("RunQueued", 100)],
            None,
        ),
        (
            "canceled-after-the-queue",
            "Canceled",
            Some(450),
            vec![("RunQueued", 100), ("RunDequeued", 400)],
            None,
        ),
        (
            "never-launched",
            "Failure",
            Some(700),
            vec![
                ("RunQueued", 100),
                ("RunDequeued", 400),
                ("RunLaunchFailed", 700),
            ],
            None,
        ),
        (
            "dequeue-not-recorded",
            "Success",
            Some(500),
            vec![("RunQueued", 100), ("StepStart", 420)],
            None,
        ),
    ];

    let runs: Vec<String> = cases
        .iter()
        .map(|(id, status, end_time, _, _)| {
            let end_time = end_time.map_or("NONE".to_string(), |end| end.to_string());
            format!(
                "{{ run_id: '{id}', status: '{status}', start_time: 100, end_time: {end_time}, \
                   tags: [], node_names: ['a'] }}"
            )
        })
        .collect();
    let events: Vec<String> = cases
        .iter()
        .flat_map(|(id, _, _, events, _)| {
            events.iter().map(move |(event_type, timestamp)| {
                format!(
                    "{{ run_id: '{id}', event_type: '{event_type}', timestamp: {timestamp}, \
                       metadata: [] }}"
                )
            })
        })
        .collect();
    db.query(format!(
        "INSERT INTO runs [{}] RETURN NONE; INSERT INTO events [{}] RETURN NONE",
        runs.join(", "),
        events.join(", ")
    ))
    .await
    .unwrap()
    .check()
    .unwrap();

    apply_migrations(&db, &embedded_migrations()).await.unwrap();

    let mut began: Vec<(String, Option<i64>)> = db
        .query("SELECT VALUE [run_id, execution_start_time] FROM runs")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    began.sort();
    let mut expected: Vec<(String, Option<i64>)> = cases
        .iter()
        .map(|(id, _, _, _, began)| (id.to_string(), *began))
        .collect();
    expected.sort();
    assert_eq!(began, expected);

    let stamps = stamps_at(&db, 15).await;
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (15, 14, 14)
    );
    assert!(check_compatibility(stamps, Capability::Read, 14).is_ok());
    assert!(check_compatibility(stamps, Capability::ReadWrite, 14).is_ok());
}

/// V5 adds `exclusive`/`partitions` to a SCHEMAFULL table. `DEFAULT` only
/// fills at create time, so live pre-V5 slot rows must be backfilled: the
/// conflict clause reads a missing `exclusive` as NONE (falsy), which would
/// let a materialize run straight past an in-flight exclusive action, and
/// lease renewal would trip field coercion on the next UPDATE.
#[tokio::test]
async fn test_v5_backfills_exclusive_on_legacy_slot_rows() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    // A store holding pre-V5 rows was written by SurrealDB 3, and stays at
    // its compatibility level once converted.
    db.query(format!(
        "DEFINE DATABASE OVERWRITE {DEFAULT_DATABASE} COMPATIBILITY 3"
    ))
    .await
    .unwrap()
    .check()
    .unwrap();

    let pre_v5: Vec<Migration> = embedded_migrations().into_iter().take(4).collect();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &pre_v5,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .expect("apply V1..V4");

    // Two legacy holders of one asset pool: an exclusive action (which took
    // the whole pool under the old rule) and an ordinary materialize.
    for (step, slots) in [("action", 1_000_000), ("mat", 1)] {
        db.query(format!(
            "INSERT INTO concurrency_slots {{ code_location_id: 'default', \
                 pool_key: '__asset__:orders', run_id: 'legacy', step_key: '{step}', \
                 slots_consumed: {slots}, claimed_at: 1, lease_expires_at: 9223372036854775807, \
                 last_heartbeat: 1 }} RETURN NONE"
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
    }

    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply V5");

    let exclusive_of = async |step: &str| -> Vec<bool> {
        db.query(format!(
            "SELECT VALUE exclusive FROM concurrency_slots WHERE step_key = '{step}'"
        ))
        .await
        .unwrap()
        .take(0)
        .expect("legacy slot rows must read back after V5")
    };
    assert_eq!(
        exclusive_of("action").await,
        vec![true],
        "the whole-pool holder is recovered as an exclusive action"
    );
    assert_eq!(
        exclusive_of("mat").await,
        vec![false],
        "a one-slot holder is a materialize"
    );

    // Lease renewal is an UPDATE — it must not trip coercion on these rows.
    db.query(
        "UPDATE concurrency_slots SET lease_expires_at = 99, last_heartbeat = 99 \
             WHERE run_id = 'legacy'",
    )
    .await
    .unwrap()
    .check()
    .expect("legacy slot rows must stay writable after V5");
}

/// Editing an already-applied migration is caught by refinery's checksum
/// (the frozen-baseline guard — a different V1 body diverges).
#[tokio::test]
async fn test_edited_migration_is_refused_as_divergent() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    apply_migrations(&db, &embedded_migrations())
        .await
        .expect("apply real V1");

    // Same version + name, different body → different checksum → divergent.
    let edited =
        vec![Migration::unapplied("V1__base", "DEFINE TABLE tampered SCHEMAFULL;").unwrap()];
    let mut backend = SurrealMigrate { db: db.clone() };
    let res = backend
        .migrate(
            &edited,
            true,
            false,
            false,
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await;
    assert!(res.is_err(), "an edited applied migration must be refused");
}

/// The cross-process lease serializes openers: a second acquire returns
/// `None`, releasing hands it on, and renewal only works for the holder.
#[tokio::test]
async fn test_migration_lease_serializes_and_hands_off() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create embedded storage");
    let db = &storage.db;

    let holder = try_acquire_migration_lease(db)
        .await
        .unwrap()
        .expect("first acquire must win");
    assert!(
        try_acquire_migration_lease(db).await.unwrap().is_none(),
        "a held, unexpired lease must block a second acquire"
    );
    renew_migration_lease(db, &holder).await.unwrap();
    assert!(
        renew_migration_lease(db, "not-the-holder").await.is_err(),
        "renewing a lease we do not hold must fail"
    );
    release_migration_lease(db, &holder).await.unwrap();
    let next = try_acquire_migration_lease(db)
        .await
        .unwrap()
        .expect("a released lease must be re-acquirable");
    assert_ne!(next, holder, "each acquire mints a fresh holder token");
}

/// The open-time floor guard, exhaustively. `build` is a literal here, so the
/// matrix is independent of `SCHEMA_VERSION`.
#[test]
fn test_check_compatibility_matrix() {
    let s = |version, min_reader, min_writer| SchemaStamps {
        version,
        min_reader,
        min_writer,
    };
    use Capability::{Migrate, Read, ReadWrite};
    assert!(check_compatibility(s(3, 1, 2), ReadWrite, 3).is_ok());
    assert!(check_compatibility(s(3, 1, 2), Read, 3).is_ok());
    assert!(check_compatibility(s(5, 1, 5), Read, 3).is_ok());
    assert!(check_compatibility(s(5, 1, 5), ReadWrite, 3).is_err());
    assert!(check_compatibility(s(5, 4, 5), Read, 3).is_err());
    assert!(check_compatibility(s(2, 1, 2), ReadWrite, 3).is_err());
    assert!(check_compatibility(s(2, 1, 2), Read, 3).is_err());
    assert!(check_compatibility(s(2, 1, 2), Migrate, 3).is_ok());
    assert!(check_compatibility(s(3, 1, 2), Migrate, 3).is_ok());
    assert!(check_compatibility(s(5, 1, 5), Migrate, 3).is_err());
}

/// A build newer than the database surfaces the typed `SchemaMigrationNeeded`
/// (the contract the PyO3 boundary downcasts) and names the fix.
#[test]
fn test_behind_build_surfaces_typed_error() {
    let stamps = SchemaStamps {
        version: 1,
        min_reader: 1,
        min_writer: 1,
    };
    let err = check_compatibility(stamps, Capability::ReadWrite, 2).unwrap_err();
    assert!(
        err.downcast_ref::<SchemaMigrationNeeded>().is_some(),
        "{err}"
    );
    assert!(err.to_string().contains("rivers db migrate"), "{err}");
}

/// On a brand-new store `migration_meta` is undefined; `read_schema_stamps`
/// maps that to `None` (uninitialized) so the first opener can bootstrap.
#[tokio::test]
async fn test_read_stamps_on_undefined_table_is_none() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let stamps = read_schema_stamps(&db).await.unwrap();
    assert!(
        stamps.is_none(),
        "an undefined migration_meta table reads as uninitialized, got {stamps:?}"
    );
}

/// Stamps come from the *latest* migration's row, not a per-column max — so a
/// higher floor on an older row never leaks into the current contract.
#[tokio::test]
async fn test_read_stamps_uses_latest_row_not_column_max() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    db.query(
            "DEFINE TABLE migration_meta SCHEMAFULL; \
             DEFINE FIELD version ON migration_meta TYPE int; \
             DEFINE FIELD class ON migration_meta TYPE string; \
             DEFINE FIELD min_reader ON migration_meta TYPE int; \
             DEFINE FIELD min_writer ON migration_meta TYPE int; \
             UPSERT migration_meta:1 CONTENT { version: 1, class: 'x', min_reader: 1, min_writer: 9 }; \
             UPSERT migration_meta:2 CONTENT { version: 2, class: 'x', min_reader: 1, min_writer: 2 };",
        )
        .await
        .unwrap()
        .check()
        .unwrap();
    let s = read_schema_stamps(&db).await.unwrap().unwrap();
    assert_eq!(
        (s.version, s.min_reader, s.min_writer),
        (2, 1, 2),
        "stamps must be the latest (v2) row, not a per-column max (which would give min_writer 9)"
    );
}

/// An uninitialized store is built by the first opener (the read-only UI
/// included): `ensure_compatible` runs the migrations and the guard reads current.
#[tokio::test]
async fn test_uninitialized_store_is_initialized_by_a_reader() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    ensure_compatible(&db, Capability::Read).await.unwrap();
    let stamps = read_schema_stamps(&db).await.unwrap();
    assert!(
        matches!(stamps, Some(s) if s.version == SCHEMA_VERSION),
        "first-opener init must reach the current version, got {stamps:?}"
    );
}

/// The first open of an empty database applies every migration in one
/// transaction: a failure leaves the database empty, so the retry sets it up
/// again instead of finding a store stamped behind this build.
#[tokio::test]
async fn test_failed_first_open_leaves_the_database_empty() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    let mut failing = embedded_migrations();
    failing.push(Migration::unapplied("V99__fails", "THROW 'injected failure';").unwrap());
    apply_migrations(&db, &failing).await.unwrap_err();
    assert_eq!(read_schema_stamps(&db).await.unwrap(), None);

    ensure_compatible(&db, Capability::ReadWrite).await.unwrap();
    assert_eq!(
        read_schema_stamps(&db).await.unwrap().map(|s| s.version),
        Some(SCHEMA_VERSION)
    );
}

/// Reader/writer split: a DB whose writer floor is far ahead refuses an old
/// `ReadWrite` opener but keeps an old read-only one (the UI) running.
#[tokio::test]
async fn test_open_allows_old_reader_but_refuses_old_writer() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create embedded storage");
    plant_stamps(&storage.db, SCHEMA_VERSION + 2, 1, SCHEMA_VERSION + 2).await;
    assert!(
        ensure_compatible(&storage.db, Capability::Read)
            .await
            .is_ok(),
        "an older read-only opener must keep working"
    );
    assert!(
        ensure_compatible(&storage.db, Capability::ReadWrite)
            .await
            .is_err(),
        "an older writer must be refused"
    );
}

/// Explicit migrate is idempotent and refuses a downgrade (DB ahead of build).
#[tokio::test]
async fn test_migrate_is_idempotent_and_refuses_downgrade() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create embedded storage");
    migrate_to_current(&storage.db).await.unwrap();
    assert!(matches!(
        read_schema_stamps(&storage.db).await.unwrap(),
        Some(s) if s.version == SCHEMA_VERSION
    ));
    plant_stamps(&storage.db, SCHEMA_VERSION + 1, 1, 1).await;
    assert!(
        migrate_to_current(&storage.db).await.is_err(),
        "downgrade must be refused"
    );
}

/// The floor constants are the floors the newest migration records.
#[tokio::test]
async fn test_build_stamps_match_the_newest_migration() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    assert_eq!(stamps_at(&storage.db, SCHEMA_VERSION).await, BUILD_STAMPS);
    assert_eq!(
        read_schema_stamps(&storage.db).await.unwrap(),
        Some(BUILD_STAMPS)
    );
}

/// A migration stops a process only when it raises that process's floor
/// above its build. Literal versions, so the matrix is independent of this build.
#[test]
fn test_migration_stops_matrix() {
    let s = |version, min_reader, min_writer| SchemaStamps {
        version,
        min_reader,
        min_writer,
    };
    let (writer, reader) = (true, false);

    // The write floor rises from 10 to 12.
    let (from, to) = (s(11, 2, 10), s(12, 2, 12));
    assert!(migration_stops(from, to, writer, Some(11)));
    assert!(!migration_stops(from, to, writer, Some(12)));
    assert!(migration_stops(from, to, writer, None));
    // The read floor stays: no reader stops, known or not.
    assert!(!migration_stops(from, to, reader, Some(2)));
    assert!(!migration_stops(from, to, reader, None));

    // Both floors rise.
    let (from, to) = (s(12, 2, 12), s(13, 13, 13));
    assert!(migration_stops(from, to, reader, Some(12)));
    assert!(!migration_stops(from, to, reader, Some(13)));

    // No floor rises: nothing stops.
    let (from, to) = (s(10, 2, 10), s(11, 2, 10));
    assert!(!migration_stops(from, to, writer, Some(10)));
    assert!(!migration_stops(from, to, writer, None));

    // The store is current or ahead: there is no migration.
    assert!(!migration_stops(s(12, 2, 12), s(12, 2, 12), writer, None));
    assert!(!migration_stops(
        s(13, 13, 13),
        s(12, 2, 12),
        writer,
        Some(1)
    ));
}

/// Rewind the stamps to `version` with the given floors, as a store that an
/// older build migrated last.
async fn rewind_stamps(db: &Surreal<Any>, version: u32, min_reader: u32, min_writer: u32) {
    db.query(format!(
        "DELETE {MIGRATION_META_TABLE} WHERE version >= {version}"
    ))
    .await
    .unwrap()
    .check()
    .unwrap();
    plant_stamps(db, version, min_reader, min_writer).await;
}

#[tokio::test]
async fn test_schema_status_lists_what_a_migration_stops() {
    use crate::assets::remote::LocationRef;
    use crate::storage::CodeLocationDirectoryEntry;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let location = |name: &str| LocationRef::new("team", name).unwrap();
    storage
        .upsert_code_location_registration(&location("new"), CodeLocationDirectoryEntry::new("a"))
        .await
        .unwrap();
    storage
        .kv_set(
            "code_location:team/old",
            br#"{"identity":"b","last_seen":7}"#,
        )
        .await
        .unwrap();
    storage.record_component("ui", false, 5).await.unwrap();

    let current = storage.schema_status().await.unwrap();
    assert_eq!(current.stamps, Some(BUILD_STAMPS));
    assert_eq!(
        current.processes.iter().filter(|p| p.stops).count(),
        0,
        "a current store has no migration: {current:?}"
    );

    rewind_stamps(&storage.db, SCHEMA_VERSION - 1, 1, 1).await;
    let behind = storage.schema_status().await.unwrap();
    assert_eq!(behind.stamps.map(|s| s.version), Some(SCHEMA_VERSION - 1));
    let stops: Vec<_> = behind
        .processes
        .iter()
        .map(|p| (p.name.as_str(), p.code_location, p.seen, p.stops))
        .collect();
    assert_eq!(
        stops,
        vec![
            ("team/new", true, None, false),
            ("team/old", true, Some(7), true),
            ("ui", false, Some(5), false),
        ]
    );
}

/// `rivers db status` must not change the store: an inspect open neither
/// initializes an empty store nor refuses one that is behind.
#[tokio::test]
async fn test_inspect_neither_initializes_nor_refuses() {
    let db = any::connect("mem://").await.unwrap();
    db.use_ns(DEFAULT_NAMESPACE)
        .use_db(DEFAULT_DATABASE)
        .await
        .unwrap();
    ensure_compatible(&db, Capability::Inspect).await.unwrap();
    assert_eq!(read_schema_stamps(&db).await.unwrap(), None);

    ensure_compatible(&db, Capability::ReadWrite).await.unwrap();
    rewind_stamps(&db, SCHEMA_VERSION - 1, 1, 1).await;
    assert!(ensure_compatible(&db, Capability::ReadWrite).await.is_err());
    ensure_compatible(&db, Capability::Inspect).await.unwrap();
    assert_eq!(
        read_schema_stamps(&db).await.unwrap().map(|s| s.version),
        Some(SCHEMA_VERSION - 1)
    );
}

#[tokio::test]
async fn test_a_migration_that_stops_a_process_is_blocked() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .kv_set("code_location:team/old", br#"{"identity":"b"}"#)
        .await
        .unwrap();
    storage.record_component("ui", false, 5).await.unwrap();

    storage.ensure_migration_stops_nothing().await.unwrap();

    rewind_stamps(&storage.db, SCHEMA_VERSION - 1, 1, 1).await;
    let err = storage.ensure_migration_stops_nothing().await.unwrap_err();
    let blocked = err.downcast_ref::<MigrationBlocked>().unwrap();
    let names: Vec<_> = blocked.processes.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["team/old"]);
    assert_eq!(
        err.to_string(),
        "the migration stops team/old: update them first, or migrate with force"
    );
    assert_eq!(
        read_schema_stamps(&storage.db)
            .await
            .unwrap()
            .map(|s| s.version),
        Some(SCHEMA_VERSION - 1)
    );
}

/// The floors do not rise, so the old build keeps working after the migration.
#[tokio::test]
async fn test_a_migration_that_keeps_the_floors_is_not_blocked() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .kv_set("code_location:team/old", br#"{"identity":"b"}"#)
        .await
        .unwrap();

    rewind_stamps(&storage.db, SCHEMA_VERSION - 1, MIN_READER, MIN_WRITER).await;

    storage.ensure_migration_stops_nothing().await.unwrap();
}

/// The chart upgrades the operator and the UI with the migration, and their
/// records are from the builds before it. They must not block it.
#[tokio::test]
async fn test_an_old_operator_or_ui_record_does_not_block_a_migration() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    for (name, writes) in [("operator", true), ("ui", false)] {
        let record = format!(
            r#"{{"schema_version":1,"rivers_version":"0.1.0","writes":{writes},"started_at":5}}"#
        );
        storage
            .kv_set(&format!("component:{name}"), record.as_bytes())
            .await
            .unwrap();
    }

    rewind_stamps(&storage.db, SCHEMA_VERSION - 1, 1, 1).await;

    storage.ensure_migration_stops_nothing().await.unwrap();
    let status = storage.schema_status().await.unwrap();
    let rows: Vec<_> = status
        .processes
        .iter()
        .map(|p| (p.name.as_str(), p.schema_version, p.stops))
        .collect();
    assert_eq!(
        rows,
        vec![("operator", Some(1), false), ("ui", Some(1), false)]
    );
}
