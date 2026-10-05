use super::super::*;
use super::*;

async fn count_rows(db: &Surreal<Any>, table: &str) -> u64 {
    let mut r = db
        .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
        .await
        .unwrap();
    let n: Option<u64> = r.take((0, "n")).unwrap();
    n.unwrap_or(0)
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

    apply_migrations(&db).await.expect("apply V2");

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
    let stamps = read_schema_stamps(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (SCHEMA_VERSION, 2, 10),
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
    apply_migrations(&db).await.expect("apply V3");
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

    apply_migrations(&db).await.expect("apply V6");

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

    apply_migrations(&db).await.expect("apply V7");

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

    let stamps = read_schema_stamps(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamps.version, stamps.min_reader, stamps.min_writer),
        (SCHEMA_VERSION, 2, 10),
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
    apply_migrations(&db).await.expect("re-run");
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

    apply_migrations(&db).await.unwrap();

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
    let stamps = read_schema_stamps(&db).await.unwrap().unwrap();
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

    apply_migrations(&db).await.expect("apply V5");

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
    apply_migrations(&db).await.expect("apply real V1");

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
        .expect("failed to create rocksdb storage");
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

/// Reader/writer split: a DB whose writer floor is far ahead refuses an old
/// `ReadWrite` opener but keeps an old read-only one (the UI) running.
#[tokio::test]
async fn test_open_allows_old_reader_but_refuses_old_writer() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
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
        .expect("failed to create rocksdb storage");
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
