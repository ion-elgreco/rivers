use super::*;

#[tokio::test]
async fn a_new_schema_gets_the_tables_and_the_stamps() {
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();

    let stamps = storage.schema_stamps().await.unwrap();
    assert_eq!(stamps, Some(super::super::migration::BUILD_STAMPS));

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema()",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert!(tables > 20, "only {tables} tables");

    // A second open of the same schema passes the guard and adds nothing.
    let again = PostgresStorage::connect_with_options(
        storage.options.clone(),
        storage.endpoint.clone(),
        StorageRetryConfig::default(),
        Capability::ReadWrite,
    )
    .await
    .unwrap();
    assert_eq!(again.schema, storage.schema);
}

#[tokio::test]
async fn the_url_of_a_test_schema_opens_a_store_in_that_schema() {
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let url = create_test_schema(&url).await.unwrap();
    let storage = PostgresStorage::connect(&url, Capability::ReadWrite)
        .await
        .unwrap();

    assert!(url.ends_with(&storage.schema), "{url} / {}", storage.schema);
    assert!(storage.schema.starts_with("t_"));
    assert!(storage.schema_stamps().await.unwrap().is_some());
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA {} CASCADE",
        storage.schema
    )))
    .execute(storage.pool())
    .await
    .unwrap();
}

/// A queued run, written with SQL: the test is of the table, not of the run code.
async fn insert_run(storage: &PostgresStorage, run_id: &str) {
    sqlx::query("INSERT INTO runs (run_id, status, start_time) VALUES ($1, 'Queued', 1)")
        .bind(run_id)
        .execute(storage.pool())
        .await
        .unwrap();
}

const SOON: std::time::Duration = std::time::Duration::from_millis(500);
const LATE: std::time::Duration = std::time::Duration::from_secs(5);

#[tokio::test]
async fn a_change_of_a_table_reaches_its_listeners() {
    use futures_util::StreamExt;
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    let mut changes = storage.subscribe_table("runs").await.unwrap();

    insert_run(&storage, "r1").await;

    let change = tokio::time::timeout(LATE, changes.next()).await;
    assert_eq!(change, Ok(Some(())), "no notification for a new run");
}

#[tokio::test]
async fn a_statement_that_changes_no_row_wakes_no_listener() {
    use futures_util::StreamExt;
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    insert_run(&storage, "r1").await;
    let mut runs = storage.subscribe_table("runs").await.unwrap();
    let mut slots = storage.subscribe_table("concurrency_slots").await.unwrap();
    let mut outcomes = storage
        .subscribe_table("backfill_run_outcomes")
        .await
        .unwrap();

    // What an idle daemon sends every 250 ms, and what each status change sends.
    for statement in [
        "DELETE FROM concurrency_slots WHERE lease_expires_at <= 0",
        "UPDATE runs SET block_reason = 'x' WHERE status = 'Started'",
        "INSERT INTO backfill_run_outcomes SELECT * FROM backfill_run_outcomes",
    ] {
        sqlx::query(statement)
            .execute(storage.pool())
            .await
            .unwrap();
    }

    assert!(tokio::time::timeout(SOON, runs.next()).await.is_err());
    assert!(tokio::time::timeout(SOON, slots.next()).await.is_err());
    assert!(tokio::time::timeout(SOON, outcomes.next()).await.is_err());

    // Each kind of change still notifies.
    for statement in [
        "UPDATE runs SET block_reason = 'x' WHERE status = 'Queued'",
        "DELETE FROM runs",
    ] {
        sqlx::query(statement)
            .execute(storage.pool())
            .await
            .unwrap();
        let change = tokio::time::timeout(LATE, runs.next()).await;
        assert_eq!(change, Ok(Some(())), "no notification for `{statement}`");
    }
}

#[tokio::test]
async fn a_listener_that_loses_its_connection_ends() {
    use futures_util::StreamExt;
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    // A name of its own: the test must end only its own listener.
    let table = format!("t{}", uuid::Uuid::new_v4().simple());
    let mut changes = storage.subscribe_table(&table).await.unwrap();

    let ended: i64 = sqlx::query_scalar(
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
         WHERE pid <> pg_backend_pid() AND query LIKE '%' || $1 || '%'",
    )
    .bind(format!("rivers_{table}"))
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(ended, 1, "the listener connection was not found");

    // The changes during the gap are lost, so the caller must learn of it.
    let change = tokio::time::timeout(LATE, changes.next()).await;
    assert_eq!(
        change,
        Ok(None),
        "the stream goes on after a lost connection"
    );
}

#[tokio::test]
async fn the_schema_of_a_store_is_where_its_tables_are() {
    use sqlx::Connection;
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();

    // An empty schema ahead of the store's schema on the search path: new
    // objects would go there, but the tables are found in the second one.
    let empty = format!("t_empty_{}", uuid::Uuid::new_v4().simple());
    let mut admin = sqlx::PgConnection::connect_with(&storage.options)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {empty}")))
        .execute(&mut admin)
        .await
        .unwrap();
    let both = storage
        .options
        .clone()
        .options([("search_path", format!("{empty},{}", storage.schema))]);
    let opened = PostgresStorage::connect_with_options(
        both,
        storage.endpoint.clone(),
        StorageRetryConfig::default(),
        Capability::ReadWrite,
    )
    .await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP SCHEMA {empty}")))
        .execute(&mut admin)
        .await
        .unwrap();

    assert_eq!(opened.unwrap().schema, storage.schema);
}

#[tokio::test]
async fn a_block_reason_that_does_not_change_writes_no_row() {
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    insert_run(&storage, "r1").await;
    let version = || async {
        sqlx::query_scalar::<_, String>("SELECT ctid::text FROM runs WHERE run_id = 'r1'")
            .fetch_one(storage.pool())
            .await
            .unwrap()
    };

    for reason in [Some("global run limit (10/10)"), None] {
        storage
            .set_block_reason_by_status("default", RunStatus::Queued, reason)
            .await
            .unwrap();
        let written = version().await;
        storage
            .set_block_reason_by_status("default", RunStatus::Queued, reason)
            .await
            .unwrap();
        storage.update_run_block_reason("r1", reason).await.unwrap();
        assert_eq!(version().await, written, "the row was written again");
    }
}
