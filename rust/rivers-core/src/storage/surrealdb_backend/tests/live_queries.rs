//! Live queries against a SurrealDB server. A server queues the
//! notifications of each connection and ends a connection whose queue is
//! full; an embedded store has no such queue. The tests run when
//! `RIVERS_TEST_SURREAL_ENDPOINT` names a WebSocket endpoint.

use std::time::Duration;

use futures_util::StreamExt;

use super::*;

/// Rows of one commit: many times the notifications that a server queues for
/// one connection (100 by default).
const BURST: usize = 5_000;

/// Two stores on one database of the test server, with a table `burst`.
/// `None` when no test server is configured.
async fn two_stores() -> Option<(SurrealStorage, SurrealStorage)> {
    let endpoint = std::env::var("RIVERS_TEST_SURREAL_ENDPOINT").ok()?;
    let mut config = SurrealConnectConfig::unauthenticated(endpoint);
    config.database = format!("t{}", uuid::Uuid::new_v4().simple());

    let first = SurrealStorage::connect(config.clone())
        .await
        .expect("failed to connect to the test server");
    let second = SurrealStorage::connect(config)
        .await
        .expect("failed to connect to the test server");
    first
        .db
        .query("DEFINE TABLE burst SCHEMALESS")
        .await
        .unwrap()
        .check()
        .unwrap();
    Some((first, second))
}

/// Write [`BURST`] rows to `burst` in one commit.
async fn write_burst(store: &SurrealStorage) {
    store
        .db
        .query("INSERT INTO burst (array::map(array::range(0, $rows), |$n: int| { n: $n })) RETURN NONE")
        .bind(("rows", BURST as i64))
        .await
        .unwrap()
        .check()
        .unwrap();
}

/// Wait until the server ends the connection of `changes`.
async fn connection_ended(changes: &mut LiveTable) {
    let ended = async { while changes.next().await.is_some() {} };
    tokio::time::timeout(Duration::from_secs(10), ended)
        .await
        .expect("the server kept the connection of the live query");
}

#[tokio::test]
async fn a_burst_of_live_notifications_leaves_a_running_query_alone() {
    let Some((reader, writer)) = two_stores().await else {
        return;
    };
    let mut changes = reader.subscribe_table("burst").await.unwrap();
    let db = reader.db.clone();
    let running = tokio::spawn(async move { db.query("SLEEP 2s; RETURN 'done'").await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    write_burst(&writer).await;
    connection_ended(&mut changes).await;

    let mut response = running
        .await
        .unwrap()
        .expect("the query ended with the live query");
    let done: Option<String> = response.take(1).unwrap();
    assert_eq!(done.as_deref(), Some("done"));
}

#[tokio::test]
async fn a_live_query_opens_again_after_the_server_ended_its_connection() {
    let Some((reader, writer)) = two_stores().await else {
        return;
    };
    let mut changes = reader.subscribe_table("burst").await.unwrap();
    write_burst(&writer).await;
    connection_ended(&mut changes).await;

    let mut changes = reader.subscribe_table("burst").await.unwrap();
    writer
        .db
        .query("CREATE burst SET n = -1 RETURN NONE")
        .await
        .unwrap()
        .check()
        .unwrap();

    let change = tokio::time::timeout(Duration::from_secs(10), changes.next()).await;
    assert_eq!(change.expect("the new live query saw no change"), Some(()));
}
