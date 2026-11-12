//! Tests against a SurrealDB server. An embedded store has no message limit,
//! so only a server shows a message that is too large. The tests run when
//! `RIVERS_TEST_SURREAL_ENDPOINT` names a WebSocket endpoint.

use super::*;

const MESSAGE_LIMIT: usize = 1 << 20;

/// A store on the test server with a 1 MiB message limit, in a database of
/// its own. `None` when no test server is configured.
async fn limited_storage() -> Option<SurrealStorage> {
    let endpoint = std::env::var("RIVERS_TEST_SURREAL_ENDPOINT").ok()?;
    let mut config =
        SurrealConnectConfig::unauthenticated(endpoint).with_max_message_size(MESSAGE_LIMIT);
    config.database = format!("t{}", uuid::Uuid::new_v4().simple());

    Some(
        SurrealStorage::connect(config)
            .await
            .expect("failed to connect to the test server"),
    )
}

#[test]
fn surreal_connect_config_carries_the_message_limit() {
    let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000");
    assert_eq!(cfg.max_message_size, None);
    assert_eq!(
        cfg.with_max_message_size(MESSAGE_LIMIT).max_message_size,
        Some(MESSAGE_LIMIT)
    );
}

#[tokio::test]
async fn message_above_the_limit_is_rejected() {
    let Some(storage) = limited_storage().await else {
        return;
    };
    let small = vec![b'x'; 1024];
    storage.kv_set("small", &small).await.unwrap();

    let error = storage
        .kv_set("large", &vec![b'x'; 2 * MESSAGE_LIMIT])
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("too long"), "{error:#}");

    assert_eq!(storage.kv_get("small").await.unwrap(), Some(small));
    assert_eq!(storage.kv_get("large").await.unwrap(), None);
}
