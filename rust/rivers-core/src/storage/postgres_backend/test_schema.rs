//! The schema of a test store. Each test store has a schema of its own, so
//! tests do not see each other.

use sqlx::postgres::PgConnectOptions;
use sqlx::{AssertSqlSafe, Connection, PgConnection};

/// A schema that a test binary left behind is this old, in seconds.
const LEFT_BEHIND_SECS: i64 = 600;

/// One sweeper of left-behind schemas at a time. The value has no meaning.
const SWEEP_LOCK: i64 = 0x7269_7665_7273_0002;

/// A new, empty schema in the database at `url`, and the URL that opens a
/// store in it. For the tests of the Python package: they open a store from
/// a URL, in processes of their own.
pub async fn create_test_schema(url: &str) -> anyhow::Result<String> {
    let schema = create(&url.parse()?).await?;
    let separator = if url.contains('?') { '&' } else { '?' };
    Ok(format!(
        "{url}{separator}options=-c%20search_path%3D{schema}"
    ))
}

/// A new schema in the database of `admin`; gives its name.
async fn create(admin: &PgConnectOptions) -> anyhow::Result<String> {
    let seconds = super::now_nanos() / 1_000_000_000;
    let name = format!("t_{seconds}_{}", uuid::Uuid::new_v4().simple());
    let mut conn = PgConnection::connect_with(admin).await?;
    // A test binary that ends, or is killed, does not drop all its stores.
    if uuid::Uuid::new_v4().as_u128() % 16 == 0 {
        sweep(&mut conn, seconds - LEFT_BEHIND_SECS).await;
    }
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA {name}")))
        .execute(&mut conn)
        .await?;
    conn.close().await?;
    Ok(name)
}

/// The schema of a test store of this crate: it goes when the store is dropped.
#[cfg(test)]
pub(super) struct TestSchema {
    admin: PgConnectOptions,
    name: String,
}

#[cfg(test)]
impl TestSchema {
    /// A new schema in the database of `admin`.
    pub(super) async fn create(admin: PgConnectOptions) -> anyhow::Result<Self> {
        let name = create(&admin).await?;
        Ok(Self { admin, name })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
impl Drop for TestSchema {
    fn drop(&mut self) {
        let admin = self.admin.clone();
        let drop = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name);
        // `drop` cannot await, and the runtime of the test can be at its end.
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async {
                if let Ok(mut conn) = PgConnection::connect_with(&admin).await {
                    let _ = sqlx::raw_sql(AssertSqlSafe(drop)).execute(&mut conn).await;
                    let _ = conn.close().await;
                }
            });
        });
    }
}

/// Drop the test schemas made before `before` (seconds since the epoch).
async fn sweep(conn: &mut PgConnection, before: i64) {
    let alone = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(SWEEP_LOCK)
        .fetch_one(&mut *conn)
        .await;
    if !matches!(alone, Ok(true)) {
        return;
    }
    let left_behind: Vec<String> = sqlx::query_scalar(
        "SELECT nspname::text FROM pg_namespace WHERE nspname ~ '^t_[0-9]+_[0-9a-f]+$' \
         AND split_part(nspname, '_', 2)::bigint < $1",
    )
    .bind(before)
    .fetch_all(&mut *conn)
    .await
    .unwrap_or_default();
    for schema in left_behind {
        let drop = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
        let _ = sqlx::raw_sql(AssertSqlSafe(drop)).execute(&mut *conn).await;
    }
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(SWEEP_LOCK)
        .execute(&mut *conn)
        .await;
}
