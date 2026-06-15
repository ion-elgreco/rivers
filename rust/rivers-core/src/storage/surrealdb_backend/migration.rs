//! Schema versioning and migration: stamps, the capability floor guard, the
//! cross-process lease, and the data-heal steps. The only place schema is applied.

use std::collections::HashMap;

use anyhow::Context;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::types::{Bytes, RecordId, SurrealValue};

use super::{
    BASE_SCHEMA, DbAssetPartitionWrite, DbDynamicPartition, DbKv, PartitionKey, now_nanos,
};

/// Max schema version this build understands. The schema is the fold of
/// [`MIGRATION_STEPS`] (base is step `to: 1`); change it only by adding a step.
const SCHEMA_VERSION: u32 = 3;
/// `kv` key holding the combined [`SchemaStamps`] record
const SCHEMA_STAMPS_KEY: &str = "schema_stamps";

/// What a caller intends to do — selects which floor [`check_compatibility`] enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Read-only consumer (the UI). Gated by `min_reader`.
    Read,
    /// Reads and writes (code locations, daemon, executors). Gated by `min_writer`.
    ReadWrite,
    /// The migrator (`rivers db migrate`) — may advance `version`, exempt from the refusal.
    Migrate,
}

/// How far back a build may be and still use a database with this step applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepClass {
    /// Adds only optional/defaulted schema or records. Raises neither floor.
    Additive,
    /// An old writer re-introduces a healed shape. Raises `min_writer`.
    WriteBreaking,
    /// Reshapes rows past an old reader. Raises both floors (`min_writer >= min_reader`).
    #[allow(dead_code)] // no read-breaking step exists yet; the guard handles it
    ReadBreaking,
}

/// A step's async body, boxed as a function pointer (async fns aren't `fn` items).
type MigrationFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;

/// One migration step: the version it brings the DB *to*, its [`StepClass`], and
/// its idempotent `run`.
struct MigrationStep {
    to: u32,
    class: StepClass,
    run: for<'a> fn(&'a Surreal<Any>) -> MigrationFut<'a>,
}

const MIGRATION_STEPS: &[MigrationStep] = &[
    // v0 -> v1: build the base schema (all tables + indexes); every store starts
    // unstamped at v0 and runs it. Additive — the baseline raises no floor.
    MigrationStep {
        to: 1,
        class: StepClass::Additive,
        run: |db| Box::pin(apply_base_schema(db)),
    },
    // v1 -> v2: canonicalize Multi partition-key dim order. Old readers parse
    // sorted keys fine; an old writer re-introduces unsorted keys.
    MigrationStep {
        to: 2,
        class: StepClass::WriteBreaking,
        run: |db| Box::pin(migrate_multi_partition_key_order(db)),
    },
    // v2 -> v3: record (not rewrite) dynamic keys with reserved chars.
    MigrationStep {
        to: 3,
        class: StepClass::Additive,
        run: |db| Box::pin(scan_reserved_char_dynamic_keys(db)),
    },
];

/// Reader/writer floors for a database at `version`, folding each step's [`StepClass`].
fn floors_for_version(version: u32) -> (u32, u32) {
    let mut min_reader = 1;
    let mut min_writer = 1;
    for step in MIGRATION_STEPS.iter().filter(|s| s.to <= version) {
        match step.class {
            StepClass::Additive => {}
            StepClass::WriteBreaking => min_writer = min_writer.max(step.to),
            StepClass::ReadBreaking => {
                min_reader = min_reader.max(step.to);
                min_writer = min_writer.max(step.to);
            }
        }
    }
    (min_reader, min_writer)
}

/// The three stamps, stored as one `kv` record so the guard reads a consistent
/// triple. `version` is what's applied; floors come from [`floors_for_version`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SchemaStamps {
    version: u32,
    min_reader: u32,
    min_writer: u32,
}

impl SchemaStamps {
    /// Stamps for a database at `version` (floors derived from the ledger).
    fn for_version(version: u32) -> Self {
        let (min_reader, min_writer) = floors_for_version(version);
        Self {
            version,
            min_reader,
            min_writer,
        }
    }

    /// Stamps for a database at the schema this build understands.
    fn current() -> Self {
        Self::for_version(SCHEMA_VERSION)
    }
}

/// Read the stamps, or `None` if uninitialized — an undefined `kv` table errors
/// (not empties), and that maps to `None` (the open path then inits or refuses).
async fn read_schema_stamps(db: &Surreal<Any>) -> anyhow::Result<Option<SchemaStamps>> {
    let mut response = match db
        .query("SELECT * FROM kv WHERE key = $key LIMIT 1")
        .bind(("key", SCHEMA_STAMPS_KEY.to_string()))
        .await
    {
        Ok(response) => response,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let rows: Vec<DbKv> = match response.take(0) {
        Ok(rows) => rows,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    match rows.into_iter().next() {
        // Fail closed on an unparsable stamp: never silently re-init over a
        // store whose version we cannot read.
        Some(kv) => Ok(Some(serde_json::from_slice(&kv.value).context(
            "schema stamp is corrupt or was written by an incompatible build; \
             refusing to open the store",
        )?)),
        None => Ok(None),
    }
}

/// True if `err` is specifically "table not defined" (how a fresh store presents).
/// Matched structurally, not by a bare substring other errors share.
fn is_undefined_table_error(err: &surrealdb::Error) -> bool {
    use surrealdb::types::{ErrorDetails, NotFoundError};
    if matches!(
        err.details(),
        ErrorDetails::NotFound(Some(NotFoundError::Table { .. }))
    ) {
        return true;
    }
    // Some engines surface this as a plain message; still require "table" so the
    // narrowing keeps excluding the other "<x> does not exist" errors.
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("table") && msg.contains("does not exist")
}

/// Persist the stamps as one atomic upsert on the unique `key` index — no torn triple.
async fn write_schema_stamps(db: &Surreal<Any>, stamps: SchemaStamps) -> anyhow::Result<()> {
    let body = serde_json::to_vec(&stamps)?;
    db.query(
        "INSERT INTO kv { key: $key, value: $value } \
         ON DUPLICATE KEY UPDATE value = $value",
    )
    .bind(("key", SCHEMA_STAMPS_KEY.to_string()))
    .bind(("value", Bytes::from(body)))
    .await?
    .check()?;
    Ok(())
}

/// Primitives needed before any stamp or lease: the `kv` and `migration_lock`
/// tables. Applied before the base schema; their only definition site.
const BOOTSTRAP_SCHEMA: &str = "\
DEFINE TABLE IF NOT EXISTS kv SCHEMAFULL; \
DEFINE FIELD IF NOT EXISTS key ON kv TYPE string; \
DEFINE FIELD IF NOT EXISTS value ON kv TYPE bytes; \
DEFINE INDEX IF NOT EXISTS idx_kv_key ON kv FIELDS key UNIQUE; \
DEFINE TABLE IF NOT EXISTS migration_lock SCHEMAFULL; \
DEFINE FIELD IF NOT EXISTS holder ON migration_lock TYPE string; \
DEFINE FIELD IF NOT EXISTS expires_at ON migration_lock TYPE int;";

/// Define the bootstrap primitives ([`BOOTSTRAP_SCHEMA`]). Idempotent; init/migrate
/// path only, never a normal connect.
pub(super) async fn ensure_bootstrap(db: &Surreal<Any>) -> anyhow::Result<()> {
    db.query(BOOTSTRAP_SCHEMA).await?.check()?;
    Ok(())
}

/// v0 → v1: apply [`BASE_SCHEMA`], the first step, so a fresh DB is built by the
/// chain. Idempotent (`DEFINE ... IF NOT EXISTS`).
async fn apply_base_schema(db: &Surreal<Any>) -> anyhow::Result<()> {
    db.query(BASE_SCHEMA).await?.check()?;
    Ok(())
}

/// DB is older than this build. Typed so the PyO3 boundary maps it to
/// `SchemaMigrationNeededError` for the `rivers dev` prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaMigrationNeeded {
    /// Schema version the database is stamped at.
    pub db_version: u32,
    /// Schema version this build expects.
    pub build_version: u32,
}

impl std::fmt::Display for SchemaMigrationNeeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "database needs migration: it is at schema v{} but this rivers build expects v{}; run `rivers db migrate`",
            self.db_version, self.build_version
        )
    }
}

impl std::error::Error for SchemaMigrationNeeded {}

/// Open-time floor guard: decide whether `cap` may proceed against `stamps`.
/// Pure — I/O lives in [`ensure_compatible`].
fn check_compatibility(stamps: SchemaStamps, cap: Capability, build: u32) -> anyhow::Result<()> {
    // The migrator is the one caller that may run ahead of the database (it is
    // about to advance it); it must only refuse a downgrade.
    if cap == Capability::Migrate {
        if build < stamps.version {
            anyhow::bail!(
                "cannot migrate: database is at schema v{} but this rivers build understands only v{build}; upgrade rivers first",
                stamps.version
            );
        }
        return Ok(());
    }
    // Past the Migrate early-return, only Read/ReadWrite reach here.
    let (floor, verb) = if cap == Capability::Read {
        (stamps.min_reader, "read")
    } else {
        (stamps.min_writer, "write")
    };
    if build < floor {
        anyhow::bail!(
            "this rivers build (schema v{build}) is too old to {verb} a database at schema v{} (requires v{floor}); upgrade rivers",
            stamps.version
        );
    }
    if build > stamps.version {
        return Err(SchemaMigrationNeeded {
            db_version: stamps.version,
            build_version: build,
        }
        .into());
    }
    if build < stamps.version {
        tracing::info!(
            build,
            db_version = stamps.version,
            capability = ?cap,
            "database schema is ahead of this build but compatible for its capability"
        );
    }
    Ok(())
}

/// Open-time gate. Schema isn't applied here (only in [`migrate_to_current`]):
/// a stamped store is judged by [`check_compatibility`], an uninitialized one is
/// bootstrapped by the first opener (UI included).
pub(super) async fn ensure_compatible(db: &Surreal<Any>, cap: Capability) -> anyhow::Result<()> {
    // `rivers db migrate` opens `Migrate`: always run the setup/upgrade.
    if cap == Capability::Migrate {
        return migrate_to_current(db).await;
    }
    match read_schema_stamps(db).await? {
        Some(stamps) => check_compatibility(stamps, cap, SCHEMA_VERSION),
        // First opener of an uninitialized store bootstraps it (any capability,
        // UI included); a stamped-but-behind DB was already refused above.
        None => migrate_to_current(db).await,
    }
}

/// Cross-process migration lease — a migration must run alone. One
/// `migration_lock:lease` record, heartbeat-renewed; a crash frees it after one TTL.
const MIGRATION_LEASE_TTL_SECS: i64 = 30;
/// Renew well inside the TTL so a slow round-trip never lets an active holder's
/// lease lapse.
const MIGRATION_LEASE_RENEW_SECS: u64 = 10;
/// How long a process waiting on the lease sleeps between attempts.
const MIGRATION_LEASE_POLL_SECS: u64 = 1;

/// True if `CREATE migration_lock:lease` failed because the id is already taken
/// (another opener holds it). Structural match, with a message fallback.
fn is_lease_taken_error(err: &anyhow::Error) -> bool {
    use surrealdb::types::ErrorDetails;
    if err
        .chain()
        .find_map(|e| e.downcast_ref::<surrealdb::Error>())
        .is_some_and(|se| matches!(se.details(), ErrorDetails::AlreadyExists(_)))
    {
        return true;
    }
    let m = err.to_string();
    m.contains("already exists") || m.contains("already contains")
}

/// Take the lease: clear any expired one, then `CREATE` the fixed id — one caller
/// wins, losers get already-exists → `None`. Returns the holder token, or `None`.
async fn try_acquire_migration_lease(db: &Surreal<Any>) -> anyhow::Result<Option<String>> {
    let holder = uuid::Uuid::new_v4().to_string();
    let now_ns = now_nanos();
    let exp_ns = now_ns + MIGRATION_LEASE_TTL_SECS * 1_000_000_000;
    let outcome: anyhow::Result<()> = async {
        db.query(
            "DELETE migration_lock:lease WHERE expires_at <= $now; \
             CREATE migration_lock:lease SET holder = $holder, expires_at = $exp;",
        )
        .bind(("now", now_ns))
        .bind(("exp", exp_ns))
        .bind(("holder", holder.clone()))
        .await?
        .check()?;
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(Some(holder)),
        Err(err) if is_lease_taken_error(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

/// Push the expiry forward. Errors if we no longer hold it (aborts the migration).
async fn renew_migration_lease(db: &Surreal<Any>, holder: &str) -> anyhow::Result<()> {
    let exp_ns = now_nanos() + MIGRATION_LEASE_TTL_SECS * 1_000_000_000;
    // UPDATE doesn't return a usable row count via the Rust SDK, so a follow-up
    // SELECT counts whether we still hold the record (see `renew_slot_lease`).
    let mut response = db
        .query(
            "UPDATE migration_lock:lease SET expires_at = $exp WHERE holder = $holder; \
             SELECT count() AS total FROM migration_lock:lease WHERE holder = $holder GROUP ALL",
        )
        .bind(("holder", holder.to_string()))
        .bind(("exp", exp_ns))
        .await?
        .check()?;
    let renewed: Option<u32> = response.take((1, "total"))?;
    if renewed.unwrap_or(0) == 0 {
        anyhow::bail!("lost the migration lease (another process took over)");
    }
    Ok(())
}

/// Release the lease (best-effort; a crash is covered by the TTL).
async fn release_migration_lease(db: &Surreal<Any>, holder: &str) -> anyhow::Result<()> {
    db.query("DELETE migration_lock:lease WHERE holder = $holder")
        .bind(("holder", holder.to_string()))
        .await?
        .check()?;
    Ok(())
}

/// Drive `work` while renewing the lease on a timer (so it outlives the TTL).
/// A failed renewal drops `work`, cancelling the migration.
async fn with_lease_renewal<F>(db: &Surreal<Any>, holder: &str, work: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(work);
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_secs(MIGRATION_LEASE_RENEW_SECS));
    ticker.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            result = &mut work => return result,
            _ = ticker.tick() => renew_migration_lease(db, holder).await?,
        }
    }
}

/// Initialize or upgrade a store to [`SCHEMA_VERSION`] and stamp it — backs both
/// first-opener init and `rivers db migrate`. Idempotent, refuses a downgrade,
/// and serializes openers through the lease.
async fn migrate_to_current(db: &Surreal<Any>) -> anyhow::Result<()> {
    ensure_bootstrap(db).await?;
    let mut waited = false;
    loop {
        // Fast path: already current → nothing to do, no lease needed. A
        // downgrade (database ahead of this build) is refused here, before locking.
        if let Some(stamps) = read_schema_stamps(db).await? {
            check_compatibility(stamps, Capability::Migrate, SCHEMA_VERSION)?;
            if stamps.version == SCHEMA_VERSION {
                return Ok(());
            }
        }
        match try_acquire_migration_lease(db).await? {
            Some(holder) => {
                let result = migrate_under_lease(db, &holder).await;
                // Release on every outcome so a transient failure doesn't wedge
                // the next opener; a crash is handled by the lease TTL instead.
                if let Err(err) = release_migration_lease(db, &holder).await {
                    tracing::warn!(error = %err, "failed to release migration lease");
                }
                return result;
            }
            // Another process is migrating. Wait, then re-loop: it either
            // finishes (the fast path returns) or its lease lapses (we retry).
            None => {
                if !waited {
                    tracing::info!("another process is migrating storage; waiting for it");
                    waited = true;
                }
                tokio::time::sleep(std::time::Duration::from_secs(MIGRATION_LEASE_POLL_SECS)).await;
            }
        }
    }
}

/// The migration body, under the lease. Re-reads the version (a prior holder may
/// have finished), then runs the pending steps and stamps — under lease renewal.
async fn migrate_under_lease(db: &Surreal<Any>, holder: &str) -> anyhow::Result<()> {
    let from = match read_schema_stamps(db).await? {
        Some(stamps) => {
            check_compatibility(stamps, Capability::Migrate, SCHEMA_VERSION)?;
            stamps.version
        }
        // Unstamped means a fresh store (version 0): the chain builds it from
        // empty. Pre-versioning stores are not supported.
        None => 0,
    };
    // Raced: a prior holder migrated between our fast-path check and acquire.
    if from >= SCHEMA_VERSION {
        return Ok(());
    }
    with_lease_renewal(db, holder, async {
        run_migration_steps(db, from).await?;
        write_schema_stamps(db, SchemaStamps::current()).await
    })
    .await?;
    tracing::info!(from, to = SCHEMA_VERSION, "storage schema migrated");
    Ok(())
}

/// `kv` key listing dynamic partition keys with display-reserved characters,
/// for operator remediation (delete and re-register under a clean name).
const RESERVED_DYNAMIC_KEYS_KEY: &str = "reserved_dynamic_keys";

/// One recorded reserved-char dynamic key, kept in `kv` for operator inspection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ReservedDynamicKey {
    code_location_id: String,
    partitions_def_name: String,
    partition_key: String,
}

/// v2 → v3: reserved-char dynamic keys can't round-trip the display path (UI, gRPC).
/// Renaming would change user-visible keys, so warn and record them for the operator.
async fn scan_reserved_char_dynamic_keys(db: &Surreal<Any>) -> anyhow::Result<()> {
    let mut result = db
        .query(
            "SELECT code_location_id, partitions_def_name, partition_key, create_timestamp \
             FROM dynamic_partitions",
        )
        .await?;
    let rows: Vec<DbDynamicPartition> = result.take(0)?;
    let offenders: Vec<ReservedDynamicKey> = rows
        .into_iter()
        .filter(|r| PartitionKey::reserved_display_char(&r.partition_key).is_some())
        .map(|r| ReservedDynamicKey {
            code_location_id: r.code_location_id,
            partitions_def_name: r.partitions_def_name,
            partition_key: r.partition_key,
        })
        .collect();
    if offenders.is_empty() {
        return Ok(());
    }
    for e in offenders.iter().take(20) {
        tracing::warn!(
            code_location = %e.code_location_id,
            partitions_def = %e.partitions_def_name,
            key = %e.partition_key,
            "dynamic partition key contains display-reserved characters; \
             delete it and re-register under a clean name"
        );
    }
    if offenders.len() > 20 {
        tracing::warn!(
            more = offenders.len() - 20,
            "additional reserved-character dynamic keys recorded in kv \
             'reserved_dynamic_keys'"
        );
    }
    let body = serde_json::to_vec(&offenders)?;
    db.query(
        "INSERT INTO kv { key: $key, value: $value } \
         ON DUPLICATE KEY UPDATE value = $value",
    )
    .bind(("key", RESERVED_DYNAMIC_KEYS_KEY.to_string()))
    .bind(("value", Bytes::from(body)))
    .await?
    .check()?;
    Ok(())
}

/// Run every [`MIGRATION_STEPS`] entry past `from`, in order — base step (`to: 1`)
/// included, so `from = 0` builds from empty. Idempotent; the caller stamps.
pub(super) async fn run_migration_steps(db: &Surreal<Any>, from: u32) -> anyhow::Result<()> {
    for step in MIGRATION_STEPS.iter().filter(|s| s.to > from) {
        (step.run)(db).await?;
    }
    Ok(())
}

/// Upsert the heal's canonical row. Values come from a pre-loop snapshot, so a
/// live materialization may land newer pointers in between (guarded below).
async fn upsert_canonical_partition_row(
    db: &Surreal<Any>,
    write: DbAssetPartitionWrite,
) -> anyhow::Result<()> {
    // Pointer fields only move forward: a stale snapshot must never roll
    // back a row a live materialization just advanced. last_timestamp is
    // compared first and assigned last so the guards all see the old value.
    db.query(
        "INSERT INTO asset_partitions $row ON DUPLICATE KEY UPDATE \
         last_event_id = IF $input.last_timestamp > last_timestamp THEN $input.last_event_id ELSE last_event_id END, \
         last_run_id = IF $input.last_timestamp > last_timestamp THEN $input.last_run_id ELSE last_run_id END, \
         last_timestamp = IF $input.last_timestamp > last_timestamp THEN $input.last_timestamp ELSE last_timestamp END",
    )
    .bind(("row", write))
    .await?
    .check()?;
    Ok(())
}

/// v1 → v2: re-sort persisted `Multi` partition-key dims. Earlier HashMap order
/// defeated the `asset_partitions` UNIQUE index and `events` key lookups. Idempotent.
async fn migrate_multi_partition_key_order(db: &Surreal<Any>) -> anyhow::Result<()> {
    fn is_sorted(dims: &[(String, Vec<String>)]) -> bool {
        dims.windows(2).all(|w| w[0].0 <= w[1].0)
    }

    #[derive(Debug, SurrealValue)]
    struct PartRow {
        id: RecordId,
        code_location_id: String,
        asset_key: String,
        partition_key: PartitionKey,
        last_event_id: Option<String>,
        last_run_id: Option<String>,
        last_timestamp: Option<i64>,
    }
    let mut result = db
        .query("SELECT id, code_location_id, asset_key, partition_key, last_event_id, last_run_id, last_timestamp FROM asset_partitions WHERE partition_key.variant = 'Multi'")
        .await?;
    let rows: Vec<PartRow> = result.take(0)?;
    // Group by logical key (PartitionKey's Eq/Hash are order-insensitive):
    // unsorted duplicates collapse onto the newest row.
    let mut groups: HashMap<(String, String, PartitionKey), Vec<PartRow>> = HashMap::new();
    for row in rows {
        groups
            .entry((
                row.code_location_id.clone(),
                row.asset_key.clone(),
                row.partition_key.clone(),
            ))
            .or_default()
            .push(row);
    }
    let mut rewritten = 0usize;
    for ((_, _, key), group) in groups {
        let needs_rewrite = group.len() > 1
            || group.iter().any(
                |r| matches!(&r.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)),
            );
        if !needs_rewrite {
            continue;
        }
        let latest = group
            .iter()
            .max_by_key(|r| r.last_timestamp.unwrap_or(i64::MIN))
            .expect("group is non-empty");
        let write = DbAssetPartitionWrite {
            code_location_id: latest.code_location_id.clone(),
            asset_key: latest.asset_key.clone(),
            partition_key: key,
            last_event_id: latest.last_event_id.clone().unwrap_or_default(),
            last_run_id: latest.last_run_id.clone().unwrap_or_default(),
            last_timestamp: latest.last_timestamp.unwrap_or_default(),
        };
        // Canonical row first, then delete the legacy ones: a crash between
        // leaves a recoverable duplicate, never a lost partition.
        upsert_canonical_partition_row(db, write).await?;
        for row in &group {
            if matches!(&row.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)) {
                db.query("DELETE $rec")
                    .bind(("rec", row.id.clone()))
                    .await?
                    .check()?;
            }
        }
        rewritten += 1;
    }

    #[derive(Debug, SurrealValue)]
    struct EventRow {
        id: RecordId,
        partition_key: PartitionKey,
    }
    let mut result = db
        .query("SELECT id, partition_key FROM events WHERE partition_key.variant = 'Multi'")
        .await?;
    let events: Vec<EventRow> = result.take(0)?;
    let to_rewrite: Vec<EventRow> = events
        .into_iter()
        .filter(|ev| matches!(&ev.partition_key, PartitionKey::Multi { dims } if !is_sorted(dims)))
        .collect();
    let event_rewrites = to_rewrite.len();
    // Chunked multi-statement updates: one round-trip per 200 rows instead
    // of per row. Re-binding the same key canonicalizes (into_value sorts).
    for chunk in to_rewrite.chunks(200) {
        let stmts: String = (0..chunk.len())
            .map(|i| format!("UPDATE $r{i} SET partition_key = $p{i};"))
            .collect();
        let mut query = db.query(stmts);
        for (i, ev) in chunk.iter().enumerate() {
            query = query
                .bind((format!("r{i}"), ev.id.clone()))
                .bind((format!("p{i}"), ev.partition_key.clone()));
        }
        query.await?.check()?;
    }
    if rewritten > 0 || event_rewrites > 0 {
        tracing::info!(
            partitions = rewritten,
            events = event_rewrites,
            "migrated Multi partition keys to canonical dim order"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;

    /// Legacy HashMap-ordered dims must collapse onto the canonical form, and
    /// unsorted event keys get rewritten.
    #[tokio::test]
    async fn test_migration_canonicalizes_legacy_multi_rows() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        // Legacy row with unsorted dims, written raw to bypass the
        // canonicalizing serializer.
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();
        // The same logical partition written by a current build (sorted).
        let sorted = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        storage
            .upsert_asset_partitions(vec![DbAssetPartitionWrite {
                code_location_id: cl.to_string(),
                asset_key: "inv".to_string(),
                partition_key: sorted.clone(),
                last_event_id: "e2".to_string(),
                last_run_id: "r2".to_string(),
                last_timestamp: 2,
            }])
            .await
            .unwrap();
        // Legacy event with unsorted dims.
        storage
            .db
            .query(
                "CREATE events CONTENT { code_location_id: 'default', \
                 event_type: 'Materialization', asset_key: 'inv', run_id: 'r1', \
                 partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 timestamp: 1, metadata: [] }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        migrate_multi_partition_key_order(&storage.db)
            .await
            .unwrap();

        let parts = storage
            .get_materialized_partitions(cl, "inv")
            .await
            .unwrap();
        assert_eq!(parts, vec![sorted.clone()], "one canonical row survives");
        let ts = storage.get_partition_timestamps(cl, "inv").await.unwrap();
        assert_eq!(ts, vec![(sorted.clone(), 2)], "newest row's values win");
        // The event is reachable through an equality lookup with the
        // canonical (sorted) bind.
        let mut result = storage
            .db
            .query("SELECT count() AS total FROM events WHERE partition_key = $pk GROUP ALL")
            .bind(("pk", sorted))
            .await
            .unwrap();
        let total: Option<u64> = result.take((0, "total")).unwrap();
        assert_eq!(
            total,
            Some(1),
            "event key must be rewritten to canonical order"
        );
    }

    /// Reserved-char dynamic keys can't be silently renamed, so v3 records them
    /// for operator remediation.
    #[tokio::test]
    async fn test_v3_records_reserved_char_dynamic_keys() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        write_schema_stamps(&storage.db, SchemaStamps::for_version(2))
            .await
            .unwrap();
        storage
            .db
            .query(
                "CREATE dynamic_partitions CONTENT { code_location_id: 'default', \
                 partitions_def_name: 'colors', partition_key: 'us|eu', \
                 create_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        migrate_to_current(&storage.db).await.unwrap();

        let mut res = storage
            .db
            .query("SELECT * FROM kv WHERE key = $key")
            .bind(("key", RESERVED_DYNAMIC_KEYS_KEY.to_string()))
            .await
            .unwrap();
        let rows: Vec<DbKv> = res.take(0).unwrap();
        assert_eq!(rows.len(), 1, "offending keys must be recorded in kv");
        let body = String::from_utf8(rows[0].value.to_vec()).unwrap();
        assert!(body.contains("colors") && body.contains("us|eu"), "{body}");
    }

    /// A live materialization can land newer pointers between the heal's snapshot
    /// and its write, so the upsert must not roll them back.
    #[tokio::test]
    async fn test_migration_upsert_never_rolls_back_newer_pointers() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let key = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        let row = |event: &str, ts: i64| DbAssetPartitionWrite {
            code_location_id: "default".to_string(),
            asset_key: "a".to_string(),
            partition_key: key.clone(),
            last_event_id: event.to_string(),
            last_run_id: format!("run-{event}"),
            last_timestamp: ts,
        };
        // The live writer's row lands first with newer pointers...
        upsert_canonical_partition_row(&storage.db, row("ev-new", 200))
            .await
            .unwrap();
        // ...then the migration applies its stale snapshot values.
        upsert_canonical_partition_row(&storage.db, row("ev-old", 100))
            .await
            .unwrap();
        #[derive(Debug, SurrealValue)]
        struct Ptr {
            last_event_id: String,
            last_timestamp: i64,
        }
        let mut res = storage
            .db
            .query(
                "SELECT last_event_id, last_timestamp FROM asset_partitions WHERE asset_key = 'a'",
            )
            .await
            .unwrap();
        let rows: Vec<Ptr> = res.take(0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_timestamp, 200, "newer pointers must survive");
        assert_eq!(rows[0].last_event_id, "ev-new");
    }

    /// Concurrent stamp writers converge on one record (atomic upsert on the kv
    /// unique index). Both tasks share one connection — the lease test covers
    /// cross-opener serialization.
    #[tokio::test]
    async fn test_concurrent_schema_stamps_both_succeed() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let db_a = storage.db.clone();
        let db_b = storage.db.clone();
        let a = tokio::spawn(async move {
            for _ in 0..25 {
                write_schema_stamps(&db_a, SchemaStamps::for_version(2)).await?;
            }
            anyhow::Ok(())
        });
        let b = tokio::spawn(async move {
            for _ in 0..25 {
                write_schema_stamps(&db_b, SchemaStamps::for_version(3)).await?;
            }
            anyhow::Ok(())
        });
        a.await.unwrap().expect("stamp task A failed");
        b.await.unwrap().expect("stamp task B failed");
        let state = read_schema_stamps(&storage.db).await.unwrap();
        assert!(
            matches!(state, Some(s) if s.version == 2 || s.version == 3),
            "one of the stamps must win, got {state:?}"
        );
        // The upsert converged on exactly one record — no duplicate stamp rows.
        let mut count = storage
            .db
            .query("SELECT count() AS n FROM kv WHERE key = $key GROUP ALL")
            .bind(("key", SCHEMA_STAMPS_KEY.to_string()))
            .await
            .unwrap();
        let n: Option<u64> = count.take((0, "n")).unwrap();
        assert_eq!(n, Some(1), "exactly one stamp record must survive");
    }

    /// The lease serializes openers: a second acquire returns `None`, releasing
    /// hands it on, and renewal only works for the current holder. Exercised at
    /// the lease primitives (RocksDB allows one handle per path per process).
    #[tokio::test]
    async fn test_migration_lease_serializes_and_hands_off() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let db = &storage.db;

        // First acquire wins; the lease is now held and unexpired.
        let holder = try_acquire_migration_lease(db)
            .await
            .unwrap()
            .expect("first acquire must win");
        // A second opener cannot take the held lease.
        assert!(
            try_acquire_migration_lease(db).await.unwrap().is_none(),
            "a held, unexpired lease must block a second acquire"
        );
        // The holder can renew; a token we don't hold cannot.
        renew_migration_lease(db, &holder).await.unwrap();
        assert!(
            renew_migration_lease(db, "not-the-holder").await.is_err(),
            "renewing a lease we do not hold must fail"
        );
        // Releasing hands the lease to the next caller, with a fresh token.
        release_migration_lease(db, &holder).await.unwrap();
        let next = try_acquire_migration_lease(db)
            .await
            .unwrap()
            .expect("a released lease must be re-acquirable");
        assert_ne!(next, holder, "each acquire mints a fresh holder token");
    }

    /// A fresh database is stamped with the current schema version at
    /// construction, so it never pays the legacy table scans again.
    #[tokio::test]
    async fn test_fresh_database_stamped_with_current_schema_version() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        assert!(matches!(
            read_schema_stamps(&storage.db).await.unwrap(),
            Some(s) if s == SchemaStamps::current()
        ));
    }

    /// On a brand-new store the `kv` table is undefined; `read_schema_stamps` must
    /// map that to `None` (not a hard error) so the first opener can bootstrap.
    /// Exercised on a raw connection, exactly how a fresh store presents.
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
            "an undefined kv table must read as uninitialized, got {stamps:?}"
        );
    }

    /// A fresh store is built entirely by replaying the chain from v0 (the base
    /// schema is step `to: 1`). Running it on a bootstrap-only store must define
    /// every domain table; the later heal steps are no-ops on the empty store.
    #[tokio::test]
    async fn test_fresh_store_built_entirely_by_migrations() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();
        ensure_bootstrap(&db).await.unwrap();
        run_migration_steps(&db, 0).await.unwrap();
        for table in [
            "events",
            "assets",
            "asset_partitions",
            "runs",
            "dynamic_partitions",
            "ticks",
        ] {
            // A defined table yields rows (here, none); an undefined table
            // errors — so a clean empty read proves step `to: 1` created it.
            let mut r = db
                .query(format!("SELECT * FROM {table} LIMIT 1"))
                .await
                .unwrap_or_else(|e| panic!("table `{table}` is not defined: {e}"));
            let rows: Vec<serde_json::Value> = r
                .take(0)
                .unwrap_or_else(|e| panic!("table `{table}` is not defined: {e}"));
            assert!(rows.is_empty(), "fresh store table `{table}` must be empty");
        }
    }

    /// The ledger must be one step per version from v1 to [`SCHEMA_VERSION`],
    /// sorted and contiguous — else heals run out of order or a floor goes stale.
    #[test]
    fn test_migration_ledger_is_well_formed() {
        let tos: Vec<u32> = MIGRATION_STEPS.iter().map(|s| s.to).collect();
        assert_eq!(
            tos,
            (1..=SCHEMA_VERSION).collect::<Vec<_>>(),
            "MIGRATION_STEPS must hold exactly one entry per version in 1..=SCHEMA_VERSION \
             (the base schema is `to: 1`), sorted and contiguous (the last reaching \
             SCHEMA_VERSION); got {tos:?}"
        );
    }

    /// FNV-1a over `bytes` — a stable hash for pinning [`BASE_SCHEMA`]
    /// (`DefaultHasher` is not stable across releases, so roll our own).
    const fn fnv1a(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut i = 0;
        while i < bytes.len() {
            hash ^= bytes[i] as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            i += 1;
        }
        hash
    }

    /// [`BASE_SCHEMA`] is the frozen v1 baseline: a schema change is a new
    /// [`MIGRATION_STEPS`] entry, never an edit here — else a stamped database
    /// (which never re-applies the baseline) would diverge from a fresh one.
    #[test]
    fn test_base_schema_is_frozen() {
        const FROZEN: u64 = 0xf87c_b994_c069_9f60;
        assert_eq!(
            fnv1a(BASE_SCHEMA.as_bytes()),
            FROZEN,
            "BASE_SCHEMA changed — do not edit the v1 baseline. Add a MIGRATION_STEPS \
             entry that issues the DDL and bump SCHEMA_VERSION instead. If you are \
             deliberately re-baselining v1 (no released database has it yet), update \
             FROZEN to {:#018x}.",
            fnv1a(BASE_SCHEMA.as_bytes())
        );
    }

    /// Floors fold the per-step classes: the v1→v2 write-breaking step lifts
    /// `min_writer`; the v2→v3 additive step lifts neither.
    #[test]
    fn test_floors_for_version() {
        assert_eq!(floors_for_version(0), (1, 1), "no steps applied yet");
        assert_eq!(
            floors_for_version(1),
            (1, 1),
            "v0->v1 base schema is additive"
        );
        assert_eq!(floors_for_version(2), (1, 2), "v1->v2 is write-breaking");
        assert_eq!(floors_for_version(3), (1, 2), "v2->v3 is additive");
    }

    /// The open-time range check, exhaustively. `build` is fixed in production;
    /// here we sweep it against planted stamps to cover every branch.
    #[test]
    fn test_check_compatibility_matrix() {
        let s = |version, min_reader, min_writer| SchemaStamps {
            version,
            min_reader,
            min_writer,
        };
        use Capability::{Migrate, Read, ReadWrite};
        // At the database's own version: always fine.
        assert!(check_compatibility(s(3, 1, 2), ReadWrite, 3).is_ok());
        assert!(check_compatibility(s(3, 1, 2), Read, 3).is_ok());
        // Database ahead: a reader below min_writer but at/above min_reader is
        // fine; a writer below min_writer is refused (the reader/writer split).
        assert!(check_compatibility(s(5, 1, 5), Read, 3).is_ok());
        assert!(check_compatibility(s(5, 1, 5), ReadWrite, 3).is_err());
        // Build too old to even read.
        assert!(check_compatibility(s(5, 4, 5), Read, 3).is_err());
        // Build newer than the database → needs migration, for any non-migrate
        // capability (never silently proceed against an un-migrated schema).
        assert!(check_compatibility(s(2, 1, 2), ReadWrite, 3).is_err());
        assert!(check_compatibility(s(2, 1, 2), Read, 3).is_err());
        // Migrate: ahead-or-equal proceeds; a downgrade is refused.
        assert!(check_compatibility(s(2, 1, 2), Migrate, 3).is_ok());
        assert!(check_compatibility(s(3, 1, 2), Migrate, 3).is_ok());
        assert!(check_compatibility(s(5, 1, 5), Migrate, 3).is_err());
    }

    /// Reader/writer split: a DB far ahead for writers refuses an old `ReadWrite`
    /// opener but keeps an old read-only one (the UI) running.
    #[tokio::test]
    async fn test_open_allows_old_reader_but_refuses_old_writer() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        write_schema_stamps(
            &storage.db,
            SchemaStamps {
                version: SCHEMA_VERSION + 2,
                min_reader: 1,
                min_writer: SCHEMA_VERSION + 2,
            },
        )
        .await
        .unwrap();
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

    /// A build newer than the database refuses to open it (rather than silently
    /// migrating, as the old open path did) and names the fix.
    #[tokio::test]
    async fn test_open_refuses_when_database_is_behind_build() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        write_schema_stamps(&storage.db, SchemaStamps::for_version(SCHEMA_VERSION - 1))
            .await
            .unwrap();
        let err = ensure_compatible(&storage.db, Capability::ReadWrite)
            .await
            .unwrap_err();
        // The typed error is the contract the PyO3 boundary downcasts to map to
        // `SchemaMigrationNeededError` (which the `rivers dev` prompt catches).
        assert!(
            err.downcast_ref::<SchemaMigrationNeeded>().is_some(),
            "a behind-build open must surface the typed SchemaMigrationNeeded: {err}"
        );
        assert!(err.to_string().contains("rivers db migrate"), "{err}");
    }

    /// An uninitialized store is bootstrapped by the first opener, the read-only
    /// UI included.
    #[tokio::test]
    async fn test_uninitialized_store_is_initialized_by_a_reader() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        // Wipe the stamp the init wrote — back to an uninitialized store.
        storage
            .db
            .query("DELETE FROM kv")
            .await
            .unwrap()
            .check()
            .unwrap();
        // A read-only opener initializes it in place — no waiting, no error.
        ensure_compatible(&storage.db, Capability::Read)
            .await
            .unwrap();
        assert!(matches!(
            read_schema_stamps(&storage.db).await.unwrap(),
            Some(s) if s == SchemaStamps::current()
        ));
    }

    /// Explicit migrate is idempotent and refuses a downgrade.
    #[tokio::test]
    async fn test_migrate_is_idempotent_and_refuses_downgrade() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        // A second migrate on an already-current database is a no-op.
        migrate_to_current(&storage.db).await.unwrap();
        assert!(matches!(
            read_schema_stamps(&storage.db).await.unwrap(),
            Some(s) if s == SchemaStamps::current()
        ));
        // A database newer than this build must never be migrated (downgrade).
        write_schema_stamps(&storage.db, SchemaStamps::for_version(SCHEMA_VERSION + 1))
            .await
            .unwrap();
        assert!(
            migrate_to_current(&storage.db).await.is_err(),
            "downgrade must be refused"
        );
    }

    /// A database already at the current version must not re-run migrations:
    /// a planted unsorted row stays untouched.
    #[tokio::test]
    async fn test_migrations_skip_when_version_is_current() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e1', last_run_id: 'r1', last_timestamp: 1 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        // An explicit migrate on an already-current database is a no-op.
        migrate_to_current(&storage.db).await.unwrap();

        #[derive(Debug, SurrealValue)]
        struct PkRow {
            partition_key: PartitionKey,
        }
        let mut result = storage
            .db
            .query("SELECT partition_key FROM asset_partitions")
            .await
            .unwrap();
        let rows: Vec<PkRow> = result.take(0).unwrap();
        assert_eq!(rows.len(), 1);
        let PartitionKey::Multi { dims } = &rows[0].partition_key else {
            unreachable!()
        };
        assert_eq!(
            dims[0].0, "region",
            "a database at the current version must not be rescanned"
        );
    }

    /// A crash mid-migration must never lose a partition: the canonical row is
    /// written before any legacy row is deleted, so a sorted row is updated in
    /// place (its id survives) and the duplicate is removed afterwards.
    #[tokio::test]
    async fn test_migration_never_deletes_the_canonical_row() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
        let sorted = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        storage
            .upsert_asset_partitions(vec![DbAssetPartitionWrite {
                code_location_id: cl.to_string(),
                asset_key: "inv".to_string(),
                partition_key: sorted.clone(),
                last_event_id: "e1".to_string(),
                last_run_id: "r1".to_string(),
                last_timestamp: 1,
            }])
            .await
            .unwrap();
        #[derive(Debug, SurrealValue)]
        struct IdRow {
            id: RecordId,
            last_event_id: String,
            last_timestamp: i64,
        }
        let mut result = storage
            .db
            .query("SELECT id, last_event_id, last_timestamp FROM asset_partitions")
            .await
            .unwrap();
        let before: Vec<IdRow> = result.take(0).unwrap();
        assert_eq!(before.len(), 1);
        let canonical_id = before[0].id.clone();
        // Legacy duplicate with unsorted dims and newer values (raw CREATE
        // bypasses the canonicalizing serializer).
        storage
            .db
            .query(
                "CREATE asset_partitions CONTENT { code_location_id: 'default', \
                 asset_key: 'inv', partition_key: { variant: 'Multi', dims: \
                 [['region', ['eu']], ['date', ['2024-01-01']]] }, \
                 last_event_id: 'e9', last_run_id: 'r9', last_timestamp: 5 }",
            )
            .await
            .unwrap()
            .check()
            .unwrap();

        migrate_multi_partition_key_order(&storage.db)
            .await
            .unwrap();

        let mut result = storage
            .db
            .query("SELECT id, last_event_id, last_timestamp FROM asset_partitions")
            .await
            .unwrap();
        let after: Vec<IdRow> = result.take(0).unwrap();
        assert_eq!(after.len(), 1, "one canonical row survives");
        assert_eq!(
            after[0].id, canonical_id,
            "the canonical row must be updated in place, never deleted"
        );
        assert_eq!(after[0].last_event_id, "e9", "newest row's values win");
        assert_eq!(after[0].last_timestamp, 5);
    }
}
