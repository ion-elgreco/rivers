//! Log-storage benchmark for issue #24 — compare storing step logs as raw
//! strings in `events.metadata` (status quo) against SurrealDB's blob options:
//! an inline `bytes` field, a dedicated logs table (string/bytes), and the
//! experimental file/bucket API.
//!
//! Run:
//! ```sh
//! cargo test -p rivers-core --profile ci --test log_storage_bench -- --ignored --nocapture
//! ```
//!
//! Every scenario gets a fresh embedded RocksDB with the production V1 schema
//! applied, writes in batches of 64 (EventWriter::BATCH_SIZE) across 8 runs,
//! then reads logs back per run with the production queries.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use surrealdb::Surreal;
use surrealdb::engine::any::{self, Any};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealdb::types::{Bytes, File, Object, RecordId, SurrealValue};

const SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/storage/surrealdb_backend/migrations/V1__base.surql"
));

/// Mirrors EventWriter::BATCH_SIZE.
const BATCH: usize = 64;
const RUNS: usize = 8;
const STEPS_PER_RUN: usize = 8;

#[derive(Debug, Clone, SurrealValue)]
struct EventRow {
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    code_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
}

#[derive(Debug, Clone, SurrealValue)]
struct EventRowBlob {
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    code_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
    log_blob: Option<Bytes>,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct EventOut {
    id: RecordId,
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<Object>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    code_version: Option<String>,
    input_data_versions: Vec<(String, String)>,
    log_blob: Option<Bytes>,
}

#[derive(Debug, Clone, SurrealValue)]
struct LogRowStr {
    run_id: String,
    asset_key: String,
    timestamp: i64,
    stream: String,
    content: String,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct LogOutStr {
    id: RecordId,
    run_id: String,
    asset_key: String,
    timestamp: i64,
    stream: String,
    content: String,
}

#[derive(Debug, Clone, SurrealValue)]
struct LogRowBytes {
    run_id: String,
    asset_key: String,
    timestamp: i64,
    stream: String,
    content: Bytes,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct LogOutBytes {
    id: RecordId,
    run_id: String,
    asset_key: String,
    timestamp: i64,
    stream: String,
    content: Bytes,
}

#[derive(Debug, Clone, Copy, Default)]
struct Metrics {
    write_ms: f64,
    read_ms: f64,
    structured_ms: f64,
    disk_mb: f64,
}

fn make_log(size: usize, seed: usize) -> String {
    let mut s = String::with_capacity(size + 128);
    let mut i = 0usize;
    while s.len() < size {
        writeln!(
            s,
            "2026-07-17T12:00:00.{:06}Z INFO rivers.step worker={} iter={} processed batch rows={} elapsed_ms={}",
            i,
            seed,
            i,
            1000 + (i * 37) % 9000,
            (i * 13) % 400
        )
        .unwrap();
        i += 1;
    }
    s.truncate(size);
    s
}

fn run_id(i: usize) -> String {
    format!("run-{i}")
}

fn dir_size_mb(path: &Path) -> f64 {
    fn walk(p: &Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    total += walk(&path);
                } else if let Ok(md) = e.metadata() {
                    total += md.len();
                }
            }
        }
        total
    }
    walk(path) as f64 / (1024.0 * 1024.0)
}

async fn connect_db(dir: &Path, experimental: bool) -> Surreal<Any> {
    let url = format!("rocksdb://{}", dir.display());
    let db = if experimental {
        let mut caps = Capabilities::default();
        caps.allow_all_experimental_features();
        any::connect((url, Config::new().capabilities(caps)))
            .await
            .expect("connect")
    } else {
        any::connect(url).await.expect("connect")
    };
    db.use_ns("rivers").use_db("rivers").await.expect("use ns/db");
    db.query(SCHEMA).await.expect("apply schema").check().expect("schema ok");
    db
}

fn log_event_row(run: usize, idx: usize, payload: Option<String>) -> EventRow {
    EventRow {
        code_location_id: "default".into(),
        event_type: "LogOutput".into(),
        asset_key: Some(format!("asset_{}", idx % STEPS_PER_RUN)),
        run_id: run_id(run),
        timestamp: idx as i64,
        sort_order: 1,
        metadata: payload.map(|p| vec![("stdout".to_string(), p)]).unwrap_or_default(),
        data_version: None,
        code_version: None,
        input_data_versions: vec![],
    }
}

fn structured_rows() -> Vec<EventRow> {
    let mut rows = Vec::new();
    for run in 0..RUNS {
        for step in 0..STEPS_PER_RUN {
            for (event_type, sort_order) in [("StepStart", 0i64), ("StepSuccess", 4)] {
                rows.push(EventRow {
                    code_location_id: "default".into(),
                    event_type: event_type.into(),
                    asset_key: Some(format!("asset_{step}")),
                    run_id: run_id(run),
                    timestamp: step as i64,
                    sort_order,
                    metadata: vec![],
                    data_version: None,
                    code_version: None,
                    input_data_versions: vec![],
                });
            }
        }
    }
    rows
}

async fn seed_structured(db: &Surreal<Any>) {
    db.query("INSERT INTO events $rows RETURN NONE")
        .bind(("rows", structured_rows()))
        .await
        .expect("seed structured")
        .check()
        .expect("seed structured ok");
}

/// Production structured-events page query, timed across all runs.
async fn probe_structured(db: &Surreal<Any>) -> f64 {
    let t = Instant::now();
    for run in 0..RUNS {
        let mut res = db
            .query(
                "SELECT * FROM events WHERE run_id = $id AND event_type != 'LogOutput' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT 50 START 0",
            )
            .bind(("id", run_id(run)))
            .await
            .expect("structured query");
        let rows: Vec<EventOut> = res.take(0).expect("structured take");
        assert_eq!(rows.len(), STEPS_PER_RUN * 2);
    }
    t.elapsed().as_secs_f64() * 1e3
}

/// Status quo: payload as a string in `events.metadata`. `return_full` mirrors
/// `store_events` (SDK insert returns every inserted row); otherwise RETURN NONE.
async fn bench_events_string(
    dir: &Path,
    n_events: usize,
    payload_size: usize,
    return_full: bool,
) -> Metrics {
    let db = connect_db(dir, false).await;
    let rows: Vec<EventRow> = (0..n_events)
        .map(|i| log_event_row(i % RUNS, i, Some(make_log(payload_size, i))))
        .collect();

    let t = Instant::now();
    for chunk in rows.chunks(BATCH) {
        if return_full {
            let mut res = db
                .query("INSERT INTO events $rows")
                .bind(("rows", chunk.to_vec()))
                .await
                .expect("insert");
            let returned: Vec<EventOut> = res.take(0).expect("insert take");
            assert_eq!(returned.len(), chunk.len());
        } else {
            db.query("INSERT INTO events $rows RETURN NONE")
                .bind(("rows", chunk.to_vec()))
                .await
                .expect("insert")
                .check()
                .expect("insert ok");
        }
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    seed_structured(&db).await;

    let t = Instant::now();
    let mut total_bytes = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query(
                "SELECT * FROM events WHERE run_id = $id AND event_type = 'LogOutput' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(("id", run_id(run)))
            .await
            .expect("log query");
        let rows: Vec<EventOut> = res.take(0).expect("log take");
        for r in &rows {
            total_bytes += r.metadata.iter().map(|(_, v)| v.len()).sum::<usize>();
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total_bytes, n_events * payload_size);

    let structured_ms = probe_structured(&db).await;
    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms,
        disk_mb: dir_size_mb(dir),
    }
}

/// Payload moved to a dedicated `bytes` field on the same events rows.
async fn bench_events_bytes(dir: &Path, n_events: usize, payload_size: usize) -> Metrics {
    let db = connect_db(dir, false).await;
    db.query("DEFINE FIELD IF NOT EXISTS log_blob ON events TYPE option<bytes>;")
        .await
        .expect("define log_blob")
        .check()
        .expect("define log_blob ok");

    let rows: Vec<EventRowBlob> = (0..n_events)
        .map(|i| {
            let base = log_event_row(i % RUNS, i, None);
            EventRowBlob {
                code_location_id: base.code_location_id,
                event_type: base.event_type,
                asset_key: base.asset_key,
                run_id: base.run_id,
                timestamp: base.timestamp,
                sort_order: base.sort_order,
                metadata: vec![],
                data_version: None,
                code_version: None,
                input_data_versions: vec![],
                log_blob: Some(Bytes::from(make_log(payload_size, i).into_bytes())),
            }
        })
        .collect();

    let t = Instant::now();
    for chunk in rows.chunks(BATCH) {
        db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", chunk.to_vec()))
            .await
            .expect("insert")
            .check()
            .expect("insert ok");
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    seed_structured(&db).await;

    let t = Instant::now();
    let mut total_bytes = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query(
                "SELECT * FROM events WHERE run_id = $id AND event_type = 'LogOutput' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(("id", run_id(run)))
            .await
            .expect("log query");
        let rows: Vec<EventOut> = res.take(0).expect("log take");
        for r in &rows {
            total_bytes += r.log_blob.as_ref().map(|b| b.len()).unwrap_or(0);
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total_bytes, n_events * payload_size);

    let structured_ms = probe_structured(&db).await;
    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms,
        disk_mb: dir_size_mb(dir),
    }
}

const RUN_LOGS_SCHEMA_STR: &str = "
DEFINE TABLE IF NOT EXISTS run_logs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS run_id ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON run_logs TYPE int;
DEFINE FIELD IF NOT EXISTS stream ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS content ON run_logs TYPE string;
DEFINE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs FIELDS run_id;
";

const RUN_LOGS_SCHEMA_BYTES: &str = "
DEFINE TABLE IF NOT EXISTS run_logs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS run_id ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS asset_key ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON run_logs TYPE int;
DEFINE FIELD IF NOT EXISTS stream ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS content ON run_logs TYPE bytes;
DEFINE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs FIELDS run_id;
";

/// Separate lightly-indexed logs table; no LogOutput rows in events at all.
async fn bench_logs_table(
    dir: &Path,
    n_events: usize,
    payload_size: usize,
    as_bytes: bool,
) -> Metrics {
    let db = connect_db(dir, false).await;
    let schema = if as_bytes {
        RUN_LOGS_SCHEMA_BYTES
    } else {
        RUN_LOGS_SCHEMA_STR
    };
    db.query(schema).await.expect("run_logs schema").check().expect("run_logs ok");

    let rows_bytes: Vec<LogRowBytes> = if as_bytes {
        (0..n_events)
            .map(|i| LogRowBytes {
                run_id: run_id(i % RUNS),
                asset_key: format!("asset_{}", i % STEPS_PER_RUN),
                timestamp: i as i64,
                stream: "stdout".into(),
                content: Bytes::from(make_log(payload_size, i).into_bytes()),
            })
            .collect()
    } else {
        vec![]
    };
    let rows_str: Vec<LogRowStr> = if as_bytes {
        vec![]
    } else {
        (0..n_events)
            .map(|i| LogRowStr {
                run_id: run_id(i % RUNS),
                asset_key: format!("asset_{}", i % STEPS_PER_RUN),
                timestamp: i as i64,
                stream: "stdout".into(),
                content: make_log(payload_size, i),
            })
            .collect()
    };

    let t = Instant::now();
    if as_bytes {
        for chunk in rows_bytes.chunks(BATCH) {
            db.query("INSERT INTO run_logs $rows RETURN NONE")
                .bind(("rows", chunk.to_vec()))
                .await
                .expect("insert")
                .check()
                .expect("insert ok");
        }
    } else {
        for chunk in rows_str.chunks(BATCH) {
            db.query("INSERT INTO run_logs $rows RETURN NONE")
                .bind(("rows", chunk.to_vec()))
                .await
                .expect("insert")
                .check()
                .expect("insert ok");
        }
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    seed_structured(&db).await;

    let t = Instant::now();
    let mut total_bytes = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query("SELECT * FROM run_logs WHERE run_id = $id ORDER BY timestamp ASC")
            .bind(("id", run_id(run)))
            .await
            .expect("log query");
        if as_bytes {
            let rows: Vec<LogOutBytes> = res.take(0).expect("log take");
            total_bytes += rows.iter().map(|r| r.content.len()).sum::<usize>();
        } else {
            let rows: Vec<LogOutStr> = res.take(0).expect("log take");
            total_bytes += rows.iter().map(|r| r.content.len()).sum::<usize>();
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total_bytes, n_events * payload_size);

    let structured_ms = probe_structured(&db).await;
    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms,
        disk_mb: dir_size_mb(dir),
    }
}

/// Experimental file/bucket API: payload via file::put, events row keeps a pointer.
async fn bench_bucket(
    dir: &Path,
    bucket_dir: &Path,
    n_events: usize,
    payload_size: usize,
) -> Metrics {
    let db = connect_db(dir, true).await;
    db.query(format!(
        "DEFINE BUCKET IF NOT EXISTS logs BACKEND \"file://{}\";",
        bucket_dir.display()
    ))
    .await
    .expect("define bucket")
    .check()
    .expect("define bucket ok");

    let mut payloads: Vec<(String, Bytes)> = Vec::with_capacity(n_events);
    let mut pointer_rows: Vec<EventRow> = Vec::with_capacity(n_events);
    for i in 0..n_events {
        let key = format!("{}/{}.log", run_id(i % RUNS), i);
        payloads.push((
            key.clone(),
            Bytes::from(make_log(payload_size, i).into_bytes()),
        ));
        let mut row = log_event_row(i % RUNS, i, None);
        row.metadata = vec![("log_file".to_string(), key)];
        pointer_rows.push(row);
    }

    let t = Instant::now();
    for (key, data) in payloads {
        db.query("RETURN file::put($f, $data);")
            .bind(("f", File::new("logs", key)))
            .bind(("data", data))
            .await
            .expect("file::put")
            .check()
            .expect("file::put ok");
    }
    for chunk in pointer_rows.chunks(BATCH) {
        db.query("INSERT INTO events $rows RETURN NONE")
            .bind(("rows", chunk.to_vec()))
            .await
            .expect("insert")
            .check()
            .expect("insert ok");
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    seed_structured(&db).await;

    let t = Instant::now();
    let mut total_bytes = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query(
                "SELECT * FROM events WHERE run_id = $id AND event_type = 'LogOutput' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC",
            )
            .bind(("id", run_id(run)))
            .await
            .expect("log query");
        let rows: Vec<EventOut> = res.take(0).expect("log take");
        for r in &rows {
            let key = r
                .metadata
                .iter()
                .find(|(k, _)| k == "log_file")
                .map(|(_, v)| v.clone())
                .expect("pointer");
            let mut res = db
                .query("RETURN file::get($f);")
                .bind(("f", File::new("logs", key)))
                .await
                .expect("file::get");
            let data: Option<Bytes> = res.take(0).expect("file::get take");
            total_bytes += data.expect("file missing").len();
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total_bytes, n_events * payload_size);

    let structured_ms = probe_structured(&db).await;
    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms,
        disk_mb: dir_size_mb(dir) + dir_size_mb(bucket_dir),
    }
}

fn fresh_dir(base: &Path, name: &str) -> PathBuf {
    let dir = base.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scenario dir");
    dir
}

#[tokio::test]
#[ignore = "benchmark — run explicitly with --ignored --nocapture"]
async fn log_storage_bench() {
    let base = PathBuf::from("/tmp/rivers_log_bench");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("create base dir");
    let canonical_base = base.canonicalize().expect("canonicalize base");

    // The bucket file backend checks canonical paths against this allowlist.
    // The LazyLock reading it is first dereferenced on bucket connect, which
    // happens well after this point.
    unsafe {
        std::env::set_var(
            "SURREAL_BUCKET_FOLDER_ALLOWLIST",
            format!("{}:{}", base.display(), canonical_base.display()),
        );
    }

    let cases: [(&str, usize, usize); 3] = [
        ("1KiB", 1024, 512),
        ("64KiB", 64 * 1024, 512),
        ("1MiB", 1024 * 1024, 128),
    ];

    for (label, payload_size, n_events) in cases {
        let total_mb = (payload_size * n_events) as f64 / (1024.0 * 1024.0);
        println!(
            "\n=== payload {label} × {n_events} events ({total_mb:.0} MiB total, {RUNS} runs, batch {BATCH}) ==="
        );
        println!(
            "{:<28} {:>10} {:>10} {:>12} {:>9}",
            "scenario", "write ms", "read ms", "struct-page", "disk MB"
        );

        let dir = fresh_dir(&canonical_base, &format!("db_str_full_{label}"));
        let m = bench_events_string(&dir, n_events, payload_size, true).await;
        print_row("events/string/return-full", m);

        let dir = fresh_dir(&canonical_base, &format!("db_str_none_{label}"));
        let m = bench_events_string(&dir, n_events, payload_size, false).await;
        print_row("events/string/return-none", m);

        let dir = fresh_dir(&canonical_base, &format!("db_bytes_{label}"));
        let m = bench_events_bytes(&dir, n_events, payload_size).await;
        print_row("events/bytes/return-none", m);

        let dir = fresh_dir(&canonical_base, &format!("db_logtbl_str_{label}"));
        let m = bench_logs_table(&dir, n_events, payload_size, false).await;
        print_row("logs-table/string", m);

        let dir = fresh_dir(&canonical_base, &format!("db_logtbl_bytes_{label}"));
        let m = bench_logs_table(&dir, n_events, payload_size, true).await;
        print_row("logs-table/bytes", m);

        let dir = fresh_dir(&canonical_base, &format!("db_bucket_{label}"));
        let bucket_dir = fresh_dir(&canonical_base, &format!("bucket_{label}"));
        let m = bench_bucket(&dir, &bucket_dir, n_events, payload_size).await;
        print_row("bucket/file-api", m);
    }
}

fn print_row(name: &str, m: Metrics) {
    println!(
        "{:<28} {:>10.1} {:>10.1} {:>12.2} {:>9.1}",
        name, m.write_ms, m.read_ms, m.structured_ms, m.disk_mb
    );
}

// ── run_logs table design choices (issue #24 follow-up) ──
//
// Per step, three captured streams: stdout = S, stderr = S/4, logs = S/2.
// Compares: one table with a stream column (row per stream) vs three tables
// vs one wide row per step; and idx(run_id) vs idx(run_id, timestamp).

#[derive(Debug, Clone, SurrealValue)]
struct StreamRow {
    code_location_id: String,
    run_id: String,
    step_key: String,
    stream: String,
    timestamp: i64,
    content: String,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct StreamRowOut {
    id: RecordId,
    code_location_id: String,
    run_id: String,
    step_key: String,
    stream: String,
    timestamp: i64,
    content: String,
}

#[derive(Debug, Clone, SurrealValue)]
struct WideRow {
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct WideRowOut {
    id: RecordId,
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
}

const STREAM_TABLE_SCHEMA: &str = "
DEFINE TABLE IF NOT EXISTS run_logs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON run_logs TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS run_id ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS stream ON run_logs TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON run_logs TYPE int;
DEFINE FIELD IF NOT EXISTS content ON run_logs TYPE string;
DEFINE INDEX IF NOT EXISTS idx_run_logs_run ON run_logs FIELDS run_id;
";

const STREAM_TABLE_TS_IDX: &str =
    "DEFINE INDEX IF NOT EXISTS idx_run_logs_run_ts ON run_logs FIELDS run_id, timestamp;";

fn three_tables_schema() -> String {
    ["run_stdout", "run_stderr", "run_rustlogs"]
        .iter()
        .map(|t| {
            format!(
                "DEFINE TABLE IF NOT EXISTS {t} SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON {t} TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS run_id ON {t} TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON {t} TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON {t} TYPE int;
DEFINE FIELD IF NOT EXISTS content ON {t} TYPE string;
DEFINE INDEX IF NOT EXISTS idx_{t}_run ON {t} FIELDS run_id;
"
            )
        })
        .collect()
}

const WIDE_TABLE_SCHEMA: &str = "
DEFINE TABLE IF NOT EXISTS run_logs_wide SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS code_location_id ON run_logs_wide TYPE string DEFAULT 'default';
DEFINE FIELD IF NOT EXISTS run_id ON run_logs_wide TYPE string;
DEFINE FIELD IF NOT EXISTS step_key ON run_logs_wide TYPE string;
DEFINE FIELD IF NOT EXISTS timestamp ON run_logs_wide TYPE int;
DEFINE FIELD IF NOT EXISTS stdout ON run_logs_wide TYPE option<string>;
DEFINE FIELD IF NOT EXISTS stderr ON run_logs_wide TYPE option<string>;
DEFINE FIELD IF NOT EXISTS logs ON run_logs_wide TYPE option<string>;
DEFINE INDEX IF NOT EXISTS idx_run_logs_wide_run ON run_logs_wide FIELDS run_id;
";

/// (stdout, stderr, logs) payloads for one step.
fn step_streams(base: usize, seed: usize) -> (String, String, String) {
    (
        make_log(base, seed),
        make_log(base / 4, seed + 1),
        make_log(base / 2, seed + 2),
    )
}

fn expected_total(base: usize, n_steps: usize) -> usize {
    n_steps * (base + base / 4 + base / 2)
}

async fn bench_stream_rows(dir: &Path, n_steps: usize, base: usize, ts_index: bool) -> Metrics {
    let db = connect_db(dir, false).await;
    db.query(STREAM_TABLE_SCHEMA).await.expect("schema").check().expect("schema ok");
    if ts_index {
        db.query(STREAM_TABLE_TS_IDX).await.expect("idx").check().expect("idx ok");
    }

    let mut rows: Vec<StreamRow> = Vec::with_capacity(n_steps * 3);
    for i in 0..n_steps {
        let (stdout, stderr, logs) = step_streams(base, i * 3);
        for (stream, content) in [("stdout", stdout), ("stderr", stderr), ("logs", logs)] {
            rows.push(StreamRow {
                code_location_id: "default".into(),
                run_id: run_id(i % RUNS),
                step_key: format!("asset_{}", i % STEPS_PER_RUN),
                stream: stream.into(),
                timestamp: i as i64,
                content,
            });
        }
    }

    let t = Instant::now();
    for chunk in rows.chunks(BATCH) {
        db.query("INSERT INTO run_logs $rows RETURN NONE")
            .bind(("rows", chunk.to_vec()))
            .await
            .expect("insert")
            .check()
            .expect("insert ok");
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let mut total = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query("SELECT * FROM run_logs WHERE run_id = $id ORDER BY timestamp ASC")
            .bind(("id", run_id(run)))
            .await
            .expect("read");
        let out: Vec<StreamRowOut> = res.take(0).expect("take");
        total += out.iter().map(|r| r.content.len()).sum::<usize>();
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total, expected_total(base, n_steps));

    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms: 0.0,
        disk_mb: dir_size_mb(dir),
    }
}

#[derive(Debug, Clone, SurrealValue)]
struct NoStreamRow {
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    content: String,
}

#[derive(Debug, SurrealValue)]
#[allow(dead_code)]
struct NoStreamRowOut {
    id: RecordId,
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    content: String,
}

async fn bench_three_tables(dir: &Path, n_steps: usize, base: usize) -> Metrics {
    let db = connect_db(dir, false).await;
    db.query(three_tables_schema()).await.expect("schema").check().expect("schema ok");

    let mut per_table: [Vec<NoStreamRow>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for i in 0..n_steps {
        let (stdout, stderr, logs) = step_streams(base, i * 3);
        for (slot, content) in [(0usize, stdout), (1, stderr), (2, logs)] {
            per_table[slot].push(NoStreamRow {
                code_location_id: "default".into(),
                run_id: run_id(i % RUNS),
                step_key: format!("asset_{}", i % STEPS_PER_RUN),
                timestamp: i as i64,
                content,
            });
        }
    }

    let tables = ["run_stdout", "run_stderr", "run_rustlogs"];
    let t = Instant::now();
    for (slot, table) in tables.iter().enumerate() {
        for chunk in per_table[slot].chunks(BATCH) {
            db.query(format!("INSERT INTO {table} $rows RETURN NONE"))
                .bind(("rows", chunk.to_vec()))
                .await
                .expect("insert")
                .check()
                .expect("insert ok");
        }
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let mut total = 0usize;
    for run in 0..RUNS {
        for table in tables {
            let mut res = db
                .query(format!(
                    "SELECT * FROM {table} WHERE run_id = $id ORDER BY timestamp ASC"
                ))
                .bind(("id", run_id(run)))
                .await
                .expect("read");
            let out: Vec<NoStreamRowOut> = res.take(0).expect("take");
            total += out.iter().map(|r| r.content.len()).sum::<usize>();
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total, expected_total(base, n_steps));

    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms: 0.0,
        disk_mb: dir_size_mb(dir),
    }
}

async fn bench_wide_rows(dir: &Path, n_steps: usize, base: usize) -> Metrics {
    let db = connect_db(dir, false).await;
    db.query(WIDE_TABLE_SCHEMA).await.expect("schema").check().expect("schema ok");

    let rows: Vec<WideRow> = (0..n_steps)
        .map(|i| {
            let (stdout, stderr, logs) = step_streams(base, i * 3);
            WideRow {
                code_location_id: "default".into(),
                run_id: run_id(i % RUNS),
                step_key: format!("asset_{}", i % STEPS_PER_RUN),
                timestamp: i as i64,
                stdout: Some(stdout),
                stderr: Some(stderr),
                logs: Some(logs),
            }
        })
        .collect();

    let t = Instant::now();
    for chunk in rows.chunks(BATCH) {
        db.query("INSERT INTO run_logs_wide $rows RETURN NONE")
            .bind(("rows", chunk.to_vec()))
            .await
            .expect("insert")
            .check()
            .expect("insert ok");
    }
    let write_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let mut total = 0usize;
    for run in 0..RUNS {
        let mut res = db
            .query("SELECT * FROM run_logs_wide WHERE run_id = $id ORDER BY timestamp ASC")
            .bind(("id", run_id(run)))
            .await
            .expect("read");
        let out: Vec<WideRowOut> = res.take(0).expect("take");
        for r in &out {
            total += r.stdout.as_ref().map_or(0, |s| s.len())
                + r.stderr.as_ref().map_or(0, |s| s.len())
                + r.logs.as_ref().map_or(0, |s| s.len());
        }
    }
    let read_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(total, expected_total(base, n_steps));

    drop(db);
    Metrics {
        write_ms,
        read_ms,
        structured_ms: 0.0,
        disk_mb: dir_size_mb(dir),
    }
}

#[tokio::test]
#[ignore = "benchmark — run explicitly with --ignored --nocapture"]
async fn run_logs_design_bench() {
    let base_dir = PathBuf::from("/tmp/rivers_log_bench_design");
    let _ = std::fs::remove_dir_all(&base_dir);
    std::fs::create_dir_all(&base_dir).expect("create base dir");
    let base_dir = base_dir.canonicalize().expect("canonicalize");

    let cases: [(&str, usize, usize); 3] = [
        ("1KiB", 1024, 512),
        ("64KiB", 64 * 1024, 512),
        ("1MiB", 1024 * 1024, 128),
    ];

    for (label, base, n_steps) in cases {
        let total_mb = expected_total(base, n_steps) as f64 / (1024.0 * 1024.0);
        println!(
            "\n=== stdout {label}/step (+ stderr ¼, logs ½) × {n_steps} steps ({total_mb:.0} MiB total) ==="
        );
        println!(
            "{:<28} {:>10} {:>10} {:>9}",
            "scenario", "write ms", "read ms", "disk MB"
        );

        let dir = fresh_dir(&base_dir, &format!("stream_{label}"));
        let m = bench_stream_rows(&dir, n_steps, base, false).await;
        print_design_row("one-table/stream-rows", m);

        let dir = fresh_dir(&base_dir, &format!("stream_ts_{label}"));
        let m = bench_stream_rows(&dir, n_steps, base, true).await;
        print_design_row("one-table/stream+ts-idx", m);

        let dir = fresh_dir(&base_dir, &format!("three_{label}"));
        let m = bench_three_tables(&dir, n_steps, base).await;
        print_design_row("three-tables", m);

        let dir = fresh_dir(&base_dir, &format!("wide_{label}"));
        let m = bench_wide_rows(&dir, n_steps, base).await;
        print_design_row("wide-row-per-step", m);
    }
}

fn print_design_row(name: &str, m: Metrics) {
    println!(
        "{:<28} {:>10.1} {:>10.1} {:>9.1}",
        name, m.write_ms, m.read_ms, m.disk_mb
    );
}
