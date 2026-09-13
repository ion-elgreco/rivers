//! Storage backend benchmark: SurrealDB against PostgreSQL.
//!
//! Measures the storage layer only — every method on the `AnyStorage`
//! surface. It says nothing about scheduling, execution, or the UI.
//!
//! Both backends implement one `StorageBackend` trait and pass one conformance
//! suite, so the same scenario code runs against either. Only the URL changes.
//!
//! ## Comparing fairly
//!
//! Embedded SurrealDB runs inside the process. A PostgreSQL server runs across
//! a socket. Timing those against each other measures the socket, not the
//! backend. So the honest comparison is **server against server** — a SurrealDB
//! server and a PostgreSQL server, both reached the same way. Embedded
//! SurrealDB is worth measuring too, but as the local-development baseline it
//! actually is, labelled as such.
//!
//! ```text
//! docker run -d --name bench-pg   -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench \
//!     -p 55433:5432 postgres:18-alpine
//! docker run -d --name bench-surreal -p 8100:8000 surrealdb/surrealdb:v3 \
//!     start --user root --pass root
//!
//! cargo run --release -p rivers-bench-storage -- \
//!     --postgres postgres://postgres:bench@localhost:55433/bench \
//!     --surreal  ws://localhost:8100 \
//!     --embedded
//! ```

mod fixtures;
mod harness;
mod scenarios;

use anyhow::{Context, Result};
use clap::Parser;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::migration::Capability;
use rivers_core::storage::url::StorageUrl;
use rivers_core::storage::{CodeLocationContext, ScopedStorageHandle};
use std::sync::Arc;

use fixtures::Config;

/// One code location, because every per-location query shape is identical
/// across locations and a second would only dilute the numbers.
const CL: &str = "bench";

#[derive(Parser)]
#[command(
    name = "rivers-bench-storage",
    about = "Compare rivers storage backends on the paths profiling found expensive"
)]
struct Args {
    /// PostgreSQL server URL, e.g. postgres://user:pass@host:5432/db
    #[arg(long)]
    postgres: Option<String>,

    /// SurrealDB server URL, e.g. ws://localhost:8000
    #[arg(long)]
    surreal: Option<String>,

    /// Database-scoped SurrealDB user. A `ws://` URL carries no credentials,
    /// so they come in separately — unlike PostgreSQL, which embeds them.
    #[arg(long, default_value = "bench")]
    surreal_user: String,

    #[arg(long, default_value = "bench")]
    surreal_pass: String,

    /// Also measure embedded SurrealDB, reported as the local-dev baseline.
    #[arg(long)]
    embedded: bool,

    /// Timed iterations per scenario.
    #[arg(long, default_value_t = 10)]
    iters: usize,

    /// Untimed runs before timing starts. The first call on a pool pays for
    /// the handshake and, on PostgreSQL, for planning a statement never seen
    /// before.
    #[arg(long, default_value_t = 2)]
    warmup: usize,

    /// Events per `store_events` call.
    #[arg(long, default_value_t = 500)]
    event_batch: usize,

    /// Rows seeded before the read scenarios. Reads against an empty table
    /// make every backend look identical.
    #[arg(long, default_value_t = 5_000)]
    seed_rows: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config {
        warmup: args.warmup,
        iters: args.iters,
        event_batch: args.event_batch,
        seed_rows: args.seed_rows,
    };

    // Server backends first, so the report's "vs first" column compares like
    // with like and the embedded row reads as the outlier it is.
    let mut targets: Vec<(String, String)> = Vec::new();
    if let Some(url) = &args.surreal {
        targets.push(("SurrealDB server".to_string(), url.clone()));
    }
    if let Some(url) = &args.postgres {
        targets.push(("PostgreSQL server".to_string(), url.clone()));
    }
    let tempdir = tempdir_path();
    if args.embedded {
        targets.push((
            "SurrealDB embedded".to_string(),
            format!("rocksdb://{tempdir}"),
        ));
    }
    anyhow::ensure!(
        !targets.is_empty(),
        "nothing to measure: pass at least one of --surreal, --postgres, --embedded"
    );

    println!("rivers storage benchmark");
    println!(
        "  {} timed iterations after {} warmup; {} events per batch; {} seed rows",
        cfg.iters, cfg.warmup, cfg.event_batch, cfg.seed_rows
    );

    let mut rows = Vec::new();
    for (name, url) in &targets {
        println!("\n==> {name}");
        let mut parsed: StorageUrl = url
            .parse()
            .with_context(|| format!("parsing the {name} url"))?;
        // Both servers authenticate, so neither gets a free pass on the
        // connection handshake the other pays for.
        if let StorageUrl::SurrealRemote(cfg) = parsed {
            parsed = StorageUrl::SurrealRemote(
                cfg.with_credentials(args.surreal_user.clone(), args.surreal_pass.clone()),
            );
        }
        let redacted = parsed.redacted();
        println!("    connecting to {redacted}");
        let storage = AnyStorage::open(parsed, Capability::Migrate)
            .await
            .with_context(|| format!("connecting to {name} at {redacted}"))?;
        let handle = ScopedStorageHandle::new(Arc::new(storage), CodeLocationContext::new(CL));

        println!("    seeding {} rows", cfg.seed_rows);
        fixtures::seed(&handle, &cfg).await?;

        println!("    measuring");
        rows.extend(scenarios::run_all(&handle, name, &cfg).await?);
    }

    harness::report(&rows);
    println!();
    Ok(())
}

/// A fresh directory for the embedded run. Reusing one would measure a store
/// already warm from a previous run.
fn tempdir_path() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("rivers-bench-storage-{nanos}"))
        .to_string_lossy()
        .into_owned()
}
