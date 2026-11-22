//! SSR entry point for the rivers web UI.
//!
//! Embeds the compiled WASM/JS client bundle and serves it alongside the
//! Leptos+Axum server-rendered application.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser};
use rivers_core::storage::StorageBackend;
use rivers_core::storage::any::AnyStorage;
use rivers_core::storage::migration::Capability;
use rivers_core::storage::url::{StorageUrl, redact_password};
use rivers_ui::code_location_registry::Registry;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rivers-ui", about = "rivers web UI")]
struct Args {
    #[arg(long, default_value = ".rivers/storage/")]
    storage_path: String,

    /// Connect to a SurrealDB server instead of embedded storage.
    #[arg(long)]
    surreal_endpoint: Option<String>,

    /// Storage URL; its scheme selects the backend (`postgres://`, `ws://`,
    /// `surrealkv://`). Pass it as the env var, so that its password is not
    /// in process lists.
    #[arg(long, env = "RIVERS_STORAGE_URL", hide_env_values = true)]
    storage_url: Option<String>,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value_t = 3000)]
    port: u16,

    /// Generate a synthetic graph with approximately N nodes instead of reading from storage.
    /// Accepts: 100, 1k, 10k, 50k or any number.
    #[arg(long)]
    synthetic: Option<String>,

    /// URL of the operator-hosted `CodeLocationRegistry` gRPC service.
    /// Example: `http://rivers-operator-registry.rivers.svc:50052`.
    /// If omitted the UI starts with no known code locations.
    #[arg(long, env = "RIVERS_REGISTRY_URL")]
    registry_url: Option<String>,

    /// Bearer token for the registry. Required when `--registry-url` is set.
    /// Passed as an env var so it doesn't land in process lists.
    #[arg(long, env = "RIVERS_REGISTRY_TOKEN", hide_env_values = true)]
    registry_token: Option<String>,
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(rivers_core::storage::surrealdb_backend::SURREALDB_STACK_SIZE)
        .build()
        .expect("failed to build the tokio runtime")
        .block_on(serve());
}

/// The store to open. The source decides first: the command line, then the
/// environment, then the default. Within one source the URL wins over the
/// endpoint, and the endpoint over the path. A SurrealDB URL takes namespace,
/// database and credentials from the `RIVERS_SURREAL_*` env vars that the
/// operator stamps on every rivers pod.
fn storage_location(
    args: &Args,
    url_on_command_line: bool,
    path_on_command_line: bool,
) -> Result<StorageUrl, String> {
    let resolve =
        |raw: &str| rivers_k8s::env::resolve_storage_url(raw).map_err(|e| format!("{e:#}"));
    let endpoint = match args.surreal_endpoint.as_deref() {
        None => None,
        Some(raw) => match resolve(raw) {
            Ok(url @ StorageUrl::SurrealRemote(_)) => Some((raw.to_string(), url)),
            _ => {
                return Err(format!(
                    "--surreal-endpoint takes a SurrealDB server (ws://, wss://, http:// or \
                     https://), not {}; pass any other storage URL with --storage-url",
                    redact_password(raw)
                ));
            }
        },
    };
    let url = match args.storage_url.as_deref() {
        None => None,
        Some(raw) => Some((raw.to_string(), resolve(raw)?)),
    };
    let path = (
        format!("surrealkv://{}", args.storage_path),
        StorageUrl::SurrealEmbedded {
            path: args.storage_path.clone(),
        },
    );

    let on_command_line: Vec<_> = [
        ("--storage-url", url.clone().filter(|_| url_on_command_line)),
        ("--surreal-endpoint", endpoint),
        (
            "--storage-path",
            Some(path.clone()).filter(|_| path_on_command_line),
        ),
    ]
    .into_iter()
    .filter_map(|(flag, setting)| Some((flag, setting?)))
    .collect();
    if let [(flag, (location, chosen)), rest @ ..] = on_command_line.as_slice() {
        if let Some((other, _)) = rest.iter().find(|(_, (other, _))| other != location) {
            return Err(format!(
                "{flag} and {other} name different stores; pass one of them"
            ));
        }
        return Ok(chosen.clone());
    }
    Ok(url.map_or(path.1, |(_, url)| url))
}

async fn serve() {
    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    let on_command_line = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
    let url = storage_location(
        &args,
        on_command_line("storage_url"),
        on_command_line("storage_path"),
    )
    .unwrap_or_else(|msg| {
        Args::command()
            .error(clap::error::ErrorKind::ArgumentConflict, msg)
            .exit()
    });

    // Default filter mirrors the Python crate (`python/src/lib.rs`) so the
    // standalone UI binary surfaces the same `rivers::*` events that the
    // in-process `rivers dev` UI does. Without this init, every existing
    // `tracing::info!` call in the UI server (start, shutdown, registry,
    // storage) is silent — making K8s deployments unobservable.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("rivers=info,rivers_core=info,rivers_ui=info,warn")
            }),
        )
        .with_target(true)
        .compact()
        .init();

    let backend = url.backend_name();
    let location = url.redacted();
    let remote = url.is_remote();
    // The production UI is a read-only storage consumer; writes go through
    // gRPC to code locations. Open `Read` so a write-breaking migration for
    // newer writers does not lock the UI out.
    let storage = AnyStorage::open(url, Capability::Read)
        .await
        .expect("Failed to open storage");
    if remote
        && let Err(error) = storage
            .record_component("ui", false, rivers_core::util::now_ts())
            .await
    {
        tracing::warn!(%error, "could not record the UI's build in storage");
    }
    tracing::info!(
        target: "rivers::storage",
        backend,
        %location,
        "storage ready"
    );
    let storage = Arc::new(storage);

    // Synthetic mode is a developer override that pins a fixed graph for
    // every code location. In production the standalone UI is multi-CL
    // and reads the active CL's topology from storage on each request —
    // there is no global topology to pre-load.
    let graph = args
        .synthetic
        .as_deref()
        .map(|scale| Arc::new(rivers_ui::synthetic::core_topology(scale)));

    let registry = match (args.registry_url, args.registry_token) {
        (Some(url), Some(token)) if !token.is_empty() => {
            Registry::grpc(url, token).expect("failed to build registry client")
        }
        (Some(_), _) => {
            panic!("--registry-url requires --registry-token (or RIVERS_REGISTRY_TOKEN)")
        }
        (None, _) => Registry::storage(storage.clone(), None),
    };

    // Bad RIVERS_AUTH_* config or an unreachable issuer aborts startup
    // rather than silently skipping auth.
    let auth = rivers_ui::auth::AuthRuntime::from_env()
        .await
        .expect("invalid RIVERS_AUTH_* configuration");

    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!(target: "rivers::ui", "received SIGTERM, shutting down");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(target: "rivers::ui", "received SIGINT, shutting down");
            }
        }
        token.cancel();
    });

    rivers_ui::start_server(
        storage, graph, args.host, args.port, registry, auth, shutdown,
    )
    .await
    .expect("Server error");
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgres://rivers:secret@db/rivers";

    /// `env_url`: a `RIVERS_STORAGE_URL` from the environment.
    fn location(cli: &[&str], env_url: Option<&str>) -> Result<StorageUrl, String> {
        let matches = Args::command()
            .try_get_matches_from(std::iter::once("rivers-ui").chain(cli.iter().copied()))
            .unwrap();
        let mut args = Args::from_arg_matches(&matches).unwrap();
        let on_command_line = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
        let url_on_command_line = on_command_line("storage_url");
        if !url_on_command_line {
            args.storage_url = env_url.map(str::to_string);
        }
        storage_location(&args, url_on_command_line, on_command_line("storage_path"))
    }

    #[test]
    fn a_flag_wins_over_the_url_of_the_environment() {
        let endpoint = location(&["--surreal-endpoint", "ws://surrealdb:8000"], Some(URL)).unwrap();
        assert_eq!(endpoint.redacted(), "ws://surrealdb:8000");
        let path = location(&["--storage-path", "/data/rivers"], Some(URL)).unwrap();
        assert_eq!(path.redacted(), "surrealkv:///data/rivers");
        assert_eq!(
            location(&[], Some(URL)).unwrap().backend_name(),
            "PostgreSQL"
        );
    }

    #[test]
    fn within_the_command_line_the_url_wins_unless_the_flags_name_different_stores() {
        let err = location(
            &[
                "--storage-url",
                URL,
                "--surreal-endpoint",
                "ws://surrealdb:8000",
            ],
            None,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "--storage-url and --surreal-endpoint name different stores; pass one of them"
        );
        let same = location(
            &[
                "--storage-url",
                "ws://surrealdb:8000",
                "--surreal-endpoint",
                "ws://surrealdb:8000",
            ],
            None,
        )
        .unwrap();
        assert_eq!(same.redacted(), "ws://surrealdb:8000");
    }

    #[test]
    fn the_endpoint_takes_only_a_surrealdb_server() {
        let err = location(&["--surreal-endpoint", URL], None).unwrap_err();
        assert!(
            err.contains("--surreal-endpoint takes a SurrealDB server"),
            "{err}"
        );
        assert!(err.contains("postgres://rivers:***@db/rivers"), "{err}");
        assert!(err.contains("--storage-url"), "{err}");
        assert!(!err.contains("secret"), "{err}");
    }
}
