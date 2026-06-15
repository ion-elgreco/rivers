//! SSR entry point for the rivers web UI.
//!
//! Embeds the compiled WASM/JS client bundle and serves it alongside the
//! Leptos+Axum server-rendered application.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;
use rivers_core::assets::graph::GraphTopology;
use rivers_core::storage::surrealdb_backend::{Capability, SurrealStorage};
use rivers_ui::code_location_registry::Registry;
use rivers_ui::synthetic::{generate_synthetic_graph, parse_node_count};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rivers-ui", about = "rivers web UI")]
struct Args {
    #[arg(long, default_value = ".rivers/storage/")]
    storage_path: String,

    /// Connect to a remote SurrealDB instead of embedded storage.
    /// When set, --storage-path is ignored.
    #[arg(long)]
    surreal_endpoint: Option<String>,

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

#[tokio::main]
async fn main() {
    let args = Args::parse();

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

    let storage = Arc::new(if let Some(ref endpoint) = args.surreal_endpoint {
        // Build the connect config from the same env vars the operator stamps
        // on every rivers pod. CLI flag wins for the endpoint (so a developer
        // can `cargo run` against a local SurrealDB without setting envs);
        // namespace/database/credentials always come from env.
        let mut config = rivers_k8s::env::detect_surreal_connect_config();
        config.endpoint = endpoint.clone();
        let authenticated = config.credentials.is_some();
        // The production UI is a read-only storage consumer; writes go through
        // gRPC to code locations. Open `Read` so a write-breaking migration for
        // newer writers does not lock the UI out.
        let storage = SurrealStorage::connect_with_capability(config, Capability::Read)
            .await
            .expect("Failed to connect to remote SurrealDB");
        tracing::info!(
            target: "rivers::storage",
            backend = "remote",
            endpoint = %endpoint,
            authenticated,
            "storage ready"
        );
        storage
    } else {
        let storage =
            SurrealStorage::new_embedded_with_capability(&args.storage_path, Capability::Read)
                .await
                .expect("Failed to open embedded storage");
        tracing::info!(
            target: "rivers::storage",
            backend = "embedded",
            path = %args.storage_path,
            "storage ready"
        );
        storage
    });

    // Synthetic mode is a developer override that pins a fixed graph for
    // every code location. In production the standalone UI is multi-CL
    // and reads the active CL's topology from storage on each request —
    // there is no global topology to pre-load.
    let graph = args.synthetic.as_ref().map(|scale| {
        let n = parse_node_count(scale);
        let g = generate_synthetic_graph(n);
        Arc::new(GraphTopology {
            nodes: g
                .nodes
                .into_iter()
                .map(|n| rivers_core::assets::graph::TopologyNode {
                    name: n.name,
                    kind: n
                        .kind
                        .parse()
                        .expect("synthetic graph produced invalid NodeKind"),
                    group: n.group,
                    parent_graph: n.parent_graph,
                })
                .collect(),
            edges: g.edges,
        })
    });

    let registry = match (args.registry_url, args.registry_token) {
        (Some(url), Some(token)) if !token.is_empty() => {
            Registry::grpc(url, token).expect("failed to build registry client")
        }
        (Some(_), _) => {
            panic!("--registry-url requires --registry-token (or RIVERS_REGISTRY_TOKEN)")
        }
        (None, _) => {
            tracing::warn!(
                target: "rivers::ui",
                "no --registry-url provided; UI will show no code locations"
            );
            Registry::empty()
        }
    };

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

    rivers_ui::start_server(storage, graph, args.host, args.port, registry, shutdown)
        .await
        .expect("Server error");
}
