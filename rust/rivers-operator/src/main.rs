mod codelocation;
mod leader;
mod metrics;
mod run;
mod webhook;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use futures_util::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use kube_client::{Api, Client};
use kube_runtime::Controller;
use kube_runtime::watcher::Config as WatcherConfig;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::code_location::CodeLocation;
use rivers_k8s::crd::run::Run;
use tracing_subscriber::EnvFilter;

const METRICS_ADDR_ENV: &str = "RIVERS_METRICS_ADDR";
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9090";
const LEASE_NAME: &str = "rivers-operator-leader";
const CODE_LOCATION_SA_ENV: &str = "RIVERS_CODE_LOCATION_SERVICE_ACCOUNT";
const DEFAULT_CODE_LOCATION_SA: &str = "rivers-code-location";
const REGISTRY_ADDR_ENV: &str = "RIVERS_REGISTRY_ADDR";
const DEFAULT_REGISTRY_ADDR: &str = "0.0.0.0:50052";
const REGISTRY_TOKEN_ENV: &str = "RIVERS_REGISTRY_TOKEN";
const WEBHOOK_ADDR_ENV: &str = "RIVERS_WEBHOOK_ADDR";
const DEFAULT_WEBHOOK_ADDR: &str = "0.0.0.0:9443";
const WEBHOOK_CERT_DIR_ENV: &str = "RIVERS_WEBHOOK_CERT_DIR";
const DEFAULT_WEBHOOK_CERT_DIR: &str = "/etc/webhook-cert";
// Conventional kubernetes.io/tls Secret keys — both cert-manager and any
// hand-issued Secret of type kubernetes.io/tls write these exact names.
const TLS_CERT_FILE: &str = "tls.crt";
const TLS_KEY_FILE: &str = "tls.key";
const WEBHOOK_DISABLED_ENV: &str = "RIVERS_WEBHOOK_DISABLED";
// When set to "true"/"1", all registry probes use plain HTTP instead of
// HTTPS. Only useful for trusted in-cluster or local-dev registries
// (e.g. k3d's HTTP-only local registry). Default is HTTPS.
const ALLOW_INSECURE_REGISTRY_ENV: &str = "RIVERS_ALLOW_INSECURE_REGISTRY";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let client = Client::try_default().await?;
    let namespace = rivers_k8s::env::detect_namespace();
    let surreal_config = rivers_k8s::env::detect_surreal_connect_config();

    tracing::info!(
        %namespace,
        endpoint = %surreal_config.endpoint,
        surreal_ns = %surreal_config.namespace,
        surreal_db = %surreal_config.database,
        authenticated = surreal_config.credentials.is_some(),
        "connecting to SurrealDB"
    );
    let storage = Arc::new(SurrealStorage::connect(surreal_config).await?);

    let runs: Api<Run> = Api::namespaced(client.clone(), &namespace);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let code_locations: Api<CodeLocation> = Api::namespaced(client.clone(), &namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(client.clone(), &namespace);

    let surreal_pod_cfg = rivers_k8s::env::SurrealPodConfig::from_env();

    // Built up front so the run reconciler's Context can reference it; the
    // watcher + gRPC tasks are spawned later via `spawn_registry_service`.
    let directory_state = Arc::new(codelocation::DirectoryState::new());

    let run_ctx = Arc::new(run::Context {
        client: client.clone(),
        namespace: namespace.clone(),
        storage,
        directory: directory_state.clone(),
        surreal_pod_cfg: surreal_pod_cfg.clone(),
    });

    let pod_identity = pod_identity();
    let leader = Arc::new(leader::spawn(
        client.clone(),
        namespace.clone(),
        LEASE_NAME.to_string(),
        pod_identity.clone(),
    ));

    let allow_insecure_registry = std::env::var(ALLOW_INSECURE_REGISTRY_ENV)
        .ok()
        .is_some_and(|v| matches!(v.as_str(), "true" | "1"));
    if allow_insecure_registry {
        tracing::warn!(
            "{} is set; all registry probes will use plain HTTP",
            ALLOW_INSECURE_REGISTRY_ENV
        );
    }

    let cl_ctx = Arc::new(codelocation::Context {
        client: client.clone(),
        namespace: namespace.clone(),
        registry: Arc::new(codelocation::RegistryClient::with_insecure(
            allow_insecure_registry,
        )),
        leader: leader.clone(),
        code_location_service_account: std::env::var(CODE_LOCATION_SA_ENV)
            .unwrap_or_else(|_| DEFAULT_CODE_LOCATION_SA.to_string()),
        surreal_pod_cfg: surreal_pod_cfg.clone(),
    });

    let metrics_addr =
        std::env::var(METRICS_ADDR_ENV).unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string());
    tokio::spawn(serve_metrics(metrics_addr));

    let synced = webhook::Synced::new();
    spawn_registry_service(
        client.clone(),
        namespace.clone(),
        directory_state.clone(),
        Some(synced.clone()),
    )?;
    spawn_webhook_server(client.clone(), namespace.clone(), directory_state, synced).await?;

    tracing::info!(%pod_identity, "starting rivers-operator");

    let run_controller = Controller::new(runs, WatcherConfig::default())
        .owns(pods, WatcherConfig::default())
        .run(run::reconcile, run::error_policy, run_ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::debug!(?o, "run reconciled"),
                Err(e) => tracing::error!(%e, "run reconcile failed"),
            }
        });

    let code_location_controller = Controller::new(code_locations, WatcherConfig::default())
        .owns(deployments, WatcherConfig::default())
        .owns(services, WatcherConfig::default())
        .run(codelocation::reconcile, codelocation::error_policy, cl_ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::debug!(?o, "code_location reconciled"),
                Err(e) => tracing::error!(%e, "code_location reconcile failed"),
            }
        });

    tokio::join!(run_controller, code_location_controller);

    Ok(())
}

async fn serve_metrics(addr: String) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler));
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            tracing::info!(%addr, "metrics/health endpoint listening");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(%e, "metrics server exited");
            }
        }
        Err(e) => {
            tracing::error!(%addr, %e, "failed to bind metrics listener");
        }
    }
}

/// Spawn the CodeLocation directory watcher + gRPC server. The token is
/// required config — the Helm chart wires it from `rivers-registry-token`;
/// a missing token means the operator is misconfigured, so we fail fast
/// rather than silently disable a feature the UI depends on. The shared
/// `DirectoryState` is constructed upstream so the run reconciler's Context
/// can reference the same instance the watcher feeds.
fn spawn_registry_service(
    client: Client,
    namespace: String,
    state: Arc<codelocation::DirectoryState>,
    synced: Option<webhook::Synced>,
) -> anyhow::Result<()> {
    let token = match std::env::var(REGISTRY_TOKEN_ENV) {
        Ok(t) if !t.is_empty() => t,
        _ => anyhow::bail!(
            "{REGISTRY_TOKEN_ENV} is required — the Helm chart wires this from \
             the `rivers-registry-token` Secret; check chart values and RBAC"
        ),
    };
    let addr = std::env::var(REGISTRY_ADDR_ENV)
        .unwrap_or_else(|_| DEFAULT_REGISTRY_ADDR.to_string())
        .parse::<std::net::SocketAddr>()
        .map_err(|e| anyhow::anyhow!("{REGISTRY_ADDR_ENV}: invalid address: {e}"))?;

    tokio::spawn(codelocation::run_directory_watcher(
        client,
        namespace,
        state.clone(),
        synced,
    ));
    tokio::spawn(async move {
        if let Err(e) = codelocation::directory::serve(state, addr, token).await {
            tracing::error!(%e, "registry gRPC server exited");
        }
    });
    Ok(())
}

/// Bring up the admission webhook: load the serving cert from the mounted
/// cert-manager Secret, then spawn the HTTPS server (which itself spawns a
/// reload loop to pick up rotations). Skipped (with a warning) if
/// `RIVERS_WEBHOOK_DISABLED=1`, useful when iterating on the operator
/// binary outside Helm (e.g. `cargo run -p rivers-operator` against a kind
/// cluster) where the cert-manager `Issuer` + `Certificate` aren't
/// installed and the cert files therefore aren't mounted.
async fn spawn_webhook_server(
    client: Client,
    namespace: String,
    directory: Arc<codelocation::DirectoryState>,
    synced: webhook::Synced,
) -> anyhow::Result<()> {
    if std::env::var(WEBHOOK_DISABLED_ENV).as_deref() == Ok("1") {
        tracing::warn!(
            target: "rivers::operator::webhook",
            "{WEBHOOK_DISABLED_ENV}=1 — admission webhook NOT started; \
             Run CRs will not be digest-stamped"
        );
        // Mark synced so /readyz still works for callers that gate on it.
        synced.mark_ready();
        return Ok(());
    }

    let addr = std::env::var(WEBHOOK_ADDR_ENV)
        .unwrap_or_else(|_| DEFAULT_WEBHOOK_ADDR.to_string())
        .parse::<std::net::SocketAddr>()
        .map_err(|e| anyhow::anyhow!("{WEBHOOK_ADDR_ENV}: invalid address: {e}"))?;
    let cert_dir = std::env::var(WEBHOOK_CERT_DIR_ENV)
        .unwrap_or_else(|_| DEFAULT_WEBHOOK_CERT_DIR.to_string());
    let cert_dir = std::path::PathBuf::from(cert_dir);
    let cert_path = cert_dir.join(TLS_CERT_FILE);
    let key_path = cert_dir.join(TLS_KEY_FILE);

    let code_locations_api: Api<CodeLocation> = Api::namespaced(client, &namespace);
    let server_directory = directory;
    tokio::spawn(async move {
        if let Err(e) = webhook::serve(
            addr,
            cert_path,
            key_path,
            server_directory,
            code_locations_api,
            synced,
        )
        .await
        {
            tracing::error!(%e, "admission webhook server exited");
        }
    });
    Ok(())
}

async fn metrics_handler() -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::OK, metrics::scrape())
}

async fn healthz_handler() -> &'static str {
    "ok"
}

fn pod_identity() -> String {
    std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("rivers-operator-{}", uuid::Uuid::new_v4()))
}
