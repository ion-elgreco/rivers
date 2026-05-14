//! HTTPS admission server. Routes:
//!   POST /mutate-run          → Run AdmissionReview handler.
//!   POST /mutate-codelocation → CodeLocation AdmissionReview handler.
//!   GET  /readyz              → 200 only after the CodeLocation reflector has synced.
//!   GET  /healthz             → 200 unconditionally.
//!
//! The serving cert + key are mounted into the pod from a Secret managed by
//! cert-manager (`Issuer` + `Certificate` shipped by the Helm chart). The
//! same Secret is referenced by the `MutatingWebhookConfiguration` via the
//! `cert-manager.io/inject-ca-from` annotation, so cert-manager's
//! `cainjector` keeps the `caBundle` field in sync. cert-manager rotates the
//! cert before expiry and rewrites the Secret in place; the kubelet refreshes
//! the volume mount, and a background task here reloads the rustls config
//! atomically — no operator restart required.
//!
//! The readiness gate is load-bearing — the pod must not receive admission
//! traffic until its reflector has completed its initial list, otherwise a
//! follower replica would manufacture spurious "codeLocation not found"
//! rejections under `failurePolicy: Fail`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum_server::tls_rustls::RustlsConfig;
use kube_client::Api;
use kube_core::admission::AdmissionReview;
use rivers_k8s::crd::code_location::CodeLocation;
use rivers_k8s::crd::run::Run;

use crate::codelocation::DirectoryState;
use crate::webhook::admission::{
    AdmissionDeps, handle_codelocation_admission, handle_run_admission,
};

/// How often the background task re-reads the cert files from disk. cert-
/// manager rotates ~30 days before expiry, so polling every 10 minutes is
/// well within the safety margin and effectively free.
const CERT_RELOAD_INTERVAL: Duration = Duration::from_secs(600);

/// Atomic flag flipped by the CodeLocation reflector once `Event::InitDone`
/// fires. The readiness probe reads this flag — until it is set, kube
/// Endpoints management excludes the pod from the webhook Service, so the
/// API server never dispatches admission traffic to an un-synced replica.
#[derive(Clone, Default)]
pub struct Synced(Arc<AtomicBool>);

impl Synced {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Returns `true` if this is the first time the flag has been flipped,
    /// `false` on every subsequent call (e.g. watch reconnects). Lets the
    /// caller log "synced" exactly once per process lifetime instead of on
    /// every InitDone the reflector emits.
    pub fn mark_ready(&self) -> bool {
        self.0
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct WebhookState {
    deps: AdmissionDeps,
    synced: Synced,
}

/// Run the admission HTTPS server forever. Returns only on a fatal error
/// (TLS bind failure, missing cert files, etc.). The cert + key are loaded
/// from `cert_path` / `key_path` (typically a Secret mounted by the Helm
/// chart), and a background task polls those paths every
/// [`CERT_RELOAD_INTERVAL`] to pick up cert-manager rotations in-process.
pub async fn serve(
    addr: SocketAddr,
    cert_path: PathBuf,
    key_path: PathBuf,
    directory: Arc<DirectoryState>,
    code_locations: Api<CodeLocation>,
    synced: Synced,
) -> Result<()> {
    let state = WebhookState {
        deps: AdmissionDeps {
            directory,
            code_locations: Some(code_locations),
        },
        synced,
    };

    let app = Router::new()
        .route("/mutate-run", post(mutate_run_handler))
        .route("/mutate-codelocation", post(mutate_codelocation_handler))
        .route("/readyz", get(readyz_handler))
        .route("/healthz", get(healthz_handler))
        .with_state(state);

    let tls_config = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .with_context(|| {
            format!(
                "loading webhook serving cert from {} + {} — in production \
                 these are mounted by the Helm chart from the cert-manager-managed \
                 Secret rivers-operator-webhook-cert. Ensure cert-manager is \
                 installed (https://cert-manager.io) and the Certificate is \
                 Ready, or set RIVERS_WEBHOOK_DISABLED=1 for ad-hoc dev",
                cert_path.display(),
                key_path.display(),
            )
        })?;

    tokio::spawn(reload_cert_loop(
        tls_config.clone(),
        cert_path.clone(),
        key_path.clone(),
    ));

    tracing::info!(
        target: "rivers::operator::webhook",
        %addr,
        cert_path = %cert_path.display(),
        "admission webhook listening (HTTPS)"
    );
    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await
        .context("admission webhook server exited")?;
    Ok(())
}

/// Periodically re-read the cert + key files from disk and atomically swap
/// them into the live `RustlsConfig`. The 10-min interval fits comfortably
/// inside the kubelet's ~60s volume-mount sync window after cert-manager
/// rotates the Secret.
async fn reload_cert_loop(config: RustlsConfig, cert_path: PathBuf, key_path: PathBuf) {
    let mut interval = tokio::time::interval(CERT_RELOAD_INTERVAL);
    interval.tick().await; // skip the immediate first tick — cert is fresh
    loop {
        interval.tick().await;
        match config.reload_from_pem_file(&cert_path, &key_path).await {
            Ok(()) => tracing::debug!(
                target: "rivers::operator::webhook",
                "reloaded webhook serving cert from disk"
            ),
            Err(e) => tracing::warn!(
                target: "rivers::operator::webhook",
                error = %e,
                "failed to reload webhook serving cert; continuing with previously-loaded cert"
            ),
        }
    }
}

async fn mutate_run_handler(
    State(state): State<WebhookState>,
    Json(review): Json<AdmissionReview<Run>>,
) -> impl IntoResponse {
    let response = handle_run_admission(review, &state.deps).await;
    (StatusCode::OK, Json(response))
}

async fn mutate_codelocation_handler(
    State(_state): State<WebhookState>,
    Json(review): Json<AdmissionReview<CodeLocation>>,
) -> impl IntoResponse {
    let response = handle_codelocation_admission(review).await;
    (StatusCode::OK, Json(response))
}

async fn readyz_handler(State(state): State<WebhookState>) -> (StatusCode, &'static str) {
    if state.synced.is_ready() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "waiting for informer sync")
    }
}

async fn healthz_handler() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_starts_unready() {
        let s = Synced::new();
        assert!(!s.is_ready());
    }

    #[test]
    fn synced_mark_ready_flips_flag_and_returns_true_first_time() {
        let s = Synced::new();
        assert!(s.mark_ready(), "first flip returns true");
        assert!(s.is_ready());
        assert!(
            !s.mark_ready(),
            "subsequent flips return false (already set)"
        );
    }

    #[test]
    fn synced_clones_share_state() {
        let a = Synced::new();
        let b = a.clone();
        assert!(!b.is_ready());
        assert!(a.mark_ready());
        assert!(b.is_ready());
        assert!(!b.mark_ready(), "clone sees the same set state");
    }
}
