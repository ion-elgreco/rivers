//! UI authentication: `oidc` and `forward` modes behind one middleware;
//! mode `none` leaves the router untouched.

pub mod config;
#[cfg(test)]
mod flow_tests;
#[cfg(test)]
pub(crate) mod test_cookies;
pub mod forward;
pub mod identity;
pub mod middleware;
pub mod oidc;
pub mod pages;
pub mod session;

use axum::Router;
use axum::routing::get;
use std::sync::Arc;

pub use config::{Allowlists, AuthConfig, AuthMode};
pub use identity::Identity;

#[derive(Debug)]
pub enum RuntimeKind {
    Oidc(Arc<oidc::OidcRuntime>),
    Forward(forward::ForwardRuntime),
}

/// Resolved auth state; built once at startup (OIDC discovery failures
/// abort startup).
#[derive(Debug)]
pub struct AuthRuntime {
    pub allow: Allowlists,
    pub kind: RuntimeKind,
}

impl AuthRuntime {
    pub async fn initialize(config: AuthConfig) -> anyhow::Result<Option<Arc<Self>>> {
        let kind = match config.mode {
            AuthMode::None => return Ok(None),
            AuthMode::Oidc(cfg) => {
                RuntimeKind::Oidc(Arc::new(oidc::OidcRuntime::initialize(cfg).await?))
            }
            AuthMode::Forward(cfg) => RuntimeKind::Forward(forward::ForwardRuntime::new(cfg)?),
        };
        Ok(Some(Arc::new(Self {
            allow: config.allow,
            kind,
        })))
    }

    pub async fn from_env() -> anyhow::Result<Option<Arc<Self>>> {
        Self::initialize(AuthConfig::from_env()?).await
    }

    pub fn mode_str(&self) -> &'static str {
        match self.kind {
            RuntimeKind::Oidc(_) => "oidc",
            RuntimeKind::Forward(_) => "forward",
        }
    }

    /// Where the UI's sign-out control should point, if anywhere.
    pub fn logout_url(&self) -> Option<String> {
        match &self.kind {
            RuntimeKind::Oidc(_) => Some(crate::routes::LOGOUT.to_string()),
            RuntimeKind::Forward(f) => f.cfg.logout_url.clone(),
        }
    }
}

/// Leptos context handle so server fns can reach the auth runtime.
#[derive(Clone)]
pub struct AuthCtx(pub Option<Arc<AuthRuntime>>);

fn auth_routes(state: oidc::OidcState) -> Router {
    Router::new()
        .route(crate::routes::LOGIN, get(oidc::login))
        .route(crate::routes::CALLBACK, get(oidc::callback))
        .route(crate::routes::LOGOUT, get(oidc::logout))
        .with_state(state)
}

/// Wrap a fully-built router with the auth gate; `None` returns it
/// untouched. The `/auth/*` routes mount only in OIDC mode, with an
/// `OidcState` so the handlers need no runtime mode check.
pub fn apply_auth(router: Router, rt: Option<Arc<AuthRuntime>>) -> Router {
    let Some(rt) = rt else { return router };
    let router = match &rt.kind {
        RuntimeKind::Oidc(o) => router.merge(auth_routes(oidc::OidcState {
            oidc: o.clone(),
            allow: rt.allow.clone(),
        })),
        RuntimeKind::Forward(_) => router,
    };
    router.layer(axum::middleware::from_fn_with_state(
        rt,
        middleware::require_auth,
    ))
}
