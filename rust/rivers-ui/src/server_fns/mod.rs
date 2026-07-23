//! Leptos server functions for data fetching and write actions.

pub mod actions;
pub mod assets;
pub mod automation;
pub mod backfills;
pub mod graph;
pub mod locations;
pub mod overview;
pub mod pools;
pub mod runs;
pub mod user;

/// The request's authenticated session identity, or `None` in auth mode
/// `none` (the middleware inserts the extension only when auth is enabled).
#[cfg(feature = "ssr")]
pub(crate) async fn current_identity() -> Option<crate::auth::Identity> {
    leptos_axum::extract::<axum::Extension<crate::auth::Identity>>()
        .await
        .ok()
        .map(|axum::Extension(id)| id)
}

#[cfg(feature = "ssr")]
pub(crate) async fn resolve_identity(
    loc_ns: &str,
    loc_name: &str,
) -> Result<rivers_core::storage::CodeLocationContext, leptos::prelude::ServerFnError> {
    let state = leptos::prelude::expect_context::<crate::state::AppState>();
    let entry = state
        .registry
        .lookup(loc_ns, loc_name)
        .await
        .ok_or_else(|| {
            leptos::prelude::ServerFnError::new(format!(
                "code location {loc_ns}/{loc_name} not found in registry"
            ))
        })?;
    Ok(rivers_core::storage::CodeLocationContext::new(
        entry.identity,
    ))
}
