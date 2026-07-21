//! Server function exposing the request's authenticated identity.

use leptos::prelude::*;

use crate::types::{CurrentUser, UserRef};

/// `None` when auth mode is `none`; otherwise the middleware has already
/// inserted the identity extension.
#[server]
pub async fn get_current_user() -> Result<Option<CurrentUser>, ServerFnError> {
    use crate::auth::{AuthCtx, Identity};

    let auth = expect_context::<AuthCtx>();
    let Some(rt) = auth.0 else {
        return Ok(None);
    };
    let identity = leptos_axum::extract::<axum::Extension<Identity>>()
        .await
        .ok()
        .map(|axum::Extension(id)| id);
    Ok(identity.map(|id| CurrentUser {
        user: UserRef {
            subject: id.subject,
            email: id.email,
            name: id.name,
        },
        logout_url: rt.logout_url(),
    }))
}
