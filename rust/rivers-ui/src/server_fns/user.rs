//! Server function exposing the request's authenticated identity.

use leptos::prelude::*;

use crate::types::CurrentUser;

/// `None` when auth mode is `none`; otherwise the middleware has already
/// inserted the identity extension.
#[server]
pub async fn get_current_user() -> Result<Option<CurrentUser>, ServerFnError> {
    use crate::auth::AuthCtx;
    use crate::types::UserRef;

    let auth = expect_context::<AuthCtx>();
    let Some(rt) = auth.0 else {
        return Ok(None);
    };
    let identity = super::current_identity().await;
    Ok(identity.map(|id| CurrentUser {
        user: UserRef {
            subject: id.subject,
            email: id.email,
            name: id.name,
        },
        logout_url: rt.logout_url(),
    }))
}
