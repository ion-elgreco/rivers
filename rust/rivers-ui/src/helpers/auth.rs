use leptos::prelude::*;

/// Payload of the auth middleware's API 401, in server_fn's `Variant|payload`
/// wire format so the client decodes it as `ServerFnError::ServerError` and
/// [`is_unauthorized`] can key on it. An empty 401 body would decode as a
/// generic `Deserialization` error, indistinguishable from real failures.
pub const UNAUTHORIZED_MARKER: &str = "unauthorized: sign-in required";

/// True when a server-fn call failed because the session is missing/expired
/// — the client should bounce through `/auth/login`.
pub fn is_unauthorized(err: &ServerFnError) -> bool {
    matches!(err, ServerFnError::ServerError(msg) if msg == UNAUTHORIZED_MARKER)
}

/// Hard-redirect into the login flow, preserving path+query as `rd`. No-op
/// under SSR; idempotent so overlapping triggers don't stack navigations.
#[cfg(target_arch = "wasm32")]
pub fn redirect_to_login() {
    use std::cell::Cell;
    thread_local! { static REDIRECTING: Cell<bool> = const { Cell::new(false) }; }
    if REDIRECTING.with(|r| r.replace(true)) {
        return;
    }
    let loc = window().location();
    let path = loc.pathname().unwrap_or_else(|_| "/".into());
    let query = loc.search().unwrap_or_default();
    let rd = js::encode_uri_component(&format!("{path}{query}"));
    let _ = loc.assign(&format!("{}?rd={rd}", crate::routes::LOGIN));
}

#[cfg(not(target_arch = "wasm32"))]
pub fn redirect_to_login() {}

#[cfg(target_arch = "wasm32")]
mod js {
    use wasm_bindgen::prelude::wasm_bindgen;
    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_name = encodeURIComponent)]
        pub fn encode_uri_component(s: &str) -> String;
    }
}

/// Re-check auth after a signal the session may have lapsed (e.g. the live
/// stream closed) and redirect to login only if actually unauthorized — a live
/// session or a transient error is left alone. No-op under SSR.
#[cfg(target_arch = "wasm32")]
pub fn spawn_login_redirect_if_unauthorized() {
    leptos::task::spawn_local(async {
        if let Err(e) = crate::server_fns::user::get_current_user().await
            && is_unauthorized(&e)
        {
            redirect_to_login();
        }
    });
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_login_redirect_if_unauthorized() {}
