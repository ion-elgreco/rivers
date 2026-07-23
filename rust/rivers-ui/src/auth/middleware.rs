//! Request gate: everything outside the public allowlist requires an
//! `Identity`, inserted into request extensions.

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use std::net::SocketAddr;
use std::sync::Arc;

use super::pages::forbidden_page;
use super::session::read_session;
use super::{AuthRuntime, RuntimeKind};

fn is_public(path: &str) -> bool {
    path == "/healthz"
        || path == "/readyz"
        || path == crate::routes::LOGIN
        || path == crate::routes::CALLBACK
        || path == crate::routes::LOGOUT
}

#[cfg(test)]
mod tests {
    use super::is_public;
    use crate::routes;

    /// The gate must treat every auth route as public — otherwise the login
    /// flow itself would require a session. Ties the route consts to `is_public`.
    #[test]
    fn auth_routes_are_public() {
        assert!(is_public(routes::LOGIN));
        assert!(is_public(routes::CALLBACK));
        assert!(is_public(routes::LOGOUT));
        assert!(is_public("/healthz"));
        assert!(is_public("/readyz"));
        assert!(!is_public("/"));
        assert!(!is_public("/api/events"));
        // Only the three real handlers are public — not the whole `/auth/`
        // namespace. An unmatched `/auth/*` path must still hit the gate so a
        // future catch-all SSR fallback can't leak through it.
        assert!(!is_public("/auth/foobar"));
        assert!(!is_public("/auth/"));
    }
}

fn wants_html(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}

pub async fn require_auth(
    State(rt): State<Arc<AuthRuntime>>,
    mut req: Request,
    next: Next,
) -> Response {
    if is_public(req.uri().path()) {
        return next.run(req).await;
    }
    match &rt.kind {
        RuntimeKind::Oidc(o) => {
            if let Some(identity) =
                read_session(req.headers(), &o.cookie_key, o.cfg.secure_cookies())
            {
                if !rt.allow.permits(&identity) {
                    return forbidden_page(&identity, Some(crate::routes::LOGOUT));
                }
                req.extensions_mut().insert(identity);
                return next.run(req).await;
            }
            // Documents bounce to login; API/SSE get a 401 (a 302-to-IdP
            // would confuse fetch/EventSource). The body is server_fn's
            // `Variant|payload` wire format so a server-fn caller decodes a
            // recognizable ServerError and can redirect into the login flow.
            if req.method() == Method::GET && wants_html(req.headers()) {
                let rd = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("/");
                let login = format!(
                    "{}?rd={}",
                    crate::routes::LOGIN,
                    utf8_percent_encode(rd, NON_ALPHANUMERIC)
                );
                return Redirect::to(&login).into_response();
            }
            (
                StatusCode::UNAUTHORIZED,
                format!("ServerError|{}", crate::helpers::UNAUTHORIZED_MARKER),
            )
                .into_response()
        }
        RuntimeKind::Forward(f) => {
            // Peer before headers — untrusted-peer headers are forgeable.
            let peer = req
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ci| ci.0.ip());
            if !peer.is_some_and(|ip| f.peer_trusted(ip)) {
                return (
                    StatusCode::FORBIDDEN,
                    "request did not arrive via a trusted proxy (RIVERS_AUTH_FORWARD_TRUSTED_PROXIES)\n",
                )
                    .into_response();
            }
            match f.identity_from_headers(req.headers()) {
                Some(identity) if rt.allow.permits(&identity) => {
                    req.extensions_mut().insert(identity);
                    next.run(req).await
                }
                Some(identity) => forbidden_page(&identity, f.cfg.logout_url.as_deref()),
                None => (
                    StatusCode::UNAUTHORIZED,
                    format!(
                        "no identity headers present (expected {:?} from the auth proxy) — \
                         the proxy authenticated nothing or drops its auth response headers; \
                         check its forwardAuth/auth_request wiring\n",
                        f.cfg.user_header
                    ),
                )
                    .into_response(),
            }
        }
    }
}
