//! OIDC authorization-code + PKCE flow against any discovered issuer.

use anyhow::Context;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use axum_extra::extract::cookie::Key;
use openidconnect::core::{
    CoreAuthDisplay, CoreAuthPrompt, CoreAuthenticationFlow, CoreErrorResponseType, CoreGenderClaim,
    CoreJsonWebKey, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
    CoreRevocableToken, CoreTokenIntrospectionResponse, CoreTokenType,
};
use openidconnect::{
    AdditionalClaims, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken,
    EmptyExtraTokenFields, EndpointMaybeSet, EndpointNotSet, EndpointSet, IdTokenFields,
    IssuerUrl, Nonce, PkceCodeChallenge, PkceCodeVerifier, ProviderMetadataWithLogout,
    RedirectUrl, RevocationErrorResponseType, Scope, StandardErrorResponse,
    StandardTokenResponse, TokenResponse, reqwest,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::config::OidcConfig;
use super::identity::Identity;
use super::pages::{error_page, forbidden_page};
use super::session::{
    PendingLogin, clear_cookies, now_ts, pending_login_jar, read_pending_login,
    session_response,
};
use super::{AuthRuntime, RuntimeKind};

/// Catch-all additional claims — the configurable groups claim is read
/// from the map.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawClaims(pub serde_json::Map<String, serde_json::Value>);
impl AdditionalClaims for RawClaims {}

type RiversIdTokenFields = IdTokenFields<
    RawClaims,
    EmptyExtraTokenFields,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;
type RiversTokenResponse = StandardTokenResponse<RiversIdTokenFields, CoreTokenType>;

/// `CoreClient` with `RawClaims` substituted, at the endpoint typestates
/// `from_provider_metadata` + `set_redirect_uri` produce.
pub type OidcClient = Client<
    RawClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    RiversTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    StandardErrorResponse<RevocationErrorResponseType>,
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

pub struct OidcRuntime {
    /// Swapped out when the IdP rotates signing keys (see
    /// [`OidcRuntime::refresh_keys_rate_limited`]); reads clone the `Arc`.
    client: std::sync::RwLock<Arc<OidcClient>>,
    /// Unix-seconds of the last key refresh; gates the refresh rate so a
    /// burst of unverifiable tokens can't hammer the IdP. `0` = never.
    last_key_refresh: std::sync::atomic::AtomicI64,
    pub http: reqwest::Client,
    pub cfg: OidcConfig,
    pub cookie_key: Key,
    pub secure_cookies: bool,
    pub end_session_endpoint: Option<String>,
}

/// Minimum spacing between on-miss JWKS refreshes.
const KEY_REFRESH_COOLDOWN_SECS: i64 = 300;

/// Bounds every IdP round-trip (discovery, JWKS, code exchange). Discovery
/// runs before the UI binds its listener, so an unbounded request against a
/// hung IdP would stall startup with `/healthz` never served.
#[cfg(not(test))]
const IDP_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
#[cfg(test)]
const IDP_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

impl std::fmt::Debug for OidcRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcRuntime")
            .field("issuer", &self.cfg.issuer)
            .field("client_id", &self.cfg.client_id)
            .finish_non_exhaustive()
    }
}

/// Run OIDC discovery and build the verifying client. Re-runnable so signing
/// keys can be refreshed after the IdP rotates them.
async fn discover_client(
    cfg: &OidcConfig,
    http: &reqwest::Client,
) -> anyhow::Result<(OidcClient, Option<String>)> {
    let issuer = IssuerUrl::new(cfg.issuer.clone()).context("invalid RIVERS_AUTH_OIDC_ISSUER")?;
    let metadata = ProviderMetadataWithLogout::discover_async(issuer, http)
        .await
        .with_context(|| format!("OIDC discovery against {} failed", cfg.issuer))?;
    let end_session_endpoint = metadata
        .additional_metadata()
        .end_session_endpoint
        .clone()
        .map(|u| u.to_string());
    let redirect = RedirectUrl::new(format!("{}/auth/callback", cfg.public_url))
        .context("invalid redirect URL derived from RIVERS_AUTH_PUBLIC_URL")?;
    let client = OidcClient::from_provider_metadata(
        metadata,
        ClientId::new(cfg.client_id.clone()),
        cfg.client_secret.clone().map(ClientSecret::new),
    )
    .set_redirect_uri(redirect);
    Ok((client, end_session_endpoint))
}

impl OidcRuntime {
    pub async fn initialize(cfg: OidcConfig) -> anyhow::Result<Self> {
        let http = reqwest::ClientBuilder::new()
            // Never follow IdP redirects (SSRF).
            .redirect(reqwest::redirect::Policy::none())
            .timeout(IDP_HTTP_TIMEOUT)
            .build()
            .context("failed to build OIDC http client")?;
        let (client, end_session_endpoint) = discover_client(&cfg, &http).await?;
        let secure_cookies = cfg.public_url.starts_with("https://");
        let cookie_key = match &cfg.cookie_secret {
            Some(bytes) => Key::derive_from(bytes),
            None => {
                tracing::warn!(
                    target: "rivers::auth",
                    "RIVERS_AUTH_COOKIE_SECRET not set — using an ephemeral key; \
                     sessions will not survive restarts or span replicas"
                );
                Key::generate()
            }
        };
        Ok(Self {
            client: std::sync::RwLock::new(Arc::new(client)),
            last_key_refresh: std::sync::atomic::AtomicI64::new(0),
            http,
            cfg,
            cookie_key,
            secure_cookies,
            end_session_endpoint,
        })
    }

    /// Current verifying client. Cheap `Arc` clone; the guard is never held
    /// across an `.await`.
    fn client(&self) -> Arc<OidcClient> {
        self.client.read().unwrap().clone()
    }

    /// Re-run discovery to pick up rotated IdP signing keys, then swap the
    /// verifying client. Rate-limited to at most once per
    /// [`KEY_REFRESH_COOLDOWN_SECS`] so a flood of tokens with unknown `kid`s
    /// can't turn into a flood of discovery requests. Returns whether a
    /// refresh actually ran.
    async fn refresh_keys_rate_limited(&self) -> bool {
        use std::sync::atomic::Ordering;
        let now = now_ts();
        if now - self.last_key_refresh.load(Ordering::Relaxed) < KEY_REFRESH_COOLDOWN_SECS {
            return false;
        }
        // Claim the window before the network round-trip so concurrent
        // failures collapse into one refresh.
        self.last_key_refresh.store(now, Ordering::Relaxed);
        match discover_client(&self.cfg, &self.http).await {
            Ok((client, _)) => {
                *self.client.write().unwrap() = Arc::new(client);
                tracing::info!(target: "rivers::auth", "refreshed OIDC signing keys");
                true
            }
            Err(e) => {
                tracing::warn!(
                    target: "rivers::auth",
                    error = %format!("{e:#}"),
                    "OIDC signing-key refresh failed"
                );
                false
            }
        }
    }
}

fn expect_oidc(rt: &AuthRuntime) -> Option<&OidcRuntime> {
    match &rt.kind {
        RuntimeKind::Oidc(o) => Some(o),
        RuntimeKind::Forward(_) => None,
    }
}

/// Reject anything that isn't a same-origin absolute path (open-redirect and
/// header-injection guard).
pub fn sanitize_rd(rd: Option<&str>) -> String {
    match rd {
        Some(rd)
            if rd.starts_with('/')
                && !rd.starts_with("//")
                && !rd.contains('\\')
                && rd.chars().all(|c| c.is_ascii_graphic()) =>
        {
            rd.to_string()
        }
        _ => "/".to_string(),
    }
}

#[derive(Debug, Deserialize)]
pub struct LoginQuery {
    pub rd: Option<String>,
}

pub async fn login(
    State(rt): State<Arc<AuthRuntime>>,
    Query(q): Query<LoginQuery>,
) -> Response {
    let Some(o) = expect_oidc(&rt) else {
        return error_page(axum::http::StatusCode::NOT_FOUND, "Not found", "", None);
    };
    let client = o.client();
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let mut auth_req = client.authorize_url(
        CoreAuthenticationFlow::AuthorizationCode,
        CsrfToken::new_random,
        Nonce::new_random,
    );
    for scope in &o.cfg.scopes {
        // `authorize_url` adds `openid` itself.
        if scope != "openid" {
            auth_req = auth_req.add_scope(Scope::new(scope.clone()));
        }
    }
    let (auth_url, csrf, nonce) = auth_req.set_pkce_challenge(pkce_challenge).url();
    let pending = PendingLogin {
        state: csrf.secret().clone(),
        nonce: nonce.secret().clone(),
        pkce_verifier: pkce_verifier.secret().clone(),
        rd: sanitize_rd(q.rd.as_deref()),
        iat: now_ts(),
    };
    (
        pending_login_jar(&o.cookie_key, o.secure_cookies, &pending),
        Redirect::to(auth_url.as_str()),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

pub async fn callback(
    State(rt): State<Arc<AuthRuntime>>,
    Query(q): Query<CallbackQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(o) = expect_oidc(&rt) else {
        return error_page(axum::http::StatusCode::NOT_FOUND, "Not found", "", None);
    };
    let fail = |detail: String| {
        (
            clear_cookies(o.secure_cookies),
            error_page(
                axum::http::StatusCode::BAD_GATEWAY,
                "Sign-in failed",
                &detail,
                Some("/auth/login"),
            ),
        )
            .into_response()
    };

    if let Some(err) = &q.error {
        let detail = match &q.error_description {
            Some(d) => format!("{err}: {d}"),
            None => err.clone(),
        };
        return fail(format!("the identity provider returned an error — {detail}"));
    }
    let Some(pending) = read_pending_login(&headers, &o.cookie_key, o.secure_cookies) else {
        return fail("login session missing or expired".to_string());
    };
    if q.state.as_deref() != Some(pending.state.as_str()) {
        return fail("state parameter mismatch".to_string());
    }
    let Some(code) = q.code else {
        return fail("missing authorization code".to_string());
    };

    let client = o.client();
    let exchange = match client.exchange_code(AuthorizationCode::new(code)) {
        Ok(req) => req.set_pkce_verifier(PkceCodeVerifier::new(pending.pkce_verifier.clone())),
        Err(e) => return fail(format!("token endpoint not configured: {e}")),
    };
    let token = match exchange.request_async(&o.http).await {
        Ok(token) => token,
        Err(e) => return fail(format!("code exchange failed: {e}")),
    };
    let Some(id_token) = token.id_token() else {
        return fail("token response contained no ID token".to_string());
    };
    let nonce = Nonce::new(pending.nonce.clone());
    let claims = match id_token.claims(&client.id_token_verifier(), &nonce) {
        Ok(claims) => claims,
        Err(first_err) => {
            // The signing key may have rotated since startup; refresh the
            // JWKS once (rate-limited) and retry before giving up.
            if o.refresh_keys_rate_limited().await {
                let client = o.client();
                match id_token.claims(&client.id_token_verifier(), &nonce) {
                    Ok(claims) => claims,
                    Err(e) => return fail(format!("ID token validation failed: {e}")),
                }
            } else {
                return fail(format!("ID token validation failed: {first_err}"));
            }
        }
    };

    // Persist only allowlist-relevant groups: a large IdP groups claim would
    // otherwise blow the encrypted session cookie past the 4 KB browser limit
    // and redirect-loop the login. `permits` only tests against these.
    let groups = rt
        .allow
        .relevant_groups(extract_groups(&claims.additional_claims().0, &o.cfg.groups_claim));
    let identity = Identity {
        subject: claims.subject().to_string(),
        email: claims.email().map(|e| e.to_string()),
        name: claims
            .name()
            .and_then(|n| n.get(None))
            .map(|n| n.to_string()),
        groups,
        expires_at: now_ts() + o.cfg.session_ttl_secs,
    };
    if !rt.allow.permits(&identity) {
        // Session cookie still set so the forbidden page's sign-out link
        // works; allowlists are re-checked per request against the identity
        // snapshot taken here (IdP-side changes land at the next sign-in).
        let (jar, clear_state) = session_response(&o.cookie_key, o.secure_cookies, &identity);
        return (jar, clear_state, forbidden_page(&identity, Some("/auth/logout"))).into_response();
    }
    tracing::info!(
        target: "rivers::auth",
        subject = %identity.subject,
        email = identity.email.as_deref().unwrap_or(""),
        "user signed in"
    );
    let (jar, clear_state) = session_response(&o.cookie_key, o.secure_cookies, &identity);
    (jar, clear_state, Redirect::to(&pending.rd)).into_response()
}

fn extract_groups(claims: &serde_json::Map<String, serde_json::Value>, claim: &str) -> Vec<String> {
    // Literal name first — Auth0-style namespaced claims contain dots
    // (`https://myapp.example.com/groups`). Only then treat dots as nested
    // traversal (Keycloak realm roles live at `realm_access.roles`).
    let mut value = claims.get(claim);
    if value.is_none() {
        let mut cursor = claims;
        let mut parts = claim.split('.').peekable();
        while let Some(part) = parts.next() {
            match cursor.get(part) {
                Some(serde_json::Value::Object(next)) if parts.peek().is_some() => cursor = next,
                Some(v) if parts.peek().is_none() => {
                    value = Some(v);
                    break;
                }
                _ => break,
            }
        }
    }
    match value {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Some(serde_json::Value::String(s)) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

pub async fn logout(State(rt): State<Arc<AuthRuntime>>) -> Response {
    let Some(o) = expect_oidc(&rt) else {
        return error_page(axum::http::StatusCode::NOT_FOUND, "Not found", "", None);
    };
    let clear = clear_cookies(o.secure_cookies);
    if o.cfg.rp_logout {
        if let Some(end) = &o.end_session_endpoint {
            if let Ok(mut url) = url::Url::parse(end) {
                // No tokens stored, so no id_token_hint; client_id is
                // the spec's alternative.
                url.query_pairs_mut()
                    .append_pair("client_id", &o.cfg.client_id)
                    .append_pair("post_logout_redirect_uri", &o.cfg.public_url);
                return (clear, Redirect::to(url.as_str())).into_response();
            }
        }
    }
    (clear, Redirect::to("/")).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_rd_rejects_offsite() {
        assert_eq!(sanitize_rd(Some("/runs?page=2")), "/runs?page=2");
        assert_eq!(sanitize_rd(Some("//evil.com/x")), "/");
        assert_eq!(sanitize_rd(Some("https://evil.com")), "/");
        assert_eq!(sanitize_rd(Some("/ok\\..")), "/");
        assert_eq!(sanitize_rd(Some("/with space")), "/");
        assert_eq!(sanitize_rd(Some("/crlf\r\nInjected: x")), "/");
        assert_eq!(sanitize_rd(None), "/");
    }

    #[test]
    fn groups_from_array_string_and_nested() {
        let claims: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({
                "groups": ["eng", "admins"],
                "flat": "a, b ,",
                "realm_access": {"roles": ["r1", "r2"]},
                // Auth0-style namespaced claim: the name itself contains dots.
                "https://myapp.example.com/groups": ["ns1", "ns2"],
            }),
        )
        .unwrap();
        assert_eq!(extract_groups(&claims, "groups"), vec!["eng", "admins"]);
        assert_eq!(extract_groups(&claims, "flat"), vec!["a", "b"]);
        assert_eq!(extract_groups(&claims, "realm_access.roles"), vec!["r1", "r2"]);
        assert_eq!(
            extract_groups(&claims, "https://myapp.example.com/groups"),
            vec!["ns1", "ns2"],
            "literal claim names must win over dotted traversal"
        );
        assert!(extract_groups(&claims, "missing").is_empty());
    }
}
