//! Router-level auth tests: forward-mode gating semantics and the full OIDC
//! authorization-code flow against an in-process mock IdP.

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode, header};
use axum::response::Json;
use axum::routing::{get, post};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

use super::config::{Allowlists, AuthConfig, AuthMode, ForwardConfig, OidcConfig};
use super::oidc::is_signing_key_failure;
use super::identity::Identity;
use super::test_cookies::CookieStore;
use super::oidc::RawClaims;
use super::{AuthRuntime, apply_auth};

// Throwaway 2048-bit key for the mock IdP. Test fixture only.
const TEST_RSA_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAkgjwrXObaRzNuBLLy3mkGlPeAQZxxYxh1I7U2SQcqQF9/Y+L
wxcyTVHD+sqHUdrBV1IBukVgSlDSKa3gjckdx6ejA4TV0UEvunixmjh8fbTze64B
d1u7EJ6iBTmGW2TKFFsciFdA81DCGnbNat1wCT77Gk5lst50NWTejBsDPyLO1hPT
cS3nJgL4FiMGgT/auuL6Y8+JO0jhwpO30pvqViTR1BPsJIMr+OJCrfNiAmdM8bUn
GqA65YONrRmLdDrBOvRBDyG4jjYYHChGhUP609aPjI7R6xl0y34b5OBuUdtALYF/
GQDBRt1pPgBS9kWOyHpKLjNq6om9m2yVLYnNGQIDAQABAoIBABC5DwfrKrClQ0Dh
KiQ8DZjpjLfVAJdM2vEdMBjzjof4q0WrjVItva1mqnN1g3x8o/z3X5+bN9/FaeOq
99bUtovOpxdikUemrYFZi+EAt0TVQWKi4Pka8IzX1ZmVO6JgpCwjk4dgvTJ/x30s
qJukYvg0FaDZZGz7a2xSh5JHZPOdlleN0Av6tuvnOhgtq5cx/6SMxVGMv9F5I96m
pJ3E3wsD/1vCV7jP2JkbNm2JY288uxU7sHBpqfxi+HbXTMCLNgk4qo68h36xr9wD
V51aAJB2W/UGQ8cV2oNxC62HGpPtyVmskx5v8jnGutTyDOMh02SC/imh6im8QLCs
jCl8/d8CgYEAxkTPk3yWwaZJLGNZXltf0VVMdbwW1EUC+bQlxtQOlPbeG7FiLNFj
K9Ft/hpPRta5XLEQ8gnPbQOPLrcnJpwSOpbsa4JhLbMizYR0KXx/8517oycMsnxG
M2YuJf8zPsTnDF3emBZ/YzXiVwxo+5VetiVm0x+BeUK8d5qqLeyzHssCgYEAvI6O
HsWtq0YhHnp+tfBvyZcDN+743QvvaT+1Y84F/1dt4Zt8tHGPtnDs091D6cGMNtSC
sskHzQW+muxtLK90Dp9MnarYw9NR+BrCGD8RKCRxkgc0oOhcBvhRojPVQ1Kcb4wN
jYp+Y3WvRZ85MDZkSMFUvhh/3TZD3TWvuLodwysCgYBDtMPd7bHdt1dNnS+rlTCH
X8WYfv6cxmRZuTcdStUf8Z2vf0ezXl2rXP1exMVFv5XVHXJX9RmsdIa0wT7RZIKl
F1zs6b0dygqcfBre//EB1EmgUXl4ig+/BanEt/1b9gmgo32cGjKuQnxklYxUPZH2
SZdviVbBfhS2E08CF86jOQKBgQCw0ADHLFk5ZY7C9N0DIQ7Ce6Bh/+5P4dRD3qDq
kRQgp8x7JYHf9ylrTCNYXIFFnuArvkU8/7QX9k4RGqkZoQF0gL6ojr+rieqwe+8M
K3+cI+h3pdgdFybMxmhOcMqH0dyt4SgIVRlFjOKpp7BJ3IdXjis4AuNL/YnP0nsP
/z7PdwKBgAIfzRzk6TjxkdgNhZjpktphmJ7nEgkZ7TNwmTwPb84UwPzXd8uAioTi
s8Lyi6IwpNePz0nwihn3m81hWHEaMgn/NhXcMDk4o6+J7YS/y7ysFE0UlYbUa0B+
ich5kRt1VwWXYpNYnj6zAgf8FbN+I+3B0B+bYPXZdKM241NxUHI/
-----END RSA PRIVATE KEY-----";

fn app(rt: Option<Arc<AuthRuntime>>) -> Router {
    let inner = Router::new()
        .route("/", get(|| async { "home" }))
        .route(
            "/api/whoami",
            get(|req: Request<Body>| async move {
                req.extensions()
                    .get::<Identity>()
                    .map(|i| i.subject.clone())
                    .unwrap_or_default()
            }),
        )
        .route("/healthz", get(|| async { StatusCode::OK }));
    apply_auth(inner, rt)
}

fn req(uri: &str, peer: Option<&str>, headers: &[(&str, &str)]) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let mut req = builder.body(Body::empty()).unwrap();
    if let Some(ip) = peer {
        let addr = SocketAddr::new(ip.parse().unwrap(), 40000);
        req.extensions_mut().insert(ConnectInfo(addr));
    }
    req
}



async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

// ── forward mode ────────────────────────────────────────────────────────

async fn forward_rt(allow: Allowlists) -> Arc<AuthRuntime> {
    AuthRuntime::initialize(AuthConfig {
        mode: AuthMode::Forward(ForwardConfig {
            trusted_proxies: vec!["10.42.0.0/16".into()],
            user_header: "Remote-User".into(),
            email_header: "Remote-Email".into(),
            groups_header: "Remote-Groups".into(),
            name_header: "Remote-Name".into(),
            logout_url: None,
        }),
        allow,
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn forward_untrusted_peer_is_403_even_with_headers() {
    let app = app(Some(forward_rt(Allowlists::default()).await));
    let resp = app
        .oneshot(req("/", Some("192.168.1.9"), &[("Remote-User", "admin")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn forward_missing_connect_info_is_403() {
    let app = app(Some(forward_rt(Allowlists::default()).await));
    let resp = app
        .oneshot(req("/", None, &[("Remote-User", "admin")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn forward_trusted_peer_without_headers_is_401() {
    let app = app(Some(forward_rt(Allowlists::default()).await));
    let resp = app.oneshot(req("/", Some("10.42.7.7"), &[])).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn forward_trusted_peer_with_headers_passes_identity() {
    let app = app(Some(forward_rt(Allowlists::default()).await));
    let resp = app
        .oneshot(req(
            "/api/whoami",
            Some("10.42.7.7"),
            &[("Remote-User", "jdoe"), ("Remote-Email", "john.doe@example.com")],
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "jdoe");
}

#[tokio::test]
async fn forward_healthz_is_public() {
    let app = app(Some(forward_rt(Allowlists::default()).await));
    let resp = app
        .oneshot(req("/healthz", Some("192.168.1.9"), &[]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn forward_allowlists_gate_authenticated_users() {
    let allow = Allowlists {
        users: vec!["alice".into()],
        ..Default::default()
    };
    let app = app(Some(forward_rt(allow).await));
    let resp = app
        .clone()
        .oneshot(req("/", Some("10.42.7.7"), &[("Remote-User", "bob")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = app
        .oneshot(req("/", Some("10.42.7.7"), &[("Remote-User", "alice")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── oidc mode, mock IdP ─────────────────────────────────────────────────

struct MockIdp {
    issuer: String,
    /// Nonce the relying party sent in the authorize redirect; the test
    /// parses it off the login response and plants it here so the token
    /// endpoint can bake it into the ID token.
    nonce: Mutex<Option<String>>,
    subject: String,
    email: String,
    /// `email_verified` claim baked into the ID token; `Some(true)` is the
    /// normal org-IdP case. Flip to `Some(false)`/`None` to model an IdP that
    /// lets a user self-assert an unverified address.
    email_verified: Mutex<Option<bool>>,
    groups: Vec<String>,
    /// Current signing-key id; both `/jwks` and `/token` read it, so flipping
    /// it simulates an IdP key rotation.
    key_id: Mutex<String>,
    /// When true, the discovery endpoint 500s — simulates a transient IdP
    /// outage during a key-refresh attempt.
    discovery_fails: Mutex<bool>,
    /// When armed, `/discovery` signals `discovery_entered` and then blocks on
    /// `discovery_release`, pinning one key-refresh mid-flight so a second
    /// concurrent callback races it.
    hold_discovery: std::sync::atomic::AtomicBool,
    discovery_entered: tokio::sync::Notify,
    discovery_release: tokio::sync::Notify,
    /// Number of `/token` exchanges served — lets a test observe that a second
    /// callback has passed code exchange.
    token_hits: std::sync::atomic::AtomicUsize,
    /// One-shot: when armed, the FIRST `/token` call signals `token_entered` and
    /// blocks on `token_release`, pinning one callback between its client
    /// snapshot and its id-token validation while another refreshes the keys.
    hold_token: std::sync::atomic::AtomicBool,
    token_entered: tokio::sync::Notify,
    token_release: tokio::sync::Notify,
    /// When set, `/token` returns an RFC 6749 `invalid_grant` error body.
    token_errors: std::sync::atomic::AtomicBool,
}

type MockState = Arc<MockIdp>;

async fn discovery(State(idp): State<MockState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    if *idp.discovery_fails.lock().unwrap() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if idp.hold_discovery.load(std::sync::atomic::Ordering::Relaxed) {
        idp.discovery_entered.notify_one();
        idp.discovery_release.notified().await;
    }
    let iss = &idp.issuer;
    Json(serde_json::json!({
        "issuer": iss,
        "authorization_endpoint": format!("{iss}/authorize"),
        "token_endpoint": format!("{iss}/token"),
        "jwks_uri": format!("{iss}/jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "end_session_endpoint": format!("{iss}/logout"),
    }))
    .into_response()
}

fn signing_key(kid: &str) -> openidconnect::core::CoreRsaPrivateSigningKey {
    openidconnect::core::CoreRsaPrivateSigningKey::from_pem(
        TEST_RSA_PEM,
        Some(openidconnect::JsonWebKeyId::new(kid.into())),
    )
    .unwrap()
}

async fn jwks(State(idp): State<MockState>) -> Json<serde_json::Value> {
    use openidconnect::PrivateSigningKey;
    use openidconnect::core::CoreJsonWebKeySet;
    let kid = idp.key_id.lock().unwrap().clone();
    let jwks = CoreJsonWebKeySet::new(vec![signing_key(&kid).as_verification_key()]);
    Json(serde_json::to_value(&jwks).unwrap())
}

async fn token(State(idp): State<MockState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    if idp.token_errors.load(std::sync::atomic::Ordering::Relaxed) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "authorization code is invalid or has expired",
            })),
        )
            .into_response();
    }
    // One-shot pin: park the first callback here (after it snapshotted the
    // verifying client) so a second callback can rotate the keys underneath it.
    if idp.hold_token.swap(false, std::sync::atomic::Ordering::Relaxed) {
        idp.token_entered.notify_one();
        idp.token_release.notified().await;
    }
    use openidconnect::core::{
        CoreGenderClaim, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm, CoreTokenType,
    };
    use openidconnect::{
        AccessToken, Audience, EmptyExtraTokenFields, EndUserEmail, EndUserName, IdToken,
        IdTokenClaims, IdTokenFields, IssuerUrl, LocalizedClaim, Nonce, StandardClaims,
        StandardTokenResponse, SubjectIdentifier,
    };

    let mut name = LocalizedClaim::new();
    name.insert(None, EndUserName::new("John Doe".into()));
    let mut groups = serde_json::Map::new();
    groups.insert("groups".into(), serde_json::json!(idp.groups));
    let claims = IdTokenClaims::<RawClaims, CoreGenderClaim>::new(
        IssuerUrl::new(idp.issuer.clone()).unwrap(),
        vec![Audience::new("rivers".into())],
        chrono::Utc::now() + chrono::Duration::seconds(300),
        chrono::Utc::now(),
        StandardClaims::new(SubjectIdentifier::new(idp.subject.clone()))
            .set_email(Some(EndUserEmail::new(idp.email.clone())))
            .set_email_verified(*idp.email_verified.lock().unwrap())
            .set_name(Some(name)),
        RawClaims(groups),
    )
    .set_nonce(idp.nonce.lock().unwrap().clone().map(Nonce::new));

    let kid = idp.key_id.lock().unwrap().clone();
    let id_token = IdToken::<
        RawClaims,
        CoreGenderClaim,
        CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm,
    >::new(
        claims,
        &signing_key(&kid),
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        None,
        None,
    )
    .unwrap();

    let fields = IdTokenFields::new(Some(id_token), EmptyExtraTokenFields {});
    let mut resp =
        StandardTokenResponse::new(AccessToken::new("test-access".into()), CoreTokenType::Bearer, fields);
    resp.set_expires_in(Some(&std::time::Duration::from_secs(3600)));
    idp.token_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::to_value(&resp).unwrap()).into_response()
}

async fn spawn_mock_idp(subject: &str, email: &str) -> MockState {
    spawn_mock_idp_with_groups(subject, email, vec!["eng".into(), "admins".into()]).await
}

async fn spawn_mock_idp_with_groups(subject: &str, email: &str, groups: Vec<String>) -> MockState {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(MockIdp {
        issuer,
        nonce: Mutex::new(None),
        subject: subject.to_string(),
        email: email.to_string(),
        email_verified: Mutex::new(Some(true)),
        groups,
        key_id: Mutex::new("test".into()),
        discovery_fails: Mutex::new(false),
        hold_discovery: std::sync::atomic::AtomicBool::new(false),
        discovery_entered: tokio::sync::Notify::new(),
        discovery_release: tokio::sync::Notify::new(),
        token_hits: std::sync::atomic::AtomicUsize::new(0),
        hold_token: std::sync::atomic::AtomicBool::new(false),
        token_entered: tokio::sync::Notify::new(),
        token_release: tokio::sync::Notify::new(),
        token_errors: std::sync::atomic::AtomicBool::new(false),
    });
    let router = Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/token", post(token))
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    state
}

async fn oidc_rt(idp: &MockIdp, allow: Allowlists) -> Arc<AuthRuntime> {
    AuthRuntime::initialize(AuthConfig {
        mode: AuthMode::Oidc(OidcConfig {
            issuer: idp.issuer.clone(),
            client_id: "rivers".into(),
            client_secret: None,
            public_url: "http://ui.test".parse().unwrap(),
            scopes: vec!["openid".into(), "profile".into(), "email".into()],
            groups_claim: "groups".into(),
            rp_logout: false,
            cookie_secret: None,
            session_ttl_secs: 3600,
        }),
        allow,
    })
    .await
    .unwrap()
    .unwrap()
}

/// Drive `/auth/login` and return `(callback_uri, cookie_store)` with the
/// mock primed to embed the flow's nonce.
async fn drive_login(app: &Router, idp: &MockIdp) -> (String, CookieStore) {
    let resp = app
        .clone()
        .oneshot(req("/auth/login?rd=/runs", None, &[]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    let authorize =
        url::Url::parse(resp.headers()[header::LOCATION].to_str().unwrap()).unwrap();
    assert!(authorize.as_str().starts_with(&idp.issuer));
    let params: HashMap<String, String> = authorize.query_pairs().into_owned().collect();
    assert_eq!(params["response_type"], "code");
    assert_eq!(params["client_id"], "rivers");
    assert_eq!(params["redirect_uri"], "http://ui.test/auth/callback");
    assert!(params.contains_key("code_challenge"));
    *idp.nonce.lock().unwrap() = Some(params["nonce"].clone());
    let callback = format!("/auth/callback?code=test-code&state={}", params["state"]);
    let mut store = CookieStore::default();
    store.absorb(&resp);
    (callback, store)
}

#[tokio::test]
async fn oidc_full_flow() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    // Unauthenticated document GET bounces into the login flow…
    let resp = app
        .clone()
        .oneshot(req("/", None, &[("accept", "text/html")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert!(
        resp.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .starts_with("/auth/login?rd=")
    );
    // …while API requests get a bare 401.
    let resp = app.clone().oneshot(req("/api/whoami", None, &[])).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let (callback, mut store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "callback should succeed");
    assert_eq!(resp.headers()[header::LOCATION], "/runs");
    store.absorb(&resp);

    let resp = app
        .clone()
        .oneshot(req("/api/whoami", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "sub-42");

    // Logout clears the session; a browser-faithful store must no longer
    // authenticate (regression: removals were once silently unemitted).
    let resp = app
        .clone()
        .oneshot(req("/auth/logout", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    store.absorb(&resp);
    let resp = app
        .clone()
        .oneshot(req("/api/whoami", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// A failed/forged callback must NOT clear the session cookie — otherwise any
/// cross-site `GET /auth/callback?error=x` force-logs-out a signed-in user.
#[tokio::test]
async fn callback_failure_preserves_existing_session() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    // Establish a real session.
    let (callback, mut store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    store.absorb(&resp);
    assert!(store.contains("rivers_session"), "logged in");

    // A forged callback (attacker-triggerable cross-site, no valid state).
    let resp = app
        .clone()
        .oneshot(req(
            "/auth/callback?error=access_denied",
            None,
            &[("cookie", &store.header_value())],
        ))
        .await
        .unwrap();
    store.absorb(&resp);

    // The session must survive; only a real /auth/logout ends it.
    assert!(
        store.contains("rivers_session"),
        "callback failure must not delete the session cookie"
    );
    let resp = app
        .clone()
        .oneshot(req("/api/whoami", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "still authenticated");
}

/// The session-expiry redirect in `CurrentUserChip` needs a 401 the
/// server-fn client can actually decode: an empty body decodes to a generic
/// `Deserialization` error, indistinguishable from any other failure, so the
/// client-side login redirect can never fire. The middleware must emit the
/// `Variant|payload` wire format with our marker.
#[tokio::test]
async fn api_401_body_is_server_fn_decodable() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let resp = app.oneshot(req("/api/whoami", None, &[])).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(resp).await;
    assert_eq!(
        body,
        format!("ServerError|{}", crate::helpers::UNAUTHORIZED_MARKER),
        "401 body must decode as ServerFnError::ServerError with the marker"
    );
    // What the WASM client does with it: decode → is_unauthorized.
    let decoded: leptos::prelude::ServerFnError =
        leptos::server_fn::error::FromServerFnError::de(leptos::server_fn::Bytes::from(body));
    assert!(crate::helpers::is_unauthorized(&decoded));
    assert!(!crate::helpers::is_unauthorized(
        &leptos::prelude::ServerFnError::ServerError("disk full".into())
    ));
}

/// A hung IdP (TCP accepted, no HTTP response) must fail discovery instead
/// of blocking forever: `initialize` runs before the UI binds its listener,
/// so an unbounded request hangs the process with `/healthz` never served.
#[tokio::test]
async fn oidc_initialize_times_out_against_hung_idp() {
    // Bound but never accepted — connections sit in the backlog with no
    // response bytes, which only an HTTP-client timeout escapes.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());

    let init = AuthRuntime::initialize(AuthConfig {
        mode: AuthMode::Oidc(OidcConfig {
            issuer,
            client_id: "rivers".into(),
            client_secret: None,
            public_url: "http://ui.test".parse().unwrap(),
            scopes: vec!["openid".into()],
            groups_claim: "groups".into(),
            rp_logout: false,
            cookie_secret: None,
            session_ttl_secs: 3600,
        }),
        allow: Allowlists::default(),
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), init)
        .await
        .expect("initialize must fail fast on a hung IdP, not block startup");
    assert!(result.is_err(), "hung discovery must surface an error");
    drop(listener);
}

/// IdPs rotate signing keys routinely. The verifying client snapshots the
/// JWKS at startup; without an on-miss refresh, every login fails once the
/// key rotates and only a process restart recovers. A token signed by a
/// newly-rotated key must still validate — the callback re-runs discovery and
/// retries.
#[tokio::test]
async fn oidc_refreshes_signing_keys_after_rotation() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    // Baseline: login with the startup key succeeds.
    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);

    // IdP rotates to a key the client has never seen.
    *idp.key_id.lock().unwrap() = "test-rotated".into();

    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SEE_OTHER,
        "login must recover after key rotation via JWKS refresh, got {}",
        resp.status()
    );
}

/// A refresh that fails (transient IdP outage) must NOT latch the cooldown:
/// once the IdP recovers, the very next login must be able to refresh again.
#[tokio::test]
async fn oidc_recovers_after_a_transient_refresh_failure() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    // Baseline login with the startup key.
    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);

    // Key rotates, but discovery is down: the on-miss refresh errors.
    *idp.key_id.lock().unwrap() = "rotated".into();
    *idp.discovery_fails.lock().unwrap() = true;
    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "refresh failed while discovery was down"
    );

    // IdP recovers. A failed refresh must not have consumed the cooldown, so
    // this login refreshes successfully and gets in.
    *idp.discovery_fails.lock().unwrap() = false;
    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SEE_OTHER,
        "must recover on the next login after a transient refresh failure, got {}",
        resp.status()
    );
}

/// Concurrent callbacks during a key rotation must ALL recover. The first to
/// win the refresh gate re-fetches the JWKS while the others wait on the gate,
/// then retry against the refreshed keys — a race-loser must not be turned
/// away with a 502 (the old `try_acquire` bailed instead of awaiting).
#[tokio::test]
async fn oidc_concurrent_callbacks_during_rotation_all_recover() {
    use std::sync::atomic::Ordering;
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    // One in-flight login; both concurrent callbacks replay its state cookie
    // (the mock accepts the fixed code and bakes the flow's nonce).
    let (callback, store) = drive_login(&app, &idp).await;
    let cookie = store.header_value();

    // Rotate to a never-seen key and pin the winner's refresh open.
    *idp.key_id.lock().unwrap() = "test-rotated".into();
    idp.hold_discovery.store(true, Ordering::Relaxed);

    let spawn_cb = |cookie: String, callback: String, app: Router| {
        tokio::spawn(async move {
            app.oneshot(req(&callback, None, &[("cookie", &cookie)]))
                .await
                .unwrap()
                .status()
        })
    };

    // Winner: fails validation, wins the gate, blocks inside discovery.
    let a = spawn_cb(cookie.clone(), callback.clone(), app.clone());
    idp.discovery_entered.notified().await;

    // Loser: same cookie; must WAIT on the gate, not bail with a 502.
    let b = spawn_cb(cookie.clone(), callback.clone(), app.clone());
    // Let the loser pass code exchange and reach its refresh attempt while the
    // winner is still pinned (keys not yet swapped).
    while idp.token_hits.load(Ordering::Relaxed) < 2 {
        tokio::task::yield_now().await;
    }
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }

    // Release the winner; it swaps in the rotated key, the loser then retries.
    idp.hold_discovery.store(false, Ordering::Relaxed);
    idp.discovery_release.notify_one();

    let (sa, sb) = tokio::join!(a, b);
    assert_eq!(sa.unwrap(), StatusCode::SEE_OTHER, "winner recovers");
    assert_eq!(
        sb.unwrap(),
        StatusCode::SEE_OTHER,
        "race-loser must recover too, not get a 502"
    );
}

/// A callback that snapshots the pre-rotation client but validates just AFTER a
/// concurrent login already refreshed the keys must still recover: its refresh
/// hits the cooldown (a refresh just landed), so it must retry against the live,
/// already-rotated client rather than 502. Complements
/// `oidc_concurrent_callbacks_during_rotation_all_recover`, which covers the
/// during-refresh ordering; this covers the just-after-refresh ordering.
#[tokio::test]
async fn oidc_callback_after_concurrent_refresh_still_recovers() {
    use std::sync::atomic::Ordering;
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    let cookie = store.header_value();

    // Rotate to a never-seen key; pin the loser's /token so it snapshots the
    // pre-rotation client and only validates after the winner has refreshed.
    *idp.key_id.lock().unwrap() = "test-rotated".into();
    idp.hold_token.store(true, Ordering::Relaxed);

    // Loser L: snapshots the stale client, then parks inside /token.
    let l = {
        let app = app.clone();
        let callback = callback.clone();
        let cookie = cookie.clone();
        tokio::spawn(async move {
            app.oneshot(req(&callback, None, &[("cookie", &cookie)]))
                .await
                .unwrap()
                .status()
        })
    };
    idp.token_entered.notified().await;

    // Winner W: fails against the stale key, refreshes, swaps the shared client,
    // and succeeds — advancing last_key_refresh so L's later refresh cooldowns.
    let w = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &cookie)]))
        .await
        .unwrap();
    assert_eq!(w.status(), StatusCode::SEE_OTHER, "winner recovers via refresh");

    // Release the loser: it validates the rotated token against its STALE
    // snapshot, its refresh hits the cooldown, and it must retry against the
    // live client instead of 502-ing.
    idp.token_release.notify_one();
    assert_eq!(
        l.await.unwrap(),
        StatusCode::SEE_OTHER,
        "a login validating just after a concurrent refresh must recover, not 502"
    );
}

/// Only an unidentifiable signing key is worth a JWKS refresh; a claim-level
/// failure (here: expiry) must not spend the refresh budget.
#[test]
fn only_unknown_key_failures_trigger_refresh() {
    use openidconnect::core::{
        CoreGenderClaim, CoreIdTokenVerifier, CoreJsonWebKeySet,
        CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
    };
    use openidconnect::{
        Audience, ClientId, IdToken, IdTokenClaims, IssuerUrl, Nonce, PrivateSigningKey,
        StandardClaims, SubjectIdentifier,
    };

    fn nonce_ok(_: Option<&Nonce>) -> Result<(), String> {
        Ok(())
    }
    let issuer = "http://idp.test";
    let token = |kid: &str, exp_offset: i64| {
        let claims = IdTokenClaims::<RawClaims, CoreGenderClaim>::new(
            IssuerUrl::new(issuer.into()).unwrap(),
            vec![Audience::new("rivers".into())],
            chrono::Utc::now() + chrono::Duration::seconds(exp_offset),
            chrono::Utc::now() - chrono::Duration::seconds(3600),
            StandardClaims::new(SubjectIdentifier::new("sub".into())),
            RawClaims(serde_json::Map::new()),
        );
        IdToken::<
            RawClaims,
            CoreGenderClaim,
            CoreJweContentEncryptionAlgorithm,
            CoreJwsSigningAlgorithm,
        >::new(
            claims,
            &signing_key(kid),
            CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
            None,
            None,
        )
        .unwrap()
    };
    let verifier = |kid: &str| {
        CoreIdTokenVerifier::new_public_client(
            ClientId::new("rivers".into()),
            IssuerUrl::new(issuer.into()).unwrap(),
            CoreJsonWebKeySet::new(vec![signing_key(kid).as_verification_key()]),
        )
    };

    // Signed with kid "A", verifier only knows kid "B" → NoMatchingKey.
    let unknown_kid = token("A", 300).claims(&verifier("B"), nonce_ok).unwrap_err();
    assert!(
        is_signing_key_failure(&unknown_kid),
        "unknown kid must trigger a refresh: {unknown_kid:?}"
    );

    // Signed with a known kid but expired → Expired, must NOT refresh.
    let expired = token("A", -300).claims(&verifier("A"), nonce_ok).unwrap_err();
    assert!(
        !is_signing_key_failure(&expired),
        "expiry must not trigger a refresh: {expired:?}"
    );
}

#[tokio::test]
async fn oidc_callback_rejects_state_mismatch() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let (_, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(
            "/auth/callback?code=test-code&state=forged",
            None,
            &[("cookie", &store.header_value())],
        ))
        .await
        .unwrap();
    // A forged/mismatched state is a client-side error → 4xx, not a 502
    // (a 5xx would trip ingress interception and false backend-outage alerts).
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn oidc_callback_without_state_cookie_fails() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let (callback, _) = drive_login(&app, &idp).await;
    let resp = app.clone().oneshot(req(&callback, None, &[])).await.unwrap();
    // Missing pending-login cookie is a client/session error → 4xx, not 502.
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// A token-endpoint OAuth error (invalid_grant on a reused/expired code) is a
/// client/flow error → 4xx, not a 502 that would trip ingress alerts.
#[tokio::test]
async fn oidc_token_endpoint_error_is_client_error_not_502() {
    use std::sync::atomic::Ordering;
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    idp.token_errors.store(true, Ordering::Relaxed);
    let resp = app
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// Logout without a session cookie must be a no-op: emitting a cookie removal
/// would let a cookie-less cross-site GET force-logout a signed-in user.
#[tokio::test]
async fn oidc_logout_without_session_emits_no_cookie() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let resp = app
        .oneshot(req(crate::routes::LOGOUT, None, &[]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert!(
        resp.headers().get_all(header::SET_COOKIE).iter().next().is_none(),
        "a cookie-less logout must not emit a Set-Cookie removal"
    );
}

#[tokio::test]
async fn oidc_allowlist_denies_at_callback_and_after() {
    let idp = spawn_mock_idp("sub-42", "john.doe@other.com").await;
    let allow = Allowlists {
        domains: vec!["example.com".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, mut store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    // The session cookie is set so sign-out works, but every request stays 403.
    store.absorb(&resp);
    let resp = app
        .clone()
        .oneshot(req("/", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// An unverified `email` claim must NOT satisfy the domain allowlist. An IdP
/// that lets users self-assert an address (email_verified=false) would
/// otherwise let an attacker forge membership in an allowed domain.
#[tokio::test]
async fn oidc_unverified_email_is_not_trusted_for_allowlist() {
    let idp = spawn_mock_idp("sub-42", "attacker@example.com").await;
    *idp.email_verified.lock().unwrap() = Some(false);
    let allow = Allowlists {
        domains: vec!["example.com".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "unverified email must not satisfy the domain allowlist"
    );
}

/// The normal path: a verified email claim satisfies the domain allowlist.
/// Guards against over-gating that would deny legitimate verified users.
#[tokio::test]
async fn oidc_verified_email_satisfies_domain_allowlist() {
    // email_verified defaults to Some(true) in the mock IdP.
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let allow = Allowlists {
        domains: vec!["example.com".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "verified email admits");
}

/// A verified email with surrounding whitespace still satisfies the domain
/// allowlist: the OIDC claim trim normalizes it before the domain split.
/// Without the trim the domain carries the trailing space and is denied.
#[tokio::test]
async fn oidc_padded_verified_email_is_trimmed_then_admitted() {
    let idp = spawn_mock_idp("sub-42", "  john.doe@example.com  ").await;
    let allow = Allowlists {
        domains: vec!["example.com".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SEE_OTHER,
        "a padded verified email must be trimmed, then admitted"
    );
}

#[tokio::test]
async fn oidc_groups_reach_allowlists() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    // Only the group admits this user (email domain doesn't match).
    let allow = Allowlists {
        domains: vec!["corp.com".into()],
        groups: vec!["admins".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
}

/// A user in hundreds of IdP groups must still receive a session cookie under
/// the ~4096-byte browser per-cookie limit; otherwise the browser silently
/// drops the Set-Cookie and the user redirect-loops between the app and
/// `/auth/login`. Only allowlist-relevant groups need to survive into the
/// cookie, so the stored set stays bounded regardless of claim size.
#[tokio::test]
async fn oidc_large_groups_claim_yields_bounded_cookie() {
    let big: Vec<String> = (0..500).map(|i| format!("group-{i:04}")).collect();
    let idp = spawn_mock_idp_with_groups("sub-42", "john.doe@example.com", big).await;
    // Admitted solely by one of the many groups.
    let allow = Allowlists {
        groups: vec!["group-0007".into()],
        ..Default::default()
    };
    let rt = oidc_rt(&idp, allow).await;
    let app = app(Some(rt));

    let (callback, mut store) = drive_login(&app, &idp).await;
    let resp = app
        .clone()
        .oneshot(req(&callback, None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "the matching group admits");

    let session_cookie = resp
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .find(|c| c.starts_with("rivers_session="))
        .expect("session cookie set");
    assert!(
        session_cookie.len() < 4096,
        "session cookie must stay under the 4KB browser limit, got {} bytes",
        session_cookie.len()
    );

    // The reduced group set still admits on subsequent requests.
    store.absorb(&resp);
    let resp = app
        .clone()
        .oneshot(req("/", None, &[("cookie", &store.header_value())]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
