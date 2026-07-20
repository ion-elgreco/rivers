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
    groups: Vec<String>,
}

type MockState = Arc<MockIdp>;

async fn discovery(State(idp): State<MockState>) -> Json<serde_json::Value> {
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
}

fn signing_key() -> openidconnect::core::CoreRsaPrivateSigningKey {
    openidconnect::core::CoreRsaPrivateSigningKey::from_pem(
        TEST_RSA_PEM,
        Some(openidconnect::JsonWebKeyId::new("test".into())),
    )
    .unwrap()
}

async fn jwks() -> Json<serde_json::Value> {
    use openidconnect::PrivateSigningKey;
    use openidconnect::core::CoreJsonWebKeySet;
    let jwks = CoreJsonWebKeySet::new(vec![signing_key().as_verification_key()]);
    Json(serde_json::to_value(&jwks).unwrap())
}

async fn token(State(idp): State<MockState>) -> Json<serde_json::Value> {
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
            .set_name(Some(name)),
        RawClaims(groups),
    )
    .set_nonce(idp.nonce.lock().unwrap().clone().map(Nonce::new));

    let id_token = IdToken::<
        RawClaims,
        CoreGenderClaim,
        CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm,
    >::new(
        claims,
        &signing_key(),
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        None,
        None,
    )
    .unwrap();

    let fields = IdTokenFields::new(Some(id_token), EmptyExtraTokenFields {});
    let mut resp =
        StandardTokenResponse::new(AccessToken::new("test-access".into()), CoreTokenType::Bearer, fields);
    resp.set_expires_in(Some(&std::time::Duration::from_secs(3600)));
    Json(serde_json::to_value(&resp).unwrap())
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
        groups,
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
            public_url: "http://ui.test".into(),
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
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn oidc_callback_without_state_cookie_fails() {
    let idp = spawn_mock_idp("sub-42", "john.doe@example.com").await;
    let rt = oidc_rt(&idp, Allowlists::default()).await;
    let app = app(Some(rt));

    let (callback, _) = drive_login(&app, &idp).await;
    let resp = app.clone().oneshot(req(&callback, None, &[])).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
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
