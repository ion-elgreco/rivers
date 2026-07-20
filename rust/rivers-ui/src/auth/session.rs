//! Encrypted session + OAuth-state cookies (private jar, AES-256-GCM);
//! the session cookie carries only the serialized `Identity`.

use axum::http::{HeaderMap, HeaderName, header::SET_COOKIE};
use axum::response::AppendHeaders;
use axum_extra::extract::cookie::{Cookie, Key, PrivateCookieJar, SameSite};
use serde::{Deserialize, Serialize};

use super::identity::Identity;

/// `__Host-` requires Secure + Path=/ + no Domain, which pins the cookie to
/// exactly this origin; plain names are the http://localhost dev fallback.
pub fn session_cookie_name(secure: bool) -> &'static str {
    if secure { "__Host-rivers_session" } else { "rivers_session" }
}

pub fn state_cookie_name(secure: bool) -> &'static str {
    if secure { "__Host-rivers_oauth_state" } else { "rivers_oauth_state" }
}

/// In-flight login state, minted at `/auth/login` and consumed once at
/// `/auth/callback`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingLogin {
    pub state: String,
    pub nonce: String,
    pub pkce_verifier: String,
    pub rd: String,
    pub iat: i64,
}

pub const PENDING_LOGIN_TTL_SECS: i64 = 600;

fn build(name: &'static str, value: String, secure: bool) -> Cookie<'static> {
    Cookie::build((name, value))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(secure)
        .build()
}

/// Explicit removal header. A from-scratch jar's `remove` emits nothing —
/// cookie-rs only deltas removals for cookies it saw as request originals.
fn removal_header(name: &str) -> (HeaderName, String) {
    (
        SET_COOKIE,
        format!("{name}=; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"),
    )
}

pub fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Read + decrypt a JSON payload from a private cookie.
fn read_json<T: for<'de> Deserialize<'de>>(
    headers: &HeaderMap,
    key: &Key,
    name: &str,
) -> Option<T> {
    let jar = PrivateCookieJar::from_headers(headers, key.clone());
    let cookie = jar.get(name)?;
    serde_json::from_str(cookie.value()).ok()
}

pub fn read_session(headers: &HeaderMap, key: &Key, secure: bool) -> Option<Identity> {
    let id: Identity = read_json(headers, key, session_cookie_name(secure))?;
    (id.expires_at > now_ts()).then_some(id)
}

pub fn read_pending_login(headers: &HeaderMap, key: &Key, secure: bool) -> Option<PendingLogin> {
    let pending: PendingLogin = read_json(headers, key, state_cookie_name(secure))?;
    (pending.iat + PENDING_LOGIN_TTL_SECS > now_ts()).then_some(pending)
}

/// Sets the session cookie and clears the OAuth-state cookie.
pub fn session_response(
    key: &Key,
    secure: bool,
    identity: &Identity,
) -> (PrivateCookieJar, AppendHeaders<[(HeaderName, String); 1]>) {
    let value = serde_json::to_string(identity).expect("Identity serializes");
    (
        PrivateCookieJar::new(key.clone()).add(build(session_cookie_name(secure), value, secure)),
        AppendHeaders([removal_header(state_cookie_name(secure))]),
    )
}

/// Sets the OAuth-state cookie for an in-flight login.
pub fn pending_login_jar(key: &Key, secure: bool, pending: &PendingLogin) -> PrivateCookieJar {
    let value = serde_json::to_string(pending).expect("PendingLogin serializes");
    PrivateCookieJar::new(key.clone()).add(build(state_cookie_name(secure), value, secure))
}

/// Clears both cookies (logout / callback failure).
pub fn clear_cookies(secure: bool) -> AppendHeaders<[(HeaderName, String); 2]> {
    AppendHeaders([
        removal_header(session_cookie_name(secure)),
        removal_header(state_cookie_name(secure)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::test_cookies::CookieStore;
    use axum::response::IntoResponse;

    fn identity(expires_at: i64) -> Identity {
        Identity {
            subject: "sub".into(),
            email: Some("john.doe@example.com".into()),
            name: Some("John Doe".into()),
            groups: vec!["eng".into()],
            expires_at,
        }
    }

    fn absorb(store: &mut CookieStore, resp: impl IntoResponse) {
        store.absorb(&resp.into_response());
    }

    #[test]
    fn session_roundtrip() {
        let key = Key::generate();
        let id = identity(now_ts() + 60);
        let mut store = CookieStore::default();
        absorb(&mut store, session_response(&key, false, &id));
        assert_eq!(read_session(&store.headers(), &key, false).unwrap(), id);
    }

    #[test]
    fn expired_session_rejected() {
        let key = Key::generate();
        let mut store = CookieStore::default();
        absorb(&mut store, session_response(&key, false, &identity(now_ts() - 1)));
        assert!(read_session(&store.headers(), &key, false).is_none());
    }

    #[test]
    fn wrong_key_rejected() {
        let key = Key::generate();
        let mut store = CookieStore::default();
        absorb(&mut store, session_response(&key, false, &identity(now_ts() + 60)));
        assert!(read_session(&store.headers(), &Key::generate(), false).is_none());
    }

    #[test]
    fn pending_login_roundtrip_and_ttl() {
        let key = Key::generate();
        let fresh = PendingLogin {
            state: "s".into(),
            nonce: "n".into(),
            pkce_verifier: "v".into(),
            rd: "/runs".into(),
            iat: now_ts(),
        };
        let mut store = CookieStore::default();
        absorb(&mut store, pending_login_jar(&key, false, &fresh));
        assert!(read_pending_login(&store.headers(), &key, false).is_some());

        let stale = PendingLogin {
            iat: now_ts() - PENDING_LOGIN_TTL_SECS - 1,
            ..fresh
        };
        let mut store = CookieStore::default();
        absorb(&mut store, pending_login_jar(&key, false, &stale));
        assert!(read_pending_login(&store.headers(), &key, false).is_none());
    }

    /// Regression: removal must be an explicit Set-Cookie a browser acts on —
    /// a from-scratch jar's `remove` emits nothing (caught live on k3d).
    #[test]
    fn clear_cookies_deletes_a_held_session() {
        let key = Key::generate();
        let mut store = CookieStore::default();
        absorb(&mut store, session_response(&key, false, &identity(now_ts() + 60)));
        assert!(read_session(&store.headers(), &key, false).is_some());

        absorb(&mut store, clear_cookies(false));
        assert!(!store.contains(session_cookie_name(false)));
        assert!(read_session(&store.headers(), &key, false).is_none());
    }

    #[test]
    fn session_response_clears_state_cookie() {
        let key = Key::generate();
        let mut store = CookieStore::default();
        let pending = PendingLogin {
            state: "s".into(),
            nonce: "n".into(),
            pkce_verifier: "v".into(),
            rd: "/".into(),
            iat: now_ts(),
        };
        absorb(&mut store, pending_login_jar(&key, false, &pending));
        assert!(store.contains(state_cookie_name(false)));

        absorb(&mut store, session_response(&key, false, &identity(now_ts() + 60)));
        assert!(!store.contains(state_cookie_name(false)));
        assert!(read_session(&store.headers(), &key, false).is_some());
    }
}
