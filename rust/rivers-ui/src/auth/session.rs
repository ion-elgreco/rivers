//! Encrypted session + OAuth-state cookies (private jar, AES-256-GCM);
//! the session cookie carries only the serialized `Identity`.

use axum::http::HeaderMap;
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

fn removal(name: &'static str) -> Cookie<'static> {
    Cookie::build((name, "")).path("/").build()
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
pub fn session_jar(key: &Key, secure: bool, identity: &Identity) -> PrivateCookieJar {
    let value = serde_json::to_string(identity).expect("Identity serializes");
    PrivateCookieJar::new(key.clone())
        .add(build(session_cookie_name(secure), value, secure))
        .remove(removal(state_cookie_name(secure)))
}

/// Sets the OAuth-state cookie for an in-flight login.
pub fn pending_login_jar(key: &Key, secure: bool, pending: &PendingLogin) -> PrivateCookieJar {
    let value = serde_json::to_string(pending).expect("PendingLogin serializes");
    PrivateCookieJar::new(key.clone()).add(build(state_cookie_name(secure), value, secure))
}

/// Clears both cookies (logout / callback failure).
pub fn clear_jar(key: &Key, secure: bool) -> PrivateCookieJar {
    PrivateCookieJar::new(key.clone())
        .remove(removal(session_cookie_name(secure)))
        .remove(removal(state_cookie_name(secure)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(expires_at: i64) -> Identity {
        Identity {
            subject: "sub".into(),
            email: Some("john.doe@example.com".into()),
            name: Some("John Doe".into()),
            groups: vec!["eng".into()],
            expires_at,
        }
    }

    /// Convert a jar's Set-Cookie response headers into a request Cookie
    /// header, preserving the encrypted-on-the-wire values (`jar.iter()`
    /// would yield decrypted ones).
    fn headers_from(jar: PrivateCookieJar) -> HeaderMap {
        use axum::http::header::{COOKIE, SET_COOKIE};
        use axum::response::IntoResponse;
        let resp = (jar, ()).into_response();
        let pairs: Vec<String> = resp
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or("").to_string())
            .collect();
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, pairs.join("; ").parse().unwrap());
        headers
    }

    #[test]
    fn session_roundtrip() {
        let key = Key::generate();
        let id = identity(now_ts() + 60);
        let headers = headers_from(session_jar(&key, false, &id));
        let read = read_session(&headers, &key, false).unwrap();
        assert_eq!(read, id);
    }

    #[test]
    fn expired_session_rejected() {
        let key = Key::generate();
        let id = identity(now_ts() - 1);
        let headers = headers_from(session_jar(&key, false, &id));
        assert!(read_session(&headers, &key, false).is_none());
    }

    #[test]
    fn wrong_key_rejected() {
        let key = Key::generate();
        let id = identity(now_ts() + 60);
        let headers = headers_from(session_jar(&key, false, &id));
        assert!(read_session(&headers, &Key::generate(), false).is_none());
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
        let headers = headers_from(pending_login_jar(&key, false, &fresh));
        assert!(read_pending_login(&headers, &key, false).is_some());

        let stale = PendingLogin {
            iat: now_ts() - PENDING_LOGIN_TTL_SECS - 1,
            ..fresh
        };
        let headers = headers_from(pending_login_jar(&key, false, &stale));
        assert!(read_pending_login(&headers, &key, false).is_none());
    }
}
