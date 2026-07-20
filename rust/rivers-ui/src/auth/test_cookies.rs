//! Browser-like cookie store for tests: applies Set-Cookie headers —
//! including `Max-Age=0` removals — the way a real user agent would, so
//! deletion semantics are actually exercised.

use axum::http::HeaderMap;
use axum::http::header::{COOKIE, SET_COOKIE};

#[derive(Default)]
pub(crate) struct CookieStore(std::collections::BTreeMap<String, String>);

impl CookieStore {
    pub(crate) fn absorb(&mut self, resp: &axum::response::Response) {
        for sc in resp.headers().get_all(SET_COOKIE) {
            let sc = sc.to_str().unwrap();
            let (pair, attrs) = sc.split_once(';').unwrap_or((sc, ""));
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            if attrs.contains("Max-Age=0") {
                self.0.remove(name.trim());
            } else {
                self.0.insert(name.trim().to_string(), value.to_string());
            }
        }
    }

    pub(crate) fn header_value(&self) -> String {
        self.0
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub(crate) fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if !self.0.is_empty() {
            h.insert(COOKIE, self.header_value().parse().unwrap());
        }
        h
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }
}
