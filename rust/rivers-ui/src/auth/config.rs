//! Auth configuration resolved from `RIVERS_AUTH_*` env vars.
//!
//! Secrets are env-only — never CLI flags — so they can't leak via `ps` or
//! shell history.

use anyhow::{Context, bail};
use base64::Engine;
use std::collections::HashMap;

pub const DEFAULT_SESSION_TTL_SECS: i64 = 8 * 60 * 60;
/// Cap on `RIVERS_AUTH_SESSION_TTL` (10 years) — well beyond any real session
/// yet far from i64::MAX, so `now_ts() + ttl` can't overflow.
const MAX_SESSION_TTL_SECS: i64 = 10 * 365 * 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq)]
pub enum AuthMode {
    None,
    Oidc(OidcConfig),
    Forward(ForwardConfig),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    /// External base URL (`https://rivers.example.com`); the redirect URI is
    /// `<base_url()>/auth/callback`. Never derived from the Host header.
    pub public_url: url::Url,
    pub scopes: Vec<String>,
    pub groups_claim: String,
    pub rp_logout: bool,
    /// 32+ bytes, decoded from base64. `None` → ephemeral per-process key
    /// (sessions don't survive restarts or span replicas).
    pub cookie_secret: Option<Vec<u8>>,
    pub session_ttl_secs: i64,
}

impl OidcConfig {
    /// Cookies carry the `Secure` attribute (and `__Host-` prefix) exactly when
    /// the public URL is https — a pure function of `public_url`.
    pub fn secure_cookies(&self) -> bool {
        self.public_url.scheme() == "https"
    }

    /// External base without a trailing slash, for appending absolute routes
    /// (a `url::Url` always serializes its origin with a trailing `/`).
    pub fn base_url(&self) -> &str {
        self.public_url.as_str().trim_end_matches('/')
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForwardConfig {
    /// CIDRs whose socket peers may assert identity headers (a bare IP is a
    /// single-host `/32`/`/128`).
    pub trusted_proxies: Vec<ipnet::IpNet>,
    pub user_header: String,
    pub email_header: String,
    pub groups_header: String,
    pub name_header: String,
    pub logout_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Allowlists {
    pub domains: Vec<String>,
    pub groups: Vec<String>,
    pub users: Vec<String>,
}

impl Allowlists {
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty() && self.groups.is_empty() && self.users.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub allow: Allowlists,
}

/// Split a comma-separated string, trimming each item and dropping empties.
/// The one kernel behind CSV env vars and comma-joined OIDC/proxy group claims.
pub(super) fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn csv(v: Option<String>) -> Vec<String> {
    v.map(|s| split_csv(&s)).unwrap_or_default()
}

fn flag(v: Option<String>) -> bool {
    matches!(
        v.as_deref().map(str::trim),
        Some("1") | Some("true") | Some("True") | Some("TRUE") | Some("yes")
    )
}

/// A CIDR, or a bare IP taken as a single-host `/32` (v4) / `/128` (v6).
fn parse_cidr(s: &str) -> anyhow::Result<ipnet::IpNet> {
    s.parse::<ipnet::IpNet>().or_else(|_| {
        s.parse::<std::net::IpAddr>()
            .map(ipnet::IpNet::from)
            .map_err(|_| anyhow::anyhow!("invalid trusted proxy CIDR {s:?}"))
    })
}

impl AuthConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    pub fn from_map(map: &HashMap<String, String>) -> anyhow::Result<Self> {
        Self::from_lookup(&|k| map.get(k).cloned())
    }

    fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        // One source of truth for "present and not just whitespace"; the value
        // is returned untrimmed (callers trim where they parse).
        fn nonempty(get: &dyn Fn(&str) -> Option<String>, key: &str) -> Option<String> {
            get(key).filter(|v| !v.trim().is_empty())
        }

        let allow = Allowlists {
            domains: csv(get("RIVERS_AUTH_ALLOWED_DOMAINS"))
                .into_iter()
                .map(|d| d.to_ascii_lowercase())
                .collect(),
            groups: csv(get("RIVERS_AUTH_ALLOWED_GROUPS")),
            users: csv(get("RIVERS_AUTH_ALLOWED_USERS")),
        };

        let mode = match get("RIVERS_AUTH_MODE").as_deref().map(str::trim) {
            None | Some("") | Some("none") => AuthMode::None,
            Some("oidc") => {
                let require = |key: &str| {
                    nonempty(get, key)
                        .with_context(|| format!("RIVERS_AUTH_MODE=oidc requires {key}"))
                };
                let public_url = {
                    let raw = require("RIVERS_AUTH_PUBLIC_URL")?;
                    let url = url::Url::parse(raw.trim()).with_context(|| {
                        format!("RIVERS_AUTH_PUBLIC_URL must be a valid URL, got {raw:?}")
                    })?;
                    if !matches!(url.scheme(), "http" | "https") {
                        bail!("RIVERS_AUTH_PUBLIC_URL must be an http(s) URL, got {url}");
                    }
                    if url.host_str().map(str::is_empty).unwrap_or(true) {
                        bail!("RIVERS_AUTH_PUBLIC_URL must include a host, got {raw:?}");
                    }
                    url
                };
                let cookie_secret = nonempty(get, "RIVERS_AUTH_COOKIE_SECRET")
                    .map(|v| {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(v.trim())
                            .context("RIVERS_AUTH_COOKIE_SECRET is not valid base64")?;
                        if bytes.len() < 32 {
                            bail!(
                                "RIVERS_AUTH_COOKIE_SECRET must decode to at least 32 bytes, got {}",
                                bytes.len()
                            );
                        }
                        Ok(bytes)
                    })
                    .transpose()?;
                let session_ttl_secs = nonempty(get, "RIVERS_AUTH_SESSION_TTL")
                    .map(|v| {
                        v.trim()
                            .parse::<i64>()
                            .context("RIVERS_AUTH_SESSION_TTL must be an integer (seconds)")
                    })
                    .transpose()?
                    .unwrap_or(DEFAULT_SESSION_TTL_SECS);
                // Upper bound keeps now_ts() + session_ttl_secs from overflowing
                // i64 (which would wrap negative and instantly expire sessions).
                if session_ttl_secs <= 0 || session_ttl_secs > MAX_SESSION_TTL_SECS {
                    bail!(
                        "RIVERS_AUTH_SESSION_TTL must be between 1 and {MAX_SESSION_TTL_SECS} seconds"
                    );
                }
                // Accept comma- and/or space-separated (the OAuth wire form),
                // in one pass — the Helm default is space-separated.
                let scopes = {
                    let parsed: Vec<String> = get("RIVERS_AUTH_OIDC_SCOPES")
                        .unwrap_or_default()
                        // Whitespace is itself a delimiter, so tokens never
                        // carry surrounding whitespace — no trim needed.
                        .split(|c: char| c == ',' || c.is_whitespace())
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                    if parsed.is_empty() {
                        vec!["openid".into(), "profile".into(), "email".into()]
                    } else {
                        parsed
                    }
                };
                AuthMode::Oidc(OidcConfig {
                    // Exact-match identifier (unlike public_url): keep any
                    // trailing slash or discovery's issuer compare rejects Auth0.
                    issuer: require("RIVERS_AUTH_OIDC_ISSUER")?.trim().to_string(),
                    client_id: require("RIVERS_AUTH_OIDC_CLIENT_ID")?,
                    client_secret: nonempty(get, "RIVERS_AUTH_OIDC_CLIENT_SECRET"),
                    public_url,
                    scopes,
                    groups_claim: nonempty(get, "RIVERS_AUTH_OIDC_GROUPS_CLAIM")
                        .unwrap_or_else(|| "groups".into()),
                    rp_logout: flag(get("RIVERS_AUTH_OIDC_RP_LOGOUT")),
                    cookie_secret,
                    session_ttl_secs,
                })
            }
            Some("forward") => {
                let raw = csv(get("RIVERS_AUTH_FORWARD_TRUSTED_PROXIES"));
                if raw.is_empty() {
                    bail!(
                        "RIVERS_AUTH_MODE=forward requires RIVERS_AUTH_FORWARD_TRUSTED_PROXIES \
                         (comma-separated CIDRs; 0.0.0.0/0 must be typed deliberately)"
                    );
                }
                let trusted_proxies = raw
                    .iter()
                    .map(|s| parse_cidr(s))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let header = |key: &str, default: &str| {
                    nonempty(get, key).unwrap_or_else(|| default.to_string())
                };
                AuthMode::Forward(ForwardConfig {
                    trusted_proxies,
                    user_header: header("RIVERS_AUTH_FORWARD_USER_HEADER", "Remote-User"),
                    email_header: header("RIVERS_AUTH_FORWARD_EMAIL_HEADER", "Remote-Email"),
                    groups_header: header("RIVERS_AUTH_FORWARD_GROUPS_HEADER", "Remote-Groups"),
                    name_header: header("RIVERS_AUTH_FORWARD_NAME_HEADER", "Remote-Name"),
                    logout_url: nonempty(get, "RIVERS_AUTH_FORWARD_LOGOUT_URL"),
                })
            }
            Some(other) => bail!("RIVERS_AUTH_MODE must be none|oidc|forward, got {other:?}"),
        };

        Ok(Self { mode, allow })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oidc_map(scopes: Option<&str>) -> HashMap<String, String> {
        let mut m = HashMap::from([
            ("RIVERS_AUTH_MODE".to_string(), "oidc".to_string()),
            (
                "RIVERS_AUTH_PUBLIC_URL".to_string(),
                "https://r.example.com".to_string(),
            ),
            (
                "RIVERS_AUTH_OIDC_ISSUER".to_string(),
                "https://idp.example.com".to_string(),
            ),
            (
                "RIVERS_AUTH_OIDC_CLIENT_ID".to_string(),
                "rivers".to_string(),
            ),
        ]);
        if let Some(s) = scopes {
            m.insert("RIVERS_AUTH_OIDC_SCOPES".to_string(), s.to_string());
        }
        m
    }

    fn scopes_of(cfg: AuthConfig) -> Vec<String> {
        match cfg.mode {
            AuthMode::Oidc(o) => o.scopes,
            other => panic!("expected oidc, got {other:?}"),
        }
    }

    fn oidc_cfg(map: &HashMap<String, String>) -> OidcConfig {
        match AuthConfig::from_map(map).unwrap().mode {
            AuthMode::Oidc(o) => o,
            other => panic!("expected oidc, got {other:?}"),
        }
    }

    #[test]
    fn secure_cookies_follows_public_url_scheme() {
        let mut https = oidc_map(None);
        https.insert(
            "RIVERS_AUTH_PUBLIC_URL".into(),
            "https://r.example.com".into(),
        );
        assert!(oidc_cfg(&https).secure_cookies());
        let mut http = oidc_map(None);
        http.insert(
            "RIVERS_AUTH_PUBLIC_URL".into(),
            "http://localhost:3000".into(),
        );
        assert!(!oidc_cfg(&http).secure_cookies());
    }

    #[test]
    fn public_url_must_be_a_valid_absolute_http_url() {
        // Rejected at load, not deferred to a broken redirect URI at first login:
        // garbage, missing scheme, empty host, and non-http schemes.
        for bad in ["not a url", "https://", "ftp://host", "rivers.example.com"] {
            let mut m = oidc_map(None);
            m.insert("RIVERS_AUTH_PUBLIC_URL".into(), bad.to_string());
            assert!(
                AuthConfig::from_map(&m).is_err(),
                "{bad:?} must be rejected"
            );
        }
        // A subpath deployment parses and is preserved with no trailing slash.
        let mut sub = oidc_map(None);
        sub.insert(
            "RIVERS_AUTH_PUBLIC_URL".into(),
            "https://host/rivers".into(),
        );
        assert_eq!(oidc_cfg(&sub).base_url(), "https://host/rivers");
    }

    #[test]
    fn trusted_proxies_reject_bad_cidr_and_accept_bare_ip() {
        let forward = |proxies: &str| {
            AuthConfig::from_map(&HashMap::from([
                ("RIVERS_AUTH_MODE".to_string(), "forward".to_string()),
                (
                    "RIVERS_AUTH_FORWARD_TRUSTED_PROXIES".to_string(),
                    proxies.to_string(),
                ),
            ]))
        };
        assert!(
            forward("not-a-cidr").is_err(),
            "a bad CIDR is rejected at load"
        );
        // A bare IP is accepted as a single-host net.
        let AuthMode::Forward(cfg) = forward("10.42.7.5, 10.0.0.0/8").unwrap().mode else {
            panic!("expected forward");
        };
        assert_eq!(cfg.trusted_proxies.len(), 2);
        assert!(cfg.trusted_proxies[0].contains(&"10.42.7.5".parse::<std::net::IpAddr>().unwrap()));
    }

    #[test]
    fn oidc_issuer_preserves_a_trailing_slash() {
        // The OIDC issuer is an exact-match identifier; Auth0's canonical
        // issuer ends in '/', and discovery compares issuer strings verbatim.
        // Stripping the slash would abort startup for such IdPs.
        let mut m = oidc_map(None);
        m.insert(
            "RIVERS_AUTH_OIDC_ISSUER".into(),
            "https://tenant.auth0.com/".into(),
        );
        assert_eq!(oidc_cfg(&m).issuer, "https://tenant.auth0.com/");
    }

    #[test]
    fn split_csv_trims_and_drops_empties() {
        assert_eq!(split_csv("a, b ,,c"), vec!["a", "b", "c"]);
        assert_eq!(split_csv("solo"), vec!["solo"]);
        assert!(split_csv("").is_empty());
        assert!(split_csv("   ").is_empty());
        assert!(split_csv(",, ,").is_empty());
    }

    #[test]
    fn scopes_parse_space_separated() {
        // The Helm default is space-separated (the OAuth wire form). It must
        // become three scopes, not one 3-word token.
        let cfg = AuthConfig::from_map(&oidc_map(Some("openid profile email"))).unwrap();
        assert_eq!(scopes_of(cfg), vec!["openid", "profile", "email"]);
    }

    #[test]
    fn scopes_parse_comma_separated_and_mixed() {
        let cfg = AuthConfig::from_map(&oidc_map(Some("openid, profile , email"))).unwrap();
        assert_eq!(scopes_of(cfg), vec!["openid", "profile", "email"]);
        let cfg =
            AuthConfig::from_map(&oidc_map(Some("openid profile,email offline_access"))).unwrap();
        assert_eq!(
            scopes_of(cfg),
            vec!["openid", "profile", "email", "offline_access"]
        );
    }

    #[test]
    fn scopes_default_when_unset() {
        let cfg = AuthConfig::from_map(&oidc_map(None)).unwrap();
        assert_eq!(scopes_of(cfg), vec!["openid", "profile", "email"]);
    }

    #[test]
    fn session_ttl_rejects_overflowing_values() {
        // now_ts() + session_ttl_secs must not overflow i64 (would wrap negative
        // and instantly expire every session).
        let mut m = oidc_map(None);
        m.insert("RIVERS_AUTH_SESSION_TTL".into(), i64::MAX.to_string());
        assert!(
            AuthConfig::from_map(&m).is_err(),
            "absurd TTL must be rejected"
        );
        // A sane TTL still parses.
        let mut ok = oidc_map(None);
        ok.insert("RIVERS_AUTH_SESSION_TTL".into(), "3600".to_string());
        assert!(AuthConfig::from_map(&ok).is_ok());
    }

    #[test]
    fn session_ttl_rejects_zero_and_negative() {
        // TTL <= 0 → expires_at = now_ts() + 0 (or negative) → read_session
        // rejects every session → an endless login-redirect loop.
        for bad in ["0", "-1"] {
            let mut m = oidc_map(None);
            m.insert("RIVERS_AUTH_SESSION_TTL".into(), bad.to_string());
            assert!(
                AuthConfig::from_map(&m).is_err(),
                "TTL {bad} must be rejected"
            );
        }
    }

    #[test]
    fn cookie_secret_rejects_under_32_bytes() {
        // A secret under 32 bytes would panic Key::derive_from at startup, so
        // config must reject it; exactly 32 bytes is accepted.
        let mut short = oidc_map(None);
        short.insert(
            "RIVERS_AUTH_COOKIE_SECRET".into(),
            base64::engine::general_purpose::STANDARD.encode([0u8; 16]),
        );
        assert!(
            AuthConfig::from_map(&short).is_err(),
            "a <32-byte secret must be rejected"
        );
        let mut ok = oidc_map(None);
        ok.insert(
            "RIVERS_AUTH_COOKIE_SECRET".into(),
            base64::engine::general_purpose::STANDARD.encode([0u8; 32]),
        );
        assert!(
            AuthConfig::from_map(&ok).is_ok(),
            "a 32-byte secret is accepted"
        );
    }
}
