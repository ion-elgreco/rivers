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
    /// `<public_url>/auth/callback`. Never derived from the Host header.
    pub public_url: String,
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
        self.public_url.starts_with("https://")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForwardConfig {
    /// CIDRs whose socket peers may assert identity headers.
    pub trusted_proxies: Vec<String>,
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

impl AuthConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    pub fn from_map(map: &HashMap<String, String>) -> anyhow::Result<Self> {
        Self::from_lookup(&|k| map.get(k).cloned())
    }

    fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
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
                    get(key)
                        .filter(|v| !v.trim().is_empty())
                        .with_context(|| format!("RIVERS_AUTH_MODE=oidc requires {key}"))
                };
                let public_url = require("RIVERS_AUTH_PUBLIC_URL")?
                    .trim_end_matches('/')
                    .to_string();
                if !public_url.starts_with("http://") && !public_url.starts_with("https://") {
                    bail!("RIVERS_AUTH_PUBLIC_URL must be an http(s) URL, got {public_url:?}");
                }
                let cookie_secret = get("RIVERS_AUTH_COOKIE_SECRET")
                    .filter(|v| !v.trim().is_empty())
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
                let session_ttl_secs = get("RIVERS_AUTH_SESSION_TTL")
                    .filter(|v| !v.trim().is_empty())
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
                    issuer: require("RIVERS_AUTH_OIDC_ISSUER")?.trim_end_matches('/').to_string(),
                    client_id: require("RIVERS_AUTH_OIDC_CLIENT_ID")?,
                    client_secret: get("RIVERS_AUTH_OIDC_CLIENT_SECRET")
                        .filter(|v| !v.trim().is_empty()),
                    public_url,
                    scopes,
                    groups_claim: get("RIVERS_AUTH_OIDC_GROUPS_CLAIM")
                        .filter(|v| !v.trim().is_empty())
                        .unwrap_or_else(|| "groups".into()),
                    rp_logout: flag(get("RIVERS_AUTH_OIDC_RP_LOGOUT")),
                    cookie_secret,
                    session_ttl_secs,
                })
            }
            Some("forward") => {
                let trusted_proxies = csv(get("RIVERS_AUTH_FORWARD_TRUSTED_PROXIES"));
                if trusted_proxies.is_empty() {
                    bail!(
                        "RIVERS_AUTH_MODE=forward requires RIVERS_AUTH_FORWARD_TRUSTED_PROXIES \
                         (comma-separated CIDRs; 0.0.0.0/0 must be typed deliberately)"
                    );
                }
                let header = |key: &str, default: &str| {
                    get(key)
                        .filter(|v| !v.trim().is_empty())
                        .unwrap_or_else(|| default.to_string())
                };
                AuthMode::Forward(ForwardConfig {
                    trusted_proxies,
                    user_header: header("RIVERS_AUTH_FORWARD_USER_HEADER", "Remote-User"),
                    email_header: header("RIVERS_AUTH_FORWARD_EMAIL_HEADER", "Remote-Email"),
                    groups_header: header("RIVERS_AUTH_FORWARD_GROUPS_HEADER", "Remote-Groups"),
                    name_header: header("RIVERS_AUTH_FORWARD_NAME_HEADER", "Remote-Name"),
                    logout_url: get("RIVERS_AUTH_FORWARD_LOGOUT_URL")
                        .filter(|v| !v.trim().is_empty()),
                })
            }
            Some(other) => bail!(
                "RIVERS_AUTH_MODE must be none|oidc|forward, got {other:?}"
            ),
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
            ("RIVERS_AUTH_PUBLIC_URL".to_string(), "https://r.example.com".to_string()),
            ("RIVERS_AUTH_OIDC_ISSUER".to_string(), "https://idp.example.com".to_string()),
            ("RIVERS_AUTH_OIDC_CLIENT_ID".to_string(), "rivers".to_string()),
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
        https.insert("RIVERS_AUTH_PUBLIC_URL".into(), "https://r.example.com".into());
        assert!(oidc_cfg(&https).secure_cookies());
        let mut http = oidc_map(None);
        http.insert("RIVERS_AUTH_PUBLIC_URL".into(), "http://localhost:3000".into());
        assert!(!oidc_cfg(&http).secure_cookies());
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
        let cfg = AuthConfig::from_map(&oidc_map(Some("openid profile,email offline_access"))).unwrap();
        assert_eq!(scopes_of(cfg), vec!["openid", "profile", "email", "offline_access"]);
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
        assert!(AuthConfig::from_map(&m).is_err(), "absurd TTL must be rejected");
        // A sane TTL still parses.
        let mut ok = oidc_map(None);
        ok.insert("RIVERS_AUTH_SESSION_TTL".into(), "3600".to_string());
        assert!(AuthConfig::from_map(&ok).is_ok());
    }
}
