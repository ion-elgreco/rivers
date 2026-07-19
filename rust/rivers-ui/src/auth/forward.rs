//! Forward-auth (trusted-header SSO): parse proxy identity headers, but only
//! from socket peers on the trusted CIDR list.

use axum::http::HeaderMap;
use ipnet::IpNet;
use std::net::IpAddr;

use super::config::ForwardConfig;
use super::identity::Identity;

#[derive(Debug, Clone)]
pub struct ForwardRuntime {
    trusted: Vec<IpNet>,
    pub cfg: ForwardConfig,
}

impl ForwardRuntime {
    pub fn new(cfg: ForwardConfig) -> anyhow::Result<Self> {
        let trusted = cfg
            .trusted_proxies
            .iter()
            .map(|s| {
                s.parse::<IpNet>().or_else(|_| {
                    // Accept bare IPs as /32 (v4) or /128 (v6).
                    s.parse::<IpAddr>().map(IpNet::from).map_err(|_| {
                        anyhow::anyhow!("invalid trusted proxy CIDR {s:?}")
                    })
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self { trusted, cfg })
    }

    /// Decided on the direct socket peer — never X-Forwarded-For.
    pub fn peer_trusted(&self, peer: IpAddr) -> bool {
        // An IPv4 peer on a dual-stack listener surfaces as ::ffff:a.b.c.d.
        let candidates: [IpAddr; 2] = match peer {
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => [peer, IpAddr::V4(v4)],
                None => [peer, peer],
            },
            v4 => [v4, v4],
        };
        self.trusted
            .iter()
            .any(|net| candidates.iter().any(|ip| net.contains(ip)))
    }

    pub fn identity_from_headers(&self, headers: &HeaderMap) -> Option<Identity> {
        let get = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(String::from)
        };
        let subject = get(&self.cfg.user_header)?;
        let groups = get(&self.cfg.groups_header)
            .map(|g| {
                g.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Some(Identity {
            subject,
            email: get(&self.cfg.email_header),
            name: get(&self.cfg.name_header),
            groups,
            // The proxy re-asserts identity on every request; nothing to expire.
            expires_at: i64::MAX,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn cfg() -> ForwardConfig {
        ForwardConfig {
            trusted_proxies: vec!["10.42.0.0/16".into(), "127.0.0.1".into()],
            user_header: "Remote-User".into(),
            email_header: "Remote-Email".into(),
            groups_header: "Remote-Groups".into(),
            name_header: "Remote-Name".into(),
            logout_url: None,
        }
    }

    #[test]
    fn cidr_and_bare_ip_trust() {
        let rt = ForwardRuntime::new(cfg()).unwrap();
        assert!(rt.peer_trusted("10.42.3.7".parse().unwrap()));
        assert!(rt.peer_trusted("127.0.0.1".parse().unwrap()));
        assert!(!rt.peer_trusted("10.43.0.1".parse().unwrap()));
        assert!(!rt.peer_trusted("192.168.1.1".parse().unwrap()));
        // v4-mapped v6 peer matches its v4 CIDR.
        assert!(rt.peer_trusted("::ffff:10.42.3.7".parse().unwrap()));
    }

    #[test]
    fn invalid_cidr_is_an_error() {
        let mut c = cfg();
        c.trusted_proxies = vec!["not-a-cidr".into()];
        assert!(ForwardRuntime::new(c).is_err());
    }

    #[test]
    fn identity_requires_user_header() {
        let rt = ForwardRuntime::new(cfg()).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Remote-Email", HeaderValue::from_static("a@b.com"));
        assert!(rt.identity_from_headers(&headers).is_none());

        headers.insert("Remote-User", HeaderValue::from_static("jdoe"));
        headers.insert("Remote-Groups", HeaderValue::from_static("eng, admins ,"));
        let id = rt.identity_from_headers(&headers).unwrap();
        assert_eq!(id.subject, "jdoe");
        assert_eq!(id.email.as_deref(), Some("a@b.com"));
        assert_eq!(id.groups, vec!["eng".to_string(), "admins".to_string()]);
    }
}
