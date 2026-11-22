//! Which storage backend a location names, decided by its URL scheme.
//!
//! One setting selects the backend, so callers do not carry a separate "which
//! engine" flag alongside the address. Credentials and scope stay on the
//! per-backend config, because they differ between engines.

use std::str::FromStr;

use anyhow::{Result, bail};

use super::surrealdb_backend::SurrealConnectConfig;

/// `url` with its passwords replaced, for logs and error messages: the one
/// before the host, and the value of each `password` parameter. The text
/// need not be a valid URL: a password with a `?`, `/` or `@` in it, and
/// one in the `name=value` form of a connection string, are hidden too.
pub fn redact_password(url: &str) -> String {
    let url = redact_parameters(url);
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => (String::new(), url.as_str()),
    };
    // The last `@` ends the user and the password.
    let user_and_host = rest
        .rsplit_once('@')
        .and_then(|(userinfo, host)| Some((userinfo.split_once(':')?.0, host)));
    match user_and_host {
        Some((user, host)) => format!("{scheme}{user}:***@{host}"),
        None => format!("{scheme}{rest}"),
    }
}

/// `text` with the value of each parameter whose name ends with `password`
/// replaced. A value ends at `&`, at white space or at the end.
fn redact_parameters(text: &str) -> String {
    const NAME: &str = "password=";
    let lower = text.to_ascii_lowercase();
    let mut redacted = String::with_capacity(text.len());
    let mut done = 0;
    while let Some(found) = lower[done..].find(NAME) {
        let value = done + found + NAME.len();
        redacted.push_str(&text[done..value]);
        redacted.push_str("***");
        done = text[value..]
            .find(|c: char| c == '&' || c.is_whitespace())
            .map_or(text.len(), |end| value + end);
    }
    redacted.push_str(&text[done..]);
    redacted
}

/// A parsed storage location.
#[derive(Clone)]
pub enum StorageUrl {
    /// `surrealkv://<path>` — embedded SurrealDB, the local-dev default.
    SurrealEmbedded { path: String },
    /// `mem://` — in-memory SurrealDB. Tests only, no durability.
    SurrealMemory,
    /// `ws://`, `wss://`, `http://`, `https://` — a SurrealDB server.
    ///
    /// Carries the full config so the caller can attach credentials with
    /// [`SurrealConnectConfig::with_credentials`] without re-parsing.
    SurrealRemote(SurrealConnectConfig),
    /// `postgres://`, `postgresql://` — a PostgreSQL server.
    Postgres { url: String },
}

/// The location as a log may show it: a PostgreSQL URL holds its password.
impl std::fmt::Debug for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageUrl({})", self.redacted())
    }
}

impl StorageUrl {
    /// Name of the backend this location selects, for logs and errors.
    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::SurrealEmbedded { .. } | Self::SurrealMemory | Self::SurrealRemote(_) => {
                "SurrealDB"
            }
            Self::Postgres { .. } => "PostgreSQL",
        }
    }

    /// True if the location is a server rather than a file or memory.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::SurrealRemote(_) | Self::Postgres { .. })
    }

    /// The location as text, with any password replaced by `***`. Safe to log.
    pub fn redacted(&self) -> String {
        match self {
            Self::SurrealEmbedded { path } => format!("surrealkv://{path}"),
            Self::SurrealMemory => "mem://".to_string(),
            Self::SurrealRemote(cfg) => redact_password(&cfg.endpoint),
            Self::Postgres { url } => redact_password(url),
        }
    }
}

impl FromStr for StorageUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        // An error must not show a password.
        let shown = redact_password(s);
        let Some((scheme, rest)) = s.split_once("://") else {
            bail!(
                "storage url {shown:?} has no scheme; expected one of \
                 surrealkv://, mem://, ws://, wss://, http://, https://, postgres://, postgresql://"
            );
        };
        match scheme.to_ascii_lowercase().as_str() {
            "surrealkv" => {
                if rest.is_empty() {
                    bail!("storage url {shown:?} names no path");
                }
                Ok(Self::SurrealEmbedded {
                    path: rest.to_string(),
                })
            }
            // `mem://` carries no address, so anything after it is a mistake
            // worth reporting rather than dropping.
            "mem" | "memory" => {
                if !rest.is_empty() {
                    bail!("storage url {shown:?} is in-memory and takes no address");
                }
                Ok(Self::SurrealMemory)
            }
            "ws" | "wss" | "http" | "https" => {
                if rest.is_empty() {
                    bail!("storage url {shown:?} names no host");
                }
                Ok(Self::SurrealRemote(SurrealConnectConfig::unauthenticated(
                    s,
                )))
            }
            "postgres" | "postgresql" => {
                if rest.is_empty() {
                    bail!("storage url {shown:?} names no host");
                }
                Ok(Self::Postgres { url: s.to_string() })
            }
            other => bail!(
                "storage url {shown:?} has unknown scheme {other:?}; expected one of \
                 surrealkv, mem, ws, wss, http, https, postgres, postgresql"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_url_keeps_the_whole_path() {
        let url: StorageUrl = "surrealkv:///var/lib/rivers/storage".parse().unwrap();
        match url {
            StorageUrl::SurrealEmbedded { path } => assert_eq!(path, "/var/lib/rivers/storage"),
            other => panic!("expected embedded, got {other:?}"),
        }
    }

    #[test]
    fn relative_embedded_path_survives() {
        let url: StorageUrl = "surrealkv://.rivers/storage".parse().unwrap();
        match url {
            StorageUrl::SurrealEmbedded { path } => assert_eq!(path, ".rivers/storage"),
            other => panic!("expected embedded, got {other:?}"),
        }
    }

    #[test]
    fn memory_url_parses_and_rejects_an_address() {
        assert!(matches!(
            "mem://".parse::<StorageUrl>().unwrap(),
            StorageUrl::SurrealMemory
        ));
        let err = "mem://localhost".parse::<StorageUrl>().unwrap_err();
        assert!(
            err.to_string().contains("takes no address"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn surreal_remote_keeps_the_full_endpoint_and_default_scope() {
        let url: StorageUrl = "ws://surrealdb:8000".parse().unwrap();
        match url {
            StorageUrl::SurrealRemote(cfg) => {
                assert_eq!(cfg.endpoint, "ws://surrealdb:8000");
                assert!(
                    cfg.credentials.is_none(),
                    "parsing must not invent credentials"
                );
            }
            other => panic!("expected remote surreal, got {other:?}"),
        }
    }

    #[test]
    fn postgres_url_is_passed_through_whole() {
        // The password and query string must survive: sqlx parses them, not us.
        let raw = "postgres://rivers:secret@db:5432/rivers?sslmode=require";
        let url: StorageUrl = raw.parse().unwrap();
        match url {
            StorageUrl::Postgres { url } => assert_eq!(url, raw),
            other => panic!("expected postgres, got {other:?}"),
        }
        assert!(matches!(
            "postgresql://db/rivers".parse::<StorageUrl>().unwrap(),
            StorageUrl::Postgres { .. }
        ));
    }

    #[test]
    fn scheme_matching_ignores_case() {
        assert!(matches!(
            "POSTGRES://db/rivers".parse::<StorageUrl>().unwrap(),
            StorageUrl::Postgres { .. }
        ));
    }

    #[test]
    fn a_bare_path_is_rejected_rather_than_guessed() {
        let err = "/var/lib/rivers".parse::<StorageUrl>().unwrap_err();
        assert!(err.to_string().contains("no scheme"), "unexpected: {err}");
    }

    #[test]
    fn the_debug_form_does_not_show_the_password() {
        let url: StorageUrl = "postgres://rivers:secret@db/rivers".parse().unwrap();
        let shown = format!("{url:?}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(shown.contains("db/rivers"), "{shown}");
    }

    #[test]
    fn an_error_does_not_show_the_password() {
        let err = "mysql://rivers:secret@db/rivers"
            .parse::<StorageUrl>()
            .unwrap_err();
        let msg = err.to_string();
        assert!(!msg.contains("secret"), "password in the error: {msg}");
        assert!(msg.contains("mysql"), "unexpected error: {msg}");
    }

    #[test]
    fn an_unknown_scheme_names_itself_in_the_error() {
        let err = "mysql://db/rivers".parse::<StorageUrl>().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mysql"), "error should name the scheme: {msg}");
    }

    #[test]
    fn backend_name_and_remoteness_follow_the_scheme() {
        let cases = [
            ("surrealkv://p", "SurrealDB", false),
            ("mem://", "SurrealDB", false),
            ("ws://h:8000", "SurrealDB", true),
            ("postgres://h/db", "PostgreSQL", true),
        ];
        for (raw, backend, remote) in cases {
            let url: StorageUrl = raw.parse().unwrap();
            assert_eq!(url.backend_name(), backend, "backend for {raw}");
            assert_eq!(url.is_remote(), remote, "remoteness for {raw}");
        }
    }

    #[test]
    fn redact_password_hides_only_the_password() {
        assert_eq!(
            redact_password("postgres://rivers:secret@db:5432/rivers"),
            "postgres://rivers:***@db:5432/rivers"
        );
        assert_eq!(
            redact_password("postgres://rivers@db/rivers"),
            "postgres://rivers@db/rivers"
        );
        assert_eq!(
            redact_password("postgres://db/rivers"),
            "postgres://db/rivers"
        );
        assert_eq!(
            redact_password("postgres://rivers@db/rivers?sslmode=require&password=secret"),
            "postgres://rivers@db/rivers?sslmode=require&password=***"
        );
        assert_eq!(
            redact_password("postgres://rivers:one@db/rivers?sslpassword=two&x=1"),
            "postgres://rivers:***@db/rivers?sslpassword=***&x=1"
        );
        // An `@` after the host hides more than the password, never less.
        assert_eq!(
            redact_password("postgres://rivers:one@db/rivers?x=a@b"),
            "postgres://rivers:***@b"
        );
        assert_eq!(
            redact_password("rivers:secret@db/rivers"),
            "rivers:***@db/rivers"
        );
        // A password that is not a valid part of a URL must not show either.
        assert_eq!(
            redact_password("postgres://rivers:p?ss@db:5432/rivers"),
            "postgres://rivers:***@db:5432/rivers"
        );
        assert_eq!(
            redact_password("postgres://rivers:p/s@s@db/rivers"),
            "postgres://rivers:***@db/rivers"
        );
        assert_eq!(
            redact_password("postgres://rivers@db/rivers?sslpassword=x:y@z"),
            "postgres://rivers@db/rivers?sslpassword=***"
        );
        assert_eq!(
            redact_password("host=db user=rivers password=secret dbname=rivers"),
            "host=db user=rivers password=*** dbname=rivers"
        );
    }

    /// A URL with a password can reach the SurrealDB settings by mistake.
    #[test]
    fn the_label_of_a_surreal_server_does_not_show_a_password() {
        let kind = super::super::surrealdb_backend::SurrealBackendKind::Remote {
            endpoint: "postgres://rivers:secret@db/rivers".to_string(),
        };
        assert_eq!(
            kind.label(),
            "SurrealDB (Remote: postgres://rivers:***@db/rivers)"
        );
    }
}
