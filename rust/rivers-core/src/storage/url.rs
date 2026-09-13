//! Which storage backend a location names, decided by its URL scheme.
//!
//! One setting selects the backend, so callers do not carry a separate "which
//! engine" flag alongside the address. Credentials and scope stay on the
//! per-backend config, because they differ between engines.

use std::str::FromStr;

use anyhow::{Result, bail};

use super::surrealdb_backend::SurrealConnectConfig;

/// A parsed storage location.
#[derive(Debug, Clone)]
pub enum StorageUrl {
    /// `rocksdb://<path>` — embedded SurrealDB, the local-dev default.
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
            Self::SurrealEmbedded { path } => format!("rocksdb://{path}"),
            Self::SurrealMemory => "mem://".to_string(),
            Self::SurrealRemote(cfg) => redact_password(&cfg.endpoint),
            Self::Postgres { url } => redact_password(url),
        }
    }
}

/// Strip the password from a connection url so it is safe to log.
pub fn redact_password(url: &str) -> String {
    match (url.find("://"), url.find('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end + 3 => {
            let userinfo = &url[scheme_end + 3..at];
            match userinfo.split_once(':') {
                Some((user, _)) => format!("{}{}:***{}", &url[..scheme_end + 3], user, &url[at..]),
                None => url.to_string(),
            }
        }
        _ => url.to_string(),
    }
}

impl FromStr for StorageUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let Some((scheme, rest)) = s.split_once("://") else {
            bail!(
                "storage url {s:?} has no scheme; expected one of \
                 rocksdb://, mem://, ws://, wss://, http://, https://, postgres://, postgresql://"
            );
        };
        match scheme.to_ascii_lowercase().as_str() {
            "rocksdb" => {
                if rest.is_empty() {
                    bail!("storage url {s:?} names no path");
                }
                Ok(Self::SurrealEmbedded {
                    path: rest.to_string(),
                })
            }
            // `mem://` carries no address, so anything after it is a mistake
            // worth reporting rather than dropping.
            "mem" | "memory" => {
                if !rest.is_empty() {
                    bail!("storage url {s:?} is in-memory and takes no address");
                }
                Ok(Self::SurrealMemory)
            }
            "ws" | "wss" | "http" | "https" => {
                if rest.is_empty() {
                    bail!("storage url {s:?} names no host");
                }
                Ok(Self::SurrealRemote(SurrealConnectConfig::unauthenticated(
                    s,
                )))
            }
            "postgres" | "postgresql" => {
                if rest.is_empty() {
                    bail!("storage url {s:?} names no host");
                }
                Ok(Self::Postgres { url: s.to_string() })
            }
            other => bail!(
                "storage url {s:?} has unknown scheme {other:?}; expected one of \
                 rocksdb, mem, ws, wss, http, https, postgres, postgresql"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rocksdb_url_keeps_the_whole_path() {
        let url: StorageUrl = "rocksdb:///var/lib/rivers/storage".parse().unwrap();
        match url {
            StorageUrl::SurrealEmbedded { path } => assert_eq!(path, "/var/lib/rivers/storage"),
            other => panic!("expected embedded, got {other:?}"),
        }
    }

    #[test]
    fn relative_rocksdb_path_survives() {
        let url: StorageUrl = "rocksdb://.rivers/storage".parse().unwrap();
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
    fn an_unknown_scheme_names_itself_in_the_error() {
        let err = "mysql://db/rivers".parse::<StorageUrl>().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mysql"), "error should name the scheme: {msg}");
    }

    #[test]
    fn backend_name_and_remoteness_follow_the_scheme() {
        let cases = [
            ("rocksdb://p", "SurrealDB", false),
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
}
