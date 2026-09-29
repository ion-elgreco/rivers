//! OTLP span exporter built from the `OTEL_EXPORTER_OTLP_*` env vars.
//!
//! The exporter crate reads the endpoint, headers, and timeout itself but
//! not the TLS variables, so those are read here.

use anyhow::Context;
use opentelemetry_otlp::{SpanExporter, WithTonicConfig};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

fn env_lookup(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

pub(crate) fn export_enabled() -> bool {
    otlp_var(&env_lookup, "ENDPOINT").is_some()
}

pub(crate) fn span_exporter() -> anyhow::Result<SpanExporter> {
    let mut builder = SpanExporter::builder().with_tonic();
    if let Some(tls) = tls_config(&env_lookup)? {
        builder = builder.with_tls_config(tls);
    }
    builder.build().context("building the OTLP span exporter")
}

/// Traces-specific variable first, then the generic one; empty counts as unset.
fn otlp_var(get: Lookup, name: &str) -> Option<(String, String)> {
    ["OTEL_EXPORTER_OTLP_TRACES_", "OTEL_EXPORTER_OTLP_"]
        .into_iter()
        .find_map(|prefix| {
            let var = format!("{prefix}{name}");
            get(&var)
                .filter(|value| !value.is_empty())
                .map(|value| (var, value))
        })
}

fn read_pem(get: Lookup, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let Some((var, path)) = otlp_var(get, name) else {
        return Ok(None);
    };
    std::fs::read(&path)
        .map(Some)
        .with_context(|| format!("reading {var} ({path})"))
}

/// `None` for plain `http://` without certificate variables.
fn tls_config(get: Lookup) -> anyhow::Result<Option<ClientTlsConfig>> {
    let https = otlp_var(get, "ENDPOINT")
        .is_some_and(|(_, endpoint)| endpoint.to_ascii_lowercase().starts_with("https://"));
    let ca = read_pem(get, "CERTIFICATE")?;
    let client_cert = read_pem(get, "CLIENT_CERTIFICATE")?;
    let client_key = read_pem(get, "CLIENT_KEY")?;
    if !https && ca.is_none() && client_cert.is_none() && client_key.is_none() {
        return Ok(None);
    }

    let config = match ca {
        Some(ca) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)),
        None => ClientTlsConfig::new().with_enabled_roots(),
    };
    let config = match (client_cert, client_key) {
        (Some(cert), Some(key)) => config.identity(Identity::from_pem(cert, key)),
        (None, None) => config,
        _ => anyhow::bail!(
            "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE and OTEL_EXPORTER_OTLP_CLIENT_KEY must be set together"
        ),
    };
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> HashMap<String, String> {
        vars.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn pem_file(name: &str) -> String {
        let path = std::env::temp_dir().join(format!("rivers-otel-{}-{name}", std::process::id()));
        std::fs::write(
            &path,
            b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn traces_variable_wins_over_generic() {
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces:4317"),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        assert_eq!(
            otlp_var(&get, "ENDPOINT"),
            Some((
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT".to_string(),
                "https://traces:4317".to_string()
            ))
        );
    }

    #[test]
    fn empty_traces_variable_falls_back_to_generic() {
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", ""),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        assert_eq!(
            otlp_var(&get, "ENDPOINT").map(|(_, v)| v).as_deref(),
            Some("http://generic:4317")
        );
    }

    #[test]
    fn plain_http_endpoint_needs_no_tls() {
        let vars = lookup(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")]);
        let get = |k: &str| vars.get(k).cloned();
        assert!(tls_config(&get).unwrap().is_none());
    }

    #[test]
    fn https_endpoint_gets_tls_with_system_roots() {
        let vars = lookup(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "HTTPS://collector:4317")]);
        let get = |k: &str| vars.get(k).cloned();
        assert!(tls_config(&get).unwrap().is_some());
    }

    #[test]
    fn certificate_files_are_read() {
        let ca = pem_file("ca.pem");
        let cert = pem_file("client.pem");
        let key = pem_file("client.key");
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317"),
            ("OTEL_EXPORTER_OTLP_CERTIFICATE", &ca),
            ("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", &cert),
            ("OTEL_EXPORTER_OTLP_CLIENT_KEY", &key),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        assert!(tls_config(&get).unwrap().is_some());
    }

    #[test]
    fn missing_certificate_file_names_the_variable() {
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317"),
            (
                "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
                "/nonexistent/ca.pem",
            ),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        let err = format!("{:#}", tls_config(&get).unwrap_err());
        assert!(
            err.contains("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE (/nonexistent/ca.pem)"),
            "{err}"
        );
    }

    #[test]
    fn client_certificate_without_key_is_rejected() {
        let cert = pem_file("lonely.pem");
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317"),
            ("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", &cert),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        let err = tls_config(&get).unwrap_err().to_string();
        assert!(err.contains("must be set together"), "{err}");
    }
}
