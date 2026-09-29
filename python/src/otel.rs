//! OTLP span exporter built from the `OTEL_EXPORTER_OTLP_*` env vars.
//!
//! The exporter crate reads the timeout itself. The endpoint and headers are
//! resolved here because the crate treats an empty traces-specific variable
//! as set; the TLS variables because the crate does not read them.

use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::Context;
use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use pyo3::prelude::*;
use rustls::pki_types::TrustAnchor;
use tonic::metadata::MetadataMap;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

use crate::runtime::rt;

static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

fn env_lookup(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

pub(crate) fn export_enabled() -> bool {
    otlp_var(&env_lookup, "ENDPOINT").is_some()
}

/// Batches spans for export; [`shutdown`] sends the ones still queued.
pub(crate) fn tracer() -> anyhow::Result<SdkTracer> {
    let _runtime_guard = rt().enter();
    let mut resource = Resource::builder();
    if !service_name_configured(&env_lookup) {
        resource = resource.with_service_name("rivers");
    }
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter()?)
        .with_resource(resource.build())
        .build();
    let tracer = provider.tracer("rivers");
    let _ = PROVIDER.set(provider);
    Ok(tracer)
}

/// Registered with `atexit`. Waits at most five seconds for the export.
#[pyfunction]
pub(crate) fn shutdown(py: Python<'_>) {
    let Some(provider) = PROVIDER.get() else {
        return;
    };
    if let Err(err) = py.detach(|| provider.shutdown()) {
        tracing::warn!(error = %err, "failed to export the remaining spans at exit");
    }
}

fn span_exporter() -> anyhow::Result<SpanExporter> {
    let mut builder = SpanExporter::builder().with_tonic();
    if let Some((_, endpoint)) = otlp_var(&env_lookup, "ENDPOINT") {
        builder = builder.with_endpoint(endpoint);
    }
    builder = builder.with_metadata(headers(&env_lookup));
    if let Some(tls) = tls_config(&env_lookup)? {
        builder = builder.with_tls_config(tls);
    }
    builder.build().context("building the OTLP span exporter")
}

/// `OTEL_SERVICE_NAME` or a `service.name` entry in `OTEL_RESOURCE_ATTRIBUTES`.
fn service_name_configured(get: Lookup) -> bool {
    get("OTEL_SERVICE_NAME").is_some_and(|name| !name.is_empty())
        || get("OTEL_RESOURCE_ATTRIBUTES").is_some_and(|attrs| {
            attrs.split_terminator(',').any(|entry| {
                entry
                    .split_once('=')
                    .is_some_and(|(key, _)| key.trim() == "service.name")
            })
        })
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

/// Same format as the exporter crate: comma-separated `key=value` pairs with
/// URL-encoded values. Invalid entries are skipped, as the crate does. The
/// crate still adds the headers it reads itself; for a shared key its values
/// replace these, so no header is sent twice.
fn headers(get: Lookup) -> MetadataMap {
    let Some((_, value)) = otlp_var(get, "HEADERS") else {
        return MetadataMap::new();
    };
    let headers: HeaderMap = value
        .split_terminator(',')
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            let (key, value) = (key.trim(), value.trim());
            if key.is_empty() || value.is_empty() {
                return None;
            }
            let value = url_decode(value).unwrap_or_else(|| value.to_string());
            Some((
                HeaderName::from_str(key).ok()?,
                HeaderValue::from_str(&value).ok()?,
            ))
        })
        .collect();
    MetadataMap::from_headers(headers)
}

/// `None` for a malformed `%XX` escape or bytes that are not UTF-8.
fn url_decode(value: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut rest = value.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        if byte == b'%' {
            let hex = std::str::from_utf8(tail.get(..2)?).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            rest = &tail[2..];
        } else {
            bytes.push(byte);
            rest = tail;
        }
    }
    String::from_utf8(bytes).ok()
}

fn read_pem(get: Lookup, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let Some((var, path)) = otlp_var(get, name) else {
        return Ok(None);
    };
    std::fs::read(&path)
        .map(Some)
        .with_context(|| format!("reading {var} ({path})"))
}

/// `None` for plain `http://` without certificate variables. Certificate
/// variables with a non-`https://` endpoint are an error: tonic only uses TLS
/// for the `https` scheme and would send in cleartext.
fn tls_config(get: Lookup) -> anyhow::Result<Option<ClientTlsConfig>> {
    let endpoint = otlp_var(get, "ENDPOINT");
    let https = endpoint
        .as_ref()
        .is_some_and(|(_, endpoint)| endpoint.to_ascii_lowercase().starts_with("https://"));
    let cert_vars: Vec<String> = ["CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY"]
        .into_iter()
        .filter_map(|name| otlp_var(get, name).map(|(var, _)| var))
        .collect();
    if !https
        && !cert_vars.is_empty()
        && let Some((endpoint_var, endpoint)) = &endpoint
    {
        let verb = if cert_vars.len() == 1 { "is" } else { "are" };
        anyhow::bail!(
            "{} {verb} set but {endpoint_var} ({endpoint}) is not https://",
            cert_vars.join(", ")
        );
    }
    let ca = read_pem(get, "CERTIFICATE")?;
    let client_cert = read_pem(get, "CLIENT_CERTIFICATE")?;
    let client_key = read_pem(get, "CLIENT_KEY")?;
    if !https && ca.is_none() && client_cert.is_none() && client_key.is_none() {
        return Ok(None);
    }

    let config = match ca {
        Some(ca) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)),
        None => ClientTlsConfig::new()
            .with_webpki_roots()
            .trust_anchors(native_roots()),
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

/// Empty when the image has no CA bundle; the bundled Mozilla roots still apply.
fn native_roots() -> Vec<TrustAnchor<'static>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    roots.roots
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
    fn service_name_from_env() {
        let cases: &[(&[(&str, &str)], bool)] = &[
            (&[], false),
            (&[("OTEL_SERVICE_NAME", "")], false),
            (&[("OTEL_SERVICE_NAME", "analytics-cl")], true),
            (&[("OTEL_RESOURCE_ATTRIBUTES", "team=data")], false),
            (
                &[("OTEL_RESOURCE_ATTRIBUTES", "team=data, service.name = cl")],
                true,
            ),
            (
                &[("OTEL_RESOURCE_ATTRIBUTES", "service.namespace=data")],
                false,
            ),
        ];
        for (vars, expected) in cases {
            let vars = lookup(vars);
            let get = |k: &str| vars.get(k).cloned();
            assert_eq!(service_name_configured(&get), *expected, "{vars:?}");
        }
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
    fn headers_from_env() {
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_TRACES_HEADERS", ""),
            (
                "OTEL_EXPORTER_OTLP_HEADERS",
                " Authorization = Bearer%20a%2Cb ,x-bad=%zz, =v,k=,no-equals,x-tenant=data",
            ),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        let headers = headers(&get).into_headers();
        let entries: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.to_str().unwrap()))
            .collect();
        assert_eq!(
            entries,
            [
                ("authorization", "Bearer a,b"),
                ("x-bad", "%zz"),
                ("x-tenant", "data")
            ]
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
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317"),
            ("OTEL_EXPORTER_OTLP_CERTIFICATE", &ca),
            ("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", &cert),
            ("OTEL_EXPORTER_OTLP_CLIENT_KEY", &key),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        assert!(tls_config(&get).unwrap().is_some());
    }

    #[test]
    fn certificate_with_plain_http_endpoint_is_rejected() {
        let vars = lookup(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4317"),
            ("OTEL_EXPORTER_OTLP_CERTIFICATE", "/nonexistent/ca.pem"),
            (
                "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
                "/nonexistent/client.pem",
            ),
        ]);
        let get = |k: &str| vars.get(k).cloned();
        let err = format!("{:#}", tls_config(&get).unwrap_err());
        assert_eq!(
            err,
            "OTEL_EXPORTER_OTLP_CERTIFICATE, OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE \
             are set but OTEL_EXPORTER_OTLP_ENDPOINT (http://c:4317) is not https://"
        );
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
