//! Resolves image-pull credentials for a `CodeLocation` into a
//! [`RegistryAuth`] the registry client can consume.
//!
//! We support the standard `kubernetes.io/dockerconfigjson` shape
//! and its older `kubernetes.io/dockercfg` sibling. We look up the auth entry
//! whose key best matches the registry hostname, falling back to anonymous if
//! no match is found.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use kube_client::api::Api;

use super::registry::RegistryAuth;

const DOCKERCONFIGJSON_KEY: &str = ".dockerconfigjson";
const DOCKERCFG_KEY: &str = ".dockercfg";

/// Pick creds for `registry` from a list of imagePullSecret names.
///
/// The first secret that yields a matching entry wins; on no match, returns
/// [`RegistryAuth::Anonymous`]. Soft-fails on read errors (logs + continues)
/// so a broken Secret doesn't wedge digest resolution entirely — public
/// images still work.
pub async fn resolve_auth(
    secrets_api: &Api<Secret>,
    secret_names: &[String],
    registry: &str,
) -> RegistryAuth {
    for name in secret_names {
        let secret = match secrets_api.get(name).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%name, %e, "failed to read imagePullSecret");
                continue;
            }
        };

        match extract_auth(&secret, registry) {
            Ok(Some(auth)) => return auth,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(%name, %e, "failed to parse imagePullSecret");
                continue;
            }
        }
    }
    RegistryAuth::Anonymous
}

fn extract_auth(secret: &Secret, registry: &str) -> Result<Option<RegistryAuth>> {
    let Some(data) = &secret.data else {
        return Ok(None);
    };

    if let Some(ByteString(bytes)) = data.get(DOCKERCONFIGJSON_KEY) {
        let parsed: DockerConfigJson =
            serde_json::from_slice(bytes).context("parsing .dockerconfigjson")?;
        return Ok(parsed.auths.and_then(|auths| pick_auth(&auths, registry)));
    }
    if let Some(ByteString(bytes)) = data.get(DOCKERCFG_KEY) {
        // Legacy format: top-level is the auths map directly.
        let auths: BTreeMap<String, DockerConfigEntry> =
            serde_json::from_slice(bytes).context("parsing .dockercfg")?;
        return Ok(pick_auth(&auths, registry));
    }
    Ok(None)
}

fn pick_auth(auths: &BTreeMap<String, DockerConfigEntry>, registry: &str) -> Option<RegistryAuth> {
    // Try exact host match first, then hosts embedded in https://... URLs,
    // then a trailing-slash variant. Docker config keys are notoriously
    // inconsistent across tooling.
    let candidates = [
        registry.to_string(),
        format!("https://{registry}"),
        format!("https://{registry}/v1/"),
        format!("https://{registry}/v2/"),
        format!("{registry}/"),
    ];
    for key in &candidates {
        if let Some(entry) = auths.get(key) {
            return entry.to_auth();
        }
    }
    // Fallback: any key whose host component matches.
    for (k, entry) in auths {
        if extract_host(k) == registry {
            return entry.to_auth();
        }
    }
    None
}

fn extract_host(key: &str) -> String {
    let stripped = key
        .strip_prefix("https://")
        .or_else(|| key.strip_prefix("http://"))
        .unwrap_or(key);
    stripped
        .split('/')
        .next()
        .unwrap_or(stripped)
        .trim_end_matches('/')
        .to_string()
}

#[derive(serde::Deserialize)]
struct DockerConfigJson {
    auths: Option<BTreeMap<String, DockerConfigEntry>>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DockerConfigEntry {
    #[serde(default)]
    auth: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    identity_token: Option<String>,
}

impl DockerConfigEntry {
    fn to_auth(&self) -> Option<RegistryAuth> {
        if let Some(token) = &self.identity_token
            && !token.is_empty()
        {
            return Some(RegistryAuth::Bearer {
                token: token.clone(),
            });
        }
        if let (Some(u), Some(p)) = (&self.username, &self.password)
            && !u.is_empty()
        {
            return Some(RegistryAuth::Basic {
                username: u.clone(),
                password: p.clone(),
            });
        }
        if let Some(encoded) = &self.auth {
            if encoded.is_empty() {
                return None;
            }
            use base64::Engine;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            let s = String::from_utf8(decoded).ok()?;
            let (u, p) = s.split_once(':')?;
            return Some(RegistryAuth::Basic {
                username: u.to_string(),
                password: p.to_string(),
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn json_secret(payload: serde_json::Value) -> Secret {
        let bytes = serde_json::to_vec(&payload).unwrap();
        let mut data = BTreeMap::new();
        data.insert(DOCKERCONFIGJSON_KEY.to_string(), ByteString(bytes));
        Secret {
            data: Some(data),
            ..Default::default()
        }
    }

    #[test]
    fn picks_basic_auth_from_dockerconfigjson() {
        let secret = json_secret(serde_json::json!({
            "auths": {
                "ghcr.io": { "username": "me", "password": "pw" }
            }
        }));
        let auth = extract_auth(&secret, "ghcr.io").unwrap().unwrap();
        assert!(matches!(
            auth,
            RegistryAuth::Basic { username, password }
                if username == "me" && password == "pw"
        ));
    }

    #[test]
    fn picks_auth_from_base64_auth_field() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("user:secret");
        let secret = json_secret(serde_json::json!({
            "auths": { "ghcr.io": { "auth": encoded } }
        }));
        let auth = extract_auth(&secret, "ghcr.io").unwrap().unwrap();
        assert!(matches!(
            auth,
            RegistryAuth::Basic { username, password }
                if username == "user" && password == "secret"
        ));
    }

    #[test]
    fn matches_url_style_keys() {
        let secret = json_secret(serde_json::json!({
            "auths": {
                "https://index.docker.io/v1/": { "username": "u", "password": "p" }
            }
        }));
        let auth = extract_auth(&secret, "index.docker.io").unwrap().unwrap();
        assert!(matches!(auth, RegistryAuth::Basic { .. }));
    }

    #[test]
    fn returns_none_when_no_match() {
        let secret = json_secret(serde_json::json!({
            "auths": { "ghcr.io": { "username": "u", "password": "p" } }
        }));
        assert!(
            extract_auth(&secret, "other.registry.io")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn empty_secret_returns_none() {
        let secret = Secret::default();
        assert!(extract_auth(&secret, "any").unwrap().is_none());
    }

    #[test]
    fn identity_token_wins_over_basic() {
        let secret = json_secret(serde_json::json!({
            "auths": {
                "ghcr.io": {
                    "username": "u",
                    "password": "p",
                    "identityToken": "abc-token"
                }
            }
        }));
        let auth = extract_auth(&secret, "ghcr.io").unwrap().unwrap();
        assert!(matches!(auth, RegistryAuth::Bearer { token } if token == "abc-token"));
    }

    #[test]
    fn extracts_host_from_url() {
        assert_eq!(extract_host("https://ghcr.io/v2/"), "ghcr.io");
        assert_eq!(
            extract_host("https://registry-1.docker.io"),
            "registry-1.docker.io"
        );
        assert_eq!(extract_host("localhost:5000"), "localhost:5000");
    }
}
