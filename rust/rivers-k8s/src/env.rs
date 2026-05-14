use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, SecretKeySelector};
use rivers_core::storage::surrealdb_backend::{
    DEFAULT_DATABASE, DEFAULT_NAMESPACE, SurrealConnectConfig,
};

use crate::defaults;

const K8S_NAMESPACE_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// Env var carrying the `metadata.name` of the owning `CodeLocation` CR.
/// Stamped onto run pods by the operator (see `pod_builder.rs`) and onto
/// CodeLocation daemon pods by the CodeLocation reconciler.
pub const ENV_CODE_LOCATION_NAME: &str = "RIVERS_CODE_LOCATION_NAME";

/// SurrealDB connection envs stamped on every rivers pod.
pub const ENV_SURREAL_ENDPOINT: &str = "RIVERS_SURREAL_ENDPOINT";
pub const ENV_SURREAL_NAMESPACE: &str = "RIVERS_SURREAL_NAMESPACE";
pub const ENV_SURREAL_DATABASE: &str = "RIVERS_SURREAL_DATABASE";
pub const ENV_SURREAL_USERNAME: &str = "RIVERS_SURREAL_USERNAME";
pub const ENV_SURREAL_PASSWORD: &str = "RIVERS_SURREAL_PASSWORD";
pub const ENV_SURREAL_AUTH_SECRET_NAME: &str = "RIVERS_SURREAL_AUTH_SECRET_NAME";
pub const ENV_SURREAL_AUTH_USERNAME_KEY: &str = "RIVERS_SURREAL_AUTH_USERNAME_KEY";
pub const ENV_SURREAL_AUTH_PASSWORD_KEY: &str = "RIVERS_SURREAL_AUTH_PASSWORD_KEY";

/// Read `name` from the process environment, treating empty strings as
/// unset (so a `valueFrom.secretKeyRef` that resolves to "" doesn't shadow
/// the default). Returns `None` when missing.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

/// Resolve the active namespace. Tries `RIVERS_NAMESPACE`, then the
/// downward-API mount at `/var/run/secrets/...`, then falls back to the
/// project default. Lets the same binary work in-cluster, in tests, and on
/// a developer's laptop without conditional plumbing.
pub fn detect_namespace() -> String {
    std::env::var("RIVERS_NAMESPACE")
        .or_else(|_| std::fs::read_to_string(K8S_NAMESPACE_PATH).map(|s| s.trim().to_string()))
        .unwrap_or_else(|_| defaults::NAMESPACE.to_string())
}

/// Resolved image digest, injected by the operator. Returns `None` outside
/// the operator's daemon pods.
pub fn detect_code_location_image() -> Option<String> {
    std::env::var("RIVERS_CODE_LOCATION_IMAGE").ok()
}

/// Name of the `CodeLocation` CR this daemon represents. Stamped into every
/// `Run` CR so the operator-hosted admission webhook can resolve image +
/// module by digest.
pub fn detect_code_location_name() -> Option<String> {
    std::env::var(ENV_CODE_LOCATION_NAME).ok()
}

/// Stable identity (UUID v4) of the code location, used as the storage key
/// for every per-CL row. The operator's CodeLocation reconciler injects
/// `RIVERS_CODE_LOCATION_ID` from `CodeLocation.spec.identity` (a fresh
/// UUID stamped by the mutating admission webhook). Falls back to
/// `RIVERS_CODE_LOCATION_NAME` for ad-hoc local runs that don't go through
/// the operator (single-CL dev).
pub fn detect_code_location_id() -> Option<String> {
    std::env::var("RIVERS_CODE_LOCATION_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(detect_code_location_name)
}

/// `RIVERS_DEPLOYMENT` value the rivers CLI sets from every K8s entry point
/// (`serve`, `execute`, `execute-step`). Mirrors `python/rivers/cli.py`.
pub const DEPLOYMENT_CLOUD: &str = "cloud";

/// True iff `RIVERS_DEPLOYMENT == DEPLOYMENT_CLOUD`. Canonical signal for
/// "we're running under the operator and an explicit code-location identity
/// is required."
pub fn in_cloud_deployment() -> bool {
    std::env::var("RIVERS_DEPLOYMENT").ok().as_deref() == Some(DEPLOYMENT_CLOUD)
}

/// Same as [`detect_code_location_id`] but with a dev-only fallback to
/// [`rivers_core::storage::DEFAULT_CODE_LOCATION_ID`]. Use at the boundary
/// where a `String` is required (e.g. when constructing `RunRecord`).
///
/// Panics in cloud mode when the env is missing — see the assert message.
pub fn current_code_location_id() -> String {
    if let Some(id) = detect_code_location_id() {
        return id;
    }
    assert!(
        !in_cloud_deployment(),
        "RIVERS_CODE_LOCATION_ID is required when RIVERS_DEPLOYMENT=cloud \
         but is unset. The rivers operator must inject it from \
         CodeLocation.spec.identity; falling back to DEFAULT_CODE_LOCATION_ID \
         would write events under the wrong scope."
    );
    rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string()
}

pub fn detect_module() -> String {
    std::env::var("RIVERS_MODULE").unwrap_or_else(|_| defaults::MODULE.to_string())
}

pub fn detect_surreal_endpoint() -> String {
    std::env::var(ENV_SURREAL_ENDPOINT).unwrap_or_else(|_| defaults::SURREAL_ENDPOINT.to_string())
}

/// Build a [`SurrealConnectConfig`] from the standard rivers env vars.
/// Pods get `_USERNAME` / `_PASSWORD` from `valueFrom.secretKeyRef`, so by
/// the time we read them here they're plain strings. Both must resolve to
/// non-empty values for credentials to attach; otherwise the connection
/// is unauthenticated.
pub fn detect_surreal_connect_config() -> SurrealConnectConfig {
    let mut cfg = SurrealConnectConfig {
        endpoint: env_nonempty(ENV_SURREAL_ENDPOINT)
            .unwrap_or_else(|| defaults::SURREAL_ENDPOINT.to_string()),
        namespace: env_nonempty(ENV_SURREAL_NAMESPACE)
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
        database: env_nonempty(ENV_SURREAL_DATABASE)
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string()),
        credentials: None,
    };
    if let (Some(u), Some(p)) = (
        env_nonempty(ENV_SURREAL_USERNAME),
        env_nonempty(ENV_SURREAL_PASSWORD),
    ) {
        cfg = cfg.with_credentials(u, p);
    }
    cfg
}

/// Coordinates of the K8s Secret holding rivers' SurrealDB credentials.
/// Used by pods that re-emit `valueFrom.secretKeyRef` on child pods (e.g.
/// the run pod launching step pods) without round-tripping the password
/// value through process memory. Unset (empty fields) means no auth env
/// is emitted on child pods.
#[derive(Debug, Clone, Default)]
pub struct SurrealAuthSecretRef {
    pub secret_name: String,
    pub username_key: String,
    pub password_key: String,
}

impl SurrealAuthSecretRef {
    pub fn from_env() -> Self {
        Self {
            secret_name: std::env::var(ENV_SURREAL_AUTH_SECRET_NAME).unwrap_or_default(),
            username_key: std::env::var(ENV_SURREAL_AUTH_USERNAME_KEY).unwrap_or_default(),
            password_key: std::env::var(ENV_SURREAL_AUTH_PASSWORD_KEY).unwrap_or_default(),
        }
    }

    /// True when all three coordinates are populated, which is the only state
    /// where it's safe to emit `valueFrom.secretKeyRef` env vars onto pods.
    pub fn is_set(&self) -> bool {
        !self.secret_name.is_empty()
            && !self.username_key.is_empty()
            && !self.password_key.is_empty()
    }
}

/// SurrealDB connection bundle stamped on rivers pods. The endpoint can be
/// overridden per-Run via [`with_endpoint`](Self::with_endpoint) while
/// keeping the operator-level scope and auth-secret coordinates.
#[derive(Debug, Clone)]
pub struct SurrealPodConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub auth_secret: SurrealAuthSecretRef,
}

impl SurrealPodConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: env_nonempty(ENV_SURREAL_ENDPOINT)
                .unwrap_or_else(|| defaults::SURREAL_ENDPOINT.to_string()),
            namespace: env_nonempty(ENV_SURREAL_NAMESPACE).unwrap_or_else(|| {
                rivers_core::storage::surrealdb_backend::DEFAULT_NAMESPACE.to_string()
            }),
            database: env_nonempty(ENV_SURREAL_DATABASE).unwrap_or_else(|| {
                rivers_core::storage::surrealdb_backend::DEFAULT_DATABASE.to_string()
            }),
            auth_secret: SurrealAuthSecretRef::from_env(),
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

impl Default for SurrealPodConfig {
    fn default() -> Self {
        Self {
            endpoint: defaults::SURREAL_ENDPOINT.to_string(),
            namespace: rivers_core::storage::surrealdb_backend::DEFAULT_NAMESPACE.to_string(),
            database: rivers_core::storage::surrealdb_backend::DEFAULT_DATABASE.to_string(),
            auth_secret: SurrealAuthSecretRef::default(),
        }
    }
}

/// Build the standard SurrealDB env-var block stamped on rivers pods
/// (operator, UI, CodeLocation daemon, run, step). Always emits the
/// endpoint, namespace, and database (plain values). When the bundle's
/// `auth_secret` is set, also emits:
///
/// - `RIVERS_SURREAL_USERNAME` / `RIVERS_SURREAL_PASSWORD` via
///   `valueFrom.secretKeyRef` so the secret material never lands in pod specs
/// - `RIVERS_SURREAL_AUTH_SECRET_NAME` / `_USERNAME_KEY` / `_PASSWORD_KEY` as
///   plain values so the run pod can re-emit the same secretKeyRef on step
///   pods without holding the password in memory
pub fn build_surreal_pod_env(cfg: &SurrealPodConfig) -> Vec<EnvVar> {
    let mut env = vec![
        EnvVar {
            name: ENV_SURREAL_ENDPOINT.to_string(),
            value: Some(cfg.endpoint.clone()),
            ..Default::default()
        },
        EnvVar {
            name: ENV_SURREAL_NAMESPACE.to_string(),
            value: Some(cfg.namespace.clone()),
            ..Default::default()
        },
        EnvVar {
            name: ENV_SURREAL_DATABASE.to_string(),
            value: Some(cfg.database.clone()),
            ..Default::default()
        },
    ];
    if cfg.auth_secret.is_set() {
        env.push(EnvVar {
            name: ENV_SURREAL_USERNAME.to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: cfg.auth_secret.secret_name.clone(),
                    key: cfg.auth_secret.username_key.clone(),
                    optional: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        env.push(EnvVar {
            name: ENV_SURREAL_PASSWORD.to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: cfg.auth_secret.secret_name.clone(),
                    key: cfg.auth_secret.password_key.clone(),
                    optional: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        env.push(EnvVar {
            name: ENV_SURREAL_AUTH_SECRET_NAME.to_string(),
            value: Some(cfg.auth_secret.secret_name.clone()),
            ..Default::default()
        });
        env.push(EnvVar {
            name: ENV_SURREAL_AUTH_USERNAME_KEY.to_string(),
            value: Some(cfg.auth_secret.username_key.clone()),
            ..Default::default()
        });
        env.push(EnvVar {
            name: ENV_SURREAL_AUTH_PASSWORD_KEY.to_string(),
            value: Some(cfg.auth_secret.password_key.clone()),
            ..Default::default()
        });
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mutate process-global env vars for `RIVERS_CODE_LOCATION_ID`,
    /// `RIVERS_CODE_LOCATION_NAME`, and `RIVERS_DEPLOYMENT` for the duration
    /// of `f`, restoring whatever was there afterwards. Tests that poke env
    /// vars share the same process, so we serialize them through a mutex to
    /// avoid races.
    fn with_env<R>(
        cl_id: Option<&str>,
        cl_name: Option<&str>,
        deployment: Option<&str>,
        f: impl FnOnce() -> R,
    ) -> R {
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let prev_id = std::env::var("RIVERS_CODE_LOCATION_ID").ok();
        let prev_name = std::env::var("RIVERS_CODE_LOCATION_NAME").ok();
        let prev_dep = std::env::var("RIVERS_DEPLOYMENT").ok();
        let set = |k: &str, v: Option<&str>| match v {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        };
        set("RIVERS_CODE_LOCATION_ID", cl_id);
        set("RIVERS_CODE_LOCATION_NAME", cl_name);
        set("RIVERS_DEPLOYMENT", deployment);
        let out = f();
        set("RIVERS_CODE_LOCATION_ID", prev_id.as_deref());
        set("RIVERS_CODE_LOCATION_NAME", prev_name.as_deref());
        set("RIVERS_DEPLOYMENT", prev_dep.as_deref());
        out
    }

    #[test]
    fn current_code_location_id_uses_explicit_id_in_cloud() {
        let got = with_env(
            Some("11111111-1111-4111-8111-111111111111"),
            None,
            Some(DEPLOYMENT_CLOUD),
            current_code_location_id,
        );
        assert_eq!(got, "11111111-1111-4111-8111-111111111111");
    }

    #[test]
    fn current_code_location_id_falls_back_to_name_in_dev() {
        let got = with_env(
            None,
            Some("local-cl"),
            Some("dev"),
            current_code_location_id,
        );
        assert_eq!(got, "local-cl");
    }

    #[test]
    fn current_code_location_id_defaults_in_dev_when_unset() {
        let got = with_env(None, None, Some("dev"), current_code_location_id);
        assert_eq!(got, rivers_core::storage::DEFAULT_CODE_LOCATION_ID);
    }

    #[test]
    fn current_code_location_id_defaults_when_deployment_unset() {
        // Tests and ad-hoc scripts often run with no RIVERS_DEPLOYMENT set;
        // treat that as dev so the fallback chain still works.
        let got = with_env(None, None, None, current_code_location_id);
        assert_eq!(got, rivers_core::storage::DEFAULT_CODE_LOCATION_ID);
    }

    #[test]
    #[should_panic(expected = "RIVERS_CODE_LOCATION_ID is required")]
    fn current_code_location_id_panics_in_cloud_when_unset() {
        with_env(None, None, Some(DEPLOYMENT_CLOUD), current_code_location_id);
    }

    fn env_value_of(env: &[EnvVar], name: &str) -> Option<String> {
        env.iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value.clone())
    }

    fn env_secret_ref(env: &[EnvVar], name: &str) -> Option<(String, String)> {
        env.iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value_from.as_ref())
            .and_then(|src| src.secret_key_ref.as_ref())
            .map(|sk| (sk.name.clone(), sk.key.clone()))
    }

    #[test]
    fn build_surreal_pod_env_unauthenticated_omits_credentials() {
        let cfg = SurrealPodConfig {
            endpoint: "ws://surrealdb:8000".to_string(),
            namespace: "rivers".to_string(),
            database: "main".to_string(),
            auth_secret: SurrealAuthSecretRef::default(),
        };
        let env = build_surreal_pod_env(&cfg);
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_ENDPOINT).as_deref(),
            Some("ws://surrealdb:8000")
        );
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_NAMESPACE).as_deref(),
            Some("rivers")
        );
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_DATABASE).as_deref(),
            Some("main")
        );
        assert!(
            env.iter().all(|e| e.name != ENV_SURREAL_USERNAME),
            "username env should be absent without auth"
        );
        assert!(
            env.iter().all(|e| e.name != ENV_SURREAL_PASSWORD),
            "password env should be absent without auth"
        );
        assert!(
            env.iter().all(|e| e.name != ENV_SURREAL_AUTH_SECRET_NAME),
            "secret-name coordinate should be absent without auth"
        );
    }

    #[test]
    fn build_surreal_pod_env_with_auth_emits_secret_key_refs() {
        let cfg = SurrealPodConfig {
            endpoint: "wss://prod:443".to_string(),
            namespace: "rivers".to_string(),
            database: "main".to_string(),
            auth_secret: SurrealAuthSecretRef {
                secret_name: "rivers-surrealdb-auth".to_string(),
                username_key: "username".to_string(),
                password_key: "password".to_string(),
            },
        };
        let env = build_surreal_pod_env(&cfg);
        // Plain values still present.
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_ENDPOINT).as_deref(),
            Some("wss://prod:443")
        );
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_AUTH_SECRET_NAME).as_deref(),
            Some("rivers-surrealdb-auth")
        );
        assert_eq!(
            env_value_of(&env, ENV_SURREAL_AUTH_USERNAME_KEY).as_deref(),
            Some("username")
        );
        // Username/password are sourced via secretKeyRef — never as plain values.
        assert_eq!(
            env_secret_ref(&env, ENV_SURREAL_USERNAME),
            Some(("rivers-surrealdb-auth".to_string(), "username".to_string()))
        );
        assert_eq!(
            env_secret_ref(&env, ENV_SURREAL_PASSWORD),
            Some(("rivers-surrealdb-auth".to_string(), "password".to_string()))
        );
        let user_var = env.iter().find(|e| e.name == ENV_SURREAL_USERNAME).unwrap();
        assert!(
            user_var.value.is_none(),
            "username must be valueFrom-only, never inline"
        );
    }

    #[test]
    fn surreal_auth_secret_ref_is_set_requires_all_three_fields() {
        let mut s = SurrealAuthSecretRef::default();
        assert!(!s.is_set());
        s.secret_name = "x".into();
        assert!(!s.is_set());
        s.username_key = "u".into();
        assert!(!s.is_set());
        s.password_key = "p".into();
        assert!(s.is_set());
    }
}
