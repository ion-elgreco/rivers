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
pub const ENV_OTEL_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
pub const ENV_RIVERS_OTEL_ENDPOINT: &str = "RIVERS_OTEL_ENDPOINT";
pub const ENV_OTEL_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";
pub const ENV_OTEL_HEADERS_SECRET_NAME: &str = "RIVERS_OTEL_HEADERS_SECRET_NAME";
pub const ENV_OTEL_HEADERS_SECRET_KEY: &str = "RIVERS_OTEL_HEADERS_SECRET_KEY";

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
        value_env(ENV_SURREAL_ENDPOINT, &cfg.endpoint),
        value_env(ENV_SURREAL_NAMESPACE, &cfg.namespace),
        value_env(ENV_SURREAL_DATABASE, &cfg.database),
    ];
    let auth = &cfg.auth_secret;
    if auth.is_set() {
        env.extend([
            secret_env(ENV_SURREAL_USERNAME, &auth.secret_name, &auth.username_key),
            secret_env(ENV_SURREAL_PASSWORD, &auth.secret_name, &auth.password_key),
            value_env(ENV_SURREAL_AUTH_SECRET_NAME, &auth.secret_name),
            value_env(ENV_SURREAL_AUTH_USERNAME_KEY, &auth.username_key),
            value_env(ENV_SURREAL_AUTH_PASSWORD_KEY, &auth.password_key),
        ]);
    }
    env
}

/// Git-workspace envs on every pod that runs from a tree (code-location
/// pod, run executor pod, step Jobs), read back in-pod so the Runs and step
/// Jobs it launches get the same tree (RFC-044). `RIVERS_RUN_SOURCE`
/// carries the tree's `RunSource` as JSON — one typed channel instead of a
/// fan of `RIVERS_GIT_*` vars.
pub const ENV_RUN_SOURCE: &str = "RIVERS_RUN_SOURCE";
/// Shared-mode PVC claim name; unset/empty means emptyDir fallback.
pub const ENV_WORKSPACE_PVC: &str = "RIVERS_WORKSPACE_PVC";
pub const ENV_WORKSPACE_EMPTYDIR_LIMIT: &str = "RIVERS_WORKSPACE_EMPTYDIR_LIMIT";

/// Git provenance of the tree this pod runs. `None` in image mode (no
/// `RIVERS_RUN_SOURCE`).
pub fn detect_run_source() -> Option<crate::crd::run::RunSource> {
    let source_json = env_nonempty(ENV_RUN_SOURCE)?;
    match serde_json::from_str(&source_json) {
        Ok(s) => Some(s),
        Err(e) => {
            // A malformed value is an operator/pod version skew bug — fail
            // loudly rather than silently running image-mode.
            panic!("{ENV_RUN_SOURCE} is not valid RunSource JSON: {e}");
        }
    }
}

/// Rehydrate the git workspace coordinates inside a pod that runs from a
/// tree. `None` in image mode (no `RIVERS_RUN_SOURCE`).
pub fn detect_git_workspace() -> Option<(
    crate::crd::run::RunSource,
    crate::workspace::WorkspaceVolume,
)> {
    let source = detect_run_source()?;
    let volume = match env_nonempty(ENV_WORKSPACE_PVC) {
        Some(claim_name) => crate::workspace::WorkspaceVolume::SharedPvc { claim_name },
        None => crate::workspace::WorkspaceVolume::EmptyDir {
            size_limit: env_nonempty(ENV_WORKSPACE_EMPTYDIR_LIMIT)
                .map(k8s_openapi::apimachinery::pkg::api::resource::Quantity),
        },
    };
    Some((source, volume))
}

/// OTLP export settings for pods that run Python code. The headers Secret is
/// referenced by coordinate, like the SurrealDB credentials, so the token
/// never lands in a pod spec. An empty endpoint emits nothing.
///
/// The endpoint is read from `RIVERS_OTEL_ENDPOINT`, never from the standard
/// `OTEL_EXPORTER_OTLP_ENDPOINT`: a value injected into this pod for a local
/// sidecar must not be stamped on child pods where nothing listens.
#[derive(Debug, Clone, Default)]
pub struct OtelPodConfig {
    pub endpoint: String,
    pub headers_secret_name: String,
    pub headers_secret_key: String,
}

impl OtelPodConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: env_nonempty(ENV_RIVERS_OTEL_ENDPOINT).unwrap_or_default(),
            headers_secret_name: env_nonempty(ENV_OTEL_HEADERS_SECRET_NAME).unwrap_or_default(),
            headers_secret_key: env_nonempty(ENV_OTEL_HEADERS_SECRET_KEY).unwrap_or_default(),
        }
    }

    fn headers_secret_is_set(&self) -> bool {
        !self.headers_secret_name.is_empty() && !self.headers_secret_key.is_empty()
    }
}

pub fn build_otel_pod_env(cfg: &OtelPodConfig) -> Vec<EnvVar> {
    if cfg.endpoint.is_empty() {
        return Vec::new();
    }
    let mut env = vec![
        value_env(ENV_OTEL_ENDPOINT, &cfg.endpoint),
        value_env(ENV_RIVERS_OTEL_ENDPOINT, &cfg.endpoint),
    ];
    if cfg.headers_secret_is_set() {
        env.extend([
            secret_env(
                ENV_OTEL_HEADERS,
                &cfg.headers_secret_name,
                &cfg.headers_secret_key,
            ),
            value_env(ENV_OTEL_HEADERS_SECRET_NAME, &cfg.headers_secret_name),
            value_env(ENV_OTEL_HEADERS_SECRET_KEY, &cfg.headers_secret_key),
        ]);
    }
    env
}

fn value_env(name: &str, value: impl Into<String>) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.into()),
        ..Default::default()
    }
}

fn secret_env(name: &str, secret: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret.to_string(),
                key: key.to_string(),
                optional: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `(secret, key)` of the `secretKeyRef` behind env var `name`.
#[cfg(test)]
pub(crate) fn env_secret_ref(env: &[EnvVar], name: &str) -> Option<(String, String)> {
    env.iter()
        .find(|e| e.name == name)
        .and_then(|e| e.value_from.as_ref())
        .and_then(|src| src.secret_key_ref.as_ref())
        .map(|sk| (sk.name.clone(), sk.key.clone()))
}

/// Apply `overrides` on top of `base`: a same-named entry replaces the base
/// one in place, a new name is appended. Server-side apply rejects duplicate
/// names in a container's env list.
pub fn merge_env(
    mut base: Vec<EnvVar>,
    overrides: impl IntoIterator<Item = EnvVar>,
) -> Vec<EnvVar> {
    for var in overrides {
        match base.iter_mut().find(|e| e.name == var.name) {
            Some(slot) => *slot = var,
            None => base.push(var),
        }
    }
    base
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
        with_vars(
            &[
                ("RIVERS_CODE_LOCATION_ID", cl_id),
                ("RIVERS_CODE_LOCATION_NAME", cl_name),
                ("RIVERS_DEPLOYMENT", deployment),
            ],
            f,
        )
    }

    fn with_vars<R>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> R) -> R {
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let set = |k: &str, v: Option<&str>| match v {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        };
        let prev: Vec<_> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            set(k, *v);
        }
        let out = f();
        for (k, v) in &prev {
            set(k, v.as_deref());
        }
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

    #[test]
    fn merge_env_replaces_in_place_and_appends_new_names() {
        let var = |name: &str, value: &str| EnvVar {
            name: name.to_string(),
            value: Some(value.to_string()),
            ..Default::default()
        };
        let merged = merge_env(
            vec![var("A", "1"), var("B", "2"), var("C", "3")],
            vec![var("D", "4"), var("B", "user"), var("D", "last")],
        );
        let pairs: Vec<_> = merged
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap()))
            .collect();
        assert_eq!(
            pairs,
            vec![("A", "1"), ("B", "user"), ("C", "3"), ("D", "last")]
        );
    }

    #[test]
    fn code_location_pod_env_hands_its_tree_to_what_it_launches() {
        use crate::crd::code_location::{Dependencies, DependencyMode};
        use crate::crd::run::{GitCoordinates, RunSource};
        use crate::workspace::{WorkspaceSpec, WorkspaceVolume, builder_pod_pieces};
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        let source = RunSource {
            git: GitCoordinates {
                url: "https://forge.example/acme/pipelines.git".to_string(),
                commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string(),
                r#ref: Some("refs/heads/main".to_string()),
                path: Some("analytics".to_string()),
                secret_name: Some("git-creds".to_string()),
            },
            dependencies: Dependencies {
                mode: DependencyMode::Requirements,
                files: vec!["requirements/prod.txt".to_string()],
                extras: vec![],
                groups: vec![],
                timeout_seconds: Some(900),
            },
        };
        for volume in [
            WorkspaceVolume::SharedPvc {
                claim_name: "analytics-workspace".to_string(),
            },
            WorkspaceVolume::EmptyDir {
                size_limit: Some(Quantity("5Gi".to_string())),
            },
        ] {
            let pod_env = builder_pod_pieces(&WorkspaceSpec {
                key: "9f3c1ab8d2e4-1a2b3c4d".to_string(),
                volume: volume.clone(),
                runtime_image: "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff".to_string(),
                git_url: source.git.url.clone(),
                commit: source.git.commit.clone(),
                git_ref: source.git.r#ref.clone(),
                path: source.git.path.clone(),
                secret_name: source.git.secret_name.clone(),
                deps: source.dependencies.clone(),
                keep_config_map: Some("analytics-workspace-keep".to_string()),
                keep_revisions: Some(3),
                min_tree_age: Some("1h".to_string()),
                extra_env: vec![],
            })
            .main_env;
            let vars: Vec<(&str, Option<&str>)> = [
                ENV_RUN_SOURCE,
                ENV_WORKSPACE_PVC,
                ENV_WORKSPACE_EMPTYDIR_LIMIT,
            ]
            .into_iter()
            .map(|name| {
                let value = pod_env
                    .iter()
                    .find(|e| e.name == name)
                    .and_then(|e| e.value.as_deref());
                (name, value)
            })
            .collect();

            assert_eq!(
                with_vars(&vars, detect_git_workspace),
                Some((source.clone(), volume))
            );
            assert_eq!(with_vars(&vars, detect_run_source), Some(source.clone()));
        }
        assert_eq!(
            with_vars(&[(ENV_RUN_SOURCE, None)], detect_run_source),
            None
        );
    }

    #[test]
    fn run_pod_env_hands_its_workspace_volume_to_its_step_jobs() {
        use crate::crd::run::RunSource;
        use crate::executor::{K8sStepExecutorConfig, build_step_job};
        use crate::workspace::{
            WorkspaceVolume, consumer_pod_pieces, consumer_spec_from_run_source,
        };
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
        use serde_json::json;

        let source: RunSource = serde_json::from_value(json!({
            "git": {
                "url": "https://forge.example/acme/pipelines.git",
                "commit": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
            },
            "dependencies": { "mode": "auto" },
        }))
        .unwrap();
        let image = "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff";

        for (volume, step_volumes) in [
            (
                WorkspaceVolume::EmptyDir {
                    size_limit: Some(Quantity("10Gi".to_string())),
                },
                json!([{ "name": "workspace", "emptyDir": { "sizeLimit": "10Gi" } }]),
            ),
            (
                WorkspaceVolume::SharedPvc {
                    claim_name: "analytics-workspace".to_string(),
                },
                json!([{ "name": "workspace", "persistentVolumeClaim": {
                    "claimName": "analytics-workspace", "readOnly": true } }]),
            ),
        ] {
            // The run executor pod's workspace env, as the operator builds it.
            let run_pod = consumer_pod_pieces(&consumer_spec_from_run_source(
                &source,
                image,
                volume,
                vec![],
            ));
            let vars: Vec<(&str, Option<&str>)> = [
                ENV_RUN_SOURCE,
                ENV_WORKSPACE_PVC,
                ENV_WORKSPACE_EMPTYDIR_LIMIT,
            ]
            .into_iter()
            .map(|name| {
                let value = run_pod
                    .main_env
                    .iter()
                    .find(|e| e.name == name)
                    .and_then(|e| e.value.as_deref());
                (name, value)
            })
            .collect();

            // What the in-pod Kubernetes step executor does with that env.
            let workspace = with_vars(&vars, detect_git_workspace).map(|(source, volume)| {
                consumer_spec_from_run_source(&source, image, volume, vec![])
            });
            let job = build_step_job(
                &K8sStepExecutorConfig {
                    worker_image: image.to_string(),
                    namespace: "rivers".to_string(),
                    service_account: "rivers-step-worker".to_string(),
                    worker_cpu: "1".to_string(),
                    worker_memory: "2Gi".to_string(),
                    module: "analytics.pipeline".to_string(),
                    surreal_pod_cfg: SurrealPodConfig::default(),
                    otel_pod_cfg: OtelPodConfig::default(),
                    run_id: "run-1".to_string(),
                    run_cr_name: "run-1".to_string(),
                    run_cr_uid: "uid-1".to_string(),
                    code_location_id: "cl-1".to_string(),
                    extra_env: vec![],
                    partition_key: None,
                    workspace,
                },
                "parse_document",
            );

            let step_pod = job.spec.unwrap().template.spec.unwrap();
            assert_eq!(
                serde_json::to_value(&step_pod.volumes).unwrap(),
                step_volumes
            );
            assert_eq!(step_pod.volumes.unwrap(), run_pod.volumes);
        }
    }

    #[test]
    fn build_otel_pod_env_is_empty_without_endpoint() {
        let cfg = OtelPodConfig {
            headers_secret_name: "otel-headers".into(),
            headers_secret_key: "headers".into(),
            ..Default::default()
        };
        assert!(build_otel_pod_env(&cfg).is_empty());
    }

    #[test]
    fn build_otel_pod_env_endpoint_only_omits_headers() {
        let cfg = OtelPodConfig {
            endpoint: "https://otlp.example.com:4317".into(),
            ..Default::default()
        };
        let env = build_otel_pod_env(&cfg);
        assert_eq!(
            env_value_of(&env, ENV_OTEL_ENDPOINT).as_deref(),
            Some("https://otlp.example.com:4317")
        );
        assert_eq!(env.len(), 2, "no header env without a Secret: {env:?}");
    }

    #[test]
    fn otel_pod_config_from_env_ignores_ambient_otlp_endpoint() {
        let ambient_only = with_vars(
            &[
                (ENV_OTEL_ENDPOINT, Some("http://localhost:4317")),
                (ENV_RIVERS_OTEL_ENDPOINT, None),
            ],
            OtelPodConfig::from_env,
        );
        assert_eq!(ambient_only.endpoint, "");

        let both = with_vars(
            &[
                (ENV_OTEL_ENDPOINT, Some("http://localhost:4317")),
                (
                    ENV_RIVERS_OTEL_ENDPOINT,
                    Some("https://otlp.example.com:4317"),
                ),
            ],
            OtelPodConfig::from_env,
        );
        assert_eq!(both.endpoint, "https://otlp.example.com:4317");
    }

    #[test]
    fn build_otel_pod_env_emits_rivers_endpoint_coordinate() {
        let cfg = OtelPodConfig {
            endpoint: "https://otlp.example.com:4317".into(),
            ..Default::default()
        };
        let env = build_otel_pod_env(&cfg);
        assert_eq!(
            env_value_of(&env, ENV_RIVERS_OTEL_ENDPOINT).as_deref(),
            Some("https://otlp.example.com:4317")
        );
    }

    #[test]
    fn build_otel_pod_env_with_headers_emits_secret_key_ref_and_coordinates() {
        let cfg = OtelPodConfig {
            endpoint: "https://otlp.example.com:4317".into(),
            headers_secret_name: "otel-headers".into(),
            headers_secret_key: "headers".into(),
        };
        let env = build_otel_pod_env(&cfg);
        assert_eq!(
            env_secret_ref(&env, ENV_OTEL_HEADERS),
            Some(("otel-headers".to_string(), "headers".to_string()))
        );
        assert_eq!(
            env_value_of(&env, ENV_OTEL_HEADERS_SECRET_NAME).as_deref(),
            Some("otel-headers")
        );
        assert_eq!(
            env_value_of(&env, ENV_OTEL_HEADERS_SECRET_KEY).as_deref(),
            Some("headers")
        );
    }
}
