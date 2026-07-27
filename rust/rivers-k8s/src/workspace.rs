//! Workspace pod-spec pieces for git-sourced CodeLocations (RFC-044).
//!
//! This is the ONE builder for workspace mounts, init containers, volumes
//! and env — consumed by the operator (code-location Deployment, run
//! executor pod) and by the step-Job builder in [`crate::executor`], which
//! runs *inside the run pod*. That call-site is why this lives in
//! `rivers-k8s` rather than the operator crate: the dependency arrow points
//! operator → rivers-k8s, and a builder in the operator could never be
//! reached from the step-job path.
//!
//! Two volume modes, one shape: `subPath` mounts keep `/workspace/src` and
//! `/workspace/venv` byte-identical whether the tree lives on the shared
//! RWX PVC or in a per-pod `emptyDir`. What varies is the `volumes:` stanza
//! and which pods carry an init container:
//!
//! * **builder** (the code-location pod) — always runs
//!   `rivers-workspace-sync`; in shared mode it additionally mounts the PVC
//!   root at `/workspaces` so the prune step can see sibling trees, and
//!   receives the keep-set via `configMapKeyRef` (never inline — an inline
//!   value would live in the pod template and roll the Deployment on run
//!   lifecycle).
//! * **consumer** (run executor pod, step Jobs) — in shared mode mounts the
//!   finished tree read-only with NO init container, NO git credentials; in
//!   fallback mode it builds its own tree and looks like the builder minus
//!   the prune surface.

use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMapKeySelector, Container, EnvVar, EnvVarSource, SecretVolumeSource,
    SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::crd::code_location::{Dependencies, DependencyMode};

pub const WORKSPACE_VOLUME: &str = "workspace";
pub const GIT_CREDS_VOLUME: &str = "git-credentials";
pub const WORKSPACE_MOUNT: &str = "/workspace";
pub const WORKSPACES_ROOT_MOUNT: &str = "/workspaces";
pub const UV_CACHE_MOUNT: &str = "/uv-cache";
pub const UV_CACHE_SUBPATH: &str = "cache";
pub const GIT_CREDS_MOUNT: &str = "/etc/rivers/git";
/// Interpreter entry inside a materialized tree — pod `command` in git mode.
pub const VENV_RIVERS_BIN: &str = "/workspace/venv/bin/rivers";
pub const VENV_PATH: &str = "/workspace/venv";
pub const SYNC_COMMAND: &str = "rivers-workspace-sync";
pub const KEEP_CONFIG_MAP_KEY: &str = "keep";

/// `<commit[..12]>-<digest[..8]>`: the tree is keyed on *both* the code and
/// the interpreter it was built against — a chart upgrade that moves the
/// runtime image must not reuse the old venv.
pub fn workspace_key(commit: &str, runtime_image_ref: &str) -> String {
    let commit12: String = commit.chars().take(12).collect();
    let digest8: String = match runtime_image_ref.split_once("@sha256:") {
        Some((_, hex)) => hex.chars().take(8).collect(),
        // Undigested refs shouldn't reach here (the operator pins digests);
        // key on the sanitized ref tail so the shape stays stable anyway.
        None => {
            let tail: String = runtime_image_ref
                .chars()
                .rev()
                .filter(|c| c.is_ascii_alphanumeric())
                .take(8)
                .collect();
            format!("{:0>8}", tail.chars().rev().collect::<String>())
        }
    };
    format!("{commit12}-{digest8}")
}

/// Where the workspace volume lives.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkspaceVolume {
    /// One RWX PVC per CodeLocation, trees shared across its pods.
    SharedPvc { claim_name: String },
    /// Per-pod scratch — every pod builds its own tree.
    EmptyDir { size_limit: Option<Quantity> },
}

/// Everything needed to mount (and, for builders, materialize) a workspace.
#[derive(Clone, Debug)]
pub struct WorkspaceSpec {
    /// [`workspace_key`] — the subPath of this tree on the volume.
    pub key: String,
    pub volume: WorkspaceVolume,
    /// Digest-pinned runtime image; also the init container's image.
    pub runtime_image: String,
    pub git_url: String,
    /// Pinned 40-hex commit. Also stamped on the main container so a new
    /// commit changes the pod template and rolls the Deployment.
    pub commit: String,
    /// Resolved ref (`refs/heads/main`) — fetch fallback when the server
    /// refuses SHA-in-want.
    pub git_ref: Option<String>,
    pub path: Option<String>,
    /// Git credentials Secret (`username`/`password` or
    /// `identity`+`known_hosts`), mounted read-only at [`GIT_CREDS_MOUNT`].
    pub secret_name: Option<String>,
    pub deps: Dependencies,
    /// Builder only: keep-set ConfigMap consumed via `configMapKeyRef`.
    pub keep_config_map: Option<String>,
    pub keep_revisions: Option<u32>,
    pub min_tree_age: Option<String>,
    /// The CL's `spec.env`, applied to the init container too (RFC-036
    /// extended: `UV_INDEX_URL` & co. are needed at install time).
    pub extra_env: Vec<EnvVar>,
}

/// Pod-spec fragments to graft onto a Deployment / Pod / Job template.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct WorkspacePodPieces {
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub main_mounts: Vec<VolumeMount>,
    pub main_env: Vec<EnvVar>,
}

/// Pieces for the **code-location pod** — the one pod that materializes
/// trees (and, in shared mode, prunes siblings).
pub fn builder_pod_pieces(spec: &WorkspaceSpec) -> WorkspacePodPieces {
    WorkspacePodPieces {
        init_containers: vec![sync_init_container(spec, SyncRole::Builder)],
        volumes: volumes(spec, /* read_only_pvc */ false),
        main_mounts: vec![workspace_mount(spec, /* read_only */ false)],
        main_env: main_env(spec),
    }
}

/// The consumer-side [`WorkspaceSpec`] derived from a Run's stamped
/// provenance. Shared by the operator (executor pod) and the in-pod
/// step-Job builder so the two cannot disagree about the tree: same key
/// computation, same coordinates, no prune surface.
pub fn consumer_spec_from_run_source(
    source: &crate::crd::run::RunSource,
    runtime_image: &str,
    volume: WorkspaceVolume,
    extra_env: Vec<EnvVar>,
) -> WorkspaceSpec {
    WorkspaceSpec {
        key: workspace_key(&source.git.commit, runtime_image),
        volume,
        runtime_image: runtime_image.to_string(),
        git_url: source.git.url.clone(),
        commit: source.git.commit.clone(),
        git_ref: source.git.r#ref.clone(),
        path: source.git.path.clone(),
        secret_name: source.git.secret_name.clone(),
        deps: source.dependencies.clone(),
        keep_config_map: None,
        keep_revisions: None,
        min_tree_age: None,
        extra_env,
    }
}

/// Pieces for **run executor pods and step Jobs**. Shared mode: read-only
/// mount, no init container, no credentials — the admission chain
/// guarantees the tree exists (CL `Ready` ⟹ tree built). Fallback mode:
/// the pod builds its own tree, but never prunes.
pub fn consumer_pod_pieces(spec: &WorkspaceSpec) -> WorkspacePodPieces {
    match &spec.volume {
        WorkspaceVolume::SharedPvc { .. } => WorkspacePodPieces {
            init_containers: Vec::new(),
            volumes: volumes_without_creds(spec, /* read_only_pvc */ true),
            main_mounts: vec![workspace_mount(spec, /* read_only */ true)],
            main_env: main_env(spec),
        },
        WorkspaceVolume::EmptyDir { .. } => WorkspacePodPieces {
            init_containers: vec![sync_init_container(spec, SyncRole::Consumer)],
            volumes: volumes(spec, false),
            main_mounts: vec![workspace_mount(spec, false)],
            main_env: main_env(spec),
        },
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SyncRole {
    /// Mounts the volume root, receives the keep-set, prunes.
    Builder,
    /// Fallback-mode consumer: syncs its private tree, never prunes.
    Consumer,
}

fn sync_init_container(spec: &WorkspaceSpec, role: SyncRole) -> Container {
    let mut env = vec![
        env_var("RIVERS_GIT_URL", &spec.git_url),
        env_var("RIVERS_GIT_COMMIT", &spec.commit),
    ];
    if let Some(r) = &spec.git_ref {
        env.push(env_var("RIVERS_GIT_REF", r));
    }
    if let Some(p) = &spec.path {
        env.push(env_var("RIVERS_GIT_PATH", p));
    }
    env.push(env_var("RIVERS_DEPS_MODE", deps_mode_name(spec.deps.mode)));
    if !spec.deps.files.is_empty() {
        env.push(env_var("RIVERS_DEPS_FILES", &spec.deps.files.join(",")));
    }
    if !spec.deps.extras.is_empty() {
        env.push(env_var("RIVERS_DEPS_EXTRAS", &spec.deps.extras.join(",")));
    }
    if !spec.deps.groups.is_empty() {
        env.push(env_var("RIVERS_DEPS_GROUPS", &spec.deps.groups.join(",")));
    }
    if let Some(t) = spec.deps.timeout_seconds {
        env.push(env_var("RIVERS_DEPS_TIMEOUT_SECONDS", &t.to_string()));
    }
    env.push(env_var("UV_PROJECT_ENVIRONMENT", VENV_PATH));
    env.push(env_var("UV_CACHE_DIR", UV_CACHE_MOUNT));
    env.push(env_var("UV_LINK_MODE", "copy"));
    env.push(env_var("UV_COMPILE_BYTECODE", "1"));

    if role == SyncRole::Builder {
        if let Some(cm) = &spec.keep_config_map {
            env.push(EnvVar {
                name: "RIVERS_WORKSPACE_KEEP".to_string(),
                value_from: Some(EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: cm.clone(),
                        key: KEEP_CONFIG_MAP_KEY.to_string(),
                        // Missing ConfigMap degrades to an empty keep-set;
                        // the recency + age floors still guard, and the pod
                        // starts instead of wedging on config resolution.
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        if let Some(n) = spec.keep_revisions {
            env.push(env_var("RIVERS_WORKSPACE_KEEP_REVISIONS", &n.to_string()));
        }
        if let Some(age) = &spec.min_tree_age {
            env.push(env_var("RIVERS_WORKSPACE_MIN_AGE", age));
        }
    }
    env.extend(spec.extra_env.iter().cloned());

    let mut mounts = vec![
        workspace_mount(spec, false),
        VolumeMount {
            name: WORKSPACE_VOLUME.to_string(),
            mount_path: UV_CACHE_MOUNT.to_string(),
            sub_path: Some(UV_CACHE_SUBPATH.to_string()),
            ..Default::default()
        },
    ];
    // The prune step needs to see sibling trees — volume root, builder only,
    // and only where siblings exist (the shared PVC).
    if role == SyncRole::Builder && matches!(spec.volume, WorkspaceVolume::SharedPvc { .. }) {
        mounts.push(VolumeMount {
            name: WORKSPACE_VOLUME.to_string(),
            mount_path: WORKSPACES_ROOT_MOUNT.to_string(),
            ..Default::default()
        });
    }
    if spec.secret_name.is_some() {
        mounts.push(VolumeMount {
            name: GIT_CREDS_VOLUME.to_string(),
            mount_path: GIT_CREDS_MOUNT.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    Container {
        name: "workspace".to_string(),
        image: Some(spec.runtime_image.clone()),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: Some(vec![SYNC_COMMAND.to_string()]),
        // Default policy reads only /dev/termination-log; the fallback puts
        // uv's/git's stderr tail where the reconciler can lift it into
        // status.message.
        termination_message_policy: Some("FallbackToLogsOnError".to_string()),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            run_as_non_root: Some(true),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        env: Some(env),
        volume_mounts: Some(mounts),
        ..Default::default()
    }
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        ..Default::default()
    }
}

/// serde name of the mode (`auto` / `uvSync` / …) — the script speaks the
/// CRD's vocabulary.
fn deps_mode_name(mode: DependencyMode) -> &'static str {
    match mode {
        DependencyMode::Auto => "auto",
        DependencyMode::UvSync => "uvSync",
        DependencyMode::Requirements => "requirements",
        DependencyMode::None => "none",
    }
}

fn workspace_mount(spec: &WorkspaceSpec, read_only: bool) -> VolumeMount {
    VolumeMount {
        name: WORKSPACE_VOLUME.to_string(),
        mount_path: WORKSPACE_MOUNT.to_string(),
        sub_path: Some(spec.key.clone()),
        read_only: if read_only { Some(true) } else { None },
        ..Default::default()
    }
}

fn workspace_volume(spec: &WorkspaceSpec, read_only_pvc: bool) -> Volume {
    match &spec.volume {
        WorkspaceVolume::SharedPvc { claim_name } => Volume {
            name: WORKSPACE_VOLUME.to_string(),
            persistent_volume_claim: Some(
                k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                    claim_name: claim_name.clone(),
                    read_only: if read_only_pvc { Some(true) } else { None },
                },
            ),
            ..Default::default()
        },
        WorkspaceVolume::EmptyDir { size_limit } => Volume {
            name: WORKSPACE_VOLUME.to_string(),
            empty_dir: Some(k8s_openapi::api::core::v1::EmptyDirVolumeSource {
                size_limit: size_limit.clone(),
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

fn volumes(spec: &WorkspaceSpec, read_only_pvc: bool) -> Vec<Volume> {
    let mut vols = vec![workspace_volume(spec, read_only_pvc)];
    if let Some(secret) = &spec.secret_name {
        vols.push(Volume {
            name: GIT_CREDS_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(secret.clone()),
                default_mode: Some(0o400),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    vols
}

fn volumes_without_creds(spec: &WorkspaceSpec, read_only_pvc: bool) -> Vec<Volume> {
    vec![workspace_volume(spec, read_only_pvc)]
}

fn main_env(spec: &WorkspaceSpec) -> Vec<EnvVar> {
    vec![
        env_var("VIRTUAL_ENV", VENV_PATH),
        // In the pod template ⇒ a new commit rolls the Deployment.
        env_var("RIVERS_GIT_COMMIT", &spec.commit),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use serde_json::json;

    fn deps() -> Dependencies {
        Dependencies {
            mode: DependencyMode::Auto,
            files: vec![],
            extras: vec!["dev".to_string()],
            groups: vec![],
            timeout_seconds: Some(300),
        }
    }

    fn spec(volume: WorkspaceVolume) -> WorkspaceSpec {
        WorkspaceSpec {
            key: "9f3c1ab8d2e4-1a2b3c4d".to_string(),
            volume,
            runtime_image: "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff".to_string(),
            git_url: "https://forge.example/acme/pipelines.git".to_string(),
            commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string(),
            git_ref: Some("refs/heads/main".to_string()),
            path: Some("analytics".to_string()),
            secret_name: Some("git-creds".to_string()),
            deps: deps(),
            keep_config_map: Some("analytics-workspace-keep".to_string()),
            keep_revisions: Some(3),
            min_tree_age: Some("1h".to_string()),
            extra_env: vec![k8s_openapi::api::core::v1::EnvVar {
                name: "UV_INDEX_URL".to_string(),
                value: Some("https://pypi.internal/simple".to_string()),
                ..Default::default()
            }],
        }
    }

    fn shared() -> WorkspaceVolume {
        WorkspaceVolume::SharedPvc {
            claim_name: "analytics-workspace".to_string(),
        }
    }

    fn fallback() -> WorkspaceVolume {
        WorkspaceVolume::EmptyDir {
            size_limit: Some(Quantity("2Gi".to_string())),
        }
    }

    #[test]
    fn workspace_key_is_commit12_dash_digest8() {
        let key = workspace_key(
            "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
            "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffffeeee00001111222233334444555566667777",
        );
        assert_eq!(key, "9f3c1ab8d2e4-1a2b3c4d");
        // A different runtime digest must produce a different key — the venv
        // was built against a different interpreter.
        let other = workspace_key(
            "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
            "ghcr.io/acme/rivers-runtime@sha256:ffffffffaaaa000011112222333344445555666677778888",
        );
        assert_ne!(key, other);
    }

    #[test]
    fn workspace_key_survives_undigested_ref() {
        // Shouldn't occur (the operator pins digests), but the key must not
        // panic or collide with digested forms.
        let key = workspace_key("9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8", "runtime:latest");
        assert!(key.starts_with("9f3c1ab8d2e4-"));
        assert_eq!(key.len(), "9f3c1ab8d2e4-".len() + 8);
    }

    #[test]
    fn builder_shared_golden() {
        let pieces = builder_pod_pieces(&spec(shared()));
        assert_eq!(
            serde_json::to_value(&pieces.init_containers).unwrap(),
            json!([{
                "name": "workspace",
                "image": "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff",
                "imagePullPolicy": "IfNotPresent",
                "command": ["rivers-workspace-sync"],
                "terminationMessagePolicy": "FallbackToLogsOnError",
                "securityContext": {
                    "allowPrivilegeEscalation": false,
                    "capabilities": { "drop": ["ALL"] },
                    "runAsNonRoot": true,
                },
                "env": [
                    { "name": "RIVERS_GIT_URL", "value": "https://forge.example/acme/pipelines.git" },
                    { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                    { "name": "RIVERS_GIT_REF", "value": "refs/heads/main" },
                    { "name": "RIVERS_GIT_PATH", "value": "analytics" },
                    { "name": "RIVERS_DEPS_MODE", "value": "auto" },
                    { "name": "RIVERS_DEPS_EXTRAS", "value": "dev" },
                    { "name": "RIVERS_DEPS_TIMEOUT_SECONDS", "value": "300" },
                    { "name": "UV_PROJECT_ENVIRONMENT", "value": "/workspace/venv" },
                    { "name": "UV_CACHE_DIR", "value": "/uv-cache" },
                    { "name": "UV_LINK_MODE", "value": "copy" },
                    { "name": "UV_COMPILE_BYTECODE", "value": "1" },
                    // Keep-set via ConfigMap indirection — see module docs.
                    // optional: a missing ConfigMap degrades to an empty
                    // keep-set; the recency + age floors still guard.
                    { "name": "RIVERS_WORKSPACE_KEEP", "valueFrom": { "configMapKeyRef": {
                        "name": "analytics-workspace-keep", "key": "keep", "optional": true }}},
                    { "name": "RIVERS_WORKSPACE_KEEP_REVISIONS", "value": "3" },
                    { "name": "RIVERS_WORKSPACE_MIN_AGE", "value": "1h" },
                    { "name": "UV_INDEX_URL", "value": "https://pypi.internal/simple" },
                ],
                "volumeMounts": [
                    { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d" },
                    { "name": "workspace", "mountPath": "/uv-cache", "subPath": "cache" },
                    { "name": "workspace", "mountPath": "/workspaces" },
                    { "name": "git-credentials", "mountPath": "/etc/rivers/git", "readOnly": true },
                ],
            }])
        );
        assert_eq!(
            serde_json::to_value(&pieces.volumes).unwrap(),
            json!([
                { "name": "workspace", "persistentVolumeClaim": { "claimName": "analytics-workspace" } },
                { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 256 } },
            ])
        );
        assert_eq!(
            serde_json::to_value(&pieces.main_mounts).unwrap(),
            json!([
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d" },
            ])
        );
        assert_eq!(
            serde_json::to_value(&pieces.main_env).unwrap(),
            json!([
                { "name": "VIRTUAL_ENV", "value": "/workspace/venv" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
            ])
        );
    }

    #[test]
    fn builder_fallback_differs_only_where_documented() {
        let shared_pieces = builder_pod_pieces(&spec(shared()));
        let fb = builder_pod_pieces(&spec(fallback()));

        // emptyDir volumes stanza.
        assert_eq!(
            serde_json::to_value(&fb.volumes).unwrap(),
            json!([
                { "name": "workspace", "emptyDir": { "sizeLimit": "2Gi" } },
                { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 256 } },
            ])
        );
        // No /workspaces root mount (nothing to prune), otherwise identical.
        let fb_init = &fb.init_containers[0];
        let sh_init = &shared_pieces.init_containers[0];
        assert_eq!(fb_init.env, sh_init.env, "init env identical across modes");
        let fb_mounts = serde_json::to_value(fb_init.volume_mounts.as_ref().unwrap()).unwrap();
        assert_eq!(
            fb_mounts,
            json!([
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d" },
                { "name": "workspace", "mountPath": "/uv-cache", "subPath": "cache" },
                { "name": "git-credentials", "mountPath": "/etc/rivers/git", "readOnly": true },
            ])
        );
        // Main container identical across modes.
        assert_eq!(fb.main_mounts, shared_pieces.main_mounts);
        assert_eq!(fb.main_env, shared_pieces.main_env);
    }

    #[test]
    fn consumer_shared_has_no_init_no_creds_and_readonly_mount() {
        let pieces = consumer_pod_pieces(&spec(shared()));

        // The negative assertions ARE the design (RFC: "consumers need no
        // init container") — pin them as tightly as the positives.
        assert!(pieces.init_containers.is_empty(), "no init container");
        assert_eq!(
            serde_json::to_value(&pieces.volumes).unwrap(),
            json!([
                { "name": "workspace", "persistentVolumeClaim": {
                    "claimName": "analytics-workspace", "readOnly": true } },
            ]),
            "no git-credentials volume"
        );
        assert_eq!(
            serde_json::to_value(&pieces.main_mounts).unwrap(),
            json!([
                { "name": "workspace", "mountPath": "/workspace",
                  "subPath": "9f3c1ab8d2e4-1a2b3c4d", "readOnly": true },
            ])
        );
        assert_eq!(
            pieces.main_env,
            builder_pod_pieces(&spec(shared())).main_env
        );
    }

    #[test]
    fn consumer_fallback_builds_its_own_tree() {
        let pieces = consumer_pod_pieces(&spec(fallback()));
        assert_eq!(pieces.init_containers.len(), 1, "fallback consumers sync");
        // …but never prune: no /workspaces mount, no keep-set env.
        let init = &pieces.init_containers[0];
        let mounts = serde_json::to_value(init.volume_mounts.as_ref().unwrap()).unwrap();
        assert!(
            !mounts.to_string().contains("/workspaces"),
            "no PVC-root mount: {mounts}"
        );
        let env_names: Vec<&str> = init
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert!(!env_names.contains(&"RIVERS_WORKSPACE_KEEP"));
        assert!(env_names.contains(&"RIVERS_GIT_URL"));
        // Fallback consumers fetch, so they DO carry credentials.
        assert!(
            serde_json::to_value(&pieces.volumes)
                .unwrap()
                .to_string()
                .contains("git-credentials")
        );
        // Consumer main mount stays read-write in fallback (its own tree).
        assert_eq!(
            serde_json::to_value(&pieces.main_mounts).unwrap(),
            json!([
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d" },
            ])
        );
    }

    #[test]
    fn anonymous_spec_omits_credentials_everywhere() {
        let mut s = spec(shared());
        s.secret_name = None;
        let pieces = builder_pod_pieces(&s);
        let all = serde_json::to_value(&pieces).unwrap().to_string();
        assert!(!all.contains("git-credentials"), "{all}");
    }
}
