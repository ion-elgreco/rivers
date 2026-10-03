//! Workspace pod-spec pieces for git-sourced CodeLocations (RFC-044).
//!
//! This is the ONE builder for workspace mounts, init containers, volumes,
//! env and the main container's command and working directory — consumed by
//! the operator (code-location Deployment, run executor pod) and by the
//! step-Job builder in [`crate::executor`], which runs *inside the run pod*.
//! That call-site is why this lives in `rivers-k8s` rather than the operator
//! crate: the dependency arrow points operator → rivers-k8s, and a builder
//! in the operator could never be reached from the step-job path.
//!
//! Two volume modes, one shape: `subPath` mounts keep `/workspace/src` and
//! `/workspace/venv` byte-identical whether the tree lives on the shared
//! RWX PVC or in a per-pod `emptyDir`. What varies is the `volumes:` stanza,
//! the uv cache (only the shared PVC keeps one) and which pods carry an init
//! container:
//!
//! * **builder** (the code-location pod) — always runs
//!   `rivers-workspace-sync`; in shared mode it then prunes the trees beside
//!   its own ([`Prune`]): it mounts the PVC root at `/workspaces` to see
//!   them, and receives the keep-set via `configMapKeyRef` (never inline — an
//!   inline value would live in the pod template and roll the Deployment on
//!   run lifecycle).
//! * **consumer** (run executor pod, step Jobs) — in shared mode mounts the
//!   finished tree read-only with NO init container, NO git credentials; in
//!   fallback mode it builds its own tree, as the builder does there: an
//!   `emptyDir` holds no other tree to prune.

use std::time::Duration;

use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMapKeySelector, Container, EnvVar, EnvVarSource, PodSecurityContext,
    SecretVolumeSource, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::crd::run::RunSource;

pub const WORKSPACE_VOLUME: &str = "workspace";
pub const GIT_CREDS_VOLUME: &str = "git-credentials";
pub const WORKSPACE_MOUNT: &str = "/workspace";
pub const WORKSPACES_ROOT_MOUNT: &str = "/workspaces";
pub const UV_CACHE_MOUNT: &str = "/uv-cache";
pub const UV_CACHE_SUBPATH: &str = "cache";
pub const GIT_CREDS_MOUNT: &str = "/etc/rivers/git";
/// The pod `command` in git mode, whatever the dependencies mode: in mode
/// `none` the sync script links it to the runtime image's `rivers`.
const VENV_RIVERS_BIN: &str = "/workspace/venv/bin/rivers";
pub const VENV_PATH: &str = "/workspace/venv";
/// The checkout.
const SRC_PATH: &str = "/workspace/src";
pub const SYNC_COMMAND: &str = "rivers-workspace-sync";
/// The init container that runs [`SYNC_COMMAND`].
pub const SYNC_CONTAINER: &str = "workspace";
pub const KEEP_CONFIG_MAP_KEY: &str = "keep";
/// GID of the runtime image's `USER` (deploy/docker/Dockerfile.runtime).
pub const RUNTIME_GID: i64 = 65532;

/// Where the workspace volume lives.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkspaceVolume {
    /// One RWX PVC per CodeLocation, trees shared across its pods.
    SharedPvc { claim_name: String },
    /// Per-pod scratch — every pod builds its own tree.
    EmptyDir { size_limit: Option<Quantity> },
}

/// A tree and the volume it lives on.
#[derive(Clone, Debug)]
pub struct WorkspaceSpec {
    /// The tree: [`RunSource::workspace_key`] is its subpath on the volume,
    /// and the pods that run it carry it in `RIVERS_RUN_SOURCE`.
    pub source: RunSource,
    pub volume: WorkspaceVolume,
    /// The CL's `spec.env`, applied to the init container too (RFC-036
    /// extended: `UV_INDEX_URL` & co. are needed at install time).
    pub extra_env: Vec<EnvVar>,
}

impl WorkspaceSpec {
    /// The project directory: the checkout plus `path`.
    fn working_dir(&self) -> String {
        match self
            .source
            .git
            .path
            .as_deref()
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty())
        {
            Some(p) => format!("{SRC_PATH}/{p}"),
            None => SRC_PATH.to_string(),
        }
    }
}

/// The trees the code-location pod keeps when it prunes the shared PVC: its
/// own, those in the keep-set, the `keep_revisions` newest, and those younger
/// than `min_tree_age`.
#[derive(Clone, Debug)]
pub struct Prune {
    /// The keep-set ConfigMap.
    pub keep_config_map: String,
    pub keep_revisions: u32,
    pub min_tree_age: Duration,
}

/// Pod-spec fragments to graft onto a Deployment / Pod / Job template.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct WorkspacePodPieces {
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    /// The tree's `rivers`, run from the project directory.
    pub main_command: Vec<String>,
    pub main_working_dir: String,
    pub main_mounts: Vec<VolumeMount>,
    pub main_env: Vec<EnvVar>,
    pub pod_security_context: PodSecurityContext,
}

/// Pieces for the **code-location pod** — the one pod that builds trees on
/// the shared PVC, and then prunes the trees beside its own as `prune` says.
/// In fallback mode it builds its own tree like any other pod.
pub fn builder_pod_pieces(spec: &WorkspaceSpec, prune: &Prune) -> WorkspacePodPieces {
    pod_pieces(
        spec,
        match spec.volume {
            WorkspaceVolume::SharedPvc { .. } => Shape::Builds(Build::Shared(prune)),
            WorkspaceVolume::EmptyDir { .. } => Shape::Builds(Build::Own),
        },
    )
}

/// Pieces for **run executor pods and step Jobs**. Shared mode: read-only
/// mount, no init container, no credentials — the admission chain
/// guarantees the tree exists (CL `Ready` ⟹ tree built). Fallback mode:
/// the pod builds its own tree.
pub fn consumer_pod_pieces(spec: &WorkspaceSpec) -> WorkspacePodPieces {
    pod_pieces(
        spec,
        match spec.volume {
            WorkspaceVolume::SharedPvc { .. } => Shape::Mounts,
            WorkspaceVolume::EmptyDir { .. } => Shape::Builds(Build::Own),
        },
    )
}

/// How a pod gets its tree.
#[derive(Clone, Copy)]
enum Shape<'a> {
    /// Run executor pods and step Jobs in shared mode: they mount the tree
    /// the code-location pod built, read-only.
    Mounts,
    /// The pod builds its tree in its init container, and so fetches.
    Builds(Build<'a>),
}

#[derive(Clone, Copy)]
enum Build<'a> {
    /// The code-location pod in shared mode builds on the PVC, then prunes
    /// the trees beside its own.
    Shared(&'a Prune),
    /// Every pod in fallback mode builds in its own `emptyDir`.
    Own,
}

fn pod_pieces(spec: &WorkspaceSpec, shape: Shape) -> WorkspacePodPieces {
    let key = spec.source.workspace_key();
    let read_only = matches!(shape, Shape::Mounts);
    let mut pieces = WorkspacePodPieces {
        init_containers: Vec::new(),
        volumes: vec![workspace_volume(&spec.volume, read_only)],
        main_command: vec![VENV_RIVERS_BIN.to_string()],
        main_working_dir: spec.working_dir(),
        main_mounts: vec![workspace_mount(&key, read_only)],
        main_env: main_env(spec),
        pod_security_context: pod_security_context(),
    };
    if let Shape::Builds(build) = shape {
        pieces
            .init_containers
            .push(sync_init_container(spec, &key, build));
        pieces.volumes.extend(credentials_volume(spec));
    }
    pieces
}

fn sync_init_container(spec: &WorkspaceSpec, key: &str, build: Build) -> Container {
    let git = &spec.source.git;
    let deps = &spec.source.dependencies;
    let mut env = vec![
        env_var("RIVERS_GIT_URL", &git.url),
        env_var("RIVERS_GIT_COMMIT", &git.commit),
    ];
    if let Some(r) = &git.r#ref {
        env.push(env_var("RIVERS_GIT_REF", r));
    }
    if let Some(p) = &git.path {
        env.push(env_var("RIVERS_GIT_PATH", p));
    }
    env.push(env_var("RIVERS_DEPS_MODE", deps.mode.as_str()));
    if !deps.files.is_empty() {
        env.push(env_var("RIVERS_DEPS_FILES", &deps.files.join(",")));
    }
    if !deps.extras.is_empty() {
        env.push(env_var("RIVERS_DEPS_EXTRAS", &deps.extras.join(",")));
    }
    if !deps.groups.is_empty() {
        env.push(env_var("RIVERS_DEPS_GROUPS", &deps.groups.join(",")));
    }
    if let Some(t) = deps.timeout_seconds {
        env.push(env_var("RIVERS_DEPS_TIMEOUT_SECONDS", &t.to_string()));
    }
    env.push(env_var("UV_PROJECT_ENVIRONMENT", VENV_PATH));
    // A fallback pod's cache is never reused, and it would hold a second copy
    // of the venv inside the size-limited emptyDir.
    env.push(match build {
        Build::Shared(_) => env_var("UV_CACHE_DIR", UV_CACHE_MOUNT),
        Build::Own => env_var("UV_NO_CACHE", "1"),
    });
    env.push(env_var("UV_LINK_MODE", "copy"));
    env.push(env_var("UV_COMPILE_BYTECODE", "1"));

    let mut mounts = vec![workspace_mount(key, false)];
    if let Build::Shared(prune) = build {
        // The prune never removes this tree, whatever the keep-set says.
        env.push(env_var("RIVERS_WORKSPACE_KEY", key));
        env.push(EnvVar {
            name: "RIVERS_WORKSPACE_KEEP".to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: prune.keep_config_map.clone(),
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
        env.push(env_var(
            "RIVERS_WORKSPACE_KEEP_REVISIONS",
            &prune.keep_revisions.to_string(),
        ));
        env.push(env_var(
            "RIVERS_WORKSPACE_MIN_AGE_SECONDS",
            &prune.min_tree_age.as_secs().to_string(),
        ));
        mounts.push(VolumeMount {
            name: WORKSPACE_VOLUME.to_string(),
            mount_path: UV_CACHE_MOUNT.to_string(),
            sub_path: Some(UV_CACHE_SUBPATH.to_string()),
            ..Default::default()
        });
        // The volume root: the prune walks the trees beside this one.
        mounts.push(VolumeMount {
            name: WORKSPACE_VOLUME.to_string(),
            mount_path: WORKSPACES_ROOT_MOUNT.to_string(),
            ..Default::default()
        });
    }
    if git.secret_name.is_some() {
        mounts.push(VolumeMount {
            name: GIT_CREDS_VOLUME.to_string(),
            mount_path: GIT_CREDS_MOUNT.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    Container {
        name: SYNC_CONTAINER.to_string(),
        image: Some(spec.source.runtime_image.clone()),
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
        env: Some(crate::env::merge_env(env, spec.extra_env.iter().cloned())),
        volume_mounts: Some(mounts),
        ..Default::default()
    }
}

/// The non-root sync container reads the root-owned Secret files, and writes
/// the shared PVC where the storage driver applies fsGroup, through this group.
/// `OnRootMismatch`: otherwise every pod start re-chowns every tree on the PVC.
fn pod_security_context() -> PodSecurityContext {
    PodSecurityContext {
        fs_group: Some(RUNTIME_GID),
        fs_group_change_policy: Some("OnRootMismatch".to_string()),
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

fn workspace_mount(key: &str, read_only: bool) -> VolumeMount {
    VolumeMount {
        name: WORKSPACE_VOLUME.to_string(),
        mount_path: WORKSPACE_MOUNT.to_string(),
        sub_path: Some(key.to_string()),
        read_only: read_only.then_some(true),
        ..Default::default()
    }
}

fn workspace_volume(volume: &WorkspaceVolume, read_only: bool) -> Volume {
    match volume {
        WorkspaceVolume::SharedPvc { claim_name } => Volume {
            name: WORKSPACE_VOLUME.to_string(),
            persistent_volume_claim: Some(
                k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                    claim_name: claim_name.clone(),
                    read_only: read_only.then_some(true),
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

fn credentials_volume(spec: &WorkspaceSpec) -> Option<Volume> {
    let secret = spec.source.git.secret_name.as_ref()?;
    Some(Volume {
        name: GIT_CREDS_VOLUME.to_string(),
        secret: Some(SecretVolumeSource {
            secret_name: Some(secret.clone()),
            default_mode: Some(0o440),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// The tree's provenance and volume ride along on every pod that runs from
/// it: [`crate::env::detect_git_workspace`] reads them back in-pod, so the
/// Runs and step Jobs a pod launches get the same tree.
fn main_env(spec: &WorkspaceSpec) -> Vec<EnvVar> {
    let source = serde_json::to_string(&spec.source).expect("RunSource serializes");
    let mut env = vec![
        // The CLI imports the module from ".", which no longer finds it once
        // code changes directory, also in the worker processes it starts.
        env_var("PYTHONPATH", &spec.working_dir()),
        env_var("VIRTUAL_ENV", VENV_PATH),
        // In the pod template ⇒ a new commit rolls the Deployment.
        env_var("RIVERS_GIT_COMMIT", &spec.source.git.commit),
        env_var(crate::env::ENV_RUN_SOURCE, &source),
    ];
    match &spec.volume {
        WorkspaceVolume::SharedPvc { claim_name } => {
            env.push(env_var(crate::env::ENV_WORKSPACE_PVC, claim_name));
        }
        WorkspaceVolume::EmptyDir { size_limit } => {
            if let Some(limit) = size_limit {
                env.push(env_var(crate::env::ENV_WORKSPACE_EMPTYDIR_LIMIT, &limit.0));
            }
        }
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::code_location::{Dependencies, DependencyMode};
    use crate::crd::run::GitCoordinates;
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
            source: RunSource {
                git: GitCoordinates {
                    url: "https://forge.example/acme/pipelines.git".to_string(),
                    commit: "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8".to_string(),
                    r#ref: Some("refs/heads/main".to_string()),
                    path: Some("analytics".to_string()),
                    secret_name: Some("git-creds".to_string()),
                },
                dependencies: deps(),
                runtime_image: "ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff".to_string(),
            },
            volume,
            extra_env: vec![EnvVar {
                name: "UV_INDEX_URL".to_string(),
                value: Some("https://pypi.internal/simple".to_string()),
                ..Default::default()
            }],
        }
    }

    /// The chart's default floors.
    fn prune() -> Prune {
        Prune {
            keep_config_map: "analytics-workspace-keep".to_string(),
            keep_revisions: 3,
            min_tree_age: Duration::from_secs(3600),
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
    fn builder_shared_golden() {
        let pieces = builder_pod_pieces(&spec(shared()), &prune());
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
                    { "name": "RIVERS_WORKSPACE_KEY", "value": "9f3c1ab8d2e4-1a2b3c4d-03a30844" },
                    // Keep-set via ConfigMap indirection — see module docs.
                    // optional: a missing ConfigMap degrades to an empty
                    // keep-set; the recency + age floors still guard.
                    { "name": "RIVERS_WORKSPACE_KEEP", "valueFrom": { "configMapKeyRef": {
                        "name": "analytics-workspace-keep", "key": "keep", "optional": true }}},
                    { "name": "RIVERS_WORKSPACE_KEEP_REVISIONS", "value": "3" },
                    { "name": "RIVERS_WORKSPACE_MIN_AGE_SECONDS", "value": "3600" },
                    { "name": "UV_INDEX_URL", "value": "https://pypi.internal/simple" },
                ],
                "volumeMounts": [
                    { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d-03a30844" },
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
                { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 0o440 } },
            ])
        );
        assert_eq!(pieces.main_command, ["/workspace/venv/bin/rivers"]);
        assert_eq!(pieces.main_working_dir, "/workspace/src/analytics");
        assert_eq!(
            serde_json::to_value(&pieces.main_mounts).unwrap(),
            json!([
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d-03a30844" },
            ])
        );
        assert_eq!(
            serde_json::to_value(&pieces.main_env).unwrap(),
            json!([
                { "name": "PYTHONPATH", "value": "/workspace/src/analytics" },
                { "name": "VIRTUAL_ENV", "value": "/workspace/venv" },
                { "name": "RIVERS_GIT_COMMIT", "value": "9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8" },
                { "name": "RIVERS_RUN_SOURCE", "value": concat!(
                    r#"{"git":{"url":"https://forge.example/acme/pipelines.git","#,
                    r#""commit":"9f3c1ab8d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8","#,
                    r#""ref":"refs/heads/main","path":"analytics","secretName":"git-creds"},"#,
                    r#""dependencies":{"mode":"auto","extras":["dev"],"timeoutSeconds":300},"#,
                    r#""runtimeImage":"ghcr.io/acme/rivers-runtime@sha256:1a2b3c4dffff"}"#,
                ) },
                { "name": "RIVERS_WORKSPACE_PVC", "value": "analytics-workspace" },
            ])
        );
    }

    #[test]
    fn fallback_pods_differ_from_the_shared_builder_only_where_documented() {
        let shared_pieces = builder_pod_pieces(&spec(shared()), &prune());
        let fb = consumer_pod_pieces(&spec(fallback()));

        // emptyDir volumes stanza.
        assert_eq!(
            serde_json::to_value(&fb.volumes).unwrap(),
            json!([
                { "name": "workspace", "emptyDir": { "sizeLimit": "2Gi" } },
                { "name": "git-credentials", "secret": { "secretName": "git-creds", "defaultMode": 0o440 } },
            ])
        );
        // No uv cache and no prune, otherwise identical.
        let env_without = |c: &Container, names: &[&str]| -> Vec<EnvVar> {
            c.env
                .iter()
                .flatten()
                .filter(|e| !names.contains(&e.name.as_str()))
                .cloned()
                .collect()
        };
        assert_eq!(
            env_without(&fb.init_containers[0], &["UV_NO_CACHE"]),
            env_without(
                &shared_pieces.init_containers[0],
                &[
                    "UV_CACHE_DIR",
                    "RIVERS_WORKSPACE_KEY",
                    "RIVERS_WORKSPACE_KEEP",
                    "RIVERS_WORKSPACE_KEEP_REVISIONS",
                    "RIVERS_WORKSPACE_MIN_AGE_SECONDS",
                ]
            ),
        );
        let fb_mounts =
            serde_json::to_value(fb.init_containers[0].volume_mounts.as_ref().unwrap()).unwrap();
        assert_eq!(
            fb_mounts,
            json!([
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d-03a30844" },
                { "name": "git-credentials", "mountPath": "/etc/rivers/git", "readOnly": true },
            ])
        );
        // Main container: same mounts; env differs only in the volume entry.
        assert_eq!(fb.main_mounts, shared_pieces.main_mounts);
        let (fb_volume_env, fb_env): (Vec<_>, Vec<_>) = fb
            .main_env
            .iter()
            .partition(|e| e.name == crate::env::ENV_WORKSPACE_EMPTYDIR_LIMIT);
        let (shared_volume_env, shared_env): (Vec<_>, Vec<_>) = shared_pieces
            .main_env
            .iter()
            .partition(|e| e.name == crate::env::ENV_WORKSPACE_PVC);
        assert_eq!(fb_env, shared_env);
        assert_eq!(
            serde_json::to_value(&fb_volume_env).unwrap(),
            json!([{ "name": "RIVERS_WORKSPACE_EMPTYDIR_LIMIT", "value": "2Gi" }])
        );
        assert_eq!(
            serde_json::to_value(&shared_volume_env).unwrap(),
            json!([{ "name": "RIVERS_WORKSPACE_PVC", "value": "analytics-workspace" }])
        );
    }

    #[test]
    fn the_fallback_code_location_pod_is_built_like_its_run_pods() {
        let cl_pod = builder_pod_pieces(&spec(fallback()), &prune());
        let prune_env: Vec<_> = cl_pod.init_containers[0]
            .env
            .iter()
            .flatten()
            .filter(|e| e.name.starts_with("RIVERS_WORKSPACE_"))
            .map(|e| e.name.as_str())
            .collect();

        assert_eq!(prune_env, Vec::<&str>::new());
        assert_eq!(
            serde_json::to_value(&cl_pod).unwrap(),
            serde_json::to_value(consumer_pod_pieces(&spec(fallback()))).unwrap()
        );
    }

    #[test]
    fn the_builder_tells_its_prune_which_tree_it_mounts() {
        let pieces = builder_pod_pieces(&spec(shared()), &prune());
        let init = &pieces.init_containers[0];
        let key = init
            .env
            .iter()
            .flatten()
            .find(|e| e.name == "RIVERS_WORKSPACE_KEY")
            .and_then(|e| e.value.as_deref());
        let sub_paths: Vec<_> = init
            .volume_mounts
            .iter()
            .flatten()
            .chain(&pieces.main_mounts)
            .filter(|m| m.mount_path == WORKSPACE_MOUNT)
            .map(|m| m.sub_path.as_deref())
            .collect();

        assert_eq!(key, Some("9f3c1ab8d2e4-1a2b3c4d-03a30844"));
        assert_eq!(sub_paths, [key, key]);
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
                  "subPath": "9f3c1ab8d2e4-1a2b3c4d-03a30844", "readOnly": true },
            ])
        );
        assert_eq!(
            pieces.main_env,
            builder_pod_pieces(&spec(shared()), &prune()).main_env
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
                { "name": "workspace", "mountPath": "/workspace", "subPath": "9f3c1ab8d2e4-1a2b3c4d-03a30844" },
            ])
        );
    }

    #[test]
    fn only_the_shared_builder_keeps_a_uv_cache() {
        for (shape, pieces, cache_env, cache_mounts) in [
            (
                "builder shared",
                builder_pod_pieces(&spec(shared()), &prune()),
                json!([{ "name": "UV_CACHE_DIR", "value": "/uv-cache" }]),
                json!([{ "name": "workspace", "mountPath": "/uv-cache", "subPath": "cache" }]),
            ),
            (
                "builder fallback",
                builder_pod_pieces(&spec(fallback()), &prune()),
                json!([{ "name": "UV_NO_CACHE", "value": "1" }]),
                json!([]),
            ),
            (
                "consumer fallback",
                consumer_pod_pieces(&spec(fallback())),
                json!([{ "name": "UV_NO_CACHE", "value": "1" }]),
                json!([]),
            ),
        ] {
            let init = &pieces.init_containers[0];
            let env: Vec<_> = init
                .env
                .iter()
                .flatten()
                .filter(|e| matches!(e.name.as_str(), "UV_CACHE_DIR" | "UV_NO_CACHE"))
                .collect();
            assert_eq!(serde_json::to_value(&env).unwrap(), cache_env, "{shape}");
            let mounts: Vec<_> = init
                .volume_mounts
                .iter()
                .flatten()
                .filter(|m| {
                    m.mount_path == UV_CACHE_MOUNT
                        || m.sub_path.as_deref() == Some(UV_CACHE_SUBPATH)
                })
                .collect();
            assert_eq!(
                serde_json::to_value(&mounts).unwrap(),
                cache_mounts,
                "{shape}"
            );
        }
    }

    #[test]
    fn every_pod_shape_runs_the_trees_rivers_from_the_project_directory() {
        for (shape, pieces) in [
            (
                "builder shared",
                builder_pod_pieces(&spec(shared()), &prune()),
            ),
            (
                "builder fallback",
                builder_pod_pieces(&spec(fallback()), &prune()),
            ),
            ("consumer shared", consumer_pod_pieces(&spec(shared()))),
            ("consumer fallback", consumer_pod_pieces(&spec(fallback()))),
        ] {
            let pythonpath: Vec<_> = pieces
                .main_env
                .iter()
                .filter(|e| e.name == "PYTHONPATH")
                .map(|e| e.value.as_deref())
                .collect();

            assert_eq!(
                pieces.main_command,
                ["/workspace/venv/bin/rivers"],
                "{shape}"
            );
            assert_eq!(
                pieces.main_working_dir, "/workspace/src/analytics",
                "{shape}"
            );
            assert_eq!(pythonpath, [Some("/workspace/src/analytics")], "{shape}");
        }
    }

    #[test]
    fn the_project_directory_is_the_checkout_plus_path() {
        for (path, dir) in [
            (None, "/workspace/src"),
            (Some(""), "/workspace/src"),
            (Some("analytics"), "/workspace/src/analytics"),
            (
                Some("services/analytics/"),
                "/workspace/src/services/analytics",
            ),
            (Some("/deep/dir/"), "/workspace/src/deep/dir"),
        ] {
            let mut s = spec(shared());
            s.source.git.path = path.map(str::to_string);
            assert_eq!(s.working_dir(), dir, "{path:?}");
        }
    }

    #[test]
    fn every_pod_shape_sets_the_runtime_fs_group() {
        for (shape, pieces) in [
            (
                "builder shared",
                builder_pod_pieces(&spec(shared()), &prune()),
            ),
            (
                "builder fallback",
                builder_pod_pieces(&spec(fallback()), &prune()),
            ),
            ("consumer shared", consumer_pod_pieces(&spec(shared()))),
            ("consumer fallback", consumer_pod_pieces(&spec(fallback()))),
        ] {
            assert_eq!(
                serde_json::to_value(&pieces.pod_security_context).unwrap(),
                json!({ "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch" }),
                "{shape}"
            );
        }
    }

    #[test]
    fn user_env_replaces_same_named_sync_env() {
        let mut s = spec(shared());
        s.extra_env.push(EnvVar {
            name: "UV_COMPILE_BYTECODE".to_string(),
            value: Some("0".to_string()),
            ..Default::default()
        });
        let fb = WorkspaceSpec {
            volume: fallback(),
            ..s.clone()
        };
        for (shape, pieces) in [
            ("builder shared", builder_pod_pieces(&s, &prune())),
            ("builder fallback", builder_pod_pieces(&fb, &prune())),
            ("consumer fallback", consumer_pod_pieces(&fb)),
        ] {
            let env = pieces.init_containers[0].env.as_ref().unwrap();
            let compile: Vec<_> = env
                .iter()
                .filter(|e| e.name == "UV_COMPILE_BYTECODE")
                .map(|e| e.value.as_deref())
                .collect();
            // Server-side apply rejects duplicate names in a container's env.
            assert_eq!(compile, vec![Some("0")], "{shape}");
        }
    }

    #[test]
    fn anonymous_spec_omits_credentials_everywhere() {
        let mut s = spec(shared());
        s.source.git.secret_name = None;
        let pieces = builder_pod_pieces(&s, &prune());
        let all = serde_json::to_value(&pieces).unwrap().to_string();
        assert!(!all.contains("git-credentials"), "{all}");
    }
}
