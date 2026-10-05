//! `rivers-runtime`: the pod-side binary of the runtime image. Its
//! `workspace-sync` command is the `workspace` init container of git-sourced
//! CodeLocation pods.

pub mod workspace_sync;
