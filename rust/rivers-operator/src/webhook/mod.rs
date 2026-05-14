//! Operator-hosted admission webhook for `Run` and `CodeLocation` CRs.
//!
//! Run webhook: on CREATE, stamps `.spec.image` (from the referenced
//! `CodeLocation.status.resolvedImage`), `.spec.module` (from
//! `CodeLocation.spec.module`), and `.spec.codeLocationRef.identity` (from
//! `CodeLocation.spec.identity`) into the incoming Run. On UPDATE, rejects
//! mutations to those fields plus `codeLocationRef` — Run specs are
//! immutable post-creation.
//!
//! CodeLocation webhook: on CREATE, stamps a fresh UUID v4 into
//! `.spec.identity` if empty (or rejects a malformed user-supplied one). On
//! UPDATE, rejects any change to `.spec.identity` (immutable).
//!
//! Reads from the same `DirectoryState` cache that backs the
//! `CodeLocationRegistry` gRPC service, with a one-shot live `GET` fallback
//! against the API server (default `GetParams` = quorum read from etcd) to
//! cover the brief window where a follower replica's reflector hasn't yet
//! seen a freshly-created `CodeLocation`.
//!
//! TLS is provided by cert-manager: the Helm chart ships an `Issuer` +
//! `Certificate` pair that writes a Secret mounted into the operator pod,
//! and a `cert-manager.io/inject-ca-from` annotation on the
//! `MutatingWebhookConfiguration` keeps the matching `caBundle` in sync.
//! Rotation is handled in-process by [`server::reload_cert_loop`].

mod admission;
mod server;

pub use server::{Synced, serve};
