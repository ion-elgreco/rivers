//! DTO types for the UI — mirrors rivers-core types without surrealdb dependencies.
//! Used in server function signatures so they work on both SSR and WASM hydration targets.

use serde::{Deserialize, Serialize};

mod assets;
mod automation;
mod backfills;
mod config;
#[cfg(feature = "ssr")]
mod conversions;
mod events;
mod graph;
mod metadata;
mod partitions;
mod pools;
mod runs;

pub use assets::*;
pub use automation::*;
pub use backfills::*;
pub use config::*;
pub use events::*;
pub use graph::*;
pub use metadata::*;
pub use partitions::*;
pub use pools::*;
pub use runs::*;

/// Format a partition key to match gRPC's `py_partition_key_display` (`Multi` →
/// sorted `dim=v|dim=v`), so UI keys line up with the heatmap's gRPC-windowed keys.
#[cfg(feature = "ssr")]
pub(crate) fn partition_key_to_display(pk: rivers_core::storage::PartitionKey) -> String {
    pk.to_display()
}

/// Human-readable label for an identity: `name`, else `email`, else
/// `subject`. The single precedence rule every UI identity type shares.
/// An empty or whitespace-only string counts as absent, so a claim like
/// `name:""` or `name:" "` falls through instead of rendering blank.
pub(crate) fn display_name<'a>(
    name: Option<&'a str>,
    email: Option<&'a str>,
    subject: &'a str,
) -> &'a str {
    let non_empty = |s: &&'a str| !s.trim().is_empty();
    name.filter(non_empty)
        .or(email.filter(non_empty))
        .unwrap_or(subject)
}

/// Whether the host can reload its code location, and how the last reload went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DevReloadState {
    pub enabled: bool,
    /// Counts code locations that came back up; a reload is done once it moves.
    pub generation: u64,
    /// Why the last attempt did not come back up; cleared by the next attempt.
    pub error: Option<String>,
}

/// The signed-in user as exposed to the shell; `None` means auth mode
/// `none`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentUser {
    pub user: UserRef,
    /// Where the sign-out control points; `None` hides it (forward mode
    /// without a configured proxy logout URL).
    pub logout_url: Option<String>,
}

impl CurrentUser {
    pub fn display(&self) -> &str {
        self.user.display()
    }
}

/// One code location discovered via the operator's `CodeLocationRegistry`.
/// Mirrors the proto `CodeLocationEntry` shape so it serializes cleanly to
/// both SSR responses and WASM-side hydration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocationEntry {
    pub namespace: String,
    pub name: String,
    /// In-cluster DNS + port of the backing Service, e.g. `analytics.team-data.svc:3001`.
    /// No URL scheme — callers prepend `http://` when dialing.
    pub grpc_endpoint: String,
    pub image: String,
    pub module: String,
    /// "Pending" | "Deploying" | "Ready" | "Failed". Only `Ready` entries are
    /// safe to dial.
    pub phase: String,
    pub observed_generation: i64,
    /// Stable identity (UUID) from `CodeLocation.spec.identity`.
    pub identity: String,
}

impl CodeLocationEntry {
    /// True when the operator-reported `phase` is exactly `"Ready"` — the
    /// only state where the entry is safe to dial. Other phases (`Pending`,
    /// `Deploying`, `Failed`) mean the backing pod isn't serving yet.
    pub fn is_ready(&self) -> bool {
        self.phase == "Ready"
    }
}

/// A page of rows + the matching total — what every paginated server-fn returns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    pub rows: Vec<T>,
    pub total: u64,
}

#[cfg(test)]
mod display_tests {
    use super::{CurrentUser, UserRef, display_name};

    /// All UI identity types share one precedence: name → email → subject.
    /// Guards against the copies silently diverging.
    #[test]
    fn display_precedence_is_shared() {
        assert_eq!(display_name(Some("Jane"), Some("e@x"), "sub"), "Jane");
        assert_eq!(display_name(None, Some("e@x"), "sub"), "e@x");
        assert_eq!(display_name(None, None, "sub"), "sub");

        // An empty string is not a present value: fall through to the next
        // source so an IdP returning name:"" (or a verified email:"") renders
        // the real identity, not a blank chip/forbidden-page/audit label.
        assert_eq!(display_name(Some(""), Some("e@x"), "sub"), "e@x");
        assert_eq!(display_name(Some(""), Some(""), "sub"), "sub");
        assert_eq!(display_name(None, Some(""), "sub"), "sub");

        // Whitespace-only is absent too — complements the producer-side trim in
        // oidc/forward so a spaces-only claim falls through, not renders blank.
        assert_eq!(display_name(Some("   "), Some("e@x"), "sub"), "e@x");
        assert_eq!(display_name(Some("  "), None, "sub"), "sub");

        let full = UserRef {
            subject: "sub".into(),
            email: Some("e@x".into()),
            name: Some("Jane".into()),
        };
        assert_eq!(full.display(), "Jane");
        let cu = CurrentUser {
            user: full.clone(),
            logout_url: None,
        };
        // CurrentUser delegates to its nested UserRef.
        assert_eq!(cu.display(), full.display());

        let subject_only = UserRef {
            subject: "sub".into(),
            email: None,
            name: None,
        };
        assert_eq!(subject_only.display(), "sub");
    }
}
