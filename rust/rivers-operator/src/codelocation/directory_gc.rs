//! Drops the storage directory entries of deleted CodeLocations. A code
//! location writes its own entry, so nothing else removes it when its CR goes.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kube_client::ResourceExt;
use rivers_core::assets::remote::LocationRef;
use rivers_core::storage::StorageBackend;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::code_location::CodeLocation;

use super::DirectoryState;
use crate::leader::LeaderGate;

const TARGET: &str = "rivers::operator::codelocation";

/// How often the leader sweeps the directory, and how long an entry must be
/// silent before a sweep drops it.
pub const SWEEP_EVERY: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct DirectoryGc {
    storage: Arc<SurrealStorage>,
    leader: LeaderGate,
}

impl DirectoryGc {
    pub fn new(storage: Arc<SurrealStorage>, leader: LeaderGate) -> Self {
        Self { storage, leader }
    }

    /// Drop `cl`'s entry if it still names `cl`'s identity. Only the leader
    /// drops; on the others, the next sweep of the leader does.
    pub async fn deleted(&self, cl: &CodeLocation) -> anyhow::Result<bool> {
        if !self.leader.is_leader() || cl.spec.identity.is_empty() {
            return Ok(false);
        }

        let location = LocationRef::new(cl.namespace().unwrap_or_default(), cl.name_any())?;
        let dropped = self
            .storage
            .delete_code_location_if(&location, &cl.spec.identity)
            .await?;
        if dropped {
            tracing::info!(target: TARGET, %location, "dropped the directory entry of a deleted code location");
        }
        Ok(dropped)
    }

    /// Drop each entry in `namespace` that no CR in `state` backs with the
    /// same identity and that has been silent for [`SWEEP_EVERY`]. A CR
    /// without an identity backs each entry of its name, because its pod
    /// registers under the CR name. Returns how many it dropped.
    pub async fn sweep(&self, namespace: &str, state: &DirectoryState) -> anyhow::Result<usize> {
        let cutoff = now_nanos() - SWEEP_EVERY.as_nanos() as i64;
        let mut dropped = 0;
        for (location, entry) in self.storage.list_code_locations().await? {
            if location.namespace() != namespace || entry.last_seen.is_some_and(|t| t > cutoff) {
                continue;
            }

            let backed = state
                .lookup_code_location(namespace, location.name())
                .await
                .is_some_and(|cl| {
                    cl.spec.identity.is_empty() || cl.spec.identity == entry.identity
                });
            if backed {
                continue;
            }

            if self
                .storage
                .delete_code_location_if(&location, &entry.identity)
                .await?
            {
                tracing::info!(
                    target: TARGET,
                    %location,
                    identity = %entry.identity,
                    "dropped a directory entry that no CodeLocation backs"
                );
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    /// [`Self::sweep`] every [`SWEEP_EVERY`] while this replica leads.
    pub async fn sweep_loop(self, namespace: String, state: Arc<DirectoryState>) {
        let mut ticks = tokio::time::interval(SWEEP_EVERY);
        loop {
            ticks.tick().await;
            if !self.leader.is_leader() {
                continue;
            }
            if let Err(e) = self.sweep(&namespace, &state).await {
                tracing::warn!(target: TARGET, error = %e, "code location directory sweep");
            }
        }
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

#[cfg(test)]
mod tests {
    use rivers_core::storage::CodeLocationDirectoryEntry;
    use rivers_k8s::crd::code_location::CodeLocationSpec;

    use super::*;

    const NS: &str = "rivers";

    fn cl(name: &str, identity: &str) -> CodeLocation {
        let spec: CodeLocationSpec = serde_json::from_value(serde_json::json!({
            "image": "ghcr.io/acme/pipeline",
            "identity": identity,
        }))
        .unwrap();
        let mut cl = CodeLocation::new(name, spec);
        cl.metadata.namespace = Some(NS.into());
        cl
    }

    fn location(namespace: &str, name: &str) -> LocationRef {
        LocationRef::new(namespace, name).unwrap()
    }

    async fn gc(leader: LeaderGate) -> DirectoryGc {
        DirectoryGc::new(
            Arc::new(SurrealStorage::new_memory().await.unwrap()),
            leader,
        )
    }

    async fn register(
        gc: &DirectoryGc,
        location: &LocationRef,
        identity: &str,
        last_seen: impl Into<Option<i64>>,
    ) {
        let entry = CodeLocationDirectoryEntry {
            last_seen: last_seen.into(),
            ..CodeLocationDirectoryEntry::new(identity)
        };
        gc.storage
            .upsert_code_location_registration(location, entry)
            .await
            .unwrap();
    }

    async fn identities(gc: &DirectoryGc) -> Vec<(String, String)> {
        let mut listed: Vec<_> = gc
            .storage
            .list_code_locations()
            .await
            .unwrap()
            .into_iter()
            .map(|(location, entry)| (location.to_string(), entry.identity))
            .collect();
        listed.sort();
        listed
    }

    fn silent() -> i64 {
        now_nanos() - 2 * SWEEP_EVERY.as_nanos() as i64
    }

    #[tokio::test]
    async fn a_deleted_code_location_drops_its_entry() {
        let gc = gc(LeaderGate::leading()).await;
        register(&gc, &location(NS, "warehouse"), "wh-1", now_nanos()).await;

        assert!(gc.deleted(&cl("warehouse", "wh-1")).await.unwrap());
        assert_eq!(identities(&gc).await, vec![]);
    }

    #[tokio::test]
    async fn an_entry_of_a_recreated_code_location_stays() {
        let gc = gc(LeaderGate::leading()).await;
        register(&gc, &location(NS, "warehouse"), "wh-2", now_nanos()).await;

        assert!(!gc.deleted(&cl("warehouse", "wh-1")).await.unwrap());
        assert_eq!(
            identities(&gc).await,
            vec![("rivers/warehouse".into(), "wh-2".into())]
        );
    }

    #[tokio::test]
    async fn a_follower_drops_nothing() {
        let gc = gc(LeaderGate::new()).await;
        register(&gc, &location(NS, "warehouse"), "wh-1", now_nanos()).await;

        assert!(!gc.deleted(&cl("warehouse", "wh-1")).await.unwrap());
        assert_eq!(identities(&gc).await.len(), 1);
    }

    #[tokio::test]
    async fn sweep_drops_silent_entries_that_no_code_location_backs() {
        let gc = gc(LeaderGate::leading()).await;
        let state = DirectoryState::new();
        state.upsert_code_location(cl("warehouse", "wh-1")).await;
        state.upsert_code_location(cl("analytics", "an-2")).await;

        register(&gc, &location(NS, "warehouse"), "wh-1", silent()).await;
        register(&gc, &location(NS, "analytics"), "an-1", silent()).await;
        register(&gc, &location(NS, "gone"), "gone-1", silent()).await;
        register(&gc, &location(NS, "starting"), "st-1", now_nanos()).await;
        register(&gc, &location("dev", "default"), "default", silent()).await;

        assert_eq!(gc.sweep(NS, &state).await.unwrap(), 2);
        assert_eq!(
            identities(&gc).await,
            vec![
                ("dev/default".into(), "default".into()),
                ("rivers/starting".into(), "st-1".into()),
                ("rivers/warehouse".into(), "wh-1".into()),
            ]
        );
    }

    #[tokio::test]
    async fn sweep_keeps_the_entry_of_a_code_location_without_identity() {
        for last_seen in [None, Some(silent())] {
            let gc = gc(LeaderGate::leading()).await;
            let state = DirectoryState::new();
            state.upsert_code_location(cl("warehouse", "")).await;
            register(&gc, &location(NS, "warehouse"), "warehouse", last_seen).await;

            assert_eq!(
                gc.sweep(NS, &state).await.unwrap(),
                0,
                "last_seen: {last_seen:?}"
            );
            assert_eq!(
                identities(&gc).await,
                vec![("rivers/warehouse".into(), "warehouse".into())],
                "last_seen: {last_seen:?}"
            );
        }
    }
}
