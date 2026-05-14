//! CodeLocation directory gRPC service.
//!
//! [`DirectoryState`] is fed by [`run_watcher`] — a dedicated
//! `kube_runtime::watcher` stream separate from the reconciler, so CR
//! deletions are visible without needing a finalizer.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::stream::{self, Stream, StreamExt};
use kube_client::{Api, ResourceExt};
use kube_runtime::watcher;
use kube_runtime::watcher::{Config as WatcherConfig, Event};
use rivers_api::code_location_event::Type as EventType;
use rivers_api::code_location_registry_service_server::{
    CodeLocationRegistryService, CodeLocationRegistryServiceServer,
};
use rivers_api::{
    CodeLocationEntry, CodeLocationEvent, ListCodeLocationsRequest, ListCodeLocationsResponse,
    WatchCodeLocationsRequest,
};
use rivers_k8s::crd::code_location::{CodeLocation, CodeLocationSpec};
use tokio::sync::{RwLock, broadcast};
use tonic::service::Interceptor;
use tonic::{Request, Response, Status};

/// Capacity of the broadcast channel. Events are small; 256 keeps slow
/// subscribers from losing events under normal churn but bounds memory.
const BROADCAST_CAPACITY: usize = 256;

/// Ordering is namespace-first so iteration yields entries grouped by
/// namespace — handy for snapshots.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    namespace: String,
    name: String,
}

impl EntryKey {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

/// In-memory directory of known CodeLocations, plus a broadcast channel for
/// live updates. All mutations go through [`DirectoryState::upsert`] /
/// [`DirectoryState::delete`] so every observable change emits exactly one
/// event.
///
/// Two parallel maps:
/// - `entries` carries the gRPC-projected, status-dependent shape consumed
///   by the admission webhook and the UI registry stream. Only populated
///   once a CR has a status (so its `resolved_image` / `grpc_endpoint` are
///   meaningful).
/// - `specs` carries the full `CodeLocationSpec` for every CR the watcher
///   has seen, regardless of status. Used by the run reconciler to resolve
///   `spec.env` without an extra kube API GET per reconcile.
pub struct DirectoryState {
    entries: RwLock<BTreeMap<EntryKey, CodeLocationEntry>>,
    specs: RwLock<BTreeMap<EntryKey, Arc<CodeLocationSpec>>>,
    tx: broadcast::Sender<CodeLocationEvent>,
}

impl Default for DirectoryState {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectoryState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            entries: RwLock::new(BTreeMap::new()),
            specs: RwLock::new(BTreeMap::new()),
            tx,
        }
    }

    /// Subscribe to live ADDED/MODIFIED/DELETED events. The receiver only sees
    /// events emitted after the subscription; pair with [`Self::snapshot`] for
    /// initial state.
    pub fn subscribe(&self) -> broadcast::Receiver<CodeLocationEvent> {
        self.tx.subscribe()
    }

    /// Pass `""` to disable the namespace filter.
    pub async fn snapshot(&self, namespace_filter: &str) -> Vec<CodeLocationEntry> {
        let map = self.entries.read().await;
        if namespace_filter.is_empty() {
            map.values().cloned().collect()
        } else {
            map.values()
                .filter(|e| e.namespace == namespace_filter)
                .cloned()
                .collect()
        }
    }

    /// O(log n) lookup by `(namespace, name)`. Used by the admission webhook
    /// to resolve a Run's `codeLocationRef` without scanning the snapshot.
    /// Returns `None` if the entry isn't in the cache; callers may then fall
    /// back to a live API server `GET` to cover the brief startup window
    /// where a follower's reflector hasn't seen the CR yet.
    pub async fn lookup(&self, namespace: &str, name: &str) -> Option<CodeLocationEntry> {
        let key = EntryKey::new(namespace, name);
        self.entries.read().await.get(&key).cloned()
    }

    /// Lookup the cached `CodeLocationSpec` by `(namespace, name)`. Populated
    /// by the watcher for every observed CR (status-independent), so this is
    /// the path the run reconciler uses when resolving `spec.env`. Returns
    /// `None` only during the brief startup window before the watcher syncs.
    pub async fn lookup_spec(&self, namespace: &str, name: &str) -> Option<Arc<CodeLocationSpec>> {
        let key = EntryKey::new(namespace, name);
        self.specs.read().await.get(&key).cloned()
    }

    /// Insert or update an entry. Emits `ADDED` on first sight, `MODIFIED` on
    /// subsequent changes, and nothing at all on a byte-for-byte no-op.
    pub async fn upsert(&self, entry: CodeLocationEntry) {
        let key = EntryKey::new(&entry.namespace, &entry.name);
        let event_type = {
            let mut map = self.entries.write().await;
            match map.get(&key) {
                Some(prior) if prior == &entry => return,
                Some(_) => {
                    map.insert(key, entry.clone());
                    EventType::Modified
                }
                None => {
                    map.insert(key, entry.clone());
                    EventType::Added
                }
            }
        };
        let _ = self.tx.send(CodeLocationEvent {
            r#type: event_type as i32,
            entry: Some(entry),
        });
    }

    /// Cache the full spec for `(namespace, name)`. Silent — no broadcast
    /// event, since spec-level changes don't affect the gRPC consumers that
    /// subscribe to the entry stream.
    pub async fn upsert_spec(&self, namespace: &str, name: &str, spec: Arc<CodeLocationSpec>) {
        let key = EntryKey::new(namespace, name);
        self.specs.write().await.insert(key, spec);
    }

    /// Drop the entry and its cached spec, emitting a `DELETED` event if the
    /// entry was present. Silent for unknown `(namespace, name)` pairs.
    pub async fn delete(&self, namespace: &str, name: &str) {
        let key = EntryKey::new(namespace, name);
        let removed = {
            let mut map = self.entries.write().await;
            map.remove(&key)
        };
        self.specs.write().await.remove(&key);
        if let Some(entry) = removed {
            let _ = self.tx.send(CodeLocationEvent {
                r#type: EventType::Deleted as i32,
                entry: Some(entry),
            });
        }
    }
}

/// Drive the directory state from a dedicated `watcher` stream. Outlives any
/// individual gRPC client; a single watcher feeds all subscribers.
///
/// `synced_signal` (when provided) is flipped to `ready` on the first
/// `Event::InitDone` — used by the admission webhook's readiness probe so
/// the API server doesn't dispatch admission traffic to an un-synced
/// follower replica.
pub async fn run_watcher(
    client: kube_client::Client,
    namespace: String,
    state: Arc<DirectoryState>,
    synced_signal: Option<crate::webhook::Synced>,
) {
    let api: Api<CodeLocation> = Api::namespaced(client, &namespace);
    let mut stream = watcher::watcher(api, WatcherConfig::default()).boxed();

    while let Some(ev) = stream.next().await {
        match ev {
            Ok(Event::Apply(obj)) | Ok(Event::InitApply(obj)) => {
                let ns = obj.namespace().unwrap_or_default();
                let name = obj.name_any();
                state
                    .upsert_spec(&ns, &name, Arc::new(obj.spec.clone()))
                    .await;
                if let Some(entry) = project_entry(&obj) {
                    state.upsert(entry).await;
                }
            }
            Ok(Event::Delete(obj)) => {
                let ns = obj.namespace().unwrap_or_default();
                let name = obj.name_any();
                state.delete(&ns, &name).await;
            }
            Ok(Event::Init) => {}
            Ok(Event::InitDone) => {
                if let Some(s) = &synced_signal
                    && s.mark_ready()
                {
                    tracing::info!(
                        target: "rivers::operator::codelocation",
                        "code location reflector synced; webhook readiness flipped"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "code location directory watcher"),
        }
    }
}

/// Project a CR into the gRPC-facing entry shape. Returns `None` before the
/// reconciler has produced a status — we don't want to publish entries whose
/// `resolved_image` / `grpc_endpoint` are still empty. Visible to the
/// admission webhook which calls it on the live-GET fallback path.
pub(crate) fn project_entry(cl: &CodeLocation) -> Option<CodeLocationEntry> {
    let status = cl.status.as_ref()?;
    let namespace = cl.namespace()?;
    Some(CodeLocationEntry {
        namespace,
        name: cl.name_any(),
        grpc_endpoint: status.grpc_endpoint.clone().unwrap_or_default(),
        image: status.resolved_image.clone().unwrap_or_default(),
        module: cl.spec.module.clone(),
        phase: status
            .phase
            .as_ref()
            .map_or("Unknown", |p| p.as_str())
            .to_string(),
        observed_generation: status.observed_generation.unwrap_or_default(),
        identity: cl.spec.identity.clone(),
    })
}

/// tonic `Interceptor` enforcing `authorization: Bearer <token>` on every RPC.
/// Constant-time comparison so a timing attack can't map out the token byte
/// by byte.
#[derive(Clone)]
pub struct BearerAuth {
    expected_header: Arc<[u8]>,
}

impl BearerAuth {
    /// Pre-format `Bearer <token>` once so per-request comparison is a fixed-time
    /// memcmp instead of an allocating concat.
    pub fn new(token: &str) -> Self {
        let expected = format!("Bearer {token}").into_bytes();
        Self {
            expected_header: Arc::from(expected),
        }
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let Some(hdr) = req.metadata().get("authorization") else {
            return Err(Status::unauthenticated("missing authorization"));
        };
        if constant_time_eq(hdr.as_bytes(), &self.expected_header) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid token"))
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Backed by a shared [`DirectoryState`] so the watcher and all gRPC
/// subscribers see the same view.
pub struct DirectoryService {
    state: Arc<DirectoryState>,
}

impl DirectoryService {
    pub fn new(state: Arc<DirectoryState>) -> Self {
        Self { state }
    }
}

type WatchStream = Pin<Box<dyn Stream<Item = Result<CodeLocationEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl CodeLocationRegistryService for DirectoryService {
    type WatchStream = WatchStream;

    async fn list(
        &self,
        req: Request<ListCodeLocationsRequest>,
    ) -> Result<Response<ListCodeLocationsResponse>, Status> {
        let entries = self.state.snapshot(&req.into_inner().namespace).await;
        Ok(Response::new(ListCodeLocationsResponse { entries }))
    }

    async fn watch(
        &self,
        req: Request<WatchCodeLocationsRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let filter = req.into_inner().namespace;

        // Subscribe *before* snapshotting so events that fire during the snapshot
        // aren't dropped; client-side dedup handles the "ADDED then MODIFIED for
        // same generation" case naturally via `observed_generation`.
        let rx = self.state.subscribe();
        let snapshot = self.state.snapshot(&filter).await;

        let seeded: Vec<Result<CodeLocationEvent, Status>> = snapshot
            .into_iter()
            .map(|e| {
                Ok(CodeLocationEvent {
                    r#type: EventType::Added as i32,
                    entry: Some(e),
                })
            })
            .chain(std::iter::once(Ok(CodeLocationEvent {
                r#type: EventType::Synced as i32,
                entry: None,
            })))
            .collect();

        let live = stream::unfold((rx, filter), |(mut rx, filter)| async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        if filter.is_empty()
                            || ev.entry.as_ref().is_some_and(|e| e.namespace == filter)
                        {
                            return Some((Ok(ev), (rx, filter)));
                        }
                    }
                    // Drop the stream: our state is still correct, but this
                    // subscriber missed updates. Returning an error makes the
                    // client reconnect, re-snapshot, and pick up a consistent
                    // view — better than a silent gap.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, "directory watch stream lagged");
                        return Some((
                            Err(Status::data_loss(format!(
                                "watch stream lagged by {n} events; reconnect"
                            ))),
                            (rx, filter),
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });

        let combined = stream::iter(seeded).chain(live);
        Ok(Response::new(Box::pin(combined)))
    }
}

/// Start the tonic server. Caller is responsible for passing a non-empty
/// `token`; we don't re-check here since the caller already did.
pub async fn serve(
    state: Arc<DirectoryState>,
    addr: std::net::SocketAddr,
    token: String,
) -> anyhow::Result<()> {
    let auth = BearerAuth::new(&token);
    let svc =
        CodeLocationRegistryServiceServer::with_interceptor(DirectoryService::new(state), auth);
    tracing::info!(%addr, "code location registry gRPC listening");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_api::code_location_registry_service_client::CodeLocationRegistryServiceClient;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Request;
    use tonic::metadata::MetadataValue;
    use tonic::transport::{Channel, Endpoint};

    fn entry(ns: &str, name: &str, phase: &str) -> CodeLocationEntry {
        CodeLocationEntry {
            namespace: ns.into(),
            name: name.into(),
            grpc_endpoint: format!("{name}.{ns}.svc:3001"),
            image: format!("repo@sha256:{name}"),
            module: "pkg.mod".into(),
            phase: phase.into(),
            observed_generation: 1,
            identity: format!("id-{name}"),
        }
    }

    #[tokio::test]
    async fn upsert_emits_added_then_modified_then_dedups() {
        let state = DirectoryState::new();
        let mut rx = state.subscribe();

        state.upsert(entry("ns", "a", "Deploying")).await;
        state.upsert(entry("ns", "a", "Ready")).await;
        state.upsert(entry("ns", "a", "Ready")).await; // no-op
        // Sentinel: if the no-op leaked an event, we'll see Ready before
        // this one. If it didn't, the sentinel is the third event directly.
        state.upsert(entry("ns", "sentinel", "Ready")).await;

        let first = rx.recv().await.unwrap();
        assert_eq!(first.r#type, EventType::Added as i32);
        assert_eq!(first.entry.as_ref().unwrap().phase, "Deploying");

        let second = rx.recv().await.unwrap();
        assert_eq!(second.r#type, EventType::Modified as i32);
        assert_eq!(second.entry.as_ref().unwrap().phase, "Ready");

        let third = rx.recv().await.unwrap();
        assert_eq!(third.entry.as_ref().unwrap().name, "sentinel");
    }

    #[tokio::test]
    async fn delete_emits_event_and_removes_from_snapshot() {
        let state = DirectoryState::new();
        state.upsert(entry("ns", "a", "Ready")).await;
        state.upsert(entry("ns", "b", "Ready")).await;
        let mut rx = state.subscribe();

        state.delete("ns", "a").await;
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.r#type, EventType::Deleted as i32);
        assert_eq!(ev.entry.unwrap().name, "a");

        let snap = state.snapshot("").await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].name, "b");
    }

    #[tokio::test]
    async fn delete_unknown_is_a_noop() {
        let state = DirectoryState::new();
        let mut rx = state.subscribe();
        state.delete("ns", "missing").await;
        state.upsert(entry("ns", "sentinel", "Ready")).await;

        // If the spurious delete leaked an event, it'd arrive before the
        // sentinel ADDED. If it didn't, the sentinel is first.
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.r#type, EventType::Added as i32);
        assert_eq!(ev.entry.unwrap().name, "sentinel");
    }

    #[tokio::test]
    async fn snapshot_namespace_filter() {
        let state = DirectoryState::new();
        state.upsert(entry("team-a", "x", "Ready")).await;
        state.upsert(entry("team-b", "y", "Ready")).await;

        assert_eq!(state.snapshot("").await.len(), 2);
        let only_a = state.snapshot("team-a").await;
        assert_eq!(only_a.len(), 1);
        assert_eq!(only_a[0].namespace, "team-a");
    }

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    struct ServerHandle {
        addr: SocketAddr,
        _task: tokio::task::JoinHandle<()>,
    }

    async fn spawn_server(state: Arc<DirectoryState>, token: &str) -> ServerHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let auth = BearerAuth::new(token);
        let svc =
            CodeLocationRegistryServiceServer::with_interceptor(DirectoryService::new(state), auth);
        let task = tokio::spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        ServerHandle { addr, _task: task }
    }

    async fn connect(addr: SocketAddr) -> Channel {
        Endpoint::try_from(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    fn with_token<T>(mut req: Request<T>, token: &str) -> Request<T> {
        let val: MetadataValue<_> = format!("Bearer {token}").parse().unwrap();
        req.metadata_mut().insert("authorization", val);
        req
    }

    #[tokio::test]
    async fn list_requires_bearer_token() {
        let state = Arc::new(DirectoryState::new());
        state.upsert(entry("ns", "x", "Ready")).await;
        let srv = spawn_server(state, "s3cret").await;
        let mut client = CodeLocationRegistryServiceClient::new(connect(srv.addr).await);

        let unauth = client
            .list(Request::new(ListCodeLocationsRequest::default()))
            .await;
        assert_eq!(
            unauth.unwrap_err().code(),
            tonic::Code::Unauthenticated,
            "missing token must be rejected"
        );

        let wrong = client
            .list(with_token(
                Request::new(ListCodeLocationsRequest::default()),
                "nope",
            ))
            .await;
        assert_eq!(wrong.unwrap_err().code(), tonic::Code::Unauthenticated);

        let ok = client
            .list(with_token(
                Request::new(ListCodeLocationsRequest::default()),
                "s3cret",
            ))
            .await
            .unwrap();
        let resp = ok.into_inner();
        assert_eq!(resp.entries.len(), 1);
        assert_eq!(resp.entries[0].name, "x");
    }

    #[tokio::test]
    async fn list_filters_by_namespace() {
        let state = Arc::new(DirectoryState::new());
        state.upsert(entry("team-a", "x", "Ready")).await;
        state.upsert(entry("team-b", "y", "Ready")).await;
        let srv = spawn_server(state, "tok").await;
        let mut client = CodeLocationRegistryServiceClient::new(connect(srv.addr).await);

        let req = Request::new(ListCodeLocationsRequest {
            namespace: "team-b".into(),
        });
        let resp = client
            .list(with_token(req, "tok"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.entries.len(), 1);
        assert_eq!(resp.entries[0].namespace, "team-b");
    }

    #[tokio::test]
    async fn watch_emits_snapshot_synced_then_live_events() {
        let state = Arc::new(DirectoryState::new());
        state.upsert(entry("ns", "a", "Ready")).await;
        state.upsert(entry("ns", "b", "Ready")).await;
        let srv = spawn_server(state.clone(), "tok").await;
        let mut client = CodeLocationRegistryServiceClient::new(connect(srv.addr).await);

        let req = Request::new(WatchCodeLocationsRequest::default());
        let mut stream = client
            .watch(with_token(req, "tok"))
            .await
            .unwrap()
            .into_inner();

        // Snapshot: ADDEDs in full, then SYNCED — ordering is the invariant
        // (clients rely on SYNCED meaning "initial set is complete").
        let e0 = stream.message().await.unwrap().unwrap();
        let e1 = stream.message().await.unwrap().unwrap();
        let e2 = stream.message().await.unwrap().unwrap();
        assert_eq!(e0.r#type, EventType::Added as i32);
        assert_eq!(e1.r#type, EventType::Added as i32);
        assert_eq!(e2.r#type, EventType::Synced as i32);
        let mut names = [e0.entry.unwrap().name, e1.entry.unwrap().name];
        names.sort();
        assert_eq!(names, ["a", "b"]);

        // Live updates after SYNCED.
        tokio::spawn({
            let state = state.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                state.upsert(entry("ns", "c", "Ready")).await;
                state.delete("ns", "a").await;
            }
        });

        let ev = stream.message().await.unwrap().unwrap();
        assert_eq!(ev.r#type, EventType::Added as i32);
        assert_eq!(ev.entry.unwrap().name, "c");

        let ev = stream.message().await.unwrap().unwrap();
        assert_eq!(ev.r#type, EventType::Deleted as i32);
        assert_eq!(ev.entry.unwrap().name, "a");
    }

    #[tokio::test]
    async fn watch_filters_live_events_by_namespace() {
        let state = Arc::new(DirectoryState::new());
        let srv = spawn_server(state.clone(), "tok").await;
        let mut client = CodeLocationRegistryServiceClient::new(connect(srv.addr).await);

        let req = Request::new(WatchCodeLocationsRequest {
            namespace: "team-a".into(),
        });
        let mut stream = client
            .watch(with_token(req, "tok"))
            .await
            .unwrap()
            .into_inner();

        // SYNCED fires even on empty snapshot.
        let first = stream.message().await.unwrap().unwrap();
        assert_eq!(first.r#type, EventType::Synced as i32);

        tokio::spawn({
            let state = state.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                state.upsert(entry("team-b", "ignored", "Ready")).await;
                state.upsert(entry("team-a", "kept", "Ready")).await;
            }
        });

        let ev = stream.message().await.unwrap().unwrap();
        assert_eq!(ev.r#type, EventType::Added as i32);
        let got = ev.entry.unwrap();
        assert_eq!(got.namespace, "team-a");
        assert_eq!(got.name, "kept");
    }
}
