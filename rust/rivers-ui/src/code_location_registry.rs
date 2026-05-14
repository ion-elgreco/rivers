//! Client for the operator-hosted `CodeLocationRegistryService` gRPC service.
//! Used by SSR server functions to discover which code locations exist
//! before dialing their per-location gRPC endpoints.
//!
//! Two concrete backends — kept as an enum rather than a trait + dyn dispatch
//! since the set of implementations is closed: either we're talking to a real
//! operator (`Grpc`) or we're in `rivers dev` with a single in-process backend
//! (`Static`).
//!
//! The `Grpc` backend holds an in-memory cache of all known entries fed by a
//! background `Watch` stream — every `list()` / `lookup()` is an O(1) read
//! against the cache, not an RPC. On any stream failure (connection refused,
//! transport drop, or the server's `DataLoss` status — emitted when the
//! broadcast subscriber lags) the watcher clears the cache and reconnects
//! with exponential backoff, so the next snapshot is always coherent.

use anyhow::{Context, Result};
use rivers_api::rivers::code_location_event::Type as EventType;
use rivers_api::rivers::code_location_registry_service_client::CodeLocationRegistryServiceClient;
use rivers_api::rivers::code_location_service_client::CodeLocationServiceClient;
use rivers_api::rivers::{CodeLocationEvent, WatchCodeLocationsRequest};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::types::CodeLocationEntry;

type EntryMap = HashMap<(String, String), CodeLocationEntry>;

/// Attaches `authorization: Bearer <token>` to every outbound RPC.
#[derive(Clone)]
pub struct BearerAuth {
    header: MetadataValue<tonic::metadata::Ascii>,
}

impl BearerAuth {
    fn new(token: &str) -> Result<Self> {
        let header = MetadataValue::try_from(format!("Bearer {token}"))
            .context("registry token contains non-ASCII characters")?;
        Ok(Self { header })
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", self.header.clone());
        Ok(req)
    }
}

/// A code-location directory. Produced at startup; every SSR request reads
/// from it via `AppState::registry`.
#[derive(Clone)]
pub enum Registry {
    /// In-process fixed list. Used by `rivers dev`, where a single synthetic
    /// entry points at the in-process gRPC server.
    Static(Arc<Vec<CodeLocationEntry>>),
    /// Live gRPC connection to the operator. Backed by an in-memory cache fed
    /// by a background `Watch` stream — see [`GrpcRegistry::run_watcher`].
    Grpc(Arc<GrpcRegistry>),
}

impl Registry {
    /// Synthetic single-entry registry for `rivers dev` mode. Stamps a fake
    /// `dev/default` entry pre-marked `Ready` so SSR pages bypass the
    /// operator-watcher path and dial the in-process gRPC backend directly.
    pub fn dev_single(endpoint: String, module: String) -> Self {
        Self::Static(Arc::new(vec![CodeLocationEntry {
            namespace: "dev".to_string(),
            name: "default".to_string(),
            grpc_endpoint: endpoint,
            image: "local".to_string(),
            module,
            phase: "Ready".to_string(),
            observed_generation: 0,
            identity: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        }]))
    }

    /// Empty registry — no code locations. Used by tests and as a temporary
    /// placeholder before the gRPC connection is set up.
    pub fn empty() -> Self {
        Self::Static(Arc::new(Vec::new()))
    }

    /// Build a registry against a remote operator and start its background
    /// watcher. Must be called from inside a tokio runtime — we spawn the
    /// watcher task immediately.
    pub fn grpc(endpoint: String, token: String) -> Result<Self> {
        let g = Arc::new(GrpcRegistry::new(endpoint, token)?);
        tokio::spawn(GrpcRegistry::run_watcher(g.clone()));
        Ok(Self::Grpc(g))
    }

    /// Discriminator for the active backend — `"embedded"` for `Static`,
    /// `"grpc"` for `Grpc`. Used by the deployment page to render the right
    /// Code Locations sub-label without exposing the enum to UI code.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Static(_) => "embedded",
            Self::Grpc(_) => "grpc",
        }
    }

    /// Snapshot of currently-known entries. O(1) for both backends — `Grpc`
    /// reads from the in-memory map maintained by the watcher.
    pub async fn list(&self) -> Result<Vec<CodeLocationEntry>> {
        match self {
            Self::Static(entries) => Ok((**entries).clone()),
            Self::Grpc(g) => Ok(g.snapshot().await),
        }
    }

    /// Lookup an entry by `(namespace, name)`. Returns `None` if not in the
    /// cache — caller decides whether that's a 404 or a "not yet synced".
    pub async fn lookup(&self, namespace: &str, name: &str) -> Option<CodeLocationEntry> {
        match self {
            Self::Static(entries) => entries
                .iter()
                .find(|e| e.namespace == namespace && e.name == name)
                .cloned(),
            Self::Grpc(g) => g.lookup(namespace, name).await,
        }
    }

    /// Pick the first `Ready` entry. Used as the implicit "active" location
    /// by server fns that haven't been wired through with explicit `(ns,name)`.
    pub async fn first_ready(&self) -> Result<CodeLocationEntry> {
        let entries = self.list().await?;
        entries
            .into_iter()
            .find(|e| e.is_ready())
            .context("no Ready code location available")
    }
}

/// Holds the cache + the lazily-dialed channel used by the watcher. The
/// channel is reused across reconnects because tonic's `Channel` is itself a
/// connection pool — its `connect()` only fails fast when the endpoint is
/// genuinely unreachable, not on transient stream drops.
pub struct GrpcRegistry {
    endpoint: Endpoint,
    auth: BearerAuth,
    channel: Mutex<Option<Channel>>,
    entries: Arc<RwLock<EntryMap>>,
}

impl GrpcRegistry {
    /// Build the registry without dialing yet. The first cache miss /
    /// `run_watcher` call lazily establishes the channel.
    pub fn new(endpoint: String, token: String) -> Result<Self> {
        // tonic's Endpoint builder requires a URL scheme; the operator Service
        // speaks plain-HTTP/2 (TLS is terminated at the NetworkPolicy/token
        // boundary in-cluster, consistent with the rest of rivers's gRPC).
        let endpoint = Endpoint::from_shared(endpoint)
            .context("invalid registry endpoint — expected URL like http://host:port")?;
        let auth = BearerAuth::new(&token)?;
        Ok(Self {
            endpoint,
            auth,
            channel: Mutex::new(None),
            entries: Arc::new(RwLock::new(EntryMap::new())),
        })
    }

    /// Snapshot the in-memory entry cache. O(n) clone of the watcher's view.
    pub async fn snapshot(&self) -> Vec<CodeLocationEntry> {
        self.entries.read().await.values().cloned().collect()
    }

    /// O(log n) cache lookup. Returns `None` if the watcher hasn't seen the
    /// entry yet (or it was deleted).
    pub async fn lookup(&self, namespace: &str, name: &str) -> Option<CodeLocationEntry> {
        self.entries
            .read()
            .await
            .get(&(namespace.to_string(), name.to_string()))
            .cloned()
    }

    async fn channel(&self) -> Result<Channel> {
        let mut guard = self.channel.lock().await;
        if let Some(ch) = guard.as_ref() {
            return Ok(ch.clone());
        }
        let ch = self
            .endpoint
            .connect()
            .await
            .context("failed to connect to CodeLocationRegistry")?;
        *guard = Some(ch.clone());
        Ok(ch)
    }

    /// Long-lived background task that keeps the entry cache in sync with
    /// the operator. Loops forever: connect → consume the Watch stream →
    /// on any error, clear the cache and back off → reconnect.
    ///
    /// Clearing on reconnect is load-bearing: the operator resends the full
    /// snapshot at the start of every Watch (seeded ADDED events followed by
    /// a SYNCED marker), so anything we'd kept from the prior session could
    /// race with the new snapshot if it had been deleted server-side while
    /// we were disconnected.
    async fn run_watcher(self_: Arc<Self>) {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
        const MAX_BACKOFF: Duration = Duration::from_secs(60);
        let mut backoff = INITIAL_BACKOFF;

        loop {
            match self_.watch_once().await {
                Ok(()) => {
                    tracing::info!(target: "rivers::ui", "registry watch stream closed; reconnecting");
                    backoff = INITIAL_BACKOFF;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "rivers::ui",
                        error = format!("{e:#}"),
                        backoff_ms = backoff.as_millis() as u64,
                        "registry watch failed; reconnecting after backoff"
                    );
                    // Drop the cached channel so the next attempt redials —
                    // some transport-level failures stick to the channel.
                    *self_.channel.lock().await = None;
                }
            }
            // Reset cache before sleeping — readers should see an empty
            // directory rather than a stale one during the disconnect.
            let mut cache = self_.entries.write().await;
            if !cache.is_empty() {
                cache.clear();
            }
            drop(cache);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Drive one Watch stream to completion. Returns `Ok(())` on a clean
    /// server-side close, `Err` on any transport or status failure.
    async fn watch_once(&self) -> Result<()> {
        let channel = self.channel().await?;
        let mut client =
            CodeLocationRegistryServiceClient::with_interceptor(channel, self.auth.clone());

        let mut stream = client
            .watch(WatchCodeLocationsRequest {
                namespace: String::new(),
            })
            .await
            .context("CodeLocationRegistry.Watch call failed")?
            .into_inner();

        // Stage events until SYNCED, then atomically swap the staged map into
        // place. This guarantees readers never observe a half-built initial
        // snapshot.
        let mut staging: EntryMap = EntryMap::new();
        let mut synced = false;

        loop {
            match stream.message().await {
                Ok(Some(ev)) => {
                    apply_event(&self.entries, &mut staging, &mut synced, ev).await;
                }
                Ok(None) => return Ok(()),
                Err(status) => {
                    return Err(anyhow::anyhow!(
                        "registry watch stream error: code={:?} msg={}",
                        status.code(),
                        status.message()
                    ));
                }
            }
        }
    }
}

async fn apply_event(
    entries: &RwLock<EntryMap>,
    staging: &mut EntryMap,
    synced: &mut bool,
    ev: CodeLocationEvent,
) {
    let ty = EventType::try_from(ev.r#type).unwrap_or(EventType::Unspecified);
    if matches!(ty, EventType::Synced) {
        *entries.write().await = std::mem::take(staging);
        *synced = true;
        return;
    }
    let Some(entry) = ev.entry else { return };
    let entry: CodeLocationEntry = entry.into();
    let key = (entry.namespace.clone(), entry.name.clone());
    let mutate = |map: &mut EntryMap| match ty {
        EventType::Added | EventType::Modified => {
            map.insert(key, entry);
        }
        EventType::Deleted => {
            map.remove(&key);
        }
        // SYNCED returned early above; Unspecified / unknown variants are dropped.
        EventType::Synced | EventType::Unspecified => {}
    };
    if *synced {
        mutate(&mut *entries.write().await);
    } else {
        mutate(staging);
    }
}

/// Connect a per-location `CodeLocationServiceClient`. The registry hands back a
/// `host:port` endpoint; tonic's `Channel::from_shared` requires a URL with
/// scheme, so we prepend `http://` for bare `host:port` strings.
pub async fn connect_code_location(endpoint: &str) -> Result<CodeLocationServiceClient<Channel>> {
    let url = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let channel = Endpoint::from_shared(url)
        .context("invalid code-location endpoint")?
        .connect()
        .await
        .context("failed to connect to code location")?;
    Ok(CodeLocationServiceClient::new(channel))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn static_registry_lists_entries_verbatim() {
        let r = Registry::Static(Arc::new(vec![
            entry("a", "one", "Ready"),
            entry("b", "two", "Pending"),
        ]));
        let got = r.list().await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.iter().any(|e| e.name == "one"));
        assert!(got.iter().any(|e| e.phase == "Pending"));
    }

    #[tokio::test]
    async fn first_ready_skips_non_ready_entries() {
        let r = Registry::Static(Arc::new(vec![
            entry("a", "pending", "Pending"),
            entry("b", "failed", "Failed"),
            entry("c", "ok", "Ready"),
        ]));
        let pick = r.first_ready().await.unwrap();
        assert_eq!(pick.name, "ok");
    }

    #[tokio::test]
    async fn first_ready_errors_when_empty() {
        let r = Registry::empty();
        let err = r.first_ready().await.unwrap_err();
        assert!(err.to_string().contains("no Ready"));
    }

    #[tokio::test]
    async fn first_ready_errors_when_all_non_ready() {
        let r = Registry::Static(Arc::new(vec![entry("a", "x", "Pending")]));
        assert!(r.first_ready().await.is_err());
    }

    #[tokio::test]
    async fn dev_single_marks_entry_ready() {
        let r = Registry::dev_single("http://127.0.0.1:3001".into(), "my.mod".into());
        let got = r.list().await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0].is_ready());
        assert_eq!(got[0].module, "my.mod");
    }

    #[tokio::test]
    async fn static_lookup_finds_by_key() {
        let r = Registry::Static(Arc::new(vec![
            entry("a", "one", "Ready"),
            entry("b", "two", "Pending"),
        ]));
        assert_eq!(r.lookup("a", "one").await.map(|e| e.name).unwrap(), "one");
        assert_eq!(
            r.lookup("b", "two").await.map(|e| e.phase).unwrap(),
            "Pending"
        );
        assert!(r.lookup("a", "two").await.is_none());
    }

    #[test]
    fn bearer_auth_builds_header() {
        let a = BearerAuth::new("hunter2").unwrap();
        // MetadataValue's Display impl reveals the raw token — only safe here
        // because this is a test fixture.
        assert_eq!(a.header.to_str().unwrap(), "Bearer hunter2");
    }

    #[test]
    fn bearer_auth_rejects_control_chars() {
        // A header value with a CR is invalid per HTTP/2 grammar; tonic's
        // Ascii metadata parser rejects it. Guard so if someone passes a
        // malformed token we surface a startup error instead of silently
        // sending a broken header.
        assert!(BearerAuth::new("bad\rtoken").is_err());
    }

    #[tokio::test]
    async fn apply_event_stages_until_synced_then_swaps() {
        let cache = RwLock::new(EntryMap::new());
        let mut staging = EntryMap::new();
        let mut synced = false;

        // Pre-Synced ADDED writes only to staging, not the live cache.
        let ev = CodeLocationEvent {
            r#type: EventType::Added as i32,
            entry: Some(rivers_api::rivers::CodeLocationEntry {
                namespace: "n".into(),
                name: "a".into(),
                grpc_endpoint: "a.n.svc:3001".into(),
                image: "img".into(),
                module: "m".into(),
                phase: "Ready".into(),
                observed_generation: 1,
                identity: "id-a".into(),
            }),
        };
        apply_event(&cache, &mut staging, &mut synced, ev).await;
        assert_eq!(staging.len(), 1);
        assert!(cache.read().await.is_empty());

        // SYNCED promotes staging to the live cache.
        let synced_ev = CodeLocationEvent {
            r#type: EventType::Synced as i32,
            entry: None,
        };
        apply_event(&cache, &mut staging, &mut synced, synced_ev).await;
        assert!(synced);
        assert_eq!(cache.read().await.len(), 1);
        assert!(staging.is_empty());

        // Post-Synced DELETED writes to the live cache directly.
        let deleted = CodeLocationEvent {
            r#type: EventType::Deleted as i32,
            entry: Some(rivers_api::rivers::CodeLocationEntry {
                namespace: "n".into(),
                name: "a".into(),
                grpc_endpoint: String::new(),
                image: String::new(),
                module: String::new(),
                phase: String::new(),
                observed_generation: 0,
                identity: String::new(),
            }),
        };
        apply_event(&cache, &mut staging, &mut synced, deleted).await;
        assert!(cache.read().await.is_empty());
    }

    // --- In-process end-to-end tests against a fake CodeLocationRegistryService ---

    use futures_util::stream::Stream;
    use rivers_api::rivers::code_location_registry_service_server::{
        CodeLocationRegistryService as RegistryTrait, CodeLocationRegistryServiceServer,
    };
    use rivers_api::rivers::{
        CodeLocationEntry as ProtoEntry, ListCodeLocationsRequest, ListCodeLocationsResponse,
    };
    use std::pin::Pin;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    /// Fake server that records the auth header it last received and emits
    /// Slot owning the watch-stream receiver — set by the test, taken by
    /// the server's first `watch` call.
    type WatchRxSlot = Arc<AsyncMutex<Option<mpsc::Receiver<Result<CodeLocationEvent, Status>>>>>;

    /// Watch events from a per-test mpsc channel. `Clone`-able so the test
    /// retains a handle while a clone lives inside the tonic server.
    #[derive(Clone)]
    struct FakeRegistry {
        expected_token: String,
        last_seen_auth: Arc<AsyncMutex<Option<String>>>,
        watch_tx: WatchRxSlot,
    }

    #[tonic::async_trait]
    impl RegistryTrait for FakeRegistry {
        type WatchStream =
            Pin<Box<dyn Stream<Item = Result<CodeLocationEvent, Status>> + Send + 'static>>;

        async fn list(
            &self,
            req: Request<ListCodeLocationsRequest>,
        ) -> Result<Response<ListCodeLocationsResponse>, Status> {
            let got = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            *self.last_seen_auth.lock().await = got.clone();
            let expected = format!("Bearer {}", self.expected_token);
            if got.as_deref() != Some(expected.as_str()) {
                return Err(Status::unauthenticated("bad token"));
            }
            Ok(Response::new(ListCodeLocationsResponse {
                entries: Vec::new(),
            }))
        }

        async fn watch(
            &self,
            req: Request<WatchCodeLocationsRequest>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            let got = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            *self.last_seen_auth.lock().await = got.clone();
            let expected = format!("Bearer {}", self.expected_token);
            if got.as_deref() != Some(expected.as_str()) {
                return Err(Status::unauthenticated("bad token"));
            }
            let rx = self
                .watch_tx
                .lock()
                .await
                .take()
                .ok_or_else(|| Status::internal("watch already consumed"))?;
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            Ok(Response::new(Box::pin(stream) as Self::WatchStream))
        }
    }

    async fn spawn_fake(
        token: &str,
    ) -> (
        String,
        FakeRegistry,
        mpsc::Sender<Result<CodeLocationEvent, Status>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel(16);
        let fake = FakeRegistry {
            expected_token: token.to_string(),
            last_seen_auth: Arc::new(AsyncMutex::new(None)),
            watch_tx: Arc::new(AsyncMutex::new(Some(rx))),
        };
        let svc = CodeLocationRegistryServiceServer::new(fake.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (format!("http://{addr}"), fake, tx)
    }

    fn proto(ns: &str, name: &str, phase: &str) -> ProtoEntry {
        ProtoEntry {
            namespace: ns.into(),
            name: name.into(),
            grpc_endpoint: format!("{name}.{ns}.svc:3001"),
            image: format!("img@sha256:{name}"),
            module: "m".into(),
            phase: phase.into(),
            observed_generation: 7,
            identity: format!("id-{name}"),
        }
    }

    /// Helper: drive a watcher, push initial snapshot + SYNCED, wait until the
    /// cache is populated.
    async fn wait_for_size(reg: &Registry, target: usize) {
        for _ in 0..200 {
            if reg.list().await.unwrap().len() == target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "registry never reached size {target}, last: {}",
            reg.list().await.unwrap().len()
        );
    }

    #[tokio::test]
    async fn watcher_caches_initial_snapshot_after_synced() {
        let (url, fake, tx) = spawn_fake("s3cret").await;
        let reg = Registry::grpc(url, "s3cret".into()).unwrap();

        // Seed two ADDED events + SYNCED, exactly mimicking the operator.
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Added as i32,
            entry: Some(proto("team", "svc", "Ready")),
        }))
        .await
        .unwrap();
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Added as i32,
            entry: Some(proto("team", "other", "Pending")),
        }))
        .await
        .unwrap();
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Synced as i32,
            entry: None,
        }))
        .await
        .unwrap();

        wait_for_size(&reg, 2).await;
        let entries = reg.list().await.unwrap();
        assert_eq!(entries.len(), 2);

        // Bearer was attached even on the streaming RPC.
        let auth = fake.last_seen_auth.lock().await.clone();
        assert_eq!(auth.as_deref(), Some("Bearer s3cret"));

        // Lookup hits the cache.
        let hit = reg.lookup("team", "svc").await.unwrap();
        assert_eq!(hit.observed_generation, 7);

        // first_ready picks the only Ready entry.
        let pick = reg.first_ready().await.unwrap();
        assert_eq!(pick.name, "svc");
    }

    #[tokio::test]
    async fn watcher_applies_modified_and_deleted_post_synced() {
        let (url, _fake, tx) = spawn_fake("t").await;
        let reg = Registry::grpc(url, "t".into()).unwrap();

        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Added as i32,
            entry: Some(proto("ns", "a", "Pending")),
        }))
        .await
        .unwrap();
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Synced as i32,
            entry: None,
        }))
        .await
        .unwrap();
        wait_for_size(&reg, 1).await;

        // Modified flips the phase.
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Modified as i32,
            entry: Some(proto("ns", "a", "Ready")),
        }))
        .await
        .unwrap();
        for _ in 0..50 {
            if reg.lookup("ns", "a").await.map(|e| e.phase) == Some("Ready".into()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            reg.lookup("ns", "a").await.map(|e| e.phase).unwrap(),
            "Ready"
        );

        // Deleted removes it.
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Deleted as i32,
            entry: Some(proto("ns", "a", "Ready")),
        }))
        .await
        .unwrap();
        wait_for_size(&reg, 0).await;
    }

    #[tokio::test]
    async fn watcher_clears_cache_on_data_loss_and_reconnects() {
        // Single connection: send a snapshot, then an error. The watcher is
        // expected to clear the cache. We don't push a second snapshot here
        // (the FakeRegistry only honours one Watch); that's fine — the test
        // is asserting clearing-on-error specifically.
        let (url, _fake, tx) = spawn_fake("t").await;
        let reg = Registry::grpc(url, "t".into()).unwrap();

        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Added as i32,
            entry: Some(proto("ns", "a", "Ready")),
        }))
        .await
        .unwrap();
        tx.send(Ok(CodeLocationEvent {
            r#type: EventType::Synced as i32,
            entry: None,
        }))
        .await
        .unwrap();
        wait_for_size(&reg, 1).await;

        // Server-side data_loss → watcher should clear cache.
        tx.send(Err(Status::data_loss("lagged"))).await.unwrap();
        wait_for_size(&reg, 0).await;
    }

    #[tokio::test]
    async fn watcher_with_wrong_token_keeps_cache_empty() {
        let (url, _fake, _tx) = spawn_fake("right").await;
        let reg = Registry::grpc(url, "wrong".into()).unwrap();
        // Watcher should fail Watch, log+backoff, never populate the cache.
        // Wait a beat so the watcher has had time to run.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(reg.list().await.unwrap().len(), 0);
        assert!(reg.first_ready().await.is_err());
    }
}
