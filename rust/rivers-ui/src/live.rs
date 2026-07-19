//! Server-side plumbing for live updates: one SurrealDB LIVE query per
//! table, fanned out into a single broadcast channel tagged by channel
//! name, exposed as a single `/api/events?channels=…` SSE endpoint.
//!
//! - A **channel** is a named group of tables whose notifications are
//!   treated as equivalent triggers by the UI (see [`LIVE_CHANNELS`]).
//! - One background task per `(channel, table)` pair holds the LIVE query
//!   open, reconnecting with exponential backoff (250 ms → 30 s).
//! - All tasks share one [`broadcast::Sender<&'static str>`]; the SSE
//!   handler subscribes, filters by client-requested channels, and emits
//!   `event: {channel}-changed\ndata: 1` events.
//!
//! **Python-write guarantee.** All backend writes flow through the same
//! `Arc<SurrealStorage>` (Python daemon via PyO3 in `rivers dev`, or a
//! shared remote SurrealDB in K8s), so the live queries here see every
//! mutation regardless of which process wrote it.

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// One UI-facing channel plus the set of tables whose notifications feed it.
pub struct LiveChannel {
    pub name: &'static str,
    /// Pre-computed `"{name}-changed"` SSE event name. Stored rather than
    /// `format!`-ed in the SSE handler so per-event emission to each client
    /// allocates zero Strings on the hot path.
    pub event_name: &'static str,
    pub tables: &'static [&'static str],
}

/// Every channel a page can subscribe to. The `name` is what clients pass
/// in `?channels=…`; `event_name` is `"{name}-changed"` pre-computed once;
/// `tables` are the SurrealDB tables whose LIVE queries feed this channel.
pub const LIVE_CHANNELS: &[LiveChannel] = &[
    LiveChannel {
        name: "runs",
        event_name: "runs-changed",
        tables: &["runs"],
    },
    LiveChannel {
        name: "assets",
        event_name: "assets-changed",
        tables: &["assets", "asset_partitions"],
    },
    LiveChannel {
        name: "events",
        event_name: "events-changed",
        tables: &["events", "run_logs"],
    },
    LiveChannel {
        name: "backfills",
        event_name: "backfills-changed",
        tables: &["backfills"],
    },
    LiveChannel {
        name: "automation",
        event_name: "automation-changed",
        tables: &["ticks", "condition_ticks", "condition_evals"],
    },
    LiveChannel {
        name: "pools",
        event_name: "pools-changed",
        tables: &["concurrency_pools", "concurrency_slots", "pending_steps"],
    },
    // Subscribers on this channel care about which assets light up on the
    // DAG (materialization / staleness changes), not the topology itself —
    // topology comes from the code-location gRPC and is static per session.
    LiveChannel {
        name: "lineage",
        event_name: "lineage-changed",
        tables: &["assets", "asset_partitions"],
    },
];

/// Per-channel counters maintained by the broadcaster tasks. Values are
/// aggregated across all `(channel, table)` tasks that share a channel
/// name — a reconnect on any underlying table increments the channel's
/// count; a notification on any of them updates the timestamp.
#[derive(Default)]
pub struct ChannelMetrics {
    /// Number of times a live query was reopened after the first
    /// successful subscribe. A steadily-rising counter means the
    /// broadcaster is silently reconnecting — the 5-min safety-net poll
    /// hides this at the UI layer.
    pub reconnects: AtomicU64,
    /// Unix-millisecond timestamp of a recent notification on any table
    /// feeding this channel. `0` means no event seen yet. Writes use
    /// `Relaxed` ordering — under concurrent writes from sibling-table
    /// tasks the stored value is approximate, which is fine for operator
    /// observability.
    pub last_event_unix_ms: AtomicU64,
}

/// Handle on the broadcaster's per-channel counters. Cheap to clone.
#[derive(Clone)]
pub struct LiveMetrics {
    channels: Arc<HashMap<&'static str, ChannelMetrics>>,
}

impl LiveMetrics {
    fn new() -> Self {
        let mut m = HashMap::with_capacity(LIVE_CHANNELS.len());
        for ch in LIVE_CHANNELS {
            m.insert(ch.name, ChannelMetrics::default());
        }
        Self {
            channels: Arc::new(m),
        }
    }

    /// Serializable point-in-time view of every channel's counters, in
    /// [`LIVE_CHANNELS`] order.
    pub fn snapshot(&self) -> Vec<ChannelSnapshot> {
        LIVE_CHANNELS
            .iter()
            .map(|ch| {
                let m = &self.channels[ch.name];
                ChannelSnapshot {
                    channel: ch.name,
                    reconnects: m.reconnects.load(Ordering::Relaxed),
                    last_event_unix_ms: m.last_event_unix_ms.load(Ordering::Relaxed),
                }
            })
            .collect()
    }
}

/// Per-channel observability snapshot returned by [`debug_live`]. Lets
/// operators distinguish a healthy quiet channel (recent `last_event_unix_ms`,
/// low `reconnects`) from a silently reconnecting one (high `reconnects`,
/// stale `last_event_unix_ms`).
#[derive(Serialize)]
pub struct ChannelSnapshot {
    pub channel: &'static str,
    pub reconnects: u64,
    /// `0` when no event has been observed since process start.
    pub last_event_unix_ms: u64,
}

/// Spawn one background task per `(channel, table)` pair. Every task holds
/// a LIVE query open for the process lifetime, reconnects on DB hiccups,
/// and forwards each notification as a broadcast tick tagged with the
/// channel name. Returns the shared sender plus a [`LiveMetrics`] handle
/// the diagnostic endpoint reads from.
pub fn spawn_live_broadcasters(
    storage: Arc<SurrealStorage>,
    shutdown: CancellationToken,
) -> (broadcast::Sender<&'static str>, LiveMetrics) {
    let (tx, _rx) = broadcast::channel::<&'static str>(256);
    let metrics = LiveMetrics::new();
    for channel in LIVE_CHANNELS {
        for table in channel.tables {
            spawn_one(
                storage.clone(),
                shutdown.clone(),
                tx.clone(),
                channel.name,
                table,
                metrics.clone(),
            );
        }
    }
    (tx, metrics)
}

fn spawn_one(
    storage: Arc<SurrealStorage>,
    shutdown: CancellationToken,
    tx: broadcast::Sender<&'static str>,
    channel_name: &'static str,
    table: &'static str,
    metrics: LiveMetrics,
) {
    tokio::spawn(async move {
        channel_loop(
            move || {
                let storage = storage.clone();
                async move { storage.subscribe_table(table).await }
            },
            shutdown,
            tx,
            channel_name,
            Some(table),
            metrics,
        )
        .await;
    });
}

/// Extracted broadcaster policy: repeatedly calls `subscribe` to obtain a
/// notification stream, forwards each `()` yield as a broadcast tick tagged
/// with `channel_name`, reconnects with exponential backoff on subscribe
/// failure or stream end, and emits one synthetic tick on every reconnect
/// after the first so clients catch up on anything missed during the gap.
///
/// Decoupled from [`SurrealStorage`] (subscribe is a closure) so tests can
/// drive it with a deterministic mock stream.
///
/// - `table`: optional diagnostic tag for tracing; `None` is used by tests
///   that don't care about the attribute.
async fn channel_loop<F, Fut, S>(
    subscribe: F,
    shutdown: CancellationToken,
    tx: broadcast::Sender<&'static str>,
    channel_name: &'static str,
    table: Option<&'static str>,
    metrics: LiveMetrics,
) where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<S>>,
    S: futures_core::Stream<Item = ()> + Unpin,
{
    use futures_util::StreamExt;

    const INITIAL_BACKOFF_MS: u64 = 250;
    const MAX_BACKOFF_MS: u64 = 30_000;
    // Minimum healthy uptime before a successful subscribe is allowed to
    // reset the backoff. Without this, a stream that opens and ends within
    // the same millisecond (session expiry, flapping connection) would loop
    // at the initial 250 ms delay — a thundering herd × 11 broadcaster tasks.
    const MIN_HEALTHY_MS: u128 = 5_000;

    let channel_metrics = metrics
        .channels
        .get(channel_name)
        .expect("every LIVE_CHANNELS entry is registered in LiveMetrics");
    let mut backoff_ms = INITIAL_BACKOFF_MS;
    let mut first_attempt = true;

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        tracing::info!(
            target: "rivers::ui",
            channel = channel_name,
            table = table.unwrap_or("<mock>"),
            "live broadcaster: opening live query"
        );
        let mut stream = match subscribe().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    target: "rivers::ui",
                    channel = channel_name,
                    table = table.unwrap_or("<mock>"),
                    error = format!("{e:#}"),
                    backoff_ms,
                    "live broadcaster: subscribe failed, retrying after backoff"
                );
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
                    _ = shutdown.cancelled() => break,
                }
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                continue;
            }
        };
        // After a reconnect, nudge clients to refetch — they may have
        // missed notifications during the gap. Harmless on the very
        // first connect (no subscribers yet).
        if !first_attempt {
            channel_metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(channel_name);
        }
        first_attempt = false;
        let opened_at = std::time::Instant::now();

        while let Some(()) = stream.next().await {
            if shutdown.is_cancelled() {
                break;
            }
            channel_metrics
                .last_event_unix_ms
                .store(now_unix_ms(), Ordering::Relaxed);
            let _ = tx.send(channel_name);
        }
        if shutdown.is_cancelled() {
            break;
        }
        if opened_at.elapsed().as_millis() >= MIN_HEALTHY_MS {
            backoff_ms = INITIAL_BACKOFF_MS;
        } else {
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        }
        tracing::warn!(
            target: "rivers::ui",
            channel = channel_name,
            table = table.unwrap_or("<mock>"),
            backoff_ms,
            "live query stream ended, reconnecting"
        );
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
            _ = shutdown.cancelled() => break,
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Query string for `/api/events` — single comma-separated `channels=`
/// param. Unknown channel names are dropped during parsing.
#[derive(Deserialize)]
pub(crate) struct EventsQuery {
    #[serde(default)]
    channels: String,
}

/// JSON diagnostic endpoint for live-broadcaster observability. Returns the
/// per-channel reconnect count and last-event timestamp so operators can
/// detect silently reconnecting live queries — the 5-min safety-net poll
/// hides this at the UI layer.
pub(crate) async fn debug_live(metrics: LiveMetrics) -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "now_unix_ms": now_unix_ms(),
        "channels": metrics.snapshot(),
    }))
}

/// Parse a comma-separated `?channels=…` query-string value against
/// [`LIVE_CHANNELS`]. Unknown names are dropped. Returns the surviving
/// `(channel_name, event_name)` pairs so the hot path neither re-parses
/// nor re-looks-up per event.
fn resolve_wanted(channels_param: &str) -> Vec<(&'static str, &'static str)> {
    channels_param
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|name| {
            LIVE_CHANNELS
                .iter()
                .find(|c| c.name == name)
                .map(|c| (c.name, c.event_name))
        })
        .collect()
}

/// Core stream that yields pre-computed SSE event names for each broadcast
/// tick the subscriber cares about. Split out of [`events_sse`] so tests
/// can drive it directly without spinning up an HTTP server or an SSE
/// `Event` decoder.
///
/// - Broadcast messages whose channel name isn't in `wanted` are dropped.
/// - `RecvError::Lagged` emits every wanted event name once (so each
///   client listener fires and refetches) and then continues.
/// - `RecvError::Closed` ends the stream.
/// - `shutdown` cancellation ends the stream immediately (load-bearing:
///   Axum's `with_graceful_shutdown` waits for open responses to finish,
///   and an SSE body never finishes on its own — without this, `rivers dev`
///   hangs at the shutdown barrier until every browser tab disconnects).
/// - If `wanted` is empty, the stream ends on the first Lagged and
///   otherwise only ever drops ticks — useful for placeholder pages that
///   subscribe to nothing.
fn event_stream(
    rx: broadcast::Receiver<&'static str>,
    wanted: Vec<(&'static str, &'static str)>,
    shutdown: CancellationToken,
) -> impl futures_core::Stream<Item = &'static str> {
    use tokio::sync::broadcast::error::RecvError;
    // `pending` holds pre-computed event names ready to emit. On `Lagged` we
    // fill it with every wanted event name so each client listener fires
    // once; on a normal match we queue just the matched one.
    futures_util::stream::unfold(
        (rx, wanted, Vec::<&'static str>::new(), shutdown),
        |(mut rx, wanted, mut pending, shutdown)| async move {
            loop {
                if shutdown.is_cancelled() {
                    return None;
                }
                if let Some(event_name) = pending.pop() {
                    return Some((event_name, (rx, wanted, pending, shutdown)));
                }
                let recv = tokio::select! {
                    result = rx.recv() => result,
                    _ = shutdown.cancelled() => return None,
                };
                match recv {
                    Ok(channel) => {
                        if let Some((_, event_name)) = wanted.iter().find(|(n, _)| *n == channel) {
                            pending.push(*event_name);
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        pending.extend(wanted.iter().map(|(_, e)| *e));
                        if pending.is_empty() {
                            return None;
                        }
                    }
                    Err(RecvError::Closed) => return None,
                }
            }
        },
    )
}

/// SSE handler: filters broadcast ticks to the client-requested channels
/// and emits one `{channel}-changed` event per tick. Clients declare
/// interest via `?channels=runs,assets,…`; unknown names are dropped.
///
/// `data: 1` is load-bearing — per the SSE spec, events with an empty
/// `data:` field are silently discarded by `EventSource`, even though
/// they appear on the wire to `curl`.
pub(crate) async fn events_sse(
    tx: broadcast::Sender<&'static str>,
    shutdown: CancellationToken,
    query: Query<EventsQuery>,
) -> impl IntoResponse {
    use futures_util::StreamExt;
    let wanted = resolve_wanted(&query.channels);
    let rx = tx.subscribe();
    let stream = event_stream(rx, wanted, shutdown).map(|event_name| {
        Ok::<_, std::convert::Infallible>(Event::default().event(event_name).data("1"))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Collect up to `n` events with a per-event timeout. Returns what was
    /// received so far on timeout — callers assert on the collected set.
    async fn collect_n<S: futures_core::Stream<Item = &'static str> + Unpin>(
        stream: &mut S,
        n: usize,
        per_event: Duration,
    ) -> Vec<&'static str> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match timeout(per_event, stream.next()).await {
                Ok(Some(name)) => out.push(name),
                _ => break,
            }
        }
        out
    }

    #[test]
    fn resolve_wanted_drops_unknown_and_preserves_known() {
        let w = resolve_wanted("runs,unknown,lineage,,assets");
        let names: Vec<&str> = w.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec!["runs", "lineage", "assets"]);
        // Event names are pre-computed and match the `{name}-changed` contract.
        let events: Vec<&str> = w.iter().map(|(_, e)| *e).collect();
        assert_eq!(
            events,
            vec!["runs-changed", "lineage-changed", "assets-changed"]
        );
    }

    #[test]
    fn resolve_wanted_empty_param_returns_empty() {
        assert!(resolve_wanted("").is_empty());
    }

    /// End-to-end through the real `event_stream`: broadcast ticks the
    /// client cares about arrive; ticks for other channels are filtered out;
    /// the event name is the pre-computed `{channel}-changed` string, not
    /// the raw channel name.
    #[tokio::test]
    async fn event_stream_filters_and_maps_names() {
        let (tx, _) = broadcast::channel::<&'static str>(16);
        let rx = tx.subscribe();
        let wanted = resolve_wanted("runs,lineage");
        let mut stream = Box::pin(event_stream(rx, wanted, CancellationToken::new()));

        // "assets" isn't in `wanted`; "runs" and "lineage" are.
        tx.send("assets").unwrap();
        tx.send("runs").unwrap();
        tx.send("lineage").unwrap();
        tx.send("pools").unwrap();

        let got = collect_n(&mut stream, 2, Duration::from_millis(500)).await;
        assert_eq!(got, vec!["runs-changed", "lineage-changed"]);
    }

    /// When the client falls behind, `broadcast::Receiver::recv` returns
    /// `Lagged`. The stream must emit every wanted event name exactly once
    /// so every per-channel listener on the browser side fires and refetches.
    #[tokio::test]
    async fn event_stream_lagged_emits_all_wanted_once() {
        let (tx, _) = broadcast::channel::<&'static str>(2);
        let rx = tx.subscribe();
        let wanted = resolve_wanted("runs,assets,lineage");
        let mut stream = Box::pin(event_stream(rx, wanted, CancellationToken::new()));

        // Overflow the 2-slot buffer before the stream polls — this forces
        // the next recv() to surface `Lagged`.
        for _ in 0..10 {
            tx.send("runs").unwrap();
        }

        let got = collect_n(&mut stream, 4, Duration::from_millis(500)).await;
        // Order within the pending drain isn't contractual (Vec::pop is LIFO),
        // so compare as a set.
        let set: std::collections::HashSet<_> = got.into_iter().collect();
        assert!(set.contains("runs-changed"));
        assert!(set.contains("assets-changed"));
        assert!(set.contains("lineage-changed"));
        assert_eq!(set.len(), 3);
    }

    /// A subscriber that requested no channels should silently drop every
    /// tick (and close on Lagged / sender drop) rather than spin-yielding.
    #[tokio::test]
    async fn event_stream_empty_wanted_never_yields() {
        let (tx, _) = broadcast::channel::<&'static str>(16);
        let rx = tx.subscribe();
        let mut stream = Box::pin(event_stream(rx, Vec::new(), CancellationToken::new()));

        tx.send("runs").unwrap();
        tx.send("assets").unwrap();

        // Give the stream ample time — it must not produce anything.
        let got = timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            got.is_err(),
            "empty-subscription stream yielded an unexpected event: {got:?}"
        );
    }

    /// Closing the broadcast sender ends the stream with `None` — the
    /// client-side EventSource will see a clean disconnect.
    #[tokio::test]
    async fn event_stream_ends_when_sender_closed() {
        let (tx, _) = broadcast::channel::<&'static str>(8);
        let rx = tx.subscribe();
        let wanted = resolve_wanted("runs");
        let mut stream = Box::pin(event_stream(rx, wanted, CancellationToken::new()));

        drop(tx);

        let got = timeout(Duration::from_millis(500), stream.next()).await;
        assert_eq!(
            got.expect("stream should terminate, not hang"),
            None,
            "stream must yield None once the sender is closed"
        );
    }

    /// Regression: cancelling the shutdown token must end the SSE stream
    /// even when the broadcast sender is still alive. Without this, Axum's
    /// `with_graceful_shutdown` hangs forever on a single connected tab
    /// (broadcasters exit on shutdown but the Router still holds a clone
    /// of `tx`, so `rx.recv()` never errors).
    #[tokio::test]
    async fn event_stream_ends_on_shutdown() {
        let (tx, _rx_keep) = broadcast::channel::<&'static str>(16);
        let rx = tx.subscribe();
        let wanted = resolve_wanted("runs");
        let shutdown = CancellationToken::new();
        let mut stream = Box::pin(event_stream(rx, wanted, shutdown.clone()));

        // Sender is still alive; the only thing that should end the
        // stream is cancellation.
        shutdown.cancel();

        let got = timeout(Duration::from_millis(500), stream.next()).await;
        assert_eq!(
            got.expect("stream should terminate, not hang on shutdown"),
            None,
            "stream must yield None once shutdown is cancelled, even with a live sender"
        );
    }

    /// Regression for `rivers dev` shutdown hang: with an SSE client
    /// connected, `axum::serve(...).with_graceful_shutdown(...)` must
    /// complete within a bounded window after the shutdown token fires.
    /// Before the SSE-shutdown fix, the server would wait indefinitely
    /// for the open SSE body to finish (it never does on its own).
    #[tokio::test(flavor = "multi_thread")]
    async fn axum_graceful_shutdown_completes_with_open_sse_client() {
        use axum::Router;
        use axum::routing::get;
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};

        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let (tx, _metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());

        let app = Router::new().route(
            "/api/events",
            get({
                let tx = tx.clone();
                let sse_shutdown = shutdown.clone();
                move |query: axum::extract::Query<EventsQuery>| {
                    let tx = tx.clone();
                    let shutdown = sse_shutdown.clone();
                    async move { events_sse(tx, shutdown, query).await }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // The production pattern — graceful shutdown keyed on our token.
        let serve_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(serve_shutdown.cancelled_owned())
                .await
                .unwrap();
        });

        // Let LIVE queries register, then connect an SSE client and
        // deliberately leave it hanging (don't read or close it from the
        // client side — this is the "browser tab still open" scenario).
        tokio::time::sleep(Duration::from_millis(400)).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"GET /api/events?channels=runs HTTP/1.1\r\n\
                  Host: t\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Fire shutdown. The server task must complete — if it hangs,
        // this is the regression.
        shutdown.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(3), server).await;
        outcome
            .expect(
                "axum::serve with_graceful_shutdown hung while an SSE client was connected — \
             events_sse is not observing the shutdown token",
            )
            .expect("server task panicked");

        drop(client);
    }

    /// Every channel declared in [`LIVE_CHANNELS`] is registered in
    /// [`LiveMetrics`] so `spawn_one`'s lookup (which `expect`s the entry
    /// to exist) cannot panic in practice.
    #[test]
    fn live_metrics_covers_every_channel() {
        let m = LiveMetrics::new();
        for ch in LIVE_CHANNELS {
            assert!(
                m.channels.contains_key(ch.name),
                "channel {} missing from LiveMetrics",
                ch.name
            );
        }
        // And snapshot is in the declared order.
        let snap = m.snapshot();
        let snap_names: Vec<&str> = snap.iter().map(|s| s.channel).collect();
        let decl_names: Vec<&str> = LIVE_CHANNELS.iter().map(|c| c.name).collect();
        assert_eq!(snap_names, decl_names);
    }

    /// Every channel has a pre-computed `{name}-changed` event name; catches
    /// a typo in `LIVE_CHANNELS` where event_name doesn't match the name.
    #[test]
    fn live_channels_event_names_match_name_changed_contract() {
        for ch in LIVE_CHANNELS {
            let expected = format!("{}-changed", ch.name);
            assert_eq!(
                ch.event_name, expected,
                "channel {}'s event_name is {:?}, expected {:?}",
                ch.name, ch.event_name, expected
            );
        }
    }

    /// Full-path integration test over HTTP: verifies that a write to any
    /// channel's table is **perceived as an SSE event** by a subscribed
    /// client — `SurrealStorage` (in-memory) → `spawn_live_broadcasters` →
    /// `/api/events` Axum route → raw TCP SSE client → `event: {X}-changed`
    /// + `data: 1` on the wire.
    ///
    /// Matrix: one scenario per channel in [`LIVE_CHANNELS`]. Each scenario
    /// subscribes a fresh SSE client to the channel, performs a write to
    /// one of its tables, and asserts the event surfaces. The `assets`
    /// scenario also subscribes to `lineage` (whose tables alias `assets`')
    /// and asserts both event names arrive on the same connection — that
    /// covers the shared-table fan-out path.
    ///
    /// **Per-table coverage caveat.** We write to ONE table per channel
    /// (not every table in the channel's `tables` slice). Sibling tables
    /// within a channel share the same [`spawn_one`] code path, and the
    /// per-table LIVE-query primitive is covered by
    /// `test_subscribe_table_yields_on_change` in `rivers-core`. Writing
    /// to every table would require hand-crafting inserts for tables like
    /// `condition_ticks` / `pending_steps` that are normally populated by
    /// internal transactions — high boilerplate for marginal coverage.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_channel_delivers_sse_events_over_http() {
        use axum::Router;
        use axum::routing::get;
        use rivers_core::storage::{
            AssetRecord, BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
            EventRecord, EventType, LaunchedBy, RunRecord, RunStatus, StorageBackend, TickRecord,
        };
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        // 1. In-memory storage + broadcaster + minimal Axum router.
        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .expect("build in-memory SurrealStorage"),
        );
        let shutdown = CancellationToken::new();
        let (tx, _metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());

        let app = Router::new().route(
            "/api/events",
            get({
                let tx = tx.clone();
                let sse_shutdown = shutdown.clone();
                move |query: axum::extract::Query<EventsQuery>| {
                    let tx = tx.clone();
                    let shutdown = sse_shutdown.clone();
                    async move { events_sse(tx, shutdown, query).await }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // All 13 (channel, table) LIVE queries must be registered before
        // any write fires — otherwise the write races past an unregistered
        // subscription.
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Helper: open a fresh SSE client subscribing to `channels`, run
        // `write`, and read bytes until every `expected_events` substring
        // appears — each formatted `event: X-changed` so a typo in either
        // the channel name or the pre-computed event_name trips the assert.
        async fn expect_events(
            addr: SocketAddr,
            channels: &str,
            write: impl std::future::Future<Output = ()>,
            expected_events: &[&str],
        ) {
            let mut client = TcpStream::connect(addr).await.expect("connect");
            client
                .write_all(
                    format!(
                        "GET /api/events?channels={channels} HTTP/1.1\r\n\
                         Host: t\r\nConnection: keep-alive\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write request");

            // Let the SSE handler subscribe on the broadcast::Sender
            // BEFORE we fire the write. `broadcast::Receiver::recv` only
            // sees messages sent after `tx.subscribe()`.
            tokio::time::sleep(Duration::from_millis(150)).await;

            write.await;

            let mut buf = vec![0u8; 8192];
            let mut total = Vec::<u8>::new();
            let mut remaining: Vec<&str> = expected_events.to_vec();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !remaining.is_empty() {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if left.is_zero() {
                    break;
                }
                match tokio::time::timeout(left, client.read(&mut buf)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        total.extend_from_slice(&buf[..n]);
                        let s = std::str::from_utf8(&total).unwrap_or("");
                        remaining.retain(|needle| !s.contains(needle));
                    }
                    _ => break,
                }
            }

            // `data: 1` is the load-bearing SSE payload — empty `data:`
            // is silently dropped by browser `EventSource` even though it
            // shows up on the wire to `curl`. Assert it's present.
            let wire = String::from_utf8_lossy(&total);
            assert!(
                remaining.is_empty(),
                "channels={channels:?} missing expected events {remaining:?}; \
                 saw {} bytes:\n{wire}",
                total.len()
            );
            assert!(
                wire.contains("data: 1"),
                "channels={channels:?} saw event header but no `data: 1` payload — \
                 browser EventSource would drop this event silently.\n{wire}"
            );
        }

        // Scenario: `runs` — `create_run` writes to `runs` table.
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "runs",
                async move {
                    let run = RunRecord {
                        run_id: "sse_runs".into(),
                        job_name: Some("j".into()),
                        status: RunStatus::Queued,
                        start_time: 1,
                        end_time: None,
                        tags: vec![],
                        node_names: vec![],
                        priority: 0,
                        partition_key: None,
                        block_reason: None,
                        launched_by: LaunchedBy::Manual { user: None },
                        code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID
                            .to_string(),
                    };
                    storage.create_run(&run).await.expect("create_run");
                },
                &["event: runs-changed"],
            )
            .await;
        }

        // Scenario: `assets` + `lineage` — one write to `assets` table
        // must fan out to BOTH channel event names (lineage shares tables
        // with assets; this proves the shared-table fan-out works).
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "assets,lineage",
                async move {
                    let asset = AssetRecord {
                        code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID
                            .to_string(),
                        asset_key: "sse_asset".into(),
                        tags: vec![],
                        kinds: vec![],
                        asset_group: None,
                        code_version: None,
                        last_event_id: None,
                        last_run_id: None,
                        last_timestamp: None,
                        last_data_version: None,
                        last_materialization_code_version: None,
                        last_input_data_versions: vec![],
                        pool: vec![],
                    };
                    storage
                        .for_code_location(&rivers_core::storage::CodeLocationContext::new(
                            rivers_core::storage::DEFAULT_CODE_LOCATION_ID,
                        ))
                        .register_assets(&[asset])
                        .await
                        .expect("register_assets");
                },
                &["event: assets-changed", "event: lineage-changed"],
            )
            .await;
        }

        // Scenario: `events` — `store_event` writes to `events` table.
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "events",
                async move {
                    let ev = EventRecord {
                        code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID
                            .to_string(),
                        event_type: EventType::Materialization { data_version: None },
                        asset_key: Some("sse_asset".into()),
                        run_id: "sse_runs".into(),
                        partition_key: None,
                        timestamp: 1,
                        metadata: vec![],
                        input_data_versions: vec![],
                    };
                    storage.store_event(&ev).await.expect("store_event");
                },
                &["event: events-changed"],
            )
            .await;
        }

        // Scenario: `backfills` — `create_backfill` writes to `backfills`.
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "backfills",
                async move {
                    let bf = BackfillRecord {
                        code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID
                            .to_string(),
                        backfill_id: "sse_bf".into(),
                        status: BackfillStatus::Requested,
                        strategy: BackfillStrategy::MultiRun,
                        failure_policy: BackfillFailurePolicy::Continue,
                        asset_selection: vec!["sse_asset".into()],
                        job_name: None,
                        partition_keys: vec![],
                        run_ids: vec![],
                        completed_partitions: vec![],
                        failed_partitions: vec![],
                        canceled_partitions: vec![],
                        max_concurrency: 1,
                        tags: vec![],
                        create_time: 1,
                        end_time: None,
                        error: None,
                        launched_by: rivers_core::storage::LaunchedBy::default(),
                    };
                    storage.create_backfill(&bf).await.expect("create_backfill");
                },
                &["event: backfills-changed"],
            )
            .await;
        }

        // Scenario: `automation` — `store_tick` writes to `ticks`, one of
        // three tables feeding this channel (others: condition_ticks,
        // condition_evals — same `spawn_one` code path, covered by the
        // `subscribe_table` primitive test in rivers-core).
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "automation",
                async move {
                    let tick = TickRecord {
                        code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID
                            .to_string(),
                        automation_name: "sse_sched".into(),
                        automation_type: "Schedule".into(),
                        status: "Success".into(),
                        timestamp: 1,
                        run_ids: vec![],
                        backfill_ids: vec![],
                        skip_reason: None,
                        error: None,
                        cursor: None,
                    };
                    storage.store_tick(&tick).await.expect("store_tick");
                },
                &["event: automation-changed"],
            )
            .await;
        }

        // Scenario: `pools` — `set_pool_limit` writes to `concurrency_pools`
        // (sibling tables `concurrency_slots` / `pending_steps` share code
        // path; see caveat above).
        {
            let storage = storage.clone();
            expect_events(
                addr,
                "pools",
                async move {
                    storage
                        .for_code_location(&rivers_core::storage::CodeLocationContext::new(
                            rivers_core::storage::DEFAULT_CODE_LOCATION_ID,
                        ))
                        .set_pool_limit("sse_pool", 5, 60)
                        .await
                        .expect("set_pool_limit");
                },
                &["event: pools-changed"],
            )
            .await;
        }

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(500), server).await;
    }

    /// Observability contract: `LiveMetrics.last_event_unix_ms` starts at
    /// `0` for every channel and must advance past `0` on the channel(s)
    /// whose table saw a write. Tests against the real broadcaster — not a
    /// unit stub — so a regression that breaks the update call site in
    /// `spawn_one` (e.g. storing on the wrong channel's metrics) trips the
    /// assert.
    #[tokio::test(flavor = "multi_thread")]
    async fn metrics_advance_after_write() {
        use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus, StorageBackend};

        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .expect("build in-memory SurrealStorage"),
        );
        let shutdown = CancellationToken::new();
        let (_tx, metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());

        // At startup every channel is zeroed.
        for snap in metrics.snapshot() {
            assert_eq!(
                snap.last_event_unix_ms, 0,
                "channel {} should start with last_event_unix_ms=0",
                snap.channel
            );
            assert_eq!(
                snap.reconnects, 0,
                "channel {} starts with 0 reconnects",
                snap.channel
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Single write to `runs` — only the `runs` channel should advance.
        let run = RunRecord {
            run_id: "metrics_probe".into(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        };
        storage.create_run(&run).await.expect("create_run");

        // Give the LIVE notification time to flow through `spawn_one`'s
        // while-let and update the counter.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut runs_ts = 0u64;
        while tokio::time::Instant::now() < deadline {
            runs_ts = metrics
                .snapshot()
                .into_iter()
                .find(|s| s.channel == "runs")
                .unwrap()
                .last_event_unix_ms;
            if runs_ts > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            runs_ts > 0,
            "runs channel last_event_unix_ms never advanced past 0 after create_run"
        );

        // Channels whose tables we didn't write to must still be at 0 — a
        // broadcaster bug that fan-out-stores to every channel on every
        // yield would leak here.
        for snap in metrics.snapshot() {
            if snap.channel == "runs" {
                continue;
            }
            assert_eq!(
                snap.last_event_unix_ms, 0,
                "channel {} erroneously advanced without a write to its tables",
                snap.channel
            );
        }

        shutdown.cancel();
    }

    /// Design contract: one live query per table, regardless of connected
    /// client count. Multiple SSE clients subscribed to the same channel
    /// must ALL receive each broadcast tick — a regression that turned the
    /// broadcast into a unicast would go uncaught by the single-client
    /// matrix test.
    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_sse_clients_receive_same_event() {
        use axum::Router;
        use axum::routing::get;
        use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus, StorageBackend};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let (tx, _metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());

        let app = Router::new().route(
            "/api/events",
            get({
                let tx = tx.clone();
                let sse_shutdown = shutdown.clone();
                move |query: axum::extract::Query<EventsQuery>| {
                    let tx = tx.clone();
                    let shutdown = sse_shutdown.clone();
                    async move { events_sse(tx, shutdown, query).await }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Open three concurrent SSE clients on the `runs` channel.
        async fn open_client(addr: std::net::SocketAddr) -> TcpStream {
            let mut c = TcpStream::connect(addr).await.unwrap();
            c.write_all(
                b"GET /api/events?channels=runs HTTP/1.1\r\n\
                  Host: t\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
            c
        }
        let mut c1 = open_client(addr).await;
        let mut c2 = open_client(addr).await;
        let mut c3 = open_client(addr).await;

        // Let all three subscribe on the broadcast::Sender.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Single DB write.
        let run = RunRecord {
            run_id: "fanout_probe".into(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        };
        storage.create_run(&run).await.unwrap();

        // Helper: drain bytes until `event: runs-changed` appears.
        async fn saw_event(client: &mut TcpStream) -> bool {
            let mut buf = vec![0u8; 4096];
            let mut total = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if left.is_zero() {
                    return false;
                }
                match tokio::time::timeout(left, client.read(&mut buf)).await {
                    Ok(Ok(0)) => return false,
                    Ok(Ok(n)) => {
                        total.extend_from_slice(&buf[..n]);
                        if std::str::from_utf8(&total)
                            .unwrap_or("")
                            .contains("event: runs-changed")
                        {
                            return true;
                        }
                    }
                    _ => return false,
                }
            }
        }

        let (g1, g2, g3) = tokio::join!(saw_event(&mut c1), saw_event(&mut c2), saw_event(&mut c3));
        assert!(g1, "client 1 never saw runs-changed");
        assert!(g2, "client 2 never saw runs-changed");
        assert!(g3, "client 3 never saw runs-changed");

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(500), server).await;
    }

    /// Channel filtering per client: a subscriber to `runs` must NOT
    /// receive events for a write that hits `assets` (and vice versa).
    /// The `events_sse` wanted-set filter is the only thing separating
    /// channels on the wire — a one-character bug would let everything
    /// through.
    #[tokio::test(flavor = "multi_thread")]
    async fn sse_client_isolation_across_channels() {
        use axum::Router;
        use axum::routing::get;
        use rivers_core::storage::{AssetRecord, LaunchedBy, RunRecord, RunStatus, StorageBackend};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let (tx, _metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());

        let app = Router::new().route(
            "/api/events",
            get({
                let tx = tx.clone();
                let sse_shutdown = shutdown.clone();
                move |query: axum::extract::Query<EventsQuery>| {
                    let tx = tx.clone();
                    let shutdown = sse_shutdown.clone();
                    async move { events_sse(tx, shutdown, query).await }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Client A subscribes to `runs`; client B to `assets`.
        let mut client_runs = TcpStream::connect(addr).await.unwrap();
        client_runs
            .write_all(
                b"GET /api/events?channels=runs HTTP/1.1\r\n\
                  Host: t\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();
        let mut client_assets = TcpStream::connect(addr).await.unwrap();
        client_assets
            .write_all(
                b"GET /api/events?channels=assets HTTP/1.1\r\n\
                  Host: t\r\nConnection: keep-alive\r\n\r\n",
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Write ONLY to `runs`. Client B (assets) must see no `runs-changed`.
        let run = RunRecord {
            run_id: "iso_probe".into(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        };
        storage.create_run(&run).await.unwrap();

        // Read both clients for a bounded window.
        async fn drain(client: &mut TcpStream, window: Duration) -> String {
            let mut buf = vec![0u8; 4096];
            let mut total = Vec::new();
            let deadline = tokio::time::Instant::now() + window;
            loop {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if left.is_zero() {
                    break;
                }
                match tokio::time::timeout(left, client.read(&mut buf)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => total.extend_from_slice(&buf[..n]),
                    _ => break,
                }
            }
            String::from_utf8_lossy(&total).into_owned()
        }

        let runs_stream = drain(&mut client_runs, Duration::from_millis(1500)).await;
        assert!(
            runs_stream.contains("event: runs-changed"),
            "client_runs didn't receive its own event:\n{runs_stream}"
        );

        let assets_stream = drain(&mut client_assets, Duration::from_millis(1500)).await;
        assert!(
            !assets_stream.contains("event: runs-changed"),
            "client_assets leaked a runs-changed event (channel filter broken):\n{assets_stream}"
        );
        assert!(
            !assets_stream.contains("event: assets-changed"),
            "client_assets saw an assets-changed without any asset write happening:\n{assets_stream}"
        );

        // Now write to `assets`. Client B should receive; client A should still only see its own.
        let asset = AssetRecord {
            code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "iso_asset".into(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        };
        storage
            .for_code_location(&rivers_core::storage::CodeLocationContext::new(
                rivers_core::storage::DEFAULT_CODE_LOCATION_ID,
            ))
            .register_assets(&[asset])
            .await
            .unwrap();

        let assets_after = drain(&mut client_assets, Duration::from_millis(1500)).await;
        assert!(
            assets_after.contains("event: assets-changed"),
            "client_assets didn't receive its event:\n{assets_after}"
        );
        let _ = LaunchedBy::Manual { user: None };

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(500), server).await;
    }

    /// `/debug/live` diagnostic endpoint returns well-formed JSON with one
    /// channel snapshot per entry in [`LIVE_CHANNELS`], in that order. A
    /// regression in the route (wrong content-type, panicking handler,
    /// missing field) would fail this.
    #[tokio::test(flavor = "multi_thread")]
    async fn debug_live_http_endpoint_serves_valid_json() {
        use axum::Router;
        use axum::routing::get;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let storage = Arc::new(
            rivers_core::storage::surrealdb_backend::SurrealStorage::new_memory()
                .await
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let (_tx, metrics) = spawn_live_broadcasters(storage.clone(), shutdown.clone());
        let debug_metrics = metrics.clone();

        let app = Router::new().route(
            "/debug/live",
            get(move || {
                let m = debug_metrics.clone();
                async move { debug_live(m).await }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /debug/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        let mut raw = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut raw)).await;
        let response = String::from_utf8_lossy(&raw);

        assert!(
            response.starts_with("HTTP/1.1 200"),
            "expected 200 OK, got:\n{response}"
        );
        assert!(
            response
                .to_lowercase()
                .contains("content-type: application/json"),
            "expected JSON content-type, got:\n{response}"
        );

        // Parse the JSON body (everything after the blank CRLF line).
        let body_start = response
            .find("\r\n\r\n")
            .expect("response should have headers/body separator");
        let body = &response[body_start + 4..];
        let json: serde_json::Value = serde_json::from_str(body)
            .unwrap_or_else(|e| panic!("body isn't valid JSON: {e}\nbody:\n{body}"));

        assert!(
            json.get("now_unix_ms").and_then(|v| v.as_u64()).is_some(),
            "missing or non-numeric `now_unix_ms`"
        );
        let channels = json
            .get("channels")
            .and_then(|v| v.as_array())
            .expect("missing `channels` array");
        assert_eq!(
            channels.len(),
            LIVE_CHANNELS.len(),
            "channels array should have one entry per LIVE_CHANNELS"
        );
        for (entry, expected) in channels.iter().zip(LIVE_CHANNELS.iter()) {
            assert_eq!(
                entry.get("channel").and_then(|v| v.as_str()),
                Some(expected.name),
                "channels array order doesn't match LIVE_CHANNELS"
            );
            assert!(entry.get("reconnects").and_then(|v| v.as_u64()).is_some());
            assert!(
                entry
                    .get("last_event_unix_ms")
                    .and_then(|v| v.as_u64())
                    .is_some()
            );
        }

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(500), server).await;
    }

    // ── Reconnect path ────────────────────────────────────────────────
    //
    // These tests drive [`channel_loop`] directly with a mock `subscribe`
    // closure so we can exercise the reconnect logic without fault-injecting
    // into a real SurrealDB LIVE query stream. All tests use `runs` as the
    // channel name (registered in [`LIVE_CHANNELS`] so the metrics lookup
    // succeeds).

    use std::sync::atomic::AtomicU32;

    /// Core reconnect contract: when the subscribed stream ends, the loop
    /// re-subscribes, increments `reconnects`, and sends a synthetic kick
    /// on the broadcast channel (so connected clients refetch after the
    /// gap). `last_event_unix_ms` must stay at `0` since the mock stream
    /// never yields a real item — proves the synthetic kick is separate
    /// from the per-yield path.
    #[tokio::test(start_paused = true)]
    async fn channel_loop_emits_synthetic_kick_and_increments_reconnects() {
        // Mock subscribe: every call returns an immediately-empty stream.
        // Each reconnect = one synthetic kick on the broadcast; zero real
        // yields so last_event_unix_ms stays at 0.
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let subscribe = move || {
            cc.fetch_add(1, Ordering::Relaxed);
            async move { Ok::<_, anyhow::Error>(futures_util::stream::empty::<()>()) }
        };

        let shutdown = CancellationToken::new();
        let shutdown_t = shutdown.clone();
        let (tx, mut rx) = broadcast::channel::<&'static str>(64);
        let metrics = LiveMetrics::new();
        let metrics_t = metrics.clone();

        let handle = tokio::spawn(async move {
            channel_loop(subscribe, shutdown_t, tx, "runs", None, metrics_t).await;
        });

        // With `start_paused = true`, the runtime auto-advances the virtual
        // clock to each sleep's deadline whenever every runnable task is
        // idle. A 4000ms sleep here lets the loop cycle through its 250ms
        // → 500ms → 1000ms → 2000ms backoff (< MIN_HEALTHY_MS = 5000ms, so
        // backoff grows each time) deterministically and instantly in
        // wall-clock terms.
        tokio::time::sleep(Duration::from_millis(4000)).await;

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

        let calls = call_count.load(Ordering::Relaxed);
        assert!(
            calls >= 3,
            "expected the loop to re-subscribe at least 3 times, got {calls}"
        );

        let snap = metrics
            .snapshot()
            .into_iter()
            .find(|s| s.channel == "runs")
            .unwrap();
        // reconnects = resubscribes after the first successful open, so
        // reconnects == calls - 1.
        assert_eq!(
            snap.reconnects,
            (calls - 1) as u64,
            "reconnects counter should equal (subscribe calls - 1)"
        );
        assert!(
            snap.reconnects >= 2,
            "expected ≥ 2 reconnects, got {}",
            snap.reconnects
        );
        assert_eq!(
            snap.last_event_unix_ms, 0,
            "no real yields happened, last_event_unix_ms should stay 0"
        );

        // Every non-first resubscribe emits one synthetic kick. We should
        // have received ≥ (calls - 1) messages on the broadcast.
        let mut received = 0u64;
        while let Ok(ch) = rx.try_recv() {
            assert_eq!(ch, "runs");
            received += 1;
        }
        assert!(
            received >= (calls as u64 - 1),
            "expected ≥ {} synthetic kicks on the broadcast, got {received}",
            calls - 1
        );
    }

    /// Subscribe-failure path: when the subscribe closure returns `Err`,
    /// the loop must retry with exponential backoff (not spin). With the
    /// 250ms → 500ms → 1000ms → 2000ms schedule, a 3500ms window should
    /// see exactly 4 attempts (at t=0, 250, 750, 1750), never 5+ which
    /// would indicate a missing backoff. Uses `start_paused` for clock
    /// determinism.
    #[tokio::test(start_paused = true)]
    async fn channel_loop_backs_off_on_subscribe_error() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let subscribe = move || {
            cc.fetch_add(1, Ordering::Relaxed);
            async move {
                Err::<futures_util::stream::Empty<()>, _>(anyhow::anyhow!("fake subscribe failure"))
            }
        };

        let shutdown = CancellationToken::new();
        let shutdown_t = shutdown.clone();
        let (tx, _rx) = broadcast::channel::<&'static str>(16);
        let metrics = LiveMetrics::new();

        let handle = tokio::spawn(async move {
            channel_loop(subscribe, shutdown_t, tx, "runs", None, metrics).await;
        });

        // Sleep past the first four backoff windows (250 + 500 + 1000 +
        // 2000 = 3750ms deadline). At t=3500 the runtime auto-advanced up
        // to each backoff deadline so attempts 1–4 fire at t=0, 250, 750,
        // 1750. A 5th attempt's 4000ms wait is still unelapsed at t=3500.
        tokio::time::sleep(Duration::from_millis(3500)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

        let calls = call_count.load(Ordering::Relaxed);
        // At t=0 attempt 1, t=250 attempt 2, t=750 attempt 3, t=1750
        // attempt 4. At t=3500 we're still inside the 2000ms wait after
        // attempt 4, so a 5th attempt must NOT have fired.
        assert_eq!(
            calls, 4,
            "expected exactly 4 subscribe attempts in a 3500ms window with exponential backoff, \
             got {calls} — either backoff is broken (too many) or the loop hung (too few)"
        );
    }

    /// Shutdown cancellation must break the loop promptly even when the
    /// stream is actively yielding — the while-let guards `shutdown` on
    /// every iteration. Uses a never-ending stream so the only way out
    /// is the cancellation check.
    #[tokio::test(flavor = "multi_thread")]
    async fn channel_loop_honors_shutdown_on_active_stream() {
        // Never-ending mock stream: `pending` yields once per poll in real
        // terms but will never return `Poll::Ready(None)`. To make
        // progress, we produce items periodically so the while-let body
        // runs and its shutdown check fires.
        let subscribe = || async {
            Ok::<_, anyhow::Error>(Box::pin(futures_util::stream::unfold((), |_| async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Some(((), ()))
            })))
        };

        let shutdown = CancellationToken::new();
        let shutdown_t = shutdown.clone();
        let (tx, _rx) = broadcast::channel::<&'static str>(64);
        let metrics = LiveMetrics::new();

        let handle = tokio::spawn(async move {
            channel_loop(subscribe, shutdown_t, tx, "runs", None, metrics).await;
        });

        // Let the loop spin for a beat, then cancel.
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.cancel();

        // Loop must exit within a bounded window of the cancellation — the
        // while-let checks `shutdown` on every tick and breaks.
        tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("channel_loop did not exit within 500ms of shutdown")
            .expect("channel_loop task panicked");
    }
}
