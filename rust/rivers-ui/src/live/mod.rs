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
    deadline: Option<tokio::time::Instant>,
) -> impl futures_core::Stream<Item = &'static str> {
    use tokio::sync::broadcast::error::RecvError;
    // `pending` holds pre-computed event names ready to emit. On `Lagged` we
    // fill it with every wanted event name so each client listener fires
    // once; on a normal match we queue just the matched one.
    futures_util::stream::unfold(
        (rx, wanted, Vec::<&'static str>::new(), shutdown, deadline),
        |(mut rx, wanted, mut pending, shutdown, deadline)| async move {
            loop {
                if shutdown.is_cancelled() {
                    return None;
                }
                if let Some(event_name) = pending.pop() {
                    return Some((event_name, (rx, wanted, pending, shutdown, deadline)));
                }
                let recv = tokio::select! {
                    result = rx.recv() => result,
                    _ = shutdown.cancelled() => return None,
                    _ = async {
                        match deadline {
                            Some(d) => tokio::time::sleep_until(d).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => return None,
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
    max_age: Option<std::time::Duration>,
    query: Query<EventsQuery>,
) -> impl IntoResponse {
    use futures_util::StreamExt;
    let wanted = resolve_wanted(&query.channels);
    let rx = tx.subscribe();
    // Bound the stream so the client reconnects (and re-runs the auth
    // middleware) before its session would expire — an open stream is never
    // otherwise re-validated.
    let deadline = max_age.map(|d| tokio::time::Instant::now() + d);
    let stream = event_stream(rx, wanted, shutdown, deadline).map(|event_name| {
        Ok::<_, std::convert::Infallible>(Event::default().event(event_name).data("1"))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Longest an SSE stream stays open before it must reconnect and
/// re-authenticate, in seconds.
const MAX_SSE_STREAM_SECS: i64 = 3600;

/// How long an SSE stream opened at `now` may run: the lesser of the session's
/// remaining lifetime (`expires_at`) and [`MAX_SSE_STREAM_SECS`] — the cap
/// bounds forward-mode streams whose identity never expires. Zero once the
/// session is already past `expires_at`, ending the stream immediately.
pub(crate) fn session_max_age(expires_at: i64, now: i64) -> std::time::Duration {
    let deadline = expires_at.min(now.saturating_add(MAX_SSE_STREAM_SECS));
    std::time::Duration::from_secs(deadline.saturating_sub(now).max(0) as u64)
}

#[cfg(test)]
mod tests;
