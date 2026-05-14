//! Client-side hook for live updates.
//!
//! [`use_live_kick`] opens one EventSource on `/api/events?channels=…`,
//! subscribes to `{channel}-changed` events for each requested channel,
//! and calls `on_kick` at a bounded rate. The SSE payload is never read —
//! each event is a pure "refetch now" signal; the callback decides what
//! state to invalidate. The hook returns a reactive [`LiveStatus`] signal
//! for the UI chip.
//!
//! # Rate-cap contract (leading-edge + trailing-edge throttle)
//!
//! Both edges fire and the window self-extends under sustained load:
//!
//! - **Leading edge.** The first event of a quiet period fires `on_kick`
//!   immediately (no latency floor on an idle system).
//! - **Suppression window.** After a fire, further fires are suppressed for
//!   `throttle_ms`; events arriving during the window set a "pending" flag.
//! - **Trailing edge.** At the end of the window, if pending is set, fire
//!   once more and open *another* suppression window.
//!
//! This gives at most one fire per `throttle_ms`, regardless of event rate.
//! Straight trailing-edge debounce would starve under sustained bursts
//! faster than the window: every event resets the timer, the timer never
//! fires, the UI never refetches. The throttle variant cannot starve.
//!
//! # Safety-net refresh
//!
//! Every [`SAFETY_NET_INTERVAL_MS`] the hook fires `on_kick` unconditionally.
//! This covers silent EventSource failures (corporate proxy idle timeouts,
//! sleep/wake, mobile handoffs, CDNs that buffer event-stream responses) that
//! neither the browser's auto-reconnect nor the `error` event detect cleanly.
//! Load-bearing — do not remove.
//!
//! # Tab-hidden behaviour
//!
//! A backgrounded tab keeps an SSE connection slot open on the server and
//! fires refetches the user can't see. On `visibilitychange → hidden` we
//! close the `EventSource` and cancel the trailing timer; on return to
//! visible we open a new connection and force one immediate kick so the UI
//! catches up on anything that happened while hidden.

use leptos::prelude::*;

/// Tri-state connection health of an SSE subscription, driven by
/// `EventSource`'s `readyState` transitions. Wire to [`LiveStatusChip`]
/// for the tiny indicator in each page's topbar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStatus {
    /// Connection is open and receiving events (`readyState = OPEN`).
    Live,
    /// Initial connect in progress or browser auto-reconnecting after a
    /// dropped connection (`readyState = CONNECTING`). Typically transient.
    Reconnecting,
    /// Connection closed and the browser gave up (`readyState = CLOSED`).
    /// The 5-min safety-net poll still runs — data won't be stuck forever,
    /// but the live fast-path is gone until the page reloads.
    Stale,
}

/// Safety-net refresh cadence — forces a refetch every 5 minutes regardless
/// of SSE state. See module docs. Only referenced from the hydrate-feature
/// branch of [`use_live_kick`].
#[cfg(feature = "hydrate")]
const SAFETY_NET_INTERVAL_MS: u32 = 300_000;

// Single-tenant global slots for the currently-live EventSource and the
// Rust Closures registered on it. Only one page is mounted at a time so
// there should only ever be one in-flight SSE subscription — these slots
// enforce that by closing any prior ES + dropping its Closures before a
// new one is opened. Belt-and-suspenders against any path where a page
// unmount doesn't fire `on_cleanup` promptly (route swap, navigation race,
// etc.); without `ACTIVE_ES`, stale ESs pile up and exhaust the browser's
// 6-connection-per-origin HTTP/1.1 pool, stalling `/api/events`.
// `ACTIVE_ES_CLOSURES` is the per-open() companion so those Closures can
// drop deterministically (Closure is !Send, so `StoredValue<_>` isn't an
// option — WASM single-thread + thread_local is the right fit).
// `ACTIVE_VIS` holds the document-level visibilitychange Closure paired
// with its AbortController so a new mount can abort-then-drop the
// previous page's listener (order matters — drop first would leave a
// dangling JS reference).
#[cfg(feature = "hydrate")]
struct VisHandle {
    // Keeps the Rust Closure alive while the browser holds a reference via
    // `addEventListener`. Field is underscore-prefixed because nothing
    // reads from it — drop-on-replacement is the whole contract.
    _closure: leptos::wasm_bindgen::closure::Closure<dyn Fn()>,
    // `None` when AbortController wasn't available (very old browsers);
    // in that path we fall back to the leaky `.forget()` registration
    // and don't store the handle here at all.
    abort_controller: web_sys::AbortController,
}

#[cfg(feature = "hydrate")]
thread_local! {
    static ACTIVE_ES: std::cell::RefCell<Option<web_sys::EventSource>> =
        const { std::cell::RefCell::new(None) };
    static ACTIVE_ES_CLOSURES: std::cell::RefCell<Vec<Box<dyn std::any::Any>>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static ACTIVE_VIS: std::cell::RefCell<Option<VisHandle>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "hydrate")]
pub fn use_live_kick(
    channels: &'static [&'static str],
    throttle_ms: u32,
    on_kick: Callback<()>,
) -> ReadSignal<LiveStatus> {
    use leptos::wasm_bindgen::JsCast;
    use leptos::wasm_bindgen::closure::Closure;
    use web_sys::{AbortController, AddEventListenerOptions, EventSource};

    // Initial status: Reconnecting. Transitions to Live on the first `open`
    // event, or Stale on a terminal `error`.
    let (status, set_status) = signal(LiveStatus::Reconnecting);

    if channels.is_empty() {
        return status;
    }

    // One AbortController per hook invocation — its abort() detaches the
    // document-scoped visibilitychange listener registered below. Before
    // this plumbing, those listeners piled up on `document` across nav
    // swaps; each leaked one still held a reference to the unmounted
    // page's `es_slot`, and on the next visibility toggle would open a
    // zombie EventSource for the *old* page's channels. After a few swaps
    // those zombies saturated the browser's 6-connection-per-origin
    // HTTP/1.1 limit, stalling `/api/events`. The controller is bundled
    // with the Closure into `ACTIVE_VIS` so a new mount's swap aborts
    // and drops the previous one in the correct order.
    let abort_controller = AbortController::new().ok();

    // Throttle state — shared between the event handler and the trailing
    // timer. `StoredValue` is used because both closures outlive the hook
    // body and the component's reactive Owner keeps the slots alive.
    let suppressed: StoredValue<bool> = StoredValue::new(false);
    let pending: StoredValue<bool> = StoredValue::new(false);
    let timer: StoredValue<Option<TimeoutHandle>> = StoredValue::new(None);

    // Current connection. `None` while the tab is hidden.
    let es_slot: StoredValue<Option<EventSource>> = StoredValue::new(None);

    // The current ES's Rust-side Closures live in `ACTIVE_ES_CLOSURES`
    // (module-level thread_local) rather than per-hook — see module docs.
    // Without this tracking they'd have to be `.forget()`-ed (leaked) per
    // open() call, and every tab toggle or reconnect would accumulate
    // `2 + channels.len()` dangling closures for the lifetime of the
    // session.

    let open = move || {
        use web_sys::MessageEvent;
        // Close any previously-active EventSource, whether it came from
        // this hook instance or from a previous page's hook whose cleanup
        // hasn't fired yet (route-swap interleaving: the new page's mount
        // can run *before* the old page's `on_cleanup`). The global slot
        // guarantees at-most-one ES across the whole app — without it,
        // stale ESs saturate the browser's 6-connection-per-origin pool
        // and new `/api/events` requests stall.
        ACTIVE_ES.with(|slot| {
            if let Some(prev) = slot.borrow_mut().take() {
                prev.close();
            }
        });
        if let Some(prev) = es_slot.with_value(|s| s.clone()) {
            prev.close();
        }
        // Old browser-side listeners were detached by the `close()` calls
        // above, so it's now safe to drop the old Closures.
        ACTIVE_ES_CLOSURES.with(|slot| slot.borrow_mut().clear());

        let url = format!("/api/events?channels={}", channels.join(","));
        let es = match EventSource::new(&url) {
            Ok(es) => es,
            Err(err) => {
                leptos::logging::warn!("use_live_kick: failed to open {url}: {err:?}");
                let _ = set_status.try_set(LiveStatus::Stale);
                return;
            }
        };

        let mut closures: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(2 + channels.len());

        // `open` → Live. `error` → Reconnecting while the browser retries
        // (readyState = CONNECTING), Stale once it gives up (readyState =
        // CLOSED). EventSource auto-reconnects on transient drops; a single
        // error event isn't fatal.
        let open_cb = Closure::<dyn Fn()>::new(move || {
            let _ = set_status.try_set(LiveStatus::Live);
        });
        let _ = es.add_event_listener_with_callback("open", open_cb.as_ref().unchecked_ref());
        closures.push(Box::new(open_cb));

        let es_for_err = es.clone();
        let err_cb = Closure::<dyn Fn()>::new(move || {
            // EventSource::CLOSED == 2; CONNECTING == 0; OPEN == 1.
            let new_status = if es_for_err.ready_state() == EventSource::CLOSED {
                LiveStatus::Stale
            } else {
                LiveStatus::Reconnecting
            };
            let _ = set_status.try_set(new_status);
        });
        let _ = es.add_event_listener_with_callback("error", err_cb.as_ref().unchecked_ref());
        closures.push(Box::new(err_cb));

        for channel in channels {
            let handler = Closure::<dyn Fn(MessageEvent)>::new(move |_ev: MessageEvent| {
                if suppressed.get_value() {
                    pending.set_value(true);
                } else {
                    on_kick.run(());
                    suppressed.set_value(true);
                    schedule_trailing(suppressed, pending, timer, on_kick, throttle_ms);
                }
            });
            let event_name = format!("{channel}-changed");
            let _ =
                es.add_event_listener_with_callback(&event_name, handler.as_ref().unchecked_ref());
            closures.push(Box::new(handler));
        }
        ACTIVE_ES_CLOSURES.with(|slot| *slot.borrow_mut() = closures);

        // Register in both the per-instance slot (for this hook's own
        // cleanup) and the global slot (so a future mount can close us
        // even if our cleanup never fires).
        es_slot.set_value(Some(es.clone()));
        ACTIVE_ES.with(|slot| *slot.borrow_mut() = Some(es));
    };

    open();

    // 5-min safety-net refresh. Fires unconditionally alongside SSE kicks so
    // a silent connection failure (that `error` didn't catch) doesn't leave
    // the UI stuck.
    {
        use core::time::Duration;
        if let Ok(handle) = set_interval_with_handle(
            move || on_kick.run(()),
            Duration::from_millis(SAFETY_NET_INTERVAL_MS as u64),
        ) {
            on_cleanup(move || handle.clear());
        }
    }

    // visibilitychange: tear down / rebuild the connection so backgrounded
    // tabs cost zero server resources. On return to visible we force one
    // kick to cover any events we missed while hidden. The listener is
    // registered with `{ signal }` so `on_cleanup → controller.abort()`
    // detaches it on page unmount — without this, the closure leaks onto
    // `document` and can open zombie EventSources after navigation.
    let vis_closure = Closure::<dyn Fn()>::new(move || {
        let hidden = leptos::web_sys::window()
            .and_then(|w| w.document())
            .map(|d| d.hidden())
            .unwrap_or(false);

        if hidden {
            if let Some(es) = es_slot.with_value(|o| o.clone()) {
                es.close();
            }
            es_slot.set_value(None);
            // ES is closed → browser no longer references the listeners →
            // drop the Rust Closures so they don't linger until the next
            // open() or unmount.
            ACTIVE_ES_CLOSURES.with(|slot| slot.borrow_mut().clear());
            if let Some(h) = timer.get_value() {
                h.clear();
            }
            timer.set_value(None);
            suppressed.set_value(false);
            pending.set_value(false);
            // Hidden → the source we just closed isn't "live" anymore.
            // Chip won't be visible to the user right now anyway, but keep
            // the signal semantically accurate.
            let _ = set_status.try_set(LiveStatus::Reconnecting);
        } else if es_slot.with_value(|o| o.is_none()) {
            let _ = set_status.try_set(LiveStatus::Reconnecting);
            open();
            on_kick.run(());
        }
    });
    if let Some(document) = leptos::web_sys::window().and_then(|w| w.document()) {
        match abort_controller {
            Some(ctrl) => {
                // Happy path: register with `{ signal }` so abort() detaches,
                // then park the (Closure, AbortController) pair in
                // ACTIVE_VIS so a new mount — or our own on_cleanup — can
                // abort-then-drop in the correct order.
                let opts = AddEventListenerOptions::new();
                opts.set_signal(&ctrl.signal());
                let _ = document.add_event_listener_with_callback_and_add_event_listener_options(
                    "visibilitychange",
                    vis_closure.as_ref().unchecked_ref(),
                    &opts,
                );
                let new_handle = VisHandle {
                    _closure: vis_closure,
                    abort_controller: ctrl,
                };
                ACTIVE_VIS.with(|slot| {
                    // Abort predecessor's browser-side listener FIRST, then
                    // let the old handle drop (releasing its Closure).
                    // Order matters: dropping the Closure while the browser
                    // still holds a reference would cause the listener to
                    // fire into freed memory — wasm-bindgen throws a JS
                    // exception on that path rather than corrupting Rust
                    // state, but it's still observable.
                    if let Some(old) = slot.borrow_mut().take() {
                        old.abort_controller.abort();
                    }
                    *slot.borrow_mut() = Some(new_handle);
                });
            }
            None => {
                // AbortController unavailable (very old browser) — fall back
                // to the leaky registration. Without a signal there's no
                // safe way to detach later, so we must leak the Closure;
                // re-attaching ACTIVE_VIS here would risk a dangling ref.
                let _ = document.add_event_listener_with_callback(
                    "visibilitychange",
                    vis_closure.as_ref().unchecked_ref(),
                );
                vis_closure.forget();
            }
        }
    } else {
        // No document (shouldn't happen in a real browser hydrate) — drop
        // the Closure; nothing registered it.
        drop(vis_closure);
    }

    on_cleanup(move || {
        let our_es = es_slot.with_value(|o| o.clone());
        if let Some(es) = &our_es {
            es.close();
        }
        es_slot.set_value(None);
        // Clear the global ES slot + its companion Closures — but only if
        // they still point at *our* ES. A newer hook instance may have
        // already swapped itself in (common race: new page's `open()`
        // runs before the old page's `on_cleanup`), in which case those
        // globals belong to it and we must not drop them.
        if let Some(ours) = our_es {
            use leptos::wasm_bindgen::JsValue;
            let is_ours = ACTIVE_ES.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .map(|current| {
                        let a: &JsValue = ours.as_ref();
                        let b: &JsValue = current.as_ref();
                        a == b
                    })
                    .unwrap_or(false)
            });
            if is_ours {
                ACTIVE_ES.with(|slot| *slot.borrow_mut() = None);
                ACTIVE_ES_CLOSURES.with(|slot| slot.borrow_mut().clear());
                // Abort then drop our visibilitychange handle. A newer
                // mount's overwrite would have done this already via the
                // ACTIVE_VIS swap; is_ours = true means no one else has,
                // so it's our job. Aborting before dropping the Closure
                // is the invariant.
                ACTIVE_VIS.with(|slot| {
                    if let Some(h) = slot.borrow_mut().take() {
                        h.abort_controller.abort();
                    }
                });
            }
        }
        if let Some(h) = timer.get_value() {
            h.clear();
        }
        timer.set_value(None);
    });

    status
}

/// End-of-window handler: if events arrived during the current suppression
/// window (i.e. `pending` is set), fire one trailing kick and *restart* the
/// window so sustained bursts stay rate-capped. Otherwise clear suppression
/// and let the next event fire immediately on the leading edge.
///
/// Calls itself to extend suppression — each "extension" is one additional
/// scheduled timer, not a stack frame, so there is no real recursion depth.
#[cfg(feature = "hydrate")]
fn schedule_trailing(
    suppressed: StoredValue<bool>,
    pending: StoredValue<bool>,
    timer: StoredValue<Option<TimeoutHandle>>,
    on_kick: Callback<()>,
    ms: u32,
) {
    use core::time::Duration;
    let handle = set_timeout_with_handle(
        move || {
            if pending.get_value() {
                pending.set_value(false);
                on_kick.run(());
                schedule_trailing(suppressed, pending, timer, on_kick, ms);
            } else {
                suppressed.set_value(false);
                timer.set_value(None);
            }
        },
        Duration::from_millis(ms as u64),
    );
    timer.set_value(handle.ok());
}

#[cfg(not(feature = "hydrate"))]
pub fn use_live_kick(
    _channels: &'static [&'static str],
    _throttle_ms: u32,
    _on_kick: Callback<()>,
) -> ReadSignal<LiveStatus> {
    // SSR has no EventSource; pages render their initial HTML, then hydration
    // takes over and replaces this no-op signal with the real one.
    let (status, _) = signal(LiveStatus::Reconnecting);
    status
}

/// Small topbar indicator showing the current [`LiveStatus`]: a coloured dot
/// + label (`Live` / `Reconnecting` / `Stale`) plus a manual Refresh button
/// that calls `on_refresh` — useful when the chip is stuck on `Stale` and
/// the user wants to force a pull before the next 5-min safety-net tick.
///
/// CSS classes: `.live-chip`, `.live-chip--live`, `.live-chip--reconnecting`,
/// `.live-chip--stale`.
#[component]
pub fn LiveStatusChip(
    #[prop(into)] status: Signal<LiveStatus>,
    on_refresh: Callback<()>,
) -> impl IntoView {
    let cls = move || {
        let s = match status.get() {
            LiveStatus::Live => "live",
            LiveStatus::Reconnecting => "reconnecting",
            LiveStatus::Stale => "stale",
        };
        format!("live-chip live-chip--{s}")
    };
    let title = move || match status.get() {
        LiveStatus::Live => "Connected — updates stream in real time",
        LiveStatus::Reconnecting => "Reconnecting — browser is retrying",
        LiveStatus::Stale => {
            "Live updates unavailable — falling back to 5-min poll. Click Refresh to force a pull."
        }
    };
    let label = move || match status.get() {
        LiveStatus::Live => "Live",
        LiveStatus::Reconnecting => "Reconnecting",
        LiveStatus::Stale => "Stale",
    };
    view! {
        <div class="live-chip-group" title=title>
            <div class=cls>
                <span class="live-chip-dot"></span>
                <span class="live-chip-label">{label}</span>
            </div>
            <button
                class="btn btn-small live-chip-refresh"
                on:click=move |_| on_refresh.run(())
                title="Refresh now"
            >
                "Refresh"
            </button>
        </div>
    }
}
