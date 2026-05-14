//! Reactive "now" clock and `<RelTime>` component.
//!
//! Why this exists: relative-time labels ("5m ago") computed once at fetch
//! time become stale the longer the user stares at a page. We keep a single
//! global `now` signal that ticks once per second; `<RelTime>` reads it so
//! every relative label across every page updates in lockstep.
//!
//! Hydration safety: SSR computes server-now once in `shell()` and stamps
//! it on `<body data-ssr-now=…>`. On hydration, the WASM bootstrap reads
//! that attribute and uses it as the signal's initial value, so the first
//! reactive render produces the exact same string the server emitted. The
//! per-second tick interval starts inside an `Effect` (client-only), so it
//! never runs during SSR and can't interfere with hydration matching.

use leptos::prelude::*;

/// Reactive unix-seconds clock. Read with [`use_now`].
#[derive(Clone, Copy)]
pub struct NowContext(pub Signal<i64>);

/// SSR-only handoff: shell stamps the value before `<App/>` so `App` sees
/// the *same* value shell wrote into `<body data-ssr-now>`. Without this,
/// shell and App would each call `chrono::Utc::now()` separately and could
/// disagree by a second across the SSR/hydrate boundary.
#[cfg(feature = "ssr")]
#[derive(Clone, Copy)]
pub struct SsrInitialNow(pub i64);

/// Returns the reactive now signal. Falls back to a static current-time
/// derivation if the context isn't installed (e.g. component used outside
/// the App tree in tests).
pub fn use_now() -> Signal<i64> {
    use_context::<NowContext>()
        .map(|c| c.0)
        .unwrap_or_else(|| Signal::derive(|| chrono::Utc::now().timestamp()))
}

fn read_initial_now() -> i64 {
    #[cfg(feature = "ssr")]
    {
        if let Some(s) = use_context::<SsrInitialNow>() {
            return s.0;
        }
    }
    #[cfg(feature = "hydrate")]
    {
        if let Some(val) = leptos::web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.body())
            .and_then(|b| b.get_attribute("data-ssr-now"))
            .and_then(|s| s.parse::<i64>().ok())
        {
            return val;
        }
    }
    chrono::Utc::now().timestamp()
}

/// Install the global clock. Call once near the top of `App`.
pub fn provide_now_context() {
    let initial = read_initial_now();
    let now_signal = RwSignal::new(initial);
    provide_context(NowContext(now_signal.into()));

    #[cfg(feature = "hydrate")]
    {
        // Effects don't run during SSR or while hydration is matching the
        // initial DOM — they run once mount completes — so the per-second
        // tick starts strictly after the static SSR text has been adopted.
        Effect::new(move |_| {
            use core::time::Duration;
            if let Ok(handle) = set_interval_with_handle(
                move || now_signal.set(chrono::Utc::now().timestamp()),
                Duration::from_secs(1),
            ) {
                on_cleanup(move || handle.clear());
            }
        });
    }
}

/// Reactive relative-time label. Replaces ad-hoc `format_relative_time(ts)`
/// calls at view sites so the label re-renders on every clock tick.
#[component]
pub fn RelTime(ts: i64) -> impl IntoView {
    let now = use_now();
    move || crate::helpers::format_relative_time(ts, now.get())
}

/// Optional-timestamp variant: renders `fallback` when `ts` is `None`.
#[component]
pub fn RelTimeOpt(
    ts: Option<i64>,
    #[prop(default = "never")] fallback: &'static str,
) -> impl IntoView {
    let now = use_now();
    move || match ts {
        Some(t) => crate::helpers::format_relative_time(t, now.get()),
        None => fallback.to_string(),
    }
}
