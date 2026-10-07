//! Reload requests for the code location under `rivers dev`.
//!
//! The host enables this once and then waits on it; the UI's reload control
//! posts requests here and the host reports how each reload went. Requests
//! posted while one is in flight collapse into a single trailing reload.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use tokio::sync::Notify;

use crate::types::DevReloadState;

static STATE: Mutex<DevReloadState> = Mutex::new(DevReloadState {
    enabled: false,
    generation: 0,
    error: None,
});
static PENDING: AtomicBool = AtomicBool::new(false);
/// Wakes the host.
static REQUESTED: Notify = Notify::const_new();
/// Wakes the callers waiting for a verdict.
static SETTLED: Notify = Notify::const_new();

pub fn enable() {
    STATE.lock().unwrap().enabled = true;
}

pub fn disable() {
    STATE.lock().unwrap().enabled = false;
    PENDING.store(false, Relaxed);
    SETTLED.notify_waiters();
}

/// Post a reload request and return the generation it starts from; `None`
/// when no host is listening. A new attempt clears the verdict of the
/// previous one.
pub fn request() -> Option<u64> {
    let mut state = STATE.lock().unwrap();
    if !state.enabled {
        return None;
    }
    state.error = None;
    PENDING.store(true, Relaxed);
    REQUESTED.notify_one();
    Some(state.generation)
}

/// Wait for a request and consume it.
pub async fn requested() {
    while !PENDING.swap(false, Relaxed) {
        REQUESTED.notified().await;
    }
}

/// The host reports the code location serving as `generation` (the first
/// is 0); subscribed tabs refetch their definitions.
pub fn serving(generation: u64) {
    {
        let mut state = STATE.lock().unwrap();
        state.generation = generation;
        state.error = None;
    }
    SETTLED.notify_waiters();
    crate::live::kick(crate::components::live::DEFINITIONS_CHANNEL);
}

/// The host reports a code location that did not come back up.
pub fn failed(message: String) {
    STATE.lock().unwrap().error = Some(message);
    SETTLED.notify_waiters();
}

pub fn state() -> DevReloadState {
    STATE.lock().unwrap().clone()
}

/// Resolve once a reload posted at generation `before` has a verdict: the
/// code location came back (the generation moved), it failed, or the host
/// stopped listening.
pub async fn settled(before: u64) -> DevReloadState {
    loop {
        let notified = SETTLED.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let state = state();
        if !state.enabled || state.generation > before || state.error.is_some() {
            return state;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;

    // The statics are process-wide; these tests take turns.
    static TURN: Mutex<()> = Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        TURN.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[tokio::test]
    async fn a_request_is_dropped_while_no_host_listens() {
        let _turn = lock();
        disable();
        assert!(request().is_none());
        assert!(
            timeout(Duration::from_millis(10), requested())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn requests_collapse_into_one_wait() {
        let _turn = lock();
        enable();
        assert!(request().is_some());
        assert!(request().is_some());
        assert!(
            timeout(Duration::from_millis(10), requested())
                .await
                .is_ok()
        );
        assert!(
            timeout(Duration::from_millis(10), requested())
                .await
                .is_err()
        );
        disable();
    }

    #[tokio::test]
    async fn a_request_wakes_a_waiting_host() {
        let _turn = lock();
        enable();
        let waiter = tokio::spawn(timeout(Duration::from_secs(5), requested()));
        sleep(Duration::from_millis(50)).await;
        assert!(request().is_some());
        assert!(waiter.await.unwrap().is_ok());
        disable();
    }

    #[tokio::test]
    async fn the_state_reports_the_outcome_of_the_last_attempt() {
        let _turn = lock();
        enable();
        let before = state().generation;
        failed("exit code 1".into());
        assert_eq!(state().error.as_deref(), Some("exit code 1"));
        assert_eq!(request(), Some(before));
        assert_eq!(state().error, None, "a new attempt clears the verdict");
        serving(before + 1);
        let after = state();
        assert_eq!(after.generation, before + 1);
        assert_eq!(after.error, None);
        assert!(after.enabled);
        disable();
        assert!(!state().enabled);
    }

    #[tokio::test]
    async fn a_waiter_settles_on_the_verdict() {
        let _turn = lock();
        enable();

        let before = request().unwrap();
        let waiter = tokio::spawn(settled(before));
        sleep(Duration::from_millis(50)).await;
        serving(before + 1);
        let verdict = timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(verdict.generation, before + 1);
        assert_eq!(verdict.error, None);

        let before = request().unwrap();
        let waiter = tokio::spawn(settled(before));
        sleep(Duration::from_millis(50)).await;
        failed("boom".into());
        let verdict = timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(verdict.error.as_deref(), Some("boom"));

        let before = request().unwrap();
        let waiter = tokio::spawn(settled(before));
        sleep(Duration::from_millis(50)).await;
        disable();
        let verdict = timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(!verdict.enabled);
    }
}
