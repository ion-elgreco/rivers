//! The event writer's batch: events on their way to the store.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use surrealdb::types::RecordId;
use tokio::time::Instant;

use super::EventRecord;
use super::surrealdb_backend::new_event_record_id;

/// Most events in one send; a bigger batch goes in chunks of this size.
pub const BATCH_SIZE: usize = 64;

/// Time after a failed send in which [`EventBatch::send_unless_paused`] does
/// nothing.
const PAUSE_AFTER_FAILURE: Duration = Duration::from_secs(5);

/// How long the send at the end of a run keeps trying.
const FINAL_SEND_LIMIT: Duration = Duration::from_secs(5 * 60);

/// An event and the id it is stored under.
#[derive(Debug, Clone)]
pub struct EventWithId {
    pub id: RecordId,
    pub event: EventRecord,
}

impl EventWithId {
    pub fn new(event: EventRecord) -> Self {
        Self {
            id: new_event_record_id(),
            event,
        }
    }
}

/// Where an [`EventBatch`] stores its events.
pub trait EventStore: Sync {
    /// Stores each event under its id. Sent again with the same ids, each
    /// event is stored once.
    fn store_events_with_ids(
        &self,
        events: &[EventWithId],
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Events on their way to the store, oldest first.
///
/// An event gets its id when it joins the batch. A send that fails keeps its
/// events, and the next send stores them with the newer ones.
#[derive(Default)]
pub struct EventBatch {
    events: Vec<EventWithId>,
    /// Set by a failed send. Until this time, [`Self::send_unless_paused`]
    /// does nothing.
    paused_until: Option<Instant>,
}

impl EventBatch {
    pub fn push(&mut self, event: EventRecord) {
        self.events.push(EventWithId::new(event));
    }

    pub fn extend(&mut self, events: impl IntoIterator<Item = EventRecord>) {
        self.events.extend(events.into_iter().map(EventWithId::new));
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Sends the events, oldest first, in chunks of [`BATCH_SIZE`]. Each
    /// stored chunk leaves the batch. The first chunk that fails stays with
    /// the chunks after it, and its error comes back: `Ok` means the whole
    /// batch is stored.
    pub async fn send(&mut self, store: &impl EventStore) -> Result<()> {
        let mut stored = 0;
        let mut result = Ok(());
        for chunk in self.events.chunks(BATCH_SIZE) {
            if let Err(e) = store.store_events_with_ids(chunk).await {
                result = Err(e);
                break;
            }
            stored += chunk.len();
        }
        self.events.drain(..stored);

        match &result {
            Ok(()) => self.paused_until = None,
            Err(e) => {
                self.paused_until = Some(Instant::now() + PAUSE_AFTER_FAILURE);
                tracing::warn!(
                    kept = self.events.len(),
                    "failed to store events; they are kept for the next send: {e:#}"
                );
            }
        }
        result
    }

    /// [`Self::send`], unless a send failed less than
    /// [`PAUSE_AFTER_FAILURE`] ago. For the sends that no caller waits for:
    /// a kept batch is not sent again on every new event.
    pub async fn send_unless_paused(&mut self, store: &impl EventStore) {
        let paused = self
            .paused_until
            .is_some_and(|until| Instant::now() < until);
        if !paused {
            let _ = self.send(store).await;
        }
    }

    /// Sends until the whole batch is stored, with [`PAUSE_AFTER_FAILURE`]
    /// before each send again. No send starts after [`FINAL_SEND_LIMIT`]:
    /// then the error of the last send comes back.
    pub async fn send_until_stored(&mut self, store: &impl EventStore) -> Result<()> {
        let deadline = Instant::now() + FINAL_SEND_LIMIT;
        loop {
            if let Some(until) = self.paused_until {
                tokio::time::sleep_until(until).await;
            }

            let Err(e) = self.send(store).await else {
                return Ok(());
            };
            if self.paused_until.is_some_and(|until| until >= deadline) {
                return Err(e.context(format!(
                    "{} events were not stored after trying for {} minutes",
                    self.events.len(),
                    FINAL_SEND_LIMIT.as_secs() / 60
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Mutex;

    use super::*;
    use crate::storage::EventType;

    /// What the store does with one send.
    #[derive(Clone, Copy)]
    enum Outcome {
        Stored,
        Failed,
        /// The events are stored, then the send fails: an attempt that
        /// stopped after its insert.
        StoredThenFailed,
    }

    /// Keeps events by id, as `INSERT IGNORE` does.
    struct Store {
        outcome: fn(usize) -> Outcome,
        /// Each send: when it started and how many events it carried.
        sends: Mutex<Vec<(Instant, usize)>>,
        stored: Mutex<Vec<EventWithId>>,
    }

    impl Store {
        /// `outcome` gets the number of the send, from 0.
        fn new(outcome: fn(usize) -> Outcome) -> Self {
            Self {
                outcome,
                sends: Mutex::default(),
                stored: Mutex::default(),
            }
        }

        /// The times of the stored events, in the order they were stored.
        fn stored_times(&self) -> Vec<i64> {
            let stored = self.stored.lock().unwrap();
            stored.iter().map(|e| e.event.timestamp).collect()
        }

        fn send_sizes(&self) -> Vec<usize> {
            let sends = self.sends.lock().unwrap();
            sends.iter().map(|&(_, size)| size).collect()
        }

        fn send_times(&self) -> Vec<Instant> {
            let sends = self.sends.lock().unwrap();
            sends.iter().map(|&(at, _)| at).collect()
        }
    }

    impl EventStore for Store {
        async fn store_events_with_ids(&self, events: &[EventWithId]) -> Result<()> {
            let mut sends = self.sends.lock().unwrap();
            let outcome = (self.outcome)(sends.len());
            sends.push((Instant::now(), events.len()));

            if matches!(outcome, Outcome::Failed) {
                anyhow::bail!("the store is down");
            }

            let mut stored = self.stored.lock().unwrap();
            for event in events {
                if !stored.iter().any(|kept| kept.id == event.id) {
                    stored.push(event.clone());
                }
            }

            match outcome {
                Outcome::StoredThenFailed => anyhow::bail!("the store stopped"),
                _ => Ok(()),
            }
        }
    }

    /// One event at each of `times`.
    fn events(times: Range<usize>) -> Vec<EventRecord> {
        times
            .map(|at| EventRecord {
                code_location_id: "cl".to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("a".to_string()),
                run_id: "r1".to_string(),
                partition_key: None,
                timestamp: at as i64,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .collect()
    }

    fn times(times: Range<usize>) -> Vec<i64> {
        times.map(|at| at as i64).collect()
    }

    #[tokio::test]
    async fn a_failed_send_keeps_its_events_for_the_next_send() {
        let store = Store::new(|send| match send {
            0 => Outcome::Failed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        batch.extend(events(0..10));
        assert!(batch.send(&store).await.is_err());

        batch.extend(events(10..15));
        batch.send(&store).await.unwrap();

        assert_eq!(store.stored_times(), times(0..15));
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn a_send_after_one_that_stored_and_then_failed_stores_each_event_once() {
        let store = Store::new(|send| match send {
            0 => Outcome::StoredThenFailed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        batch.extend(events(0..10));
        assert!(batch.send(&store).await.is_err());

        batch.send(&store).await.unwrap();

        assert_eq!(store.send_sizes(), [10, 10]);
        assert_eq!(store.stored_times(), times(0..10));
    }

    #[tokio::test]
    async fn a_kept_batch_is_sent_oldest_first_in_chunks() {
        let store = Store::new(|send| match send {
            1 => Outcome::Failed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        let count = 3 * BATCH_SIZE + 8;
        batch.extend(events(0..count));

        assert!(batch.send(&store).await.is_err());
        assert_eq!(store.send_sizes(), [BATCH_SIZE, BATCH_SIZE]);
        assert_eq!(store.stored_times(), times(0..BATCH_SIZE));
        assert_eq!(batch.len(), count - BATCH_SIZE);

        batch.send(&store).await.unwrap();
        assert_eq!(
            store.send_sizes(),
            [BATCH_SIZE, BATCH_SIZE, BATCH_SIZE, BATCH_SIZE, 8]
        );
        assert_eq!(store.stored_times(), times(0..count));
        assert!(batch.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn after_a_failed_send_the_next_send_that_no_caller_waits_for_waits_the_pause() {
        let store = Store::new(|send| match send {
            0 => Outcome::Failed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        batch.extend(events(0..BATCH_SIZE));
        assert!(batch.send(&store).await.is_err());

        batch.send_unless_paused(&store).await;
        tokio::time::sleep(PAUSE_AFTER_FAILURE - Duration::from_millis(1)).await;
        batch.send_unless_paused(&store).await;
        assert_eq!(store.send_sizes(), [BATCH_SIZE]);

        tokio::time::sleep(Duration::from_millis(1)).await;
        batch.send_unless_paused(&store).await;
        assert_eq!(store.send_sizes(), [BATCH_SIZE, BATCH_SIZE]);
        assert!(batch.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn after_a_failed_send_a_send_that_a_caller_waits_for_goes_at_once() {
        let store = Store::new(|send| match send {
            0 => Outcome::Failed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        batch.extend(events(0..1));
        assert!(batch.send(&store).await.is_err());

        batch.extend(events(1..2));
        batch.send(&store).await.unwrap();

        let sent_at = store.send_times();
        assert_eq!(sent_at[0], sent_at[1]);
        assert_eq!(store.stored_times(), times(0..2));
    }

    #[tokio::test(start_paused = true)]
    async fn the_final_send_tries_again_until_the_store_comes_back() {
        let store = Store::new(|send| match send {
            0..3 => Outcome::Failed,
            _ => Outcome::Stored,
        });
        let mut batch = EventBatch::default();
        batch.extend(events(0..10));
        let started = Instant::now();

        batch.send_until_stored(&store).await.unwrap();

        assert_eq!(started.elapsed(), 3 * PAUSE_AFTER_FAILURE);
        assert_eq!(store.stored_times(), times(0..10));
        assert!(batch.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn the_final_send_gives_up_after_the_limit_with_a_pause_between_sends() {
        let store = Store::new(|_| Outcome::Failed);
        let mut batch = EventBatch::default();
        batch.extend(events(0..10));
        let started = Instant::now();

        let error = batch.send_until_stored(&store).await.unwrap_err();

        let sent_at = store.send_times();
        assert_eq!(
            sent_at.len() as u32,
            FINAL_SEND_LIMIT.div_duration_f64(PAUSE_AFTER_FAILURE) as u32
        );
        for pair in sent_at.windows(2) {
            assert_eq!(pair[1] - pair[0], PAUSE_AFTER_FAILURE);
        }
        assert_eq!(started.elapsed(), FINAL_SEND_LIMIT - PAUSE_AFTER_FAILURE);
        assert_eq!(batch.len(), 10);
        assert!(
            format!("{error:#}").contains("the store is down"),
            "{error:#}"
        );
    }
}
