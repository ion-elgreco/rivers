//! PostgreSQL backend: the event write path.
//!
//! One send of events is one call of `rivers_store_events`
//! (`schema/events.sql`): it stores the events and applies what they change
//! in one transaction. Runs that store materializations of one asset at the
//! same time share its lock, and wait for each other only on its asset row,
//! which a send updates last.

use std::borrow::Cow;

use surrealdb::types::{RecordId, RecordIdKey};

use super::*;
use crate::storage::surrealdb_backend::new_event_record_id;

#[cfg(test)]
mod lock_tests;
#[cfg(test)]
pub(super) mod tests;

const STORE_EVENTS: &str = "SELECT rivers_store_events($1, $2, $3, $4, $5, $6, $7, $8, $9, \
     $10, $11, $12, $13, $14, $15, $16, $17, $18)";

/// The SQLSTATE of a send that locked an asset shared and then found a
/// failure row of its partitions: it runs again with the assets alone.
const FAILURE_ROWS_CAME_IN: &str = "RV001";

impl PostgresStorage {
    pub(super) async fn store_event(&self, event: &EventRecord) -> Result<String> {
        let id = new_event_record_id();
        self.store(&[(event, &id)], true)
            .await
            .context("failed to store event")?;
        Ok(record_id_str(&id))
    }

    pub(super) async fn store_events(&self, events: &[EventRecord]) -> Result<Vec<String>> {
        let ids: Vec<RecordId> = events.iter().map(|_| new_event_record_id()).collect();
        let batch: Vec<(&EventRecord, &RecordId)> = events.iter().zip(&ids).collect();
        self.store(&batch, false)
            .await
            .context("failed to batch store events")?;
        Ok(ids.iter().map(record_id_str).collect())
    }

    pub(super) async fn store_events_with_ids(&self, events: &[EventWithId]) -> Result<()> {
        let batch: Vec<(&EventRecord, &RecordId)> =
            events.iter().map(|e| (&e.event, &e.id)).collect();
        self.store(&batch, false)
            .await
            .context("failed to batch store events")
    }

    /// Store `batch` and apply its effects. A send again with the same ids
    /// stores each event once. `single`: the one event of `store_event`.
    async fn store(&self, batch: &[(&EventRecord, &RecordId)], single: bool) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let send = Send::new(batch)?;
        // The writes of this backend lock rows in one order, but a send of
        // several assets can still meet a write of their asset rows from
        // another area in a deadlock.
        self.retry(|| async {
            match self.send(&send, single, false).await {
                Err(e) if failure_rows_came_in(&e) => self.send(&send, single, true).await,
                done => done,
            }
        })
        .await
    }

    async fn send(&self, send: &Send<'_>, single: bool, alone: bool) -> Result<()> {
        sqlx::query(STORE_EVENTS)
            .bind(&send.id)
            .bind(&send.reference)
            .bind(&send.cl)
            .bind(&send.event_type)
            .bind(&send.asset)
            .bind(&send.run)
            .bind(&send.key)
            .bind(&send.ord)
            .bind(&send.timestamp)
            .bind(&send.sort_order)
            .bind(&send.metadata)
            .bind(&send.data_version)
            .bind(&send.inputs)
            .bind(&send.member_of)
            .bind(&send.member_ord)
            .bind(&send.member_key)
            .bind(single)
            .bind(alone)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// The arguments of `rivers_store_events`: one entry per event, and one per
/// member of the partition of each materialization and step failure of an
/// asset.
#[derive(Default)]
struct Send<'a> {
    id: Vec<Cow<'a, str>>,
    reference: Vec<Cow<'a, str>>,
    cl: Vec<Cow<'a, str>>,
    event_type: Vec<&'static str>,
    asset: Vec<Option<Cow<'a, str>>>,
    run: Vec<Cow<'a, str>>,
    key: Vec<Option<serde_json::Value>>,
    ord: Vec<Option<Vec<u8>>>,
    timestamp: Vec<i64>,
    sort_order: Vec<i64>,
    metadata: Vec<serde_json::Value>,
    data_version: Vec<Option<Cow<'a, str>>>,
    inputs: Vec<serde_json::Value>,
    member_of: Vec<i32>,
    member_ord: Vec<Vec<u8>>,
    member_key: Vec<serde_json::Value>,
}

impl<'a> Send<'a> {
    fn new(batch: &[(&'a EventRecord, &'a RecordId)]) -> Result<Self> {
        let mut send = Send::default();
        for (place, &(event, id)) in (1..).zip(batch) {
            let key = event.partition_key.as_ref();
            send.id.push(storable(event_key(id)?));
            send.reference.push(storable(record_id_str(id)));
            send.cl.push(storable(&event.code_location_id));
            send.event_type.push(event.event_type.type_name());
            send.asset.push(event.asset_key.as_deref().map(storable));
            send.run.push(storable(&event.run_id));
            send.key
                .push(key.map(key_json).transpose()?.map(storable_json));
            send.ord.push(key.map(partition_ord));
            send.timestamp.push(event.timestamp);
            send.sort_order.push(event.event_type.sort_order());
            send.metadata.push(storable_json(to_json(&event.metadata)?));
            send.data_version
                .push(event.event_type.data_version().map(storable));
            send.inputs
                .push(storable_json(to_json(&event.input_data_versions)?));

            let has_members =
                event.event_type.is_materialization() || event.event_type == EventType::StepFailure;
            if let (true, Some(_), Some(key)) = (has_members, &event.asset_key, key) {
                for member in key.members() {
                    send.member_of.push(place);
                    send.member_ord.push(partition_ord(&member));
                    send.member_key.push(storable_json(key_json(&member)?));
                }
            }
        }
        Ok(send)
    }
}

/// PostgreSQL text and jsonb hold no NUL: it becomes U+FFFD, the replacement character.
pub(super) fn storable<'a>(text: impl Into<Cow<'a, str>>) -> Cow<'a, str> {
    let text = text.into();
    if text.contains('\0') {
        Cow::Owned(text.replace('\0', "\u{FFFD}"))
    } else {
        text
    }
}

/// Make each string and each field name of `value` [`storable`]. A value
/// without a NUL stays as it is.
fn make_storable(value: &mut serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::String(text) => {
            if let Cow::Owned(stored) = storable(text.as_str()) {
                *text = stored;
            }
        }
        Value::Array(items) => items.iter_mut().for_each(make_storable),
        Value::Object(fields) => {
            if fields.keys().any(|name| name.contains('\0')) {
                *fields = std::mem::take(fields)
                    .into_iter()
                    .map(|(name, field)| (storable(name).into_owned(), field))
                    .collect();
            }
            fields.values_mut().for_each(make_storable);
        }
        _ => {}
    }
}

fn storable_json(mut value: serde_json::Value) -> serde_json::Value {
    make_storable(&mut value);
    value
}

/// The text of an event id: the key of its row.
fn event_key(id: &RecordId) -> Result<&str> {
    match &id.key {
        RecordIdKey::String(key) => Ok(key),
        other => anyhow::bail!("an event id has a text key, not {other:?}"),
    }
}

fn failure_rows_came_in(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|e| e.downcast_ref::<sqlx::Error>())
        .and_then(|e| e.as_database_error())
        .and_then(|e| e.code())
        .is_some_and(|code| code == FAILURE_ROWS_CAME_IN)
}
