//! The pages of events: the Events tab of an asset and the event views of a
//! run.

use super::super::events::{EVENT_COLUMNS, event_from_row};
use super::*;

/// The order of the events of a run, oldest first.
const OLDEST_FIRST: &str = " ORDER BY timestamp, sort_order, id COLLATE \"C\"";

/// The order of the events of an asset, newest first.
const NEWEST_FIRST: &str = " ORDER BY timestamp DESC, sort_order DESC, id COLLATE \"C\" DESC";

/// One page of the events of an asset of the types `event_types`, newest
/// first. With one type the page reads the events of that type only, in
/// time order.
pub(super) fn push_asset_events_page(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: &str,
    asset_key: &str,
    event_types: &[String],
    offset: u64,
    limit: u64,
) {
    query
        .push(format_args!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE code_location_id = "
        ))
        .push_bind(code_location_id)
        .push(" AND asset_key = ")
        .push_bind(asset_key);
    match event_types {
        [event_type] => query
            .push(" AND event_type = ")
            .push_bind(event_type.as_str()),
        _ => query
            .push(" AND event_type = ANY(")
            .push_bind(event_types)
            .push(")"),
    };
    query.push(NEWEST_FIRST);
    push_limit(query, offset, limit);
}

/// The events of an asset of each of `event_types` that it has.
pub(super) fn push_count_asset_events(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: &str,
    asset_key: &str,
    event_types: &[String],
) {
    query
        .push("SELECT event_type, count(*) FROM events WHERE code_location_id = ")
        .push_bind(code_location_id)
        .push(" AND asset_key = ")
        .push_bind(asset_key)
        .push(" AND event_type = ANY(")
        .push_bind(event_types)
        .push(") GROUP BY event_type");
}

/// The step events of a run, oldest first.
pub(super) fn push_run_step_events(query: &mut QueryBuilder<Postgres>, run_id: &str) {
    query
        .push(format_args!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE run_id = "
        ))
        .push_bind(run_id)
        .push(" AND event_type IN ('StepStart', 'StepSuccess', 'StepFailure')")
        .push(OLDEST_FIRST);
}

/// The `WHERE` clause of the events of a run, of one asset and of one type
/// when given.
fn push_run_events_filter(
    query: &mut QueryBuilder<Postgres>,
    run_id: &str,
    asset_key: Option<&str>,
    event_type: Option<&str>,
) {
    query.push(" WHERE run_id = ").push_bind(run_id);
    if let Some(asset_key) = asset_key {
        query.push(" AND asset_key = ").push_bind(asset_key);
    }
    if let Some(event_type) = event_type {
        query.push(" AND event_type = ").push_bind(event_type);
    }
}

/// One page of the events of a run, oldest first.
pub(super) fn push_run_events_page(
    query: &mut QueryBuilder<Postgres>,
    run_id: &str,
    asset_key: Option<&str>,
    event_type: Option<&str>,
    offset: u64,
    limit: u64,
) {
    query.push(format_args!("SELECT {EVENT_COLUMNS} FROM events"));
    push_run_events_filter(query, run_id, asset_key, event_type);
    query.push(OLDEST_FIRST);
    push_limit(query, offset, limit);
}

fn events_from_rows(rows: &[sqlx::postgres::PgRow]) -> Result<Vec<StoredEvent>> {
    rows.iter().map(event_from_row).collect()
}

impl PostgresStorage {
    pub async fn get_events_for_asset_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        event_types: &[String],
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        if event_types.is_empty() {
            return Ok((vec![], 0));
        }
        let mut count = QueryBuilder::new("");
        push_count_asset_events(&mut count, code_location_id, asset_key, event_types);
        let counts: HashMap<String, i64> = count
            .build_query_as::<(String, i64)>()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .collect();
        let count_of = |event_type: &String| counts.get(event_type).copied().unwrap_or(0) as u64;
        let total: u64 = event_types.iter().map(count_of).sum();
        if offset >= total {
            return Ok((vec![], total));
        }

        let found: Vec<String> = event_types
            .iter()
            .filter(|event_type| count_of(event_type) > 0)
            .cloned()
            .collect();
        let mut page = QueryBuilder::new("");
        push_asset_events_page(
            &mut page,
            code_location_id,
            asset_key,
            &found,
            offset,
            limit,
        );
        let rows = page.build().fetch_all(&self.pool).await?;
        Ok((events_from_rows(&rows)?, total))
    }

    pub async fn get_run_step_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        let mut query = QueryBuilder::new("");
        push_run_step_events(&mut query, run_id);
        events_from_rows(&query.build().fetch_all(&self.pool).await?)
    }

    pub async fn get_run_structured_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        self.run_events_page(run_id, asset_key, None, offset, limit)
            .await
    }

    pub async fn get_run_asset_events_page(
        &self,
        run_id: &str,
        asset_key: &str,
        event_type: &str,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        self.run_events_page(run_id, Some(asset_key), Some(event_type), offset, limit)
            .await
    }

    async fn run_events_page(
        &self,
        run_id: &str,
        asset_key: Option<&str>,
        event_type: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<StoredEvent>, u64)> {
        let mut page = QueryBuilder::new("");
        push_run_events_page(&mut page, run_id, asset_key, event_type, offset, limit);
        let mut count = QueryBuilder::new("SELECT count(*) FROM events");
        push_run_events_filter(&mut count, run_id, asset_key, event_type);
        let (rows, total) = tokio::try_join!(
            page.build().fetch_all(&self.pool),
            count.build_query_scalar::<i64>().fetch_one(&self.pool),
        )?;
        Ok((events_from_rows(&rows)?, total as u64))
    }
}
