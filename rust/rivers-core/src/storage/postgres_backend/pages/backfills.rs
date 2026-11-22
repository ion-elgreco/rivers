//! The pages of backfills.

use super::super::backfills::{
    BACKFILL_COLUMNS, backfill_from_row, backfill_status_text, load_backfill_progress,
};
use super::*;

/// The `WHERE` clause of the backfills of `filter`.
fn push_backfill_filter(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    filter: &BackfillFilter,
) {
    query.push(" WHERE true");
    if let Some(cl) = code_location_id {
        query.push(" AND code_location_id = ").push_bind(cl);
    }
    if let Some(status) = &filter.status {
        query
            .push(" AND status = ")
            .push_bind(backfill_status_text(status));
    }
}

/// One page of the backfills of `filter`, newest first.
pub(super) fn push_backfills_page(
    query: &mut QueryBuilder<Postgres>,
    code_location_id: Option<&str>,
    offset: u64,
    limit: u64,
    filter: &BackfillFilter,
) {
    query.push(format_args!("SELECT {BACKFILL_COLUMNS} FROM backfills"));
    push_backfill_filter(query, code_location_id, filter);
    query.push(" ORDER BY create_time DESC, backfill_id COLLATE \"C\" DESC");
    push_limit(query, offset, limit);
}

impl PostgresStorage {
    pub async fn get_all_backfills_page(
        &self,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page(None, offset, limit, filter).await
    }

    pub async fn get_backfills_page(
        &self,
        code_location_id: &str,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        self.backfills_page(Some(code_location_id), offset, limit, filter)
            .await
    }

    async fn backfills_page(
        &self,
        code_location_id: Option<&str>,
        offset: u64,
        limit: u64,
        filter: &BackfillFilter,
    ) -> Result<BackfillsPage> {
        let mut page = QueryBuilder::new("");
        push_backfills_page(&mut page, code_location_id, offset, limit, filter);
        let mut count = QueryBuilder::new("SELECT count(*) FROM backfills");
        push_backfill_filter(&mut count, code_location_id, filter);
        let (rows, total) = tokio::try_join!(
            page.build().fetch_all(&self.pool),
            count.build_query_scalar::<i64>().fetch_one(&self.pool),
        )?;
        let backfills = rows.iter().map(backfill_from_row).collect::<Result<_>>()?;
        Ok(BackfillsPage {
            rows: load_backfill_progress(&self.pool, backfills).await?,
            total: total as u64,
        })
    }

    pub async fn get_all_backfills_summary(&self) -> Result<BackfillsSummary> {
        self.backfills_summary(None).await
    }

    pub async fn get_backfills_summary(&self, code_location_id: &str) -> Result<BackfillsSummary> {
        self.backfills_summary(Some(code_location_id)).await
    }

    async fn backfills_summary(&self, code_location_id: Option<&str>) -> Result<BackfillsSummary> {
        let mut query = QueryBuilder::new(
            "SELECT count(*) AS total, \
                 count(*) FILTER (WHERE status = 'InProgress') AS in_progress, \
                 count(*) FILTER (WHERE status = 'CompletedSuccess') AS completed_success, \
                 count(*) FILTER (WHERE status = 'CompletedFailed') AS completed_failed, \
                 count(*) FILTER (WHERE status = 'Canceled') AS canceled \
             FROM backfills",
        );
        if let Some(cl) = code_location_id {
            query.push(" WHERE code_location_id = ").push_bind(cl);
        }
        let row = query.build().fetch_one(&self.pool).await?;
        let count = |column: &str| -> Result<u64> { Ok(row.try_get::<i64, _>(column)? as u64) };
        Ok(BackfillsSummary {
            total: count("total")?,
            in_progress: count("in_progress")?,
            completed_success: count("completed_success")?,
            completed_failed: count("completed_failed")?,
            canceled: count("canceled")?,
        })
    }
}
