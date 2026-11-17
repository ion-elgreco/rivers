//! Backfills: the header row, the requested keys in pages, and the steps
//! from `Requested` to a final status. The runs are in `backfill_runs`.

use std::collections::HashMap;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::types::SurrealValue;

use crate::storage::retry;
use crate::storage::{BackfillRecord, BackfillStatus, KeyItem, PartitionKey};

use super::*;

/// Largest size of one page of requested keys, in bytes of key text.
const KEY_PAGE_BYTES: usize = 256 * 1024;

/// Split the requested keys of a backfill into pages of at most about
/// [`KEY_PAGE_BYTES`], in order. A key larger than a page is a page of its
/// own. There is always a page 0.
fn key_pages(keys: &[PartitionKey]) -> Vec<Vec<PartitionKey>> {
    let mut pages: Vec<Vec<PartitionKey>> = vec![Vec::new()];
    let mut page_bytes = 0usize;

    for key in keys {
        let bytes = key_bytes(key);
        let page = pages.last_mut().expect("there is always a page");
        if !page.is_empty() && page_bytes + bytes > KEY_PAGE_BYTES {
            pages.push(vec![key.clone()]);
            page_bytes = bytes;
        } else {
            page.push(key.clone());
            page_bytes += bytes;
        }
    }
    pages
}

/// About the number of bytes `key` takes in a stored row.
fn key_bytes(key: &PartitionKey) -> usize {
    const PER_KEY: usize = 32;
    const PER_VALUE: usize = 4;
    const PER_SPAN: usize = 160;

    let values = |values: &[String]| -> usize { values.iter().map(|v| v.len() + PER_VALUE).sum() };
    let items = |items: &[KeyItem]| -> usize {
        items
            .iter()
            .map(|item| match item {
                KeyItem::Value(value) => value.len() + PER_VALUE,
                KeyItem::Span(_) => PER_SPAN,
            })
            .sum()
    };
    match key {
        PartitionKey::Single { keys } => PER_KEY + values(keys),
        PartitionKey::Multi { dims } => {
            PER_KEY
                + dims
                    .iter()
                    .map(|(name, vals)| name.len() + PER_VALUE + values(vals))
                    .sum::<usize>()
        }
        PartitionKey::Set { keys } => PER_KEY + keys.iter().map(key_bytes).sum::<usize>(),
        PartitionKey::SingleItems { items: held } => PER_KEY + items(held),
        PartitionKey::MultiItems { dims } => {
            PER_KEY
                + dims
                    .iter()
                    .map(|(name, held)| name.len() + PER_VALUE + items(held))
                    .sum::<usize>()
        }
    }
}

/// Fill in the linked runs and the counts of `backfills`, read from their own
/// tables.
pub(super) async fn load_backfill_progress(
    db: &Surreal<Any>,
    mut backfills: Vec<BackfillRecord>,
) -> Result<Vec<BackfillRecord>> {
    #[derive(SurrealValue)]
    struct RunRow {
        backfill_id: String,
        run_id: String,
    }
    #[derive(SurrealValue)]
    struct CountRow {
        backfill_id: String,
        outcome: String,
        members: u64,
        #[surreal(default)]
        failed_members: u64,
    }

    if backfills.is_empty() {
        return Ok(backfills);
    }
    let ids: Vec<String> = backfills.iter().map(|b| b.backfill_id.clone()).collect();
    let mut result = db
        .query(LOAD_BACKFILL_PROGRESS)
        .bind(("ids", ids))
        .await?
        .check()?;
    let runs: Vec<RunRow> = result.take(0)?;
    let per_run: Vec<CountRow> = result.take(1)?;
    let per_partition: Vec<CountRow> = result.take(2)?;

    let index: HashMap<String, usize> = backfills
        .iter()
        .enumerate()
        .map(|(i, b)| (b.backfill_id.clone(), i))
        .collect();
    for row in runs {
        if let Some(&i) = index.get(&row.backfill_id) {
            backfills[i].run_ids.push(row.run_id);
        }
    }
    for row in per_run.into_iter().chain(per_partition) {
        let Some(&i) = index.get(&row.backfill_id) else {
            continue;
        };
        let backfill = &mut backfills[i];
        match row.outcome.as_str() {
            "Completed" => {
                backfill.completed_partitions += row.members.saturating_sub(row.failed_members);
                backfill.failed_partitions += row.failed_members;
            }
            "Failed" => backfill.failed_partitions += row.members,
            "Canceled" => backfill.canceled_partitions += row.members,
            other => anyhow::bail!("unknown backfill outcome '{other}'"),
        }
    }
    Ok(backfills)
}

impl SurrealStorage {
    /// Key pages go in before the header, so a backfill that can be read has
    /// all its keys. The last page goes in with the header.
    pub(super) async fn create_backfill_rows(
        &self,
        backfill: &BackfillRecord,
        partition_keys: &[PartitionKey],
    ) -> Result<()> {
        let mut row = backfill.clone();
        row.num_partitions = partition_keys
            .iter()
            .map(|key| key.member_count() as u64)
            .sum();

        let mut pages = key_pages(partition_keys);
        let last_page = pages.pop().unwrap_or_default();
        const WRITE_KEY_PAGE: &str = "INSERT INTO backfill_key_pages \
             { backfill_id: $id, page: $page, partition_keys: $keys } \
             ON DUPLICATE KEY UPDATE partition_keys = $input.partition_keys";

        for (page, keys) in pages.iter().enumerate() {
            retry::with_retry(&self.retry_config, || async {
                self.db
                    .query(WRITE_KEY_PAGE)
                    .bind(("id", row.backfill_id.clone()))
                    .bind(("page", page))
                    .bind(("keys", keys.clone()))
                    .await
                    .context("failed to store the requested keys of the backfill")?
                    .check()?;
                Ok(())
            })
            .await?;
        }

        let result = retry::with_retry(&self.retry_config, || async {
            self.db
                .query(format!(
                    "BEGIN TRANSACTION;\n\
                     CREATE backfills CONTENT object::remove($backfill, {BACKFILL_PROGRESS_COLUMNS});\n\
                     {WRITE_KEY_PAGE};\n\
                     COMMIT TRANSACTION;"
                ))
                .bind(("backfill", row.clone()))
                .bind(("id", row.backfill_id.clone()))
                .bind(("page", pages.len()))
                .bind(("keys", last_page.clone()))
                .await
                .context("failed to create backfill")?
                .check()?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_backfill", &row.backfill_id)
    }

    /// One page per read, so no message holds more than a page of keys.
    pub(super) async fn read_backfill_keys(&self, backfill_id: &str) -> Result<Vec<PartitionKey>> {
        let mut keys: Vec<PartitionKey> = Vec::new();

        for page in 0usize.. {
            let rows: Vec<Vec<PartitionKey>> = retry::with_retry(&self.retry_config, || async {
                let mut result = self
                    .db
                    .query(
                        "SELECT VALUE partition_keys FROM backfill_key_pages \
                         WHERE backfill_id = $id AND page = $page",
                    )
                    .bind(("id", backfill_id.to_string()))
                    .bind(("page", page))
                    .await?
                    .check()?;
                Ok(result.take(0)?)
            })
            .await?;

            match rows.into_iter().next() {
                Some(page_keys) => keys.extend(page_keys),
                None => break,
            }
        }
        Ok(keys)
    }

    pub(super) async fn read_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM backfills WHERE backfill_id = $id LIMIT 1")
                .bind(("id", backfill_id.to_string()))
                .await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            Ok(load_backfill_progress(&self.db, rows).await?.pop())
        })
        .await
    }

    pub(super) async fn backfill_status(
        &self,
        backfill_id: &str,
    ) -> Result<Option<BackfillStatus>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT VALUE status FROM backfills WHERE backfill_id = $id")
                .bind(("id", backfill_id.to_string()))
                .await?
                .check()?;
            let rows: Vec<BackfillStatus> = result.take(0)?;
            Ok(rows.into_iter().next())
        })
        .await
    }

    pub(super) async fn start_backfill_row(
        &self,
        backfill_id: &str,
        planned_runs: usize,
    ) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET status = 'InProgress', planned_runs = $planned_runs \
                     WHERE backfill_id = $id AND status = 'Requested' RETURN VALUE backfill_id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("planned_runs", planned_runs))
                .await?
                .check()?;
            let started: Vec<String> = result.take(0)?;
            Ok(!started.is_empty())
        })
        .await
    }

    pub(super) async fn complete_backfill(
        &self,
        backfill_id: &str,
    ) -> Result<Option<BackfillStatus>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(COMPLETE_BACKFILL)
                .bind(("id", backfill_id.to_string()))
                .bind(("now", now_nanos()))
                .await?
                .check()?;
            let last = result.num_statements() - 1;
            let status: Option<BackfillStatus> = result.take(last)?;
            Ok(status)
        })
        .await
    }

    pub(super) async fn cancel_backfill_row(&self, backfill_id: &str) -> Result<BackfillStatus> {
        // Settle first: if every run already finished, the cancel prevented
        // nothing and the derived outcome wins.
        if let Some(settled) = self.complete_backfill(backfill_id).await? {
            return Ok(settled);
        }
        let now = now_nanos();
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'Canceled', end_time = $now \
                     WHERE backfill_id = $id AND status IN ['Requested', 'InProgress']",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("now", now))
                .await?
                .check()?;
            Ok(())
        })
        .await?;
        self.backfill_status(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))
    }

    /// Flip a zero-run `InProgress` backfill back to `Requested` so the
    /// pickup loop re-executes it (guarded — a backfill that gained runs or
    /// moved on is left alone). Returns whether the flip applied.
    pub async fn resume_stalled_backfill(&self, backfill_id: &str) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET status = 'Requested' \
                         WHERE backfill_id = $id AND status = 'InProgress' \
                         AND (SELECT VALUE run_id FROM backfill_runs \
                             WHERE backfill_id = $id LIMIT 1) = [] \
                         AND (SELECT VALUE run_number FROM backfill_run_outcomes \
                             WHERE backfill_id = $id LIMIT 1) = [] \
                         RETURN VALUE backfill_id",
                )
                .bind(("id", backfill_id.to_string()))
                .await?;
            let flipped: Vec<String> = result.take(0)?;
            Ok(!flipped.is_empty())
        })
        .await
    }

    /// Mark a backfill `CompletedFailed` with the submission error recorded.
    /// A backfill that already ended keeps its status.
    pub async fn fail_backfill(&self, backfill_id: &str, error: &str) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'CompletedFailed', \
                         end_time = $end_time, error = $error \
                         WHERE backfill_id = $id AND status IN ['Requested', 'InProgress']",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("end_time", now_nanos()))
                .bind(("error", error.to_string()))
                .await?
                .check()?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(values: &[&str]) -> PartitionKey {
        PartitionKey::Single {
            keys: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    #[test]
    fn no_keys_are_one_empty_page() {
        assert_eq!(key_pages(&[]), vec![Vec::<PartitionKey>::new()]);
    }

    #[test]
    fn small_keys_share_a_page_in_order() {
        let keys = vec![single(&["a"]), single(&["b"]), single(&["c"])];
        assert_eq!(key_pages(&keys), vec![keys]);
    }

    #[test]
    fn keys_past_the_page_size_start_a_new_page() {
        let value = "v".repeat(KEY_PAGE_BYTES / 4);
        let keys: Vec<PartitionKey> = (0..9).map(|_| single(&[&value])).collect();

        let pages = key_pages(&keys);

        assert_eq!(pages.len(), 3);
        assert_eq!(pages.concat(), keys);
        assert!(pages.iter().all(|page| page.len() == 3));
    }

    #[test]
    fn a_key_larger_than_a_page_is_a_page_of_its_own() {
        let big = single(&["v".repeat(2 * KEY_PAGE_BYTES).as_str()]);
        let keys = vec![single(&["a"]), big.clone(), single(&["b"])];

        assert_eq!(
            key_pages(&keys),
            vec![vec![single(&["a"])], vec![big], vec![single(&["b"])]]
        );
    }
}
