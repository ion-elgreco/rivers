//! The version row of each asset, with the id `[code_location_id,
//! asset_key]`, and the versions its stored data reached. A writer that read
//! a version row writes it back only if its revision did not change in
//! between.

use std::collections::HashSet;

use anyhow::{Context, Result};
use surrealdb::types::SurrealValue;

use crate::storage::retry;
use crate::storage::{
    AssetVersionHistoryEntry, AssetVersionRecord, AssetVersionState, MigrationKind,
};

use super::*;

const SELECT_VERSIONS: &str = "\
    SELECT code_location_id, asset_key, applied_version, partitions, state, target_version, \
        reason, started_at, run_id, backfill_id, owner, lease_expires_at, revision, updated_at \
    FROM asset_versions WHERE code_location_id = $cl ORDER BY asset_key";

const CREATE_VERSION: &str =
    "CREATE type::record('asset_versions', [$cl, $asset_key]) CONTENT $row RETURN NONE";

const UPDATE_VERSION: &str = "\
    UPDATE type::record('asset_versions', [$cl, $asset_key]) CONTENT $row \
    WHERE revision = $expected RETURN VALUE revision";

const UPSERT_HISTORY: &str = "\
    UPSERT type::record('asset_version_history', [$cl, $asset_key, $version]) CONTENT $row \
    RETURN NONE";

const SELECT_HISTORY: &str = "\
    SELECT code_location_id, asset_key, version, kind, breaking, run_id, backfill_id, filled, \
        failed, finished_at \
    FROM asset_version_history WHERE code_location_id = $cl AND asset_key = $asset_key \
    ORDER BY version";

#[derive(Clone, SurrealValue)]
struct VersionRow {
    code_location_id: String,
    asset_key: String,
    applied_version: i64,
    partitions: Option<String>,
    state: String,
    target_version: Option<i64>,
    reason: Option<String>,
    started_at: Option<i64>,
    run_id: Option<String>,
    backfill_id: Option<String>,
    owner: Option<String>,
    lease_expires_at: Option<i64>,
    revision: i64,
    updated_at: i64,
}

impl VersionRow {
    fn new(
        code_location_id: &str,
        record: &AssetVersionRecord,
        revision: u64,
        updated_at: i64,
    ) -> Result<Self> {
        Ok(Self {
            code_location_id: code_location_id.to_string(),
            asset_key: record.asset_key.clone(),
            applied_version: i64::from(record.applied_version),
            partitions: record.partitions.clone(),
            state: record.state.as_str().to_string(),
            target_version: record.target_version.map(i64::from),
            reason: record.reason.clone(),
            started_at: record.started_at,
            run_id: record.run_id.clone(),
            backfill_id: record.backfill_id.clone(),
            owner: record.owner.clone(),
            lease_expires_at: record.lease_expires_at,
            revision: i64::try_from(revision)?,
            updated_at,
        })
    }

    fn into_record(self) -> Result<AssetVersionRecord> {
        Ok(AssetVersionRecord {
            applied_version: u32::try_from(self.applied_version)
                .context("applied_version of an asset version row")?,
            state: AssetVersionState::parse(&self.state)?,
            target_version: self
                .target_version
                .map(u32::try_from)
                .transpose()
                .context("target_version of an asset version row")?,
            revision: u64::try_from(self.revision).context("revision of an asset version row")?,
            code_location_id: self.code_location_id,
            asset_key: self.asset_key,
            partitions: self.partitions,
            reason: self.reason,
            started_at: self.started_at,
            run_id: self.run_id,
            backfill_id: self.backfill_id,
            owner: self.owner,
            lease_expires_at: self.lease_expires_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Clone, SurrealValue)]
struct HistoryRow {
    code_location_id: String,
    asset_key: String,
    version: i64,
    kind: String,
    breaking: bool,
    run_id: Option<String>,
    backfill_id: Option<String>,
    filled: Option<i64>,
    failed: Option<i64>,
    finished_at: i64,
}

impl HistoryRow {
    fn new(code_location_id: &str, entry: &AssetVersionHistoryEntry) -> Result<Self> {
        Ok(Self {
            code_location_id: code_location_id.to_string(),
            asset_key: entry.asset_key.clone(),
            version: i64::from(entry.version),
            kind: entry.kind.as_str().to_string(),
            breaking: entry.breaking,
            run_id: entry.run_id.clone(),
            backfill_id: entry.backfill_id.clone(),
            filled: entry.filled.map(i64::try_from).transpose()?,
            failed: entry.failed.map(i64::try_from).transpose()?,
            finished_at: entry.finished_at,
        })
    }

    fn into_entry(self) -> Result<AssetVersionHistoryEntry> {
        let count = |n: Option<i64>| n.map(u64::try_from).transpose();
        Ok(AssetVersionHistoryEntry {
            version: u32::try_from(self.version).context("version of a history row")?,
            kind: MigrationKind::parse(&self.kind)?,
            filled: count(self.filled).context("filled of a history row")?,
            failed: count(self.failed).context("failed of a history row")?,
            code_location_id: self.code_location_id,
            asset_key: self.asset_key,
            breaking: self.breaking,
            run_id: self.run_id,
            backfill_id: self.backfill_id,
            finished_at: self.finished_at,
        })
    }
}

impl SurrealStorage {
    pub(super) async fn select_asset_versions(
        &self,
        code_location_id: &str,
        asset_keys: Option<&[String]>,
    ) -> Result<Vec<AssetVersionRecord>> {
        if asset_keys.is_some_and(<[String]>::is_empty) {
            return Ok(vec![]);
        }

        let rows: Vec<VersionRow> = retry::with_retry(&self.retry_config, || async {
            let mut response = self
                .db
                .query(SELECT_VERSIONS)
                .bind(("cl", code_location_id.to_string()))
                .await?
                .check()?;
            Ok(response.take(0)?)
        })
        .await?;

        // `asset_key IN $keys` would test the list against each row of the
        // location: the rows are filtered here.
        let wanted: Option<HashSet<&str>> =
            asset_keys.map(|keys| keys.iter().map(String::as_str).collect());
        rows.into_iter()
            .filter(|row| {
                wanted
                    .as_ref()
                    .is_none_or(|wanted| wanted.contains(row.asset_key.as_str()))
            })
            .map(VersionRow::into_record)
            .collect()
    }

    pub(super) async fn write_asset_version(
        &self,
        code_location_id: &str,
        record: &AssetVersionRecord,
        expected_revision: Option<u64>,
    ) -> Result<bool> {
        let revision = expected_revision.map_or(1, |expected| expected + 1);
        let row = VersionRow::new(code_location_id, record, revision, now_nanos())?;
        let expected = expected_revision.map(i64::try_from).transpose()?;

        retry::with_retry(&self.retry_config, || async {
            let Some(expected) = expected else {
                let created: Result<()> = async {
                    self.db
                        .query(CREATE_VERSION)
                        .bind(("cl", code_location_id.to_string()))
                        .bind(("asset_key", record.asset_key.clone()))
                        .bind(("row", row.clone()))
                        .await?
                        .check()?;
                    Ok(())
                }
                .await;
                return match created {
                    Ok(()) => Ok(true),
                    Err(error) if is_already_exists_error(&error) => Ok(false),
                    Err(error) => Err(error),
                };
            };

            let mut response = self
                .db
                .query(UPDATE_VERSION)
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", record.asset_key.clone()))
                .bind(("row", row.clone()))
                .bind(("expected", expected))
                .await?
                .check()?;
            let written: Vec<i64> = response.take(0)?;
            Ok(!written.is_empty())
        })
        .await
    }

    pub(super) async fn upsert_asset_version_history(
        &self,
        code_location_id: &str,
        entry: &AssetVersionHistoryEntry,
    ) -> Result<()> {
        let row = HistoryRow::new(code_location_id, entry)?;

        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(UPSERT_HISTORY)
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", entry.asset_key.clone()))
                .bind(("version", row.version))
                .bind(("row", row.clone()))
                .await?
                .check()?;
            Ok(())
        })
        .await
    }

    pub(super) async fn select_asset_version_history(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<AssetVersionHistoryEntry>> {
        let rows: Vec<HistoryRow> = retry::with_retry(&self.retry_config, || async {
            let mut response = self
                .db
                .query(SELECT_HISTORY)
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .await?
                .check()?;
            Ok(response.take(0)?)
        })
        .await?;

        rows.into_iter().map(HistoryRow::into_entry).collect()
    }
}
