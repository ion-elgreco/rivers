//! PostgreSQL backend: concurrency pools, slot claims and leases.
//!
//! The writes of the slot rows of a step go through `self.retry`: a claim
//! and a free or a renewal of the same step lock its rows in different
//! orders, and the server breaks such a deadlock by failing one of them.

use sqlx::postgres::PgRow;

use super::*;

mod claim;
#[cfg(test)]
mod tests;

use claim::Claim;

impl PostgresStorage {
    pub(super) async fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO concurrency_pools \
                 (code_location_id, pool_key, slot_limit, lease_duration_secs) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (code_location_id, pool_key) DO UPDATE SET \
                 slot_limit = EXCLUDED.slot_limit, \
                 lease_duration_secs = EXCLUDED.lease_duration_secs",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .bind(limit)
        .bind(i64::from(lease_duration_secs))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn get_pool_limits(&self, code_location_id: &str) -> Result<Vec<PoolLimit>> {
        let rows = sqlx::query(
            "SELECT code_location_id, pool_key, slot_limit, lease_duration_secs \
             FROM concurrency_pools WHERE code_location_id = $1 \
             ORDER BY pool_key COLLATE \"C\" ASC",
        )
        .bind(code_location_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(PoolLimit {
                    code_location_id: row.try_get("code_location_id")?,
                    pool_key: row.try_get("pool_key")?,
                    slot_limit: row.try_get("slot_limit")?,
                    lease_duration_secs: row.try_get::<i64, _>("lease_duration_secs")? as u32,
                })
            })
            .collect()
    }

    pub(super) async fn get_pool_info(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<PoolInfo> {
        self.pool_infos(code_location_id, Some(pool_key))
            .await?
            .pop()
            .with_context(|| format!("pool '{pool_key}' not configured"))
    }

    pub(super) async fn get_all_pool_infos(&self, code_location_id: &str) -> Result<Vec<PoolInfo>> {
        self.pool_infos(code_location_id, None).await
    }

    /// The pools of a code location (or the one named `pool_key`) with the
    /// slots of their live leases and their waiting steps.
    async fn pool_infos(
        &self,
        code_location_id: &str,
        pool_key: Option<&str>,
    ) -> Result<Vec<PoolInfo>> {
        let rows = sqlx::query(
            "SELECT p.pool_key, p.slot_limit, p.lease_duration_secs, \
                 (SELECT COALESCE(sum(s.slots_consumed), 0) FROM concurrency_slots s \
                     WHERE s.code_location_id = p.code_location_id \
                     AND s.pool_key = p.pool_key AND s.lease_expires_at > $2)::bigint AS claimed, \
                 (SELECT count(*) FROM pending_steps q \
                     WHERE q.code_location_id = p.code_location_id \
                     AND q.pool_key = p.pool_key) AS pending \
             FROM concurrency_pools p \
             WHERE p.code_location_id = $1 AND ($3::text IS NULL OR p.pool_key = $3) \
             ORDER BY p.pool_key COLLATE \"C\" ASC",
        )
        .bind(code_location_id)
        .bind(now_nanos())
        .bind(pool_key)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(PoolInfo {
                    pool_key: row.try_get("pool_key")?,
                    slot_limit: row.try_get("slot_limit")?,
                    lease_duration_secs: row.try_get::<i64, _>("lease_duration_secs")? as u32,
                    claimed_count: row.try_get::<i64, _>("claimed")? as u32,
                    pending_count: row.try_get::<i64, _>("pending")? as u32,
                })
            })
            .collect()
    }

    pub(super) async fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
        scope: Option<&AssetScope>,
    ) -> Result<ConcurrencyClaimStatus> {
        anyhow::ensure!(!pools.is_empty(), "pools must not be empty");
        let claim = Claim {
            code_location_id,
            run_id,
            step_key,
            scope,
        };
        self.retry(|| claim.once(self, pools, priority, lease_duration_secs))
            .await
    }

    pub(super) async fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<Vec<SlotHolder>> {
        let rows = sqlx::query(
            "SELECT run_id, step_key, slots_consumed, claimed_at, lease_expires_at \
             FROM concurrency_slots \
             WHERE code_location_id = $1 AND pool_key = $2 AND lease_expires_at > $3 \
             ORDER BY claimed_at ASC",
        )
        .bind(code_location_id)
        .bind(pool_key)
        .bind(now_nanos())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(slot_holder_from_row).collect()
    }

    pub(super) async fn free_concurrency_slots(&self, run_id: &str, step_key: &str) -> Result<()> {
        self.retry(|| async {
            sqlx::query(
                "WITH slots AS ( \
                     DELETE FROM concurrency_slots WHERE run_id = $1 AND step_key = $2) \
                 DELETE FROM pending_steps WHERE run_id = $1 AND step_key = $2",
            )
            .bind(run_id)
            .bind(step_key)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }

    pub(super) async fn free_concurrency_slots_for_run(&self, run_id: &str) -> Result<()> {
        self.retry(|| async {
            sqlx::query(
                "WITH slots AS (DELETE FROM concurrency_slots WHERE run_id = $1) \
                 DELETE FROM pending_steps WHERE run_id = $1",
            )
            .bind(run_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }

    pub(super) async fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> Result<u32> {
        self.retry(|| async {
            let now = now_nanos();
            let renewed = sqlx::query(
                "UPDATE concurrency_slots SET lease_expires_at = $1, last_heartbeat = $2 \
                 WHERE run_id = $3 AND step_key = $4",
            )
            .bind(now + i64::from(lease_duration_secs) * 1_000_000_000)
            .bind(now)
            .bind(run_id)
            .bind(step_key)
            .execute(&self.pool)
            .await?;
            Ok(renewed.rows_affected() as u32)
        })
        .await
    }

    pub(super) async fn free_expired_leases(&self) -> Result<u32> {
        self.retry(|| async {
            let freed = sqlx::query("DELETE FROM concurrency_slots WHERE lease_expires_at <= $1")
                .bind(now_nanos())
                .execute(&self.pool)
                .await?;
            Ok(freed.rows_affected() as u32)
        })
        .await
    }
}

fn slot_holder_from_row(row: &PgRow) -> Result<SlotHolder> {
    Ok(SlotHolder {
        run_id: row.try_get("run_id")?,
        step_key: row.try_get("step_key")?,
        slots_consumed: row.try_get::<i64, _>("slots_consumed")? as u32,
        claimed_at: row.try_get("claimed_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
    })
}
