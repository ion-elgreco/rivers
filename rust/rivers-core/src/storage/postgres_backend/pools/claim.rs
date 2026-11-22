//! The claim of concurrency slots: one transaction per attempt.

use super::*;
use crate::storage::{ASSET_POOL_PREFIX, BlockReason, PoolBlockDetail};

/// How a pool admits a claim.
#[derive(Clone, Copy, PartialEq)]
enum Admit {
    /// While the slots of the live leases stay within this limit.
    Slots(i32),
    /// While no live lease conflicts with the scope of the claim: an
    /// asset's implicit pool.
    Overlap,
}

/// The step that claims, and what it touches on an asset's implicit pool.
pub(super) struct Claim<'a> {
    pub(super) code_location_id: &'a str,
    pub(super) run_id: &'a str,
    pub(super) step_key: &'a str,
    pub(super) scope: Option<&'a AssetScope>,
}

type Tx = sqlx::Transaction<'static, sqlx::Postgres>;

impl Claim<'_> {
    /// One transaction: lock the pool rows, read what holds them, then
    /// claim every pool or queue the step.
    pub(super) async fn once(
        &self,
        storage: &PostgresStorage,
        pools: &[(String, u32)],
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        let now = now_nanos();
        let lease_expires_at = now + i64::from(lease_duration_secs) * 1_000_000_000;
        let mut tx = storage.pool.begin().await?;

        // Each claimer of a pool waits here for the claim before it. The
        // reads below are later statements, so they see what that claim
        // wrote. All claimers lock in the order of the pool names, so two
        // claims of the same pools cannot wait for each other.
        let keys: Vec<&str> = pools.iter().map(|(key, _)| key.as_str()).collect();
        let limits: HashMap<String, i32> = sqlx::query(
            "SELECT pool_key, slot_limit FROM concurrency_pools \
             WHERE code_location_id = $1 AND pool_key = ANY($2) \
             ORDER BY pool_key COLLATE \"C\" FOR UPDATE",
        )
        .bind(self.code_location_id)
        .bind(&keys)
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(|row| Ok((row.try_get("pool_key")?, row.try_get("slot_limit")?)))
        .collect::<Result<_>>()?;

        // An asset's implicit pool admits by overlap whatever its limit,
        // so a limit of -1 cannot switch the exclusion off.
        let mut limited: Vec<(&str, u32, Admit)> = Vec::new();
        for (pool_key, slots) in pools {
            let limit = *limits
                .get(pool_key)
                .with_context(|| format!("pool '{pool_key}' not configured"))?;
            if pool_key.starts_with(ASSET_POOL_PREFIX) {
                limited.push((pool_key, *slots, Admit::Overlap));
            } else if limit >= 0 {
                limited.push((pool_key, *slots, Admit::Slots(limit)));
            }
        }
        if limited.is_empty() {
            tx.commit().await?;
            return Ok(ConcurrencyClaimStatus::Claimed);
        }

        let holds = self.read_holds(&mut tx, &limited, now).await?;
        let blocked: Vec<PoolBlockDetail> = limited
            .iter()
            .filter_map(|&(pool_key, slots, admit)| {
                let (used, conflict) = holds.get(pool_key).copied().unwrap_or_default();
                let (full, claimed, limit) = match admit {
                    // Shown as a pool of one slot that is taken.
                    Admit::Overlap => (conflict, 1, 1),
                    Admit::Slots(limit) => (
                        used + i64::from(slots) > i64::from(limit),
                        used as u32,
                        limit,
                    ),
                };
                full.then(|| PoolBlockDetail {
                    pool_key: pool_key.to_string(),
                    claimed,
                    limit,
                })
            })
            .collect();

        let status = if blocked.is_empty() {
            self.take_slots(&mut tx, &limited, now, lease_expires_at)
                .await?;
            ConcurrencyClaimStatus::Claimed
        } else {
            self.queue(&mut tx, blocked, priority, now).await?
        };
        tx.commit().await?;
        Ok(status)
    }

    /// Per pool: the slots of the live leases of other steps, and for an
    /// asset's implicit pool whether a live lease of another step
    /// conflicts with the scope of this claim. The step's own row is left
    /// out: its claim takes that row over.
    ///
    /// Two non-exclusive holders never conflict. Beyond that a whole-asset
    /// scope (no partitions) conflicts with everything, and otherwise the
    /// two partition sets must share a key.
    async fn read_holds(
        &self,
        tx: &mut Tx,
        limited: &[(&str, u32, Admit)],
        now: i64,
    ) -> Result<HashMap<String, (i64, bool)>> {
        let rows = sqlx::query(
            "SELECT p.pool_key, \
                 CASE WHEN p.asset THEN 0 ELSE ( \
                     SELECT COALESCE(sum(s.slots_consumed), 0) FROM concurrency_slots s \
                     WHERE s.code_location_id = $3 AND s.pool_key = p.pool_key \
                     AND s.lease_expires_at > $4 AND (s.run_id <> $5 OR s.step_key <> $6) \
                 ) END::bigint AS used, \
                 p.asset AND EXISTS ( \
                     SELECT 1 FROM concurrency_slots s \
                     WHERE s.code_location_id = $3 AND s.pool_key = p.pool_key \
                     AND s.lease_expires_at > $4 AND (s.run_id <> $5 OR s.step_key <> $6) \
                     AND ($7::boolean OR s.exclusive) \
                     AND ($8::text[] IS NULL OR s.partitions IS NULL OR s.partitions && $8) \
                 ) AS conflict \
             FROM UNNEST($1::text[], $2::boolean[]) AS p(pool_key, asset)",
        )
        .bind(limited.iter().map(|p| p.0).collect::<Vec<_>>())
        .bind(
            limited
                .iter()
                .map(|p| p.2 == Admit::Overlap)
                .collect::<Vec<_>>(),
        )
        .bind(self.code_location_id)
        .bind(now)
        .bind(self.run_id)
        .bind(self.step_key)
        .bind(self.exclusive())
        .bind(self.partitions())
        .fetch_all(&mut **tx)
        .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get("pool_key")?,
                    (row.try_get("used")?, row.try_get("conflict")?),
                ))
            })
            .collect()
    }

    /// Write a slot row in each pool, or take over the row that an earlier
    /// attempt of the step (a retry pod, a resume) left behind.
    async fn take_slots(
        &self,
        tx: &mut Tx,
        limited: &[(&str, u32, Admit)],
        now: i64,
        lease_expires_at: i64,
    ) -> Result<()> {
        for &(pool_key, slots, admit) in limited {
            let asset = admit == Admit::Overlap;
            sqlx::query(
                "INSERT INTO concurrency_slots (code_location_id, pool_key, run_id, step_key, \
                     slots_consumed, claimed_at, lease_expires_at, last_heartbeat, \
                     partitions, exclusive) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $6, $8, $9) \
                 ON CONFLICT (code_location_id, pool_key, run_id, step_key) DO UPDATE SET \
                     slots_consumed = EXCLUDED.slots_consumed, \
                     claimed_at = EXCLUDED.claimed_at, \
                     lease_expires_at = EXCLUDED.lease_expires_at, \
                     last_heartbeat = EXCLUDED.last_heartbeat, \
                     partitions = EXCLUDED.partitions, \
                     exclusive = EXCLUDED.exclusive",
            )
            .bind(self.code_location_id)
            .bind(pool_key)
            .bind(self.run_id)
            .bind(self.step_key)
            .bind(i64::from(slots))
            .bind(now)
            .bind(lease_expires_at)
            .bind(if asset { self.partitions() } else { None })
            .bind(asset && self.exclusive())
            .execute(&mut **tx)
            .await?;
        }
        sqlx::query("DELETE FROM pending_steps WHERE run_id = $1 AND step_key = $2")
            .bind(self.run_id)
            .bind(self.step_key)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Queue the step on the first pool that blocks it. Its position is the
    /// number of steps waiting on that pool, itself included. A step that
    /// waits already keeps the time it was first queued.
    async fn queue(
        &self,
        tx: &mut Tx,
        blocked: Vec<PoolBlockDetail>,
        priority: i32,
        now: i64,
    ) -> Result<ConcurrencyClaimStatus> {
        let first_pool = blocked[0].pool_key.clone();
        let reason = match <[PoolBlockDetail; 1]>::try_from(blocked) {
            Ok([b]) => BlockReason::PoolFull {
                pool_key: b.pool_key,
                claimed: b.claimed,
                limit: b.limit,
            },
            Err(pools) => BlockReason::PoolsFull { pools },
        };
        sqlx::query(
            "INSERT INTO pending_steps (code_location_id, pool_key, run_id, step_key, \
                 priority, enqueued_at, block_reason) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (run_id, step_key) DO UPDATE SET \
                 code_location_id = EXCLUDED.code_location_id, \
                 pool_key = EXCLUDED.pool_key, \
                 priority = EXCLUDED.priority, \
                 block_reason = EXCLUDED.block_reason",
        )
        .bind(self.code_location_id)
        .bind(&first_pool)
        .bind(self.run_id)
        .bind(self.step_key)
        .bind(priority)
        .bind(now)
        .bind(reason.to_string())
        .execute(&mut **tx)
        .await?;
        let position: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pending_steps WHERE code_location_id = $1 AND pool_key = $2",
        )
        .bind(self.code_location_id)
        .bind(&first_pool)
        .fetch_one(&mut **tx)
        .await?;
        Ok(ConcurrencyClaimStatus::Pending {
            position: position as u32,
            reason,
        })
    }

    /// No scope: the whole asset, exclusively.
    fn exclusive(&self) -> bool {
        self.scope.is_none_or(|s| s.exclusive)
    }

    /// `None`: the whole asset.
    fn partitions(&self) -> Option<&[String]> {
        self.scope.and_then(|s| s.partitions.as_deref())
    }
}
