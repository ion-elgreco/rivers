use anyhow::{Context, Result};
use surrealdb::types::RecordId;

use crate::storage::{
    ASSET_POOL_PREFIX, AssetScope, BlockReason, ConcurrencyClaimStatus, PoolBlockDetail, PoolLimit,
};

use super::*;

impl SurrealStorage {
    /// `claiming_step` (run id, step key) leaves that step's own row out of
    /// the count: its claim takes the row over.
    pub(super) async fn query_pool_usage(
        &self,
        code_location_id: &str,
        pool_key: &str,
        now_ns: i64,
        claiming_step: Option<(&str, &str)>,
    ) -> Result<(PoolLimit, u32)> {
        let not_own_row = if claiming_step.is_some() {
            "AND (run_id != $run_id OR step_key != $step_key) "
        } else {
            ""
        };
        let mut query = self
            .db
            .query(format!(
                "SELECT * FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $pool_key LIMIT 1; \
                 SELECT math::sum(slots_consumed) AS total FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now {not_own_row}GROUP ALL",
            ))
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns));
        if let Some((run_id, step_key)) = claiming_step {
            query = query
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()));
        }
        let mut result = query.await?;

        let pools: Vec<PoolLimit> = result.take(0)?;
        let pool = pools
            .into_iter()
            .next()
            .with_context(|| format!("pool '{}' not configured", pool_key))?;
        let claimed: Option<u32> = result.take((1, "total"))?;
        Ok((pool, claimed.unwrap_or(0)))
    }

    /// SurrealQL predicate that finds a live slot conflicting with `scope` on
    /// an implicit asset pool. Empty result = free to claim. Shared verbatim
    /// by the pre-check probe and the claim transaction — the two enforce the
    /// same rule and must never drift. `pool_param`/`parts_param` name the
    /// bind parameters at each site.
    ///
    /// Two non-exclusive holders never conflict, so the cheap `exclusive` test
    /// is first and the set comparison is skipped entirely on the hot path
    /// (concurrent materializes). Beyond that a whole-asset scope (`NONE`)
    /// conflicts with everything, and otherwise the partition sets must
    /// actually intersect.
    pub(super) fn asset_conflict_predicate(
        pool_param: &str,
        parts_param: &str,
        scope: Option<&AssetScope>,
    ) -> String {
        let (mine_exclusive, mine_is_whole_asset) = match scope {
            Some(s) => (s.exclusive, s.partitions.is_none()),
            // No scope supplied: treat as whole-asset exclusive, matching how a
            // pre-V5 row (partitions NONE) reads back.
            None => (true, true),
        };
        let overlap = if mine_is_whole_asset {
            "true".to_string()
        } else {
            format!("(partitions IS NONE OR partitions CONTAINSANY ${parts_param})")
        };
        format!(
            "SELECT VALUE id FROM concurrency_slots \
                 WHERE code_location_id = $cl AND pool_key = ${pool_param} \
                 AND lease_expires_at > $now \
                 AND (run_id != $run_id OR step_key != $step_key) \
                 AND ({mine_exclusive} OR exclusive) \
                 AND {overlap} LIMIT 1"
        )
    }

    /// The claim transaction's `LET` wrapper around the shared predicate.
    pub(super) fn asset_conflict_clause(i: usize, scope: Option<&AssetScope>) -> String {
        format!(
            "LET $conf_{i} = ({});\n",
            Self::asset_conflict_predicate(&format!("p{i}"), &format!("parts{i}"), scope)
        )
    }

    /// Build a SurrealQL transaction that atomically checks capacity and claims slots.
    ///
    /// `asset_pools` are the indices of `pools` whose admission is by partition
    /// overlap instead of slot count; they still take their slot so the shared
    /// `claim_version` bump keeps concurrent claims serialized.
    pub(super) fn build_claim_transaction(
        pools: &[(String, u32)],
        asset_pools: &[usize],
        scope: Option<&AssetScope>,
    ) -> String {
        let mut q = String::from("BEGIN TRANSACTION;\n");

        // Asset pools are admitted by overlap, not capacity — their `$lim`/
        // `$used` would be computed (a full slot aggregate inside the write
        // transaction) and referenced by nothing. `$used` leaves out the
        // step's own row, which the UPSERT below takes over.
        for (i, (_, _slots)) in pools.iter().enumerate() {
            if asset_pools.contains(&i) {
                continue;
            }
            q += &format!(
                "LET $lim_{i} = (SELECT VALUE slot_limit \
                     FROM concurrency_pools \
                     WHERE code_location_id = $cl AND pool_key = $p{i})[0] ?? 0;\n\
                 LET $used_{i} = (SELECT VALUE math::sum(slots_consumed) \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $p{i} \
                     AND lease_expires_at > $now \
                     AND (run_id != $run_id OR step_key != $step_key) \
                     GROUP ALL)[0] ?? 0;\n"
            );
        }
        for &i in asset_pools {
            q += &Self::asset_conflict_clause(i, scope);
        }

        // Asset pools carry no capacity condition — they are admitted purely by
        // partition overlap, so a limit of -1 (or any value) cannot switch the
        // exclusion off. They still take a slot, which is what bumps
        // `claim_version` and serializes concurrent claims.
        let mut conditions: Vec<String> = pools
            .iter()
            .enumerate()
            .filter(|(i, _)| !asset_pools.contains(i))
            .map(|(i, (_, slots))| format!("($used_{i} + {slots}) <= $lim_{i}"))
            .collect();
        conditions.extend(
            asset_pools
                .iter()
                .map(|i| format!("array::len($conf_{i}) == 0")),
        );
        q += &format!("IF {} {{\n", conditions.join(" AND "));

        for i in 0..pools.len() {
            q += &format!(
                "  UPDATE concurrency_pools \
                     SET claim_version = claim_version + 1 \
                     WHERE code_location_id = $cl AND pool_key = $p{i};\n"
            );
        }

        for (i, (_, slots)) in pools.iter().enumerate() {
            let scope_fields = if asset_pools.contains(&i) {
                match scope {
                    Some(s) if s.partitions.is_some() => {
                        format!(
                            ", partitions = <set<string>> $parts{i}, exclusive = {}",
                            s.exclusive
                        )
                    }
                    Some(s) => format!(", partitions = NONE, exclusive = {}", s.exclusive),
                    None => ", partitions = NONE, exclusive = true".to_string(),
                }
            } else {
                String::new()
            };
            // A step run again under its run and step key (a retry pod, a
            // resume) takes over the row its killed attempt left behind.
            q += &format!(
                "  UPSERT concurrency_slots SET \
                     code_location_id = $cl, \
                     pool_key = $p{i}, run_id = $run_id, step_key = $step_key, \
                     slots_consumed = {slots}, claimed_at = $now, \
                     lease_expires_at = $lease_exp, last_heartbeat = $now{scope_fields} \
                     WHERE code_location_id = $cl AND pool_key = $p{i} \
                     AND run_id = $run_id AND step_key = $step_key;\n"
            );
        }

        q += "  DELETE FROM pending_steps \
                  WHERE run_id = $run_id AND step_key = $step_key;\n";
        q += "};\n";
        q += "COMMIT TRANSACTION;\n";
        // Only the rows this claim wrote: when the `IF` is false, the rows an
        // earlier attempt of the step left behind are still there.
        q += "SELECT count() AS total FROM concurrency_slots \
                  WHERE run_id = $run_id AND step_key = $step_key \
                  AND claimed_at = $now GROUP ALL;\n";
        q
    }

    /// Statement index of the post-COMMIT SELECT in the claim transaction query.
    /// Two capacity `LET`s per counted pool, one conflict `LET` per
    /// asset-scoped pool.
    pub(super) fn claim_check_statement_index(num_pools: usize, num_asset_pools: usize) -> usize {
        2 * (num_pools - num_asset_pools) + num_asset_pools + 3
    }
}

/// Sentinel error type used by `claim_concurrency_slots` to encode the "snapshot saw the pool as full" race as a retryable failure.
#[derive(Debug)]
pub(super) struct PoolContended;

impl std::fmt::Display for PoolContended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pool snapshot saw full capacity, retry needed")
    }
}

impl std::error::Error for PoolContended {}

impl SurrealStorage {
    /// Whether a live slot on `pool_key` conflicts with `scope`. Mirrors
    /// [`Self::asset_conflict_clause`] so the pre-check and the transaction
    /// agree on what "blocked" means.
    /// Asset-pool pre-check, one round trip: the existence probe (whose
    /// missing-row error is load-bearing — without a `concurrency_pools` row
    /// the claim transaction's `claim_version` bump matches nothing, silently
    /// removing the fence that serializes claims) plus the same conflict rule
    /// the transaction enforces. Returns whether a live slot conflicts.
    async fn asset_pool_precheck(
        &self,
        code_location_id: &str,
        pool_key: &str,
        run_id: &str,
        step_key: &str,
        now_ns: i64,
        scope: Option<&AssetScope>,
    ) -> Result<bool> {
        let mine_parts = scope.and_then(|s| s.partitions.clone());
        let mut q = self
            .db
            .query(format!(
                "SELECT VALUE 1 FROM concurrency_pools \
                 WHERE code_location_id = $cl AND pool_key = $pool_key LIMIT 1; \
                 {}",
                Self::asset_conflict_predicate("pool_key", "parts", scope)
            ))
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("step_key", step_key.to_string()))
            .bind(("now", now_ns));
        if let Some(parts) = mine_parts {
            q = q.bind(("parts", parts));
        }
        let mut response = q.await?;
        let exists: Vec<i64> = response.take(0)?;
        anyhow::ensure!(!exists.is_empty(), "pool '{}' not configured", pool_key);
        let hits: Vec<RecordId> = response.take(1)?;
        Ok(!hits.is_empty())
    }

    /// One attempt of the [`PerCodeLocationStorage::claim_concurrency_slots`] flow.
    pub(super) async fn try_claim_concurrency_slots_once(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
        scope: Option<&AssetScope>,
    ) -> Result<ConcurrencyClaimStatus> {
        let now_ns = now_nanos();
        let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

        let mut blocked = Vec::new();
        let mut limited_pools: Vec<(String, u32)> = Vec::new();
        let mut asset_pools: Vec<usize> = Vec::new();
        for (pool_key, slots_needed) in pools {
            // An implicit asset pool is never "unlimited": its admission is by
            // partition overlap, not capacity, so dropping it here would remove
            // the exclusion *and* the shared `claim_version` write that fences
            // concurrent claims. `rivers pools set __asset__:x -1` must not be
            // able to switch destructive-action exclusion off. The pre-check
            // enforces the same overlap rule as the transaction — without it a
            // step blocked purely by scope would fall through, fail the
            // transaction's `IF`, and surface as PoolContended, which retries
            // and then hard-fails instead of waiting like any blocked claim.
            if pool_key.starts_with(ASSET_POOL_PREFIX) {
                let conflict = self
                    .asset_pool_precheck(
                        code_location_id,
                        pool_key,
                        run_id,
                        step_key,
                        now_ns,
                        scope,
                    )
                    .await?;
                if conflict {
                    blocked.push(PoolBlockDetail {
                        pool_key: pool_key.clone(),
                        claimed: 1,
                        limit: 1,
                    });
                }
                asset_pools.push(limited_pools.len());
                limited_pools.push((pool_key.clone(), *slots_needed));
                continue;
            }
            let (pool, current_used) = self
                .query_pool_usage(code_location_id, pool_key, now_ns, Some((run_id, step_key)))
                .await?;
            if pool.slot_limit < 0 {
                continue;
            }
            limited_pools.push((pool_key.clone(), *slots_needed));
            if current_used + *slots_needed > pool.slot_limit as u32 {
                blocked.push(PoolBlockDetail {
                    pool_key: pool_key.clone(),
                    claimed: current_used,
                    limit: pool.slot_limit,
                });
            }
        }

        if limited_pools.is_empty() {
            return Ok(ConcurrencyClaimStatus::Claimed);
        }

        if !blocked.is_empty() {
            let first_pool = blocked[0].pool_key.clone();
            let reason = if blocked.len() == 1 {
                let b = &blocked[0];
                BlockReason::PoolFull {
                    pool_key: b.pool_key.clone(),
                    claimed: b.claimed,
                    limit: b.limit,
                }
            } else {
                BlockReason::PoolsFull { pools: blocked }
            };
            let reason_str = reason.to_string();

            let mut result = self
                .db
                .query(
                    "UPSERT pending_steps SET \
                         code_location_id = $cl, \
                         pool_key = $pool_key, \
                         run_id = $run_id, \
                         step_key = $step_key, \
                         priority = $priority, \
                         enqueued_at = $now, \
                         block_reason = $reason \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM pending_steps \
                         WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("pool_key", first_pool))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("priority", priority))
                .bind(("now", now_ns))
                .bind(("reason", reason_str))
                .await?;
            let position: Option<u32> = result.take((1, "total"))?;

            return Ok(ConcurrencyClaimStatus::Pending {
                position: position.unwrap_or(1),
                reason,
            });
        }

        let txn_query = Self::build_claim_transaction(&limited_pools, &asset_pools, scope);
        let mut q = self.db.query(&txn_query);
        for (i, (pool_key, _)) in limited_pools.iter().enumerate() {
            q = q.bind((format!("p{i}"), pool_key.clone()));
        }
        if let Some(parts) = scope.and_then(|s| s.partitions.as_ref()) {
            for &i in &asset_pools {
                q = q.bind((format!("parts{i}"), parts.clone()));
            }
        }
        q = q
            .bind(("cl", code_location_id.to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("step_key", step_key.to_string()))
            .bind(("now", now_ns))
            .bind(("lease_exp", lease_exp));

        let mut response = q.await?.check()?;

        // Indexed by the pools actually in the transaction: unlimited ones were
        // dropped above. Counting `pools` instead reads a statement past the
        // end, so a transaction that committed is reported as contention — and
        // the retry then finds the slots it already claimed.
        let check_idx = Self::claim_check_statement_index(limited_pools.len(), asset_pools.len());
        let count: Option<u32> = response.take((check_idx, "total"))?;

        if count.unwrap_or(0) > 0 {
            Ok(ConcurrencyClaimStatus::Claimed)
        } else {
            Err(anyhow::Error::new(PoolContended))
        }
    }
}
