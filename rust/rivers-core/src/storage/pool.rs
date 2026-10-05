use surrealdb::types::SurrealValue;

use super::default_code_location_id;

/// Pool configuration as stored in the `concurrency_pools` table.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct PoolLimit {
    /// Owning code location; pools are per-CL — CL-A's `default` is independent of CL-B's.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub pool_key: String,
    /// Slot limit. `-1` means unlimited (no capacity enforcement).
    pub slot_limit: i32,
    #[serde(default = "default_lease_duration")]
    pub lease_duration_secs: u32,
}

pub const DEFAULT_LEASE_DURATION_SECS: u32 = 300;

fn default_lease_duration() -> u32 {
    DEFAULT_LEASE_DURATION_SECS
}

/// Prefix of the pool every asset with an exclusive action implicitly gets.
/// Claims on such a pool are admitted by [`AssetScope`] overlap rather than by
/// slot count, so the storage layer has to recognise the key.
pub const ASSET_POOL_PREFIX: &str = "__asset__:";

/// What a step touches on an asset's implicit pool, and whether it needs the
/// touched partitions to itself.
///
/// Admission is set overlap, not counting: two non-exclusive holders never
/// conflict, and an exclusive one conflicts only where the partitions actually
/// intersect. That is what lets `delete(p1)` run beside a materialize of `p2`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetScope {
    /// Rendered partition keys the step touches. `None` = the whole asset,
    /// which conflicts with every other scope.
    pub partitions: Option<Vec<String>>,
    /// True for an action step — the side that demands exclusivity.
    pub exclusive: bool,
}

/// Runtime pool info: configuration + current usage.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolInfo {
    pub pool_key: String,
    /// Slot limit. `-1` means unlimited (no capacity enforcement).
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    /// Sum of `slots_consumed` for active (non-expired) leases.
    pub claimed_count: u32,
    /// Number of steps waiting in `pending_steps`.
    pub pending_count: u32,
}

/// A single active slot holder in a concurrency pool.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct SlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

/// Why a step is blocked from claiming concurrency slots.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockReason {
    /// Single pool is at capacity.
    PoolFull {
        pool_key: String,
        claimed: u32,
        limit: i32,
    },
    /// Multiple pools are at capacity (multi-pool claim).
    PoolsFull { pools: Vec<PoolBlockDetail> },
}

impl BlockReason {
    /// Every blocking pool is an asset's implicit pool — held by a live step
    /// that renews its lease, so the wait ends when that step does.
    pub fn only_asset_pools(&self) -> bool {
        match self {
            BlockReason::PoolFull { pool_key, .. } => pool_key.starts_with(ASSET_POOL_PREFIX),
            BlockReason::PoolsFull { pools } => pools
                .iter()
                .all(|p| p.pool_key.starts_with(ASSET_POOL_PREFIX)),
        }
    }
}

impl std::fmt::Display for BlockReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockReason::PoolFull {
                pool_key,
                claimed,
                limit,
            } => write!(f, "pool '{}' full ({}/{})", pool_key, claimed, limit),
            BlockReason::PoolsFull { pools } => {
                write!(f, "pools full: ")?;
                for (i, p) in pools.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}' ({}/{})", p.pool_key, p.claimed, p.limit)?;
                }
                Ok(())
            }
        }
    }
}

/// Detail for a single pool that is blocking a claim.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PoolBlockDetail {
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
}

/// Result of attempting to claim concurrency slots.
#[derive(Debug, Clone, PartialEq)]
pub enum ConcurrencyClaimStatus {
    /// Slots successfully claimed in all requested pools.
    Claimed,
    /// Step is pending — at least one pool is full.
    Pending { position: u32, reason: BlockReason },
}
