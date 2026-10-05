/// The asset an implicit exclusive-action pool (`__asset__:<asset>`) belongs
/// to; `None` for a user pool.
pub fn asset_pool_asset(pool_key: &str) -> Option<&str> {
    pool_key.strip_prefix("__asset__:")
}

/// POOLS / SLOTS CLAIMED tiles over user pools only: an asset's implicit pool
/// admits by partition overlap, not by slots, so it has no capacity to add up.
/// Returns `(pool count, claimed slots, slots suffix)`.
pub fn user_pool_totals(pools: &[crate::types::PoolInfo]) -> (usize, u32, String) {
    let user: Vec<&crate::types::PoolInfo> = pools
        .iter()
        .filter(|p| asset_pool_asset(&p.pool_key).is_none())
        .collect();
    let claimed = user.iter().map(|p| p.claimed_count).sum();
    let slot_sum: i32 = user
        .iter()
        .filter(|p| p.slot_limit > 0)
        .map(|p| p.slot_limit)
        .sum();
    let suffix = if user.iter().any(|p| p.slot_limit < 0) || slot_sum == 0 {
        "claimed".to_string()
    } else {
        format!("of {slot_sum}")
    };
    (user.len(), claimed, suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_asset_pools_stay_out_of_slot_totals() {
        let pool = |key: &str, limit: i32, claimed: u32| crate::types::PoolInfo {
            pool_key: key.to_string(),
            slot_limit: limit,
            lease_duration_secs: 60,
            claimed_count: claimed,
            pending_count: 0,
        };
        assert_eq!(asset_pool_asset("__asset__:orders"), Some("orders"));
        assert_eq!(asset_pool_asset("db"), None);
        let pools = [pool("db", 4, 1), pool("__asset__:orders", -1, 2)];
        assert_eq!(user_pool_totals(&pools), (1, 1, "of 4".to_string()));
    }
}
