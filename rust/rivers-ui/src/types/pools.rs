use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    pub claimed_count: u32,
    pub pending_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDetail {
    pub info: PoolInfo,
    pub holders: Vec<SlotHolder>,
}
