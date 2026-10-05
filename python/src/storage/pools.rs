use pyo3::prelude::*;

#[pyclass(
    name = "PoolLimit",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolLimit {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
}

impl From<rivers_core::storage::PoolLimit> for PyPoolLimit {
    fn from(p: rivers_core::storage::PoolLimit) -> Self {
        Self {
            pool_key: p.pool_key,
            slot_limit: p.slot_limit,
            lease_duration_secs: p.lease_duration_secs,
        }
    }
}

#[pyclass(
    name = "PoolInfo",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolInfo {
    pub pool_key: String,
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    pub claimed_count: u32,
    pub pending_count: u32,
}

impl From<rivers_core::storage::PoolInfo> for PyPoolInfo {
    fn from(p: rivers_core::storage::PoolInfo) -> Self {
        Self {
            pool_key: p.pool_key,
            slot_limit: p.slot_limit,
            lease_duration_secs: p.lease_duration_secs,
            claimed_count: p.claimed_count,
            pending_count: p.pending_count,
        }
    }
}

#[pyclass(
    name = "SlotHolder",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PySlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

impl From<rivers_core::storage::SlotHolder> for PySlotHolder {
    fn from(s: rivers_core::storage::SlotHolder) -> Self {
        Self {
            run_id: s.run_id,
            step_key: s.step_key,
            slots_consumed: s.slots_consumed,
            claimed_at: s.claimed_at,
            lease_expires_at: s.lease_expires_at,
        }
    }
}

#[pyclass(
    name = "PoolBlockDetail",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyPoolBlockDetail {
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
}

impl From<rivers_core::storage::PoolBlockDetail> for PyPoolBlockDetail {
    fn from(p: rivers_core::storage::PoolBlockDetail) -> Self {
        Self {
            pool_key: p.pool_key,
            claimed: p.claimed,
            limit: p.limit,
        }
    }
}

#[pyclass(
    name = "BlockReason",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyBlockReason {
    /// "pool_full" or "pools_full"
    pub kind: String,
    /// For pool_full: the single blocking pool. For pools_full: the first pool.
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
    /// For pools_full: all blocking pools. Empty for pool_full.
    pub pools: Vec<PyPoolBlockDetail>,
}

#[pymethods]
impl PyBlockReason {
    fn __repr__(&self) -> String {
        if self.kind == "pool_full" {
            format!(
                "BlockReason.pool_full(pool_key='{}', claimed={}, limit={})",
                self.pool_key, self.claimed, self.limit
            )
        } else {
            let details: Vec<String> = self
                .pools
                .iter()
                .map(|p| format!("'{}'({}/{})", p.pool_key, p.claimed, p.limit))
                .collect();
            format!("BlockReason.pools_full({})", details.join(", "))
        }
    }
}

impl From<rivers_core::storage::BlockReason> for PyBlockReason {
    fn from(r: rivers_core::storage::BlockReason) -> Self {
        match r {
            rivers_core::storage::BlockReason::PoolFull {
                pool_key,
                claimed,
                limit,
            } => Self {
                kind: "pool_full".to_string(),
                pool_key,
                claimed,
                limit,
                pools: vec![],
            },
            rivers_core::storage::BlockReason::PoolsFull { pools } => {
                let first = &pools[0];
                Self {
                    kind: "pools_full".to_string(),
                    pool_key: first.pool_key.clone(),
                    claimed: first.claimed,
                    limit: first.limit,
                    pools: pools.into_iter().map(PyPoolBlockDetail::from).collect(),
                }
            }
        }
    }
}

#[pyclass(
    name = "ConcurrencyClaimStatus",
    frozen,
    get_all,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyConcurrencyClaimStatus {
    /// "claimed" or "pending"
    pub status: String,
    /// Queue position (only meaningful when status == "pending").
    pub position: u32,
    /// Block reason (only present when status == "pending").
    pub reason: Option<PyBlockReason>,
}

#[pymethods]
impl PyConcurrencyClaimStatus {
    #[getter]
    fn is_claimed(&self) -> bool {
        self.status == "claimed"
    }

    fn __repr__(&self) -> String {
        if self.status == "claimed" {
            "ConcurrencyClaimStatus.Claimed".to_string()
        } else {
            format!(
                "ConcurrencyClaimStatus.Pending(position={}, reason={})",
                self.position,
                self.reason
                    .as_ref()
                    .map(|r| r.__repr__())
                    .unwrap_or_default()
            )
        }
    }
}

impl From<rivers_core::storage::ConcurrencyClaimStatus> for PyConcurrencyClaimStatus {
    fn from(s: rivers_core::storage::ConcurrencyClaimStatus) -> Self {
        match s {
            rivers_core::storage::ConcurrencyClaimStatus::Claimed => Self {
                status: "claimed".to_string(),
                position: 0,
                reason: None,
            },
            rivers_core::storage::ConcurrencyClaimStatus::Pending { position, reason } => Self {
                status: "pending".to_string(),
                position,
                reason: Some(PyBlockReason::from(reason)),
            },
        }
    }
}
