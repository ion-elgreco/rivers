use std::any::Any;
use std::future::Future;

use anyhow::Result;

use crate::storage::CoordinatorRunInfo;

pub enum RunHealthStatus {
    Healthy,
    Exited,
    Missing,
    Unknown(String),
}

pub trait RunBackend: Send + Sync {
    fn launch(
        &self,
        run_info: &CoordinatorRunInfo,
        ctx: &(dyn Any + Send + Sync),
    ) -> impl Future<Output = Result<()>> + Send;

    fn terminate_run(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;

    fn check_run_health(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<RunHealthStatus>> + Send;
}
