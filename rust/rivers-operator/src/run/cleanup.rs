use k8s_openapi::api::batch::v1::Job;
use kube_client::Api;
use kube_client::api::{DeleteParams, ListParams};

use super::reconcile::{Context, Error};

/// Background-delete every step-worker Job created on behalf of `run_id`.
/// Soft-fails on API errors (logged) so a transient failure doesn't block a
/// Run's transition out of Cancelling / TimedOut — the next reconcile pass
/// will retry.
pub async fn cleanup_step_jobs(ctx: &Context, run_id: &str) -> Result<(), Error> {
    let jobs_api: Api<Job> = Api::namespaced(ctx.client.clone(), &ctx.namespace);
    let label_selector = format!("rivers.io/run-id={run_id},rivers.io/component=step-worker");
    let lp = ListParams::default().labels(&label_selector);

    match jobs_api
        .delete_collection(&DeleteParams::background(), &lp)
        .await
    {
        Ok(either) => {
            let count = either.left().map(|list| list.items.len()).unwrap_or(0);
            if count > 0 {
                tracing::info!(run_id = %run_id, count, "deleted step jobs");
            }
        }
        Err(e) => {
            tracing::warn!(run_id = %run_id, error = %e, "failed to delete step jobs");
        }
    }

    Ok(())
}
