//! Server functions for write actions (materialize, observe) dispatched via gRPC.

use leptos::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::SubmitPartitionKey;

/// Session identity as a proto `UserRef`; `None` in auth mode `none`.
#[cfg(feature = "ssr")]
async fn current_user_ref() -> Option<rivers_api::rivers::UserRef> {
    let identity = leptos_axum::extract::<axum::Extension<crate::auth::Identity>>()
        .await
        .ok()
        .map(|axum::Extension(id)| id)?;
    Some(rivers_api::rivers::UserRef {
        subject: identity.subject,
        email: identity.email,
        name: identity.name,
    })
}

/// Convert a UI-side `SubmitPartitionKey` into the proto wire type
/// `ProtoPartitionKey`. SSR-only because `rivers_api` isn't compiled on
/// WASM.
#[cfg(feature = "ssr")]
fn submit_to_proto(pk: SubmitPartitionKey) -> rivers_api::rivers::ProtoPartitionKey {
    use rivers_api::rivers::{
        MultiPartitionDimension, MultiPartitionKey, ProtoPartitionKey, SinglePartitionKey,
        proto_partition_key,
    };
    match pk {
        SubmitPartitionKey::Single(key) => ProtoPartitionKey {
            kind: Some(proto_partition_key::Kind::Single(SinglePartitionKey {
                keys: vec![key],
            })),
        },
        SubmitPartitionKey::Multi(dims) => ProtoPartitionKey {
            kind: Some(proto_partition_key::Kind::Multi(MultiPartitionKey {
                dimensions: dims
                    .into_iter()
                    .map(|(name, value)| MultiPartitionDimension {
                        name,
                        keys: vec![value],
                    })
                    .collect(),
            })),
        },
    }
}

/// Result of a `trigger_materialize` call. Always fire-and-forget — the
/// caller navigates to the run page (`run_id`) and polls live status.
/// `status` reports the dispatcher mode (`"queued"` or `"direct"`) so
/// the UI can adjust copy if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeResult {
    pub run_id: String,
    pub status: String,
}

/// Trigger a materialization run for `selection` (assets) at the given code
/// location. `partition_key` is required iff every asset in the selection is
/// partitioned. Fire-and-forget: returns the `run_id` immediately; the
/// caller polls the run-detail page for completion.
#[server]
pub async fn trigger_materialize(
    loc_ns: String,
    loc_name: String,
    selection: Option<Vec<String>>,
    partition_key: Option<SubmitPartitionKey>,
    tags: Option<Vec<(String, String)>>,
) -> Result<MaterializeResult, ServerFnError> {
    use rivers_api::rivers::{MaterializeRequest, Tag};

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .materialize(MaterializeRequest {
            selection: selection.unwrap_or_default(),
            partition_key: partition_key.map(submit_to_proto),
            tags: tags
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| Tag { key: k, value: v })
                .collect(),
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let r = resp.into_inner();
    Ok(MaterializeResult {
        run_id: r.run_id,
        status: r.status,
    })
}

/// Re-execute a run by id, server-side: replays it on its original partition,
/// reusing tags + job/materialization shape. Returns the new `run_id`.
#[server]
pub async fn rerun_run(
    loc_ns: String,
    loc_name: String,
    run_id: String,
) -> Result<MaterializeResult, ServerFnError> {
    use rivers_api::rivers::RerunRunRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .rerun_run(RerunRunRequest {
            run_id,
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let r = resp.into_inner();
    Ok(MaterializeResult {
        run_id: r.run_id,
        status: r.status,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillRerunResult {
    pub backfill_id: String,
    pub num_partitions: u32,
    pub num_runs: u32,
    pub status: String,
}

// `LaunchBackfill` and `MaterializeMissing` both return a `LaunchBackfillResponse`;
// map it once.
#[cfg(feature = "ssr")]
impl From<rivers_api::rivers::LaunchBackfillResponse> for BackfillRerunResult {
    fn from(r: rivers_api::rivers::LaunchBackfillResponse) -> Self {
        Self {
            backfill_id: r.backfill_id,
            num_partitions: r.num_partitions,
            num_runs: r.num_runs,
            status: r.status,
        }
    }
}

/// Re-execute an existing backfill by id. Server reads the original record and resubmits
/// with identical configuration (partition keys, strategy, failure policy, concurrency, tags).
#[server]
pub async fn rerun_backfill(
    loc_ns: String,
    loc_name: String,
    backfill_id: String,
) -> Result<BackfillRerunResult, ServerFnError> {
    use rivers_api::rivers::RerunBackfillRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .rerun_backfill(RerunBackfillRequest {
            backfill_id,
            dry_run: false,
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let r = resp.into_inner();
    Ok(BackfillRerunResult {
        backfill_id: r.backfill_id,
        num_partitions: r.num_partitions,
        num_runs: r.num_runs,
        status: r.status,
    })
}

/// Backfill an asset's missing partitions — the server computes the set (full
/// universe − materialized). Backs the "Materialize Missing" button.
#[server]
pub async fn materialize_missing_partitions(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
) -> Result<BackfillRerunResult, ServerFnError> {
    use rivers_api::rivers::MaterializeMissingRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .materialize_missing(MaterializeMissingRequest {
            asset_key,
            // Matches `repo.backfill`'s default fan-out.
            max_concurrency: 4,
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp.into_inner().into())
}

/// Launch a backfill over an explicit set of partition keys. With `job_name` set
/// it targets that job (its own plan + executor; the server resolves its assets,
/// so `selection` may be empty); otherwise it's an ad-hoc materialization of
/// `selection`. Used when a multi-partition selection is large enough to warrant
/// one backfill instead of many individual runs. Strategy defers to each asset's
/// configured default; concurrency matches `repo.backfill`'s default.
#[server]
pub async fn launch_backfill(
    loc_ns: String,
    loc_name: String,
    // `Option` (not a bare `Vec`) so an empty selection survives the server-fn
    // arg encoding — a job backfill sends `None` (the server resolves the job's
    // assets). Same reason `trigger_materialize` takes `Option<Vec<String>>`.
    selection: Option<Vec<String>>,
    partition_keys: Vec<SubmitPartitionKey>,
    tags: Option<Vec<(String, String)>>,
    job_name: Option<String>,
) -> Result<BackfillRerunResult, ServerFnError> {
    use rivers_api::rivers::{LaunchBackfillRequest, Tag};

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .launch_backfill(LaunchBackfillRequest {
            selection: selection.unwrap_or_default(),
            partition_keys: partition_keys.into_iter().map(submit_to_proto).collect(),
            partition_range: None,
            strategy: None,
            failure_policy: String::new(),
            max_concurrency: 4,
            tags: tags
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| Tag { key: k, value: v })
                .collect(),
            dry_run: false,
            job_name,
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp.into_inner().into())
}

/// Run a named job at the given code location. Fire-and-forget: returns
/// the `run_id` immediately; the caller polls the run-detail page for
/// completion. The dispatcher mode (`"queued"` or `"direct"`) is not
/// reported here — the UI navigates to the run page either way.
#[server]
pub async fn execute_job(
    loc_ns: String,
    loc_name: String,
    job_name: String,
    partition_key: Option<SubmitPartitionKey>,
) -> Result<MaterializeResult, ServerFnError> {
    use rivers_api::rivers::ExecuteJobRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .execute_job(ExecuteJobRequest {
            job_name,
            partition_key: partition_key.map(submit_to_proto),
            user: current_user_ref().await,
        })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let r = resp.into_inner();
    Ok(MaterializeResult {
        run_id: r.run_id,
        status: String::new(),
    })
}

/// Request cancellation of an in-flight run. Routes to the owning code
/// location's gRPC service, which persists the cancel flag in storage and
/// signals the run backend (Local in-process / K8s pod kill). Returns
/// `true` once the request was accepted.
#[server]
pub async fn cancel_run(
    loc_ns: String,
    loc_name: String,
    run_id: String,
) -> Result<bool, ServerFnError> {
    use rivers_api::rivers::CancelRunRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .cancel_run(CancelRunRequest { run_id })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp.into_inner().success)
}
