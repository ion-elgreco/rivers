use k8s_openapi::api::core::v1::Pod;
use kube_client::Api;
use kube_runtime::controller::Action;
use rivers_core::storage::StorageBackend;
use rivers_k8s::crd::run::{Run, RunPhase};

use super::cleanup;
use super::reconcile::{
    Context, Error, delete_pod_if_exists, patch_status, sync_run_status_to_storage,
};

/// True iff `spec.timeoutSeconds` is set and that many seconds have elapsed
/// since `status.startedAt`.
pub fn is_timed_out(run: &Run) -> bool {
    let timeout_seconds = match run.spec.timeout_seconds {
        Some(t) => t,
        None => return false,
    };

    let started_at = run
        .status
        .as_ref()
        .and_then(|s| s.started_at.as_deref())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&chrono::Utc));

    match started_at {
        Some(t) => {
            let elapsed = (chrono::Utc::now() - t).num_seconds().max(0) as u64;
            elapsed >= timeout_seconds
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_k8s::crd::run::{RunCrdStatus, RunSpec};

    fn test_run_with_timeout(timeout: Option<u64>, started_at: Option<String>) -> Run {
        let spec: RunSpec = serde_json::from_value(serde_json::json!({
            "codeLocationRef": { "name": "demo" },
            "image": "img:v1",
            "target": "job"
        }))
        .unwrap();
        let mut run = Run::new(
            "test-run",
            RunSpec {
                timeout_seconds: timeout,
                ..spec
            },
        );
        run.status = Some(RunCrdStatus {
            phase: Some(RunPhase::Running),
            started_at,
            ..Default::default()
        });
        run
    }

    #[test]
    fn not_timed_out_when_no_timeout_configured() {
        let run = test_run_with_timeout(None, Some(chrono::Utc::now().to_rfc3339()));
        assert!(!is_timed_out(&run));
    }

    #[test]
    fn not_timed_out_within_limit() {
        let started = chrono::Utc::now();
        let run = test_run_with_timeout(Some(3600), Some(started.to_rfc3339()));
        assert!(!is_timed_out(&run));
    }

    #[test]
    fn timed_out_when_exceeded() {
        let started = chrono::Utc::now() - chrono::Duration::seconds(120);
        let run = test_run_with_timeout(Some(60), Some(started.to_rfc3339()));
        assert!(is_timed_out(&run));
    }

    #[test]
    fn not_timed_out_when_no_started_at() {
        let run = test_run_with_timeout(Some(60), None);
        assert!(!is_timed_out(&run));
    }

    #[test]
    fn timed_out_at_exact_boundary() {
        let started = chrono::Utc::now() - chrono::Duration::seconds(60);
        let run = test_run_with_timeout(Some(60), Some(started.to_rfc3339()));
        assert!(is_timed_out(&run));
    }

    #[test]
    fn not_timed_out_with_invalid_timestamp() {
        let run = test_run_with_timeout(Some(60), Some("not-a-timestamp".to_string()));
        assert!(!is_timed_out(&run));
    }
}

/// Mark a Run as `TimedOut`: kill its executor pod, clean up step jobs,
/// stamp the timed-out outcome into storage, and patch the CR's terminal
/// status.
pub async fn transition_to_timed_out(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    run: &Run,
    name: &str,
    ctx: &Context,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();
    let timeout = run.spec.timeout_seconds.unwrap_or(0);

    tracing::warn!(run = %name, timeout_seconds = timeout, "run timed out, terminating");

    if let Err(e) = ctx.storage.request_cancellation(run_id).await {
        tracing::warn!(run = %name, %e, "failed to signal cancellation for timeout");
    }

    if let Some(ref pod_name) = status.executor_pod
        && delete_pod_if_exists(pods_api, pod_name).await?
    {
        tracing::info!(run = %name, pod = %pod_name, "deleted executor pod (timeout)");
    }

    if let Err(e) = cleanup::cleanup_step_jobs(ctx, run_id).await {
        tracing::warn!(run = %name, %e, "failed to clean up step jobs");
    }

    let mut new_status = status.clone();
    new_status.phase = Some(RunPhase::TimedOut);
    new_status.completed_at = Some(chrono::Utc::now().to_rfc3339());
    new_status.message = Some(format!("Run exceeded {timeout}s timeout"));

    if let Ok(progress) = ctx.storage.get_run_progress(run_id).await {
        new_status.completed_steps = Some(progress.completed_steps);
        new_status.total_steps = Some(progress.total_steps);
    }

    patch_status(runs_api, name, &new_status).await?;
    sync_run_status_to_storage(&ctx.storage, run_id, &RunPhase::TimedOut).await;

    Ok(Action::await_change())
}

#[cfg(test)]
mod async_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use rivers_core::storage::{EventRecord, EventType, StorageBackend};

    use crate::run::test_helpers::*;

    async fn seed_step_events(storage: &SurrealStorage, run_id: &str, completed: u32, total: u32) {
        let ts = chrono::Utc::now().timestamp();
        for i in 0..total {
            storage
                .store_event(&EventRecord {
                    code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepStart,
                    asset_key: Some(format!("step_{i}")),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: ts + i as i64,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        for i in 0..completed {
            storage
                .store_event(&EventRecord {
                    code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    event_type: EventType::StepSuccess,
                    asset_key: Some(format!("step_{i}")),
                    run_id: run_id.to_string(),
                    partition_key: None,
                    timestamp: ts + total as i64 + i as i64,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn transition_to_timed_out_sets_phase_and_signals_storage() {
        let mut api_state_val = MockApiState::default();
        api_state_val.pods.insert(
            "test-run-executor".to_string(),
            test_pod("test-run-executor", "Running"),
        );
        let api_state = Arc::new(Mutex::new(api_state_val));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_step_events(&storage, "run-1", 3, 10).await;
        let ctx = make_context(client, storage.clone());

        let mut run = test_run_running("run-1", Some(3));
        run.spec.timeout_seconds = Some(60);

        transition_to_timed_out(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        assert!(storage.is_cancelled("run-1").await.unwrap());

        let state = api_state.lock().unwrap();
        assert!(
            state
                .requests
                .iter()
                .any(|r| r.method == "DELETE" && r.path.contains("/pods/")),
            "should delete executor pod"
        );

        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::TimedOut));
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(10));
        assert_eq!(status.message.as_deref(), Some("Run exceeded 60s timeout"));
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn transition_to_timed_out_handles_no_executor_pod() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage);

        let mut run = test_run_running("run-1", None);
        run.status.as_mut().unwrap().executor_pod = None;
        run.spec.timeout_seconds = Some(30);

        transition_to_timed_out(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::TimedOut));
        assert_eq!(status.message.as_deref(), Some("Run exceeded 30s timeout"));
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn transition_to_timed_out_syncs_failure_to_storage() {
        let mut api_state_val = MockApiState::default();
        api_state_val.pods.insert(
            "test-run-executor".to_string(),
            test_pod("test-run-executor", "Running"),
        );
        let api_state = Arc::new(Mutex::new(api_state_val));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_run_record(&storage, "run-1").await;
        let ctx = make_context(client, storage.clone());

        let mut run = test_run_running("run-1", None);
        run.spec.timeout_seconds = Some(60);

        transition_to_timed_out(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let run_record = storage.get_run("run-1").await.unwrap().unwrap();
        assert_eq!(run_record.status, rivers_core::storage::RunStatus::Failure);
        assert!(run_record.end_time.is_some());
    }
}
