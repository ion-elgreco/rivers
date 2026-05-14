use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube_client::Api;
use kube_runtime::controller::Action;
use rivers_core::storage::StorageBackend;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::run::{CANCEL_ANNOTATION, CONDITION_CANCELLING, Run, RunPhase};

use super::cleanup;
use super::reconcile::{
    Context, Error, PodState, delete_pod_if_exists, make_condition, patch_status, pod_phase,
    push_condition, sync_run_status_to_storage,
};

/// Operators write this through `kubectl annotate`; the daemon-side
/// `terminate_run` path applies the same annotation via SSA.
pub fn is_cancel_requested(run: &Run) -> bool {
    run.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(CANCEL_ANNOTATION))
        .is_some_and(|v| v == "true")
}

/// Begin cooperative cancellation: signal the executor through SurrealDB,
/// kill any in-flight step jobs, then transition the Run to `Cancelling` so
/// the next reconcile pass enforces the grace period.
pub async fn start_cancelling(
    runs_api: &Api<Run>,
    run: &Run,
    name: &str,
    ctx: &Context,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();

    if let Err(e) = ctx.storage.request_cancellation(run_id).await {
        tracing::warn!(run = %name, %e, "failed to signal cancellation in SurrealDB");
    }

    if let Err(e) = cleanup::cleanup_step_jobs(ctx, run_id).await {
        tracing::warn!(run = %name, %e, "failed to clean up step jobs during cancel");
    }

    let mut new_status = status.clone();
    new_status.phase = Some(RunPhase::Cancelling);
    push_condition(
        &mut new_status,
        make_condition(CONDITION_CANCELLING, "CancelRequested"),
    );

    patch_status(runs_api, name, &new_status).await?;

    tracing::info!(run = %name, "transitioning to Cancelling, killed step jobs, signaled SurrealDB");
    Ok(Action::requeue(Duration::from_secs(5)))
}

/// Drive a `Cancelling` Run toward terminal state. Finalizes as `Cancelled`
/// once the executor pod exits or the per-Run `cancelGracePeriodSeconds`
/// expires (in which case the pod is force-deleted first).
pub async fn reconcile_cancelling(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    run: &Run,
    name: &str,
    ctx: &Context,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();
    let grace_period = run.spec.cancel_grace_period_seconds;

    let cancelling_since = status
        .conditions
        .iter()
        .find(|c| c.r#type == CONDITION_CANCELLING)
        .and_then(|c| c.last_transition_time.as_deref())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&chrono::Utc));

    let elapsed = cancelling_since
        .map(|t| (chrono::Utc::now() - t).num_seconds().max(0) as u64)
        .unwrap_or(0);

    let pod_exited = match status.executor_pod.as_deref() {
        None => true,
        Some(pod_name) => match pods_api.get_opt(pod_name).await? {
            Some(pod) => matches!(pod_phase(&pod), PodState::Succeeded | PodState::Failed),
            None => true,
        },
    };

    if pod_exited {
        if let Err(e) = cleanup::cleanup_step_jobs(ctx, run_id).await {
            tracing::warn!(run = %name, %e, "failed to clean up step jobs");
        }
        return finalize_cancelled(runs_api, &ctx.storage, run, name, None).await;
    }

    if elapsed >= grace_period {
        let pod_name = status.executor_pod.as_deref().unwrap();
        tracing::warn!(
            run = %name,
            pod = %pod_name,
            elapsed_secs = elapsed,
            grace = grace_period,
            "grace period expired, force-deleting executor pod"
        );

        delete_pod_if_exists(pods_api, pod_name).await?;
        if let Err(e) = cleanup::cleanup_step_jobs(ctx, run_id).await {
            tracing::warn!(run = %name, %e, "failed to clean up step jobs");
        }

        let msg = format!("Cancelled after {elapsed}s grace period (pod force-killed)");
        return finalize_cancelled(runs_api, &ctx.storage, run, name, Some(&msg)).await;
    }

    tracing::debug!(
        run = %name,
        elapsed_secs = elapsed,
        grace = grace_period,
        "waiting for executor to drain"
    );
    // Pod watch triggers reconcile on exit; this is a safety-net requeue capped at 30s.
    let remaining = grace_period.saturating_sub(elapsed).clamp(5, 30);
    Ok(Action::requeue(Duration::from_secs(remaining)))
}

async fn finalize_cancelled(
    runs_api: &Api<Run>,
    storage: &SurrealStorage,
    run: &Run,
    name: &str,
    message: Option<&str>,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();

    let mut new_status = status.clone();
    new_status.phase = Some(RunPhase::Cancelled);
    new_status.completed_at = Some(chrono::Utc::now().to_rfc3339());

    if let Ok(progress) = storage.get_run_progress(run_id).await {
        new_status.completed_steps = Some(progress.completed_steps);
        new_status.total_steps = Some(progress.total_steps);
    }
    new_status.message = Some(message.unwrap_or("Run was cancelled").to_string());

    patch_status(runs_api, name, &new_status).await?;
    sync_run_status_to_storage(storage, run_id, &RunPhase::Cancelled).await;
    tracing::info!(run = %name, "cancellation complete");
    Ok(Action::await_change())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use rivers_core::storage::{EventRecord, EventType, StorageBackend};

    use crate::run::test_helpers::*;

    fn test_run_with_annotations(annotations: Option<BTreeMap<String, String>>) -> Run {
        let mut run = Run::new("test-run", test_run_spec());
        run.metadata.annotations = annotations;
        run
    }

    #[test]
    fn cancel_requested_when_annotation_true() {
        let annotations = BTreeMap::from([(CANCEL_ANNOTATION.to_string(), "true".to_string())]);
        let run = test_run_with_annotations(Some(annotations));
        assert!(is_cancel_requested(&run));
    }

    #[test]
    fn cancel_not_requested_when_annotation_false() {
        let annotations = BTreeMap::from([(CANCEL_ANNOTATION.to_string(), "false".to_string())]);
        let run = test_run_with_annotations(Some(annotations));
        assert!(!is_cancel_requested(&run));
    }

    #[test]
    fn cancel_not_requested_when_no_annotations() {
        let run = test_run_with_annotations(None);
        assert!(!is_cancel_requested(&run));
    }

    #[test]
    fn cancel_not_requested_when_empty_annotations() {
        let run = test_run_with_annotations(Some(BTreeMap::new()));
        assert!(!is_cancel_requested(&run));
    }

    #[test]
    fn cancel_not_requested_when_wrong_value() {
        let annotations = BTreeMap::from([(CANCEL_ANNOTATION.to_string(), "yes".to_string())]);
        let run = test_run_with_annotations(Some(annotations));
        assert!(!is_cancel_requested(&run));
    }

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
    async fn start_cancelling_signals_storage_and_transitions() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());

        let run = test_run_running("run-1", Some(2));

        start_cancelling(&runs_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        assert!(storage.is_cancelled("run-1").await.unwrap());

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Cancelling));
        assert_eq!(status.run_id.as_deref(), Some("run-1"));
        assert!(status.conditions.iter().any(|c| {
            c.r#type == CONDITION_CANCELLING && c.reason.as_deref() == Some("CancelRequested")
        }));
    }

    #[tokio::test]
    async fn reconcile_cancelling_finalizes_when_pod_exited() {
        let mut api_state_val = MockApiState::default();
        api_state_val.pods.insert(
            "test-run-executor".to_string(),
            test_pod("test-run-executor", "Succeeded"),
        );
        let api_state = Arc::new(Mutex::new(api_state_val));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_step_events(&storage, "run-1", 3, 5).await;
        let ctx = make_context(client, storage);

        let cancelling_since = chrono::Utc::now().to_rfc3339();
        let run = test_run_cancelling("run-1", &cancelling_since);

        reconcile_cancelling(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Cancelled));
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(5));
        assert_eq!(status.message.as_deref(), Some("Run was cancelled"));
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn reconcile_cancelling_finalizes_when_pod_missing() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage);

        let cancelling_since = chrono::Utc::now().to_rfc3339();
        let run = test_run_cancelling("run-1", &cancelling_since);

        reconcile_cancelling(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Cancelled));
        assert_eq!(status.message.as_deref(), Some("Run was cancelled"));
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn reconcile_cancelling_waits_during_grace_period() {
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
        let ctx = make_context(client, storage);

        let cancelling_since = chrono::Utc::now().to_rfc3339();
        let run = test_run_cancelling("run-1", &cancelling_since);

        reconcile_cancelling(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        assert_eq!(
            patch_count(&state),
            0,
            "should not patch status during grace period wait"
        );
    }

    #[tokio::test]
    async fn reconcile_cancelling_force_kills_after_grace_period() {
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
        let ctx = make_context(client, storage);

        let past = chrono::Utc::now() - chrono::Duration::seconds(600);
        let mut run = test_run_cancelling("run-1", &past.to_rfc3339());
        run.spec.cancel_grace_period_seconds = 300;

        reconcile_cancelling(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let state = api_state.lock().unwrap();

        assert!(
            state
                .requests
                .iter()
                .any(|r| r.method == "DELETE" && r.path.contains("/pods/")),
            "should force-delete executor pod"
        );

        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Cancelled));
        assert!(status.completed_at.is_some());
        assert!(status.message.as_deref().unwrap().contains("grace period"));
        assert!(status.message.as_deref().unwrap().contains("force-killed"));
    }

    #[tokio::test]
    async fn finalize_cancelled_syncs_status_to_storage() {
        let mut api_state_val = MockApiState::default();
        api_state_val.pods.insert(
            "test-run-executor".to_string(),
            test_pod("test-run-executor", "Succeeded"),
        );
        let api_state = Arc::new(Mutex::new(api_state_val));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_run_record(&storage, "run-1").await;
        let ctx = make_context(client, storage.clone());

        let cancelling_since = chrono::Utc::now().to_rfc3339();
        let run = test_run_cancelling("run-1", &cancelling_since);

        reconcile_cancelling(&runs_api, &pods_api, &run, "test-run", &ctx)
            .await
            .unwrap();

        let run_record = storage.get_run("run-1").await.unwrap().unwrap();
        assert_eq!(run_record.status, rivers_core::storage::RunStatus::Canceled);
        assert!(run_record.end_time.is_some());
    }
}
