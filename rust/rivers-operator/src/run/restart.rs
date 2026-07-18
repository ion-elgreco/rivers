use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube_client::Api;
use kube_client::api::PostParams;
use kube_runtime::controller::Action;
use rivers_core::storage::StorageBackend;
use rivers_k8s::crd::run::{CONDITION_EXECUTOR_RESTARTED, Run, RunPhase};

use super::pod_builder::build_executor_pod;
use super::reconcile::{
    Context, Error, apply_outcome_to_status, delete_pod_if_exists, fetch_cl_env, make_condition,
    patch_status, push_condition, sync_run_status_to_storage,
};

/// Bumps `restartsWithoutProgress` (resetting to 0 if storage shows new
/// completed steps since the last restart), and either spawns a fresh
/// executor pod in resume mode, or transitions the Run to Failed when the
/// `restartsWithoutProgress` budget is exhausted.
pub async fn handle_executor_failure(
    runs_api: &Api<Run>,
    pods_api: &Api<Pod>,
    ctx: &Context,
    run: &Run,
    name: &str,
) -> Result<Action, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();
    let max_restarts = run.spec.max_restarts;
    let storage = ctx.storage.as_ref();

    // A stored outcome means the executor COMPLETED the run before exiting —
    // a deliberate failure/cancellation, not a crash. Honor it; restarting
    // would replay the whole run.
    if let Ok(Some(outcome)) = storage.get_run_outcome(run_id).await {
        let mut new_status = status.clone();
        new_status.completed_at = Some(chrono::Utc::now().to_rfc3339());
        apply_outcome_to_status(&mut new_status, outcome);
        patch_status(runs_api, name, &new_status).await?;
        if let Some(ref phase) = new_status.phase {
            sync_run_status_to_storage(storage, run_id, phase).await;
        }
        tracing::info!(
            run = %name,
            phase = ?new_status.phase,
            "executor exited after completing the run; honoring stored outcome"
        );
        return Ok(Action::await_change());
    }

    let progress = storage.get_run_progress(run_id).await.ok();
    let current_completed = progress.as_ref().map(|p| p.completed_steps).unwrap_or(0);
    let last_known_completed = status.completed_steps.unwrap_or(0);
    let progress_made = current_completed > last_known_completed;

    let mut new_status = status.clone();
    new_status.total_restarts += 1;

    if progress_made {
        new_status.restarts_without_progress = 0;
        new_status.last_progress_at = Some(chrono::Utc::now().to_rfc3339());
        tracing::info!(
            run = %name,
            completed = current_completed,
            "progress made since last restart, resetting counter"
        );
    } else {
        new_status.restarts_without_progress += 1;
    }

    if let Some(ref p) = progress {
        new_status.completed_steps = Some(p.completed_steps);
        new_status.total_steps = Some(p.total_steps);
    }

    if new_status.restarts_without_progress > max_restarts {
        tracing::warn!(
            run = %name,
            restarts = new_status.restarts_without_progress,
            max = max_restarts,
            "exceeded max restarts without progress"
        );

        new_status.completed_at = Some(chrono::Utc::now().to_rfc3339());

        match storage.get_run_outcome(run_id).await {
            Ok(Some(outcome)) => apply_outcome_to_status(&mut new_status, outcome),
            _ => {
                new_status.phase = Some(RunPhase::Failed);
                new_status.message = Some(format!(
                    "Executor failed {} time(s) without progress (max: {max_restarts})",
                    new_status.restarts_without_progress,
                ));
            }
        }

        patch_status(runs_api, name, &new_status).await?;
        if let Some(ref phase) = new_status.phase {
            sync_run_status_to_storage(storage, run_id, phase).await;
        }
        tracing::info!(run = %name, phase = ?new_status.phase, "run terminated after max restarts");
        return Ok(Action::await_change());
    }

    // Named by the lifetime counter — the budget counter resets on progress
    // and would reuse names, 409-adopting an old completed pod.
    let pod_name = format!("{name}-executor-{}", new_status.total_restarts);

    tracing::info!(
        run = %name,
        pod = %pod_name,
        restart = new_status.restarts_without_progress,
        max = max_restarts,
        "restarting executor pod"
    );

    if let Some(old_pod) = status.executor_pod.as_deref() {
        let _ = delete_pod_if_exists(pods_api, old_pod).await;
    }

    let cl_env = fetch_cl_env(ctx, run).await?;
    let pod = build_executor_pod(run, &pod_name, run_id, true, &cl_env, &ctx.surreal_pod_cfg);
    match pods_api.create(&PostParams::default(), &pod).await {
        Ok(_) => {}
        Err(kube_client::Error::Api(ref e)) if e.code == 409 => {
            tracing::info!(run = %name, pod = %pod_name, "restart pod already exists");
        }
        Err(e) => return Err(e.into()),
    }

    let budget_msg = format!(
        "Restart {}/{max_restarts}",
        new_status.restarts_without_progress
    );
    new_status.executor_pod = Some(pod_name);
    push_condition(
        &mut new_status,
        make_condition(CONDITION_EXECUTOR_RESTARTED, &budget_msg),
    );

    patch_status(runs_api, name, &new_status).await?;

    Ok(Action::requeue(Duration::from_secs(5)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use rivers_core::storage::{EventRecord, EventType, RunOutcome, StorageBackend};

    use crate::run::test_helpers::*;

    async fn seed_step_events(
        storage: &rivers_core::storage::surrealdb_backend::SurrealStorage,
        run_id: &str,
        completed: u32,
        total: u32,
    ) {
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
    async fn restart_with_no_progress_increments_counter() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        let run = test_run_running("run-1", Some(0));

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Running));
        assert_eq!(status.run_id.as_deref(), Some("run-1"));
        assert_eq!(status.restarts_without_progress, 1);
        assert_eq!(status.completed_steps, Some(0));
        assert_eq!(status.total_steps, Some(0));
        assert_eq!(status.executor_pod.as_deref(), Some("test-run-executor-1"));
        assert_eq!(status.completed_at, None);
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.r#type == CONDITION_EXECUTOR_RESTARTED)
        );
    }

    #[tokio::test]
    async fn restart_with_progress_resets_counter() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_step_events(&storage, "run-1", 3, 5).await;

        let mut run = test_run_running("run-1", Some(1));
        run.status.as_mut().unwrap().restarts_without_progress = 2;

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Running));
        assert_eq!(status.restarts_without_progress, 0);
        assert_eq!(status.total_restarts, 1);
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(5));
        assert!(status.last_progress_at.is_some());
        // Named by the lifetime counter — the budget reset must not reuse -0.
        assert_eq!(status.executor_pod.as_deref(), Some("test-run-executor-1"));
        assert_eq!(status.completed_at, None);
    }

    #[tokio::test]
    async fn restart_pod_names_stay_monotonic_across_progress_resets() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_step_events(&storage, "run-1", 3, 5).await;

        // Third lifetime restart, budget previously reset by progress.
        let mut run = test_run_running("run-1", Some(1));
        run.status.as_mut().unwrap().restarts_without_progress = 1;
        run.status.as_mut().unwrap().total_restarts = 2;

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.restarts_without_progress, 0); // progress reset
        assert_eq!(status.total_restarts, 3); // never resets
        assert_eq!(status.executor_pod.as_deref(), Some("test-run-executor-3"));
    }

    #[tokio::test]
    async fn max_restarts_exceeded_transitions_to_failed() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_step_events(&storage, "run-1", 2, 5).await;

        let mut run = test_run_running("run-1", Some(2));
        run.spec.max_restarts = 2;
        run.status.as_mut().unwrap().restarts_without_progress = 2;

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Failed));
        assert_eq!(status.restarts_without_progress, 3);
        assert_eq!(status.completed_steps, Some(2));
        assert_eq!(status.total_steps, Some(5));
        assert!(status.completed_at.is_some());
        assert_eq!(
            status.message.as_deref(),
            Some("Executor failed 3 time(s) without progress (max: 2)")
        );
    }

    #[tokio::test]
    async fn stored_outcome_is_honored_without_restart() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_run_record(&storage, "run-1").await;
        storage
            .set_run_outcome(
                "run-1",
                &RunOutcome::Failure {
                    message: "all attempts exhausted".to_string(),
                    completed_steps: 0,
                    total_steps: 1,
                },
            )
            .await
            .unwrap();

        let run = test_run_running("run-1", Some(0));
        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        // The deliberate failure is honored as-is: no restart accounting, no
        // replacement executor pod.
        assert_eq!(status.phase, Some(RunPhase::Failed));
        assert_eq!(status.message.as_deref(), Some("all attempts exhausted"));
        assert_eq!(status.restarts_without_progress, 0);
        assert!(status.completed_at.is_some());
        assert!(
            !state
                .requests
                .iter()
                .any(|r| r.method == "POST" && r.path.contains("/pods")),
            "must not create a restart pod when the run completed"
        );
    }

    #[tokio::test]
    async fn max_restarts_exceeded_uses_storage_outcome_when_available() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_step_events(&storage, "run-1", 4, 5).await;
        storage
            .set_run_outcome(
                "run-1",
                &RunOutcome::Failure {
                    message: "step_d failed: division by zero".to_string(),
                    completed_steps: 4,
                    total_steps: 5,
                },
            )
            .await
            .unwrap();

        let mut run = test_run_running("run-1", Some(4));
        run.spec.max_restarts = 1;
        run.status.as_mut().unwrap().restarts_without_progress = 1;

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.phase, Some(RunPhase::Failed));
        assert_eq!(status.completed_steps, Some(4));
        assert_eq!(status.total_steps, Some(5));
        assert_eq!(
            status.message.as_deref(),
            Some("step_d failed: division by zero")
        );
        assert!(status.completed_at.is_some());
    }

    #[tokio::test]
    async fn max_restarts_syncs_failure_to_storage() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        seed_run_record(&storage, "run-1").await;

        let mut run = test_run_running("run-1", Some(0));
        run.spec.max_restarts = 0;

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let run_record = storage.get_run("run-1").await.unwrap().unwrap();
        assert_eq!(run_record.status, rivers_core::storage::RunStatus::Failure);
        assert!(run_record.end_time.is_some());
    }

    #[tokio::test]
    async fn restart_creates_new_pod_with_resume_flag() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        let run = test_run_running("run-1", Some(0));

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let create = state
            .requests
            .iter()
            .find(|r| r.method == "POST" && r.path.contains("/pods"))
            .expect("no pod create request");
        let pod: Pod = serde_json::from_value(create.body.clone().unwrap()).unwrap();
        let spec = pod.spec.unwrap();
        let args = spec.containers[0].args.as_ref().unwrap();
        assert!(args.contains(&"--resume".to_string()));
    }

    #[tokio::test]
    async fn restart_deletes_old_pod() {
        let mut api_state_val = MockApiState::default();
        api_state_val.pods.insert(
            "test-run-executor".to_string(),
            test_pod("test-run-executor", "Failed"),
        );
        let api_state = Arc::new(Mutex::new(api_state_val));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let ctx = make_context(client, storage.clone());
        let run = test_run_running("run-1", Some(0));

        handle_executor_failure(&runs_api, &pods_api, &ctx, &run, "test-run")
            .await
            .unwrap();

        let state = api_state.lock().unwrap();
        let delete = state
            .requests
            .iter()
            .find(|r| r.method == "DELETE" && r.path.contains("/pods/test-run-executor"))
            .expect("should delete old executor pod");
        assert!(delete.path.ends_with("/test-run-executor"));
    }
}
