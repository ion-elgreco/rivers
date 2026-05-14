use kube_client::Api;
use rivers_core::storage::StorageBackend;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_k8s::crd::run::Run;

use super::reconcile::{Error, patch_status};

/// Bumps `lastProgressAt` only when forward progress happened. Returns
/// `true` if the patch was applied (the restart-without-progress detector
/// keys off this), `false` on no-op or transient read failure (logged +
/// swallowed so a flaky storage hop doesn't kill the reconcile pass).
pub async fn update_progress(
    runs_api: &Api<Run>,
    storage: &SurrealStorage,
    run: &Run,
    name: &str,
) -> Result<bool, Error> {
    let status = run.status.as_ref().unwrap();
    let run_id = status.run_id.as_deref().unwrap();

    let progress = match storage.get_run_progress(run_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(run = %name, %e, "failed to read run progress");
            return Ok(false);
        }
    };

    let old_completed = status.completed_steps.unwrap_or(0);
    let new_completed = progress.completed_steps;

    if new_completed == old_completed && status.total_steps == Some(progress.total_steps) {
        return Ok(false);
    }

    let mut new_status = status.clone();
    new_status.completed_steps = Some(progress.completed_steps);
    new_status.total_steps = Some(progress.total_steps);

    if new_completed > old_completed {
        new_status.last_progress_at = Some(chrono::Utc::now().to_rfc3339());
    }

    patch_status(runs_api, name, &new_status).await?;

    tracing::debug!(
        run = %name,
        completed = progress.completed_steps,
        total = progress.total_steps,
        "updated progress"
    );

    Ok(new_completed > old_completed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

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
    async fn update_progress_patches_when_steps_changed() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_step_events(&storage, "run-1", 3, 5).await;

        let run = test_run_running("run-1", Some(1));

        let advanced = update_progress(&runs_api, &storage, &run, "test-run")
            .await
            .unwrap();
        assert!(advanced);

        let state = api_state.lock().unwrap();
        let status = last_status_patch(&state);
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(5));
        assert!(status.last_progress_at.is_some());
    }

    #[tokio::test]
    async fn update_progress_skips_when_unchanged() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_step_events(&storage, "run-1", 3, 5).await;

        let mut run = test_run_running("run-1", Some(3));
        run.status.as_mut().unwrap().total_steps = Some(5);

        let advanced = update_progress(&runs_api, &storage, &run, "test-run")
            .await
            .unwrap();
        assert!(!advanced);

        let state = api_state.lock().unwrap();
        assert_eq!(
            patch_count(&state),
            0,
            "should not patch when progress unchanged"
        );
    }

    #[tokio::test]
    async fn update_progress_returns_false_when_no_events() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        let run = test_run_running("run-1", Some(0));

        let advanced = update_progress(&runs_api, &storage, &run, "test-run")
            .await
            .unwrap();
        assert!(!advanced);
    }

    #[tokio::test]
    async fn update_progress_only_total_changed_no_last_progress_at() {
        let api_state = Arc::new(Mutex::new(MockApiState::default()));
        let client = mock_client(api_state.clone());
        let runs_api: Api<Run> = Api::namespaced(client.clone(), "default");

        let storage = memory_storage().await;
        seed_step_events(&storage, "run-1", 3, 10).await;

        let mut run = test_run_running("run-1", Some(3));
        run.status.as_mut().unwrap().total_steps = Some(5);

        let advanced = update_progress(&runs_api, &storage, &run, "test-run")
            .await
            .unwrap();
        assert!(!advanced);

        let state = api_state.lock().unwrap();
        assert_eq!(
            patch_count(&state),
            1,
            "total_steps changed so a patch should fire"
        );
        let status = last_status_patch(&state);
        assert_eq!(status.completed_steps, Some(3));
        assert_eq!(status.total_steps, Some(10));
        assert!(
            status.last_progress_at.is_none(),
            "completed didn't advance so last_progress_at should not be set"
        );
    }
}
