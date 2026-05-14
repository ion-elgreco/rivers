use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rivers_core::storage::StorageBackend;

#[derive(Debug, Clone, PartialEq)]
pub struct ResumeState {
    pub completed_steps: HashSet<String>,
    pub data_versions: HashMap<String, String>,
}

pub async fn build_resume_state(
    storage: &impl StorageBackend,
    run_id: &str,
) -> Result<ResumeState> {
    let (completed_steps, data_versions) = tokio::try_join!(
        storage.get_completed_step_keys(run_id),
        storage.get_step_data_versions(run_id),
    )?;

    tracing::info!(
        run_id,
        completed = completed_steps.len(),
        "Built resume state — skipping completed steps"
    );

    Ok(ResumeState {
        completed_steps,
        data_versions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use rivers_core::storage::{EventRecord, EventType, StorageBackend};

    #[tokio::test]
    async fn test_build_resume_state_empty_run() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let state = build_resume_state(&storage, "no-such-run").await.unwrap();
        assert_eq!(
            state,
            ResumeState {
                completed_steps: HashSet::new(),
                data_versions: HashMap::new(),
            }
        );
    }

    #[tokio::test]
    async fn test_build_resume_state_with_completed_steps() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let run_id = "resume-test-1";

        storage
            .store_event(&EventRecord {
                code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("step_a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        storage
            .store_event(&EventRecord {
                code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv1".to_string()),
                },
                asset_key: Some("step_a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 99,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        storage
            .store_event(&EventRecord {
                code_location_id: rivers_core::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("step_b".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 200,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();

        let state = build_resume_state(&storage, run_id).await.unwrap();
        assert_eq!(
            state,
            ResumeState {
                completed_steps: HashSet::from(["step_a".to_string()]),
                data_versions: HashMap::from([("step_a".to_string(), "dv1".to_string())]),
            }
        );
    }
}
