//! Run queue coordinator — one dequeue cycle per tick.

use anyhow::Result;

use crate::concurrency::{RunQueueConfig, TagConcurrencyCounter};
use crate::storage::surrealdb_backend::SurrealStorage;
use crate::storage::{
    CoordinatorRunInfo, EventRecord, EventType, RunStatus, ScopedStorageHandle, StorageBackend,
};
use crate::util::now_ts;

pub struct RunQueueCoordinator {
    config: RunQueueConfig,
    /// Scoped so daemons sharing a SurrealDB only dequeue and block their own runs.
    storage: ScopedStorageHandle<SurrealStorage>,
}

impl RunQueueCoordinator {
    pub fn new(config: RunQueueConfig, storage: ScopedStorageHandle<SurrealStorage>) -> Self {
        Self { config, storage }
    }

    pub fn config(&self) -> &RunQueueConfig {
        &self.config
    }

    pub fn code_location_id(&self) -> &str {
        self.storage.code_location_id()
    }

    /// One dequeue cycle. Returns run records that were transitioned to NotStarted.
    pub async fn tick(&self) -> Result<Vec<CoordinatorRunInfo>> {
        let scoped = self.storage.scoped();
        let (expired, in_progress_runs, mut queued) = scoped.coordinator_tick_query().await?;
        if expired > 0 {
            tracing::info!(
                target: "rivers::coordinator",
                freed = expired,
                "freed expired concurrency slot leases"
            );
        }

        let in_progress_count = in_progress_runs.len();
        let max = self.config.max_concurrent_runs;
        if max >= 0 && in_progress_count >= max as usize {
            tracing::trace!(
                target: "rivers::coordinator",
                in_progress = in_progress_count,
                max = max,
                "at run capacity, skipping dequeue cycle"
            );
            let reason = format!("global run limit ({in_progress_count}/{max})");
            let _ = scoped
                .set_block_reason_by_status(RunStatus::Queued, Some(&reason))
                .await;
            return Ok(vec![]);
        }

        let slots_available = if max < 0 {
            usize::MAX
        } else {
            (max as usize).saturating_sub(in_progress_count)
        };

        let mut counter = TagConcurrencyCounter::from_runs(
            &in_progress_runs,
            &self.config.tag_concurrency_limits,
        );

        // priority DESC, start_time ASC — sorting in Rust is faster than DB-side.
        queued.sort_unstable_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.start_time.cmp(&b.start_time))
        });

        let mut to_launch: Vec<CoordinatorRunInfo> = Vec::new();

        for run in queued {
            if to_launch.len() >= slots_available {
                break;
            }
            if let Some(reason) = counter.is_blocked(&run) {
                tracing::debug!(
                    target: "rivers::coordinator",
                    run_id = %run.run_id,
                    reason = %reason,
                    "run blocked by tag concurrency limit"
                );
                let _ = self
                    .storage
                    .backend()
                    .update_run_block_reason(&run.run_id, Some(&reason.to_string()))
                    .await;
                continue;
            }
            counter.record_launch(&run);
            to_launch.push(run);
        }

        for run in &to_launch {
            self.storage
                .backend()
                .update_run_status(&run.run_id, RunStatus::NotStarted, None)
                .await?;
            if let Err(e) = self
                .storage
                .backend()
                .store_event(&EventRecord {
                    code_location_id: run.code_location_id.clone(),
                    event_type: EventType::RunDequeued,
                    asset_key: None,
                    run_id: run.run_id.clone(),
                    partition_key: None,
                    timestamp: now_ts(),
                    metadata: vec![("priority".to_string(), run.priority.to_string())],
                    input_data_versions: vec![],
                })
                .await
            {
                tracing::warn!(run_id = %run.run_id, error = %e, "failed to persist RunDequeued event");
            }
            tracing::info!(
                target: "rivers::coordinator",
                run_id = %run.run_id,
                priority = run.priority,
                "dequeued run"
            );
        }

        Ok(to_launch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::concurrency::TagConcurrencyLimit;
    use crate::storage::{
        CodeLocationContext, DEFAULT_CODE_LOCATION_ID, LaunchedBy, RunRecord, RunStatus,
    };

    async fn make_storage() -> Arc<SurrealStorage> {
        Arc::new(
            SurrealStorage::new_memory()
                .await
                .expect("failed to create in-memory storage"),
        )
    }

    fn make_coordinator(
        config: RunQueueConfig,
        storage: Arc<SurrealStorage>,
    ) -> RunQueueCoordinator {
        RunQueueCoordinator::new(
            config,
            ScopedStorageHandle::new(storage, CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID)),
        )
    }

    fn queued_run(id: &str, priority: i32, start_time: i64, tags: Vec<(&str, &str)>) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".into()),
            status: RunStatus::Queued,
            start_time,
            end_time: None,
            tags: tags
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            node_names: vec![],
            priority,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
        }
    }

    fn started_run(id: &str, tags: Vec<(&str, &str)>) -> RunRecord {
        RunRecord {
            status: RunStatus::Started,
            ..queued_run(id, 0, 0, tags)
        }
    }

    #[tokio::test]
    async fn tick_empty_returns_nothing() {
        let storage = make_storage().await;
        let coord = make_coordinator(RunQueueConfig::default(), storage);
        let launched = coord.tick().await.unwrap();
        assert!(launched.is_empty());
    }

    #[tokio::test]
    async fn tick_dequeues_under_capacity() {
        let storage = make_storage().await;
        for i in 0..3 {
            storage
                .create_run(&queued_run(&format!("q-{i}"), 0, i as i64, vec![]))
                .await
                .unwrap();
        }

        let coord = make_coordinator(RunQueueConfig::default(), storage.clone());
        let launched = coord.tick().await.unwrap();

        assert_eq!(launched.len(), 3);
        for id in ["q-0", "q-1", "q-2"] {
            let run = storage.get_run(id).await.unwrap().unwrap();
            assert_eq!(run.status, RunStatus::NotStarted);
        }
    }

    #[tokio::test]
    async fn tick_global_limit_blocks_all_queued() {
        let storage = make_storage().await;
        for i in 0..2 {
            storage
                .create_run(&started_run(&format!("ip-{i}"), vec![]))
                .await
                .unwrap();
        }
        storage
            .create_run(&queued_run("q-0", 0, 0, vec![]))
            .await
            .unwrap();

        let config = RunQueueConfig {
            max_concurrent_runs: 2,
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage.clone());
        let launched = coord.tick().await.unwrap();

        assert!(launched.is_empty());
        let run = storage.get_run("q-0").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Queued);
        assert!(
            run.block_reason
                .as_deref()
                .is_some_and(|r| r.contains("global run limit"))
        );
    }

    #[tokio::test]
    async fn tick_partial_dequeue_fills_remaining_slots() {
        let storage = make_storage().await;
        for i in 0..3 {
            storage
                .create_run(&started_run(&format!("ip-{i}"), vec![]))
                .await
                .unwrap();
        }
        for i in 0..5 {
            storage
                .create_run(&queued_run(&format!("q-{i}"), 0, i as i64, vec![]))
                .await
                .unwrap();
        }

        let config = RunQueueConfig {
            max_concurrent_runs: 5,
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage);
        let launched = coord.tick().await.unwrap();

        assert_eq!(launched.len(), 2);
    }

    #[tokio::test]
    async fn tick_tag_limit_blocks_over_budget_runs() {
        let storage = make_storage().await;
        storage
            .create_run(&started_run("ip-0", vec![("env", "prod")]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("q-prod-1", 0, 0, vec![("env", "prod")]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("q-prod-2", 0, 1, vec![("env", "prod")]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("q-staging", 0, 2, vec![("env", "staging")]))
            .await
            .unwrap();

        let config = RunQueueConfig {
            max_concurrent_runs: 10,
            tag_concurrency_limits: vec![TagConcurrencyLimit {
                key: "env".into(),
                value: Some("prod".into()),
                per_unique_value: false,
                limit: 1,
            }],
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage.clone());
        let launched = coord.tick().await.unwrap();

        let launched_ids: Vec<&str> = launched.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(launched_ids, vec!["q-staging"]);

        let blocked = storage.get_run("q-prod-1").await.unwrap().unwrap();
        assert_eq!(blocked.status, RunStatus::Queued);
        assert!(blocked.block_reason.is_some());
    }

    #[tokio::test]
    async fn tick_respects_priority_order() {
        let storage = make_storage().await;
        storage
            .create_run(&queued_run("low", 1, 0, vec![]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("high", 10, 0, vec![]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("mid", 5, 0, vec![]))
            .await
            .unwrap();

        let config = RunQueueConfig {
            max_concurrent_runs: 2,
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage);
        let launched = coord.tick().await.unwrap();

        let ids: Vec<&str> = launched.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, vec!["high", "mid"]);
    }

    #[tokio::test]
    async fn tick_start_time_breaks_priority_ties() {
        let storage = make_storage().await;
        storage
            .create_run(&queued_run("later", 5, 200, vec![]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("earlier", 5, 100, vec![]))
            .await
            .unwrap();

        let config = RunQueueConfig {
            max_concurrent_runs: 1,
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage);
        let launched = coord.tick().await.unwrap();

        assert_eq!(launched.len(), 1);
        assert_eq!(launched[0].run_id, "earlier");
    }

    #[tokio::test]
    async fn tick_negative_max_means_unlimited() {
        let storage = make_storage().await;
        for i in 0..20 {
            storage
                .create_run(&queued_run(&format!("q-{i}"), 0, i as i64, vec![]))
                .await
                .unwrap();
        }

        let config = RunQueueConfig {
            max_concurrent_runs: -1,
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage);
        let launched = coord.tick().await.unwrap();

        assert_eq!(launched.len(), 20);
    }

    #[tokio::test]
    async fn tick_record_launch_blocks_later_same_tag_runs() {
        let storage = make_storage().await;
        storage
            .create_run(&queued_run("q-prod-1", 0, 0, vec![("env", "prod")]))
            .await
            .unwrap();
        storage
            .create_run(&queued_run("q-prod-2", 0, 1, vec![("env", "prod")]))
            .await
            .unwrap();

        let config = RunQueueConfig {
            max_concurrent_runs: 10,
            tag_concurrency_limits: vec![TagConcurrencyLimit {
                key: "env".into(),
                value: Some("prod".into()),
                per_unique_value: false,
                limit: 1,
            }],
            ..RunQueueConfig::default()
        };
        let coord = make_coordinator(config, storage.clone());
        let launched = coord.tick().await.unwrap();

        let ids: Vec<&str> = launched.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, vec!["q-prod-1"]);

        let blocked = storage.get_run("q-prod-2").await.unwrap().unwrap();
        assert_eq!(blocked.status, RunStatus::Queued);
        assert!(blocked.block_reason.is_some());
    }

    #[tokio::test]
    async fn tick_emits_run_dequeued_events() {
        let storage = make_storage().await;
        storage
            .create_run(&queued_run("q-0", 7, 0, vec![]))
            .await
            .unwrap();

        let coord = make_coordinator(RunQueueConfig::default(), storage.clone());
        coord.tick().await.unwrap();

        let events = storage.get_events_for_run("q-0").await.unwrap();
        let dequeued = events
            .iter()
            .find(|e| matches!(e.event_type, EventType::RunDequeued))
            .expect("expected RunDequeued event");
        assert_eq!(
            dequeued.metadata,
            vec![("priority".to_string(), "7".to_string())]
        );
    }

    /// Regression: two daemons sharing one SurrealDB must only dequeue runs
    /// belonging to their own code location. Before the fix, daemon B's
    /// `coordinator_tick_query` would return daemon A's queued runs, then
    /// `K8sRunBackend::launch` would stamp them with B's image+module, and
    /// the executor pod would crash on an unknown asset.
    #[tokio::test]
    async fn tick_isolates_runs_by_code_location() {
        let storage = make_storage().await;

        let run_a = RunRecord {
            code_location_id: "cl-a".to_string(),
            ..queued_run("a-1", 0, 0, vec![])
        };
        storage.create_run(&run_a).await.unwrap();

        let run_b = RunRecord {
            code_location_id: "cl-b".to_string(),
            ..queued_run("b-1", 0, 1, vec![])
        };
        storage.create_run(&run_b).await.unwrap();

        let coord_a = RunQueueCoordinator::new(
            RunQueueConfig::default(),
            ScopedStorageHandle::new(storage.clone(), CodeLocationContext::new("cl-a")),
        );
        let launched_a = coord_a.tick().await.unwrap();
        assert_eq!(launched_a.len(), 1);
        assert_eq!(launched_a[0].run_id, "a-1");
        assert_eq!(launched_a[0].code_location_id, "cl-a");

        // CL-B's run is still Queued — daemon A did NOT touch it.
        let b_after_a = storage.get_run("b-1").await.unwrap().unwrap();
        assert_eq!(b_after_a.status, RunStatus::Queued);

        let coord_b = RunQueueCoordinator::new(
            RunQueueConfig::default(),
            ScopedStorageHandle::new(storage.clone(), CodeLocationContext::new("cl-b")),
        );
        let launched_b = coord_b.tick().await.unwrap();
        assert_eq!(launched_b.len(), 1);
        assert_eq!(launched_b[0].run_id, "b-1");
        assert_eq!(launched_b[0].code_location_id, "cl-b");
    }

    /// `set_block_reason_by_status` must scope to the calling CL: when CL-A
    /// hits its global limit, it must not stamp block reasons on CL-B's
    /// queued runs.
    #[tokio::test]
    async fn block_reason_scoped_to_code_location() {
        let storage = make_storage().await;

        for i in 0..2 {
            let r = RunRecord {
                code_location_id: "cl-a".to_string(),
                ..started_run(&format!("a-ip-{i}"), vec![])
            };
            storage.create_run(&r).await.unwrap();
        }
        let a_q = RunRecord {
            code_location_id: "cl-a".to_string(),
            ..queued_run("a-q", 0, 0, vec![])
        };
        storage.create_run(&a_q).await.unwrap();

        let b_q = RunRecord {
            code_location_id: "cl-b".to_string(),
            ..queued_run("b-q", 0, 0, vec![])
        };
        storage.create_run(&b_q).await.unwrap();

        let coord_a = RunQueueCoordinator::new(
            RunQueueConfig {
                max_concurrent_runs: 2,
                ..RunQueueConfig::default()
            },
            ScopedStorageHandle::new(storage.clone(), CodeLocationContext::new("cl-a")),
        );
        let launched = coord_a.tick().await.unwrap();
        assert!(launched.is_empty());

        let a_after = storage.get_run("a-q").await.unwrap().unwrap();
        assert!(
            a_after
                .block_reason
                .as_deref()
                .is_some_and(|r| r.contains("global run limit"))
        );

        // CL-B's queued run is NOT touched.
        let b_after = storage.get_run("b-q").await.unwrap().unwrap();
        assert_eq!(b_after.block_reason, None);
    }
}
