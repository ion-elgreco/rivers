use super::*;

// ── get_observations_since tests ──

#[tokio::test]
async fn test_get_observations_since() {
    let storage = make_storage().await;
    register(&storage, &["ext_a", "ext_b"]).await;

    // Store an observation at ts=1000
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("ext_a".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Store an observation at ts=2000
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("ext_b".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 2000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Store a materialization at ts=1500 (should NOT be returned)
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("m1".to_string()),
            },
            asset_key: Some("ext_a".to_string()),
            run_id: "r1".to_string(),
            partition_key: None,
            timestamp: 1500,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Query observations since ts=0 — should return both observations, not the materialization
    // Ordered DESC by timestamp
    let all = storage
        .get_observations_since(crate::storage::DEFAULT_CODE_LOCATION_ID, 0)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    let expected_0 = StoredEvent {
        id: all[0].id.clone(),
        event_type: EventType::Observation {
            data_version: Some("v2".to_string()),
        },
        asset_key: Some("ext_b".to_string()),
        run_id: String::new(),
        partition_key: None,
        timestamp: 2000,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    let expected_1 = StoredEvent {
        id: all[1].id.clone(),
        event_type: EventType::Observation {
            data_version: Some("v1".to_string()),
        },
        asset_key: Some("ext_a".to_string()),
        run_id: String::new(),
        partition_key: None,
        timestamp: 1000,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    assert_eq!(all[0], expected_0);
    assert_eq!(all[1], expected_1);

    // Query observations since ts=1000 — should return only the one at ts=2000
    let recent = storage
        .get_observations_since(crate::storage::DEFAULT_CODE_LOCATION_ID, 1000)
        .await
        .unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0], expected_0);

    // Query observations since ts=2000 — should return nothing
    let none = storage
        .get_observations_since(crate::storage::DEFAULT_CODE_LOCATION_ID, 2000)
        .await
        .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn test_condition_evals_store_and_retrieve() {
    use super::ConditionEvalRecord;

    let storage = make_storage().await;

    let tree_json = serde_json::to_vec(&serde_json::json!({
            "node_idx": 0, "label": "All of", "node_type": "And",
            "status": "True", "children": [
                {"node_idx": 1, "label": "missing", "node_type": "Leaf", "status": "True", "children": []}
            ]
        }))
        .unwrap();

    let evals: Vec<ConditionEvalRecord> = (0..5)
        .map(|i| ConditionEvalRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tick_id: format!("tick_{i}"),
            timestamp: 1000 + i * 30,
            fired: i % 2 == 0,
            eval_duration_us: 50 + i as u64 * 10,
            run_ids: vec![],
            tree_json: tree_json.clone(),
            selection_json: None,
        })
        .collect();
    let ids = storage.store_condition_evals_batch(&evals).await.unwrap();
    assert_eq!(ids.len(), 5);

    // Retrieve — should be ordered DESC by timestamp, full struct equality
    let stored = storage
        .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 100)
        .await
        .unwrap();
    assert_eq!(stored.len(), 5);
    for (idx, actual) in stored.iter().enumerate() {
        let i = 4 - idx as i64; // maps to original index (DESC)
        let expected = StoredConditionEval {
            id: actual.id.clone(),
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "my_asset".to_string(),
            tick_id: format!("tick_{i}"),
            timestamp: 1000 + i * 30,
            fired: i % 2 == 0,
            eval_duration_us: 50 + i as u64 * 10,
            run_ids: vec![],
            tree_json: tree_json.clone(),
            selection_json: None,
        };
        assert_eq!(*actual, expected);
    }

    // Limit works
    let limited = storage
        .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 2)
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);

    // Different asset key returns empty
    let other = storage
        .get_condition_evals(crate::storage::DEFAULT_CODE_LOCATION_ID, "other_asset", 100)
        .await
        .unwrap();
    assert!(other.is_empty());
}
