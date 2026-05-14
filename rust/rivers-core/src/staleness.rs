//! Staleness computation for asset records.

use std::collections::HashMap;

use crate::storage::{AssetRecord, StaleCause, StaleCauseCategory, StaleStatus};

/// Compute staleness for all assets given their records and the dependency graph edges.
///
/// `edges` are `(from, to)` pairs where `from` depends on `to` (i.e., `to` is upstream of `from`).
/// This matches the convention used by `GraphTopology.edges`.
///
/// Returns a map from asset_key to (status, causes).
pub fn compute_staleness(
    records: &[AssetRecord],
    edges: &[(String, String)],
) -> HashMap<String, (StaleStatus, Vec<StaleCause>)> {
    let record_map: HashMap<&str, &AssetRecord> =
        records.iter().map(|r| (r.asset_key.as_str(), r)).collect();

    let mut upstream_deps: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut downstream_deps: HashMap<&str, Vec<&str>> = HashMap::new();

    for (from, to) in edges {
        upstream_deps
            .entry(from.as_str())
            .or_default()
            .push(to.as_str());
        downstream_deps
            .entry(to.as_str())
            .or_default()
            .push(from.as_str());
    }

    // Kahn's algorithm — upstreams must be processed before downstreams so
    // transitive staleness propagates correctly below.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    for r in records {
        in_degree.entry(r.asset_key.as_str()).or_insert(0);
    }
    for (from, _to) in edges {
        *in_degree.entry(from.as_str()).or_insert(0) += 1;
    }

    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|&(_, &deg)| deg == 0)
        .map(|(&k, _)| k)
        .collect();
    queue.sort(); // deterministic order
    let mut order: Vec<&str> = Vec::with_capacity(records.len());

    while let Some(node) = queue.pop() {
        order.push(node);
        if let Some(downs) = downstream_deps.get(node) {
            for &d in downs {
                if let Some(deg) = in_degree.get_mut(d) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(d);
                        queue.sort(); // keep deterministic
                    }
                }
            }
        }
    }

    let mut results: HashMap<String, (StaleStatus, Vec<StaleCause>)> = HashMap::new();

    for &asset_key in &order {
        let record = match record_map.get(asset_key) {
            Some(r) => r,
            None => continue,
        };

        let mut causes: Vec<StaleCause> = Vec::new();

        if record.last_data_version.is_none() {
            results.insert(asset_key.to_string(), (StaleStatus::Missing, vec![]));
            continue;
        }

        if record.code_version.is_some()
            && record.code_version != record.last_materialization_code_version
        {
            causes.push(StaleCause {
                asset_key: asset_key.to_string(),
                category: StaleCauseCategory::Code,
                reason: "code version changed".to_string(),
            });
        }

        let input_versions: HashMap<&str, &str> = record
            .last_input_data_versions
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        if let Some(deps) = upstream_deps.get(asset_key) {
            for &dep_key in deps {
                if let Some((dep_status, _)) = results.get(dep_key)
                    && (*dep_status == StaleStatus::Stale || *dep_status == StaleStatus::Missing)
                {
                    causes.push(StaleCause {
                        asset_key: asset_key.to_string(),
                        category: StaleCauseCategory::Data {
                            dependency: dep_key.to_string(),
                        },
                        reason: format!("upstream '{}' is stale", dep_key),
                    });
                    continue;
                }

                if let Some(dep_record) = record_map.get(dep_key) {
                    let current_dep_version = dep_record.last_data_version.as_deref();
                    let consumed_dep_version = input_versions.get(dep_key).copied();

                    match (current_dep_version, consumed_dep_version) {
                        (Some(current), Some(consumed)) if current != consumed => {
                            causes.push(StaleCause {
                                asset_key: asset_key.to_string(),
                                category: StaleCauseCategory::Data {
                                    dependency: dep_key.to_string(),
                                },
                                reason: format!("upstream '{}' has new data version", dep_key),
                            });
                        }
                        (Some(_), None) => {
                            causes.push(StaleCause {
                                asset_key: asset_key.to_string(),
                                category: StaleCauseCategory::Data {
                                    dependency: dep_key.to_string(),
                                },
                                reason: format!("upstream '{}' has data not yet consumed", dep_key),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }

        let status = if causes.is_empty() {
            StaleStatus::UpToDate
        } else {
            StaleStatus::Stale
        };

        results.insert(asset_key.to_string(), (status, causes));
    }

    // Assets not reached by the topo sort (orphaned from edges).
    for record in records {
        if !results.contains_key(&record.asset_key) {
            if record.last_data_version.is_none() {
                results.insert(record.asset_key.clone(), (StaleStatus::Missing, vec![]));
            } else {
                let mut causes = Vec::new();
                if record.code_version.is_some()
                    && record.code_version != record.last_materialization_code_version
                {
                    causes.push(StaleCause {
                        asset_key: record.asset_key.clone(),
                        category: StaleCauseCategory::Code,
                        reason: "code version changed".to_string(),
                    });
                }
                let status = if causes.is_empty() {
                    StaleStatus::UpToDate
                } else {
                    StaleStatus::Stale
                };
                results.insert(record.asset_key.clone(), (status, causes));
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(key: &str) -> AssetRecord {
        AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    #[test]
    fn test_never_materialized_is_missing() {
        let records = vec![make_record("a")];
        let result = compute_staleness(&records, &[]);
        assert_eq!(result["a"].0, StaleStatus::Missing);
    }

    #[test]
    fn test_materialized_no_deps_is_up_to_date() {
        let mut a = make_record("a");
        a.last_data_version = Some("v1".to_string());
        let result = compute_staleness(&[a], &[]);
        assert_eq!(result["a"].0, StaleStatus::UpToDate);
    }

    #[test]
    fn test_code_version_changed_is_stale() {
        let mut a = make_record("a");
        a.code_version = Some("v2".to_string());
        a.last_data_version = Some("dv1".to_string());
        a.last_materialization_code_version = Some("v1".to_string());
        let result = compute_staleness(&[a], &[]);
        assert_eq!(result["a"].0, StaleStatus::Stale);
        assert_eq!(result["a"].1.len(), 1);
        assert_eq!(result["a"].1[0].category, StaleCauseCategory::Code);
    }

    #[test]
    fn test_code_version_same_is_up_to_date() {
        let mut a = make_record("a");
        a.code_version = Some("v1".to_string());
        a.last_data_version = Some("dv1".to_string());
        a.last_materialization_code_version = Some("v1".to_string());
        let result = compute_staleness(&[a], &[]);
        assert_eq!(result["a"].0, StaleStatus::UpToDate);
    }

    #[test]
    fn test_upstream_data_changed_is_stale() {
        let mut a = make_record("a");
        a.last_data_version = Some("dv2".to_string()); // upstream updated

        let mut b = make_record("b");
        b.last_data_version = Some("dv_b".to_string());
        b.last_input_data_versions = vec![("a".to_string(), "dv1".to_string())]; // consumed old

        let edges = vec![("b".to_string(), "a".to_string())];
        let result = compute_staleness(&[a, b], &edges);

        assert_eq!(result["a"].0, StaleStatus::UpToDate);
        assert_eq!(result["b"].0, StaleStatus::Stale);
        assert_eq!(
            result["b"].1[0].category,
            StaleCauseCategory::Data {
                dependency: "a".to_string()
            }
        );
    }

    #[test]
    fn test_upstream_up_to_date_downstream_up_to_date() {
        let mut a = make_record("a");
        a.last_data_version = Some("dv1".to_string());

        let mut b = make_record("b");
        b.last_data_version = Some("dv_b".to_string());
        b.last_input_data_versions = vec![("a".to_string(), "dv1".to_string())]; // matches

        let edges = vec![("b".to_string(), "a".to_string())];
        let result = compute_staleness(&[a, b], &edges);

        assert_eq!(result["a"].0, StaleStatus::UpToDate);
        assert_eq!(result["b"].0, StaleStatus::UpToDate);
    }

    #[test]
    fn test_transitive_staleness() {
        // a -> b -> c, where a has new code
        let mut a = make_record("a");
        a.code_version = Some("v2".to_string());
        a.last_data_version = Some("dv1".to_string());
        a.last_materialization_code_version = Some("v1".to_string());

        let mut b = make_record("b");
        b.last_data_version = Some("dv_b".to_string());
        b.last_input_data_versions = vec![("a".to_string(), "dv1".to_string())];

        let mut c = make_record("c");
        c.last_data_version = Some("dv_c".to_string());
        c.last_input_data_versions = vec![("b".to_string(), "dv_b".to_string())];

        let edges = vec![
            ("b".to_string(), "a".to_string()),
            ("c".to_string(), "b".to_string()),
        ];
        let result = compute_staleness(&[a, b, c], &edges);

        assert_eq!(result["a"].0, StaleStatus::Stale);
        assert_eq!(result["b"].0, StaleStatus::Stale);
        assert_eq!(result["c"].0, StaleStatus::Stale);
    }

    #[test]
    fn test_upstream_missing_makes_downstream_stale() {
        let a = make_record("a");

        let mut b = make_record("b");
        b.last_data_version = Some("dv_b".to_string());

        let edges = vec![("b".to_string(), "a".to_string())];
        let result = compute_staleness(&[a, b], &edges);

        assert_eq!(result["a"].0, StaleStatus::Missing);
        assert_eq!(result["b"].0, StaleStatus::Stale);
    }

    #[test]
    fn test_no_code_version_no_staleness_check() {
        let mut a = make_record("a");
        a.last_data_version = Some("dv1".to_string());
        let result = compute_staleness(&[a], &[]);
        assert_eq!(result["a"].0, StaleStatus::UpToDate);
    }

    #[test]
    fn test_multiple_causes() {
        let mut a = make_record("a");
        a.last_data_version = Some("dv2".to_string());

        let mut b = make_record("b");
        b.code_version = Some("v2".to_string());
        b.last_data_version = Some("dv_b".to_string());
        b.last_materialization_code_version = Some("v1".to_string());
        b.last_input_data_versions = vec![("a".to_string(), "dv1".to_string())];

        let edges = vec![("b".to_string(), "a".to_string())];
        let result = compute_staleness(&[a, b], &edges);

        assert_eq!(result["b"].0, StaleStatus::Stale);
        assert_eq!(result["b"].1.len(), 2);
    }
}
