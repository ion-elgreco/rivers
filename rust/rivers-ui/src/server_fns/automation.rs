//! Server functions for querying automation data (jobs, schedules, sensors, conditions).

use leptos::prelude::*;

use crate::types::{
    ConditionEvalRecord, ConditionTickDetail, ConditionTickRecord, JobRecord, ScheduleRecord,
    SensorRecord, TickRecord,
};

/// Format the next firing of `expr` after `now` in `"%Y-%m-%d %H:%M:%S UTC"`,
/// or `None` if `expr` doesn't parse / has no next occurrence.
///
/// `.with_seconds_optional()` accepts both 5-field (`"*/5 * * * *"`) and
/// 6-field (`"*/30 0 0 * * *"`) expressions — the daemon scheduler, partition
/// defs, and condition eval all parse with this flag, so the UI's "next tick"
/// preview must match or it'll silently disagree with what the daemon ticks.
///
/// Gated behind the `ssr` feature because `croner` is an `ssr`-only dep — the
/// `#[server]` callers below stub themselves out under `hydrate` (the macro
/// emits a network-call shim instead of the body), so this helper is unused
/// in wasm builds.
#[cfg(feature = "ssr")]
fn next_occurrence_from(expr: &str, now: chrono::DateTime<chrono::Utc>) -> Option<String> {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse(expr)
        .ok()
        .and_then(|c| c.find_next_occurrence(&now, false).ok())
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
}

/// Compute next tick time for a cron expression. Returns formatted string.
#[server]
pub async fn get_next_tick(cron_expression: String) -> Result<Option<String>, ServerFnError> {
    Ok(next_occurrence_from(&cron_expression, chrono::Utc::now()))
}

/// Compute next tick times for multiple cron expressions in a single request.
#[server]
pub async fn get_next_ticks(
    cron_expressions: Vec<(String, String)>,
) -> Result<Vec<(String, Option<String>)>, ServerFnError> {
    let now = chrono::Utc::now();
    Ok(cron_expressions
        .into_iter()
        .map(|(name, expr)| (name, next_occurrence_from(&expr, now)))
        .collect())
}

#[server]
pub async fn get_schedules(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<ScheduleRecord>, ServerFnError> {
    use cron_descriptor::cronparser::cron_expression_descriptor::get_description_cron;
    use rivers_api::rivers::GetSchedulesRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .get_schedules(GetSchedulesRequest {})
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp
        .into_inner()
        .schedules
        .into_iter()
        .map(|s| {
            let cron_description = get_description_cron(&s.cron_schedule).ok();
            ScheduleRecord {
                name: s.name,
                cron_schedule: s.cron_schedule,
                cron_description,
                job_name: s.job_name,
                status: s.status,
                timezone: s.timezone,
                description: s.description,
                tags: s.tags.into_iter().map(|t| (t.key, t.value)).collect(),
            }
        })
        .collect())
}

#[server]
pub async fn get_sensors(
    loc_ns: String,
    loc_name: String,
) -> Result<Vec<SensorRecord>, ServerFnError> {
    use rivers_api::rivers::GetSensorsRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .get_sensors(GetSensorsRequest {})
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp
        .into_inner()
        .sensors
        .into_iter()
        .map(|s| SensorRecord {
            name: s.name,
            job_name: s.job_name,
            status: s.status,
            minimum_interval: s.minimum_interval,
            description: s.description,
            asset_selection: s.asset_selection,
            tags: s.tags.into_iter().map(|t| (t.key, t.value)).collect(),
        })
        .collect())
}

#[server]
pub async fn get_jobs(loc_ns: String, loc_name: String) -> Result<Vec<JobRecord>, ServerFnError> {
    use rivers_api::rivers::GetJobsRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .get_jobs(GetJobsRequest {})
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(resp
        .into_inner()
        .jobs
        .into_iter()
        .map(|j| JobRecord {
            name: j.name,
            asset_selection: j.asset_selection,
            executor_type: j.executor_type,
        })
        .collect())
}

#[server]
pub async fn get_ticks(
    loc_ns: String,
    loc_name: String,
    automation_name: String,
    limit: Option<usize>,
) -> Result<Vec<TickRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_ticks(&automation_name, limit.unwrap_or(50))
        .await
        .map(|ticks| {
            ticks
                .into_iter()
                .map(|t| TickRecord {
                    id: format!("{:?}", t.id),
                    automation_name: t.automation_name,
                    automation_type: t.automation_type,
                    status: t.status,
                    timestamp: t.timestamp,
                    run_ids: t.run_ids,
                    backfill_ids: t.backfill_ids,
                    skip_reason: t.skip_reason,
                    error: t.error,
                    cursor: t.cursor,
                })
                .collect()
        })
        .map_err(|e| ServerFnError::new(e.to_string()))
}

#[server]
pub async fn evaluate_schedule(
    loc_ns: String,
    loc_name: String,
    schedule_name: String,
) -> Result<Vec<String>, ServerFnError> {
    use rivers_api::rivers::EvaluateScheduleRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .evaluate_schedule(EvaluateScheduleRequest { schedule_name })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let inner = resp.into_inner();
    if let Some(reason) = inner.skip_reason {
        return Err(ServerFnError::new(format!("Skipped: {reason}")));
    }
    Ok(inner.run_ids)
}

#[server]
pub async fn evaluate_sensor(
    loc_ns: String,
    loc_name: String,
    sensor_name: String,
) -> Result<Vec<String>, ServerFnError> {
    use rivers_api::rivers::EvaluateSensorRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .evaluate_sensor(EvaluateSensorRequest { sensor_name })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let inner = resp.into_inner();
    if let Some(reason) = inner.skip_reason {
        return Err(ServerFnError::new(format!("Skipped: {reason}")));
    }
    Ok(inner.run_ids)
}

#[server]
pub async fn observe_asset(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
) -> Result<bool, ServerFnError> {
    use rivers_api::rivers::ObserveAssetRequest;

    let state = expect_context::<crate::state::AppState>();
    let (_, mut client) = state
        .connect_to(&loc_ns, &loc_name)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let resp = client
        .observe_asset(ObserveAssetRequest { asset_key })
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    let inner = resp.into_inner();
    if !inner.success {
        return Err(ServerFnError::new(
            inner
                .error
                .unwrap_or_else(|| "Observation failed".to_string()),
        ));
    }
    Ok(true)
}

/// Per-asset condition evaluation history for the asset-detail page's
/// automation tab. Joins each fired eval against its tick's runs/backfills
/// and filters to those that actually touched `asset_key` — stored evals
/// have empty `run_ids`/`backfill_ids` because the eval is written before
/// any runs/backfills exist. Defaults to 50 most-recent evals.
#[server]
pub async fn get_condition_evals(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    limit: Option<usize>,
) -> Result<Vec<ConditionEvalRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    use std::collections::{HashMap, HashSet};
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();

    let mut evals: Vec<ConditionEvalRecord> = state
        .storage
        .for_code_location(&ctx)
        .get_condition_evals(&asset_key, limit.unwrap_or(50))
        .await
        .map(|evals| {
            evals
                .into_iter()
                .map(ConditionEvalRecord::from_stored)
                .collect()
        })
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    // Stored eval records have run_ids/backfill_ids empty because the eval is
    // written before any runs/backfills exist. Reconstruct per-eval by joining
    // each eval's tick against runs (`RunRecord.node_names`) and backfills
    // (`BackfillRecord.asset_selection`), filtering to this asset.
    let fired_tick_ids: HashSet<String> = evals
        .iter()
        .filter(|e| e.fired)
        .map(|e| e.tick_id.clone())
        .collect();

    if !fired_tick_ids.is_empty() {
        let ticks = state
            .storage
            .for_code_location(&ctx)
            .get_condition_ticks(fired_tick_ids.len().max(200))
            .await
            .unwrap_or_default();

        let mut tick_work: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
        for t in ticks {
            let tid = format!("{}:{:?}", t.id.table.as_str(), t.id.key);
            if fired_tick_ids.contains(&tid) {
                tick_work.insert(tid, (t.run_ids, t.backfill_ids));
            }
        }

        let all_run_ids: Vec<String> = tick_work
            .values()
            .flat_map(|(r, _)| r.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let all_bf_ids: HashSet<String> = tick_work
            .values()
            .flat_map(|(_, b)| b.iter().cloned())
            .collect();

        // Runs branch and backfills branch are independent — poll them
        // concurrently. Inside each branch, remain sequential (N is small).
        let asset_key_ref = &asset_key;
        let storage_ref = &state.storage;
        let (asset_run_ids, asset_bf_ids) = tokio::join!(
            async move {
                let mut out: HashSet<String> = HashSet::new();
                if all_run_ids.is_empty() {
                    return out;
                }
                if let Ok(runs) = storage_ref.get_runs_by_ids(&all_run_ids, None).await {
                    for r in runs {
                        if r.node_names.contains(asset_key_ref) {
                            out.insert(r.run_id);
                        }
                    }
                }
                out
            },
            async move {
                let mut out: HashSet<String> = HashSet::new();
                for bid in &all_bf_ids {
                    if let Ok(Some(bf)) = storage_ref.get_backfill(bid).await
                        && bf.asset_selection.contains(asset_key_ref)
                    {
                        out.insert(bf.backfill_id);
                    }
                }
                out
            },
        );

        for eval in &mut evals {
            if !eval.fired {
                continue;
            }
            if let Some((runs, bfs)) = tick_work.get(&eval.tick_id) {
                eval.run_ids = runs
                    .iter()
                    .filter(|id| asset_run_ids.contains(*id))
                    .cloned()
                    .collect();
                eval.backfill_ids = bfs
                    .iter()
                    .filter(|id| asset_bf_ids.contains(*id))
                    .cloned()
                    .collect();
            }
        }
    }

    Ok(evals)
}

/// Fetch the latest condition evaluation for each of the given asset keys.
/// Returns a map of asset_key → latest ConditionEvalRecord.
#[server]
pub async fn get_latest_condition_evals(
    loc_ns: String,
    loc_name: String,
    asset_keys: Vec<String>,
) -> Result<Vec<(String, Option<ConditionEvalRecord>)>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);
    let mut results = Vec::with_capacity(asset_keys.len());
    for key in asset_keys {
        let latest = scoped
            .get_condition_evals(&key, 1)
            .await
            .ok()
            .and_then(|evals| evals.into_iter().next())
            .map(ConditionEvalRecord::from_stored);
        results.push((key, latest));
    }
    Ok(results)
}

#[server]
pub async fn get_condition_ticks(
    loc_ns: String,
    loc_name: String,
    limit: Option<usize>,
) -> Result<Vec<ConditionTickRecord>, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    state
        .storage
        .for_code_location(&ctx)
        .get_condition_ticks(limit.unwrap_or(50))
        .await
        .map(|ticks| {
            ticks
                .into_iter()
                .map(ConditionTickRecord::from_stored)
                .collect()
        })
        .map_err(|e| ServerFnError::new(e.to_string()))
}

/// Expand one condition tick into its per-asset evaluation rows for the
/// automation tick-detail view. Joins each row's tick against runs and
/// backfills to populate per-asset `run_ids` / `backfill_ids` (the stored
/// eval records leave these empty since the eval is written before any
/// runs/backfills exist).
#[server]
pub async fn get_condition_tick_detail(
    loc_ns: String,
    loc_name: String,
    tick_id: String,
) -> Result<ConditionTickDetail, ServerFnError> {
    use rivers_core::storage::StorageBackend;
    use std::collections::HashMap;
    let ctx = super::resolve_identity(&loc_ns, &loc_name).await?;
    let state = expect_context::<crate::state::AppState>();
    let scoped = state.storage.for_code_location(&ctx);

    let mut evals: Vec<ConditionEvalRecord> = scoped
        .get_condition_evals_for_tick(&tick_id)
        .await
        .map(|evals| {
            evals
                .into_iter()
                .map(ConditionEvalRecord::from_stored)
                .collect()
        })
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    // Stored eval records have run_ids/backfill_ids empty because the eval is
    // written before any runs/backfills exist. We reconstruct the per-asset
    // mapping at read time by joining against `RunRecord.node_names` (runs)
    // and `BackfillRecord.asset_selection` (backfills).
    if !evals.is_empty() {
        // Single lookup for the tick; pull both run_ids and backfill_ids off it.
        let (tick_run_ids, tick_backfill_ids) = scoped
            .get_condition_ticks(200)
            .await
            .ok()
            .and_then(|ticks| {
                ticks
                    .into_iter()
                    .find(|t| format!("{}:{:?}", t.id.table.as_str(), t.id.key) == tick_id)
                    .map(|t| (t.run_ids, t.backfill_ids))
            })
            .unwrap_or_default();

        // asset_key → Vec<run_id> built from RunRecord.node_names join.
        let mut asset_to_runs: HashMap<String, Vec<String>> = HashMap::new();
        if !tick_run_ids.is_empty()
            && let Ok(runs) = state.storage.get_runs_by_ids(&tick_run_ids, None).await
        {
            for r in runs {
                for a in r.node_names {
                    asset_to_runs.entry(a).or_default().push(r.run_id.clone());
                }
            }
        }

        // asset_key → Vec<backfill_id>. Per-backfill fetch is unavoidable with
        // the current storage API, but tick_backfill_ids is typically small.
        let mut asset_to_backfills: HashMap<String, Vec<String>> = HashMap::new();
        for bf_id in &tick_backfill_ids {
            if let Ok(Some(bf)) = state.storage.get_backfill(bf_id).await {
                for a in &bf.asset_selection {
                    asset_to_backfills
                        .entry(a.clone())
                        .or_default()
                        .push(bf.backfill_id.clone());
                }
            }
        }

        for eval in &mut evals {
            if let Some(runs) = asset_to_runs.remove(&eval.asset_key) {
                eval.run_ids = runs;
            }
            if let Some(bfs) = asset_to_backfills.remove(&eval.asset_key) {
                eval.backfill_ids = bfs;
            }
        }
    }

    Ok(ConditionTickDetail { evals })
}

#[cfg(all(test, feature = "ssr"))]
mod tests {
    use super::next_occurrence_from;
    use chrono::TimeZone;

    #[test]
    fn five_field_cron_parses() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        // Every 5 minutes — next firing after midnight is 00:05.
        let next = next_occurrence_from("*/5 * * * *", now).expect("should parse");
        assert_eq!(next, "2026-01-01 00:05:00 UTC");
    }

    #[test]
    fn six_field_cron_parses() {
        // Regression: parser used to drop 6-field expressions, leaving the
        // UI's "next tick" preview empty for schedules the daemon happily
        // ticks (e.g. sub-minute or seconds-precise crons).
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        // Every 30 seconds at midnight — next firing is 00:00:30.
        let next = next_occurrence_from("*/30 0 0 * * *", now).expect("should parse");
        assert_eq!(next, "2026-01-01 00:00:30 UTC");
    }

    #[test]
    fn invalid_cron_returns_none() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(next_occurrence_from("not a cron", now).is_none());
    }
}
