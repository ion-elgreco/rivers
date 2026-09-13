//! Every storage method, measured.
//!
//! The suite covers the whole `AnyStorage` surface — the 21 inherent methods,
//! the 47 on `StorageBackend`, and the 47 reached through `ScopedStorage`.
//! Covering all of them rather than a chosen few is the point: a backend that
//! wins the paths someone thought to measure can still lose the ones they did
//! not.
//!
//! Calls go through `ScopedStorageHandle`, the same wrapper the operator, UI,
//! and daemon use. Reaching past it would measure a path rivers never takes.

use std::collections::HashMap;

use anyhow::Result;
use rivers_core::storage::{
    AssetRecord, BackfillFilter, BackfillStatus, ConditionEvalRecord, EventRecord, LogRecord,
    PartitionKey, RunFilter, RunOutcome, RunRecord, RunStatus, SortOrder, StorageBackend,
    TickRecord,
};

use crate::fixtures::{
    self, ASSETS, AUTOMATION, Config, Handle, PARTITIONS_DEF, POOL, SEED_ASSET, SEED_BACKFILL,
    SEED_RUN, SEED_TICK_ID, now_nanos,
};
use crate::harness::{Measured, Row, measure, measure_with_setup};

/// Collects rows for one backend, so each scenario is one call rather than
/// five lines of struct literal.
struct Bench<'a> {
    handle: &'a Handle,
    backend: String,
    cfg: &'a Config,
    rows: Vec<Row>,
}

impl<'a> Bench<'a> {
    fn new(handle: &'a Handle, backend: &str, cfg: &'a Config) -> Self {
        Self {
            handle,
            backend: backend.to_string(),
            cfg,
            rows: Vec::new(),
        }
    }

    fn push(&mut self, scenario: &str, unit: &str, measured: Measured) {
        self.rows.push(Row {
            scenario: scenario.to_string(),
            backend: self.backend.clone(),
            unit: unit.to_string(),
            measured,
        });
    }
}

macro_rules! b {
    ($b:expr, $name:literal, $unit:literal, $units:expr, $f:expr) => {{
        let m = measure($units, $b.cfg.warmup, $b.cfg.iters, $f).await?;
        $b.push($name, $unit, m);
    }};
}

/// Same, with untimed per-iteration setup for operations that consume state.
macro_rules! bs {
    ($b:expr, $name:literal, $unit:literal, $units:expr, $setup:expr, $f:expr) => {{
        let m = measure_with_setup($units, $b.cfg.warmup, $b.cfg.iters, $setup, $f).await?;
        $b.push($name, $unit, m);
    }};
}

pub async fn run_all(handle: &Handle, backend: &str, cfg: &Config) -> Result<Vec<Row>> {
    let mut b = Bench::new(handle, backend, cfg);
    event_writes(&mut b).await?;
    event_reads(&mut b).await?;
    run_writes(&mut b).await?;
    run_reads(&mut b).await?;
    run_pages(&mut b).await?;
    run_coordination(&mut b).await?;
    assets(&mut b).await?;
    partitions(&mut b).await?;
    backfills(&mut b).await?;
    pools(&mut b).await?;
    ticks(&mut b).await?;
    kv_and_state(&mut b).await?;
    Ok(b.rows)
}

// ── events ──

async fn event_writes(b: &mut Bench<'_>) -> Result<()> {
    let (h, cfg) = (b.handle, b.cfg);
    let cl = h.code_location_id();

    // Profiling put the event drain at 99% of a large backfill's cost. It is
    // the single number that decides whether a backend is usable at scale.
    b!(
        b,
        "store_events (batch)",
        "events",
        cfg.event_batch as u64,
        |i| async move {
            let events: Vec<EventRecord> = (0..cfg.event_batch)
                .map(|j| {
                    fixtures::event(
                        cl,
                        &format!("drain_run_{i}"),
                        &format!("asset_{}", j % ASSETS),
                        i * cfg.event_batch + j,
                    )
                })
                .collect();
            h.backend().store_events(&events).await.map(|_| ())
        }
    );

    b!(b, "store_event (single)", "events", 1, |i| async move {
        h.backend()
            .store_event(&fixtures::event(cl, "single_run", SEED_ASSET, i))
            .await
            .map(|_| ())
    });

    b!(b, "store_run_logs (100)", "rows", 100, |i| async move {
        let logs: Vec<LogRecord> = (0..100)
            .map(|j| fixtures::log(cl, &format!("log_run_{i}_{j}"), "step"))
            .collect();
        h.backend().store_run_logs(&logs).await
    });
    Ok(())
}

async fn event_reads(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    b!(b, "get_events_for_run", "calls", 1, |_| async move {
        h.backend().get_events_for_run(SEED_RUN).await.map(|_| ())
    });
    b!(b, "get_events_for_asset", "calls", 1, |_| async move {
        h.scoped()
            .get_events_for_asset(SEED_ASSET, 100)
            .await
            .map(|_| ())
    });
    b!(b, "get_events_for_step", "calls", 1, |_| async move {
        h.backend()
            .get_events_for_step(SEED_RUN, SEED_ASSET)
            .await
            .map(|_| ())
    });
    b!(b, "get_step_terminal_events", "calls", 1, |_| async move {
        h.backend()
            .get_step_terminal_events(SEED_RUN, SEED_ASSET)
            .await
            .map(|_| ())
    });
    b!(b, "get_completed_step_keys", "calls", 1, |_| async move {
        h.backend()
            .get_completed_step_keys(SEED_RUN)
            .await
            .map(|_| ())
    });
    b!(b, "get_step_data_versions", "calls", 1, |_| async move {
        h.backend()
            .get_step_data_versions(SEED_RUN)
            .await
            .map(|_| ())
    });
    b!(b, "get_run_logs", "calls", 1, |_| async move {
        h.backend().get_run_logs(SEED_RUN).await.map(|_| ())
    });
    b!(
        b,
        "get_latest_materialization",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_latest_materialization(SEED_ASSET, None)
                .await
                .map(|_| ())
        }
    );
    b!(
        b,
        "get_latest_materialization (partitioned)",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_latest_materialization(SEED_ASSET, Some("2024-01-05"))
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_observations_since", "calls", 1, |_| async move {
        h.backend().get_observations_since(cl, 0).await.map(|_| ())
    });
    b!(b, "get_latest_observation_ts", "calls", 1, |_| async move {
        h.backend().get_latest_observation_ts(cl).await.map(|_| ())
    });
    // An N+1 in the SurrealDB backend: one `get_events_for_run` per run id.
    b!(b, "step_completion (20 runs)", "runs", 20, |_| async move {
        let ids: Vec<String> = (0..20).map(|i| format!("seed_run_{i}")).collect();
        h.backend()
            .step_completion(SEED_ASSET, &ids)
            .await
            .map(|_| ())
    });
    b!(b, "get_events_for_asset_page", "pages", 1, |_| async move {
        h.backend()
            .get_events_for_asset_page(cl, SEED_ASSET, &["Materialization".to_string()], 0, 50)
            .await
            .map(|_| ())
    });
    b!(b, "get_run_asset_events_page", "pages", 1, |_| async move {
        h.backend()
            .get_run_asset_events_page(SEED_RUN, SEED_ASSET, "Materialization", 0, 50)
            .await
            .map(|_| ())
    });
    b!(b, "get_run_step_events", "calls", 1, |_| async move {
        h.backend().get_run_step_events(SEED_RUN).await.map(|_| ())
    });
    b!(
        b,
        "get_run_structured_events_page",
        "pages",
        1,
        |_| async move {
            h.backend()
                .get_run_structured_events_page(SEED_RUN, None, 0, 50)
                .await
                .map(|_| ())
        }
    );
    Ok(())
}

// ── runs ──

async fn run_writes(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    b!(b, "create_runs (batch 500)", "runs", 500, |i| async move {
        let runs: Vec<RunRecord> = (0..500)
            .map(|j| fixtures::run(cl, &format!("cr_{i}_{j}"), j))
            .collect();
        h.backend().create_runs(&runs).await
    });
    b!(b, "create_run (single)", "runs", 1, |i| async move {
        h.backend()
            .create_run(&fixtures::run(cl, &format!("cr1_{i}"), i))
            .await
    });
    // The enqueue pair writes the run and its RunQueued event in one
    // transaction, so it costs more than create_run by design.
    b!(b, "enqueue_run", "runs", 1, |i| async move {
        h.backend()
            .enqueue_run(&fixtures::run(cl, &format!("enq_{i}"), i))
            .await
    });
    b!(b, "enqueue_runs (batch 500)", "runs", 500, |i| async move {
        let runs: Vec<RunRecord> = (0..500)
            .map(|j| fixtures::run(cl, &format!("enqb_{i}_{j}"), j))
            .collect();
        h.backend().enqueue_runs(&runs).await
    });

    bs!(
        b,
        "update_run_status",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_run(&fixtures::run(cl, &format!("upd_{i}"), i))
                .await
        },
        |i| async move {
            h.backend()
                .update_run_status(&format!("upd_{i}"), RunStatus::Success, Some(now_nanos()))
                .await
        }
    );
    bs!(
        b,
        "try_start_run",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_run(&fixtures::run(cl, &format!("tsr_{i}"), i))
                .await
        },
        |i| async move {
            h.backend()
                .try_start_run(&format!("tsr_{i}"))
                .await
                .map(|_| ())
        }
    );
    b!(b, "update_run_block_reason", "calls", 1, |_| async move {
        h.backend()
            .update_run_block_reason(SEED_RUN, Some("pool full"))
            .await
    });
    b!(
        b,
        "set_block_reason_by_status",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .set_block_reason_by_status(RunStatus::Queued, None)
                .await
        }
    );
    bs!(
        b,
        "cancel_queued_run",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_run(&fixtures::run(cl, &format!("cqr_{i}"), 1))
                .await
        },
        |i| async move {
            h.backend()
                .cancel_queued_run(&format!("cqr_{i}"))
                .await
                .map(|_| ())
        }
    );
    // delete_run cascades across events, logs, slots, pending steps, and the
    // cancel flag, so it is the most expensive single-run write there is.
    bs!(
        b,
        "delete_run (cascade)",
        "calls",
        1,
        |i| async move {
            let id = format!("del_{i}");
            let mut r = fixtures::run(cl, &id, 0);
            r.status = RunStatus::Success;
            h.backend().create_run(&r).await?;
            let events: Vec<EventRecord> = (0..20)
                .map(|j| fixtures::event(cl, &id, SEED_ASSET, j))
                .collect();
            h.backend().store_events(&events).await.map(|_| ())
        },
        |i| async move {
            h.backend()
                .delete_run(&format!("del_{i}"))
                .await
                .map(|_| ())
        }
    );
    Ok(())
}

async fn run_reads(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;

    b!(b, "get_run", "calls", 1, |_| async move {
        h.backend().get_run(SEED_RUN).await.map(|_| ())
    });
    b!(b, "get_runs_by_ids (100)", "runs", 100, |_| async move {
        let ids: Vec<String> = (0..100).map(|i| format!("seed_run_{i}")).collect();
        h.backend().get_runs_by_ids(&ids, None).await.map(|_| ())
    });
    b!(b, "get_runs (limit 100)", "calls", 1, |_| async move {
        h.scoped().get_runs(100, None).await.map(|_| ())
    });
    b!(b, "get_all_runs (limit 100)", "calls", 1, |_| async move {
        h.backend().get_all_runs(100, None).await.map(|_| ())
    });
    b!(b, "get_runs_since", "calls", 1, |_| async move {
        h.scoped()
            .get_runs_since(0, None, SortOrder::Desc)
            .await
            .map(|_| ())
    });
    b!(b, "get_all_runs_since", "calls", 1, |_| async move {
        h.backend().get_all_runs_since(0, None).await.map(|_| ())
    });
    b!(b, "get_queued_runs", "calls", 1, |_| async move {
        h.scoped().get_queued_runs().await.map(|_| ())
    });
    b!(b, "get_all_queued_runs", "calls", 1, |_| async move {
        h.backend().get_all_queued_runs().await.map(|_| ())
    });
    b!(b, "count_in_progress_runs", "calls", 1, |_| async move {
        h.backend().count_in_progress_runs().await.map(|_| ())
    });
    b!(b, "get_in_progress_runs", "calls", 1, |_| async move {
        h.backend().get_in_progress_runs().await.map(|_| ())
    });
    b!(
        b,
        "get_stalled_not_started_runs",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_stalled_not_started_runs(now_nanos())
                .await
                .map(|_| ())
        }
    );
    // The coordinator polls this every tick, so its cost is paid forever.
    b!(b, "coordinator_tick_query", "calls", 1, |_| async move {
        h.scoped().coordinator_tick_query().await.map(|_| ())
    });
    Ok(())
}

async fn run_pages(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    // Profiling found 7 of 13 UI queries scaling with the dataset rather than
    // the page, so the page queries get measured filtered and unfiltered.
    b!(b, "get_all_runs_page", "pages", 1, |_| async move {
        h.backend()
            .get_all_runs_page(0, 50, &RunFilter::default())
            .await
            .map(|_| ())
    });
    b!(b, "get_runs_page", "pages", 1, |_| async move {
        h.backend()
            .get_runs_page(cl, 0, 50, &RunFilter::default())
            .await
            .map(|_| ())
    });
    // No index can serve a substring match on either backend, so this is what
    // a full scan costs.
    b!(
        b,
        "get_all_runs_page (job substring)",
        "pages",
        1,
        |_| async move {
            let filter = RunFilter {
                job_substring: Some("job_1".to_string()),
                ..RunFilter::default()
            };
            h.backend()
                .get_all_runs_page(0, 50, &filter)
                .await
                .map(|_| ())
        }
    );
    b!(
        b,
        "get_all_runs_page (asset + partition filter)",
        "pages",
        1,
        |_| async move {
            let filter = RunFilter {
                asset_substring: Some("asset_1".to_string()),
                partition_substring: Some("2024-01".to_string()),
                ..RunFilter::default()
            };
            h.backend()
                .get_all_runs_page(0, 50, &filter)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_all_runs_summary", "calls", 1, |_| async move {
        h.backend()
            .get_all_runs_summary(now_nanos() - 86_400_000_000_000)
            .await
            .map(|_| ())
    });
    b!(b, "get_runs_summary", "calls", 1, |_| async move {
        h.backend()
            .get_runs_summary(cl, now_nanos() - 86_400_000_000_000)
            .await
            .map(|_| ())
    });
    b!(
        b,
        "get_all_last_run_per_job (20)",
        "jobs",
        20,
        |_| async move {
            let jobs: Vec<String> = (0..20).map(|i| format!("job_{i}")).collect();
            h.backend()
                .get_all_last_run_per_job(&jobs)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_last_run_per_job (20)", "jobs", 20, |_| async move {
        let jobs: Vec<String> = (0..20).map(|i| format!("job_{i}")).collect();
        h.backend()
            .get_last_run_per_job(cl, &jobs)
            .await
            .map(|_| ())
    });
    Ok(())
}

async fn run_coordination(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;

    b!(b, "get_run_progress", "calls", 1, |_| async move {
        h.backend().get_run_progress(SEED_RUN).await.map(|_| ())
    });
    b!(b, "set_run_outcome", "calls", 1, |i| async move {
        h.backend()
            .set_run_outcome(
                &format!("outcome_{i}"),
                &RunOutcome::Success {
                    completed_steps: 3,
                    total_steps: 3,
                },
            )
            .await
    });
    b!(b, "get_run_outcome", "calls", 1, |_| async move {
        h.backend().get_run_outcome("outcome_0").await.map(|_| ())
    });
    b!(b, "request_cancellation", "calls", 1, |i| async move {
        h.backend().request_cancellation(&format!("cx_{i}")).await
    });
    b!(b, "is_cancelled", "calls", 1, |_| async move {
        h.backend().is_cancelled("cx_0").await.map(|_| ())
    });
    Ok(())
}

// ── assets ──

async fn assets(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    // register_assets was 97% of `resolve()`, which every code-location
    // startup and every code push pays.
    b!(
        b,
        "register_assets (1000 upsert)",
        "assets",
        1_000,
        |i| async move {
            let assets: Vec<AssetRecord> = (0..1_000)
                .map(|j| fixtures::asset(cl, i * 1_000 + j))
                .collect();
            h.scoped().register_assets(&assets).await
        }
    );
    b!(b, "get_asset_record", "calls", 1, |_| async move {
        h.scoped().get_asset_record(SEED_ASSET).await.map(|_| ())
    });
    b!(
        b,
        "get_asset_records (catalog)",
        "calls",
        1,
        |_| async move { h.scoped().get_asset_records().await.map(|_| ()) }
    );
    b!(
        b,
        "get_asset_records_by_keys (100)",
        "assets",
        100,
        |_| async move {
            let keys: Vec<String> = (0..100).map(|i| format!("asset_{i}")).collect();
            h.scoped()
                .get_asset_records_by_keys(&keys)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_assets_by_tag", "calls", 1, |_| async move {
        h.scoped().get_assets_by_tag("team:t1").await.map(|_| ())
    });
    b!(b, "get_assets_by_kind", "calls", 1, |_| async move {
        h.scoped().get_assets_by_kind("python").await.map(|_| ())
    });
    b!(b, "get_assets_by_group", "calls", 1, |_| async move {
        h.scoped().get_assets_by_group("group_1").await.map(|_| ())
    });
    b!(b, "compute_staleness", "calls", 1, |_| async move {
        h.scoped().compute_staleness().await.map(|_| ())
    });
    Ok(())
}

// ── partitions ──

async fn partitions(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;

    b!(
        b,
        "add_dynamic_partitions (1000)",
        "partitions",
        1_000,
        |i| async move {
            let keys: Vec<String> = (0..1_000).map(|j| format!("p_{i}_{j}")).collect();
            h.scoped()
                .add_dynamic_partitions(PARTITIONS_DEF, &keys)
                .await
        }
    );
    bs!(
        b,
        "delete_dynamic_partition",
        "calls",
        1,
        |i| async move {
            h.scoped()
                .add_dynamic_partitions(PARTITIONS_DEF, &[format!("del_p_{i}")])
                .await
        },
        |i| async move {
            h.scoped()
                .delete_dynamic_partition(PARTITIONS_DEF, &format!("del_p_{i}"))
                .await
        }
    );
    b!(b, "get_dynamic_partitions", "calls", 1, |_| async move {
        h.scoped()
            .get_dynamic_partitions(PARTITIONS_DEF)
            .await
            .map(|_| ())
    });
    b!(b, "has_dynamic_partition", "calls", 1, |_| async move {
        h.scoped()
            .has_dynamic_partition(PARTITIONS_DEF, "seed_p_1")
            .await
            .map(|_| ())
    });
    b!(b, "count_dynamic_partitions", "calls", 1, |_| async move {
        h.scoped()
            .count_dynamic_partitions(PARTITIONS_DEF)
            .await
            .map(|_| ())
    });
    b!(
        b,
        "get_materialized_partitions",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_materialized_partitions(SEED_ASSET)
                .await
                .map(|_| ())
        }
    );
    b!(
        b,
        "count_materialized_partitions",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .count_materialized_partitions(SEED_ASSET)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_partition_events", "calls", 1, |_| async move {
        h.scoped()
            .get_partition_events(SEED_ASSET, "2024-01-05", 50)
            .await
            .map(|_| ())
    });
    b!(b, "get_partition_timestamps", "calls", 1, |_| async move {
        h.scoped()
            .get_partition_timestamps(SEED_ASSET)
            .await
            .map(|_| ())
    });
    b!(
        b,
        "get_partition_timestamps_since",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_partition_timestamps_since(SEED_ASSET, 0)
                .await
                .map(|_| ())
        }
    );
    b!(
        b,
        "get_in_progress_partitions",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_in_progress_partitions(SEED_ASSET)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_failed_partitions", "calls", 1, |_| async move {
        let materialized: HashMap<PartitionKey, i64> = HashMap::new();
        h.scoped()
            .get_failed_partitions(SEED_ASSET, &materialized)
            .await
            .map(|_| ())
    });
    Ok(())
}

// ── backfills ──

async fn backfills(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    b!(b, "create_backfill", "calls", 1, |i| async move {
        h.backend()
            .create_backfill(&fixtures::backfill(
                cl,
                &format!("bf_{i}"),
                BackfillStatus::Requested,
            ))
            .await
    });
    b!(b, "get_backfill", "calls", 1, |_| async move {
        h.backend().get_backfill(SEED_BACKFILL).await.map(|_| ())
    });
    b!(b, "get_backfills", "calls", 1, |_| async move {
        h.scoped().get_backfills(Some(50), None).await.map(|_| ())
    });
    b!(b, "get_all_backfills_page", "pages", 1, |_| async move {
        h.backend()
            .get_all_backfills_page(0, 50, &BackfillFilter::default())
            .await
            .map(|_| ())
    });
    b!(b, "get_backfills_page", "pages", 1, |_| async move {
        h.backend()
            .get_backfills_page(cl, 0, 50, &BackfillFilter::default())
            .await
            .map(|_| ())
    });
    b!(b, "get_all_backfills_summary", "calls", 1, |_| async move {
        h.backend().get_all_backfills_summary().await.map(|_| ())
    });
    b!(b, "get_backfills_summary", "calls", 1, |_| async move {
        h.backend().get_backfills_summary(cl).await.map(|_| ())
    });
    b!(b, "update_backfill_status", "calls", 1, |_| async move {
        h.backend()
            .update_backfill_status(SEED_BACKFILL, BackfillStatus::InProgress, None)
            .await
    });
    // The progress merge is a read-modify-write on PostgreSQL (jsonb has no
    // set union) against an in-place `array::union` on SurrealDB.
    b!(b, "update_backfill_progress", "calls", 1, |i| async move {
        h.backend()
            .update_backfill_progress(
                SEED_BACKFILL,
                &[format!("seed_run_{i}")],
                &[fixtures::partition(i)],
                &[],
                &[],
            )
            .await
    });
    b!(b, "link_backfill_run", "calls", 1, |i| async move {
        h.backend()
            .link_backfill_run(SEED_BACKFILL, &format!("seed_run_{i}"))
            .await
            .map(|_| ())
    });
    b!(b, "try_complete_backfill", "calls", 1, |_| async move {
        h.backend()
            .try_complete_backfill(SEED_BACKFILL, &[])
            .await
            .map(|_| ())
    });
    bs!(
        b,
        "cancel_backfill",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_backfill(&fixtures::backfill(
                    cl,
                    &format!("cbf_{i}"),
                    BackfillStatus::InProgress,
                ))
                .await
        },
        |i| async move {
            h.backend()
                .cancel_backfill(&format!("cbf_{i}"))
                .await
                .map(|_| ())
        }
    );
    bs!(
        b,
        "fail_backfill",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_backfill(&fixtures::backfill(
                    cl,
                    &format!("fbf_{i}"),
                    BackfillStatus::InProgress,
                ))
                .await
        },
        |i| async move { h.backend().fail_backfill(&format!("fbf_{i}"), "boom").await }
    );
    bs!(
        b,
        "resume_stalled_backfill",
        "calls",
        1,
        |i| async move {
            h.backend()
                .create_backfill(&fixtures::backfill(
                    cl,
                    &format!("rbf_{i}"),
                    BackfillStatus::InProgress,
                ))
                .await
        },
        |i| async move {
            h.backend()
                .resume_stalled_backfill(&format!("rbf_{i}"))
                .await
                .map(|_| ())
        }
    );
    bs!(
        b,
        "enqueue_backfill_runs (100)",
        "runs",
        100,
        |i| async move {
            h.backend()
                .create_backfill(&fixtures::backfill(
                    cl,
                    &format!("ebf_{i}"),
                    BackfillStatus::InProgress,
                ))
                .await
        },
        |i| async move {
            let runs: Vec<RunRecord> = (0..100)
                .map(|j| fixtures::run(cl, &format!("ebr_{i}_{j}"), j))
                .collect();
            h.backend()
                .enqueue_backfill_runs(&runs, &format!("ebf_{i}"))
                .await
                .map(|_| ())
        }
    );
    Ok(())
}

// ── concurrency pools ──

async fn pools(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;

    b!(b, "set_pool_limit", "calls", 1, |i| async move {
        h.scoped()
            .set_pool_limit(&format!("pool_{}", i % 8), 100, 300)
            .await
    });
    b!(b, "get_pool_limits", "calls", 1, |_| async move {
        h.scoped().get_pool_limits().await.map(|_| ())
    });
    b!(b, "get_pool_info", "calls", 1, |_| async move {
        h.scoped().get_pool_info(POOL).await.map(|_| ())
    });
    b!(b, "get_all_pool_infos", "calls", 1, |_| async move {
        h.scoped().get_all_pool_infos().await.map(|_| ())
    });
    b!(b, "get_pool_slot_holders", "calls", 1, |_| async move {
        h.scoped().get_pool_slot_holders(POOL).await.map(|_| ())
    });
    // The only write that has to serialise against itself. Measured
    // uncontended — one client, no competition — which is the opposite of
    // when it matters, so read this as a floor rather than a verdict.
    b!(b, "claim_concurrency_slots", "claims", 1, |i| async move {
        h.scoped()
            .claim_concurrency_slots(
                &[(POOL.to_string(), 1)],
                &format!("claim_run_{i}"),
                "step",
                0,
                300,
            )
            .await
            .map(|_| ())
    });
    b!(b, "renew_slot_lease", "calls", 1, |i| async move {
        h.backend()
            .renew_slot_lease(&format!("claim_run_{i}"), "step", 300)
            .await
            .map(|_| ())
    });
    bs!(
        b,
        "free_concurrency_slots",
        "calls",
        1,
        |i| async move {
            h.scoped()
                .claim_concurrency_slots(
                    &[(POOL.to_string(), 1)],
                    &format!("free_run_{i}"),
                    "step",
                    0,
                    300,
                )
                .await
                .map(|_| ())
        },
        |i| async move {
            h.backend()
                .free_concurrency_slots(&format!("free_run_{i}"), "step")
                .await
        }
    );
    bs!(
        b,
        "free_concurrency_slots_for_run",
        "calls",
        1,
        |i| async move {
            h.scoped()
                .claim_concurrency_slots(
                    &[(POOL.to_string(), 1)],
                    &format!("freerun_{i}"),
                    "step",
                    0,
                    300,
                )
                .await
                .map(|_| ())
        },
        |i| async move {
            h.backend()
                .free_concurrency_slots_for_run(&format!("freerun_{i}"))
                .await
        }
    );
    b!(b, "free_expired_leases", "calls", 1, |_| async move {
        h.backend().free_expired_leases().await.map(|_| ())
    });
    Ok(())
}

// ── ticks and condition evaluation ──

async fn ticks(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;
    let cl = h.code_location_id();

    b!(b, "store_tick", "ticks", 1, |i| async move {
        h.backend()
            .store_tick(&fixtures::tick(cl, i))
            .await
            .map(|_| ())
    });
    b!(b, "store_ticks_batch (100)", "ticks", 100, |i| async move {
        let ticks: Vec<TickRecord> = (0..100).map(|j| fixtures::tick(cl, i * 100 + j)).collect();
        h.backend().store_ticks_batch(&ticks).await.map(|_| ())
    });
    b!(b, "get_ticks", "calls", 1, |_| async move {
        h.scoped().get_ticks(AUTOMATION, 50).await.map(|_| ())
    });
    b!(b, "store_condition_tick", "ticks", 1, |i| async move {
        h.backend()
            .store_condition_tick(&fixtures::condition_tick(cl, i))
            .await
            .map(|_| ())
    });
    b!(b, "get_condition_ticks", "calls", 1, |_| async move {
        h.scoped().get_condition_ticks(50).await.map(|_| ())
    });
    b!(
        b,
        "store_condition_evals_batch (100)",
        "evals",
        100,
        |i| async move {
            let evals: Vec<ConditionEvalRecord> = (0..100)
                .map(|j| fixtures::condition_eval(cl, SEED_TICK_ID, i * 100 + j))
                .collect();
            h.backend()
                .store_condition_evals_batch(&evals)
                .await
                .map(|_| ())
        }
    );
    b!(b, "get_condition_evals", "calls", 1, |_| async move {
        h.scoped()
            .get_condition_evals(SEED_ASSET, 50)
            .await
            .map(|_| ())
    });
    b!(
        b,
        "get_condition_evals_for_tick",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_condition_evals_for_tick(SEED_TICK_ID)
                .await
                .map(|_| ())
        }
    );
    // The prune family deletes everything past a keep-window, so each
    // iteration reseeds what it is about to remove.
    bs!(
        b,
        "prune_ticks",
        "calls",
        1,
        |i| async move {
            let ticks: Vec<TickRecord> =
                (0..100).map(|j| fixtures::tick(cl, i * 100 + j)).collect();
            h.backend().store_ticks_batch(&ticks).await.map(|_| ())
        },
        |_| async move { h.scoped().prune_ticks(AUTOMATION, 50).await.map(|_| ()) }
    );
    bs!(
        b,
        "prune_condition_ticks",
        "calls",
        1,
        |i| async move {
            h.backend()
                .store_condition_tick(&fixtures::condition_tick(cl, i))
                .await
                .map(|_| ())
        },
        |_| async move { h.scoped().prune_condition_ticks(50).await.map(|_| ()) }
    );
    bs!(
        b,
        "prune_condition_evals",
        "calls",
        1,
        |i| async move {
            let evals: Vec<ConditionEvalRecord> = (0..100)
                .map(|j| fixtures::condition_eval(cl, SEED_TICK_ID, i * 100 + j))
                .collect();
            h.backend()
                .store_condition_evals_batch(&evals)
                .await
                .map(|_| ())
        },
        |_| async move {
            h.scoped()
                .prune_condition_evals(SEED_ASSET, 50)
                .await
                .map(|_| ())
        }
    );
    Ok(())
}

// ── key/value and daemon state ──

async fn kv_and_state(b: &mut Bench<'_>) -> Result<()> {
    let h = b.handle;

    b!(b, "kv_set", "calls", 1, |i| async move {
        h.backend().kv_set(&format!("k_{i}"), b"value").await
    });
    b!(b, "kv_get", "calls", 1, |_| async move {
        h.backend().kv_get("bench:key").await.map(|_| ())
    });
    // Fan-out mapping keys, scoped by (asset, partition, data version).
    b!(b, "set_dynamic_keys (100)", "keys", 100, |i| async move {
        let keys: Vec<String> = (0..100).map(|j| format!("dk_{i}_{j}")).collect();
        h.scoped()
            .set_dynamic_keys(SEED_ASSET, None, "v1", &keys)
            .await
    });
    b!(b, "get_dynamic_keys", "calls", 1, |_| async move {
        h.scoped()
            .get_dynamic_keys(SEED_ASSET, None, "v1")
            .await
            .map(|_| ())
    });
    // The condition daemon reads and writes this blob every tick, so its size
    // grows with the asset count — a whole-blob round trip, not a row.
    b!(b, "set_condition_eval_state", "calls", 1, |_| async move {
        let state = rivers_core::condition::ConditionEvalState::default();
        h.scoped().set_condition_eval_state(&state).await
    });
    b!(b, "get_condition_eval_state", "calls", 1, |_| async move {
        h.scoped().get_condition_eval_state().await.map(|_| ())
    });
    b!(
        b,
        "set_condition_pending_dispatch",
        "calls",
        1,
        |_| async move {
            let pending = rivers_core::condition::PendingDispatch::default();
            h.scoped().set_condition_pending_dispatch(&pending).await
        }
    );
    b!(
        b,
        "get_condition_pending_dispatch",
        "calls",
        1,
        |_| async move {
            h.scoped()
                .get_condition_pending_dispatch()
                .await
                .map(|_| ())
        }
    );
    b!(b, "set_graph_topology", "calls", 1, |_| async move {
        let topology = rivers_core::assets::graph::GraphTopology::default();
        h.scoped().set_graph_topology(&topology).await
    });
    b!(b, "get_graph_topology", "calls", 1, |_| async move {
        h.scoped().get_graph_topology().await.map(|_| ())
    });
    // Opening a change subscription: a LISTEN on PostgreSQL, a LIVE query on
    // SurrealDB. The stream is dropped immediately — only setup is timed.
    b!(b, "subscribe_table (open)", "calls", 1, |_| async move {
        h.backend().subscribe_table("runs").await.map(|_| ())
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Every storage method must appear in a scenario.
    ///
    /// Source-text matching, not reflection: Rust cannot enumerate a trait's
    /// methods at runtime. It is crude, but it fails loudly the moment someone
    /// adds a method to the storage API and forgets to measure it — which is
    /// exactly the drift a "benchmarks everything" claim rots into.
    #[test]
    fn every_storage_method_is_benchmarked() {
        use std::collections::HashSet;

        fn fn_names(text: &str) -> HashSet<String> {
            let mut out = HashSet::new();
            for line in text.lines() {
                let t = line.trim_start();
                if let Some(rest) = t.strip_prefix("fn ")
                    && let Some(name) = rest.split(['(', '<']).next()
                {
                    out.insert(name.trim().to_string());
                }
            }
            out
        }

        /// Text between `marker` and the first line that is exactly `end`.
        fn section<'a>(src: &'a str, marker: &str, end: &str) -> &'a str {
            let start = src.find(marker).unwrap_or_else(|| panic!("no {marker}"));
            let tail = &src[start..];
            let stop = tail
                .find(&format!("\n{end}\n"))
                .unwrap_or(tail.len().saturating_sub(1));
            &tail[..stop]
        }

        let any = include_str!("../../rivers-core/src/storage/any.rs");
        let mod_rs = include_str!("../../rivers-core/src/storage/mod.rs");

        let inherent = fn_names(section(any, "    delegate_inherent! {", "    }"));
        let per_cl_src = {
            let start = mod_rs
                .find("pub(crate) trait PerCodeLocationStorage")
                .unwrap();
            let stop = mod_rs.find("pub trait StorageBackend").unwrap();
            &mod_rs[start..stop]
        };
        let per_cl = fn_names(per_cl_src);
        let backend = fn_names(section(mod_rs, "pub trait StorageBackend", "}"));

        let mut all: HashSet<String> = inherent;
        all.extend(per_cl);
        all.extend(backend);
        // Not storage operations: a supertrait requirement, a KV key builder,
        // and the scoping helper that hands out `ScopedStorage` itself.
        for helper in ["clone", "dynamic_keys_kv_key", "for_code_location"] {
            all.remove(helper);
        }

        let scenarios = include_str!("scenarios.rs");
        let mut missing: Vec<String> = all
            .into_iter()
            .filter(|m| !scenarios.contains(&format!(".{m}(")))
            .collect();
        missing.sort();

        assert!(
            missing.is_empty(),
            "storage methods with no benchmark scenario: {missing:?}"
        );
    }
}
