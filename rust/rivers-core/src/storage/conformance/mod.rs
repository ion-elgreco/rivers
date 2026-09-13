//! Storage conformance suite — one set of cases, run against every backend.
//!
//! A case receives the storage handle instead of building one, so adding a
//! backend means adding a row to `conformance_suite!`, not a copy of the
//! suite. Tests that reach past the `StorageBackend` API into SurrealDB stay
//! in `surrealdb_backend.rs`.

mod cases;

/// A live PostgreSQL to run the suite against, or `None` to skip that row.
///
/// The skip prints, because the harness reports a skipped test as `ok` and a
/// PostgreSQL row that never ran would otherwise look green.
fn postgres_url() -> Option<String> {
    let url = std::env::var("RIVERS_TEST_POSTGRES_URL")
        .ok()
        .filter(|u| !u.is_empty());
    if url.is_none() {
        eprintln!("SKIPPED: RIVERS_TEST_POSTGRES_URL is unset, no PostgreSQL to test against");
    }
    url
}

/// Storage over a schema of this case's own, migrated and empty.
///
/// A schema per case, because the cases assume they own the database. The
/// search_path comes from connect options rather than a `SET`: that would bind
/// only one connection, and the next query from the pool would land in `public`.
async fn postgres_fixture(url: &str, case: &str) -> crate::storage::any::AnyStorage {
    use sqlx::{AssertSqlSafe, PgPool};
    let schema = format!("rivers_conf_{case}");
    let admin = PgPool::connect(url).await.expect("connect");
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};"
    )))
    .execute(&admin)
    .await
    .expect("fresh schema");
    admin.close().await;

    let scoped = format!("{url}?options=-c%20search_path%3D{schema}");
    crate::storage::any::AnyStorage::postgres_connect(
        &scoped,
        crate::storage::migration::Capability::Migrate,
    )
    .await
    .expect("migrate the test schema")
}

/// Generate one real test per (case, backend) pair.
///
/// `test_temp_dir!` keys off the calling function, so the embedded row expands
/// it inside each generated test rather than in a shared fixture — otherwise
/// every embedded case would share one directory.
macro_rules! conformance_suite {
    ($($case:ident),* $(,)?) => {
        mod surreal_memory {
            use super::cases;
            use crate::storage::any::AnyStorage;
            use std::sync::Arc;
            $(
                #[tokio::test]
                async fn $case() {
                    let storage = Arc::new(
                        AnyStorage::surreal_memory().await.expect("in-memory storage"),
                    );
                    cases::$case(&storage).await;
                }
            )*
        }

        mod postgres {
            use super::cases;
            use std::sync::Arc;
            $(
                #[tokio::test]
                async fn $case() {
                    let Some(url) = super::postgres_url() else { return };
                    let storage = Arc::new(
                        super::postgres_fixture(&url, stringify!($case)).await,
                    );
                    cases::$case(&storage).await;
                }
            )*
        }

        mod surreal_embedded {
            use super::cases;
            use crate::storage::any::AnyStorage;
            use std::sync::Arc;
            $(
                #[tokio::test]
                async fn $case() {
                    let temp = test_temp_dir::test_temp_dir!();
                    let storage = Arc::new(
                        AnyStorage::surreal_embedded(
                            temp.as_path_untracked().to_str().unwrap(),
                        )
                        .await
                        .expect("embedded storage"),
                    );
                    cases::$case(&storage).await;
                }
            )*
        }
    };
}

conformance_suite!(
    run_logs_roundtrip,
    test_store_and_retrieve_event,
    test_events_ordered_by_timestamp_desc,
    test_events_for_run,
    test_asset_record_upsert_on_materialization,
    test_get_asset_records,
    test_latest_materialization,
    test_count_materialized_partitions,
    test_count_dynamic_partitions,
    test_add_dynamic_partitions_rejects_reserved_and_empty_keys,
    test_latest_materialization_with_partition,
    test_observation_does_not_upsert_asset,
    test_observation_updates_registered_asset,
    test_observation_does_not_overwrite_materialization_fields,
    test_run_lifecycle,
    test_get_runs_with_limit,
    test_get_runs_filtered_by_status,
    test_get_all_runs_page_pagination_and_total,
    test_get_all_runs_page_filters,
    test_get_all_last_run_per_job,
    test_get_all_last_run_per_job_empty_input,
    test_get_all_last_run_per_job_batches_many_jobs_correctly,
    test_get_all_runs_summary_counts,
    test_subscribe_table_yields_on_change,
    test_create_runs_batch,
    test_create_runs_batch_empty,
    test_create_runs_batch_preserves_fields,
    test_kv_get_set,
    test_kv_independent_keys,
    test_dynamic_keys_round_trip,
    test_dynamic_keys_isolated_per_code_location,
    test_event_with_metadata,
    test_events_limit,
    test_nonexistent_asset_returns_none,
    test_nonexistent_run_returns_none,
    test_register_assets_with_catalog_fields,
    test_register_preserves_materialization_fields,
    test_materialization_preserves_catalog_fields,
    test_get_assets_by_tag,
    test_get_assets_by_kind,
    test_dynamic_partitions_add_and_get,
    test_dynamic_partitions_idempotent_add,
    test_dynamic_partitions_delete,
    test_dynamic_partitions_delete_nonexistent,
    test_dynamic_partitions_has,
    test_dynamic_partitions_isolated_by_name,
    test_dynamic_partitions_empty,
    test_get_assets_by_group,
    test_events_same_timestamp_deterministic_order,
    test_runs_filtered_by_status,
    test_runs_for_asset_filtering,
    test_ticks_ordered_desc_and_counted,
    test_runs_with_tags_and_node_names,
    test_events_batch_same_timestamp_deterministic,
    test_step_retry_event_round_trips_with_metadata,
    test_input_versions_from_event_not_storage,
    test_get_observations_since,
    test_condition_evals_store_and_retrieve,
    test_step_completion_completed_flag,
    test_get_asset_records_by_keys,
    test_get_runs_by_ids,
    test_get_runs_by_ids_orders_by_start_time,
    test_get_runs_since,
    test_store_tick_single,
    test_prune_ticks,
    test_store_condition_tick,
    test_get_condition_ticks_ordering_and_limit,
    test_prune_condition_ticks,
    test_get_condition_evals_for_tick,
    test_get_partition_events,
    test_get_materialized_partitions,
    test_get_partition_timestamps,
    test_get_in_progress_partitions,
    test_get_failed_partitions,
    test_get_failed_partitions_includes_marked_in_success_run,
    test_get_failed_partitions_uses_latest_event_per_partition,
    test_get_failed_partitions_ignores_step_level_failures,
    test_get_failed_partitions_expands_set_failure,
    test_get_all_backfills_page_pagination_and_filter,
    test_get_all_backfills_summary_counts,
    test_queued_run_round_trip,
    test_get_queued_runs_priority_ordering,
    test_get_queued_runs_returns_all,
    test_enqueue_runs_bulk_round_trip,
    test_count_in_progress_runs,
    test_queued_to_not_started_transition,
    test_negative_priority_backfill,
    test_set_and_get_pool_limits,
    test_set_pool_limit_upsert,
    test_get_pool_info_empty,
    test_get_pool_info_not_found,
    test_asset_record_with_pool,
    test_asset_pool_preserved_on_re_register,
    test_claim_single_pool_success,
    test_claim_single_pool_full,
    test_claim_and_release_then_reclaim,
    test_claim_weighted_slots,
    test_claim_multi_pool_all_or_none,
    test_claim_multi_pool_block_reason_pools_full,
    test_free_concurrency_slots_for_run,
    test_free_concurrency_slots_for_run_clears_pending,
    test_claim_removes_pending_on_success,
    test_claim_unconfigured_pool_errors,
    test_claim_empty_pools_errors,
    test_concurrent_claims_limit_one,
    test_free_step_only_affects_that_step,
    test_claim_multi_pool_weighted,
    test_expired_slots_not_counted,
    test_expired_slot_frees_capacity,
    test_renew_slot_lease,
    test_renew_multi_pool_lease,
    test_renew_nonexistent_step_returns_zero,
    test_free_expired_leases,
    test_free_expired_leases_none_expired,
    test_free_expired_leases_empty,
    test_renewal_prevents_expiry_gc,
    test_executor_sequential_claim_release,
    test_executor_concurrent_claim_limit,
    test_executor_run_level_cleanup,
    test_executor_lease_renewal_pattern,
    test_concurrency_event_types_roundtrip,
    test_run_block_reason_persistence,
    test_get_pool_slot_holders_returns_active_slots,
    test_get_pool_slot_holders_excludes_expired,
    test_get_pool_slot_holders_empty_pool,
    test_get_all_pool_infos_batched,
    test_get_all_pool_infos_empty,
    test_cancel_queued_run_cleans_pending_steps,
    test_cancel_queued_run_transitions_to_canceled,
    test_cancel_queued_run_noop_for_non_queued,
    test_cancel_queued_run_not_found,
    test_delete_run_removes_run_events_logs_and_cancel_flag,
    test_delete_run_leaves_other_runs_untouched,
    test_delete_run_refuses_active_runs,
    test_delete_run_deletes_canceled_run,
    test_delete_run_not_found,
    test_cancel_backfill_late_cancel_settles_to_success,
    test_cancel_backfill_late_cancel_keeps_failure_outcome,
    test_cancel_backfill_never_overwrites_terminal_status,
    test_cancel_backfill_cancels_live_backfill,
    test_cancel_backfill_requested_flips_to_canceled,
    test_cancel_backfill_not_found,
    test_cancel_queued_run_cancels_not_started,
    test_try_start_run_starts_not_started,
    test_try_start_run_refuses_canceled,
    test_try_start_run_missing_run_errors,
    test_get_stalled_not_started_runs,
    test_get_stalled_not_started_runs_scoped_to_cl,
    test_runs_page_queued_filter_includes_not_started,
    test_run_launch_failed_event_round_trip,
    test_enqueue_backfill_runs_links_atomically,
    test_enqueue_backfill_runs_sweeps_batch_after_cancel,
    test_link_backfill_run_only_while_in_progress,
    test_resume_stalled_backfill_flips_only_zero_run_in_progress,
    test_fail_backfill_records_error,
    test_get_run_progress_empty,
    test_get_run_progress_counts_steps,
    test_get_run_progress_excludes_per_partition_failures,
    test_get_step_terminal_events_filters_types,
    test_get_run_progress_counts_retried_step_once,
    test_run_outcome_roundtrip,
    test_run_outcome_failure,
    test_run_outcome_cancelled,
    test_run_outcome_overwrite,
    test_cancellation_flag,
    test_get_events_for_step,
    test_get_events_for_step_different_runs,
    test_get_completed_step_keys_empty,
    test_get_completed_step_keys,
    test_get_step_data_versions_empty,
    test_get_step_data_versions,
    test_completed_step_keys_ignores_other_runs,
    graph_topology_isolated_per_code_location,
    condition_eval_state_isolated_per_code_location,
    scoped_run_queries_isolated_per_code_location,
    runs_page_isolated_per_code_location,
    runs_summary_isolated_per_code_location,
    last_run_per_job_isolated_per_code_location,
    backfills_page_isolated_per_code_location,
    backfills_summary_isolated_per_code_location,
);
