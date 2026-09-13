//! Storage conformance suite — one set of cases, run against every backend.
//!
//! A case receives the storage handle instead of building one, so adding a
//! backend means one more `#[values]` entry, not a copy of the suite. Tests
//! that reach past the `StorageBackend` API into SurrealDB stay in
//! `surrealdb_backend.rs`.

mod cases;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::storage::any::AnyStorage;

/// A live PostgreSQL to run the suite against, or `None` to skip that row.
///
/// Skipping is for laptops without a server. Under CI it panics instead: the
/// harness reports a skipped test as `ok`, so a PostgreSQL service that failed
/// to start would take the whole backend out of the run and still look green.
fn postgres_url() -> Option<String> {
    let url = std::env::var("RIVERS_TEST_POSTGRES_URL")
        .ok()
        .filter(|u| !u.is_empty());
    if url.is_none() {
        assert!(
            std::env::var("CI").is_err(),
            "RIVERS_TEST_POSTGRES_URL is unset under CI — the PostgreSQL service \
             did not start, and skipping it would report a green run that never \
             tested the backend"
        );
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

/// One backend a conformance case runs against.
#[derive(Clone, Copy, Debug)]
enum Backend {
    SurrealMemory,
    SurrealEmbedded,
    Postgres,
}

/// Storage for one case on one backend, or `None` when that backend is not
/// available — which today means only PostgreSQL, with no server configured.
///
/// The returned `TestTempDir` is the embedded row's directory guard. It
/// deletes on drop, so the caller has to hold it for the length of the test.
async fn fixture(
    backend: Backend,
    case: &str,
) -> Option<(
    std::sync::Arc<crate::storage::any::AnyStorage>,
    Option<test_temp_dir::TestTempDir>,
)> {
    use crate::storage::any::AnyStorage;
    use std::sync::Arc;

    let (storage, temp) = match backend {
        Backend::SurrealMemory => (
            AnyStorage::surreal_memory()
                .await
                .expect("in-memory storage"),
            None,
        ),
        Backend::SurrealEmbedded => {
            // Named after the case rather than taken from `test_temp_dir!()`,
            // which keys off the calling function — here that would be this
            // one, and every case would share a directory.
            let temp = test_temp_dir::TestTempDir::from_complete_item_path(&format!(
                "rivers_core::storage::conformance::{case}"
            ));
            let path = temp.as_path_untracked().to_str().unwrap().to_string();
            (
                AnyStorage::surreal_embedded(&path)
                    .await
                    .expect("embedded storage"),
                Some(temp),
            )
        }
        Backend::Postgres => (postgres_fixture(&postgres_url()?, case).await, None),
    };
    Some((Arc::new(storage), temp))
}

/// A conformance case, erased to one type so `rstest` can list them.
///
/// The cases are `async fn` items with distinct opaque return types, which no
/// parameter can name. Boxing the future gives them all one signature; a
/// non-capturing closure then coerces to the plain fn pointer `#[case]` needs.
type Case = for<'a> fn(&'a Arc<AnyStorage>) -> Pin<Box<dyn Future<Output = ()> + 'a>>;

/// Wrap one case as a [`Case`] beside its name, which the fixtures use to give
/// each case its own PostgreSQL schema and its own embedded directory.
macro_rules! case {
    ($name:ident) => {
        (
            stringify!($name),
            (|storage| Box::pin(cases::$name(storage))) as Case,
        )
    };
}

/// Every case against every backend: 180 cases x 3 backends.
///
/// `rstest` takes the whole matrix, so adding a backend is one `#[values]`
/// entry and adding a case is one `#[case]` line.
#[rstest::rstest]
#[case::run_logs_roundtrip(case!(run_logs_roundtrip))]
#[case::test_store_and_retrieve_event(case!(test_store_and_retrieve_event))]
#[case::test_events_ordered_by_timestamp_desc(case!(test_events_ordered_by_timestamp_desc))]
#[case::test_events_for_run(case!(test_events_for_run))]
#[case::test_asset_record_upsert_on_materialization(case!(test_asset_record_upsert_on_materialization))]
#[case::test_get_asset_records(case!(test_get_asset_records))]
#[case::test_latest_materialization(case!(test_latest_materialization))]
#[case::test_count_materialized_partitions(case!(test_count_materialized_partitions))]
#[case::test_count_dynamic_partitions(case!(test_count_dynamic_partitions))]
#[case::test_add_dynamic_partitions_rejects_reserved_and_empty_keys(case!(test_add_dynamic_partitions_rejects_reserved_and_empty_keys))]
#[case::test_latest_materialization_with_partition(case!(test_latest_materialization_with_partition))]
#[case::test_observation_does_not_upsert_asset(case!(test_observation_does_not_upsert_asset))]
#[case::test_observation_updates_registered_asset(case!(test_observation_updates_registered_asset))]
#[case::test_observation_does_not_overwrite_materialization_fields(case!(test_observation_does_not_overwrite_materialization_fields))]
#[case::test_run_lifecycle(case!(test_run_lifecycle))]
#[case::test_get_runs_with_limit(case!(test_get_runs_with_limit))]
#[case::test_get_runs_filtered_by_status(case!(test_get_runs_filtered_by_status))]
#[case::test_get_all_runs_page_pagination_and_total(case!(test_get_all_runs_page_pagination_and_total))]
#[case::test_get_all_runs_page_filters(case!(test_get_all_runs_page_filters))]
#[case::test_get_all_last_run_per_job(case!(test_get_all_last_run_per_job))]
#[case::test_get_all_last_run_per_job_empty_input(case!(test_get_all_last_run_per_job_empty_input))]
#[case::test_get_all_last_run_per_job_batches_many_jobs_correctly(case!(test_get_all_last_run_per_job_batches_many_jobs_correctly))]
#[case::test_get_all_runs_summary_counts(case!(test_get_all_runs_summary_counts))]
#[case::test_subscribe_table_yields_on_change(case!(test_subscribe_table_yields_on_change))]
#[case::test_create_runs_batch(case!(test_create_runs_batch))]
#[case::test_create_runs_batch_empty(case!(test_create_runs_batch_empty))]
#[case::test_create_runs_batch_preserves_fields(case!(test_create_runs_batch_preserves_fields))]
#[case::test_kv_get_set(case!(test_kv_get_set))]
#[case::test_kv_independent_keys(case!(test_kv_independent_keys))]
#[case::test_dynamic_keys_round_trip(case!(test_dynamic_keys_round_trip))]
#[case::test_dynamic_keys_isolated_per_code_location(case!(test_dynamic_keys_isolated_per_code_location))]
#[case::test_event_with_metadata(case!(test_event_with_metadata))]
#[case::test_events_limit(case!(test_events_limit))]
#[case::test_nonexistent_asset_returns_none(case!(test_nonexistent_asset_returns_none))]
#[case::test_nonexistent_run_returns_none(case!(test_nonexistent_run_returns_none))]
#[case::test_register_assets_with_catalog_fields(case!(test_register_assets_with_catalog_fields))]
#[case::test_register_preserves_materialization_fields(case!(test_register_preserves_materialization_fields))]
#[case::test_materialization_preserves_catalog_fields(case!(test_materialization_preserves_catalog_fields))]
#[case::test_get_assets_by_tag(case!(test_get_assets_by_tag))]
#[case::test_get_assets_by_kind(case!(test_get_assets_by_kind))]
#[case::test_dynamic_partitions_add_and_get(case!(test_dynamic_partitions_add_and_get))]
#[case::test_dynamic_partitions_idempotent_add(case!(test_dynamic_partitions_idempotent_add))]
#[case::test_dynamic_partitions_delete(case!(test_dynamic_partitions_delete))]
#[case::test_dynamic_partitions_delete_nonexistent(case!(test_dynamic_partitions_delete_nonexistent))]
#[case::test_dynamic_partitions_has(case!(test_dynamic_partitions_has))]
#[case::test_dynamic_partitions_isolated_by_name(case!(test_dynamic_partitions_isolated_by_name))]
#[case::test_dynamic_partitions_empty(case!(test_dynamic_partitions_empty))]
#[case::test_get_assets_by_group(case!(test_get_assets_by_group))]
#[case::test_events_same_timestamp_deterministic_order(case!(test_events_same_timestamp_deterministic_order))]
#[case::test_runs_filtered_by_status(case!(test_runs_filtered_by_status))]
#[case::test_runs_for_asset_filtering(case!(test_runs_for_asset_filtering))]
#[case::test_ticks_ordered_desc_and_counted(case!(test_ticks_ordered_desc_and_counted))]
#[case::test_runs_with_tags_and_node_names(case!(test_runs_with_tags_and_node_names))]
#[case::test_events_batch_same_timestamp_deterministic(case!(test_events_batch_same_timestamp_deterministic))]
#[case::test_step_retry_event_round_trips_with_metadata(case!(test_step_retry_event_round_trips_with_metadata))]
#[case::test_input_versions_from_event_not_storage(case!(test_input_versions_from_event_not_storage))]
#[case::test_get_observations_since(case!(test_get_observations_since))]
#[case::test_condition_evals_store_and_retrieve(case!(test_condition_evals_store_and_retrieve))]
#[case::test_step_completion_completed_flag(case!(test_step_completion_completed_flag))]
#[case::test_get_asset_records_by_keys(case!(test_get_asset_records_by_keys))]
#[case::test_get_runs_by_ids(case!(test_get_runs_by_ids))]
#[case::test_get_runs_by_ids_orders_by_start_time(case!(test_get_runs_by_ids_orders_by_start_time))]
#[case::test_get_runs_since(case!(test_get_runs_since))]
#[case::test_store_tick_single(case!(test_store_tick_single))]
#[case::test_prune_ticks(case!(test_prune_ticks))]
#[case::test_store_condition_tick(case!(test_store_condition_tick))]
#[case::test_get_condition_ticks_ordering_and_limit(case!(test_get_condition_ticks_ordering_and_limit))]
#[case::test_prune_condition_ticks(case!(test_prune_condition_ticks))]
#[case::test_get_condition_evals_for_tick(case!(test_get_condition_evals_for_tick))]
#[case::test_get_partition_events(case!(test_get_partition_events))]
#[case::test_get_materialized_partitions(case!(test_get_materialized_partitions))]
#[case::test_get_partition_timestamps(case!(test_get_partition_timestamps))]
#[case::test_get_in_progress_partitions(case!(test_get_in_progress_partitions))]
#[case::test_get_failed_partitions(case!(test_get_failed_partitions))]
#[case::test_get_failed_partitions_includes_marked_in_success_run(case!(test_get_failed_partitions_includes_marked_in_success_run))]
#[case::test_get_failed_partitions_uses_latest_event_per_partition(case!(test_get_failed_partitions_uses_latest_event_per_partition))]
#[case::test_get_failed_partitions_ignores_step_level_failures(case!(test_get_failed_partitions_ignores_step_level_failures))]
#[case::test_get_failed_partitions_expands_set_failure(case!(test_get_failed_partitions_expands_set_failure))]
#[case::test_get_all_backfills_page_pagination_and_filter(case!(test_get_all_backfills_page_pagination_and_filter))]
#[case::test_get_all_backfills_summary_counts(case!(test_get_all_backfills_summary_counts))]
#[case::test_queued_run_round_trip(case!(test_queued_run_round_trip))]
#[case::test_get_queued_runs_priority_ordering(case!(test_get_queued_runs_priority_ordering))]
#[case::test_get_queued_runs_returns_all(case!(test_get_queued_runs_returns_all))]
#[case::test_enqueue_runs_bulk_round_trip(case!(test_enqueue_runs_bulk_round_trip))]
#[case::test_count_in_progress_runs(case!(test_count_in_progress_runs))]
#[case::test_queued_to_not_started_transition(case!(test_queued_to_not_started_transition))]
#[case::test_negative_priority_backfill(case!(test_negative_priority_backfill))]
#[case::test_set_and_get_pool_limits(case!(test_set_and_get_pool_limits))]
#[case::test_set_pool_limit_upsert(case!(test_set_pool_limit_upsert))]
#[case::test_get_pool_info_empty(case!(test_get_pool_info_empty))]
#[case::test_get_pool_info_not_found(case!(test_get_pool_info_not_found))]
#[case::test_asset_record_with_pool(case!(test_asset_record_with_pool))]
#[case::test_asset_pool_preserved_on_re_register(case!(test_asset_pool_preserved_on_re_register))]
#[case::test_claim_single_pool_success(case!(test_claim_single_pool_success))]
#[case::test_claim_single_pool_full(case!(test_claim_single_pool_full))]
#[case::test_claim_and_release_then_reclaim(case!(test_claim_and_release_then_reclaim))]
#[case::test_claim_weighted_slots(case!(test_claim_weighted_slots))]
#[case::test_claim_multi_pool_all_or_none(case!(test_claim_multi_pool_all_or_none))]
#[case::test_claim_multi_pool_block_reason_pools_full(case!(test_claim_multi_pool_block_reason_pools_full))]
#[case::test_free_concurrency_slots_for_run(case!(test_free_concurrency_slots_for_run))]
#[case::test_free_concurrency_slots_for_run_clears_pending(case!(test_free_concurrency_slots_for_run_clears_pending))]
#[case::test_claim_removes_pending_on_success(case!(test_claim_removes_pending_on_success))]
#[case::test_claim_unconfigured_pool_errors(case!(test_claim_unconfigured_pool_errors))]
#[case::test_claim_empty_pools_errors(case!(test_claim_empty_pools_errors))]
#[case::test_concurrent_claims_limit_one(case!(test_concurrent_claims_limit_one))]
#[case::test_free_step_only_affects_that_step(case!(test_free_step_only_affects_that_step))]
#[case::test_claim_multi_pool_weighted(case!(test_claim_multi_pool_weighted))]
#[case::test_expired_slots_not_counted(case!(test_expired_slots_not_counted))]
#[case::test_expired_slot_frees_capacity(case!(test_expired_slot_frees_capacity))]
#[case::test_renew_slot_lease(case!(test_renew_slot_lease))]
#[case::test_renew_multi_pool_lease(case!(test_renew_multi_pool_lease))]
#[case::test_renew_nonexistent_step_returns_zero(case!(test_renew_nonexistent_step_returns_zero))]
#[case::test_free_expired_leases(case!(test_free_expired_leases))]
#[case::test_free_expired_leases_none_expired(case!(test_free_expired_leases_none_expired))]
#[case::test_free_expired_leases_empty(case!(test_free_expired_leases_empty))]
#[case::test_renewal_prevents_expiry_gc(case!(test_renewal_prevents_expiry_gc))]
#[case::test_executor_sequential_claim_release(case!(test_executor_sequential_claim_release))]
#[case::test_executor_concurrent_claim_limit(case!(test_executor_concurrent_claim_limit))]
#[case::test_executor_run_level_cleanup(case!(test_executor_run_level_cleanup))]
#[case::test_executor_lease_renewal_pattern(case!(test_executor_lease_renewal_pattern))]
#[case::test_concurrency_event_types_roundtrip(case!(test_concurrency_event_types_roundtrip))]
#[case::test_run_block_reason_persistence(case!(test_run_block_reason_persistence))]
#[case::test_get_pool_slot_holders_returns_active_slots(case!(test_get_pool_slot_holders_returns_active_slots))]
#[case::test_get_pool_slot_holders_excludes_expired(case!(test_get_pool_slot_holders_excludes_expired))]
#[case::test_get_pool_slot_holders_empty_pool(case!(test_get_pool_slot_holders_empty_pool))]
#[case::test_get_all_pool_infos_batched(case!(test_get_all_pool_infos_batched))]
#[case::test_get_all_pool_infos_empty(case!(test_get_all_pool_infos_empty))]
#[case::test_cancel_queued_run_cleans_pending_steps(case!(test_cancel_queued_run_cleans_pending_steps))]
#[case::test_cancel_queued_run_transitions_to_canceled(case!(test_cancel_queued_run_transitions_to_canceled))]
#[case::test_cancel_queued_run_noop_for_non_queued(case!(test_cancel_queued_run_noop_for_non_queued))]
#[case::test_cancel_queued_run_not_found(case!(test_cancel_queued_run_not_found))]
#[case::test_delete_run_removes_run_events_logs_and_cancel_flag(case!(test_delete_run_removes_run_events_logs_and_cancel_flag))]
#[case::test_delete_run_leaves_other_runs_untouched(case!(test_delete_run_leaves_other_runs_untouched))]
#[case::test_delete_run_refuses_active_runs(case!(test_delete_run_refuses_active_runs))]
#[case::test_delete_run_deletes_canceled_run(case!(test_delete_run_deletes_canceled_run))]
#[case::test_delete_run_not_found(case!(test_delete_run_not_found))]
#[case::test_cancel_backfill_late_cancel_settles_to_success(case!(test_cancel_backfill_late_cancel_settles_to_success))]
#[case::test_cancel_backfill_late_cancel_keeps_failure_outcome(case!(test_cancel_backfill_late_cancel_keeps_failure_outcome))]
#[case::test_cancel_backfill_never_overwrites_terminal_status(case!(test_cancel_backfill_never_overwrites_terminal_status))]
#[case::test_cancel_backfill_cancels_live_backfill(case!(test_cancel_backfill_cancels_live_backfill))]
#[case::test_cancel_backfill_requested_flips_to_canceled(case!(test_cancel_backfill_requested_flips_to_canceled))]
#[case::test_cancel_backfill_not_found(case!(test_cancel_backfill_not_found))]
#[case::test_cancel_queued_run_cancels_not_started(case!(test_cancel_queued_run_cancels_not_started))]
#[case::test_try_start_run_starts_not_started(case!(test_try_start_run_starts_not_started))]
#[case::test_try_start_run_refuses_canceled(case!(test_try_start_run_refuses_canceled))]
#[case::test_try_start_run_missing_run_errors(case!(test_try_start_run_missing_run_errors))]
#[case::test_get_stalled_not_started_runs(case!(test_get_stalled_not_started_runs))]
#[case::test_get_stalled_not_started_runs_scoped_to_cl(case!(test_get_stalled_not_started_runs_scoped_to_cl))]
#[case::test_runs_page_queued_filter_includes_not_started(case!(test_runs_page_queued_filter_includes_not_started))]
#[case::test_run_launch_failed_event_round_trip(case!(test_run_launch_failed_event_round_trip))]
#[case::test_enqueue_backfill_runs_links_atomically(case!(test_enqueue_backfill_runs_links_atomically))]
#[case::test_enqueue_backfill_runs_sweeps_batch_after_cancel(case!(test_enqueue_backfill_runs_sweeps_batch_after_cancel))]
#[case::test_link_backfill_run_only_while_in_progress(case!(test_link_backfill_run_only_while_in_progress))]
#[case::test_resume_stalled_backfill_flips_only_zero_run_in_progress(case!(test_resume_stalled_backfill_flips_only_zero_run_in_progress))]
#[case::test_fail_backfill_records_error(case!(test_fail_backfill_records_error))]
#[case::test_get_run_progress_empty(case!(test_get_run_progress_empty))]
#[case::test_get_run_progress_counts_steps(case!(test_get_run_progress_counts_steps))]
#[case::test_get_run_progress_excludes_per_partition_failures(case!(test_get_run_progress_excludes_per_partition_failures))]
#[case::test_get_step_terminal_events_filters_types(case!(test_get_step_terminal_events_filters_types))]
#[case::test_get_run_progress_counts_retried_step_once(case!(test_get_run_progress_counts_retried_step_once))]
#[case::test_run_outcome_roundtrip(case!(test_run_outcome_roundtrip))]
#[case::test_run_outcome_failure(case!(test_run_outcome_failure))]
#[case::test_run_outcome_cancelled(case!(test_run_outcome_cancelled))]
#[case::test_run_outcome_overwrite(case!(test_run_outcome_overwrite))]
#[case::test_cancellation_flag(case!(test_cancellation_flag))]
#[case::test_get_events_for_step(case!(test_get_events_for_step))]
#[case::test_get_events_for_step_different_runs(case!(test_get_events_for_step_different_runs))]
#[case::test_get_completed_step_keys_empty(case!(test_get_completed_step_keys_empty))]
#[case::test_get_completed_step_keys(case!(test_get_completed_step_keys))]
#[case::test_get_step_data_versions_empty(case!(test_get_step_data_versions_empty))]
#[case::test_get_step_data_versions(case!(test_get_step_data_versions))]
#[case::test_completed_step_keys_ignores_other_runs(case!(test_completed_step_keys_ignores_other_runs))]
#[case::graph_topology_isolated_per_code_location(case!(graph_topology_isolated_per_code_location))]
#[case::condition_eval_state_isolated_per_code_location(case!(condition_eval_state_isolated_per_code_location))]
#[case::scoped_run_queries_isolated_per_code_location(case!(scoped_run_queries_isolated_per_code_location))]
#[case::runs_page_isolated_per_code_location(case!(runs_page_isolated_per_code_location))]
#[case::runs_summary_isolated_per_code_location(case!(runs_summary_isolated_per_code_location))]
#[case::last_run_per_job_isolated_per_code_location(case!(last_run_per_job_isolated_per_code_location))]
#[case::backfills_page_isolated_per_code_location(case!(backfills_page_isolated_per_code_location))]
#[case::backfills_summary_isolated_per_code_location(case!(backfills_summary_isolated_per_code_location))]
#[tokio::test]
async fn conformance(
    #[case] case: (&'static str, Case),
    #[values(Backend::SurrealMemory, Backend::SurrealEmbedded, Backend::Postgres)] backend: Backend,
) {
    let (name, run_case) = case;
    let Some((storage, _temp)) = fixture(backend, name).await else {
        return;
    };
    run_case(&storage).await;
}
