"""Integration tests for automation condition evaluation cross-interactions.

Tests verify that when schedules, sensors, or observations trigger upstream
materializations, downstream assets with eager() automation conditions are
properly detected and materialized by the real daemon condition eval loop.

All tests use actual AutomationDaemon instances with a fast condition eval
interval (1s) to avoid waiting 30s per tick.
"""

import time

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for_asset_materialized as _wait_for_asset_materialized
from _polling import wait_until as _wait_until


def _stale(storage, key):
    """Live staleness for one asset. ``stale_status`` is no longer persisted —
    callers go through ``Storage.compute_staleness()``."""
    return storage.compute_staleness().get(key, ("Missing", []))


def _wait_for_asset_up_to_date(storage, key, timeout=15.0, prev_version=None):
    """Poll until asset record shows UpToDate and has been materialized, or timeout.

    If *prev_version* is given, the record's last_data_version must also differ
    from it — this detects a genuine re-materialization rather than a stale
    pre-existing state.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = storage.get_asset_record(key)
        if (
            record
            and _stale(storage, key)[0] == "UpToDate"
            and record.last_data_version is not None
            and (prev_version is None or record.last_data_version != prev_version)
        ):
            return record
        time.sleep(0.2)
    return storage.get_asset_record(key)


# ---------------------------------------------------------------------------
# Test: Schedule materializes upstream → downstream eager() fires via daemon
# ---------------------------------------------------------------------------


class TestScheduleTriggersEagerCondition:
    def test_schedule_materializes_upstream_daemon_materializes_downstream(
        self, storage
    ):
        """A per-second schedule re-materializes 'source' (via a scoped job).
        Both assets start pre-materialized and UpToDate. The schedule gives
        source new data, making processed stale. The daemon condition eval
        loop detects the upstream change and triggers materialization of
        'processed'.

        Graph: source (schedule → ingest_job) → processed (eager)
        """

        @rs.Asset(name="source", io_handler=rs.InMemoryIOHandler())
        def source() -> int:
            return 42

        @rs.Asset(
            name="processed",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def processed(source: int) -> int:
            return source * 2

        # Job that only materializes 'source'
        ingest_job = rs.Job(name="ingest_job", assets=[source])

        repo = rs.CodeRepository(
            assets=[source, processed],
            jobs=[ingest_job],
            schedules=[],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize both assets so they're UpToDate
        repo.materialize()
        assert _stale(storage, "source")[0] == "UpToDate"
        assert _stale(storage, "processed")[0] == "UpToDate"

        # Now add a schedule that re-materializes source
        call_count = 0

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",  # every 5 seconds
            job_name="ingest_job",
            default_status=rs.ScheduleStatus.Running,
        )
        def every_second(context: rs.ScheduleEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("already triggered")

        repo2 = rs.CodeRepository(
            assets=[source, processed],
            jobs=[ingest_job],
            schedules=[every_second],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        # Capture pre-daemon versions so the waits below can't return the
        # original materialization and pass with a dead daemon.
        prev_source_version = storage.get_asset_record("source").last_data_version
        prev_processed_version = storage.get_asset_record("processed").last_data_version

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for downstream to be re-materialized by condition eval
            # (schedule re-materializes source → processed becomes stale → eager fires)
            record = _wait_for_asset_up_to_date(
                storage, "processed", timeout=20, prev_version=prev_processed_version
            )
            assert record is not None
            status, causes = _stale(storage, "processed")
            assert status == "UpToDate", (
                "'processed' should have been materialized by the daemon condition eval"
            )
            assert causes == []

            # Verify provenance: processed consumed source's latest data
            source_rec = storage.get_asset_record("source")
            assert source_rec.last_data_version != prev_source_version, (
                "the schedule must have re-materialized 'source'"
            )
            assert len(record.last_input_data_versions) == 1
            assert record.last_input_data_versions[0][0] == "source"
            assert record.last_input_data_versions[0][1] == source_rec.last_data_version
        finally:
            daemon.stop()

    def test_schedule_chain_three_layers(self, storage):
        """Schedule materializes A → B (eager) gets materialized by daemon →
        C (eager) gets materialized by daemon, all in a single run.

        Flow:
        1. Daemon starts, baseline tick captures current dep state
        2. Schedule fires (5s cron), creates run for A
        3. Condition eval sees A in-progress → eager for B,C blocked (AnyDepsInProgress)
        4. A's run completes → condition eval detects completion, re-fetches records
        5. AnyDepsUpdated fires for B (A's timestamp changed) → B and C fire together
        6. Single materialize(selection=["b","c"]) executes in topological order

        Graph: A (schedule → source_job) → B (eager) → C (eager)
        """

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def b(a: int) -> int:
            return a + 10

        @rs.Asset(
            name="c",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def c(b: int) -> int:
            return b * 100

        source_job = rs.Job(name="source_job", assets=[a])

        # resolve WITHOUT schedule, materialize everything → UpToDate
        repo1 = rs.CodeRepository(
            assets=[a, b, c],
            jobs=[source_job],
            schedules=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        assert _stale(storage, "a")[0] == "UpToDate"
        assert _stale(storage, "b")[0] == "UpToDate"
        assert _stale(storage, "c")[0] == "UpToDate"
        prev_b_version = storage.get_asset_record("b").last_data_version
        prev_c_version = storage.get_asset_record("c").last_data_version

        # new repo WITH schedule targeting only 'a' (5s cron so baseline
        # captures clean state before schedule fires)
        call_count = 0

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",
            job_name="source_job",
            default_status=rs.ScheduleStatus.Running,
        )
        def trigger_a(context: rs.ScheduleEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        repo2 = rs.CodeRepository(
            assets=[a, b, c],
            jobs=[source_job],
            schedules=[trigger_a],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for end of chain to be re-materialized
            record_c = _wait_for_asset_up_to_date(
                storage, "c", timeout=30, prev_version=prev_c_version
            )
            assert record_c is not None
            assert _stale(storage, "c")[0] == "UpToDate", (
                "'c' should have been materialized via chain: schedule→a, eager→b, eager→c"
            )

            # Verify intermediate was also re-materialized
            record_b = storage.get_asset_record("b")
            assert record_b.last_data_version is not None
            assert record_b.last_data_version != prev_b_version
            assert _stale(storage, "b")[0] == "UpToDate"
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Sensor materializes upstream → downstream eager() fires via daemon
# ---------------------------------------------------------------------------


class TestSensorTriggersEagerCondition:
    def test_sensor_materializes_upstream_daemon_materializes_downstream(self, storage):
        """A sensor triggers materialization of 'ingested' (via a scoped job).
        Downstream 'report' has eager(). The daemon condition eval loop
        materializes 'report'.

        Graph: ingested (sensor → ingest_job) → report (eager)

        Pre-materialization pattern: resolve without sensor, materialize all,
        then add sensor and start daemon.
        """

        @rs.Asset(name="ingested", io_handler=rs.InMemoryIOHandler())
        def ingested() -> int:
            return 100

        @rs.Asset(
            name="report",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def report(ingested: int) -> int:
            return ingested * 2

        ingest_job = rs.Job(name="ingest_job", assets=[ingested])

        # resolve WITHOUT sensor, materialize everything → UpToDate
        repo1 = rs.CodeRepository(
            assets=[ingested, report],
            jobs=[ingest_job],
            sensors=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        assert _stale(storage, "ingested")[0] == "UpToDate"
        assert _stale(storage, "report")[0] == "UpToDate"
        prev_report_version = storage.get_asset_record("report").last_data_version

        # new repo WITH sensor targeting only 'ingested'
        call_count = 0

        @rs.Sensor(
            job_name="ingest_job",
            minimum_interval="5s",
            default_status=rs.SensorStatus.Running,
        )
        def data_sensor(context: rs.SensorEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("already ingested")

        repo2 = rs.CodeRepository(
            assets=[ingested, report],
            jobs=[ingest_job],
            sensors=[data_sensor],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            record = _wait_for_asset_up_to_date(
                storage, "report", timeout=20, prev_version=prev_report_version
            )
            assert record is not None
            status, causes = _stale(storage, "report")
            assert status == "UpToDate", (
                "'report' should have been materialized by daemon after sensor triggered 'ingested'"
            )
            assert causes == []

            # Verify provenance
            ingested_rec = storage.get_asset_record("ingested")
            assert len(record.last_input_data_versions) == 1
            assert record.last_input_data_versions[0][0] == "ingested"
            assert (
                record.last_input_data_versions[0][1] == ingested_rec.last_data_version
            )
        finally:
            daemon.stop()

    def test_sensor_fan_out_multiple_downstream_eager(self, storage):
        """Sensor materializes one upstream → three downstream eager assets
        all get materialized by daemon.

        Graph: ingested (sensor → ingest_job) → report_a (eager)
                                               → report_b (eager)
                                               → report_c (eager)

        Pre-materialization pattern: resolve without sensor, materialize all,
        then add sensor and start daemon.
        """

        @rs.Asset(name="ingested", io_handler=rs.InMemoryIOHandler())
        def ingested() -> int:
            return 1

        @rs.Asset(
            name="report_a",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def report_a(ingested: int) -> int:
            return ingested + 1

        @rs.Asset(
            name="report_b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def report_b(ingested: int) -> int:
            return ingested + 2

        @rs.Asset(
            name="report_c",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def report_c(ingested: int) -> int:
            return ingested + 3

        ingest_job = rs.Job(name="ingest_job", assets=[ingested])

        # resolve WITHOUT sensor, materialize everything → UpToDate
        repo1 = rs.CodeRepository(
            assets=[ingested, report_a, report_b, report_c],
            jobs=[ingest_job],
            sensors=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        prev_versions = {}
        for key in ["report_a", "report_b", "report_c"]:
            rec = storage.get_asset_record(key)
            assert _stale(storage, key)[0] == "UpToDate"
            prev_versions[key] = rec.last_data_version

        # new repo WITH sensor targeting only 'ingested'
        call_count = 0

        @rs.Sensor(
            job_name="ingest_job",
            minimum_interval="5s",
            default_status=rs.SensorStatus.Running,
        )
        def ingest_sensor(context: rs.SensorEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        repo2 = rs.CodeRepository(
            assets=[ingested, report_a, report_b, report_c],
            jobs=[ingest_job],
            sensors=[ingest_sensor],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            for key in ["report_a", "report_b", "report_c"]:
                record = _wait_for_asset_up_to_date(
                    storage, key, timeout=20, prev_version=prev_versions[key]
                )
                assert record is not None
                status, causes = _stale(storage, key)
                assert status == "UpToDate", (
                    f"'{key}' should have been re-materialized by daemon"
                )
                assert causes == []
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Schedule → Schedule → automation condition (chained triggers)
# ---------------------------------------------------------------------------


class TestChainedScheduleToCondition:
    def test_two_schedules_feed_into_eager_downstream(self, storage):
        """Schedule A materializes 'raw' (raw_job). Schedule B materializes
        'clean' (clean_job, depends on raw). 'analytics' (eager) depends on
        'clean' — daemon should materialize it.

        Graph: raw (schedule A → raw_job) → clean (schedule B → clean_job) → analytics (eager)

        Pre-materialization pattern: resolve without schedules, materialize all,
        then add schedules and start daemon.
        """

        @rs.Asset(name="raw", io_handler=rs.InMemoryIOHandler())
        def raw() -> int:
            return 10

        @rs.Asset(name="clean", io_handler=rs.InMemoryIOHandler())
        def clean(raw: int) -> int:
            return raw + 1

        @rs.Asset(
            name="analytics",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def analytics(clean: int) -> int:
            return clean * 100

        raw_job = rs.Job(name="raw_job", assets=[raw])
        clean_job = rs.Job(name="clean_job", assets=[clean], allow_incomplete_deps=True)

        # resolve WITHOUT schedules, materialize everything → UpToDate
        repo1 = rs.CodeRepository(
            assets=[raw, clean, analytics],
            jobs=[raw_job, clean_job],
            schedules=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        assert _stale(storage, "raw")[0] == "UpToDate"
        assert _stale(storage, "clean")[0] == "UpToDate"
        assert _stale(storage, "analytics")[0] == "UpToDate"
        prev_analytics_version = storage.get_asset_record("analytics").last_data_version

        # new repo WITH schedules targeting raw and clean
        raw_count = 0
        clean_count = 0

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",
            job_name="raw_job",
            name="sched_raw",
            default_status=rs.ScheduleStatus.Running,
        )
        def sched_raw(context: rs.ScheduleEvaluationContext):
            nonlocal raw_count
            raw_count += 1
            if raw_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",
            job_name="clean_job",
            name="sched_clean",
            default_status=rs.ScheduleStatus.Running,
        )
        def sched_clean(context: rs.ScheduleEvaluationContext):
            nonlocal clean_count
            clean_count += 1
            # Wait a tick so raw is materialized first
            if 2 <= clean_count <= 2:
                return rs.RunRequest()
            return rs.SkipReason("waiting" if clean_count < 2 else "done")

        repo2 = rs.CodeRepository(
            assets=[raw, clean, analytics],
            jobs=[raw_job, clean_job],
            schedules=[sched_raw, sched_clean],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            record = _wait_for_asset_up_to_date(
                storage, "analytics", timeout=30, prev_version=prev_analytics_version
            )
            assert record is not None
            assert _stale(storage, "analytics")[0] == "UpToDate", (
                "'analytics' should have been materialized by daemon after schedule chain"
            )

            # Verify clean was re-materialized by schedule
            clean_rec = storage.get_asset_record("clean")
            assert clean_rec.last_data_version is not None

            # Verify provenance chain
            assert len(record.last_input_data_versions) == 1
            assert record.last_input_data_versions[0][0] == "clean"
            assert record.last_input_data_versions[0][1] == clean_rec.last_data_version
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: External observation → downstream eager condition fires via daemon
# ---------------------------------------------------------------------------


class TestObservationTriggersEagerCondition:
    def test_observation_triggers_downstream_eager(self, storage):
        """External asset with on_cron condition gets observed by daemon →
        downstream 'snapshot' (eager) detects change and gets materialized.

        Graph: ext_feed (on_cron, external) → snapshot (eager)

        No pre-observation: ext_feed starts as Missing. The daemon should:
        1. Tick 1: on_cron establishes baseline, ext_feed is Missing so
           eager() blocks snapshot (~AnyDepsMissing is false)
        2. Tick 2: on_cron fires → ext_feed gets observed (no longer Missing)
        3. Tick 3: eager() sees AnyDepsUpdated + ~AnyDepsMissing → snapshot materializes
        """
        handler = rs.InMemoryIOHandler()

        obs_counter = [0]

        @rs.Asset.external(
            io_handler=handler,
            automation_condition=rs.AutomationCondition.on_cron("* * * * * *"),
        )
        def ext_feed(context: rs.AssetExecutionContext):
            obs_counter[0] += 1
            handler.handle_output(
                rs.OutputContext(asset_name="ext_feed"), {"price": 100}
            )
            return rs.Observation(data_version=f"v{obs_counter[0]}")

        @rs.Asset(
            name="snapshot",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def snapshot(ext_feed: dict) -> dict:
            return {"snapshot_price": ext_feed["price"]}

        repo = rs.CodeRepository(
            assets=[ext_feed, snapshot],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Verify ext_feed starts as Missing
        ext_rec = storage.get_asset_record("ext_feed")
        assert _stale(storage, "ext_feed")[0] == "Missing"

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for ext_feed to be observed first
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                ext_rec = storage.get_asset_record("ext_feed")
                if ext_rec and ext_rec.last_data_version is not None:
                    break
                time.sleep(0.2)
            ext_rec = storage.get_asset_record("ext_feed")
            assert ext_rec.last_data_version is not None, (
                "ext_feed should have been observed by daemon"
            )

            # Wait for snapshot to be materialized by eager()
            record = _wait_for_asset_materialized(storage, "snapshot", timeout=20)
            assert record is not None
            assert _stale(storage, "snapshot")[0] == "UpToDate", (
                "'snapshot' should have been materialized by daemon after ext_feed observation"
            )

            # Verify provenance
            assert len(record.last_input_data_versions) == 1
            assert record.last_input_data_versions[0][0] == "ext_feed"
        finally:
            daemon.stop()

    def test_observation_chain_through_multiple_layers(self, storage):
        """ext_feed (on_cron) → aggregated (eager) → report (eager).
        Daemon observes ext_feed, then materializes aggregated, then report.

        Graph: ext_feed (on_cron, external) → aggregated (eager) → report (eager)

        Observation pre-setup pattern: resolve without on_cron, observe ext_feed
        and materialize all downstream so everything is UpToDate. Then create
        new repo with on_cron condition and start daemon.
        """
        handler = rs.InMemoryIOHandler()

        obs_counter = [0]

        @rs.Asset.external(
            io_handler=handler,
            name="ext_feed",
        )
        def ext_feed_plain(context: rs.AssetExecutionContext):
            handler.handle_output(rs.OutputContext(asset_name="ext_feed"), [10, 20, 30])
            return rs.Observation(data_version="v0")

        @rs.Asset(
            name="aggregated",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def aggregated(ext_feed: list) -> int:
            return sum(ext_feed)

        @rs.Asset(
            name="report",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def report(aggregated: int) -> str:
            return f"total={aggregated}"

        # resolve WITHOUT on_cron, observe + materialize all → UpToDate
        repo1 = rs.CodeRepository(
            assets=[ext_feed_plain, aggregated, report],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.observe(asset_names=["ext_feed"])
        repo1.materialize()

        assert storage.get_asset_record("ext_feed").last_data_version is not None
        assert _stale(storage, "aggregated")[0] == "UpToDate"
        assert _stale(storage, "report")[0] == "UpToDate"
        prev_report_version = storage.get_asset_record("report").last_data_version
        prev_agg_version = storage.get_asset_record("aggregated").last_data_version

        # new repo WITH on_cron condition on ext_feed
        @rs.Asset.external(
            io_handler=handler,
            automation_condition=rs.AutomationCondition.on_cron("*/5 * * * * *"),
        )
        def ext_feed(context: rs.AssetExecutionContext):
            obs_counter[0] += 1
            handler.handle_output(rs.OutputContext(asset_name="ext_feed"), [10, 20, 30])
            return rs.Observation(data_version=f"v{obs_counter[0]}")

        repo2 = rs.CodeRepository(
            assets=[ext_feed, aggregated, report],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for end of chain to be re-materialized
            record = _wait_for_asset_up_to_date(
                storage, "report", timeout=30, prev_version=prev_report_version
            )
            assert record is not None
            assert _stale(storage, "report")[0] == "UpToDate", (
                "'report' should have been materialized via chain: "
                "on_cron→observe, eager→aggregated, eager→report"
            )

            # Intermediate should also be re-materialized
            agg_rec = storage.get_asset_record("aggregated")
            assert agg_rec.last_data_version is not None
            assert agg_rec.last_data_version != prev_agg_version
            assert _stale(storage, "aggregated")[0] == "UpToDate"
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Mixed — schedule + sensor feeding downstream eager
# ---------------------------------------------------------------------------


class TestMixedTriggersToEager:
    def test_schedule_and_sensor_both_feed_eager_downstream(self, storage):
        """Schedule materializes 'sched_data' (sched_job), sensor materializes
        'sensor_data' (sensor_job). Both feed into 'combined' (eager).
        Daemon should materialize 'combined'.

        Graph: sched_data (schedule → sched_job) ─┐
                                                    ├→ combined (eager)
               sensor_data (sensor → sensor_job) ─┘

        Pre-materialization pattern: resolve without schedule/sensor, materialize
        all, then add schedule+sensor and start daemon.
        """

        @rs.Asset(name="sched_data", io_handler=rs.InMemoryIOHandler())
        def sched_data() -> int:
            return 1

        @rs.Asset(name="sensor_data", io_handler=rs.InMemoryIOHandler())
        def sensor_data() -> int:
            return 2

        @rs.Asset(
            name="combined",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def combined(sched_data: int, sensor_data: int) -> int:
            return sched_data + sensor_data

        sched_job = rs.Job(name="sched_job", assets=[sched_data])
        sensor_job = rs.Job(name="sensor_job", assets=[sensor_data])

        # resolve WITHOUT schedule/sensor, materialize everything → UpToDate
        repo1 = rs.CodeRepository(
            assets=[sched_data, sensor_data, combined],
            jobs=[sched_job, sensor_job],
            schedules=[],
            sensors=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        assert _stale(storage, "sched_data")[0] == "UpToDate"
        assert _stale(storage, "sensor_data")[0] == "UpToDate"
        assert _stale(storage, "combined")[0] == "UpToDate"
        prev_combined_version = storage.get_asset_record("combined").last_data_version

        # new repo WITH schedule + sensor targeting upstreams
        sched_count = 0
        sensor_count = 0

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",
            job_name="sched_job",
            name="sched_trigger",
            default_status=rs.ScheduleStatus.Running,
        )
        def sched_trigger(context: rs.ScheduleEvaluationContext):
            nonlocal sched_count
            sched_count += 1
            if sched_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        @rs.Sensor(
            name="sensor_trigger",
            job_name="sensor_job",
            minimum_interval="5s",
            default_status=rs.SensorStatus.Running,
        )
        def sensor_trigger(context: rs.SensorEvaluationContext):
            nonlocal sensor_count
            sensor_count += 1
            if sensor_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        repo2 = rs.CodeRepository(
            assets=[sched_data, sensor_data, combined],
            jobs=[sched_job, sensor_job],
            schedules=[sched_trigger],
            sensors=[sensor_trigger],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            record = _wait_for_asset_up_to_date(
                storage, "combined", timeout=20, prev_version=prev_combined_version
            )
            assert record is not None
            assert _stale(storage, "combined")[0] == "UpToDate", (
                "'combined' should have been materialized by daemon "
                "after both upstreams are ready"
            )

            # Both upstreams should have been re-materialized
            assert storage.get_asset_record("sched_data").last_data_version is not None
            assert storage.get_asset_record("sensor_data").last_data_version is not None

            # Provenance should show both inputs
            input_names = {iv[0] for iv in record.last_input_data_versions}
            assert input_names == {"sched_data", "sensor_data"}
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Staleness assertions with exact values
# ---------------------------------------------------------------------------


class TestExactStaleCauses:
    def test_upstream_rematerialization_exact_stale_causes(self, storage):
        """After upstream re-materialization, downstream has exact stale causes."""

        @rs.Asset(name="upstream", io_handler=rs.InMemoryIOHandler())
        def upstream() -> int:
            return 1

        @rs.Asset(
            name="downstream",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def downstream(upstream: int) -> int:
            return upstream + 1

        repo = rs.CodeRepository(assets=[upstream, downstream])
        repo.resolve(storage=storage)
        repo.materialize()

        # Both up-to-date, no causes
        snapshot = storage.compute_staleness()
        assert snapshot["upstream"][0] == "UpToDate"
        assert snapshot["upstream"][1] == []
        assert snapshot["downstream"][0] == "UpToDate"
        assert snapshot["downstream"][1] == []

        # Re-materialize upstream only
        repo.materialize(selection=["upstream"])

        down_status, down_causes = _stale(storage, "downstream")
        assert down_status == "Stale"
        assert len(down_causes) == 1
        cause = down_causes[0]
        assert cause.asset_key == "downstream"
        assert cause.category == "Data"
        assert cause.dependency == "upstream"

    def test_code_version_exact_stale_causes(self, storage):
        """Code version change produces exact stale cause."""

        @rs.Asset(name="x", code_version="v1", io_handler=rs.InMemoryIOHandler())
        def x_v1() -> int:
            return 1

        repo = rs.CodeRepository(assets=[x_v1])
        repo.resolve(storage=storage)
        repo.materialize()

        assert _stale(storage, "x")[1] == []

        # Change code version
        @rs.Asset(name="x", code_version="v2", io_handler=rs.InMemoryIOHandler())
        def x_v2() -> int:
            return 2

        repo2 = rs.CodeRepository(assets=[x_v2])
        repo2.resolve(storage=storage)

        status, causes = _stale(storage, "x")
        assert status == "Stale"
        assert len(causes) == 1
        cause = causes[0]
        assert cause.asset_key == "x"
        assert cause.category == "Code"
        assert cause.dependency is None

    def test_transitive_exact_stale_causes(self, storage):
        """A (code change) → B (data stale) → C (data stale).
        Each has exactly one stale cause pointing to the correct dependency."""

        @rs.Asset(name="a", code_version="v1", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(name="b", io_handler=rs.InMemoryIOHandler())
        def b(a: int) -> int:
            return a + 1

        @rs.Asset(name="c", io_handler=rs.InMemoryIOHandler())
        def c(b: int) -> int:
            return b + 1

        repo = rs.CodeRepository(assets=[a, b, c])
        repo.resolve(storage=storage)
        repo.materialize()

        # Change a's code version
        @rs.Asset(name="a", code_version="v2", io_handler=rs.InMemoryIOHandler())
        def a_v2() -> int:
            return 10

        repo2 = rs.CodeRepository(assets=[a_v2, b, c])
        repo2.resolve(storage=storage)

        snapshot = storage.compute_staleness()

        # A: code stale
        status_a, causes_a = snapshot["a"]
        assert status_a == "Stale"
        assert len(causes_a) == 1
        assert causes_a[0].category == "Code"
        assert causes_a[0].dependency is None

        # B: data stale from A
        status_b, causes_b = snapshot["b"]
        assert status_b == "Stale"
        assert len(causes_b) == 1
        assert causes_b[0].category == "Data"
        assert causes_b[0].dependency == "a"

        # C: data stale from B
        status_c, causes_c = snapshot["c"]
        assert status_c == "Stale"
        assert len(causes_c) == 1
        assert causes_c[0].category == "Data"
        assert causes_c[0].dependency == "b"


# ---------------------------------------------------------------------------
# Test: Selective condition evaluation (time-based + downstream only)
# ---------------------------------------------------------------------------


class TestSelectiveConditionEval:
    def test_cron_subgraph_fires_independent_subgraph_skipped(self, storage):
        """Selective eval: after the initial tick, only cron + downstream get
        evaluated when nothing changed in storage. An independent eager subgraph
        should fire once on the initial tick (catch-up) but NOT on subsequent
        selective ticks.

        Graph:
          ext_feed (on_cron, external) → snapshot (eager)   [cron subgraph]
          source (no condition) → independent (eager)       [independent subgraph]

        The daemon logs which assets fire. We verify:
        - ext_feed fires multiple times (cron triggers each tick)
        - snapshot fires at least once
        - independent fires at most once (initial catch-up only)
        """
        handler = rs.InMemoryIOHandler()

        obs_counter = [0]
        independent_materialize_count = [0]

        @rs.Asset.external(
            io_handler=handler,
            automation_condition=rs.AutomationCondition.on_cron("* * * * * *"),
        )
        def ext_feed(context: rs.AssetExecutionContext):
            obs_counter[0] += 1
            handler.handle_output(
                rs.OutputContext(asset_name="ext_feed"), {"v": obs_counter[0]}
            )
            return rs.Observation(data_version=f"v{obs_counter[0]}")

        @rs.Asset(
            name="snapshot",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def snapshot(ext_feed: dict) -> dict:
            return {"snap": ext_feed["v"]}

        @rs.Asset(name="source", io_handler=handler)
        def source() -> int:
            return 1

        @rs.Asset(
            name="independent",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def independent(source: int) -> int:
            independent_materialize_count[0] += 1
            return source * 10

        repo = rs.CodeRepository(
            assets=[ext_feed, snapshot, source, independent],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize source + independent so they're UpToDate
        repo.materialize(selection=["source", "independent"])
        assert _stale(storage, "independent")[0] == "UpToDate"
        # Reset counter after pre-materialization
        independent_materialize_count[0] = 0

        daemon = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="1s"
        )
        daemon.start()
        try:
            # Wait for multiple cron observations to prove selective eval is running
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if obs_counter[0] >= 3:
                    break
                time.sleep(0.2)
            assert obs_counter[0] >= 3, (
                f"Expected at least 3 observations, got {obs_counter[0]}"
            )

            # snapshot should have been materialized (downstream of cron)
            snap_rec = storage.get_asset_record("snapshot")
            assert snap_rec.last_data_version is not None

            # independent should have materialized at most once (initial catch-up)
            # On subsequent selective ticks, it's not in the eval set
            assert independent_materialize_count[0] <= 1, (
                f"independent materialized {independent_materialize_count[0]} times, "
                f"expected at most 1 (initial catch-up only)"
            )
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Same-tick cascading via WillBeRequested
# ---------------------------------------------------------------------------


class TestSameTickCascading:
    def test_will_be_requested_cascades_through_fresh_chain(self, storage):
        """Three-layer chain starting from Missing. WillBeRequested enables all
        layers to fire in one daemon tick via Missing.newly_true cascading.

        Graph: source (eager) → mid (eager) → leaf (eager)

        All assets start Missing. On the first daemon tick:
          source's eager: Missing.newly_true() fires → source in requested_this_tick
          mid's eager: Missing.newly_true() fires AND any_deps_missing suppressed
            (source is Missing but WillBeRequested → Missing & !true = false)
          leaf's eager: same cascading from mid's WillBeRequested

        Without WillBeRequested in any_deps_missing, mid would be blocked
        (!any_deps_missing = false since source is Missing), requiring source
        to materialize first. With it, all three fire in one tick.
        """

        @rs.Asset(
            name="source",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def source() -> int:
            return 42

        @rs.Asset(
            name="mid",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def mid(source: int) -> int:
            return source * 10

        @rs.Asset(
            name="leaf",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def leaf(mid: int) -> int:
            return mid + 1

        repo = rs.CodeRepository(
            assets=[source, mid, leaf],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # All start as Missing
        assert _stale(storage, "source")[0] == "Missing"
        assert _stale(storage, "mid")[0] == "Missing"
        assert _stale(storage, "leaf")[0] == "Missing"

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # All three should fire on the first tick and materialize together
            record = _wait_for_asset_up_to_date(storage, "leaf", timeout=15)
            assert record is not None
            snapshot = storage.compute_staleness()
            assert snapshot["leaf"][0] == "UpToDate", (
                "'leaf' should have been materialized via same-tick cascading: "
                "eager→source, eager→mid, eager→leaf"
            )

            # Intermediate and source should also be UpToDate
            mid_rec = storage.get_asset_record("mid")
            assert mid_rec.last_data_version is not None
            assert snapshot["mid"][0] == "UpToDate"

            source_rec = storage.get_asset_record("source")
            assert source_rec.last_data_version is not None
            assert snapshot["source"][0] == "UpToDate"

            # Verify provenance chain
            assert len(record.last_input_data_versions) == 1
            assert record.last_input_data_versions[0][0] == "mid"
            assert record.last_input_data_versions[0][1] == mid_rec.last_data_version

            assert len(mid_rec.last_input_data_versions) == 1
            assert mid_rec.last_input_data_versions[0][0] == "source"
            assert (
                mid_rec.last_input_data_versions[0][1] == source_rec.last_data_version
            )
        finally:
            daemon.stop()

    def test_will_be_requested_cascades_through_deep_pre_materialized_chain(
        self, storage
    ):
        """Five-layer pre-materialized chain. Schedule fires on A, then B→C→D→E
        all cascade in the same tick via WillBeRequested.

        Graph: a (schedule → a_job) → b (eager) → c (eager) → d (eager) → e (eager)

        After schedule materializes A:
          tick: b fires (NewlyUpdated for a) → requested_this_tick
                c fires (WillBeRequested for b)
                d fires (WillBeRequested for c)
                e fires (WillBeRequested for d)
                All four downstreams materialize in one batch.
        """

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def b(a: int) -> int:
            return a + 10

        @rs.Asset(
            name="c",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def c(b: int) -> int:
            return b + 100

        @rs.Asset(
            name="d",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def d(c: int) -> int:
            return c + 1000

        @rs.Asset(
            name="e",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def e(d: int) -> int:
            return d + 10000

        a_job = rs.Job(name="a_job", assets=[a])

        # pre-materialize everything
        repo1 = rs.CodeRepository(
            assets=[a, b, c, d, e],
            jobs=[a_job],
            schedules=[],
            default_executor=rs.Executor.in_process(),
        )
        repo1.resolve(storage=storage)
        repo1.materialize()

        snapshot = storage.compute_staleness()
        for key in ["a", "b", "c", "d", "e"]:
            assert snapshot[key][0] == "UpToDate"
        prev_e_version = storage.get_asset_record("e").last_data_version
        prev_d_version = storage.get_asset_record("d").last_data_version

        # add schedule targeting only 'a'
        call_count = 0

        @rs.Schedule(
            cron_schedule="*/5 * * * * *",
            job_name="a_job",
            default_status=rs.ScheduleStatus.Running,
        )
        def trigger_a(context: rs.ScheduleEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("done")

        repo2 = rs.CodeRepository(
            assets=[a, b, c, d, e],
            jobs=[a_job],
            schedules=[trigger_a],
            default_executor=rs.Executor.in_process(),
        )
        repo2.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo2,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for end of 5-layer chain to be re-materialized
            record_e = _wait_for_asset_up_to_date(
                storage, "e", timeout=30, prev_version=prev_e_version
            )
            assert record_e is not None
            snapshot = storage.compute_staleness()
            assert snapshot["e"][0] == "UpToDate", (
                "'e' should have been materialized via cascading: "
                "schedule→a, eager→b, eager→c, eager→d, eager→e"
            )

            # All intermediate layers should also be re-materialized
            record_d = storage.get_asset_record("d")
            assert record_d.last_data_version != prev_d_version
            assert snapshot["d"][0] == "UpToDate"

            for key in ["b", "c"]:
                assert snapshot[key][0] == "UpToDate", (
                    f"'{key}' should have been re-materialized in the cascade"
                )
        finally:
            daemon.stop()


class TestPartitionedNewlyUpdatedRefire:
    def test_no_refire_of_preexisting_partition_on_fresh_start(self, storage):
        """A partitioned ``newly_updated()`` condition must not re-dispatch a
        partition that was already materialized before the condition daemon's
        evaluation state existed.

        A prior run (backfill/manual) materializes the partition, then the daemon
        starts with a fresh evaluation state — a restart or redeploy. On that
        initial tick the partition has a materialization timestamp but no
        previous baseline; without the ``is_initial`` guard it is misread as
        "newly updated" and re-dispatched every tick.
        """
        pd = rs.PartitionsDefinition.static_(["p1", "p2"])

        @rs.Asset(
            name="a",
            io_handler=rs.InMemoryIOHandler(),
            partitions_def=pd,
            automation_condition=rs.AutomationCondition.newly_updated(),
        )
        def a() -> str:
            return "x"

        repo = rs.CodeRepository(
            assets=[a],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        pk = rs.PartitionKey.single("p1")

        # A prior run materialized p1 before the condition daemon ever evaluated.
        repo.materialize(selection=["a"], partition_key=pk)
        assert storage.get_latest_materialization("a", "p1") is not None, (
            "precondition: p1 must be materialized before the daemon starts"
        )

        def _p1_runs():
            return [r for r in storage.get_runs(limit=500) if r.partition_key == pk]

        baseline = len(_p1_runs())

        # Start the condition daemon with a fresh evaluation state (the restart).
        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="300ms",
        )
        daemon.start()
        try:
            # Several initial ticks — long enough for the buggy path to
            # re-dispatch (and the in-process run to land).
            time.sleep(4.0)
        finally:
            daemon.stop()

        after = len(_p1_runs())
        assert after == baseline, (
            "condition daemon re-dispatched an already-materialized partition on "
            f"a fresh start: {baseline} -> {after} runs for p1"
        )


class TestDepAggregateCounterStability:
    def test_no_spurious_refire_when_dep_count_changes(self, storage):
        """A stateful node after a dep-aggregate must keep a stable index when
        the dep count changes (same condition tree → fingerprint unchanged → the
        latch is not reset). Before the deterministic-counter fix the aggregate
        consumed one index slot per dep, so dropping an upstream shifted the
        trailing ``newly_true``, which then read its latch from the wrong key and
        spuriously re-fired.
        """
        # `any_deps_match(execution_failed)` is false (no dep failed) so `.any()`
        # scans ALL deps — its slot count grows with the dep count. The trailing
        # `newly_true(~execution_failed)` latches true once r materializes and
        # must then stay suppressed.
        agg = rs.AutomationCondition.any_deps_match(
            rs.AutomationCondition.execution_failed()
        )
        latch = (~rs.AutomationCondition.execution_failed()).newly_true()
        cond = agg | latch

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(name="b", io_handler=rs.InMemoryIOHandler())
        def b() -> int:
            return 2

        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r(a: int, b: int) -> int:
            return a + b

        repo1 = rs.CodeRepository(
            assets=[a, b, r], default_executor=rs.Executor.in_process()
        )
        repo1.resolve(storage=storage)
        repo1.materialize(selection=["a", "b"])  # so r can load its deps when it fires

        # Run 1: r has TWO deps. `newly_true` fires once (rising edge), r
        # materializes, then the latch suppresses further fires.
        daemon1 = AutomationDaemon(
            repo=repo1, storage=storage, condition_eval_interval="300ms"
        )
        daemon1.start()
        try:
            time.sleep(3.0)
        finally:
            daemon1.stop()

        baseline = len(storage.get_runs(limit=500))

        # Redeploy: r now depends on ONE dep (same condition object → same
        # fingerprint → the persisted latch survives). Only the dep count changed.
        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r_one_dep(a: int) -> int:
            return a

        repo2 = rs.CodeRepository(
            assets=[a, r_one_dep], default_executor=rs.Executor.in_process()
        )
        repo2.resolve(storage=storage)

        # Run 2: with the fix, r's `newly_true` keeps the same node index, reads
        # its latch correctly, and stays suppressed → no new runs.
        daemon2 = AutomationDaemon(
            repo=repo2, storage=storage, condition_eval_interval="300ms"
        )
        daemon2.start()
        try:
            time.sleep(3.0)
        finally:
            daemon2.stop()

        after = len(storage.get_runs(limit=500))
        assert after == baseline, (
            "dropping an upstream dep shifted the trailing newly_true's node "
            f"index and spuriously re-fired r: {baseline} -> {after} runs"
        )


class TestDepAggregateLatchPersistence:
    def test_since_latch_inside_aggregate_persists_across_restart(self, storage):
        """A ``Since`` latch INSIDE ``all_deps_match`` (the shape ``on_cron``
        uses) must persist per-dep across ticks/restarts.

        With no firing reset, once ``a`` is newly-updated the latch stays true and
        the aggregate keeps firing. After ``r`` materializes, ``newly_updated(a)``
        goes false (its floor caught up), so only the persisted latch keeps it
        firing. Without per-dep persistence the latch is written to the root's
        state but read from the dep's — it never round-trips, so ``r`` stops.
        """
        cond = rs.AutomationCondition.all_deps_match(
            rs.AutomationCondition.newly_updated().since(
                rs.AutomationCondition.execution_failed()
            )
        )

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r(a: int) -> int:
            return a

        repo = rs.CodeRepository(
            assets=[a, r], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
        # a is updated, r is not → newly_updated(a) fires in the dep pivot.
        repo.materialize(selection=["a"])

        # Run 1: latch sets true, r materializes; after r's floor catches up the
        # trigger is false but the latch holds, so r keeps firing.
        daemon1 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        daemon1.start()
        try:
            time.sleep(3.0)
        finally:
            daemon1.stop()
        assert storage.get_latest_materialization("r", None) is not None, (
            "run 1: r should have materialized once its dep was newly updated"
        )

        baseline = len(storage.get_runs(limit=500))

        # Run 2 (restart): a is unchanged and r is materialized, so the
        # newly_updated(a) trigger is false. Only the persisted per-dep latch can
        # keep the aggregate firing.
        daemon2 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        daemon2.start()
        try:
            time.sleep(3.0)
        finally:
            daemon2.stop()

        after = len(storage.get_runs(limit=500))
        assert after > baseline, (
            "the Since latch inside all_deps_match did not persist per-dep: r "
            f"stopped firing after its trigger went false ({baseline} -> {after} runs)"
        )


class TestDepAggregateShortCircuitLatch:
    def test_short_circuit_dep_does_not_drop_sibling_latch(self, storage):
        """In ``all_deps_match(newly_updated().since(execution_failed()))`` over
        two deps, a dep whose value goes false must not drop a *later* dep's
        latch. ``.all()`` short-circuits on the first false dep, and a stateful
        aggregate must still evaluate the rest so each dep keeps its latch.

        Dep ``a`` (sorts first, so it is evaluated first) fails — its
        ``.since(execution_failed)`` resets, short-circuiting the aggregate. Dep
        ``b`` did not fail; its latch must survive so that once ``a`` recovers the
        aggregate fires again. With the bug ``b``'s latch is dropped the moment
        ``a`` short-circuits, so ``r`` never fires again.
        """
        fail = {"on": False}
        cond = rs.AutomationCondition.all_deps_match(
            rs.AutomationCondition.newly_updated().since(
                rs.AutomationCondition.execution_failed()
            )
        )

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            if fail["on"]:
                raise RuntimeError("induced failure to reset a's latch")
            return 1

        @rs.Asset(name="b", io_handler=rs.InMemoryIOHandler())
        def b() -> int:
            return 2

        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r(a: int, b: int) -> int:
            return a + b

        repo = rs.CodeRepository(
            assets=[a, b, r], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
        repo.materialize(selection=["a", "b"])  # both deps updated → both latches

        # Phase A: both deps newly-updated → both Since latches set; r fires.
        d1 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        d1.start()
        try:
            _wait_until(
                lambda: storage.get_latest_materialization("r", None) is not None
            )
        finally:
            d1.stop()
        assert storage.get_latest_materialization("r", None) is not None, (
            "phase A: r should materialize once both deps are newly updated"
        )

        # Phase B: a fails → its reset fires → on the next tick `.all()`
        # short-circuits at a and would skip b.
        fail["on"] = True
        repo.materialize(selection=["a"], raise_on_error=False)

        # Phase C: daemon evaluates with a failed → aggregate false (r does not
        # fire), but b's latch must survive the skipped tick.
        d2 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        d2.start()
        try:
            time.sleep(3.0)
        finally:
            d2.stop()

        # Phase D: a recovers (re-materialized newer than r) → newly_updated(a)
        # true again, execution_failed(a) false.
        fail["on"] = False
        repo.materialize(selection=["a"])

        # Phase E: a satisfies the aggregate again; b can only contribute via its
        # persisted latch (newly_updated(b) is false — b is older than r). With
        # the bug b's latch was dropped in phase C, so r never fires again.
        baseline = len(storage.get_runs(limit=500))
        d3 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        d3.start()
        try:
            _wait_until(lambda: len(storage.get_runs(limit=500)) > baseline)
        finally:
            d3.stop()
        after = len(storage.get_runs(limit=500))
        assert after > baseline, (
            "a short-circuiting the aggregate (after its failure) dropped b's "
            f"persisted latch, so r stopped firing ({baseline} -> {after} runs)"
        )


class TestSiblingDepAggregateLatch:
    def test_sibling_aggregates_do_not_clobber_shared_dep_latch(self, storage):
        """Two dep-aggregates over the same dep, combined with ``&``, each keep an
        independent ``Since`` latch. After ``r`` materializes both ``newly_updated``
        triggers go false, so each aggregate fires only from its persisted latch.
        If the second aggregate's per-dep write clobbered the first's, the ``&``
        goes false and ``r`` stops firing.
        """

        def leg():
            return rs.AutomationCondition.any_deps_match(
                rs.AutomationCondition.newly_updated().since(
                    rs.AutomationCondition.execution_failed()
                )
            )

        cond = leg() & leg()  # And([agg, agg]); both pivot the same dep

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r(a: int) -> int:
            return a

        repo = rs.CodeRepository(
            assets=[a, r], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
        repo.materialize(selection=["a"])  # newly_updated(a) → both legs latch

        # Run 1: both legs latch; r fires. After r materializes, both triggers go
        # false and only the persisted latches keep it firing.
        daemon1 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        daemon1.start()
        try:
            _wait_until(
                lambda: storage.get_latest_materialization("r", None) is not None
            )
        finally:
            daemon1.stop()
        assert storage.get_latest_materialization("r", None) is not None, (
            "run 1: r should materialize once a is newly updated"
        )
        baseline = len(storage.get_runs(limit=500))

        # Run 2 (restart): newly_updated(a) is false (r is newer). Each `&` leg can
        # only fire from its persisted per-dep latch; if one was clobbered the
        # conjunction goes false and r stops.
        daemon2 = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        daemon2.start()
        try:
            _wait_until(lambda: len(storage.get_runs(limit=500)) > baseline)
        finally:
            daemon2.stop()
        after = len(storage.get_runs(limit=500))
        assert after > baseline, (
            "a sibling aggregate clobbered the other's per-dep latch: r stopped "
            f"firing after both triggers went false ({baseline} -> {after} runs)"
        )


class TestOnCronWithDepsFiresMidPeriod:
    def test_on_cron_fires_when_dep_updates_after_boundary(self, storage):
        """``on_cron`` with a dependency must fire when the dep updates *after*
        the cron boundary — anywhere within the period, not only on the single
        tick the boundary is crossed.

        Regression test for the one-tick-pulse bug (fix ``1c4bb76``). The gate
        used to be ``CronTickPassed.since_last_handled()``, true only on the tick
        the boundary was detected. But the dep-side evidence inside
        ``all_deps_updated_since_cron`` (``newly_updated().since(cron_tick)``) is
        *reset on that same boundary tick*, so the gate and the dep evidence were
        never true together and ``on_cron``-with-deps could never fire in
        production. The gate now latches from the boundary until the asset is
        handled, so a dep update later in the period fires it.

        Graph: a (plain) -> r (``on_cron`` every 15s, depends on a)
        """
        period = 15
        cond = rs.AutomationCondition.on_cron(f"*/{period} * * * * *")

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="r", io_handler=rs.InMemoryIOHandler(), automation_condition=cond
        )
        def r(a: int) -> int:
            return a

        repo = rs.CodeRepository(
            assets=[a, r], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
        # Materialize in dep order so r's floor is newer than a: on_cron's dep
        # evidence (newly_updated(a)) starts false, and the cron boundary alone
        # must not fire r.
        repo.materialize(selection=["a", "r"])

        # Count only runs that materialize r — the dep bump below issues its own
        # run for a, which must not be mistaken for r firing.
        def r_run_count():
            return sum(
                1 for run in storage.get_runs(limit=500) if "r" in run.node_names
            )

        daemon = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="300ms"
        )
        daemon.start()
        try:
            # Let the daemon take at least one baseline eval tick, then wait for a
            # cron boundary to be crossed *while it is running* so the gate arms.
            # (`time.time() // period` ticks over at each */period second mark,
            # the same instants the seconds-field cron fires.)
            time.sleep(1.0)
            p0 = int(time.time() // period)
            while int(time.time() // period) == p0:
                time.sleep(0.05)
            # A boundary just passed; give the daemon a couple of ticks to detect
            # it and evaluate. We are now ~1.5s into a fresh period (~13s runway
            # before the next boundary would reset the dep evidence).
            time.sleep(1.5)

            # The boundary alone, with a still up to date, must NOT have fired r.
            baseline = r_run_count()
            assert baseline == 1, (
                "the cron boundary fired r even though its dep was not updated "
                f"since the boundary ({baseline} runs materialized r)"
            )

            # Update the dep *after* the boundary, well within the period.
            repo.materialize(selection=["a"])

            # Latched gate + fresh dep evidence now overlap -> r fires within a
            # tick or two. Under the old pulse gate the two were never true
            # together, so r would never fire and this wait would time out.
            _wait_until(lambda: r_run_count() > baseline, timeout=8.0)
            after = r_run_count()
            assert after > baseline, (
                "on_cron did not fire for a dependency updated within the period "
                f"after the boundary tick ({baseline} -> {after} runs materializing "
                "r) — the cron gate must stay armed past the boundary tick"
            )
        finally:
            daemon.stop()
