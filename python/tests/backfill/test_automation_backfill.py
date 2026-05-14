"""Integration tests for BackfillRequest from sensors/schedules."""

import pytest
import rivers as rs


class TestBackfillRequestType:
    def test_construction(self):
        keys = [
            rs.PartitionKey.single("2024-01-01"),
            rs.PartitionKey.single("2024-01-02"),
        ]
        req = rs.BackfillRequest(
            selection=["asset_a", "asset_b"],
            partition_keys=keys,
            max_concurrency=8,
        )
        assert req.selection == ["asset_a", "asset_b"]
        assert len(req.partition_keys) == 2
        assert req.partition_keys[0] == rs.PartitionKey.single("2024-01-01")
        assert req.partition_keys[1] == rs.PartitionKey.single("2024-01-02")
        assert req.max_concurrency == 8
        assert req.strategy is None
        assert req.failure_policy is None
        assert req.tags is None
        assert req.partition_range is None

    def test_with_strategy(self):
        req = rs.BackfillRequest(
            selection=["asset_a"],
            partition_keys=[rs.PartitionKey.single("x")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert req.strategy == rs.BackfillStrategy.SingleRun()
        assert req.selection == ["asset_a"]

    def test_with_range(self):
        rng = rs.PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-01-31")
        req = rs.BackfillRequest(
            selection=["asset_a"],
            partition_range=rng,
        )
        assert req.partition_keys is None
        assert req.partition_range is not None
        assert req.selection == ["asset_a"]

    def test_with_failure_policy_and_tags(self):
        req = rs.BackfillRequest(
            selection=["a"],
            partition_keys=[rs.PartitionKey.single("x")],
            failure_policy="stop_on_failure",
            tags={"team": "data", "env": "prod"},
        )
        assert req.failure_policy == "stop_on_failure"
        assert req.tags == {"team": "data", "env": "prod"}

    def test_repr(self):
        req = rs.BackfillRequest(
            selection=["a", "b"],
            partition_keys=[rs.PartitionKey.single("x")],
            max_concurrency=2,
        )
        r = repr(req)
        assert "BackfillRequest" in r
        assert '["a", "b"]' in r
        assert "max_concurrency=2" in r


class TestSensorResultWithBackfills:
    def test_sensor_result_with_backfill_requests(self):
        bf = rs.BackfillRequest(
            selection=["asset_a"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        result = rs.SensorResult(run_requests=[bf], cursor="123")
        assert result.run_requests is not None
        assert len(result.run_requests) == 1
        assert isinstance(result.run_requests[0], rs.BackfillRequest)
        assert result.run_requests[0].selection == ["asset_a"]
        assert len(result.run_requests[0].partition_keys) == 2
        assert result.cursor == "123"

    def test_sensor_result_mixed_run_and_backfill(self):
        result = rs.SensorResult(
            run_requests=[
                rs.RunRequest(partition_key="x", tags={"k": "v"}),
                rs.BackfillRequest(
                    selection=["a"],
                    partition_keys=[rs.PartitionKey.single("y")],
                    max_concurrency=16,
                ),
            ],
        )
        assert len(result.run_requests) == 2

        run = result.run_requests[0]
        assert isinstance(run, rs.RunRequest)
        assert run.partition_key == "x"
        assert run.tags == {"k": "v"}

        bf = result.run_requests[1]
        assert isinstance(bf, rs.BackfillRequest)
        assert bf.selection == ["a"]
        assert bf.partition_keys[0] == rs.PartitionKey.single("y")
        assert bf.max_concurrency == 16

    def test_sensor_result_backfill_and_skip_mutually_exclusive(self):
        with pytest.raises(Exception, match="cannot have both"):
            rs.SensorResult(
                run_requests=[
                    rs.BackfillRequest(
                        selection=["a"],
                        partition_keys=[rs.PartitionKey.single("x")],
                    ),
                ],
                skip_reason="nope",
            )

    def test_sensor_result_invalid_type_rejected(self):
        with pytest.raises(Exception, match="RunRequest or BackfillRequest"):
            rs.SensorResult(run_requests=["not_a_request"])


class TestSensorEmitsBackfill:
    def test_sensor_returns_backfill_in_sensor_result(self):
        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]))
        def my_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SensorResult(
                run_requests=[
                    rs.BackfillRequest(
                        selection=["my_asset"],
                        partition_keys=[
                            rs.PartitionKey.single("a"),
                            rs.PartitionKey.single("b"),
                        ],
                        max_concurrency=2,
                    ),
                ],
                cursor="done",
            )

        repo = rs.CodeRepository(assets=[my_asset], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 1
        bf = result.run_requests[0]
        assert isinstance(bf, rs.BackfillRequest)
        assert bf.selection == ["my_asset"]
        assert len(bf.partition_keys) == 2
        assert bf.partition_keys[0] == rs.PartitionKey.single("a")
        assert bf.partition_keys[1] == rs.PartitionKey.single("b")
        assert bf.max_concurrency == 2
        assert result.cursor == "done"

    def test_sensor_returns_single_backfill_request_directly(self):
        """A sensor can return a BackfillRequest directly (not wrapped in SensorResult)."""

        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["x"]))
        def some_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Sensor(job_name="j")
        def direct_sensor(context: rs.SensorEvaluationContext):
            return rs.BackfillRequest(
                selection=["some_asset"],
                partition_keys=[rs.PartitionKey.single("x")],
            )

        repo = rs.CodeRepository(assets=[some_asset], sensors=[direct_sensor])
        result = repo.evaluate_sensor("direct_sensor")

        backfills = [
            r for r in result.run_requests if isinstance(r, rs.BackfillRequest)
        ]
        runs = [r for r in result.run_requests if isinstance(r, rs.RunRequest)]
        assert len(backfills) == 1
        assert len(runs) == 0
        assert backfills[0].selection == ["some_asset"]
        assert backfills[0].partition_keys[0] == rs.PartitionKey.single("x")

    def test_sensor_returns_mixed_list_directly(self):
        """A sensor can return a plain list mixing RunRequest and BackfillRequest."""

        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a"]))
        def list_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Sensor(job_name="j")
        def list_sensor(context: rs.SensorEvaluationContext):
            return [
                rs.RunRequest(partition_key="a"),
                rs.BackfillRequest(
                    selection=["list_asset"],
                    partition_keys=[rs.PartitionKey.single("a")],
                ),
            ]

        repo = rs.CodeRepository(assets=[list_asset], sensors=[list_sensor])
        result = repo.evaluate_sensor("list_sensor")

        runs = [r for r in result.run_requests if isinstance(r, rs.RunRequest)]
        backfills = [
            r for r in result.run_requests if isinstance(r, rs.BackfillRequest)
        ]
        assert len(runs) == 1
        assert runs[0].partition_key == "a"
        assert len(backfills) == 1
        assert backfills[0].selection == ["list_asset"]


class TestScheduleEmitsBackfill:
    def test_schedule_returns_backfill_request(self):
        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b"]))
        def sched_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_schedule(context: rs.ScheduleEvaluationContext):
            return rs.BackfillRequest(
                selection=["sched_asset"],
                partition_keys=[
                    rs.PartitionKey.single("a"),
                    rs.PartitionKey.single("b"),
                ],
            )

        repo = rs.CodeRepository(assets=[sched_asset], schedules=[my_schedule])
        result = repo.evaluate_schedule("my_schedule")

        backfills = [
            r for r in result.run_requests if isinstance(r, rs.BackfillRequest)
        ]
        runs = [r for r in result.run_requests if isinstance(r, rs.RunRequest)]
        assert len(backfills) == 1
        assert len(runs) == 0
        assert backfills[0].selection == ["sched_asset"]
        assert backfills[0].partition_keys[0] == rs.PartitionKey.single("a")
        assert backfills[0].partition_keys[1] == rs.PartitionKey.single("b")

    def test_schedule_returns_mixed_list(self):
        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a"]))
        def mix_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def mixed_schedule(context: rs.ScheduleEvaluationContext):
            return [
                rs.RunRequest(partition_key="a"),
                rs.BackfillRequest(
                    selection=["mix_asset"],
                    partition_keys=[rs.PartitionKey.single("a")],
                    strategy=rs.BackfillStrategy.single_run(),
                ),
            ]

        repo = rs.CodeRepository(assets=[mix_asset], schedules=[mixed_schedule])
        result = repo.evaluate_schedule("mixed_schedule")

        runs = [r for r in result.run_requests if isinstance(r, rs.RunRequest)]
        backfills = [
            r for r in result.run_requests if isinstance(r, rs.BackfillRequest)
        ]
        assert len(runs) == 1
        assert runs[0].partition_key == "a"
        assert runs[0].job_name == "my_job"  # default applied
        assert len(backfills) == 1
        assert backfills[0].selection == ["mix_asset"]
        assert backfills[0].strategy == rs.BackfillStrategy.SingleRun()
