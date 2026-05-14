from typing import Any

import pytest
import rivers as rs
from rivers.exceptions import ExecutionError, GraphValidationError, NodeNotFoundError


def test_job_creation():
    @rs.Asset
    def source():
        return [1, 2, 3]

    @rs.Asset
    def sink(source: list):
        return [x * 2 for x in source]

    repo = rs.CodeRepository(
        assets=[source, sink],
        jobs=[
            rs.Job(
                name="test", assets=[source, sink], executor=rs.Executor.in_process()
            )
        ],
    )
    job = repo.get_job("test")
    assert job is not None


def test_job_execute_in_process():
    @rs.Asset
    def source():
        return [1, 2, 3]

    @rs.Asset
    def sink(source: list):
        return [x * 2 for x in source]

    repo = rs.CodeRepository(
        assets=[source, sink],
        jobs=[
            rs.Job(
                name="test", assets=[source, sink], executor=rs.Executor.in_process()
            )
        ],
    )
    repo.get_job("test").execute()

    assert repo.load_node("source") == [1, 2, 3]
    assert repo.load_node("sink") == [2, 4, 6]


def test_job_execute_chain():
    @rs.Asset
    def step_a():
        return 10

    @rs.Asset
    def step_b(step_a: int):
        return step_a + 5

    @rs.Asset
    def step_c(step_b: int):
        return step_b * 2

    repo = rs.CodeRepository(
        assets=[step_a, step_b, step_c],
        jobs=[
            rs.Job(
                name="chain",
                assets=[step_a, step_b, step_c],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("chain").execute()

    assert repo.load_node("step_a") == 10
    assert repo.load_node("step_b") == 15
    assert repo.load_node("step_c") == 30


def test_job_execute_diamond():
    @rs.Asset
    def source():
        return 10

    @rs.Asset
    def left(source: int):
        return source + 1

    @rs.Asset
    def right(source: int):
        return source + 2

    @rs.Asset
    def merge(left: int, right: int):
        return left + right

    repo = rs.CodeRepository(
        assets=[source, left, right, merge],
        jobs=[
            rs.Job(
                name="diamond",
                assets=[source, left, right, merge],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("diamond").execute()

    assert repo.load_node("source") == 10
    assert repo.load_node("left") == 11
    assert repo.load_node("right") == 12
    assert repo.load_node("merge") == 23


def test_job_no_deps():
    """Assets with no dependencies should still execute."""

    @rs.Asset
    def standalone():
        return 42

    repo = rs.CodeRepository(
        assets=[standalone],
        jobs=[
            rs.Job(name="solo", assets=[standalone], executor=rs.Executor.in_process())
        ],
    )
    repo.get_job("solo").execute()

    assert repo.load_node("standalone") == 42


def test_job_independent_assets():
    """Disconnected independent assets in a job are valid."""

    @rs.Asset
    def foo():
        return 1

    @rs.Asset
    def bar():
        return 2

    @rs.Asset
    def baz():
        return 3

    repo = rs.CodeRepository(
        assets=[foo, bar, baz],
        jobs=[
            rs.Job(
                name="independent",
                assets=[foo, bar, baz],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("independent").execute()

    assert repo.load_node("foo") == 1
    assert repo.load_node("bar") == 2
    assert repo.load_node("baz") == 3


def test_job_broken_chain_raises():
    """Job with missing intermediate dependency raises ValueError."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b(a: int):
        return a + 1

    @rs.Asset
    def c(b: int):
        return b + 1

    with pytest.raises(
        GraphValidationError, match="depends on 'a' which is not in the job"
    ):
        repo = rs.CodeRepository(
            assets=[a, b, c],
            jobs=[
                rs.Job(name="broken", assets=[b, c], executor=rs.Executor.in_process())
            ],
        )
        repo.resolve()


def test_job_asset_not_in_repo_raises():
    """Job referencing an asset not in the repository raises ValueError."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b():
        return 2

    with pytest.raises(NodeNotFoundError, match="not found in repository"):
        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="bad", assets=[a, b], executor=rs.Executor.in_process())],
        )
        repo.resolve()


def test_job_allow_incomplete_deps_with_io_handler():
    """allow_incomplete_deps=True succeeds when missing dep has io_handler."""

    class DummyHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return 42

    @rs.Asset(io_handler=DummyHandler())
    def a() -> int:
        return 1

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(
        assets=[a, b],
        jobs=[
            rs.Job(
                name="partial",
                assets=[b],
                executor=rs.Executor.in_process(),
                allow_incomplete_deps=True,
            )
        ],
    )
    repo.get_job("partial").execute()
    # b loads a via io_handler's load_input (returns 42)
    assert repo.load_node("b") == 43


def test_job_allow_incomplete_deps_without_io_handler_raises():
    """allow_incomplete_deps=True requires an explicit IO handler on incomplete deps
    so their outputs can be loaded without re-executing."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b(a: int):
        return a + 1

    repo = rs.CodeRepository(
        assets=[a, b],
        jobs=[
            rs.Job(
                name="bad",
                assets=[b],
                executor=rs.Executor.in_process(),
                allow_incomplete_deps=True,
            )
        ],
    )
    with pytest.raises(GraphValidationError, match="no io_handler"):
        repo.resolve()


def test_job_execute_before_validation_raises():
    """Executing a job not added to a CodeRepository raises ValueError."""

    @rs.Asset
    def a():
        return 1

    job = rs.Job(name="orphan", assets=[a], executor=rs.Executor.in_process())

    with pytest.raises(ExecutionError, match="not been validated"):
        job.execute()


def test_multiple_jobs_from_same_repo():
    """Multiple jobs can share the same repository."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b(a: int):
        return a + 1

    @rs.Asset
    def c():
        return 99

    repo = rs.CodeRepository(
        assets=[a, b, c],
        jobs=[
            rs.Job(name="job_ab", assets=[a, b], executor=rs.Executor.in_process()),
            rs.Job(name="job_c", assets=[c], executor=rs.Executor.in_process()),
        ],
    )

    repo.get_job("job_ab").execute()
    assert repo.load_node("a") == 1
    assert repo.load_node("b") == 2

    repo.get_job("job_c").execute()
    assert repo.load_node("c") == 99


def test_get_job_not_found_raises():
    """get_job with unknown name raises ValueError."""

    @rs.Asset
    def a():
        return 1

    repo = rs.CodeRepository(assets=[a])

    with pytest.raises(NodeNotFoundError, match="not found"):
        repo.get_job("nonexistent")


def test_duplicate_job_name_raises():
    """Two jobs with the same name raises ValueError."""

    @rs.Asset
    def a():
        return 1

    with pytest.raises(GraphValidationError, match="Duplicate job name"):
        repo = rs.CodeRepository(
            assets=[a],
            jobs=[
                rs.Job(name="dupe", assets=[a], executor=rs.Executor.in_process()),
                rs.Job(name="dupe", assets=[a], executor=rs.Executor.in_process()),
            ],
        )
        repo.resolve()


def test_default_executor_inherited():
    """Job without executor inherits from repo's default executor."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b(a: int):
        return a + 1

    repo = rs.CodeRepository(
        assets=[a, b],
        default_executor=rs.Executor.in_process(),
        jobs=[rs.Job(name="test", assets=[a, b])],
    )
    repo.get_job("test").execute()
    assert repo.load_node("a") == 1
    assert repo.load_node("b") == 2


def test_materialize_shorthand():
    """repo.materialize() runs all non-external assets via an ephemeral job."""

    @rs.Asset
    def a():
        return 5

    @rs.Asset
    def b(a: int):
        return a + 10

    repo = rs.CodeRepository(
        assets=[a, b],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("a") == 5
    assert repo.load_node("b") == 15


def test_job_executor_overrides_default():
    """Explicit job executor takes precedence over default."""

    @rs.Asset
    def a():
        return 1

    repo = rs.CodeRepository(
        assets=[a],
        default_executor=rs.Executor.parallel(max_workers=2),
        jobs=[rs.Job(name="test", assets=[a], executor=rs.Executor.in_process())],
    )
    repo.get_job("test").execute()
    assert repo.load_node("a") == 1


def test_no_explicit_default_uses_parallel():
    """When no default_executor is passed, parallel is used."""

    @rs.Asset
    def a():
        return 42

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()
    assert repo.load_node("a") == 42


def test_schedule_triggers_correct_job():
    """Schedule RunRequest with job_name triggers only that job's assets.

    Simulates the daemon flow: evaluate_schedule → get RunRequest.job_name →
    get_job(job_name).execute(). Verifies the run only contains the job's
    assets, not all repo assets.
    """

    @rs.Asset
    def source() -> Any:
        return 10

    @rs.Asset
    def fast_step(source: int) -> Any:
        return source + 1

    @rs.Asset
    def slow_step(source: int) -> Any:
        return source * 2

    @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b"]))
    def partitioned(context: rs.AssetExecutionContext, source: int) -> Any:
        return source

    @rs.Schedule(cron_schedule="*/2 * * * *", job_name="fast_job")
    def fast_schedule(context: rs.ScheduleEvaluationContext):
        return rs.RunRequest(run_key=f"fast-{context.scheduled_execution_time}")

    repo = rs.CodeRepository(
        assets=[source, fast_step, slow_step, partitioned],
        jobs=[
            rs.Job(
                name="fast_job",
                assets=[source, fast_step],
                executor=rs.Executor.in_process(),
            ),
            rs.Job(
                name="slow_job",
                assets=[source, slow_step],
                executor=rs.Executor.in_process(),
            ),
        ],
        schedules=[fast_schedule],
    )

    # Evaluate schedule — returns RunRequest with job_name="fast_job"
    tick = repo.evaluate_schedule("fast_schedule")
    assert len(tick.run_requests) == 1
    assert tick.run_requests[0].job_name == "fast_job"

    # Simulate daemon: use job_name from RunRequest to execute the correct job
    job_name = tick.run_requests[0].job_name
    repo.get_job(job_name).execute()

    # Should contain fast_job's assets
    assert repo.load_node("source") == 10
    assert repo.load_node("fast_step") == 11


def test_job_executes_only_its_assets():
    """A named job should only execute its own assets, not all repo assets.

    Regression test: the daemon was calling materialize(selection=None) which
    selected ALL assets. When the repo had partitioned assets without a
    partition_key, this failed silently. The fix is to use get_job().execute()
    which respects the job's asset selection.
    """

    @rs.Asset
    def shared_source():
        return 10

    @rs.Asset
    def pipeline_a(shared_source: int):
        return shared_source + 1

    @rs.Asset
    def pipeline_b(shared_source: int):
        return shared_source + 2

    @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["x", "y"]))
    def partitioned_asset(context: rs.AssetExecutionContext, shared_source: int):
        return shared_source * 100

    repo = rs.CodeRepository(
        assets=[shared_source, pipeline_a, pipeline_b, partitioned_asset],
        jobs=[
            rs.Job(
                name="job_a",
                assets=[shared_source, pipeline_a],
                executor=rs.Executor.in_process(),
            ),
            rs.Job(
                name="job_b",
                assets=[shared_source, pipeline_b],
                executor=rs.Executor.in_process(),
            ),
        ],
    )

    # job_a should only run shared_source + pipeline_a
    repo.get_job("job_a").execute()
    assert repo.load_node("shared_source") == 10
    assert repo.load_node("pipeline_a") == 11

    # job_b should only run shared_source + pipeline_b
    repo.get_job("job_b").execute()
    assert repo.load_node("shared_source") == 10
    assert repo.load_node("pipeline_b") == 12
