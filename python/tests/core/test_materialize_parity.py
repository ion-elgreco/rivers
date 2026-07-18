"""Parity tests: repo.materialize() vs Job.execute() for fan-out pipelines.

`Job::validate_and_build_plan` (python/src/job.rs) calls
`plan.apply_fan_out_kinds(step_kinds)`, which rewrites map-step kinds from
`Normal` to `Mapped` so the executor fans out element-wise.

`materialize_with_launcher` (python/src/repository/mod.rs) builds its plan via
`ExecutionPlan::from_subgraph(...)` but never calls `apply_fan_out_kinds`. As a
result the map step runs once with the whole list as input, producing a type
error like ``Asset '<name>/<task>' returned value of type 'list' but expected
'<int>'`` and skipping every downstream step.

These tests fail today and should pass after `materialize` is rebuilt on top of
the same plan-building path Job uses.
"""

import rivers as rs

IN_PROCESS = rs.Executor.in_process()


@rs.Task
def double(x: int) -> int:
    return x * 2


@rs.Task
def sum_all(values: list) -> int:
    return sum(values)


@rs.Asset
def numbers() -> list:
    return [1, 2, 3, 4, 5]


@rs.Asset.from_graph()
def doubled():
    nums = numbers()
    mapped = nums.map(double)
    return sum_all(mapped.collect())


def test_job_execute_fan_out_baseline():
    """Baseline: Job.execute() fans out correctly."""
    repo = rs.CodeRepository(
        assets=[numbers, doubled],
        tasks=[double, sum_all],
        jobs=[rs.Job(name="pipeline", assets=[numbers, doubled], executor=IN_PROCESS)],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("doubled") == 30  # (1+2+3+4+5) * 2


def test_materialize_default_fan_out():
    """repo.materialize() (no selection) on a fan-out graph asset must apply
    fan-out kinds; today the map step runs once on the whole list."""
    repo = rs.CodeRepository(
        assets=[numbers, doubled],
        tasks=[double, sum_all],
        default_executor=IN_PROCESS,
    )
    result = repo.materialize(raise_on_error=False)
    assert result.success, f"materialize failed: failed={result.failed_assets}"
    assert repo.load_node("doubled") == 30


def test_materialize_selection_fan_out():
    """repo.materialize(selection=[...]) on a fan-out graph asset must apply
    fan-out kinds; today the map step runs once on the whole list."""
    repo = rs.CodeRepository(
        assets=[numbers, doubled],
        tasks=[double, sum_all],
        default_executor=IN_PROCESS,
    )
    result = repo.materialize(selection=["numbers", "doubled"], raise_on_error=False)
    assert result.success, f"materialize failed: failed={result.failed_assets}"
    assert repo.load_node("doubled") == 30


# ---------------------------------------------------------------------------
# Tests pinning materialize-only features the refactor must preserve.
# ---------------------------------------------------------------------------


@rs.Asset
def simple() -> int:
    return 1


@rs.Asset
def always_fails() -> int:
    raise RuntimeError("boom")


def test_materialize_tags_propagate_to_run_record(storage):
    """materialize(tags=...) must end up in RunRecord.tags and priority must
    be derived via priority_from_tags. The refactor must preserve this."""
    repo = rs.CodeRepository(assets=[simple], default_executor=IN_PROCESS)
    repo.resolve(storage=storage)

    result = repo.materialize(
        selection=["simple"],
        tags=[("team", "data"), ("rivers/priority", "5")],
    )
    assert result.success

    run = storage.get_run(result.run_id)
    assert run is not None
    tag_dict = dict(run.tags)
    assert tag_dict["team"] == "data"
    assert tag_dict["rivers/priority"] == "5"
    assert run.priority == 5


def test_materialize_raise_on_error_false_collects_failures():
    """materialize(raise_on_error=False) must return a PyRunResult with all
    failures listed, not raise on the first one. Job has no equivalent;
    the refactor must keep this materialize-only mode working."""
    repo = rs.CodeRepository(assets=[simple, always_fails], default_executor=IN_PROCESS)
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    failed_names = [name for name, _ in result.failed_assets]
    assert "always_fails" in failed_names


def test_materialize_captures_stdout_to_run_logs(storage):
    """materialize() must install stdout/stderr capture so step prints land in
    run_logs rows, the same way Job.execute() does. Job.execute() calls
    rivers._capture.install (job.rs:319); materialize_with_launcher does not."""
    import rivers._capture as cap

    cap._installed = False  # force install path to run

    @rs.Asset
    def chatty() -> int:
        print("hello from materialize")
        return 7

    repo = rs.CodeRepository(assets=[chatty], default_executor=IN_PROCESS)
    repo.resolve(storage=storage)

    result = repo.materialize(selection=["chatty"])
    assert result.success

    logs = [
        log for log in storage.get_run_logs(result.run_id) if log.step_key == "chatty"
    ]
    assert logs, "materialize did not produce any run_logs rows"
    assert "hello from materialize" in (logs[0].stdout or "")
