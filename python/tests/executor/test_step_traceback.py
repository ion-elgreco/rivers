"""A failed step stores its Python traceback in ``run_logs``.

The run page shows it the way an IDE does: each frame with the source lines
around it, the running expression marked, library frames apart from the
user's, and the chain of causes. The process that ran the step captures it,
so the source lines come from that machine: a loky worker captures its own.
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path

import obstore.store
import pytest

import rivers as rs

SOURCE = Path(__file__).read_text().splitlines()


def line_of(marker: str) -> int:
    """The line of this file that ends with ``# <marker>``, from 1."""
    hits = [
        i + 1 for i, line in enumerate(SOURCE) if line.rstrip().endswith(f"# {marker}")
    ]
    assert len(hits) == 1, (marker, hits)
    return hits[0]


@dataclass
class Setup:
    executor: rs.Executor
    handler: rs.BaseIOHandler | None
    parallel: bool


@pytest.fixture(params=["in_process", "parallel"])
def setup(request, tmp_path):
    """The parallel executor runs a lone sync step in-process, so every run
    here adds a sibling step, and its IO handler must pickle."""
    if request.param == "in_process":
        return Setup(rs.Executor.in_process(), None, parallel=False)
    store = obstore.store.LocalStore(str(tmp_path / "io"), mkdir=True)
    return Setup(
        rs.Executor.parallel(max_workers=2),
        rs.PickleIOHandler(store=store),
        parallel=True,
    )


def run(storage, setup, *assets) -> str:
    """Materialize ``assets`` next to a sibling step that succeeds."""

    @rs.Asset(io_handler=setup.handler)
    def steady() -> int:
        return 1

    repo = rs.CodeRepository(assets=[*assets, steady], default_executor=setup.executor)
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    return result.run_id


def tracebacks(storage, run_id, step):
    """``step``'s stored tracebacks as ``(row time, traceback)``, oldest first."""
    return [
        (log.timestamp, json.loads(log.traceback))
        for log in storage.get_run_logs(run_id)
        if log.step_key == step and log.traceback
    ]


def event_times(storage, step, event_type):
    return sorted(
        e.timestamp
        for e in storage.get_events_for_asset(step)
        if e.event_type == event_type and e.partition_key is None
    )


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_failed_step_stores_its_traceback(storage, setup, tmp_path, is_async):
    pid_file = tmp_path / "pid"

    if is_async:

        @rs.Asset(io_handler=setup.handler)
        async def boom() -> int:
            pid_file.write_text(str(os.getpid()))
            values = {"a": 1}
            return values["missing"]  # boom-async

    else:

        @rs.Asset(io_handler=setup.handler)
        def boom() -> int:
            pid_file.write_text(str(os.getpid()))
            values = {"a": 1}
            return values["missing"]  # boom-sync

    run_id = run(storage, setup, boom)

    # A loky worker runs only the parallel executor's sync steps.
    ran_in_worker = int(pid_file.read_text()) != os.getpid()
    assert ran_in_worker == (setup.parallel and not is_async)

    [(stored_at, tb)] = tracebacks(storage, run_id, "boom")
    # The row carries its event's time: that is how the UI pairs them.
    assert [stored_at] == event_times(storage, "boom", "StepFailure")
    [failure] = [
        e for e in storage.get_events_for_asset("boom") if e.event_type == "StepFailure"
    ]
    assert dict(failure.metadata)["error"] == "KeyError: 'missing'"
    assert "Traceback" not in json.dumps(failure.metadata)

    [exc] = tb["exceptions"]
    assert (exc["type"], exc["value"], exc.get("module")) == (
        "KeyError",
        "'missing'",
        None,
    )
    frame = exc["frames"][-1]
    lineno = line_of("boom-async" if is_async else "boom-sync")
    assert frame["function"] == "boom"
    assert os.path.samefile(frame["abs_path"], __file__)
    assert frame["filename"].endswith("tests/executor/test_step_traceback.py")
    assert frame["abs_path"].replace("\\", "/").endswith(frame["filename"])
    assert frame["in_app"] is True
    assert frame["lineno"] == lineno
    assert frame["context_line"] == SOURCE[lineno - 1]
    assert frame["pre_context"] == SOURCE[lineno - 6 : lineno - 1]
    assert frame["post_context"] == SOURCE[lineno : lineno + 5]
    expr = 'values["missing"]'
    start = SOURCE[lineno - 1].index(expr)
    assert (frame["colno"], frame["end_lineno"], frame["end_colno"]) == (
        start + 1,
        lineno,
        start + len(expr),
    )
    assert tb["text"].startswith("Traceback (most recent call last):\n")
    assert tb["text"].endswith("KeyError: 'missing'\n")


def _load(path):
    raise FileNotFoundError(path)  # load-raise


@pytest.mark.parametrize("link", ["cause", "context", "suppressed"])
def test_traceback_follows_the_exception_chain(storage, setup, link):
    @rs.Asset(io_handler=setup.handler)
    def chained() -> int:
        try:
            _load("/data/input.csv")  # load-call
        except FileNotFoundError as e:
            if link == "cause":
                raise RuntimeError("could not read input") from e  # raise-cause
            if link == "context":
                raise RuntimeError("could not read input")  # raise-context
            raise RuntimeError("could not read input") from None  # raise-suppressed

    run_id = run(storage, setup, chained)
    [(_, tb)] = tracebacks(storage, run_id, "chained")

    def summary(exc):
        return (
            exc["type"],
            exc["value"],
            exc.get("chain"),
            [(f["function"], f["lineno"]) for f in exc["frames"]],
        )

    raised = summary(tb["exceptions"][-1])
    assert raised == (
        "RuntimeError",
        "could not read input",
        None if link == "suppressed" else link,
        [("chained", line_of(f"raise-{link}"))],
    )
    if link == "suppressed":
        assert len(tb["exceptions"]) == 1
        return
    assert len(tb["exceptions"]) == 2
    assert summary(tb["exceptions"][0]) == (
        "FileNotFoundError",
        "/data/input.csv",
        None,
        [("chained", line_of("load-call")), ("_load", line_of("load-raise"))],
    )
    expected = (
        "The above exception was the direct cause"
        if link == "cause"
        else "During handling of the above exception"
    )
    assert expected in tb["text"]


def _recurse(n):
    if n == 0:
        raise ValueError("bottom")  # recurse-bottom
    return _recurse(n - 1)  # recurse-call


def test_repeated_frames_fold_as_python_prints_them(storage, setup):
    @rs.Asset(io_handler=setup.handler)
    def deep() -> int:
        return _recurse(50)  # recurse-start

    run_id = run(storage, setup, deep)
    [(_, tb)] = tracebacks(storage, run_id, "deep")
    [exc] = tb["exceptions"]
    call = line_of("recurse-call")
    assert [
        (f["function"], f["lineno"], f.get("repeated", 0)) for f in exc["frames"]
    ] == [
        ("deep", line_of("recurse-start"), 0),
        ("_recurse", call, 0),
        ("_recurse", call, 0),
        ("_recurse", call, 47),
        ("_recurse", line_of("recurse-bottom"), 0),
    ]
    assert "[Previous line repeated 47 more times]" in tb["text"]


def test_library_frames_are_marked_and_keep_one_line(storage, setup):
    @rs.Asset(io_handler=setup.handler)
    def parse() -> int:
        return json.loads("{")  # parse-call

    run_id = run(storage, setup, parse)
    [(_, tb)] = tracebacks(storage, run_id, "parse")
    [exc] = tb["exceptions"]
    assert (exc["type"], exc["module"]) == ("JSONDecodeError", "json.decoder")
    user, *library = exc["frames"]
    assert (user["function"], user["lineno"], user["in_app"]) == (
        "parse",
        line_of("parse-call"),
        True,
    )
    assert len(user["pre_context"]) == 5
    assert library, "json.loads runs through the standard library"
    for frame in library:
        assert frame["in_app"] is False
        assert frame["filename"].startswith("json/"), frame["filename"]
        assert frame["context_line"].strip()
        assert "pre_context" not in frame and "post_context" not in frame


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_each_failed_attempt_stores_its_traceback(storage, setup, is_async):
    retry = rs.RetryPolicy(max_retries=1)
    if is_async:

        @rs.Asset(io_handler=setup.handler, retry=retry)
        async def flaky() -> int:
            raise ValueError("still broken")  # flaky-async

    else:

        @rs.Asset(io_handler=setup.handler, retry=retry)
        def flaky() -> int:
            raise ValueError("still broken")  # flaky-sync

    run_id = run(storage, setup, flaky)
    stored = tracebacks(storage, run_id, "flaky")
    assert len(stored) == 2
    (first_at, first), (second_at, second) = stored
    assert [first_at] == event_times(storage, "flaky", "StepRetry")
    assert [second_at] == event_times(storage, "flaky", "StepFailure")
    marker = "flaky-async" if is_async else "flaky-sync"
    for tb in (first, second):
        [exc] = tb["exceptions"]
        assert (exc["type"], exc["value"]) == ("ValueError", "still broken")
        assert exc["frames"][-1]["lineno"] == line_of(marker)


def test_multi_asset_failure_stores_a_traceback_per_output(storage, setup):
    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("left", io_handler=setup.handler),
            rs.AssetDef("right", io_handler=setup.handler),
        ],
    )
    def pair():
        raise ValueError("no pair today")  # pair-raise

    run_id = run(storage, setup, pair)
    for output in ("left", "right"):
        [(stored_at, tb)] = tracebacks(storage, run_id, output)
        assert [stored_at] == event_times(storage, output, "StepFailure")
        [exc] = tb["exceptions"]
        assert exc["frames"][-1]["lineno"] == line_of("pair-raise")


def test_exception_group_members_are_captured(storage, setup):
    @rs.Asset(io_handler=setup.handler)
    def fan() -> int:
        errors = []
        for exc in (ValueError("first"), KeyError("second")):
            try:
                raise exc  # group-member
            except Exception as e:
                errors.append(e)
        raise ExceptionGroup("two failures", errors)  # noqa: F821  # group-raise

    run_id = run(storage, setup, fan)
    [(_, tb)] = tracebacks(storage, run_id, "fan")
    [group] = tb["exceptions"]
    assert (group["type"], group["value"]) == (
        "ExceptionGroup",
        "two failures (2 sub-exceptions)",
    )
    assert group["frames"][-1]["lineno"] == line_of("group-raise")
    members = [
        (chain[-1]["type"], chain[-1]["value"], chain[-1]["frames"][-1]["lineno"])
        for chain in group["group"]
    ]
    assert members == [
        ("ValueError", "first", line_of("group-member")),
        ("KeyError", "'second'", line_of("group-member")),
    ]


def test_errors_without_python_frames_store_no_traceback(storage):
    """rivers checks the return type in Rust, so that error has no frames.
    In-process only: the parallel worker does not check return types."""

    @rs.Asset
    def wrong_type() -> int:
        return "not an int"

    run_id = run(storage, Setup(rs.Executor.in_process(), None, False), wrong_type)
    assert event_times(storage, "wrong_type", "StepFailure")
    assert tracebacks(storage, run_id, "wrong_type") == []
