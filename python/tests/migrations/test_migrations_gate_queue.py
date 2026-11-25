"""The run queue keeps a queued run that the asset version gate holds, with
the reason, and fails a queued run that the gate refuses."""

import contextlib

import rivers as rs
from _polling import wait_for_run_terminal
from rivers._core import AutomationDaemon


def _repo(storage, **declared):
    """A resolved repository in run-queue mode of ``orders`` at ``declared``
    and of ``customers``."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), **declared)
    def orders() -> int:
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def customers() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[orders, customers],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(dequeue_interval="50ms"),
    )
    repo.resolve(storage=storage)
    return repo


@contextlib.contextmanager
def _daemon(repo, storage):
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        yield
    finally:
        daemon.stop()


def test_the_queue_holds_the_runs_of_a_migrating_asset(storage):
    assert _repo(storage).materialize().success
    repo = _repo(storage, schema_version=2)
    held = repo._submit_run(["orders"]).run_id
    other = repo._submit_run(["customers"]).run_id

    with _daemon(repo, storage):
        # One tick sees both runs, in the order they were queued.
        assert wait_for_run_terminal(storage, other).status == "Success"

    run = storage.get_run(held)
    assert (run.status, run.block_reason) == (
        "Queued",
        "orders is migrating to version 2",
    )


def test_the_queue_fails_the_runs_of_old_code(storage):
    _repo(storage, schema_version=3)
    old = _repo(storage, schema_version=2)
    run_id = old._submit_run(["orders"]).run_id

    with _daemon(old, storage):
        run = wait_for_run_terminal(storage, run_id)

    assert run.status == "Failure"
    errors = [
        dict(event.metadata)["error"]
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "RunLaunchFailed"
    ]
    assert errors == [
        "this code declares orders version 2; the stored data is at version 3"
    ]
