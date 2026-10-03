"""Thread-safety of asset and action results whose metadata dict another
thread changes while rivers converts it."""

import gc
import json
import threading
import time

import pytest
from _threads import N_THREADS, run_in_child, run_threads

import rivers as rs

N_KEYS = 100
N_BUILDS = 3


class SlowInt:
    """An int whose conversion sleeps and collects garbage."""

    def __init__(self, value):
        self.value = value

    def __index__(self):
        time.sleep(0.01)
        gc.collect()
        return self.value


def changing_metadata():
    """The metadata dict the tests change; converting ``k0`` is slow, so the
    dict changes while it is converted."""
    return {"k0": SlowInt(0)} | {f"k{i}": i for i in range(1, N_KEYS)}


def build_while_changing(shared, build):
    """Call ``build()`` ``N_BUILDS`` times on each of ``N_THREADS - 1`` threads
    while another thread keeps adding a key to ``shared`` and removing it."""
    stop = threading.Event()

    def change():
        n = 0
        while not stop.is_set():
            if n % 2 == 0:
                shared[f"x{n}"] = n
            else:
                del shared[f"x{n - 1}"]
            n += 1

    changer = threading.Thread(target=change)
    changer.start()
    try:
        results, errors = run_threads(
            lambda i: [build() for _ in range(N_BUILDS)], n=N_THREADS - 1
        )
    finally:
        stop.set()
        changer.join()
    return [r for built in results if built for r in built], errors


def is_snapshot(metadata):
    """True if ``metadata`` is the changing dict at one moment: every original
    key and at most one added key."""
    added = [int(k[1:]) for k in metadata if k.startswith("x")]
    expected = {f"k{i}": i for i in range(N_KEYS)} | {f"x{n}": n for n in added}
    return metadata == expected and len(added) <= 1


ACTION_RESULTS = {
    "unchanged": rs.ActionResult.unchanged,
    "materialized": rs.ActionResult.materialized,
}


@pytest.mark.parametrize("outcome", ACTION_RESULTS)
def test_action_result_from_a_changing_dict(outcome):
    """An action result built from a dict that another thread changes keeps
    a consistent copy of it and never panics."""
    shared = changing_metadata()
    results, errors = build_while_changing(
        shared, lambda: ACTION_RESULTS[outcome](metadata=shared)
    )

    assert errors == []
    assert len(results) == (N_THREADS - 1) * N_BUILDS
    assert all(
        is_snapshot({k: v.raw_value() for k, v in r.metadata.items()}) for r in results
    )


ASSET_RESULTS = {
    "output": lambda metadata: rs.Output(1, metadata=metadata),
    "materialization": lambda metadata: rs.Materialization(metadata=metadata),
    "observation": lambda metadata: rs.Observation(metadata=metadata),
}


def record_results_in_threads(kind):
    """Child process of ``test_asset_result_from_a_changing_dict``: print the
    metadata of every recorded event as JSON."""
    shared = changing_metadata()

    def a():
        return ASSET_RESULTS[kind](shared)

    if kind == "observation":
        asset = rs.Asset.external(io_handler=rs.InMemoryIOHandler())(a)
    else:
        asset = rs.Asset(a)
    repo = rs.CodeRepository(assets=[asset], default_executor=rs.Executor.in_process())
    run = repo.observe if kind == "observation" else repo.materialize
    run_ids, errors = build_while_changing(shared, lambda: run().run_id)
    metadata = [
        {k: next(iter(json.loads(v).values()))["value"] for k, v in event.metadata}
        for run_id in run_ids
        for event in repo.storage.get_events_for_run(run_id)
        if event.event_type in ("Materialization", "Observation")
    ]
    print(json.dumps({"errors": errors, "metadata": metadata}))


@pytest.mark.parametrize("kind", ASSET_RESULTS)
def test_asset_result_from_a_changing_dict(kind, tmp_path):
    """An asset result whose metadata dict another thread changes records a
    consistent copy of it and never panics."""
    proc = run_in_child(
        "concurrency.test_thread_safety_results:record_results_in_threads",
        kind,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result["errors"] == []
    assert len(result["metadata"]) == (N_THREADS - 1) * N_BUILDS
    assert all(is_snapshot(m) for m in result["metadata"])
