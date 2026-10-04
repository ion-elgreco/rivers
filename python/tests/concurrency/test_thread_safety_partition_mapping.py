"""Thread-safety of definitions whose ``partition_mapping`` dict another
thread changes while rivers converts it."""

import gc
import threading
import time

import pytest
from _threads import N_THREADS, run_threads

import rivers as rs

N_KEYS = 100
N_BUILDS = 3
ADDED = rs.PartitionMapping.identity()


class SlowKey:
    """A key for the asset ``k0`` whose ``name`` sleeps and collects garbage."""

    @property
    def name(self):
        time.sleep(0.01)
        gc.collect()
        return "k0"


def changing_mapping():
    """The ``partition_mapping`` dict the tests change; naming its first key is
    slow, so the dict changes while it is converted."""
    return {SlowKey(): rs.PartitionMapping.time_window(0)} | {
        f"k{i}": rs.PartitionMapping.time_window(i) for i in range(1, N_KEYS)
    }


def define_while_changing(shared, define):
    """Call ``define()`` ``N_BUILDS`` times on each of ``N_THREADS - 1`` threads
    while another thread keeps adding a key to ``shared`` and removing it."""
    stop = threading.Event()

    def change():
        n = 0
        while not stop.is_set():
            if n % 2 == 0:
                shared[f"x{n}"] = ADDED
            else:
                del shared[f"x{n - 1}"]
            n += 1

    changer = threading.Thread(target=change)
    changer.start()
    try:
        results, errors = run_threads(
            lambda i: [define() for _ in range(N_BUILDS)], n=N_THREADS - 1
        )
    finally:
        stop.set()
        changer.join()
    return [r for defined in results if defined for r in defined], errors


def is_snapshot(mapping):
    """True if ``mapping`` is the changing dict at one moment: every original
    key and at most one added key."""
    added = [k for k in mapping if k.startswith("x")]
    expected = {
        f"k{i}": rs.PartitionMapping.time_window(i) for i in range(N_KEYS)
    } | dict.fromkeys(added, ADDED)
    return mapping == expected and len(added) <= 1


def set_on_asset_def(mapping):
    asset_def = rs.AssetDef(name="a")
    asset_def.partition_mapping = mapping
    return asset_def


ASSET_DEFS = {
    "init": lambda mapping: rs.AssetDef(name="a", partition_mapping=mapping),
    "setter": set_on_asset_def,
}


@pytest.mark.parametrize("how", ASSET_DEFS)
def test_asset_def_from_a_changing_partition_mapping(how):
    """An AssetDef given a ``partition_mapping`` dict that another thread
    changes keeps a consistent copy of it and never panics."""
    shared = changing_mapping()
    defs, errors = define_while_changing(shared, lambda: ASSET_DEFS[how](shared))

    assert errors == []
    assert len(defs) == (N_THREADS - 1) * N_BUILDS
    assert all(is_snapshot(d.partition_mapping) for d in defs)


TASKS = {
    "task": lambda mapping: rs.Task(name="t", partition_mapping=mapping),
    "bash_task": lambda mapping: rs.BashTask("t", "true", partition_mapping=mapping),
}


@pytest.mark.parametrize("kind", TASKS)
def test_task_from_a_changing_partition_mapping(kind):
    """A task given a ``partition_mapping`` dict that another thread changes
    is defined and never panics."""
    shared = changing_mapping()
    tasks, errors = define_while_changing(shared, lambda: TASKS[kind](shared))

    assert errors == []
    assert [t.name for t in tasks] == ["t"] * ((N_THREADS - 1) * N_BUILDS)
