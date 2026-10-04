"""Thread-safety of AssetDef, which keeps its setters, while rivers reads it.

The races these tests catch show up on free-threaded CPython (3.14t).
"""

import threading

import pytest
from _threads import run_threads

import rivers as rs

BUILD_ROUNDS = 100
BUILDS_PER_THREAD = 3
SETS_PER_THREAD = 2000


def ingest_class():
    class Ingest(rs.MultiAsset):
        customers = rs.AssetDef()
        orders = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"customers": 10, "orders": 20}

    return Ingest


def test_class_form_multi_asset_keeps_asset_defs_unnamed():
    Ingest = ingest_class()

    repo = rs.CodeRepository(assets=[Ingest], default_executor=rs.Executor.in_process())

    assert repo.materialize().success
    assert {n: repo.load_node(n) for n in ("customers", "orders")} == {
        "customers": 10,
        "orders": 20,
    }
    assert (Ingest.customers.name, Ingest.orders.name) == (None, None)


def test_concurrent_repository_builds_from_one_multi_asset_class():
    @rs.Asset
    def total(customers: int, orders: int) -> int:
        return customers + orders

    def build_round(cls):
        def builds(i):
            repos = []
            for _ in range(BUILDS_PER_THREAD):
                repo = rs.CodeRepository(
                    assets=[cls, total], default_executor=rs.Executor.in_process()
                )
                repo.validate()  # `total` finds both outputs by name
                repos.append(repo)
            return repos

        return run_threads(builds)

    # A fresh class per round: a write into its AssetDefs on the first build
    # would race the other threads' builds.
    for _ in range(BUILD_ROUNDS):
        results, errors = build_round(ingest_class())
        assert errors == []

    repo = results[0][0]
    assert repo.materialize().success
    assert repo.load_node("total") == 30


# A setter that meets a read in progress raises PyO3's "Already borrowed"
# RuntimeError, and a read that meets a set raises rivers' RuntimeError: both
# are ordinary exceptions. A PanicException is a BaseException and fails.
CONTENDED = ("Already borrowed", "another thread")


def test_asset_def_setter_races_from_multi():
    ad = rs.AssetDef("out", tags=["a"])
    values = (["a"], ["b"])

    def race(i):
        reads = []
        for k in range(SETS_PER_THREAD):
            try:
                if i % 2:
                    ad.tags = values[k % 2]
                else:
                    asset = rs.Asset.from_multi(output_defs=[ad])
                    reads.append(asset.output_defs[0].tags)
            except RuntimeError as e:
                if not any(s in str(e) for s in CONTENDED):
                    raise
        return reads

    results, errors = run_threads(race)

    assert errors == []
    reads = [tags for per_thread in results[::2] for tags in per_thread]
    assert reads
    assert all(tags in values for tags in reads)


@pytest.mark.parametrize("read", ["from_multi", "eq"])
def test_asset_def_setter_while_rivers_runs_python(read):
    """rivers compares io_handlers (Python ``__eq__``) while it reads an
    AssetDef; a setter on another thread meanwhile must not be locked out."""
    in_eq, set_done = threading.Event(), threading.Event()

    class PausingHandler(rs.InMemoryIOHandler):
        def __eq__(self, other):
            in_eq.set()
            set_done.wait(timeout=5)
            return True

    ad = rs.AssetDef(
        "out",
        tags=["a"],
        io_handler=PausingHandler(),
        deps=[rs.AssetDef.input("raw", io_handler=PausingHandler())],
    )
    reads = {
        "from_multi": lambda: (
            rs.Asset.from_multi(
                output_defs=[ad],
                deps=[rs.AssetDef.input("raw", io_handler=PausingHandler())],
            )
            .output_defs[0]
            .tags
        ),
        "eq": lambda: ad == rs.AssetDef("out", tags=["a"], io_handler=PausingHandler()),
    }

    def set_tags():
        in_eq.wait(timeout=5)
        try:
            ad.tags = ["b"]
        finally:
            set_done.set()

    results, errors = run_threads(lambda i: set_tags() if i else reads[read](), n=2)

    assert errors == []
    assert in_eq.is_set()
    assert (results[0], ad.tags) == ({"from_multi": ["a"], "eq": True}[read], ["b"])
