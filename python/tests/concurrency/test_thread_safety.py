"""Thread-safety of rivers objects shared between Python threads.

The failures these tests catch show up on free-threaded CPython (3.14t), where
threads run Python and rivers code at the same time.
"""

import threading

import pytest

import rivers as rs
from rivers.exceptions import AssetDefinitionError

N_THREADS = 16
CALLS_PER_THREAD = 2000
GRAPHS_PER_THREAD = 5


def run_threads(fn, n=N_THREADS):
    """Run ``fn(i)`` on ``n`` threads released together; return (results, errors)."""
    barrier = threading.Barrier(n)
    results = [None] * n
    errors = []

    def body(i):
        barrier.wait()
        try:
            results[i] = fn(i)
        except BaseException as e:  # PyO3 panics are BaseExceptions
            errors.append(f"{type(e).__name__}: {e}")

    threads = [threading.Thread(target=body, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, errors


def test_task_concurrent_direct_calls():
    @rs.Task
    def add_one(x):
        return x + 1

    results, errors = run_threads(
        lambda i: sum(add_one(i) for _ in range(CALLS_PER_THREAD))
    )

    assert errors == []
    assert results == [CALLS_PER_THREAD * (i + 1) for i in range(N_THREADS)]


def test_task_factory_decorates_many_functions():
    etl = rs.Task(tags=["etl"])

    @etl
    def extract():
        return "e"

    @etl
    def load():
        return "l"

    assert [(t.name, t.tags, t()) for t in (extract, load)] == [
        ("extract", ["etl"], "e"),
        ("load", ["etl"], "l"),
    ]
    assert etl.name is None


def test_task_factory_shared_across_threads():
    etl = rs.Task(tags=["etl"])

    def decorate(i):
        def body():
            return i

        body.__name__ = f"task_{i}"
        task = etl(body)
        return task.name, task.tags, task()

    results, errors = run_threads(decorate)

    assert errors == []
    assert results == [(f"task_{i}", ["etl"], i) for i in range(N_THREADS)]


ASSET_FACTORIES = {
    "single": lambda: rs.Asset(partitions_def="shared"),
    "multi": lambda: rs.Asset.from_multi(
        output_defs=[rs.AssetDef("out")], partitions_def="shared"
    ),
    "graph": lambda: rs.Asset.from_graph(partitions_def="shared"),
    "external": lambda: rs.Asset.external(
        io_handler=rs.InMemoryIOHandler(), partitions_def="shared"
    ),
}


def wrapped_fn(asset):
    return asset.observe_fn if asset.is_external else asset._asset_fn


@pytest.mark.parametrize("kind", ASSET_FACTORIES)
def test_asset_factory_decorates_many_functions(kind):
    factory = ASSET_FACTORIES[kind]()

    def first(): ...

    def second(): ...

    assets = [factory(first), factory(second)]

    assert [(type(a), a.name, a.partitions_def, wrapped_fn(a)) for a in assets] == [
        (type(factory), "first", "shared", first),
        (type(factory), "second", "shared", second),
    ]
    assert factory._name is None


def test_asset_factories_materialize_each_function():
    single = rs.Asset(group="etl")
    graph = rs.Asset.from_graph(group="etl")

    @single
    def source() -> int:
        return 3

    @single
    def other() -> int:
        return 5

    @rs.Task
    def double(source: int) -> int:
        return source * 2

    @rs.Task
    def triple(source: int) -> int:
        return source * 3

    @graph
    def doubled(source: int):
        return double(source)

    @graph
    def tripled(source: int):
        return triple(source)

    assets = [source, other, doubled, tripled]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[double, triple],
        default_executor=rs.Executor.in_process(),
    )

    assert repo.materialize().success
    assert {a.name: (a.group, repo.load_node(a.name)) for a in assets} == {
        "source": ("etl", 3),
        "other": ("etl", 5),
        "doubled": ("etl", 6),
        "tripled": ("etl", 9),
    }


@pytest.mark.parametrize("shared_factory", [False, True], ids=["fresh", "shared"])
def test_concurrent_graph_composition(shared_factory):
    @rs.Asset
    def source() -> int:
        return 3

    @rs.Task
    def double(source: int) -> int:
        return source * 2

    make_graph = rs.Asset.from_graph() if shared_factory else rs.Asset.from_graph

    def compose(i):
        graphs = []
        for k in range(GRAPHS_PER_THREAD):

            def body():
                return double(source())

            body.__name__ = f"g_{i}_{k}"
            graphs.append(make_graph(body))
        return graphs

    results, errors = run_threads(compose)

    assert errors == []
    graphs = [g for per_thread in results for g in per_thread]
    repo = rs.CodeRepository(
        assets=[source, *graphs],
        tasks=[double],
        default_executor=rs.Executor.in_process(),
    )
    assert repo.materialize().success
    assert {g.name: repo.load_node(g.name) for g in graphs} == {
        f"g_{i}_{k}": 6 for i in range(N_THREADS) for k in range(GRAPHS_PER_THREAD)
    }


@pytest.mark.parametrize("kind", ASSET_FACTORIES)
def test_wrapped_asset_called_outside_composition_raises(kind):
    def body(): ...

    asset = ASSET_FACTORIES[kind]()(body)

    with pytest.raises(AssetDefinitionError, match="already"):
        asset(lambda: None)
    assert wrapped_fn(asset) is body


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
