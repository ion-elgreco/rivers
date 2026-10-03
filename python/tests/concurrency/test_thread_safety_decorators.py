"""Thread-safety of Task and Asset decorators shared between Python threads.

The races these tests catch show up on free-threaded CPython (3.14t), where
threads run Python and rivers code at the same time.
"""

import pytest
from _threads import N_THREADS, run_threads

import rivers as rs
from rivers.exceptions import AssetDefinitionError, TaskDefinitionError

CALLS_PER_THREAD = 2000
GRAPHS_PER_THREAD = 5


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


DECORATORS = {
    **{kind: (make, AssetDefinitionError) for kind, make in ASSET_FACTORIES.items()},
    "task": (lambda: rs.Task(tags=["etl"]), TaskDefinitionError),
}


def func(): ...


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [((), {}), ((42,), {}), ((func, func), {}), ((func,), {"name": "x"})],
    ids=["no_args", "non_callable", "two_functions", "keyword"],
)
@pytest.mark.parametrize("kind", DECORATORS)
def test_decorator_needs_one_function(kind, args, kwargs):
    make, error = DECORATORS[kind]

    with pytest.raises(error, match="decorator needs one function"):
        make()(*args, **kwargs)
