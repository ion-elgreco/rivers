"""Class-form assets across the loky process boundary.

The regression suite for the io-handler ref fix: a well-formed
`IOHandlerRef("mod", "Class.method")` used to reconstruct to None in the
worker ('NoneType' object has no attribute 'load_input') because only the
decorator form rebinds the module-level name to the asset object. The fix is
a parent-side shippability probe plus a one-segment parent walk in the child.

Every topology here is 2 steps per level — one sync step per level
short-circuits to InProcess and never touches loky transport.
"""

import importlib
import json
import os
import pickle
import subprocess
import sys
import textwrap
from pathlib import Path

import obstore.store
import pytest

import rivers as rs

MP = rs.Executor.parallel(max_workers=2)


@pytest.fixture(scope="module")
def class_mod(tmp_path_factory):
    """Import the helper module with store dir + PYTHONPATH visible to loky children."""
    mp = pytest.MonkeyPatch()
    store_dir = tmp_path_factory.mktemp("class_store")
    tests_dir = Path(__file__).resolve().parents[1]
    mp.setenv("RIVERS_TEST_CLASS_STORE", str(store_dir))
    existing = os.environ.get("PYTHONPATH")
    pythonpath = f"{tests_dir}{os.pathsep}{existing}" if existing else str(tests_dir)
    mp.setenv("PYTHONPATH", pythonpath)
    # Warm loky children spawned by earlier tests froze their env at spawn and
    # would import the helper module without the vars above — force a fresh pool.
    from loky import get_reusable_executor

    get_reusable_executor().shutdown(kill_workers=True)
    mod = importlib.import_module("executor.class_assets_helpers")
    mod = importlib.reload(mod)  # rebind classes to the fresh store dir
    yield mod
    mp.undo()


def test_loky_topology_crosses_process_boundary(class_mod):
    """Guard the suite's premise: 2-wide levels escape the InProcess shortcut."""
    m = class_mod
    repo = rs.CodeRepository(assets=[m.PidLeft, m.PidRight], default_executor=MP)
    repo.materialize()
    assert repo.load_node("pid_left") != os.getpid()
    assert repo.load_node("pid_right") != os.getpid()


def test_loky_class_assets_by_reference(class_mod):
    """Class-form assets ship handlers by reference through real loky."""
    m = class_mod
    repo = rs.CodeRepository(assets=[m.CA, m.CB, m.CC, m.CD], default_executor=MP)
    repo.materialize()
    assert repo.load_node("cc") == 11
    assert repo.load_node("cd") == 22


def test_loky_decorator_control_group(class_mod):
    """The decorator-form by-reference path is unchanged."""
    m = class_mod
    repo = rs.CodeRepository(assets=[m.da, m.db, m.dc, m.dd], default_executor=MP)
    repo.materialize()
    assert repo.load_node("dc") == 11
    assert repo.load_node("dd") == 22


def test_loky_inherited_materialize_rebinds_cls(class_mod):
    """A verb inherited from a template base binds cls to the subclass in the worker."""
    m = class_mod
    repo = rs.CodeRepository(assets=[m.SeedOne, m.SeedTwo], default_executor=MP)
    repo.materialize()
    assert repo.load_node("seed_one") == 100
    assert repo.load_node("seed_two") == 200


def test_loky_multi_class_asset_per_output_handlers(class_mod):
    """Per-output AssetDef handlers have no import path — the probe falls back
    to shipping the raw handler instead of a ref that reconstructs to None."""
    m = class_mod
    repo = rs.CodeRepository(assets=[m.MIngest, m.MSide], default_executor=MP)
    repo.materialize()
    assert repo.load_node("m_left") == 5
    assert repo.load_node("m_right") == 6
    assert repo.load_node("m_side") == 7


def test_loky_multi_class_asset_output_override_beats_class_handler(class_mod):
    """A per-output AssetDef handler wins over the class-level one.

    Both outputs ship the same `Class.materialize` callable, so a ref built
    from it reconstructs to the *class* handler — right for `mo_plain`, wrong
    for `mo_special`, whose data would land in the class-level store.
    """
    m = class_mod
    repo = rs.CodeRepository(assets=[m.MOverride, m.MSide], default_executor=MP)
    repo.materialize()
    assert repo.load_node("mo_special") == 1
    assert repo.load_node("mo_plain") == 2


def test_loky_local_class_assets_fall_back_to_pickle(tmp_path):
    """Classes defined inside a function (<locals> qualname) can't ship by
    reference; the whole chain falls back to cloudpickle by value."""
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )

    class LA(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 1

    class LB(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 2

    class LC(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls, la: int) -> int:
            return la + 10

    class LD(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls, lb: int) -> int:
            return lb + 20

    repo = rs.CodeRepository(assets=[LA, LB, LC, LD], default_executor=MP)
    repo.materialize()
    assert repo.load_node("lc") == 11
    assert repo.load_node("ld") == 22


@pytest.mark.parametrize(
    ("attr", "node", "expected"),
    [
        ("AliasedVerb", "aliased_verb", 30),
        ("factory_made", "factory_made", 41),
        ("NoWrapsVerb", "no_wraps_verb", 50),
    ],
    ids=["classmethod-alias", "type-factory", "no-wraps-decorator"],
)
def test_loky_unimportable_verb_path_ships_by_value(class_mod, attr, node, expected):
    """A verb whose `Owner.<__name__>` path is not a module attribute ships by
    value. A FuncRef built from that path fails while the worker unpickles the
    call, so loky marks the pool broken and the sibling step fails too."""
    m = class_mod
    repo = rs.CodeRepository(assets=[getattr(m, attr), m.CA], default_executor=MP)
    repo.materialize()
    assert repo.load_node(node) == expected
    assert repo.load_node("ca") == 10


def test_loky_async_class_assets_coexist(class_mod):
    """Async bodies run on the orchestrator while sync siblings cross loky —
    the submit-before-async ordering must hold for class-form assets too."""
    m = class_mod
    repo = rs.CodeRepository(
        assets=[m.AsyncLeft, m.AsyncRight, m.PidLeft, m.PidRight],
        default_executor=MP,
    )
    repo.materialize()
    assert repo.load_node("async_left") == 5
    assert repo.load_node("async_right") == 7
    assert repo.load_node("pid_left") != os.getpid()


def test_loky_graph_asset_by_reference(class_mod):
    """A graph asset's 2-wide seed level ships by reference; the composed
    task's output lands like any other asset."""
    m = class_mod
    repo = rs.CodeRepository(
        assets=[m.g_seed, m.g_seed_b, m.GLokyPipeline],
        tasks=[m.g_add_one],
        default_executor=MP,
    )
    repo.materialize()
    assert repo.load_node("g_loky_pipeline") == 4
    assert repo.load_node("g_seed_b") == 4


@pytest.fixture
def worker_module(tmp_path, monkeypatch):
    """Import a module from a file under ``tmp_path`` that loky workers import too."""
    from loky import get_reusable_executor

    existing = os.environ.get("PYTHONPATH")
    pythonpath = f"{tmp_path}{os.pathsep}{existing}" if existing else str(tmp_path)
    monkeypatch.setenv("PYTHONPATH", pythonpath)
    monkeypatch.syspath_prepend(str(tmp_path))
    # Warm workers froze PYTHONPATH at spawn.
    get_reusable_executor().shutdown(kill_workers=True)
    names = []

    def load(source: str):
        name = f"worker_mod_{tmp_path.name}_{len(names)}"
        (tmp_path / f"{name}.py").write_text(source)
        names.append(name)
        importlib.invalidate_caches()
        return importlib.import_module(name)

    yield load
    get_reusable_executor().shutdown(kill_workers=True)
    for name in names:
        sys.modules.pop(name, None)


def _pickle_handler(root: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(root), mkdir=True))


def _stored(root: Path) -> dict:
    """What a PickleIOHandler holds under ``root``, by asset path."""
    return {
        p.relative_to(root).with_suffix("").as_posix(): pickle.loads(p.read_bytes())
        for p in sorted(root.rglob("*.pkl"))
    }


# Module-level assets that name their io_handler by resource key. resolve()
# swaps the key for the handler on these same objects in the parent, while a
# loky worker imports the module fresh and holds only the key.
KEY_HANDLER_MODULE = textwrap.dedent(
    """
    import rivers as rs


    @rs.Asset(io_handler="warehouse")
    def orders() -> list:
        return [3, 4]


    @rs.Asset(io_handler="warehouse")
    def customers() -> list:
        return ["ada", "bob"]


    @rs.Asset(io_handler="warehouse")
    def order_total(orders: list) -> int:
        return sum(orders)


    @rs.Asset(io_handler="warehouse")
    def customer_count(customers: list) -> int:
        return len(customers)


    @rs.Asset.external(io_handler="warehouse")
    def feed():
        return rs.Observation(data_version="v1")


    @rs.Asset(io_handler="warehouse")
    def feed_total(feed: list) -> int:
        return sum(feed)


    @rs.Asset(io_handler="warehouse")
    def feed_count(feed: list) -> int:
        return len(feed)


    @rs.Task
    def left() -> int:
        return 3


    @rs.Task
    def right() -> int:
        return 4


    @rs.Task
    def add(a: int, b: int) -> int:
        return a + b


    @rs.Asset.from_graph(io_handler="warehouse", node_io_handler="scratch")
    def total():
        return add(left(), right())


    @rs.Task(io_handler="warehouse")
    def tally() -> list:
        return [5, 6]


    @rs.Task(io_handler="warehouse")
    def roster() -> list:
        return ["cy", "di"]


    @rs.Asset(io_handler="warehouse")
    def tally_total(tally: list) -> int:
        return sum(tally)


    @rs.Asset(io_handler="warehouse")
    def roster_count(roster: list) -> int:
        return len(roster)
    """
)


@pytest.mark.parametrize(
    ("selection", "seed", "expected"),
    [
        (
            ["orders", "customers"],
            {},
            {"customers": ["ada", "bob"], "orders": [3, 4]},
        ),
        (
            ["orders", "customers", "order_total", "customer_count"],
            {},
            {
                "customer_count": 2,
                "customers": ["ada", "bob"],
                "order_total": 7,
                "orders": [3, 4],
            },
        ),
        (
            ["feed_total", "feed_count"],
            {"feed": [1, 2, 3]},
            {"feed": [1, 2, 3], "feed_count": 3, "feed_total": 6},
        ),
    ],
    ids=["one-level", "downstream-reads", "external-upstream"],
)
def test_loky_resource_key_io_handler_ships_resolved_handler(
    worker_module, tmp_path, selection, seed, expected
):
    """An io_handler named by resource key crosses loky as the resolved handler.

    The parent probe saw the resolved handler on the module-level asset and
    shipped an IOHandlerRef, which the worker's fresh import rebuilt to None:
    the write was skipped while the run succeeded, and a load hit None.
    """
    m = worker_module(KEY_HANDLER_MODULE)
    warehouse = tmp_path / "warehouse"
    handler = _pickle_handler(warehouse)
    for name, value in seed.items():
        (warehouse / f"{name}.pkl").write_bytes(pickle.dumps(value))
    names = [
        "orders",
        "customers",
        "order_total",
        "customer_count",
        "feed",
        "feed_total",
        "feed_count",
    ]
    repo = rs.CodeRepository(
        assets=[getattr(m, n) for n in names],
        resources={"warehouse": handler},
        default_executor=MP,
    )
    repo.materialize(selection=selection)
    assert _stored(warehouse) == expected


def test_loky_resource_key_node_io_handler_ships_resolved_handler(
    worker_module, tmp_path
):
    """A graph's node_io_handler named by resource key reaches its 2-wide inner
    level; the worker's fresh import has no handler under either key."""
    m = worker_module(KEY_HANDLER_MODULE)
    warehouse, scratch = tmp_path / "warehouse", tmp_path / "scratch"
    repo = rs.CodeRepository(
        assets=[m.total],
        tasks=[m.left, m.right, m.add],
        resources={
            "warehouse": _pickle_handler(warehouse),
            "scratch": _pickle_handler(scratch),
        },
        default_executor=MP,
    )
    repo.materialize()
    assert _stored(scratch) == {"total/add": 7, "total/left": 3, "total/right": 4}
    assert _stored(warehouse) == {"total": 7}


def test_loky_resource_key_task_io_handler_ships_resolved_handler(
    worker_module, tmp_path
):
    """A module-level task's io_handler named by resource key crosses loky as
    the resolved handler, for the task's write and for the downstream load."""
    m = worker_module(KEY_HANDLER_MODULE)
    warehouse = tmp_path / "warehouse"
    repo = rs.CodeRepository(
        assets=[m.tally_total, m.roster_count],
        tasks=[m.tally, m.roster],
        resources={"warehouse": _pickle_handler(warehouse)},
        default_executor=MP,
    )
    repo.materialize()
    assert _stored(warehouse) == {
        "roster": ["cy", "di"],
        "roster_count": 2,
        "tally": [5, 6],
        "tally_total": 11,
    }


# Only the parent's import builds the handler, as a handler read from
# process-local state would; the worker's fresh import has none.
PARENT_ONLY_HANDLER_MODULE = """
import os

import obstore.store

import rivers as rs

_handler = (
    rs.PickleIOHandler(store=obstore.store.LocalStore({store!r}, mkdir=True))
    if os.getpid() == {pid}
    else None
)


@rs.Asset(io_handler=_handler)
def orders() -> list:
    return [3, 4]


@rs.Asset(io_handler=_handler)
def customers() -> list:
    return ["ada", "bob"]
"""


def test_loky_handler_ref_without_worker_handler_fails_step(worker_module, tmp_path):
    """A shipped handler ref that finds no handler in the worker fails the step
    and names the asset and the ref, instead of skipping the write."""
    store = tmp_path / "store"
    m = worker_module(
        PARENT_ONLY_HANDLER_MODULE.format(store=str(store), pid=os.getpid())
    )
    repo = rs.CodeRepository(assets=[m.orders, m.customers], default_executor=MP)
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    errors = dict(result.failed_assets)
    assert sorted(errors) == ["customers", "orders"]
    for name, error in errors.items():
        assert (
            f"asset '{name}': io_handler reference {m.__name__}.{name} "
            "found no handler in the worker"
        ) in error
    assert _stored(store) == {}


# `__main__` is not importable in a loky worker, so every class below ships by
# value with its whole class body, declarations included.
PIPELINE_SCRIPT = textwrap.dedent(
    """
    import json
    import pathlib
    import sys

    import obstore.store

    import rivers as rs

    root = pathlib.Path(sys.argv[1])
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(root / "store"), mkdir=True)
    )


    def record_success(context: rs.HookContext):
        (root / "hook_ran").write_text(context.asset_name)


    class Orders(rs.MultiAsset):
        io_handler = handler
        orders = rs.AssetDef()
        returns = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"orders": 10, "returns": 2}


    class Regions(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 3


    class Customers(rs.Asset):
        io_handler = handler
        automation_condition = rs.AutomationCondition.eager()
        deps = [rs.AssetDef.dep("regions")]
        hooks = [rs.Hook.success(record_success)]
        compute = rs.Compute(cpu="1", memory="512Mi")

        @classmethod
        def materialize(cls, orders: int) -> int:
            return orders * 2


    class ReturnRate(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls, returns: int) -> int:
            return returns + 100


    if __name__ == "__main__":
        repo = rs.CodeRepository(
            assets=[Orders, Regions, Customers, ReturnRate],
            default_executor=rs.Executor.parallel(max_workers=2),
        )
        repo.materialize()
        names = ["orders", "returns", "regions", "customers", "return_rate"]
        print(json.dumps({n: repo.load_node(n) for n in names}))
    """
)


def test_script_class_assets_with_declarations_run_on_parallel(tmp_path):
    """`python pipeline.py` with class-form assets that carry AssetDef outputs,
    an automation condition, deps, hooks and compute. Both levels are 2 wide,
    so every step crosses the loky boundary."""
    script = tmp_path / "pipeline.py"
    script.write_text(PIPELINE_SCRIPT)
    result = subprocess.run(
        [sys.executable, str(script), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=50,
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    printed = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert json.loads(printed[-1]) == {
        "orders": 10,
        "returns": 2,
        "regions": 3,
        "customers": 20,
        "return_rate": 102,
    }
    assert (tmp_path / "hook_ran").read_text() == "customers"


# A template whose verb reads module globals that must stay in its module: a
# lock cannot be pickled, and a large constant must not be copied per step.
SNAPSHOT_TEMPLATE_MODULE = textwrap.dedent(
    """
    import pathlib
    import sys
    import threading

    import obstore.store

    import rivers as rs

    _LOCK = threading.Lock()
    _ROWS = {"users": ["ada", "bob"], "orders": [3, 4, 5]}
    _PADDING = "x" * 5_000_000


    class TableSnapshot(rs.Asset):
        io_handler = rs.PickleIOHandler(
            store=obstore.store.LocalStore(
                str(pathlib.Path(__file__).parent / "store"), mkdir=True
            )
        )
        table = ""

        @classmethod
        def materialize(cls) -> dict:
            with _LOCK:
                return {
                    "rows": _ROWS[cls.table],
                    "padding_from_module": _PADDING is sys.modules[__name__]._PADDING,
                }
    """
)

SNAPSHOT_PIPELINE_SCRIPT = textwrap.dedent(
    """
    import json

    import rivers as rs
    from snapshots import TableSnapshot


    class UsersSnapshot(TableSnapshot):
        table = "users"


    class OrdersSnapshot(TableSnapshot):
        table = "orders"


    if __name__ == "__main__":
        repo = rs.CodeRepository(
            assets=[UsersSnapshot, OrdersSnapshot],
            default_executor=rs.Executor.parallel(max_workers=2),
        )
        repo.materialize()
        names = ["users_snapshot", "orders_snapshot"]
        print(json.dumps({n: repo.load_node(n) for n in names}))
    """
)

SNAPSHOTS = {
    "users_snapshot": {"rows": ["ada", "bob"], "padding_from_module": True},
    "orders_snapshot": {"rows": [3, 4, 5], "padding_from_module": True},
}


def test_script_subclasses_of_imported_template_run_on_parallel(tmp_path):
    """`python pipeline.py` whose subclasses inherit `materialize` from a
    template in an importable module. The worker finds the verb through the
    subclass, so the template's function is not pickled with the globals it
    reads: the lock would fail both steps, the constant would ship with each."""
    (tmp_path / "snapshots.py").write_text(SNAPSHOT_TEMPLATE_MODULE)
    script = tmp_path / "pipeline.py"
    script.write_text(SNAPSHOT_PIPELINE_SCRIPT)
    existing = os.environ.get("PYTHONPATH")
    pythonpath = f"{tmp_path}{os.pathsep}{existing}" if existing else str(tmp_path)
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=50,
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": pythonpath},
    )
    assert result.returncode == 0, result.stderr[-4000:]
    printed = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert json.loads(printed[-1]) == SNAPSHOTS


def test_loky_local_subclasses_of_imported_template(worker_module):
    """The same template, subclassed inside a function."""
    m = worker_module(SNAPSHOT_TEMPLATE_MODULE)

    class UsersSnapshot(m.TableSnapshot):
        table = "users"

    class OrdersSnapshot(m.TableSnapshot):
        table = "orders"

    repo = rs.CodeRepository(
        assets=[UsersSnapshot, OrdersSnapshot], default_executor=MP
    )
    repo.materialize()
    assert {name: repo.load_node(name) for name in SNAPSHOTS} == SNAPSHOTS


class _LabelHandler(rs.BaseIOHandler):
    """A picklable handler that compares by value."""

    label: str = ""

    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


def _optimize(ctx):
    return None


def _alert(context):
    return None


class TestDeclarationPickling:
    """Each declaration type round-trips through pickle with every field set."""

    @pytest.mark.parametrize(
        ("io_handler", "partitions_def"),
        [
            (_LabelHandler(label="orders"), rs.PartitionsDefinition.static_(["eu"])),
            ("warehouse", "regions"),
        ],
        ids=["inline", "by-name"],
    )
    def test_asset_def(self, io_handler, partitions_def):
        ad = rs.AssetDef(
            name="orders",
            tags=["core"],
            kinds=["delta", "table"],
            group="sales",
            code_version="v2",
            io_handler=io_handler,
            metadata={"owner": "data"},
            partitions_def=partitions_def,
            partition_mapping={"raw": rs.PartitionMapping.time_window(-1)},
            pool=["db", "api"],
            pool_slots={"db": 3},
            deps=[rs.AssetDef.input("raw", metadata={"k": "v"})],
            actions=[
                rs.AssetAction(
                    name="optimize", outcome=rs.Outcome.Unchanged, description="compact"
                )(_optimize)
            ],
        )
        restored = pickle.loads(pickle.dumps(ad))
        assert restored == ad
        assert restored.kinds == ["delta", "table"]
        assert restored.pool == [("db", 3), ("api", 1)]
        assert restored.partitions_def == partitions_def
        assert restored.partition_mapping == {
            "raw": rs.PartitionMapping.time_window(-1)
        }
        assert [(d.name, d.is_input, d.metadata) for d in restored.deps] == [
            ("raw", True, {"k": "v"})
        ]
        assert [(a.name, a.outcome, a.description) for a in restored.actions] == [
            ("optimize", rs.Outcome.Unchanged, "compact")
        ]

    def test_unnamed_asset_def(self):
        restored = pickle.loads(pickle.dumps(rs.AssetDef()))
        assert restored.name is None
        assert restored == rs.AssetDef()

    @pytest.mark.parametrize(
        "dep",
        [
            rs.AssetDef.input(
                "raw",
                partition_mapping=rs.PartitionMapping.time_window(-1),
                io_handler=_LabelHandler(label="raw"),
                metadata={"k": "v"},
            ),
            rs.AssetDef.dep(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            ),
        ],
        ids=["input", "lineage"],
    )
    def test_dep_def(self, dep):
        restored = pickle.loads(pickle.dumps(dep))
        assert repr(restored) == repr(dep)
        assert (
            restored.name,
            restored.is_input,
            restored.metadata,
            restored.partition_mapping,
        ) == (dep.name, dep.is_input, dep.metadata, dep.partition_mapping)

    @pytest.mark.parametrize(
        "hook",
        [rs.Hook.success(_alert), rs.Hook.failure(_alert, name="page")],
        ids=["success", "failure"],
    )
    def test_hook(self, hook):
        restored = pickle.loads(pickle.dumps(hook))
        assert type(restored) is type(hook)
        assert restored.name == hook.name
        with pytest.raises(Exception, match="already bound"):
            restored(_alert)

    def test_compute(self):
        restored = pickle.loads(
            pickle.dumps(rs.Compute(cpu="2", memory="4Gi", gpu="1"))
        )
        assert (restored.cpu, restored.memory, restored.gpu) == ("2", "4Gi", "1")

    @pytest.mark.parametrize(
        "condition",
        [
            (
                rs.AutomationCondition.on_cron("0 6 * * *", timezone="Europe/Amsterdam")
                & ~rs.AutomationCondition.last_executed_with_tags(
                    tag_values=[("team", "data")]
                )
            ).with_label("nightly"),
            rs.AutomationCondition.eager() | rs.AutomationCondition.missing(),
        ],
        ids=["labelled", "unlabelled"],
    )
    def test_automation_condition(self, condition):
        restored = pickle.loads(pickle.dumps(condition))
        assert restored.label == condition.label
        assert restored.description == condition.description
        assert [c.description for c in restored.children] == [
            c.description for c in condition.children
        ]
