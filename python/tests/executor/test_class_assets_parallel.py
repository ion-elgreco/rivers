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
