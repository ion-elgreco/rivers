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
import os
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
