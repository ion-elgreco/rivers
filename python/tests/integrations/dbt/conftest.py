"""The ``shop`` dbt project: parsed once per session, copied for each test."""

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

import obstore.store
import pytest

import rivers as rs


@pytest.fixture(scope="session")
def parsed_shop(tmp_path_factory) -> Path:
    """The project with its manifest and the selections of ``_shop`` cached."""
    for module in ("dbt.version", "duckdb", "pyarrow"):
        pytest.importorskip(module)
    from rivers.integrations.dbt import DbtProject

    from ._shop import SHOP, daily_assets, shop_assets, write_profile

    root = tmp_path_factory.mktemp("parsed") / "shop"
    shutil.copytree(SHOP, root)
    write_profile(root)
    DbtProject(root, profiles_dir=root).prepare()
    shop_assets(root)
    daily_assets(root)
    return root


@pytest.fixture
def shop(parsed_shop, tmp_path) -> Path:
    """A copy of the parsed project, with its warehouse in ``tmp_path``."""
    from ._shop import write_profile

    root = tmp_path / "shop"
    shutil.copytree(parsed_shop, root)
    write_profile(root)
    return root


class Executor(NamedTuple):
    executor: rs.Executor
    io_handler: Callable[[], rs.BaseIOHandler]
    in_worker: bool
    """Whether a step that shares its level with another runs in a worker process."""


@pytest.fixture(params=["in_process", "parallel"])
def run_with(request, tmp_path) -> Executor:
    """An executor, and a picklable IO handler that the parallel executor needs on every step."""
    store = obstore.store.LocalStore(str(tmp_path / "store"), mkdir=True)
    io_handler = lambda: rs.PickleIOHandler(store=store)  # noqa: E731
    if request.param == "in_process":
        return Executor(rs.Executor.in_process(), io_handler, False)
    return Executor(rs.Executor.parallel(max_workers=2), io_handler, True)
